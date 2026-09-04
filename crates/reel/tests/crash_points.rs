//! Crash point enumeration for the reel store
//!
//! Representative streams are driven over the simulator and crashed before every io
//! boundary in turn, then reopened, and after each reopen the constant time counters
//! must equal a from scratch scan of what the reel serves. Torn footers and partial
//! seals are out of reach here, since a footer is only written by an explicit seal the
//! public engine surface does not expose.

#[allow(dead_code)]
mod harness;

use std::collections::{BTreeMap, BTreeSet};

use reel::format::column::RecordKey;
use reel::format::record::{BatchFrame, RecordHeader, HEADER_LEN};
use reel::io::fault::{FaultKind, FaultPlan};
use reel::io::sim_backend::{DurableImage, SimIo};
use reel::{
    ByteCount, CompactPass, CompactRate, Preallocate, RecordWrite, ReelConfig, ReelStore,
    RepairPath, ShardShapes, SyncPolicy, ThreadBudget, SEGMENT_SUFFIX,
};
use reel_core::{Store, Value};
use reel_mock::MemoryStore;

use harness::fixture::OPEN_COLUMNS;
use harness::observe::observe;
use harness::op_stream::{self, StreamOp};
use harness::reel_harness::{assert_recount, flip_largest_segment, ReelHarness};
use harness::wire::{apply_mutation, framed_value, group_prefix, wire_key, RECORDS, RECORDS_CF};

/// Group most targeted tests write into
const GROUP: u16 = 7;

/// Seeds the general enumeration draws its streams from
///
/// Every boundary of a stream is crashed at and each crash replays the stream from the
/// start, so a stream costs the square of its length while a seed costs one stream.
const CRASH_SEEDS: &[u64] = &[1, 42, 7, 1337];

/// Length of each general enumeration stream, quadratic in what it costs to raise
const CRASH_LEN: usize = 5;

/// Seeds the multi tail enumerations draw their streams from
///
/// At four tails the variable is the configuration rather than the stream: what four
/// tails add is the version guard ordering records several appenders committed
/// independently.
const MULTI_TAIL_SEEDS: &[u64] = &[1, 42];

/// Seeds the index checkpoint enumeration draws its stream from
///
/// One, because the checkpoint is most of the io the stream crosses: a cue seals every
/// tail and the file is written, synced and published, and every boundary is a replay.
const CHECKPOINT_SEEDS: &[u64] = &[1];

/// Seeds the scatter enumeration draws its streams from, narrower because scatter
/// already replays every stream at two sector sizes
const SCATTER_SEEDS: &[u64] = &[1, 42];

/// Segment size that rolls a few times over a short stream
const SEGMENT_SMALL: u64 = 16 * 1024;

/// Segment size large enough that a short run never rolls
const SEGMENT_LARGE: u64 = 1024 * 1024;

/// Space reserved ahead of the write head per allocation step
const ALLOC_CHUNK: u64 = 4 * 1024;

/// A small payload for the targeted streams
const SMALL_PAYLOAD: usize = 200;

/// A payload large enough to dominate a tight segment
const LARGE_PAYLOAD: usize = 20_000;

/// Segment size that rolls right after one large record
const SEGMENT_TIGHT: u64 = 24 * 1024;

/// First op position the sync error search schedules at
const SYNC_ERROR_FROM: u64 = 4;

/// Last op position the sync error search schedules at
const SYNC_ERROR_TO: u64 = 20;

/// First op position the out of space fault is scheduled at
const ENOSPC_FROM: u64 = 8;

/// Last op position the out of space fault is scheduled at
const ENOSPC_TO: u64 = 24;

/// Overwrites of one key for the multi tail stream
const OVERWRITE_COUNT: u8 = 6;

/// Payload length every version of the multi tail key carries
const SAME_KEY_LEN: usize = 200;

/// Segment size that holds several compaction records before it rolls
///
/// The fill records plus the segment header reach this size, so the first overwrite is
/// what rolls the segment, leaving it half shadowed and worth compacting.
const COMPACT_SEG_BYTES: u64 = 20 * 1024;

/// Payload length each compaction record carries
const COMPACT_PAYLOAD: usize = 3000;

/// Records written into the compaction source segment before overwrites
const COMPACT_FILL: u8 = 4;

/// Segment size the merge stream rolls a run out of
const MERGE_SEG_BYTES: u64 = 20 * 1024;

/// Keys the merge stream writes per round
const MERGE_KEYS: u8 = 6;

/// Payload each of those keys carries
const MERGE_PAYLOAD: usize = 900;

/// The key the merge stream deletes, which is the resurrection gate
const MERGE_DELETED: u8 = 3;

/// The keys that must still read back after a crash inside the merge
const MERGE_KEPT: &[u8] = &[1, 2, 4, 5, 6];

/// Rewrite passes the merge setup drives before it gives up on the volume settling
const MERGE_SETTLE_PASSES: u32 = 64;

/// Keys the sub group range delete stream writes before it deletes
const RANGE_KEYS: u8 = 9;

/// First address the sub group range delete covers
const RANGE_LO: u8 = 3;

/// First address past the sub group range delete
const RANGE_HI: u8 = 7;

/// Sector sizes a scattered crash is replayed at
///
/// At block size a hole takes a whole aligned block, so a header and whatever packs in
/// behind it go together. The smaller size is where the model gets stronger than a torn
/// prefix: a hole can drop a payload while keeping its header, or land inside a payload.
const SCATTER_SECTORS: [u32; 2] = [512, 4096];

fn crash_config(active_tails: u32, sync: SyncPolicy, segment_bytes: u64) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(segment_bytes),
        alloc_chunk: ByteCount::from_bytes(ALLOC_CHUNK),
        preallocate: Preallocate::Chunk,
        sync,
        active_tails: ThreadBudget::threads(active_tails),
        ..ReelConfig::default()
    }
}

fn put(group: u16, address: u8, len: usize, fill: u8) -> StreamOp {
    StreamOp::Put {
        group,
        address,
        len,
        fill,
    }
}

fn enumerate(config: ReelConfig, ops: &[StreamOp], seed: u64, durable: bool) {
    enumerate_over(ReelHarness::new(config), ops, seed, durable);
}

fn enumerate_over(harness: ReelHarness, ops: &[StreamOp], seed: u64, durable: bool) {
    let total = harness.boundary_count(ops);
    assert!(total > 0, "the stream crosses no io boundary");

    for crash_at in 0..total {
        let (sim, acknowledged) = harness.run(FaultPlan::new(seed).with_crash(crash_at), ops);
        let reopened = harness.reopen(sim.durable_image());
        assert_recount(&reopened, crash_at);
        if durable {
            assert_durable_prefix(&reopened, ops, acknowledged, crash_at);
        }
    }
}

// every crash boundary of a mixed stream reproduces the durable prefix at one tail
#[test]
fn every_boundary_single_tail() {
    for seed in CRASH_SEEDS {
        let ops = op_stream::generate_durable(*seed, CRASH_LEN);
        enumerate(
            crash_config(1, SyncPolicy::EveryPut, SEGMENT_SMALL),
            &ops,
            *seed,
            true,
        );
    }
}

// every crash boundary at four tails reproduces the version ordered durable prefix
#[test]
fn every_boundary_multi_tail() {
    for seed in MULTI_TAIL_SEEDS {
        let ops = op_stream::generate_durable(*seed, CRASH_LEN);
        enumerate(
            crash_config(4, SyncPolicy::EveryPut, SEGMENT_SMALL),
            &ops,
            *seed,
            true,
        );
    }
}

// every crash boundary reproduces the durable prefix into open-addressed shards
//
// The other side of the reopen: a rebuild lands each shard's keys in one bulk pass, and
// crashing mid-write makes that pass absorb a different run at every boundary.
#[test]
fn every_boundary_open_shards() {
    for seed in MULTI_TAIL_SEEDS {
        let ops = op_stream::generate_durable(*seed, CRASH_LEN);
        let config = ReelConfig {
            shard_shapes: ShardShapes::Declared,
            ..crash_config(1, SyncPolicy::EveryPut, SEGMENT_SMALL)
        };
        enumerate_over(
            ReelHarness::with_columns(config, OPEN_COLUMNS),
            &ops,
            *seed,
            true,
        );
    }
}

// every crash boundary of a stream that writes its index down reproduces the prefix
//
// A crash mid-write leaves a half file the reopen must refuse, and a crash after it
// leaves a whole file describing a volume the rest of the stream has moved past, so the
// segments it vouches for have to be exactly the ones still standing unchanged.
#[test]
fn every_boundary_across_an_index_checkpoint() {
    for seed in CHECKPOINT_SEEDS {
        let ops = op_stream::generate_durable(*seed, CRASH_LEN);
        let config = crash_config(1, SyncPolicy::EveryPut, SEGMENT_SMALL);
        let harness = ReelHarness::new(config);
        let after = ops.len() / 2;
        let total = harness.boundary_count_across_index_checkpoint(&ops, after);
        assert!(total > 0, "the stream crosses no io boundary");

        for crash_at in 0..total {
            let plan = FaultPlan::new(*seed).with_crash(crash_at);
            let (sim, acknowledged) = harness.run_across_index_checkpoint(plan, &ops, after);
            let reopened = harness.reopen(sim.durable_image());
            assert_recount(&reopened, crash_at);
            assert_durable_prefix(&reopened, &ops, acknowledged, crash_at);
        }
    }
}

// under the never policy a crash keeps or drops the last records, both consistent
#[test]
fn every_boundary_never() {
    for seed in CRASH_SEEDS {
        let ops = op_stream::generate_durable(*seed, CRASH_LEN);
        enumerate(
            crash_config(1, SyncPolicy::Never, SEGMENT_SMALL),
            &ops,
            *seed,
            false,
        );
    }
}

// every crash boundary reproduces the acknowledged prefix when sectors scatter
//
// A device does not lose a clean suffix: what it had not committed comes back in
// whatever order it scheduled, so the image can hold a fresh sector after a stale one.
#[test]
fn scattered_crash_keeps_the_durable_prefix() {
    for (seed, sector) in seeds_and_sectors() {
        let ops = op_stream::generate_durable(seed, CRASH_LEN);
        let harness = ReelHarness::new(crash_config(1, SyncPolicy::EveryPut, SEGMENT_SMALL));
        let total = harness.boundary_count(&ops);
        assert!(total > 0, "the stream crosses no io boundary");

        let mut at_risk = 0u64;
        for crash_at in 0..total {
            let plan = FaultPlan::new(seed)
                .with_crash(crash_at)
                .with_scatter(sector);
            let (sim, acknowledged) = harness.run(plan, &ops);
            at_risk = at_risk.max(sim.unsynced_bytes());
            let reopened = harness.reopen(sim.durable_image());
            assert_recount(&reopened, crash_at);
            assert_only_written_values(&reopened, &ops, crash_at);
            assert_durable_prefix(&reopened, &ops, acknowledged, crash_at);
        }
        assert!(
            at_risk > 0,
            "no boundary left unsynced bytes, so nothing scattered"
        );
    }
}

// a scattered crash with the hot path unsynced still reopens self consistent
//
// Nothing was promised durable here, so records may be missing in any pattern. What may
// not happen is the reel disagreeing with itself about what it serves.
#[test]
fn scattered_crash_stays_consistent() {
    for (seed, sector) in seeds_and_sectors() {
        let ops = op_stream::generate_durable(seed, CRASH_LEN);
        let harness = ReelHarness::new(crash_config(1, SyncPolicy::Never, SEGMENT_SMALL));
        let total = harness.boundary_count(&ops);

        let mut at_risk = 0u64;
        for crash_at in 0..total {
            let plan = FaultPlan::new(seed)
                .with_crash(crash_at)
                .with_scatter(sector);
            let (sim, _) = harness.run(plan, &ops);
            at_risk = at_risk.max(sim.unsynced_bytes());
            let reopened = harness.reopen(sim.durable_image());
            assert_recount(&reopened, crash_at);
            assert_only_written_values(&reopened, &ops, crash_at);
        }
        assert!(
            at_risk > 0,
            "no boundary left unsynced bytes, so nothing scattered"
        );
    }
}

// on a sole copy no crash schedule leaves a footer naming unlanded bytes
//
// The peerless seal syncs the records before the footer that speaks for them. With
// peers a resolved key whose bytes never landed is a checksum miss that enqueues a
// repair; on a sole copy it would be silent loss.
#[test]
fn a_sole_copy_footer_never_outlives_its_records() {
    // Each record rolls the tight segment, so the stream crosses several seals. A stream
    // that never seals makes this sweep pass for any ordering at all.
    let ops: Vec<StreamOp> = (1..=5u8)
        .map(|address| put(GROUP, address, LARGE_PAYLOAD, address))
        .collect();
    // One seed, both sectors: the schedule variety here is the sector size and the
    // per-file scatter hash.
    for (seed, sector) in seeds_and_sectors().into_iter().take(SCATTER_SECTORS.len()) {
        let sole = ReelConfig {
            verify_reads: true,
            repair: RepairPath::None,
            ..crash_config(1, SyncPolicy::Never, SEGMENT_TIGHT)
        };
        let harness = ReelHarness::new(sole);
        let total = harness.boundary_count(&ops);
        assert!(total > 0, "the stream crosses no io boundary");

        for crash_at in 0..total {
            let plan = FaultPlan::new(seed)
                .with_crash(crash_at)
                .with_scatter(sector);
            let (sim, _) = harness.run(plan, &ops);
            let reopened = harness.reopen(sim.durable_image());
            assert_recount(&reopened, crash_at);
            assert_only_written_values(&reopened, &ops, crash_at);
        }
    }
}

// a scattered crash across four tails keeps the acknowledged prefix too
#[test]
fn scattered_crash_multi_tail() {
    for seed in SCATTER_SEEDS.iter().copied() {
        let sector = SCATTER_SECTORS[0];
        let ops = op_stream::generate_durable(seed, CRASH_LEN);
        let harness = ReelHarness::new(crash_config(4, SyncPolicy::EveryPut, SEGMENT_SMALL));
        let total = harness.boundary_count(&ops);

        let mut at_risk = 0u64;
        for crash_at in 0..total {
            let plan = FaultPlan::new(seed)
                .with_crash(crash_at)
                .with_scatter(sector);
            let (sim, acknowledged) = harness.run(plan, &ops);
            at_risk = at_risk.max(sim.unsynced_bytes());
            let reopened = harness.reopen(sim.durable_image());
            assert_recount(&reopened, crash_at);
            assert_only_written_values(&reopened, &ops, crash_at);
            assert_durable_prefix(&reopened, &ops, acknowledged, crash_at);
        }
        assert!(
            at_risk > 0,
            "no boundary left unsynced bytes, so nothing scattered"
        );
    }
}

// a hole inside a record is rejected rather than served with the bytes that survived
//
// A payload spanning many sectors keeps a header that still describes its record while
// the bytes behind it are only partly committed. Nothing structural says that record is
// bad, so recovery has to reject it on its checksum.
#[test]
fn scattered_hole_inside_a_record_is_rejected() {
    let ops: Vec<StreamOp> = (1..=4u8)
        .map(|byte| put(GROUP, byte, LARGE_PAYLOAD, byte))
        .collect();

    for sector in SCATTER_SECTORS {
        let harness = ReelHarness::new(crash_config(1, SyncPolicy::Never, SEGMENT_LARGE));
        let total = harness.boundary_count(&ops);
        assert!(total > 0, "the stream crosses no io boundary");

        let mut at_risk = 0u64;
        for crash_at in 0..total {
            let plan = FaultPlan::new(1).with_crash(crash_at).with_scatter(sector);
            let (sim, _) = harness.run(plan, &ops);
            at_risk = at_risk.max(sim.unsynced_bytes());
            let reopened = harness.reopen(sim.durable_image());
            assert_recount(&reopened, crash_at);
            assert_only_written_values(&reopened, &ops, crash_at);
        }
        assert!(
            at_risk > 0,
            "no boundary left unsynced bytes, so nothing scattered"
        );
    }
}

// every crash boundary of a sub group range delete leaves each key whole or gone
//
// A range inside a group walks the keys and appends a tombstone for each, so a crash
// lands between tombstones and half applies the delete, which is allowed.
#[test]
fn every_boundary_of_a_sub_group_range_delete() {
    // A fixed stream rather than a generated one, so a seed only names the plan.
    let ops = sub_range_stream();
    let harness = ReelHarness::new(crash_config(1, SyncPolicy::EveryPut, SEGMENT_LARGE));
    let total = harness.boundary_count(&ops);
    assert!(total > 0, "the stream crosses no io boundary");

    for crash_at in 0..total {
        let (sim, acknowledged) = harness.run(FaultPlan::new(1).with_crash(crash_at), &ops);
        let reopened = harness.reopen(sim.durable_image());

        assert_recount(&reopened, crash_at);
        assert_only_written_values(&reopened, &ops, crash_at);
        assert_outside_the_range_survives(&reopened, acknowledged, crash_at);
    }
}

/// Fill a group, then delete a range inside it, leaving keys on both sides
fn sub_range_stream() -> Vec<StreamOp> {
    let mut ops: Vec<StreamOp> = (0..RANGE_KEYS)
        .map(|byte| put(GROUP, byte, SMALL_PAYLOAD, byte))
        .collect();
    ops.push(StreamOp::DeleteRange {
        group: GROUP,
        lo: RANGE_LO,
        hi: RANGE_HI,
    });
    ops
}

// Assert the keys the range never covered are untouched by however far it got
//
// Only the puts that acknowledged before the crash are expected at all, since under
// this policy an acknowledged put is a durable one and the rest never happened.
fn assert_outside_the_range_survives(reopened: &ReelStore, acknowledged: usize, context: u64) {
    for byte in 0..RANGE_KEYS {
        if (RANGE_LO..RANGE_HI).contains(&byte) || usize::from(byte) >= acknowledged {
            continue;
        }
        let served = Store::get(reopened, RECORDS_CF, &wire_key(GROUP, byte)).expect("get");
        assert_eq!(
            served,
            Some(Value::new(framed_value(SMALL_PAYLOAD, byte))),
            "a key outside the deleted range went missing at {context}"
        );
    }
}

// a sync error rejects the put and keeps every acknowledged record consistent
#[test]
fn sync_error() {
    // The op a sync lands on moves whenever the open sequence changes, so the scheduled
    // position is searched for rather than pinned.
    let harness = ReelHarness::new(crash_config(1, SyncPolicy::EveryPut, SEGMENT_LARGE));
    let mut proven = false;

    for at in SYNC_ERROR_FROM..=SYNC_ERROR_TO {
        let sim = SimIo::new(FaultPlan::new(1).with_fault(at, FaultKind::SyncError));
        let store = harness.open_or_panic(sim.clone());

        let mut acknowledged = Vec::new();
        let mut rejected = Vec::new();
        for byte in 1..=6u8 {
            if apply_mutation(&store, &put(GROUP, byte, SMALL_PAYLOAD, byte)).is_err() {
                rejected.push(byte);
            } else {
                acknowledged.push(byte);
            }
        }
        store.flush().ok();
        if rejected.is_empty() || acknowledged.is_empty() {
            continue;
        }
        proven = true;

        assert_recount(&store, at);
        for byte in &rejected {
            assert!(!contains(&store, *byte), "a rejected record is absent");
        }
        for byte in &acknowledged {
            assert!(
                contains(&store, *byte),
                "an acknowledged record stays durable"
            );
            assert_eq!(
                get(&store, *byte),
                Some(framed_value(SMALL_PAYLOAD, *byte)),
                "an acknowledged record reads back after the sync error"
            );
        }
    }
    assert!(
        proven,
        "no scheduled position rejected a put while acknowledging another"
    );
}

// an out of space append errors to the caller and stays consistent
#[test]
fn enospc_append() {
    let harness = ReelHarness::new(crash_config(1, SyncPolicy::Never, SEGMENT_LARGE));
    let mut plan = FaultPlan::new(1);
    for at in ENOSPC_FROM..=ENOSPC_TO {
        plan = plan.with_fault(at, FaultKind::EnospcAppend);
    }
    let sim = SimIo::new(plan);
    let store = harness.open_or_panic(sim.clone());

    let mut rejected = Vec::new();
    for byte in 1..=6u8 {
        if apply_mutation(&store, &put(GROUP, byte, SMALL_PAYLOAD, byte)).is_err() {
            rejected.push(byte);
        }
    }
    store.flush().ok();

    assert!(
        !rejected.is_empty(),
        "the out of space rejects at least one append"
    );
    assert_recount(&store, ENOSPC_FROM);
    for byte in &rejected {
        assert!(!contains(&store, *byte), "a rejected record is absent");
    }
}

/// Addresses the batch that survives every torn-batch arm carries
const KEPT_BATCH: &[u8] = &[1, 2, 3];

/// Addresses the batch that is torn in every torn-batch arm
const TORN_BATCH: &[u8] = &[4, 5, 6];

/// Address of the point write standing between the two batches
const NEIGHBOUR: u8 = 9;

/// Where inside a batch a write is cut off
///
/// The frame, the first record, one in the middle and the last: a writev that stopped
/// at any of them leaves a run that must not be applied at all.
#[derive(Clone, Copy, Debug)]
enum Tear {
    Frame,
    FirstRecord,
    MidBatch,
    LastRecord,
}

// a batch cut off part way through leaves nothing of itself behind
//
// The reservation is one range and the write is one writev, so a crash inside it lands
// at some byte of the run and the bytes past it stay the zeros the tail reserved. The
// frame is what recovery reads that by: it declares how many records follow and how
// many bytes they take, so a run that stops short is dropped whole.
#[test]
fn a_torn_batch_leaves_nothing_of_itself() {
    for tear in [
        Tear::Frame,
        Tear::FirstRecord,
        Tear::MidBatch,
        Tear::LastRecord,
    ] {
        let harness = ReelHarness::new(crash_config(1, SyncPolicy::EveryPut, SEGMENT_LARGE));
        let sim = SimIo::new(FaultPlan::new(1));
        let store = harness.open_or_panic(sim.clone());
        store
            .apply_batch(puts(KEPT_BATCH))
            .expect("the batch that survives");
        apply_mutation(&store, &put(GROUP, NEIGHBOUR, SMALL_PAYLOAD, NEIGHBOUR)).expect("point");
        store
            .apply_batch(puts(TORN_BATCH))
            .expect("the batch that tears");
        store.flush().expect("flush");

        // Taken with the store still open, so the tail is unsealed and is walked back
        // rather than read from a footer.
        let mut image = sim.durable_image();
        cut_the_last_batch(&mut image, tear);
        let reopened = harness.reopen(image);

        assert_recount(&reopened, tear as u64);
        for address in TORN_BATCH {
            assert!(
                get(&reopened, *address).is_none(),
                "a torn batch left {address} behind at {tear:?}",
            );
        }
        for address in KEPT_BATCH {
            assert!(
                get(&reopened, *address).is_some(),
                "a confirmed batch lost {address} to a later tear at {tear:?}",
            );
        }
        assert!(
            get(&reopened, NEIGHBOUR).is_some(),
            "the point write beside the torn batch went with it at {tear:?}",
        );
    }
}

// a torn batch carrying a range delete applies neither the range nor its puts
//
// The range is a record of the run like any other, so the crash that drops the run
// drops the delete with it and the keys it covered are still there.
#[test]
fn a_torn_range_batch_applies_neither_half() {
    for tear in [Tear::Frame, Tear::MidBatch, Tear::LastRecord] {
        let harness = ReelHarness::new(crash_config(1, SyncPolicy::EveryPut, SEGMENT_LARGE));
        let sim = SimIo::new(FaultPlan::new(1));
        let store = harness.open_or_panic(sim.clone());
        for address in 0..RANGE_KEYS {
            apply_mutation(&store, &put(GROUP, address, SMALL_PAYLOAD, address)).expect("fill");
        }

        let mut writes = puts(&[RANGE_KEYS]);
        writes.push(RecordWrite::DeleteRange {
            start: address_key(RANGE_LO),
            end: Some(wire_key(GROUP, RANGE_HI)),
        });
        writes.extend(puts(&[RANGE_KEYS + 1]));
        store.apply_batch(writes).expect("the batch that tears");
        store.flush().expect("flush");

        let mut image = sim.durable_image();
        cut_the_last_batch(&mut image, tear);
        let reopened = harness.reopen(image);

        assert_recount(&reopened, tear as u64);
        for address in [RANGE_KEYS, RANGE_KEYS + 1] {
            assert!(
                get(&reopened, address).is_none(),
                "a torn batch left the put at {address} behind at {tear:?}",
            );
        }
        for address in RANGE_LO..RANGE_HI {
            assert!(
                get(&reopened, address).is_some(),
                "a torn batch swept {address} with a range delete that never landed at {tear:?}",
            );
        }
    }
}

/// The puts one batch carries, one small record an address
fn puts(addresses: &[u8]) -> Vec<RecordWrite> {
    addresses
        .iter()
        .map(|address| RecordWrite::Put {
            key: address_key(*address),
            payload: framed_value(SMALL_PAYLOAD, *address),
        })
        .collect()
}

/// The record key one address of the test group is addressed by
fn address_key(address: u8) -> RecordKey {
    RecordKey::from_bytes(RECORDS, &wire_key(GROUP, address)).expect("key")
}

/// Cut the last batch of the image at a point inside it, as a stopped write would
///
/// Everything from the cut to the end of the segment goes back to the zeros the tail
/// had reserved there, which is what a writev that never got that far leaves.
fn cut_the_last_batch(image: &mut DurableImage, tear: Tear) {
    for (path, bytes) in image.iter_mut() {
        if !path.to_string_lossy().ends_with(SEGMENT_SUFFIX) {
            continue;
        }
        let Some(at) = tear_offset(bytes, tear) else {
            continue;
        };
        bytes[at as usize..].fill(0);
        return;
    }
    panic!("no segment of the image holds a framed batch");
}

/// Where in the last batch of a segment a tear falls
fn tear_offset(bytes: &[u8], tear: Tear) -> Option<u64> {
    let (frame_at, frame) = last_frame(bytes)?;
    let run_at = frame_at + BatchFrame::SPAN;
    let mut starts = Vec::new();
    let mut at = run_at;
    while at < run_at + frame.span {
        let header = RecordHeader::unpack(&bytes[at as usize..]).ok()?;
        starts.push(at);
        at += header.span();
    }
    let last = *starts.last()?;
    Some(match tear {
        // Inside the declaration, which the frame's own checksum covers.
        Tear::Frame => frame_at + HEADER_LEN as u64 + 2,
        Tear::FirstRecord => run_at + HEADER_LEN as u64,
        Tear::MidBatch => starts[starts.len() / 2],
        Tear::LastRecord => last + HEADER_LEN as u64,
    })
}

/// The last batch frame a segment holds, and where it sits
fn last_frame(bytes: &[u8]) -> Option<(u64, BatchFrame)> {
    let mut found = None;
    let mut at = 0u64;
    while at + HEADER_LEN as u64 <= bytes.len() as u64 {
        let Ok(header) = RecordHeader::unpack(&bytes[at as usize..]) else {
            break;
        };
        if header.is_unwritten() || !header.fits_within(bytes.len() as u64 - at) {
            break;
        }
        if header.flags.is_batch_frame() {
            let from = (at + header.prefix_len()) as usize;
            let declaration = &bytes[from..from + header.length as usize];
            found = BatchFrame::unpack(&header, declaration).map(|frame| (at, frame));
        }
        at += header.span();
    }
    found
}

// a read time checksum failure treats a corrupted record as missing and stays consistent
#[test]
fn bit_rot() {
    let harness = ReelHarness::new(crash_config(1, SyncPolicy::EveryPut, SEGMENT_TIGHT));
    let sim = SimIo::new(FaultPlan::new(1));
    let store = harness.open_or_panic(sim.clone());
    apply_mutation(&store, &put(GROUP, 1, LARGE_PAYLOAD, 1)).expect("large put");
    apply_mutation(&store, &put(GROUP, 2, SMALL_PAYLOAD, 2)).expect("roll put");
    store.flush().expect("flush");

    let mut image = sim.durable_image();
    assert!(
        flip_largest_segment(&mut image),
        "a segment is available to corrupt"
    );
    let reopened = harness.reopen(image);

    assert!(
        get(&reopened, 1).is_none(),
        "the rotted key reads as missing"
    );
    assert!(get(&reopened, 2).is_some(), "the intact key survives");

    reopened.scrub_once().expect("scrub");
    assert_recount(&reopened, 0);
    assert!(
        get(&reopened, 1).is_none(),
        "the rotted key stays missing after a scrub"
    );
}

// a crash under multi tail same key traffic recovers by highest version
#[test]
fn multi_tail_same_key() {
    let harness = ReelHarness::new(crash_config(4, SyncPolicy::EveryPut, SEGMENT_LARGE));
    let ops = same_key_stream(OVERWRITE_COUNT);
    let total = harness.boundary_count(&ops);
    assert!(total > 0, "the stream crosses no io boundary");

    for crash_at in 0..total {
        let (sim, _) = harness.run(FaultPlan::new(1).with_crash(crash_at), &ops);
        let reopened = harness.reopen(sim.durable_image());
        assert_recount(&reopened, crash_at);
        assert!(
            reopened.totals().count <= 1,
            "at most one live copy of the key"
        );
        if let Some(value) = get(&reopened, 1) {
            assert!(
                is_written_version(&value),
                "the survivor is a written version"
            );
        }
    }
}

// a crash mid group drop rebuilds the rest and a re drop is idempotent
#[test]
fn group_drop() {
    let harness = ReelHarness::new(crash_config(1, SyncPolicy::EveryPut, SEGMENT_LARGE));
    let ops = drop_stream();
    let total = harness.boundary_count(&ops);
    assert!(total > 0, "the stream crosses no io boundary");

    for crash_at in 0..total {
        let (sim, _) = harness.run(FaultPlan::new(1).with_crash(crash_at), &ops);
        let reopened = harness.reopen(sim.durable_image());
        assert_recount(&reopened, crash_at);

        redrop_group(&reopened);
        assert_recount(&reopened, crash_at);
        assert!(
            !observe(&reopened).per_group.contains_key(&7),
            "the dropped group is gone"
        );
        redrop_group(&reopened);
        assert!(
            !observe(&reopened).per_group.contains_key(&7),
            "re dropping is idempotent"
        );
    }
}

// a crash in the middle of a compaction rewrite keeps the live key and restarts
#[test]
fn mid_compaction() {
    let harness = ReelHarness::new(crash_config(1, SyncPolicy::EveryPut, COMPACT_SEG_BYTES));

    let probe_sim = SimIo::new(FaultPlan::new(0));
    let probe = harness.open_or_panic(probe_sim.clone());
    write_compaction_setup(&probe);
    let setup_ops = probe_sim.ops();
    probe.compact_once().expect("probe compaction");
    let total_ops = probe_sim.ops();
    drop(probe);
    assert!(
        total_ops > setup_ops,
        "the setup makes compaction rewrite a segment"
    );

    for crash_at in setup_ops..total_ops {
        let sim = SimIo::new(FaultPlan::new(1).with_crash(crash_at));
        let store = harness.open_or_panic(sim.clone());
        write_compaction_setup(&store);
        let _ = store.compact_once();
        drop(store);

        let reopened = harness.reopen(sim.durable_image());
        assert_recount(&reopened, crash_at);
        assert!(get(&reopened, 3).is_some(), "one rewritten key survives");
        assert!(
            get(&reopened, 4).is_some(),
            "the other rewritten key survives"
        );
        reopened.compact_once().expect("compaction restarts");
        assert_recount(&reopened, crash_at);
    }
}

// a crash in the middle of a merge keeps every live key and keeps the deleted one dead
//
// The sources stay the authority until the output is sealed and the repoints published,
// so a reopen has to land on one side of that or the other. The delete is the gate: its
// tombstone rides the merge like any other row, and losing it hands back the version
// underneath.
#[test]
fn mid_merge() {
    let harness = ReelHarness::new(merge_config());

    let probe_sim = SimIo::new(FaultPlan::new(0));
    let probe = harness.open_or_panic(probe_sim.clone());
    write_merge_setup(&probe);
    let setup_ops = probe_sim.ops();
    let report = probe.merge_once().expect("probe merge");
    let total_ops = probe_sim.ops();
    drop(probe);
    assert!(
        report.runs_merged >= 2,
        "the setup left {} runs, so the merge below collapses nothing",
        report.runs_merged,
    );
    assert!(total_ops > setup_ops, "the merge crossed no io boundary");

    for crash_at in setup_ops..total_ops {
        let sim = SimIo::new(FaultPlan::new(1).with_crash(crash_at));
        let store = harness.open_or_panic(sim.clone());
        write_merge_setup(&store);
        let _ = store.merge_once();
        drop(store);

        let reopened = harness.reopen(sim.durable_image());
        assert_recount(&reopened, crash_at);
        for address in MERGE_KEPT {
            assert!(
                merged_value(&reopened, *address).is_some(),
                "a merged key went missing at {crash_at}",
            );
        }
        assert!(
            merged_value(&reopened, MERGE_DELETED).is_none(),
            "a merge crash brought a deleted key back at {crash_at}",
        );

        // And the pass restarts, so the crash left a volume a merge can still work on.
        let _ = reopened.merge_once();
        assert_recount(&reopened, crash_at);
        assert!(
            merged_value(&reopened, MERGE_DELETED).is_none(),
            "the restarted merge brought a deleted key back at {crash_at}",
        );
    }
}

/// A volume that seals by rewriting and takes a merge when asked
///
/// The dead ratio is at one so compaction reclaims only wholly dead segments, which
/// leaves the runs standing for the merge to collapse. The index stays resident, since a
/// paged rebuild counts nothing it left in a footer and the recount would then be
/// measuring the residency instead of the crash.
fn merge_config() -> ReelConfig {
    ReelConfig {
        rewrite_on_seal: true,
        merge_sorted_runs: true,
        compact_dead_ratio: 1.0,
        compact_mbps: CompactRate::Mbps(100_000),
        ..crash_config(1, SyncPolicy::EveryPut, MERGE_SEG_BYTES)
    }
}

/// Write two rounds of overlapping keys, settling each into a sorted run
///
/// The second round rewrites half of the first, so neither run goes wholly dead and both
/// are standing when the merge arrives. The delete lands last, in its own run.
fn write_merge_setup(store: &ReelStore) {
    for address in 1..=MERGE_KEYS {
        apply_mutation(store, &put(GROUP, address, MERGE_PAYLOAD, address)).expect("first round");
    }
    settle_runs(store);
    for address in 1..=MERGE_KEYS {
        if address % 2 == 0 {
            apply_mutation(store, &put(GROUP, address, MERGE_PAYLOAD, address + 100))
                .expect("second round");
        }
    }
    settle_runs(store);
    apply_mutation(
        store,
        &StreamOp::Delete {
            group: GROUP,
            address: MERGE_DELETED,
        },
    )
    .expect("delete");
    settle_runs(store);
}

/// Flush, seal and rewrite until the volume has nothing left to put in key order
fn settle_runs(store: &ReelStore) {
    store.flush().expect("flush");
    drop(store.cue().expect("cue"));
    store.page_out_sealed().expect("page out");
    for _ in 0..MERGE_SETTLE_PASSES {
        let pass = store.compact_once().expect("compact");
        store.flush().expect("flush");
        store.page_out_sealed().expect("page out");
        if matches!(pass, CompactPass::Idle) {
            return;
        }
    }
}

/// What one key of the merge stream reads back as
fn merged_value(store: &ReelStore, address: u8) -> Option<Vec<u8>> {
    let key = RecordKey::from_bytes(RECORDS, &wire_key(GROUP, address)).expect("key");
    store
        .get(&key)
        .expect("get")
        .map(|value| value.as_ref().to_vec())
}

// Assert every record the reopened reel serves is a version the stream actually wrote
//
// A crash may drop any record, so what survives is not fixed, but a surviving record is
// one that was written. The counters agree with a scan either way and a damaged record
// is still internally consistent, so this is the only check that catches one.
fn assert_only_written_values(reopened: &ReelStore, ops: &[StreamOp], context: u64) {
    let mut written: BTreeMap<Vec<u8>, BTreeSet<Vec<u8>>> = BTreeMap::new();
    for op in ops {
        if let StreamOp::Put {
            group,
            address,
            len,
            fill,
        }
        | StreamOp::Overwrite {
            group,
            address,
            len,
            fill,
        } = op
        {
            written
                .entry(wire_key(*group, *address))
                .or_default()
                .insert(framed_value(*len, *fill));
        }
    }

    for (key, value) in observe(reopened).records {
        let versions = match written.get(&key) {
            Some(versions) => versions,
            None => panic!("the reel serves a key the stream never wrote at {context}"),
        };
        assert!(
            versions.contains(&value),
            "the reel serves a payload never written for its key at {context}"
        );
    }
}

/// Every seed paired with every sector size a scattered crash is replayed at
fn seeds_and_sectors() -> Vec<(u64, u32)> {
    let mut out = Vec::new();
    for seed in SCATTER_SEEDS {
        for sector in SCATTER_SECTORS {
            out.push((*seed, sector));
        }
    }
    out
}

fn write_compaction_setup(store: &ReelStore) {
    for address in 1..=COMPACT_FILL {
        apply_mutation(store, &put(GROUP, address, COMPACT_PAYLOAD, address))
            .expect("fill segment");
    }
    apply_mutation(store, &put(GROUP, 1, COMPACT_PAYLOAD, 11)).expect("overwrite first key");
    apply_mutation(store, &put(GROUP, 2, COMPACT_PAYLOAD, 12)).expect("overwrite second key");
    store.flush().expect("flush");
}

fn same_key_stream(count: u8) -> Vec<StreamOp> {
    let mut ops = Vec::with_capacity(count as usize);
    for fill in 1..=count {
        ops.push(put(GROUP, 1, SAME_KEY_LEN, fill));
    }
    ops
}

fn is_written_version(value: &[u8]) -> bool {
    for fill in 1..=OVERWRITE_COUNT {
        if value == framed_value(SAME_KEY_LEN, fill).as_slice() {
            return true;
        }
    }
    false
}

fn drop_stream() -> Vec<StreamOp> {
    let mut ops = Vec::new();
    for byte in 1..=3u8 {
        ops.push(put(7, byte, SMALL_PAYLOAD, byte));
    }
    for byte in 1..=3u8 {
        ops.push(put(8, byte, SMALL_PAYLOAD, byte));
    }
    ops.push(StreamOp::DropGroup { group: 7 });
    ops
}

fn redrop_group(store: &ReelStore) {
    let start = group_prefix(7);
    let end = group_prefix(8);
    Store::delete_range(store, RECORDS_CF, &start, &end).expect("re drop records");
    // The counters converge at the sweep the maintenance tick runs, so the recount that
    // follows checks the sweep's own accounting.
    while store.sweep_covers().expect("sweep") {}
}

fn contains(store: &ReelStore, address: u8) -> bool {
    Store::contains(store, RECORDS_CF, &wire_key(GROUP, address)).expect("contains")
}

fn get(store: &ReelStore, address: u8) -> Option<Vec<u8>> {
    Store::get(store, RECORDS_CF, &wire_key(GROUP, address))
        .expect("get")
        .map(Value::into_vec)
}

// Assert the reopened reel reproduces the acknowledged prefix on every untouched key
//
// A memory store replays the ops that acknowledged before the crash, which under a
// synced policy are exactly the durable ones. The op that crashed is excluded, since its
// effect may or may not have reached the durable image.
fn assert_durable_prefix(
    reopened: &ReelStore,
    ops: &[StreamOp],
    acknowledged: usize,
    context: u64,
) {
    let expected = MemoryStore::new();
    for op in &ops[..acknowledged] {
        apply_mutation(&expected, op).expect("memory model mutation");
    }
    let excluded = Excluded::from_crashing(ops.get(acknowledged));

    let got = observe(reopened);
    let want = observe(&expected);
    assert_agrees_outside(&got.records, &want.records, &excluded, context);
}

// Assert two ordered key and value dumps match on every key outside the excluded set
fn assert_agrees_outside(
    got: &[(Vec<u8>, Vec<u8>)],
    want: &[(Vec<u8>, Vec<u8>)],
    excluded: &Excluded,
    context: u64,
) {
    let got: Vec<_> = got
        .iter()
        .filter(|(key, _)| !excluded.covers(key))
        .collect();
    let want: Vec<_> = want
        .iter()
        .filter(|(key, _)| !excluded.covers(key))
        .collect();
    assert_eq!(
        got, want,
        "record content diverged from the durable prefix at {context}"
    );
}

// The keys the crashing op could still be settling, left out of the durable check
enum Excluded {
    Nothing,
    Key(Vec<u8>),
    Group(u16),
}

impl Excluded {
    fn from_crashing(op: Option<&StreamOp>) -> Excluded {
        match op {
            Some(StreamOp::Put { group, address, .. })
            | Some(StreamOp::Overwrite { group, address, .. })
            | Some(StreamOp::Delete { group, address }) => {
                Excluded::Key(wire_key(*group, *address))
            }
            Some(StreamOp::DropGroup { group }) => Excluded::Group(*group),
            Some(StreamOp::DeleteRange { .. })
            | Some(StreamOp::IterFrom { .. })
            | Some(StreamOp::IterRange { .. })
            | Some(StreamOp::IterKeysPrefix { .. })
            | Some(StreamOp::Reopen)
            | None => Excluded::Nothing,
        }
    }

    fn covers(&self, key: &[u8]) -> bool {
        match self {
            Excluded::Nothing => false,
            Excluded::Key(exact) => key == exact.as_slice(),
            Excluded::Group(group) => {
                key.len() >= 2 && u16::from_be_bytes([key[0], key[1]]) == *group
            }
        }
    }
}
