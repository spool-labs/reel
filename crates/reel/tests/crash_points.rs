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
use reel::format::journal::{journal_path, read_groups, JOURNAL_SUFFIX};
use reel::format::record::KEYLESS_PREFIX;
use reel::io::fault::{FaultKind, FaultPlan};
use reel::io::sim_backend::{DurableImage, SimIo};
use reel::{
    ByteCount, IndexResidency, Preallocate, RecordWrite, ReelConfig, ReelStore, RepairPath,
    SyncPolicy, ThreadBudget, SEGMENT_SUFFIX,
};
use reel_core::{Direction, Store, Value};
use reel_mock::MemoryStore;

use harness::observe::observe;
use harness::op_stream::{self, StreamOp};
use harness::reel_harness::{
    assert_recount, counter_totals, flip_largest_segment, scan_totals, ReelHarness,
};
use harness::wire::{
    apply_mutation, framed_value, group_prefix, wire_key, RECORDS, RECORDS_CF, RECORD_KEY_LEN,
    TEST_COLUMNS,
};

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

/// Payload bytes that fill a small segment in three records, so a short stream seals often
const SEAL_PAYLOAD: usize = 5_000;

/// First op position the sync error search schedules at
const SYNC_ERROR_FROM: u64 = 4;

/// Last op position the sync error search schedules at
const SYNC_ERROR_TO: u64 = 20;

/// First op position the out of space fault is scheduled at
const ENOSPC_FROM: u64 = 12;

/// Last op position the out of space fault is scheduled at
const ENOSPC_TO: u64 = 28;

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

/// The merge stream rolls a segment of this size, about one round of its keys
const MERGE_SEG_BYTES: u64 = 20 * 1024;

/// Keys the merge stream writes per round
const MERGE_KEYS: u8 = 6;

/// Payload each of those keys carries
const MERGE_PAYLOAD: usize = 3000;

/// Rounds over every key before the delete, a sealed segment each, past the merge depth
const MERGE_ROUNDS: u8 = 12;

/// Rounds over the kept keys after the delete, enough to seal the segment holding it
const MERGE_AFTER_ROUNDS: u8 = 2;

/// The key the merge stream deletes, which is the resurrection gate
const MERGE_DELETED: u8 = 3;

/// The keys that must still read back after a crash inside the merge
const MERGE_KEPT: &[u8] = &[1, 2, 4, 5, 6];

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
    let harness = ReelHarness::new(config);
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

/// A stream that seals several times, leaving keys with versions in more than one segment
fn sealing_stream() -> Vec<StreamOp> {
    let mut ops: Vec<StreamOp> = (1..=5u8)
        .map(|address| put(GROUP, address, SEAL_PAYLOAD, address))
        .collect();
    ops.push(StreamOp::Overwrite {
        group: GROUP,
        address: 2,
        len: SEAL_PAYLOAD,
        fill: 20,
    });
    ops.push(StreamOp::Delete {
        group: GROUP,
        address: 3,
    });
    ops.push(put(GROUP, 6, SEAL_PAYLOAD, 6));
    ops.push(StreamOp::Overwrite {
        group: GROUP,
        address: 4,
        len: SEAL_PAYLOAD,
        fill: 40,
    });
    ops
}

// every crash boundary of a paged stream reopens with the spot index answering as the footers do
#[test]
fn every_boundary_spot_index_answers_as_the_footers() {
    let ops = sealing_stream();
    let keys: Vec<RecordKey> = (1..=6u8)
        .map(|address| RecordKey::from_bytes(RECORDS, &wire_key(GROUP, address)).expect("key"))
        .collect();
    // Unsynced, since the test compares the two read paths over whatever landed
    let paged = ReelConfig {
        index: IndexResidency::Paged,
        ..crash_config(1, SyncPolicy::Never, SEGMENT_SMALL)
    };
    let harness = ReelHarness::new(paged);
    let total = harness.boundary_count(&ops);
    assert!(total > 0, "the stream crosses no io boundary");

    let mut loaded = 0u64;
    for crash_at in 0..total {
        let (sim, _) = harness.run(FaultPlan::new(1).with_crash(crash_at), &ops);
        let reopened = harness.reopen(sim.durable_image());
        // A paged open counts no sealed segment, so while one stands the counters are a floor
        match reopened.born_segments() {
            0 => assert_recount(&reopened, crash_at),
            _ => assert!(
                counter_totals(&reopened).count <= scan_totals(&reopened).count,
                "the counters overcounted under born segments at {crash_at}"
            ),
        }
        loaded = loaded.max(reopened.index().spot_held());
        // A read as of a cue takes the footer search, the answer to match
        let cue = reopened.cue().expect("cue");
        for key in &keys {
            let live = reopened.get(key).expect("get").map(|value| value.to_vec());
            let footers = reopened
                .get_at(key, &cue)
                .expect("cue read")
                .map(|value| value.to_vec());
            assert_eq!(live, footers, "key {key:?} after a crash at {crash_at}");
        }
    }
    assert!(
        loaded > 0,
        "no reopen loaded the spot index, so the comparison proved nothing"
    );
}

// every crash boundary of a paged stream reopens with its walks answering as the gets do
#[test]
fn every_boundary_walks_answer_as_the_gets() {
    let ops = sealing_stream();
    let keys: Vec<Vec<u8>> = (1..=6u8).map(|address| wire_key(GROUP, address)).collect();
    let paged = ReelConfig {
        index: IndexResidency::Paged,
        ..crash_config(1, SyncPolicy::Never, SEGMENT_SMALL)
    };
    let harness = ReelHarness::with_columns(paged, TEST_COLUMNS);
    let total = harness.boundary_count(&ops);
    assert!(total > 0, "the stream crosses no io boundary");

    for crash_at in 0..total {
        let (sim, _) = harness.run(FaultPlan::new(1).with_crash(crash_at), &ops);
        let reopened = harness.reopen(sim.durable_image());
        let mut want: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for key in &keys {
            if let Some(value) = Store::get(&reopened, RECORDS_CF, key).expect("get") {
                want.push((key.clone(), value.to_vec()));
            }
        }
        let up: Vec<(Vec<u8>, Vec<u8>)> = Store::iter(&reopened, RECORDS_CF)
            .expect("iter")
            .map(|(key, value)| (key, value.to_vec()))
            .collect();
        assert_eq!(up, want, "a walk up after a crash at {crash_at}");
        let mut down: Vec<(Vec<u8>, Vec<u8>)> = Store::iter_from(
            &reopened,
            RECORDS_CF,
            &[0xFF; RECORD_KEY_LEN],
            Direction::Desc,
        )
        .expect("iter from")
        .map(|(key, value)| (key, value.to_vec()))
        .collect();
        down.reverse();
        assert_eq!(down, want, "a walk down after a crash at {crash_at}");
        let alone: Vec<Vec<u8>> = reopened
            .iter_keys_from(RECORDS_CF, None, Direction::Asc)
            .expect("keys")
            .collect();
        let want_keys: Vec<Vec<u8>> = want.iter().map(|(key, _)| key.clone()).collect();
        assert_eq!(alone, want_keys, "a key walk after a crash at {crash_at}");
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
#[derive(Clone, Copy, Debug)]
enum Tear {
    FirstRecord,
    MidBatch,
    LastRecord,
}

// a batch cut off part way through leaves nothing of itself behind
#[test]
fn a_torn_batch_leaves_nothing_of_itself() {
    for tear in [Tear::FirstRecord, Tear::MidBatch, Tear::LastRecord] {
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
    for tear in [Tear::FirstRecord, Tear::MidBatch, Tear::LastRecord] {
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
fn cut_the_last_batch(image: &mut DurableImage, tear: Tear) {
    let journals: Vec<(std::path::PathBuf, Vec<u8>)> = image
        .iter()
        .filter(|(path, _)| path.to_string_lossy().ends_with(JOURNAL_SUFFIX))
        .cloned()
        .collect();
    for (path, bytes) in image.iter_mut() {
        if !path.to_string_lossy().ends_with(SEGMENT_SUFFIX) {
            continue;
        }
        let Some((_, journal)) = journals
            .iter()
            .find(|(journal, _)| *journal == journal_path(path))
        else {
            continue;
        };
        let Some(at) = tear_offset(journal, tear) else {
            continue;
        };
        bytes[at as usize..].fill(0);
        return;
    }
    panic!("no segment of the image holds a batch");
}

/// Find the tear's offset inside the journal's last batch
fn tear_offset(journal: &[u8], tear: Tear) -> Option<u64> {
    let (groups, _) = read_groups(journal);
    let batch = groups.into_iter().rev().find(|group| group.len() > 1)?;
    let mut starts: Vec<u64> = batch.iter().map(|row| u64::from(row.offset)).collect();
    starts.sort_unstable();
    // Past a record's own check, so the bytes that tear are its payload
    let inside = KEYLESS_PREFIX as u64;
    Some(match tear {
        Tear::FirstRecord => starts[0] + inside,
        Tear::MidBatch => starts[starts.len() / 2],
        Tear::LastRecord => *starts.last()? + inside,
    })
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

// a crash in the middle of a key merge keeps every live key and keeps the deleted one dead
#[test]
fn mid_key_merge() {
    let harness = ReelHarness::new(key_merge_config());

    let probe_sim = SimIo::new(FaultPlan::new(0));
    let probe = harness.open_or_panic(probe_sim.clone());
    write_merge_setup(&probe);
    let setup_ops = probe_sim.ops();
    let depth = probe.index().overlap_depth();
    let report = probe.merge_when_due().expect("probe merge");
    let total_ops = probe_sim.ops();
    drop(probe);
    assert!(
        report.is_some_and(|report| report.runs_merged >= 2),
        "the setup stacked the walk {depth} deep, so the merge below folds nothing",
    );
    assert!(total_ops > setup_ops, "the merge crossed no io boundary");

    for crash_at in setup_ops..total_ops {
        let sim = SimIo::new(FaultPlan::new(1).with_crash(crash_at));
        let store = harness.open_or_panic(sim.clone());
        write_merge_setup(&store);
        let _ = store.merge_when_due();
        drop(store);

        let reopened = harness.reopen(sim.durable_image());
        assert_merged_answers(&reopened, crash_at);

        // A second merge proves the crash left a volume a merge can still work on
        let _ = reopened.merge_when_due();
        assert_merged_answers(&reopened, crash_at);
    }
}

/// A paged volume, where a key merge folds the walk once it stacks past the merge depth
fn key_merge_config() -> ReelConfig {
    ReelConfig {
        index: IndexResidency::Paged,
        ..crash_config(1, SyncPolicy::EveryPut, MERGE_SEG_BYTES)
    }
}

/// A key's fill in one round of the merge stream
fn merge_fill(round: u8, address: u8) -> u8 {
    round * 16 + address
}

/// Write rounds over the same keys, then the delete and enough after it to seal it
fn write_merge_setup(store: &ReelStore) {
    for round in 1..=MERGE_ROUNDS {
        for address in 1..=MERGE_KEYS {
            apply_mutation(
                store,
                &put(GROUP, address, MERGE_PAYLOAD, merge_fill(round, address)),
            )
            .expect("round");
        }
    }
    apply_mutation(
        store,
        &StreamOp::Delete {
            group: GROUP,
            address: MERGE_DELETED,
        },
    )
    .expect("delete");
    for round in MERGE_ROUNDS + 1..=MERGE_ROUNDS + MERGE_AFTER_ROUNDS {
        for address in MERGE_KEPT {
            apply_mutation(
                store,
                &put(GROUP, *address, MERGE_PAYLOAD, merge_fill(round, *address)),
            )
            .expect("round after the delete");
        }
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");
}

/// Every key of the merge stream reads back as the last round left it, by get and by walk
fn assert_merged_answers(store: &ReelStore, crash_at: u64) {
    let last = MERGE_ROUNDS + MERGE_AFTER_ROUNDS;
    let want: Vec<(Vec<u8>, Vec<u8>)> = MERGE_KEPT
        .iter()
        .map(|address| {
            (
                wire_key(GROUP, *address),
                framed_value(MERGE_PAYLOAD, merge_fill(last, *address)),
            )
        })
        .collect();
    for (key, value) in &want {
        let found = Store::get(store, RECORDS_CF, key)
            .expect("get")
            .map(|value| value.to_vec());
        assert_eq!(
            found.as_ref(),
            Some(value),
            "a merged key read back wrong at {crash_at}"
        );
    }
    assert!(
        Store::get(store, RECORDS_CF, &wire_key(GROUP, MERGE_DELETED))
            .expect("get")
            .is_none(),
        "a merge crash brought a deleted key back at {crash_at}",
    );
    let walked: Vec<(Vec<u8>, Vec<u8>)> = Store::iter(store, RECORDS_CF)
        .expect("iter")
        .map(|(key, value)| (key, value.to_vec()))
        .collect();
    assert_eq!(walked, want, "a walk after a merge crash at {crash_at}");
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
