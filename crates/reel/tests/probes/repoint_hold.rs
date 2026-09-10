//! What a merge's repoint batch holds the publish barrier for, and what a read sees
//!
//! Sweeps hold time against batch size, with a spanning reader beside it. Both arms are
//! the engine's own machinery: `repoint` inside `publish_pass`, which takes the exclusive
//! barrier over every stripe, and `publish_batch`, which takes only the stripes its own
//! keys fall in and is here as the comparator. The batching is the one proxy, since
//! compaction repoints as it copies and there is no batched repoint to drive. The arms
//! run in that order on one index, `repoint` keeping an entry's sequence number while
//! `publish_batch` lands on a newer one that would leave the repoints' guard stale. The
//! reader asks for four keys, a single-key get being handed straight to the shard
//! without the barrier because it cannot be half a batch.
//!
//! Opt-in, run with:
//!   cargo test -p tape-reel --release --test probes -- repoint_hold

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Instant;

use reel::format::loc::{Loc, SegmentId};
use reel::format::lsn::Lsn;
use reel::index::column::KeyMove;
use reel::{
    Codec, ColumnId, ColumnSet, ColumnSpec, IndexResidency, KeyWidth, MapShape, RecordKey,
    ReelIndex, ShardShapes,
};

const ACCOUNTS: ColumnId = ColumnId(1);

/// A pubkey-width column, which is what the index is priced by
const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ACCOUNTS,
    name: "accounts",
    key_width: KeyWidth::Fixed(32),
    shard_bytes: 0,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

/// Keys the index holds, which is what a repoint has to find its entry inside of
///
/// Four million rather than a few thousand because the cost being measured is a
/// descent, and a tree small enough to sit in L2 answers one in a fraction of what a
/// real one does. This is the per-entry constant rather than the depth at scale.
const KEYS: u64 = 4_000_000;

/// Entries each arm repoints at each batch size
///
/// Fixed across the sweep so every row does the same total work and only the
/// granularity changes.
const ENTRIES: u64 = 1_048_576;

/// Batches every row runs at least, so the widest one still has a distribution
const MIN_BATCHES: u64 = 64;

/// Batch sizes the sweep walks, in entries repointed under one hold
const SIZES: &[u64] = &[64, 512, 4_096, 32_768];

/// Payload length each entry names, which only sizes the segment bookkeeping
const LEN: u32 = 200;

/// Where the repoints move their entries to, standing in for a merged run
const MERGED: SegmentId = SegmentId(9_000);

/// Keys the spanning reader asks for at once
///
/// Four, since one key cannot be half a batch and the engine answers it without the
/// barrier at all.
const READ_KEYS: usize = 4;

/// A key nothing about its bytes says the order of, so the shards fill evenly
fn key(at: u64) -> RecordKey {
    let mixed = at
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .rotate_left(29)
        .wrapping_mul(0xBF58_476D_1CE4_E5B9);
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&mixed.to_be_bytes());
    bytes[8..16].copy_from_slice(&at.to_be_bytes());
    RecordKey::from_bytes(ACCOUNTS, &bytes).expect("key")
}

/// Where the source record sat, which is what the index is holding before a repoint
fn source(at: u64) -> Loc {
    Loc::new(SegmentId((at >> 20) as u32 + 1), at as u32, LEN)
}

/// Where the merge put it, which is one run and an offset that ascends with the key
fn merged(at: u64) -> Loc {
    Loc::new(MERGED, at as u32, LEN)
}

/// An index holding every key, each naming a record in the run it was written in
fn filled() -> ReelIndex {
    let index =
        ReelIndex::new(COLUMNS, IndexResidency::Resident, ShardShapes::Tree).expect("index");
    for at in 0..KEYS {
        index
            .insert(&key(at), source(at), Lsn(at + 1), None)
            .expect("insert");
    }
    index
}

/// Batches a size runs, held at a floor so the widest one is still a distribution
fn batches(size: u64) -> u64 {
    (ENTRIES / size).max(MIN_BATCHES)
}

/// One quantile of a sorted run of holds, in microseconds
fn at_quantile(sorted: &[u64], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let at = ((sorted.len() as f64 - 1.0) * quantile).round() as usize;
    sorted[at] as f64 / 1_000.0
}

/// What a spanning read costs while nothing at all is publishing
///
/// The row every blip below is read against: without it a worst get of half a
/// millisecond could be the barrier or could be the ask.
fn quiet_read(index: &ReelIndex) -> (f64, f64) {
    let mut worst = 0u64;
    let mut total = 0u64;
    let rounds = 200_000u64;
    for round in 0..rounds {
        let asked = spanning(round);
        let began = Instant::now();
        std::hint::black_box(index.get_many(&asked).expect("get many"));
        let took = began.elapsed().as_nanos() as u64;
        worst = worst.max(took);
        total += took;
    }
    (
        total as f64 / rounds as f64 / 1_000.0,
        worst as f64 / 1_000.0,
    )
}

/// The keys one spanning read asks for, walked so no round repeats the last
fn spanning(round: u64) -> Vec<RecordKey> {
    (0..READ_KEYS as u64)
        .map(|slot| key((round * 13 + slot * 7) % KEYS))
        .collect()
}

/// Run one batch-size row with a spanning reader beside it, and say what both saw
///
/// The reader is started before the first batch and stopped after the last, so its
/// worst get is one that waited on one of these holds and nothing else. Each batch
/// times its own hold rather than being timed from here, since what a batch builds
/// before it can enter is not part of what it holds.
fn row(index: &ReelIndex, size: u64, mut batch: impl FnMut(u64, u64) -> (u64, u64)) -> Row {
    let stop = AtomicBool::new(false);
    let worst_read = AtomicU64::new(0);
    let reads = AtomicU64::new(0);
    let mut holds: Vec<u64> = Vec::with_capacity(batches(size) as usize);
    let mut landed = 0u64;

    thread::scope(|scope| {
        scope.spawn(|| {
            let mut round = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let asked = spanning(round);
                let began = Instant::now();
                std::hint::black_box(index.get_many(&asked).expect("get many"));
                let took = began.elapsed().as_nanos() as u64;
                worst_read.fetch_max(took, Ordering::Relaxed);
                reads.fetch_add(1, Ordering::Relaxed);
                round += 1;
            }
        });

        for round in 0..batches(size) {
            let first = (round * size) % (KEYS - size);
            let (held, moved) = batch(first, size);
            landed += moved;
            holds.push(held);
        }
        stop.store(true, Ordering::Relaxed);
    });

    holds.sort_unstable();
    Row {
        size,
        batches: holds.len() as u64,
        landed,
        p50: at_quantile(&holds, 0.50),
        p99: at_quantile(&holds, 0.99),
        worst: at_quantile(&holds, 1.0),
        per_entry: holds.iter().sum::<u64>() as f64 / (holds.len() as f64 * size as f64),
        read_worst: worst_read.load(Ordering::Relaxed) as f64 / 1_000.0,
        reads: reads.load(Ordering::Relaxed),
    }
}

/// One batch size's answer, in what the holder paid and what the reader beside it did
struct Row {
    size: u64,
    batches: u64,
    landed: u64,
    p50: f64,
    p99: f64,
    worst: f64,
    per_entry: f64,
    read_worst: f64,
    reads: u64,
}

fn header(arm: &str) {
    println!();
    println!("{arm}");
    println!(
        "{:>8} {:>9} {:>10} {:>10} {:>10} {:>10} {:>12} {:>10}",
        "batch", "batches", "p50 us", "p99 us", "max us", "ns/entry", "read max us", "reads",
    );
}

fn print(row: &Row) {
    println!(
        "{:>8} {:>9} {:>10.1} {:>10.1} {:>10.1} {:>10.0} {:>12.1} {:>10}",
        row.size,
        row.batches,
        row.p50,
        row.p99,
        row.worst,
        row.per_entry,
        row.read_worst,
        row.reads,
    );
    assert!(row.landed > 0, "a batch of {} repointed nothing", row.size);
}

// what a batch of repoints holds the barrier for, and what it costs a reader
//
// Measurement only, no bound asserted: a probe failing on a timing would fail on a
// loaded laptop and say nothing. The only assertion is that each batch moved entries.
pub fn hold_by_batch_size() {
    let index = filled();

    let (quiet_mean, quiet_worst) = quiet_read(&index);
    println!();
    println!(
        "{KEYS} keys resident, spanning read of {READ_KEYS} keys: quiet mean {quiet_mean:.2} us, quiet max {quiet_worst:.1} us",
    );

    header("repoint under the barrier, which is section 6.5's repoint batch");
    for size in SIZES {
        let row = row(&index, *size, |first, count| {
            // The keys a merge is repointing are the keys it just wrote, so it holds
            // them already: built ahead of the clock, and both arms charged the same.
            let keys: Vec<RecordKey> = (first..first + count).map(key).collect();
            let locs: Vec<Loc> = (first..first + count).map(merged).collect();
            let began = Instant::now();
            let landed = index.publish_pass(|| {
                let mut landed = 0u64;
                for (slot, key) in keys.iter().enumerate() {
                    let lsn = Lsn(first + slot as u64 + 1);
                    if index.repoint(key, locs[slot], lsn).expect("repoint") {
                        landed += 1;
                    }
                }
                landed
            });
            (began.elapsed().as_nanos() as u64, landed)
        });
        print(&row);
    }

    header("publish_batch, which is what a write batch takes today");
    let mut issued = KEYS;
    for size in SIZES {
        let row = row(&index, *size, |first, count| {
            let keys: Vec<RecordKey> = (first..first + count).map(key).collect();
            let locs: Vec<Loc> = (first..first + count).map(merged).collect();
            let moves: Vec<KeyMove<'_>> = keys
                .iter()
                .zip(&locs)
                .enumerate()
                .map(|(slot, (key, loc))| KeyMove {
                    column: key.column,
                    key: key.as_slice(),
                    loc: *loc,
                    lsn: Lsn(issued + slot as u64 + 1),
                    carried: None,
                    is_delete: false,
                })
                .collect();
            issued += count;
            let began = Instant::now();
            let landed = index.publish_batch(&moves, &[]).len() as u64;
            (began.elapsed().as_nanos() as u64, landed)
        });
        print(&row);
    }

    // The arms report what they held for, not what they moved, so this is what says the
    // holds were spent on real repoints.
    let landed = index.get(&key(0)).expect("get").expect("key 0");
    assert_eq!(
        landed.loc.segment, MERGED,
        "the repoints held the barrier and moved nothing"
    );
}
