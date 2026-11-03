//! The heap glibc holds after a mass delete and the maintenance that follows it
//! Run with `cargo test --release -p tape-reel --test held_heap -- --ignored --nocapture`

#![cfg(all(target_os = "linux", target_env = "gnu"))]

use rand::rngs::SmallRng;
use rand::{Rng, RngCore, SeedableRng};
use tempfile::TempDir;

use reel::config::{CompactRate, ReelConfig, SyncPolicy, ThreadBudget};
use reel::format::column::{Codec, ColumnId, ColumnSet, ColumnSpec};
use reel::units::ByteCount;
use reel::{KeyWidth, ReelStore, Store, WriteBatch};

const COLUMNS: ColumnSet = &[
    column(1, "meta", KeyWidth::Variable, 0),
    column(3, "track", KeyWidth::Fixed(32), 1),
    column(4, "track_lookup", KeyWidth::Variable, 0),
    column(5, "track_data", KeyWidth::Fixed(32), 1),
    column(12, "spool_pending_repair", KeyWidth::Fixed(34), 2),
    column(14, "slice", KeyWidth::Fixed(34), 2),
    column(21, "event_log", KeyWidth::Variable, 0),
];

/// A lz4 column, as a node declares every one
const fn column(id: u8, name: &'static str, key_width: KeyWidth, shard_bytes: u8) -> ColumnSpec {
    ColumnSpec {
        id: ColumnId(id),
        name,
        key_width,
        shard_bytes,
        purge_mark: None,
        codec: Codec::Lz4,
    }
}

/// The test loads the store with this many tracks
const TRACKS: usize = 160_000;

/// One track in this many has a slice
const SLICE_EVERY: usize = 4;

/// Bytes in one slice, which do not shrink under lz4
const SLICE_BYTES: usize = 100 * 1024;

/// Out of every hundred tracks, this many are deleted
const DELETED_PERCENT: u32 = 75;

/// Segment size, small enough that the slices span many segments
const SEGMENT_BYTES: u64 = 1024 * 1024 * 1024;

/// The maintenance loop runs at most this many ticks before the test calls it settled
const MAX_TICKS: usize = 400;

/// The test fails when glibc holds more free bytes than this past the burst
const HELD_CEILING: usize = 128 * 1024 * 1024;

/// What glibc says about its heap
#[derive(Clone, Copy)]
struct Heap {
    in_use: usize,
    held_free: usize,
}

fn heap() -> Heap {
    // SAFETY: mallinfo2 reads the allocator's own statistics and takes no arguments
    let info = unsafe { libc::mallinfo2() };
    Heap {
        in_use: info.uordblks,
        held_free: info.fordblks,
    }
}

fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn report(phase: &str, store: &ReelStore) {
    let now = heap();
    println!(
        "{phase:>10} in use {:>9.1} MiB  held free {:>9.1} MiB  live {:>9.1} MiB  dead {:>9.1} MiB",
        mib(now.in_use),
        mib(now.held_free),
        mib(store.totals().bytes.to_bytes() as usize),
        mib(store.dead_bytes().to_bytes() as usize),
    );
}

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(SEGMENT_BYTES),
        sync: SyncPolicy::Bytes(ByteCount::mb(16)),
        compact_mbps: CompactRate::Auto,
        active_tails: ThreadBudget::threads(1),
        ..ReelConfig::default()
    }
}

fn address(track: usize) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    let mut rng = SmallRng::seed_from_u64(track as u64);
    rng.fill_bytes(&mut bytes);
    bytes
}

fn slice_key(track: usize) -> [u8; 34] {
    let mut bytes = [0u8; 34];
    bytes[..2].copy_from_slice(&((track % 80) as u16).to_be_bytes());
    bytes[2..].copy_from_slice(&address(track));
    bytes
}

fn lookup_key(track: usize) -> Vec<u8> {
    let mut bytes = b"lookup".to_vec();
    bytes.extend_from_slice(&(track as u64).to_be_bytes());
    bytes
}

fn event_key(track: usize) -> Vec<u8> {
    let mut bytes = b"event".to_vec();
    bytes.extend_from_slice(&(track as u64).to_be_bytes());
    bytes
}

fn small(rng: &mut SmallRng, len: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; len];
    rng.fill_bytes(&mut bytes[..len / 2]);
    bytes
}

/// Puts one track's rows the way a node writes them
fn put_track(batch: &mut WriteBatch, rng: &mut SmallRng, track: usize) {
    let address = address(track).to_vec();
    batch.put_owned("track", address.clone(), small(rng, 240));
    batch.put_owned("track_data", address.clone(), small(rng, 600));
    batch.put_owned("track_lookup", lookup_key(track), address);
    batch.put_owned("event_log", event_key(track), small(rng, 160));
    if track.is_multiple_of(SLICE_EVERY) {
        let mut slice = vec![0u8; SLICE_BYTES];
        rng.fill_bytes(&mut slice);
        batch.put_owned("slice", slice_key(track).to_vec(), slice);
        batch.put_owned(
            "spool_pending_repair",
            slice_key(track).to_vec(),
            vec![1u8; 8],
        );
    }
}

/// Deletes one track's rows the way a node's collection does
fn delete_track(batch: &mut WriteBatch, track: usize) {
    let address = address(track).to_vec();
    batch.delete_owned("track", address.clone());
    batch.delete_owned("track_data", address);
    batch.delete_owned("track_lookup", lookup_key(track));
    batch.delete_owned("event_log", event_key(track));
    if track.is_multiple_of(SLICE_EVERY) {
        batch.delete_owned("slice", slice_key(track).to_vec());
        batch.delete_owned("spool_pending_repair", slice_key(track).to_vec());
    }
}

/// What maintenance has done so far, which stops moving once it settles
fn progress(store: &ReelStore) -> (ByteCount, u64, u64, u64) {
    let counters = store.compaction_counters();
    (
        store.dead_bytes(),
        counters.segments_rewritten,
        counters.segments_unlinked_whole,
        counters.runs_merged,
    )
}

/// Runs maintenance until a few ticks in a row change nothing
fn settle(store: &ReelStore) -> usize {
    let mut quiet = 0usize;
    let mut last = progress(store);
    for tick in 0..MAX_TICKS {
        Store::reclaim_space(store).expect("reclaim");
        store.maintain_once().expect("maintain");
        let now = progress(store);
        match now == last {
            true => quiet += 1,
            false => quiet = 0,
        }
        last = now;
        if quiet == 4 {
            return tick;
        }
    }
    MAX_TICKS
}

// a mass delete and its maintenance leave glibc holding little freed heap
#[test]
#[ignore = "writes 4 GiB of real files, run explicitly with --ignored --nocapture"]
fn held_heap() {
    println!();
    let home = TempDir::new_in(env!("CARGO_TARGET_TMPDIR")).expect("tempdir");
    let store = ReelStore::open(home.path().to_path_buf(), config(), COLUMNS).expect("open");
    let mut rng = SmallRng::seed_from_u64(7);
    report("open", &store);

    for start in (0..TRACKS).step_by(64) {
        let mut batch = WriteBatch::new();
        for track in start..(start + 64).min(TRACKS) {
            put_track(&mut batch, &mut rng, track);
        }
        store.write_batch(batch).expect("load");
    }
    Store::put(&store, "meta", b"epoch", &[1u8; 8]).expect("meta");
    store.flush().expect("flush");
    let ticks = settle(&store);
    report("loaded", &store);
    println!("settled in {ticks} ticks");
    let before = heap();

    let mut picker = SmallRng::seed_from_u64(11);
    let mut deleted = 0usize;
    let mut batch = WriteBatch::new();
    for track in 0..TRACKS {
        if picker.gen_range(0..100) < DELETED_PERCENT {
            delete_track(&mut batch, track);
            deleted += 1;
        }
        if batch.len() >= 256 {
            store
                .write_batch(std::mem::take(&mut batch))
                .expect("delete");
        }
    }
    store.write_batch(batch).expect("delete");
    store.flush().expect("flush");
    report("deleted", &store);
    println!("deleted {deleted} of {TRACKS} tracks");

    let ticks = settle(&store);
    report("settled", &store);
    let counters = store.compaction_counters();
    println!(
        "settled in {ticks} ticks, rewritten {} unlinked {} runs merged {}",
        counters.segments_rewritten, counters.segments_unlinked_whole, counters.runs_merged,
    );
    let after = heap();

    assert!(
        counters.segments_rewritten + counters.segments_unlinked_whole > 0,
        "maintenance retired nothing, so the burst measured nothing"
    );
    let grown = after.held_free.saturating_sub(before.held_free);
    assert!(
        grown < HELD_CEILING,
        "glibc holds {:.1} MiB more freed heap after the burst, with {:.1} MiB in use",
        mib(grown),
        mib(after.in_use),
    );
}
