//! What the footer filter removes, in searches and in time
//!
//! A paged column answers a key its map gave up by searching every sealed segment
//! whose key range covers it, and the keys here are uniform, so every range covers
//! every key and a miss visits the whole volume. Counts rather than microseconds: how
//! many searches a miss makes is the engine's answer and is the same everywhere, while
//! how long one costs is the device's and wants a real box.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::TempDir;

use reel::config::{IndexResidency, ReelConfig, SyncPolicy, ThreadBudget};
use reel::format::column::{Codec, ColumnId, ColumnSet, ColumnSpec, MapShape, RecordKey};
use reel::io::fault::FaultPlan;
use reel::io::sim_backend::SimIo;
use reel::units::ByteCount;
use reel::{KeyWidth, Preallocate, ProbeCounts, ReelStore, MAP_EVERYTHING};

const ROWS: ColumnId = ColumnId(1);

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ROWS,
    name: "rows",
    key_width: KeyWidth::Fixed(16),
    shard_bytes: 0,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

/// Keys written, spread over the space so every segment's range covers every key
const KEYS: u64 = 6_000;

/// Payload each key carries, sized so the volume seals several segments
const PAYLOAD: usize = 512;

/// Keys looked up that were never written
const MISSES: u64 = 2_000;

/// Searches a get the per-segment bits have to leave, past the one holding the key
///
/// One, plus room for the odd false positive. What a filter is worth is what it leaves
/// rather than the share of asks it removes, since the walk stops once no segment left
/// can hold a newer row.
const SEARCHES_PER_GET: f64 = 1.5;

/// How much of the unfiltered search work the bits have to remove to earn them
const SEARCH_SAVING: u64 = 4;

/// A key nothing about its bytes tells you the order of
///
/// The segments each take a run of writes, so ordered keys would give each a tidy
/// range and the range check alone would rule most of them out. Scattering is what a
/// column keyed by hash or address does on its own.
fn key(at: u64) -> RecordKey {
    let mixed = at
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .rotate_left(29)
        .wrapping_mul(0xBF58_476D_1CE4_E5B9);
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&mixed.to_be_bytes());
    bytes[8..].copy_from_slice(&at.to_be_bytes());
    RecordKey::from_bytes(ROWS, &bytes).expect("key")
}

fn config(filter_bits: u8) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(256 * 1024),
        alloc_chunk: ByteCount::from_bytes(64 * 1024),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        index: IndexResidency::Paged,
        filter_bits,
        // The read path the engine serves callers with
        map_above: MAP_EVERYTHING,
        ..ReelConfig::default()
    }
}

/// A paged volume of sealed segments, with its keys handed to the footers
fn filled(filter_bits: u8) -> ReelStore {
    let store = ReelStore::open_with_io(
        PathBuf::from("/filters"),
        config(filter_bits),
        COLUMNS,
        Arc::new(SimIo::new(FaultPlan::new(5))),
    )
    .expect("open");

    let payload = vec![0x3Cu8; PAYLOAD];
    for at in 0..KEYS {
        store.put(&key(at), &payload).expect("put");
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");
    store
}

/// Probe for keys that were never written, and say what the segments were asked
fn miss_counts(store: &ReelStore) -> ProbeCounts {
    let before = store.filter_probes();
    for at in KEYS..KEYS + MISSES {
        assert!(
            store.get(&key(at)).expect("get").is_none(),
            "a key nothing wrote"
        );
    }
    store.filter_probes().since(before)
}

/// Probe for keys that were written, and say what finding them cost
fn hit_counts(store: &ReelStore, keys: u64) -> ProbeCounts {
    let before = store.filter_probes();
    for at in 0..keys {
        assert!(
            store.get(&key(at)).expect("get").is_some(),
            "key {at} went missing"
        );
    }
    store.filter_probes().since(before)
}

// a probe costs what its class deserves: a miss no segment, a hit one search
//
// The sealed-keys filter stands ahead of the fan-out, so a key nothing wrote is
// answered before any segment is asked. What the per-segment bits still remove is the
// searches of keys that exist somewhere.
pub fn filters_remove_the_searches() {
    let unfiltered = filled(0);

    // Misses die at the sealed-keys filter, bits or none. The few that leak through are
    // its false positives rather than a fan-out.
    let bare_misses = miss_counts(&unfiltered);
    assert!(
        bare_misses.asked < MISSES,
        "{} segment asks for {MISSES} misses reached past the sealed-keys filter",
        bare_misses.asked,
    );

    // Hits are the per-segment filters' remaining work.
    let bare = hit_counts(&unfiltered, KEYS);
    assert!(
        bare.asked > KEYS,
        "a hit should visit more than one segment"
    );
    assert_eq!(
        bare.skipped, 0,
        "nothing rules a segment out without a filter"
    );

    let filtered = filled(10);
    let counts = hit_counts(&filtered, KEYS);
    assert_eq!(
        counts.asked, bare.asked,
        "the same segments are asked either way"
    );
    println!(
        "  asks/get {:.2}, searches/get {:.2} bare against {:.2} filtered, {:.1}% of asks ruled out",
        bare.asked as f64 / KEYS as f64,
        bare.searched() as f64 / KEYS as f64,
        counts.searched() as f64 / KEYS as f64,
        counts.skipped as f64 / counts.asked as f64 * 100.0,
    );

    // Weighed on the searches the bits leave rather than the share of the asks they
    // remove, since the walk stops once no segment left can hold a newer row and most
    // of the segments a filter used to rule out are no longer asked about at all.
    let searches = counts.searched() as f64 / KEYS as f64;
    assert!(
        searches < SEARCHES_PER_GET,
        "{searches:.2} searches a get through a filter, past the one segment holding the key",
    );
    assert!(
        counts.searched() * SEARCH_SAVING < bare.searched(),
        "{} searches with a filter against {} without, which is not worth their bytes",
        counts.searched(),
        bare.searched(),
    );
}

// every key written is still found through a filter, which is the only hard rule
pub fn nothing_written_is_lost() {
    let store = filled(10);

    for at in 0..KEYS {
        assert!(
            store.get(&key(at)).expect("get").is_some(),
            "key {at} went missing"
        );
    }
}

// a deleted key stays deleted, since its tombstone is in the filter with the rest
pub fn tombstones_are_filtered_in() {
    let store = filled(10);
    for at in 0..64 {
        store.delete(&key(at)).expect("delete");
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");

    for at in 0..64 {
        assert!(
            store.get(&key(at)).expect("get").is_none(),
            "key {at} came back"
        );
    }
    for at in 64..128 {
        assert!(
            store.get(&key(at)).expect("get").is_some(),
            "key {at} went with them"
        );
    }
}

/// A volume and whatever has to stay alive beside it
struct Volume {
    /// The volume the rows are timed against
    store: ReelStore,

    /// Held so a real directory outlives the store rooted in it
    _dir: Option<TempDir>,
}

/// The same volume on a real directory, so the page cache takes part
fn filled_posix(bits: u8) -> Volume {
    filled_at(config(bits))
}

/// A real volume that gives its read pages back, so a search is a read again
///
/// The rows above are floors because a footer block a search wants sits in the page
/// cache from the write that put it there. Only Linux honours the request, so on any
/// other machine this row is the warm one wearing a label.
fn filled_cold(bits: u8) -> Volume {
    filled_at(ReelConfig { ..config(bits) })
}

fn filled_at(config: ReelConfig) -> Volume {
    let dir = TempDir::new().expect("tempdir");
    let store = ReelStore::open(dir.path().to_path_buf(), config, COLUMNS).expect("open");
    let payload = vec![0x3Cu8; PAYLOAD];
    for at in 0..KEYS {
        store.put(&key(at), &payload).expect("put");
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");
    Volume {
        store,
        _dir: Some(dir),
    }
}

/// Time a miss and a hit at each filter size, against the same volume unfiltered
fn miss_gain(sizes: &[u8], open: impl Fn(u8) -> Volume) {
    println!(
        "{:>6} {:>14} {:>14} {:>10}",
        "bits", "per miss", "per hit", "miss gain"
    );

    let mut bare_miss = 0f64;
    for bits in sizes {
        let volume = open(*bits);
        // Warm first, so what is timed is the search rather than the first touch of
        // every footer on the volume.
        miss_counts(&volume.store);

        let began = Instant::now();
        for at in KEYS..KEYS + MISSES {
            volume.store.get(&key(at)).expect("get");
        }
        let miss = began.elapsed().as_secs_f64() / MISSES as f64;

        let began = Instant::now();
        for at in 0..MISSES {
            volume.store.get(&key(at)).expect("get");
        }
        let hit = began.elapsed().as_secs_f64() / MISSES as f64;

        if *bits == 0 {
            bare_miss = miss;
        }
        println!(
            "{:>6} {:>14} {:>14} {:>9.2}x",
            bits,
            format!("{:.2?}", Duration::from_secs_f64(miss)),
            format!("{:.2?}", Duration::from_secs_f64(hit)),
            bare_miss / miss.max(f64::MIN_POSITIVE),
        );
    }
}

// what a miss costs with the searches and without them, which is the floor of the win
//
// The simulator holds its files in memory, so a search here is a memcpy off a warm
// buffer. That is the cheapest a search can be, which makes the ratio a lower bound.
pub fn miss_time() {
    // libtest leaves the test name line open, so a header needs a newline ahead of it.
    println!();
    miss_gain(&[0, 4, 7, 10, 14], |bits| Volume {
        store: filled(bits),
        _dir: None,
    });
}

// the same pair against real descriptors and the real page cache
pub fn miss_time_posix() {
    // libtest leaves the test name line open, so a header needs a newline ahead of it.
    println!();
    miss_gain(&[0, 10], filled_posix);
}

// and against a volume that gives its pages back, where a search is a real read
//
// The only one of the three rows that is not a floor, and Linux only in any
// meaningful sense.
pub fn miss_time_cold() {
    // libtest leaves the test name line open, so a header needs a newline ahead of it.
    println!();
    miss_gain(&[0, 4, 7, 10, 14], filled_cold);
}

// what the filter costs and what it removes, at the sizes worth considering
pub fn filter_cost() {
    // libtest leaves the test name line open, so a header needs a newline ahead of it.
    println!();
    println!(
        "{:>6} {:>12} {:>12} {:>10} {:>14} {:>14}",
        "bits", "asked", "searched", "removed", "blocks/search", "filter bytes",
    );
    for bits in [0u8, 4, 7, 10, 14] {
        let store = filled(bits);
        let counts = miss_counts(&store);

        println!(
            "{:>6} {:>12} {:>12} {:>9.1}% {:>14.2} {:>14}",
            bits,
            counts.asked,
            counts.searched(),
            (counts.skipped as f64 / counts.asked as f64) * 100.0,
            counts.blocks as f64 / counts.searched().max(1) as f64,
            KEYS * u64::from(bits) / 8,
        );
    }
}

/// The same volume with no room to hold a parsed footer
///
/// A volume whose footers fit answers a key by searching one in memory and never
/// touches a block, so shrinking the bound is what puts a laptop on the path a volume
/// past it runs on all the time.
fn filled_blocked(filter_bits: u8) -> ReelStore {
    let store = ReelStore::open_with_io(
        PathBuf::from("/blocked"),
        ReelConfig {
            footer_cache: ByteCount::from_bytes(0),
            ..config(filter_bits)
        },
        COLUMNS,
        Arc::new(SimIo::new(FaultPlan::new(5))),
    )
    .expect("open");

    let payload = vec![0x3Cu8; PAYLOAD];
    for at in 0..KEYS {
        store.put(&key(at), &payload).expect("put");
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");
    store
}

// a hit against a held footer reads no blocks at all
//
// A footer in hand is searched in memory, so the block counter means nothing without
// knowing which of the two search paths a volume is on.
pub fn a_held_footer_costs_no_blocks() {
    let store = filled(0);
    let counts = hit_counts(&store, KEYS);

    assert!(counts.searched() > 0, "no search reached a sealed segment");
    assert_eq!(
        counts.blocks, 0,
        "a footer held whole should be searched in memory"
    );
}

// a hit past the footer bound costs a run of block loads before it reaches its row
//
// The search binary searches the partition's blocks by their first key and then reads
// the one block that could hold it, so the bound is one load per halving of the block
// count plus the final block. A count above that is no longer a binary search.
pub fn a_blocked_hit_costs_a_run_of_block_loads() {
    let store = filled_blocked(0);
    let counts = hit_counts(&store, KEYS);

    assert!(
        counts.blocks > counts.searched(),
        "a search that loaded no block found its row somewhere else",
    );
    let per_search = counts.blocks as f64 / counts.searched() as f64;
    // Blocks in the partition a search crosses. Segments take equal runs of the writes
    // here, so one segment's share of the keys is what the halvings count against.
    let segments = store.index().segments_snapshot().len().max(1) as f64;
    let blocks = (KEYS as f64 / segments).max(2.0);
    let bound = blocks.log2() + 2.0;
    assert!(
        per_search <= bound,
        "{per_search:.1} block loads a search, past the {bound:.1} a binary search over at most {blocks:.0} blocks costs",
    );
}

/// Record the depth sweep carries, small so the depth comes from the segment size
const DEEP_PAYLOAD: usize = 64;

/// Bytes a record of that size costs a segment, key, header and all
///
/// Sizes a segment by the keys wanted in it rather than by bytes. Near enough is
/// enough, since the row prints the segments it actually got.
const DEEP_RECORD_BYTES: u64 = 128;

/// Sealed segments each row of the sweep writes
///
/// Held constant across the rows, since a moving segment count would fold the fan-out
/// into the depth the sweep is trying to measure.
const DEEP_SEGMENTS: u64 = 4;

/// A blocked volume of a given depth, with the whole of it handed to footers
fn paged_blocked(keys_per_segment: u64, filter_bits: u8) -> (ReelStore, u64) {
    let keys = keys_per_segment * DEEP_SEGMENTS;
    let store = ReelStore::open_with_io(
        PathBuf::from("/stride"),
        ReelConfig {
            footer_cache: ByteCount::from_bytes(0),
            segment_bytes: ByteCount::from_bytes(keys_per_segment * DEEP_RECORD_BYTES),
            ..config(filter_bits)
        },
        COLUMNS,
        Arc::new(SimIo::new(FaultPlan::new(5))),
    )
    .expect("open");

    let payload = vec![0x3Cu8; DEEP_PAYLOAD];
    for at in 0..keys {
        store.put(&key(at), &payload).expect("put");
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");
    (store, keys)
}

// what a paged lookup asks for before it touches its record, as the partition deepens
//
// The segment count is held still and only the keys in each one move, so what
// separates the rows is the depth of one binary search. The reads column is what the
// block cache leaves for the device once the search has asked, and it sits just under
// the loads column by construction: one bound covers footers, directories and blocks
// alike, so a volume with no room for a footer has none for a block either.
pub fn paged_lookup_block_reads() {
    println!();
    println!(
        "{:>14} {:>10} {:>12} {:>14} {:>14}",
        "keys/segment", "segments", "searches", "blocks/search", "reads/search",
    );
    for keys_per_segment in [10_000u64, 40_000, 160_000] {
        let (store, keys) = paged_blocked(keys_per_segment, 10);
        let segments = store.index().segments_snapshot().len().max(1);
        let counts = hit_counts(&store, keys);
        println!(
            "{:>14} {:>10} {:>12} {:>14.2} {:>14.2}",
            keys_per_segment,
            segments,
            counts.searched(),
            counts.blocks as f64 / counts.searched().max(1) as f64,
            counts.block_reads as f64 / counts.searched().max(1) as f64,
        );
    }
}

/// Keys a bounded-cache row reads over and over, the working set inside the volume
const HOT_KEYS: u64 = 200;

/// Rounds the hot set is read for, so a policy has passes to get it wrong in
const HOT_ROUNDS: u64 = 20;

/// A paged volume whose footer cache holds a share of what it would like to
///
/// Big enough that the hot working set fits several times over, small enough that a
/// scan of the whole volume cannot stay resident beside it. That gap is where an
/// eviction policy is the only thing separating two caches.
fn filled_bounded(cache_bytes: u64) -> ReelStore {
    let store = ReelStore::open_with_io(
        PathBuf::from("/bounded"),
        ReelConfig {
            footer_cache: ByteCount::from_bytes(cache_bytes),
            ..config(10)
        },
        COLUMNS,
        Arc::new(SimIo::new(FaultPlan::new(5))),
    )
    .expect("open");

    let payload = vec![0x3Cu8; PAYLOAD];
    for at in 0..KEYS {
        store.put(&key(at), &payload).expect("put");
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");
    store
}

// what a hot working set costs once the cache cannot hold the whole volume
//
// Counts rather than time: how many reads a policy leaves is the engine's answer and
// is the same on any machine. The scan between rounds is the term that decides it,
// since strict insertion order gives up the hot entries on schedule however often
// they were read, and a hand that clears a bit only on the way past does not.
pub fn a_bounded_cache_keeps_its_working_set() {
    println!();
    println!("| cache bytes | reads/hot ask | reads/scan ask |");
    println!("|---|---|---|");
    for cache_bytes in [64u64 * 1024, 256 * 1024, 1024 * 1024] {
        let store = filled_bounded(cache_bytes);
        // One pass over everything first, so the rows below are steady state rather
        // than the cost of filling an empty cache.
        let _ = hit_counts(&store, KEYS);

        let mut hot = ProbeCounts::default();
        let mut scan = ProbeCounts::default();
        for _ in 0..HOT_ROUNDS {
            let before = store.filter_probes();
            for at in 0..HOT_KEYS {
                assert!(store.get(&key(at)).expect("get").is_some());
            }
            hot = add(hot, store.filter_probes().since(before));

            // The scan is what evicts: it walks keys the hot set never asks for.
            let before = store.filter_probes();
            for at in HOT_KEYS..KEYS {
                assert!(store.get(&key(at)).expect("get").is_some());
            }
            scan = add(scan, store.filter_probes().since(before));
        }

        println!(
            "| {cache_bytes} | {:.3} | {:.3} |",
            hot.block_reads as f64 / (HOT_KEYS * HOT_ROUNDS) as f64,
            scan.block_reads as f64 / ((KEYS - HOT_KEYS) * HOT_ROUNDS) as f64,
        );
    }
}

/// Two readings of the counters, added rather than differenced
fn add(left: ProbeCounts, right: ProbeCounts) -> ProbeCounts {
    ProbeCounts {
        asked: left.asked + right.asked,
        skipped: left.skipped + right.skipped,
        blocks: left.blocks + right.blocks,
        block_reads: left.block_reads + right.block_reads,
        map_reads: left.map_reads + right.map_reads,
    }
}
