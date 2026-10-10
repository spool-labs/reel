//! How many segment searches the footer filter removes, and what that saves in time

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::TempDir;

use reel::config::{ReelConfig, SyncPolicy, ThreadBudget};
use reel::format::column::{Codec, ColumnId, ColumnSet, ColumnSpec, RecordKey};
use reel::format::loc::SegmentId;
use reel::io::fault::FaultPlan;
use reel::io::sim_backend::SimIo;
use reel::units::ByteCount;
use reel::{KeyWidth, ProbeCounts, ReelStore, MAP_EVERYTHING};

const ROWS: ColumnId = ColumnId(1);

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ROWS,
    name: "rows",
    key_width: KeyWidth::Fixed(16),
    shard_bytes: 0,
    purge_mark: None,
    codec: Codec::None,
}];

/// Keys written, spread over the space so every segment's range covers every key
const KEYS: u64 = 6_000;

/// Payload bytes per key, enough that the volume seals several segments
const PAYLOAD: usize = 512;

/// Keys looked up that were never written
const MISSES: u64 = 2_000;

/// A filtered get may average this many searches: one for the key plus room for false positives
const SEARCHES_PER_GET: f64 = 1.5;

/// Filters must cut the unfiltered searches by more than this factor
const SEARCH_SAVING: u64 = 4;

/// A key scattered over the space, so segment key ranges cannot rule it out
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
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        filter_bits,
        // Callers read on this path
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

/// Looks up keys that were never written and counts the segment probes
fn miss_counts(store: &ReelStore) -> ProbeCounts {
    let cue = store.cue().expect("cue");
    let before = store.filter_probes();
    for at in KEYS..KEYS + MISSES {
        assert!(
            store.get_at(&key(at), &cue).expect("get").is_none(),
            "a key nothing wrote"
        );
    }
    store.filter_probes().since(before)
}

/// Looks up written keys and counts the segment probes
fn hit_counts(store: &ReelStore, keys: u64) -> ProbeCounts {
    // Ask the index directly at a cue point, since the spot index answers store reads first
    let cue = store.cue().expect("cue");
    // The index only searches segments it knows of, and learns a cue's seal on the next store read
    store.page_out_sealed().expect("page out");
    let before = store.filter_probes();
    for at in 0..keys {
        assert!(
            store
                .index()
                .get_at(&key(at), cue.at())
                .expect("get")
                .is_some(),
            "key {at} went missing"
        );
    }
    store.filter_probes().since(before)
}

// a miss searches no segment and a filtered hit searches about one
pub fn filters_remove_the_searches() {
    let unfiltered = filled(0);

    // Misses die at the spot index, bits or none
    let bare_misses = miss_counts(&unfiltered);
    assert!(
        bare_misses.asked < MISSES,
        "{} segment asks for {MISSES} misses reached past the spot index",
        bare_misses.asked,
    );

    // Hits are what the per-segment filters have left to do
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

    // The walk already stops early, so check the searches left per get
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

// every written key is still found through a filter
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

/// A store and the directory it lives in
struct Volume {
    /// The store under test
    store: ReelStore,

    /// Keeps a real directory alive as long as the store
    _dir: Option<TempDir>,
}

/// The same volume on a real directory, so the page cache takes part
fn filled_posix(bits: u8) -> Volume {
    filled_at(config(bits))
}

/// A real volume for the cold row, with the same config as `filled_posix`
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
        // Warm up first so the timing leaves out the first touch of every footer
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

// what a miss costs at each filter size in memory, a lower bound on the gain
pub fn miss_time() {
    println!();
    miss_gain(&[0, 4, 7, 10, 14], |bits| Volume {
        store: filled(bits),
        _dir: None,
    });
}

// the same pair against real descriptors and the real page cache
pub fn miss_time_posix() {
    println!();
    miss_gain(&[0, 10], filled_posix);
}

// the same timing on a real directory at every filter size
pub fn miss_time_cold() {
    println!();
    miss_gain(&[0, 4, 7, 10, 14], filled_cold);
}

// filter bytes against the searches they remove, at each filter size
pub fn filter_cost() {
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
pub fn a_held_footer_costs_no_blocks() {
    let store = filled(0);
    // A seal leaves the footer cache empty, so seal the tail and then load each footer
    store.cue().expect("cue");
    for segment in 0..256 {
        store.segment_footer(SegmentId(segment)).expect("footer");
    }
    let counts = hit_counts(&store, KEYS);

    assert!(counts.searched() > 0, "no search reached a sealed segment");
    assert_eq!(
        counts.blocks, 0,
        "a footer held whole should be searched in memory"
    );
}

// a hit past the footer bound costs a binary search of block loads
pub fn a_blocked_hit_costs_a_run_of_block_loads() {
    let store = filled_blocked(0);
    let counts = hit_counts(&store, KEYS);

    assert!(
        counts.blocks > counts.searched(),
        "a search that loaded no block found its row somewhere else",
    );
    let per_search = counts.blocks as f64 / counts.searched() as f64;
    // Each segment holds an equal share of the keys, which bounds its block count
    let segments = store.index().segments_snapshot().len().max(1) as f64;
    let blocks = (KEYS as f64 / segments).max(2.0);
    let bound = blocks.log2() + 2.0;
    assert!(
        per_search <= bound,
        "{per_search:.1} block loads a search, past the {bound:.1} a binary search over at most {blocks:.0} blocks costs",
    );
}

/// The depth sweep writes records this small, so the segment size sets the depth
const DEEP_PAYLOAD: usize = 64;

/// One such record takes about this many bytes in a segment, key and header included
const DEEP_RECORD_BYTES: u64 = 128;

/// Each row of the sweep writes this many sealed segments
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

// block loads and device reads per paged lookup as the partition deepens
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

/// The bounded-cache rows read these keys over and over
const HOT_KEYS: u64 = 200;

/// The bounded-cache rows read the hot set this many times
const HOT_ROUNDS: u64 = 20;

/// A paged volume whose footer cache holds only part of its footers
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

// device reads for a hot working set once the cache cannot hold the whole volume
pub fn a_bounded_cache_keeps_its_working_set() {
    println!();
    println!("| cache bytes | reads/hot ask | reads/scan ask |");
    println!("|---|---|---|");
    for cache_bytes in [64u64 * 1024, 256 * 1024, 1024 * 1024] {
        let store = filled_bounded(cache_bytes);
        // Fill the cache with one full pass so the rows show steady state
        let _ = hit_counts(&store, KEYS);

        let mut hot = ProbeCounts::default();
        let mut scan = ProbeCounts::default();
        for _ in 0..HOT_ROUNDS {
            let before = store.filter_probes();
            for at in 0..HOT_KEYS {
                assert!(store.get(&key(at)).expect("get").is_some());
            }
            hot = add(hot, store.filter_probes().since(before));

            // The scan evicts: it reads every key outside the hot set
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

/// Sums two counter readings
fn add(left: ProbeCounts, right: ProbeCounts) -> ProbeCounts {
    ProbeCounts {
        asked: left.asked + right.asked,
        skipped: left.skipped + right.skipped,
        blocks: left.blocks + right.blocks,
        block_reads: left.block_reads + right.block_reads,
        map_reads: left.map_reads + right.map_reads,
    }
}
