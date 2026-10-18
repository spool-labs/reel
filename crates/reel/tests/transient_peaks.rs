//! The peak heap that a short walk and a seal take, counted by the global allocator
//! Run with `cargo test --release -p tape-reel --test transient_peaks -- --nocapture`

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use tempfile::TempDir;

use reel::format::column::{KeyBytes, RecordKey};
use reel::format::footer::{FooterEntry, SegmentFooter};
use reel::format::loc::{Loc, SegmentId};
use reel::format::lsn::Lsn;
use reel::format::record::Flags;
use reel::index::column::ColumnIndex;
use reel::index::counters::SegmentTable;
use reel::index::entry::Entry;
use reel::index::paged::SealedRanges;
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, CompactPass, KeyWidth, ReelConfig,
    ReelStore, SyncPolicy, ThreadBudget,
};
use reel_core::{Direction, Store, WriteBatch};

/// Bytes live across every thread
static LIVE: AtomicUsize = AtomicUsize::new(0);

/// The most bytes live at once since the last reset
static PEAK: AtomicUsize = AtomicUsize::new(0);

/// Tests in this file share the counters, so they take turns
static TURN: Mutex<()> = Mutex::new(());

/// The system allocator with a running total and its high mark
struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        grow(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        grow(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // A large block shrinks where it lies, so a shrink only gives bytes back
        if new_size <= layout.size() {
            LIVE.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            return unsafe { System.realloc(ptr, layout, new_size) };
        }
        // Both buffers are live while the bytes move
        grow(new_size);
        let moved = unsafe { System.realloc(ptr, layout, new_size) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        moved
    }
}

fn grow(bytes: usize) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The peak bytes `work` adds over what was live when it started
fn peak_of<Out>(work: impl FnOnce() -> Out) -> (usize, Out) {
    let start = LIVE.load(Ordering::Relaxed);
    PEAK.store(start, Ordering::Relaxed);
    let out = work();
    (PEAK.load(Ordering::Relaxed).saturating_sub(start), out)
}

fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

const WALKED: &str = "walked";
const FILLER: &str = "filler";

/// A small column a walk reads, beside a wide one that makes every footer heavy
const COLUMNS: ColumnSet = &[
    ColumnSpec {
        id: ColumnId(1),
        name: WALKED,
        key_width: KeyWidth::Fixed(16),
        shard_bytes: 0,
        purge_mark: None,
        codec: Codec::None,
    },
    ColumnSpec {
        id: ColumnId(2),
        name: FILLER,
        key_width: KeyWidth::Fixed(32),
        shard_bytes: 0,
        purge_mark: None,
        codec: Codec::None,
    },
];

/// Filler rows written beside each walked row
const FILLER_PER_ROW: u64 = 16;

/// Walked rows per segment, about what one segment of this size holds
const ROWS_PER_SEGMENT: u64 = 1_000;

/// Rows a short walk takes
const FEW: usize = 5;

/// A short walk may add this much heap, whatever the segment count
const WALK_CEILING: usize = 512 * 1024;

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(1024 * 1024),
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        scrub_mbps: 0,
        ..ReelConfig::default()
    }
}

fn walked_key(row: u64) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[8..].copy_from_slice(&row.to_be_bytes());
    key
}

/// Ascending keys, so no two segments overlap and no merge covers them
fn filler_key(row: u64) -> [u8; 32] {
    let mut key = [0x5au8; 32];
    key[..8].copy_from_slice(&row.to_be_bytes());
    key
}

/// A store whose sealed segments are all read through their footers, reopened so nothing is held
fn sealed_store(dir: &TempDir, segments: u64) -> ReelStore {
    let store = ReelStore::open(dir.path().to_path_buf(), config(), COLUMNS).expect("open");
    for row in 0..segments * ROWS_PER_SEGMENT {
        Store::put(&store, WALKED, &walked_key(row), b"w").expect("walked");
        for at in 0..FILLER_PER_ROW {
            Store::put(&store, FILLER, &filler_key(row * FILLER_PER_ROW + at), b"f")
                .expect("filler");
        }
    }
    store.maintain_once().expect("tick");
    reopened(dir, store)
}

/// The store closed and opened again, so no walk has opened its runs yet
fn reopened(dir: &TempDir, store: ReelStore) -> ReelStore {
    store.close().expect("close");
    drop(store);
    let store = ReelStore::open(dir.path().to_path_buf(), config(), COLUMNS).expect("reopen");
    store.page_out_sealed().expect("page out");
    store
}

/// The peak heap of a short range walk and of a walk's first few rows
fn walk_peaks(segments: u64) -> (usize, usize) {
    let dir = TempDir::new().expect("tempdir");
    let store = sealed_store(&dir, segments);
    let middle = segments * ROWS_PER_SEGMENT / 2;

    let (ranged, rows) = peak_of(|| {
        Store::iter_range(
            &store,
            WALKED,
            &walked_key(middle),
            &walked_key(middle + FEW as u64),
        )
        .expect("range")
        .count()
    });
    assert_eq!(rows, FEW, "the range walk returns its rows");

    let store = reopened(&dir, store);
    let (first, rows) = peak_of(|| {
        Store::iter_from(&store, WALKED, &walked_key(0), Direction::Asc)
            .expect("iter_from")
            .take(FEW)
            .count()
    });
    assert_eq!(rows, FEW, "the walk from the start returns its rows");
    (ranged, first)
}

// a short walk's heap stays bounded however many sealed segments the column spans
#[test]
fn a_short_walk_reads_no_whole_footers() {
    let _turn = TURN.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut worst = 0;
    for segments in [8, 32] {
        let (ranged, first) = walk_peaks(segments);
        println!(
            "{segments} segments: range walk {:.2} MiB, first rows {:.2} MiB",
            mib(ranged),
            mib(first)
        );
        worst = worst.max(ranged).max(first);
    }
    assert!(
        worst < WALK_CEILING,
        "a short walk peaked at {:.2} MiB",
        mib(worst)
    );
}

/// Rows in the footer a seal replaces
const SEALED_ROWS: u64 = 100_000;

/// A fresh footer may reserve this much, whatever the footer before it held
const FRESH_CEILING: usize = 64 * 1024;

// the footer a seal starts the next segment with reserves nothing for the old footer's rows
#[test]
fn a_seal_reserves_no_old_footer() {
    let _turn = TURN.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut last = SegmentFooter::empty();
    for row in 0..SEALED_ROWS {
        let key = RecordKey::from_bytes(ColumnId(2), &filler_key(row)).expect("key");
        last.push(&FooterEntry::new(
            key,
            Lsn(row + 1),
            row as u32,
            1,
            Flags::DATA,
        ));
    }

    let (fresh, next) = peak_of(|| last.empty_like());
    println!(
        "a fresh footer after {SEALED_ROWS} rows took {:.2} MiB",
        mib(fresh)
    );
    assert!(next.is_empty(), "the fresh footer holds no rows");
    assert!(
        fresh < FRESH_CEILING,
        "the fresh footer reserved {:.2} MiB",
        mib(fresh)
    );
}

/// Rows that outlive the kill, the same whatever the segment holds
const KEPT_ROWS: u64 = 100_000;

/// A compaction pass may add this much heap, whatever the segment's row count
const COMPACT_CEILING: usize = 32 * 1024 * 1024;

/// Rows per write batch
const BATCH_ROWS: u64 = 10_000;

/// Bytes in each filler record that pushes the rows' segment to its seal
const FILLER_BYTES: usize = 1024 * 1024;

/// One segment large enough for every row, and a footer cache too small for its footer
fn compact_config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(256 * 1024 * 1024),
        footer_cache: ByteCount::from_bytes(1024 * 1024),
        ..config()
    }
}

/// Write a segment's worth of filler and delete it, so the rows' segment seals
fn seal_with_filler(store: &ReelStore) {
    let value = vec![0x33u8; FILLER_BYTES];
    let fillers = compact_config().segment_bytes.to_bytes() / FILLER_BYTES as u64;
    for row in 0..fillers {
        Store::put(store, FILLER, &filler_key(row), &value).expect("filler");
    }
    for row in 0..fillers {
        Store::delete(store, FILLER, &filler_key(row)).expect("filler delete");
    }
}

/// The peak heap of compacting one sealed segment of `rows` small records, most of them deleted
fn compact_peak(rows: u64) -> usize {
    let dir = TempDir::new().expect("tempdir");
    let store = ReelStore::open(dir.path().to_path_buf(), compact_config(), COLUMNS).expect("open");
    let value = [7u8; 16];
    for first in (0..rows).step_by(BATCH_ROWS as usize) {
        let mut batch = WriteBatch::new();
        for row in first..(first + BATCH_ROWS).min(rows) {
            batch.put(WALKED, &walked_key(row), &value);
        }
        Store::write_batch(&store, batch).expect("puts");
    }
    seal_with_filler(&store);

    let every = rows / KEPT_ROWS;
    for first in (0..rows).step_by(BATCH_ROWS as usize) {
        let mut batch = WriteBatch::new();
        for row in (first..(first + BATCH_ROWS).min(rows)).filter(|row| row % every != 0) {
            batch.delete(WALKED, &walked_key(row));
        }
        Store::write_batch(&store, batch).expect("deletes");
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");

    let before = store.compaction_counters();
    let mut peak = 0;
    for _ in 0..4 {
        let (taken, pass) = peak_of(|| store.compact_once().expect("compact"));
        peak = peak.max(taken);
        if pass == CompactPass::Copied {
            break;
        }
    }
    let after = store.compaction_counters();
    assert!(
        after.segments_rewritten > before.segments_rewritten,
        "the pass rewrote the segment"
    );

    let kept = Store::iter_from(&store, WALKED, &walked_key(0), Direction::Asc)
        .expect("iter_from")
        .count() as u64;
    assert_eq!(kept, rows.div_ceil(every), "every kept row survives");
    for row in [0, 1, every, rows / 2, rows - 1] {
        let held = Store::get(&store, WALKED, &walked_key(row)).expect("get");
        assert_eq!(held.is_some(), row % every == 0, "row {row} reads as it should");
    }
    peak
}

// a compaction pass's heap stays bounded however many rows the segment's footer lists
#[test]
fn a_compaction_pass_reads_no_whole_footer() {
    let _turn = TURN.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut worst = 0;
    for rows in [1_000_000, 2_000_000] {
        let peak = compact_peak(rows);
        println!("{rows} rows: compaction peak {:.2} MiB", mib(peak));
        worst = worst.max(peak);
    }
    assert!(
        worst < COMPACT_CEILING,
        "a compaction pass peaked at {:.2} MiB",
        mib(worst)
    );
}

/// The node's slice column: a two-byte spool, then a track address
const SLICES: ColumnSpec = ColumnSpec {
    id: ColumnId(14),
    name: "slice",
    key_width: KeyWidth::Fixed(34),
    shard_bytes: 2,
    purge_mark: None,
    codec: Codec::None,
};

/// The segment every tombstone lands in, sealed before the prune
const GRAVES_AT: SegmentId = SegmentId(2);

/// A prune may add this much past the packed shard it builds
const PRUNE_SLACK: usize = 1024 * 1024;

/// One spool's key, its address spread like a hash
fn slice_key(row: u64) -> [u8; 34] {
    let mut key = [0u8; 34];
    key[..2].copy_from_slice(&7u16.to_be_bytes());
    let mut word = row;
    for at in 0..4 {
        word = word.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut mixed = word;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^= mixed >> 31;
        key[2 + at * 8..10 + at * 8].copy_from_slice(&mixed.to_be_bytes());
    }
    key
}

/// A prune's peak over `rows` keys with nine in ten deleted, and the packed shard's heap
fn prune_peak(rows: u64) -> (usize, usize) {
    let index = ColumnIndex::new(&SLICES).expect("index");
    let segments = SegmentTable::new();
    for row in 0..rows {
        let entry = Entry::new(Loc::new(SegmentId(1), row as u32, 10), Lsn(row + 1));
        index.insert(&slice_key(row), entry, &segments, &|_, _| false);
    }
    let tombstone = Loc::new(GRAVES_AT, 0, 0);
    for row in (0..rows).filter(|row| row % 10 != 0) {
        index.remove(
            &slice_key(row),
            Lsn(rows + row + 1),
            tombstone,
            &segments,
            &|_, _| false,
        );
    }
    let sealed = SealedRanges::new();
    let edge = KeyBytes::new(&[0u8; 34]).expect("key");
    sealed.note(GRAVES_AT, edge.clone(), edge);

    let empty = ColumnIndex::new(&SLICES).expect("index").heap_bytes();
    let (peak, pruned) = peak_of(|| index.prune_tombstones(Lsn(u64::MAX), &sealed));
    assert_eq!(pruned, rows - rows.div_ceil(10), "every grave was prunable");
    for row in (0..rows).step_by(10) {
        assert!(index.get(&slice_key(row)).is_some(), "a survivor went missing");
    }
    (peak, (index.heap_bytes() - empty) as usize)
}

// a prune that packs a shard holds no more than the packed shard on top of the old one
#[test]
fn a_prune_packs_in_place() {
    let _turn = TURN.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    for rows in [250_000, 1_000_000] {
        let (peak, packed) = prune_peak(rows);
        println!(
            "{rows} rows: prune peaked {:.2} MiB beside a packed shard of {:.2} MiB",
            mib(peak),
            mib(packed)
        );
        assert!(
            peak <= packed + PRUNE_SLACK,
            "the prune of {rows} rows peaked at {:.2} MiB",
            mib(peak)
        );
    }
}

/// A footer of `rows` slice rows in hashed key order, nine in ten of them tombstones
fn graves_footer(rows: u64) -> SegmentFooter {
    let mut footer = SegmentFooter::empty();
    for row in 0..rows {
        let key = RecordKey::from_bytes(SLICES.id, &slice_key(row)).expect("key");
        let flags = match row % 10 {
            0 => Flags::DATA,
            _ => Flags::TOMBSTONE,
        };
        footer.push(&FooterEntry::new(key, Lsn(row + 1), row as u32, 100, flags));
    }
    footer
}

/// A pack may hold this much per row it sorts, for the order and the moves it walks
const PACK_BYTES_PER_ROW: usize = 24;

/// A pack may add this much whatever the row count, for the stretch in hand, the filter and the tail
const PACK_SLACK: usize = 8 * 1024 * 1024;

/// The peak heap of packing a footer of `rows` graves, and how long the pack took
fn seal_footer_peak(rows: u64) -> (usize, std::time::Duration) {
    let mut footer = graves_footer(rows);
    let began = std::time::Instant::now();
    // Each stretch is dropped once handed over, the way a seal writes it and lets it go
    let (peak, _) = peak_of(|| footer.pack_into(10, |_| Ok(())).expect("pack"));
    (peak, began.elapsed())
}

// a seal's footer pack holds its sort order and a stretch of rows, never another copy of them
#[test]
fn a_seal_packs_its_footer_without_copies() {
    let _turn = TURN.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    for rows in [1_000_000, 4_000_000] {
        let (peak, took) = seal_footer_peak(rows);
        println!("{rows} rows: footer pack peaked {:.2} MiB in {took:.2?}", mib(peak));
        let ceiling = rows as usize * PACK_BYTES_PER_ROW + PACK_SLACK;
        assert!(
            peak <= ceiling,
            "the pack of {rows} rows peaked at {:.2} MiB, over {:.2} MiB",
            mib(peak),
            mib(ceiling)
        );
    }
}
