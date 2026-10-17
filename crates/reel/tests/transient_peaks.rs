//! The peak heap that a short walk and a seal take, counted by the global allocator
//! Run with `cargo test --release -p tape-reel --test transient_peaks -- --nocapture`

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use tempfile::TempDir;

use reel::format::column::RecordKey;
use reel::format::footer::{FooterEntry, SegmentFooter};
use reel::format::lsn::Lsn;
use reel::format::record::Flags;
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, ReelConfig, ReelStore, SyncPolicy,
    ThreadBudget,
};
use reel_core::{Direction, Store};

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
