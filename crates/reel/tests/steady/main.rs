//! Zero allocations, zero frees and zero page faults once open has returned, on real files and real engine
//! threads. One run at a time, since the fault counters belong to the whole process
#![allow(clippy::expect_used, clippy::panic, clippy::print_stderr, clippy::print_stdout)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::backtrace::Backtrace;
use std::cell::Cell;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::process::{self, Command};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use reel::engine::unit_bounds::unit_bound;
use reel::engine::planner::MOST_WALKLESS_STEPS;
use reel::engine::reel_store::ReelStore;
use reel::segment::summary_orders::{SHARE_SCALE, reclaim_share};
use reel::engine::walk_job::FREE_IN_PLACE_BYTES;
use reel::guard::hold;
use reel::io::plain::PlainIo;
use reel::segment::filter_arena::FilterArena;
use reel::segment::segment_tail::SegmentNumber;
use reel_api::direction::Direction;
use reel_api::maintenance_report::{MaintenanceReport, UNIT_KINDS};
use reel_api::scan::ScanStep;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, Nanoseconds, RowCount, VolumeId};
use reel_api::column_id::ColumnId;
use reel::config::{ColumnConfig, KeyShape};
use reel::record::codec::Codec;
use reel_api::value_list::ValueList;
use reel_api::write_batch::WriteBatch;

mod store_config;

use store_config::{BULK, META, PLAIN, config, scratch_root};
use reel_sim::wait::wait_until;

const BUDGET: ByteCount = ByteCount(192 * 1024 * 1024);
const WORKERS_VARIABLE: &str = "REEL_STEADY_WORKERS";
// set in the child that runs the store again at 512 MiB, with the complete table on META and a sorted column of its own
const INDEXED_VARIABLE: &str = "REEL_STEADY_INDEXED";
const INDEXED_BUDGET: ByteCount = ByteCount(512 * 1024 * 1024);
const SORTED_ROWS: RowCount = RowCount(100_000);
const SORTED: ColumnId = ColumnId(4);
// the width of every steady key, "key-" and six digits
const SORTED_WIDTH: u16 = 10;
// set in the child that makes the damaged store, so the measured process runs one open
const PREPARE_ROOT: &str = "REEL_STEADY_PREPARE";
const PAGE_BYTES: usize = 4_096;
// the region the gate probe maps, a few pages
#[cfg(target_os = "linux")]
const PROBE_REGION_BYTES: u64 = 1024 * 1024;
const MOST_OPEN_BLOCKS: usize = 32_768;
#[cfg(target_os = "linux")]
const MOST_BLOCK_PAGES: usize = 65_536;
const KEYS: usize = 512;
const SCAN_ROWS: usize = 8;
const SCAN_KEY_BYTES: usize = 128;
const LONG_ROWS: usize = 4_000;
const LONG_VALUE: &[u8] = b"a value on each column of a long batch, a value on each column of a long batch";
const SMALL_BYTES: usize = 100;
const CHUNKED_BYTES: usize = 200_000;
const BURST_RECORDS: usize = 8_000;
const BURST_FREES: usize = BURST_RECORDS / 2;
/// The most thread CPU any unit with a work bound may take on real files, a small multiple of the worst measured on the churn and
/// insert arms. Maintenance threads run at the lowest priority, so their wall time on a busy machine measures the scheduler
const UNIT_TIME_LINE: Nanoseconds = Nanoseconds(100_000_000);
/// The most wall time any unit with a work bound may take, ten times the CPU line, so a stall on a read or a sync still fails
const UNIT_WALL_LINE: Nanoseconds = Nanoseconds(10 * UNIT_TIME_LINE.0);
const LONG_GET_BYTES: usize = 4 * 1024 * 1024;
const ROUNDS: usize = 1_500;
const FLUSH_ROUNDS: usize = 64;
const STACK_STRETCH: usize = 1024 * 1024;
const SEAL_ROUNDS: usize = 300;
const MOST_WARM_ROUNDS: usize = 20 * SEAL_ROUNDS;
const MOST_WINDOW_ROUNDS: usize = 20 * ROUNDS;
const SEAL_WAIT: Duration = Duration::from_millis(1);
const MOST_SEAL_WAITS: u32 = 20_000;
const MOST_SITES: u64 = 8;
const ARENA_SHARE: ByteCount = ByteCount(64 * 1024);
const ARENA_SEGMENTS: usize = 8;
const ARENA_BLOCK_BYTES: usize = 1_024;
const ARENA_OFFSET: u32 = 100;
const ARENA_READERS: usize = 3;
const ARENA_ROUNDS: u64 = 20_000;
const LOW_PATTERN: u8 = 0x5A;
const STAT_BYTES: usize = 1_024;
const STAT_PATH: &str = "/proc/self/stat";
const MINOR_FAULTS_FIELD: usize = 7;
const MAJOR_FAULTS_FIELD: usize = 9;
const STATUS_PATH: &str = "/proc/self/status";
const RESIDENT_FIELD: &str = "VmRSS:";
const KIB: u64 = 1_024;
// the spec lets memory_report miss the process heap by this share, as a divisor
const REPORT_SLACK_DIVISOR: u64 = 10;

type Store = ReelStore<PlainIo>;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static FREES: AtomicU64 = AtomicU64::new(0);
static LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static IS_WINDOW: AtomicBool = AtomicBool::new(false);
static SITES: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static IS_TRACING: Cell<bool> = const { Cell::new(false) };
}

/// Counts every allocation and free of every thread
struct CountingAllocator;

#[global_allocator]
static COUNTER: CountingAllocator = CountingAllocator;

// inside the window, print where the first few allocations came from, so a failure names its site
fn note_site(what: &str) -> bool {
    if IS_TRACING.with(Cell::get) {
        return false;
    }
    if !IS_WINDOW.load(Ordering::Acquire) {
        return true;
    }
    IS_TRACING.with(|is_tracing| is_tracing.set(true));
    if SITES.fetch_add(1, Ordering::AcqRel) < MOST_SITES {
        let trace = Backtrace::force_capture().to_string();
        let frames: Vec<&str> = trace.lines().filter(|line| line.contains("reel")).take(6).collect();
        eprintln!("{what} in the window:\n{}", frames.join("\n"));
    }
    IS_TRACING.with(|is_tracing| is_tracing.set(false));
    true
}

// every heap block of a page or more that open leaves live, so the gate can ask the kernel which pages were written
static OPEN_ADDRESSES: [AtomicUsize; MOST_OPEN_BLOCKS] = [const { AtomicUsize::new(0) }; MOST_OPEN_BLOCKS];
static OPEN_LENGTHS: [AtomicUsize; MOST_OPEN_BLOCKS] = [const { AtomicUsize::new(0) }; MOST_OPEN_BLOCKS];
static IS_OPENING: AtomicBool = AtomicBool::new(false);

fn note_block(address: *mut u8, length: usize) {
    if length < PAGE_BYTES || !IS_OPENING.load(Ordering::Acquire) {
        return;
    }
    for slot in 0..MOST_OPEN_BLOCKS {
        if OPEN_ADDRESSES[slot].compare_exchange(0, address as usize, Ordering::AcqRel, Ordering::Acquire).is_ok() {
            OPEN_LENGTHS[slot].store(length, Ordering::Release);
            return;
        }
    }
}

fn forget_block(address: *mut u8, length: usize) {
    if length < PAGE_BYTES {
        return;
    }
    for slot in &OPEN_ADDRESSES {
        if slot.compare_exchange(address as usize, 0, Ordering::AcqRel, Ordering::Acquire).is_ok() {
            return;
        }
    }
}

// count the pages of open's blocks no one has written, which the kernel has yet to back
#[cfg(target_os = "linux")]
fn unwritten_pages() -> usize {
    let mut unwritten = 0;
    let mut resident = vec![0_u8; MOST_BLOCK_PAGES];
    for slot in 0..MOST_OPEN_BLOCKS {
        let address = OPEN_ADDRESSES[slot].load(Ordering::Acquire);
        if address == 0 {
            continue;
        }
        let end = (address + OPEN_LENGTHS[slot].load(Ordering::Acquire)) / PAGE_BYTES * PAGE_BYTES;
        let start = address.div_ceil(PAGE_BYTES) * PAGE_BYTES;
        let mut page = start;
        while page < end {
            let pages = ((end - page) / PAGE_BYTES).min(MOST_BLOCK_PAGES);
            // SAFETY: the range lies in a live allocation, and the buffer holds a byte a page
            let status = unsafe { libc::mincore(page as *mut libc::c_void, pages * PAGE_BYTES, resident.as_mut_ptr()) };
            assert_eq!(status, 0, "mincore");
            let missing = resident[..pages].iter().filter(|byte| **byte & 1 == 0).count();
            if missing > 0 {
                println!("{missing} of {pages} pages unwritten in a block of {} B", OPEN_LENGTHS[slot].load(Ordering::Acquire));
            }
            unwritten += missing;
            page += pages * PAGE_BYTES;
        }
    }
    unwritten
}

#[cfg(not(target_os = "linux"))]
fn unwritten_pages() -> usize {
    0
}

// SAFETY: every method only counts and then hands its call to the system allocator as it came
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // the trace's own allocations count for nothing
        if note_site("allocation") {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        LIVE_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        // SAFETY: the caller's layout goes to the system allocator unchanged, under this call's own contract
        let block = unsafe { System.alloc(layout) };
        note_block(block, layout.size());
        block
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // the trace's own allocations count for nothing
        if note_site("allocation") {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        LIVE_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        // SAFETY: the caller's layout goes to the system allocator unchanged, under this call's own contract
        let block = unsafe { System.alloc_zeroed(layout) };
        note_block(block, layout.size());
        block
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        if note_site("free") {
            FREES.fetch_add(1, Ordering::Relaxed);
        }
        LIVE_BYTES.fetch_sub(layout.size() as u64, Ordering::Relaxed);
        forget_block(pointer, layout.size());
        // SAFETY: the caller's layout and pointer go to the system allocator unchanged, under this call's own contract
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // the trace's own allocations count for nothing
        if note_site("reallocation") {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        LIVE_BYTES.fetch_add(size as u64, Ordering::Relaxed);
        LIVE_BYTES.fetch_sub(layout.size() as u64, Ordering::Relaxed);
        // SAFETY: the caller's layout and pointer go to the system allocator unchanged, under this call's own contract
        unsafe { System.realloc(pointer, layout, size) }
    }
}

/// Everything a round uses, built before the window opens
struct Inputs {
    keys: Vec<Vec<u8>>,
    missing: Vec<u8>,
    damaged: Vec<u8>,
    damaged_end: Vec<u8>,
    small: Vec<u8>,
    chunked: Vec<u8>,
    compressible: Vec<u8>,
    batch: WriteBatch,
    plain_batch: WriteBatch,
    large_batch: WriteBatch,
    long_batch: WriteBatch,
    buf: Vec<u8>,
    scan_key: Vec<u8>,
    values: ValueList,
    is_indexed: bool,
}

/// What the process counted at one moment
#[derive(Clone, Copy, Debug, Default)]
struct Counts {
    allocations: u64,
    frees: u64,
    major_faults: u64,
}

/// The open stat file, read with no allocation
struct FaultCounter {
    file: Option<File>,
    bytes: [u8; STAT_BYTES],
}

impl FaultCounter {
    fn new() -> Self {
        Self {
            file: File::open(STAT_PATH).ok(),
            bytes: [0; STAT_BYTES],
        }
    }

    // minflt and majflt are fields 10 and 12, which sit 7 and 9 fields past the one after the name
    fn read(&mut self) -> (u64, u64) {
        let Some(file) = &self.file else {
            return (0, 0);
        };
        let length = file.read_at(&mut self.bytes, 0).expect("read stat");
        let text = &self.bytes[..length];
        let name_end = text.iter().rposition(|byte| *byte == b')').expect("stat name");
        let mut field = 0;
        let mut minor = 0;
        let mut major = 0;
        let mut value = 0;
        for byte in &text[name_end + 2..] {
            if *byte == b' ' {
                if field == MINOR_FAULTS_FIELD {
                    minor = value;
                }
                if field == MAJOR_FAULTS_FIELD {
                    major = value;
                }
                field += 1;
                value = 0;
            } else if byte.is_ascii_digit() {
                value = value * 10 + u64::from(*byte - b'0');
            }
        }
        (minor, major)
    }
}

fn counts(faults: &mut FaultCounter) -> Counts {
    let (_minor_faults, major_faults) = faults.read();
    Counts {
        allocations: ALLOCATIONS.load(Ordering::Acquire),
        frees: FREES.load(Ordering::Acquire),
        major_faults,
    }
}

// a store's scratch folder, gone when this drops, so a run that panics leaves nothing on a small tmpfs
struct ScratchRoot(PathBuf);

impl Drop for ScratchRoot {
    fn drop(&mut self) {
        let _removed = fs::remove_dir_all(&self.0);
    }
}

fn open(root: &Path) -> Store {
    let is_indexed = env::var_os(INDEXED_VARIABLE).is_some();
    let mut config = config(root, if is_indexed { INDEXED_BUDGET } else { BUDGET });
    if is_indexed {
        config.columns.push(ColumnConfig {
            id: SORTED,
            name: "sorted".to_string(),
            key_shape: KeyShape::Fixed { width: SORTED_WIDTH },
            codec: Codec::None,
            volume: VolumeId(0),
            complete_rows: None,
            sorted_rows: Some(SORTED_ROWS),
            least_value: None,
        });
        assert!(config.columns.iter().any(|column| column.id == META && column.complete_rows.is_some()), "no complete table at 512 MiB");
    }
    // a gate run sets this to hold the same line with workers beside the planner
    if let Some(workers) = env::var(WORKERS_VARIABLE).ok().and_then(|workers| workers.parse().ok()) {
        config.threads.maintenance_workers = Some(workers);
    }
    ReelStore::open(PlainIo::new(), &config).expect("open")
}

// rows past one group's buffer, so they go out in pieces
fn long_batch() -> WriteBatch {
    let mut batch = WriteBatch::new();
    for number in 0..LONG_ROWS {
        let key = format!("long-{number:06}");
        batch.put(META, key.as_bytes(), LONG_VALUE);
        batch.put(BULK, key.as_bytes(), LONG_VALUE);
    }
    batch
}

// three batches: small rows on the shared encoders, the same with no range delete, and a compressed large value on the
// large one
fn batches(keys: &[Vec<u8>], small: &[u8], compressible: &[u8]) -> (WriteBatch, WriteBatch, WriteBatch) {
    let mut plain_batch = WriteBatch::new();
    plain_batch.put(META, &keys[0], small);
    plain_batch.put(META, &keys[1], small);
    plain_batch.delete(BULK, &keys[1]);
    let mut batch = WriteBatch::new();
    batch.put(META, &keys[0], small);
    batch.put(META, &keys[1], small);
    batch.delete(BULK, &keys[1]);
    batch.delete_range(META, b"range-c", b"range-d");
    let mut large_batch = WriteBatch::new();
    large_batch.put(BULK, &keys[2], compressible);
    large_batch.put(META, &keys[2], small);
    (batch, plain_batch, large_batch)
}

fn inputs() -> Inputs {
    let mut chunked = vec![0; CHUNKED_BYTES];
    for (index, byte) in chunked.iter_mut().enumerate() {
        *byte = (index.wrapping_mul(2_654_435_761) >> 13) as u8;
    }
    let keys: Vec<Vec<u8>> = (0..KEYS).map(|number| format!("key-{number:06}").into_bytes()).collect();
    let small = vec![7; SMALL_BYTES];
    let compressible = vec![b'x'; CHUNKED_BYTES];
    let (batch, plain_batch, large_batch) = batches(&keys, &small, &compressible);
    let long_batch = long_batch();
    Inputs {
        keys,
        missing: b"key-missing".to_vec(),
        damaged: b"damaged-000100".to_vec(),
        damaged_end: b"damaged-000101".to_vec(),
        small,
        chunked,
        compressible,
        batch,
        plain_batch,
        large_batch,
        long_batch,
        buf: vec![1; CHUNKED_BYTES * 2],
        scan_key: vec![1; SCAN_KEY_BYTES],
        values: ValueList::new(),
        is_indexed: env::var_os(INDEXED_VARIABLE).is_some(),
    }
}

fn wait_for_seal(store: &Store, before: u64) {
    for _wait in 0..MOST_SEAL_WAITS {
        if store.state.volumes[0].seals_done.load(Ordering::Acquire) > before {
            return;
        }
        thread::sleep(SEAL_WAIT);
    }
    panic!("a seal never finished");
}

// one pass of every path a running store takes
fn round(store: &Store, inputs: &mut Inputs, number: usize) {
    let key = &inputs.keys[number % KEYS];
    let other = &inputs.keys[(number + KEYS / 2) % KEYS];
    let buf = &mut inputs.buf;

    store.put(META, key, &inputs.small).expect("put");
    store.put(META, other, &inputs.chunked).expect("put chunked");
    store.put(BULK, key, &inputs.compressible).expect("put lz4");
    assert!(store.get(META, key, buf).expect("get"));
    assert!(store.get(META, other, buf).expect("get chunked"));
    assert!(store.get(BULK, key, buf).expect("get lz4"));
    assert!(!store.get(META, &inputs.missing, buf).expect("get a miss"));
    assert!(store.contains(META, key).expect("contains"));
    let keys: [&[u8]; 3] = [key, other, &inputs.missing];
    store.get_many(META, &keys, &mut inputs.values).expect("get_many");
    let cue = store.cue().expect("cue");
    assert!(cue.get(META, key, buf).expect("get at a cue"));
    assert!(cue.contains(META, key).expect("contains at a cue"));
    cue.get_many(META, &keys, &mut inputs.values).expect("get_many at a cue");
    drop(cue);
    let mut scan = store.iter_prefix(BULK, b"key-").expect("scan");
    while scan.next_key(&mut inputs.scan_key).expect("step") == ScanStep::Found {}
    drop(scan);
    let mut scan = store.iter_from(BULK, b"key-z", Direction::Descending).expect("scan");
    for _row in 0..SCAN_ROWS {
        if scan.next_row(&mut inputs.scan_key, buf).expect("step") == ScanStep::End {
            break;
        }
    }
    drop(scan);
    // the plain small shape runs beside the complete one, on the same keys
    store.put(PLAIN, key, &inputs.small).expect("put plain");
    assert!(store.get(PLAIN, key, buf).expect("get plain"));
    assert!(!store.get(PLAIN, &inputs.missing, buf).expect("get a plain miss"));
    store.get_many(PLAIN, &keys, &mut inputs.values).expect("get_many plain");
    let mut scan = store.iter_prefix(PLAIN, b"key-").expect("plain scan");
    for _row in 0..SCAN_ROWS {
        if scan.next_row(&mut inputs.scan_key, buf).expect("step") == ScanStep::End {
            break;
        }
    }
    drop(scan);
    let cue = store.cue().expect("cue");
    assert!(cue.get(PLAIN, key, buf).expect("get plain at a cue"));
    cue.get_many(PLAIN, &keys, &mut inputs.values).expect("get_many plain at a cue");
    drop(cue);
    if inputs.is_indexed {
        let missing = &inputs.missing[..SORTED_WIDTH as usize];
        store.put(SORTED, key, &inputs.small).expect("put sorted");
        assert!(store.get(SORTED, key, buf).expect("get sorted"));
        assert!(!store.get(SORTED, missing, buf).expect("get a sorted miss"));
        let cue = store.cue().expect("cue");
        assert!(cue.get(SORTED, key, buf).expect("get sorted at a cue"));
        drop(cue);
        let mut scan = store.iter_prefix(SORTED, b"key-").expect("sorted scan");
        for _row in 0..SCAN_ROWS {
            if scan.next_row(&mut inputs.scan_key, buf).expect("step") == ScanStep::End {
                break;
            }
        }
        drop(scan);
        store.delete(SORTED, other).expect("delete sorted");
    }
    store.delete(PLAIN, other).expect("delete plain");
    store.delete(BULK, other).expect("delete");
    // a segment with covers sits out compaction, so every other one has none
    let has_covers = number.div_ceil(SEAL_ROUNDS).is_multiple_of(2);
    if has_covers {
        store.delete_range(META, b"range-a", b"range-b").expect("delete range");
    }
    let batch = if has_covers { &inputs.batch } else { &inputs.plain_batch };
    store.write_batch(batch).expect("batch");
    store.write_batch(&inputs.large_batch).expect("large batch");
    if number.is_multiple_of(FLUSH_ROUNDS) {
        store.write_batch(&inputs.long_batch).expect("long batch");
    }

    let refused = store.get(META, &inputs.damaged, buf);
    assert!(matches!(refused, Err(StoreError::Corruption { .. })));
    store.clear_damage(META, &inputs.damaged_end, Some(&inputs.damaged_end)).expect("clear");

    if number.is_multiple_of(FLUSH_ROUNDS) {
        store.flush().expect("flush");
    }
    if number.is_multiple_of(SEAL_ROUNDS) {
        let before = store.state.volumes[0].seals_done.load(Ordering::Acquire);
        store.seal_now();
        wait_for_seal(store, before);
    }
}

/// Read the dead share of the volume's most dead segment
fn most_dead_percent(store: &Store) -> u64 {
    let summary = hold(&store.state.volumes[0].summary);
    100 * summary.rows().map(|row| reclaim_share(&row)).max().unwrap_or(0) / SHARE_SCALE
}

// an unlink and a free in place have both run since these counts
fn has_reclaimed(store: &Store, unlinked_before: u64, freed_before: u64) -> bool {
    let volume = &store.state.volumes[0];
    volume.segments_unlinked.load(Ordering::Acquire) > unlinked_before && volume.bytes_freed.load(Ordering::Acquire) > freed_before
}

// a long get beside a burst of frees answers every time: a free moves no count a read that succeeded looks at
fn free_burst() -> u64 {
    let root = scratch_root();
    let _cleanup = ScratchRoot(root.clone());
    let store = open(&root);
    let record = vec![1; FREE_IN_PLACE_BYTES.0 as usize];
    store.put(META, b"long", &vec![2; LONG_GET_BYTES]).expect("put");
    let write_records = || {
        for number in 0..BURST_RECORDS {
            store.put(META, format!("burst-{number}").as_bytes(), &record).expect("put");
        }
    };
    write_records();
    let before = store.state.volumes[0].seals_done.load(Ordering::Acquire);
    store.seal_now();
    wait_for_seal(&store, before);

    let frees_before = store.state.cues.counts().frees;
    let can_free = store.state.volumes[0].can_free_blocks;
    let is_done = AtomicBool::new(false);
    let (gets, errors) = thread::scope(|scope| {
        let reader = scope.spawn(|| {
            let (mut buf, mut gets, mut errors) = (Vec::new(), 0, 0);
            while !is_done.load(Ordering::Acquire) {
                gets += 1;
                if !store.get(META, b"long", &mut buf).is_ok_and(|found| found && buf.len() == LONG_GET_BYTES) {
                    errors += 1;
                }
            }
            (gets, errors)
        });
        write_records();
        store.seal_now();
        // the frees follow the walk, which follows the seal
        let mut waits = 0;
        while can_free && store.state.cues.counts().frees - frees_before < BURST_FREES as u64 && waits < MOST_SEAL_WAITS {
            waits += 1;
            thread::sleep(SEAL_WAIT);
        }
        is_done.store(true, Ordering::Release);
        reader.join().expect("join")
    });
    let frees = store.state.cues.counts().frees - frees_before;
    let refused = store.state.cues.refused_reads();
    println!("{gets} long gets beside {frees} frees: {errors} errors, {refused} refused");
    store.close().expect("close");
    drop(store);
    errors + refused + u64::from(can_free && frees == 0)
}

fn has_rows_file(root: &Path) -> bool {
    let entries = fs::read_dir(root).expect("list");
    entries.flatten().any(|entry| entry.path().extension().is_some_and(|extension| extension == "rows"))
}

// a sealed segment that opens damaged, so a run has refused reads and a clear to do
fn damage(root: &Path, inputs: &Inputs) {
    let store = open(root);
    for number in 0..200 {
        let key = format!("damaged-{number:06}");
        store.put(META, key.as_bytes(), &inputs.small).expect("put");
    }
    let before = store.state.volumes[0].seals_done.load(Ordering::Acquire);
    store.seal_now();
    wait_for_seal(&store, before);
    // the table has the segment before its rows file goes, and a tail with rows beside it reads as open
    wait_until(|| !has_rows_file(root), "the rows file never went");
    store.close().expect("close");
    drop(store);

    for entry in fs::read_dir(root).expect("list") {
        let path = entry.expect("entry").path();
        if path.extension().is_some_and(|extension| extension == "data") {
            let file = OpenOptions::new().read(true).write(true).open(&path).expect("open");
            let length = file.metadata().expect("metadata").len();
            // the blocks go with the tails, or a rebuild would bring the segment back before the run starts
            file.write_all_at(&vec![0; length as usize], 0).expect("write");
        }
    }
}

// read each thread's name and faults, for a failure to say which thread took them
fn thread_faults() -> Vec<(String, u64, u64)> {
    let mut threads = Vec::new();
    let Ok(entries) = fs::read_dir("/proc/self/task") else {
        return threads;
    };
    for entry in entries.flatten() {
        let Ok(stat) = fs::read_to_string(entry.path().join("stat")) else {
            continue;
        };
        let (Some(name_start), Some(name_end)) = (stat.find('('), stat.rfind(')')) else {
            continue;
        };
        let fields: Vec<&str> = stat[name_end + 2..].split(' ').collect();
        let minor = fields[MINOR_FAULTS_FIELD].parse().unwrap_or(0);
        let major = fields[MAJOR_FAULTS_FIELD].parse().unwrap_or(0);
        let tid = entry.file_name().to_string_lossy().to_string();
        threads.push((format!("{tid} {}", &stat[name_start + 1..name_end]), minor, major));
    }
    threads
}

// read how much of the process is resident, or nothing where the file doesn't exist
fn resident_bytes() -> Option<u64> {
    let status = fs::read_to_string(STATUS_PATH).ok()?;
    let line = status.lines().find(|line| line.starts_with(RESIDENT_FIELD))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib * KIB)
}

// the sorted counts and seals done, read outside the window since a report allocates
fn sorted_counts(store: &Store) -> (MaintenanceReport, u64) {
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    (report, store.state.volumes[0].seals_done.load(Ordering::Acquire))
}

// the window read through the index, scanned it and sealed into it, and no row passed the declared capacity, so the mark
// never set and the fallback took no part
fn check_sorted((before, seals_before): (MaintenanceReport, u64), (after, seals_after): (MaintenanceReport, u64)) -> u64 {
    println!(
        "sorted in the window: {} answers, {} scan keys, {} seals, {} past capacity",
        after.sorted_answers - before.sorted_answers,
        after.sorted_scan_keys - before.sorted_scan_keys,
        seals_after - seals_before,
        after.sorted_past_capacity,
    );
    let is_exercised = after.sorted_answers > before.sorted_answers && after.sorted_scan_keys > before.sorted_scan_keys && seals_after > seals_before;
    u64::from(!is_exercised || after.sorted_past_capacity > 0)
}

// the seal fills a complete table and a sorted index in the window, which only a budget of 512 MiB has room for
fn indexed_arm() -> u64 {
    let program = env::current_exe().expect("path");
    let status = Command::new(program).env(INDEXED_VARIABLE, "1").status().expect("run");
    println!("indexed arm at 512 MiB: {status}");
    u64::from(!status.success())
}

// an earlier open leaves the allocator holding freed pages, so the process that counts runs none
fn prepare(root: &Path) {
    let program = env::current_exe().expect("path");
    let status = Command::new(program).env(PREPARE_ROOT, root).status().expect("run");
    assert!(status.success());
}

// readers on real threads race a drop and a reload of one extent's room, and only ever answer from the segment they asked for
fn check_arena_race() {
    let arena = FilterArena::new(ARENA_SHARE, ARENA_SEGMENTS);
    let fill = |segment: SegmentNumber, pattern: u8| {
        assert!(arena.begin(segment, 1, ARENA_BLOCK_BYTES as u32));
        arena.add(segment, ARENA_OFFSET, &[pattern; ARENA_BLOCK_BYTES]);
    };
    let is_done = AtomicBool::new(false);
    fill(SegmentNumber(1), LOW_PATTERN);
    thread::scope(|scope| {
        for _reader in 0..ARENA_READERS {
            scope.spawn(|| {
                while !is_done.load(Ordering::Acquire) {
                    if let Some(bits) = arena.copy_filter(SegmentNumber(1), ARENA_OFFSET) {
                        assert!(bits.iter().all(|byte| *byte == LOW_PATTERN), "a probe read another segment's bytes");
                    }
                }
            });
        }
        for _round in 0..ARENA_ROUNDS {
            arena.drop_segment(SegmentNumber(1));
            fill(SegmentNumber(2), !LOW_PATTERN);
            arena.drop_segment(SegmentNumber(2));
            fill(SegmentNumber(1), LOW_PATTERN);
        }
        is_done.store(true, Ordering::Release);
    });
}

// a host's reclaim takes resident pages back and the next touch counts as a fault, so a page stays once it's touched. A page
// nothing has touched still faults, which is the fault this test looks for
#[cfg(target_os = "linux")]
fn lock_touched_pages() {
    // SAFETY: mlockall takes flags and no memory of ours
    let locked = unsafe { libc::mlockall(libc::MCL_CURRENT | libc::MCL_FUTURE | libc::MCL_ONFAULT) };
    println!("touched pages {}", if locked == 0 { "locked" } else { "unlocked" });
    assert_eq!(locked, 0, "steady needs its pages locked: raise the memlock limit");
}

#[cfg(not(target_os = "linux"))]
fn lock_touched_pages() {
    println!("touched pages unlocked");
}

fn main() {
    lock_touched_pages();
    if let Some(root) = env::var_os(PREPARE_ROOT) {
        damage(Path::new(&root), &inputs());
        return;
    }
    check_arena_race();

    let root = scratch_root();
    let cleanup = ScratchRoot(root.clone());
    let mut inputs = inputs();
    prepare(&root);

    let live_before = LIVE_BYTES.load(Ordering::Acquire);
    let resident_before = resident_bytes();
    IS_OPENING.store(true, Ordering::Release);
    let store = open(&root);
    IS_OPENING.store(false, Ordering::Release);
    let live_grown = LIVE_BYTES.load(Ordering::Acquire) - live_before;
    let report = store.memory_report();
    let reported = report.heap_total().0;
    let pools = reported + report.thread_stacks.0;
    let resident_grown = resident_bytes().zip(resident_before).map(|(after, before)| after.saturating_sub(before));
    println!("open: {live_grown} B live, {reported} B reported, {} B of stacks, resident grew {resident_grown:?}", report.thread_stacks.0);
    println!("{report:?}");
    // the gate has to fail a fault outside a populate before its pass over the window means anything
    let mut failures = gate_catches_faults();
    if live_grown.abs_diff(reported) > reported / REPORT_SLACK_DIVISOR {
        println!("memory_report misses the heap by more than a tenth");
        failures += 1;
    }
    // the whole-process check above is too coarse for one buffer, so this asks about each block open left live
    let unwritten = unwritten_pages();
    if unwritten > 0 {
        println!("open left {unwritten} pages of its blocks unwritten");
        failures += 1;
    }
    if resident_grown.is_some_and(|grown| grown < pools) {
        println!("open left pages unwritten: resident grew less than memory_report holds");
        failures += 1;
    }
    assert_eq!(store.damaged_segments(), 1);

    // the first run of a path faults in the binary's own pages, so every path runs before the window
    let mut number = 0;
    // a rewrite needs a half-dead segment, and only more writes make one, so the rounds go on until a rewrite has run
    let has_warmed = || {
        let volume = &store.state.volumes[0];
        volume.segments_unlinked.load(Ordering::Acquire) > 0 && volume.segments_rewritten.load(Ordering::Acquire) > 0
    };
    while number <= 2 * SEAL_ROUNDS || !has_warmed() {
        assert!(
            number < MOST_WARM_ROUNDS,
            "no segment reached the bar in {number} rounds, the most dead stands at {}%",
            most_dead_percent(&store),
        );
        round(&store, &mut inputs, number);
        number += 1;
    }

    // the walk runs behind the seals, and its first run is a path like any other
    wait_until(|| (0..store.state.volumes.len()).all(|volume| store.state.pending_walks(volume) == 0), "the walks never finished");

    let unlinked_before = store.state.volumes[0].segments_unlinked.load(Ordering::Acquire);
    let freed_before = store.state.volumes[0].bytes_freed.load(Ordering::Acquire);
    let rewritten_before = store.state.volumes[0].segments_rewritten.load(Ordering::Acquire);
    let sorted_before = sorted_counts(&store);
    touch_stack();
    let mut faults = FaultCounter::new();
    let _first_read = faults.read();
    let snapshot_before = thread_snapshot();
    let before = counts(&mut faults);
    let main_before = main_thread_faults();
    IS_WINDOW.store(true, Ordering::Release);
    // whether a reclaim lands in the window is timing, so the rounds go on until one has, and the counts cover it
    let mut rounds = 0;
    while rounds < ROUNDS || !has_reclaimed(&store, unlinked_before, freed_before) {
        rounds += 1;
        if rounds > MOST_WINDOW_ROUNDS {
            break;
        }
        round(&store, &mut inputs, rounds);
    }
    IS_WINDOW.store(false, Ordering::Release);
    let main_after = main_thread_faults();
    let after = counts(&mut faults);
    let unlinked = store.state.volumes[0].segments_unlinked.load(Ordering::Acquire) - unlinked_before;
    let freed = store.state.volumes[0].bytes_freed.load(Ordering::Acquire) - freed_before;
    let rewritten = store.state.volumes[0].segments_rewritten.load(Ordering::Acquire) - rewritten_before;
    // the counts are read with compaction held and no map under way, so no populate straddles the read, and the read's own
    // faults land on this thread after its count was taken
    store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
    let snapshot_after = thread_snapshot();
    store.state.volumes[0].holds.compaction.store(false, Ordering::Release);
    let unit_failures = check_units(&store);
    let sorted_after = sorted_counts(&store);

    let allocations_before_close = ALLOCATIONS.load(Ordering::Acquire);
    IS_WINDOW.store(true, Ordering::Release);
    store.close().expect("close");
    IS_WINDOW.store(false, Ordering::Release);
    let close_allocations = ALLOCATIONS.load(Ordering::Acquire) - allocations_before_close;
    drop(store);
    drop(cleanup);

    let allocations = after.allocations - before.allocations;
    let frees = after.frees - before.frees;
    let major_faults = after.major_faults - before.major_faults;
    // each thread's own populates come off its count and nothing else does, so a writer, a reader or the seal gets no allowance,
    // and the caller's thread counts its own faults with nothing taken off
    let main_faults = main_after - main_before;
    let gate = fault_gate(&snapshot_before, &snapshot_after, main_thread_id());
    for line in &gate.lines {
        println!("{line}");
    }
    let minor_faults = gate.excess + main_faults;
    println!("{rounds} rounds: {allocations} allocations, {frees} frees, {minor_faults} minor faults past each thread's own populates ({} populate faults, {main_faults} faults on the caller's thread), {major_faults} major faults", gate.populated);
    println!("close: {close_allocations} allocations");
    println!("{unlinked} unlinks in the window");
    println!("{freed} bytes freed in place in the window");
    println!("{rewritten} rewrites in the window");
    failures += free_burst() + unit_failures;
    if inputs.is_indexed {
        failures += check_sorted(sorted_before, sorted_after);
    }
    if env::var_os(INDEXED_VARIABLE).is_none() {
        failures += indexed_arm();
    }
    if unlinked == 0 || freed == 0 || rewritten == 0 {
        failures += 1;
    }
    if failures + allocations + frees + minor_faults + major_faults + close_allocations > 0 {
        process::exit(1);
    }
}

// write a stretch of this thread's stack, since the caller owns it and a read that goes a frame deeper in the window would fault
#[inline(never)]
fn touch_stack() {
    let mut stretch = [0_u8; STACK_STRETCH];
    for byte in stretch.iter_mut().step_by(1024) {
        *byte = 1;
    }
    std::hint::black_box(&mut stretch);
}

// every unit with a work bound stays under it and under the time line, and the longest of each kind shows
fn check_units(store: &Store) -> u64 {
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    let mut failures = 0;
    for (kind, reading) in UNIT_KINDS.iter().zip(&report.longest_units) {
        let bound = unit_bound(*kind);
        println!("unit {kind:?}: {} work, {} us, {} us of thread CPU", reading.work, reading.time.0 / 1_000, reading.cpu.0 / 1_000);
        let is_past = bound.is_some_and(|bound| reading.work > bound) || (bound.is_some() && (reading.cpu > UNIT_TIME_LINE || reading.time > UNIT_WALL_LINE));
        if is_past {
            println!("unit {kind:?} is past its bound {bound:?}, the {} ms CPU line or the {} ms wall line", UNIT_TIME_LINE.0 / 1_000_000, UNIT_WALL_LINE.0 / 1_000_000);
            failures += 1;
        }
        // a kind that did work and read no CPU has lost its clock, and a CPU line over it would pass anything
        if bound.is_some() && reading.work > 0 && reading.cpu.0 == 0 {
            println!("unit {kind:?} did work and read no thread CPU");
            failures += 1;
        }
    }
    println!("planner: {} steps in a row before the walk's turn, last {:?}", report.longest_walkless_steps, report.longest_walkless_kind);
    if report.longest_walkless_steps > MOST_WALKLESS_STEPS {
        println!("the planner held the walk back past {MOST_WALKLESS_STEPS} steps");
        failures += 1;
    }
    failures
}

// read a thread's kernel id from the name thread_faults gives it
fn thread_id(name: &str) -> u64 {
    name.split(' ').next().and_then(|id| id.parse().ok()).unwrap_or(0)
}

// add up every thread's populate faults
fn populate_total() -> u64 {
    reel::region::POPULATE_FAULTS.iter().map(|(_id, total)| total.load(Ordering::Acquire)).sum()
}

/// One thread's faults and the faults its own region populates took, by kernel id
struct ThreadCount {
    id: u64,
    name: String,
    minor: u64,
    major: u64,
    populated: u64,
}

/// The faults threads took past their own populates, the populates themselves, and a line for each thread that faulted
struct FaultGate {
    excess: u64,
    populated: u64,
    lines: Vec<String>,
}

// read every thread's faults and its own populates when no map is under way and none landed during the read, so no populate
// straddles it
fn thread_snapshot() -> Vec<ThreadCount> {
    loop {
        while reel::region::MAPS_IN_FLIGHT.load(Ordering::Acquire) > 0 {
            thread::yield_now();
        }
        let total = populate_total();
        let counts: Vec<ThreadCount> = thread_faults()
            .into_iter()
            .map(|(name, minor, major)| {
                let id = thread_id(&name);
                ThreadCount { id, populated: reel::region::populate_faults_of(id), name, minor, major }
            })
            .collect();
        if reel::region::MAPS_IN_FLIGHT.load(Ordering::Acquire) == 0 && populate_total() == total {
            return counts;
        }
    }
}

// count each thread's faults past its own populates between two snapshots, leaving out the caller's thread, which counts its
// own faults with nothing taken off
fn fault_gate(before: &[ThreadCount], after: &[ThreadCount], caller: u64) -> FaultGate {
    let mut gate = FaultGate { excess: 0, populated: 0, lines: Vec::new() };
    for thread in after.iter().filter(|thread| thread.id != caller) {
        let earlier = before.iter().find(|earlier| earlier.id == thread.id);
        let (minor, major, populated) = earlier.map_or((0, 0, 0), |earlier| (earlier.minor, earlier.major, earlier.populated));
        let faults = thread.minor.saturating_sub(minor);
        let own = thread.populated.saturating_sub(populated);
        let majors = thread.major.saturating_sub(major);
        let past = faults.saturating_sub(own) + majors;
        gate.populated += own;
        gate.excess += past;
        if past > 0 {
            gate.lines.push(format!("thread {}: {faults} minor with {own} of its own populates, {majors} major", thread.name));
        }
    }
    gate
}

// read the calling thread's own minor faults, which takes no allocation and no file read
#[cfg(target_os = "linux")]
fn main_thread_faults() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: the call writes one rusage into the buffer, which is that size
    assert_eq!(unsafe { libc::getrusage(libc::RUSAGE_THREAD, usage.as_mut_ptr()) }, 0, "getrusage");
    // SAFETY: the call succeeded, so it wrote the whole struct
    u64::try_from(unsafe { usage.assume_init() }.ru_minflt).unwrap_or(0)
}

#[cfg(not(target_os = "linux"))]
fn main_thread_faults() -> u64 {
    0
}

// read the calling thread's kernel id
#[cfg(target_os = "linux")]
fn main_thread_id() -> u64 {
    // SAFETY: gettid takes nothing and can't fail
    u64::try_from(unsafe { libc::gettid() }).unwrap_or(0)
}

#[cfg(not(target_os = "linux"))]
fn main_thread_id() -> u64 {
    0
}

// write one page no one has touched, which faults once
#[cfg(target_os = "linux")]
fn fault_fresh_page() {
    // SAFETY: an anonymous private mapping with no address asked for touches no memory of ours
    let page = unsafe { libc::mmap(std::ptr::null_mut(), PAGE_BYTES, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_PRIVATE | libc::MAP_ANONYMOUS, -1, 0) };
    assert_ne!(page, libc::MAP_FAILED, "mmap");
    // SAFETY: the page is ours and this long
    unsafe { std::ptr::write_volatile(page.cast::<u8>(), 1) };
    // SAFETY: the mapping is the one just made, and nothing points into it
    unsafe { libc::munmap(page, PAGE_BYTES) };
}

// the gate passes a region's populate on its own thread, and fails a fault outside it there or on the caller's thread
#[cfg(target_os = "linux")]
fn gate_catches_faults() -> u64 {
    let mut failures = 0;
    let steps = std::sync::Arc::new(std::sync::Barrier::new(2));
    let worker_steps = std::sync::Arc::clone(&steps);
    let worker = thread::Builder::new()
        .name("gate-probe".to_string())
        .spawn(move || {
            touch_stack();
            worker_steps.wait();
            worker_steps.wait();
            let region = reel::region::Region::map(ByteCount(PROBE_REGION_BYTES)).expect("map");
            worker_steps.wait();
            worker_steps.wait();
            fault_fresh_page();
            worker_steps.wait();
            worker_steps.wait();
            drop(region);
        })
        .expect("spawn");
    let caller = main_thread_id();
    steps.wait();
    let start = thread_snapshot();
    steps.wait();
    steps.wait();
    let mapped = thread_snapshot();
    steps.wait();
    let populate_only = fault_gate(&start, &mapped, caller);
    if populate_only.excess > 0 || populate_only.populated == 0 {
        println!("gate probe: a populate alone left {} faults past it, with {} populated", populate_only.excess, populate_only.populated);
        failures += 1;
    }
    steps.wait();
    let faulted = thread_snapshot();
    steps.wait();
    if fault_gate(&mapped, &faulted, caller).excess == 0 {
        println!("gate probe: a fault on a worker outside a populate passed the gate");
        failures += 1;
    }
    worker.join().expect("join");
    let caller_before = main_thread_faults();
    fault_fresh_page();
    if main_thread_faults() == caller_before {
        println!("gate probe: a fault on the caller's thread passed the gate");
        failures += 1;
    }
    failures
}

#[cfg(not(target_os = "linux"))]
fn gate_catches_faults() -> u64 {
    0
}
