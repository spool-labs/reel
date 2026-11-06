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

use reel::engine::reel_store::ReelStore;
use reel::engine::walk_job::FREE_IN_PLACE_BYTES;
use reel::io::plain::PlainIo;
use reel::segment::segment_tail::SECOND_TAIL_DISTANCE;
use reel_api::direction::Direction;
use reel_api::scan::ScanStep;
use reel_api::store_error::StoreError;
use reel_api::units::ByteCount;
use reel_api::value_list::ValueList;
use reel_api::write_batch::WriteBatch;

mod store_config;

use store_config::{BULK, META, PLAIN, config, scratch_root};

const BUDGET: ByteCount = ByteCount(192 * 1024 * 1024);
// set in the child that makes the damaged store, so the measured process runs one open
const PREPARE_ROOT: &str = "REEL_STEADY_PREPARE";
const PAGE_BYTES: usize = 4_096;
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
const LONG_GET_BYTES: usize = 4 * 1024 * 1024;
const ROUNDS: usize = 1_500;
const FLUSH_ROUNDS: usize = 64;
const SEAL_ROUNDS: usize = 300;
const MOST_WARM_ROUNDS: usize = 20 * SEAL_ROUNDS;
const MOST_WINDOW_ROUNDS: usize = 20 * ROUNDS;
const SEAL_WAIT: Duration = Duration::from_millis(1);
const MOST_SEAL_WAITS: u32 = 20_000;
const MOST_SITES: u64 = 8;
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

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // the trace's own allocations count for nothing
        if note_site("allocation") {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        LIVE_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
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
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // the trace's own allocations count for nothing
        if note_site("reallocation") {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        LIVE_BYTES.fetch_add(size as u64, Ordering::Relaxed);
        LIVE_BYTES.fetch_sub(layout.size() as u64, Ordering::Relaxed);
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
}

/// What the process counted at one moment
#[derive(Clone, Copy, Debug, Default)]
struct Counts {
    allocations: u64,
    frees: u64,
    minor_faults: u64,
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
    let (minor_faults, major_faults) = faults.read();
    Counts {
        allocations: ALLOCATIONS.load(Ordering::Acquire),
        frees: FREES.load(Ordering::Acquire),
        minor_faults,
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
    ReelStore::open(PlainIo::new(), &config(root, BUDGET)).expect("open")
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
    let cue = store.cue().expect("cue");
    assert!(cue.get(PLAIN, key, buf).expect("get plain at a cue"));
    cue.get_many(PLAIN, &keys, &mut inputs.values).expect("get_many plain at a cue");
    drop(cue);
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
    while has_rows_file(root) {
        thread::sleep(SEAL_WAIT);
    }
    store.close().expect("close");
    drop(store);

    for entry in fs::read_dir(root).expect("list") {
        let path = entry.expect("entry").path();
        if path.extension().is_some_and(|extension| extension == "data") {
            let file = OpenOptions::new().read(true).write(true).open(&path).expect("open");
            let length = file.metadata().expect("metadata").len();
            for end in [length, length - SECOND_TAIL_DISTANCE] {
                let mut byte = [0];
                file.read_exact_at(&mut byte, end - 3).expect("read");
                byte[0] ^= 1;
                file.write_all_at(&byte, end - 3).expect("write");
            }
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

// an earlier open leaves the allocator holding freed pages, so the process that counts runs none
fn prepare(root: &Path) {
    let program = env::current_exe().expect("path");
    let status = Command::new(program).env(PREPARE_ROOT, root).status().expect("run");
    assert!(status.success());
}

fn main() {
    if let Some(root) = env::var_os(PREPARE_ROOT) {
        damage(Path::new(&root), &inputs());
        return;
    }

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
    let mut failures = 0;
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
    while number <= 2 * SEAL_ROUNDS || store.state.volumes[0].segments_unlinked.load(Ordering::Acquire) == 0 {
        assert!(number < MOST_WARM_ROUNDS, "no unlink ran before the window");
        round(&store, &mut inputs, number);
        number += 1;
    }

    // the walk runs behind the seals, and its first run is a path like any other
    while (0..store.state.volumes.len()).any(|volume| store.state.pending_walks(volume) > 0) {
        thread::sleep(SEAL_WAIT);
    }

    let unlinked_before = store.state.volumes[0].segments_unlinked.load(Ordering::Acquire);
    let freed_before = store.state.volumes[0].bytes_freed.load(Ordering::Acquire);
    // a rewrite is a path too, and it follows the last walk
    let mut waits = 0;
    while store.state.volumes[0].segments_rewritten.load(Ordering::Acquire) == 0 {
        waits += 1;
        assert!(waits < MOST_SEAL_WAITS, "no rewrite ran before the window");
        thread::sleep(SEAL_WAIT);
    }
    let rewritten_before = store.state.volumes[0].segments_rewritten.load(Ordering::Acquire);
    let mut faults = FaultCounter::new();
    let _first_read = faults.read();
    let threads_before = thread_faults();
    let before = counts(&mut faults);
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
    let after = counts(&mut faults);
    let unlinked = store.state.volumes[0].segments_unlinked.load(Ordering::Acquire) - unlinked_before;
    let freed = store.state.volumes[0].bytes_freed.load(Ordering::Acquire) - freed_before;
    let rewritten = store.state.volumes[0].segments_rewritten.load(Ordering::Acquire) - rewritten_before;
    let threads_after = thread_faults();

    let allocations_before_close = ALLOCATIONS.load(Ordering::Acquire);
    IS_WINDOW.store(true, Ordering::Release);
    store.close().expect("close");
    IS_WINDOW.store(false, Ordering::Release);
    let close_allocations = ALLOCATIONS.load(Ordering::Acquire) - allocations_before_close;
    drop(store);
    drop(cleanup);

    let allocations = after.allocations - before.allocations;
    let frees = after.frees - before.frees;
    let minor_faults = after.minor_faults - before.minor_faults;
    let major_faults = after.major_faults - before.major_faults;
    if minor_faults + major_faults > 0 {
        for (name, minor, major) in threads_after {
            let before = threads_before.iter().find(|thread| thread.0 == name);
            let (minor_before, major_before) = before.map_or((0, 0), |thread| (thread.1, thread.2));
            if minor != minor_before || major != major_before {
                println!("thread {name}: {} minor, {} major", minor - minor_before, major - major_before);
            }
        }
    }
    println!("{rounds} rounds: {allocations} allocations, {frees} frees, {minor_faults} minor faults, {major_faults} major faults");
    println!("close: {close_allocations} allocations");
    println!("{unlinked} unlinks in the window");
    println!("{freed} bytes freed in place in the window");
    println!("{rewritten} rewrites in the window");
    failures += free_burst();
    if unlinked == 0 || freed == 0 || rewritten == 0 {
        failures += 1;
    }
    if failures + allocations + frees + minor_faults + major_faults + close_allocations > 0 {
        process::exit(1);
    }
}
