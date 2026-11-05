//! Zero allocations, zero frees and zero page faults once open has returned, on real files and real engine
//! threads. One run at a time, since the fault counters belong to the whole process
#![allow(clippy::expect_used, clippy::panic, clippy::print_stderr, clippy::print_stdout)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::backtrace::Backtrace;
use std::cell::Cell;
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::io::plain::PlainIo;
use reel::record::codec::Codec;
use reel::segment::segment_tail::SECOND_TAIL_DISTANCE;
use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, VolumeId};

const META: ColumnId = ColumnId(1);
const BULK: ColumnId = ColumnId(2);
const BUDGET: ByteCount = ByteCount(192 * 1024 * 1024);
const KEYS: usize = 512;
const SMALL_BYTES: usize = 100;
const CHUNKED_BYTES: usize = 200_000;
const ROUNDS: usize = 1_500;
const FLUSH_ROUNDS: usize = 64;
const SEAL_ROUNDS: usize = 300;
const SEAL_WAIT: Duration = Duration::from_millis(1);
const MOST_SEAL_WAITS: u32 = 20_000;
const MOST_SITES: u64 = 16;
const STAT_BYTES: usize = 1_024;
const STAT_PATH: &str = "/proc/self/stat";
const MINOR_FAULTS_FIELD: usize = 7;
const MAJOR_FAULTS_FIELD: usize = 9;

type Store = ReelStore<PlainIo>;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static FREES: AtomicU64 = AtomicU64::new(0);
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

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // the trace's own allocations count for nothing
        if note_site("allocation") {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // the trace's own allocations count for nothing
        if note_site("allocation") {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        if note_site("free") {
            FREES.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // the trace's own allocations count for nothing
        if note_site("reallocation") {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
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
    buf: Vec<u8>,
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

fn config(root: &Path) -> StoreConfig {
    let column = |id: ColumnId, name: &str, codec: Codec| ColumnConfig {
        id,
        name: name.to_string(),
        key_shape: KeyShape::Variable { most: 64 },
        codec,
        volume: VolumeId(0),
    };
    StoreConfig {
        memory_budget: BUDGET,
        volumes: vec![VolumeConfig {
            name: "steady".to_string(),
            roots: vec![root.to_path_buf()],
            sync: SyncPolicy::Never,
        }],
        columns: vec![column(META, "meta", Codec::None), column(BULK, "bulk", Codec::Lz4)],
    }
}

fn open(root: &Path) -> Store {
    ReelStore::open(PlainIo::new(), &config(root)).expect("open")
}

fn inputs() -> Inputs {
    let mut chunked = vec![0; CHUNKED_BYTES];
    for (index, byte) in chunked.iter_mut().enumerate() {
        *byte = (index.wrapping_mul(2_654_435_761) >> 13) as u8;
    }
    Inputs {
        keys: (0..KEYS).map(|number| format!("key-{number:06}").into_bytes()).collect(),
        missing: b"key-missing".to_vec(),
        damaged: b"damaged-000100".to_vec(),
        damaged_end: b"damaged-000101".to_vec(),
        small: vec![7; SMALL_BYTES],
        chunked,
        compressible: vec![b'x'; CHUNKED_BYTES],
        buf: vec![1; CHUNKED_BYTES * 2],
    }
}

fn wait_for_seal(store: &Store, before: u64) {
    for _wait in 0..MOST_SEAL_WAITS {
        if store.state.volumes[0].table.segment_count() > before {
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
    store.delete(BULK, other).expect("delete");

    let refused = store.get(META, &inputs.damaged, buf);
    assert!(matches!(refused, Err(StoreError::Corruption { .. })));
    store.clear_damage(META, &inputs.damaged_end, Some(&inputs.damaged_end)).expect("clear");

    if number.is_multiple_of(FLUSH_ROUNDS) {
        store.flush().expect("flush");
    }
    if number.is_multiple_of(SEAL_ROUNDS) {
        let before = store.state.volumes[0].table.segment_count();
        store.seal_now();
        wait_for_seal(store, before);
    }
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
    let before = store.state.volumes[0].table.segment_count();
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

fn scratch_root() -> PathBuf {
    let root = std::env::temp_dir().join(format!("reel-steady-{}", process::id()));
    fs::create_dir_all(&root).expect("create");
    root
}

fn main() {
    let root = scratch_root();
    let mut inputs = inputs();
    damage(&root, &inputs);

    let store = open(&root);
    assert_eq!(store.damaged_segments(), 1);

    // the first run of a path faults in the binary's own pages, so every path runs before the window
    for number in 0..SEAL_ROUNDS + 1 {
        round(&store, &mut inputs, number);
    }

    let mut faults = FaultCounter::new();
    let _first_read = faults.read();
    let before = counts(&mut faults);
    IS_WINDOW.store(true, Ordering::Release);
    for number in 1..=ROUNDS {
        round(&store, &mut inputs, number);
    }
    IS_WINDOW.store(false, Ordering::Release);
    let after = counts(&mut faults);

    store.close().expect("close");
    drop(store);
    fs::remove_dir_all(&root).expect("remove");

    let allocations = after.allocations - before.allocations;
    let frees = after.frees - before.frees;
    let minor_faults = after.minor_faults - before.minor_faults;
    let major_faults = after.major_faults - before.major_faults;
    println!("{ROUNDS} rounds: {allocations} allocations, {frees} frees, {minor_faults} minor faults, {major_faults} major faults");
    if allocations + frees + minor_faults + major_faults > 0 {
        process::exit(1);
    }
}
