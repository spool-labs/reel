//! Raw throughput across the wait shapes: blocking, awaitable, facade
//!
//! Same records, same tails, same backend, so a row differs from the one above it in
//! nothing but the shape of the wait. Block takes the sync inline and parks to wait;
//! future leaves a waker and runs the turn it is handed back; forward gives that turn to
//! the tail's sealer instead, which is what a runtime worker has to do; facade runs the
//! block op on a paired worker behind a rendezvous channel, which is what spawn_blocking
//! costs without a runtime. The read matrix runs the same modes over the same records
//! and adds the axis the async door exists for, reads in flight per caller thread. It
//! pins mapped reads off on every row, since the async door never maps and a future row
//! raced against a mapped blocking row would compare a driver read to a memcpy.
//!
//! Ignored by default. Run with:
//!   cargo test -p tape-reel --test raw_async --release -- --ignored --nocapture --test-threads=1
//!
//! Knobs, all optional, each refusing a value it cannot read rather than falling back:
//! REEL_ASYNC_MODES, REEL_ASYNC_BACKEND, REEL_ASYNC_SYNC, REEL_ASYNC_SIZES,
//! REEL_ASYNC_THREADS, REEL_ASYNC_TAILS, REEL_ASYNC_DEPTH, REEL_ASYNC_BYTES,
//! REEL_ASYNC_MAX_BYTES, REEL_ASYNC_MAX_OPS, REEL_ASYNC_DIR, REEL_ASYNC_DROP_CACHES.
//! DROP_CACHES needs root and is what makes a read row a device number rather than a
//! memcpy.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::Thread;
use std::time::{Duration, Instant};

use tempfile::TempDir;

use reel::append::admission::InflightBudget;
use reel::append::Commit;
use reel::io::select::select_backend;
use reel::io::slots::SLOT_COUNT;
use reel::reel::segment::{FdCache, IoDriver};
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, Durability, IoBackend, KeyWidth, MapShape,
    RecordKey, Reel, ReelConfig, ReelShared, ReelStore, SyncPolicy, ThreadBudget,
};

/// Bytes every bench key occupies
const KEY_WIDTH: usize = 16;

/// The column the read matrix fills and reads back
///
/// The shard count matters: a column with one shard puts every reader behind one lock
/// and measures that lock.
const READ_COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: "raw",
    key_width: KeyWidth::Fixed(KEY_WIDTH as u16),
    shard_bytes: 2,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

/// The record column of the fixture set, keyed by a group and an id
const RECORDS: ColumnId = ColumnId(1);

/// The blob column of the fixture set, keyed by an id alone
const BLOB: ColumnId = ColumnId(2);

/// Bytes the group takes at the front of a record key
const GROUP_PREFIX_LEN: usize = 2;

/// Bytes a content address occupies
const ID_LEN: usize = 32;

/// Bytes a record key occupies
const RECORD_KEY_LEN: usize = GROUP_PREFIX_LEN + ID_LEN;

/// A caller-declared column set of the shape a deployment hands the engine
///
/// The write matrix drives a bare reel rather than a store, so it needs the shape the
/// tails were sized against: records leading with a group, blobs with a content address.
const TEST_COLUMNS: ColumnSet = &[
    ColumnSpec {
        id: RECORDS,
        name: "records",
        key_width: KeyWidth::Fixed(RECORD_KEY_LEN as u16),
        shard_bytes: 2,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
    ColumnSpec {
        id: BLOB,
        name: "blob_data",
        key_width: KeyWidth::Fixed(ID_LEN as u16),
        shard_bytes: 0,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
];

/// Odd stride that walks a record set in an order unrelated to its layout
const READ_STRIDE: u64 = 0x9E37_79B9_7F4A_7C15 | 1;

/// Reads in flight per caller thread the future rows sweep
///
/// A blocking row's depth is its reader count, so the block and facade rows run at one.
const DEFAULT_DEPTHS: &str = "1,8,32,128";

/// Bytes each cell writes, small enough to iterate on and env-raised for a box
const DEFAULT_CELL_BYTES: u64 = 128 * 1024 * 1024;

/// Bytes a cell may grow to chasing a phase long enough to trust
///
/// A cell short of the minimum is rerun with more records up to this ceiling, and a cell
/// still short here keeps its flag.
const DEFAULT_MAX_CELL_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Record sizes the matrix sweeps
const DEFAULT_SIZES: &str = "4096,65536";

/// Writer counts the matrix sweeps
const DEFAULT_THREADS: &str = "1,4,8";

/// Sync policies the matrix sweeps: no waits at all, then real ones
const DEFAULT_SYNC: &str = "never,1048576";

/// Records one cell will write however small they are
const DEFAULT_MAX_OPS: u64 = 500_000;

/// Shortest phase worth quoting a throughput from
const MIN_PHASE_SECS: f64 = 1.0;

/// Rounds the no-io wait-point probes charge each call over
const PROBE_ROUNDS: u64 = 2_000_000;

/// How long the executor tests give a drive before a silent park counts as a hang
const DRIVE_DEADLINE: Duration = Duration::from_secs(10);

/// The cadence the executor test's waking thread scans for seated wakers on
const WAKE_DELAY: Duration = Duration::from_millis(1);

/// Flights the delayed-wake test drives, more than its seats so seats are reused
const DELAYED_FLIGHTS: usize = 32;

/// Seats the delayed-wake test drives them through
const DELAYED_DEPTH: usize = 4;

/// Flights the inline test drives, all ready at first poll
const INLINE_FLIGHTS: usize = 1000;

/// Seats the inline test drives them through
const INLINE_DEPTH: usize = 8;

/// How a cell's writers take the engine's waits
#[derive(Clone, Copy, Eq, PartialEq)]
enum Mode {
    /// The shipped blocking path: sync inline, waits park the thread
    Block,

    /// The awaitable path: waits leave a waker, turns come back to be run
    Future,

    /// The same waits with owed turns handed to the sealer instead of run
    Forward,

    /// The blocking path behind a rendezvous handoff, one worker per writer
    Facade,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Mode::Block => "block",
            Mode::Future => "future",
            Mode::Forward => "forward",
            Mode::Facade => "facade",
        }
    }
}

fn env_string(name: &str, fallback: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| fallback.to_string())
}

/// Numbers from a comma-separated knob, refusing anything that is not one
///
/// A dropped entry is a sweep that quietly ran fewer cells than it was asked for, so an
/// entry that will not parse takes the run down.
fn parse_list(name: &str, text: &str) -> Vec<u64> {
    let mut list = Vec::new();
    for item in text.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        match item.parse() {
            Ok(value) => list.push(value),
            Err(_) => panic!("{name} entry `{item}` is not a number"),
        }
    }
    list
}

fn env_list(name: &str, fallback: &str) -> Vec<u64> {
    parse_list(name, &env_string(name, fallback))
}

/// A byte count from a knob, refusing anything that is not a plain number of bytes
///
/// There are no suffixes: a byte count is bytes, and a value that will not read takes
/// the run down rather than silently becoming the default.
fn parse_bytes(name: &str, text: &str) -> u64 {
    match text.trim().parse() {
        Ok(value) => value,
        Err(_) => panic!("{name} `{text}` is not a plain number of bytes"),
    }
}

fn env_bytes(name: &str, fallback: u64) -> u64 {
    match std::env::var(name) {
        Ok(text) => parse_bytes(name, &text),
        Err(_) => fallback,
    }
}

/// Say so when the op cap, not the byte target, is what sizes a cell
///
/// The two caps meet at a `min`, so the smaller wins silently and a cell asked for in
/// past-memory bytes can come back small enough to be a page-cache number. The run still
/// goes ahead, since the default pair truncates by five percent.
fn warn_where_ops_bind(sizes: &[u64], max_cell_bytes: u64, max_ops: u64) {
    for &size in sizes {
        let wanted = max_cell_bytes / size.max(1);
        if wanted <= max_ops {
            continue;
        }
        println!(
            "REEL_ASYNC_MAX_OPS {max_ops} binds at size {size}: the {} MiB ceiling wants \
             {wanted} ops and the cell tops out at {} MiB",
            max_cell_bytes / (1024 * 1024),
            max_ops * size / (1024 * 1024),
        );
    }
}

/// The named entries of a knob, trailing commas allowed and emptiness refused
///
/// An empty axis prints no rows and no complaint, so it is refused here instead.
fn named_entries(name: &str, text: &str) -> Vec<String> {
    let mut entries = Vec::new();
    for entry in text.split(',') {
        let entry = entry.trim();
        if !entry.is_empty() {
            entries.push(entry.to_string());
        }
    }
    assert!(!entries.is_empty(), "{name} names nothing to run");
    entries
}

fn modes() -> Vec<Mode> {
    let named = env_string("REEL_ASYNC_MODES", "block,future,forward,facade");
    let mut modes = Vec::new();
    for name in named_entries("REEL_ASYNC_MODES", &named) {
        match name.as_str() {
            "block" => modes.push(Mode::Block),
            "future" => modes.push(Mode::Future),
            "forward" => modes.push(Mode::Forward),
            "facade" => modes.push(Mode::Facade),
            other => panic!("REEL_ASYNC_MODES `{other}` is not a known mode"),
        }
    }
    modes
}

fn backends() -> Vec<(String, IoBackend)> {
    let named = env_string("REEL_ASYNC_BACKEND", "posix");
    let mut backends = Vec::new();
    for name in named_entries("REEL_ASYNC_BACKEND", &named) {
        let backend = match name.as_str() {
            "posix" => IoBackend::Posix,
            "uring" => IoBackend::Uring,
            "uring_direct" => IoBackend::UringDirect,
            other => panic!("REEL_ASYNC_BACKEND `{other}` is not a known backend"),
        };
        backends.push((name, backend));
    }
    backends
}

fn syncs() -> Vec<(String, SyncPolicy)> {
    let named = env_string("REEL_ASYNC_SYNC", DEFAULT_SYNC);
    let mut syncs = Vec::new();
    for name in named_entries("REEL_ASYNC_SYNC", &named) {
        let policy = match name.as_str() {
            "never" => SyncPolicy::Never,
            "0" => SyncPolicy::EveryPut,
            bytes => {
                SyncPolicy::Bytes(ByteCount::from_bytes(parse_bytes("REEL_ASYNC_SYNC", bytes)))
            }
        };
        syncs.push((name, policy));
    }
    syncs
}

// a byte knob that is not a plain byte count takes the run down rather than the default
#[test]
#[should_panic(expected = "REEL_ASYNC_BYTES `48G`")]
fn a_suffixed_byte_target() {
    parse_bytes("REEL_ASYNC_BYTES", "48G");
}

// a list knob drops no entry quietly
#[test]
#[should_panic(expected = "REEL_ASYNC_SIZES entry `4k`")]
fn a_bad_list_entry() {
    parse_list("REEL_ASYNC_SIZES", "4096,4k");
}

// a knob naming nothing the harness knows leaves no axis to sweep
#[test]
#[should_panic(expected = "REEL_ASYNC_MODES names nothing")]
fn an_empty_axis() {
    named_entries("REEL_ASYNC_MODES", " , ");
}

// plain values still read, spaces and a trailing comma included
#[test]
fn good_values_read() {
    assert_eq!(parse_bytes("REEL_ASYNC_BYTES", " 4096 "), 4096);
    assert_eq!(
        parse_list("REEL_ASYNC_SIZES", "4096, 65536,"),
        vec![4096, 65536]
    );
    assert_eq!(
        named_entries("REEL_ASYNC_MODES", "block, future,"),
        vec!["block", "future"]
    );
}

/// A payload of incompressible bytes, so nothing downstream can cheat on it
fn payload(size: usize) -> Vec<u8> {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut out = Vec::with_capacity(size + 8);
    while out.len() < size {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(size);
    out
}

/// The key for one record, scrambled half leading so shards spread
fn key_at(index: u64) -> RecordKey {
    let mut bytes = [0u8; KEY_WIDTH];
    bytes[0..8].copy_from_slice(&index.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes());
    bytes[8..16].copy_from_slice(&index.to_le_bytes());
    RecordKey::from_bytes(ColumnId(1), &bytes).expect("key fits")
}

/// Drive one future to its answer on the calling thread
///
/// The waker unparks the thread that is polling, so a wait costs a park and an unpark
/// and no completion crosses to a thread that was not already involved.
fn block_on<Answered: Future>(future: Answered) -> Answered::Output {
    use std::task::Wake;
    use std::thread::{self, Thread};

    struct Unparker(Thread);

    impl Wake for Unparker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let mut future = Box::pin(future);
    let waker = Waker::from(Arc::new(Unparker(thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(answer) = future.as_mut().poll(&mut cx) {
            return answer;
        }
        thread::park();
    }
}

/// One writer's put loop in one wait shape, against one tail of the reel
fn run_writer(reel: &Reel, mode: Mode, thread: u64, per_thread: u64, body: &[u8]) {
    let tails = reel.tails();
    let tail = &tails[thread as usize % tails.len()];
    match mode {
        Mode::Block => {
            for step in 0..per_thread {
                let at = thread * per_thread + step;
                tail.append_data(key_at(at), body.to_vec(), 0, Commit::PerRecord)
                    .expect("put");
            }
        }
        Mode::Future => {
            for step in 0..per_thread {
                let at = thread * per_thread + step;
                tail.append_data(key_at(at), body.to_vec(), 0, Commit::Batched)
                    .expect("put");
                match block_on(tail.owed_turn()).expect("owed sync") {
                    Durability::Settled => {}
                    Durability::Owed(turn) => tail.take_turn(turn).expect("turn"),
                }
            }
        }
        // The same records and the same wait, differing in who runs the fsync: a bench
        // thread may take the turn and a runtime worker may not, so both are priced.
        Mode::Forward => {
            for step in 0..per_thread {
                let at = thread * per_thread + step;
                tail.append_data(key_at(at), body.to_vec(), 0, Commit::Batched)
                    .expect("put");
                block_on(tail.sync_if_owed_wait()).expect("owed sync");
            }
        }
        // Handled by run_mode, which owns the worker threads
        Mode::Facade => unreachable!("facade writers run behind their workers"),
    }
}

/// Run one cell's put phase across its writers and return the elapsed seconds
fn run_mode(reel: &Reel, mode: Mode, writers: u64, per_thread: u64, body: &[u8]) -> f64 {
    let start = Instant::now();
    std::thread::scope(|scope| {
        for thread in 0..writers {
            match mode {
                Mode::Block | Mode::Future | Mode::Forward => {
                    let body = &body;
                    scope.spawn(move || run_writer(reel, mode, thread, per_thread, body));
                }
                Mode::Facade => {
                    // One worker per writer, ops crossing on rendezvous channels, so
                    // every op pays both handoffs spawn_blocking would charge it.
                    let (ask, ask_rx) = mpsc::sync_channel::<u64>(0);
                    let (reply, reply_rx) = mpsc::sync_channel::<()>(0);
                    let body = &body;
                    scope.spawn(move || {
                        let tails = reel.tails();
                        let tail = &tails[thread as usize % tails.len()];
                        while let Ok(at) = ask_rx.recv() {
                            tail.append_data(key_at(at), body.to_vec(), 0, Commit::PerRecord)
                                .expect("facade put");
                            if reply.send(()).is_err() {
                                break;
                            }
                        }
                    });
                    scope.spawn(move || {
                        for step in 0..per_thread {
                            ask.send(thread * per_thread + step).expect("ask");
                            reply_rx.recv().expect("reply");
                        }
                    });
                }
            }
        }
    });
    start.elapsed().as_secs_f64()
}

/// A reel on a real backend in a fresh directory
fn open_reel(base: &std::path::Path, backend: IoBackend, sync: SyncPolicy) -> Reel {
    let config = ReelConfig {
        io_backend: backend,
        sync,
        active_tails: ThreadBudget::threads(env_bytes("REEL_ASYNC_TAILS", 0) as u32),
        scrub_mbps: 0,
        ..ReelConfig::default()
    };
    std::fs::create_dir_all(base).expect("reel dir");
    let driver = Arc::new(IoDriver::new(select_backend(&config)));
    let budget = Arc::new(InflightBudget::default());
    let fd_cache = Arc::new(FdCache::new(reel::DEFAULT_FD_CACHE as usize));
    let shared = Arc::new(ReelShared::new(
        base.to_path_buf(),
        driver,
        budget,
        fd_cache,
        config,
        TEST_COLUMNS,
        1,
    ));
    Reel::open(shared, Vec::new()).expect("open reel")
}

/// One measured run of a cell: tail count, put seconds, durable seconds, cpu seconds
#[allow(clippy::too_many_arguments)]
fn measure_cell(
    root: &Option<String>,
    backend: IoBackend,
    sync: SyncPolicy,
    size: u64,
    writers: u64,
    mode: Mode,
    per_thread: u64,
    body: &[u8],
) -> (usize, f64, f64, f64) {
    let temp = TempDir::new().expect("tempdir");
    let base = match root {
        Some(root) => std::path::PathBuf::from(root)
            .join(format!("raw-async-{size}-{writers}-{}", mode.name())),
        None => temp.path().to_path_buf(),
    };
    let _ = std::fs::remove_dir_all(&base);
    let reel = open_reel(&base, backend, sync);
    let tails = reel.tails().len();

    // Segment creation preallocates under the default policy, so every tail is warmed
    // before the clock starts. Rolls inside the phase still count.
    for (at, tail) in reel.tails().iter().enumerate() {
        tail.append_data(
            key_at(u64::MAX - at as u64),
            payload(64),
            0,
            Commit::PerRecord,
        )
        .expect("warm put");
    }
    reel.flush().expect("warm flush");

    let cpu_before = process_cpu_secs();
    let put_secs = run_mode(&reel, mode, writers, per_thread, body);
    let flush_start = Instant::now();
    reel.flush().expect("flush");
    let durable_secs = put_secs + flush_start.elapsed().as_secs_f64();
    let cpu_secs = process_cpu_secs() - cpu_before;
    reel.close().expect("close");
    drop(reel);
    let _ = std::fs::remove_dir_all(&base);
    (tails, put_secs, durable_secs, cpu_secs)
}

// the three wait shapes on the same records, tails, syncs, and backend
#[test]
#[ignore = "writes real files, run explicitly on the box under test"]
fn async_matrix() {
    // libtest leaves the test name line open, so a header printed into it starts a
    // screen-width right of the rows underneath.
    println!();
    let sizes = env_list("REEL_ASYNC_SIZES", DEFAULT_SIZES);
    let thread_counts = env_list("REEL_ASYNC_THREADS", DEFAULT_THREADS);
    let cell_bytes = env_bytes("REEL_ASYNC_BYTES", DEFAULT_CELL_BYTES);
    let max_cell_bytes = env_bytes("REEL_ASYNC_MAX_BYTES", DEFAULT_MAX_CELL_BYTES).max(cell_bytes);
    let max_ops = env_bytes("REEL_ASYNC_MAX_OPS", DEFAULT_MAX_OPS).max(1);
    let root = std::env::var("REEL_ASYNC_DIR").ok();

    println!(
        "cell {} MiB growing to {} MiB when a phase is short, capped at {} ops, dir {}",
        cell_bytes / (1024 * 1024),
        max_cell_bytes / (1024 * 1024),
        max_ops,
        root.clone().unwrap_or_else(|| "tempdir".to_string())
    );
    warn_where_ops_bind(&sizes, max_cell_bytes, max_ops);
    println!(
        "\n{:>13} {:>8} {:>10} {:>9} {:>8} {:>6} {:>10} {:>12} {:>13} {:>10} {:>10}",
        "backend",
        "mode",
        "sync",
        "size",
        "writers",
        "tails",
        "ops",
        "cached MB/s",
        "durable MB/s",
        "put us",
        "cpu us",
    );
    println!(
        "{:>13} {:>8} {:>10} {:>9} {:>8} {:>6} {:>10} {:>12} {:>13} {:>10} {:>10} {:>10}",
        "", "", "", "", "", "", "", "", "", "", "per op", "warning"
    );

    for (backend_name, backend) in backends() {
        for (sync_name, sync) in syncs() {
            for &size in &sizes {
                for &writers in &thread_counts {
                    for mode in modes() {
                        let body = payload(size as usize);
                        // Whichever of the byte and op caps binds first, and never below
                        // one op a writer.
                        let most =
                            (max_cell_bytes / size.max(1)).min(max_ops).max(writers) / writers;
                        let mut per_thread = ((cell_bytes / size.max(1)).min(max_ops).max(writers)
                            / writers)
                            .min(most);

                        // A phase under the minimum is thread spawn and timer noise
                        // wearing a throughput number's clothes, so a short cell is
                        // rerun with more records rather than printed.
                        let (tails, put_secs, durable_secs, cpu_secs) = loop {
                            let measured = measure_cell(
                                &root, backend, sync, size, writers, mode, per_thread, &body,
                            );
                            if measured.1 >= MIN_PHASE_SECS || per_thread >= most {
                                break measured;
                            }
                            let scale =
                                (MIN_PHASE_SECS * 1.4 / measured.1.max(1e-6)).clamp(2.0, 64.0);
                            per_thread = ((per_thread as f64 * scale) as u64)
                                .min(most)
                                .max(per_thread + 1);
                        };
                        let count = per_thread * writers;

                        let bytes = (count * size) as f64;
                        let flag = if put_secs < MIN_PHASE_SECS {
                            "too short"
                        } else {
                            ""
                        };
                        println!(
                            "{backend_name:>13} {:>8} {sync_name:>10} {size:>9} {writers:>8} \
                             {tails:>6} {count:>10} {:>12.0} {:>13.0} {:>10.2} {:>10.3} {flag:>10}",
                            mode.name(),
                            bytes / put_secs / 1e6,
                            bytes / durable_secs / 1e6,
                            durable_secs * 1e6 / count as f64,
                            cpu_secs * 1e6 / count as f64,
                        );
                    }
                }
            }
        }
    }
}

/// Where the wakers land: the seats that woke and the caller thread to rouse
///
/// One board per reader thread, so a push is a short uncontended lock.
struct FlightBoard {
    /// Seats whose wakers rang since the caller last drained
    ready: Mutex<Vec<usize>>,

    /// The caller to unpark when a seat lands on the queue
    caller: Thread,
}

/// The waker one seat hands its future: name the seat, rouse the caller
struct FlightWaker {
    /// The board the seat lands on
    board: Arc<FlightBoard>,

    /// Which of the caller's in-flight reads woke
    seat: usize,
}

impl Wake for FlightWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // The push lands before the unpark, so a caller roused between its empty check
        // and its park re-reads the queue and finds this seat.
        self.board
            .ready
            .lock()
            .expect("ready queue")
            .push(self.seat);
        self.board.caller.unpark();
    }
}

/// Take the seats woken since the last drain, parking until there are any
///
/// A wake cannot be lost to the park: its push lands before its unpark, and an unpark
/// with nobody parked leaves the token that makes the next park return at once.
fn take_woken(board: &FlightBoard) -> Vec<usize> {
    loop {
        let woken = std::mem::take(&mut *board.ready.lock().expect("ready queue"));
        if !woken.is_empty() {
            return woken;
        }
        std::thread::park();
    }
}

/// Drive count launches through a fixed set of seats, parking between wakes
///
/// The caller polls only the seats the queue names, so an unready future costs a poll
/// per wake rather than a poll per sweep. A woken seat is polled until it goes pending,
/// so a launch answering at once hands the seat the next launch and the depth stands
/// while work lasts. Futures are boxed because a seat set sized at run time cannot be
/// pinned in place.
fn drive_flights<Flight: Future>(
    count: usize,
    depth: usize,
    mut launch: impl FnMut(usize) -> Flight,
    mut answer: impl FnMut(Flight::Output),
) {
    let board = Arc::new(FlightBoard {
        ready: Mutex::new(Vec::new()),
        caller: std::thread::current(),
    });
    let seats = depth.clamp(1, count.max(1));
    let mut wakers: Vec<Waker> = Vec::with_capacity(seats);
    for seat in 0..seats {
        let waker = FlightWaker {
            board: Arc::clone(&board),
            seat,
        };
        wakers.push(Waker::from(Arc::new(waker)));
    }
    let mut flying: Vec<Option<Pin<Box<Flight>>>> = Vec::new();
    flying.resize_with(seats, || None);

    let mut next = 0usize;
    let mut landed = 0usize;
    let mut woken: Vec<usize> = (0..seats).collect();
    while landed < count {
        for &seat in &woken {
            loop {
                if flying[seat].is_none() {
                    if next >= count {
                        break;
                    }
                    flying[seat] = Some(Box::pin(launch(next)));
                    next += 1;
                }
                let mut cx = Context::from_waker(&wakers[seat]);
                match flying[seat]
                    .as_mut()
                    .expect("seated")
                    .as_mut()
                    .poll(&mut cx)
                {
                    Poll::Ready(answered) => {
                        flying[seat] = None;
                        landed += 1;
                        answer(answered);
                    }
                    Poll::Pending => break,
                }
            }
        }
        if landed < count {
            woken = take_woken(&board);
        }
    }
}

/// A store on a real backend in a fresh directory, never serving from a mapping
///
/// The async door always reads through the driver, so a mapped blocking row would win
/// every warm cell for a reason that has nothing to do with the wait.
fn open_store(base: &Path, backend: IoBackend) -> ReelStore {
    let config = ReelConfig {
        io_backend: backend,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(env_bytes("REEL_ASYNC_TAILS", 0) as u32),
        scrub_mbps: 0,
        map_above: None,
        ..ReelConfig::default()
    };
    std::fs::create_dir_all(base).expect("reel dir");
    ReelStore::open(base.to_path_buf(), config, READ_COLUMNS).expect("open store")
}

/// The records one reader visits, in an order unrelated to where they landed
///
/// Insertion order makes the read pattern follow the file layout, which measures layout
/// rather than the engine. Every mode walks the plan its reader was given.
fn read_plan(thread: u64, per_thread: u64, count: u64) -> Vec<u64> {
    let mut plan = Vec::with_capacity(per_thread as usize);
    for step in 0..per_thread {
        plan.push((step.wrapping_mul(READ_STRIDE).wrapping_add(thread)) % count);
    }
    plan
}

/// One reader's plan taken one record at a time, the shipped blocking path
fn read_block(store: &ReelStore, keys: &[RecordKey], plan: &[u64]) -> u64 {
    let mut found = 0u64;
    for at in plan {
        if let Some(value) = store.get(&keys[*at as usize]).expect("get") {
            found += value.len() as u64;
        }
    }
    found
}

/// One reader's plan with a fixed number of reads in flight at once
///
/// One caller thread holding many outstanding reads rather than one thread per read. A
/// seat that answers takes the next record of the plan, so what stands at the device is
/// the depth rather than one.
fn read_futures(store: &ReelStore, keys: &[RecordKey], plan: &[u64], depth: usize) -> u64 {
    let mut found = 0u64;
    drive_flights(
        plan.len(),
        depth,
        |at| store.get_wait(&keys[plan[at] as usize]),
        |read| {
            if let Some(value) = read.expect("awaited get") {
                found += value.len() as u64;
            }
        },
    );
    found
}

/// Run one cell's read phase across its readers and return the elapsed seconds
fn read_mode(
    store: &ReelStore,
    keys: &[RecordKey],
    mode: Mode,
    readers: u64,
    per_thread: u64,
    depth: usize,
) -> (f64, u64) {
    let count = per_thread * readers;
    let found = AtomicU64::new(0);
    let start = Instant::now();
    std::thread::scope(|scope| {
        for thread in 0..readers {
            let plan = read_plan(thread, per_thread, count);
            let found = &found;
            match mode {
                Mode::Block => {
                    scope.spawn(move || {
                        found.fetch_add(read_block(store, keys, &plan), Ordering::Relaxed);
                    });
                }
                Mode::Future => {
                    scope.spawn(move || {
                        found.fetch_add(read_futures(store, keys, &plan, depth), Ordering::Relaxed);
                    });
                }
                // Never reached: depths_for sweeps no depth for it, so no read row
                Mode::Forward => unreachable!("the forwarded turn is a write shape"),
                Mode::Facade => {
                    // One worker per reader, reads crossing on rendezvous channels, so
                    // every read pays both handoffs spawn_blocking would charge it.
                    let (ask, ask_rx) = mpsc::sync_channel::<u64>(0);
                    let (reply, reply_rx) = mpsc::sync_channel::<u64>(0);
                    scope.spawn(move || {
                        while let Ok(at) = ask_rx.recv() {
                            let read = store.get(&keys[at as usize]).expect("facade get");
                            let len = read.map(|value| value.len() as u64).unwrap_or(0);
                            if reply.send(len).is_err() {
                                break;
                            }
                        }
                    });
                    scope.spawn(move || {
                        let mut got = 0u64;
                        for at in plan {
                            ask.send(at).expect("ask");
                            got += reply_rx.recv().expect("reply");
                        }
                        found.fetch_add(got, Ordering::Relaxed);
                    });
                }
            }
        }
    });
    (start.elapsed().as_secs_f64(), found.load(Ordering::Relaxed))
}

/// Give the whole page cache back to the kernel, so the next read reaches the drive
///
/// Needs root and Linux. Anywhere else this is a no-op and the numbers stay warm.
fn drop_page_cache() {
    #[cfg(target_os = "linux")]
    {
        use std::io::Write;
        let _ = std::process::Command::new("sync").status();
        match std::fs::OpenOptions::new()
            .write(true)
            .open("/proc/sys/vm/drop_caches")
        {
            Ok(mut file) => {
                if file.write_all(b"3\n").is_err() {
                    eprintln!("could not drop the page cache, reads stay warm");
                }
            }
            Err(_) => eprintln!("could not open drop_caches, reads stay warm"),
        }
    }
}

/// Processor seconds this process has burned, every thread of it counted
///
/// A depth curve that stops climbing has run out of device or out of processor, and
/// those look identical in a throughput column. User plus system across the whole
/// process charges the engine's own threads to the row that woke them.
fn process_cpu_secs() -> f64 {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
        return 0.0;
    }
    let seconds = |time: libc::timeval| time.tv_sec as f64 + time.tv_usec as f64 / 1e6;
    seconds(usage.ru_utime) + seconds(usage.ru_stime)
}

/// One measured run of a read cell: read seconds, payload bytes served, cpu seconds
#[allow(clippy::too_many_arguments)]
fn measure_read_cell(
    root: &Option<String>,
    backend: IoBackend,
    size: u64,
    readers: u64,
    mode: Mode,
    depth: usize,
    per_thread: u64,
    body: &[u8],
    drop_caches: bool,
) -> (f64, f64, f64) {
    let temp = TempDir::new().expect("tempdir");
    let base = match root {
        Some(root) => std::path::PathBuf::from(root)
            .join(format!("raw-async-read-{size}-{readers}-{}", mode.name())),
        None => temp.path().to_path_buf(),
    };
    let _ = std::fs::remove_dir_all(&base);
    let store = open_store(&base, backend);
    let count = per_thread * readers;
    let keys: Vec<RecordKey> = (0..count).map(key_at).collect();

    // The fill is not the measurement: it goes down the plain way and is flushed before
    // the clock starts, and the read phase alone is what a row quotes.
    std::thread::scope(|scope| {
        for thread in 0..readers {
            let (store, keys) = (&store, &keys);
            scope.spawn(move || {
                for step in 0..per_thread {
                    let at = (thread * per_thread + step) as usize;
                    store.put(&keys[at], body).expect("put");
                }
            });
        }
    });
    store.flush().expect("flush");

    // The only way to get a cold read on a box whose memory dwarfs the cell: without it
    // the fill has just laid every record into memory and the read phase is a memcpy.
    if drop_caches {
        drop_page_cache();
    }

    let cpu_before = process_cpu_secs();
    let (read_secs, found) = read_mode(&store, &keys, mode, readers, per_thread, depth);
    let cpu_secs = process_cpu_secs() - cpu_before;
    assert!(found > 0, "the read phase found nothing to read");
    drop(store);
    let _ = std::fs::remove_dir_all(&base);
    (read_secs, found as f64, cpu_secs)
}

/// The depths one mode's rows sweep, which is the future mode's axis alone
///
/// Nothing at all for the forwarded mode: what it forwards is a flush and a read owes
/// none, so its rows would be the future rows renamed.
fn depths_for(mode: Mode, readers: u64, depths: &[u64]) -> Vec<usize> {
    if matches!(mode, Mode::Forward) {
        return Vec::new();
    }

    // A blocking reader's depth is the reader count, so sweeping the knob under it would
    // print one row several times under different headings.
    if matches!(mode, Mode::Block | Mode::Facade) {
        return vec![1];
    }

    // The driver holds one completion slot per op in flight and the readers share them,
    // so a set wider than a reader's share parks that reader inside a submission waiting
    // for a slot only it could free. Clamped rather than refused.
    let share = (SLOT_COUNT / readers as usize).max(1);
    let mut swept: Vec<usize> = Vec::new();
    for &depth in depths {
        let depth = (depth as usize).min(share);
        if !swept.contains(&depth) {
            swept.push(depth);
        }
    }
    swept
}

// the three wait shapes reading the same records, and reads in flight per reader
#[test]
#[ignore = "writes real files, run explicitly on the box under test"]
fn read_matrix() {
    // libtest leaves the test name line open, so a header printed into it starts a
    // screen-width right of the rows underneath.
    println!();
    let sizes = env_list("REEL_ASYNC_SIZES", DEFAULT_SIZES);
    let thread_counts = env_list("REEL_ASYNC_THREADS", DEFAULT_THREADS);
    let depths = env_list("REEL_ASYNC_DEPTH", DEFAULT_DEPTHS);
    let cell_bytes = env_bytes("REEL_ASYNC_BYTES", DEFAULT_CELL_BYTES);
    let max_cell_bytes = env_bytes("REEL_ASYNC_MAX_BYTES", DEFAULT_MAX_CELL_BYTES).max(cell_bytes);
    let max_ops = env_bytes("REEL_ASYNC_MAX_OPS", DEFAULT_MAX_OPS).max(1);
    let root = std::env::var("REEL_ASYNC_DIR").ok();
    let drop_caches = std::env::var("REEL_ASYNC_DROP_CACHES").is_ok();

    println!(
        "cell {} MiB growing to {} MiB when a phase is short, capped at {} ops, \
         drop_caches {}, dir {}",
        cell_bytes / (1024 * 1024),
        max_cell_bytes / (1024 * 1024),
        max_ops,
        drop_caches,
        root.clone().unwrap_or_else(|| "tempdir".to_string())
    );
    warn_where_ops_bind(&sizes, max_cell_bytes, max_ops);
    println!("mapped reads off on every row, since the async door never maps");
    if !drop_caches {
        println!(
            "the page cache is kept: the fill left every record in memory, so the read \
             columns are a warm number and not a device one"
        );
    }
    println!(
        "\n{:>13} {:>8} {:>7} {:>9} {:>8} {:>10} {:>12} {:>10} {:>10}",
        "backend", "mode", "depth", "size", "readers", "ops", "read MB/s", "read us", "cpu us",
    );
    println!(
        "{:>13} {:>8} {:>7} {:>9} {:>8} {:>10} {:>12} {:>10} {:>10} {:>10}",
        "", "", "", "", "", "", "", "", "per op", "warning"
    );

    for (backend_name, backend) in backends() {
        for &size in &sizes {
            for &readers in &thread_counts {
                for mode in modes() {
                    for &depth in &depths_for(mode, readers, &depths) {
                        let body = payload(size as usize);
                        // Whichever of the byte and op caps binds first, and never below
                        // one op a reader.
                        let most =
                            (max_cell_bytes / size.max(1)).min(max_ops).max(readers) / readers;
                        let mut per_thread = ((cell_bytes / size.max(1)).min(max_ops).max(readers)
                            / readers)
                            .min(most);

                        // A phase under the minimum is thread spawn and timer noise
                        // wearing a throughput number's clothes, so a short cell is
                        // rerun with more records rather than printed.
                        let (read_secs, bytes, cpu_secs) = loop {
                            let measured = measure_read_cell(
                                &root,
                                backend,
                                size,
                                readers,
                                mode,
                                depth,
                                per_thread,
                                &body,
                                drop_caches,
                            );
                            if measured.0 >= MIN_PHASE_SECS || per_thread >= most {
                                break measured;
                            }
                            let scale =
                                (MIN_PHASE_SECS * 1.4 / measured.0.max(1e-6)).clamp(2.0, 64.0);
                            per_thread = ((per_thread as f64 * scale) as u64)
                                .min(most)
                                .max(per_thread + 1);
                        };
                        let count = per_thread * readers;

                        let flag = if read_secs < MIN_PHASE_SECS {
                            "too short"
                        } else {
                            ""
                        };
                        println!(
                            "{backend_name:>13} {:>8} {depth:>7} {size:>9} {readers:>8} \
                             {count:>10} {:>12.0} {:>10.2} {:>10.3} {flag:>10}",
                            mode.name(),
                            bytes / read_secs / 1e6,
                            read_secs * 1e6 / count as f64,
                            cpu_secs * 1e6 / count as f64,
                        );
                    }
                }
            }
        }
    }
}

// what each admission wait shape costs when nothing waits, and when everything does
#[test]
#[ignore = "cpu probe, run explicitly"]
fn wait_points() {
    // libtest leaves the test name line open, so a header printed into it starts a
    // screen-width right of the rows underneath.
    println!();
    println!("admission wait shapes, no io underneath");

    // The path that does not wait: the blocking call answers from one compare and swap,
    // the future takes the waitlist gate to read the state it guards.
    let roomy = InflightBudget::new(ByteCount::from_bytes(u64::MAX / 2));
    let acquire_ns = charge(PROBE_ROUNDS, || {
        roomy.acquire(4096);
        roomy.release(4096);
    });
    let reserve_ns = charge(PROBE_ROUNDS, || {
        block_on(roomy.reserve(4096));
        roomy.release(4096);
    });
    println!(
        "\n{:>24} {:>12} {:>12}",
        "fast path, ns/op", "acquire", "reserve"
    );
    println!("{:>24} {acquire_ns:>12.0} {reserve_ns:>12.0}", "");

    // The path that always waits: a budget admitting one op at a time under more writers
    // than that, so every release is a wakeup and every acquire waited for one.
    println!(
        "\n{:>24} {:>12} {:>12}",
        "contended, ops/s", "acquire", "reserve"
    );
    for writers in [2u64, 4, 8] {
        let tight = InflightBudget::new(ByteCount::from_bytes(4096));
        let per_thread = 200_000u64;
        let park_secs = contend(&tight, writers, per_thread, false);
        let wait_secs = contend(&tight, writers, per_thread, true);
        let ops = (writers * per_thread) as f64;
        println!(
            "{:>21} {writers:>2} {:>12.0} {:>12.0}",
            "writers",
            ops / park_secs,
            ops / wait_secs,
        );
    }
}

/// Nanoseconds one call costs, averaged over a round count
fn charge<Work>(rounds: u64, mut work: Work) -> f64
where
    Work: FnMut(),
{
    let start = Instant::now();
    for _ in 0..rounds {
        work();
    }
    start.elapsed().as_secs_f64() * 1e9 / rounds as f64
}

/// Writers fighting over a budget that admits one of them at a time
fn contend(budget: &InflightBudget, writers: u64, per_thread: u64, as_future: bool) -> f64 {
    let start = Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..writers {
            scope.spawn(move || {
                for _ in 0..per_thread {
                    if as_future {
                        block_on(budget.reserve(4096));
                    } else {
                        budget.acquire(4096);
                    }
                    budget.release(4096);
                }
            });
        }
    });
    start.elapsed().as_secs_f64()
}

/// What the executor test's waking thread flips and its flight reads
struct LandingState {
    /// Whether the flight may answer its next poll
    is_ready: bool,

    /// The waker the flight left at its last pending poll
    waker: Option<Waker>,
}

/// A flight that answers only after another thread rings the waker it left
struct DelayedFlight {
    /// The state the waking thread flips
    state: Arc<Mutex<LandingState>>,

    /// What the flight answers with, its launch number
    number: usize,
}

impl Future for DelayedFlight {
    type Output = usize;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<usize> {
        let mut state = self.state.lock().expect("landing state");
        if state.is_ready {
            return Poll::Ready(self.number);
        }
        state.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

/// Launch one delayed flight, leaving its state where the waking thread scans
fn launch_delayed(at: usize, launched: &Mutex<Vec<Arc<Mutex<LandingState>>>>) -> DelayedFlight {
    let state = Arc::new(Mutex::new(LandingState {
        is_ready: false,
        waker: None,
    }));
    launched
        .lock()
        .expect("launched flights")
        .push(Arc::clone(&state));
    DelayedFlight { state, number: at }
}

/// Mark one flight ready and ring its waker, reporting whether it was waiting
fn ring(state: &Mutex<LandingState>) -> bool {
    let waker = {
        let mut held = state.lock().expect("landing state");
        if held.is_ready {
            return false;
        }
        let Some(waker) = held.waker.take() else {
            return false;
        };
        held.is_ready = true;
        waker
    };
    waker.wake();
    true
}

/// Ring every flight that has left a waker, until the asked count has landed
///
/// Newest first, so the seats land out of turn, and the scan cadence leaves the driving
/// thread parked most of the time, which is the path under test.
fn wake_from_afar(launched: &Mutex<Vec<Arc<Mutex<LandingState>>>>, count: usize) {
    let mut released = 0usize;
    while released < count {
        std::thread::sleep(WAKE_DELAY);
        let seen: Vec<Arc<Mutex<LandingState>>> =
            launched.lock().expect("launched flights").clone();
        for state in seen.iter().rev() {
            if ring(state) {
                released += 1;
            }
        }
    }
}

// wakes from another thread reach every parked seat and none is lost to the park race
#[test]
fn delayed_wakes() {
    let launched: Arc<Mutex<Vec<Arc<Mutex<LandingState>>>>> = Arc::new(Mutex::new(Vec::new()));
    let landed: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    let waking = std::thread::spawn({
        let launched = Arc::clone(&launched);
        move || wake_from_afar(&launched, DELAYED_FLIGHTS)
    });

    let (done, done_rx) = mpsc::channel();
    let driving = std::thread::spawn({
        let launched = Arc::clone(&launched);
        let landed = Arc::clone(&landed);
        move || {
            drive_flights(
                DELAYED_FLIGHTS,
                DELAYED_DEPTH,
                |at| launch_delayed(at, &launched),
                |number| landed.lock().expect("landed flights").push(number),
            );
            done.send(()).expect("report");
        }
    });
    done_rx
        .recv_timeout(DRIVE_DEADLINE)
        .expect("a lost wake parked the driver forever");
    driving.join().expect("driving thread");
    waking.join().expect("waking thread");

    let mut landed = landed.lock().expect("landed flights").clone();
    landed.sort_unstable();
    let wanted: Vec<usize> = (0..DELAYED_FLIGHTS).collect();
    assert_eq!(landed, wanted);
}

// a set that answers at first poll drains through the seats without parking
#[test]
fn inline_flights() {
    let mut landed = 0u64;

    drive_flights(
        INLINE_FLIGHTS,
        INLINE_DEPTH,
        |at| std::future::ready(at as u64),
        |answered| landed += answered,
    );

    let wanted: u64 = (0..INLINE_FLIGHTS as u64).sum();
    assert_eq!(landed, wanted);
}
