//! One block's working set, cold, at rising read depth
//!
//! A block touches 1,000 to 3,000 distinct accounts, and at 100 us a cold read issued one
//! at a time that is 300 ms, a failed slot at either slot length; the same reads 64 deep
//! is 5 ms. Depth is the mechanism rather than an optimisation, and that stays a model
//! until this is run.
//!
//! The volume is the mainnet account size shape: 0, 165 and 200 byte values at 3, 75 and
//! 20, under 32 byte keys, and the index stays resident so a read is the record fetch and
//! nothing else. The page cache is dropped between legs; on a box without root the drop
//! fails, says so, and every number after it is warm and worthless.
//!
//! Run on a real Linux box as root:
//!   cargo test -p tape-reel --test account_depth --release -- --ignored --nocapture
//!
//! Knobs, all optional: REEL_DEPTH_DIR, REEL_DEPTH_ACCOUNTS, REEL_DEPTH_SET,
//! REEL_DEPTH_DEPTHS, REEL_DEPTH_THREADS, REEL_DEPTH_BACKENDS.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::Thread;
use std::time::Instant;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, IoBackend, KeyWidth, MapShape, RecordKey,
    ReelConfig, ReelStore, SyncPolicy, ThreadBudget, MAP_EVERYTHING,
};

const ACCOUNTS_CF: ColumnId = ColumnId(1);

/// The accounts column: 32 byte keys, values on the record, nothing carried
const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ACCOUNTS_CF,
    name: "accounts",
    key_width: KeyWidth::Fixed(32),
    shard_bytes: 2,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

/// Accounts the volume holds, enough that a 3,000 key sample shares few pages
const DEFAULT_ACCOUNTS: u64 = 20_000_000;

/// Distinct accounts one leg reads, which is one block's working set
const DEFAULT_SET: u64 = 3_000;

/// Reads in flight the awaited legs sweep, one being the blocking door's twin
const DEFAULT_DEPTHS: &str = "1,4,8,16,32,64,128,256";

/// Reader threads the blocking legs sweep, which is the blocking door's own depth
///
/// A validator loads accounts from its replay pool the same way, so this is the incumbent
/// the awaited rows have to beat, not depth 1.
const DEFAULT_THREADS: &str = "1,8,16,32,64";

/// The mainnet value shape: sizes and their weights
const SIZES: [usize; 3] = [0, 165, 200];
const WEIGHTS: [u64; 3] = [3, 75, 20];

/// A stride coprime to any account count that is not a multiple of it, so a
/// leg's sample is distinct keys by construction
const SAMPLE_STEP: u64 = 1_000_003;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// The size the nth account takes, per the weights
fn account_size(at: u64) -> usize {
    let total: u64 = WEIGHTS.iter().sum();
    let mut point = at % total;
    for (size, weight) in SIZES.iter().zip(WEIGHTS) {
        if point < weight {
            return *size;
        }
        point -= weight;
    }
    SIZES[0]
}

/// A scattered 32 byte key, so key order and file order share nothing
fn account_key(at: u64) -> RecordKey {
    let mixed = at
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .rotate_left(29)
        .wrapping_mul(0xBF58_476D_1CE4_E5B9);
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&mixed.to_be_bytes());
    bytes[8..16].copy_from_slice(&at.to_be_bytes());
    RecordKey::from_bytes(ACCOUNTS_CF, &bytes).expect("key")
}

/// The keys one leg reads: distinct within the leg, shifted between legs
///
/// Materialised ahead of the timed region, since an awaited get borrows its key for the
/// life of the future and a key built in the launch closure dies first.
fn sample(leg: u64, set: u64, accounts: u64) -> Vec<RecordKey> {
    (0..set)
        .map(|at| {
            account_key(
                (leg.wrapping_mul(7_919)
                    .wrapping_add(at.wrapping_mul(SAMPLE_STEP)))
                    % accounts,
            )
        })
        .collect()
}

/// Give the whole page cache back to the kernel, so the next read reaches the drive
///
/// Needs root and Linux. Anywhere else this is a no-op and the caller's numbers stay warm.
fn drop_page_cache() -> bool {
    #[cfg(target_os = "linux")]
    {
        use std::io::Write;
        let _ = std::process::Command::new("sync").status();
        match std::fs::OpenOptions::new()
            .write(true)
            .open("/proc/sys/vm/drop_caches")
        {
            Ok(mut file) => return file.write_all(b"3\n").is_ok(),
            Err(_) => return false,
        }
    }
    #[allow(unreachable_code)]
    false
}

/// Where the wakers land: the seats that woke and the caller thread to rouse
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
/// A woken seat is polled until it goes pending, and a launch that answers at once hands
/// the seat the next launch, so the depth stands while work lasts.
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

/// Processor seconds this process has burned, every thread of it counted
///
/// A depth curve that stops climbing has either run out of device or run out of processor,
/// and those look identical in a wall column. Counted across the whole process, so the
/// engine's own threads are charged to their leg.
fn process_cpu_secs() -> f64 {
    #[cfg(unix)]
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut usage) == 0 {
            let user = usage.ru_utime.tv_sec as f64 + usage.ru_utime.tv_usec as f64 * 1e-6;
            let sys = usage.ru_stime.tv_sec as f64 + usage.ru_stime.tv_usec as f64 * 1e-6;
            return user + sys;
        }
    }
    0.0
}

fn config(backend: IoBackend) -> ReelConfig {
    ReelConfig {
        io_backend: backend,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        scrub_mbps: 0,
        // The async door never maps, and a mapped blocking leg would race a page fault
        // against a driver read, so both doors read unmapped here.
        map_above: None,
        segment_bytes: ByteCount::from_bytes(64 * 1024 * 1024),
        ..ReelConfig::default()
    }
}

fn backends() -> Vec<(String, IoBackend)> {
    let asked = std::env::var("REEL_DEPTH_BACKENDS").unwrap_or_else(|_| "posix,uring".to_string());
    asked
        .split(',')
        .filter(|name| !name.is_empty())
        .map(|name| {
            let backend = match name {
                "posix" => IoBackend::Posix,
                "uring" => IoBackend::Uring,
                "uring_direct" => IoBackend::UringDirect,
                other => panic!("unknown backend {other}"),
            };
            (name.to_string(), backend)
        })
        .collect()
}

/// One cold leg: drop the cache, read the set, and account for every key
///
/// A blocking leg's width is reader threads and an awaited leg's is reads in flight on one
/// caller. The wall clock covers the whole set either way, so the rows compare.
fn leg(store: &ReelStore, keys: &[RecordKey], door: &str, width: usize) -> (f64, f64) {
    if !drop_page_cache() {
        println!("page cache NOT dropped, this leg is warm");
    }
    let mut found = 0u64;
    let cpu_before = process_cpu_secs();
    let start = Instant::now();
    match door {
        "await" => {
            drive_flights(
                keys.len(),
                width,
                |at| store.get_wait(&keys[at]),
                |read| {
                    if read.expect("awaited get").is_some() {
                        found += 1;
                    }
                },
            );
        }
        _ => {
            let chunk = keys.len().div_ceil(width.max(1));
            found = std::thread::scope(|scope| {
                let readers: Vec<_> = keys
                    .chunks(chunk)
                    .map(|part| {
                        scope.spawn(move || {
                            let mut landed = 0u64;
                            for key in part {
                                if store.get(key).expect("get").is_some() {
                                    landed += 1;
                                }
                            }
                            landed
                        })
                    })
                    .collect();
                readers
                    .into_iter()
                    .map(|reader| reader.join().expect("reader"))
                    .sum()
            });
        }
    }
    let wall = start.elapsed().as_secs_f64();
    let cpu = process_cpu_secs() - cpu_before;
    // Zero byte accounts still answer Some, so every sampled key must land.
    assert_eq!(found, keys.len() as u64, "every account in the set answers");
    (wall, cpu)
}

/// The block working set, cold, serial and then at depth
#[test]
#[ignore = "measurement; run as root on a real Linux box"]
fn cold_depth() {
    let accounts = env_u64("REEL_DEPTH_ACCOUNTS", DEFAULT_ACCOUNTS);
    let set = env_u64("REEL_DEPTH_SET", DEFAULT_SET);
    let depths: Vec<usize> = std::env::var("REEL_DEPTH_DEPTHS")
        .unwrap_or_else(|_| DEFAULT_DEPTHS.to_string())
        .split(',')
        .filter_map(|d| d.parse().ok())
        .collect();
    let threads: Vec<usize> = std::env::var("REEL_DEPTH_THREADS")
        .unwrap_or_else(|_| DEFAULT_THREADS.to_string())
        .split(',')
        .filter_map(|t| t.parse().ok())
        .collect();

    println!();
    println!(
        "{accounts} accounts, mainnet shape 0/165/200 B at 3/75/20, set {set}, index resident"
    );
    println!(
        "{:>13} {:>7} {:>6} {:>10} {:>10} {:>10} {:>9}",
        "backend", "door", "lanes", "set ms", "us/acct", "cpu ms", "verdict"
    );

    for (name, backend) in backends() {
        let dir = match std::env::var("REEL_DEPTH_DIR") {
            Ok(base) => {
                let dir = std::path::PathBuf::from(base).join(format!("depth-{name}"));
                std::fs::create_dir_all(&dir).expect("depth dir");
                dir
            }
            Err(_) => tempfile::TempDir::new().expect("tempdir").keep(),
        };
        let store = match ReelStore::open(dir.clone(), config(backend), COLUMNS) {
            Ok(store) => store,
            Err(refused) => {
                println!("{name:>13} not serving here, skipped: {refused}");
                continue;
            }
        };

        let payloads: Vec<Vec<u8>> = SIZES.iter().map(|size| vec![0x5Au8; *size]).collect();
        let fill = Instant::now();
        for at in 0..accounts {
            let size = account_size(at);
            let payload = payloads.iter().find(|p| p.len() == size).expect("payload");
            store.put(&account_key(at), payload).expect("put");
        }
        store.flush().expect("flush");
        println!("{name:>13} filled in {:.0} s", fill.elapsed().as_secs_f64());

        let mut salt = 0u64;
        let mut run = |store: &ReelStore, door: &str, width: usize| {
            salt += 1;
            let keys = sample(salt, set, accounts);
            let (wall, cpu) = leg(store, &keys, door, width);
            let per_account = wall * 1e6 / set as f64;
            let verdict = if wall * 1e3 < 10.0 { "fits" } else { "" };
            println!(
                "{name:>13} {door:>7} {width:>6} {:>10.1} {:>10.1} {:>10.0} {verdict:>9}",
                wall * 1e3,
                per_account,
                cpu * 1e3,
            );
        };

        for width in &threads {
            run(&store, "block", *width);
        }
        for depth in &depths {
            run(&store, "await", *depth);
        }

        drop(store);

        // The blocking door's fast configuration gets its own cold rows: the same volume
        // reopened with every record mapped, so a read is a page fault instead of a pread.
        // Cold at width is the question, since a faulting thread reads at depth one.
        if matches!(backend, IoBackend::Posix) {
            let reopen = Instant::now();
            let mapped_config = ReelConfig {
                map_above: MAP_EVERYTHING,
                ..config(backend)
            };
            let mapped =
                ReelStore::open(dir.clone(), mapped_config, COLUMNS).expect("reopen mapped");
            println!(
                "{name:>13} reopened mapped in {:.0} s",
                reopen.elapsed().as_secs_f64()
            );
            for width in &threads {
                run(&mapped, "mapped", *width);
            }
            drop(mapped);
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
