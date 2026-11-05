//! The I/O interface behaves the same on both backends, and the simulator injects each fault

use std::env;
use std::fs;
use std::io::{Error, ErrorKind, IoSlice};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use reel::io::job::{Job, JobStep};
use reel::io::lock_outcome::LockOutcome;
use reel::io::plain::PlainIo;
use reel_sim::file_image::BitMask;
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_disk::{FIRST_PROCESS, OperationCount};
use reel_sim::simulated_io::SimulatedIo;
use reel::guard::hold;
use reel_sim::simulated_task::{JobCount, JobIndex};
use reel::io::{Io, IoFile, IoTask};
use reel_api::units::{ByteCount, FileOffset, Nanoseconds};

const RESERVED_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const NEAR_ROT: u64 = 200;

const SEED: Seed = Seed(7);
const SEED_COUNT: u64 = 64;
const SIMULATED_ROOT: &str = "/disk/root";
const RESERVED: ByteCount = ByteCount(4_096);
const JOB_LIMIT: u64 = 5;
const TORN_BYTES: usize = 2_048;
const SECTOR: usize = 512;
const QUIET_WAIT: Duration = Duration::from_millis(30);
const DEADLINE: Nanoseconds = Nanoseconds(1_000_000);
const DRAW_BOUND: u64 = 1_000;
const DRAW_COUNT: u64 = 100;
const SETTLE_ATTEMPTS: u64 = 100;

struct CountingJob {
    count: Arc<AtomicU64>,
}

impl Job for CountingJob {
    fn step(&mut self) -> JobStep {
        let reached = self.count.fetch_add(1, Ordering::Relaxed) + 1;
        if reached >= JOB_LIMIT {
            return JobStep::Finished;
        }
        JobStep::Worked
    }
}

struct IdleJob {
    steps: Arc<AtomicU64>,
    answer: JobStep,
}

impl Job for IdleJob {
    fn step(&mut self) -> JobStep {
        self.steps.fetch_add(1, Ordering::Relaxed);
        self.answer
    }
}

struct StoppingJob<Handle: IoTask + 'static> {
    task: Arc<Mutex<Option<Handle>>>,
    steps: Arc<AtomicU64>,
}

impl<Handle: IoTask + 'static> Job for StoppingJob<Handle> {
    fn step(&mut self) -> JobStep {
        self.steps.fetch_add(1, Ordering::Relaxed);
        if let Some(task) = hold(&self.task).as_ref() {
            task.stop().expect("stop from inside");
        }
        JobStep::Worked
    }
}

fn check_self_stop<Backend: Io>(io: &Backend) -> Arc<AtomicU64> {
    let steps = Arc::new(AtomicU64::new(0));
    let slot = Arc::new(Mutex::new(None));
    let job = StoppingJob {
        task: Arc::clone(&slot),
        steps: Arc::clone(&steps),
    };
    let task = io.spawn_job("stopping", job).expect("spawn");
    *hold(&slot) = Some(task);
    steps
}

fn plain_root(test: &str) -> PathBuf {
    let root = env::temp_dir().join(format!("reel-simulation-{}-{test}", process::id()));
    let _removed = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("temporary directory");
    root
}

fn simulated(seed: Seed) -> (SimulatedIo, PathBuf) {
    let io = SimulatedIo::new(seed);
    let root = PathBuf::from(SIMULATED_ROOT);
    io.create_directory(&root).expect("root directory");
    (io, root)
}

fn kind_of<Value>(result: Result<Value, Error>) -> Option<ErrorKind> {
    match result {
        Ok(_value) => None,
        Err(error) => Some(error.kind()),
    }
}

fn write_bytes<Handle: IoFile>(file: &Handle, offset: u64, bytes: &[u8]) {
    let parts = [IoSlice::new(bytes)];
    file.write_at(FileOffset(offset), &parts).expect("write");
}

fn read_bytes<Handle: IoFile>(file: &Handle, offset: u64, length: usize) -> Vec<u8> {
    let mut buf = vec![0; length];
    file.read_exact_at(FileOffset(offset), &mut buf).expect("read");
    buf
}

fn check_files<Backend: Io>(io: &Backend, root: &Path) {
    let path = root.join("first");
    let file = io.create_file(&path, RESERVED).expect("create");
    assert_eq!(file.length().expect("length"), RESERVED);
    assert_eq!(kind_of(io.create_file(&path, ByteCount(0))), Some(ErrorKind::AlreadyExists));

    let parts = [IoSlice::new(b"head"), IoSlice::new(b"body")];
    file.write_at(FileOffset(10), &parts).expect("write");
    assert_eq!(read_bytes(&file, 10, 8), b"headbody");

    file.sync().expect("sync");
    io.sync_directory(root).expect("directory sync");
    file.truncate(ByteCount(12)).expect("truncate");
    assert_eq!(file.length().expect("length"), ByteCount(12));

    let mut buf = [0; 8];
    let count = file.read_at(FileOffset(10), &mut buf).expect("short read");
    assert_eq!(count, ByteCount(2));
    let past_end = file.read_exact_at(FileOffset(10), &mut buf);
    assert_eq!(kind_of(past_end), Some(ErrorKind::UnexpectedEof));

    let read_only = io.open_read_only(&path).expect("read only");
    assert_eq!(read_bytes(&read_only, 10, 2), b"he");

    let renamed = root.join("second");
    io.rename_file(&path, &renamed).expect("rename");
    assert_eq!(io.list_directory(root).expect("list"), vec![renamed.clone()]);
    assert_eq!(kind_of(io.open_file(&path)), Some(ErrorKind::NotFound));

    io.remove_file(&renamed).expect("remove");
    assert_eq!(read_bytes(&read_only, 10, 2), b"he");
    assert!(io.list_directory(root).expect("list").is_empty());
    assert!(io.free_space(root).expect("free space").0 > 0);

    let copy = read_only.clone();
    assert_eq!(read_bytes(&copy, 10, 2), b"he");
    io.close_file(read_only);
    io.close_file(file);
}

fn check_lock<Backend: Io>(io: &Backend, root: &Path) {
    let path = root.join("lock");
    let held = io.lock(&path).expect("lock");
    assert!(matches!(held, LockOutcome::Held(_)));

    let refused = io.lock(&path).expect("second lock");
    assert!(matches!(refused, LockOutcome::Refused { .. }));
}

fn check_clock<Backend: Io>(io: &Backend) {
    let first = io.now();
    let second = io.now();
    assert!(second >= first);
}

// every file operation behaves the same on both backends
#[test]
fn file_contract() {
    let root = plain_root("files");
    check_files(&PlainIo::new(), &root);
    let _removed = fs::remove_dir_all(&root);

    let (io, root) = simulated(SEED);
    check_files(&io, &root);
}

// the lock and the clock behave the same on both backends
#[test]
fn lock_clock() {
    let root = plain_root("lock");
    let plain = PlainIo::new();
    check_lock(&plain, &root);
    check_clock(&plain);
    let _removed = fs::remove_dir_all(&root);

    let (io, root) = simulated(SEED);
    check_lock(&io, &root);
    check_clock(&io);
}

// a second locker learns the holder's process on both backends
#[test]
fn lock_holder() {
    let root = plain_root("holder");
    let path = root.join("lock");
    let plain = PlainIo::new();
    let held = plain.lock(&path).expect("lock");
    let refused = plain.lock(&path).expect("second lock");
    assert!(matches!(held, LockOutcome::Held(_)));
    match refused {
        LockOutcome::Refused { holder } => assert_eq!(holder.0, process::id()),
        LockOutcome::Held(_) => panic!("second lock must be refused"),
    }
    let _removed = fs::remove_dir_all(&root);

    let (io, root) = simulated(SEED);
    let path = root.join("lock");
    let _held = io.lock(&path).expect("lock");
    match io.lock(&path).expect("second lock") {
        LockOutcome::Refused { holder } => assert_eq!(holder, FIRST_PROCESS),
        LockOutcome::Held(_) => panic!("second lock must be refused"),
    }
}

// a job runs to its end on a real thread
#[test]
fn plain_jobs() {
    let io = PlainIo::new();
    let count = Arc::new(AtomicU64::new(0));
    let job = CountingJob {
        count: Arc::clone(&count),
    };

    let task = io.spawn_job("counting", job).expect("spawn");
    while count.load(Ordering::Relaxed) < JOB_LIMIT {
        thread::sleep(Duration::from_millis(1));
    }
    task.stop().expect("stop");
}

// an idle job sleeps with no timer until a wake, and a stop ends it
#[test]
fn plain_idle() {
    let io = PlainIo::new();
    let steps = Arc::new(AtomicU64::new(0));
    let job = IdleJob {
        steps: Arc::clone(&steps),
        answer: JobStep::Idle,
    };

    let task = io.spawn_job("idle", job).expect("spawn");
    thread::sleep(QUIET_WAIT);
    assert_eq!(steps.load(Ordering::Relaxed), 1);

    task.wake();
    while steps.load(Ordering::Relaxed) < 2 {
        thread::sleep(Duration::from_millis(1));
    }
    task.stop().expect("stop");
}

// a job with a deadline runs again when the clock reaches it
#[test]
fn plain_deadline() {
    let io = PlainIo::new();
    let steps = Arc::new(AtomicU64::new(0));
    let job = IdleJob {
        steps: Arc::clone(&steps),
        answer: JobStep::IdleUntil { deadline: DEADLINE },
    };

    let task = io.spawn_job("deadline", job).expect("spawn");
    while steps.load(Ordering::Relaxed) < 2 {
        thread::sleep(Duration::from_millis(1));
    }
    task.stop().expect("stop");
}

// the simulation steps a job by hand and stops at its end
#[test]
fn simulated_jobs() {
    let (io, _root) = simulated(SEED);
    let count = Arc::new(AtomicU64::new(0));
    let job = CountingJob {
        count: Arc::clone(&count),
    };
    let _task = io.spawn_job("counting", job).expect("spawn");

    while io.step_any().is_some() {}

    assert_eq!(count.load(Ordering::Relaxed), JOB_LIMIT);
    assert_eq!(io.live_jobs(), JobCount(0));
}

// the scheduler leaves an idle job alone until a wake, or until its deadline
#[test]
fn simulated_idle() {
    let (io, _root) = simulated(SEED);
    let steps = Arc::new(AtomicU64::new(0));
    let idle = IdleJob {
        steps: Arc::clone(&steps),
        answer: JobStep::Idle,
    };
    let task = io.spawn_job("idle", idle).expect("spawn");

    assert_eq!(io.step_any(), Some(JobStep::Idle));
    assert_eq!(io.step_any(), None);
    task.wake();
    assert_eq!(io.step_any(), Some(JobStep::Idle));
    assert_eq!(steps.load(Ordering::Relaxed), 2);
    task.stop().expect("stop");

    let timed = IdleJob {
        steps: Arc::clone(&steps),
        answer: JobStep::IdleUntil { deadline: DEADLINE },
    };
    let _task = io.spawn_job("timed", timed).expect("spawn");
    assert!(io.step_any().is_some());
    assert_eq!(io.step_any(), None);
    io.advance_clock(DEADLINE);
    assert!(io.step_any().is_some());
}

// a handle from a killed process can't reach the next process's job
#[test]
fn stale_task() {
    let (io, _root) = simulated(SEED);
    let steps = Arc::new(AtomicU64::new(0));
    let first = IdleJob {
        steps: Arc::clone(&steps),
        answer: JobStep::Idle,
    };
    let stale = io.spawn_job("first", first).expect("spawn");
    io.kill();

    let second = IdleJob {
        steps: Arc::clone(&steps),
        answer: JobStep::Idle,
    };
    let _task = io.spawn_job("second", second).expect("spawn");
    assert_eq!(io.step_job(JobIndex(0)), Some(JobStep::Idle));

    stale.wake();
    assert_eq!(io.step_any(), None);
    stale.stop().expect("stop");
    assert_eq!(io.live_jobs(), JobCount(1));
}

// a kill keeps unsynced writes, and stale handles stop working
#[test]
fn kill_survivors() {
    let (io, root) = simulated(SEED);
    let path = root.join("file");
    let file = io.create_file(&path, ByteCount(0)).expect("create");
    write_bytes(&file, 0, b"kept");

    io.kill();

    assert!(file.length().is_err());
    let reopened = io.open_file(&path).expect("reopen");
    assert_eq!(read_bytes(&reopened, 0, 4), b"kept");
}

// a sync and a directory sync carry a file through a power cut
#[test]
fn synced_survives() {
    for seed in 0..SEED_COUNT {
        let (io, root) = simulated(Seed(seed));
        let path = root.join("file");
        let file = io.create_file(&path, ByteCount(0)).expect("create");
        write_bytes(&file, 0, b"kept");
        file.sync().expect("sync");
        io.sync_directory(&root).expect("directory sync");

        io.power_cut();

        let reopened = io.open_file(&path).expect("reopen");
        assert_eq!(read_bytes(&reopened, 0, 4), b"kept");
    }
}

// an unsynced write can be lost, kept or torn across a power cut
#[test]
fn unsynced_outcomes() {
    let bytes = vec![7; TORN_BYTES];
    let mut lengths: Vec<u64> = Vec::new();

    for seed in 0..SEED_COUNT {
        let (io, root) = simulated(Seed(seed));
        let path = root.join("file");
        let file = io.create_file(&path, ByteCount(0)).expect("create");
        io.sync_directory(&root).expect("directory sync");
        write_bytes(&file, 0, &bytes);

        io.power_cut();

        let reopened = io.open_file(&path).expect("reopen");
        let length = reopened.length().expect("length").0;
        if !lengths.contains(&length) {
            lengths.push(length);
        }
    }

    assert!(lengths.contains(&0));
    assert!(lengths.contains(&(TORN_BYTES as u64)));
    assert!(lengths.len() > 2);
}

// a power cut can keep the tail of a write without its head
#[test]
fn torn_tail() {
    let bytes = vec![7; TORN_BYTES];
    let mut tails_without_heads = 0;

    for seed in 0..SEED_COUNT {
        let (io, root) = simulated(Seed(seed));
        let path = root.join("file");
        let file = io.create_file(&path, RESERVED).expect("create");
        file.sync().expect("sync");
        io.sync_directory(&root).expect("directory sync");
        write_bytes(&file, 0, &bytes);

        io.power_cut();

        let reopened = io.open_file(&path).expect("reopen");
        let image = read_bytes(&reopened, 0, TORN_BYTES);
        if image[0] == 0 && image[TORN_BYTES - SECTOR] == 7 {
            tails_without_heads += 1;
        }
    }

    assert!(tails_without_heads > 0);
}

// a crash that lands on a write leaves a leading part the seed picks
#[test]
fn torn_kill() {
    let bytes = vec![7; TORN_BYTES];
    let mut lengths: Vec<u64> = Vec::new();

    for seed in 0..SEED_COUNT {
        let (io, root) = simulated(Seed(seed));
        let path = root.join("file");
        let file = io.create_file(&path, ByteCount(0)).expect("create");
        io.arm_crash(OperationCount(0));

        let parts = [IoSlice::new(&bytes)];
        assert!(file.write_at(FileOffset(0), &parts).is_err());
        io.kill();

        let reopened = io.open_file(&path).expect("reopen");
        let length = reopened.length().expect("length").0;
        assert_eq!(read_bytes(&reopened, 0, length as usize), vec![7; length as usize]);
        if !lengths.contains(&length) {
            lengths.push(length);
        }
    }

    assert!(lengths.len() > 2);
}

// a name no directory sync covered can vanish in a power cut
#[test]
fn unsynced_name() {
    let mut vanished = 0;
    for seed in 0..SEED_COUNT {
        let (io, root) = simulated(Seed(seed));
        let path = root.join("file");
        let file = io.create_file(&path, ByteCount(0)).expect("create");
        file.sync().expect("sync");

        io.power_cut();

        if io.open_file(&path).is_err() {
            vanished += 1;
        }
    }

    assert!(vanished > 0);
    assert!(vanished < SEED_COUNT);
}

// a failed sync reports an error and its writes are not durable
#[test]
fn failed_sync() {
    let (io, root) = simulated(SEED);
    let path = root.join("file");
    let file = io.create_file(&path, ByteCount(0)).expect("create");
    io.sync_directory(&root).expect("directory sync");
    write_bytes(&file, 0, b"lost");
    io.fail_syncs(OperationCount(1));

    assert!(file.sync().is_err());
    assert!(file.sync().is_ok());
    io.power_cut();

    let reopened = io.open_file(&path).expect("reopen");
    assert_eq!(reopened.length().expect("length"), ByteCount(0));
}

// a full disk fails at creation and in a growing write
#[test]
fn full_disk() {
    let (io, root) = simulated(SEED);
    io.add_disk(&root, RESERVED);
    let path = root.join("file");

    let too_large = ByteCount(RESERVED.0 + 1);
    assert_eq!(kind_of(io.create_file(&path, too_large)), Some(ErrorKind::StorageFull));

    let file = io.create_file(&path, RESERVED).expect("create");
    assert_eq!(io.free_space(&root).expect("free space"), ByteCount(0));
    write_bytes(&file, 0, b"inside");
    let parts = [IoSlice::new(b"past")];
    let growing = file.write_at(FileOffset(RESERVED.0), &parts);
    assert_eq!(kind_of(growing), Some(ErrorKind::StorageFull));
}

// an unlinked file keeps its space until its last handle closes
#[test]
fn held_space() {
    let (io, root) = simulated(SEED);
    io.add_disk(&root, RESERVED);
    let path = root.join("file");
    let file = io.create_file(&path, RESERVED).expect("create");

    io.remove_file(&path).expect("remove");
    assert_eq!(io.free_space(&root).expect("free space"), ByteCount(0));

    io.close_file(file);
    assert_eq!(io.free_space(&root).expect("free space"), RESERVED);
}

// rot flips the stored bits, also through a power cut
#[test]
fn bit_rot() {
    let (io, root) = simulated(SEED);
    let path = root.join("file");
    let file = io.create_file(&path, ByteCount(0)).expect("create");
    write_bytes(&file, 0, &[0b0000_1111]);
    file.sync().expect("sync");
    io.sync_directory(&root).expect("directory sync");

    io.rot(&path, FileOffset(0), BitMask(0b0000_0001)).expect("rot");
    assert_eq!(read_bytes(&file, 0, 1), [0b0000_1110]);

    io.power_cut();
    let reopened = io.open_file(&path).expect("reopen");
    assert_eq!(read_bytes(&reopened, 0, 1), [0b0000_1110]);
}

// an armed crash fails every later operation until the process is replaced
#[test]
fn armed_crash() {
    let (io, root) = simulated(SEED);
    let path = root.join("file");
    let file = io.create_file(&path, ByteCount(0)).expect("create");
    io.arm_crash(OperationCount(1));

    write_bytes(&file, 0, b"first");
    assert!(file.length().is_err());
    assert!(io.is_crashed());
    let parts = [IoSlice::new(b"never")];
    assert!(file.write_at(FileOffset(5), &parts).is_err());

    io.kill();
    let reopened = io.open_file(&path).expect("reopen");
    assert_eq!(reopened.length().expect("length"), ByteCount(5));
}

// two runs from one seed make the same choices
#[test]
fn seed_replay() {
    let first = SimulatedIo::new(SEED);
    let second = SimulatedIo::new(SEED);

    for _ in 0..DRAW_COUNT {
        assert_eq!(first.draw(DRAW_BOUND), second.draw(DRAW_BOUND));
    }
}


// a job that stops its own task ends after that step on both backends
#[test]
fn self_stop() {
    let (io, _root) = simulated(SEED);
    let steps = check_self_stop(&io);
    while io.step_any().is_some() {}
    assert_eq!(steps.load(Ordering::Relaxed), 1);
    assert_eq!(io.live_jobs(), JobCount(0));

    let plain = PlainIo::new();
    let steps = check_self_stop(&plain);
    while steps.load(Ordering::Relaxed) == 0 {
        thread::sleep(Duration::from_millis(1));
    }

    let mut is_settled = false;
    let mut reached = steps.load(Ordering::Relaxed);
    for _ in 0..SETTLE_ATTEMPTS {
        thread::sleep(QUIET_WAIT);
        let latest = steps.load(Ordering::Relaxed);
        is_settled = latest == reached;
        if is_settled {
            break;
        }
        reached = latest;
    }
    assert!(is_settled);
}

// a use after close and a second close are counted, and a copy of a handle is the same handle
#[test]
fn closed_handle() {
    let (io, root) = simulated(SEED);
    let file = io.create_file(&root.join("file"), ByteCount(0)).expect("create");
    let copy = file.clone();

    assert_eq!(io.open_handle_count(), 1);
    io.close_file(file);
    assert_eq!(io.open_handle_count(), 0);

    assert!(copy.length().is_err());
    assert_eq!(io.handle_faults(), OperationCount(1));
    io.close_file(copy);
    assert_eq!(io.handle_faults(), OperationCount(2));
}

// room set aside reads as zeros, a write lands inside it, and a sync keeps its length
#[test]
fn reserved_room() {
    let io = SimulatedIo::new(Seed(40));
    let path = PathBuf::from("/disk/contract/reserved");
    io.create_directory(Path::new("/disk/contract")).expect("directory");
    let file = io.create_file(&path, ByteCount(RESERVED_BYTES)).expect("create");
    assert_eq!(file.length().expect("length"), ByteCount(RESERVED_BYTES));

    let far = FileOffset(RESERVED_BYTES - 8);
    let mut bytes = [1; 8];
    file.read_exact_at(far, &mut bytes).expect("read");
    assert_eq!(bytes, [0; 8]);

    file.write_at(FileOffset(100), &[IoSlice::new(b"held")]).expect("write");
    file.sync().expect("sync");
    file.read_exact_at(FileOffset(100), &mut bytes[..4]).expect("read");
    assert_eq!(&bytes[..4], b"held");
    file.read_exact_at(far, &mut bytes).expect("read");
    assert_eq!(bytes, [0; 8]);
    assert_eq!(file.length().expect("length"), ByteCount(RESERVED_BYTES));

    // a flip gives the file bytes up to it, so this stays near the start
    let near = FileOffset(NEAR_ROT);
    io.rot(&path, near, BitMask(1)).expect("rot");
    file.read_exact_at(near, &mut bytes).expect("read");
    assert_eq!(bytes[0], 1);
    io.close_file(file);
}
