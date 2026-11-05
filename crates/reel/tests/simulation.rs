//! Deterministic simulation: the engine on a simulated disk, clock and scheduler
#![cfg(test)]

use std::env;
use std::fs;
use std::io::{ErrorKind, IoSlice};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use reel::io::job::{Job, JobStep};
use reel::io::lock_outcome::LockOutcome;
use reel::io::plain::PlainIo;
use reel::io::simulated::simulated_io::SimulatedIo;
use reel::io::{Io, IoFile, IoTask};
use reel::units::{ByteCount, FileOffset, ProcessId};

const SEED: u64 = 7;
const SIMULATED_ROOT: &str = "/disk/root";
const RESERVED: ByteCount = ByteCount(4_096);
const JOB_LIMIT: u64 = 5;

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

struct IdleJob;

impl Job for IdleJob {
    fn step(&mut self) -> JobStep {
        JobStep::Idle
    }
}

fn plain_root(test: &str) -> PathBuf {
    let root = env::temp_dir().join(format!("reel-simulation-{}-{test}", process::id()));
    let _removed = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("temporary directory");
    root
}

fn simulated(seed: u64) -> (SimulatedIo, PathBuf) {
    let io = SimulatedIo::new(seed);
    let root = PathBuf::from(SIMULATED_ROOT);
    io.create_directory(&root).expect("root directory");
    (io, root)
}

fn write_bytes<Handle: IoFile>(file: &Handle, offset: u64, bytes: &[u8]) {
    file.write_at(FileOffset(offset), &[IoSlice::new(bytes)]).expect("write");
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
    assert_eq!(
        io.create_file(&path, ByteCount(0)).err().map(|error| error.kind()),
        Some(ErrorKind::AlreadyExists)
    );

    let head = b"head";
    let body = b"body";
    file.write_at(FileOffset(10), &[IoSlice::new(head), IoSlice::new(body)]).expect("write");
    assert_eq!(read_bytes(&file, 10, 8), b"headbody");

    file.sync().expect("sync");
    io.sync_directory(root).expect("directory sync");
    file.truncate(ByteCount(12)).expect("truncate");
    assert_eq!(file.length().expect("length"), ByteCount(12));

    let mut buf = [0; 8];
    let count = file.read_at(FileOffset(10), &mut buf).expect("short read");
    assert_eq!(count, ByteCount(2));
    assert_eq!(
        file.read_exact_at(FileOffset(10), &mut buf).err().map(|error| error.kind()),
        Some(ErrorKind::UnexpectedEof)
    );

    let read_only = io.open_read_only(&path).expect("read only");
    assert_eq!(read_bytes(&read_only, 10, 2), b"he");

    let renamed = root.join("second");
    io.rename_file(&path, &renamed).expect("rename");
    assert_eq!(io.list_directory(root).expect("list"), vec![renamed.clone()]);
    assert_eq!(
        io.open_file(&path).err().map(|error| error.kind()),
        Some(ErrorKind::NotFound)
    );

    io.remove_file(&renamed).expect("remove");
    assert_eq!(read_bytes(&read_only, 10, 2), b"he");
    assert!(io.list_directory(root).expect("list").is_empty());
    assert!(io.free_space(root).expect("free space").0 > 0);
}

fn check_lock<Backend: Io>(io: &Backend, root: &Path) {
    let path = root.join("lock");
    let held = io.lock(&path).expect("lock");
    assert!(matches!(held, LockOutcome::Held(_)));
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

// a second locker in the simulation learns the holder's process
#[test]
fn lock_refusal() {
    let (io, root) = simulated(SEED);
    let path = root.join("lock");

    let held = io.lock(&path).expect("lock");
    let refused = io.lock(&path).expect("second lock");

    assert!(matches!(held, LockOutcome::Held(_)));
    assert!(matches!(refused, LockOutcome::Refused { holder: ProcessId(1_000) }));
}

// a job runs to its end on a real thread and a stop ends an idle one
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

    let idle = io.spawn_job("idle", IdleJob).expect("spawn");
    idle.wake();
    idle.stop().expect("stop");
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
    assert_eq!(io.live_jobs(), 0);
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
    for seed in 0..64 {
        let (io, root) = simulated(seed);
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
    let bytes = vec![7; 2_048];
    let mut lengths: Vec<u64> = Vec::new();

    for seed in 0..64 {
        let (io, root) = simulated(seed);
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
    assert!(lengths.contains(&2_048));
    assert!(lengths.len() > 2);
}

// a name no directory sync covered can vanish in a power cut
#[test]
fn unsynced_name() {
    let mut vanished = 0;
    for seed in 0..64 {
        let (io, root) = simulated(seed);
        let path = root.join("file");
        let file = io.create_file(&path, ByteCount(0)).expect("create");
        file.sync().expect("sync");

        io.power_cut();

        if io.open_file(&path).is_err() {
            vanished += 1;
        }
    }

    assert!(vanished > 0);
    assert!(vanished < 64);
}

// a failed sync reports an error and its writes are not durable
#[test]
fn failed_sync() {
    let (io, root) = simulated(SEED);
    let path = root.join("file");
    let file = io.create_file(&path, ByteCount(0)).expect("create");
    io.sync_directory(&root).expect("directory sync");
    write_bytes(&file, 0, b"lost");
    io.fail_syncs(1);

    assert!(file.sync().is_err());
    assert!(file.sync().is_ok());
    io.power_cut();

    let reopened = io.open_file(&path).expect("reopen");
    assert_eq!(reopened.length().expect("length"), ByteCount(0));
}

// a full disk fails at creation and in a growing write, and space comes back
#[test]
fn full_disk() {
    let (io, root) = simulated(SEED);
    io.add_disk(&root, RESERVED);
    let path = root.join("file");

    let too_large = ByteCount(RESERVED.0 + 1);
    assert_eq!(
        io.create_file(&path, too_large).err().map(|error| error.kind()),
        Some(ErrorKind::StorageFull)
    );

    let file = io.create_file(&path, RESERVED).expect("create");
    assert_eq!(io.free_space(&root).expect("free space"), ByteCount(0));
    write_bytes(&file, 0, b"inside");
    let growing = file.write_at(FileOffset(RESERVED.0), &[IoSlice::new(b"past")]);
    assert_eq!(growing.err().map(|error| error.kind()), Some(ErrorKind::StorageFull));

    io.remove_file(&path).expect("remove");
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

    io.rot(&path, FileOffset(0), 0b0000_0001).expect("rot");
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
    io.arm_crash(1);

    write_bytes(&file, 0, b"first");
    assert!(file.write_at(FileOffset(5), &[IoSlice::new(b"never")]).is_err());
    assert!(io.is_crashed());

    io.kill();
    let reopened = io.open_file(&path).expect("reopen");
    assert_eq!(reopened.length().expect("length"), ByteCount(5));
}

// two runs from one seed make the same choices
#[test]
fn seed_replay() {
    let first = SimulatedIo::new(SEED);
    let second = SimulatedIo::new(SEED);

    for _ in 0..100 {
        assert_eq!(first.draw(1_000), second.draw(1_000));
    }
}
