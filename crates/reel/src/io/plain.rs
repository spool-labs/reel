//! The plain-syscall backend, used on every platform

use std::collections::hash_map::RandomState;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::hash::{BuildHasher, Hasher};
use std::hint::black_box;
use std::io::{Error, ErrorKind, Result};
use std::mem::MaybeUninit;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::{Arc, Barrier};
use std::thread::{self, Builder};
use std::time::{Duration, Instant};

use reel_api::units::{ByteCount, FileOffset, Nanoseconds, ProcessId};

use crate::io::{Io, IoFile};
use crate::io::job::Job;
use crate::io::lock_outcome::LockOutcome;
use crate::io::plain_file::{PlainFile, reserve_space};
use crate::io::plain_task::{PlainTask, TaskSignal, run_job};
#[cfg(not(target_os = "linux"))]
use crate::io::ring::NoRing;
#[cfg(target_os = "linux")]
use crate::io::uring::UringDriver;
use crate::pool::TOUCH_BYTE;

/// Width of the process id in a lock file, padded with spaces
pub const HOLDER_WIDTH: usize = 10;
/// The file open writes in a root to learn whether its filesystem gives blocks back
pub const FREE_PROBE_NAME: &str = "free-probe";
/// Bytes of that file, two blocks of any filesystem reel runs on
pub const FREE_PROBE_BYTES: usize = 8 * 1024;
/// Layers of stacked devices a look at a disk goes down through
pub const MOST_STACK_DEPTH: usize = 8;
/// The stack of a job thread, which holds a seal's and a merge's deepest calls with room to spare
pub const JOB_STACK_BYTES: ByteCount = ByteCount(1024 * 1024);
/// What a job's touch leaves alone: the guard page, the thread's own data that glibc keeps in the stack, and the frames
/// above the touch, about 15 KiB in a debug build. A kernel with 64 KiB pages takes 64 KiB for the guard alone
pub const STACK_SLACK_BYTES: usize = 128 * 1024;
/// The part of a job's stack written through at its start
pub const TOUCHED_STACK_BYTES: usize = JOB_STACK_BYTES.0 as usize - STACK_SLACK_BYTES;
/// Reads of a lock file before giving up on a holder writing its id
pub const HOLDER_READ_ATTEMPTS: u64 = 100;
/// Wait between reads of a lock file
pub const HOLDER_READ_WAIT: Duration = Duration::from_millis(1);

/// How long a waiting caller sleeps between looks
pub const PAUSE_WAIT: Duration = Duration::from_micros(100);

/// The backend that calls the operating system directly
#[derive(Clone, Debug)]
pub struct PlainIo {
    origin: Instant,
    has_ring: bool,
}

/// A held file lock, released when the file closes
#[derive(Debug)]
pub struct PlainLock {
    pub file: File,
}

impl PlainIo {
    /// Start a backend whose clock counts from now, with async reads on the blocking pool
    pub fn new() -> Self {
        Self { origin: Instant::now(), has_ring: false }
    }

    /// Start a backend whose async reads go through the kernel's ring
    pub fn with_ring() -> Self {
        Self { origin: Instant::now(), has_ring: true }
    }
}

impl Default for PlainIo {
    fn default() -> Self {
        Self::new()
    }
}

impl Io for PlainIo {
    type File = PlainFile;
    type Lock = PlainLock;
    type Task = PlainTask;
    #[cfg(target_os = "linux")]
    type Ring = UringDriver;
    #[cfg(not(target_os = "linux"))]
    type Ring = NoRing;

    #[cfg(target_os = "linux")]
    fn open_ring(&self) -> Result<Option<UringDriver>> {
        // a kernel or a sandbox with no ring leaves the blocking pool, and the report says so
        Ok(if self.has_ring { UringDriver::open().ok() } else { None })
    }

    #[cfg(not(target_os = "linux"))]
    fn open_ring(&self) -> Result<Option<NoRing>> {
        Ok(None)
    }

    fn create_file(&self, path: &Path, reserved: ByteCount) -> Result<PlainFile> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        let file = options.open(path)?;
        if reserved.0 == 0 {
            return Ok(PlainFile::new(file));
        }

        if let Err(error) = reserve_space(&file, reserved) {
            // a segment that failed its reservation must not be left for open to find
            let _removed = fs::remove_file(path);
            return Err(error);
        }
        Ok(PlainFile::new(file))
    }

    fn open_file(&self, path: &Path) -> Result<PlainFile> {
        let mut options = OpenOptions::new();
        options.read(true).write(true);
        Ok(PlainFile::new(options.open(path)?))
    }

    fn open_read_only(&self, path: &Path) -> Result<PlainFile> {
        Ok(PlainFile::new(File::open(path)?))
    }

    fn can_free_blocks(&self, root: &Path) -> bool {
        let path = root.join(FREE_PROBE_NAME);
        let probe = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&path);
        let Ok(probe) = probe else {
            return false;
        };
        let is_written = probe.write_all_at(&[1; FREE_PROBE_BYTES], 0).is_ok();
        let file = PlainFile::new(probe);
        let is_freed = is_written && file.free_range(FileOffset(0), ByteCount(FREE_PROBE_BYTES as u64)).is_ok();
        file.close();
        // a probe left behind is the next open's to replace
        let _removed = fs::remove_file(&path);
        is_freed
    }

    fn close_file(&self, file: PlainFile) {
        file.close();
    }

    fn remove_file(&self, path: &Path) -> Result<()> {
        fs::remove_file(path)
    }

    fn rename_file(&self, from: &Path, to: &Path) -> Result<()> {
        fs::rename(from, to)
    }

    fn create_directory(&self, path: &Path) -> Result<()> {
        fs::create_dir_all(path)
    }

    fn sync_directory(&self, path: &Path) -> Result<()> {
        File::open(path)?.sync_all()
    }

    fn list_directory(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let mut paths = Vec::new();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                paths.push(entry.path());
            }
        }
        paths.sort();
        Ok(paths)
    }

    fn is_rotational(&self, path: &Path) -> Option<bool> {
        rotational_under(path)
    }

    fn free_space(&self, path: &Path) -> Result<ByteCount> {
        let directory = File::open(path)?;
        let mut stats = MaybeUninit::<libc::statvfs>::zeroed();
        // SAFETY: the descriptor is open and the pointer is valid for the call
        let status = unsafe { libc::fstatvfs(directory.as_raw_fd(), stats.as_mut_ptr()) };
        if status != 0 {
            return Err(Error::last_os_error());
        }

        // SAFETY: fstatvfs filled the struct, and every field is a plain integer
        let stats = unsafe { stats.assume_init() };
        let blocks = widen(stats.f_bavail);
        let block_size = widen(stats.f_frsize);
        Ok(ByteCount(blocks.saturating_mul(block_size)))
    }

    fn lock(&self, path: &Path) -> Result<LockOutcome<PlainLock>> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        let file = options.open(path)?;

        match file.try_lock() {
            Ok(()) => {
                // one padded write, so a reader never sees a cut-down id
                let holder = format!("{:>HOLDER_WIDTH$}", process::id());
                file.write_all_at(holder.as_bytes(), 0)?;
                Ok(LockOutcome::Held(PlainLock { file }))
            },
            Err(TryLockError::WouldBlock) => Ok(LockOutcome::Refused {
                holder: read_holder(&file)?,
            }),
            Err(TryLockError::Error(error)) => Err(error),
        }
    }

    fn now(&self) -> Nanoseconds {
        let elapsed = self.origin.elapsed();
        let nanoseconds = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        Nanoseconds(nanoseconds)
    }

    fn random_word(&self) -> u64 {
        RandomState::new().build_hasher().finish()
    }

    fn pause(&self) {
        thread::sleep(PAUSE_WAIT);
    }

    fn job_stack_bytes(&self) -> ByteCount {
        JOB_STACK_BYTES
    }

    fn spawn_job<Work: Job>(&self, name: &'static str, job: Work) -> Result<PlainTask> {
        let signal = Arc::new(TaskSignal::default());
        let thread_signal = Arc::clone(&signal);
        let thread_io = self.clone();
        // open isn't over until the stack is written, so it waits for the thread to get that far
        let ready = Arc::new(Barrier::new(2));
        let thread_ready = Arc::clone(&ready);
        let thread = Builder::new()
            .name(name.to_string())
            .stack_size(JOB_STACK_BYTES.0 as usize)
            .spawn(move || {
                touch_stack();
                thread_ready.wait();
                run_job(job, &thread_signal, &thread_io);
            })?;
        ready.wait();
        Ok(PlainTask::new(signal, thread))
    }
}

/// Write through the thread's stack now, so a job never faults one of its pages in later
#[inline(never)]
pub fn touch_stack() {
    let mut stack = [TOUCH_BYTE; TOUCHED_STACK_BYTES];
    black_box(&mut stack);
}

/// Read the holder's id, waiting out the moment a first holder takes to write it
pub fn read_holder(file: &File) -> Result<ProcessId> {
    let mut buf = [0; HOLDER_WIDTH];
    for _attempt in 0..HOLDER_READ_ATTEMPTS {
        let count = file.read_at(&mut buf, 0)?;
        if count == HOLDER_WIDTH {
            return parse_holder(&buf);
        }
        thread::sleep(HOLDER_READ_WAIT);
    }
    Err(Error::from(ErrorKind::InvalidData))
}

/// Parse a padded process id
pub fn parse_holder(buf: &[u8]) -> Result<ProcessId> {
    let text = String::from_utf8_lossy(buf);
    match text.trim().parse() {
        Ok(process) => Ok(ProcessId(process)),
        Err(_unreadable) => Err(Error::from(ErrorKind::InvalidData)),
    }
}

/// Widen a field whose width differs between platforms
pub fn widen<Field: Into<u64>>(field: Field) -> u64 {
    field.into()
}

/// Say whether any disk under a path spins, following a stacked device down to its slaves
#[cfg(target_os = "linux")]
pub fn rotational_under(path: &Path) -> Option<bool> {
    use std::os::linux::fs::MetadataExt;

    let device = fs::metadata(path).ok()?.st_dev();
    // the kernel's encoding of a device number
    let major = ((device >> 8) & 0xFFF) | ((device >> 32) & !0xFFF);
    let minor = (device & 0xFF) | ((device >> 12) & !0xFF);
    let block = fs::canonicalize(format!("/sys/dev/block/{major}:{minor}")).ok()?;
    block_spins(&block, 0)
}

/// Say whether a block device spins, for an operating system with no way to ask
#[cfg(not(target_os = "linux"))]
pub fn rotational_under(_path: &Path) -> Option<bool> {
    None
}

/// Check a block device or anything stacked under it spins. A partition asks its disk
#[cfg(target_os = "linux")]
pub fn block_spins(block: &Path, depth: usize) -> Option<bool> {
    if depth > MOST_STACK_DEPTH {
        return None;
    }
    let slaves: Vec<_> = fs::read_dir(block.join("slaves")).map(|entries| entries.flatten().map(|entry| entry.path()).collect()).unwrap_or_default();
    if !slaves.is_empty() {
        let mut known = None;
        for slave in slaves {
            let device = fs::canonicalize(slave).ok()?;
            match block_spins(&device, depth + 1) {
                Some(true) => return Some(true),
                Some(false) => known = Some(false),
                None => {},
            }
        }
        return known;
    }
    if block.join("partition").exists() {
        return block_spins(block.parent()?, depth + 1);
    }
    let flag = fs::read_to_string(block.join("queue/rotational")).ok()?;
    Some(flag.trim() == "1")
}
