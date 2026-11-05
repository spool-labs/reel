//! The plain-syscall backend, used on every platform

use std::collections::hash_map::RandomState;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::hash::{BuildHasher, Hasher};
use std::io::{Error, ErrorKind, Result};
use std::mem::MaybeUninit;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;
use std::thread::{self, Builder};
use std::time::{Duration, Instant};

use reel_api::units::{ByteCount, Nanoseconds, ProcessId};

use crate::io::Io;
use crate::io::job::Job;
use crate::io::lock_outcome::LockOutcome;
use crate::io::plain_file::{PlainFile, reserve_space};
use crate::io::plain_task::{PlainTask, TaskSignal, run_job};

/// Width of the process id in a lock file, padded with spaces
pub const HOLDER_WIDTH: usize = 10;
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
}

/// A held file lock, released when the file closes
#[derive(Debug)]
pub struct PlainLock {
    pub file: File,
}

impl PlainIo {
    /// Start a backend whose clock counts from now
    pub fn new() -> Self {
        Self { origin: Instant::now() }
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

    fn spawn_job<Work: Job>(&self, name: &'static str, job: Work) -> Result<PlainTask> {
        let signal = Arc::new(TaskSignal::default());
        let thread_signal = Arc::clone(&signal);
        let thread_io = self.clone();
        let thread = Builder::new()
            .name(name.to_string())
            .spawn(move || run_job(job, &thread_signal, &thread_io))?;
        Ok(PlainTask::new(signal, thread))
    }
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
