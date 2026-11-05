//! The plain-syscall backend, used on every platform

use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{Error, ErrorKind, Read, Result, Seek, SeekFrom, Write};
use std::mem::MaybeUninit;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;
use std::thread::Builder;
use std::time::Instant;

use crate::io::Io;
use crate::io::job::Job;
use crate::io::lock_outcome::LockOutcome;
use crate::io::plain_file::{PlainFile, reserve_space};
use crate::io::plain_task::{PlainTask, TaskSignal, run_job};
use crate::units::{ByteCount, Nanoseconds, ProcessId};

/// The backend that calls the operating system directly
#[derive(Clone, Debug)]
pub struct PlainIo {
    origin: Instant,
}

/// A held file lock, released when the file closes
#[derive(Debug)]
pub struct PlainLock {
    file: File,
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
        let mut file = options.open(path)?;

        match file.try_lock() {
            Ok(()) => {
                file.set_len(0)?;
                file.seek(SeekFrom::Start(0))?;
                write!(file, "{}", process::id())?;
                Ok(LockOutcome::Held(PlainLock { file }))
            },
            Err(TryLockError::WouldBlock) => Ok(LockOutcome::Refused {
                holder: read_holder(&mut file)?,
            }),
            Err(TryLockError::Error(error)) => Err(error),
        }
    }

    fn now(&self) -> Nanoseconds {
        let elapsed = self.origin.elapsed();
        let nanoseconds = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        Nanoseconds(nanoseconds)
    }

    fn spawn_job<Work: Job>(&self, name: &'static str, job: Work) -> Result<PlainTask> {
        let signal = Arc::new(TaskSignal::default());
        let thread_signal = Arc::clone(&signal);
        let thread = Builder::new()
            .name(name.to_string())
            .spawn(move || run_job(job, &thread_signal))?;
        Ok(PlainTask::new(signal, thread))
    }
}

/// Read the process id the lock's holder wrote into the file
pub fn read_holder(file: &mut File) -> Result<ProcessId> {
    let mut text = String::new();
    file.read_to_string(&mut text)?;
    match text.trim().parse() {
        Ok(process) => Ok(ProcessId(process)),
        Err(_unreadable) => Err(Error::from(ErrorKind::InvalidData)),
    }
}

impl PlainLock {
    /// Read the holder's process id back from the lock file
    pub fn holder(&mut self) -> Result<ProcessId> {
        self.file.seek(SeekFrom::Start(0))?;
        read_holder(&mut self.file)
    }
}

/// Widen a field whose width differs between platforms
pub fn widen<Field: Into<u64>>(field: Field) -> u64 {
    field.into()
}
