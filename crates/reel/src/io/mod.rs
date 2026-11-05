//! The one interface every file operation, clock read and engine thread goes through

pub mod job;
pub mod lock_outcome;
pub mod plain;
pub mod plain_file;
pub mod plain_task;
pub mod simulated;

use std::io::{IoSlice, Result};
use std::path::{Path, PathBuf};

use reel_api::units::{ByteCount, FileOffset, Nanoseconds};

use crate::io::job::Job;
use crate::io::lock_outcome::LockOutcome;

/// One open file
pub trait IoFile: Send + Sync {
    /// Read from the offset, and return how many bytes the file had
    fn read_at(&self, offset: FileOffset, buf: &mut [u8]) -> Result<ByteCount>;

    /// Fill the whole buffer from the offset
    fn read_exact_at(&self, offset: FileOffset, buf: &mut [u8]) -> Result<()>;

    /// Write every part at the offset in one write call
    fn write_at(&self, offset: FileOffset, parts: &[IoSlice]) -> Result<()>;

    /// Make every earlier write survive a power cut
    fn sync(&self) -> Result<()>;

    /// Read the file's length
    fn length(&self) -> Result<ByteCount>;

    /// Cut the file back to a length
    fn truncate(&self, length: ByteCount) -> Result<()>;
}

/// A background job's handle
pub trait IoTask: Send + Sync {
    /// Tell an idle job to look for work
    fn wake(&self);

    /// Stop the job after its current step and wait for it
    fn stop(&self) -> Result<()>;
}

/// Every way the engine touches the outside world
pub trait Io: Clone + Send + Sync + 'static {
    /// An open file
    type File: IoFile;
    /// A held lock, released when dropped
    type Lock: Send + Sync;
    /// A running job
    type Task: IoTask;

    /// Create a new file with its space reserved, and fail when the disk can't hold it
    fn create_file(&self, path: &Path, reserved: ByteCount) -> Result<Self::File>;

    /// Open an existing file to read and write
    fn open_file(&self, path: &Path) -> Result<Self::File>;

    /// Open an existing file to read
    fn open_read_only(&self, path: &Path) -> Result<Self::File>;

    /// Unlink a file, which stays readable through its open handles
    fn remove_file(&self, path: &Path) -> Result<()>;

    /// Replace a name with another in the same directory
    fn rename_file(&self, from: &Path, to: &Path) -> Result<()>;

    /// Create a directory and its parents
    fn create_directory(&self, path: &Path) -> Result<()>;

    /// Make the directory's creates, renames and unlinks survive a power cut
    fn sync_directory(&self, path: &Path) -> Result<()>;

    /// List the files in a directory, sorted by path
    fn list_directory(&self, path: &Path) -> Result<Vec<PathBuf>>;

    /// Read how many bytes the disk under a directory can still take
    fn free_space(&self, path: &Path) -> Result<ByteCount>;

    /// Take a process-wide lock on a file, or learn which process holds it
    fn lock(&self, path: &Path) -> Result<LockOutcome<Self::Lock>>;

    /// Read the monotonic clock
    fn now(&self) -> Nanoseconds;

    /// Start a job on an engine thread
    fn spawn_job<Work: Job>(&self, name: &'static str, job: Work) -> Result<Self::Task>;
}
