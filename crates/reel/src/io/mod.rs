//! The one interface every file operation, clock read and engine thread goes through

pub mod device_error;
pub mod job;
pub mod lock_outcome;
pub mod plain;
pub mod plain_file;
pub mod plain_task;

use std::io::{IoSlice, IoSliceMut, Result};
use std::path::{Path, PathBuf};

use reel_api::units::{ByteCount, FileOffset, Nanoseconds};

use crate::io::job::Job;
use crate::io::lock_outcome::LockOutcome;

/// One open file, whose handles clone with no allocation
pub trait IoFile: Clone + Send + Sync {
    /// Read from the offset, and return how many bytes the file had
    fn read_at(&self, offset: FileOffset, buf: &mut [u8]) -> Result<ByteCount>;

    /// Fill the whole buffer from the offset
    fn read_exact_at(&self, offset: FileOffset, buf: &mut [u8]) -> Result<()>;

    /// Fill every part from the offset in one read call
    fn read_parts_at(&self, offset: FileOffset, parts: &mut [IoSliceMut]) -> Result<()>;

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

    /// Close a handle, once no read still uses it
    fn close_file(&self, file: Self::File);

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

    /// Draw a random word for a new store's id and key
    fn random_word(&self) -> u64;

    /// Pick the unit a planner tries first, a volume in turn unless the backend says otherwise
    fn next_unit(&self, last: usize, count: usize) -> usize {
        (last + 1) % count
    }

    /// Let background work run while a caller waits for it
    fn pause(&self);

    /// Start a job on an engine thread
    fn spawn_job<Work: Job>(&self, name: &'static str, job: Work) -> Result<Self::Task>;

    /// Read the stack a job's thread holds, which memory_report shows
    fn job_stack_bytes(&self) -> ByteCount;
}
