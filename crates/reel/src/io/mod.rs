//! The one interface every file operation, clock read and engine thread goes through

pub mod device_error;
pub mod job;
pub mod lock_outcome;
pub mod plain;
pub mod plain_file;
pub mod plain_task;
pub mod ring;
#[cfg(target_os = "linux")]
pub mod uring;

use std::future::{Future, ready};
use std::io::{IoSlice, IoSliceMut, Result};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use reel_api::units::{ByteCount, FileOffset, Nanoseconds};

use crate::io::job::Job;
use crate::io::lock_outcome::LockOutcome;
use crate::io::ring::RingDriver;

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

    /// Read the bytes the file takes on its disk, which a free in place lowers
    fn held_bytes(&self) -> Result<ByteCount>;

    /// Give a range's whole blocks back to the disk, leaving the file's length as it is and the range reading as zeros
    fn free_range(&self, offset: FileOffset, length: ByteCount) -> Result<()>;

    /// Cut the file back to a length
    fn truncate(&self, length: ByteCount) -> Result<()>;
}

/// How a read waits for its device, so one read path serves the blocking calls and the completion ring
pub trait DeviceReads<File>: Sync {
    /// Fill the whole buffer from the offset
    fn read_exact(&self, file: &File, offset: FileOffset, buf: &mut [u8]) -> impl Future<Output = Result<()>> + Send;

    /// Fill every part from the offset
    fn read_parts(&self, file: &File, offset: FileOffset, parts: &mut [IoSliceMut<'_>]) -> impl Future<Output = Result<()>> + Send;

    /// Say whether a read can sit waiting with buffers in hand, so it needs buffers no blocking read waits for
    fn holds_across_waits(&self) -> bool {
        false
    }
}

/// Reads that block the calling thread, so every one is done by its first poll
#[derive(Clone, Copy, Debug)]
pub struct InPlace;

impl<File: IoFile> DeviceReads<File> for InPlace {
    fn read_exact(&self, file: &File, offset: FileOffset, buf: &mut [u8]) -> impl Future<Output = Result<()>> + Send {
        ready(file.read_exact_at(offset, buf))
    }

    fn read_parts(&self, file: &File, offset: FileOffset, parts: &mut [IoSliceMut<'_>]) -> impl Future<Output = Result<()>> + Send {
        ready(file.read_parts_at(offset, parts))
    }
}

/// Run a read on this thread, for a read path whose device reads finish where they stand
pub fn in_place<Output>(read: impl Future<Output = Output>) -> Output {
    let mut read = pin!(read);
    let mut context = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(output) = read.as_mut().poll(&mut context) {
            return output;
        }
    }
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
    /// A completion queue for async reads
    type Ring: RingDriver<Self::File>;
    /// A running job
    type Task: IoTask;

    /// Open the ring async reads complete through, or nothing where they run on the blocking pool
    fn open_ring(&self) -> Result<Option<Self::Ring>>;

    /// Create a new file with its space reserved, and fail when the disk can't hold it
    fn create_file(&self, path: &Path, reserved: ByteCount) -> Result<Self::File>;

    /// Open an existing file to read and write
    fn open_file(&self, path: &Path) -> Result<Self::File>;

    /// Open an existing file to read
    fn open_read_only(&self, path: &Path) -> Result<Self::File>;

    /// Check the filesystem under a root can give a file's blocks back in place
    fn can_free_blocks(&self, root: &Path) -> bool;

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

    /// Say whether any disk under a path spins, or nothing when this backend can't tell
    fn is_rotational(&self, path: &Path) -> Option<bool>;

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
