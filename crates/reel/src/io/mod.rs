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

use std::future::{Future, pending, ready};
use std::io::{IoSlice, IoSliceMut, Result};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use reel_api::units::{ByteCount, FileOffset, Nanoseconds, WritePlace};

use crate::engine::read_path::SealedRow;
use crate::io::job::Job;
use crate::io::lock_outcome::LockOutcome;
use crate::io::ring::RingDriver;
use crate::row_kind::RowKind;
use crate::segment::row_block::FooterRow;
use crate::segment::segment_tail::SegmentNumber;

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

    /// Read what the kernel holds with no wait for the device: how many bytes came, or nothing when it would block
    fn try_read_at(&self, offset: FileOffset, buf: &mut [u8]) -> Result<Option<ByteCount>>;

    /// Read parts from what the kernel holds in one call, as `try_read_at` does
    fn try_read_parts_at(&self, offset: FileOffset, parts: &mut [IoSliceMut]) -> Result<Option<ByteCount>>;

    /// Start a range on its way to the device, with no wait for it and no promise it lands
    fn start_writeback(&self, offset: FileOffset, length: ByteCount) -> Result<()>;

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

/// A device and what its reads go to, which travel down a read path together
pub struct ReadOn<'a, Device, Target> {
    pub device: &'a Device,
    pub target: &'a Target,
}

impl<'a, Device, Target> ReadOn<'a, Device, Target> {
    /// Pair a device with what it reads
    pub fn new(device: &'a Device, target: &'a Target) -> Self {
        Self { device, target }
    }
}

impl<Device, Target> Clone for ReadOn<'_, Device, Target> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Device, Target> Copy for ReadOn<'_, Device, Target> {}

/// How far a try's walk over a key's segments got. It lives in the get's own future, so the ring pass goes on from the read that
/// would block, and starts over when the list moved under it
#[derive(Debug, Default)]
pub struct WalkMark {
    generation: AtomicU64,
    next: AtomicUsize,
    best_segment: AtomicU64,
    best_place: AtomicU64,
    best_offset: AtomicU64,
    best_length: AtomicU64,
    best_kind: AtomicU8,
    best_block: AtomicU32,
}

/// Where a ring pass goes on from
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WalkResume {
    pub next: usize,
    pub best: Option<SealedRow>,
}

impl WalkMark {
    /// Note the walk is about to ask a segment of its first page, and the bound on overlap keeps a walk there. A generation of zero
    /// means no walk has run, so the count is kept one above it
    pub fn note(&self, generation: u64, next: usize) {
        self.generation.store(generation + 1, Ordering::Relaxed);
        self.next.store(next, Ordering::Relaxed);
    }

    /// Forget the place, for a walk past its first page
    pub fn clear(&self) {
        self.generation.store(0, Ordering::Relaxed);
    }

    /// Note the newest row so far
    pub fn note_best(&self, row: &SealedRow) {
        self.best_segment.store(row.segment.0 + 1, Ordering::Relaxed);
        self.best_place.store(row.row.place.0, Ordering::Relaxed);
        self.best_offset.store(row.row.offset.0, Ordering::Relaxed);
        self.best_length.store(row.row.length.0, Ordering::Relaxed);
        self.best_kind.store(row.row.kind.as_byte(), Ordering::Relaxed);
        self.best_block.store(row.block_offset, Ordering::Relaxed);
    }

    /// Take the place a try left, when the list still stands as it saw it. It goes either way, so a read that starts over walks whole
    pub fn take(&self, generation: u64) -> Option<WalkResume> {
        if self.generation.swap(0, Ordering::Relaxed) != generation + 1 {
            return None;
        }
        let segment = self.best_segment.load(Ordering::Relaxed);
        let best = segment.checked_sub(1).and_then(|segment| {
            let kind = RowKind::from_byte(self.best_kind.load(Ordering::Relaxed))?;
            let row = FooterRow {
                place: WritePlace(self.best_place.load(Ordering::Relaxed)),
                offset: FileOffset(self.best_offset.load(Ordering::Relaxed)),
                length: ByteCount(self.best_length.load(Ordering::Relaxed)),
                kind,
            };
            Some(SealedRow { segment: SegmentNumber(segment), row, block_offset: self.best_block.load(Ordering::Relaxed) })
        });
        Some(WalkResume { next: self.next.load(Ordering::Relaxed), best })
    }
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

    /// Read the mark a try leaves its place in
    fn try_mark(&self) -> Option<&WalkMark> {
        None
    }

    /// Read the mark a ring pass takes over from
    fn resume_mark(&self) -> Option<&WalkMark> {
        None
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

/// Reads that answer from what the kernel holds and never finish past that, so a poll that comes back pending found a read
/// that would block, or one that failed, and the long way hears the real answer
#[derive(Clone, Copy, Debug, Default)]
pub struct OnlyWarm<'mark>(pub Option<&'mark WalkMark>);

impl<File: IoFile> DeviceReads<File> for OnlyWarm<'_> {
    fn try_mark(&self) -> Option<&WalkMark> {
        self.0
    }

    fn read_exact(&self, file: &File, offset: FileOffset, buf: &mut [u8]) -> impl Future<Output = Result<()>> + Send {
        let mut done = 0;
        while done < buf.len() {
            match file.try_read_at(FileOffset(offset.0 + done as u64), &mut buf[done..]) {
                Ok(Some(ByteCount(count))) if count > 0 => done += count as usize,
                Ok(Some(_) | None) | Err(_) => break,
            }
        }
        finished_or_never(done == buf.len())
    }

    fn read_parts(&self, file: &File, offset: FileOffset, parts: &mut [IoSliceMut<'_>]) -> impl Future<Output = Result<()>> + Send {
        let total: u64 = parts.iter().map(|part| part.len() as u64).sum();
        let read = file.try_read_parts_at(offset, parts);
        finished_or_never(matches!(read, Ok(Some(ByteCount(count))) if count == total))
    }
}

/// Finish now when a read came whole, and never when it didn't
pub async fn finished_or_never(is_whole: bool) -> Result<()> {
    if !is_whole {
        pending::<()>().await;
    }
    Ok(())
}

/// Poll a read once, and return its answer when nothing in it had to wait
pub fn poll_once<Output>(read: impl Future<Output = Output>) -> Option<Output> {
    let mut read = pin!(read);
    match read.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => Some(output),
        Poll::Pending => None,
    }
}

/// Run a read on this thread, for a read path whose device reads finish where they stand
#[inline(always)]
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

    /// Check the job has ended, so nothing it owed will come
    fn has_ended(&self) -> bool;

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
