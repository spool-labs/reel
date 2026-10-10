//! Synchronous POSIX backend that runs each op at submit and queues its completion

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::fs::{self, OpenOptions};
use std::io;
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Instant;

use libc::{c_int, c_void};

use crate::error::{ReelError, Result};
use crate::io::direct::{
    covering_span, cut_into, cut_split_into, wanted_window, AlignedBuf, DIRECT_ALIGN,
};
use crate::io::op::{
    Advice, Completion, FileId, Op, Outcome, ReadBuf, SegmentEntry, SyncRangeMode, WriteBuf,
};
use crate::io::{ReelIo, ServingBackend};
use crate::sync::{lock, read, write};

/// The most buffers one vectored call takes on Linux and macOS, so wider writes split
pub(crate) const MAX_IOVECS: usize = 1024;

/// The preadv2 flag that answers a read from the page cache or fails with EAGAIN
#[cfg(target_os = "linux")]
const RWF_NOWAIT: c_int = 0x0000_0008;

/// The widest span a thread's pooled staging buffer serves, which also sizes ring buffers
pub(crate) const STAGE_BYTES: usize = 128 * 1024;

/// Set the flag, skipping the store once it is already set
fn prove(flag: &AtomicBool) {
    if !flag.load(Ordering::Relaxed) {
        flag.store(true, Ordering::Relaxed);
    }
}

/// Read from resident pages only, returning the result and errno together
#[cfg(target_os = "linux")]
fn nowait_preadv(
    fd: RawFd,
    iov: *const libc::iovec,
    count: c_int,
    offset: u64,
) -> (libc::ssize_t, c_int) {
    // Safety: the list holds count buffers with owned room, and the kernel writes no more
    let ret = unsafe { libc::preadv2(fd, iov, count, offset as libc::off_t, RWF_NOWAIT) };
    let errno = match ret < 0 {
        true => io::Error::last_os_error().raw_os_error().unwrap_or(0),
        false => 0,
    };
    (ret, errno)
}

/// Other kernels have no page-cache-only read, so every probe reports cold
#[cfg(not(target_os = "linux"))]
fn nowait_preadv(
    _fd: RawFd,
    _iov: *const libc::iovec,
    _count: c_int,
    _offset: u64,
) -> (libc::ssize_t, c_int) {
    (-1, libc::EAGAIN)
}

/// The outcome of one non-blocking page cache probe
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WarmVerdict {
    /// The cache held the whole read, and this is what it filled
    Warm(usize),

    /// The pages are not all resident, so the read goes to the device
    Cold,

    /// The kernel does not know the flag, so the probe retires
    Retire,

    /// The read failed for a reason of its own
    Failed,
}

/// Classify a probe's return and errno
fn warm_verdict(ret: libc::ssize_t, errno: c_int, wanted: usize, is_proven: bool) -> WarmVerdict {
    if ret >= 0 {
        let filled = ret as usize;
        return match filled == wanted {
            true => WarmVerdict::Warm(filled),
            false => WarmVerdict::Cold,
        };
    }
    match errno {
        libc::EAGAIN => WarmVerdict::Cold,
        libc::EOPNOTSUPP | libc::EINVAL if !is_proven => WarmVerdict::Retire,
        _ => WarmVerdict::Failed,
    }
}

/// An awaited point read longer than this skips the probe
const POINT_PROBE_MAX: usize = 1024 * 1024;

/// Whether an awaited point read may still ask the page cache before it queues
#[derive(Debug)]
struct WarmReads {
    /// Whether the probe may still be issued at all
    is_live: AtomicBool,

    /// Whether any probe has answered, after which a refusal counts as a read error
    is_proven: AtomicBool,
}

impl WarmReads {
    fn new() -> WarmReads {
        WarmReads {
            is_live: AtomicBool::new(true),
            is_proven: AtomicBool::new(false),
        }
    }

    fn is_live(&self) -> bool {
        self.is_live.load(Ordering::Relaxed)
    }

    fn retire(&self, errno: c_int) {
        self.is_live.store(false, Ordering::Relaxed);
        let error = io::Error::from_raw_os_error(errno);
        tracing::warn!("reel point reads stop asking the page cache first: {error}");
    }

    /// Fill both buffers from resident pages, committing only if the whole record came
    fn warm_split(&self, fd: RawFd, offset: u64, head: &mut ReadBuf, body: &mut ReadBuf) -> bool {
        let (head_ptr, head_len) = head.as_mut_ptr();
        let (body_ptr, body_len) = body.as_mut_ptr();
        let iovecs = [
            libc::iovec {
                iov_base: head_ptr as *mut c_void,
                iov_len: head_len,
            },
            libc::iovec {
                iov_base: body_ptr as *mut c_void,
                iov_len: body_len,
            },
        ];
        let wanted = head_len + body_len;
        let (ret, errno) = nowait_preadv(fd, iovecs.as_ptr(), iovecs.len() as c_int, offset);
        match warm_verdict(ret, errno, wanted, self.is_proven.load(Ordering::Relaxed)) {
            WarmVerdict::Warm(_) => {
                prove(&self.is_proven);
                // Safety: the kernel reported filling both buffers whole
                unsafe {
                    head.commit(head_len);
                    body.commit(body_len);
                }
                true
            }
            WarmVerdict::Cold => {
                prove(&self.is_proven);
                false
            }
            WarmVerdict::Retire => {
                self.retire(errno);
                false
            }
            WarmVerdict::Failed => false,
        }
    }
}

/// Counts of the awaited point path's warm probes
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WarmReadCounts {
    /// Awaited point reads that asked the page cache before queueing anything
    pub asked: u64,

    /// Reads the cache answered whole, with no op queued
    pub served: u64,
}

/// The counts behind WarmReadCounts
#[derive(Debug, Default)]
struct WarmCounts {
    asked: AtomicU64,
    served: AtomicU64,
}

impl WarmCounts {
    fn snapshot(&self) -> WarmReadCounts {
        WarmReadCounts {
            asked: self.asked.load(Ordering::Relaxed),
            served: self.served.load(Ordering::Relaxed),
        }
    }
}

/// Synchronous POSIX backend executing ops at submit and queuing completions
#[derive(Debug)]
pub struct PosixBackend {
    files: RwLock<HashMap<FileId, OwnedFd>>,
    id: u64,
    closes: AtomicU64,
    next_file_id: AtomicU64,
    completions: Mutex<VecDeque<Completion>>,
    ops: AtomicU64,
    sync_count: AtomicU64,
    sync_nanos: AtomicU64,
    warm: WarmReads,
    warm_counts: WarmCounts,
    is_direct: bool,
}

impl PosixBackend {
    /// Build a posix backend over buffered descriptors
    pub fn new() -> PosixBackend {
        PosixBackend::with_direct(false)
    }

    /// Build a backend whose opens bypass the page cache on Linux when `is_direct` is set
    pub fn with_direct(is_direct: bool) -> PosixBackend {
        PosixBackend {
            files: RwLock::new(HashMap::new()),
            id: NEXT_BACKEND.fetch_add(1, Ordering::Relaxed),
            closes: AtomicU64::new(0),
            next_file_id: AtomicU64::new(0),
            completions: Mutex::new(VecDeque::new()),
            ops: AtomicU64::new(0),
            sync_count: AtomicU64::new(0),
            sync_nanos: AtomicU64::new(0),
            warm: WarmReads::new(),
            warm_counts: WarmCounts::default(),
            is_direct,
        }
    }

    /// Counts of the awaited point path's warm probes so far
    pub fn warm_reads(&self) -> WarmReadCounts {
        self.warm_counts.snapshot()
    }

    /// Answer a whole framed record from resident pages, or leave it to the driver
    pub fn warm_split(
        &self,
        file: FileId,
        offset: u64,
        head: &mut ReadBuf,
        body: &mut ReadBuf,
    ) -> bool {
        if self.is_direct || !self.warm.is_live() {
            return false;
        }
        if head.wanted() + body.wanted() > POINT_PROBE_MAX {
            return false;
        }
        let Ok(fd) = self.fd_of(file) else {
            return false;
        };
        self.warm_counts.asked.fetch_add(1, Ordering::Relaxed);
        let served = self.warm.warm_split(fd, offset, head, body);
        if served {
            self.warm_counts.served.fetch_add(1, Ordering::Relaxed);
        }
        served
    }

    /// Descriptors this backend currently holds open
    pub fn open_file_count(&self) -> usize {
        self.read_files().len()
    }

    /// Ops this backend has executed, whichever door they arrived through
    pub fn ops(&self) -> u64 {
        self.ops.load(Ordering::Relaxed)
    }

    /// Device flushes this backend has asked for
    pub fn sync_count(&self) -> u64 {
        self.sync_count.load(Ordering::Relaxed)
    }

    /// Nanoseconds spent inside those flushes, waiting on the drive
    pub fn sync_nanos(&self) -> u64 {
        self.sync_nanos.load(Ordering::Relaxed)
    }

    /// Run one flush, counting it and the time it took
    fn billed_sync(&self, run: impl FnOnce() -> Result<()>) -> Result<()> {
        let started = Instant::now();
        let outcome = run();
        let nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.sync_count.fetch_add(1, Ordering::Relaxed);
        self.sync_nanos.fetch_add(nanos, Ordering::Relaxed);
        outcome
    }

    /// Run one op on the calling thread and answer it, for a backend sharing this table
    pub fn dispatch(&self, op: Op) -> Completion {
        self.ops.fetch_add(1, Ordering::Relaxed);
        self.execute(op)
    }

    fn execute(&self, op: Op) -> Completion {
        match op {
            Op::Open { tag, path, create } => {
                let mut options = OpenOptions::new();
                options.read(true).write(true).create(create);
                if self.is_direct {
                    direct_open_flag(&mut options);
                }
                let outcome = match options.open(&path) {
                    Ok(file) => {
                        let id = FileId(self.next_file_id.fetch_add(1, Ordering::Relaxed));
                        self.write_files().insert(id, OwnedFd::from(file));
                        Outcome::Opened(Ok(id))
                    }
                    Err(error) => Outcome::Opened(Err(ReelError::Io(error))),
                };
                Completion { tag, outcome }
            }
            Op::Writev {
                tag,
                file,
                offset,
                bufs,
            } => {
                let result = match self.fd_of(file) {
                    Ok(fd) if self.is_direct => direct_writev(fd, &bufs, offset),
                    Ok(fd) => pwritev_all(fd, &bufs, offset),
                    Err(error) => Err(error),
                };
                Completion {
                    tag,
                    outcome: Outcome::Wrote { result, bufs },
                }
            }
            Op::Pread {
                tag,
                file,
                offset,
                mut buf,
            } => {
                let result = match self.fd_of(file) {
                    Ok(fd) if self.is_direct => direct_pread(fd, &mut buf, offset),
                    Ok(fd) => pread_into(fd, &mut buf, offset),
                    Err(error) => Err(error),
                };
                Completion {
                    tag,
                    outcome: Outcome::Read { result, buf },
                }
            }
            Op::PreadSplit {
                tag,
                file,
                offset,
                mut head,
                mut body,
            } => {
                let result = match self.fd_of(file) {
                    Ok(fd) if self.is_direct => {
                        direct_pread_split(fd, &mut head, &mut body, offset)
                    }
                    Ok(fd) => preadv_into(fd, &mut head, &mut body, offset),
                    Err(error) => Err(error),
                };
                Completion {
                    tag,
                    outcome: Outcome::ReadSplit { result, head, body },
                }
            }
            Op::SyncData { tag, file } => Completion {
                tag,
                outcome: Outcome::Done(self.billed_sync(|| self.sync_data(file))),
            },
            Op::SyncFull { tag, file } => Completion {
                tag,
                outcome: Outcome::Done(self.billed_sync(|| self.sync_full(file))),
            },
            Op::SyncRange {
                tag,
                file,
                offset,
                len,
                mode,
            } => Completion {
                tag,
                outcome: Outcome::Done(self.sync_range(file, offset, len, mode)),
            },
            Op::SyncDir { tag, dir } => Completion {
                tag,
                outcome: Outcome::Done(sync_dir(&dir)),
            },
            Op::Rename { tag, from, to } => Completion {
                tag,
                outcome: Outcome::Done(fs::rename(&from, &to).map_err(ReelError::Io)),
            },
            Op::Unlink { tag, path } => Completion {
                tag,
                outcome: Outcome::Done(fs::remove_file(&path).map_err(ReelError::Io)),
            },
            Op::Close { tag, file } => Completion {
                tag,
                // Dropping the descriptor closes it and frees an unlinked file's blocks
                outcome: Outcome::Done(match self.close_file(file) {
                    Some(_) => Ok(()),
                    None => Err(unknown_file()),
                }),
            },
            Op::List { tag, dir } => Completion {
                tag,
                outcome: Outcome::Listed(list_dir(&dir)),
            },
            Op::Length { tag, file } => Completion {
                tag,
                outcome: Outcome::Length(self.length(file)),
            },
            Op::Allocate {
                tag,
                file,
                offset,
                len,
            } => Completion {
                tag,
                outcome: Outcome::Done(self.allocate(file, offset, len)),
            },
            Op::Release {
                tag,
                file,
                offset,
                len,
            } => Completion {
                tag,
                outcome: Outcome::Done(self.release(file, offset, len)),
            },
            Op::Truncate { tag, file, len } => Completion {
                tag,
                outcome: Outcome::Done(self.truncate(file, len)),
            },
            Op::Advise {
                tag,
                file,
                offset,
                len,
                advice,
            } => Completion {
                tag,
                outcome: Outcome::Done(self.advise(file, offset, len, advice)),
            },
        }
    }

    fn sync_data(&self, file: FileId) -> Result<()> {
        let fd = self.fd_of(file)?;
        raw_sync_data(fd)
    }

    fn sync_full(&self, file: FileId) -> Result<()> {
        let fd = self.fd_of(file)?;
        raw_sync_full(fd)
    }

    fn sync_range(&self, file: FileId, offset: u64, len: u64, mode: SyncRangeMode) -> Result<()> {
        let fd = self.fd_of(file)?;
        raw_sync_range(fd, offset, len, mode)
    }

    fn allocate(&self, file: FileId, offset: u64, len: u64) -> Result<()> {
        let fd = self.fd_of(file)?;
        raw_allocate(fd, offset, len)
    }

    fn release(&self, file: FileId, offset: u64, len: u64) -> Result<()> {
        let fd = self.fd_of(file)?;
        raw_release(fd, offset, len)
    }

    fn truncate(&self, file: FileId, len: u64) -> Result<()> {
        let fd = self.fd_of(file)?;
        truncate_to(fd, len)
    }

    fn length(&self, file: FileId) -> Result<u64> {
        let fd = self.fd_of(file)?;
        raw_length(fd)
    }

    fn advise(&self, file: FileId, offset: u64, len: u64, advice: Advice) -> Result<()> {
        let fd = self.fd_of(file)?;
        raw_advise(fd, offset, len, advice)
    }

    /// The descriptor behind a handle, from a per-thread cache checked against the close count
    pub(crate) fn fd_of(&self, file: FileId) -> Result<RawFd> {
        let generation = self.closes.load(Ordering::Relaxed);
        let way = resolved_way(self.id, file);
        if let Some(fd) = RESOLVED.with(|held| held[way].get().hit(self.id, file, generation)) {
            return Ok(fd);
        }
        let files = self.read_files();
        match files.get(&file) {
            Some(handle) => {
                let fd = handle.as_raw_fd();
                RESOLVED.with(|held| {
                    held[way].set(Resolved {
                        backend: self.id,
                        file,
                        fd,
                        generation,
                    })
                });
                Ok(fd)
            }
            None => Err(unknown_file()),
        }
    }

    /// How many files this backend has closed, which is what stales a resolved fd
    pub fn close_generation(&self) -> u64 {
        self.closes.load(Ordering::Relaxed)
    }

    fn read_files(&self) -> RwLockReadGuard<'_, HashMap<FileId, OwnedFd>> {
        read(&self.files)
    }

    fn write_files(&self) -> RwLockWriteGuard<'_, HashMap<FileId, OwnedFd>> {
        write(&self.files)
    }

    /// Remove a file and stale every cached fd, bumping the count before the fd closes
    fn close_file(&self, file: FileId) -> Option<OwnedFd> {
        let taken = self.write_files().remove(&file);
        self.closes.fetch_add(1, Ordering::AcqRel);
        taken
    }

    fn lock_completions(&self) -> MutexGuard<'_, VecDeque<Completion>> {
        lock(&self.completions)
    }
}

impl Default for PosixBackend {
    fn default() -> PosixBackend {
        PosixBackend::new()
    }
}

impl ReelIo for PosixBackend {
    /// Posix, direct when this backend was built for direct opens
    fn serving(&self) -> ServingBackend {
        match self.is_direct {
            true => ServingBackend::PosixDirect,
            false => ServingBackend::Posix,
        }
    }

    fn sync_count(&self) -> u64 {
        PosixBackend::sync_count(self)
    }

    fn sync_nanos(&self) -> u64 {
        PosixBackend::sync_nanos(self)
    }

    fn warm_split(
        &self,
        file: FileId,
        offset: u64,
        head: &mut ReadBuf,
        body: &mut ReadBuf,
    ) -> bool {
        PosixBackend::warm_split(self, file, offset, head, body)
    }

    fn submit(&self, ops: Vec<Op>) -> Result<()> {
        let mut done = Vec::with_capacity(ops.len());
        for op in ops {
            done.push(self.dispatch(op));
        }
        let mut queue = self.lock_completions();
        for completion in done {
            queue.push_back(completion);
        }
        Ok(())
    }

    fn submit_batch(&self, ops: &mut Vec<Op>, out: &mut Vec<Completion>) -> bool {
        out.reserve(ops.len());
        for op in ops.drain(..) {
            out.push(self.dispatch(op));
        }
        true
    }

    fn submit_inline(&self, op: Op) -> std::result::Result<Completion, Op> {
        Ok(self.dispatch(op))
    }

    fn poll(&self, out: &mut Vec<Completion>) -> Result<usize> {
        let mut queue = self.lock_completions();
        let drained = queue.len();
        while let Some(completion) = queue.pop_front() {
            out.push(completion);
        }
        Ok(drained)
    }
}

fn unknown_file() -> ReelError {
    ReelError::Io(io::Error::new(
        io::ErrorKind::NotFound,
        "unknown reel file handle",
    ))
}

fn checked(ret: c_int) -> Result<()> {
    if ret == -1 {
        Err(ReelError::Io(io::Error::last_os_error()))
    } else {
        Ok(())
    }
}

fn truncate_to(fd: RawFd, length: u64) -> Result<()> {
    checked(unsafe { libc::ftruncate(fd, length as libc::off_t) })
}

/// Open with O_DIRECT so the page cache is bypassed, on Linux only
#[cfg(target_os = "linux")]
fn direct_open_flag(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.custom_flags(libc::O_DIRECT);
}

#[cfg(not(target_os = "linux"))]
fn direct_open_flag(_options: &mut OpenOptions) {}

/// Write a drain through one aligned buffer, since the device takes whole blocks
fn direct_writev(fd: RawFd, bufs: &[WriteBuf], offset: u64) -> Result<u64> {
    let total: usize = bufs.iter().map(|buf| buf.as_slice().len()).sum();
    if total == 0 {
        return Ok(0);
    }
    if !offset.is_multiple_of(DIRECT_ALIGN as u64) {
        return Err(ReelError::Backend(format!(
            "a direct write starts at {offset}, which is not a block boundary",
        )));
    }

    let mut staged = AlignedBuf::uninit(total)?;
    let mut at = 0usize;
    for buf in bufs {
        let bytes = buf.as_slice();
        staged.as_mut_slice()[at..at + bytes.len()].copy_from_slice(bytes);
        at += bytes.len();
    }
    // Zero the rounding tail, since it reaches the device too
    staged.zero_from(at);

    let span = staged.len();
    let wrote = write_all_at(fd, staged.as_ptr(), span, offset)?;
    // Report the caller's framed length, without the rounding
    Ok(wrote.min(total as u64))
}

/// Issue one direct write, resuming a short one from where it stopped
fn write_all_at(fd: RawFd, ptr: *mut u8, len: usize, offset: u64) -> Result<u64> {
    let mut written = 0usize;
    while written < len {
        let at = offset.saturating_add(written as u64) as libc::off_t;
        // Safety: written stays inside the len-byte allocation, which is owned and initialized
        let ret = unsafe { libc::pwrite(fd, ptr.add(written) as *const c_void, len - written, at) };
        if ret < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(ReelError::Io(error));
        }
        if ret == 0 {
            return Err(ReelError::Io(io::Error::new(
                io::ErrorKind::WriteZero,
                "a direct write made no progress",
            )));
        }
        written += ret as usize;
    }
    Ok(written as u64)
}

thread_local! {
    /// This thread's aligned staging buffer for direct reads, allocated once at full size
    static STAGE: RefCell<Option<AlignedBuf>> = const { RefCell::new(None) };
}

#[cfg(test)]
thread_local! {
    /// Stage buffers this thread has allocated, which a test holds to one
    static STAGE_ALLOCS: Cell<usize> = const { Cell::new(0) };

    /// Bytes this thread's covering reads have asked the kernel to fill
    static COVERING_BYTES: Cell<u64> = const { Cell::new(0) };
}

/// Stage buffers this thread has allocated since it started
#[cfg(test)]
fn stage_allocations() -> usize {
    STAGE_ALLOCS.with(|count| count.get())
}

/// Bytes this thread's covering reads have asked the kernel for since it started
#[cfg(test)]
fn covering_bytes() -> u64 {
    COVERING_BYTES.with(|count| count.get())
}

/// Run something against an aligned buffer wide enough for a span, pooled up to the cap
fn with_stage<Answer>(
    span: usize,
    run: impl FnOnce(&AlignedBuf) -> Result<Answer>,
) -> Result<Answer> {
    if span > STAGE_BYTES {
        let staged = AlignedBuf::uninit(span)?;
        return run(&staged);
    }
    STAGE.with(|held| {
        let mut slot = held.borrow_mut();
        if slot.is_none() {
            *slot = Some(AlignedBuf::uninit(STAGE_BYTES)?);
            #[cfg(test)]
            STAGE_ALLOCS.with(|count| count.set(count.get() + 1));
        }
        run(slot.as_ref().expect("the stage was filled above"))
    })
}

/// Read an unaligned range through the blocks around it and hand the window to `cut`
fn read_covering(
    fd: RawFd,
    offset: u64,
    len: u64,
    cut: impl FnOnce(&[u8]) -> usize,
) -> Result<usize> {
    let (start, span) = covering_span(offset, len);
    let span = span as usize;
    let skip = (offset - start) as usize;
    with_stage(span, |staged| {
        // Read only the span, since the pooled stage is as wide as the cap
        let filled = read_at(fd, staged.as_ptr(), span, start)?;
        let (from, to) = wanted_window(filled, skip, len as usize);
        // Safety: the read reported filling at least this many bytes from the start
        let bytes = unsafe { staged.filled(to) };
        Ok(cut(&bytes[from..]))
    })
}

/// Issue one direct read, treating a stop inside a block as the end of the file
fn read_at(fd: RawFd, ptr: *mut u8, len: usize, offset: u64) -> Result<usize> {
    #[cfg(test)]
    COVERING_BYTES.with(|count| count.set(count.get() + len as u64));
    let mut filled = 0usize;
    while filled < len {
        let at = offset.saturating_add(filled as u64) as libc::off_t;
        // Safety: as in write_all_at, the range is inside the owned allocation
        let ret = unsafe { libc::pread(fd, ptr.add(filled) as *mut c_void, len - filled, at) };
        if ret < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(ReelError::Io(error));
        }
        if ret == 0 {
            break;
        }
        filled += ret as usize;
        if !filled.is_multiple_of(DIRECT_ALIGN) {
            break;
        }
    }
    Ok(filled)
}

/// Fill one buffer from a direct read of the blocks that contain its range
fn direct_pread(fd: RawFd, buf: &mut ReadBuf, offset: u64) -> Result<usize> {
    let wanted = buf.wanted() as u64;
    if wanted == 0 {
        return Ok(0);
    }
    read_covering(fd, offset, wanted, |bytes| cut_into(bytes, buf))
}

/// Fill a header and a payload buffer from one direct read of their shared range
fn direct_pread_split(
    fd: RawFd,
    head: &mut ReadBuf,
    body: &mut ReadBuf,
    offset: u64,
) -> Result<usize> {
    let wanted = (head.wanted() + body.wanted()) as u64;
    if wanted == 0 {
        return Ok(0);
    }
    read_covering(fd, offset, wanted, |bytes| {
        cut_split_into(bytes, head, body)
    })
}

/// Write every buffer at the offset, however many calls it takes, or return an error
fn pwritev_all(fd: RawFd, bufs: &[WriteBuf], offset: u64) -> Result<u64> {
    let mut iovecs = IOVEC_SCRATCH.with(|held| held.take());
    let written = pwritev_all_into(&mut iovecs, fd, bufs, offset);
    IOVEC_SCRATCH.with(|held| held.set(iovecs));
    written
}

thread_local! {
    /// The iovec list this thread reuses for vectored writes
    static IOVEC_SCRATCH: Cell<Vec<libc::iovec>> = const { Cell::new(Vec::new()) };
}

/// Each thread caches descriptors for this many files at once
const RESOLVED_WAYS: usize = 4;

thread_local! {
    /// This thread's recently resolved descriptors, indexed by way
    static RESOLVED: [Cell<Resolved>; RESOLVED_WAYS] =
        const { [const { Cell::new(Resolved::none()) }; RESOLVED_WAYS] };
}

/// The next backend's id, so a cached descriptor records which backend owns it
static NEXT_BACKEND: AtomicU64 = AtomicU64::new(0);

/// A file's cache way, mixed with the backend id since every backend numbers files from zero
fn resolved_way(backend: u64, file: FileId) -> usize {
    (backend ^ file.0) as usize % RESOLVED_WAYS
}

/// One cached descriptor for a file
#[derive(Clone, Copy)]
struct Resolved {
    backend: u64,
    file: FileId,
    fd: RawFd,
    generation: u64,
}

impl Resolved {
    /// An empty entry, whose all-ones generation never matches a real close count
    const fn none() -> Resolved {
        Resolved {
            backend: u64::MAX,
            file: FileId(0),
            fd: -1,
            generation: u64::MAX,
        }
    }

    /// The descriptor, if this entry is for this file and nothing has closed since
    fn hit(self, backend: u64, file: FileId, generation: u64) -> Option<RawFd> {
        (self.backend == backend && self.file == file && self.generation == generation)
            .then_some(self.fd)
    }
}

/// The vectored write itself, using the iovec list it is handed
fn pwritev_all_into(
    iovecs: &mut Vec<libc::iovec>,
    fd: RawFd,
    bufs: &[WriteBuf],
    offset: u64,
) -> Result<u64> {
    let mut cursor = 0usize;
    let mut consumed = 0usize;
    let mut written = 0u64;

    skip_spent(bufs, &mut cursor, &mut consumed);
    while cursor < bufs.len() {
        iovecs.clear();
        let mut scan = cursor;
        while scan < bufs.len() && iovecs.len() < MAX_IOVECS {
            let slice = bufs[scan].as_slice();
            let bytes = if scan == cursor {
                &slice[consumed..]
            } else {
                slice
            };
            if !bytes.is_empty() {
                iovecs.push(libc::iovec {
                    iov_base: bytes.as_ptr() as *mut c_void,
                    iov_len: bytes.len(),
                });
            }
            scan += 1;
        }
        if iovecs.is_empty() {
            break;
        }

        let count = c_int::try_from(iovecs.len()).unwrap_or(c_int::MAX);
        let at = offset.saturating_add(written) as libc::off_t;
        // Safety: the list holds count owned buffers, and the kernel reads no more
        let ret = unsafe { libc::pwritev(fd, iovecs.as_ptr(), count, at) };
        if ret < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(ReelError::Io(error));
        }
        if ret == 0 {
            return Err(ReelError::Io(io::Error::new(
                io::ErrorKind::WriteZero,
                "vectored write made no progress",
            )));
        }

        written += ret as u64;
        let mut advance = ret as usize;
        while advance > 0 && cursor < bufs.len() {
            let remaining = bufs[cursor].len() - consumed;
            if advance >= remaining {
                advance -= remaining;
                cursor += 1;
                consumed = 0;
            } else {
                consumed += advance;
                advance = 0;
            }
        }
        skip_spent(bufs, &mut cursor, &mut consumed);
    }
    Ok(written)
}

/// Step past buffers with nothing left to write, so the write head always moves
fn skip_spent(bufs: &[WriteBuf], cursor: &mut usize, consumed: &mut usize) {
    while *cursor < bufs.len() && bufs[*cursor].len() == *consumed {
        *cursor += 1;
        *consumed = 0;
    }
}

/// Read into a buffer's room, looping on short reads until one returns nothing
fn pread_into(fd: RawFd, buf: &mut ReadBuf, offset: u64) -> Result<usize> {
    let (base, wanted) = buf.as_mut_ptr();
    let mut filled = 0usize;
    while filled < wanted {
        // Safety: filled never passes the room, so the pointer stays in the buffer
        let ptr = unsafe { base.add(filled) };
        let read = one_pread(fd, ptr, wanted - filled, offset + filled as u64)?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    unsafe { buf.commit(filled) };
    Ok(filled)
}

/// One read call at an offset, without moving the descriptor's own cursor
fn one_pread(fd: RawFd, ptr: *mut u8, len: usize, offset: u64) -> Result<usize> {
    // Safety: the caller's buffer owns this room, and the kernel writes no more than len
    let ret = unsafe { libc::pread(fd, ptr as *mut c_void, len, offset as libc::off_t) };
    if ret < 0 {
        return Err(ReelError::Io(io::Error::last_os_error()));
    }
    Ok(ret as usize)
}

/// Read one contiguous range into two buffers in a single call
fn preadv_into(fd: RawFd, head: &mut ReadBuf, body: &mut ReadBuf, offset: u64) -> Result<usize> {
    let (head_ptr, head_len) = head.as_mut_ptr();
    let (body_ptr, body_len) = body.as_mut_ptr();
    let iovecs = [
        libc::iovec {
            iov_base: head_ptr as *mut c_void,
            iov_len: head_len,
        },
        libc::iovec {
            iov_base: body_ptr as *mut c_void,
            iov_len: body_len,
        },
    ];
    let count = iovecs.len() as c_int;
    let at = offset as libc::off_t;
    // Safety: the list holds two buffers with owned room, and the kernel writes no more
    let ret = unsafe { libc::preadv(fd, iovecs.as_ptr(), count, at) };
    if ret < 0 {
        return Err(ReelError::Io(io::Error::last_os_error()));
    }
    // The kernel fills the head first, so a short read cuts the body first
    let filled = ret as usize;
    unsafe {
        head.commit(filled.min(head_len));
        body.commit(filled.saturating_sub(head_len));
    }
    Ok(filled)
}

fn raw_length(fd: RawFd) -> Result<u64> {
    let mut status = unsafe { std::mem::zeroed::<libc::stat>() };
    checked(unsafe { libc::fstat(fd, &mut status) })?;
    Ok(status.st_size as u64)
}

fn sync_dir(dir: &Path) -> Result<()> {
    let handle = OpenOptions::new().read(true).open(dir)?;
    checked(unsafe { libc::fsync(handle.as_raw_fd()) })
}

fn list_dir(dir: &Path) -> Result<Vec<SegmentEntry>> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_file() {
            let name = entry.file_name().to_string_lossy().into_owned();
            entries.push(SegmentEntry {
                name,
                len: metadata.len(),
            });
        }
    }
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(entries)
}

#[cfg(target_os = "linux")]
fn raw_sync_data(fd: RawFd) -> Result<()> {
    checked(unsafe { libc::fdatasync(fd) })
}

#[cfg(not(target_os = "linux"))]
fn raw_sync_data(fd: RawFd) -> Result<()> {
    checked(unsafe { libc::fsync(fd) })
}

#[cfg(target_os = "linux")]
fn raw_sync_full(fd: RawFd) -> Result<()> {
    checked(unsafe { libc::fsync(fd) })
}

#[cfg(not(target_os = "linux"))]
fn raw_sync_full(fd: RawFd) -> Result<()> {
    checked(unsafe { libc::fsync(fd) })
}

#[cfg(target_os = "linux")]
fn raw_sync_range(fd: RawFd, offset: u64, len: u64, mode: SyncRangeMode) -> Result<()> {
    let flags = match mode {
        SyncRangeMode::Write => libc::SYNC_FILE_RANGE_WRITE,
        SyncRangeMode::WaitBeforeWriteWaitAfter => {
            libc::SYNC_FILE_RANGE_WAIT_BEFORE
                | libc::SYNC_FILE_RANGE_WRITE
                | libc::SYNC_FILE_RANGE_WAIT_AFTER
        }
    };
    checked(unsafe {
        libc::sync_file_range(fd, offset as libc::off64_t, len as libc::off64_t, flags)
    })
}

#[cfg(not(target_os = "linux"))]
fn raw_sync_range(_fd: RawFd, _offset: u64, _len: u64, _mode: SyncRangeMode) -> Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn raw_allocate(fd: RawFd, offset: u64, len: u64) -> Result<()> {
    // Reserve blocks but keep the length. Without support, writes allocate as they go
    let ret = unsafe {
        libc::fallocate(
            fd,
            libc::FALLOC_FL_KEEP_SIZE,
            offset as libc::off_t,
            len as libc::off_t,
        )
    };
    if ret == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(code) if code == libc::ENOSYS || code == libc::EOPNOTSUPP => Ok(()),
        _ => Err(ReelError::Io(error)),
    }
}

/// Reserve a byte range on macOS, where F_PEOFPOSMODE takes a length past the end of file
#[cfg(target_os = "macos")]
fn raw_allocate(fd: RawFd, offset: u64, len: u64) -> Result<()> {
    let end = offset.saturating_add(len);
    // Measure in allocated blocks, since the length stays at the last written byte
    let mut status = unsafe { std::mem::zeroed::<libc::stat>() };
    checked(unsafe { libc::fstat(fd, &mut status) })?;
    let held = status.st_blocks as u64 * 512;
    if let Some(shortfall) = end.checked_sub(held).filter(|wanted| *wanted > 0) {
        let mut store = libc::fstore_t {
            fst_flags: libc::F_ALLOCATECONTIG,
            fst_posmode: libc::F_PEOFPOSMODE,
            fst_offset: 0,
            fst_length: shortfall as libc::off_t,
            fst_bytesalloc: 0,
        };
        let mut reserved = unsafe { libc::fcntl(fd, libc::F_PREALLOCATE, &mut store) };
        if reserved == -1 {
            store.fst_flags = libc::F_ALLOCATEALL;
            reserved = unsafe { libc::fcntl(fd, libc::F_PREALLOCATE, &mut store) };
        }
        let _ = reserved;
    }
    // The length stays at the last written byte, and the reservation lives past it
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn raw_allocate(fd: RawFd, offset: u64, len: u64) -> Result<()> {
    extend_to(fd, offset.saturating_add(len))
}

/// Punch a hole over a byte range and keep the length, or do nothing if unsupported
#[cfg(target_os = "linux")]
fn raw_release(fd: RawFd, offset: u64, len: u64) -> Result<()> {
    let mode = libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE;
    let ret = unsafe { libc::fallocate(fd, mode, offset as libc::off_t, len as libc::off_t) };
    if ret == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(code) if code == libc::ENOSYS || code == libc::EOPNOTSUPP => Ok(()),
        _ => Err(ReelError::Io(error)),
    }
}

/// Punch a hole over a byte range on macOS and keep the length, or do nothing if unsupported
#[cfg(target_os = "macos")]
fn raw_release(fd: RawFd, offset: u64, len: u64) -> Result<()> {
    let hole = libc::fpunchhole_t {
        fp_flags: 0,
        reserved: 0,
        fp_offset: offset as libc::off_t,
        fp_length: len as libc::off_t,
    };
    if unsafe { libc::fcntl(fd, libc::F_PUNCHHOLE, &hole) } == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(code) if code == libc::ENOTSUP || code == libc::EINVAL => Ok(()),
        _ => Err(ReelError::Io(error)),
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn raw_release(_fd: RawFd, _offset: u64, _len: u64) -> Result<()> {
    Ok(())
}

/// Pass the hint to posix_fadvise for the range
#[cfg(target_os = "linux")]
fn raw_advise(fd: RawFd, offset: u64, len: u64, advice: Advice) -> Result<()> {
    let flag = match advice {
        Advice::Sequential => libc::POSIX_FADV_SEQUENTIAL,
        Advice::Random => libc::POSIX_FADV_RANDOM,
        Advice::DontNeed => libc::POSIX_FADV_DONTNEED,
    };
    let ret = unsafe { libc::posix_fadvise(fd, offset as libc::off_t, len as libc::off_t, flag) };
    if ret != 0 {
        Err(ReelError::Io(io::Error::from_raw_os_error(ret)))
    } else {
        Ok(())
    }
}

/// Map the hint to per-descriptor read-ahead on macOS, dropping DontNeed
#[cfg(target_os = "macos")]
fn raw_advise(fd: RawFd, _offset: u64, _len: u64, advice: Advice) -> Result<()> {
    let (command, argument) = match advice {
        Advice::Sequential => (libc::F_RDAHEAD, 1),
        Advice::Random => (libc::F_RDAHEAD, 0),
        Advice::DontNeed => return Ok(()),
    };
    checked(unsafe { libc::fcntl(fd, command, argument as c_int) })
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn raw_advise(_fd: RawFd, _offset: u64, _len: u64, _advice: Advice) -> Result<()> {
    Ok(())
}

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;
    use crate::io::op::{OwnedBuf, Tag};
    use tempfile::tempdir;

    fn drain_one(backend: &PosixBackend) -> Completion {
        let mut out = Vec::new();
        let count = backend.poll(&mut out).expect("poll");
        assert_eq!(count, 1);
        out.pop().expect("one completion")
    }

    fn opened(outcome: Outcome) -> Result<FileId> {
        match outcome {
            Outcome::Opened(result) => result,
            _ => Err(ReelError::Backend("expected opened".to_string())),
        }
    }

    fn wrote(outcome: Outcome) -> Result<u64> {
        match outcome {
            Outcome::Wrote { result, .. } => result,
            _ => Err(ReelError::Backend("expected wrote".to_string())),
        }
    }

    fn read_bytes(outcome: Outcome) -> (Result<usize>, OwnedBuf) {
        match outcome {
            Outcome::Read { result, buf } => (result, buf.into_vec()),
            _ => (
                Err(ReelError::Backend("expected read".to_string())),
                Vec::new(),
            ),
        }
    }

    fn listed(outcome: Outcome) -> Result<Vec<SegmentEntry>> {
        match outcome {
            Outcome::Listed(result) => result,
            _ => Err(ReelError::Backend("expected listed".to_string())),
        }
    }

    fn done(outcome: Outcome) -> Result<()> {
        match outcome {
            Outcome::Done(result) => result,
            _ => Err(ReelError::Backend("expected done".to_string())),
        }
    }

    fn open_file(backend: &PosixBackend, path: &Path, create: bool) -> FileId {
        backend
            .submit(vec![Op::Open {
                tag: Tag(1),
                path: path.to_path_buf(),
                create,
            }])
            .expect("submit open");
        opened(drain_one(backend).outcome).expect("open result")
    }

    // a closed file's cached descriptor stops answering
    #[test]
    fn closing_a_file_stales_the_cached_descriptor() {
        let dir = tempdir().expect("tempdir");
        let backend = PosixBackend::new();

        let first = open_file(&backend, &dir.path().join("segment-0"), true);
        backend
            .submit(vec![Op::Writev {
                tag: Tag(2),
                file: first,
                offset: 0,
                bufs: vec![WriteBuf::owned(b"first".to_vec())],
            }])
            .expect("submit write");
        assert_eq!(wrote(drain_one(&backend).outcome).expect("wrote"), 5);
        // Resolve it so this thread caches its descriptor
        assert!(backend.fd_of(first).is_ok());

        backend
            .submit(vec![Op::Close {
                tag: Tag(3),
                file: first,
            }])
            .expect("submit close");
        done(drain_one(&backend).outcome).expect("close");

        // The next open may reuse the fd number the close gave up
        let second = open_file(&backend, &dir.path().join("segment-1"), true);
        assert!(backend.fd_of(second).is_ok(), "the new file resolves");
        assert!(
            backend.fd_of(first).is_err(),
            "a closed file still answered with a descriptor"
        );
    }

    // one thread reading two volumes keeps their cached descriptors apart
    #[test]
    fn two_backends_do_not_share_a_cached_descriptor() {
        let dir = tempdir().expect("tempdir");
        let first = PosixBackend::new();
        let second = PosixBackend::new();

        let here = open_file(&first, &dir.path().join("segment-0"), true);
        let there = open_file(&second, &dir.path().join("segment-1"), true);
        assert_eq!(here, there, "both backends number their files from zero");

        let mine = first.fd_of(here).expect("the first backend's file");
        let yours = second.fd_of(there).expect("the second backend's file");
        assert_ne!(
            mine, yours,
            "one backend answered with the other's descriptor"
        );

        // Ask each again, so the cache holds both at once
        assert_eq!(first.fd_of(here).expect("still the first"), mine);
        assert_eq!(second.fd_of(there).expect("still the second"), yours);
    }

    // a direct backend's descriptors have O_DIRECT set, read back off the fd
    #[cfg(target_os = "linux")]
    #[test]
    fn direct_backend_opens_direct() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("segment-0");

        let buffered = PosixBackend::new();
        let file = open_file(&buffered, &path, true);
        let flags = unsafe { libc::fcntl(buffered.fd_of(file).expect("fd"), libc::F_GETFL) };
        assert_eq!(
            flags & libc::O_DIRECT,
            0,
            "a buffered open carried the direct flag"
        );

        let direct = PosixBackend::with_direct(true);
        let file = open_file(&direct, &path, true);
        let flags = unsafe { libc::fcntl(direct.fd_of(file).expect("fd"), libc::F_GETFL) };
        assert_ne!(
            flags & libc::O_DIRECT,
            0,
            "a direct open did not carry the flag"
        );
    }

    // a vectored write lands and reads back byte for byte
    #[test]
    fn write_then_read_roundtrip() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("segment-0");
        let backend = PosixBackend::new();
        let file = open_file(&backend, &path, true);

        backend
            .submit(vec![Op::Writev {
                tag: Tag(2),
                file,
                offset: 0,
                bufs: vec![
                    WriteBuf::owned(b"hello".to_vec()),
                    WriteBuf::owned(b" reel".to_vec()),
                ],
            }])
            .expect("submit write");
        assert_eq!(wrote(drain_one(&backend).outcome).expect("wrote"), 10);

        backend
            .submit(vec![Op::Pread {
                tag: Tag(3),
                file,
                offset: 0,
                buf: ReadBuf::new(10),
            }])
            .expect("submit read");
        let (result, buf) = read_bytes(drain_one(&backend).outcome);
        assert_eq!(result.expect("read"), 10);
        assert_eq!(&buf, b"hello reel");
    }

    // allocate reserves without touching the logical file size
    #[test]
    fn allocate_leaves_the_length_alone() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("segment-1");
        let backend = PosixBackend::new();
        let file = open_file(&backend, &path, true);

        backend
            .submit(vec![Op::Allocate {
                tag: Tag(2),
                file,
                offset: 0,
                len: 4096,
            }])
            .expect("submit allocate");
        done(drain_one(&backend).outcome).expect("allocate");

        backend
            .submit(vec![Op::List {
                tag: Tag(3),
                dir: dir.path().to_path_buf(),
            }])
            .expect("submit list");
        let entries = listed(drain_one(&backend).outcome).expect("list");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].len, 0, "a reservation extended the length");
    }

    // reserving a segment in pieces reserves about the segment's size
    #[test]
    fn extending_in_chunks_reserves_only_the_segment() {
        use std::os::fd::AsRawFd;

        const CHUNK: u64 = 64 * 1024;
        const CHUNKS: u64 = 16;

        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("segment-2");
        let backend = PosixBackend::new();
        let file = open_file(&backend, &path, true);

        for chunk in 0..CHUNKS {
            backend
                .submit(vec![Op::Allocate {
                    tag: Tag(2),
                    file,
                    offset: chunk * CHUNK,
                    len: CHUNK,
                }])
                .expect("submit allocate");
            done(drain_one(&backend).outcome).expect("allocate");
        }

        let logical = CHUNK * CHUNKS;
        let held = std::fs::File::open(&path).expect("open the segment");
        let mut status = unsafe { std::mem::zeroed::<libc::stat>() };
        assert_eq!(unsafe { libc::fstat(held.as_raw_fd(), &mut status) }, 0);
        assert_eq!(status.st_size, 0, "a reservation extended the length");

        // Blocks are 512-byte units, with slack for a filesystem that rounds up
        let blocks = status.st_blocks as u64 * 512;
        assert!(
            blocks <= logical * 2,
            "reserved {blocks} bytes of blocks for a {logical} byte segment taken in {CHUNKS} chunks"
        );
    }

    // data, full, and range syncs all report success on a written file
    #[test]
    fn syncs_report_success() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("segment-2");
        let backend = PosixBackend::new();
        let file = open_file(&backend, &path, true);

        backend
            .submit(vec![Op::Writev {
                tag: Tag(2),
                file,
                offset: 0,
                bufs: vec![WriteBuf::owned(vec![7; 4096])],
            }])
            .expect("submit write");
        wrote(drain_one(&backend).outcome).expect("wrote");

        backend
            .submit(vec![
                Op::SyncData { tag: Tag(3), file },
                Op::SyncRange {
                    tag: Tag(4),
                    file,
                    offset: 0,
                    len: 4096,
                    mode: SyncRangeMode::Write,
                },
                Op::SyncFull { tag: Tag(5), file },
            ])
            .expect("submit syncs");
        let mut out = Vec::new();
        backend.poll(&mut out).expect("poll");
        assert_eq!(out.len(), 3);
        for completion in out {
            done(completion.outcome).expect("sync ok");
        }
    }

    // a sync is counted and timed whatever the platform calls underneath
    #[test]
    fn sync_is_billed() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("segment-3");
        let backend = PosixBackend::new();
        let file = open_file(&backend, &path, true);
        assert_eq!(backend.sync_count(), 0);

        backend
            .submit(vec![Op::SyncFull { tag: Tag(2), file }])
            .expect("submit sync full");
        done(drain_one(&backend).outcome).expect("sync full");
        assert_eq!(backend.sync_count(), 1);
    }

    // advise returns success and is treated as a hint
    #[test]
    fn advise_ok() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("segment-4");
        let backend = PosixBackend::new();
        let file = open_file(&backend, &path, true);

        backend
            .submit(vec![Op::Advise {
                tag: Tag(2),
                file,
                offset: 0,
                len: 0,
                advice: Advice::DontNeed,
            }])
            .expect("submit advise");
        done(drain_one(&backend).outcome).expect("advise");
    }

    // rename moves a file and unlink removes it
    #[test]
    fn rename_then_unlink() {
        let dir = tempdir().expect("tempdir");
        let from = dir.path().join("segment-5");
        let to = dir.path().join("segment-5-renamed");
        let backend = PosixBackend::new();
        open_file(&backend, &from, true);

        backend
            .submit(vec![Op::Rename {
                tag: Tag(2),
                from: from.clone(),
                to: to.clone(),
            }])
            .expect("submit rename");
        done(drain_one(&backend).outcome).expect("rename");

        backend
            .submit(vec![Op::List {
                tag: Tag(3),
                dir: dir.path().to_path_buf(),
            }])
            .expect("submit list");
        let entries = listed(drain_one(&backend).outcome).expect("list");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "segment-5-renamed");

        backend
            .submit(vec![Op::Unlink {
                tag: Tag(4),
                path: to,
            }])
            .expect("submit unlink");
        done(drain_one(&backend).outcome).expect("unlink");

        backend
            .submit(vec![Op::List {
                tag: Tag(5),
                dir: dir.path().to_path_buf(),
            }])
            .expect("submit list");
        let entries = listed(drain_one(&backend).outcome).expect("list");
        assert!(entries.is_empty());
    }

    // an unknown file handle fails its op without poisoning the queue
    #[test]
    fn unknown_handle_errors() {
        let backend = PosixBackend::new();
        backend
            .submit(vec![Op::SyncData {
                tag: Tag(1),
                file: FileId(999),
            }])
            .expect("submit sync");
        assert!(done(drain_one(&backend).outcome).is_err());
    }

    // the op count moves once per op, whichever door the op arrived through
    #[test]
    fn the_op_count_moves_per_op() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("segment-6");

        let backend = PosixBackend::new();
        assert_eq!(backend.ops(), 0, "a fresh backend has run nothing");

        let file = open_file(&backend, &path, true);
        assert_eq!(backend.ops(), 1, "the open is one op");

        backend
            .submit(vec![Op::SyncData { tag: Tag(2), file }])
            .expect("submit sync");
        drain_one(&backend);
        assert_eq!(backend.ops(), 2, "and the sync is the second");
    }

    /// A file of striped bytes, so a window off by one is visible
    fn striped_file(path: &Path, len: usize) -> Vec<u8> {
        let bytes: Vec<u8> = (0..len).map(|at| (at % 251) as u8).collect();
        std::fs::write(path, &bytes).expect("write the fixture");
        bytes
    }

    fn read_only_fd(path: &Path) -> std::fs::File {
        std::fs::File::open(path).expect("open the fixture")
    }

    // a covering read allocates one staging buffer per thread, whatever it reads
    #[test]
    fn a_covering_read_stages_once_per_thread() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("staged");
        let bytes = striped_file(&path, 64 * 1024);
        let file = read_only_fd(&path);

        // On its own thread, so the count is of this test's reads alone
        let allocs = std::thread::spawn(move || {
            let before = stage_allocations();
            for step in 0..1000u64 {
                let offset = (step * 37) % 60_000;
                let taken = read_covering(file.as_raw_fd(), offset, 100, |window| {
                    assert_eq!(window.len(), 100, "the cut was not the window at {offset}");
                    window.len()
                })
                .expect("covering read");
                assert_eq!(taken, 100);
            }
            stage_allocations() - before
        })
        .join()
        .expect("the staging thread");

        assert_eq!(
            allocs, 1,
            "a thousand reads allocated {allocs} stage buffers"
        );
        assert_eq!(bytes.len(), 64 * 1024);
    }

    // a covering read asks the kernel for the covering span only
    #[test]
    fn a_covering_read_asks_for_the_span() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("span");
        striped_file(&path, 64 * 1024);
        let file = read_only_fd(&path);

        for (offset, len, want) in [(0u64, 100u64, 4096u64), (4095, 2, 8192), (5000, 4000, 8192)] {
            let before = covering_bytes();
            read_covering(file.as_raw_fd(), offset, len, |window| window.len())
                .expect("covering read");
            assert_eq!(
                covering_bytes() - before,
                want,
                "the read for {len} bytes at {offset} asked for the wrong span",
            );
        }
    }

    // a span past the cap buys its own buffer and leaves the pooled one alone
    #[test]
    fn a_wide_covering_read_leaves_the_stage_alone() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("wide");
        let bytes = striped_file(&path, 2 * STAGE_BYTES);
        let file = read_only_fd(&path);

        // Off a block boundary and past the cap, so the span outgrows the pooled buffer
        let at = 4_095u64;
        let wide = STAGE_BYTES as u64 + 4_096;
        let (allocs, taken) = std::thread::spawn(move || {
            let before = stage_allocations();
            let mut taken = Vec::new();
            for step in 0..8u64 {
                // A narrow read between wide ones shows if a wide read replaced the pool
                read_covering(file.as_raw_fd(), 100 + step, 64, |window| window.len())
                    .expect("narrow covering read");

                let before_bytes = covering_bytes();
                taken.clear();
                read_covering(file.as_raw_fd(), at, wide, |window| {
                    taken.extend_from_slice(window);
                    window.len()
                })
                .expect("wide covering read");
                let (_, span) = covering_span(at, wide);
                assert_eq!(
                    covering_bytes() - before_bytes,
                    span,
                    "the wide read asked for the wrong span",
                );
            }
            (stage_allocations() - before, taken)
        })
        .join()
        .expect("the staging thread");

        assert_eq!(allocs, 1, "the wide reads allocated {allocs} stage buffers");
        let at = at as usize;
        assert_eq!(
            taken.as_slice(),
            &bytes[at..at + wide as usize],
            "the wide cut was not the window",
        );
    }

    // a widened read hands out the window and never the padding around it
    #[test]
    fn a_covering_read_hands_out_only_the_window() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("window");
        let bytes = striped_file(&path, 32 * 1024);
        let file = read_only_fd(&path);

        for (offset, len) in [
            (0u64, 10u64),
            (1, 4095),
            (4095, 2),
            (5000, 4000),
            (8192, 4096),
        ] {
            let mut taken = Vec::new();
            read_covering(file.as_raw_fd(), offset, len, |window| {
                taken.extend_from_slice(window);
                window.len()
            })
            .expect("covering read");
            let at = offset as usize;
            assert_eq!(
                taken.as_slice(),
                &bytes[at..at + len as usize],
                "the cut for {len} bytes at {offset} was not the window",
            );
        }
    }

    // a window running off the end of the file comes back short
    #[test]
    fn a_covering_read_at_the_end_is_short() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("truncated");
        // Not a multiple of the block, so the last read stops mid-block
        let bytes = striped_file(&path, 6000);
        let file = read_only_fd(&path);

        let mut taken = Vec::new();
        let filled = read_covering(file.as_raw_fd(), 5000, 4000, |window| {
            taken.extend_from_slice(window);
            window.len()
        })
        .expect("a short covering read is not an error");

        assert_eq!(filled, 1000, "the window stopped at the end of the file");
        assert_eq!(
            taken.as_slice(),
            &bytes[5000..],
            "the short cut is the tail"
        );
    }

    // a read that stops before the window starts cuts nothing
    #[test]
    fn a_covering_read_stopping_before_the_window_cuts_nothing() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("before");
        // One byte into the second block, so the read of that block fills one byte
        striped_file(&path, DIRECT_ALIGN + 1);
        let file = read_only_fd(&path);

        let mut taken = Vec::new();
        let filled = read_covering(file.as_raw_fd(), DIRECT_ALIGN as u64 + 4, 10, |window| {
            taken.extend_from_slice(window);
            window.len()
        })
        .expect("a read short of the window is not an error");

        assert_eq!(filled, 0, "a window past what the file holds took bytes");
        assert!(
            taken.is_empty(),
            "the cut named bytes the read never filled"
        );
    }

    // a direct read past the end comes back short and without EINVAL
    #[cfg(target_os = "linux")]
    #[test]
    fn a_direct_covering_read_at_the_end_is_short() {
        use std::os::unix::fs::OpenOptionsExt;

        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("direct-truncated");
        let bytes = striped_file(&path, 6000);
        let file = match std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(&path)
        {
            Ok(file) => file,
            Err(error) => {
                println!("skipped: this filesystem refused a direct open: {error}");
                return;
            }
        };

        let mut taken = Vec::new();
        let filled = read_covering(file.as_raw_fd(), 5000, 4000, |window| {
            taken.extend_from_slice(window);
            window.len()
        })
        .expect("a short direct read is not an error");

        assert_eq!(filled, 1000, "the window stopped at the end of the file");
        assert_eq!(
            taken.as_slice(),
            &bytes[5000..],
            "the short cut is the tail"
        );
    }

    // a probe that filled the whole read is the answer
    #[test]
    fn warm_verdict_takes_a_full_fill() {
        assert_eq!(warm_verdict(4000, 0, 4000, true), WarmVerdict::Warm(4000));
    }

    // a probe that filled part of the read commits nothing and goes to the device
    #[test]
    fn warm_verdict_refuses_a_partial_fill() {
        assert_eq!(warm_verdict(2048, 0, 4000, true), WarmVerdict::Cold);
        assert_eq!(warm_verdict(0, 0, 4000, true), WarmVerdict::Cold);
    }

    // EAGAIN means the pages are not resident, so the read is cold
    #[test]
    fn warm_verdict_reads_eagain_as_cold() {
        assert_eq!(
            warm_verdict(-1, libc::EAGAIN, 4000, true),
            WarmVerdict::Cold
        );
    }

    // a kernel that has never answered and refuses the flag retires the probe
    #[test]
    fn warm_verdict_retires_an_unknown_flag() {
        assert_eq!(
            warm_verdict(-1, libc::EOPNOTSUPP, 4000, false),
            WarmVerdict::Retire
        );
        assert_eq!(
            warm_verdict(-1, libc::EINVAL, 4000, false),
            WarmVerdict::Retire
        );
    }

    // once the probe has answered, the same codes are the read's own error
    #[test]
    fn warm_verdict_reports_errors_after_proof() {
        assert_eq!(
            warm_verdict(-1, libc::EOPNOTSUPP, 4000, true),
            WarmVerdict::Failed
        );
        assert_eq!(
            warm_verdict(-1, libc::EIO, 4000, false),
            WarmVerdict::Failed
        );
    }
}
