//! Synchronous POSIX backend for the ring-shaped I/O trait
//!
//! Ops execute at submit against blocking libc syscalls and their completions
//! queue for the next poll, so this stands in for a completion backend without a
//! call site changing shape. A sync hands the bytes to the drive; whether the
//! drive has written them before it answers is the drive's business.

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

/// Buffers one vectored call carries, the portable floor of the kernel's own cap
///
/// Linux and macOS both stop at 1024 iovecs and fail the whole call past it, so a
/// drain wider than this is split across several calls rather than rejected.
pub(crate) const MAX_IOVECS: usize = 1024;

/// Answer a read from the page cache or refuse it, rather than waiting on a device
///
/// Resident pages answer for the price of a syscall the read owed anyway, and
/// anything else comes back EAGAIN, which is what tells the route it is cold.
#[cfg(target_os = "linux")]
const RWF_NOWAIT: c_int = 0x0000_0008;

/// Window length above which the warm probe is skipped
///
/// A probe that comes back short has copied whatever was resident for nothing, so
/// it is worth issuing only where that waste is bounded by a few pages.
const WARM_PROBE_MAX: usize = 16 * 1024;

/// Largest covering span a thread keeps a staging buffer for
///
/// A window is a few blocks and a record can be sixty four megabytes, and
/// keeping the larger would hold that per thread. The ring's registered buffers
/// are this wide too, so one number decides both.
pub(crate) const STAGE_BYTES: usize = 128 * 1024;

/// Whether the volume may still read a window around the page cache
///
/// Its two reads prove themselves separately: EINVAL means both that the kernel
/// does not know a flag and that a direct read was unaligned, so one latch for
/// both would let a defect disable the other.
#[derive(Debug)]
struct ColdReads {
    /// Whether the route may still be taken at all
    is_live: AtomicBool,

    /// Whether one probe has come back, which settles what its refusals mean
    probe_proven: AtomicBool,

    /// Whether one direct read has come back, which settles what its refusals mean
    direct_proven: AtomicBool,
}

impl ColdReads {
    fn new() -> ColdReads {
        ColdReads {
            is_live: AtomicBool::new(true),
            probe_proven: AtomicBool::new(false),
            direct_proven: AtomicBool::new(false),
        }
    }

    fn is_live(&self) -> bool {
        self.is_live.load(Ordering::Relaxed)
    }

    fn is_probe_proven(&self) -> bool {
        self.probe_proven.load(Ordering::Relaxed)
    }

    /// Note that a probe came back, which settles what its refusals mean
    fn note_probe(&self) {
        prove(&self.probe_proven);
    }

    /// Note that a direct read came back, which settles what its refusals mean
    fn note_direct(&self) {
        prove(&self.direct_proven);
    }

    /// Retire the plane on a refused direct read, and say whether to fall back
    ///
    /// Only a direct read that has never once answered can retire, since past
    /// that the codes are ones a read can earn honestly. The probe reads the
    /// other descriptor and vouches for nothing here.
    fn retire_on_refusal(&self, error: &ReelError) -> bool {
        if self.direct_proven.load(Ordering::Relaxed) || !is_unknown_flag(error) {
            return false;
        }
        self.retire(error);
        true
    }

    fn retire(&self, error: &ReelError) {
        self.is_live.store(false, Ordering::Relaxed);
        tracing::warn!(
            "reel windows fall back to the page cache: the cold read plane was refused: {error}"
        );
    }

    /// Answer a window from the page cache without blocking, or say it is cold
    ///
    /// A partial fill is never committed: a short read reads as a window the
    /// volume cannot answer and buys a second read.
    fn warm_read(&self, fd: RawFd, buf: &mut ReadBuf, offset: u64) -> Result<Option<usize>> {
        let (ptr, len) = buf.as_mut_ptr();
        let iovec = libc::iovec {
            iov_base: ptr as *mut c_void,
            iov_len: len,
        };
        let (ret, errno) = nowait_preadv(fd, &iovec, 1, offset);
        match warm_verdict(ret, errno, len, self.is_probe_proven()) {
            WarmVerdict::Warm(filled) => {
                self.note_probe();
                // Safety: the kernel reported filling exactly this many bytes of
                // the room the buffer handed over.
                unsafe { buf.commit(filled) };
                Ok(Some(filled))
            }
            WarmVerdict::Cold => {
                self.note_probe();
                Ok(None)
            }
            WarmVerdict::Retire => {
                self.retire(&ReelError::Io(io::Error::from_raw_os_error(errno)));
                Ok(None)
            }
            WarmVerdict::Failed => Err(ReelError::Io(io::Error::from_raw_os_error(errno))),
        }
    }
}

/// Latch a plane's proof, leaving the line alone once it is set
fn prove(flag: &AtomicBool) {
    if !flag.load(Ordering::Relaxed) {
        flag.store(true, Ordering::Relaxed);
    }
}

/// Read what the page cache already holds, refusing rather than waiting on a device
///
/// The result and the errno come back together, so the classification is one pure
/// function over both. Vectored, since a framed record fills two buffers at once.
#[cfg(target_os = "linux")]
fn nowait_preadv(
    fd: RawFd,
    iov: *const libc::iovec,
    count: c_int,
    offset: u64,
) -> (libc::ssize_t, c_int) {
    // Safety: the list names count buffers of the room their owners handed over,
    // and the kernel writes no more than that.
    let ret = unsafe { libc::preadv2(fd, iov, count, offset as libc::off_t, RWF_NOWAIT) };
    let errno = match ret < 0 {
        true => io::Error::last_os_error().raw_os_error().unwrap_or(0),
        false => 0,
    };
    (ret, errno)
}

/// No kernel anywhere else answers a read only from its resident pages
///
/// Reported cold, which sends the read to the driver rather than guessing.
#[cfg(not(target_os = "linux"))]
fn nowait_preadv(
    _fd: RawFd,
    _iov: *const libc::iovec,
    _count: c_int,
    _offset: u64,
) -> (libc::ssize_t, c_int) {
    (-1, libc::EAGAIN)
}

/// What one non-blocking probe of the page cache settled
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WarmVerdict {
    /// The cache held the whole window, and this is what it filled
    Warm(usize),

    /// The pages are not all resident, so the window goes to the device
    Cold,

    /// The kernel does not know the flag, so the plane retires
    Retire,

    /// The read failed for a reason of its own
    Failed,
}

/// What a probe's return and errno mean for the window it was asked about
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

/// What the cold-window route did, over every window it was handed
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ColdReadCounts {
    /// Windows the reel routed to this plane
    pub routed: u64,

    /// Windows the page cache answered whole, with no device op
    pub warm: u64,

    /// Windows that went around the cache to the device
    pub direct: u64,

    /// Bytes those reads asked the device for, the covering span included
    pub direct_bytes: u64,
}

/// The counts behind ColdReadCounts
#[derive(Debug, Default)]
struct ColdCounts {
    routed: AtomicU64,
    warm: AtomicU64,
    direct: AtomicU64,
    direct_bytes: AtomicU64,
}

impl ColdCounts {
    fn snapshot(&self) -> ColdReadCounts {
        ColdReadCounts {
            routed: self.routed.load(Ordering::Relaxed),
            warm: self.warm.load(Ordering::Relaxed),
            direct: self.direct.load(Ordering::Relaxed),
            direct_bytes: self.direct_bytes.load(Ordering::Relaxed),
        }
    }
}

/// Record length above which an awaited point read is not probed
///
/// A short probe wastes one memcpy rather than a device read, so the bound sits
/// well above WARM_PROBE_MAX: an ordinary record runs to tens of kilobytes, and
/// excluding them would leave the knob armed and firing on nothing.
const POINT_PROBE_MAX: usize = 1024 * 1024;

/// Whether an awaited point read may still ask the page cache before it queues
///
/// Separate from ColdReads: this probe reads a buffered descriptor, and a refusal
/// earned on the ranged plane's direct one says nothing about it.
#[derive(Debug)]
struct WarmReads {
    /// Whether the probe may still be issued at all
    is_live: AtomicBool,

    /// Whether one probe has come back, which settles what its refusals mean
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

    /// Fill both buffers from resident pages, or leave them untouched and say so
    ///
    /// The header and payload go into one call. Nothing is committed unless the
    /// whole record came, since a partial fill reaches the caller as a record it
    /// cannot frame. A failure of its own is not reported: the queued read behind
    /// it meets the same condition on the same descriptor.
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
                // Safety: the kernel reported filling the whole of both buffers, and
                // it fills the first before the second.
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

/// What the awaited point path's warm probe did, over every read it was handed
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WarmReadCounts {
    /// Awaited point reads that asked the page cache before queueing anything
    pub asked: u64,

    /// Those the cache answered whole, with no op, no slot and no completion
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

/// Whether an error is the kernel saying it does not know a flag it was handed
///
/// Which of the two a kernel picks depends on how far the call got before the
/// flag was looked at, so both count.
fn is_unknown_flag(error: &ReelError) -> bool {
    let code = match error {
        ReelError::Io(io) => io.raw_os_error(),
        _ => None,
    };
    matches!(code, Some(libc::EOPNOTSUPP) | Some(libc::EINVAL))
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
    cold: ColdReads,
    cold_counts: ColdCounts,
    warm: WarmReads,
    warm_counts: WarmCounts,
    is_direct: bool,
}

impl PosixBackend {
    /// Build a posix backend over buffered descriptors
    pub fn new() -> PosixBackend {
        PosixBackend::with_direct(false)
    }

    /// Build a backend whose opens bypass the page cache entirely
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
            cold: ColdReads::new(),
            cold_counts: ColdCounts::default(),
            warm: WarmReads::new(),
            warm_counts: WarmCounts::default(),
            is_direct,
        }
    }

    /// What the cold-window route has done, over the ops the sampler selected
    pub fn cold_reads(&self) -> ColdReadCounts {
        self.cold_counts.snapshot()
    }

    /// What the awaited point path's warm probe has done, over the sampled reads
    pub fn warm_reads(&self) -> WarmReadCounts {
        self.warm_counts.snapshot()
    }

    /// Answer a whole framed record from resident pages, or leave it to the driver
    ///
    /// A direct volume is never asked, since the probe could only refuse; neither
    /// is a record past the probe bound or one on a retired backend.
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

    /// Whether the cold-window plane is still live on this backend
    ///
    /// A refusal retires the plane here rather than on the reel, so this tells a
    /// routed read that fell back from one that was never routed.
    pub fn cold_plane_live(&self) -> bool {
        self.cold.is_live()
    }

    /// Descriptors this backend currently holds open
    ///
    /// The count a handle cache bounds, since nothing else sees a descriptor leak.
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

    /// Run one flush, billing its count and the time the drive took
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
            Op::Open {
                tag,
                path,
                create,
                direct,
            } => {
                let mut options = OpenOptions::new();
                options.read(true).write(true).create(create);
                if self.is_direct || direct {
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
            Op::PreadCold {
                tag,
                file,
                direct,
                offset,
                mut buf,
                probe,
            } => {
                let result = match self.fd_of(file) {
                    Ok(fd) => self.cold_pread(fd, direct, &mut buf, offset, probe),
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
                // Dropping the owned descriptor is the close, and for a file whose
                // last link is already gone it is also what returns its blocks.
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

    /// Serve a window from the plane its route names
    ///
    /// The probe and the device read are one op, one slot and one completion: a
    /// second flight for the probe would double the async door's slot traffic.
    fn cold_pread(
        &self,
        fd: RawFd,
        direct: FileId,
        buf: &mut ReadBuf,
        offset: u64,
        probe: bool,
    ) -> Result<usize> {
        self.cold_counts.routed.fetch_add(1, Ordering::Relaxed);
        if probe && self.cold.is_live() && buf.wanted() <= WARM_PROBE_MAX {
            if let Some(filled) = self.cold.warm_read(fd, buf, offset)? {
                self.cold_counts.warm.fetch_add(1, Ordering::Relaxed);
                return Ok(filled);
            }
        }
        // A probe that retired the plane leaves the volume with the reads it had
        // before, since without the probe warm and cold cannot be told apart.
        if !self.cold.is_live() {
            return pread_into(fd, buf, offset);
        }

        let direct_fd = self.fd_of(direct)?;
        match direct_pread(direct_fd, buf, offset) {
            Ok(filled) => {
                self.cold.note_direct();
                let (_, span) = covering_span(offset, buf.wanted() as u64);
                self.cold_counts.direct.fetch_add(1, Ordering::Relaxed);
                self.cold_counts
                    .direct_bytes
                    .fetch_add(span, Ordering::Relaxed);
                Ok(filled)
            }
            // A kernel that refuses the direct read leaves the volume with the
            // plane it had before rather than failing a read the cache can serve.
            Err(error) if self.cold.retire_on_refusal(&error) => pread_into(fd, buf, offset),
            Err(error) => Err(error),
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

    fn truncate(&self, file: FileId, len: u64) -> Result<()> {
        let fd = self.fd_of(file)?;
        checked(unsafe { libc::ftruncate(fd, len as libc::off_t) })
    }

    fn length(&self, file: FileId) -> Result<u64> {
        let fd = self.fd_of(file)?;
        raw_length(fd)
    }

    fn advise(&self, file: FileId, offset: u64, len: u64, advice: Advice) -> Result<()> {
        let fd = self.fd_of(file)?;
        raw_advise(fd, offset, len, advice)
    }

    /// The descriptor behind a handle, for a backend that shares this table
    ///
    /// Answered from a per-thread cache before the table, since ops arrive in runs
    /// against one file. Identifiers are never reused, so only a close can stale a
    /// cached pair, and every close moves a generation the hit checks.
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

    /// Take a file out of the table and stale every thread's cached descriptor
    ///
    /// The count moves before the descriptor is dropped, so a thread that resolved
    /// the old one has already been staled by the time the fd could be reused.
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
    /// Posix, told apart by the descriptors this backend opened for itself
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

/// Ask for a descriptor the page cache does not stand behind
///
/// Only Linux spells this as an open flag. The macOS fcntl is not the same
/// promise: the transfer still goes through the cache. Direct means Linux.
#[cfg(target_os = "linux")]
fn direct_open_flag(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.custom_flags(libc::O_DIRECT);
}

#[cfg(not(target_os = "linux"))]
fn direct_open_flag(_options: &mut OpenOptions) {}

/// Write a drain through one aligned buffer, since the device takes whole blocks
///
/// The volume already framed the drain on a boundary, so only the address is
/// left. The buffers are gathered into one aligned run because a vectored direct
/// write needs every buffer block sized, which a header and payload are not.
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
    // Only the rounding tail is left unwritten, and it reaches the device, so it
    // is the one part that has to be zeroed rather than whatever was on the heap.
    staged.zero_from(at);

    let span = staged.len();
    let wrote = write_all_at(fd, staged.as_ptr(), span, offset)?;
    // What the caller framed is what it gets told landed, since the rounding is
    // this function's business and not the write head's.
    Ok(wrote.min(total as u64))
}

/// Issue one direct write, resuming a short one from where it stopped
fn write_all_at(fd: RawFd, ptr: *mut u8, len: usize, offset: u64) -> Result<u64> {
    let mut written = 0usize;
    while written < len {
        let at = offset.saturating_add(written as u64) as libc::off_t;
        // Safety: the pointer names an allocation of len bytes and written is
        // always inside it, so the range handed over is owned and initialized.
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
    /// The aligned buffer this thread stages a direct read through
    ///
    /// Allocated once at full size rather than grown, since a per-read aligned
    /// allocation is the whole of a small direct read's penalty.
    static STAGE: RefCell<Option<AlignedBuf>> = const { RefCell::new(None) };
}

#[cfg(test)]
thread_local! {
    /// Stage buffers this thread has allocated, which a test holds to one
    static STAGE_ALLOCS: Cell<usize> = const { Cell::new(0) };

    /// Bytes this thread's covering reads have asked the kernel to fill
    ///
    /// What separates reading the covering span from reading the whole stage.
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

/// Run something against an aligned buffer wide enough for a span
///
/// The borrow is held for the whole call, so neither an error nor a panic loses
/// the buffer. Re-entering would panic on the double borrow and cannot happen:
/// all that runs inside is a pread and a memcpy.
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

/// Read a range that is aligned to nothing by reading the blocks around it
///
/// The read is widened to the blocks holding the record and the caller is handed
/// the middle; the padding is never named outside this function. A read off the
/// end comes back short, cut against what actually landed.
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
        // The read takes the span, never the buffer's own length: a pooled stage
        // is as wide as the cap, and reading that much would fetch many blocks
        // for one small window.
        let filled = read_at(fd, staged.as_ptr(), span, start)?;
        let (from, to) = wanted_window(filled, skip, len as usize);
        // Safety: the read reported filling this many bytes from the buffer's start,
        // so the range named is exactly what the kernel wrote.
        let bytes = unsafe { staged.filled(to) };
        Ok(cut(&bytes[from..]))
    })
}

/// Issue one direct read, stopping at the end of the file rather than short-cycling
///
/// A direct descriptor refuses a resume off a block boundary, so a read that
/// stopped part way into a block is the end of the file rather than a read to
/// continue; resuming would earn EINVAL instead of the short read.
fn read_at(fd: RawFd, ptr: *mut u8, len: usize, offset: u64) -> Result<usize> {
    #[cfg(test)]
    COVERING_BYTES.with(|count| count.set(count.get() + len as u64));
    let mut filled = 0usize;
    while filled < len {
        let at = offset.saturating_add(filled as u64) as libc::off_t;
        // Safety: as in write_all_at, the range is inside the owned allocation.
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

/// Write every buffer at the offset, however many calls the kernel needs
///
/// A vectored write is capped at a fixed number of buffers and may report fewer
/// bytes than it was handed, so a wide drain is split and a partial write resumes
/// where it stopped. All or nothing: every byte lands or an error comes back, so
/// a caller can trust the write head it advances.
fn pwritev_all(fd: RawFd, bufs: &[WriteBuf], offset: u64) -> Result<u64> {
    let mut iovecs = IOVEC_SCRATCH.with(|held| held.take());
    let written = pwritev_all_into(&mut iovecs, fd, bufs, offset);
    IOVEC_SCRATCH.with(|held| held.set(iovecs));
    written
}

thread_local! {
    /// The iovec list this thread writes through, kept rather than rebuilt
    static IOVEC_SCRATCH: Cell<Vec<libc::iovec>> = const { Cell::new(Vec::new()) };
}

/// Files this thread remembers a descriptor for at once
const RESOLVED_WAYS: usize = 4;

thread_local! {
    /// The descriptors this thread last resolved, one way per file it is working
    static RESOLVED: [Cell<Resolved>; RESOLVED_WAYS] =
        const { [const { Cell::new(Resolved::none()) }; RESOLVED_WAYS] };
}

/// Identity handed to the next backend, so a cached descriptor names whose it is
static NEXT_BACKEND: AtomicU64 = AtomicU64::new(0);

/// The way a file's descriptor is remembered in
///
/// Mixed with the backend, since each numbers its files from zero and two volumes
/// would otherwise land in the same way and evict each other.
fn resolved_way(backend: u64, file: FileId) -> usize {
    (backend ^ file.0) as usize % RESOLVED_WAYS
}

/// One thread's memory of the descriptor behind a file
#[derive(Clone, Copy)]
struct Resolved {
    backend: u64,
    file: FileId,
    fd: RawFd,
    generation: u64,
}

impl Resolved {
    /// A thread that has resolved nothing yet
    ///
    /// The sentinel is the generation, not the identifier: no close can precede
    /// the first resolve, so a generation of all ones matches nothing.
    const fn none() -> Resolved {
        Resolved {
            backend: u64::MAX,
            file: FileId(0),
            fd: -1,
            generation: u64::MAX,
        }
    }

    /// The descriptor, if this memory is of the right file and nothing has closed
    fn hit(self, backend: u64, file: FileId, generation: u64) -> Option<RawFd> {
        (self.backend == backend && self.file == file && self.generation == generation)
            .then_some(self.fd)
    }
}

/// The vectored write itself, filling a list it is handed rather than its own
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
        // Safety: the list names count buffers of the bytes their owners handed
        // over, and the kernel reads no more than that.
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

/// Read into a buffer's uninitialized room, going back for whatever a call left
///
/// One read call is capped at 0x7ffff000 bytes however much was asked for, so
/// taking a short answer at its word is a silent truncation at the two gigabyte
/// line. Only a call that read nothing ends the loop, which is the end of file.
fn pread_into(fd: RawFd, buf: &mut ReadBuf, offset: u64) -> Result<usize> {
    let (base, wanted) = buf.as_mut_ptr();
    let mut filled = 0usize;
    while filled < wanted {
        // Safety: filled is never past the room, so the pointer and length name
        // room the buffer owns.
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
    // Safety: the pointer and length name room the caller's buffer owns, and the
    // kernel writes no more than that.
    let ret = unsafe { libc::pread(fd, ptr as *mut c_void, len, offset as libc::off_t) };
    if ret < 0 {
        return Err(ReelError::Io(io::Error::last_os_error()));
    }
    Ok(ret as usize)
}

/// Read one contiguous range into two buffers in a single call
///
/// A framed record is a header followed by its payload, so the split hands the
/// payload buffer straight back rather than shifting it down over the header.
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
    // Safety: the list names two buffers of the room their owners handed over, and
    // the kernel writes no more than that.
    let ret = unsafe { libc::preadv(fd, iovecs.as_ptr(), count, at) };
    if ret < 0 {
        return Err(ReelError::Io(io::Error::last_os_error()));
    }
    // The kernel fills the first buffer before the second, so a short read leaves
    // the head whole and cuts the body, and a shorter one cuts the head itself.
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
    let end = offset.saturating_add(len);
    let ret = unsafe { libc::fallocate(fd, 0, offset as libc::off_t, len as libc::off_t) };
    if ret == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(code) if code == libc::ENOSYS || code == libc::EOPNOTSUPP => extend_to(fd, end),
        _ => Err(ReelError::Io(error)),
    }
}

/// Set the logical size to an end the file has not reached, never shrinking
///
/// A reservation step can arrive after the write head has passed its end, and a
/// truncate down would destroy records that already landed.
fn extend_to(fd: RawFd, end: u64) -> Result<()> {
    if raw_length(fd)? >= end {
        return Ok(());
    }
    truncate_to(fd, end)
}

/// Reserve a byte range on macOS, which counts its length differently from Linux
///
/// F_PEOFPOSMODE reserves its length past the current end of file, so the ask is
/// how much to add rather than where to reach: handing it the absolute end
/// reserves the whole file again on every extension. The shortfall is worked out
/// here, and a range already covered asks for nothing.
#[cfg(target_os = "macos")]
fn raw_allocate(fd: RawFd, offset: u64, len: u64) -> Result<()> {
    let end = offset.saturating_add(len);
    let held = raw_length(fd)?;
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
    extend_to(fd, end)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn raw_allocate(fd: RawFd, offset: u64, len: u64) -> Result<()> {
    extend_to(fd, offset.saturating_add(len))
}

/// Linux says everything per range, and has nothing to say about a file as a whole
///
/// Cache hygiene here is the running stream of range hints, so the file-wide ask
/// is the one with nothing behind it.
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

/// macOS says everything per descriptor, and has nothing to say about a range
///
/// Its flags stick to the descriptor rather than the range, so a range hint has
/// no honest translation and is dropped rather than made permanent.
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
                direct: false,
            }])
            .expect("submit open");
        opened(drain_one(backend).outcome).expect("open result")
    }

    // a closed file's descriptor is not answered from the cache the next op checks
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
        // Resolve it, so this thread is holding its descriptor.
        assert!(backend.fd_of(first).is_ok());

        backend
            .submit(vec![Op::Close {
                tag: Tag(3),
                file: first,
            }])
            .expect("submit close");
        done(drain_one(&backend).outcome).expect("close");

        // The next open is free to take the number the close gave up.
        let second = open_file(&backend, &dir.path().join("segment-1"), true);
        assert!(backend.fd_of(second).is_ok(), "the new file resolves");
        assert!(
            backend.fd_of(first).is_err(),
            "a closed file still answered with a descriptor"
        );
    }

    // one thread reading two volumes does not confuse their files for each other
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

        // And back the other way, so the cache is not merely ordered correctly once.
        assert_eq!(first.fd_of(here).expect("still the first"), mine);
        assert_eq!(second.fd_of(there).expect("still the second"), yours);
    }

    // a direct backend's descriptors really carry the flag, read back off the fd
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

    // allocate reserves and sets the logical file size
    #[test]
    fn allocate_sets_size() {
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
        assert_eq!(entries[0].len, 4096);
    }

    // extending a segment chunk by chunk reserves the segment, not the sum of offsets
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
        assert_eq!(
            status.st_size as u64, logical,
            "the logical size is the segment"
        );

        // Blocks are reported in 512 byte units on both platforms this builds for.
        // A little slack, since a filesystem may round a reservation up.
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

        // On its own thread, so the count is of this test's reads alone.
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

    // a covering read asks the kernel for the covering span, not for the stage
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

        // Off a block boundary and past the cap, so the span is wider than the
        // window on both ends and wider than the pooled buffer.
        let at = 4_095u64;
        let wide = STAGE_BYTES as u64 + 4_096;
        let (allocs, taken) = std::thread::spawn(move || {
            let before = stage_allocations();
            let mut taken = Vec::new();
            for step in 0..8u64 {
                // A narrow read between the wide ones, so a wide read that replaced
                // the pool shows up in the count rather than being absorbed.
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

    // a window running off the end of the file comes back short, not as an error
    #[test]
    fn a_covering_read_at_the_end_is_short() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("truncated");
        // Not a multiple of the block, so the last read stops mid-block.
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
        // One byte into the second block, so the read of that block fills one byte.
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

    // a direct descriptor's read past the end comes back short, not EINVAL
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

    // a retired plane serves the window off the buffered descriptor
    #[test]
    fn a_retired_plane_reads_buffered() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("retired");
        let bytes = striped_file(&path, 16 * 1024);

        let backend = PosixBackend::new();
        let file = open_file(&backend, &path, false);
        backend
            .submit(vec![Op::Open {
                tag: Tag(2),
                path: path.clone(),
                create: false,
                direct: true,
            }])
            .expect("submit the direct open");
        let direct = opened(drain_one(&backend).outcome).expect("direct open");

        backend.cold.retire(&ReelError::Backend("test".to_string()));
        backend
            .submit(vec![Op::PreadCold {
                tag: Tag(3),
                file,
                direct,
                offset: 5000,
                buf: ReadBuf::new(1000),
                probe: true,
            }])
            .expect("submit the routed read");
        let (result, taken) = read_bytes(drain_one(&backend).outcome);

        assert_eq!(result.expect("the buffered fallback read"), 1000);
        assert_eq!(taken.as_slice(), &bytes[5000..6000]);
        assert_eq!(backend.cold_reads().routed, 1, "the read was still routed");
        assert_eq!(
            backend.cold_reads().direct,
            0,
            "and never reached the device plane"
        );
    }

    // a probe coming back leaves the direct read's own fallback intact
    #[test]
    fn a_probe_leaves_the_direct_plane_unproven() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("unproven");
        striped_file(&path, 16 * 1024);
        let file = std::fs::File::open(&path).expect("open the probed descriptor");
        let refused = ReelError::Io(io::Error::from_raw_os_error(libc::EINVAL));

        let cold = ColdReads::new();
        let mut buf = ReadBuf::new(4000);
        cold.warm_read(file.as_raw_fd(), &mut buf, 0)
            .expect("the probe answered");
        if !cold.is_probe_proven() {
            println!("skipped: this filesystem refused the probe, so it latched nothing");
            return;
        }

        assert!(
            cold.retire_on_refusal(&refused),
            "the probe proved the direct plane"
        );
        cold.note_direct();
        assert!(
            !cold.retire_on_refusal(&refused),
            "a plane that answered retired anyway"
        );
    }

    // a probe that filled the whole window is the answer
    #[test]
    fn warm_verdict_takes_a_full_fill() {
        assert_eq!(warm_verdict(4000, 0, 4000, true), WarmVerdict::Warm(4000));
    }

    // a probe that filled part of the window commits nothing and goes to the device
    #[test]
    fn warm_verdict_refuses_a_partial_fill() {
        assert_eq!(warm_verdict(2048, 0, 4000, true), WarmVerdict::Cold);
        assert_eq!(warm_verdict(0, 0, 4000, true), WarmVerdict::Cold);
    }

    // the pages are not resident, which is what the probe exists to find out
    #[test]
    fn warm_verdict_reads_eagain_as_cold() {
        assert_eq!(
            warm_verdict(-1, libc::EAGAIN, 4000, true),
            WarmVerdict::Cold
        );
    }

    // a kernel that has never answered and refuses the flag retires the plane
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

    // once the plane has answered, the same codes are the read's own error
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
