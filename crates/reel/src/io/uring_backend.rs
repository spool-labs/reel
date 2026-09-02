//! Ring backend over io_uring, one ring per thread on both doors
//!
//! A ring belongs to the thread that submits to it, so there is no shard lock and
//! no completion handed to a thread that did not ask for it, which is what makes
//! SINGLE_ISSUER and DEFER_TASKRUN legal. A caller with no thread has no ring, so
//! an engine thread owns one per shard, takes ops through a bounded inbox, and
//! files the completions into the driver's slot table.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::thread::JoinHandle;

use io_uring::{cqueue, opcode, types, EnterFlags, IoUring};

use crate::config::{IowqWorkers, RingTuning, TaskRun};
use crate::error::{ReelError, Result};
use crate::io::direct::{
    align_up, covering_span, cut_into, cut_split_into, wanted_window, AlignedBuf, DIRECT_ALIGN,
    DIRECT_REQUEST_BYTES,
};
use crate::io::op::{Completion, FileId, Op, Outcome, ReadBuf, Tag, WriteBuf};
use crate::io::posix_backend::{PosixBackend, MAX_IOVECS, STAGE_BYTES};
use crate::io::slots::{SlotTable, SLOT_COUNT};
use crate::io::ServingBackend;
use crate::io::{DoorCounts, ReelIo};
use crate::sync::lock;

/// Submission slots each ring is built with
const RING_ENTRIES: u32 = 256;

/// Entries a slot's iovec list holds without going back to the allocator
///
/// A split read is exactly two, which is the op the list is built for most often.
const IOVEC_ROOM: usize = 2;

/// Registered descriptor slots one ring keeps, the ceiling on files it can hold
const MAX_REGISTERED_FILES: u32 = 4096;

/// Bytes one registered buffer holds, the room a staged op lands in
///
/// A block over the staging width, since a read aligned to nothing rounds down at
/// the front and up at the back. A span past what the pool serves goes off the ring.
const REGISTERED_BUFFER_BYTES: usize = STAGE_BYTES + DIRECT_ALIGN;

/// Registered buffers one ring keeps, the ceiling on direct ops it can have out
///
/// What the direct door costs per submitting thread. Too few and a wide batch
/// waits on a buffer the way it waits on a slot, too many and every thread pins
/// pages it never fills.
const REGISTERED_BUFFERS: usize = 32;

/// The place a completion goes back to when no batch is holding one for it
const UNORDERED: u32 = u32::MAX;

/// The user data the engine thread's inbox poll carries, which names no op
///
/// Every slot the slab addresses is below its capacity, so this reaches none of
/// them and a completion carrying it can only be the poll's own.
const KICK_TAG: u64 = u64::MAX;

/// Rounds a spinning wait asks the queue before it starts yielding the core
const SPIN_ROUNDS: u32 = 64;

/// Rounds a spinning wait gives up on and sleeps for a completion instead
const MAX_SPIN_ROUNDS: u32 = 1_000_000;

/// The iovec array a vectored op hands the ring, owned by the slot it flies in
///
/// The pointers name buffers the same record owns for the whole flight, and the
/// slot holds list and record together until the completion comes back. It lives
/// in the slot so the allocation is bought once per slot, not once per op.
struct IoVecs(Vec<libc::iovec>);

unsafe impl Send for IoVecs {}

impl IoVecs {
    /// An empty list with room for the ops that take the fewest entries
    fn new() -> IoVecs {
        IoVecs(Vec::with_capacity(IOVEC_ROOM))
    }

    /// Point the list at these spans and nothing else
    ///
    /// The room already bought serves anything that fits it, so a list refilled
    /// with what it held last time touches the allocator not at all.
    fn refill(&mut self, spans: impl IntoIterator<Item = (*mut libc::c_void, usize)>) {
        self.0.clear();
        self.0.extend(
            spans
                .into_iter()
                .map(|(iov_base, iov_len)| libc::iovec { iov_base, iov_len }),
        );
    }

    fn as_ptr(&self) -> *const libc::iovec {
        self.0.as_ptr()
    }

    fn len(&self) -> u32 {
        self.0.len() as u32
    }
}

/// The aligned buffers one ring registered, which its direct ops fly through
///
/// A direct descriptor refuses a buffer wherever the allocator put it, so an op is
/// staged through one of these. Registering hands the kernel the pages once, so a
/// submission names an index rather than pinning a span per op. The pool belongs
/// to one thread's ring, so nothing locks it.
struct Buffers {
    /// The buffers themselves, at the addresses the kernel pinned
    held: Vec<AlignedBuf>,

    /// Buffers carrying no op, popped from the back
    free: Vec<u16>,
}

impl Buffers {
    /// A pool holding nothing, for a ring whose ops carry the caller's own buffers
    fn none() -> Buffers {
        Buffers {
            held: Vec::new(),
            free: Vec::new(),
        }
    }

    /// The buffers a pool is made of, before any ring has been told about them
    fn allocate() -> Buffers {
        let mut held = Vec::with_capacity(REGISTERED_BUFFERS);
        let mut free = Vec::with_capacity(REGISTERED_BUFFERS);
        for at in 0..REGISTERED_BUFFERS {
            let Ok(buf) = AlignedBuf::uninit(REGISTERED_BUFFER_BYTES) else {
                return Buffers::none();
            };
            held.push(buf);
            // Popped from the back, so the first ops take the low buffers and a trace
            // of the indices reads in the order they were claimed.
            free.push((REGISTERED_BUFFERS - 1 - at) as u16);
        }
        Buffers { held, free }
    }

    /// Hand a ring a pool of its own, or none when the kernel will not take one
    ///
    /// A refusal leaves the volume where it was before the pool existed, with its
    /// direct ops answered one at a time on the posix path.
    fn register(ring: &IoUring) -> Buffers {
        let pool = Buffers::allocate();
        if !pool.is_live() {
            return pool;
        }
        let mut spans = Vec::with_capacity(pool.held.len());
        for buf in &pool.held {
            spans.push(libc::iovec {
                iov_base: buf.as_ptr().cast(),
                iov_len: buf.len(),
            });
        }
        // SAFETY: every span names a buffer this pool owns and never moves, and
        // the ring is dropped before the pool.
        if unsafe { ring.submitter().register_buffers(&spans) }.is_err() {
            tracing::debug!("this ring has no buffer pool, so its direct ops take the posix path");
            return Buffers::none();
        }
        pool
    }

    /// Whether the kernel took a pool for this ring at all
    fn is_live(&self) -> bool {
        !self.held.is_empty()
    }

    /// Whether every buffer the kernel took is carrying an op
    ///
    /// A pool that was refused is never starved: its volume's ops go to the posix
    /// path one at a time rather than waiting on a buffer that is not coming.
    fn is_starved(&self) -> bool {
        self.is_live() && self.free.is_empty()
    }

    /// Take a buffer for an op of this span, or nothing when none can serve it
    ///
    /// The ceiling is the request width rather than the buffer's, so an op the pool
    /// has room for but the device would answer in two goes off the ring instead.
    fn claim(&mut self, span: usize) -> Option<u16> {
        if span > DIRECT_REQUEST_BYTES {
            return None;
        }
        self.free.pop()
    }

    /// Give a buffer back, its op having come off the ring
    fn release(&mut self, at: u16) {
        self.free.push(at);
    }

    /// One buffer as writable bytes, for gathering a write into it
    fn as_mut_slice(&mut self, at: u16) -> &mut [u8] {
        self.held[at as usize].as_mut_slice()
    }

    /// The address a submission names, which the allocation guarantees is aligned
    fn as_ptr(&self, at: u16) -> *mut u8 {
        self.held[at as usize].as_ptr()
    }

    /// The leading bytes a read landed in one buffer
    ///
    /// # Safety
    ///
    /// The count has to be one a completion reported, so the range named is exactly
    /// what the kernel filled.
    unsafe fn filled(&self, at: u16, count: usize) -> &[u8] {
        unsafe { self.held[at as usize].filled(count) }
    }
}

/// What a submitted op is waiting on, held until the ring hands it back
///
/// The buffers live here for the length of the flight, since the kernel fills them
/// after the submission returns. A record the kernel refuses is answered from here.
enum Pending {
    /// A write, holding the buffers it is copying out of
    Wrote { tag: Tag, bufs: Vec<WriteBuf> },

    /// A read into one buffer
    Read { tag: Tag, buf: ReadBuf },

    /// A read split across a header buffer and a payload buffer
    ReadSplit {
        tag: Tag,
        head: ReadBuf,
        body: ReadBuf,
    },

    /// A write gathered into a registered buffer, holding the caller's buffers too
    ///
    /// The framed count is what the caller asked to write and is told landed; the
    /// buffer rounds it up to whole blocks.
    StagedWrite {
        tag: Tag,
        staged: u16,
        bufs: Vec<WriteBuf>,
        framed: usize,
    },

    /// A read landing in a registered buffer, cut into the caller's on completion
    ///
    /// The read was widened to the blocks holding the range, so the bytes the caller
    /// asked for start `skip` into whatever lands.
    StagedRead {
        tag: Tag,
        staged: u16,
        skip: usize,
        buf: ReadBuf,
    },

    /// A split read landing in a registered buffer, cut into both on completion
    StagedSplit {
        tag: Tag,
        staged: u16,
        skip: usize,
        head: ReadBuf,
        body: ReadBuf,
    },
}

impl Pending {
    /// Turn a ring result into the completion the op's shape calls for
    ///
    /// The pool comes in because a staged op reads its bytes out of the buffer it
    /// flew through and hands that buffer back, neither of which it can do alone.
    fn complete(self, result: i32, buffers: &mut Buffers) -> Completion {
        let failed = ring_error(result);
        match self {
            Pending::Wrote { tag, bufs } => Completion {
                tag,
                outcome: Outcome::Wrote {
                    result: match failed {
                        Some(error) => Err(error),
                        None => Ok(result as u64),
                    },
                    bufs,
                },
            },
            Pending::Read { tag, mut buf } => {
                let filled = result.max(0) as usize;
                // The kernel wrote this many bytes into the buffer's room, which is
                // the one thing that makes committing them sound.
                unsafe { buf.commit(filled) };
                Completion {
                    tag,
                    outcome: Outcome::Read {
                        result: match failed {
                            Some(error) => Err(error),
                            None => Ok(filled),
                        },
                        buf,
                    },
                }
            }
            Pending::ReadSplit {
                tag,
                mut head,
                mut body,
            } => {
                let filled = result.max(0) as usize;
                let head_len = head.wanted();
                // A vectored read fills the first buffer before the second, so a
                // short read cuts the body and a shorter one cuts the head itself.
                unsafe {
                    head.commit(filled.min(head_len));
                    body.commit(filled.saturating_sub(head_len));
                }
                Completion {
                    tag,
                    outcome: Outcome::ReadSplit {
                        result: match failed {
                            Some(error) => Err(error),
                            None => Ok(filled),
                        },
                        head,
                        body,
                    },
                }
            }
            Pending::StagedWrite {
                tag,
                staged,
                bufs,
                framed,
            } => {
                buffers.release(staged);
                Completion {
                    tag,
                    outcome: Outcome::Wrote {
                        result: match failed {
                            Some(error) => Err(error),
                            None => Ok((result as u64).min(framed as u64)),
                        },
                        bufs,
                    },
                }
            }
            Pending::StagedRead {
                tag,
                staged,
                skip,
                mut buf,
            } => {
                let (from, to) = wanted_window(result.max(0) as usize, skip, buf.wanted());
                // Safety: the completion reported filling this many bytes from the
                // buffer's start, so the range named is exactly what the kernel wrote.
                let landed = unsafe { buffers.filled(staged, to) };
                let taken = cut_into(&landed[from..], &mut buf);
                buffers.release(staged);
                Completion {
                    tag,
                    outcome: Outcome::Read {
                        result: match failed {
                            Some(error) => Err(error),
                            None => Ok(taken),
                        },
                        buf,
                    },
                }
            }
            Pending::StagedSplit {
                tag,
                staged,
                skip,
                mut head,
                mut body,
            } => {
                let wanted = head.wanted() + body.wanted();
                let (from, to) = wanted_window(result.max(0) as usize, skip, wanted);
                // Safety: as above, the count came off the completion.
                let landed = unsafe { buffers.filled(staged, to) };
                let taken = cut_split_into(&landed[from..], &mut head, &mut body);
                buffers.release(staged);
                Completion {
                    tag,
                    outcome: Outcome::ReadSplit {
                        result: match failed {
                            Some(error) => Err(error),
                            None => Ok(taken),
                        },
                        head,
                        body,
                    },
                }
            }
        }
    }
}

/// One slot: what is in flight there, where its answer belongs, and its generation
struct Slot {
    /// Bumped on every reuse, so a completion for a flight that is over is stale
    generation: u32,

    /// The place in a batch's answers this op's completion goes back to
    order: u32,

    /// The op and the buffers it owns, held here until its completion returns
    pending: Option<Pending>,

    /// The iovec list the op in this slot handed the kernel
    iovecs: IoVecs,
}

/// One completion and the place in a batch's answers it belongs to
struct Reaped {
    order: u32,
    completion: Completion,
}

/// Ops in flight against one ring, each in the slot its completion names
///
/// A submission's user data carries the slot rather than a key to hash back, with
/// a generation in the high half so a completion for an op already taken cannot
/// land on whatever moved in. One slot per queue entry bounds the ops in flight.
struct Inflight {
    /// Every slot, live or free
    slots: Vec<Slot>,

    /// Slots carrying nothing, popped from the back
    free: Vec<u32>,

    /// Completions taken off the ring and not yet handed to whoever wanted them
    reaped: Vec<Reaped>,

    /// Reads out on the ring, which is what decides whether a wait spins or sleeps
    reads_out: usize,
}

impl Inflight {
    /// A slab with one slot per completion the ring can report
    fn with_capacity(entries: usize) -> Inflight {
        let mut slots = Vec::with_capacity(entries);
        let mut free = Vec::with_capacity(entries);
        for at in 0..entries {
            slots.push(Slot {
                generation: 0,
                order: UNORDERED,
                pending: None,
                iovecs: IoVecs::new(),
            });
            // Popped from the back, so the first ops land in the low slots and a
            // trace of the user data reads in the order it was submitted.
            free.push((entries - 1 - at) as u32);
        }
        Inflight {
            slots,
            free,
            reaped: Vec::new(),
            reads_out: 0,
        }
    }

    /// Whether every op out on this ring is a write
    fn holds_only_writes(&self) -> bool {
        self.reads_out == 0
    }

    /// Whether every slot is carrying an op
    fn is_full(&self) -> bool {
        self.free.is_empty()
    }

    /// Whether the kernel owes this ring nothing at all
    fn is_idle(&self) -> bool {
        self.free.len() == self.slots.len()
    }

    /// The slot the next insert will place an op in
    ///
    /// A vectored op points the kernel into its slot's iovec list, so the list is
    /// filled before the op is placed and one thread owns the slab throughout.
    fn next_free(&self) -> u32 {
        *self.free.last().expect("a slot is free before an insert")
    }

    /// The iovec list a slot lends the op it is about to carry
    fn iovecs_mut(&mut self, at: u32) -> &mut IoVecs {
        &mut self.slots[at as usize].iovecs
    }

    /// Place an op and return the user data its completion will carry
    fn insert(&mut self, pending: Pending, order: u32) -> u64 {
        if !matches!(pending, Pending::Wrote { .. }) {
            self.reads_out += 1;
        }
        let at = self.free.pop().expect("a slot is free before an insert");
        let slot = &mut self.slots[at as usize];
        slot.generation = slot.generation.wrapping_add(1);
        slot.order = order;
        slot.pending = Some(pending);
        (slot.generation as u64) << 32 | at as u64
    }

    /// Take the op a completion names, or nothing when it names no live op
    fn take(&mut self, user_data: u64) -> Option<(u32, Pending)> {
        let at = (user_data & 0xffff_ffff) as usize;
        let generation = (user_data >> 32) as u32;
        let slot = self.slots.get_mut(at)?;
        if slot.generation != generation {
            return None;
        }
        let pending = slot.pending.take()?;
        if !matches!(pending, Pending::Wrote { .. }) {
            self.reads_out -= 1;
        }
        let order = slot.order;
        self.free.push(at as u32);
        Some((order, pending))
    }

    /// Move the completions no batch is holding a place for into the output
    fn take_free(&mut self, out: &mut Vec<Completion>) -> usize {
        let mut taken = 0;
        let mut at = 0;
        while at < self.reaped.len() {
            if self.reaped[at].order != UNORDERED {
                at += 1;
                continue;
            }
            out.push(self.reaped.swap_remove(at).completion);
            taken += 1;
        }
        taken
    }

    /// Move the completions a batch is holding places for into its answers
    fn take_ordered(&mut self, filled: &mut [Option<Completion>]) -> usize {
        let mut taken = 0;
        let mut at = 0;
        while at < self.reaped.len() {
            if self.reaped[at].order == UNORDERED {
                at += 1;
                continue;
            }
            let reaped = self.reaped.swap_remove(at);
            filled[reaped.order as usize] = Some(reaped.completion);
            taken += 1;
        }
        taken
    }
}

/// Which descriptor a submission names
///
/// A plain one is resolved by the kernel per op; a registered one was handed over
/// once and is named by its slot, which is a table lookup against an index.
enum RingTarget {
    Plain(types::Fd),
    Registered(types::Fixed),
}

/// The registered descriptor table one ring holds, keyed by the handle not the fd
///
/// Identifiers are never reused, so only a close stales a slot, and a table built
/// against an older close count is given up whole before the next op. A
/// registration pins the file, which is why the table is bounded rather than grown.
struct Files {
    /// Whether the kernel took a descriptor table for this ring at all
    is_registered: bool,

    /// The volume's close count this table was built against
    generation: u64,

    /// The slot each handle was placed in
    held: HashMap<FileId, u32>,

    /// The handle the last op named, since ops arrive in runs against one file
    last: Option<(FileId, u32)>,

    /// The next slot to hand out, which only rewinds when the table is given up
    next: u32,
}

impl Files {
    /// An empty table, registered when the kernel took one
    fn new(is_registered: bool, generation: u64) -> Files {
        Files {
            is_registered,
            generation,
            held: HashMap::new(),
            last: None,
            next: 0,
        }
    }

    /// The descriptor a submission names, placing the file in a slot the first time
    fn target_for(
        &mut self,
        ring: &IoUring,
        file: FileId,
        posix: &PosixBackend,
    ) -> Result<RingTarget> {
        if !self.is_registered {
            return Ok(RingTarget::Plain(types::Fd(posix.fd_of(file)?)));
        }

        if let Some((held, slot)) = self.last {
            if held == file {
                return Ok(RingTarget::Registered(types::Fixed(slot)));
            }
        }
        if let Some(slot) = self.held.get(&file) {
            let slot = *slot;
            self.last = Some((file, slot));
            return Ok(RingTarget::Registered(types::Fixed(slot)));
        }

        let fd = posix.fd_of(file)?;
        let slot = self.next;
        if slot >= MAX_REGISTERED_FILES {
            return Ok(RingTarget::Plain(types::Fd(fd)));
        }
        if ring.submitter().register_files_update(slot, &[fd]).is_err() {
            return Ok(RingTarget::Plain(types::Fd(fd)));
        }
        self.next = slot + 1;
        self.held.insert(file, slot);
        self.last = Some((file, slot));
        Ok(RingTarget::Registered(types::Fixed(slot)))
    }

    /// Whether a close has moved the volume past this table
    fn is_stale(&self, posix: &PosixBackend) -> bool {
        self.is_registered && self.generation != posix.close_generation()
    }

    /// Hand every slot back, because a file somewhere on the volume has closed
    ///
    /// A refused clear is not a hazard, since a slot handed out again is registered
    /// again. The caller flushes first, since the kernel resolves a fixed slot at
    /// submission and a queued entry would read whatever moved in.
    fn give_up(&mut self, ring: &IoUring, generation: u64) {
        if self.next > 0 {
            let emptied = vec![-1; self.next as usize];
            let _ = ring.submitter().register_files_update(0, &emptied);
        }
        self.held.clear();
        self.last = None;
        self.next = 0;
        self.generation = generation;
    }
}

/// The eventfd an engine thread watches on its own ring, and whether it is armed
struct Kick {
    fd: RawFd,
    is_armed: bool,
}

/// One ring and the ops in flight against it, owned by one thread
///
/// Every method takes &mut self and there is no lock anywhere in it, which is what
/// a ring under SINGLE_ISSUER needs.
struct Ring {
    /// The kernel's own ring, submitted to and reaped from this thread alone
    ring: IoUring,

    /// The ops in flight against it, each in the slot its completion names
    inflight: Inflight,

    /// The descriptors registered with it, so an op names a slot rather than an fd
    files: Files,

    /// The buffers registered with it, declared after the ring so it drops first
    buffers: Buffers,

    /// Whether descriptors bypass the page cache, so no op carries a caller buffer
    is_direct: bool,

    /// User data of entries queued for the kernel, taken back when an enter fails
    queued: Vec<u64>,

    /// The inbox this ring watches, on an engine thread and nowhere else
    kick: Option<Kick>,

    /// Whether the kernel holds this ring's completion work, as the kernel answered
    /// and not as the tuning asked
    asks_for_completions: bool,

    /// The volume's door tally, which this ring's per-op decisions feed
    doors: Arc<DoorTally>,
}

impl Ring {
    /// Build this thread's ring under the volume's tuning
    fn new(core: &Core) -> Result<Ring> {
        let ring = ring_under(core.taskrun)?;
        // A kernel that will not take the table leaves the ring on plain
        // descriptors, so a refusal is a lost optimization not a lost volume.
        let is_registered = ring
            .submitter()
            .register_files_sparse(MAX_REGISTERED_FILES)
            .is_ok();
        if !is_registered {
            core.doors.note_files_refused();
        }
        let workers = core.tuning.iowq_workers;
        if workers.is_asked() && !cap_workers(&ring, workers) {
            core.doors.note_workers_refused();
        }
        if core.tuning.pinned_iowq && !pin_workers(&ring) {
            core.doors.note_workers_refused();
        }
        // The kernel clamps the completion queue, so the bound on ops in flight is
        // read back from the ring rather than assumed.
        let entries = ring.params().cq_entries() as usize;
        // A direct descriptor refuses the caller's own buffer. A buffered volume
        // registers none, since staging there would buy a copy for nothing.
        let buffers = match core.is_direct && core.tuning.registered_buffers {
            true => Buffers::register(&ring),
            false => Buffers::none(),
        };
        if core.is_direct && core.tuning.registered_buffers && !buffers.is_live() {
            core.doors.note_pool_refused();
        }
        Ok(Ring {
            ring,
            inflight: Inflight::with_capacity(entries),
            files: Files::new(is_registered, core.posix.close_generation()),
            buffers,
            is_direct: core.is_direct,
            queued: Vec::with_capacity(entries),
            kick: None,
            asks_for_completions: core.taskrun.is_asked_for(),
            doors: Arc::clone(&core.doors),
        })
    }

    /// Whether a thread spins for what the ring is holding rather than sleeping for it
    ///
    /// A write lands in page cache, and spinning for one is 8.1 us against 13.0 us at
    /// the commit p50. One read out is a device round trip, where the same spin burns
    /// 10.8x the cycles for nothing.
    ///
    /// Reads the ops out rather than `&self`, so the rule can be asserted without a
    /// kernel to build a ring on.
    fn spins_for(inflight: &Inflight) -> bool {
        inflight.holds_only_writes()
    }

    /// Whether the ring has room to take another op
    ///
    /// A direct op needs a registered buffer as much as a slot, so a starved pool
    /// is a ring with no room. A pool the kernel refused never says this.
    fn is_full(&self) -> bool {
        self.inflight.is_full() || self.buffers.is_starved()
    }

    /// Whether the kernel owes this ring nothing at all
    fn is_idle(&self) -> bool {
        self.inflight.is_idle()
    }

    /// Hand the kernel what is queued, answering what it refuses on the spot
    ///
    /// A busy ring is a full completion queue rather than a failure, so the answer
    /// is to empty it and ask again. An enter that fails outright consumed nothing,
    /// so the entries still queued are taken back and answered.
    fn flush(&mut self) -> bool {
        if self.ring.submission().is_empty() {
            self.queued.clear();
            return true;
        }
        loop {
            match self.ring.submit() {
                Ok(_) => {
                    self.queued.clear();
                    return true;
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) if error.raw_os_error() == Some(libc::EBUSY) && self.drain() > 0 => {}
                Err(error) => {
                    self.refuse(error.raw_os_error().unwrap_or(libc::EIO));
                    return false;
                }
            }
        }
    }

    /// Answer every entry the kernel never took with the error that stopped it
    fn refuse(&mut self, errno: i32) {
        let queued = std::mem::take(&mut self.queued);
        for user_data in queued {
            if let Some((order, pending)) = self.inflight.take(user_data) {
                let completion = pending.complete(-errno, &mut self.buffers);
                self.inflight.reaped.push(Reaped { order, completion });
            }
        }
    }

    /// Run the completion work the kernel is holding for this thread, if any
    ///
    /// The crate's submit asks only when it is also waiting for a completion, so a
    /// thread that means to peek has to ask by hand: this is the ask and not the wait.
    /// The mode is read before the flag because reading the flag borrows the submission
    /// queue, whose drop stores the tail back, and a spin comes through here every round.
    fn run_owed_work(&mut self) {
        if !self.asks_for_completions || !self.ring.submission().taskrun() {
            return;
        }
        // SAFETY: an enter submitting nothing and waiting for nothing, on the thread
        // that owns this ring, which is what the mode requires.
        let entered = unsafe {
            self.ring
                .submitter()
                .enter::<libc::sigset_t>(0, 0, EnterFlags::GETEVENTS.bits(), None)
        };
        if let Err(error) = entered {
            // The work stays queued and the flag stays up, so the next look asks again.
            tracing::debug!("the ring refused to run its own completion work: {error}");
        }
    }

    /// Move whatever the ring has finished into the reaped list
    ///
    /// A completion queue is shared memory, so this costs no syscall on a ring the
    /// kernel posts into as it goes. One holding its work is asked first, so every
    /// reader of the queue goes through here.
    fn drain(&mut self) -> usize {
        self.run_owed_work();
        let mut drained = 0;
        let mut has_kicked = false;
        let mut is_still_armed = false;
        {
            let mut queue = self.ring.completion();
            queue.sync();
            for cqe in &mut queue {
                if cqe.user_data() == KICK_TAG {
                    has_kicked = true;
                    is_still_armed = cqueue::more(cqe.flags());
                    continue;
                }
                if let Some((order, pending)) = self.inflight.take(cqe.user_data()) {
                    let completion = pending.complete(cqe.result(), &mut self.buffers);
                    self.inflight.reaped.push(Reaped { order, completion });
                    drained += 1;
                }
            }
        }
        if has_kicked {
            self.kicked(is_still_armed);
        }
        drained
    }

    /// Sleep until the ring has finished at least one of the ops it holds
    ///
    /// The wait hands the kernel what is queued on its way in, so it is the submission
    /// as much as the sleep. An interrupted or busy wait means look again; an enter
    /// that failed outright took nothing.
    fn park(&mut self) -> Result<()> {
        let waited = self.ring.submit_and_wait(1);
        match waited {
            Ok(_) => {
                self.queued.clear();
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {
                self.queued.clear();
                Ok(())
            }
            Err(error) if error.raw_os_error() == Some(libc::EBUSY) => {
                self.queued.clear();
                Ok(())
            }
            Err(error) => {
                self.refuse(error.raw_os_error().unwrap_or(libc::EIO));
                Err(ReelError::Io(error))
            }
        }
    }

    /// Wait for the ring to report at least one more completion
    ///
    /// The ops out own buffers the kernel may still be writing into, so a wait that
    /// fails keeps asking rather than taking that memory back.
    fn wait_more(&mut self) {
        if !Ring::spins_for(&self.inflight) {
            self.sleep_once();
            return;
        }
        // The spin never enters the kernel, so it has to hand the entries over itself
        // or the loop below would ask a queue that stays empty.
        self.flush();
        let mut rounds = 0u32;
        loop {
            if self.drain() > 0 {
                return;
            }
            rounds += 1;
            if rounds > MAX_SPIN_ROUNDS {
                // A spin this long is not a completion about to land, so the thread
                // sleeps for one rather than holding a core to find out.
                self.doors.note_spun_out();
                self.sleep_once();
                return;
            }
            if rounds > SPIN_ROUNDS {
                std::thread::yield_now();
            }
        }
    }

    /// Sleep in the kernel once and take whatever that woke it for
    fn sleep_once(&mut self) {
        if let Err(error) = self.park() {
            tracing::debug!("the ring refused a wait: {error}");
            std::thread::yield_now();
        }
        self.drain();
    }

    /// Take what has landed for a batch, reaping the queue first
    fn harvest(&mut self, filled: &mut [Option<Completion>]) -> usize {
        self.drain();
        self.inflight.take_ordered(filled)
    }

    /// Put one op on the ring, or answer it here when it cannot go there
    ///
    /// An op the ring does not serve and a handle it does not know are both facts
    /// about one op, so they answer on the posix path rather than fail the batch.
    fn stage(&mut self, op: Op, posix: &PosixBackend, order: u32) -> Option<Completion> {
        let file = match ring_file(&op) {
            Some(file) => file,
            None => {
                self.doors.note_off_ring(1);
                return Some(posix.dispatch(op));
            }
        };
        // A close anywhere retires the table, and queued entries still name its
        // slots, so they go over while the table they were built against stands.
        if self.files.is_stale(posix) {
            self.flush();
            let generation = posix.close_generation();
            self.files.give_up(&self.ring, generation);
        }
        let target = match self.files.target_for(&self.ring, file, posix) {
            Ok(target) => target,
            Err(_) => {
                self.doors.note_off_ring(1);
                return Some(posix.dispatch(op));
            }
        };

        // The submission is built against the slot the op is about to take, since a
        // vectored one points at that slot's iovec list, and the record goes in
        // before the entry because the kernel may complete the moment it does.
        let at = self.inflight.next_free();
        let built = match self.is_direct {
            true => build_staged(target, op, &mut self.buffers),
            false => Ok(build_entry(target, op, self.inflight.iovecs_mut(at))),
        };
        let (entry, pending) = match built {
            Ok(built) => built,
            // The pool cannot serve this one, so it goes where every direct op went
            // before there was a pool.
            Err(op) => {
                self.doors.note_off_ring(1);
                return Some(posix.dispatch(op));
            }
        };
        self.doors.note_ring();
        let user_data = self.inflight.insert(pending, order);
        let entry = entry.user_data(user_data);

        loop {
            // A full submission queue is not an error, only a queue that has to go
            // to the kernel before it can take more.
            //
            // SAFETY: the entry names buffers held for the whole flight, either the
            // caller's own or a registered buffer the record holds.
            if unsafe { self.ring.submission().push(&entry) }.is_ok() {
                self.queued.push(user_data);
                return None;
            }
            if !self.flush() {
                // The entry never reached the queue, so the record is taken back and
                // answered rather than left waiting for a completion that cannot come.
                let taken = self.inflight.take(user_data);
                return taken.map(|(_, pending)| pending.complete(-libc::EIO, &mut self.buffers));
            }
            self.ring.submission().sync();
        }
    }

    /// Watch an inbox on this ring, so one park sees the device and the callers
    fn watch(&mut self, fd: RawFd) {
        self.kick = Some(Kick {
            fd,
            is_armed: false,
        });
        self.arm();
    }

    /// Put the inbox poll back on the ring when it is not already there
    fn arm(&mut self) {
        let Some(kick) = &self.kick else {
            return;
        };
        if kick.is_armed {
            return;
        }
        let entry = opcode::PollAdd::new(types::Fd(kick.fd), libc::POLLIN as u32)
            .multi(true)
            .build()
            .user_data(KICK_TAG);
        loop {
            // SAFETY: the poll names a descriptor the inbox owns for as long as the
            // engine thread runs, and no buffer at all.
            if unsafe { self.ring.submission().push(&entry) }.is_ok() {
                break;
            }
            if !self.flush() {
                return;
            }
            self.ring.submission().sync();
        }
        if let Some(kick) = &mut self.kick {
            kick.is_armed = true;
        }
    }

    /// Take the counter a kick left, and note whether the poll stayed armed
    fn kicked(&mut self, is_still_armed: bool) {
        let Some(kick) = &mut self.kick else {
            return;
        };
        kick.is_armed = is_still_armed;
        let mut ticks = [0u8; 8];
        // SAFETY: an ffi read of the eight bytes an eventfd counter holds.
        let _ = unsafe { libc::read(kick.fd, ticks.as_mut_ptr().cast(), ticks.len()) };
    }
}

/// What every thread's ring for one backend is built from
///
/// Behind an Arc because the engine thread owns a ring of its own, and a thread
/// holding a ring for a closed volume drops it the next time it looks.
struct Core {
    posix: Arc<PosixBackend>,
    tuning: RingTuning,

    /// The completion mode this kernel took, settled once for every thread's ring
    taskrun: TaskRun,

    is_direct: bool,
    doors: Arc<DoorTally>,
}

/// Ops that reached a ring against ops that went to posix instead
///
/// Every fall-through is silent by design, which is fine until a leg reports a
/// ring number it never took.
#[derive(Default)]
struct DoorTally {
    reached_ring: AtomicBool,
    off_ring: AtomicU64,
    pool_refused: AtomicBool,
    files_refused: AtomicBool,
    workers_refused: AtomicBool,
    spun_out: AtomicU64,
}

impl DoorTally {
    /// One op went on a ring
    ///
    /// A load first, so the line stays shared once the flag is up and the hot
    /// path pays a predictable branch rather than a store per op.
    fn note_ring(&self) {
        if !self.reached_ring.load(Ordering::Relaxed) {
            self.reached_ring.store(true, Ordering::Relaxed);
        }
    }

    /// Ops that took another door
    fn note_off_ring(&self, ops: usize) {
        self.off_ring.fetch_add(ops as u64, Ordering::Relaxed);
    }

    /// A thread's pool was refused by the kernel
    fn note_pool_refused(&self) {
        if !self.pool_refused.load(Ordering::Relaxed) {
            self.pool_refused.store(true, Ordering::Relaxed);
        }
    }

    /// A spinning wait gave up and slept instead, which reads as the ring's completion
    /// mode and its wait no longer agreeing
    fn note_spun_out(&self) {
        self.spun_out.fetch_add(1, Ordering::Relaxed);
    }

    /// A ring's sparse file table was refused by the kernel
    fn note_files_refused(&self) {
        if !self.files_refused.load(Ordering::Relaxed) {
            self.files_refused.store(true, Ordering::Relaxed);
        }
    }

    /// A ring's worker cap or pin was refused by the kernel
    fn note_workers_refused(&self) {
        if !self.workers_refused.load(Ordering::Relaxed) {
            self.workers_refused.store(true, Ordering::Relaxed);
        }
    }

    fn counts(&self) -> DoorCounts {
        DoorCounts {
            reached_ring: self.reached_ring.load(Ordering::Relaxed),
            off_ring: self.off_ring.load(Ordering::Relaxed),
            pool_refused: self.pool_refused.load(Ordering::Relaxed),
            files_refused: self.files_refused.load(Ordering::Relaxed),
            workers_refused: self.workers_refused.load(Ordering::Relaxed),
        }
    }
}

thread_local! {
    /// Rings this thread owns, one for each backend it has submitted to
    ///
    /// A ring belongs to the thread that built it, so nothing locks one. An entry
    /// whose backend has been dropped goes the next time this thread looks.
    static RINGS: RefCell<Vec<Owned>> = const { RefCell::new(Vec::new()) };
}

/// One thread's ring for one backend, named by the core it was built from
struct Owned {
    core: Weak<Core>,
    ring: Ring,
}

/// Ops handed to the engine thread, each moved into a slot the inbox already owns
///
/// The driver claims a completion slot before it submits, so a push that finds no
/// room is a caller that submitted without claiming.
struct Inbox {
    /// The ops waiting for the engine thread, and what it is doing about them
    queue: Mutex<Queue>,

    /// Written to wake the engine thread out of its ring's wait
    kick: OwnedFd,
}

/// The ops waiting for the engine thread and what it is doing about them
struct Queue {
    /// One slot per op the driver can have claimed, so a push never allocates
    slots: Vec<Option<Op>>,

    /// Where the next take comes from
    head: usize,

    /// How many slots from the head are carrying an op
    len: usize,

    /// Whether the engine thread is in its ring's wait rather than working
    is_parked: bool,

    /// Whether the engine thread has been asked to stop
    is_stopping: bool,

    /// Whether the engine thread gave up, so a later caller is told rather than queued
    is_failed: bool,
}

impl Queue {
    /// An empty queue as wide as the completion slots that feed it
    fn new() -> Queue {
        let mut slots = Vec::with_capacity(SLOT_COUNT);
        for _ in 0..SLOT_COUNT {
            slots.push(None);
        }
        Queue {
            slots,
            head: 0,
            len: 0,
            is_parked: false,
            is_stopping: false,
            is_failed: false,
        }
    }

    /// Slots carrying nothing
    fn room(&self) -> usize {
        self.slots.len() - self.len
    }

    /// Move one op into the slot behind the last
    fn push(&mut self, op: Op) {
        let at = (self.head + self.len) % self.slots.len();
        self.slots[at] = Some(op);
        self.len += 1;
    }

    /// Move every op waiting here into the output, oldest first
    fn take_all(&mut self, out: &mut Vec<Op>) {
        for _ in 0..self.len {
            if let Some(op) = self.slots[self.head].take() {
                out.push(op);
            }
            self.head = (self.head + 1) % self.slots.len();
        }
        self.len = 0;
    }
}

impl Inbox {
    /// An inbox with an eventfd of its own, or why the kernel would not give one
    fn new() -> Result<Inbox> {
        // SAFETY: an ffi call taking two integers and answering with a descriptor.
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            return Err(ReelError::Io(std::io::Error::last_os_error()));
        }
        // SAFETY: the descriptor is fresh from the kernel and owned by nothing else.
        let kick = unsafe { OwnedFd::from_raw_fd(fd) };
        Ok(Inbox {
            queue: Mutex::new(Queue::new()),
            kick,
        })
    }

    /// The descriptor the engine thread's ring polls
    fn kick_fd(&self) -> RawFd {
        self.kick.as_raw_fd()
    }

    /// Hand the engine a whole batch, which costs one kick however wide it is
    fn hand(&self, ops: Vec<Op>) -> Result<()> {
        let should_kick = {
            let mut queue = lock(&self.queue);
            if queue.is_failed {
                return Err(engine_gone());
            }
            if queue.room() < ops.len() {
                return Err(inbox_full(ops.len(), queue.room()));
            }
            for op in ops {
                queue.push(op);
            }
            queue.is_parked
        };
        match should_kick {
            true => self.kick(),
            false => Ok(()),
        }
    }

    /// Hand the engine one op, taking the lock and the kick once as a batch does
    fn hand_one(&self, op: Op) -> Result<()> {
        let should_kick = {
            let mut queue = lock(&self.queue);
            if queue.is_failed {
                return Err(engine_gone());
            }
            if queue.room() == 0 {
                return Err(inbox_full(1, 0));
            }
            queue.push(op);
            queue.is_parked
        };
        match should_kick {
            true => self.kick(),
            false => Ok(()),
        }
    }

    /// Wake the engine thread out of its ring's wait
    fn kick(&self) -> Result<()> {
        let tick = 1u64.to_ne_bytes();
        // SAFETY: an ffi write of exactly the eight bytes an eventfd counter takes,
        // from a buffer of that width.
        let written =
            unsafe { libc::write(self.kick.as_raw_fd(), tick.as_ptr().cast(), tick.len()) };
        if written < 0 {
            let error = std::io::Error::last_os_error();
            // A counter that will not take another tick already has one pending,
            // which is the wake this was asking for.
            if error.kind() == std::io::ErrorKind::WouldBlock {
                return Ok(());
            }
            return Err(ReelError::Io(error));
        }
        Ok(())
    }

    /// Take everything waiting, and say whether the engine has been asked to stop
    fn take_all(&self, out: &mut Vec<Op>) -> bool {
        let mut queue = lock(&self.queue);
        queue.take_all(out);
        queue.is_stopping
    }

    /// Mark the engine parked, unless something arrived that it should see first
    fn should_park(&self) -> bool {
        let mut queue = lock(&self.queue);
        if queue.len > 0 || queue.is_stopping {
            return false;
        }
        queue.is_parked = true;
        true
    }

    /// Say the engine is working again
    fn unpark(&self) {
        lock(&self.queue).is_parked = false;
    }

    /// Say the engine gave up, so nothing else is queued for it
    fn fail(&self) {
        lock(&self.queue).is_failed = true;
    }

    /// Ask the engine to finish what it holds and stop
    fn stop(&self) -> Result<()> {
        lock(&self.queue).is_stopping = true;
        self.kick()
    }
}

/// The thread that owns a ring for callers who have no thread of their own
struct Engine {
    inbox: Arc<Inbox>,
    worker: Option<JoinHandle<()>>,
    sink: Arc<SlotTable>,
}

impl Engine {
    /// Start the engine thread, or report why it could not take a ring
    ///
    /// The ring is built on the thread that will own it, since SINGLE_ISSUER binds
    /// a ring to its creating task, so the outcome comes back over a channel.
    fn start(core: &Arc<Core>, sink: &Arc<SlotTable>, shard: usize) -> Result<Engine> {
        let inbox = Arc::new(Inbox::new()?);
        let (opened, answer) = channel();
        let worker = std::thread::Builder::new()
            .name(format!("reel-ring-{shard}"))
            .spawn({
                let core = Arc::clone(core);
                let inbox = Arc::clone(&inbox);
                let sink = Arc::clone(sink);
                move || serve(core, inbox, sink, opened)
            })
            .map_err(ReelError::Io)?;
        match answer.recv() {
            Ok(Ok(())) => Ok(Engine {
                inbox,
                worker: Some(worker),
                sink: Arc::clone(sink),
            }),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(engine_gone()),
        }
    }

    /// Hand the engine a batch, which costs one kick however wide it is
    fn hand(&self, ops: Vec<Op>, sink: &Arc<SlotTable>) -> Result<()> {
        debug_assert!(
            Arc::ptr_eq(&self.sink, sink),
            "two drivers over one ring backend file into one table"
        );
        self.inbox.hand(ops)
    }

    /// Hand the engine one op, the awaited door's own arm
    fn hand_one(&self, op: Op, sink: &Arc<SlotTable>) -> Result<()> {
        debug_assert!(
            Arc::ptr_eq(&self.sink, sink),
            "two drivers over one ring backend file into one table"
        );
        self.inbox.hand_one(op)
    }
}

impl Drop for Engine {
    /// Stop the thread and wait for it, so nothing outlives the backend
    fn drop(&mut self) {
        let _ = self.inbox.stop();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

/// Run one ring for every caller that has no thread to run one with
///
/// The inbox's poll sits on the same ring, so the park is the only place the loop
/// sleeps and either side wakes it.
fn serve(core: Arc<Core>, inbox: Arc<Inbox>, sink: Arc<SlotTable>, opened: Sender<Result<()>>) {
    let mut ring = match Ring::new(&core) {
        Ok(ring) => ring,
        Err(error) => {
            let _ = opened.send(Err(error));
            return;
        }
    };
    ring.watch(inbox.kick_fd());
    ring.flush();
    if opened.send(Ok(())).is_err() {
        return;
    }

    let mut taken: Vec<Op> = Vec::new();
    let mut drained: Vec<Completion> = Vec::new();
    loop {
        let is_stopping = inbox.take_all(&mut taken);
        if !taken.is_empty() {
            deliver(&mut ring, &mut taken, &core.posix, &mut drained);
        }
        ring.drain();
        ring.inflight.take_free(&mut drained);
        if !drained.is_empty() {
            sink.file(&mut drained);
            continue;
        }
        if is_stopping && ring.is_idle() {
            return;
        }
        ring.arm();
        ring.flush();
        if !inbox.should_park() {
            continue;
        }
        if let Err(error) = ring.park() {
            tracing::debug!("the engine ring refused a wait: {error}");
            inbox.fail();
            return;
        }
        inbox.unpark();
    }
}

/// Put a batch on the engine's ring, answering off ring ops on the engine thread
///
/// An op the kernel has to block to serve runs here rather than on the caller, so
/// the async door's promise holds for every op it takes.
fn deliver(
    ring: &mut Ring,
    ops: &mut Vec<Op>,
    posix: &PosixBackend,
    drained: &mut Vec<Completion>,
) {
    for op in ops.drain(..) {
        while ring.is_full() {
            ring.drain();
            ring.inflight.take_free(drained);
            if ring.is_full() {
                ring.wait_more();
                ring.inflight.take_free(drained);
            }
        }
        if let Some(completion) = ring.stage(op, posix, UNORDERED) {
            drained.push(completion);
        }
    }
    ring.flush();
    ring.inflight.take_free(drained);
}

/// Wait until the slab has a free slot, taking what lands for the batch on the way
///
/// What is queued goes over inside the wait rather than in front of it, since both
/// the sleep and the spin hand the entries to the kernel on their own.
fn make_room(ring: &mut Ring, filled: &mut [Option<Completion>]) -> usize {
    let mut taken = 0;
    while ring.is_full() {
        taken += ring.harvest(filled);
        if ring.is_full() {
            ring.wait_more();
            taken += ring.harvest(filled);
        }
    }
    taken
}

/// Run a whole batch on this thread's ring and answer it in submit order
///
/// The thread that submits is the thread that waits and reaps: no lock, no
/// handoff, no completion delivered to a thread that did not ask for it.
fn run_batch(ring: &mut Ring, ops: &mut Vec<Op>, posix: &PosixBackend, out: &mut Vec<Completion>) {
    let mut filled: Vec<Option<Completion>> = Vec::new();
    filled.resize_with(ops.len(), || None);
    let mut outstanding = 0usize;

    for (order, op) in ops.drain(..).enumerate() {
        // A ring with more out than its completion queue can report can lose one, so
        // a full slab waits rather than growing.
        outstanding = outstanding.saturating_sub(make_room(ring, &mut filled));
        match ring.stage(op, posix, order as u32) {
            Some(completion) => filled[order] = Some(completion),
            None => outstanding += 1,
        }
    }

    while outstanding > 0 {
        let taken = ring.harvest(&mut filled);
        if taken == 0 {
            ring.wait_more();
            continue;
        }
        outstanding = outstanding.saturating_sub(taken);
    }

    out.reserve(filled.len());
    for completion in filled.into_iter().flatten() {
        out.push(completion);
    }
}

/// Run one op on this thread's ring and wait for exactly its completion
fn run_one(ring: &mut Ring, op: Op, posix: &PosixBackend) -> Completion {
    let mut filled: [Option<Completion>; 1] = [None];
    make_room(ring, &mut filled);
    if let Some(completion) = ring.stage(op, posix, 0) {
        return completion;
    }
    loop {
        if let Some(completion) = filled[0].take() {
            return completion;
        }
        if ring.harvest(&mut filled) == 0 {
            ring.wait_more();
        }
    }
}

/// Stage a batch without waiting for it, for a caller that polls this ring itself
fn queue_batch(ring: &mut Ring, ops: Vec<Op>, posix: &PosixBackend) {
    for op in ops {
        while ring.is_full() {
            if ring.drain() == 0 && ring.is_full() {
                ring.wait_more();
            }
        }
        if let Some(completion) = ring.stage(op, posix, UNORDERED) {
            ring.inflight.reaped.push(Reaped {
                order: UNORDERED,
                completion,
            });
        }
    }
    ring.flush();
}

/// Ring backend: data plane on rings owned by their threads, control plane on posix
pub struct UringBackend {
    /// The volume's tuning and the posix backend the control plane runs on
    core: Arc<Core>,

    /// One engine per shard, each started the first time a caller lands on it
    engines: Vec<OnceLock<Engine>>,
}

thread_local! {
    /// This thread's place in the engine fan-out, taken once and kept
    ///
    /// A thread keeps its shard for life, so one engine's descriptor table stays
    /// warm. Arrival order rather than hashing puts the first N threads on N
    /// distinct engines.
    static SHARD: Cell<Option<usize>> = const { Cell::new(None) };
}

/// Shards handed out so far, which is the next caller's place in the fan-out
static SHARDS_TAKEN: AtomicUsize = AtomicUsize::new(0);

/// The shard this thread submits through, taken on its first async submit
fn shard_of(count: usize) -> usize {
    SHARD.with(|held| {
        let taken = match held.get() {
            Some(taken) => taken,
            None => {
                let taken = SHARDS_TAKEN.fetch_add(1, Ordering::Relaxed);
                held.set(Some(taken));
                taken
            }
        };
        taken % count
    })
}

/// Build one ring under one completion mode
///
/// A setup the kernel refuses is reported rather than retried, so an operator
/// finds out that the volume is not on a ring.
fn ring_under(taskrun: TaskRun) -> Result<IoUring> {
    let mut builder = IoUring::builder();
    // The kernel clamps an oversized request rather than refusing it, so asking past
    // the limit is how a ring ends up as deep as the machine allows.
    builder.setup_clamp();
    // A ring belongs to the thread that built it, so the deferred mode below can rest
    // on the promise this flag makes.
    builder.setup_single_issuer();
    match taskrun {
        TaskRun::Deferred => {
            builder.setup_defer_taskrun();
        }
        TaskRun::Cooperative => {
            builder.setup_coop_taskrun();
        }
        TaskRun::Interrupt => {}
    }
    // Both modes leave the work queued rather than run, so the ring has to say when
    // it is holding some or a thread reading its own queue would read an old one.
    if taskrun.is_asked_for() {
        builder.setup_taskrun_flag();
    }
    builder.build(RING_ENTRIES).map_err(ReelError::Io)
}

/// Build one ring under the best completion mode this kernel will take
///
/// A refusal is one errno with nothing in it naming the flag, so the step down is by
/// trial: a kernel too old for deferred work gets cooperative, then plain.
fn build_ring(wanted: TaskRun) -> Result<(IoUring, TaskRun)> {
    let mut refused = None;
    for &taskrun in wanted.and_below() {
        match ring_under(taskrun) {
            Ok(ring) => {
                if taskrun != wanted {
                    tracing::debug!(
                        "this kernel refused {wanted:?} completion work, \
                         so the ring runs {taskrun:?}"
                    );
                }
                return Ok((ring, taskrun));
            }
            Err(error) => refused = Some(error),
        }
    }
    Err(refused
        .unwrap_or_else(|| ReelError::Io(std::io::Error::other("no completion mode was tried"))))
}

/// Cap the kernel workers behind a ring, refused on a kernel before 5.15
fn cap_workers(ring: &IoUring, workers: IowqWorkers) -> bool {
    let mut caps = [workers.bounded, workers.unbounded];
    ring.submitter()
        .register_iowq_max_workers(&mut caps)
        .is_ok()
}

/// Keep a ring's kernel workers on the cores its thread may run on
fn pin_workers(ring: &IoUring) -> bool {
    // SAFETY: a zeroed set is an empty cpu set, which the kernel fills in place
    let mut cpus: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::cpu_set_t>();
    // SAFETY: the set is sized by its own type and outlives the call
    if unsafe { libc::sched_getaffinity(0, size, &mut cpus) } != 0 {
        return false;
    }
    ring.submitter().register_iowq_aff(&cpus).is_ok()
}

impl std::fmt::Debug for UringBackend {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.debug_struct("UringBackend")
            .field("direct", &self.core.is_direct)
            .finish()
    }
}

impl UringBackend {
    /// Build a ring backend, or report why the ring could not be set up
    ///
    /// Rings are built by the threads that own them, so this one setup proves the
    /// kernel takes the tuning at all rather than failing on the first put.
    pub fn new(is_direct: bool, tuning: RingTuning) -> Result<UringBackend> {
        // The mode settles here rather than per ring, so a kernel that refuses the
        // one asked for is found out once instead of by every thread in turn.
        let (probe, taskrun) = build_ring(tuning.taskrun)?;
        drop(probe);
        // One engine per thread the machine can run, which is where an async caller
        // lands: a machine that will not say its width gets one.
        let shards = std::thread::available_parallelism()
            .map(|width| width.get())
            .unwrap_or(1);
        Ok(UringBackend {
            core: Arc::new(Core {
                // The descriptor table is the inner backend's, so a direct volume's
                // opens carry the flag from here.
                posix: Arc::new(PosixBackend::with_direct(is_direct)),
                tuning,
                taskrun,
                is_direct,
                doors: Arc::new(DoorTally::default()),
            }),
            engines: (0..shards).map(|_| OnceLock::new()).collect(),
        })
    }

    /// Ops the posix path under this ring has answered
    pub fn ops(&self) -> u64 {
        self.core.posix.ops()
    }

    /// Spinning waits on this volume that gave up and slept instead
    ///
    /// Zero is the working answer; anything else is a spin that asked a queue no
    /// completion could reach.
    pub fn spin_outs(&self) -> u64 {
        self.core.doors.spun_out.load(Ordering::Relaxed)
    }

    /// Run something against this thread's ring, building it the first time
    ///
    /// A thread that cannot build a ring is handed nothing and answers on the posix
    /// path.
    fn on_ring<Out>(&self, act: impl FnOnce(Option<&mut Ring>) -> Out) -> Out {
        RINGS.with(|held| {
            let mut held = held.borrow_mut();
            held.retain(|owned| owned.core.strong_count() > 0);
            let mine = held
                .iter()
                .position(|owned| std::ptr::eq(owned.core.as_ptr(), Arc::as_ptr(&self.core)));
            let at = match mine {
                Some(at) => at,
                None => match Ring::new(&self.core) {
                    Ok(ring) => {
                        held.push(Owned {
                            core: Arc::downgrade(&self.core),
                            ring,
                        });
                        held.len() - 1
                    }
                    Err(error) => {
                        tracing::debug!("this thread has no ring, using the posix path: {error}");
                        return act(None);
                    }
                },
            };
            act(Some(&mut held[at].ring))
        })
    }

    /// Run something against this thread's ring only if it already has one
    fn on_open_ring<Out>(&self, act: impl FnOnce(&mut Ring) -> Out) -> Option<Out> {
        RINGS.with(|held| {
            let mut held = held.borrow_mut();
            let at = held
                .iter()
                .position(|owned| std::ptr::eq(owned.core.as_ptr(), Arc::as_ptr(&self.core)))?;
            Some(act(&mut held[at].ring))
        })
    }

    /// This thread's engine, started the first time a caller lands on its shard
    ///
    /// A deployment that never takes the async door starts none, and one that takes
    /// it from four threads starts four rather than the fan-out's whole width.
    fn engine(&self, sink: &Arc<SlotTable>) -> Result<&Engine> {
        let shard = shard_of(self.engines.len());
        let held = &self.engines[shard];
        if let Some(engine) = held.get() {
            return Ok(engine);
        }
        let started = Engine::start(&self.core, sink, shard)?;
        // Two callers can arrive at once and only one engine is kept. The loser's
        // thread is stopped by the drop of the value the set hands back.
        drop(held.set(started));
        held.get().ok_or_else(engine_gone)
    }

    /// Whether this volume's data ops go on a ring at all
    ///
    /// A direct volume's ops fly through the ring's registered buffers, since a
    /// direct descriptor refuses the caller's own. An operator who turns those off
    /// closes the door here rather than having every op walk in to be handed back.
    fn takes_ring(&self) -> bool {
        !self.core.is_direct || self.core.tuning.registered_buffers
    }
}

impl ReelIo for UringBackend {
    /// A ring, told apart by the descriptors its core opened
    ///
    /// This backend only exists when `UringBackend::new` set a ring up, so
    /// answering ring here cannot outlive the ring it names.
    fn serving(&self) -> ServingBackend {
        match self.core.is_direct {
            true => ServingBackend::RingDirect,
            false => ServingBackend::Ring,
        }
    }

    /// Which door this volume's ops took, which a ring leg has to be able to ask
    fn door_counts(&self) -> DoorCounts {
        self.core.doors.counts()
    }

    /// Flushes this volume asked the drive for, counted where they are issued
    ///
    /// A ring volume syncs through the inner backend like every other control op,
    /// so the count lives there and this forwards it.
    fn sync_count(&self) -> u64 {
        PosixBackend::sync_count(&self.core.posix)
    }

    /// Nanoseconds spent waiting inside those flushes, from the same place
    fn sync_nanos(&self) -> u64 {
        PosixBackend::sync_nanos(&self.core.posix)
    }

    /// The warm probe reads the descriptor table this backend already shares
    ///
    /// The inner backend owns the descriptors every ring op names, so the probe is
    /// the same non-blocking read of the same file. A direct volume's inner backend
    /// refuses it, since there is no page cache there to ask.
    fn warm_split(
        &self,
        file: FileId,
        offset: u64,
        head: &mut ReadBuf,
        body: &mut ReadBuf,
    ) -> bool {
        PosixBackend::warm_split(&self.core.posix, file, offset, head, body)
    }

    fn submit(&self, ops: Vec<Op>) -> Result<()> {
        if !self.takes_ring() {
            self.core.doors.note_off_ring(ops.len());
            return self.core.posix.submit(ops);
        }
        let mut on_ring = Vec::with_capacity(ops.len());
        let mut off_ring = Vec::new();
        for op in ops {
            match ring_file(&op) {
                Some(_) => on_ring.push(op),
                None => off_ring.push(op),
            }
        }
        if !on_ring.is_empty() {
            self.on_ring(|ring| match ring {
                Some(ring) => queue_batch(ring, on_ring, &self.core.posix),
                // No ring on this thread, so ops that named one go with the rest.
                None => off_ring.append(&mut on_ring),
            });
        }
        if !off_ring.is_empty() {
            self.core.doors.note_off_ring(off_ring.len());
            self.core.posix.submit(off_ring)?;
        }
        Ok(())
    }

    /// Service one op on this thread's own ring and hand its completion back
    ///
    /// Waiting here is what a ring per thread makes safe: the thread that submitted
    /// is the thread that waits, and nothing is shared with another.
    fn submit_inline(&self, op: Op) -> std::result::Result<Completion, Op> {
        if !self.takes_ring() || ring_file(&op).is_none() {
            self.core.doors.note_off_ring(1);
            return Ok(self.core.posix.dispatch(op));
        }
        Ok(self.on_ring(|ring| match ring {
            Some(ring) => run_one(ring, op, &self.core.posix),
            None => {
                self.core.doors.note_off_ring(1);
                self.core.posix.dispatch(op)
            }
        }))
    }

    /// Service a whole batch on this thread's own ring, in submit order
    fn submit_batch(&self, ops: &mut Vec<Op>, out: &mut Vec<Completion>) -> bool {
        if !self.takes_ring() {
            self.core.doors.note_off_ring(ops.len());
            return self.core.posix.submit_batch(ops, out);
        }
        self.on_ring(|ring| match ring {
            Some(ring) => run_batch(ring, ops, &self.core.posix, out),
            None => {
                self.core.doors.note_off_ring(ops.len());
                for op in ops.drain(..) {
                    out.push(self.core.posix.dispatch(op));
                }
            }
        });
        true
    }

    /// Hand a batch to the engine thread, for a caller with no thread of its own
    ///
    /// The completions come back through the slot table where the future left its
    /// waker, so nothing here waits or runs on the caller.
    fn submit_detached(&self, ops: Vec<Op>, sink: &Arc<SlotTable>) -> Result<()> {
        if !self.takes_ring() {
            self.core.doors.note_off_ring(ops.len());
            return self.core.posix.submit_detached(ops, sink);
        }
        self.engine(sink)?.hand(ops, sink)
    }

    /// Hand the engine one op, which is what the awaited door sends
    ///
    /// The inbox takes ops one at a time on the way in either way, so a single op
    /// goes down its own arm rather than in a vector built to be taken apart.
    fn submit_detached_one(&self, op: Op, sink: &Arc<SlotTable>) -> Result<()> {
        if !self.takes_ring() {
            self.core.doors.note_off_ring(1);
            return self.core.posix.submit_detached_one(op, sink);
        }
        self.engine(sink)?.hand_one(op, sink)
    }

    fn poll(&self, out: &mut Vec<Completion>) -> Result<usize> {
        let mut drained = self.core.posix.poll(out)?;
        if let Some(taken) = self.on_open_ring(|ring| {
            ring.drain();
            ring.inflight.take_free(out)
        }) {
            drained += taken;
        }
        Ok(drained)
    }

    /// Whether a wait on this volume's rings ever sleeps rather than spinning
    ///
    /// A wait sleeps for a read and spins for a write, so it counts as a door that parks.
    fn parks_on_wait(&self) -> bool {
        true
    }

    /// Sleep on this thread's own completion queue rather than asking it in a loop
    ///
    /// A thread with nothing outstanding has nothing to sleep for, and a ring that
    /// owes it nothing would hold the wait forever.
    fn poll_blocking(&self, out: &mut Vec<Completion>) -> Result<usize> {
        let drained = self.poll(out)?;
        if drained > 0 {
            return Ok(drained);
        }
        self.on_open_ring(|ring| {
            if !ring.is_idle() {
                ring.wait_more();
            }
        });
        self.poll(out)
    }
}

/// Bytes one ring submission may move
///
/// The kernel serves at most this much per call, and a submission is one call with
/// nothing behind it to issue the rest, so a wider op would come back short and be
/// taken for a whole one. Those go to the posix backend, which loops.
const RING_SPAN_CAP: u64 = 0x7fff_f000;

/// Bytes a write hands the ring before it is worth more as a blocking call
///
/// Every write reel issues waits for its own completion, so the ring's part is the
/// batching, and a write this wide has nothing to batch with. Set to the direct
/// door's own ceiling, so the two doors agree on what a ring write is.
const RING_WRITE_CAP: u64 = DIRECT_REQUEST_BYTES as u64;

/// Bytes a vectored write hands over across all its buffers
fn write_span(bufs: &[WriteBuf]) -> u64 {
    let mut span = 0u64;
    for buf in bufs {
        span += buf.len() as u64;
    }
    span
}

/// The file a ring op works on, or nothing when the op is not one
///
/// Only a write and the two reads qualify: an op the kernel has to block to serve
/// gains nothing but a worker thread holding the same wait.
fn ring_file(op: &Op) -> Option<FileId> {
    match op {
        // A write past the iovec cap is refused whole with EINVAL and a submission
        // has nowhere to split it, so it goes to posix, which walks it in capped
        // calls.
        Op::Writev { bufs, .. } if bufs.len() > MAX_IOVECS => None,
        Op::Writev { bufs, .. } if write_span(bufs) > RING_WRITE_CAP => None,
        Op::Pread { buf, .. } | Op::PreadCold { buf, .. }
            if buf.wanted() as u64 > RING_SPAN_CAP =>
        {
            None
        }
        Op::PreadSplit { head, body, .. }
            if (head.wanted() + body.wanted()) as u64 > RING_SPAN_CAP =>
        {
            None
        }
        Op::Writev { file, .. }
        | Op::Pread { file, .. }
        | Op::PreadCold { file, .. }
        | Op::PreadSplit { file, .. } => Some(*file),
        _ => None,
    }
}

/// Build the submission an op takes and the record that keeps its buffers alive
///
/// The buffers move into the record and the ring is handed their addresses through
/// the slot's iovec list. The user data is stamped on later.
fn build_entry(
    target: RingTarget,
    op: Op,
    iovecs: &mut IoVecs,
) -> (io_uring::squeue::Entry, Pending) {
    // The opcode builders are generic over how a descriptor is named, so the choice
    // cannot be handed over as a value and is made at each site.
    macro_rules! on_target {
        (|$fd:ident| $build:expr) => {
            match target {
                RingTarget::Plain($fd) => $build,
                RingTarget::Registered($fd) => $build,
            }
        };
    }

    match op {
        Op::Writev {
            tag, offset, bufs, ..
        } => {
            iovecs.refill(
                bufs.iter()
                    .map(|buf| (buf.as_slice().as_ptr() as *mut libc::c_void, buf.len())),
            );
            let entry = on_target!(|fd| opcode::Writev::new(fd, iovecs.as_ptr(), iovecs.len())
                .offset(offset)
                .build());
            (entry, Pending::Wrote { tag, bufs })
        }
        // The routed read takes the buffered descriptor and ignores the direct one
        // it carries, which is safe because it reads the descriptor the ring
        // registered rather than a plane the ring does not know about.
        Op::Pread {
            tag, offset, buf, ..
        }
        | Op::PreadCold {
            tag, offset, buf, ..
        } => {
            let mut buf = buf;
            let (ptr, len) = buf.as_mut_ptr();
            let entry = on_target!(|fd| opcode::Read::new(fd, ptr, len as u32)
                .offset(offset)
                .build());
            (entry, Pending::Read { tag, buf })
        }
        Op::PreadSplit {
            tag,
            offset,
            head,
            body,
            ..
        } => {
            let (mut head, mut body) = (head, body);
            let (head_ptr, head_len) = head.as_mut_ptr();
            let (body_ptr, body_len) = body.as_mut_ptr();
            iovecs.refill([
                (head_ptr as *mut libc::c_void, head_len),
                (body_ptr as *mut libc::c_void, body_len),
            ]);
            let entry = on_target!(|fd| opcode::Readv::new(fd, iovecs.as_ptr(), iovecs.len())
                .offset(offset)
                .build());
            (entry, Pending::ReadSplit { tag, head, body })
        }
        other => unreachable!("an off ring op reached the ring: {other:?}"),
    }
}

/// The span a direct op takes in a registered buffer, or nothing when it takes none
///
/// A write is already framed on a boundary, so all it owes the buffer is the
/// rounding; a read is aligned to nothing and is widened to the blocks that
/// contain it. A write starting off a boundary is refused rather than staged.
fn staged_span(op: &Op) -> Option<usize> {
    let (offset, wanted) = match op {
        Op::Writev { offset, bufs, .. } => {
            let total: usize = bufs.iter().map(|buf| buf.len()).sum();
            if total == 0 || !offset.is_multiple_of(DIRECT_ALIGN as u64) {
                return None;
            }
            return Some(align_up(total as u64) as usize);
        }
        Op::Pread { offset, buf, .. } | Op::PreadCold { offset, buf, .. } => {
            (*offset, buf.wanted())
        }
        Op::PreadSplit {
            offset, head, body, ..
        } => (*offset, head.wanted() + body.wanted()),
        _ => return None,
    };
    let (_, span) = covering_span(offset, wanted as u64);
    if span == 0 {
        return None;
    }
    Some(span as usize)
}

/// Build the submission a direct op takes through one of the ring's own buffers
///
/// A write is gathered into a registered buffer before its entry goes in and a
/// read lands in one and is cut into the caller's on completion. The op comes back
/// untouched when the pool cannot serve it.
fn build_staged(
    target: RingTarget,
    op: Op,
    buffers: &mut Buffers,
) -> std::result::Result<(io_uring::squeue::Entry, Pending), Op> {
    let Some(span) = staged_span(&op) else {
        return Err(op);
    };
    let Some(staged) = buffers.claim(span) else {
        return Err(op);
    };
    Ok(match op {
        Op::Writev {
            tag, offset, bufs, ..
        } => {
            let mut at = 0usize;
            let bytes = buffers.as_mut_slice(staged);
            for buf in &bufs {
                let held = buf.as_slice();
                bytes[at..at + held.len()].copy_from_slice(held);
                at += held.len();
            }
            // Only the rounding tail is left unwritten and it reaches the device,
            // so it is the one part that has to be zeroed.
            bytes[at..span].fill(0);
            let entry = write_fixed(target, buffers.as_ptr(staged), span, staged, offset);
            (
                entry,
                Pending::StagedWrite {
                    tag,
                    staged,
                    bufs,
                    framed: at,
                },
            )
        }
        Op::Pread {
            tag, offset, buf, ..
        }
        | Op::PreadCold {
            tag, offset, buf, ..
        } => {
            let (start, _) = covering_span(offset, buf.wanted() as u64);
            let entry = read_fixed(target, buffers.as_ptr(staged), span, staged, start);
            (
                entry,
                Pending::StagedRead {
                    tag,
                    staged,
                    skip: (offset - start) as usize,
                    buf,
                },
            )
        }
        Op::PreadSplit {
            tag,
            offset,
            head,
            body,
            ..
        } => {
            let wanted = (head.wanted() + body.wanted()) as u64;
            let (start, _) = covering_span(offset, wanted);
            let entry = read_fixed(target, buffers.as_ptr(staged), span, staged, start);
            (
                entry,
                Pending::StagedSplit {
                    tag,
                    staged,
                    skip: (offset - start) as usize,
                    head,
                    body,
                },
            )
        }
        other => unreachable!("an op with no staged span took a registered buffer: {other:?}"),
    })
}

/// A read into a registered buffer, which names the buffer by index not by address
///
/// Matched at the site, since the opcode builders are generic over how a
/// descriptor is named and the choice cannot be handed over as a value.
fn read_fixed(
    target: RingTarget,
    buf: *mut u8,
    len: usize,
    index: u16,
    offset: u64,
) -> io_uring::squeue::Entry {
    match target {
        RingTarget::Plain(fd) => opcode::ReadFixed::new(fd, buf, len as u32, index)
            .offset(offset)
            .build(),
        RingTarget::Registered(fd) => opcode::ReadFixed::new(fd, buf, len as u32, index)
            .offset(offset)
            .build(),
    }
}

/// A write out of a registered buffer, named the same way
fn write_fixed(
    target: RingTarget,
    buf: *mut u8,
    len: usize,
    index: u16,
    offset: u64,
) -> io_uring::squeue::Entry {
    match target {
        RingTarget::Plain(fd) => opcode::WriteFixed::new(fd, buf, len as u32, index)
            .offset(offset)
            .build(),
        RingTarget::Registered(fd) => opcode::WriteFixed::new(fd, buf, len as u32, index)
            .offset(offset)
            .build(),
    }
}

/// The error a negative ring result carries, or nothing when the op succeeded
fn ring_error(result: i32) -> Option<ReelError> {
    if result >= 0 {
        return None;
    }
    Some(ReelError::Io(std::io::Error::from_raw_os_error(-result)))
}

/// The error a caller gets when the engine thread is not there to take its ops
fn engine_gone() -> ReelError {
    ReelError::Io(std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        "the ring's engine thread is gone",
    ))
}

/// The error a caller gets for handing over more ops than it claimed slots for
fn inbox_full(wanted: usize, room: usize) -> ReelError {
    ReelError::Rejected(format!(
        "the engine inbox has room for {room} ops and was handed {wanted}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::op::{Advice, SyncRangeMode};

    fn tag() -> Tag {
        Tag(1)
    }

    fn file() -> FileId {
        FileId(1)
    }

    fn pending() -> Pending {
        Pending::Read {
            tag: tag(),
            buf: ReadBuf::new(1),
        }
    }

    fn read_op(tag: u64) -> Op {
        Op::Pread {
            tag: Tag(tag),
            file: file(),
            offset: 0,
            buf: ReadBuf::new(1),
        }
    }

    fn wrote() -> Pending {
        Pending::Wrote {
            tag: tag(),
            bufs: Vec::new(),
        }
    }

    fn staged_write() -> Pending {
        Pending::StagedWrite {
            tag: tag(),
            staged: 0,
            bufs: Vec::new(),
            framed: 0,
        }
    }

    // the wait spins only while the ring holds nothing but writes
    #[test]
    fn the_wait_follows_what_the_ring_holds() {
        let mut inflight = Inflight::with_capacity(4);
        assert!(Ring::spins_for(&inflight), "an empty ring holds no read");

        let write = inflight.insert(wrote(), UNORDERED);
        assert!(Ring::spins_for(&inflight), "a write is worth spinning for");

        let read = inflight.insert(pending(), UNORDERED);
        assert!(!Ring::spins_for(&inflight), "one read out ends the spin");

        assert!(inflight.take(write).is_some());
        assert!(
            !Ring::spins_for(&inflight),
            "the read is still out after the write came back"
        );
        assert!(inflight.take(read).is_some());
        assert!(Ring::spins_for(&inflight), "the read came back");
    }

    // a refused entry takes the read count it carried back with it
    #[test]
    fn a_taken_back_read_stops_counting() {
        let mut inflight = Inflight::with_capacity(2);
        let read = inflight.insert(pending(), UNORDERED);
        assert!(!inflight.holds_only_writes());
        assert!(inflight.take(read).is_some());
        assert!(inflight.holds_only_writes());
        assert!(
            inflight.take(read).is_none(),
            "a second take names a flight that is over and counts nothing"
        );
        assert!(inflight.holds_only_writes());
    }

    /// Shards come from one counter, so the tests that read it take turns
    static PLACES: Mutex<()> = Mutex::new(());

    // the first threads to submit take distinct shards
    #[test]
    fn callers_spread_across_the_shards() {
        let _held = lock(&PLACES);
        let width = 4;
        let mut taken: Vec<usize> = Vec::new();
        for _ in 0..width {
            let seen = std::thread::spawn(move || shard_of(width))
                .join()
                .expect("shard");
            taken.push(seen);
        }
        taken.sort_unstable();
        taken.dedup();
        assert_eq!(taken.len(), width, "four threads reached four engines");
    }

    // a thread keeps the shard it first took, so it reaches one engine
    #[test]
    fn a_thread_keeps_its_shard() {
        let _held = lock(&PLACES);
        let kept = std::thread::spawn(|| {
            let first = shard_of(3);
            assert_eq!(shard_of(3), first);
            assert_eq!(shard_of(3), first);
            first
        })
        .join()
        .expect("shard");
        assert!(kept < 3);
    }

    // the slot is what a completion names, so a full slab is waited on not overrun
    #[test]
    fn the_slab_fills_at_its_capacity() {
        let mut inflight = Inflight::with_capacity(2);
        assert!(!inflight.is_full());
        let first = inflight.insert(pending(), UNORDERED);
        assert!(!inflight.is_full());
        inflight.insert(pending(), UNORDERED);
        assert!(inflight.is_full(), "both slots are carrying an op");

        assert!(inflight.take(first).is_some());
        assert!(!inflight.is_full(), "taking one frees the slot it held");
    }

    // the slot named before a vectored op is built is the slot it lands in
    #[test]
    fn the_named_slot_is_the_one_the_op_takes() {
        let mut inflight = Inflight::with_capacity(3);
        for _ in 0..3 {
            let named = inflight.next_free();
            let user_data = inflight.insert(pending(), UNORDERED);
            assert_eq!(user_data as u32, named, "the op landed in another slot");
        }
    }

    // a completion for an op already taken never lands on what moved into its slot
    #[test]
    fn a_reused_slot_refuses_the_old_completion() {
        let mut inflight = Inflight::with_capacity(1);
        let first = inflight.insert(pending(), UNORDERED);
        assert!(inflight.take(first).is_some());
        let second = inflight.insert(pending(), UNORDERED);

        assert_ne!(first, second, "the same slot, a later generation");
        assert!(inflight.take(first).is_none(), "the old flight is over");
        assert!(inflight.take(second).is_some());
    }

    // a completion naming nothing is dropped rather than mistaken for an op
    #[test]
    fn an_unknown_completion_names_no_op() {
        let mut inflight = Inflight::with_capacity(1);
        assert!(inflight.take(0).is_none(), "no op was ever placed");
        assert!(
            inflight.take(u64::MAX).is_none(),
            "no slot that high exists"
        );
    }

    // a batch's answers come back in place, and loose ones wait for the next poll
    #[test]
    fn a_completion_goes_back_where_its_batch_wants_it() {
        let mut inflight = Inflight::with_capacity(4);
        let first = inflight.insert(pending(), 1);
        let loose = inflight.insert(pending(), UNORDERED);
        let mut buffers = Buffers::none();
        for user_data in [first, loose] {
            if let Some((order, held)) = inflight.take(user_data) {
                let completion = held.complete(0, &mut buffers);
                inflight.reaped.push(Reaped { order, completion });
            }
        }

        let mut filled: Vec<Option<Completion>> = vec![None, None];
        assert_eq!(inflight.take_ordered(&mut filled), 1);
        assert!(filled[0].is_none(), "nothing was submitted for that place");
        assert!(filled[1].is_some());

        let mut out = Vec::new();
        assert_eq!(
            inflight.take_free(&mut out),
            1,
            "the loose one is still here"
        );
    }

    // the inbox wraps rather than grows, and hands ops back in the order given
    #[test]
    fn the_inbox_wraps() {
        let mut queue = Queue::new();
        assert_eq!(queue.room(), SLOT_COUNT);

        for at in 0..SLOT_COUNT {
            queue.push(read_op(at as u64));
        }
        assert_eq!(queue.room(), 0, "a claim per slot fills it exactly");

        let mut taken = Vec::new();
        queue.take_all(&mut taken);
        assert_eq!(queue.room(), SLOT_COUNT);
        assert_eq!(taken.len(), SLOT_COUNT);
        assert_eq!(taken[0].tag(), Tag(0), "oldest first");
        assert_eq!(taken[SLOT_COUNT - 1].tag(), Tag(SLOT_COUNT as u64 - 1));

        queue.push(read_op(99));
        let mut wrapped = Vec::new();
        queue.take_all(&mut wrapped);
        assert_eq!(
            wrapped.len(),
            1,
            "the head walked past the end and came back"
        );
        assert_eq!(wrapped[0].tag(), Tag(99));
    }

    // an entry the kernel never took is answered with the error, not left waiting
    #[test]
    fn a_refused_entry_answers_its_op() {
        let mut inflight = Inflight::with_capacity(2);
        let user_data = inflight.insert(pending(), 0);

        let (order, held) = inflight.take(user_data).expect("the record is still there");
        let completion = held.complete(-libc::EIO, &mut Buffers::none());

        assert_eq!(order, 0);
        match completion.outcome {
            Outcome::Read { result, buf } => {
                assert!(result.is_err(), "the refusal came back as the read's error");
                assert_eq!(buf.filled(), 0);
            }
            other => panic!("a read came back as {other:?}"),
        }
    }

    // the three data ops are the ring's whole surface
    #[test]
    fn data_ops_go_on_the_ring() {
        let ops = vec![
            Op::Writev {
                tag: tag(),
                file: file(),
                offset: 0,
                bufs: Vec::new(),
            },
            Op::Pread {
                tag: tag(),
                file: file(),
                offset: 0,
                buf: ReadBuf::new(1),
            },
            Op::PreadSplit {
                tag: tag(),
                file: file(),
                offset: 0,
                head: ReadBuf::new(1),
                body: ReadBuf::new(1),
            },
        ];
        for op in ops {
            assert_eq!(ring_file(&op), Some(file()), "{op:?} belongs on the ring");
        }
    }

    // a vectored write past the kernel's iovec cap stays off the ring
    #[test]
    fn a_wide_vectored_write_stays_off_the_ring() {
        let span = |bytes: usize| WriteBuf::owned(vec![0u8; bytes]);
        let at_cap = Op::Writev {
            tag: tag(),
            file: file(),
            offset: 0,
            bufs: (0..MAX_IOVECS).map(|_| span(1)).collect(),
        };
        assert_eq!(
            ring_file(&at_cap),
            Some(file()),
            "a write filling the cap exactly still belongs on the ring",
        );

        let past_cap = Op::Writev {
            tag: tag(),
            file: file(),
            offset: 0,
            bufs: (0..MAX_IOVECS + 1).map(|_| span(1)).collect(),
        };
        assert_eq!(
            ring_file(&past_cap),
            None,
            "a write one span past the cap must take the posix path, which splits it",
        );
    }

    // a write wide enough to travel alone takes the posix path, footers included
    #[test]
    fn a_lone_wide_write_stays_off_the_ring() {
        let write = |bytes: usize| Op::Writev {
            tag: tag(),
            file: file(),
            offset: 0,
            bufs: vec![WriteBuf::owned(vec![0u8; bytes])],
        };
        assert_eq!(
            ring_file(&write(RING_WRITE_CAP as usize)),
            Some(file()),
            "a write filling the cap exactly still belongs on the ring",
        );
        assert_eq!(
            ring_file(&write(RING_WRITE_CAP as usize + 1)),
            None,
            "a footer is megabytes with nothing beside it, so it blocks where it is issued",
        );
    }

    // the pool lends one buffer per op, takes it back, and refuses a wider span
    #[test]
    fn the_pool_lends_and_takes_back() {
        let mut buffers = Buffers::allocate();
        assert!(buffers.is_live(), "the pool allocated nothing");

        let mut taken = Vec::new();
        for _ in 0..REGISTERED_BUFFERS {
            taken.push(
                buffers
                    .claim(DIRECT_REQUEST_BYTES)
                    .expect("a buffer is free"),
            );
        }

        assert!(buffers.is_starved(), "every buffer is carrying an op");
        assert!(buffers.claim(1).is_none(), "a starved pool lent one anyway");

        buffers.release(taken.pop().expect("a claim"));
        assert!(
            buffers.claim(DIRECT_REQUEST_BYTES + 1).is_none(),
            "a span past one device request was staged into a buffer that had room",
        );
        // The worst offset a record of the staging width can sit at: one byte past
        // a boundary, so the covering read pays a block at each end. The buffer has
        // the room and the device would answer it in two, so it goes off the ring.
        let (_, span) = covering_span(DIRECT_ALIGN as u64 - 1, STAGE_BYTES as u64);
        assert_eq!(span as usize, REGISTERED_BUFFER_BYTES, "the widening moved");
        assert!(
            buffers.claim(span as usize).is_none(),
            "a read widened past one request took the ring anyway",
        );
        // A run the planner capped, which is the widest read that does reach it.
        let (_, capped) = covering_span(
            DIRECT_ALIGN as u64 - 1,
            (DIRECT_REQUEST_BYTES - DIRECT_ALIGN) as u64,
        );
        assert!(
            buffers.claim(capped as usize).is_some(),
            "the widest run the planner merges does not fit one request",
        );
        buffers.release(taken.pop().expect("a claim"));
        assert!(!buffers.is_starved(), "the buffer came back");
    }

    // a pool the kernel refused never makes the ring wait
    #[test]
    fn a_refused_pool_never_waits() {
        let buffers = Buffers::none();

        assert!(!buffers.is_live());
        assert!(!buffers.is_starved());
    }

    // a direct write is counted with the reads, since it goes to the device
    #[test]
    fn a_staged_write_awaits_the_device() {
        let mut inflight = Inflight::with_capacity(2);

        inflight.insert(staged_write(), UNORDERED);

        assert!(
            !inflight.holds_only_writes(),
            "a spin would wait out a device write"
        );
    }

    // a widened read is cut back to what was asked for and returns its buffer
    #[test]
    fn a_staged_read_cuts_its_window() {
        let mut buffers = Buffers::allocate();
        let staged = buffers.claim(2 * DIRECT_ALIGN).expect("a buffer");
        let landed = buffers.as_mut_slice(staged);
        for (at, byte) in landed[..2 * DIRECT_ALIGN].iter_mut().enumerate() {
            *byte = (at % 251) as u8;
        }
        let read = Pending::StagedRead {
            tag: tag(),
            staged,
            skip: 100,
            buf: ReadBuf::new(64),
        };

        let completion = read.complete(2 * DIRECT_ALIGN as i32, &mut buffers);

        match completion.outcome {
            Outcome::Read { result, buf } => {
                assert_eq!(
                    result.expect("the read succeeded"),
                    64,
                    "the pad was billed"
                );
                let wanted: Vec<u8> = (100..164).map(|at: usize| (at % 251) as u8).collect();
                assert_eq!(
                    buf.into_vec(),
                    wanted,
                    "the cut came out of the wrong place"
                );
            }
            other => panic!("a read came back as {other:?}"),
        }
        assert_eq!(
            buffers.claim(64),
            Some(staged),
            "the buffer never came back"
        );
    }

    // a read that stopped inside its blocks answers with what landed
    #[test]
    fn a_short_staged_read_answers_what_landed() {
        let mut buffers = Buffers::allocate();
        let staged = buffers.claim(2 * DIRECT_ALIGN).expect("a buffer");
        buffers.as_mut_slice(staged)[..DIRECT_ALIGN].fill(0xAB);
        let read = Pending::StagedRead {
            tag: tag(),
            staged,
            skip: DIRECT_ALIGN - 8,
            buf: ReadBuf::new(64),
        };

        let completion = read.complete(DIRECT_ALIGN as i32, &mut buffers);

        match completion.outcome {
            Outcome::Read { result, buf } => {
                assert_eq!(
                    result.expect("the read succeeded"),
                    8,
                    "the file ended here"
                );
                assert_eq!(buf.into_vec(), vec![0xAB; 8]);
            }
            other => panic!("a read came back as {other:?}"),
        }
    }

    // a staged write reports the bytes the caller framed, not the blocks it padded to
    #[test]
    fn a_staged_write_reports_what_it_framed() {
        let mut buffers = Buffers::allocate();
        let staged = buffers.claim(DIRECT_ALIGN).expect("a buffer");
        let write = Pending::StagedWrite {
            tag: tag(),
            staged,
            bufs: vec![WriteBuf::owned(vec![7u8; 100])],
            framed: 100,
        };

        let completion = write.complete(DIRECT_ALIGN as i32, &mut buffers);

        match completion.outcome {
            Outcome::Wrote { result, bufs } => {
                assert_eq!(
                    result.expect("the write succeeded"),
                    100,
                    "the pad was billed"
                );
                assert_eq!(bufs.len(), 1, "the caller's buffers came back");
            }
            other => panic!("a write came back as {other:?}"),
        }
        assert_eq!(
            buffers.claim(64),
            Some(staged),
            "the buffer never came back"
        );
    }

    // a direct read is widened to whole blocks, a write rounded up, and neither else
    #[test]
    fn a_staged_span_covers_its_op() {
        let read = Op::Pread {
            tag: tag(),
            file: file(),
            offset: 100,
            buf: ReadBuf::new(64),
        };
        assert_eq!(
            staged_span(&read),
            Some(DIRECT_ALIGN),
            "a window inside one block"
        );

        let write = Op::Writev {
            tag: tag(),
            file: file(),
            offset: 0,
            bufs: vec![WriteBuf::owned(vec![0u8; 100])],
        };
        assert_eq!(
            staged_span(&write),
            Some(DIRECT_ALIGN),
            "a short write pads out"
        );

        let unaligned = Op::Writev {
            tag: tag(),
            file: file(),
            offset: 100,
            bufs: vec![WriteBuf::owned(vec![0u8; 100])],
        };
        assert_eq!(
            staged_span(&unaligned),
            None,
            "a write off a boundary was staged"
        );

        let sync = Op::SyncData {
            tag: tag(),
            file: file(),
        };
        assert_eq!(staged_span(&sync), None, "an op with no buffers took one");
    }

    // an op the kernel serves by blocking stays off the ring
    #[test]
    fn blocking_ops_stay_off_the_ring() {
        let ops = vec![
            Op::SyncData {
                tag: tag(),
                file: file(),
            },
            Op::SyncFull {
                tag: tag(),
                file: file(),
            },
            Op::SyncRange {
                tag: tag(),
                file: file(),
                offset: 0,
                len: 1,
                mode: SyncRangeMode::WaitBeforeWriteWaitAfter,
            },
            Op::Allocate {
                tag: tag(),
                file: file(),
                offset: 0,
                len: 1,
            },
            Op::Truncate {
                tag: tag(),
                file: file(),
                len: 1,
            },
            Op::Advise {
                tag: tag(),
                file: file(),
                offset: 0,
                len: 1,
                advice: Advice::DontNeed,
            },
        ];
        for op in ops {
            assert_eq!(ring_file(&op), None, "{op:?} belongs off the ring");
        }
    }
}
