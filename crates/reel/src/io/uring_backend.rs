//! io_uring backend with one ring per submitting thread, so no ring needs a lock

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::thread::JoinHandle;

use io_uring::{cqueue, opcode, types, EnterFlags, IoUring};

use crate::config::{RingTuning, TaskRun};
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

/// Each ring has this many submission slots
const RING_ENTRIES: u32 = 256;

/// Each slot's iovec list starts with room for this many entries, enough for a split read
const IOVEC_ROOM: usize = 2;

/// Each ring's registered file table has this many slots
const MAX_REGISTERED_FILES: u32 = 4096;

/// Bytes in one registered buffer, one block over the staging width for an unaligned read
const REGISTERED_BUFFER_BYTES: usize = STAGE_BYTES + DIRECT_ALIGN;

/// Each ring registers this many buffers, which caps its direct ops in flight
const REGISTERED_BUFFERS: usize = 32;

/// The order of a completion that no batch is waiting on
const UNORDERED: u32 = u32::MAX;

/// User data for the engine thread's inbox poll, which matches no slot
const KICK_TAG: u64 = u64::MAX;

/// A spinning wait polls the queue this many times before it yields the core
const SPIN_ROUNDS: u32 = 64;

/// A spinning wait sleeps for a completion after this many rounds
const MAX_SPIN_ROUNDS: u32 = 1_000_000;

/// A vectored op's iovec array, kept in its slot so each slot allocates it once
struct IoVecs(Vec<libc::iovec>);

// SAFETY: the slot holds every buffer the pointers target until the completion returns
unsafe impl Send for IoVecs {}

impl IoVecs {
    fn new() -> IoVecs {
        IoVecs(Vec::with_capacity(IOVEC_ROOM))
    }

    /// Point the list at these spans, reusing its allocation
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

/// One ring's registered aligned buffers, which stage its direct ops
struct Buffers {
    /// The buffers, at the addresses the kernel pinned
    held: Vec<AlignedBuf>,

    /// Free buffers, popped from the back
    free: Vec<u16>,
}

impl Buffers {
    /// An empty pool, for a ring whose ops use the caller's buffers
    fn none() -> Buffers {
        Buffers {
            held: Vec::new(),
            free: Vec::new(),
        }
    }

    /// Allocate the buffers unregistered, or an empty pool if allocation fails
    fn allocate() -> Buffers {
        let mut held = Vec::with_capacity(REGISTERED_BUFFERS);
        let mut free = Vec::with_capacity(REGISTERED_BUFFERS);
        for at in 0..REGISTERED_BUFFERS {
            let Ok(buf) = AlignedBuf::uninit(REGISTERED_BUFFER_BYTES) else {
                return Buffers::none();
            };
            held.push(buf);
            // Reversed so pops hand out the low buffers first
            free.push((REGISTERED_BUFFERS - 1 - at) as u16);
        }
        Buffers { held, free }
    }

    /// Register a pool with this ring, or return an empty one if the kernel refuses
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
        // SAFETY: each span is a buffer the pool owns and never moves, and the ring drops first
        if unsafe { ring.submitter().register_buffers(&spans) }.is_err() {
            tracing::debug!("this ring has no buffer pool, so its direct ops take the posix path");
            return Buffers::none();
        }
        pool
    }

    /// Whether this ring has a pool
    fn is_live(&self) -> bool {
        !self.held.is_empty()
    }

    /// Whether every buffer is in use, which an empty pool never is
    fn is_starved(&self) -> bool {
        self.is_live() && self.free.is_empty()
    }

    /// Take a buffer for a span of at most one device request, or nothing
    fn claim(&mut self, span: usize) -> Option<u16> {
        if span > DIRECT_REQUEST_BYTES {
            return None;
        }
        self.free.pop()
    }

    fn release(&mut self, at: u16) {
        self.free.push(at);
    }

    fn as_mut_slice(&mut self, at: u16) -> &mut [u8] {
        self.held[at as usize].as_mut_slice()
    }

    /// A buffer's address, aligned by its allocation
    fn as_ptr(&self, at: u16) -> *mut u8 {
        self.held[at as usize].as_ptr()
    }

    /// The first `count` bytes a read landed in one buffer
    ///
    /// # Safety
    ///
    /// `count` must be at most the byte count a completion reported for this buffer
    unsafe fn filled(&self, at: u16, count: usize) -> &[u8] {
        unsafe { self.held[at as usize].filled(count) }
    }
}

/// A submitted op and the buffers it owns, held until its completion returns
enum Pending {
    /// A write, holding the buffers it copies from
    Wrote { tag: Tag, bufs: Vec<WriteBuf> },

    /// A read into one buffer
    Read { tag: Tag, buf: ReadBuf },

    /// A read split across a header buffer and a payload buffer
    ReadSplit {
        tag: Tag,
        head: ReadBuf,
        body: ReadBuf,
    },

    /// A write gathered into a registered buffer, `framed` bytes before block padding
    StagedWrite {
        tag: Tag,
        staged: u16,
        bufs: Vec<WriteBuf>,
        framed: usize,
    },

    /// A read into a registered buffer, with the caller's bytes starting `skip` in
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
    /// Turn a ring result into the op's completion, releasing any staged buffer
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
                // The kernel wrote this many bytes into the buffer, so committing them is sound
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
                // A vectored read fills the head first, so a short read cuts the body first
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
                // Safety: the completion says the kernel filled at least this many bytes
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
                // Safety: as above, the count comes from the completion
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

/// One slab slot: its op in flight, its batch order, and its generation
struct Slot {
    /// Bumped on every reuse, so a completion from an earlier op is stale
    generation: u32,

    /// Where this op's completion goes in its batch's answers
    order: u32,

    /// The op and the buffers it owns, held here until its completion returns
    pending: Option<Pending>,

    /// The iovec list the op in this slot handed the kernel
    iovecs: IoVecs,
}

/// One completion and its place in a batch's answers
struct Reaped {
    order: u32,
    completion: Completion,
}

/// Ops in flight on one ring, keyed by user data that holds slot and generation
struct Inflight {
    /// Every slot, live or free
    slots: Vec<Slot>,

    /// Free slots, popped from the back
    free: Vec<u32>,

    /// Completions taken off the ring and not yet handed out
    reaped: Vec<Reaped>,

    /// Reads and direct writes in flight, which decide whether a wait spins or sleeps
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
            // Reversed so pops hand out the low slots first
            free.push((entries - 1 - at) as u32);
        }
        Inflight {
            slots,
            free,
            reaped: Vec::new(),
            reads_out: 0,
        }
    }

    /// Whether every op in flight on this ring is a buffered write
    fn holds_only_writes(&self) -> bool {
        self.reads_out == 0
    }

    /// Whether every slot is in use
    fn is_full(&self) -> bool {
        self.free.is_empty()
    }

    /// Whether this ring has no op in flight
    fn is_idle(&self) -> bool {
        self.free.len() == self.slots.len()
    }

    /// The slot the next insert will use, so a vectored op can fill its iovec list first
    fn next_free(&self) -> u32 {
        *self.free.last().expect("a slot is free before an insert")
    }

    fn iovecs_mut(&mut self, at: u32) -> &mut IoVecs {
        &mut self.slots[at as usize].iovecs
    }

    /// Place an op and return the user data for its submission
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

    /// Take the op for this user data, or nothing if no live op matches
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

    /// Move completions that no batch is waiting on into the output
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

    /// Move completions a batch is waiting on into their places in its answers
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

/// The descriptor a submission uses, a plain fd or a registered file slot
enum RingTarget {
    Plain(types::Fd),
    Registered(types::Fixed),
}

/// One ring's registered file table, keyed by handle and cleared whole after any close
struct Files {
    /// Whether the kernel accepted a file table for this ring
    is_registered: bool,

    /// The volume's close count when this table was built
    generation: u64,

    /// The slot each handle sits in
    held: HashMap<FileId, u32>,

    /// The last op's handle and slot, since ops arrive in runs against one file
    last: Option<(FileId, u32)>,

    /// The next slot to hand out, which rewinds only when the table is given up
    next: u32,
}

impl Files {
    /// An empty table, registered if the kernel accepted one
    fn new(is_registered: bool, generation: u64) -> Files {
        Files {
            is_registered,
            generation,
            held: HashMap::new(),
            last: None,
            next: 0,
        }
    }

    /// The descriptor for this file, registering it in a slot the first time
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

    /// Whether a close on the volume has made this table stale
    fn is_stale(&self, posix: &PosixBackend) -> bool {
        self.is_registered && self.generation != posix.close_generation()
    }

    /// Clear every slot after a close, once the caller has flushed queued entries
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

/// One ring and its ops in flight, owned by one thread as SINGLE_ISSUER requires
struct Ring {
    /// The kernel ring, used from this thread alone
    ring: IoUring,

    /// The ops in flight on it
    inflight: Inflight,

    /// The files registered with it
    files: Files,

    /// The buffers registered with it, declared after the ring so it drops first
    buffers: Buffers,

    /// Whether descriptors bypass the page cache, so ops go through registered buffers
    is_direct: bool,

    /// User data of entries queued for the kernel, taken back when an enter fails
    queued: Vec<u64>,

    /// The inbox eventfd this ring watches, set only on an engine thread
    kick: Option<Kick>,

    /// Whether the kernel defers this ring's completion work, per the mode it accepted
    asks_for_completions: bool,

    /// The volume's door tally, which this ring updates per op
    doors: Arc<DoorTally>,
}

impl Ring {
    /// Build this thread's ring under the volume's tuning
    fn new(core: &Core) -> Result<Ring> {
        let ring = ring_under(core.taskrun)?;
        // A refused file table leaves the ring on plain descriptors
        let is_registered = ring
            .submitter()
            .register_files_sparse(MAX_REGISTERED_FILES)
            .is_ok();
        if !is_registered {
            core.doors.note_files_refused();
        }
        // The kernel clamps the completion queue, so its size is read back from the ring
        let entries = ring.params().cq_entries() as usize;
        // Only a direct volume needs a pool, since its descriptors refuse caller buffers
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

    /// Whether a wait spins, which it does only while every op in flight is a buffered write
    fn spins_for(inflight: &Inflight) -> bool {
        inflight.holds_only_writes()
    }

    /// Whether the ring has no free slot or no free registered buffer
    fn is_full(&self) -> bool {
        self.inflight.is_full() || self.buffers.is_starved()
    }

    /// Whether this ring has no op in flight
    fn is_idle(&self) -> bool {
        self.inflight.is_idle()
    }

    /// Submit what is queued, draining on EBUSY and answering every entry if the enter fails
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

    /// Run any completion work the kernel deferred for this thread, without waiting
    fn run_owed_work(&mut self) {
        if !self.asks_for_completions || !self.ring.submission().taskrun() {
            return;
        }
        // SAFETY: this enter submits and waits for nothing, on the thread that owns the ring
        let entered = unsafe {
            self.ring
                .submitter()
                .enter::<libc::sigset_t>(0, 0, EnterFlags::GETEVENTS.bits(), None)
        };
        if let Err(error) = entered {
            // The work stays queued and the flag stays up, so the next look asks again
            tracing::debug!("the ring refused to run its own completion work: {error}");
        }
    }

    /// Move finished ops into the reaped list, running deferred work first
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

    /// Submit what is queued and sleep until at least one op completes
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

    /// Spin or sleep until the ring reports at least one more completion
    fn wait_more(&mut self) {
        if !Ring::spins_for(&self.inflight) {
            self.sleep_once();
            return;
        }
        // The spin never enters the kernel, so it submits the queued entries first
        self.flush();
        let mut rounds = 0u32;
        loop {
            if self.drain() > 0 {
                return;
            }
            rounds += 1;
            if rounds > MAX_SPIN_ROUNDS {
                // A spin this long means no completion is close, so sleep for one
                self.doors.note_spun_out();
                self.sleep_once();
                return;
            }
            if rounds > SPIN_ROUNDS {
                std::thread::yield_now();
            }
        }
    }

    /// Sleep in the kernel once, then drain what completed
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

    /// Queue one op on the ring, or answer it on the posix path when the ring cannot take it
    fn stage(&mut self, op: Op, posix: &PosixBackend, order: u32) -> Option<Completion> {
        let file = match ring_file(&op) {
            Some(file) => file,
            None => {
                self.doors.note_off_ring(1);
                return Some(posix.dispatch(op));
            }
        };
        // A close makes the table stale. Flush entries that use its slots, then clear it
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

        // A vectored op points at its slot's iovec list, so build against the next free slot
        let at = self.inflight.next_free();
        let built = match self.is_direct {
            true => build_staged(target, op, &mut self.buffers),
            false => Ok(build_entry(target, op, self.inflight.iovecs_mut(at))),
        };
        let (entry, pending) = match built {
            Ok(built) => built,
            // The pool cannot serve this op, so it goes to the posix path
            Err(op) => {
                self.doors.note_off_ring(1);
                return Some(posix.dispatch(op));
            }
        };
        self.doors.note_ring();
        let user_data = self.inflight.insert(pending, order);
        let entry = entry.user_data(user_data);

        loop {
            // SAFETY: the entry's buffers live in the record or the pool for the whole flight
            if unsafe { self.ring.submission().push(&entry) }.is_ok() {
                self.queued.push(user_data);
                return None;
            }
            if !self.flush() {
                // The entry never reached the queue, so answer its record with an error
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
            // SAFETY: the poll has no buffer, and its eventfd outlives the engine thread
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
        // SAFETY: an ffi read of the eight bytes an eventfd counter holds
        let _ = unsafe { libc::read(kick.fd, ticks.as_mut_ptr().cast(), ticks.len()) };
    }
}

/// Shared settings every thread's ring for one backend is built from
struct Core {
    posix: Arc<PosixBackend>,
    tuning: RingTuning,

    taskrun: TaskRun,

    is_direct: bool,
    doors: Arc<DoorTally>,
}

/// Which doors this volume's ops took, and what the kernel refused
#[derive(Default)]
struct DoorTally {
    reached_ring: AtomicBool,
    off_ring: AtomicU64,
    pool_refused: AtomicBool,
    files_refused: AtomicBool,
    spun_out: AtomicU64,
}

impl DoorTally {
    /// Note one op went on a ring, loading first so the cache line stays shared
    fn note_ring(&self) {
        if !self.reached_ring.load(Ordering::Relaxed) {
            self.reached_ring.store(true, Ordering::Relaxed);
        }
    }

    /// Note ops that took another door
    fn note_off_ring(&self, ops: usize) {
        self.off_ring.fetch_add(ops as u64, Ordering::Relaxed);
    }

    /// Note that the kernel refused a thread's pool
    fn note_pool_refused(&self) {
        if !self.pool_refused.load(Ordering::Relaxed) {
            self.pool_refused.store(true, Ordering::Relaxed);
        }
    }

    /// Note that a spinning wait gave up and slept
    fn note_spun_out(&self) {
        self.spun_out.fetch_add(1, Ordering::Relaxed);
    }

    /// Note that the kernel refused a ring's sparse file table
    fn note_files_refused(&self) {
        if !self.files_refused.load(Ordering::Relaxed) {
            self.files_refused.store(true, Ordering::Relaxed);
        }
    }

    fn counts(&self) -> DoorCounts {
        DoorCounts {
            reached_ring: self.reached_ring.load(Ordering::Relaxed),
            off_ring: self.off_ring.load(Ordering::Relaxed),
            pool_refused: self.pool_refused.load(Ordering::Relaxed),
            files_refused: self.files_refused.load(Ordering::Relaxed),
        }
    }
}

thread_local! {
    /// This thread's rings, one per backend it has submitted to
    static RINGS: RefCell<Vec<Owned>> = const { RefCell::new(Vec::new()) };
}

/// One thread's ring for one backend, keyed by the core it was built from
struct Owned {
    core: Weak<Core>,
    ring: Ring,
}

/// Ops waiting for the engine thread, plus the eventfd that wakes it
struct Inbox {
    /// The ops waiting for the engine thread, and its state
    queue: Mutex<Queue>,

    /// Written to wake the engine thread out of its ring's wait
    kick: OwnedFd,
}

/// A circular queue of ops waiting for the engine thread, and the thread's state
struct Queue {
    /// One slot per op the driver can have claimed, so a push never allocates
    slots: Vec<Option<Op>>,

    /// Where the next take comes from
    head: usize,

    /// How many slots from the head hold an op
    len: usize,

    /// Whether the engine thread is parked in its ring's wait
    is_parked: bool,

    /// Whether the engine thread has been asked to stop
    is_stopping: bool,

    /// Whether the engine thread gave up, so later callers get an error
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

    /// Free slots
    fn room(&self) -> usize {
        self.slots.len() - self.len
    }

    /// Add one op behind the last
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
    /// An inbox with its own eventfd, or the error from creating it
    fn new() -> Result<Inbox> {
        // SAFETY: an ffi call that takes two integers and returns a descriptor
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            return Err(ReelError::Io(std::io::Error::last_os_error()));
        }
        // SAFETY: the descriptor is fresh from the kernel and owned by nothing else
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

    /// Hand the engine a whole batch with at most one kick
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

    /// Hand the engine one op with at most one kick
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
        // SAFETY: an ffi write of eight bytes from an eight-byte buffer to an eventfd
        let written =
            unsafe { libc::write(self.kick.as_raw_fd(), tick.as_ptr().cast(), tick.len()) };
        if written < 0 {
            let error = std::io::Error::last_os_error();
            // A full counter already has a wake pending
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

    /// Mark the engine as working again
    fn unpark(&self) {
        lock(&self.queue).is_parked = false;
    }

    /// Mark the engine failed, so nothing more is queued for it
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
    /// Start the engine thread, which builds its own ring and reports back over a channel
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

    /// Hand the engine a batch with at most one kick
    fn hand(&self, ops: Vec<Op>, sink: &Arc<SlotTable>) -> Result<()> {
        debug_assert!(
            Arc::ptr_eq(&self.sink, sink),
            "two drivers over one ring backend file into one table"
        );
        self.inbox.hand(ops)
    }

    /// Hand the engine one op, for the awaited door
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

/// Run one ring for every caller with no thread of its own
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

/// Wait until the ring has room, taking what lands for the batch on the way
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
fn run_batch(ring: &mut Ring, ops: &mut Vec<Op>, posix: &PosixBackend, out: &mut Vec<Completion>) {
    let mut filled: Vec<Option<Completion>> = Vec::new();
    filled.resize_with(ops.len(), || None);
    let mut outstanding = 0usize;

    for (order, op) in ops.drain(..).enumerate() {
        // More ops in flight than the completion queue holds could lose one, so wait for room
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
    /// This thread's shard, taken in arrival order on first use and kept for life
    static SHARD: Cell<Option<usize>> = const { Cell::new(None) };
}

/// How many threads have taken a shard so far
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

/// Build one ring under one completion mode, or return the kernel's error
fn ring_under(taskrun: TaskRun) -> Result<IoUring> {
    let mut builder = IoUring::builder();
    // The kernel clamps an oversized ring size to its limit
    builder.setup_clamp();
    // Each ring has one submitting thread, which the deferred mode requires
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
    // Both modes queue completion work, so the ring flags when some is pending
    if taskrun.is_asked_for() {
        builder.setup_taskrun_flag();
    }
    builder.build(RING_ENTRIES).map_err(ReelError::Io)
}

/// Build one ring under the best completion mode the kernel accepts, stepping down on refusal
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

impl std::fmt::Debug for UringBackend {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.debug_struct("UringBackend")
            .field("direct", &self.core.is_direct)
            .finish()
    }
}

impl UringBackend {
    /// Build a ring backend, or report why the ring could not be set up
    pub fn new(is_direct: bool, tuning: RingTuning) -> Result<UringBackend> {
        // Settle the completion mode once, so every thread's ring uses what the kernel accepted
        let (probe, taskrun) = build_ring(tuning.taskrun)?;
        drop(probe);
        // One engine shard per hardware thread, or one if the count is unknown
        let shards = std::thread::available_parallelism()
            .map(|width| width.get())
            .unwrap_or(1);
        Ok(UringBackend {
            core: Arc::new(Core {
                // The inner backend opens every file, so it gets the direct flag here
                posix: Arc::new(PosixBackend::with_direct(is_direct)),
                tuning,
                taskrun,
                is_direct,
                doors: Arc::new(DoorTally::default()),
            }),
            engines: (0..shards).map(|_| OnceLock::new()).collect(),
        })
    }

    /// Ops the posix backend under this ring has answered
    pub fn ops(&self) -> u64 {
        self.core.posix.ops()
    }

    /// How many spinning waits on this volume gave up and slept
    pub fn spin_outs(&self) -> u64 {
        self.core.doors.spun_out.load(Ordering::Relaxed)
    }

    /// Run something on this thread's ring, building it first, or with None if that fails
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
    fn engine(&self, sink: &Arc<SlotTable>) -> Result<&Engine> {
        let shard = shard_of(self.engines.len());
        let held = &self.engines[shard];
        if let Some(engine) = held.get() {
            return Ok(engine);
        }
        let started = Engine::start(&self.core, sink, shard)?;
        // Two callers can race here. Dropping the loser's engine stops its thread
        drop(held.set(started));
        held.get().ok_or_else(engine_gone)
    }

    /// Whether data ops use the ring, which a direct volume does only with registered buffers
    fn takes_ring(&self) -> bool {
        !self.core.is_direct || self.core.tuning.registered_buffers
    }
}

impl ReelIo for UringBackend {
    /// A ring, direct when its descriptors bypass the page cache
    fn serving(&self) -> ServingBackend {
        match self.core.is_direct {
            true => ServingBackend::RingDirect,
            false => ServingBackend::Ring,
        }
    }

    /// Which doors this volume's ops took
    fn door_counts(&self) -> DoorCounts {
        self.core.doors.counts()
    }

    /// Flushes this volume asked the drive for, counted by the inner posix backend
    fn sync_count(&self) -> u64 {
        PosixBackend::sync_count(&self.core.posix)
    }

    /// Nanoseconds spent waiting inside those flushes, from the inner backend
    fn sync_nanos(&self) -> u64 {
        PosixBackend::sync_nanos(&self.core.posix)
    }

    /// Forward the warm probe to the inner backend, which refuses it on a direct volume
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
                // No ring on this thread, so ring ops join the posix batch
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

    /// Hand a batch to the engine thread, which files completions into the slot table
    fn submit_detached(&self, ops: Vec<Op>, sink: &Arc<SlotTable>) -> Result<()> {
        if !self.takes_ring() {
            self.core.doors.note_off_ring(ops.len());
            return self.core.posix.submit_detached(ops, sink);
        }
        self.engine(sink)?.hand(ops, sink)
    }

    /// Hand the engine one op, which is what the awaited door sends
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

    /// Waits on this volume can sleep, since a read in flight makes them park
    fn parks_on_wait(&self) -> bool {
        true
    }

    /// Wait on this thread's completion queue, skipping the wait when nothing is in flight
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

/// The most bytes the kernel moves in one call, so wider reads go to the posix backend
const RING_SPAN_CAP: u64 = 0x7fff_f000;

/// The widest write the ring takes, matching the direct door's request ceiling
const RING_WRITE_CAP: u64 = DIRECT_REQUEST_BYTES as u64;

/// Total bytes across a vectored write's buffers
fn write_span(bufs: &[WriteBuf]) -> u64 {
    let mut span = 0u64;
    for buf in bufs {
        span += buf.len() as u64;
    }
    span
}

/// The op's file if it belongs on the ring, or nothing
fn ring_file(op: &Op) -> Option<FileId> {
    match op {
        // The kernel refuses a write past the iovec cap, and posix splits it
        Op::Writev { bufs, .. } if bufs.len() > MAX_IOVECS => None,
        Op::Writev { bufs, .. } if write_span(bufs) > RING_WRITE_CAP => None,
        Op::Pread { buf, .. } if buf.wanted() as u64 > RING_SPAN_CAP => None,
        Op::PreadSplit { head, body, .. }
            if (head.wanted() + body.wanted()) as u64 > RING_SPAN_CAP =>
        {
            None
        }
        Op::Writev { file, .. } | Op::Pread { file, .. } | Op::PreadSplit { file, .. } => {
            Some(*file)
        }
        _ => None,
    }
}

/// Build an op's submission and the record that keeps its buffers alive
fn build_entry(
    target: RingTarget,
    op: Op,
    iovecs: &mut IoVecs,
) -> (io_uring::squeue::Entry, Pending) {
    // The opcode builders are generic over the descriptor type, so each site matches on it
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
        Op::Pread {
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

/// A direct op's span in a registered buffer, or nothing if it cannot be staged
fn staged_span(op: &Op) -> Option<usize> {
    let (offset, wanted) = match op {
        Op::Writev { offset, bufs, .. } => {
            let total: usize = bufs.iter().map(|buf| buf.len()).sum();
            if total == 0 || !offset.is_multiple_of(DIRECT_ALIGN as u64) {
                return None;
            }
            return Some(align_up(total as u64) as usize);
        }
        Op::Pread { offset, buf, .. } => (*offset, buf.wanted()),
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

/// Build a direct op's submission through a registered buffer, or hand the op back
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
            // Zero the rounding tail, since it reaches the device too
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

/// A read into the registered buffer at this index
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

/// A write out of the registered buffer at this index
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

/// The error for a negative ring result, or nothing when the op succeeded
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

    // taking back a refused read drops it from the read count
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

    // the slab reports full at its capacity and frees a slot on take
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

    // the slot next_free reports is the slot the next insert uses
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

    // a completion that matches no live op is dropped
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

    // the inbox wraps around and hands ops back in the order given
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

    // an entry the kernel never took is answered with the error
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
        // A staging-width read one byte short of a boundary widens past one request
        let (_, span) = covering_span(DIRECT_ALIGN as u64 - 1, STAGE_BYTES as u64);
        assert_eq!(span as usize, REGISTERED_BUFFER_BYTES, "the widening moved");
        assert!(
            buffers.claim(span as usize).is_none(),
            "a read widened past one request took the ring anyway",
        );
        // A run at the planner's cap is the widest read that still takes the ring
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

    // a staged write reports the bytes the caller framed, without the padding
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

    // a direct read widens to whole blocks, an aligned write rounds up, other ops get none
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
