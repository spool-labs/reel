//! The Linux completion ring: reads into buffers registered once at open
//!
//! Every unsafe block in this file stands at the kernel's interface. The buffers outlive the ring, a slot's bytes belong to the
//! kernel from a read's submit to its completion, and one mutex keeps the submission queue to one writer at a time.

use std::cell::UnsafeCell;
use std::io::{Error, ErrorKind, IoSliceMut, Result};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use io_uring::{IoUring, opcode, types};
use reel_api::units::{ByteCount, FileOffset};

use crate::guard::hold;
use crate::io::spin_clock::SpinClock;
use crate::io::plain_file::PlainFile;
use crate::io::ring::{RING_SLOT_BYTES, RING_SLOTS, RingDriver, reap_spin};
use crate::pool::TOUCH_BYTE;

/// The submission queue's length, past any read a ring can hold in flight with its poke beside them
pub const RING_ENTRIES: u32 = 2 * RING_SLOTS as u32;
/// The mark on a completion that only brings a reaper out of its wait
pub const POKE: u64 = u64::MAX;
/// The registered buffer every read lands in
pub const BUFFER_INDEX: u16 = 0;
/// How long the kernel's submit thread stays awake with nothing to submit before it sleeps, in milliseconds. While reads flow
/// it stays up and a start costs no syscall. A trickle of reads further apart than this wakes it once a read and keeps it up
/// for this long after, so its CPU follows the read rate. At 10 ms a read every 20 ms would keep it up half the time
pub const SUBMIT_IDLE_MS: u32 = 1;
/// How long a reaper looks at the completions in memory before it waits in the kernel. On the ccx33 150 us took a cold read from
/// 141 us to 120, and 400 us bought nothing more
pub const REAP_SPIN: Duration = Duration::from_micros(150);

/// The slots' bytes, which the kernel writes behind the compiler's back
#[derive(Debug)]
pub struct RingBytes(Box<[UnsafeCell<u8>]>);

// SAFETY: a slot belongs to one side at a time: the kernel from submit to completion, the reader after it
unsafe impl Sync for RingBytes {}
// SAFETY: as for Sync, the kernel and the reader take turns on a slot, so moving the bytes between threads races nothing
unsafe impl Send for RingBytes {}

/// A ring with its registered buffers
pub struct UringDriver {
    ring: IoUring,
    bytes: RingBytes,
    submitting: Mutex<()>,
    /// Reads started and not yet landed, which tells the reaper whether a spin can find one
    in_flight: AtomicUsize,
}

impl UringDriver {
    /// Set up the ring and register its buffers, or say why this kernel can't
    pub fn open() -> Result<Self> {
        // a kernel thread takes submissions while it's awake, and a kernel that refuses one gets a ring with no thread
        let ring = match IoUring::builder().setup_sqpoll(SUBMIT_IDLE_MS).build(RING_ENTRIES) {
            Ok(ring) => ring,
            Err(_refused) => IoUring::new(RING_ENTRIES)?,
        };
        let cells: Vec<UnsafeCell<u8>> = (0..RING_SLOTS * RING_SLOT_BYTES).map(|_byte| UnsafeCell::new(TOUCH_BYTE)).collect();
        let bytes = RingBytes(cells.into_boxed_slice());
        let vector = libc::iovec {
            iov_base: bytes.0.as_ptr().cast_mut().cast(),
            iov_len: RING_SLOTS * RING_SLOT_BYTES,
        };
        // SAFETY: the buffer lives in this driver as long as the ring does
        unsafe { ring.submitter().register_buffers(&[vector]) }?;
        Ok(Self {
            ring,
            bytes,
            submitting: Mutex::new(()),
            in_flight: AtomicUsize::new(0),
        })
    }

    /// Point at a slot's first byte
    fn slot_start(&self, slot: usize) -> *mut u8 {
        self.bytes.0[slot * RING_SLOT_BYTES].get()
    }

    /// Put one entry on the queue and tell the kernel
    fn submit(&self, entry: &io_uring::squeue::Entry) -> Result<()> {
        let _submitting = hold(&self.submitting);
        // SAFETY: the mutex keeps this the queue's one writer, and an entry's buffer outlives its read
        let pushed = unsafe {
            let mut queue = self.ring.submission_shared();
            queue.push(entry)
        };
        pushed.map_err(|_full| Error::from(ErrorKind::WouldBlock))?;
        // a pushed entry is the kernel's whatever this says: the submit thread or the reaper's next wait sends it, so the slot stays in
        // flight and a failed submit only waits for that
        let _sent = self.ring.submitter().submit();
        Ok(())
    }
}

impl RingDriver<PlainFile> for UringDriver {
    fn start(&self, slot: usize, file: &PlainFile, offset: FileOffset, length: usize) -> Result<()> {
        // the kernel writes up to the length into the slot, so a longer one would run into the next
        if length > RING_SLOT_BYTES {
            return Err(Error::from(ErrorKind::InvalidInput));
        }
        let entry = opcode::ReadFixed::new(types::Fd(file.descriptor()), self.slot_start(slot), length as u32, BUFFER_INDEX)
            .offset(offset.0)
            .build()
            .user_data(slot as u64);
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        let sent = self.submit(&entry);
        if sent.is_err() {
            self.in_flight.fetch_sub(1, Ordering::AcqRel);
        }
        sent
    }

    fn reap(&self, done: &mut dyn FnMut(usize, Result<usize>)) -> Result<bool> {
        // completions land in shared memory, so a look there costs no syscall, and a wait in the kernel costs a wake
        let spin = reap_spin(self.in_flight.load(Ordering::Acquire), REAP_SPIN);
        let mut clock = SpinClock::new(spin);
        // SAFETY: only the one reaper reads this queue
        while unsafe { self.ring.completion_shared() }.is_empty() && !clock.is_over() {
            std::hint::spin_loop();
        }
        // SAFETY: only the one reaper reads this queue
        let is_waiting = unsafe { self.ring.completion_shared() }.is_empty();
        let waited = if is_waiting { self.ring.submitter().submit_and_wait(1) } else { Ok(0) };
        match waited {
            Ok(_submitted) => {},
            Err(error) if error.raw_os_error() == Some(libc::EINTR) => return Ok(true),
            Err(error) => return Err(error),
        }
        // SAFETY: only the one reaper reads this queue
        let completions = unsafe { self.ring.completion_shared() };
        for completion in completions {
            if completion.user_data() == POKE {
                continue;
            }
            let answer = completion.result();
            let outcome = if answer < 0 { Err(Error::from_raw_os_error(-answer)) } else { Ok(answer as usize) };
            self.in_flight.fetch_sub(1, Ordering::AcqRel);
            done(completion.user_data() as usize, outcome);
        }
        Ok(true)
    }

    fn copy_out(&self, slot: usize, out: &mut [u8]) {
        assert!(out.len() <= RING_SLOT_BYTES, "a copy past its slot");
        // SAFETY: the completion ended the kernel's turn, and the slot stays held until this copy is done
        let bytes = unsafe { std::slice::from_raw_parts(self.slot_start(slot).cast_const(), out.len()) };
        out.copy_from_slice(bytes);
    }

    fn copy_parts(&self, slot: usize, parts: &mut [IoSliceMut<'_>]) {
        assert!(parts.iter().map(|part| part.len()).sum::<usize>() <= RING_SLOT_BYTES, "a copy past its slot");
        let mut at = 0;
        for part in parts {
            let length = part.len();
            // SAFETY: as in copy_out, inside the slot since the parts' lengths fit it
            let bytes = unsafe { std::slice::from_raw_parts(self.slot_start(slot).add(at).cast_const(), length) };
            part.copy_from_slice(bytes);
            at += length;
        }
    }

    fn poke(&self) {
        let entry = opcode::Nop::new().build().user_data(POKE);
        // a queue too full for this has completions on their way, and those end the wait
        let _sent = self.submit(&entry);
    }

    fn heap_bytes(&self) -> ByteCount {
        ByteCount((RING_SLOTS * RING_SLOT_BYTES) as u64)
    }
}
