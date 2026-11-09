//! The Linux completion ring: reads into buffers registered once at open
//!
//! Every unsafe block in this file stands at the kernel's interface. The buffers outlive the ring, a slot's bytes belong to the
//! kernel from a read's submit to its completion, and one mutex keeps the submission queue to one writer at a time.

use std::cell::UnsafeCell;
use std::io::{Error, ErrorKind, IoSliceMut, Result};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use io_uring::{IoUring, opcode, types};
use reel_api::units::{ByteCount, FileOffset};

use crate::guard::hold;
use crate::io::plain_file::PlainFile;
use crate::io::ring::{RING_SLOT_BYTES, RING_SLOTS, RingDriver};
use crate::pool::TOUCH_BYTE;

/// The submission queue's length, past any read a ring can hold in flight with its poke beside them
pub const RING_ENTRIES: u32 = 2 * RING_SLOTS as u32;
/// The mark on a completion that only brings a reaper out of its wait
pub const POKE: u64 = u64::MAX;
/// The registered buffer every read lands in
pub const BUFFER_INDEX: u16 = 0;
/// How long the kernel's submit thread stays awake with nothing to submit before it sleeps, in milliseconds. While reads flow
/// it stays up, and a start costs no syscall
pub const SUBMIT_IDLE_MS: u32 = 1_000;
/// How long a reaper looks at the completions in memory before it waits in the kernel, past a cold read's time on a slow disk
pub const REAP_SPIN: Duration = Duration::from_micros(400);

/// The slots' bytes, which the kernel writes behind the compiler's back
#[derive(Debug)]
pub struct RingBytes(Box<[UnsafeCell<u8>]>);

// a slot belongs to one side at a time: the kernel from submit to completion, the reader after it
unsafe impl Sync for RingBytes {}
unsafe impl Send for RingBytes {}

/// A ring with its registered buffers
pub struct UringDriver {
    ring: IoUring,
    bytes: RingBytes,
    submitting: Mutex<()>,
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
        // the buffer lives in this driver as long as the ring does
        unsafe { ring.submitter().register_buffers(&[vector]) }?;
        Ok(Self {
            ring,
            bytes,
            submitting: Mutex::new(()),
        })
    }

    /// Point at a slot's first byte
    fn slot_start(&self, slot: usize) -> *mut u8 {
        self.bytes.0[slot * RING_SLOT_BYTES].get()
    }

    /// Put one entry on the queue and tell the kernel
    fn submit(&self, entry: &io_uring::squeue::Entry) -> Result<()> {
        let _submitting = hold(&self.submitting);
        // the mutex keeps this the queue's one writer, and an entry's buffer outlives its read
        let pushed = unsafe {
            let mut queue = self.ring.submission_shared();
            queue.push(entry)
        };
        pushed.map_err(|_full| Error::from(ErrorKind::WouldBlock))?;
        self.ring.submitter().submit()?;
        Ok(())
    }
}

impl RingDriver<PlainFile> for UringDriver {
    fn start(&self, slot: usize, file: &PlainFile, offset: FileOffset, length: usize) -> Result<()> {
        let entry = opcode::ReadFixed::new(types::Fd(file.descriptor()), self.slot_start(slot), length as u32, BUFFER_INDEX)
            .offset(offset.0)
            .build()
            .user_data(slot as u64);
        self.submit(&entry)
    }

    fn reap(&self, done: &mut dyn FnMut(usize, Result<usize>)) -> Result<bool> {
        // completions land in shared memory, so a look there costs no syscall, and a wait in the kernel costs a wake
        let spun = Instant::now();
        // only the one reaper reads this queue
        while unsafe { self.ring.completion_shared() }.is_empty() && spun.elapsed() < REAP_SPIN {
            std::hint::spin_loop();
        }
        let is_waiting = unsafe { self.ring.completion_shared() }.is_empty();
        let waited = if is_waiting { self.ring.submitter().submit_and_wait(1) } else { Ok(0) };
        match waited {
            Ok(_submitted) => {},
            Err(error) if error.raw_os_error() == Some(libc::EINTR) => return Ok(true),
            Err(error) => return Err(error),
        }
        // only the one reaper reads this queue
        let completions = unsafe { self.ring.completion_shared() };
        for completion in completions {
            if completion.user_data() == POKE {
                continue;
            }
            let answer = completion.result();
            let outcome = if answer < 0 { Err(Error::from_raw_os_error(-answer)) } else { Ok(answer as usize) };
            done(completion.user_data() as usize, outcome);
        }
        Ok(true)
    }

    fn copy_out(&self, slot: usize, out: &mut [u8]) {
        // the completion ended the kernel's turn, and the slot stays held until this copy is done
        let bytes = unsafe { std::slice::from_raw_parts(self.slot_start(slot).cast_const(), out.len()) };
        out.copy_from_slice(bytes);
    }

    fn copy_parts(&self, slot: usize, parts: &mut [IoSliceMut<'_>]) {
        let mut at = 0;
        for part in parts {
            let length = part.len();
            // as in copy_out, inside the slot since the parts' lengths fit it
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
