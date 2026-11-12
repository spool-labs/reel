//! A completion ring on the simulated disk: reads finish one job step at a time, in an order the seed picks

use std::io::{IoSliceMut, Result};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use reel::guard::hold;
use reel::io::IoFile;
use reel::io::ring::{RING_SLOT_BYTES, RING_SLOTS, RingDriver};
use reel_api::units::{ByteCount, FileOffset};

use crate::count_pause::CountPause;
use crate::simulated_file::SimulatedFile;
use crate::simulated_io::SimulatedIo;

/// The job that reaps this ring
pub const RING_JOB: &str = "reel-ring";

/// A read the device hasn't reached
struct WaitingRead {
    slot: usize,
    file: SimulatedFile,
    offset: FileOffset,
    length: usize,
}

/// The reads in flight and the bytes they land in
pub struct SimulatedRing {
    io: SimulatedIo,
    waiting: Mutex<Vec<WaitingRead>>,
    slots: Vec<Mutex<Vec<u8>>>,
    /// The most reads that waited at once, so a test can see reads sent together
    most_waiting: AtomicUsize,
}

impl SimulatedRing {
    /// Read the most reads that waited at once
    pub fn most_waiting(&self) -> usize {
        self.most_waiting.load(Ordering::Relaxed)
    }

    /// Build an empty ring
    pub fn new(io: SimulatedIo) -> Self {
        Self {
            io,
            waiting: Mutex::new(Vec::with_capacity(RING_SLOTS)),
            slots: (0..RING_SLOTS).map(|_slot| Mutex::new(vec![0; RING_SLOT_BYTES])).collect(),
            most_waiting: AtomicUsize::new(0),
        }
    }
}

impl RingDriver<SimulatedFile> for SimulatedRing {
    fn start(&self, slot: usize, file: &SimulatedFile, offset: FileOffset, length: usize) -> Result<()> {
        let _pause = CountPause::new();
        let mut waiting = hold(&self.waiting);
        waiting.push(WaitingRead { slot, file: file.clone(), offset, length });
        self.most_waiting.fetch_max(waiting.len(), Ordering::Relaxed);
        drop(waiting);
        self.io.note_ring_read();
        self.io.wake_jobs(RING_JOB);
        Ok(())
    }

    fn reap(&self, done: &mut dyn FnMut(usize, Result<usize>)) -> Result<bool> {
        let pause = CountPause::new();
        let mut waiting = hold(&self.waiting);
        if waiting.is_empty() {
            return Ok(false);
        }
        let pick = self.io.draw(waiting.len() as u64) as usize;
        let read = waiting.swap_remove(pick);
        drop(waiting);
        let mut bytes = hold(&self.slots[read.slot]);
        let answer = read.file.read_at(read.offset, &mut bytes[..read.length]).map(|count| count.0 as usize);
        drop(bytes);
        drop(pause);
        done(read.slot, answer);
        Ok(true)
    }

    fn reaps_on_wait(&self) -> bool {
        true
    }

    fn copy_out(&self, slot: usize, out: &mut [u8]) {
        let length = out.len();
        out.copy_from_slice(&hold(&self.slots[slot])[..length]);
    }

    fn copy_parts(&self, slot: usize, parts: &mut [IoSliceMut<'_>]) {
        let bytes = hold(&self.slots[slot]);
        let mut at = 0;
        for part in parts {
            let length = part.len();
            part.copy_from_slice(&bytes[at..at + length]);
            at += length;
        }
    }

    fn poke(&self) {}

    fn heap_bytes(&self) -> ByteCount {
        ByteCount((RING_SLOTS * RING_SLOT_BYTES) as u64)
    }
}
