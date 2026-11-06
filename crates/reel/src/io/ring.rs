//! Async reads through a completion ring: fixed slots the device reads into, and a waker for each read in flight
//!
//! A read lands in a slot the ring owns and its future copies it out, so a future dropped part way leaves the device nothing
//! of the caller's to write into. The slot goes free when its completion comes in.

use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::io::{Error, ErrorKind, IoSliceMut, Result};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};

use reel_api::units::{ByteCount, FileOffset};

use crate::guard::hold;
use crate::io::{DeviceReads, IoFile};
use crate::pool::touched_queue;

/// Reads a ring holds in flight at once
pub const RING_SLOTS: usize = 64;
/// Bytes one slot takes, and a longer read goes through in turns
pub const RING_SLOT_BYTES: usize = 64 * 1024;
/// Async reads running at once, each with its own read buffers
pub const RING_READS: usize = 32;
/// Bytes of read buffers each running read holds
pub const RING_READ_BYTES: u64 = 256 * 1024;
/// Callers that wait in line, and any past that look again when the runtime polls them
pub const MOST_RING_WAITERS: usize = 1_024;

/// A backend's completion queue
pub trait RingDriver<File>: Send + Sync + 'static {
    /// Start a read into a slot
    fn start(&self, slot: usize, file: &File, offset: FileOffset, length: usize) -> Result<()>;

    /// Hand over finished reads, and say whether to look again. A driver may wait here for the first, and one that returns
    /// false wakes its job itself at the next read
    fn reap(&self, done: &mut dyn FnMut(usize, Result<usize>)) -> Result<bool>;

    /// Copy a finished read out of its slot
    fn copy_out(&self, slot: usize, out: &mut [u8]);

    /// Copy a finished read out of its slot into parts
    fn copy_parts(&self, slot: usize, parts: &mut [IoSliceMut<'_>]);

    /// Bring a reaper out of a wait
    fn poke(&self);

    /// Read the heap the driver holds
    fn heap_bytes(&self) -> ByteCount;
}

/// The driver of a backend with no ring, which never exists
#[derive(Clone, Copy, Debug)]
pub enum NoRing {}

impl<File> RingDriver<File> for NoRing {
    fn start(&self, _slot: usize, _file: &File, _offset: FileOffset, _length: usize) -> Result<()> {
        match *self {}
    }

    fn reap(&self, _done: &mut dyn FnMut(usize, Result<usize>)) -> Result<bool> {
        match *self {}
    }

    fn copy_out(&self, _slot: usize, _out: &mut [u8]) {
        match *self {}
    }

    fn copy_parts(&self, _slot: usize, _parts: &mut [IoSliceMut<'_>]) {
        match *self {}
    }

    fn poke(&self) {
        match *self {}
    }

    fn heap_bytes(&self) -> ByteCount {
        match *self {}
    }
}

/// What a slot is doing
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RingPhase {
    /// Nobody holds it
    Free,
    /// A read holds it and hasn't started
    Held,
    /// The device has it
    Reading,
    /// The read is over and its answer waits for the caller
    Done,
    /// The caller went away, and the completion frees it
    Dropped,
}

/// One slot's state
#[derive(Debug)]
pub struct RingSlot {
    phase: RingPhase,
    answer: Result<usize>,
    waker: Option<Waker>,
}

/// Callers waiting for one kind of room, oldest first. A wake goes to one caller, so one that leaves takes itself out of line
/// or hands on a wake it got and didn't use
#[derive(Debug)]
pub struct WaitLine {
    spots: Mutex<VecDeque<(u64, Waker)>>,
    next_id: AtomicU64,
}

/// A caller's place in a line, which it gives up however it ends
#[derive(Debug)]
pub struct LineSpot<'line> {
    line: &'line WaitLine,
    id: Option<u64>,
    is_served: bool,
}

impl WaitLine {
    /// Allocate a line with room for every caller it keeps
    pub fn new() -> Self {
        Self {
            spots: Mutex::new(touched_queue(MOST_RING_WAITERS, (0, Waker::noop().clone()))),
            next_id: AtomicU64::new(0),
        }
    }

    /// Wake the first in line
    pub fn wake_next(&self) {
        if let Some((_id, waker)) = hold(&self.spots).pop_front() {
            waker.wake();
        }
    }

    /// Wake everyone in line
    pub fn wake_all(&self) {
        while let Some((_id, waker)) = hold(&self.spots).pop_front() {
            waker.wake();
        }
    }

    /// Stand in line, or keep a caller's place with its newest waker. A full line has the runtime poll again
    pub fn stand(&self, spot: &mut Option<u64>, context: &mut Context<'_>) {
        let mut spots = hold(&self.spots);
        if let Some(entry) = spots.iter_mut().find(|entry| Some(entry.0) == *spot) {
            if !entry.1.will_wake(context.waker()) {
                entry.1 = context.waker().clone();
            }
            return;
        }
        if spots.len() == MOST_RING_WAITERS {
            drop(spots);
            *spot = None;
            context.waker().wake_by_ref();
            return;
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        spots.push_back((id, context.waker().clone()));
        *spot = Some(id);
    }

    /// Wait for a take to succeed, in line between tries. A take after standing catches room freed before the waker went in
    pub async fn wait_for<Item>(&self, take: impl Fn() -> Result<Option<Item>>) -> Result<Item> {
        let mut spot = LineSpot { line: self, id: None, is_served: false };
        poll_fn(|context| {
            if let Some(item) = take()? {
                spot.is_served = true;
                return Poll::Ready(Ok(item));
            }
            self.stand(&mut spot.id, context);
            match take()? {
                Some(item) => {
                    spot.is_served = true;
                    Poll::Ready(Ok(item))
                },
                None => Poll::Pending,
            }
        })
        .await
    }
}

impl Default for WaitLine {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for LineSpot<'_> {
    fn drop(&mut self) {
        let Some(id) = self.id else {
            return;
        };
        let mut spots = hold(&self.line.spots);
        if let Some(position) = spots.iter().position(|entry| entry.0 == id) {
            spots.remove(position);
            return;
        }
        drop(spots);
        // the wake came here, and a caller that took nothing hands it on
        if !self.is_served {
            self.line.wake_next();
        }
    }
}

/// The slots of a ring and the callers waiting for them
#[derive(Debug)]
pub struct ReadRing<Driver> {
    pub driver: Driver,
    slots: Vec<Mutex<RingSlot>>,
    free: Mutex<Vec<usize>>,
    seats: AtomicUsize,
    seat_line: WaitLine,
    slot_line: WaitLine,
    in_flight: AtomicUsize,
    is_closed: AtomicBool,
}

/// A slot a read holds
#[derive(Debug)]
pub struct RingLease<'ring, Driver> {
    ring: &'ring ReadRing<Driver>,
    slot: usize,
}

/// A place among the reads running, which goes back however the read ends
#[derive(Debug)]
pub struct RingSeat<'ring, Driver> {
    ring: &'ring ReadRing<Driver>,
}

/// Reads that complete through a ring, so a caller waits with no thread held
#[derive(Debug)]
pub struct OnRing<'ring, Driver>(pub &'ring ReadRing<Driver>);

impl<Driver> ReadRing<Driver> {
    /// Build a ring on a driver, with every slot allocated
    pub fn new(driver: Driver) -> Self {
        let slot = || RingSlot { phase: RingPhase::Free, answer: Ok(0), waker: None };
        Self {
            driver,
            slots: (0..RING_SLOTS).map(|_slot| Mutex::new(slot())).collect(),
            free: Mutex::new((0..RING_SLOTS).collect()),
            seats: AtomicUsize::new(RING_READS),
            seat_line: WaitLine::new(),
            slot_line: WaitLine::new(),
            in_flight: AtomicUsize::new(0),
            is_closed: AtomicBool::new(false),
        }
    }

    /// Read the heap the slots hold, past the driver's own
    pub fn heap_bytes(&self) -> ByteCount {
        let lists = RING_SLOTS * (size_of::<RingSlot>() + size_of::<usize>()) + 2 * MOST_RING_WAITERS * size_of::<(u64, Waker)>();
        ByteCount(lists as u64)
    }

    /// Refuse new reads, so the reads in flight can drain
    pub fn close(&self) {
        self.is_closed.store(true, Ordering::Release);
        self.seat_line.wake_all();
        self.slot_line.wake_all();
    }

    /// Check nothing is left for the device to finish
    pub fn is_drained(&self) -> bool {
        self.is_closed.load(Ordering::Acquire) && self.in_flight.load(Ordering::Acquire) == 0
    }

    /// Take a place among the reads running, or wait in line for one
    pub async fn seat(&self) -> Result<RingSeat<'_, Driver>> {
        self.seat_line.wait_for(|| self.try_seat()).await
    }

    /// Take a place if one is free
    pub fn try_seat(&self) -> Result<Option<RingSeat<'_, Driver>>> {
        self.check_open()?;
        let taken = self.seats.fetch_update(Ordering::AcqRel, Ordering::Acquire, |seats| seats.checked_sub(1));
        Ok(taken.is_ok().then(|| RingSeat { ring: self }))
    }

    /// Refuse a read on a ring that's closing
    pub fn check_open(&self) -> Result<()> {
        if self.is_closed.load(Ordering::Acquire) {
            return Err(Error::from(ErrorKind::BrokenPipe));
        }
        Ok(())
    }

    /// Take a free slot, or wait in line for one
    pub async fn claim(&self) -> Result<RingLease<'_, Driver>> {
        self.slot_line.wait_for(|| self.try_claim()).await
    }

    /// Take a free slot if one stands free
    pub fn try_claim(&self) -> Result<Option<RingLease<'_, Driver>>> {
        self.check_open()?;
        let Some(slot) = hold(&self.free).pop() else {
            return Ok(None);
        };
        hold(&self.slots[slot]).phase = RingPhase::Held;
        Ok(Some(RingLease { ring: self, slot }))
    }

    /// Give a slot back and wake the next in line
    pub fn release(&self, slot: usize) {
        {
            let mut held = hold(&self.slots[slot]);
            held.phase = RingPhase::Free;
            held.waker = None;
        }
        hold(&self.free).push(slot);
        self.slot_line.wake_next();
    }

    /// Take a completion from the driver and wake its caller, or free the slot when the caller went away
    pub fn finish(&self, slot: usize, answer: Result<usize>) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
        let mut held = hold(&self.slots[slot]);
        if held.phase == RingPhase::Dropped {
            drop(held);
            self.release(slot);
            return;
        }
        held.answer = answer;
        held.phase = RingPhase::Done;
        let waker = held.waker.take();
        drop(held);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<Driver> ReadRing<Driver> {
    /// Read up to a slot's bytes, and say how many came
    pub async fn read_once<File>(&self, file: &File, offset: FileOffset, out: &mut [u8]) -> Result<usize>
    where
        Driver: RingDriver<File>,
        File: Sync,
    {
        let lease = self.claim().await?;
        lease.start(file, offset, out.len())?;
        let length = lease.finished().await?;
        self.driver.copy_out(lease.slot, &mut out[..length]);
        Ok(length)
    }

    /// Fill a buffer a slot at a time
    pub async fn read_into<File>(&self, file: &File, offset: FileOffset, buf: &mut [u8]) -> Result<()>
    where
        Driver: RingDriver<File>,
        File: Sync,
    {
        let mut done = 0;
        while done < buf.len() {
            let end = (done + RING_SLOT_BYTES).min(buf.len());
            let read = self.read_once(file, FileOffset(offset.0 + done as u64), &mut buf[done..end]).await?;
            if read == 0 {
                return Err(Error::from(ErrorKind::UnexpectedEof));
            }
            done += read;
        }
        Ok(())
    }

    /// Fill parts in one read when a slot holds them all, and part by part when one doesn't
    pub async fn read_into_parts<File>(&self, file: &File, offset: FileOffset, parts: &mut [IoSliceMut<'_>]) -> Result<()>
    where
        Driver: RingDriver<File>,
        File: Sync,
    {
        let total: usize = parts.iter().map(|part| part.len()).sum();
        if total > RING_SLOT_BYTES {
            let mut at = offset.0;
            for part in parts {
                self.read_into(file, FileOffset(at), part).await?;
                at += part.len() as u64;
            }
            return Ok(());
        }

        let lease = self.claim().await?;
        lease.start(file, offset, total)?;
        // a short read goes round part by part, as a filled buffer does
        if lease.finished().await? != total {
            drop(lease);
            let mut at = offset.0;
            for part in parts {
                self.read_into(file, FileOffset(at), part).await?;
                at += part.len() as u64;
            }
            return Ok(());
        }
        self.driver.copy_parts(lease.slot, parts);
        Ok(())
    }
}

impl<Driver> RingLease<'_, Driver> {
    /// Hand the slot to the device, and leave it held when the start fails
    pub fn start<File>(&self, file: &File, offset: FileOffset, length: usize) -> Result<()>
    where
        Driver: RingDriver<File>,
    {
        self.ring.in_flight.fetch_add(1, Ordering::AcqRel);
        hold(&self.ring.slots[self.slot]).phase = RingPhase::Reading;
        if let Err(error) = self.ring.driver.start(self.slot, file, offset, length) {
            hold(&self.ring.slots[self.slot]).phase = RingPhase::Held;
            self.ring.in_flight.fetch_sub(1, Ordering::AcqRel);
            return Err(error);
        }
        Ok(())
    }

    /// Wait for the read and take its answer
    pub fn finished(&self) -> impl Future<Output = Result<usize>> + Send + '_
    where
        Driver: Sync,
    {
        poll_fn(|context| {
            let mut held = hold(&self.ring.slots[self.slot]);
            if held.phase != RingPhase::Done {
                if !held.waker.as_ref().is_some_and(|waker| waker.will_wake(context.waker())) {
                    held.waker = Some(context.waker().clone());
                }
                return Poll::Pending;
            }
            Poll::Ready(std::mem::replace(&mut held.answer, Ok(0)))
        })
    }
}

impl<Driver> Drop for RingLease<'_, Driver> {
    fn drop(&mut self) {
        let mut held = hold(&self.ring.slots[self.slot]);
        if held.phase == RingPhase::Reading {
            held.phase = RingPhase::Dropped;
            return;
        }
        drop(held);
        self.ring.release(self.slot);
    }
}

impl<Driver> Drop for RingSeat<'_, Driver> {
    fn drop(&mut self) {
        self.ring.seats.fetch_add(1, Ordering::AcqRel);
        self.ring.seat_line.wake_next();
    }
}

impl<File: IoFile, Driver: RingDriver<File>> DeviceReads<File> for OnRing<'_, Driver> {
    fn read_exact(&self, file: &File, offset: FileOffset, buf: &mut [u8]) -> impl Future<Output = Result<()>> + Send {
        self.0.read_into(file, offset, buf)
    }

    fn read_parts(&self, file: &File, offset: FileOffset, parts: &mut [IoSliceMut<'_>]) -> impl Future<Output = Result<()>> + Send {
        self.0.read_into_parts(file, offset, parts)
    }

    fn holds_across_waits(&self) -> bool {
        true
    }
}
