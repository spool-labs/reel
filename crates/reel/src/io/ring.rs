//! Async reads through a completion ring: fixed slots the device reads into, and a waker for each read in flight
//!
//! A read lands in a slot the ring owns and its future copies it out, so a future dropped part way leaves the device nothing
//! of the caller's to write into. The slot goes free when its completion comes in.

use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::io::{Error, ErrorKind, IoSliceMut, Result};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::task::{Context, Poll, Waker};

use reel_api::units::{ByteCount, FileOffset};

use crate::guard::hold;
use crate::io::spin_clock::SpinClock;
use crate::io::{DeviceReads, IoFile, ManyRead, WalkMark};
use crate::lease::{Ended, LeaseCell};
use crate::pool::touched_queue;

/// Reads a ring holds in flight at once
pub const RING_SLOTS: usize = 64;
/// Bytes one slot takes, and a longer read goes through in turns
pub const RING_SLOT_BYTES: usize = 64 * 1024;
/// The most reads one call keeps out at once, a quarter of the slots so several scans and the gets still find some free
pub const MOST_MANY_READS: usize = RING_SLOTS / 4;
/// Async reads running at once, each with its own read buffers
pub const RING_READS: usize = 32;
/// Bytes of read buffers each running read holds
pub const RING_READ_BYTES: u64 = 256 * 1024;
/// Callers that wait in line, and any past that look again when the runtime polls them
pub const MOST_RING_WAITERS: usize = 1_024;

/// How long a reaper looks at the completions in memory before it waits in the kernel. A read in flight lands soon, and with
/// none in flight a look finds nothing, so a reaper spins only while reads are out and its CPU follows the read rate
#[cfg(any(target_os = "linux", feature = "test-hooks"))]
pub fn reap_spin(in_flight: usize, most: Duration) -> Duration {
    if in_flight > 0 { most } else { Duration::ZERO }
}

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

    /// Say whether a blocking wait reaps on its own thread, for a driver with no reaper thread to finish its reads
    fn reaps_on_wait(&self) -> bool {
        false
    }

    /// Bring a reaper out of a wait
    fn poke(&self);

    /// Read the heap the driver holds
    fn heap_bytes(&self) -> ByteCount;
}

/// The driver of a backend with no ring, which never exists
#[cfg(not(target_os = "linux"))]
#[derive(Clone, Copy, Debug)]
pub enum NoRing {}

#[cfg(not(target_os = "linux"))]
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

/// One slot's read: the device has it while it's busy, and its answer is the bytes it read
pub type RingSlot = LeaseCell<Result<usize>>;

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
    spinners: AtomicUsize,
    is_done: Vec<AtomicBool>,
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
pub struct OnRing<'ring, Driver>(pub &'ring ReadRing<Driver>, pub Option<&'ring WalkMark>);

impl<Driver> OnRing<'_, Driver> {
    /// Check this get's try already met a read that would block, so its reads go to the ring with no try of their own
    pub fn is_cold(&self) -> bool {
        self.1.is_some()
    }
}

impl<Driver> ReadRing<Driver> {
    /// Build a ring on a driver, with every slot allocated
    pub fn new(driver: Driver) -> Self {
        let slot = RingSlot::new;
        Self {
            driver,
            slots: (0..RING_SLOTS).map(|_slot| Mutex::new(slot())).collect(),
            free: Mutex::new((0..RING_SLOTS).collect()),
            seats: AtomicUsize::new(RING_READS),
            seat_line: WaitLine::new(),
            slot_line: WaitLine::new(),
            in_flight: AtomicUsize::new(0),
            is_closed: AtomicBool::new(false),
            spinners: AtomicUsize::new(0),
            is_done: (0..RING_SLOTS).map(|_slot| AtomicBool::new(false)).collect(),
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
        Ok(Some(RingLease { ring: self, slot }))
    }

    /// Give a slot back and wake the next in line
    pub fn release(&self, slot: usize) {
        hold(&self.slots[slot]).reset();
        hold(&self.free).push(slot);
        self.slot_line.wake_next();
    }

    /// Take a completion from the driver and wake its caller, or free the slot when the caller went away
    pub fn finish(&self, slot: usize, answer: Result<usize>) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
        let ended = hold(&self.slots[slot]).end(answer);
        self.is_done[slot].store(true, Ordering::Release);
        match ended {
            Ended::Wake(Some(waker)) => waker.wake(),
            Ended::Wake(None) => {},
            Ended::Release => self.release(slot),
        }
    }
}

impl<Driver> ReadRing<Driver> {
    /// Read up to a slot's bytes, and say how many came
    async fn read_once<File>(&self, file: &File, offset: FileOffset, out: &mut [u8]) -> Result<usize>
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
    pub async fn read_into<File>(&self, file: &File, offset: FileOffset, buf: &mut [u8], is_cold: bool) -> Result<()>
    where
        Driver: RingDriver<File>,
        File: IoFile + Sync,
    {
        let mut done = 0;
        // what the kernel holds answers here, with no submit and no wake
        while !is_cold && done < buf.len() {
            match file.try_read_at(FileOffset(offset.0 + done as u64), &mut buf[done..]) {
                Ok(Some(ByteCount(count))) if count > 0 => done += count as usize,
                Ok(Some(_) | None) | Err(_) => break,
            }
        }
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
    pub async fn read_into_parts<File>(&self, file: &File, offset: FileOffset, parts: &mut [IoSliceMut<'_>], is_cold: bool) -> Result<()>
    where
        Driver: RingDriver<File>,
        File: IoFile + Sync,
    {
        let total: usize = parts.iter().map(|part| part.len()).sum();
        if !is_cold && matches!(file.try_read_parts_at(offset, parts), Ok(Some(ByteCount(count))) if count == total as u64) {
            return Ok(());
        }
        if total > RING_SLOT_BYTES {
            let mut at = offset.0;
            for part in parts {
                self.read_into(file, FileOffset(at), part, is_cold).await?;
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
                self.read_into(file, FileOffset(at), part, is_cold).await?;
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
        self.ring.is_done[self.slot].store(false, Ordering::Release);
        hold(&self.ring.slots[self.slot]).start();
        if let Err(error) = self.ring.driver.start(self.slot, file, offset, length) {
            hold(&self.ring.slots[self.slot]).unstart();
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
        poll_fn(|context| hold(&self.ring.slots[self.slot]).poll_answer(context))
    }
}

impl<Driver> Drop for RingLease<'_, Driver> {
    fn drop(&mut self) {
        let is_free = hold(&self.ring.slots[self.slot]).leave();
        if is_free {
            self.ring.release(self.slot);
        }
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
        self.0.read_into(file, offset, buf, self.is_cold())
    }

    fn read_parts(&self, file: &File, offset: FileOffset, parts: &mut [IoSliceMut<'_>]) -> impl Future<Output = Result<()>> + Send {
        self.0.read_into_parts(file, offset, parts, self.is_cold())
    }

    fn holds_across_waits(&self) -> bool {
        true
    }

    fn resume_mark(&self) -> Option<&WalkMark> {
        self.1
    }
}

/// How long a blocking read with a spin seat spins on its slot before it parks. On the ccx33 a cold 4 KiB read lands in 110 to
/// 140 us, and 120 to 1000 us all ran the same, while 60 us parked before the read landed and lost 10%
pub const SEATED_SPIN: Duration = Duration::from_micros(250);
/// How long a blocking read with no spin seat spins before it parks. On the ccx33 at 8 readers 0 and 10 us ran the same, 30 us
/// lost 4% and 100 us lost 8%
pub const WAIT_SPIN: Duration = Duration::from_micros(10);
/// Readers that may spin at once. On the ccx33 a spinning reader took a cold get from 170 us to 142 and gained 10 to 20% at 1 to 4
/// readers, and at 8 readers 2 seats ran 52 to 55k gets/s against 51 to 53k parked, 4 seats 50k and 6 seats 42k. Beside two busy
/// threads 6 seats fell to 26k with a 3.3 ms p99, while 2 seats ran 46k
pub const SPIN_SEATS: usize = 2;

/// Unparks the thread it was made on
pub struct Unpark(std::thread::Thread);

impl std::task::Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

thread_local! {
    /// This thread's waker for a parked blocking read, made once so a read allocates nothing
    static THREAD_WAKER: Waker = Waker::from(Arc::new(Unpark(std::thread::current())));
}

/// Reads that wait for a ring on the calling thread, for a blocking get. What the kernel holds answers with no submit, and a read
/// it doesn't hold goes to the ring, and the caller waits on its slot until the reaper lands it. A store with no ring reads in place
#[derive(Debug)]
pub struct RingWait<'ring, Driver>(pub Option<&'ring ReadRing<Driver>>);

impl<Driver> ReadRing<Driver> {
    fn read_once_now<File>(&self, file: &File, offset: FileOffset, out: &mut [u8]) -> Result<usize>
    where
        Driver: RingDriver<File>,
        File: IoFile,
    {
        // with every slot taken a wait would only burn a core, so the read goes to the kernel on this thread
        let Some(lease) = self.try_claim()? else {
            return file.read_at(offset, out).map(|count| count.0 as usize);
        };
        lease.start(file, offset, out.len())?;
        let length = lease.wait_now()?;
        self.driver.copy_out(lease.slot, &mut out[..length]);
        Ok(length)
    }

    /// Fill a buffer on this thread: through the ring when it fits a slot, else in one plain read
    pub fn read_into_now<File>(&self, file: &File, offset: FileOffset, buf: &mut [u8]) -> Result<()>
    where
        Driver: RingDriver<File>,
        File: IoFile,
    {
        if buf.len() > RING_SLOT_BYTES {
            return file.read_exact_at(offset, buf);
        }
        let mut done = 0;
        while done < buf.len() {
            match file.try_read_at(FileOffset(offset.0 + done as u64), &mut buf[done..]) {
                Ok(Some(ByteCount(count))) if count > 0 => done += count as usize,
                Ok(Some(_) | None) | Err(_) => break,
            }
        }
        while done < buf.len() {
            // the buffer fits one slot, so each try reads what's left, and a short read comes back for the rest
            let read = self.read_once_now(file, FileOffset(offset.0 + done as u64), &mut buf[done..])?;
            if read == 0 {
                return Err(Error::from(ErrorKind::UnexpectedEof));
            }
            done += read;
        }
        Ok(())
    }

    /// Fill several buffers on this thread: the page cache answers what it holds, the rest go to the ring together, and then
    /// each waits for its own. A read past one slot, or one that finds every slot taken, goes in one plain read
    pub fn read_many_now<File>(&self, reads: &mut [Option<ManyRead<'_, File>>])
    where
        Driver: RingDriver<File>,
        File: IoFile,
    {
        let mut leases: [Option<RingLease<'_, Driver>>; MOST_MANY_READS] = [const { None }; MOST_MANY_READS];
        for (read, lease) in reads.iter_mut().zip(leases.iter_mut()).filter_map(|(read, lease)| Some((read.as_mut()?, lease))) {
            let length = read.buf.len();
            read.is_done = length <= RING_SLOT_BYTES
                && matches!(read.file.try_read_at(read.offset, read.buf), Ok(Some(ByteCount(count))) if count == length as u64);
            if read.is_done || length > RING_SLOT_BYTES {
                continue;
            }
            if let Ok(Some(claimed)) = self.try_claim()
                && claimed.start(read.file, read.offset, length).is_ok()
            {
                *lease = Some(claimed);
            }
        }
        for (index, read) in reads.iter_mut().enumerate().filter_map(|(index, read)| Some((index, read.as_mut()?))) {
            if read.is_done {
                continue;
            }
            if let Some(lease) = leases.get_mut(index).and_then(Option::take)
                && lease.wait_now().is_ok_and(|length| length == read.buf.len())
            {
                self.driver.copy_out(lease.slot, read.buf);
                read.is_done = true;
                continue;
            }
            read.is_done = self.read_into_now(read.file, read.offset, read.buf).is_ok();
        }
    }

    /// Fill parts on this thread: through the ring when they fit a slot, else in one plain read
    pub fn read_parts_now<File>(&self, file: &File, offset: FileOffset, parts: &mut [IoSliceMut<'_>]) -> Result<()>
    where
        Driver: RingDriver<File>,
        File: IoFile,
    {
        let total: usize = parts.iter().map(|part| part.len()).sum();
        if total > RING_SLOT_BYTES {
            return file.read_parts_at(offset, parts);
        }
        if matches!(file.try_read_parts_at(offset, parts), Ok(Some(ByteCount(count))) if count == total as u64) {
            return Ok(());
        }
        let Some(lease) = self.try_claim()? else {
            return file.read_parts_at(offset, parts);
        };
        lease.start(file, offset, total)?;
        if lease.wait_now()? == total {
            self.driver.copy_parts(lease.slot, parts);
            return Ok(());
        }
        // a short read goes part by part
        drop(lease);
        let mut at = offset.0;
        for part in parts {
            self.read_into_now(file, FileOffset(at), part)?;
            at += part.len() as u64;
        }
        Ok(())
    }
}

impl<Driver> RingLease<'_, Driver> {
    /// Wait on this thread for the slot's read to land, and take its answer. A reader spins while a core is free for it, then parks
    /// until the reaper's completion wakes it, so a full box keeps its cores for the submit thread and the reaper
    pub fn wait_now<File>(&self) -> Result<usize>
    where
        Driver: RingDriver<File>,
    {
        // the first wait on a thread makes its waker, so a later park allocates nothing
        THREAD_WAKER.with(|_| ());
        let spinners = &self.ring.spinners;
        let mut is_seated = spinners.fetch_add(1, Ordering::Relaxed) < SPIN_SEATS;
        if !is_seated {
            spinners.fetch_sub(1, Ordering::Relaxed);
        }
        let spin = if is_seated { SEATED_SPIN } else { WAIT_SPIN };
        let mut clock = SpinClock::new(spin);
        let answer = loop {
            // a driver with no reaper of its own, as a simulated ring, reaps on the waiting thread until the answer lands
            if self.ring.driver.reaps_on_wait() && !self.ring.is_done[self.slot].load(Ordering::Acquire) {
                if let Err(error) = self.ring.driver.reap(&mut |slot, answer| self.ring.finish(slot, answer)) {
                    // a failed reap ends the wait, and the seat goes back for the next reader
                    if is_seated {
                        spinners.fetch_sub(1, Ordering::Relaxed);
                    }
                    return Err(error);
                }
                continue;
            }
            // the completion sets the flag once the answer has landed, so a spin reads it with no lock and locks once
            if !self.ring.is_done[self.slot].load(Ordering::Acquire) && !clock.is_over() {
                std::hint::spin_loop();
                continue;
            }
            {
                let mut held = hold(&self.ring.slots[self.slot]);
                if let Some(answer) = held.take_answer() {
                    break answer;
                }
                if clock.is_over_now() {
                    THREAD_WAKER.with(|waker| held.wait_with(waker));
                    drop(held);
                    // a parked reader gives its seat to the next
                    if is_seated {
                        spinners.fetch_sub(1, Ordering::Relaxed);
                        is_seated = false;
                    }
                    std::thread::park();
                    continue;
                }
            }
        };
        if is_seated {
            spinners.fetch_sub(1, Ordering::Relaxed);
        }
        answer
    }
}

impl<File: IoFile, Driver: RingDriver<File>> DeviceReads<File> for RingWait<'_, Driver> {
    fn read_exact(&self, file: &File, offset: FileOffset, buf: &mut [u8]) -> impl Future<Output = Result<()>> + Send {
        std::future::ready(match self.0 {
            Some(ring) => ring.read_into_now(file, offset, buf),
            None => file.read_exact_at(offset, buf),
        })
    }

    fn read_parts(&self, file: &File, offset: FileOffset, parts: &mut [IoSliceMut<'_>]) -> impl Future<Output = Result<()>> + Send {
        std::future::ready(match self.0 {
            Some(ring) => ring.read_parts_now(file, offset, parts),
            None => file.read_parts_at(offset, parts),
        })
    }

    fn read_many(&self, reads: &mut [Option<ManyRead<'_, File>>]) -> impl Future<Output = ()> + Send
    where
        File: Sync,
    {
        match self.0 {
            Some(ring) => ring.read_many_now(reads),
            None => {
                for read in reads.iter_mut().flatten() {
                    read.is_done = read.file.read_exact_at(read.offset, read.buf).is_ok();
                }
            },
        }
        std::future::ready(())
    }
}
