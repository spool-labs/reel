//! Async reads: fixed slots and a bounded pool of engine threads, so a caller's runtime thread never waits on a device
//!
//! A read moves the caller's buffers into a slot and hands them back when it's done, so it copies only its keys and needs
//! nothing that outlives the call. A future dropped part way leaves its slot to the worker, which frees it at the end.

use std::collections::VecDeque;
use std::future::poll_fn;
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use reel_api::column_id::ColumnId;
use reel_api::scan::ScanStep;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, WritePlace};
use reel_api::value_list::ValueList;

use crate::engine::engine_state::EngineState;
use crate::engine::read_path::RecordWant;
use crate::engine::scan_path::ScanWant;
use crate::engine::scan_state::ScanState;
use crate::guard::hold;
use crate::io::job::{Job, JobStep};
use crate::io::ring::{OnRing, ReadRing};
use crate::io::{DeviceReads, InPlace, Io, IoTask, in_place};
use crate::pool::{touched_capacity, touched_queue};

/// Async reads in flight at once
pub const READ_SLOTS: usize = 64;
/// Bytes of keys a slot holds, and a longer get_many goes through in turns
pub const SLOT_KEY_BYTES: usize = 16 * 1024;
/// Keys a slot holds
pub const SLOT_KEYS: usize = 1_024;
/// Keys an async scan yields at a time
pub const SCAN_BLOCK_KEYS: usize = 256;
/// Callers that wait in line for a slot, and any past that look again when the runtime polls them
pub const MOST_WAITERS: usize = 1_024;

/// What a slot is doing
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotPhase {
    /// Nobody holds it
    Free,
    /// A caller is filling it
    Held,
    /// It waits for a worker
    Queued,
    /// A worker is reading
    Running,
    /// The read is over and its answer waits for the caller
    Done,
    /// The caller went away, and the worker frees it
    Dropped,
}

/// One read, with the caller's buffers
#[derive(Debug, Default)]
#[allow(clippy::large_enum_variant, reason = "a box would allocate on every block a walk streams")]
pub enum ReadWork {
    /// Nothing
    #[default]
    Empty,
    /// One key at the newest point or a cue's
    Get {
        column: ColumnId,
        want: RecordWant,
        point: Option<WritePlace>,
        value: Vec<u8>,
    },
    /// The slot's keys at one point
    Many {
        column: ColumnId,
        point: WritePlace,
        handovers: u64,
        values: ValueList,
    },
    /// The next block of a scan's keys
    Block { scan: ScanState, keys: ValueList },
}

/// One read in flight
#[derive(Debug)]
pub struct ReadSlot {
    phase: SlotPhase,
    work: ReadWork,
    keys: Vec<u8>,
    key_ends: Vec<usize>,
    answer: Result<bool, StoreError>,
    waker: Option<Waker>,
}

/// The slots and the line of work
#[derive(Debug)]
pub struct ReadPool {
    slots: Vec<Mutex<ReadSlot>>,
    free: Mutex<Vec<usize>>,
    queue: Mutex<VecDeque<usize>>,
    waiters: Mutex<VecDeque<Waker>>,
    next_worker: AtomicUsize,
    workers: usize,
    pub scans: Mutex<Vec<ScanState>>,
}

/// A slot a caller holds
#[derive(Debug)]
pub struct SlotLease<'pool> {
    pool: &'pool ReadPool,
    index: usize,
}

/// A worker for one store
pub struct ReadWorker<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
}

impl ReadSlot {
    /// Allocate a slot's buffers
    pub fn new() -> Self {
        Self {
            phase: SlotPhase::Free,
            work: ReadWork::Empty,
            keys: touched_capacity(SLOT_KEY_BYTES, 0),
            key_ends: touched_capacity(SLOT_KEYS, 0),
            answer: Ok(false),
            waker: None,
        }
    }

    /// Start empty for the next caller
    pub fn reset(&mut self) {
        self.phase = SlotPhase::Free;
        self.keys.clear();
        self.key_ends.clear();
        self.answer = Ok(false);
        self.waker = None;
    }
}

impl Default for ReadSlot {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadPool {
    /// Allocate every slot
    pub fn new(scans: Vec<ScanState>, workers: usize) -> Self {
        let mut queue = VecDeque::with_capacity(READ_SLOTS);
        queue.extend(0..READ_SLOTS);
        queue.clear();
        Self {
            slots: (0..READ_SLOTS).map(|_slot| Mutex::new(ReadSlot::new())).collect(),
            free: Mutex::new((0..READ_SLOTS).collect()),
            queue: Mutex::new(queue),
            waiters: Mutex::new(touched_queue(MOST_WAITERS, Waker::noop().clone())),
            next_worker: AtomicUsize::new(0),
            workers,
            scans: Mutex::new(scans),
        }
    }

    /// Read the heap the pool holds
    pub fn heap_bytes(&self) -> ByteCount {
        let slot = size_of::<ReadSlot>() + SLOT_KEY_BYTES + SLOT_KEYS * size_of::<usize>();
        let lines = READ_SLOTS * 2 * size_of::<usize>() + MOST_WAITERS * size_of::<Waker>();
        ByteCount((READ_SLOTS * slot + lines) as u64)
    }

    /// Take a free slot, or wait in line for one
    pub async fn claim(&self) -> SlotLease<'_> {
        poll_fn(|context| self.poll_claim(context)).await
    }

    /// Take a free slot, or leave a waker in line. A line that's full looks again when the runtime next polls
    pub fn poll_claim(&self, context: &mut Context<'_>) -> Poll<SlotLease<'_>> {
        if let Some(lease) = self.try_claim() {
            return Poll::Ready(lease);
        }
        let mut waiters = hold(&self.waiters);
        if waiters.len() == MOST_WAITERS {
            drop(waiters);
            context.waker().wake_by_ref();
            return Poll::Pending;
        }
        waiters.push_back(context.waker().clone());
        drop(waiters);
        // a slot freed before the waker went in would wake nobody
        match self.try_claim() {
            Some(lease) => Poll::Ready(lease),
            None => Poll::Pending,
        }
    }

    /// Take a free slot
    pub fn try_claim(&self) -> Option<SlotLease<'_>> {
        let index = hold(&self.free).pop()?;
        hold(&self.slots[index]).phase = SlotPhase::Held;
        Some(SlotLease { pool: self, index })
    }

    /// Hand a slot back and wake the next in line
    pub fn release(&self, index: usize) {
        let mut slot = hold(&self.slots[index]);
        let work = mem::take(&mut slot.work);
        slot.reset();
        drop(slot);
        // a walk whose caller went away still gives its buffers back
        if let ReadWork::Block { scan, .. } = work {
            hold(&self.scans).push(scan);
        }
        hold(&self.free).push(index);
        if let Some(waiter) = hold(&self.waiters).pop_front() {
            waiter.wake();
        }
    }

    /// Take the next read for a worker, with what it needs out of the slot
    pub fn next_queued(&self) -> Option<(usize, ReadWork, Vec<u8>, Vec<usize>)> {
        loop {
            let index = hold(&self.queue).pop_front()?;
            let mut slot = hold(&self.slots[index]);
            if slot.phase == SlotPhase::Dropped {
                drop(slot);
                self.release(index);
                continue;
            }
            if slot.phase != SlotPhase::Queued {
                continue;
            }
            slot.phase = SlotPhase::Running;
            let (work, keys, ends) = (mem::take(&mut slot.work), mem::take(&mut slot.keys), mem::take(&mut slot.key_ends));
            return Some((index, work, keys, ends));
        }
    }

    /// Put a finished read back and wake its caller, or free the slot when the caller went away
    pub fn finish(&self, index: usize, work: ReadWork, keys: Vec<u8>, ends: Vec<usize>, answer: Result<bool, StoreError>) {
        let mut slot = hold(&self.slots[index]);
        slot.keys = keys;
        slot.key_ends = ends;
        slot.work = work;
        if slot.phase == SlotPhase::Dropped {
            drop(slot);
            self.release(index);
            return;
        }
        slot.answer = answer;
        slot.phase = SlotPhase::Done;
        let waker = slot.waker.take();
        drop(slot);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// End every queued read, for a store that's going
    pub fn refuse_queued(&self) {
        while let Some(index) = hold(&self.queue).pop_front() {
            let mut slot = hold(&self.slots[index]);
            if slot.phase == SlotPhase::Dropped {
                drop(slot);
                self.release(index);
                continue;
            }
            slot.answer = Err(StoreError::Closed);
            slot.phase = SlotPhase::Done;
            let waker = slot.waker.take();
            drop(slot);
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}

impl SlotLease<'_> {
    /// Fill the slot with one key and a read
    pub fn load(&self, key: &[u8], work: ReadWork) {
        let mut slot = hold(&self.pool.slots[self.index]);
        slot.keys.extend_from_slice(key);
        let end = slot.keys.len();
        slot.key_ends.push(end);
        slot.work = work;
    }

    /// Fill the slot with as many keys as it holds, at least one, and say how many
    pub fn load_keys(&self, keys: &[&[u8]], work: ReadWork) -> usize {
        let mut slot = hold(&self.pool.slots[self.index]);
        let mut taken = 0;
        for key in keys {
            let is_full = slot.keys.len() + key.len() > SLOT_KEY_BYTES || slot.key_ends.len() == SLOT_KEYS;
            if is_full && taken > 0 {
                break;
            }
            slot.keys.extend_from_slice(key);
            let end = slot.keys.len();
            slot.key_ends.push(end);
            taken += 1;
        }
        slot.work = work;
        taken
    }

    /// Fill the slot with work that has no keys
    pub fn set_work(&self, work: ReadWork) {
        hold(&self.pool.slots[self.index]).work = work;
    }

    /// Put the slot in line, and say which worker to wake
    pub fn submit(&self) -> usize {
        hold(&self.pool.slots[self.index]).phase = SlotPhase::Queued;
        hold(&self.pool.queue).push_back(self.index);
        self.pool.next_worker.fetch_add(1, Ordering::Relaxed) % self.pool.workers
    }

    /// Wait for the read and take back its work and answer
    pub async fn finished(&self) -> (ReadWork, Result<bool, StoreError>) {
        poll_fn(|context| {
            let mut slot = hold(&self.pool.slots[self.index]);
            if slot.phase != SlotPhase::Done {
                if !slot.waker.as_ref().is_some_and(|waker| waker.will_wake(context.waker())) {
                    slot.waker = Some(context.waker().clone());
                }
                return Poll::Pending;
            }
            let answer = mem::replace(&mut slot.answer, Ok(false));
            Poll::Ready((mem::take(&mut slot.work), answer))
        })
        .await
    }
}

impl Drop for SlotLease<'_> {
    fn drop(&mut self) {
        let mut slot = hold(&self.pool.slots[self.index]);
        if matches!(slot.phase, SlotPhase::Queued | SlotPhase::Running) {
            slot.phase = SlotPhase::Dropped;
            return;
        }
        drop(slot);
        self.pool.release(self.index);
    }
}

impl<Backend: Io> EngineState<Backend> {
    /// Wake a worker for a read in line, and end it if the store has gone
    pub fn wake_reader(&self, worker: usize) {
        if let Some(task) = self.read_tasks[worker].get() {
            task.wake();
        }
        if self.is_stopping.load(Ordering::Acquire) {
            self.read_pool.refuse_queued();
        }
    }

    /// Run a get on a worker
    pub async fn read_async(
        &self,
        column: ColumnId,
        key: &[u8],
        want: RecordWant,
        point: Option<WritePlace>,
        buf: &mut Vec<u8>,
    ) -> Result<bool, StoreError> {
        if let Some(ring) = &self.ring {
            return self.get_on_ring(ring, column, key, want, point, buf).await;
        }
        let lease = self.read_pool.claim().await;
        let value = mem::take(buf);
        lease.load(key, ReadWork::Get { column, want, point, value });
        self.wake_reader(lease.submit());
        let (work, answer) = lease.finished().await;
        if let ReadWork::Get { value, .. } = work {
            *buf = value;
        }
        answer
    }

    /// Run a batched get at one point on a worker, a slot of keys at a time
    pub async fn many_async(
        &self,
        column: ColumnId,
        keys: &[&[u8]],
        point: WritePlace,
        handovers: u64,
        values: &mut ValueList,
    ) -> Result<(), StoreError> {
        values.clear();
        let mut at = 0;
        while at < keys.len() {
            let lease = self.read_pool.claim().await;
            let work = ReadWork::Many { column, point, handovers, values: mem::take(values) };
            at += lease.load_keys(&keys[at..], work);
            self.wake_reader(lease.submit());
            let (work, answer) = lease.finished().await;
            if let ReadWork::Many { values: list, .. } = work {
                *values = list;
            }
            answer?;
        }
        Ok(())
    }

    /// Run one read, with its keys beside it
    pub fn run_read(&self, work: &mut ReadWork, keys: &[u8], ends: &[usize]) -> Result<bool, StoreError> {
        match work {
            ReadWork::Empty => Ok(false),
            ReadWork::Get { column, want, point, value } => {
                let key = &keys[..ends.first().copied().unwrap_or(0)];
                self.counted(self.get_one(*column, key, *want, *point, value))
            },
            ReadWork::Many { column, point, handovers, values } => {
                let mut scratch = values.take_scratch();
                let (mut start, mut answer) = (0, Ok(()));
                for end in ends {
                    answer = answer.and_then(|()| in_place(self.read_keys(&InPlace, *column, &[&keys[start..*end]], *point, *handovers, values, &mut scratch)));
                    start = *end;
                }
                values.put_scratch(scratch);
                self.corruption_errors.fetch_add(values.corrupt_count(), Ordering::Relaxed);
                answer.map(|()| true)
            },
            ReadWork::Block { scan, keys: list } => self.scan_block(scan, list),
        }
    }

    /// Read one key at the newest point or a cue's
    pub fn get_one(
        &self,
        column: ColumnId,
        key: &[u8],
        want: RecordWant,
        point: Option<WritePlace>,
        value: &mut Vec<u8>,
    ) -> Result<bool, StoreError> {
        in_place(self.get_one_on(&InPlace, column, key, want, point, value))
    }

    /// Read one key through a device's read path
    pub async fn get_one_on(
        &self,
        device: &impl DeviceReads<Backend::File>,
        column: ColumnId,
        key: &[u8],
        want: RecordWant,
        point: Option<WritePlace>,
        value: &mut Vec<u8>,
    ) -> Result<bool, StoreError> {
        let Some(point) = point else {
            return self.get_latest_on(device, column, key, want, value).await;
        };
        let info = self.column(column)?;
        let handovers = self.volumes[info.volume.0 as usize].handover_count();
        let read = self.key_read(column, key, point, true)?;
        self.get_at(device, &read, handovers, want, value).await
    }

    /// Run a get on the ring, on the caller's task, and hold no thread while the device works
    pub async fn get_on_ring(
        &self,
        ring: &ReadRing<Backend::Ring>,
        column: ColumnId,
        key: &[u8],
        want: RecordWant,
        point: Option<WritePlace>,
        value: &mut Vec<u8>,
    ) -> Result<bool, StoreError> {
        let Ok(_seat) = ring.seat().await else {
            return Err(StoreError::Closed);
        };
        let answer = self.get_one_on(&OnRing(ring), column, key, want, point, value).await;
        self.counted(answer)
    }

    /// Fill a list with the next block of a scan's keys, and say whether the walk has more
    pub fn scan_block(&self, scan: &mut ScanState, keys: &mut ValueList) -> Result<bool, StoreError> {
        let mut key = mem::take(&mut scan.out_key);
        let mut outcome = Ok(true);
        for _key in 0..SCAN_BLOCK_KEYS {
            match self.step_scan(scan, &mut key, ScanWant::Key) {
                Ok(ScanStep::Found | ScanStep::Corrupt) => keys.push_found(&key),
                Ok(ScanStep::End) => {
                    outcome = Ok(false);
                    break;
                },
                Err(error) => {
                    outcome = Err(error);
                    break;
                },
            }
        }
        scan.out_key = key;
        outcome
    }
}

impl<Backend: Io> Job for ReadWorker<Backend> {
    fn step(&mut self) -> JobStep {
        let pool = &self.state.read_pool;
        let Some((index, mut work, keys, ends)) = pool.next_queued() else {
            return JobStep::Idle;
        };
        let answer = self.state.run_read(&mut work, &keys, &ends);
        pool.finish(index, work, keys, ends, answer);
        JobStep::Worked
    }
}

