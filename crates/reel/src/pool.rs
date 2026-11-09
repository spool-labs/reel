//! Fixed buffers shared by every thread: a caller takes one or waits

use std::cell::Cell;
use std::collections::VecDeque;
use std::hint::black_box;
use std::mem::{self, MaybeUninit};
use std::ops::{Deref, DerefMut};
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering, fence};
use std::sync::{Condvar, Mutex, MutexGuard};

use reel_api::units::ByteCount;

use crate::guard::hold;

/// The byte a pool is first written with, which is not zero so the writes happen
pub const TOUCH_BYTE: u8 = 0xA5;

/// Threads take their first look at slots in the order they first came to any pool
static NEXT_THREAD: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static THREAD_NUMBER: Cell<Option<usize>> = const { Cell::new(None) };
}

/// Read this thread's number, which spreads threads over a pool's slots
pub fn thread_number() -> usize {
    THREAD_NUMBER.with(|cell| {
        cell.get().unwrap_or_else(|| {
            let number = NEXT_THREAD.fetch_add(1, Ordering::Relaxed);
            cell.set(Some(number));
            number
        })
    })
}

/// A fixed set of items, each owned by its user while in use so no lock spans a read. A thread looks first at its own slot, so
/// readers don't share a lock or a cache line. Once every item is out callers wait in line, served in the order they came, and
/// a caller who finds an item free and nobody ahead never sleeps or wakes anyone
#[derive(Debug)]
pub struct Pool<Item> {
    slots: Vec<PoolSlot<Item>>,
    line: Mutex<PoolLine>,
    queued: AtomicU64,
    freed: Condvar,
    wakes: AtomicU64,
    waits: AtomicU64,
}

/// One item's place, on cache lines of its own
#[derive(Debug)]
#[repr(align(128))]
pub struct PoolSlot<Item> {
    item: Mutex<Option<Item>>,
}

/// The queue of callers waiting for items
#[derive(Debug)]
pub struct PoolLine {
    next_ticket: u64,
    serving: u64,
}

/// An item with no buffers, which a lease leaves in its place as it hands the real one back
pub trait HollowItem {
    /// Build an item that holds nothing
    fn hollow() -> Self;
}

/// An item out of a pool, which goes back however its holder ends, a dropped read included
#[derive(Debug)]
pub struct PoolLease<'pool, Item: HollowItem> {
    pool: &'pool Pool<Item>,
    item: Item,
}

impl<Item: HollowItem> Deref for PoolLease<'_, Item> {
    type Target = Item;

    fn deref(&self) -> &Item {
        &self.item
    }
}

impl<Item: HollowItem> DerefMut for PoolLease<'_, Item> {
    fn deref_mut(&mut self) -> &mut Item {
        &mut self.item
    }
}

impl<Item: HollowItem> Drop for PoolLease<'_, Item> {
    fn drop(&mut self) {
        self.pool.give_back(mem::replace(&mut self.item, Item::hollow()));
    }
}

impl<Item: HollowItem> Pool<Item> {
    /// Take a free item for as long as the lease lives, waiting in line when all are out
    pub fn lease(&self) -> PoolLease<'_, Item> {
        PoolLease { pool: self, item: self.take() }
    }
}

impl<Item> Pool<Item> {
    /// Build a pool from its items
    pub fn new(items: Vec<Item>) -> Self {
        let pool = Self {
            slots: items.into_iter().map(|item| PoolSlot { item: Mutex::new(Some(item)) }).collect(),
            line: Mutex::new(PoolLine { next_ticket: 0, serving: 0 }),
            queued: AtomicU64::new(0),
            freed: Condvar::new(),
            wakes: AtomicU64::new(0),
            waits: AtomicU64::new(0),
        };
        // some platforms set a lock up at its first use, and that's a job for open
        for slot in &pool.slots {
            drop(hold(&slot.item));
        }
        drop(hold(&pool.line));
        pool
    }

    /// Add up what the free items hold
    pub fn heap_bytes(&self, measure: fn(&Item) -> ByteCount) -> ByteCount {
        let mut total = 0;
        for slot in &self.slots {
            if let Some(item) = hold(&slot.item).as_ref() {
                total += measure(item).0;
            }
        }
        ByteCount(total)
    }

    /// Count the takes that found every item out or a line ahead, and queued
    pub fn waits(&self) -> u64 {
        self.waits.load(Ordering::Relaxed)
    }

    /// Count the wake-ups so far, each a system call
    pub fn wakes(&self) -> u64 {
        self.wakes.load(Ordering::Relaxed)
    }

    /// Wake the queue. Passing through the line's lock means a caller past its last look is asleep before the wake comes
    pub fn wake(&self) {
        drop(hold(&self.line));
        self.wakes.fetch_add(1, Ordering::Relaxed);
        self.freed.notify_all();
    }

    /// Count the callers in the queue
    pub fn waiting(&self) -> u64 {
        self.queued.load(Ordering::Acquire)
    }

    /// Hand every item in a list back
    pub fn give_back_all(&self, items: &mut Vec<Item>) {
        if items.is_empty() {
            return;
        }
        for item in items.drain(..) {
            self.put(item);
        }
        self.wake_if_waited_for();
    }

    /// Run some work with an item, waiting for one when all are out
    pub fn with<Answer>(&self, work: impl FnOnce(&mut Item) -> Answer) -> Answer {
        let mut item = self.take();

        // a panic in the work must still hand the item back, or the pool would shrink for good and, once every item went, every
        // caller would wait forever. Pooled items are scratch each use writes before it reads, so a half-changed one is safe
        let outcome = panic::catch_unwind(AssertUnwindSafe(|| work(&mut item)));
        self.give_back(item);

        match outcome {
            Ok(answer) => answer,
            Err(payload) => panic::resume_unwind(payload),
        }
    }

    /// Take an item from the first free slot, starting at this thread's own
    pub fn grab(&self) -> Option<Item> {
        let start = thread_number();
        for offset in 0..self.slots.len() {
            let slot = &self.slots[(start + offset) % self.slots.len()];
            if let Ok(mut held) = slot.item.try_lock()
                && let Some(item) = held.take()
            {
                return Some(item);
            }
        }
        None
    }

    /// Put an item in the first empty slot, which exists since every item has one
    pub fn put(&self, item: Item) {
        let start = thread_number();
        let mut item = Some(item);
        loop {
            for offset in 0..self.slots.len() {
                let slot = &self.slots[(start + offset) % self.slots.len()];
                if let Ok(mut held) = slot.item.try_lock()
                    && held.is_none()
                {
                    *held = item.take();
                    return;
                }
            }
            // a slot frees once another put or take lets go of its lock, which takes nanoseconds
            std::hint::spin_loop();
        }
    }

    /// Count the items free right now
    pub fn free_count(&self) -> usize {
        self.slots.iter().filter(|slot| hold(&slot.item).is_some()).count()
    }

    /// Take a free item, waiting in line when all are out or others came first
    pub fn take(&self) -> Item {
        if self.queued.load(Ordering::Acquire) == 0
            && let Some(item) = self.grab()
        {
            return item;
        }
        let line = self.wait_for(1);
        let item = self.grab();
        self.leave_line(line);
        match item {
            Some(item) => item,
            // a caller past its look at the line can take the item first, and this one waits again
            None => self.take(),
        }
    }

    /// Take this many items at once, so nobody holds a part of what they need while they wait for the rest
    pub fn take_many(&self, count: usize, items: &mut Vec<Item>) {
        if count == 0 {
            return;
        }
        let mut line = self.wait_for(count);
        let wanted = items.len() + count;
        while items.len() < wanted {
            match self.grab() {
                Some(item) => items.push(item),
                None => line = self.sleep(line),
            }
        }
        self.leave_line(line);
    }

    /// Wait until these items are free and nobody who came first is still waiting
    pub fn wait_for(&self, count: usize) -> MutexGuard<'_, PoolLine> {
        let mut line = hold(&self.line);
        let ticket = line.next_ticket;
        line.next_ticket += 1;
        self.queued.fetch_add(1, Ordering::AcqRel);
        self.waits.fetch_add(1, Ordering::Relaxed);
        // pairs with the fence in `wake_if_waited_for`: this look or that one sees the other's write
        fence(Ordering::SeqCst);
        while line.serving != ticket || self.free_count() < count {
            line = self.sleep(line);
        }
        line
    }

    /// Sleep until the next wake
    pub fn sleep<'pool>(&'pool self, line: MutexGuard<'pool, PoolLine>) -> MutexGuard<'pool, PoolLine> {
        match self.freed.wait(line) {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Pass the line on, waking it only when someone is in it and an item is left for them
    pub fn leave_line(&self, mut line: MutexGuard<'_, PoolLine>) {
        line.serving += 1;
        let left = self.queued.fetch_sub(1, Ordering::AcqRel) - 1;
        drop(line);
        if left > 0 && self.free_count() > 0 {
            self.wake();
        }
    }

    /// Hand an item back, waking the queue only when there is one
    pub fn give_back(&self, item: Item) {
        self.put(item);
        self.wake_if_waited_for();
    }

    /// Wake the queue when someone is in it
    pub fn wake_if_waited_for(&self) {
        fence(Ordering::SeqCst);
        if self.queued.load(Ordering::Acquire) > 0 {
            self.wake();
        }
    }
}

/// Allocate a buffer with every page written, since zeroed memory from the allocator is mapped on first use
pub fn touched_buffer(capacity: usize) -> Vec<u8> {
    let mut buffer = vec![TOUCH_BYTE; capacity];
    black_box(buffer.as_mut_slice());
    buffer.clear();
    buffer
}

/// Allocate zeroed bytes with every page written
pub fn touched_zeroes(length: usize) -> Vec<u8> {
    let mut bytes = vec![TOUCH_BYTE; length];
    black_box(bytes.as_mut_slice());
    bytes.fill(0);
    bytes
}

/// Allocate an empty queue with room for its items and every page of that room written
pub fn touched_queue<Item: Clone>(capacity: usize, filler: Item) -> VecDeque<Item> {
    let mut items = VecDeque::with_capacity(capacity);
    items.resize(capacity, filler);
    black_box(&mut items);
    items.clear();
    items
}

/// Allocate an empty list with room for its items and every page of that room written
pub fn touched_capacity<Item: Clone>(capacity: usize, filler: Item) -> Vec<Item> {
    let mut items: Vec<Item> = Vec::with_capacity(capacity);
    // the room has to count as seen, or a compiler can turn the writes into a zeroed allocation
    black_box(items.as_mut_ptr());
    items.resize(capacity, filler);
    black_box(items.as_mut_slice());
    items.clear();
    items
}

/// Allocate an empty list of items that have no plain value yet, with every page of its room written
pub fn touched_room<Item>(capacity: usize) -> Vec<Item> {
    let mut items: Vec<Item> = Vec::with_capacity(capacity);
    for slot in items.spare_capacity_mut() {
        *slot = MaybeUninit::zeroed();
    }
    black_box(items.as_mut_ptr());
    items
}

/// Allocate a list of one value with every page written
pub fn touched_filled<Item: Clone>(length: usize, value: Item) -> Vec<Item> {
    let mut items = touched_capacity(length, value.clone());
    items.resize(length, value);
    items
}
