//! Fixed buffers shared by every thread: a caller takes one or waits

use std::cell::Cell;
use std::collections::VecDeque;
use std::hint::black_box;
use std::mem::{self, MaybeUninit};
use std::ops::{Deref, DerefMut};
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering, fence};
use std::sync::{Condvar, Mutex, MutexGuard};

use reel_core::units::ByteCount;

use crate::guard::hold;

/// The byte a pool is first written with, which is not zero so the writes happen
pub const TOUCH_BYTE: u8 = 0xA5;

/// Wake lines a pool keeps: a waiter sleeps on its ticket's line and a wake reaches the line of the ticket being served, so a
/// returned item wakes the caller it is for and, up to this many waiters, no other
pub const WAKE_LINES: usize = 64;

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
/// readers don't share a lock or a cache line. Once every item is out callers wait in line, and a running caller takes a free item
/// ahead of them unless a caller who needs several is waiting. A caller who finds an item free never sleeps or wakes anyone
#[derive(Debug)]
pub struct Pool<Item> {
    slots: Vec<PoolSlot<Item>>,
    line: Mutex<PoolLine>,
    queued: AtomicU64,
    freed: Vec<Condvar>,
    free: AtomicUsize,
    many_waiting: AtomicU64,
    /// Heads of the line that woke and found every item taken ahead of them, which shut the fast path until they are served
    passed_heads: AtomicU64,
    passes: AtomicU64,
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
        let count = items.len();
        let pool = Self {
            slots: items.into_iter().map(|item| PoolSlot { item: Mutex::new(Some(item)) }).collect(),
            line: Mutex::new(PoolLine { next_ticket: 0, serving: 0 }),
            queued: AtomicU64::new(0),
            freed: (0..WAKE_LINES).map(|_line| Condvar::new()).collect(),
            free: AtomicUsize::new(count),
            many_waiting: AtomicU64::new(0),
            passed_heads: AtomicU64::new(0),
            passes: AtomicU64::new(0),
            wakes: AtomicU64::new(0),
            waits: AtomicU64::new(0),
        };
        // some platforms set a lock up at its first use, and that's a job for open
        for slot in &pool.slots {
            drop(hold(&slot.item));
        }
        drop(hold(&pool.line));
        for line in &pool.freed {
            line.notify_all();
        }
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

    /// Count the times a head of the line woke and found every item taken ahead of it
    pub fn passes(&self) -> u64 {
        self.passes.load(Ordering::Relaxed)
    }

    /// Wake the caller the line is serving. Passing through the line's lock means a caller past its last look is asleep before
    /// the wake comes
    pub fn wake(&self) {
        let serving = hold(&self.line).serving;
        self.wakes.fetch_add(1, Ordering::Relaxed);
        self.freed[serving as usize % WAKE_LINES].notify_all();
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
                self.free.fetch_sub(1, Ordering::AcqRel);
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
                    self.free.fetch_add(1, Ordering::AcqRel);
                    return;
                }
            }
            // a slot frees once another put or take lets go of its lock, which takes nanoseconds
            std::hint::spin_loop();
        }
    }

    /// Count the items free right now
    pub fn free_count(&self) -> usize {
        self.free.load(Ordering::Acquire)
    }

    /// Take a free item, waiting in line when all are out. A running caller takes a free item ahead of the sleepers, so a line
    /// that stands costs no sleep a take, and it waits its turn only while a caller who needs several is in line, who would
    /// otherwise never see enough free at once. A head that wakes to find its item taken ahead shuts that way until it is
    /// served, so a sleeper is passed over once and never for as long as others keep taking
    pub fn take(&self) -> Item {
        if self.many_waiting.load(Ordering::Acquire) == 0
            && self.passed_heads.load(Ordering::Acquire) == 0
            && let Some(item) = self.grab()
        {
            return item;
        }
        let mut was_passed = false;
        let mut line = self.wait_for(1, &mut was_passed);
        let ticket = line.serving;
        // the head keeps its place until it has an item, so callers who take ahead never push it back
        let item = loop {
            if let Some(item) = self.grab() {
                break item;
            }
            self.mark_passed(&mut was_passed);
            line = self.sleep(line, ticket);
        };
        self.clear_passed(was_passed);
        self.leave_line(line);
        item
    }

    /// Take this many items at once, so nobody holds a part of what they need while they wait for the rest
    pub fn take_many(&self, count: usize, items: &mut Vec<Item>) {
        if count == 0 {
            return;
        }
        self.many_waiting.fetch_add(1, Ordering::AcqRel);
        let mut was_passed = false;
        let mut line = self.wait_for(count, &mut was_passed);
        let ticket = line.serving;
        let wanted = items.len() + count;
        while items.len() < wanted {
            match self.grab() {
                Some(item) => items.push(item),
                None => line = self.sleep(line, ticket),
            }
        }
        self.clear_passed(was_passed);
        self.leave_line(line);
        self.many_waiting.fetch_sub(1, Ordering::AcqRel);
    }

    /// Wait until these items are free and nobody who came first is still waiting. A head that wakes to find too few free
    /// was passed over by callers who took ahead, and it marks that once, so they queue behind it until it is served
    pub fn wait_for(&self, count: usize, was_passed: &mut bool) -> MutexGuard<'_, PoolLine> {
        let mut line = hold(&self.line);
        let ticket = line.next_ticket;
        line.next_ticket += 1;
        self.queued.fetch_add(1, Ordering::AcqRel);
        self.waits.fetch_add(1, Ordering::Relaxed);
        // pairs with the fence in `wake_if_waited_for`: this look or that one sees the other's write
        fence(Ordering::SeqCst);
        while line.serving != ticket || self.free_count() < count {
            line = self.sleep(line, ticket);
            if line.serving == ticket && self.free_count() < count {
                self.mark_passed(was_passed);
            }
        }
        line
    }

    /// Shut the way past the line once for a head that found its items taken ahead of it
    pub fn mark_passed(&self, was_passed: &mut bool) {
        if !*was_passed {
            *was_passed = true;
            self.passes.fetch_add(1, Ordering::Relaxed);
            self.passed_heads.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Open the way past the line again once a passed head has what it waited for
    pub fn clear_passed(&self, was_passed: bool) {
        if was_passed {
            self.passed_heads.fetch_sub(1, Ordering::AcqRel);
        }
    }


    /// Sleep on the ticket's wake line until the next wake
    pub fn sleep<'pool>(&'pool self, line: MutexGuard<'pool, PoolLine>, ticket: u64) -> MutexGuard<'pool, PoolLine> {
        match self.freed[ticket as usize % WAKE_LINES].wait(line) {
            Ok(guard) => guard,
            Err(poisoned) => crate::guard::recover(poisoned),
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
    touched_capacity(capacity, TOUCH_BYTE)
}

/// Allocate zeroed bytes with every page written
pub fn touched_zeroes(length: usize) -> Vec<u8> {
    let mut bytes = touched_filled(length, TOUCH_BYTE);
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

/// Build a buffer of items no clone can make, each from a closure, with every page written so open faults them in
pub fn touched_from<Item>(length: usize, mut make: impl FnMut() -> Item) -> Vec<Item> {
    let mut items: Vec<Item> = Vec::with_capacity(length);
    ask_huge_pages(items.as_mut_ptr(), length);
    // the room has to count as seen, or a compiler can turn the writes into a zeroed allocation
    black_box(items.as_mut_ptr());
    for _item in 0..length {
        items.push(make());
    }
    black_box(items.as_mut_slice());
    items
}

/// Allocate an empty list with room for its items and every page of that room written
pub fn touched_capacity<Item: Clone>(capacity: usize, filler: Item) -> Vec<Item> {
    let mut items: Vec<Item> = Vec::with_capacity(capacity);
    ask_huge_pages(items.as_mut_ptr(), capacity);
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
    ask_huge_pages(items.as_mut_ptr(), capacity);
    // the pointer counts as seen before the writes, or a compiler can turn them into a zeroed allocation
    black_box(items.as_mut_ptr());
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

/// Allocate zeroed 64-bit words with every page written. Zeros collected straight into a list can come back as a zeroed
/// allocation the kernel hasn't touched, and its first write faults on a job's thread
pub fn touched_atomics(length: usize) -> Vec<AtomicU64> {
    let mut words: Vec<AtomicU64> = Vec::with_capacity(length);
    black_box(words.as_mut_ptr());
    words.resize_with(length, || AtomicU64::new(u64::MAX));
    for word in &words {
        word.store(0, Ordering::Relaxed);
    }
    black_box(words.as_ptr());
    words
}

/// Bytes of one huge page, and the least room worth asking huge pages for
pub const HUGE_PAGE_BYTES: usize = 2 * 1024 * 1024;

/// Ask the kernel to back a buffer's room with huge pages before anything touches it, so a cache's random reads miss the TLB
/// less. Only whole huge pages inside the room can be huge, and on a system set to never the ask does nothing. A hint: it
/// never fails a buffer
pub fn ask_huge_pages<Item>(room: *mut Item, items: usize) {
    let bytes = items.saturating_mul(size_of::<Item>());
    if bytes < 2 * HUGE_PAGE_BYTES {
        return;
    }
    #[cfg(target_os = "linux")]
    {
        let start = (room as usize).next_multiple_of(HUGE_PAGE_BYTES);
        let end = (room as usize + bytes) / HUGE_PAGE_BYTES * HUGE_PAGE_BYTES;
        if end > start {
            // SAFETY: the range lies inside the room the caller owns, and the advice changes how pages back it, never its bytes
            let _answer = unsafe { libc::madvise(start as *mut libc::c_void, end - start, libc::MADV_HUGEPAGE) };
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _hint = room;
}

/// Read the anonymous memory of this process the kernel backs with huge pages, or nothing where the system doesn't say
pub fn huge_page_bytes() -> Option<ByteCount> {
    let rollup = std::fs::read_to_string("/proc/self/smaps_rollup").ok()?;
    let line = rollup.lines().find(|line| line.starts_with("AnonHugePages:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(ByteCount(kib * 1024))
}
