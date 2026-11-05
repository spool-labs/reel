//! Fixed buffers shared by every thread: a caller takes one or waits

use std::hint::black_box;
use std::mem::MaybeUninit;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Condvar, Mutex};

use reel_api::units::ByteCount;

use crate::guard::hold;

/// The byte a pool is first written with, which is not zero so the writes happen
pub const TOUCH_BYTE: u8 = 0xA5;

/// A fixed set of items, each owned by its user while in use so no lock spans a read. Callers are served in the
/// order they came, and a caller who finds items free and nobody ahead never sleeps or wakes anyone
#[derive(Debug)]
pub struct Pool<Item> {
    state: Mutex<PoolState<Item>>,
    freed: Condvar,
}

/// The free items, and the queue of callers waiting for them
#[derive(Debug)]
pub struct PoolState<Item> {
    free: Vec<Item>,
    next_ticket: u64,
    serving: u64,
}

impl<Item> PoolState<Item> {
    /// Check nobody is in the queue
    pub fn has_no_queue(&self) -> bool {
        self.next_ticket == self.serving
    }
}

impl<Item> Pool<Item> {
    /// Build a pool from its items
    pub fn new(items: Vec<Item>) -> Self {
        Self {
            state: Mutex::new(PoolState {
                free: items,
                next_ticket: 0,
                serving: 0,
            }),
            freed: Condvar::new(),
        }
    }

    /// Add up what the free items hold
    pub fn heap_bytes(&self, measure: fn(&Item) -> ByteCount) -> ByteCount {
        let mut total = 0;
        for item in hold(&self.state).free.iter() {
            total += measure(item).0;
        }
        ByteCount(total)
    }

    /// Count the callers in the queue
    pub fn waiting(&self) -> u64 {
        let state = hold(&self.state);
        state.next_ticket - state.serving
    }

    /// Run some work with an item, waiting for one when all are out
    pub fn with<Answer>(&self, work: impl FnOnce(&mut Item) -> Answer) -> Answer {
        let mut item = self.take();

        // a panic in the work must still hand the item back, or the pool would shrink for good
        let outcome = panic::catch_unwind(AssertUnwindSafe(|| work(&mut item)));
        self.give_back(item);

        match outcome {
            Ok(answer) => answer,
            Err(payload) => panic::resume_unwind(payload),
        }
    }

    /// Take a free item, waiting in line when all are out or others came first
    pub fn take(&self) -> Item {
        let mut state = hold(&self.state);
        if state.has_no_queue()
            && let Some(item) = state.free.pop()
        {
            return item;
        }

        let ticket = state.next_ticket;
        state.next_ticket += 1;
        loop {
            if state.serving == ticket
                && let Some(item) = state.free.pop()
            {
                state.serving += 1;
                let should_wake = !state.has_no_queue() && !state.free.is_empty();
                drop(state);
                if should_wake {
                    self.freed.notify_all();
                }
                return item;
            }
            state = match self.freed.wait(state) {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
    }

    /// Hand an item back, waking the queue only when there is one
    pub fn give_back(&self, item: Item) {
        let mut state = hold(&self.state);
        state.free.push(item);
        let should_wake = !state.has_no_queue();
        drop(state);
        if should_wake {
            self.freed.notify_all();
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
