//! Fixed buffers shared by every thread: a caller takes one or waits

use std::hint::black_box;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Condvar, Mutex};

use reel_api::units::ByteCount;

use crate::guard::hold;

/// The byte a pool is first written with, which is not zero so the writes happen
pub const TOUCH_BYTE: u8 = 0xA5;

/// A fixed set of items, each owned by its user while in use so no lock spans a read
#[derive(Debug)]
pub struct Pool<Item> {
    free: Mutex<Vec<Item>>,
    freed: Condvar,
}

impl<Item> Pool<Item> {
    /// Build a pool from its items
    pub fn new(items: Vec<Item>) -> Self {
        Self {
            free: Mutex::new(items),
            freed: Condvar::new(),
        }
    }

    /// Add up what the free items hold
    pub fn heap_bytes(&self, measure: fn(&Item) -> ByteCount) -> ByteCount {
        let mut total = 0;
        for item in hold(&self.free).iter() {
            total += measure(item).0;
        }
        ByteCount(total)
    }

    /// Run some work with an item, waiting for one when all are out
    pub fn with<Answer>(&self, work: impl FnOnce(&mut Item) -> Answer) -> Answer {
        let mut item = self.take();

        // a panic in the work must still hand the item back, or the pool would shrink for good
        let outcome = panic::catch_unwind(AssertUnwindSafe(|| work(&mut item)));
        hold(&self.free).push(item);
        self.freed.notify_one();

        match outcome {
            Ok(answer) => answer,
            Err(payload) => panic::resume_unwind(payload),
        }
    }

    /// Take a free item, waiting for one when all are out
    pub fn take(&self) -> Item {
        let mut free = hold(&self.free);
        loop {
            if let Some(item) = free.pop() {
                return item;
            }
            free = match self.freed.wait(free) {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
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
