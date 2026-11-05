//! Fixed buffers shared by every thread: a caller takes one or waits

use std::ops::{Deref, DerefMut};
use std::sync::{Condvar, Mutex, MutexGuard};

use reel_api::units::ByteCount;

use crate::guard::hold;

/// Wakes a waiter when dropped, which runs after the item's lock has gone
#[derive(Debug)]
pub struct Freed<'pool, Item> {
    pool: &'pool Pool<Item>,
}

/// A pooled item, handed back when dropped
#[derive(Debug)]
pub struct Pooled<'pool, Item> {
    guard: MutexGuard<'pool, Item>,
    _freed: Freed<'pool, Item>,
}

/// A fixed set of items
#[derive(Debug)]
pub struct Pool<Item> {
    items: Vec<Mutex<Item>>,
    waiters: Mutex<u64>,
    freed: Condvar,
}

impl<Item> Pool<Item> {
    /// Build a pool from its items
    pub fn new(items: Vec<Item>) -> Self {
        let mut slots = Vec::with_capacity(items.len());
        for item in items {
            slots.push(Mutex::new(item));
        }
        Self {
            items: slots,
            waiters: Mutex::new(0),
            freed: Condvar::new(),
        }
    }

    /// Count the items
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Check whether the pool holds no items
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Add up what the items hold
    pub fn heap_bytes(&self, measure: fn(&Item) -> ByteCount) -> ByteCount {
        let mut total = 0;
        for slot in &self.items {
            total += measure(&hold(slot)).0;
        }
        ByteCount(total)
    }

    /// Take a free item, starting the search at a hint so threads spread out
    pub fn take(&self, hint: usize) -> Pooled<'_, Item> {
        loop {
            if let Some(item) = self.try_take(hint) {
                return item;
            }

            let mut waiters = hold(&self.waiters);
            *waiters += 1;
            if let Some(item) = self.try_take(hint) {
                *waiters -= 1;
                return item;
            }
            waiters = match self.freed.wait(waiters) {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            *waiters -= 1;
        }
    }

    /// Take a free item if one is free right now
    pub fn try_take(&self, hint: usize) -> Option<Pooled<'_, Item>> {
        let count = self.items.len();
        for step in 0..count {
            let slot = &self.items[(hint + step) % count];
            if let Ok(guard) = slot.try_lock() {
                return Some(Pooled {
                    guard,
                    _freed: Freed { pool: self },
                });
            }
        }
        None
    }
}

impl<Item> Deref for Pooled<'_, Item> {
    type Target = Item;

    fn deref(&self) -> &Item {
        &self.guard
    }
}

impl<Item> DerefMut for Pooled<'_, Item> {
    fn deref_mut(&mut self) -> &mut Item {
        &mut self.guard
    }
}

impl<Item> Drop for Freed<'_, Item> {
    fn drop(&mut self) {
        let waiters = hold(&self.pool.waiters);
        if *waiters > 0 {
            self.pool.freed.notify_one();
        }
    }
}
