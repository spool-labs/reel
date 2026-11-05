//! Fixed buffers shared by every thread: a caller takes one or waits

use std::sync::{Condvar, Mutex, MutexGuard};

use reel_api::units::ByteCount;

use crate::guard::hold;

/// A pooled item, handed back when dropped
#[derive(Debug)]
pub struct Pooled<'pool, Item> {
    guard: Option<MutexGuard<'pool, Item>>,
    pool: &'pool Pool<Item>,
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
            if let Some(guard) = self.try_take(hint) {
                return Pooled {
                    guard: Some(guard),
                    pool: self,
                };
            }

            let mut waiters = hold(&self.waiters);
            *waiters += 1;
            if let Some(guard) = self.try_take(hint) {
                *waiters -= 1;
                return Pooled {
                    guard: Some(guard),
                    pool: self,
                };
            }
            waiters = match self.freed.wait(waiters) {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            *waiters -= 1;
        }
    }

    /// Take a free item if one is free right now
    pub fn try_take(&self, hint: usize) -> Option<MutexGuard<'_, Item>> {
        let count = self.items.len();
        for step in 0..count {
            let slot = &self.items[(hint + step) % count];
            if let Ok(guard) = slot.try_lock() {
                return Some(guard);
            }
        }
        None
    }
}

impl<Item> std::ops::Deref for Pooled<'_, Item> {
    type Target = Item;

    fn deref(&self) -> &Item {
        match &self.guard {
            Some(guard) => guard,
            None => unreachable!("a pooled item holds its guard until drop"),
        }
    }
}

impl<Item> std::ops::DerefMut for Pooled<'_, Item> {
    fn deref_mut(&mut self) -> &mut Item {
        match &mut self.guard {
            Some(guard) => guard,
            None => unreachable!("a pooled item holds its guard until drop"),
        }
    }
}

impl<Item> Drop for Pooled<'_, Item> {
    fn drop(&mut self) {
        drop(self.guard.take());
        let waiters = hold(&self.pool.waiters);
        if *waiters > 0 {
            self.pool.freed.notify_one();
        }
    }
}
