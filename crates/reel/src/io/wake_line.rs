//! A line writers wait on for progress: anything that can clear a wait moves its count and wakes whoever sleeps, and a waiter
//! that missed the move wakes on its own after the pause it took before

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use crate::guard::hold;

/// The line
#[derive(Debug)]
pub struct WakeLine {
    count: AtomicU64,
    sleepers: AtomicU32,
    lock: Mutex<()>,
    moved: Condvar,
}

impl WakeLine {
    /// Build a line with its lock and its condition set up, since on some targets the standard library builds them at their
    /// first use, and a first wait under load would allocate where the store promises none
    pub fn new() -> Self {
        let line = Self { count: AtomicU64::new(0), sleepers: AtomicU32::new(0), lock: Mutex::new(()), moved: Condvar::new() };
        drop(hold(&line.lock));
        line.moved.notify_all();
        line
    }

    /// Read the count, before a waiter checks what it waits for
    pub fn seen(&self) -> u64 {
        self.count.load(Ordering::Acquire)
    }

    /// Move the count and wake every sleeper, which costs one atomic when none sleeps
    pub fn wake(&self) {
        self.count.fetch_add(1, Ordering::AcqRel);
        if self.sleepers.load(Ordering::Acquire) > 0 {
            let _held = hold(&self.lock);
            self.moved.notify_all();
        }
    }

    /// Sleep until the count moves past what was seen, or at most a while
    pub fn wait(&self, seen: u64, most: Duration) {
        self.sleepers.fetch_add(1, Ordering::AcqRel);
        let guard = hold(&self.lock);
        // a wake between the look and the lock moved the count, so the waiter goes on at once
        if self.count.load(Ordering::Acquire) == seen {
            let _woke = self.moved.wait_timeout(guard, most);
        }
        self.sleepers.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Default for WakeLine {
    fn default() -> Self {
        Self::new()
    }
}
