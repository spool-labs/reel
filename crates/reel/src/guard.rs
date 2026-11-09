//! Locking that survives a poisoned mutex, and marks the store failed when it does

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Set once a lock reel holds was poisoned. The holder panicked part way through a change, so the state behind any lock may be
/// half written, and every volume in the process refuses writes from then on
static IS_POISONED: AtomicBool = AtomicBool::new(false);

/// Take a mutex, and keep going when an earlier holder panicked
pub fn hold<Value>(mutex: &Mutex<Value>) -> MutexGuard<'_, Value> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => recover(poisoned),
    }
}

/// Take the guard a panicked holder left, and fail every volume so nothing half written is built on or saved
pub fn recover<Guard>(poisoned: PoisonError<Guard>) -> Guard {
    IS_POISONED.store(true, Ordering::Release);
    poisoned.into_inner()
}

/// Check a lock was found poisoned
pub fn is_poisoned() -> bool {
    IS_POISONED.load(Ordering::Acquire)
}

/// A table's shards, each behind its own lock, which survive a poisoned lock the way `hold` does
#[derive(Debug)]
pub struct Shards<Shard>(pub Vec<RwLock<Shard>>);

impl<Shard> Shards<Shard> {
    /// Read a shard
    pub fn read(&self, index: usize) -> RwLockReadGuard<'_, Shard> {
        match self.0[index].read() {
            Ok(guard) => guard,
            Err(poisoned) => recover(poisoned),
        }
    }

    /// Change a shard
    pub fn write(&self, index: usize) -> RwLockWriteGuard<'_, Shard> {
        match self.0[index].write() {
            Ok(guard) => guard,
            Err(poisoned) => recover(poisoned),
        }
    }

    /// Count the shards
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check there are no shards
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
