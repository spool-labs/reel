//! Poison-recovering wrappers over the standard locks
//!
//! Every lock in the crate recovers a poisoned guard instead of panicking, so a
//! writer that panicked while holding a lock cannot wedge every later caller.

#[cfg(feature = "rendezvous")]
pub mod rendezvous;
pub mod tension;

/// The rendezvous surface a build without the feature keeps, so the sites stay put
///
/// Every marked moment lives on a production path and every one of them is a call
/// to nothing here. There is no stage to arm, so `Script` has no counterpart: only
/// a test ever held one.
#[cfg(not(feature = "rendezvous"))]
pub mod rendezvous {
    /// Whether a script has refused the point, which without one it never has
    #[inline(always)]
    pub fn refused(_name: &'static str) -> bool {
        false
    }

    /// Mark a named moment, which nothing is watching for
    #[inline(always)]
    pub fn at(_name: &'static str) {}
}

use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError};

/// Lock a mutex, recovering the guard when a panicking holder poisoned it
pub fn lock<Guarded>(mutex: &Mutex<Guarded>) -> MutexGuard<'_, Guarded> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Take a mutex only if it is free, for work another thread is already doing
///
/// Nothing comes back when someone else holds it, which is an answer rather than a
/// failure: the caller has something better to do than queue for work under way.
pub fn try_lock<Guarded>(mutex: &Mutex<Guarded>) -> Option<MutexGuard<'_, Guarded>> {
    match mutex.try_lock() {
        Ok(guard) => Some(guard),
        Err(TryLockError::Poisoned(poisoned)) => Some(poisoned.into_inner()),
        Err(TryLockError::WouldBlock) => None,
    }
}

/// Take a shared read guard, recovering it when a panicking writer poisoned it
pub fn read<Guarded>(shared: &RwLock<Guarded>) -> RwLockReadGuard<'_, Guarded> {
    shared
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Take an exclusive write guard, recovering it when a panicking writer poisoned it
pub fn write<Guarded>(shared: &RwLock<Guarded>) -> RwLockWriteGuard<'_, Guarded> {
    shared
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Primitives that swap for the model checker's under --cfg loom
///
/// A module the checker has tests for takes its locks and atomics from here rather
/// than from std, so what the checker drives is what ships. Everything else keeps
/// std directly, since the io backends cannot run inside a model.
pub mod checked {
    use std::time::Duration;

    #[cfg(loom)]
    pub use loom::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
    #[cfg(loom)]
    pub use loom::sync::{Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

    #[cfg(not(loom))]
    pub use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
    #[cfg(not(loom))]
    pub use std::sync::{Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

    /// Lock a mutex, recovering the guard when a panicking holder poisoned it
    pub fn lock<Guarded>(mutex: &Mutex<Guarded>) -> MutexGuard<'_, Guarded> {
        mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Take a shared read guard, recovering it when a panicking writer poisoned it
    pub fn read<Guarded>(shared: &RwLock<Guarded>) -> RwLockReadGuard<'_, Guarded> {
        shared
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Take an exclusive write guard, recovering it when a panicking writer poisoned it
    pub fn write<Guarded>(shared: &RwLock<Guarded>) -> RwLockWriteGuard<'_, Guarded> {
        shared
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Wait on a condition variable, recovering the guard from a poisoned lock
    pub fn wait<'guard, Guarded>(
        condition: &Condvar,
        guard: MutexGuard<'guard, Guarded>,
    ) -> MutexGuard<'guard, Guarded> {
        condition
            .wait(guard)
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Wait on a condition variable for a bounded time, recovering a poisoned guard
    ///
    /// The checker has no clock and takes this as an open wait, so a park it
    /// explores ends on the notification or not at all.
    pub fn wait_for<'guard, Guarded>(
        condition: &Condvar,
        guard: MutexGuard<'guard, Guarded>,
        limit: Duration,
    ) -> MutexGuard<'guard, Guarded> {
        let (guard, _) = condition
            .wait_timeout(guard, limit)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard
    }
}
