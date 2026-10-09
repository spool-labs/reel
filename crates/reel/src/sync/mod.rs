//! Poison-recovering wrappers over the standard locks

#[cfg(feature = "rendezvous")]
pub mod rendezvous;
pub mod tension;

/// No-op rendezvous calls for a build without the feature, so the sites stay put
#[cfg(not(feature = "rendezvous"))]
pub mod rendezvous {
    /// Whether a script has refused the point, always false without the feature
    #[inline(always)]
    pub fn refused(_name: &'static str) -> bool {
        false
    }

    /// Mark a rendezvous point, which nothing watches
    #[inline(always)]
    pub fn at(_name: &'static str) {}
}

use std::sync::{
    Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError,
};
use std::time::Duration;

/// Lock a mutex, recovering the guard when a panicking holder poisoned it
pub fn lock<Guarded>(mutex: &Mutex<Guarded>) -> MutexGuard<'_, Guarded> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Take a mutex only if it is free, for work another thread is already doing
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
