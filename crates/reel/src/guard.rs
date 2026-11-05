//! Locking that survives a poisoned mutex

use std::sync::{Mutex, MutexGuard};

/// Take a mutex, and keep going when an earlier holder panicked
pub fn hold<Value>(mutex: &Mutex<Value>) -> MutexGuard<'_, Value> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
