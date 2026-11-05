//! Keeps the simulator's own allocations out of a test's count: engine code counts, the simulated disk doesn't

use std::cell::Cell;

thread_local! {
    static PAUSES: Cell<u32> = const { Cell::new(0) };
}

/// Holds the count off while the simulator works
#[derive(Debug)]
pub struct CountPause;

impl CountPause {
    /// Hold the count off until this drops
    pub fn new() -> Self {
        PAUSES.with(|pauses| pauses.set(pauses.get() + 1));
        Self
    }
}

impl Default for CountPause {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for CountPause {
    fn drop(&mut self) {
        PAUSES.with(|pauses| pauses.set(pauses.get() - 1));
    }
}

/// Check whether the simulator is working on this thread
pub fn is_paused() -> bool {
    PAUSES.with(|pauses| pauses.get() > 0)
}
