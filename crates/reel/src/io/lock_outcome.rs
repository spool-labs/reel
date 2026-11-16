//! The result of asking for a lock

use reel_core::units::ProcessId;

/// Either the lock, or the process that holds it
#[derive(Debug)]
pub enum LockOutcome<Lock> {
    /// This process now holds the lock
    Held(Lock),
    /// Another process holds the lock
    Refused { holder: ProcessId },
}
