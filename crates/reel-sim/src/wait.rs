//! A test's wait on another thread, which fails at a deadline so a hang shows as a failure

use std::thread;
use std::time::{Duration, Instant};

/// How long a test waits for another thread before it fails
pub const WAIT_DEADLINE: Duration = Duration::from_secs(60);

/// Yield until a condition holds, and panic with the message once the deadline passes
pub fn wait_until(mut condition: impl FnMut() -> bool, message: &str) {
    let deadline = Instant::now() + WAIT_DEADLINE;
    while !condition() {
        assert!(Instant::now() < deadline, "waited {WAIT_DEADLINE:?}: {message}");
        thread::yield_now();
    }
}
