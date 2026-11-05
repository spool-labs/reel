//! A flag a writer thread sets however it ends, so a reader that loops on it can never outlive a dead writer

use std::sync::atomic::{AtomicBool, Ordering};

/// Sets its flag when dropped, which a panic does too
pub struct SetsOnDrop<'flag>(pub &'flag AtomicBool);

impl Drop for SetsOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
