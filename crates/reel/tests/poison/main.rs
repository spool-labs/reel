//! A lock reel finds poisoned fails every volume. This runs in its own binary, since the mark lasts the process

use std::sync::{Arc, Mutex};
use std::thread;

use reel::guard::{hold, is_poisoned};

// a holder that panics part way through leaves the state behind it unknown, so the next taker marks the store failed
#[test]
fn poisoned_lock_marks_failed() {
    let shared = Arc::new(Mutex::new(0u64));
    let held = Arc::clone(&shared);
    let panicked = thread::spawn(move || {
        let _guard = hold(&held);
        std::panic::resume_unwind(Box::new("a holder panicked"));
    })
    .join();
    assert!(panicked.is_err());
    assert!(!is_poisoned());
    *hold(&shared) += 1;
    assert!(is_poisoned());
}
