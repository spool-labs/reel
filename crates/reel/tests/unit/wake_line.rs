//! The line writers wait on

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use reel::io::wake_line::WakeLine;

const LONG_WAIT: Duration = Duration::from_secs(5);
const WAKE_AFTER: Duration = Duration::from_millis(20);

// a sleeper wakes when the line moves, long before its own pause would end
#[test]
fn wake_ends_a_wait() {
    let line = Arc::new(WakeLine::default());
    let seen = line.seen();
    let waiter = {
        let line = Arc::clone(&line);
        thread::spawn(move || {
            let started = Instant::now();
            line.wait(seen, LONG_WAIT);
            started.elapsed()
        })
    };
    thread::sleep(WAKE_AFTER);
    line.wake();
    let waited = waiter.join().expect("waiter");
    assert!(waited < LONG_WAIT / 2, "the wait ran {waited:?}");
}

// a wake between the look and the wait moved the count, so the wait doesn't sleep at all
#[test]
fn wake_before_the_wait_skips_it() {
    let line = WakeLine::default();
    let seen = line.seen();
    line.wake();
    let started = Instant::now();
    line.wait(seen, LONG_WAIT);
    assert!(started.elapsed() < LONG_WAIT / 2);
}
