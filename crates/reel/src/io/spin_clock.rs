//! A spin's time limit, read from the clock once every few passes, so a spinning thread waits in pause and not in the clock

use std::time::{Duration, Instant};

/// Spin passes between clock reads
pub const CLOCK_EVERY_PASSES: u32 = 32;

/// A spin's start and limit, and whether the limit has passed
#[derive(Debug)]
pub struct SpinClock {
    started: Instant,
    limit: Duration,
    passes: u32,
    is_over: bool,
}

impl SpinClock {
    /// Start a spin with this limit
    pub fn new(limit: Duration) -> Self {
        Self { started: Instant::now(), limit, passes: 0, is_over: limit.is_zero() }
    }

    /// Count a pass and say whether the limit has passed, reading the clock once every CLOCK_EVERY_PASSES passes
    pub fn is_over(&mut self) -> bool {
        if !self.is_over {
            self.passes = self.passes.wrapping_add(1);
            if self.passes.is_multiple_of(CLOCK_EVERY_PASSES) {
                self.is_over = self.started.elapsed() >= self.limit;
            }
        }
        self.is_over
    }

    /// Read the clock now and say whether the limit has passed
    pub fn is_over_now(&mut self) -> bool {
        self.is_over = self.is_over || self.started.elapsed() >= self.limit;
        self.is_over
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spin_ends_past_its_limit() {
        // the test's start comes first, so its span is never shorter than the clock's
        let started = Instant::now();
        let mut clock = SpinClock::new(Duration::from_micros(50));
        while !clock.is_over() {
            std::hint::spin_loop();
        }
        assert!(started.elapsed() >= Duration::from_micros(50));
        assert!(clock.is_over_now());
    }

    #[test]
    fn zero_limit_is_over_at_once() {
        assert!(SpinClock::new(Duration::ZERO).is_over());
    }
}
