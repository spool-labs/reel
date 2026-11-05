//! Background work, written as steps so a test can run it one step at a time

use reel_api::units::Nanoseconds;

/// What a job reports after one step
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobStep {
    /// It did some work and may have more
    Worked,
    /// It found nothing to do and waits for a wake
    Idle,
    /// It found nothing to do and waits for a wake or a clock reading
    IdleUntil { deadline: Nanoseconds },
    /// It is done for good
    Finished,
}

/// A unit of background work
pub trait Job: Send + 'static {
    /// Run one bounded step
    fn step(&mut self) -> JobStep;
}
