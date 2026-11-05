//! Background work, written as steps so a test can run it one step at a time

/// What a job reports after one step
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobStep {
    /// It did some work and may have more
    Worked,
    /// It found nothing to do and can wait for a wake
    Idle,
    /// It is done for good
    Finished,
}

/// A unit of background work
pub trait Job: Send + 'static {
    /// Run one bounded step
    fn step(&mut self) -> JobStep;
}
