//! One slot's read as its caller and its worker share it: the caller starts it and waits, the worker ends it, and whichever
//! side comes last frees the slot, so a caller that went away never leaves a slot behind

use std::task::{Context, Poll, Waker};

/// Where a slot's read stands
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeasePhase {
    /// No read is under way: the slot is free, or a caller holds it and hasn't started one
    Idle,
    /// A worker or the device has the read
    Busy,
    /// The read is over and its answer waits for the caller
    Done,
    /// The caller went away, and the side that ends the read frees the slot
    Dropped,
}

/// What ending a read leaves for the side that ended it
#[derive(Debug)]
pub enum Ended {
    /// The caller is waiting, and this wakes it
    Wake(Option<Waker>),
    /// The caller went away, so the slot goes back
    Release,
}

/// A read's phase, answer and the caller's waker
#[derive(Debug)]
pub struct LeaseCell<Answer> {
    phase: LeasePhase,
    answer: Option<Answer>,
    waker: Option<Waker>,
}

impl<Answer> LeaseCell<Answer> {
    /// Start idle
    pub const fn new() -> Self {
        Self { phase: LeasePhase::Idle, answer: None, waker: None }
    }

    /// Read the phase
    pub fn phase(&self) -> LeasePhase {
        self.phase
    }

    /// Hand the read over
    pub fn start(&mut self) {
        self.phase = LeasePhase::Busy;
    }

    /// Take the read back when the hand-over failed
    pub fn unstart(&mut self) {
        self.phase = LeasePhase::Idle;
    }

    /// End the read with its answer, and say whether to wake the caller or free the slot
    pub fn end(&mut self, answer: Answer) -> Ended {
        if self.phase == LeasePhase::Dropped {
            return Ended::Release;
        }
        self.answer = Some(answer);
        self.phase = LeasePhase::Done;
        Ended::Wake(self.waker.take())
    }

    /// Take the answer of a read that's over
    pub fn take_answer(&mut self) -> Option<Answer> {
        if self.phase != LeasePhase::Done {
            return None;
        }
        self.answer.take()
    }

    /// Take the answer, or leave the caller's waker for the end to wake
    pub fn poll_answer(&mut self, context: &Context<'_>) -> Poll<Answer> {
        if let Some(answer) = self.take_answer() {
            return Poll::Ready(answer);
        }
        self.wait_with(context.waker());
        Poll::Pending
    }

    /// Leave a waker for the end to wake, keeping one that would wake the same task
    pub fn wait_with(&mut self, waker: &Waker) {
        if !self.waker.as_ref().is_some_and(|held| held.will_wake(waker)) {
            self.waker = Some(waker.clone());
        }
    }

    /// The caller goes: a busy read keeps the slot for its end to free, and anything else frees it now. Say which
    pub fn leave(&mut self) -> bool {
        if self.phase == LeasePhase::Busy {
            self.phase = LeasePhase::Dropped;
            return false;
        }
        true
    }

    /// Start over for the next caller
    pub fn reset(&mut self) {
        self.phase = LeasePhase::Idle;
        self.answer = None;
        self.waker = None;
    }
}

impl<Answer> Default for LeaseCell<Answer> {
    fn default() -> Self {
        Self::new()
    }
}
