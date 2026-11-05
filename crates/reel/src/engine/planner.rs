//! The planner: one worker for the store, stepping job units a volume in turn
//!
//! A unit is one step of one job on one volume, bounded and stoppable. The dead-byte walk is the first, and compaction
//! joins it here. The walk is never deferred, and it yields while any writer waits on a seal for index room.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_api::units::Nanoseconds;

use crate::engine::engine_state::EngineState;
use crate::engine::walk_job::WalkJob;
use crate::io::Io;
use crate::io::job::{Job, JobStep};

/// How long the planner stands aside while a writer waits for index room
pub const WRITER_YIELD: Nanoseconds = Nanoseconds(200_000);

/// What one step of a unit found
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnitStep {
    /// It did a bounded piece of work
    Worked,
    /// It has nothing to do
    Idle,
    /// It failed, and can try again at this clock reading
    RetryAt(Nanoseconds),
}

/// Every volume's units and whose turn it is
pub struct Planner<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
    pub walks: Vec<WalkJob<Backend>>,
    pub last: usize,
}

impl<Backend: Io> Job for Planner<Backend> {
    fn step(&mut self) -> JobStep {
        let now = self.state.io.now();
        if self.state.index_waiters.load(Ordering::Acquire) > 0 {
            return JobStep::IdleUntil {
                deadline: Nanoseconds(now.0 + WRITER_YIELD.0),
            };
        }

        let count = self.walks.len();
        let first = self.state.io.next_unit(self.last, count);
        let mut soonest: Option<Nanoseconds> = None;
        for turn in 0..count {
            let index = (first + turn) % count;
            match self.walks[index].step_unit(now) {
                UnitStep::Worked => {
                    self.last = index;
                    return JobStep::Worked;
                },
                UnitStep::Idle => {},
                UnitStep::RetryAt(at) => soonest = Some(soonest.map_or(at, |other| Nanoseconds(other.0.min(at.0)))),
            }
        }
        match soonest {
            Some(deadline) => JobStep::IdleUntil { deadline },
            None => JobStep::Idle,
        }
    }
}
