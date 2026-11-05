//! The job that keeps the write order's limit and the segment counter ahead of what writers need

use std::sync::Arc;

use reel_api::units::Nanoseconds;

use crate::engine::engine_state::EngineState;
use crate::io::Io;
use crate::io::job::{Job, JobStep};

/// How often the job looks: half a run of places lasts over half an hour at a million rows a second
pub const RESERVE_PERIOD: Nanoseconds = Nanoseconds(1_000_000_000);

/// The job that saves the next reservation, so no writer waits on a root's sync
pub struct ReserveJob<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
}

impl<Backend: Io> Job for ReserveJob<Backend> {
    fn step(&mut self) -> JobStep {
        if self.state.needs_reservation() {
            self.state.reserve_ahead();
        }
        let now = self.state.io.now();
        JobStep::IdleUntil {
            deadline: Nanoseconds(now.0 + RESERVE_PERIOD.0),
        }
    }
}
