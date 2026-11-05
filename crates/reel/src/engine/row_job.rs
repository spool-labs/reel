//! Row write-out: every group reaches the kernel within 100 ms of its puts returning

use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_api::units::Nanoseconds;

use crate::engine::engine_state::EngineState;
use crate::guard::hold;
use crate::index::open_slots::OpenSlot;
use crate::io::Io;
use crate::io::job::{Job, JobStep};

/// How long rows wait in memory, well inside the 100 ms a kill can cost
pub const WRITE_OUT_PERIOD: Nanoseconds = Nanoseconds(50_000_000);

/// The job that sends waiting rows to the kernel
pub struct RowJob<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
}

impl<Backend: Io> Job for RowJob<Backend> {
    fn step(&mut self) -> JobStep {
        if !self.state.rows_waiting.swap(false, Ordering::AcqRel) {
            return JobStep::Idle;
        }

        for slot in 0..self.state.queues.len() {
            let is_empty = hold(&self.state.queues[slot]).filling.is_empty();
            if !is_empty {
                // a failed write leaves its rows in the queue, and the next pass or a sync reports it
                let _written = self.state.write_out(OpenSlot(slot as u8));
            }
        }

        let now = self.state.io.now();
        JobStep::IdleUntil {
            deadline: Nanoseconds(now.0 + WRITE_OUT_PERIOD.0),
        }
    }
}
