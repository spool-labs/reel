//! The planner: one worker for the store, stepping job units a volume in turn
//!
//! A unit is one step of one job on one volume, bounded and stoppable. The dead-byte walk is the first, and compaction
//! joins it here. The walk is never deferred. It yields while any writer waits on a seal for index room, and while a
//! seal has a segment waiting unless it has fallen too far behind: a walk that waited for quiet could wait for as long
//! as ingest lasts.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_api::units::Nanoseconds;

use crate::engine::engine_state::EngineState;
use crate::engine::open_segment::{OpenSegment, SegmentPhase};
use crate::engine::volume::Volume;
use crate::engine::walk_job::WalkJob;
use crate::guard::hold;
use crate::io::Io;
use crate::io::job::{Job, JobStep};

/// How long the planner stands aside before it looks again
pub const WRITER_YIELD: Nanoseconds = Nanoseconds(200_000);

/// A volume with this many sealed segments waiting for a walk stops standing aside for seals. A segment has thousands of
/// row blocks, so a unit a seal would fall behind for good, and this bound keeps the walk within a few segments of ingest
pub const WALK_LAG_SEGMENTS: u64 = 8;

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

impl<Backend: Io> Planner<Backend> {
    /// Check whether a seal goes first: a segment is waiting and no volume's walk has fallen too far behind. A failed
    /// volume's seal never ends, so it counts for nothing
    pub fn is_seal_first(&self) -> bool {
        let volumes = &self.state.volumes;
        let is_behind = volumes.iter().any(|volume| volume.walks_waiting.load(Ordering::Acquire) >= WALK_LAG_SEGMENTS);
        let is_sealing = |volume: &Volume<Backend>| !volume.has_failed() && hold(&volume.claims).open.iter().any(is_sealing);
        !is_behind && volumes.iter().any(is_sealing)
    }
}

/// Check a segment is marked to seal or being sealed
pub fn is_sealing<File>(segment: &OpenSegment<File>) -> bool {
    segment.phase == SegmentPhase::Sealing
}

impl<Backend: Io> Job for Planner<Backend> {
    fn step(&mut self) -> JobStep {
        let now = self.state.io.now();
        if self.state.index_waiters.load(Ordering::Acquire) > 0 {
            return JobStep::IdleUntil {
                deadline: Nanoseconds(now.0 + WRITER_YIELD.0),
            };
        }
        // a seal wakes the planner as it ends, and each one that adds to the walk's lag
        if self.is_seal_first() {
            return JobStep::Idle;
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
