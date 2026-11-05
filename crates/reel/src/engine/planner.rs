//! The planner: one worker for the store, stepping job units a volume in turn
//!
//! A unit is one step of one job on one volume, bounded and stoppable. The dead-byte walk is the first, and compaction
//! joins it here. The walk is never deferred. It yields while any writer waits on a seal for index room, and while a
//! seal has a segment waiting unless it has fallen too far behind: a walk that waited for quiet could wait for as long
//! as ingest lasts.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_api::units::Nanoseconds;

use crate::engine::compact_job::CompactJob;
use crate::engine::engine_state::EngineState;
use crate::engine::open_segment::{OpenSegment, SegmentPhase};
use crate::engine::volume::Volume;
use crate::engine::walk_job::WalkJob;
use crate::guard::hold;
use crate::io::Io;
use crate::io::job::{Job, JobStep};

/// How long the planner stands aside before it looks again
pub const WRITER_YIELD: Nanoseconds = Nanoseconds(200_000);

/// A volume with this many sealed segments waiting for a walk stops standing aside for seals, since a segment has thousands
/// of row blocks and a unit a seal would fall behind for good. This ends the standing aside and no more: a walk slower than
/// the seals still falls behind while ingest lasts
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
    pub compactions: Vec<CompactJob<Backend>>,
    pub last: usize,
}

impl<Backend: Io> Planner<Backend> {
    /// Check a seal can't finish until a reclaim frees room in the table or a directory, so standing aside would hold both up
    pub fn is_seal_stuck(&self) -> bool {
        self.state.volumes.iter().any(|volume| volume.is_seal_stuck.load(Ordering::Acquire))
    }

    /// Step a ready reclaim while the walks stand aside: it takes a few steps, and a hot write load needs what it frees most.
    /// A seal wakes the planner as it ends and each one that adds to the walk's lag, and an index wait ends on the clock
    pub fn reclaim_only(&mut self, now: Nanoseconds, is_index_wait: bool) -> JobStep {
        let mut soonest = is_index_wait.then_some(Nanoseconds(now.0 + WRITER_YIELD.0));
        for index in 0..self.compactions.len() {
            let step = self.compactions[index].step_unit(now);
            if self.note_step(index, step, &mut soonest) {
                return JobStep::Worked;
            }
        }
        match soonest {
            Some(deadline) => JobStep::IdleUntil { deadline },
            None => JobStep::Idle,
        }
    }

    /// Note what a unit's step found, and say whether it worked
    pub fn note_step(&mut self, index: usize, step: UnitStep, soonest: &mut Option<Nanoseconds>) -> bool {
        match step {
            UnitStep::Worked => {
                self.last = index;
                return true;
            },
            UnitStep::Idle => {},
            UnitStep::RetryAt(at) => *soonest = Some(soonest.map_or(at, |other| Nanoseconds(other.0.min(at.0)))),
        }
        false
    }

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
        let is_stuck = self.is_seal_stuck();
        let is_index_wait = !is_stuck && self.state.index_waiters.load(Ordering::Acquire) > 0;
        if is_index_wait || (!is_stuck && self.is_seal_first()) {
            return self.reclaim_only(now, is_index_wait);
        }

        let count = self.walks.len();
        let first = self.state.io.next_unit(self.last, count);
        let mut soonest: Option<Nanoseconds> = None;
        for turn in 0..count {
            let index = (first + turn) % count;
            // a ready reclaim runs ahead of the next walk target, and each segment it frees is one that walk won't probe
            if self.walks[index].target.is_none() {
                let step = self.compactions[index].step_unit(now);
                if self.note_step(index, step, &mut soonest) {
                    return JobStep::Worked;
                }
            }
            let step = self.walks[index].step_unit(now);
            if self.note_step(index, step, &mut soonest) {
                return JobStep::Worked;
            }
        }
        match soonest {
            Some(deadline) => JobStep::IdleUntil { deadline },
            None => JobStep::Idle,
        }
    }
}
