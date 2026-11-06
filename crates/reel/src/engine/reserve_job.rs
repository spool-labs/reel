//! The job that keeps the limit of the write order, the segment counter and the next segment ahead of what writers need, and
//! starts open segments' bytes for the device so a seal's sync finds little left to send

use std::sync::Arc;

use reel_api::units::{ByteCount, FileOffset, Nanoseconds};

use crate::engine::engine_state::EngineState;
use crate::engine::volume::Volume;
use crate::guard::hold;
use crate::io::job::{Job, JobStep};
use crate::io::{Io, IoFile};
use crate::pool::touched_filled;
use crate::segment::segment_tail::SegmentNumber;

/// How often the job looks: half a run of places lasts over half an hour at a million rows a second
pub const RESERVE_PERIOD: Nanoseconds = Nanoseconds(1_000_000_000);
/// Bytes an open segment gathers before they start for the device
pub const WRITEBACK_STEP_BYTES: u64 = 8 * 1024 * 1024;

/// How far an open slot's data file has started for the device
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WritebackMark {
    pub segment: SegmentNumber,
    pub through: u64,
}

/// A range ready to start, with the file it lies in
pub struct WritebackRange<File> {
    pub file: File,
    pub slot: usize,
    pub segment: SegmentNumber,
    pub from: u64,
    pub to: u64,
}

/// The job that saves the next reservation, so no writer waits on a root's sync
pub struct ReserveJob<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
    pub started: Vec<WritebackMark>,
}

impl<Backend: Io> ReserveJob<Backend> {
    /// Start with nothing sent for any slot
    pub fn new(state: Arc<EngineState<Backend>>) -> Self {
        let started = touched_filled(state.queues.len(), WritebackMark { segment: SegmentNumber(0), through: 0 });
        Self { state, started }
    }

    /// Find an open segment with enough new bytes, and clone its handle so the call runs with no lock held
    pub fn next_range(&self, volume: &Volume<Backend>) -> Option<WritebackRange<Backend::File>> {
        let claims = hold(&volume.claims);
        claims.open.iter().find_map(|segment| {
            let slot = segment.slot.0 as usize;
            let mark = self.started[slot];
            let from = if mark.segment == segment.number { mark.through } else { 0 };
            let to = segment.write_end.0;
            (to.saturating_sub(from) >= WRITEBACK_STEP_BYTES).then(|| WritebackRange {
                file: segment.data.clone(),
                slot,
                segment: segment.number,
                from,
                to,
            })
        })
    }

    /// Start every open segment's new bytes. The call is a hint: a failure leaves them to the seal's sync
    pub fn start_writeback(&mut self) {
        let state = Arc::clone(&self.state);
        for volume in &state.volumes {
            while let Some(range) = self.next_range(volume) {
                let _started = range.file.start_writeback(FileOffset(range.from), ByteCount(range.to - range.from));
                self.started[range.slot] = WritebackMark { segment: range.segment, through: range.to };
            }
        }
    }
}

impl<Backend: Io> Job for ReserveJob<Backend> {
    fn step(&mut self) -> JobStep {
        self.start_writeback();
        self.state.prepare_segments();
        if self.state.needs_reservation() {
            // a root that took nothing failed its volume, and writers there hear it
            let _is_held = self.state.reserve_ahead();
        }
        let now = self.state.io.now();
        JobStep::IdleUntil {
            deadline: Nanoseconds(now.0 + RESERVE_PERIOD.0),
        }
    }
}
