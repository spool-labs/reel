//! The job that keeps the limit of the write order, the segment counter, the next segment and open segments' disk room ahead of
//! what writers need, and starts open segments' bytes for the device so a seal's sync finds little left to send

use std::sync::Arc;

use reel_core::units::{ByteCount, FileOffset, Nanoseconds};

use crate::engine::engine_state::EngineState;
use crate::engine::open_segment::SegmentPhase;
use crate::engine::write_path::reserve_target;
use crate::engine::volume::Volume;
use crate::guard::hold;
use crate::io::job::{Job, JobStep};
use crate::io::{Io, IoFile, IoTask};
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

/// Room to hold ahead of an open segment's writes, with the file it lies in
pub struct RoomAhead<File> {
    pub file: File,
    pub segment: SegmentNumber,
    pub end: u64,
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

    /// Find an open segment whose writers asked for more room and that no seal has taken, mark it held so its seal waits, and
    /// clone its handle so the call runs with no lock held
    pub fn next_room(&self, volume: &Volume<Backend>) -> Option<RoomAhead<Backend::File>> {
        let mut claims = hold(&volume.claims);
        let segment = claims.open.iter_mut().find(|segment| segment.is_reserve_asked && segment.phase != SegmentPhase::Sealing)?;
        segment.is_room_holding = true;
        Some(RoomAhead {
            file: segment.data.clone(),
            segment: segment.number,
            end: reserve_target(segment, volume, segment.write_end.0),
        })
    }

    /// Hold the next step of room for every open segment that asked. A full disk stops here, and the claim that runs past the
    /// room hears it
    pub fn hold_room(&mut self) {
        let state = Arc::clone(&self.state);
        for volume in &state.volumes {
            while let Some(room) = self.next_room(volume) {
                let is_held = room.file.reserve_to(ByteCount(room.end)).is_ok();
                let mut claims = hold(&volume.claims);
                if let Some(segment) = claims.open.iter_mut().find(|segment| segment.number == room.segment) {
                    segment.is_reserve_asked = false;
                    segment.is_room_holding = false;
                    volume.progress.wake();
                    // a seal that came while the room grew waited for it
                    if segment.phase == SegmentPhase::Sealing
                        && let Some(task) = state.seal_tasks[volume.id.0 as usize].get()
                    {
                        task.wake();
                    }
                    if is_held {
                        segment.reserved_end = FileOffset(segment.reserved_end.0.max(room.end));
                    }
                }
                if !is_held {
                    break;
                }
            }
        }
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
        self.hold_room();
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
