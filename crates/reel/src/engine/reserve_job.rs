//! The job that keeps the limit of the write order, the segment counter, the next segment and open segments' disk room ahead of
//! what writers need, and starts open segments' bytes for the device so a seal's sync finds little left to send

use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_core::units::{ByteCount, FileOffset, Nanoseconds};

use crate::engine::engine_state::EngineState;
use crate::engine::open_segment::SegmentPhase;
use crate::engine::write_path::reserve_target;
use crate::engine::volume::Volume;
use crate::guard::hold;
use crate::io::job::{Job, JobStep};
use crate::io::{Io, IoFile, IoTask};
use crate::pool::touched_filled;
use crate::pool_room::{GrowPolicy, decay_nanos, grow_policy};
use crate::io::host_memory::HostMemory;
use crate::segment::segment_tail::SegmentNumber;

/// How often the job looks: half a run of places lasts over half an hour at a million rows a second
pub const RESERVE_PERIOD: Nanoseconds = Nanoseconds(1_000_000_000);
/// The least a host keeps free past every pool's growth
pub const HOST_MARGIN_BYTES: u64 = 64 * 1024 * 1024;
/// A host keeps free at least this share of its memory past every pool's growth
pub const HOST_MARGIN_DIVISOR: u64 = 32;

/// Read the bytes the pools may still grow by: what the host or the control group has free, less a margin, and no limit where
/// the host says nothing
pub fn host_left(memory: &HostMemory) -> u64 {
    let total = memory.reserve_total().0;
    if total == 0 {
        return u64::MAX;
    }
    memory.free().0.saturating_sub(HOST_MARGIN_BYTES.max(total / HOST_MARGIN_DIVISOR))
}

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
    /// Where the segment's held room ends now, so the call holds only what comes after it
    pub from: u64,
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
            from: segment.reserved_end.0,
            end: reserve_target(segment, volume, segment.write_end.0),
        })
    }

    /// Hold the next step of room for every open segment that asked. A full disk stops here, and the claim that runs past the
    /// room hears it
    pub fn hold_room(&mut self) {
        let state = &self.state;
        for volume in &state.volumes {
            while let Some(room) = self.next_room(volume) {
                let is_held = room.file.reserve_range(FileOffset(room.from), ByteCount(room.end)).is_ok();
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
        let state = &self.state;
        for volume in &state.volumes {
            while let Some(range) = self.next_range(volume) {
                let _started = range.file.start_writeback(FileOffset(range.from), ByteCount(range.to - range.from));
                self.started[range.slot] = WritebackMark { segment: range.segment, through: range.to };
            }
        }
    }

    /// Grow every pool with less than one extent free. A pool wakes the job when it gets there, and a populate that fails leaves
    /// the pool at its size for the next step to try again
    pub fn grow_pools(&self) {
        let now = self.state.io.now().0;
        if grow_policy() == GrowPolicy::Lead {
            for room in &self.state.pool_rooms {
                room.note_rate(now);
            }
        }
        // a paged pool that has held more than an extent past what it wants for the decay gives its highest empty extents back
        for room in &self.state.pool_rooms {
            let _finished = room.finish_releases();
            let _given = room.release_idle(now);
        }
        let target = self.hold_to_target(now);
        if !self.state.pool_rooms.iter().any(|room| room.next_step(room.used()).is_some()) {
            return;
        }
        // the write index and the sorted pages take a step from the record cache when the target leaves no room for it, and the
        // value pages, a cache themselves, grow only into what the target leaves
        let wanted: u64 = self.state.pool_rooms.iter().filter(|room| room.is_paged() && !room.has_unlocked_readers()).filter_map(|room| room.next_step(room.used()).map(|step| (step - room.committed()) as u64)).sum();
        let target_left = target.saturating_sub(self.held_bytes());
        if wanted > target_left {
            let _given = self.state.record_cache.give_back((wanted - target_left) as usize);
        }
        // a step takes no more than the host has left past its margin, and no more than the store's target leaves
        let target_left = target.saturating_sub(self.held_bytes());
        let mut room_left = host_left(&self.state.io.host_memory()).min(target_left);
        for room in &self.state.pool_rooms {
            let _grew = room.grow_step(&mut room_left);
        }
    }

    /// Read the memory the store holds now: what lies outside its rooms, measured at open, and every room's bytes in memory
    pub fn held_bytes(&self) -> u64 {
        let rooms: u64 = self.state.pool_rooms.iter().map(|room| room.committed() as u64).sum();
        self.state.fixed_heap.load(Ordering::Acquire) + rooms
    }

    /// Walk the ladder down to the store's memory target when it holds more, lift the caps once it has held well under the target
    /// for the decay, and return the target in force. Step 0 gives back every paged pool's empty extents past what it wants, with
    /// no cap. Step 1 takes whole record cache shards until the store is under. Step 2 holds the index pools at what they hold now
    /// for whatever is still over, and the report says the target isn't reached. The summaries never shrink, since their rows must
    /// stay
    pub fn hold_to_target(&self, now: u64) -> u64 {
        let state = &self.state;
        let held = self.held_bytes();
        // the host's own pressure lowers the target in force at once, and it rises back only through the decay below
        let pressed = pressed_target(&state.io.host_memory(), held, state.sizing.least_budget.0);
        state.is_under_pressure.store(pressed.is_some(), Ordering::Release);
        let target = state.memory_target.load(Ordering::Acquire).min(pressed.unwrap_or(u64::MAX));
        if held <= target {
            let is_well_under = held + target / UNDER_DIVISOR <= target || target >= state.memory_budget.0;
            let since = state.under_since.load(Ordering::Relaxed);
            if !is_well_under {
                state.under_since.store(0, Ordering::Relaxed);
            } else if since == 0 {
                state.under_since.store(now.max(1), Ordering::Relaxed);
            } else if now.saturating_sub(since) >= decay_nanos() {
                for room in &state.pool_rooms {
                    room.set_cap(usize::MAX);
                }
            }
            state.is_target_short.store(false, Ordering::Release);
            return target;
        }
        state.under_since.store(0, Ordering::Relaxed);
        let mut over = (held - target) as usize;
        for room in state.pool_rooms.iter().filter(|room| room.is_paged()) {
            over = over.saturating_sub(room.release_spare());
        }
        if over > 0 {
            over = over.saturating_sub(state.record_cache.give_back(over));
        }
        if over > 0 {
            for room in state.pool_rooms.iter().filter(|room| room.is_paged()) {
                room.set_cap(room.committed());
            }
        }
        state.is_target_short.store(over > 0, Ordering::Release);
        target
    }
}

/// A store holds well under its target once it holds less than the target by this share of it
pub const UNDER_DIVISOR: u64 = 16;

/// Read the target the host's pressure sets: with less than half the margin free, the store gives back what it lacks, down to its
/// least budget. Nothing when the host has room or says nothing
pub fn pressed_target(memory: &HostMemory, held: u64, least: u64) -> Option<u64> {
    let total = memory.reserve_total().0;
    if total == 0 {
        return None;
    }
    let mark = HOST_MARGIN_BYTES.max(total / HOST_MARGIN_DIVISOR) / 2;
    let free = memory.free().0;
    (free < mark).then(|| held.saturating_sub(mark - free).max(least))
}

impl<Backend: Io> Job for ReserveJob<Backend> {
    fn step(&mut self) -> JobStep {
        self.grow_pools();
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
