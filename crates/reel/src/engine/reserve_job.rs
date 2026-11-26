//! The job that keeps the limit of the write order, the segment counter, the next segment and open segments' disk room ahead of
//! what writers need, and starts open segments' bytes for the device so a seal's sync finds little left to send

use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_core::units::{ByteCount, FileOffset, Nanoseconds, RowCount};
use crate::index::sorted_keys::planned_index_bytes;

use crate::engine::complete_path::refill_ceiling;
use crate::engine::engine_state::EngineState;
use crate::engine::open_segment::SegmentPhase;
use crate::engine::write_path::reserve_target;
use crate::engine::volume::Volume;
use crate::guard::hold;
use crate::io::job::{Job, JobStep};
use crate::io::{Io, IoFile, IoTask};
use crate::pool::touched_filled;
use crate::pool_room::{GrowPolicy, decay_nanos, grow_policy};
use crate::io::host_memory::{HostMemory, PRESSURE_READ_MARK};
use crate::index::value_pages::VALUE_PAGE_BYTES;
use crate::index::spot_index::SpotIndex;
use crate::segment::block_cache::BlockCache;
use reel_core::memory_report::PressureSource;
use crate::segment::segment_tail::SegmentNumber;

/// How often the job looks: half a run of places lasts over half an hour at a million rows a second
pub const RESERVE_PERIOD: Nanoseconds = Nanoseconds(1_000_000_000);
/// How often the job looks while the group's room is under twice the margin
pub const TIGHT_PERIOD: Nanoseconds = Nanoseconds(100_000_000);
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
    // in a control group the room past what the kernel can't evict is the honest measure, since free counts page cache a neighbour
    // is about to take
    let free = memory.group_anon_room.map_or(memory.free().0, |room| room.0.min(memory.available.0));
    free.saturating_sub(host_margin(total))
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
        if matches!(grow_policy(), GrowPolicy::Lead | GrowPolicy::Mixed) {
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
        // the heap measured at open counts the whole caches, whose shards given back hold no memory
        (self.state.fixed_heap.load(Ordering::Acquire) + rooms).saturating_sub(caches_given_back(&self.state))
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
        let least = state.sizing.least_budget.0;
        // the kernel's own word comes first: a trigger that fired, or a group whose tasks stall on memory past the mark. Either
        // gives back a sixteenth of what the store holds each step it lasts
        let is_kernel_pressed = match state.pressure_source.get() {
            Some(PressureSource::Trigger) => state.is_pressure_fired.swap(false, Ordering::AcqRel),
            Some(PressureSource::Read) => {
                let is_stalled = state.io.memory_pressure().is_some_and(|stall| stall > PRESSURE_READ_MARK);
                // the first read only sets the mark, and a later one counts as pressure when reclaim failed in the group since
                let failures = state.io.memory_reclaim_failures().unwrap_or(0);
                let before = state.reclaim_failures.swap(failures, Ordering::AcqRel);
                is_stalled || (before != u64::MAX && failures > before)
            },
            _ => false,
        };
        let kernel_pressed = is_kernel_pressed.then(|| held.saturating_sub(held / UNDER_DIVISOR).max(least));
        let memory = state.io.host_memory();
        state.is_host_tight.store(is_host_tight(&memory), Ordering::Release);
        let host_pressed = pressed_target(&memory, held, least);
        let pressed = match (kernel_pressed, host_pressed) {
            (Some(kernel), Some(host)) => Some(kernel.min(host)),
            (kernel, host) => kernel.or(host),
        };
        state.is_under_pressure.store(pressed.is_some(), Ordering::Release);
        if pressed.is_some() {
            state.pressed_steps.fetch_add(1, Ordering::Relaxed);
        }
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
                // a dropped index comes back once the store has room past the decay for every row it held, the quarter its
                // refill may grow past that and the extent the last growth may end in, and the compact job refills it
                let room = (target - held).min(host_left(&memory));
                // the whole caches come back first, empty, and fill from reads
                let room = room.saturating_sub(bring_back_caches(state, room));
                // a cache still away brings the next step on soon
                state.is_bringing_back.store(whole_caches(state).any(|cache| cache.absent_shards() > 0), Ordering::Release);
                for index in state.sorted.iter().flatten().filter(|index| index.is_dropped()) {
                    let rows = RowCount(index.past_capacity().0 + index.entry_count().0);
                    let ask = refill_ceiling(planned_index_bytes(index.width(), rows).0 as usize) + index.room().extent();
                    let may = pressed.is_none() && ask as u64 <= room;
                    if may && !index.may_refill() {
                        index.set_may_refill(true);
                        state.ask_compaction();
                    } else if !may {
                        index.set_may_refill(false);
                    }
                }
            }
            state.is_target_short.store(false, Ordering::Release);
            return target;
        }
        state.under_since.store(0, Ordering::Relaxed);
        for index in state.sorted.iter().flatten() {
            index.set_may_refill(false);
        }
        state.is_bringing_back.store(false, Ordering::Release);
        let was_short = state.is_target_short.load(Ordering::Acquire);
        let mut over = (held - target) as usize;
        for room in state.pool_rooms.iter().filter(|room| room.is_paged()) {
            over = over.saturating_sub(room.release_spare());
        }
        if over > 0 {
            over = over.saturating_sub(state.record_cache.give_back(over));
        }
        // the leaves let go of the values they hold until the value pages free what is still over, and the value room gives the
        // extents that empties back
        if over > 0 {
            let mut wanted = over.div_ceil(VALUE_PAGE_BYTES) as u32;
            for index in state.sorted.iter().flatten() {
                if wanted == 0 {
                    break;
                }
                wanted = wanted.saturating_sub(index.drop_held_values(wanted));
            }
            if let Some(index) = state.sorted.iter().flatten().next() {
                let _given = index.values.room().release_spare();
            }
            // an extent waiting on its readers is out of the room's count already, so what is still over comes from the count
            over = self.held_bytes().saturating_sub(target) as usize;
        }
        // the spot index and the block caches go next, a shard at a time: a miss reads the segment again, and they fill again from
        // reads once the decay brings them back
        if over > 0 {
            over = over.saturating_sub(give_back_caches(state, over));
        }
        if over > 0 {
            for room in state.pool_rooms.iter().filter(|room| room.is_paged()) {
                room.set_cap(room.committed());
            }
        }
        // the write index holds the open rows, and only a seal gives their pages back, so a store still over seals early: smaller
        // segments and more merges, and the pages come back to the next step
        if over > 0 {
            for volume in &state.volumes {
                state.seal_fullest(volume);
            }
        }
        // the last step, under the kernel's or the host's pressure that a step's seals didn't meet: drop the largest sorted index
        // whole. Its reads ask the footers until the store has room again and a refill brings it back
        if over > 0 && pressed.is_some() && was_short {
            let largest = state.sorted.iter().flatten().filter(|index| index.entry_count().0 > 0).max_by_key(|index| index.page_count());
            if let Some(index) = largest {
                let _freed = state.drop_sorted(index);
                state.sorted_drops.fetch_add(1, Ordering::Relaxed);
                over = over.saturating_sub(index.room().release_spare());
            }
        }
        state.is_target_short.store(over > 0, Ordering::Release);
        target
    }
}

/// A store holds well under its target once it holds less than the target by this share of it
pub const UNDER_DIVISOR: u64 = 16;

/// Read the target the host's pressure sets, or nothing when the host has room or says nothing. In a control group the measure is
/// the room left past what the kernel can't evict, which falls from the first byte a neighbour takes: under the whole margin the
/// store gives back a sixteenth of what it holds each step, which outruns a neighbour that grows by the second. Elsewhere, with
/// less than half the margin free, it gives back what it lacks. A neighbour that takes the rest in one second still wins. Either way
/// the target goes no lower than the least budget
pub fn pressed_target(memory: &HostMemory, held: u64, least: u64) -> Option<u64> {
    let total = memory.reserve_total().0;
    if total == 0 {
        return None;
    }
    let margin = host_margin(total);
    if let Some(anon_room) = memory.group_anon_room {
        return (anon_room.0 < margin).then(|| held.saturating_sub(held / UNDER_DIVISOR).max(least));
    }
    let mark = margin / 2;
    let free = memory.free().0;
    (free < mark).then(|| held.saturating_sub(mark - free).max(least))
}

/// Read the margin a host keeps free past every pool: a thirty-second of it, and 64 MiB at least
pub fn host_margin(total: u64) -> u64 {
    HOST_MARGIN_BYTES.max(total / HOST_MARGIN_DIVISOR)
}

/// Check the group's room past what the kernel can't evict is under twice the margin, where the job looks ten times a second so a
/// neighbour's ramp meets a step before it meets the limit
pub fn is_host_tight(memory: &HostMemory) -> bool {
    let total = memory.reserve_total().0;
    total > 0 && memory.group_anon_room.is_some_and(|room| room.0 < 2 * host_margin(total))
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
            deadline: Nanoseconds(now.0 + if self.state.is_host_tight.load(Ordering::Acquire) || self.state.is_bringing_back.load(Ordering::Acquire) { TIGHT_PERIOD.0 } else { RESERVE_PERIOD.0 }),
        }
    }
}

/// A cache the ladder gives up a shard at a time and brings back once the store has room. A miss reads the segment again, so
/// nothing but speed waits on it, and it fills again from reads and seals
pub trait WholeCache {
    /// Give a shard's pages back, and return the bytes given
    fn give_back_shard(&self, shard: usize) -> usize;
    /// Bring a shard back empty, and say whether it was absent
    fn bring_back_shard(&self, shard: usize) -> bool;
    /// Count the shards given back
    fn absent_shards(&self) -> usize;
    /// Read the bytes one shard takes
    fn shard_bytes(&self) -> usize;
    /// Count the shards
    fn shard_count(&self) -> usize;
    /// Read the bytes the shards given back hold no more
    fn given_bytes(&self) -> u64;
}

impl WholeCache for SpotIndex {
    fn give_back_shard(&self, shard: usize) -> usize {
        SpotIndex::give_back_shard(self, shard)
    }

    fn bring_back_shard(&self, shard: usize) -> bool {
        SpotIndex::bring_back_shard(self, shard)
    }

    fn absent_shards(&self) -> usize {
        SpotIndex::absent_shards(self)
    }

    fn shard_bytes(&self) -> usize {
        SpotIndex::shard_bytes(self)
    }

    fn shard_count(&self) -> usize {
        SpotIndex::shard_count(self)
    }

    fn given_bytes(&self) -> u64 {
        SpotIndex::given_bytes(self)
    }
}

impl WholeCache for BlockCache {
    fn give_back_shard(&self, shard: usize) -> usize {
        BlockCache::give_back_shard(self, shard)
    }

    fn bring_back_shard(&self, shard: usize) -> bool {
        BlockCache::bring_back_shard(self, shard)
    }

    fn absent_shards(&self) -> usize {
        BlockCache::absent_shards(self)
    }

    fn shard_bytes(&self) -> usize {
        BlockCache::shard_bytes(self)
    }

    fn shard_count(&self) -> usize {
        BlockCache::shard_count(self)
    }

    fn given_bytes(&self) -> u64 {
        BlockCache::given_bytes(self)
    }
}

/// List the caches the ladder gives up whole, the spot index first
pub fn whole_caches<Backend: Io>(state: &EngineState<Backend>) -> impl Iterator<Item = &dyn WholeCache> {
    let fixed: [&dyn WholeCache; 3] = [&state.spot, &state.row_cache, &state.filter_cache];
    fixed.into_iter().chain(state.complete_cache.as_ref().map(|cache| cache as &dyn WholeCache))
}

/// Read the bytes the whole caches have given back, which the heap measured at open still counts
pub fn caches_given_back<Backend: Io>(state: &EngineState<Backend>) -> u64 {
    whole_caches(state).map(WholeCache::given_bytes).sum()
}

/// Give the whole caches' shards back, the highest first, until this many bytes are given or none is left, and return the bytes
pub fn give_back_caches<Backend: Io>(state: &EngineState<Backend>, wanted: usize) -> usize {
    let mut given = 0;
    for cache in whole_caches(state) {
        for shard in (0..cache.shard_count()).rev() {
            if given >= wanted {
                return given;
            }
            let bytes = cache.give_back_shard(shard);
            if bytes > 0 {
                given += bytes;
                state.cache_shards_given.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    given
}

/// Bring one shard of the whole caches back, the spot index first, when it fits in this many bytes, and return the bytes taken. One
/// a step, so the job looks at pressure and the pools between fills, and the next step comes soon while one is still away
pub fn bring_back_caches<Backend: Io>(state: &EngineState<Backend>, room: u64) -> u64 {
    for cache in whole_caches(state) {
        if cache.absent_shards() == 0 {
            continue;
        }
        let bytes = cache.shard_bytes() as u64;
        if bytes > room {
            return 0;
        }
        for shard in 0..cache.shard_count() {
            if cache.bring_back_shard(shard) {
                return bytes;
            }
        }
    }
    0
}
