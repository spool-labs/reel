//! Filling the arena: a planner step reads a few of a segment's filters at a time, newest segment first, so an open stays
//! as quick as it was and a seal has nothing more to do

use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_core::column_id::ColumnId;
use reel_core::maintenance_report::{PickKind, UnitKind};
use reel_core::units::FileOffset;

use crate::engine::reports::thread_cpu;
use crate::engine::engine_state::EngineState;
use crate::engine::volume::Volume;
use crate::io::{Io, IoFile, IoTask};
use crate::pool::touched_capacity;
use crate::segment::block_filter::open_filter;
use crate::segment::segment_tail::{MAX_COLUMNS, SegmentNumber};

/// Filters a step reads, so a step stays short beside the planner's other work
pub const FILTER_STEP_BLOCKS: usize = 64;
/// Most segments one pick looks at from the newest down, and as many again from where the last pick's sweep stopped
pub const FILTER_LOOKS: usize = 64;

/// A run of a segment's block lines
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BlockRun {
    pub next: u32,
    pub end: u32,
}

/// One volume's loader
pub struct FilterLoader<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
    pub volume: usize,
    segment: Option<SegmentNumber>,
    runs: Vec<BlockRun>,
    looked_at: u64,
    loaded: u64,
    swept_to: SegmentNumber,
}

impl<Backend: Io> FilterLoader<Backend> {
    /// Start with nothing to load
    pub fn new(state: Arc<EngineState<Backend>>, volume: usize) -> Self {
        Self {
            state,
            volume,
            segment: None,
            runs: touched_capacity(MAX_COLUMNS, BlockRun::default()),
            looked_at: u64::MAX,
            loaded: 0,
            swept_to: SegmentNumber(0),
        }
    }

    /// Count what can change which segments want loading
    pub fn changes(&self, volume: &Volume<Backend>) -> u64 {
        let left = volume.segments_rewritten.load(Ordering::Acquire) + volume.segments_unlinked.load(Ordering::Acquire);
        left + volume.seals_done.load(Ordering::Acquire) + self.state.filter_arena.grows()
    }

    /// Load a few filters, and say whether there's more to do
    pub fn step(&mut self) -> bool {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        if self.segment.is_none() && !self.pick(volume) {
            return false;
        }
        let Some(segment) = self.segment else {
            return false;
        };
        let started = state.io.now();
        let cpu_started = thread_cpu();
        let loaded = self.load_some(volume, segment);
        state.note_unit(UnitKind::FilterLoad, self.loaded, started, cpu_started);
        if !loaded {
            state.filter_arena.drop_segment(segment);
            state.filter_arena.note_loss();
            // an empty extent says the arena tried, so the look moves on and the block cache answers for this segment
            if !state.filter_arena.begin(segment, 0, 0) {
                self.looked_at = self.changes(volume);
            }
            self.segment = None;
            return true;
        }
        if self.runs.iter().all(|run| run.next == run.end) {
            self.segment = None;
        }
        true
    }

    /// Pick the newest segment the arena lacks and set its extent aside, once something has changed since the last look
    pub fn pick(&mut self, volume: &Volume<Backend>) -> bool {
        let changes = self.changes(volume);
        if changes == self.looked_at {
            return false;
        }
        let state = &self.state;
        let arena = &state.filter_arena;
        let (next, is_swept) = {
            let rows = state.hold_for_pick(self.volume, PickKind::Filter);
            let is_wanted = |segment: &&SegmentNumber| !arena.holds(**segment) && rows.get(**segment).is_some_and(|row| !row.facts.is_damaged);
            let present = rows.orders().present();
            let newest = present.iter().rev().take(FILTER_LOOKS).find(is_wanted);
            // a sweep down the older segments goes on from where the last one stopped, and starts at the top again past the oldest
            let below = if self.swept_to.0 == 0 { present } else { rows.orders().below(self.swept_to) };
            let window = &below[below.len().saturating_sub(FILTER_LOOKS)..];
            let swept = window.iter().rev().find(is_wanted);
            self.swept_to = swept.or(window.first()).copied().filter(|_segment| window.len() == FILTER_LOOKS).unwrap_or(SegmentNumber(0));
            (newest.or(swept).copied(), window.len() < FILTER_LOOKS)
        };
        let Some(segment) = next else {
            // nothing wanted waits once a sweep has reached the oldest segment
            if is_swept {
                self.looked_at = changes;
            }
            return false;
        };
        if !self.plan_extent(volume, segment) {
            // a segment that doesn't fit stays with the cache, and an older one is smaller or no better off. The reserve job grows
            // the arena for it when it can, and the grow brings this look back
            self.looked_at = changes;
            if self.state.filter_arena.is_short()
                && let Some(task) = self.state.reserve_task.get()
            {
                task.wake();
            }
            return false;
        }
        self.segment = Some(segment);
        true
    }

    /// List the segment's block runs on columns that search footers, and set aside room for their filters
    pub fn plan_extent(&mut self, volume: &Volume<Backend>, segment: SegmentNumber) -> bool {
        self.runs.clear();
        let (mut blocks, mut bits) = (0, 0_u64);
        for id in 0..MAX_COLUMNS {
            let column = ColumnId(id as u8);
            if self.state.complete_table(column).is_some() {
                continue;
            }
            let Some((first, count)) = volume.table.column_blocks(segment, column) else {
                continue;
            };
            self.runs.push(BlockRun { next: first, end: first + count });
            blocks += count;
            for block in first..first + count {
                let Some((start, end)) = self.filter_span(volume, segment, block) else {
                    return false;
                };
                bits += end.0 - start.0;
            }
        }
        let Ok(bits) = u32::try_from(bits) else {
            return false;
        };
        self.state.filter_arena.begin(segment, blocks, bits)
    }

    /// Find where a block's filter lies, its checksum included
    pub fn filter_span(&self, volume: &Volume<Backend>, segment: SegmentNumber, block: u32) -> Option<(FileOffset, FileOffset)> {
        let (line, _replacement) = volume.table.block_line(segment, block)?;
        let end = match volume.table.block_line(segment, block + 1) {
            Some((next, _replacement)) => next.header_at(),
            None => volume.table.pin(segment)?.head.index_at,
        };
        Some((FileOffset(u64::from(line.filter_offset)), end))
    }

    /// Read the next few filters into the extent, or say this segment can't go in
    pub fn load_some(&mut self, volume: &Volume<Backend>, segment: SegmentNumber) -> bool {
        let Some(pin) = volume.table.pin(segment) else {
            return false;
        };
        self.loaded = 0;
        let mut left = FILTER_STEP_BLOCKS;
        let mut buffer = self.state.job_footer_readers.lease();
        for index in 0..self.runs.len() {
            while left > 0 && self.runs[index].next < self.runs[index].end {
                let block = self.runs[index].next;
                let Some((start, end)) = self.filter_span(volume, segment, block) else {
                    return false;
                };
                let Some(stored) = buffer.filter_room((end.0 - start.0) as usize) else {
                    return false;
                };
                if pin.file.read_exact_at(start, stored).is_err() {
                    return false;
                }
                let Some(bits) = open_filter(stored) else {
                    return false;
                };
                self.state.filter_arena.add(segment, start.0 as u32, bits);
                self.runs[index].next += 1;
                self.loaded += 1;
                left -= 1;
            }
        }
        true
    }
}
