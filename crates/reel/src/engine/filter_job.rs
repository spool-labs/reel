//! Filling the arena: a planner step reads a few of a segment's filters at a time, newest segment first, so an open stays
//! as quick as it was and a seal has nothing more to do

use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::units::FileOffset;

use crate::engine::engine_state::EngineState;
use crate::engine::volume::{GaveUp, Volume};
use crate::guard::hold;
use crate::io::{Io, IoFile};
use crate::pool::touched_capacity;
use crate::segment::block_filter::open_filter;
use crate::segment::segment_tail::{MAX_COLUMNS, SegmentNumber};

/// Filters a step reads, so a step stays short beside the planner's other work
pub const FILTER_STEP_BLOCKS: usize = 64;

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
    skipped: GaveUp,
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
            skipped: GaveUp::new(),
        }
    }

    /// Count what can change which segments want loading
    pub fn changes(&self, volume: &Volume<Backend>) -> u64 {
        let left = volume.segments_rewritten.load(Ordering::Acquire) + volume.segments_unlinked.load(Ordering::Acquire);
        left + volume.seals_done.load(Ordering::Acquire)
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
        if !self.load_some(volume, segment) {
            state.filter_arena.drop_segment(segment);
            self.skipped.add(segment);
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
        let arena = &self.state.filter_arena;
        let next = {
            let rows = hold(&volume.summary);
            let wanted = rows.rows().filter(|row| !row.facts.is_damaged && !self.skipped.holds(row.facts.segment));
            wanted.map(|row| row.facts.segment).filter(|segment| !arena.holds(*segment)).max()
        };
        let Some(segment) = next else {
            self.looked_at = changes;
            return false;
        };
        if !self.plan_extent(volume, segment) {
            // a segment that doesn't fit stays with the cache, and an older one is smaller or no better off
            self.looked_at = changes;
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
        let mut left = FILTER_STEP_BLOCKS;
        let mut buffer = self.state.footer_readers.lease();
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
                left -= 1;
            }
        }
        true
    }
}
