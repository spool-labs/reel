//! The end of a walk of a segment with range deletes: what each one hides in segments already walked gets counted
//!
//! A row walked before a cover was applied was counted live, and no later walk looks at it again. A column wholly inside
//! the range goes by its block counts with no row read, so a range delete's cost doesn't follow the keys it covers. Any
//! other column is read a block at a time, and a row counts only while it's its key's newest, since an older one is dead
//! already.

use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;

use reel_api::column_id::ColumnId;
use reel_api::maintenance_report::JobKind;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, WritePlace};

use crate::config::MAX_KEY_BYTES;
use crate::engine::engine_state::EngineState;
use crate::engine::read_path::SealedRow;
use crate::engine::volume::Volume;
use crate::engine::walk_job::BlockCounts;
use crate::guard::hold;
use crate::io::{InPlace, Io, IoFile, ReadOn, in_place};
use crate::fixed_list::FixedList;
use crate::row_kind::RowKind;
use crate::segment::row_block::{ROW_BLOCK_BYTES, RowBlockWalk};
use crate::segment::segment_tail::SegmentNumber;
use crate::segment::summary::{DeadCount, SummaryRow};

/// What a step of the apply has left to do
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplyStep {
    /// It did a bounded piece of work
    Working,
    /// Every cover of the segment is applied
    Done,
}

/// The segment and blocks one cover is on
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplyTarget {
    pub segment: SegmentNumber,
    pub next_block: u32,
    pub end_block: u32,
    pub is_whole: bool,
}

/// The state of one segment's apply
pub struct CoverApply<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
    pub volume: usize,
    pub segment: Option<SegmentNumber>,
    position: usize,
    cover: Option<(ColumnId, WritePlace)>,
    start: FixedList<u8>,
    end: FixedList<u8>,
    last_other: SegmentNumber,
    target: Option<ApplyTarget>,
    block: FixedList<u8>,
    key: FixedList<u8>,
    lowest: FixedList<u8>,
    highest: FixedList<u8>,
    pub counts: BlockCounts,
}

impl<Backend: Io> CoverApply<Backend> {
    /// Allocate the fixed buffers
    pub fn new(state: Arc<EngineState<Backend>>, volume: usize) -> Self {
        let keys = MAX_KEY_BYTES as usize;
        Self {
            state,
            volume,
            segment: None,
            position: 0,
            cover: None,
            start: FixedList::on_heap(keys),
            end: FixedList::on_heap(keys),
            last_other: SegmentNumber(0),
            target: None,
            block: FixedList::filled_on_heap(ROW_BLOCK_BYTES, 0),
            key: FixedList::on_heap(keys),
            lowest: FixedList::on_heap(keys),
            highest: FixedList::on_heap(keys),
            counts: BlockCounts::default(),
        }
    }

    /// Read the heap the buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        let keys = self.start.capacity() + self.end.capacity() + self.key.capacity();
        let edges = self.lowest.capacity() + self.highest.capacity();
        ByteCount(self.counts.heap_bytes().0 + (self.block.capacity() + keys + edges) as u64)
    }

    /// Start on a segment's covers
    pub fn start(&mut self, segment: SegmentNumber) {
        self.segment = Some(segment);
        self.position = 0;
        self.cover = None;
        self.target = None;
    }

    /// Take one bounded step
    pub fn step(&mut self) -> Result<ApplyStep> {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        let Some(segment) = self.segment else {
            return Ok(ApplyStep::Done);
        };
        let Some((column, place)) = self.cover else {
            return Ok(self.next_cover(volume, segment));
        };
        let Some(target) = self.target else {
            self.next_target(volume, column, place)?;
            return Ok(ApplyStep::Working);
        };
        if target.next_block == target.end_block {
            self.target = None;
            return Ok(ApplyStep::Working);
        }
        self.apply_block(volume, column, place, target)?;
        let next_block = self.target.map_or(target.next_block, |target| target.next_block) + 1;
        self.target = Some(ApplyTarget { next_block, ..target });
        Ok(ApplyStep::Working)
    }

    /// Find the segment's next cover in the pool, or end the apply
    pub fn next_cover(&mut self, volume: &Volume<Backend>, segment: SegmentNumber) -> ApplyStep {
        while let Some((column, place, owner)) = volume.covers.cover_at(self.position, &mut self.start, &mut self.end) {
            self.position += 1;
            if owner == segment && volume.covers.mark_counting(place) {
                self.cover = Some((column, place));
                self.last_other = SegmentNumber(0);
                return ApplyStep::Working;
            }
        }
        self.segment = None;
        ApplyStep::Done
    }

    /// Find the next walked segment this cover can hide rows in, or end the cover
    pub fn next_target(
        &mut self,
        volume: &Volume<Backend>,
        column: ColumnId,
        place: WritePlace,
    ) -> Result<()> {
        let rows = hold(&volume.summary);
        let is_under = |row: &SummaryRow<'_>| {
            row.facts.is_walked()
                && row.facts.segment > self.last_other
                && row.facts.places.lowest < place
                && !row.facts.is_damaged
                && is_in_range(row, column, &self.start, &self.end)
        };
        let next = rows.rows().filter(is_under).min_by_key(|row| row.facts.segment).map(|row| row.facts.segment);
        drop(rows);
        let Some(other) = next else {
            volume.covers.mark_applied(place);
            self.cover = None;
            return Ok(());
        };
        self.last_other = other;
        let Some((first, count)) = volume.table.column_blocks(other, column) else {
            return Ok(());
        };
        let is_whole = self.is_wholly_under(volume, other, place, first, count)?;
        // a column partly under the cover starts at the block the range starts in
        let from = volume.table.blocks_from(other, column, &self.start).map(|(from, _end)| from);
        let start = if is_whole { first } else { from.unwrap_or(first) };
        self.target = Some(ApplyTarget {
            segment: other,
            next_block: start,
            end_block: first + count,
            is_whole,
        });
        Ok(())
    }

    /// Check a column of a segment lies wholly under the cover, from its first and last keys and its newest place
    pub fn is_wholly_under(
        &mut self,
        volume: &Volume<Backend>,
        other: SegmentNumber,
        place: WritePlace,
        first: u32,
        count: u32,
    ) -> Result<bool> {
        let Some(pin) = volume.table.pin(other) else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        if pin.head.places.highest >= place || count == 0 {
            return Ok(false);
        }
        let table = &volume.table;
        let (Some(low), Some(high)) = (table.walk_block(other, first), table.walk_block(other, first + count - 1)) else {
            return Ok(false);
        };
        let lines = [low.entry, high.entry];
        let (lowest, highest) = (&mut self.lowest, &mut self.highest);
        let base = pin.head.place_base;
        let is_read = self.state.footer_readers.with(|reader| reader.edge_keys(&pin.file, &lines, base, lowest, highest))?;
        Ok(is_read && self.lowest >= self.start && self.highest < self.end)
    }

    /// Count what the cover hides in one block
    pub fn apply_block(
        &mut self,
        volume: &Volume<Backend>,
        column: ColumnId,
        place: WritePlace,
        target: ApplyTarget,
    ) -> Result<()> {
        self.counts.clear();
        if target.is_whole {
            return self.apply_whole_block(volume, column, place, target);
        }
        let (Some(pin), Some(block)) = (volume.table.pin(target.segment), volume.table.walk_block(target.segment, target.next_block)) else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        let length = block.entry.block_length().0 as usize;
        self.counts.read_bytes = block.entry.block_length();
        if length > self.block.len() {
            return Err(Error::from(ErrorKind::InvalidData));
        }
        pin.file.read_exact_at(block.entry.block_at(), &mut self.block[..length])?;
        let Some(mut walk) = RowBlockWalk::open(&self.block[..length], pin.head.place_base) else {
            return Err(Error::from(ErrorKind::InvalidData));
        };
        while let Some(row) = walk.next_row(&mut self.key) {
            if self.key >= self.end {
                self.target = Some(ApplyTarget { next_block: target.end_block - 1, ..target });
                break;
            }
            let is_inside = self.key >= self.start && self.key < self.end;
            if !is_inside || row.place >= place || row.kind == RowKind::RangeDelete {
                continue;
            }
            // damage can hide the newer row that already counted this one, and a count left out only understates
            if volume.damage.covering(column, &self.key).is_some() {
                continue;
            }
            // another cover already counted this row, or a newer row did
            if volume.covers.newest_applied(column, &self.key).is_some_and(|applied| applied > row.place) {
                continue;
            }
            let newest = match self.newest_row(volume, column) {
                Ok(newest) => newest,
                Err(StoreError::Corruption { .. }) => None,
                Err(_error) => return Err(Error::from(ErrorKind::Interrupted)),
            };
            if newest.is_some_and(|found| found.segment == target.segment && found.row.place == row.place) {
                self.counts.count_dead(target.segment, column, target.next_block, &row, place)?;
            }
        }
        self.land(volume, target.segment)
    }

    /// Find a key's newest row among the walked segments and the one this walk is ending. That walk has already counted what its
    /// own rows replace, above the cover or below it, so a row only counts here while it stands on top of those too
    pub fn newest_row(
        &self,
        volume: &Volume<Backend>,
        column: ColumnId,
    ) -> std::result::Result<Option<SealedRow>, StoreError> {
        let state = &self.state;
        let walked = state.newest_walked(volume, column, &self.key, &[])?;
        let Some(segment) = self.segment else {
            return Ok(walked);
        };
        let read = state.key_read(column, &self.key, WritePlace(u64::MAX), true)?;
        let own = state.footer_readers.with(|reader| {
            let mut best = None;
            if let Some(pin) = volume.table.pin(segment) {
                in_place(state.ask_rows(ReadOn::new(&InPlace, volume), &read, segment, &pin, reader, &mut best))?;
            }
            Ok::<_, StoreError>(best)
        })?;
        Ok(match (walked, own) {
            (Some(walked), Some(own)) if own.row.place > walked.row.place => Some(own),
            (None, own) => own,
            (walked, _own) => walked,
        })
    }

    /// Count a block of a column wholly under the cover from what its counts still call live. The first block also takes the
    /// column's live bytes and graves
    pub fn apply_whole_block(
        &mut self,
        volume: &Volume<Backend>,
        column: ColumnId,
        place: WritePlace,
        target: ApplyTarget,
    ) -> Result<()> {
        let Some(block) = volume.table.walk_block(target.segment, target.next_block) else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        self.counts.read_bytes = block.entry.block_length();
        let (mut graves, mut bytes) = (0, ByteCount(0));
        if volume.table.column_blocks(target.segment, column).is_some_and(|(first, _count)| first == target.next_block) {
            (graves, bytes) = column_left(&hold(&volume.summary), target.segment, column);
        }
        self.counts.counts.push(DeadCount {
            segment: target.segment,
            column,
            block: target.next_block,
            rows: block.entry.live_rows,
            graves,
            bytes,
            kept_bytes: ByteCount(0),
            replacement: place,
        })?;
        self.land(volume, target.segment)
    }

    /// Land a block's counts on its segment
    pub fn land(&mut self, volume: &Volume<Backend>, segment: SegmentNumber) -> Result<()> {
        self.state.note_moved(JobKind::RangeDeleteApply, self.counts.read_bytes);
        self.counts.counts.sort_unstable_by_key(|count| count.block);
        hold(&volume.summary).apply_counts(segment, &self.counts.counts, None);
        for count in self.counts.counts.iter().filter(|count| count.rows > 0) {
            volume.table.drop_live(count.segment, count.block, count.rows, count.replacement);
        }
        for free in &self.counts.frees {
            let _is_done = self.state.free_record(volume, free);
        }
        Ok(())
    }
}

/// Read a column's graves and bytes a segment's counts still call live
pub fn column_left(rows: &crate::segment::summary::SummaryRows, segment: SegmentNumber, column: ColumnId) -> (u32, ByteCount) {
    let Some(row) = rows.get(segment) else {
        return (0, ByteCount(0));
    };
    let mut cursor = row.cursor();
    for _column in 0..row.column_count {
        let Some(facts) = cursor.column() else {
            break;
        };
        if facts.column == column {
            return ((facts.grave_count.0 - facts.dead_graves.0) as u32, facts.live_bytes);
        }
    }
    (0, ByteCount(0))
}

/// Check a summary row's range for a column meets a cover's. A summary keeps only a key's first bytes, so this can only be wider
/// than the truth
pub fn is_in_range(row: &SummaryRow<'_>, column: ColumnId, start: &[u8], end: &[u8]) -> bool {
    let mut cursor = row.cursor();
    for _column in 0..row.column_count {
        let Some(facts) = cursor.column() else {
            return false;
        };
        if facts.column == column {
            let is_before = !facts.bound.is_empty() && facts.bound <= start;
            return !is_before && facts.lowest < end;
        }
    }
    false
}
