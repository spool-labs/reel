//! Rebuilding a damaged segment from its row blocks: each block's header needs no index, so the walk from the data end finds
//! them all. What it finds goes into the table for the merge path to copy into a new segment, places kept, and a block that
//! fails its check leaves the span between its neighbours' keys damaged
//!
//! The segment never joins a directory, so no read goes to it. Its output does, and the spans go when that's listed.

use std::io::Result;
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::units::{FileOffset, RowCount, WritePlace};

use crate::engine::compact_job::CompactJob;
use crate::engine::open_segment::data_path;
use crate::engine::recovery::root_of_path;
use crate::engine::volume::Volume;
use crate::guard::hold;
use crate::io::device_error::read_or_damaged;
use crate::io::{Io, IoFile};
use crate::record::record_check::checksum;
use crate::row_kind::RowKind;
use crate::segment::block_entry::{BlockEntry, KeyPrefix, SharedHead};
use crate::segment::damage_list::DamagedRange;
use crate::segment::row_block::{BLOCK_HEADER_BYTES, BLOCK_MARKER, ROW_BLOCK_BYTES, RowBlockWalk, read_block_header};
use crate::segment::segment_tail::{BlockCount, ColumnTail, PlaceRange, SegmentNumber, TailHead};
use crate::segment::summary::SegmentFacts;

/// Most damaged segments a run gives up on before it stops looking
pub const MOST_REBUILD_TRIES: usize = 64;
/// Bytes a walk reads at a time looking for the next header past a bad block
pub const HEADER_LOOK_BYTES: usize = 64 * 1024;

/// A key at one edge of a lost block
#[derive(Clone, Debug, Default)]
pub struct BlockEdge {
    pub column: Option<ColumnId>,
    pub key: Vec<u8>,
}

/// What a walk of a segment's row blocks found
#[derive(Clone, Debug, Default)]
pub struct BlockFinds {
    pub columns: Vec<ColumnTail>,
    pub lines: Vec<BlockEntry>,
    pub lost: Vec<DamagedRange>,
    pub place_base: WritePlace,
}

impl<Backend: Io> CompactJob<Backend> {
    /// Start on the next damaged segment that has row blocks to rebuild from, and say whether one began
    pub fn start_rebuild(&mut self, volume: &Volume<Backend>) -> bool {
        if volume.damage.is_empty() || volume.is_rebuild_held.load(Ordering::Acquire) {
            return false;
        }
        let next = hold(&volume.damaged).iter().copied().find(|number| !self.rebuilds_tried.contains(number));
        let Some(number) = next else {
            return false;
        };
        if self.rebuilds_tried.len() < MOST_REBUILD_TRIES {
            self.rebuilds_tried.push(number);
        }
        let facts = hold(&volume.summary).get(number).map(|row| row.facts);
        let Some(facts) = facts.filter(|facts| facts.row_block_end > facts.data_end) else {
            return false;
        };
        self.rebuild_segment(volume, number, &facts).unwrap_or(false)
    }

    /// Walk a segment's blocks, put what passes in the table, and hand it to the merge path
    pub fn rebuild_segment(&mut self, volume: &Volume<Backend>, number: SegmentNumber, facts: &SegmentFacts) -> Result<bool> {
        let state = &self.state;
        let path = volume.roots.iter().map(|root| data_path(root, number)).find_map(|path| {
            state.io.open_read_only(&path).ok().map(|file| (path, file))
        });
        let Some((path, file)) = path else {
            return Ok(false);
        };
        let mut finds = BlockFinds::default();
        let columns = row_columns(volume, number);
        let walked = walk_blocks(&file, facts, &columns, &mut finds);
        let highest = facts.damage_place.max(facts.places.highest);
        if walked.is_err() || finds.lines.is_empty() {
            state.io.close_file(file);
            return Ok(false);
        }
        for range in &mut finds.lost {
            range.segment = number;
            range.highest_place = highest;
        }
        let head = TailHead {
            segment: number,
            data_end: facts.data_end,
            index_at: facts.row_block_end,
            second_index_at: facts.row_block_end,
            block_count: BlockCount(finds.lines.len() as u32),
            places: PlaceRange { lowest: facts.places.lowest, highest: facts.places.highest },
            place_base: finds.place_base,
            column_count: finds.columns.len() as u32,
            ..TailHead::default()
        };
        let root = root_of_path(volume, &path);
        if !volume.table.install(file.clone(), root, &head, &finds.columns, &finds.lines) {
            state.io.close_file(file);
            return Ok(false);
        }
        self.rewrite.rebuild = Some(number);
        self.rewrite.rebuild_blocks = finds.lines.len() as u32;
        self.rewrite.lost_spans = finds.lost;
        Ok(self.rewrite.start())
    }
}

/// List the columns a segment's summary row names, in order
pub fn row_columns<Backend: Io>(volume: &Volume<Backend>, segment: SegmentNumber) -> Vec<ColumnId> {
    let rows = hold(&volume.summary);
    let mut columns = Vec::new();
    if let Some(row) = rows.get(segment) {
        let mut cursor = row.cursor();
        for _column in 0..row.column_count {
            if let Some(facts) = cursor.column() {
                columns.push(facts.column);
            }
        }
    }
    columns.sort_unstable();
    columns
}

/// Walk a segment's row blocks from the data end, stepping over what fails its check. A run of lost blocks can hold any column
/// between the blocks around it, so `columns` says what it could reach
pub fn walk_blocks<File: IoFile>(file: &File, facts: &SegmentFacts, columns: &[ColumnId], finds: &mut BlockFinds) -> Result<()> {
    let end = facts.row_block_end.0;
    let mut at = facts.data_end.0;
    let (mut block, mut key, mut first) = (vec![0; ROW_BLOCK_BYTES], Vec::new(), Vec::new());
    let mut before = BlockEdge::default();
    let mut has_gap = false;
    while at + BLOCK_HEADER_BYTES as u64 <= end {
        match read_header_block(file, at, end, &mut block, &mut key, &mut first, finds)? {
            Some(next) => {
                let column = finds.columns.last().map(|tail| tail.column);
                if has_gap {
                    note_gap(&before, &BlockEdge { column, key: first.clone() }, columns, finds);
                    has_gap = false;
                }
                before.column = column;
                before.key.clone_from(&key);
                at = next;
            },
            None => {
                has_gap = true;
                let Some(next) = next_header(file, at + 1, end)? else {
                    break;
                };
                at = next;
            },
        }
    }
    if has_gap {
        note_gap(&before, &BlockEdge::default(), columns, finds);
    }
    Ok(())
}

/// Read the block at an offset and take it in, and return where the next starts, or nothing when it fails its check. Its first
/// and last keys are left in the buffers
pub fn read_header_block<File: IoFile>(
    file: &File,
    at: u64,
    end: u64,
    block: &mut [u8],
    key: &mut Vec<u8>,
    first_row: &mut Vec<u8>,
    finds: &mut BlockFinds,
) -> Result<Option<u64>> {
    let mut bytes = [0; BLOCK_HEADER_BYTES];
    if !read_or_damaged(file, FileOffset(at), &mut bytes)? {
        return Ok(None);
    }
    let Some(header) = read_block_header(&bytes) else {
        return Ok(None);
    };
    let block_at = at + BLOCK_HEADER_BYTES as u64;
    let length = header.block_length as usize;
    let filter_at = block_at + length as u64;
    let next = filter_at + u64::from(header.filter_length);
    if length > block.len() || next > end || !read_or_damaged(file, FileOffset(block_at), &mut block[..length])? {
        return Ok(None);
    }
    let Some(mut walk) = RowBlockWalk::open(&block[..length], header.place_base) else {
        return Ok(None);
    };

    key.clear();
    let (mut first, mut rows, mut graves) = (None, 0, 0);
    while let Some(row) = walk.next_row(key) {
        if first.is_none() {
            first_row.clone_from(key);
            first = Some(KeyPrefix::of_key(key, 0));
        }
        rows += 1;
        graves += u64::from(row.kind == RowKind::Grave);
    }
    let Some(first_key) = first else {
        return Ok(None);
    };
    finds.place_base = header.place_base;
    let line = finds.lines.len() as u32;
    finds.lines.push(BlockEntry {
        first_key,
        block_offset: block_at as u32,
        filter_offset: filter_at as u32,
        live_rows: rows as u32,
    });
    let highest = KeyPrefix::of_key(key, 0);
    match finds.columns.last_mut().filter(|column| column.column == header.column) {
        Some(column) => {
            column.highest = highest;
            column.block_count = BlockCount(column.block_count.0 + 1);
            column.row_count = RowCount(column.row_count.0 + rows);
            column.grave_count = RowCount(column.grave_count.0 + graves);
        },
        None => finds.columns.push(ColumnTail {
            column: header.column,
            shared_length: 0,
            shared_sum: checksum(&[]),
            shared_head: SharedHead::default(),
            lowest: first_key,
            highest,
            first_block: line,
            block_count: BlockCount(1),
            row_count: RowCount(rows),
            grave_count: RowCount(graves),
        }),
    }
    Ok(Some(next))
}

/// Find the next header past a bad block that passes its check
pub fn next_header<File: IoFile>(file: &File, from: u64, end: u64) -> Result<Option<u64>> {
    let marker = BLOCK_MARKER.to_le_bytes();
    let mut bytes = vec![0; HEADER_LOOK_BYTES];
    let mut at = from;
    while at + BLOCK_HEADER_BYTES as u64 <= end {
        let length = (end - at).min(HEADER_LOOK_BYTES as u64) as usize;
        if !read_or_damaged(file, FileOffset(at), &mut bytes[..length])? {
            at += length as u64;
            continue;
        }
        for offset in 0..length.saturating_sub(BLOCK_HEADER_BYTES - 1) {
            if bytes[offset..offset + marker.len()] == marker && read_block_header(&bytes[offset..]).is_some() {
                return Ok(Some(at + offset as u64));
            }
        }
        // a header that starts near the end of this window comes whole in the next
        at += (length - length.min(BLOCK_HEADER_BYTES - 1)).max(1) as u64;
    }
    Ok(None)
}

/// Note a run of lost blocks. Columns run in order through a segment, so it reaches every column between the blocks around it:
/// from the last good key in the one before, up to the first good key in the one after, and all of any between
pub fn note_gap(before: &BlockEdge, after: &BlockEdge, columns: &[ColumnId], finds: &mut BlockFinds) {
    let is_reached = |column: ColumnId| {
        before.column.is_none_or(|first| column >= first) && after.column.is_none_or(|last| column <= last)
    };
    for column in columns.iter().copied().filter(|column| is_reached(*column)) {
        let start = if before.column == Some(column) { before.key.clone() } else { Vec::new() };
        let end = (after.column == Some(column)).then(|| after.key.clone());
        finds.lost.push(DamagedRange {
            column,
            segment: SegmentNumber(0),
            highest_place: WritePlace(0),
            start,
            end,
        });
    }
}
