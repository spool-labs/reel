//! A volume's summary: one row per sealed segment, which outlives that segment's tail

use std::collections::BTreeMap;
use std::hint::black_box;
use std::io::{ErrorKind, IoSlice, Result};
use std::path::{Path, PathBuf};

use reel_core::column_id::ColumnId;
use reel_core::units::{ByteCount, DeathMark, FileOffset, RowCount, WritePlace};

use crate::fixed_list::{ListFull, ListOut};
use crate::segment::summary_bytes::SummaryBytes;
use crate::io::replace::replace_whole;
use crate::io::device_error::{SECTOR_BYTES, read_or_damaged};
use crate::io::{Io, IoFile};
use crate::lifetime::MarkRange;
use crate::little_endian::{ByteReader, read_half};
use crate::record::record_check::{Checksum, checksum};
use crate::segment::segment_table::NumberMap;
use crate::segment::segment_tail::{PlaceRange, SegmentNumber};
use crate::segment::summary_orders::SummaryOrders;

/// The bit of a row's flags that says its footer keeps range deletes
pub const HAS_COVERS_BIT: u8 = 1;
/// The bit of a row's flags that says its counts come from a backward walk
pub const IS_BACKWARD_BIT: u8 = 2;
/// The bit of a row's flags that says its segment lost rows its blocks never held
pub const HAS_LOST_ROWS_BIT: u8 = 3;
/// Opens a summary file
pub const SUMMARY_MAGIC: u64 = 0x5245_4C53_554D_4D31;
/// Closes a summary file
pub const SUMMARY_TRAILER_MAGIC: u64 = 0x5245_4C53_554D_4D32;
/// The summary's file name
pub const SUMMARY_NAME: &str = "summary";
/// The name a new summary is written under before it replaces the old one
pub const NEW_SUMMARY_NAME: &str = "summary.new";
/// The second copy's file name
pub const SECOND_SUMMARY_NAME: &str = "summary.second";
/// Bytes of a summary file's header
pub const SUMMARY_HEADER_BYTES: usize = 40;
/// Where a summary end's checksum sits
pub const END_SUM_AT: usize = SUMMARY_HEADER_BYTES - size_of::<u64>();
/// The number a summary with no whole end reads back, so every rebuilt segment takes the backward walk
pub const FORWARD_UNKNOWN: SegmentNumber = SegmentNumber(u64::MAX);
/// Opens every row, so a walk past a bad row can find the next one
pub const ROW_MARKER: u32 = 0x5253_554D;
/// Bytes of a row's head: its marker, its body's length and the head's own checksum
pub const ROW_HEAD_BYTES: usize = 3 * size_of::<u32>();
/// Bytes of a row's checksum
pub const ROW_SUM_BYTES: usize = size_of::<u64>();
/// Bytes of the fields every row has, before its columns
pub const ROW_FACT_BYTES: usize = 97;
/// Where a row's body keeps its span count
/// Where a row's body keeps its flags
pub const ROW_FLAGS_AT: usize = 56;
pub const SPAN_COUNT_AT: usize = 59;
/// Where a row's body keeps how far the dead-byte walk has come
pub const WALKED_BLOCKS_AT: usize = 73;
/// Most bytes of a key a row keeps, so a column costs about 100 B whatever the keys are
pub const SUMMARY_KEY_BYTES: usize = 32;
/// Bytes a row takes before its columns and spans
pub const MOST_ROW_FIXED_BYTES: usize = ROW_HEAD_BYTES + ROW_FACT_BYTES + ROW_SUM_BYTES;
/// Bytes a span adds to a row, before its keys
pub const MOST_ROW_SPAN_FIXED_BYTES: usize = 1 + 2 * size_of::<u16>();
/// The most bytes a column adds to a row, with its bounded keys
pub const MOST_ROW_COLUMN_BYTES: usize = COLUMN_FIXED_BYTES + 2 * SUMMARY_KEY_BYTES + 1;
/// Words of counts each column keeps in a row
pub const COLUMN_WORDS: usize = 6;
/// Bytes of a column in a row, before its keys
pub const COLUMN_FIXED_BYTES: usize = 1 + 2 * size_of::<u16>() + COLUMN_WORDS * size_of::<u64>();
/// Where a column keeps the length of its lowest key
pub const COLUMN_LOWEST_AT: usize = 1;
/// Where a column keeps the length of its bound
pub const COLUMN_BOUND_AT: usize = 3;
/// Where a column keeps its dead rows, with dead graves, live bytes and dead bytes after it
pub const COLUMN_DEAD_AT: usize = 21;
/// Marks a span that runs to its column's end
pub const NO_END: u16 = u16::MAX;

/// A segment's facts, apart from its columns
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SegmentFacts {
    pub segment: SegmentNumber,
    pub file_length: ByteCount,
    pub data_end: FileOffset,
    pub row_block_end: FileOffset,
    pub seal_mark: Checksum,
    pub places: PlaceRange,
    pub is_damaged: bool,
    pub has_covers: bool,
    pub damage_place: WritePlace,
    pub block_count: u32,
    pub walked_blocks: u32,
    pub is_backward: bool,
    pub has_lost_rows: bool,
    pub marks: MarkRange,
    pub most_block_rows: u32,
}

impl SegmentFacts {
    /// Check the dead-byte walk has finished this segment, so other walks may count against it. A merge output
    /// starts finished, and its counts come from the rows it keeps, never from its inputs' numbers, since a rewrite
    /// can drop a row an unfinished walk would have flipped later
    pub fn is_walked(&self) -> bool {
        self.walked_blocks >= self.block_count
    }
}

/// One column's range and counts in a segment
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnFacts<'key> {
    pub column: ColumnId,
    pub lowest: &'key [u8],
    pub bound: &'key [u8],
    pub row_count: RowCount,
    pub grave_count: RowCount,
    pub dead_rows: RowCount,
    pub dead_graves: RowCount,
    pub live_bytes: ByteCount,
    pub dead_bytes: ByteCount,
}

/// One walk step's dead rows in one row block of one segment. A row keeps dead rows a block, which start at zero, and a
/// block's live rows are what its line says less these
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DeadCount {
    pub segment: SegmentNumber,
    pub column: ColumnId,
    pub block: u32,
    pub rows: u32,
    pub graves: u32,
    pub bytes: ByteCount,
    pub kept_bytes: ByteCount,
    pub replacement: WritePlace,
}

/// One damaged span of a column, to its end key or the column's end
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SpanFacts<'key> {
    pub column: ColumnId,
    pub start: &'key [u8],
    pub end: Option<&'key [u8]>,
}

/// Every row of a volume's summary, stored as they go to disk
#[derive(Debug)]
pub struct SummaryRows {
    pub highest_forward: SegmentNumber,
    bytes: SummaryBytes,
    index: NumberMap,
    listed: usize,
    is_index_whole: bool,
    footer_bytes: u64,
    file_bytes: u64,
    orders: SummaryOrders,
    is_checked: bool,
    /// What both copies on disk hold when open found them alike, so a save of the same rows can leave them
    pub on_disk: Option<SavedSummary>,
}

/// The rows and forward number a summary save wrote to both copies
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SavedSummary {
    pub sum: Checksum,
    pub length: u64,
    pub forward: SegmentNumber,
}

impl SavedSummary {
    /// Sum the rows a save writes
    pub fn of(rows: &[u8], forward: SegmentNumber) -> Self {
        Self { sum: checksum(rows), length: rows.len() as u64, forward }
    }
}

/// One row read back
#[derive(Clone, Copy, Debug)]
pub struct SummaryRow<'bytes> {
    pub facts: SegmentFacts,
    pub block_dead: &'bytes [u8],
    pub column_count: u16,
    pub span_count: u16,
    pub body: &'bytes [u8],
}


impl<'bytes> ByteReader<'bytes> {
    /// Read the next column
    pub fn column(&mut self) -> Option<ColumnFacts<'bytes>> {
        let column = ColumnId(self.byte()?);
        let lowest_length = self.short()? as usize;
        let bound_length = self.short()? as usize;
        let row_count = RowCount(self.word()?);
        let grave_count = RowCount(self.word()?);
        let dead_rows = RowCount(self.word()?);
        let dead_graves = RowCount(self.word()?);
        let live_bytes = ByteCount(self.word()?);
        let dead_bytes = ByteCount(self.word()?);
        Some(ColumnFacts {
            column,
            lowest: self.take(lowest_length)?,
            bound: self.take(bound_length)?,
            row_count,
            grave_count,
            dead_rows,
            dead_graves,
            live_bytes,
            dead_bytes,
        })
    }

    /// Read the next span
    pub fn span(&mut self) -> Option<SpanFacts<'bytes>> {
        let column = ColumnId(self.byte()?);
        let start_length = self.short()? as usize;
        let end_length = self.short()?;
        let start = self.take(start_length)?;
        let end = if end_length == NO_END {
            None
        } else {
            Some(self.take(end_length as usize)?)
        };
        Some(SpanFacts { column, start, end })
    }
}

impl<'bytes> SummaryRow<'bytes> {
    /// Walk the blocks with dead rows, as each block's index and its count
    pub fn dead_blocks(&self) -> impl Iterator<Item = (u32, u32)> + 'bytes {
        let counts = self.block_dead.chunks_exact(size_of::<u32>());
        let dead = counts.map(|bytes| u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]));
        dead.enumerate().filter(|(_block, dead)| *dead > 0).map(|(block, dead)| (block as u32, dead))
    }

    /// Walk the row's columns, then its spans
    pub fn cursor(&self) -> ByteReader<'bytes> {
        ByteReader::new(self.body)
    }
}

impl SummaryRows {
    /// Allocate room for the rows
    pub fn new(capacity: ByteCount) -> Self {
        Self {
            highest_forward: SegmentNumber(0),
            bytes: SummaryBytes::heap(capacity.0 as usize),
            index: NumberMap::new(0),
            listed: 0,
            is_index_whole: false,
            footer_bytes: 0,
            file_bytes: 0,
            orders: SummaryOrders::new(0),
            is_checked: true,
            on_disk: None,
        }
    }

    /// Grow the room rows can reach, which open does once and nothing does after
    pub fn make_room(&mut self, capacity: ByteCount) {
        let wanted = capacity.0 as usize;
        let length = self.bytes.len();
        self.bytes.reserve_exact(wanted.saturating_sub(length));
        self.bytes.resize(wanted.max(length), 0);
        black_box(&mut *self.bytes);
        self.bytes.truncate(length);
    }

    /// Move the rows into a room planned for this many bytes, which a volume's own summary does once at open, so their memory
    /// follows them from then on
    pub fn move_to_room(&mut self, capacity: ByteCount) -> std::io::Result<()> {
        let rows = std::mem::replace(&mut self.bytes, SummaryBytes::heap(0));
        self.bytes = SummaryBytes::in_room(capacity.0 as usize, &rows)?;
        Ok(())
    }

    /// Take what a settled copy holds, keeping these rows' own memory, so a summary that moved into a room stays in it
    pub fn settle_from(&mut self, settled: SummaryRows) {
        let mut bytes = std::mem::replace(&mut self.bytes, SummaryBytes::heap(0));
        bytes.clear();
        bytes.extend_from_slice(&settled.bytes);
        *self = SummaryRows { bytes, ..settled };
    }


    /// Read the room the rows live in, once they moved to one
    pub fn room(&self) -> Option<&std::sync::Arc<crate::pool_room::PoolRoom>> {
        self.bytes.room()
    }

    /// Size the lookup from a segment to its row and the orders the picks ask, so a walk step and a pick go to their rows with no
    /// walk over them all
    pub fn make_index(&mut self, segments: usize) {
        self.index = NumberMap::new(segments);
        self.orders = SummaryOrders::new(segments);
        // rows from a file pass their checks here once, and after that every row is checked as it changes
        self.is_checked = self.is_checked || !self.has_bad_row();
        self.rebuild_index();
        let mut orders = std::mem::replace(&mut self.orders, SummaryOrders::new(0));
        for row in self.rows() {
            orders.place(&row);
        }
        self.orders = orders;
    }

    /// Read the orders the picks ask
    pub fn orders(&self) -> &SummaryOrders {
        &self.orders
    }

    /// Note dead bytes the walk gave back in place in a segment, which no rewrite needs to win back
    pub fn note_freed(&mut self, segment: SegmentNumber, bytes: u64) {
        self.orders.note_freed(segment, bytes);
    }

    /// Move a row to the end of its share list after a pick, so the next pick there starts with another
    pub fn rotate(&mut self, segment: SegmentNumber) {
        self.orders.rotate(segment);
    }

    /// Put the row at an offset on the lists its counts call for
    fn place_at(&mut self, at: usize) {
        if let Some((_length, Some(row))) = read_row_at(&self.bytes, at, self.is_checked) {
            self.orders.place(&row);
        }
    }

    /// Read the bytes the rows have room for
    pub fn byte_room(&self) -> ByteCount {
        ByteCount(self.bytes.capacity() as u64)
    }

    /// Read how many segments the lookup was sized for
    pub fn index_room(&self) -> usize {
        self.index.room()
    }

    /// List every row again after rows moved. A lookup too small for them all gives way to walking the rows
    pub fn rebuild_index(&mut self) {
        self.index.clear();
        self.listed = 0;
        self.is_index_whole = true;
        let mut at = 0;
        while let Some((length, row)) = self.read_at(at) {
            if let Some(number) = row.map(|row| row.facts.segment) {
                self.note_row(number, at);
            }
            at += length;
        }
    }

    /// List a row at the end, or give the lookup up when it has no room
    pub fn note_row(&mut self, number: SegmentNumber, at: usize) {
        if self.listed >= self.index.room() {
            self.is_index_whole = false;
            return;
        }
        self.listed += 1;
        self.index.insert(number, at as u32);
    }

    /// Check every row is in the lookup: once it gives way, each walk step reads the rows again
    pub fn is_lookup_whole(&self) -> bool {
        self.is_index_whole
    }

    /// Find where a segment's row starts
    pub fn find_row(&self, segment: SegmentNumber) -> Option<usize> {
        if self.is_index_whole {
            return self.index.get(segment).map(|at| at as usize);
        }
        let mut at = 0;
        while let Some((length, row)) = self.read_at(at) {
            if row.is_some_and(|row| row.facts.segment == segment) {
                return Some(at);
            }
            at += length;
        }
        None
    }

    /// Read how far the row at an offset reaches and the row, with no checksum once every row is checked
    fn read_at(&self, at: usize) -> Option<(usize, Option<SummaryRow<'_>>)> {
        read_row_at(&self.bytes, at, self.is_checked)
    }

    /// Read the stored rows
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Read the heap the rows hold
    pub fn heap_bytes(&self) -> ByteCount {
        ByteCount((self.bytes.heap_bytes() + self.index.heap_bytes()) as u64 + self.orders.heap_bytes().0)
    }

    /// Add a row, or return false when it doesn't fit
    pub fn push<'key>(
        &mut self,
        facts: &SegmentFacts,
        columns: impl Iterator<Item = ColumnFacts<'key>> + Clone,
        spans: impl Iterator<Item = SpanFacts<'key>> + Clone,
    ) -> bool {
        // a row keeps its counts in two bytes, and the pools that feed it stay well under that
        let column_count = columns.clone().count();
        let span_count = spans.clone().count();
        debug_assert!(span_count < usize::from(NO_END) && column_count < usize::from(NO_END));
        let body = row_bytes(facts.block_count, columns.clone(), spans.clone());
        if self.bytes.len() + ROW_HEAD_BYTES + body + ROW_SUM_BYTES > self.bytes.capacity() {
            return false;
        }

        let start = self.bytes.len();
        self.bytes.extend_from_slice(&row_head(body as u32));
        let body_at = self.bytes.len();
        self.put_facts(facts, column_count as u16, span_count as u16);
        self.bytes.resize(self.bytes.len() + facts.block_count as usize * size_of::<u32>(), 0);
        for column in columns {
            self.bytes.push(column.column.0);
            self.bytes.extend_from_slice(&(column.lowest.len() as u16).to_le_bytes());
            self.bytes.extend_from_slice(&(column.bound.len() as u16).to_le_bytes());
            self.bytes.extend_from_slice(&column.row_count.0.to_le_bytes());
            self.bytes.extend_from_slice(&column.grave_count.0.to_le_bytes());
            self.bytes.extend_from_slice(&column.dead_rows.0.to_le_bytes());
            self.bytes.extend_from_slice(&column.dead_graves.0.to_le_bytes());
            self.bytes.extend_from_slice(&column.live_bytes.0.to_le_bytes());
            self.bytes.extend_from_slice(&column.dead_bytes.0.to_le_bytes());
            self.bytes.extend_from_slice(column.lowest);
            self.bytes.extend_from_slice(column.bound);
        }
        self.put_spans(spans);

        let sum = checksum(&self.bytes[body_at..]);
        self.bytes.extend_from_slice(&sum.0.to_le_bytes());
        debug_assert_eq!(self.bytes.len() - start, ROW_HEAD_BYTES + body + ROW_SUM_BYTES);
        self.note_row(facts.segment, start);
        self.place_at(start);
        true
    }

    /// Add a row that must stay, giving up rows with no spans for it, and growing past the plan when there
    /// are none left, since losing spans would let an older version answer: return how many rows went
    pub fn push_kept<'key>(
        &mut self,
        facts: &SegmentFacts,
        columns: impl Iterator<Item = ColumnFacts<'key>> + Clone,
        spans: impl Iterator<Item = SpanFacts<'key>> + Clone,
    ) -> u64 {
        let mut dropped = 0;
        while !self.push(facts, columns.clone(), spans.clone()) {
            if self.drop_clean_row() {
                dropped += 1;
            } else {
                // reserve counts from the length, so this asks past what's there, however little that is
                self.bytes.reserve(self.bytes.capacity() + MOST_ROW_FIXED_BYTES + row_bytes(facts.block_count, columns.clone(), spans.clone()));
            }
        }
        dropped
    }

    /// Take out the first row with no damage on it, and return false when there is none
    pub fn drop_clean_row(&mut self) -> bool {
        let clean = self.rows().find(|row| !row.facts.is_damaged).map(|row| row.facts.segment);
        let Some(segment) = clean else {
            return false;
        };
        self.remove(segment);
        true
    }

    /// Write a row's spans
    pub fn put_spans<'key>(&mut self, spans: impl Iterator<Item = SpanFacts<'key>>) {
        for span in spans {
            self.bytes.push(span.column.0);
            self.bytes.extend_from_slice(&(span.start.len() as u16).to_le_bytes());
            let end_length = span.end.map_or(NO_END, |end| end.len() as u16);
            self.bytes.extend_from_slice(&end_length.to_le_bytes());
            self.bytes.extend_from_slice(span.start);
            if let Some(end) = span.end {
                self.bytes.extend_from_slice(end);
            }
        }
    }

    /// Replace a row's spans in place, moving what follows it, and return false when the segment has no row.
    /// A row that outgrows the room gives up rows with no spans first
    pub fn rewrite_spans<'key>(
        &mut self,
        segment: SegmentNumber,
        spans: impl Iterator<Item = SpanFacts<'key>> + Clone,
    ) -> Option<u64> {
        let mut new_bytes = 0;
        for span in spans.clone() {
            new_bytes += 1 + 2 * size_of::<u16>() + span.start.len() + span.end.map_or(0, <[u8]>::len);
        }

        let mut dropped = 0;
        loop {
            let (at, length, spans_at) = self.find_spans(segment)?;
            let body_at = at + ROW_HEAD_BYTES;
            let body_end = at + length - ROW_SUM_BYTES;
            let new_end = spans_at + new_bytes + ROW_SUM_BYTES;
            let new_length = self.bytes.len() + new_end - (at + length);
            if new_length > self.bytes.capacity() {
                if self.drop_clean_row() {
                    dropped += 1;
                    continue;
                }
                self.bytes.reserve(new_length - self.bytes.len());
            }

            let old_length = self.bytes.len();
            if new_length > old_length {
                self.bytes.resize(new_length, 0);
            }
            self.bytes.copy_within(body_end + ROW_SUM_BYTES..old_length, new_end);
            self.bytes.truncate(new_length);

            let mut span_at = spans_at;
            for span in spans.clone() {
                span_at += write_span(&mut self.bytes[span_at..], &span);
            }

            let span_count = spans.clone().count() as u16;
            self.bytes[body_at + SPAN_COUNT_AT..body_at + SPAN_COUNT_AT + 2].copy_from_slice(&span_count.to_le_bytes());
            let body_length = new_end - ROW_SUM_BYTES - body_at;
            self.bytes[at..body_at].copy_from_slice(&row_head(body_length as u32));
            let sum = checksum(&self.bytes[body_at..body_at + body_length]);
            self.bytes[body_at + body_length..new_end].copy_from_slice(&sum.0.to_le_bytes());
            self.rebuild_index();
            return Some(dropped);
        }
    }

    /// Find a segment's row, its length and where its spans start
    pub fn find_spans(&self, segment: SegmentNumber) -> Option<(usize, usize, usize)> {
        let mut at = 0;
        while let Some((length, row)) = read_row(&self.bytes[at..]) {
            if let Some(row) = row.filter(|row| row.facts.segment == segment) {
                let mut cursor = row.cursor();
                for _column in 0..row.column_count {
                    cursor.column()?;
                }
                let spans_length = cursor.rest().len();
                return Some((at, length, at + length - ROW_SUM_BYTES - spans_length));
            }
            at += length;
        }
        None
    }

    /// Note a forward walk's counts have landed, so any save that holds them holds this number too
    pub fn note_forward(&mut self, walked: SegmentNumber) {
        self.highest_forward = self.highest_forward.max(walked);
    }

    /// Apply one step of the dead-byte walk under one hold: every count, and how far the walk has come, so any saved
    /// copy has counts and a position that agree. The counts come sorted by segment
    pub fn apply_walk(&mut self, walked: SegmentNumber, blocks: u32, counts: &[DeadCount]) {
        let mut first = 0;
        while let Some(count) = counts.get(first) {
            let length = counts[first..].partition_point(|other| other.segment == count.segment);
            let position = (count.segment == walked).then_some(blocks);
            self.apply_counts(count.segment, &counts[first..first + length], position);
            first += length;
        }
        if counts.binary_search_by_key(&walked, |count| count.segment).is_err() {
            self.apply_counts(walked, &[], Some(blocks));
        }
    }

    /// Note a segment's covers as settled, so open leaves them out and the segment goes for its own reasons
    pub fn clear_covers(&mut self, segment: SegmentNumber) {
        let Some(at) = self.find_row(segment) else {
            return;
        };
        let Some((length, Some(_row))) = read_row(&self.bytes[at..]) else {
            return;
        };
        let body_at = at + ROW_HEAD_BYTES;
        self.bytes[body_at + ROW_FLAGS_AT] &= !(1 << HAS_COVERS_BIT);
        let body_end = at + length - ROW_SUM_BYTES;
        let sum = checksum(&self.bytes[body_at..body_end]);
        self.bytes[body_end..body_end + ROW_SUM_BYTES].copy_from_slice(&sum.0.to_le_bytes());
        self.place_at(at);
    }

    /// Add a segment's counts to its row and move its walk on, resealing the row once
    pub fn apply_counts(&mut self, segment: SegmentNumber, counts: &[DeadCount], position: Option<u32>) {
        let Some(at) = self.find_row(segment) else {
            return;
        };
        let Some((length, Some(row))) = read_row(&self.bytes[at..]) else {
            return;
        };
        let (column_count, block_count) = (row.column_count, row.facts.block_count);
        let body_at = at + ROW_HEAD_BYTES;
        for count in counts {
            debug_assert!(count.block < block_count);
            if count.block >= block_count {
                continue;
            }
            let field = body_at + ROW_FACT_BYTES + count.block as usize * size_of::<u32>();
            let dead = read_half(&self.bytes, field).unwrap_or(0) + count.rows;
            self.bytes[field..field + size_of::<u32>()].copy_from_slice(&dead.to_le_bytes());
            self.add_dead(body_at + block_count as usize * size_of::<u32>(), column_count, count);
        }
        if let Some(blocks) = position {
            let field = body_at + WALKED_BLOCKS_AT;
            self.bytes[field..field + size_of::<u32>()].copy_from_slice(&blocks.to_le_bytes());
        }
        let body_end = at + length - ROW_SUM_BYTES;
        let sum = checksum(&self.bytes[body_at..body_end]);
        self.bytes[body_end..body_end + ROW_SUM_BYTES].copy_from_slice(&sum.0.to_le_bytes());
        self.place_at(at);
    }

    /// Start a segment's counts over for a backward walk, which rebuilds them from its own rows. Its row may be behind
    /// counts other rows already hold, so it can't resume as a forward walk
    pub fn restart_backward(&mut self, segment: SegmentNumber) {
        let Some(at) = self.find_row(segment) else {
            return;
        };
        if let Some((length, Some(row))) = self.read_at(at) {
            let (column_count, block_count) = (row.column_count, row.facts.block_count);
            {
                let body_at = at + ROW_HEAD_BYTES;
                self.bytes[body_at + WALKED_BLOCKS_AT..body_at + WALKED_BLOCKS_AT + size_of::<u32>()].fill(0);
                self.bytes[body_at + ROW_FLAGS_AT] |= 1 << IS_BACKWARD_BIT;
                let blocks_length = block_count as usize * size_of::<u32>();
                self.bytes[body_at + ROW_FACT_BYTES..body_at + ROW_FACT_BYTES + blocks_length].fill(0);
                let mut column_at = body_at + ROW_FACT_BYTES + blocks_length;
                for _column in 0..column_count {
                    let lowest = read_short(&self.bytes[column_at + COLUMN_LOWEST_AT..]);
                    let bound = read_short(&self.bytes[column_at + COLUMN_BOUND_AT..]);
                    self.bytes[column_at + COLUMN_DEAD_AT..column_at + COLUMN_FIXED_BYTES].fill(0);
                    column_at += COLUMN_FIXED_BYTES + usize::from(lowest) + usize::from(bound);
                }
                let body_end = at + length - ROW_SUM_BYTES;
                let sum = checksum(&self.bytes[body_at..body_end]);
                self.bytes[body_end..body_end + ROW_SUM_BYTES].copy_from_slice(&sum.0.to_le_bytes());
                self.place_at(at);
            }
        }
    }

    /// Add one count to its column in a row, leaving the row's checksum to the caller. The offset passes the row's block
    /// counts
    pub fn add_dead(&mut self, body_at: usize, column_count: u16, count: &DeadCount) {
        let mut column_at = body_at + ROW_FACT_BYTES;
        for _column in 0..column_count {
            let lowest = read_short(&self.bytes[column_at + COLUMN_LOWEST_AT..]);
            let bound = read_short(&self.bytes[column_at + COLUMN_BOUND_AT..]);
            if self.bytes[column_at] == count.column.0 {
                let words = column_at + COLUMN_DEAD_AT;
                self.change_word(words, |rows| rows + u64::from(count.rows));
                self.change_word(words + size_of::<u64>(), |graves| graves + u64::from(count.graves));
                self.change_word(words + 2 * size_of::<u64>(), |live| (live + count.kept_bytes.0).saturating_sub(count.bytes.0));
                self.change_word(words + 3 * size_of::<u64>(), |dead| dead + count.bytes.0);
                return;
            }
            column_at += COLUMN_FIXED_BYTES + usize::from(lowest) + usize::from(bound);
        }
    }

    /// Change one stored word
    pub fn change_word(&mut self, at: usize, change: impl FnOnce(u64) -> u64) {
        let field = &mut self.bytes[at..at + size_of::<u64>()];
        let mut word = [0; size_of::<u64>()];
        word.copy_from_slice(field);
        field.copy_from_slice(&change(u64::from_le_bytes(word)).to_le_bytes());
    }

    /// Write a row's fixed fields
    pub fn put_facts(&mut self, facts: &SegmentFacts, columns: u16, spans: u16) {
        self.footer_bytes += footer_of(facts);
        self.file_bytes += facts.file_length.0;
        self.bytes.extend_from_slice(&facts.segment.0.to_le_bytes());
        self.bytes.extend_from_slice(&facts.file_length.0.to_le_bytes());
        self.bytes.extend_from_slice(&facts.data_end.0.to_le_bytes());
        self.bytes.extend_from_slice(&facts.row_block_end.0.to_le_bytes());
        self.bytes.extend_from_slice(&facts.seal_mark.0.to_le_bytes());
        self.bytes.extend_from_slice(&facts.places.lowest.0.to_le_bytes());
        self.bytes.extend_from_slice(&facts.places.highest.0.to_le_bytes());
        self.bytes.push(u8::from(facts.is_damaged) | (u8::from(facts.has_covers) << HAS_COVERS_BIT) | (u8::from(facts.is_backward) << IS_BACKWARD_BIT) | (u8::from(facts.has_lost_rows) << HAS_LOST_ROWS_BIT));
        self.bytes.extend_from_slice(&columns.to_le_bytes());
        self.bytes.extend_from_slice(&spans.to_le_bytes());
        self.bytes.extend_from_slice(&facts.damage_place.0.to_le_bytes());
        self.bytes.extend_from_slice(&facts.block_count.to_le_bytes());
        self.bytes.extend_from_slice(&facts.walked_blocks.to_le_bytes());
        self.bytes.extend_from_slice(&facts.marks.lowest.0.to_le_bytes());
        self.bytes.extend_from_slice(&facts.marks.highest.0.to_le_bytes());
        self.bytes.extend_from_slice(&facts.most_block_rows.to_le_bytes());
    }

    /// Walk the rows that pass their checks
    pub fn rows(&self) -> RowWalk<'_> {
        RowWalk { bytes: &self.bytes, is_checked: self.is_checked }
    }

    /// Read the bytes the rows' segments give their footers, past their records
    pub fn footer_bytes(&self) -> ByteCount {
        ByteCount(self.footer_bytes)
    }

    /// Read the bytes the rows' segment files take
    pub fn file_bytes(&self) -> ByteCount {
        ByteCount(self.file_bytes)
    }

    /// Find a segment's row
    pub fn get(&self, segment: SegmentNumber) -> Option<SummaryRow<'_>> {
        self.read_at(self.find_row(segment)?)?.1
    }

    /// Take a segment's row out
    pub fn remove(&mut self, segment: SegmentNumber) {
        let Some(at) = self.find_row(segment) else {
            return;
        };
        let Some((length, facts)) = self.read_at(at).and_then(|(length, row)| Some((length, row?.facts))) else {
            return;
        };
        self.footer_bytes = self.footer_bytes.saturating_sub(footer_of(&facts));
        self.file_bytes = self.file_bytes.saturating_sub(facts.file_length.0);
        self.orders.forget(segment);
        self.bytes.copy_within(at + length.., at);
        self.bytes.truncate(self.bytes.len() - length);
        if self.is_index_whole {
            self.index.remove(segment);
            self.index.shift_past(at as u32, length as u32);
            self.listed -= 1;
        } else {
            self.rebuild_index();
        }
    }

    /// List where each row that passes its check starts, in one walk
    pub fn offsets(&self) -> BTreeMap<SegmentNumber, usize> {
        let mut offsets = BTreeMap::new();
        let mut at = 0;
        while let Some((length, row)) = read_row(&self.bytes[at..]) {
            if let Some(row) = row {
                offsets.insert(row.facts.segment, at);
            }
            at += length;
        }
        offsets
    }

    /// Check whether some row fails its check
    pub fn has_bad_row(&self) -> bool {
        let mut at = 0;
        while at < self.bytes.len() {
            let Some((length, Some(_row))) = read_row(&self.bytes[at..]) else {
                return true;
            };
            at += length;
        }
        false
    }

    /// Read the row at an offset `offsets` gave
    pub fn row_at(&self, at: usize) -> Option<SummaryRow<'_>> {
        read_row(self.bytes.get(at..)?)?.1
    }

    /// Grow so a row of other rows fits, for the rows with spans that always stay
    pub fn make_room_for(&mut self, from: &Self, at: usize) {
        if let Some((length, _row)) = from.bytes.get(at..).and_then(read_row) {
            self.bytes.reserve(length);
        }
    }

    /// Copy a row as it stands from other rows, or return false when it doesn't fit
    pub fn copy_row(&mut self, from: &Self, at: usize) -> bool {
        let Some((length, _row)) = from.bytes.get(at..).and_then(read_row) else {
            return false;
        };
        if self.bytes.len() + length > self.bytes.capacity() {
            return false;
        }
        let start = self.bytes.len();
        self.bytes.extend_from_slice(&from.bytes[at..at + length]);
        if let Some(facts) = from.row_at(at).map(|row| row.facts) {
            self.footer_bytes += footer_of(&facts);
            self.file_bytes += facts.file_length.0;
            self.note_row(facts.segment, start);
            self.place_at(start);
        }
        true
    }
}

/// A walk over the rows that pass their checks
#[derive(Clone, Copy, Debug)]
pub struct RowWalk<'bytes> {
    bytes: &'bytes [u8],
    is_checked: bool,
}

impl<'bytes> Iterator for RowWalk<'bytes> {
    type Item = SummaryRow<'bytes>;

    fn next(&mut self) -> Option<SummaryRow<'bytes>> {
        if self.is_checked {
            let (length, row) = read_trusted_row(self.bytes)?;
            self.bytes = &self.bytes[length..];
            return Some(row);
        }
        loop {
            let (length, row) = read_row(self.bytes)?;
            self.bytes = &self.bytes[length..];
            if row.is_some() {
                return row;
            }
        }
    }
}

/// Write one span into a row, and return its bytes
pub fn write_span(out: &mut [u8], span: &SpanFacts) -> usize {
    let end = span.end.unwrap_or_default();
    let end_length = span.end.map_or(NO_END, |end| end.len() as u16);
    let keys_at = 1 + 2 * size_of::<u16>();
    out[0] = span.column.0;
    out[1..3].copy_from_slice(&(span.start.len() as u16).to_le_bytes());
    out[3..keys_at].copy_from_slice(&end_length.to_le_bytes());
    let end_at = keys_at + span.start.len();
    out[keys_at..end_at].copy_from_slice(span.start);
    out[end_at..end_at + end.len()].copy_from_slice(end);
    end_at + end.len()
}

/// Work out the bytes of a row's body
pub fn row_bytes<'key>(
    block_count: u32,
    columns: impl Iterator<Item = ColumnFacts<'key>>,
    spans: impl Iterator<Item = SpanFacts<'key>>,
) -> usize {
    let mut body: usize = ROW_FACT_BYTES + block_count as usize * size_of::<u32>();
    for column in columns {
        body += COLUMN_FIXED_BYTES + column.lowest.len() + column.bound.len();
    }
    for span in spans {
        body += 1 + 2 * size_of::<u16>() + span.start.len() + span.end.map_or(0, <[u8]>::len);
    }
    body
}

/// Add the part of a lowest key a row keeps
pub fn put_lowest(key: &[u8], out: &mut impl ListOut<u8>) -> std::result::Result<(), ListFull> {
    out.extend_from_slice(&key[..key.len().min(SUMMARY_KEY_BYTES)])
}

/// Add an exclusive bound above every key sharing a highest key's first bytes, which is nothing
/// when no key can sit above them, and which can only widen a range
pub fn put_bound(key: &[u8], out: &mut impl ListOut<u8>) -> std::result::Result<(), ListFull> {
    let start = out.len();
    if key.len() < SUMMARY_KEY_BYTES {
        out.extend_from_slice(key)?;
        return out.push(0);
    }

    out.extend_from_slice(&key[..SUMMARY_KEY_BYTES])?;
    while out.len() > start {
        let last = out.len() - 1;
        if out[last] < u8::MAX {
            out[last] += 1;
            return Ok(());
        }
        out.truncate(last);
    }
    Ok(())
}

/// Count the bytes a segment's footer takes, past its records
pub fn footer_of(facts: &SegmentFacts) -> u64 {
    facts.file_length.0.saturating_sub(facts.data_end.0)
}

/// Lay out a row's head
pub fn row_head(body_length: u32) -> [u8; ROW_HEAD_BYTES] {
    let mut head = [0; ROW_HEAD_BYTES];
    head[..4].copy_from_slice(&ROW_MARKER.to_le_bytes());
    head[4..8].copy_from_slice(&body_length.to_le_bytes());
    let sum = checksum(&head[..8]).0 as u32;
    head[8..].copy_from_slice(&sum.to_le_bytes());
    head
}

/// Read a row's body length when its head passes its check and the whole row is there
pub fn read_row_head(bytes: &[u8]) -> Option<usize> {
    let head = bytes.get(..ROW_HEAD_BYTES)?;
    if read_half(head, 0)? != ROW_MARKER || read_half(head, 8)? != checksum(&head[..8]).0 as u32 {
        return None;
    }
    let body_length = read_half(head, 4)? as usize;
    bytes.get(..ROW_HEAD_BYTES + body_length + ROW_SUM_BYTES)?;
    Some(body_length)
}

/// Read the row at an offset, with no checksum when every row passed its checks
pub fn read_row_at(bytes: &[u8], at: usize, is_checked: bool) -> Option<(usize, Option<SummaryRow<'_>>)> {
    let bytes = bytes.get(at..)?;
    if is_checked { read_trusted_row(bytes).map(|(length, row)| (length, Some(row))) } else { read_row(bytes) }
}

/// Read a row of rows that passed their checks when they were written or loaded, with no checksum: the marker and the length
/// frame it
pub fn read_trusted_row(bytes: &[u8]) -> Option<(usize, SummaryRow<'_>)> {
    if read_half(bytes, 0)? != ROW_MARKER {
        return None;
    }
    let body_length = read_half(bytes, 4)? as usize;
    let total = ROW_HEAD_BYTES + body_length + ROW_SUM_BYTES;
    let body = bytes.get(ROW_HEAD_BYTES..ROW_HEAD_BYTES + body_length)?;
    bytes.get(..total)?;
    Some((total, parse_row(body)?))
}

/// Read how far a row reaches and the row itself, which is nothing when it fails its check, and
/// after a bad head reach to the next head that passes, so one bad row costs that row alone
pub fn read_row(bytes: &[u8]) -> Option<(usize, Option<SummaryRow<'_>>)> {
    let Some(body_length) = read_row_head(bytes) else {
        let next = (1..bytes.len()).find(|at| read_row_head(&bytes[*at..]).is_some())?;
        return Some((next, None));
    };

    let total = ROW_HEAD_BYTES + body_length + ROW_SUM_BYTES;
    let body = &bytes[ROW_HEAD_BYTES..ROW_HEAD_BYTES + body_length];
    let mut sum = [0; ROW_SUM_BYTES];
    sum.copy_from_slice(&bytes[ROW_HEAD_BYTES + body_length..total]);
    if checksum(body).0 != u64::from_le_bytes(sum) {
        return Some((total, None));
    }
    Some((total, parse_row(body)))
}

/// Read a row's fixed fields
pub fn parse_row(body: &[u8]) -> Option<SummaryRow<'_>> {
    let mut cursor = ByteReader::new(body);
    let segment = SegmentNumber(cursor.word()?);
    let file_length = ByteCount(cursor.word()?);
    let data_end = FileOffset(cursor.word()?);
    let row_block_end = FileOffset(cursor.word()?);
    let seal_mark = Checksum(cursor.word()?);
    let places = PlaceRange {
        lowest: WritePlace(cursor.word()?),
        highest: WritePlace(cursor.word()?),
    };
    let flags = cursor.byte()?;
    let is_damaged = flags & 1 != 0;
    let has_covers = flags >> HAS_COVERS_BIT & 1 != 0;
    let is_backward = flags >> IS_BACKWARD_BIT & 1 != 0;
    let has_lost_rows = flags >> HAS_LOST_ROWS_BIT & 1 != 0;
    let column_count = cursor.short()?;
    let span_count = cursor.short()?;
    let damage_place = WritePlace(cursor.word()?);
    let block_count = cursor.half()?;
    let walked_blocks = cursor.half()?;
    let marks = MarkRange {
        lowest: DeathMark(cursor.word()?),
        highest: DeathMark(cursor.word()?),
    };
    let most_block_rows = cursor.half()?;
    let block_dead = cursor.take(block_count as usize * size_of::<u32>())?;
    let facts = SegmentFacts {
        segment,
        file_length,
        data_end,
        row_block_end,
        seal_mark,
        places,
        is_damaged,
        has_covers,
        damage_place,
        block_count,
        walked_blocks,
        is_backward,
        has_lost_rows,
        marks,
        most_block_rows,
    };
    Some(SummaryRow {
        facts,
        block_dead,
        column_count,
        span_count,
        body: cursor.rest(),
    })
}

/// The two ends of a summary copy
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CopyEnds {
    pub header: [u8; SUMMARY_HEADER_BYTES],
    pub trailer: [u8; SUMMARY_HEADER_BYTES],
}

/// One end of a summary copy, read back
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CopyEnd {
    pub mark: WritePlace,
    pub forward: SegmentNumber,
    pub rows_length: usize,
}

/// One summary copy, read back
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SummaryCopy {
    pub header_mark: Option<WritePlace>,
    pub trailer_mark: Option<WritePlace>,
    pub forward: Option<SegmentNumber>,
    pub rows: Vec<u8>,
}

impl SummaryCopy {
    /// Read the mark from whichever end passes
    pub fn mark(&self) -> Option<WritePlace> {
        self.header_mark.or(self.trailer_mark)
    }
}

/// What a summary read can't vouch for, when the newer copy lost a row and the two copies differ
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LostRows {
    pub has_lost_row: bool,
    pub older: Vec<SegmentNumber>,
    pub unread_covers: Vec<SegmentNumber>,
}

/// A summary file's paths, built once at open so a save allocates nothing
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SummaryPaths {
    pub root: PathBuf,
    pub current: PathBuf,
    pub second: PathBuf,
    pub new: PathBuf,
}

impl SummaryPaths {
    /// Build the paths in a root
    pub fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
            current: root.join(SUMMARY_NAME),
            second: root.join(SECOND_SUMMARY_NAME),
            new: root.join(NEW_SUMMARY_NAME),
        }
    }
}

/// Build one end of a summary copy
pub fn mark_end(magic: u64, mark: WritePlace, forward: SegmentNumber, rows: &[u8]) -> [u8; SUMMARY_HEADER_BYTES] {
    let mut end = [0; SUMMARY_HEADER_BYTES];
    end[..8].copy_from_slice(&magic.to_le_bytes());
    end[8..16].copy_from_slice(&mark.0.to_le_bytes());
    end[16..24].copy_from_slice(&(rows.len() as u64).to_le_bytes());
    end[24..END_SUM_AT].copy_from_slice(&forward.0.to_le_bytes());
    let sum = checksum(&end[..END_SUM_AT]);
    end[END_SUM_AT..].copy_from_slice(&sum.0.to_le_bytes());
    end
}

/// Build the two ends of a summary copy, which carry its mark twice
pub fn copy_ends(mark: WritePlace, forward: SegmentNumber, rows: &[u8]) -> CopyEnds {
    CopyEnds {
        header: mark_end(SUMMARY_MAGIC, mark, forward, rows),
        trailer: mark_end(SUMMARY_TRAILER_MAGIC, mark, forward, rows),
    }
}

/// Write both copies of a summary, the second after the first is synced, so a crash leaves one whole
pub fn write_summary<Backend: Io>(
    io: &Backend,
    paths: &SummaryPaths,
    mark: WritePlace,
    forward: SegmentNumber,
    rows: &[u8],
) -> Result<()> {
    let ends = copy_ends(mark, forward, rows);
    write_copy(io, paths, &paths.current, &ends, rows)?;
    write_copy(io, paths, &paths.second, &ends, rows)
}

/// Replace one copy through the new file
pub fn write_copy<Backend: Io>(
    io: &Backend,
    paths: &SummaryPaths,
    target: &Path,
    ends: &CopyEnds,
    rows: &[u8],
) -> Result<()> {
    let parts = [IoSlice::new(&ends.header), IoSlice::new(rows), IoSlice::new(&ends.trailer)];
    replace_whole(io, &paths.root, &paths.new, target, &parts)
}

/// Read a summary into rows, each from whichever copy passes its check, and return the newer mark. A copy with
/// no mark at either end leaves the other copy's rows as they are
pub fn read_summary<Backend: Io>(
    io: &Backend,
    paths: &SummaryPaths,
    rows: &mut SummaryRows,
    lost: &mut LostRows,
) -> Result<Option<WritePlace>> {
    let first = read_copy(io, &paths.current)?;
    let second = read_copy(io, &paths.second)?;
    rows.on_disk = twins(first.as_ref(), second.as_ref());
    // a row can come from either copy, and a number too high costs an undercount where one too low double counts
    let forward = first.iter().chain(&second).filter_map(|copy| copy.forward).max();
    let (newer, older) = match (first, second) {
        (Some(first), Some(second)) if second.mark() > first.mark() => (second, Some(first)),
        (Some(first), second) => (first, second),
        (None, Some(second)) => (second, None),
        (None, None) => return Ok(None),
    };

    let mark = newer.mark();
    rows.highest_forward = forward.unwrap_or(FORWARD_UNKNOWN);
    rows.bytes = SummaryBytes::Heap(newer.rows);
    rows.is_checked = false;
    if let Some(older) = older {
        let has_two_marks = mark.is_some() && older.mark().is_some() && mark != older.mark();
        let mut older_rows = SummaryRows::new(ByteCount(0));
        older_rows.bytes = SummaryBytes::Heap(older.rows);
        older_rows.is_checked = false;
        let listed = rows.offsets();
        // with two marks, a row the newer copy lost may hold more than the older copy's, so it can't stand as it is
        lost.has_lost_row = has_two_marks && rows.has_bad_row();
        rows.bytes.reserve(older_rows.bytes.len());
        for (segment, at) in older_rows.offsets() {
            if !listed.contains_key(&segment) {
                rows.copy_row(&older_rows, at);
                lost.older.push(segment);
            }
        }
    }
    Ok(mark)
}

/// Sum what both copies hold when each has both its ends and the two match whole, the rows a save of the same bytes would write
pub fn twins(first: Option<&SummaryCopy>, second: Option<&SummaryCopy>) -> Option<SavedSummary> {
    let (first, second) = (first?, second?);
    let is_whole = |copy: &SummaryCopy| copy.header_mark.is_some() && copy.header_mark == copy.trailer_mark;
    let is_alike = is_whole(first) && is_whole(second) && first.header_mark == second.header_mark && first.forward == second.forward && first.rows == second.rows;
    is_alike.then(|| SavedSummary::of(&first.rows, first.forward.unwrap_or_default()))
}

/// Read one copy's marks and rows, or nothing when it's absent. Its rows are read whether or not its ends pass
pub fn read_copy<Backend: Io>(io: &Backend, path: &Path) -> Result<Option<SummaryCopy>> {
    let file = match io.open_read_only(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };

    let bytes = read_readable(&file);
    io.close_file(file);
    let bytes = bytes?;

    let header = bytes.get(..SUMMARY_HEADER_BYTES).and_then(|end| read_end(end, SUMMARY_MAGIC));
    let trailer_at = match header {
        Some(header) => SUMMARY_HEADER_BYTES.saturating_add(header.rows_length),
        None => bytes.len().saturating_sub(SUMMARY_HEADER_BYTES),
    };
    let trailer = bytes
        .get(trailer_at..trailer_at.saturating_add(SUMMARY_HEADER_BYTES))
        .and_then(|end| read_end(end, SUMMARY_TRAILER_MAGIC));

    let rows_end = trailer_at.min(bytes.len());
    Ok(Some(SummaryCopy {
        header_mark: header.map(|end| end.mark),
        trailer_mark: trailer.map(|end| end.mark),
        forward: header.or(trailer).map(|end| end.forward),
        rows: bytes.get(SUMMARY_HEADER_BYTES..rows_end).unwrap_or_default().to_vec(),
    }))
}

/// Read one end of a copy, or nothing when it fails its check
pub fn read_end(end: &[u8], magic: u64) -> Option<CopyEnd> {
    let mut cursor = ByteReader::new(end);
    let is_ours = cursor.word() == Some(magic);
    let (mark, rows_length) = (cursor.word()?, cursor.word()?);
    let (forward, sum) = (cursor.word()?, cursor.word()?);
    if !is_ours || checksum(&end[..END_SUM_AT]).0 != sum {
        return None;
    }
    Some(CopyEnd {
        mark: WritePlace(mark),
        rows_length: rows_length as usize,
        forward: SegmentNumber(forward),
    })
}

/// Read a whole file, leaving zeroes over the sectors the device can't give back
pub fn read_readable<File: IoFile>(file: &File) -> Result<Vec<u8>> {
    let length = file.length()?;
    let mut bytes = vec![0; length.0 as usize];
    if read_or_damaged(file, FileOffset(0), &mut bytes)? {
        return Ok(bytes);
    }

    for (index, sector) in bytes.chunks_mut(SECTOR_BYTES).enumerate() {
        if !read_or_damaged(file, FileOffset((index * SECTOR_BYTES) as u64), sector)? {
            sector.fill(0);
        }
    }
    Ok(bytes)
}

/// Read a stored short
pub fn read_short(bytes: &[u8]) -> u16 {
    u16::from_le_bytes([bytes[0], bytes[1]])
}
