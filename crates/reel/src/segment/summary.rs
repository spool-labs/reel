//! A volume's summary: one row per sealed segment, which outlives that segment's tail

use std::collections::BTreeMap;
use std::hint::black_box;
use std::io::{ErrorKind, IoSlice, Result};
use std::path::{Path, PathBuf};

use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, FileOffset, RowCount, WritePlace};

use crate::io::device_error::{SECTOR_BYTES, read_or_damaged};
use crate::io::{Io, IoFile};
use crate::little_endian::read_half;
use crate::record::record_check::{Checksum, checksum};
use crate::segment::segment_tail::{PlaceRange, SegmentNumber};

/// The bit of a row's flags that says its footer keeps range deletes
pub const HAS_COVERS_BIT: u8 = 1;
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
pub const SUMMARY_HEADER_BYTES: usize = 32;
/// Opens every row, so a walk past a bad row can find the next one
pub const ROW_MARKER: u32 = 0x5253_554D;
/// Bytes of a row's head: its marker, its body's length and the head's own checksum
pub const ROW_HEAD_BYTES: usize = 3 * size_of::<u32>();
/// Bytes of a row's checksum
pub const ROW_SUM_BYTES: usize = size_of::<u64>();
/// Bytes of the fields every row has, before its columns
pub const ROW_FACT_BYTES: usize = 69;
/// Where a row's body keeps its span count
pub const SPAN_COUNT_AT: usize = 59;
/// Most bytes of a key a row keeps, so a column costs about 100 B whatever the keys are
pub const SUMMARY_KEY_BYTES: usize = 32;
/// Bytes a row takes before its columns and spans
pub const MOST_ROW_FIXED_BYTES: usize = ROW_HEAD_BYTES + ROW_FACT_BYTES + ROW_SUM_BYTES;
/// Bytes a span adds to a row, before its keys
pub const MOST_ROW_SPAN_FIXED_BYTES: usize = 1 + 2 * size_of::<u16>();
/// The most bytes a column adds to a row, with its bounded keys
pub const MOST_ROW_COLUMN_BYTES: usize = 21 + 2 * SUMMARY_KEY_BYTES + 1;
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
}

/// One column's range and counts in a segment
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnFacts<'key> {
    pub column: ColumnId,
    pub lowest: &'key [u8],
    pub bound: &'key [u8],
    pub row_count: RowCount,
    pub grave_count: RowCount,
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
    bytes: Vec<u8>,
}

/// One row read back
#[derive(Clone, Copy, Debug)]
pub struct SummaryRow<'bytes> {
    pub facts: SegmentFacts,
    pub column_count: u16,
    pub span_count: u16,
    pub body: &'bytes [u8],
}

/// A walk over the fields of a row
#[derive(Clone, Copy, Debug)]
pub struct RowCursor<'bytes> {
    bytes: &'bytes [u8],
}

impl<'bytes> RowCursor<'bytes> {
    /// Take some bytes
    pub fn take(&mut self, length: usize) -> Option<&'bytes [u8]> {
        let (field, rest) = self.bytes.split_at_checked(length)?;
        self.bytes = rest;
        Some(field)
    }

    /// Take a word
    pub fn word(&mut self) -> Option<u64> {
        let mut word = [0; size_of::<u64>()];
        word.copy_from_slice(self.take(size_of::<u64>())?);
        Some(u64::from_le_bytes(word))
    }

    /// Take a short
    pub fn short(&mut self) -> Option<u16> {
        let bytes = self.take(size_of::<u16>())?;
        Some(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    /// Take one byte
    pub fn byte(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }

    /// Read the next column
    pub fn column(&mut self) -> Option<ColumnFacts<'bytes>> {
        let column = ColumnId(self.byte()?);
        let lowest_length = self.short()? as usize;
        let bound_length = self.short()? as usize;
        let row_count = RowCount(self.word()?);
        let grave_count = RowCount(self.word()?);
        Some(ColumnFacts {
            column,
            lowest: self.take(lowest_length)?,
            bound: self.take(bound_length)?,
            row_count,
            grave_count,
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
    /// Walk the row's columns, then its spans
    pub fn cursor(&self) -> RowCursor<'bytes> {
        RowCursor { bytes: self.body }
    }
}

impl SummaryRows {
    /// Allocate room for the rows
    pub fn new(capacity: ByteCount) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity.0 as usize),
        }
    }

    /// Grow the room rows can reach, which open does once and nothing does after
    pub fn make_room(&mut self, capacity: ByteCount) {
        let wanted = capacity.0 as usize;
        let length = self.bytes.len();
        self.bytes.reserve_exact(wanted.saturating_sub(length));
        black_box(self.bytes.as_mut_ptr());
        self.bytes.resize(wanted.max(length), 0);
        self.bytes.truncate(length);
    }

    /// Read the stored rows
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Read the heap the rows hold
    pub fn heap_bytes(&self) -> ByteCount {
        ByteCount(self.bytes.capacity() as u64)
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
        let body = row_bytes(columns.clone(), spans.clone());
        if self.bytes.len() + ROW_HEAD_BYTES + body + ROW_SUM_BYTES > self.bytes.capacity() {
            return false;
        }

        let start = self.bytes.len();
        self.bytes.extend_from_slice(&row_head(body as u32));
        let body_at = self.bytes.len();
        self.put_facts(facts, column_count as u16, span_count as u16);
        for column in columns {
            self.bytes.push(column.column.0);
            self.bytes.extend_from_slice(&(column.lowest.len() as u16).to_le_bytes());
            self.bytes.extend_from_slice(&(column.bound.len() as u16).to_le_bytes());
            self.bytes.extend_from_slice(&column.row_count.0.to_le_bytes());
            self.bytes.extend_from_slice(&column.grave_count.0.to_le_bytes());
            self.bytes.extend_from_slice(column.lowest);
            self.bytes.extend_from_slice(column.bound);
        }
        self.put_spans(spans);

        let sum = checksum(&self.bytes[body_at..]);
        self.bytes.extend_from_slice(&sum.0.to_le_bytes());
        debug_assert_eq!(self.bytes.len() - start, ROW_HEAD_BYTES + body + ROW_SUM_BYTES);
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
                self.bytes.reserve(self.bytes.capacity() + MOST_ROW_FIXED_BYTES + row_bytes(columns.clone(), spans.clone()));
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
                let spans_length = cursor.bytes.len();
                return Some((at, length, at + length - ROW_SUM_BYTES - spans_length));
            }
            at += length;
        }
        None
    }

    /// Write a row's fixed fields
    pub fn put_facts(&mut self, facts: &SegmentFacts, columns: u16, spans: u16) {
        self.bytes.extend_from_slice(&facts.segment.0.to_le_bytes());
        self.bytes.extend_from_slice(&facts.file_length.0.to_le_bytes());
        self.bytes.extend_from_slice(&facts.data_end.0.to_le_bytes());
        self.bytes.extend_from_slice(&facts.row_block_end.0.to_le_bytes());
        self.bytes.extend_from_slice(&facts.seal_mark.0.to_le_bytes());
        self.bytes.extend_from_slice(&facts.places.lowest.0.to_le_bytes());
        self.bytes.extend_from_slice(&facts.places.highest.0.to_le_bytes());
        self.bytes.push(u8::from(facts.is_damaged) | (u8::from(facts.has_covers) << HAS_COVERS_BIT));
        self.bytes.extend_from_slice(&columns.to_le_bytes());
        self.bytes.extend_from_slice(&spans.to_le_bytes());
        self.bytes.extend_from_slice(&facts.damage_place.0.to_le_bytes());
    }

    /// Walk the rows that pass their checks
    pub fn rows(&self) -> RowWalk<'_> {
        RowWalk { bytes: &self.bytes }
    }

    /// Find a segment's row
    pub fn get(&self, segment: SegmentNumber) -> Option<SummaryRow<'_>> {
        self.rows().find(|row| row.facts.segment == segment)
    }

    /// Take a segment's row out
    pub fn remove(&mut self, segment: SegmentNumber) {
        let mut at = 0;
        while let Some((length, row)) = read_row(&self.bytes[at..]) {
            if row.is_some_and(|row| row.facts.segment == segment) {
                self.bytes.copy_within(at + length.., at);
                self.bytes.truncate(self.bytes.len() - length);
                return;
            }
            at += length;
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
        self.bytes.extend_from_slice(&from.bytes[at..at + length]);
        true
    }
}

/// A walk over the rows that pass their checks
#[derive(Clone, Copy, Debug)]
pub struct RowWalk<'bytes> {
    bytes: &'bytes [u8],
}

impl<'bytes> Iterator for RowWalk<'bytes> {
    type Item = SummaryRow<'bytes>;

    fn next(&mut self) -> Option<SummaryRow<'bytes>> {
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
    columns: impl Iterator<Item = ColumnFacts<'key>>,
    spans: impl Iterator<Item = SpanFacts<'key>>,
) -> usize {
    let mut body: usize = ROW_FACT_BYTES;
    for column in columns {
        body += 1 + 2 * size_of::<u16>() + 2 * size_of::<u64>() + column.lowest.len() + column.bound.len();
    }
    for span in spans {
        body += 1 + 2 * size_of::<u16>() + span.start.len() + span.end.map_or(0, <[u8]>::len);
    }
    body
}

/// Add the part of a lowest key a row keeps
pub fn put_lowest(key: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&key[..key.len().min(SUMMARY_KEY_BYTES)]);
}

/// Add an exclusive bound above every key sharing a highest key's first bytes, which is nothing
/// when no key can sit above them, and which can only widen a range
pub fn put_bound(key: &[u8], out: &mut Vec<u8>) {
    let start = out.len();
    if key.len() < SUMMARY_KEY_BYTES {
        out.extend_from_slice(key);
        out.push(0);
        return;
    }

    out.extend_from_slice(&key[..SUMMARY_KEY_BYTES]);
    while out.len() > start {
        let last = out.len() - 1;
        if out[last] < u8::MAX {
            out[last] += 1;
            return;
        }
        out.pop();
    }
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
    let mut cursor = RowCursor { bytes: body };
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
    let has_covers = flags >> HAS_COVERS_BIT != 0;
    let column_count = cursor.short()?;
    let span_count = cursor.short()?;
    let damage_place = WritePlace(cursor.word()?);
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
    };
    Some(SummaryRow {
        facts,
        column_count,
        span_count,
        body: cursor.bytes,
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
    pub rows_length: usize,
}

/// One summary copy, read back
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SummaryCopy {
    pub header_mark: Option<WritePlace>,
    pub trailer_mark: Option<WritePlace>,
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
pub fn mark_end(magic: u64, mark: WritePlace, rows: &[u8]) -> [u8; SUMMARY_HEADER_BYTES] {
    let mut end = [0; SUMMARY_HEADER_BYTES];
    end[..8].copy_from_slice(&magic.to_le_bytes());
    end[8..16].copy_from_slice(&mark.0.to_le_bytes());
    end[16..24].copy_from_slice(&(rows.len() as u64).to_le_bytes());
    let sum = checksum(&end[..24]);
    end[24..].copy_from_slice(&sum.0.to_le_bytes());
    end
}

/// Build the two ends of a summary copy, which carry its mark twice
pub fn copy_ends(mark: WritePlace, rows: &[u8]) -> CopyEnds {
    CopyEnds {
        header: mark_end(SUMMARY_MAGIC, mark, rows),
        trailer: mark_end(SUMMARY_TRAILER_MAGIC, mark, rows),
    }
}

/// Write both copies of a summary, the second after the first is synced, so a crash leaves one whole
pub fn write_summary<Backend: Io>(io: &Backend, paths: &SummaryPaths, mark: WritePlace, rows: &[u8]) -> Result<()> {
    let ends = copy_ends(mark, rows);
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
    match io.remove_file(&paths.new) {
        Ok(()) => {},
        Err(error) if error.kind() == ErrorKind::NotFound => {},
        Err(error) => return Err(error),
    }

    let file = io.create_file(&paths.new, ByteCount(0))?;
    let written = file.write_at(FileOffset(0), &[IoSlice::new(&ends.header), IoSlice::new(rows), IoSlice::new(&ends.trailer)]);
    let synced = written.and_then(|()| file.sync());
    io.close_file(file);
    synced?;
    io.rename_file(&paths.new, target)?;
    io.sync_directory(&paths.root)
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
    let (newer, older) = match (first, second) {
        (Some(first), Some(second)) if second.mark() > first.mark() => (second, Some(first)),
        (Some(first), second) => (first, second),
        (None, Some(second)) => (second, None),
        (None, None) => return Ok(None),
    };

    let mark = newer.mark();
    rows.bytes = newer.rows;
    if let Some(older) = older {
        let has_two_marks = mark.is_some() && older.mark().is_some() && mark != older.mark();
        let older_rows = SummaryRows { bytes: older.rows };
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
        rows: bytes.get(SUMMARY_HEADER_BYTES..rows_end).unwrap_or_default().to_vec(),
    }))
}

/// Read one end of a copy, or nothing when it fails its check
pub fn read_end(end: &[u8], magic: u64) -> Option<CopyEnd> {
    let mut cursor = RowCursor { bytes: end };
    let is_ours = cursor.word() == Some(magic);
    let (mark, rows_length, sum) = (cursor.word()?, cursor.word()?, cursor.word()?);
    if !is_ours || checksum(&end[..24]).0 != sum {
        return None;
    }
    Some(CopyEnd {
        mark: WritePlace(mark),
        rows_length: rows_length as usize,
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
