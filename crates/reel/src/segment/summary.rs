//! A volume's summary: one row per sealed segment, which outlives that segment's tail

use std::collections::BTreeMap;
use std::io::{Error, ErrorKind, IoSlice, Result};
use std::path::{Path, PathBuf};

use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, FileOffset, RowCount, WritePlace};

use crate::io::{Io, IoFile};
use crate::little_endian::read_half;
use crate::record::record_check::{Checksum, checksum};
use crate::segment::segment_tail::{PlaceRange, SegmentNumber};
use crate::superblock::read_whole;

/// Opens a summary file
pub const SUMMARY_MAGIC: u64 = 0x5245_4C53_554D_4D31;
/// The summary's file name
pub const SUMMARY_NAME: &str = "summary";
/// The name a new summary is written under before it replaces the old one
pub const NEW_SUMMARY_NAME: &str = "summary.new";
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
/// Most bytes of a key a row keeps, so a column costs about 100 B whatever the keys are
pub const SUMMARY_KEY_BYTES: usize = 32;
/// Bytes a row takes before its columns and spans
pub const MOST_ROW_FIXED_BYTES: usize = ROW_HEAD_BYTES + ROW_FACT_BYTES + ROW_SUM_BYTES;
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
        columns: impl ExactSizeIterator<Item = ColumnFacts<'key>> + Clone,
        spans: impl ExactSizeIterator<Item = SpanFacts<'key>> + Clone,
    ) -> bool {
        let mut body: usize = ROW_FACT_BYTES;
        for column in columns.clone() {
            body += 1 + 2 * size_of::<u16>() + 2 * size_of::<u64>() + column.lowest.len() + column.bound.len();
        }
        for span in spans.clone() {
            body += 1 + 2 * size_of::<u16>() + span.start.len() + span.end.map_or(0, <[u8]>::len);
        }
        if self.bytes.len() + ROW_HEAD_BYTES + body + ROW_SUM_BYTES > self.bytes.capacity() {
            return false;
        }

        let start = self.bytes.len();
        self.bytes.extend_from_slice(&row_head(body as u32));
        let body_at = self.bytes.len();
        self.put_facts(facts, columns.len() as u16, spans.len() as u16);
        for column in columns {
            self.bytes.push(column.column.0);
            self.bytes.extend_from_slice(&(column.lowest.len() as u16).to_le_bytes());
            self.bytes.extend_from_slice(&(column.bound.len() as u16).to_le_bytes());
            self.bytes.extend_from_slice(&column.row_count.0.to_le_bytes());
            self.bytes.extend_from_slice(&column.grave_count.0.to_le_bytes());
            self.bytes.extend_from_slice(column.lowest);
            self.bytes.extend_from_slice(column.bound);
        }
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

        let sum = checksum(&self.bytes[body_at..]);
        self.bytes.extend_from_slice(&sum.0.to_le_bytes());
        debug_assert_eq!(self.bytes.len() - start, ROW_HEAD_BYTES + body + ROW_SUM_BYTES);
        true
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
        self.bytes.push(u8::from(facts.is_damaged));
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

    /// Read the row at an offset `offsets` gave
    pub fn row_at(&self, at: usize) -> Option<SummaryRow<'_>> {
        read_row(self.bytes.get(at..)?)?.1
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
    let is_damaged = cursor.byte()? != 0;
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
        damage_place,
    };
    Some(SummaryRow {
        facts,
        column_count,
        span_count,
        body: cursor.bytes,
    })
}

/// A summary file's paths, built once at open so a save allocates nothing
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SummaryPaths {
    pub root: PathBuf,
    pub current: PathBuf,
    pub new: PathBuf,
}

impl SummaryPaths {
    /// Build the paths in a root
    pub fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
            current: root.join(SUMMARY_NAME),
            new: root.join(NEW_SUMMARY_NAME),
        }
    }
}

/// Replace a summary so a crash leaves the old one or the new one whole
pub fn write_summary<Backend: Io>(io: &Backend, paths: &SummaryPaths, mark: WritePlace, rows: &[u8]) -> Result<()> {
    match io.remove_file(&paths.new) {
        Ok(()) => {},
        Err(error) if error.kind() == ErrorKind::NotFound => {},
        Err(error) => return Err(error),
    }

    let mut header = [0; SUMMARY_HEADER_BYTES];
    header[..8].copy_from_slice(&SUMMARY_MAGIC.to_le_bytes());
    header[8..16].copy_from_slice(&mark.0.to_le_bytes());
    header[16..24].copy_from_slice(&(rows.len() as u64).to_le_bytes());
    let sum = checksum(&header[..24]);
    header[24..].copy_from_slice(&sum.0.to_le_bytes());

    let file = io.create_file(&paths.new, ByteCount(0))?;
    let written = file.write_at(FileOffset(0), &[IoSlice::new(&header), IoSlice::new(rows)]);
    let synced = written.and_then(|()| file.sync());
    io.close_file(file);
    synced?;
    io.rename_file(&paths.new, &paths.current)?;
    io.sync_directory(&paths.root)
}

/// Read a summary into rows, and return its mark, or nothing when it's absent or its header fails
pub fn read_summary<Backend: Io>(
    io: &Backend,
    paths: &SummaryPaths,
    rows: &mut SummaryRows,
) -> Result<Option<WritePlace>> {
    let file = match io.open_read_only(&paths.current) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };

    let bytes = read_whole(&file);
    io.close_file(file);
    // rows the file can't give back are rebuilt from the tails, like any row that fails its check
    let Ok(bytes) = bytes else {
        return Ok(None);
    };

    let Some(header) = bytes.get(..SUMMARY_HEADER_BYTES) else {
        return Ok(None);
    };
    let mut cursor = RowCursor { bytes: header };
    let is_ours = cursor.word() == Some(SUMMARY_MAGIC);
    let mark = cursor.word();
    let rows_length = cursor.word();
    let sum = cursor.word();
    let (Some(mark), Some(rows_length), Some(sum)) = (mark, rows_length, sum) else {
        return Err(Error::from(ErrorKind::InvalidData));
    };
    if !is_ours || checksum(&header[..24]).0 != sum {
        return Ok(None);
    }

    let end = (SUMMARY_HEADER_BYTES + rows_length as usize).min(bytes.len());
    rows.bytes.clear();
    let body = &bytes[SUMMARY_HEADER_BYTES..end];
    if rows.bytes.capacity() < body.len() {
        rows.bytes.reserve_exact(body.len());
    }
    rows.bytes.extend_from_slice(body);
    Ok(Some(WritePlace(mark)))
}
