//! A volume's summary: one row per sealed segment, which outlives that segment's tail

use std::io::{Error, ErrorKind, IoSlice, Result};
use std::path::{Path, PathBuf};

use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, FileOffset, RowCount, WritePlace};

use crate::io::{Io, IoFile};
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
/// Bytes of a row's length
pub const ROW_LENGTH_BYTES: usize = size_of::<u32>();
/// Bytes of a row's checksum
pub const ROW_SUM_BYTES: usize = size_of::<u64>();
/// Bytes of the fields every row has, before its columns
pub const ROW_FACT_BYTES: usize = 69;
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
}

/// One column's range and counts in a segment
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnFacts<'key> {
    pub column: ColumnId,
    pub lowest: &'key [u8],
    pub highest: &'key [u8],
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
        let highest_length = self.short()? as usize;
        let row_count = RowCount(self.word()?);
        let grave_count = RowCount(self.word()?);
        Some(ColumnFacts {
            column,
            lowest: self.take(lowest_length)?,
            highest: self.take(highest_length)?,
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
            body += 1 + 2 * size_of::<u16>() + 2 * size_of::<u64>() + column.lowest.len() + column.highest.len();
        }
        for span in spans.clone() {
            body += 1 + 2 * size_of::<u16>() + span.start.len() + span.end.map_or(0, <[u8]>::len);
        }
        if self.bytes.len() + ROW_LENGTH_BYTES + body + ROW_SUM_BYTES > self.bytes.capacity() {
            return false;
        }

        let start = self.bytes.len();
        self.bytes.extend_from_slice(&(body as u32).to_le_bytes());
        let body_at = self.bytes.len();
        self.put_facts(facts, columns.len() as u16, spans.len() as u16);
        for column in columns {
            self.bytes.push(column.column.0);
            self.bytes.extend_from_slice(&(column.lowest.len() as u16).to_le_bytes());
            self.bytes.extend_from_slice(&(column.highest.len() as u16).to_le_bytes());
            self.bytes.extend_from_slice(&column.row_count.0.to_le_bytes());
            self.bytes.extend_from_slice(&column.grave_count.0.to_le_bytes());
            self.bytes.extend_from_slice(column.lowest);
            self.bytes.extend_from_slice(column.highest);
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
        debug_assert_eq!(self.bytes.len() - start, ROW_LENGTH_BYTES + body + ROW_SUM_BYTES);
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
        self.bytes.extend_from_slice(&[0; ROW_FACT_BYTES - 61]);
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

    /// Keep only the rows a check passes on
    pub fn keep_where(&mut self, should_keep: impl Fn(&SummaryRow) -> bool) {
        let mut read = 0;
        let mut write = 0;
        while let Some((length, row)) = read_row(&self.bytes[read..]) {
            if row.is_some_and(|row| should_keep(&row)) {
                self.bytes.copy_within(read..read + length, write);
                write += length;
            }
            read += length;
        }
        self.bytes.truncate(write);
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

/// Read a row's length and the row itself, which is nothing when it fails its check
pub fn read_row(bytes: &[u8]) -> Option<(usize, Option<SummaryRow<'_>>)> {
    let length_bytes = bytes.get(..ROW_LENGTH_BYTES)?;
    let mut length = [0; ROW_LENGTH_BYTES];
    length.copy_from_slice(length_bytes);
    let body_length = u32::from_le_bytes(length) as usize;
    let total = ROW_LENGTH_BYTES + body_length + ROW_SUM_BYTES;
    let row_bytes = bytes.get(..total)?;

    let body = &row_bytes[ROW_LENGTH_BYTES..ROW_LENGTH_BYTES + body_length];
    let mut sum = [0; ROW_SUM_BYTES];
    sum.copy_from_slice(&row_bytes[ROW_LENGTH_BYTES + body_length..]);
    if checksum(body).0 != u64::from_le_bytes(sum) {
        return Some((total, None));
    }
    Some((total, parse_row(body)))
}

/// Read a row's fixed fields
pub fn parse_row(body: &[u8]) -> Option<SummaryRow<'_>> {
    let mut cursor = RowCursor { bytes: body };
    let facts = SegmentFacts {
        segment: SegmentNumber(cursor.word()?),
        file_length: ByteCount(cursor.word()?),
        data_end: FileOffset(cursor.word()?),
        row_block_end: FileOffset(cursor.word()?),
        seal_mark: Checksum(cursor.word()?),
        places: PlaceRange {
            lowest: WritePlace(cursor.word()?),
            highest: WritePlace(cursor.word()?),
        },
        is_damaged: cursor.byte()? != 0,
    };
    let column_count = cursor.short()?;
    let span_count = cursor.short()?;
    cursor.take(ROW_FACT_BYTES - 61)?;
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
    let bytes = bytes?;

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
