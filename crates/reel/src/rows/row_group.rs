//! One group in a rows file: a header, a row count, the rows and a checksum

use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, DeathMark, FileOffset, RowCount, WritePlace};
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::lifetime::MarkRange;
use crate::pool::touched_zeroes;
use crate::little_endian::{read_half, read_word};
use crate::segment::varint::{MAX_VARINT_BYTES, VALUE_BITS, read_varint, write_varint};
use crate::record::record_check::RecordKey;
use crate::row_kind::RowKind;

/// Opens every group, so a read past the last one stops
pub const GROUP_MAGIC: u32 = 0x524C_4752;
/// Where the group's total length sits
pub const TOTAL_LENGTH_AT: usize = 4;
/// Where the row count sits
pub const ROW_COUNT_AT: usize = 8;
/// Where the end of the last completed sync sits
pub const SYNCED_THROUGH_AT: usize = 12;
/// Where the lowest and highest death mark so far in the segment sit
pub const MARKS_AT: usize = 20;
/// Bytes of a group's header
pub const GROUP_HEADER_BYTES: usize = 36;
/// Bytes of a group's checksum
pub const GROUP_SUM_BYTES: usize = size_of::<u64>();
/// Bytes a group adds around its rows
pub const GROUP_OVERHEAD_BYTES: usize = GROUP_HEADER_BYTES + GROUP_SUM_BYTES;
/// Where a row's kind sits, after its column. The key's length, the place, the offset and the length follow as varints
pub const ROW_KIND_AT: usize = 1;
/// Bytes a row's column and kind take
pub const ROW_FIXED_BYTES: usize = 2;
/// Most bytes a row takes before its key: a key length of 16 bits, then a place, an offset and a length of 64
pub const ROW_HEADER_BYTES: usize = ROW_FIXED_BYTES + KEY_LENGTH_VARINT_BYTES + 3 * MAX_VARINT_BYTES;
/// Most bytes a key length of 16 bits takes as a varint
pub const KEY_LENGTH_VARINT_BYTES: usize = 3;

/// One row, with its key borrowed
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RowEntry<'key> {
    pub column: ColumnId,
    pub key: &'key [u8],
    pub place: WritePlace,
    pub offset: FileOffset,
    pub length: ByteCount,
    pub kind: RowKind,
}

/// A group being filled, in a buffer that never grows
#[derive(Debug)]
pub struct RowGroupBuffer {
    bytes: Vec<u8>,
    used: usize,
    row_count: u32,
}

/// What a group's header says
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupHead {
    pub row_count: RowCount,
    pub total_length: ByteCount,
    pub synced_through: FileOffset,
    pub marks: MarkRange,
}

/// A whole group read back
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParsedGroup<'bytes> {
    pub rows: &'bytes [u8],
    pub row_count: RowCount,
    pub total_length: ByteCount,
    pub synced_through: FileOffset,
    pub marks: MarkRange,
}

/// A walk over a group's rows
#[derive(Clone, Copy, Debug)]
pub struct RowWalk<'bytes> {
    rows: &'bytes [u8],
}

impl RowGroupBuffer {
    /// Allocate a group of a fixed size
    pub fn new(capacity: ByteCount) -> Self {
        Self {
            bytes: touched_zeroes(capacity.0 as usize),
            used: GROUP_HEADER_BYTES,
            row_count: 0,
        }
    }

    /// Read the heap this buffer holds
    pub fn heap_bytes(&self) -> ByteCount {
        ByteCount(self.bytes.capacity() as u64)
    }

    /// Check whether the group holds no rows
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Check rows of this many bytes fit
    pub fn has_room(&self, row_bytes: usize) -> bool {
        self.used + row_bytes + GROUP_SUM_BYTES <= self.bytes.len()
    }

    /// Add a row, or return false when it doesn't fit
    pub fn push(&mut self, row: &RowEntry) -> bool {
        let size = encoded_size(row);
        if !self.has_room(size) {
            return false;
        }
        let bytes = &mut self.bytes[self.used..self.used + size];
        bytes[0] = row.column.0;
        bytes[ROW_KIND_AT] = row.kind.as_byte();
        let mut at = ROW_FIXED_BYTES;
        for number in [row.key.len() as u64, row.place.0, row.offset.0, row.length.0] {
            // the size was worked out from these numbers, so each fits
            at += write_varint(&mut bytes[at..], number).unwrap_or(0);
        }
        bytes[at..].copy_from_slice(row.key);
        self.used += size;
        self.row_count += 1;
        true
    }

    /// Count the bytes of the rows in this group
    pub fn row_bytes(&self) -> usize {
        self.used - GROUP_HEADER_BYTES
    }

    /// Walk the rows in this group
    pub fn walk(&self) -> RowWalk<'_> {
        RowWalk {
            rows: self.rows(),
        }
    }

    /// Add another group's rows, or return false when they don't fit
    pub fn append(&mut self, other: &Self) -> bool {
        let size = other.row_bytes();
        if !self.has_room(size) {
            return false;
        }
        self.bytes[self.used..self.used + size].copy_from_slice(other.rows());
        self.used += size;
        self.row_count += other.row_count;
        true
    }

    /// Write a header into this buffer
    pub fn set_head(&mut self, head: &GroupHead) {
        self.bytes[..TOTAL_LENGTH_AT].copy_from_slice(&GROUP_MAGIC.to_le_bytes());
        self.bytes[TOTAL_LENGTH_AT..ROW_COUNT_AT].copy_from_slice(&(head.total_length.0 as u32).to_le_bytes());
        self.bytes[ROW_COUNT_AT..SYNCED_THROUGH_AT].copy_from_slice(&(head.row_count.0 as u32).to_le_bytes());
        self.bytes[SYNCED_THROUGH_AT..MARKS_AT].copy_from_slice(&head.synced_through.0.to_le_bytes());
        self.bytes[MARKS_AT..MARKS_AT + size_of::<u64>()].copy_from_slice(&head.marks.lowest.0.to_le_bytes());
        self.bytes[MARKS_AT + size_of::<u64>()..GROUP_HEADER_BYTES].copy_from_slice(&head.marks.highest.0.to_le_bytes());
    }

    /// Read the header and the rows so far
    pub fn with_head(&self) -> &[u8] {
        &self.bytes[..self.used]
    }

    /// Read the rows so far
    pub fn rows(&self) -> &[u8] {
        &self.bytes[GROUP_HEADER_BYTES..self.used]
    }

    /// Close the group and return its bytes, ready to write
    pub fn finish(&mut self, synced_through: FileOffset, marks: MarkRange, record_key: RecordKey) -> &[u8] {
        let total = self.used + GROUP_SUM_BYTES;
        self.set_head(&GroupHead {
            row_count: RowCount(u64::from(self.row_count)),
            total_length: ByteCount(total as u64),
            synced_through,
            marks,
        });

        let sum = xxh3_64_with_seed(&self.bytes[..self.used], record_key.0);
        self.bytes[self.used..total].copy_from_slice(&sum.to_le_bytes());
        &self.bytes[..total]
    }

    /// Start the next group in the same buffer
    pub fn clear(&mut self) {
        self.used = GROUP_HEADER_BYTES;
        self.row_count = 0;
    }
}

impl<'bytes> ParsedGroup<'bytes> {
    /// Walk the group's rows
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn walk(&self) -> RowWalk<'bytes> {
        RowWalk { rows: self.rows }
    }
}

impl<'bytes> RowWalk<'bytes> {
    /// Walk rows at the front of some bytes, stopping short of a row that runs past them
    pub fn over(rows: &'bytes [u8]) -> Self {
        Self { rows }
    }

    /// Count the bytes the walk hasn't reached
    pub fn left(&self) -> usize {
        self.rows.len()
    }
}

impl<'bytes> Iterator for RowWalk<'bytes> {
    type Item = RowEntry<'bytes>;

    fn next(&mut self) -> Option<RowEntry<'bytes>> {
        let fixed = self.rows.get(..ROW_FIXED_BYTES)?;
        let (column, kind) = (ColumnId(fixed[0]), RowKind::from_byte(fixed[ROW_KIND_AT])?);
        let mut at = ROW_FIXED_BYTES;
        let mut numbers = [0; 4];
        for number in &mut numbers {
            let (value, used) = read_varint(self.rows.get(at..)?)?;
            *number = value;
            at += used;
        }
        let [key_length, place, offset, length] = numbers;
        let key = self.rows.get(at..at + key_length as usize)?;
        let row = RowEntry {
            column,
            key,
            place: WritePlace(place),
            offset: FileOffset(offset),
            length: ByteCount(length),
            kind,
        };
        self.rows = &self.rows[at + key_length as usize..];
        Some(row)
    }
}

/// Size a row with its key as its numbers can take at most, which a claim holds room for
pub fn row_size(key_length: usize) -> usize {
    ROW_HEADER_BYTES + key_length
}

/// Size a row as it's written
pub fn encoded_size(row: &RowEntry) -> usize {
    let numbers = [row.key.len() as u64, row.place.0, row.offset.0, row.length.0];
    ROW_FIXED_BYTES + numbers.iter().map(|number| varint_size(*number)).sum::<usize>() + row.key.len()
}

/// Count the bytes a number takes as a varint
pub fn varint_size(number: u64) -> usize {
    (u64::BITS - number.max(1).leading_zeros()).div_ceil(VALUE_BITS) as usize
}

/// Read a group's header, or nothing when these bytes can't open a group
pub fn parse_head(bytes: &[u8]) -> Option<GroupHead> {
    let header = bytes.get(..GROUP_HEADER_BYTES)?;
    if read_half(header, 0)? != GROUP_MAGIC {
        return None;
    }
    let total = read_half(header, TOTAL_LENGTH_AT)? as usize;
    if total < GROUP_OVERHEAD_BYTES {
        return None;
    }
    Some(GroupHead {
        row_count: RowCount(u64::from(read_half(header, ROW_COUNT_AT)?)),
        total_length: ByteCount(total as u64),
        synced_through: FileOffset(read_word(header, SYNCED_THROUGH_AT)?),
        marks: MarkRange {
            lowest: DeathMark(read_word(header, MARKS_AT)?),
            highest: DeathMark(read_word(header, MARKS_AT + size_of::<u64>())?),
        },
    })
}

/// Read the group at the front of the bytes, or nothing when it's torn or absent
pub fn parse_group(bytes: &[u8], record_key: RecordKey) -> Option<ParsedGroup<'_>> {
    let head = parse_head(bytes)?;
    let sum_at = head.total_length.0 as usize - GROUP_SUM_BYTES;
    let body = bytes.get(..sum_at)?;
    let sum = read_word(bytes, sum_at)?;
    if xxh3_64_with_seed(body, record_key.0) != sum {
        return None;
    }

    Some(ParsedGroup {
        rows: &body[GROUP_HEADER_BYTES..],
        row_count: head.row_count,
        total_length: head.total_length,
        synced_through: head.synced_through,
        marks: head.marks,
    })
}
