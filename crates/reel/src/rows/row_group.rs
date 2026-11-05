//! One group in a rows file: a header, a row count, the rows and a checksum

use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, FileOffset, RowCount, WritePlace};
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::little_endian::{read_half, read_word};
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
/// Bytes of a group's header
pub const GROUP_HEADER_BYTES: usize = 20;
/// Bytes of a group's checksum
pub const GROUP_SUM_BYTES: usize = size_of::<u64>();
/// Bytes a group adds around its rows
pub const GROUP_OVERHEAD_BYTES: usize = GROUP_HEADER_BYTES + GROUP_SUM_BYTES;
/// Where a row's kind sits
pub const ROW_KIND_AT: usize = 1;
/// Where a row's key length sits
pub const ROW_KEY_LENGTH_AT: usize = 2;
/// Where a row's place sits
pub const ROW_PLACE_AT: usize = 4;
/// Where a row's offset sits
pub const ROW_OFFSET_AT: usize = 12;
/// Where a row's length sits
pub const ROW_LENGTH_AT: usize = 20;
/// Bytes of a row before its key
pub const ROW_HEADER_BYTES: usize = 28;

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

/// A whole group read back
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParsedGroup<'bytes> {
    pub rows: &'bytes [u8],
    pub row_count: RowCount,
    pub total_length: ByteCount,
    pub synced_through: FileOffset,
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
            bytes: vec![0; capacity.0 as usize],
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
        let size = row_size(row.key.len());
        if !self.has_room(size) {
            return false;
        }

        let bytes = &mut self.bytes[self.used..self.used + size];
        bytes[0] = row.column.0;
        bytes[ROW_KIND_AT] = row.kind.as_byte();
        let key_length = row.key.len() as u16;
        bytes[ROW_KEY_LENGTH_AT..ROW_PLACE_AT].copy_from_slice(&key_length.to_le_bytes());
        bytes[ROW_PLACE_AT..ROW_OFFSET_AT].copy_from_slice(&row.place.0.to_le_bytes());
        bytes[ROW_OFFSET_AT..ROW_LENGTH_AT].copy_from_slice(&row.offset.0.to_le_bytes());
        bytes[ROW_LENGTH_AT..ROW_HEADER_BYTES].copy_from_slice(&row.length.0.to_le_bytes());
        bytes[ROW_HEADER_BYTES..].copy_from_slice(row.key);

        self.used += size;
        self.row_count += 1;
        true
    }

    /// Close the group and return its bytes, ready to write
    pub fn finish(&mut self, synced_through: FileOffset, record_key: RecordKey) -> &[u8] {
        let total = self.used + GROUP_SUM_BYTES;
        self.bytes[..TOTAL_LENGTH_AT].copy_from_slice(&GROUP_MAGIC.to_le_bytes());
        self.bytes[TOTAL_LENGTH_AT..ROW_COUNT_AT].copy_from_slice(&(total as u32).to_le_bytes());
        self.bytes[ROW_COUNT_AT..SYNCED_THROUGH_AT].copy_from_slice(&self.row_count.to_le_bytes());
        self.bytes[SYNCED_THROUGH_AT..GROUP_HEADER_BYTES].copy_from_slice(&synced_through.0.to_le_bytes());

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
    pub fn walk(&self) -> RowWalk<'bytes> {
        RowWalk { rows: self.rows }
    }
}

impl<'bytes> Iterator for RowWalk<'bytes> {
    type Item = RowEntry<'bytes>;

    fn next(&mut self) -> Option<RowEntry<'bytes>> {
        let header = self.rows.get(..ROW_HEADER_BYTES)?;
        let key_length = u16::from_le_bytes([header[ROW_KEY_LENGTH_AT], header[ROW_KEY_LENGTH_AT + 1]]) as usize;
        let key = self.rows.get(ROW_HEADER_BYTES..ROW_HEADER_BYTES + key_length)?;

        let row = RowEntry {
            column: ColumnId(header[0]),
            key,
            place: WritePlace(read_word(header, ROW_PLACE_AT)?),
            offset: FileOffset(read_word(header, ROW_OFFSET_AT)?),
            length: ByteCount(read_word(header, ROW_LENGTH_AT)?),
            kind: RowKind::from_byte(header[ROW_KIND_AT])?,
        };
        self.rows = &self.rows[ROW_HEADER_BYTES + key_length..];
        Some(row)
    }
}

/// Size a row with its key
pub fn row_size(key_length: usize) -> usize {
    ROW_HEADER_BYTES + key_length
}

/// Read the group at the front of the bytes, or nothing when it's torn or absent
pub fn parse_group(bytes: &[u8], record_key: RecordKey) -> Option<ParsedGroup<'_>> {
    let header = bytes.get(..GROUP_HEADER_BYTES)?;
    if read_half(header, 0)? != GROUP_MAGIC {
        return None;
    }

    let total = read_half(header, TOTAL_LENGTH_AT)? as usize;
    let sum_at = total.checked_sub(GROUP_SUM_BYTES)?;
    let body = bytes.get(..sum_at)?;
    if sum_at < GROUP_HEADER_BYTES {
        return None;
    }
    let sum = read_word(bytes, sum_at)?;
    if xxh3_64_with_seed(body, record_key.0) != sum {
        return None;
    }

    Some(ParsedGroup {
        rows: &body[GROUP_HEADER_BYTES..],
        row_count: RowCount(u64::from(read_half(header, ROW_COUNT_AT)?)),
        total_length: ByteCount(total as u64),
        synced_through: FileOffset(read_word(header, SYNCED_THROUGH_AT)?),
    })
}
