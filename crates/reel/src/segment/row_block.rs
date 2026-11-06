//! One block of a footer's rows, with shared key prefixes and restart points

use reel_api::units::{ByteCount, FileOffset, RowCount, WritePlace};
use xxhash_rust::xxh3::xxh3_64;

use crate::pool::{touched_capacity, touched_zeroes};
use crate::little_endian::{read_half, read_word};
use crate::row_kind::RowKind;
use crate::segment::varint::{MAX_VARINT_BYTES, read_varint, write_varint};

/// Most bytes in a row block
pub const ROW_BLOCK_BYTES: usize = 64 * 1024;
/// Rows between restart points, where a key is stored whole
pub const RESTART_INTERVAL: u32 = 16;
/// Most restart points a block can hold
pub const MAX_RESTARTS: usize = ROW_BLOCK_BYTES / 2 / size_of::<u16>();
/// Bytes of the row count at the end of a block
pub const COUNT_BYTES: usize = size_of::<u32>();
/// Bytes of the restart count at the end of a block
pub const RESTART_COUNT_BYTES: usize = size_of::<u16>();
/// Bytes of the checksum at the end of a block
pub const BLOCK_SUM_BYTES: usize = size_of::<u64>();
/// Bytes at the end of a block beside its restart points
pub const BLOCK_TRAILER_BYTES: usize = COUNT_BYTES + RESTART_COUNT_BYTES + BLOCK_SUM_BYTES;

/// Bits the kind takes under the shared length, in a row's first number
pub const KIND_BITS: u32 = 2;
/// Picks the kind out of a row's first number
pub const KIND_MASK: u64 = (1 << KIND_BITS) - 1;

/// Opens every row block, so a walk with no index can find one
pub const BLOCK_MARKER: u32 = 0x524C_4248;
/// Bytes of a row block's header
pub const BLOCK_HEADER_BYTES: usize = 24;
/// Where the header keeps the row block's length
pub const HEADER_BLOCK_LENGTH_AT: usize = 4;
/// Where the header keeps the filter's length
pub const HEADER_FILTER_LENGTH_AT: usize = 8;
/// Where the header keeps the base its rows' places count from
pub const HEADER_PLACE_BASE_AT: usize = 12;
/// Where the header keeps its own checksum
pub const HEADER_SUM_AT: usize = 20;

/// What a row block's header says
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockHeader {
    pub block_length: u32,
    pub filter_length: u32,
    pub place_base: WritePlace,
}

/// A row as a footer stores it
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FooterRow {
    pub place: WritePlace,
    pub offset: FileOffset,
    pub length: ByteCount,
    pub kind: RowKind,
}

/// A block being filled, in buffers that never grow
#[derive(Debug)]
pub struct RowBlockBuilder {
    bytes: Vec<u8>,
    used: usize,
    restarts: Vec<u16>,
    last_key: Vec<u8>,
    row_count: u32,
    place_base: WritePlace,
}

/// A walk over a checked block's rows, keeping the current key in a buffer the caller lends
#[derive(Debug)]
pub struct RowBlockWalk<'block> {
    rows: &'block [u8],
    restarts: &'block [u8],
    position: usize,
    place_base: WritePlace,
    row_count: usize,
}

impl RowBlockBuilder {
    /// Allocate the buffers
    pub fn new() -> Self {
        Self {
            bytes: touched_zeroes(ROW_BLOCK_BYTES),
            used: 0,
            restarts: touched_capacity(MAX_RESTARTS, 0),
            last_key: touched_capacity(ROW_BLOCK_BYTES, 0),
            row_count: 0,
            place_base: WritePlace(0),
        }
    }

    /// Read the heap these buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        let restarts = self.restarts.capacity() * size_of::<u16>();
        ByteCount((self.bytes.capacity() + restarts + self.last_key.capacity()) as u64)
    }

    /// Set the place every row of the next blocks stores its distance from
    pub fn set_place_base(&mut self, place_base: WritePlace) {
        self.place_base = place_base;
    }

    /// Count the rows so far
    pub fn row_count(&self) -> RowCount {
        RowCount(u64::from(self.row_count))
    }

    /// Check whether the block holds no rows
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Add a row, or return false when the block is full
    pub fn push(&mut self, key: &[u8], row: &FooterRow) -> bool {
        let is_restart = self.row_count.is_multiple_of(RESTART_INTERVAL);
        let shared = if is_restart { 0 } else { shared_length(&self.last_key, key) };
        let suffix = &key[shared..];

        let restarts_after = self.restarts.len() + usize::from(is_restart);
        let trailer = restarts_after * size_of::<u16>() + BLOCK_TRAILER_BYTES;
        let most_row_bytes = 5 * MAX_VARINT_BYTES + suffix.len();
        if self.used + most_row_bytes + trailer > ROW_BLOCK_BYTES {
            return false;
        }

        if is_restart {
            self.restarts.push(self.used as u16);
        }
        self.put_varint(((shared as u64) << KIND_BITS) | u64::from(row.kind.as_byte()));
        self.put_varint(suffix.len() as u64);
        self.bytes[self.used..self.used + suffix.len()].copy_from_slice(suffix);
        self.used += suffix.len();
        self.put_varint(row.place.0 - self.place_base.0);
        self.put_varint(row.offset.0);
        self.put_varint(row.length.0);

        self.last_key.truncate(shared);
        self.last_key.extend_from_slice(suffix);
        self.row_count += 1;
        true
    }

    /// Append one number, which the room check has already allowed for
    pub fn put_varint(&mut self, number: u64) {
        if let Some(used) = write_varint(&mut self.bytes[self.used..], number) {
            self.used += used;
        }
    }

    /// Close the block and return its bytes
    pub fn finish(&mut self) -> &[u8] {
        for restart in &self.restarts {
            self.bytes[self.used..self.used + size_of::<u16>()].copy_from_slice(&restart.to_le_bytes());
            self.used += size_of::<u16>();
        }
        self.bytes[self.used..self.used + COUNT_BYTES].copy_from_slice(&self.row_count.to_le_bytes());
        self.used += COUNT_BYTES;
        let restarts = self.restarts.len() as u16;
        self.bytes[self.used..self.used + RESTART_COUNT_BYTES].copy_from_slice(&restarts.to_le_bytes());
        self.used += RESTART_COUNT_BYTES;

        let sum = xxh3_64(&self.bytes[..self.used]);
        self.bytes[self.used..self.used + BLOCK_SUM_BYTES].copy_from_slice(&sum.to_le_bytes());
        self.used += BLOCK_SUM_BYTES;
        &self.bytes[..self.used]
    }

    /// Start the next block in the same buffers
    pub fn clear(&mut self) {
        self.used = 0;
        self.restarts.clear();
        self.last_key.clear();
        self.row_count = 0;
    }
}

impl Default for RowBlockBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl<'block> RowBlockWalk<'block> {
    /// Check a stored block and start at its first row, or return nothing when it's damaged
    pub fn open(block: &'block [u8], place_base: WritePlace) -> Option<Self> {
        let sum_at = block.len().checked_sub(BLOCK_SUM_BYTES)?;
        let mut sum = [0; BLOCK_SUM_BYTES];
        sum.copy_from_slice(&block[sum_at..]);
        if xxh3_64(&block[..sum_at]) != u64::from_le_bytes(sum) {
            return None;
        }

        let count_at = sum_at.checked_sub(RESTART_COUNT_BYTES)?;
        let restarts = u16::from_le_bytes([block[count_at], block[count_at + 1]]) as usize;
        let rows_count_at = count_at.checked_sub(COUNT_BYTES)?;
        let restarts_at = rows_count_at.checked_sub(restarts * size_of::<u16>())?;
        let row_count = read_half(block, rows_count_at)? as usize;
        Some(Self {
            rows: &block[..restarts_at],
            restarts: &block[restarts_at..rows_count_at],
            position: 0,
            place_base,
            row_count,
        })
    }

    /// Start at the first row of a block already checked once
    pub fn open_checked(block: &'block [u8], place_base: WritePlace) -> Option<Self> {
        let sum_at = block.len().checked_sub(BLOCK_SUM_BYTES)?;
        let count_at = sum_at.checked_sub(RESTART_COUNT_BYTES)?;
        let restarts = u16::from_le_bytes([block[count_at], block[count_at + 1]]) as usize;
        let rows_count_at = count_at.checked_sub(COUNT_BYTES)?;
        let restarts_at = rows_count_at.checked_sub(restarts * size_of::<u16>())?;
        let row_count = read_half(block, rows_count_at)? as usize;
        Some(Self {
            rows: &block[..restarts_at],
            restarts: &block[restarts_at..rows_count_at],
            position: 0,
            place_base,
            row_count,
        })
    }

    /// Read how far the walk has come, to resume it later
    pub fn position(&self) -> usize {
        self.position
    }

    /// Go on from where another walk stopped, with its key still in the buffer
    pub fn resume(&mut self, position: usize) {
        self.position = position;
    }

    /// Count the restart points
    pub fn restart_count(&self) -> usize {
        self.restarts.len() / size_of::<u16>()
    }

    /// Read a restart point's position
    pub fn restart_at(&self, index: usize) -> usize {
        let at = index * size_of::<u16>();
        u16::from_le_bytes([self.restarts[at], self.restarts[at + 1]]) as usize
    }

    /// Fill the key buffer with the next row's key and return its row, or nothing at the end
    pub fn next_row(&mut self, key: &mut Vec<u8>) -> Option<FooterRow> {
        let bytes = self.rows.get(self.position..)?;
        if bytes.is_empty() {
            return None;
        }

        let (first, shared_bytes) = read_varint(bytes)?;
        let shared = first >> KIND_BITS;
        let kind = RowKind::from_byte((first & KIND_MASK) as u8)?;
        let (suffix_length, suffix_bytes) = read_varint(bytes.get(shared_bytes..)?)?;
        let suffix_at = shared_bytes + suffix_bytes;
        let suffix = bytes.get(suffix_at..suffix_at + suffix_length as usize)?;
        key.truncate(shared as usize);
        key.extend_from_slice(suffix);

        let place_at = suffix_at + suffix_length as usize;
        let (place, place_bytes) = read_varint(bytes.get(place_at..)?)?;
        let offset_at = place_at + place_bytes;
        let (offset, offset_bytes) = read_varint(bytes.get(offset_at..)?)?;
        let length_at = offset_at + offset_bytes;
        let (length, length_bytes) = read_varint(bytes.get(length_at..)?)?;

        self.position += length_at + length_bytes;
        Some(FooterRow {
            place: WritePlace(self.place_base.0 + place),
            offset: FileOffset(offset),
            length: ByteCount(length),
            kind,
        })
    }

    /// Count the rows of a restart point's run
    pub fn rows_at_restart(&self, index: usize) -> usize {
        let run = RESTART_INTERVAL as usize;
        self.row_count.saturating_sub(index * run).min(run)
    }

    /// Read a row of a run by its place in the run, leaving its key in the buffer
    pub fn row_in_run(&mut self, restart: usize, index: usize, key: &mut Vec<u8>) -> Option<FooterRow> {
        let mut row = self.key_at_restart(restart, key)?;
        for _row in 0..index {
            row = self.next_row(key)?;
        }
        Some(row)
    }

    /// Find the last run with a row at or before a key, and count that run's rows there, or return nothing when no row is
    pub fn seek_back(&mut self, target: &[u8], is_inclusive: bool, key: &mut Vec<u8>) -> Option<(usize, usize)> {
        let is_before = |row_key: &[u8]| row_key < target || (is_inclusive && row_key == target);
        if self.restart_count() == 0 {
            return None;
        }
        self.key_at_restart(0, key)?;
        if !is_before(key) {
            return None;
        }

        let mut low = 0;
        let mut high = self.restart_count();
        while low + 1 < high {
            let middle = (low + high) / 2;
            self.key_at_restart(middle, key)?;
            if is_before(key) {
                low = middle;
            } else {
                high = middle;
            }
        }

        self.key_at_restart(low, key)?;
        let mut rows = 1;
        while rows < self.rows_at_restart(low) {
            self.next_row(key)?;
            if !is_before(key) {
                break;
            }
            rows += 1;
        }
        Some((low, rows))
    }

    /// Read the key at a restart point, leaving the walk there
    pub fn key_at_restart(&mut self, index: usize, key: &mut Vec<u8>) -> Option<FooterRow> {
        self.position = self.restart_at(index);
        key.clear();
        self.next_row(key)
    }

    /// Move to the first row at or after a key, return its row, and leave its key in the buffer
    pub fn seek(&mut self, target: &[u8], key: &mut Vec<u8>) -> Option<FooterRow> {
        if self.restart_count() == 0 {
            return None;
        }

        let mut low = 0;
        let mut high = self.restart_count();
        while low + 1 < high {
            let middle = (low + high) / 2;
            self.key_at_restart(middle, key)?;
            if key.as_slice() < target {
                low = middle;
            } else {
                high = middle;
            }
        }

        let mut row = self.key_at_restart(low, key)?;
        while key.as_slice() < target {
            row = self.next_row(key)?;
        }
        Some(row)
    }
}

/// Lay out a row block's header
pub fn write_block_header(header: &BlockHeader) -> [u8; BLOCK_HEADER_BYTES] {
    let mut bytes = [0; BLOCK_HEADER_BYTES];
    bytes[..HEADER_BLOCK_LENGTH_AT].copy_from_slice(&BLOCK_MARKER.to_le_bytes());
    bytes[HEADER_BLOCK_LENGTH_AT..HEADER_FILTER_LENGTH_AT].copy_from_slice(&header.block_length.to_le_bytes());
    bytes[HEADER_FILTER_LENGTH_AT..HEADER_PLACE_BASE_AT].copy_from_slice(&header.filter_length.to_le_bytes());
    bytes[HEADER_PLACE_BASE_AT..HEADER_SUM_AT].copy_from_slice(&header.place_base.0.to_le_bytes());
    let sum = xxh3_64(&bytes[..HEADER_SUM_AT]) as u32;
    bytes[HEADER_SUM_AT..].copy_from_slice(&sum.to_le_bytes());
    bytes
}

/// Read a row block's header, or nothing when it fails its check
pub fn read_block_header(bytes: &[u8]) -> Option<BlockHeader> {
    let header = bytes.get(..BLOCK_HEADER_BYTES)?;
    if read_half(header, 0)? != BLOCK_MARKER {
        return None;
    }
    if read_half(header, HEADER_SUM_AT)? != xxh3_64(&header[..HEADER_SUM_AT]) as u32 {
        return None;
    }
    Some(BlockHeader {
        block_length: read_half(header, HEADER_BLOCK_LENGTH_AT)?,
        filter_length: read_half(header, HEADER_FILTER_LENGTH_AT)?,
        place_base: WritePlace(read_word(header, HEADER_PLACE_BASE_AT)?),
    })
}

/// Count the bytes two keys share at the front
pub fn shared_length(left: &[u8], right: &[u8]) -> usize {
    let mut shared = 0;
    for (left_byte, right_byte) in left.iter().zip(right.iter()) {
        if left_byte != right_byte {
            break;
        }
        shared += 1;
    }
    shared
}
