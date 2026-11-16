//! One row block's line in a segment's resident index

use reel_core::units::{ByteCount, FileOffset};

use crate::little_endian::read_half;
use crate::segment::row_block::BLOCK_HEADER_BYTES;

/// Bytes of key a block index line keeps
pub const KEY_PREFIX_BYTES: usize = 16;
/// Bytes of a column's shared prefix a tail keeps, so a range can find a segment by its keys
pub const SHARED_HEAD_BYTES: usize = 48;
/// Bytes of a key a range bound keeps
pub const KEY_BOUND_BYTES: usize = SHARED_HEAD_BYTES + KEY_PREFIX_BYTES;
/// Bytes of one line on disk
pub const BLOCK_ENTRY_BYTES: usize = 28;
/// Where the block's offset sits in a line
pub const BLOCK_OFFSET_AT: usize = KEY_PREFIX_BYTES;
/// Where the filter's offset sits in a line
pub const FILTER_OFFSET_AT: usize = 20;
/// Where the live row count sits in a line
pub const LIVE_ROWS_AT: usize = 24;

/// The bytes of a key past its column's shared prefix, padded with zeros
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct KeyPrefix(pub [u8; KEY_PREFIX_BYTES]);

/// The first bytes of a column's shared prefix, padded with zeros
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SharedHead(pub [u8; SHARED_HEAD_BYTES]);

/// A key's first bytes, padded with zeros. Bounds keep key order, and two keys that differ past them compare as equal, so a
/// range built from them can only be wider than the truth
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct KeyBound(pub [u8; KEY_BOUND_BYTES]);

/// Where one row block and its filter sit, and how many of its rows are live
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BlockEntry {
    pub first_key: KeyPrefix,
    pub block_offset: u32,
    pub filter_offset: u32,
    pub live_rows: u32,
}

impl KeyPrefix {
    /// Take a key's prefix past some shared bytes
    pub fn of_key(key: &[u8], shared: usize) -> Self {
        let mut bytes = [0; KEY_PREFIX_BYTES];
        if let Some(rest) = key.get(shared..) {
            let length = rest.len().min(KEY_PREFIX_BYTES);
            bytes[..length].copy_from_slice(&rest[..length]);
        }
        Self(bytes)
    }
}

impl SharedHead {
    /// Take the first bytes of a shared prefix
    pub fn of_shared(shared: &[u8]) -> Self {
        let mut bytes = [0; SHARED_HEAD_BYTES];
        let length = shared.len().min(SHARED_HEAD_BYTES);
        bytes[..length].copy_from_slice(&shared[..length]);
        Self(bytes)
    }
}

impl Default for SharedHead {
    fn default() -> Self {
        Self([0; SHARED_HEAD_BYTES])
    }
}

impl KeyBound {
    /// The bound above every key
    pub const HIGHEST: Self = Self([u8::MAX; KEY_BOUND_BYTES]);

    /// Take a key's first bytes
    pub fn of_key(key: &[u8]) -> Self {
        let mut bytes = [0; KEY_BOUND_BYTES];
        let length = key.len().min(KEY_BOUND_BYTES);
        bytes[..length].copy_from_slice(&key[..length]);
        Self(bytes)
    }

    /// Take the bound above every key that starts with a key's bytes, for a key a summary row cut short
    pub fn past_prefix(key: &[u8]) -> Self {
        let mut bytes = [u8::MAX; KEY_BOUND_BYTES];
        let length = key.len().min(KEY_BOUND_BYTES);
        bytes[..length].copy_from_slice(&key[..length]);
        Self(bytes)
    }
}

impl BlockEntry {
    /// Write a line into its bytes
    pub fn write(&self, out: &mut [u8]) {
        out[..KEY_PREFIX_BYTES].copy_from_slice(&self.first_key.0);
        out[BLOCK_OFFSET_AT..FILTER_OFFSET_AT].copy_from_slice(&self.block_offset.to_le_bytes());
        out[FILTER_OFFSET_AT..LIVE_ROWS_AT].copy_from_slice(&self.filter_offset.to_le_bytes());
        out[LIVE_ROWS_AT..BLOCK_ENTRY_BYTES].copy_from_slice(&self.live_rows.to_le_bytes());
    }

    /// Read a line back from its bytes
    pub fn read(bytes: &[u8]) -> Option<Self> {
        let line = bytes.get(..BLOCK_ENTRY_BYTES)?;
        let mut first_key = [0; KEY_PREFIX_BYTES];
        first_key.copy_from_slice(&line[..KEY_PREFIX_BYTES]);
        Some(Self {
            first_key: KeyPrefix(first_key),
            block_offset: read_half(line, BLOCK_OFFSET_AT)?,
            filter_offset: read_half(line, FILTER_OFFSET_AT)?,
            live_rows: read_half(line, LIVE_ROWS_AT)?,
        })
    }

    /// Check the entry can describe a block: its header past `after`, then the block, no longer than `most_length`, then its
    /// filter. A sum only shows the writer wrote these bytes, and a writer bug's offsets would underflow the reads below
    pub fn is_shaped(&self, after: u64, most_length: usize) -> bool {
        let header = u64::from(self.block_offset).checked_sub(BLOCK_HEADER_BYTES as u64);
        let length = self.filter_offset.checked_sub(self.block_offset);
        header.is_some_and(|header| header >= after) && length.is_some_and(|length| length > 0 && length as usize <= most_length)
    }

    /// Find where the block's header starts, which is where the filter before it ends
    pub fn header_at(&self) -> FileOffset {
        FileOffset(u64::from(self.block_offset) - BLOCK_HEADER_BYTES as u64)
    }

    /// Find where the row block starts
    pub fn block_at(&self) -> FileOffset {
        FileOffset(u64::from(self.block_offset))
    }

    /// Measure the row block, which ends where its filter starts
    pub fn block_length(&self) -> ByteCount {
        ByteCount(u64::from(self.filter_offset - self.block_offset))
    }
}
