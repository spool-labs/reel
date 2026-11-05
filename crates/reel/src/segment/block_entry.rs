//! One row block's line in a segment's resident index

use reel_api::units::{ByteCount, FileOffset};

use crate::segment::row_block::BLOCK_HEADER_BYTES;

/// Bytes of key a block index line keeps
pub const KEY_PREFIX_BYTES: usize = 16;
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

/// Read a little-endian half word
pub fn read_half(bytes: &[u8], at: usize) -> Option<u32> {
    let slice = bytes.get(at..at + size_of::<u32>())?;
    let mut half = [0; size_of::<u32>()];
    half.copy_from_slice(slice);
    Some(u32::from_le_bytes(half))
}
