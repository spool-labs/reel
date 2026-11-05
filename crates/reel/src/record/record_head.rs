//! The bytes at the front of every record

use reel_api::units::ByteCount;

use crate::little_endian::read_word;
use crate::record::chunk_table::{ChunkCount, TablePageIndex, page_count};
use crate::record::codec::{CHUNK_BYTES, Codec};
use crate::record::record_check::{Checksum, RecordCheck};

/// Bytes of the check
pub const CHECK_BYTES: usize = 8;
/// Where the form byte sits
pub const FORM_AT: usize = CHECK_BYTES;
/// Bytes in front of a one-chunk record's payload
pub const SMALL_HEAD_BYTES: usize = FORM_AT + 1;
/// Where a chunked record keeps its value length
pub const VALUE_LENGTH_AT: usize = SMALL_HEAD_BYTES;
/// Where a chunked record's page checksums start
pub const PAGE_SUMS_AT: usize = VALUE_LENGTH_AT + size_of::<u64>();
/// Bytes of one page checksum
pub const PAGE_SUM_BYTES: usize = size_of::<u64>();
/// Set in the form byte of a chunked record
pub const CHUNKED_FLAG: u8 = 0x80;

/// What the form byte says: the codec, and whether a table follows
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecordForm {
    pub codec: Codec,
    pub is_chunked: bool,
}

impl RecordForm {
    /// Read the byte that stores this form
    pub fn as_byte(self) -> u8 {
        let flag = if self.is_chunked { CHUNKED_FLAG } else { 0 };
        self.codec.as_byte() | flag
    }

    /// Read a stored form back
    pub fn from_byte(byte: u8) -> Option<Self> {
        Some(Self {
            codec: Codec::from_byte(byte & !CHUNKED_FLAG)?,
            is_chunked: byte & CHUNKED_FLAG != 0,
        })
    }
}

/// Count the chunks of a value
pub fn chunk_count(value_length: ByteCount) -> ChunkCount {
    ChunkCount(value_length.0.div_ceil(CHUNK_BYTES as u64))
}

/// Count the bytes of a chunked record's head
pub fn chunked_head_bytes(value_length: ByteCount) -> usize {
    head_bytes_for(chunk_count(value_length))
}

/// Count the bytes of a head over a number of chunks
pub fn head_bytes_for(chunks: ChunkCount) -> usize {
    PAGE_SUMS_AT + page_count(chunks).0 as usize * PAGE_SUM_BYTES
}

/// Read the check at the front of a record
pub fn read_check(bytes: &[u8]) -> Option<RecordCheck> {
    Some(RecordCheck(read_word(bytes, 0)?))
}

/// Read a chunked record's value length
pub fn read_value_length(bytes: &[u8]) -> Option<ByteCount> {
    Some(ByteCount(read_word(bytes, VALUE_LENGTH_AT)?))
}

/// Read one page's checksum from a chunked head
pub fn read_page_sum(bytes: &[u8], page: TablePageIndex) -> Option<Checksum> {
    Some(Checksum(read_word(bytes, PAGE_SUMS_AT + page.0 as usize * PAGE_SUM_BYTES)?))
}
