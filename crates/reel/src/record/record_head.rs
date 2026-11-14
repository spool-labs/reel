//! The bytes at the front of every record

use reel_api::units::ByteCount;

use crate::little_endian::read_word;
use crate::record::chunk_table::{ChunkCount, TablePageIndex, page_count, table_bytes};
use crate::record::codec::{CHUNK_BYTES, Codec};
use crate::record::record_check::{Checksum, RecordCheck, checksum};

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

/// Bytes of one piece of a chunk stored as written. Each piece has its own sum, so a window reads and checks only the pieces
/// it touches
pub const PIECE_BYTES: usize = 4 * 1024;
/// Bytes of one piece's sum
pub const PIECE_SUM_BYTES: usize = size_of::<u64>();
/// Pieces in a full chunk
pub const PIECES_PER_CHUNK: usize = CHUNK_BYTES / PIECE_BYTES;

/// Count the pieces of a value stored as written
pub fn piece_count(value_length: ByteCount) -> u64 {
    value_length.0.div_ceil(PIECE_BYTES as u64)
}

/// Count the bytes of the piece sums a chunked record stored as written keeps between its table and its payload
pub fn sums_bytes(value_length: ByteCount) -> u64 {
    piece_count(value_length) * PIECE_SUM_BYTES as u64
}

/// Write a chunk's piece sums into a buffer and return their length
pub fn put_piece_sums(chunk: &[u8], sums: &mut [u8]) -> usize {
    let mut length = 0;
    for piece in chunk.chunks(PIECE_BYTES) {
        sums[length..length + PIECE_SUM_BYTES].copy_from_slice(&checksum(piece).0.to_le_bytes());
        length += PIECE_SUM_BYTES;
    }
    length
}

/// Sum a chunk stored as written: the sum over its pieces' sums, which its table entry keeps, so a whole read checks a chunk
/// from its bytes alone and a window checks the stored piece sums against it
pub fn chunk_sum(chunk: &[u8]) -> Checksum {
    let mut sums = [0; PIECES_PER_CHUNK * PIECE_SUM_BYTES];
    let length = put_piece_sums(chunk, &mut sums);
    checksum(&sums[..length])
}

/// Count the bytes of a record stored as written, with no need to hash the value
pub fn plain_length(value_length: ByteCount) -> ByteCount {
    if value_length.0 <= CHUNK_BYTES as u64 {
        return ByteCount(SMALL_HEAD_BYTES as u64 + value_length.0);
    }
    let head = chunked_head_bytes(value_length) as u64;
    ByteCount(head + table_bytes(chunk_count(value_length)).0 + sums_bytes(value_length) + value_length.0)
}
