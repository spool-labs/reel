//! Lays a value out as a record, in pooled buffers so a put allocates nothing

use std::io::IoSlice;

use reel_api::units::{ByteCount, FileOffset, WritePlace};

use crate::pool::touched_zeroes;
use crate::record::chunk_table::{
    ChunkCount, ChunkEntry, ENTRIES_PER_PAGE, PAGE_END_AT, TABLE_PAGE_BYTES, TablePageIndex, page_bytes, page_count,
    table_bytes, write_entry,
};
use crate::record::codec::{CHUNK_BYTES, Codec};
use crate::record::lz4_block::Lz4State;
use crate::record::record_check::{RecordCheck, RecordIdentity, RecordKey, checksum, place_check, record_check};
use crate::record::record_head::{
    CHECK_BYTES, FORM_AT, PAGE_SUM_BYTES, PAGE_SUMS_AT, RecordForm, SMALL_HEAD_BYTES, VALUE_LENGTH_AT, chunk_count,
    chunked_head_bytes,
};

/// Parts of a record: head, table and payload
pub const RECORD_PARTS: usize = 3;

/// The lengths of a laid-out record
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecordShape {
    pub head_length: usize,
    pub table_length: usize,
    pub payload_length: usize,
    pub is_payload_stored: bool,
}

/// A one-chunk record stored as written, whose head fits the stack, so its writer needs no shared encoder
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlainRecord {
    check: RecordCheck,
}

impl PlainRecord {
    /// The form of every such record
    pub const FORM: RecordForm = RecordForm {
        codec: Codec::None,
        is_chunked: false,
    };

    /// Hash a value under its identity
    pub fn new(identity: &RecordIdentity, value: &[u8]) -> Self {
        Self {
            check: record_check(identity, Self::FORM, checksum(value)),
        }
    }

    /// Build the head for the place a claim gave this write
    pub fn head(&self, record_key: RecordKey, place: WritePlace) -> [u8; SMALL_HEAD_BYTES] {
        let mut head = [0; SMALL_HEAD_BYTES];
        head[..CHECK_BYTES].copy_from_slice(&place_check(self.check, record_key, place).0.to_le_bytes());
        head[FORM_AT] = Self::FORM.as_byte();
        head
    }
}

/// The buffers one writer borrows from the pool
#[derive(Debug)]
pub struct RecordEncoder {
    head: Vec<u8>,
    table: Vec<u8>,
    stored: Vec<u8>,
    lz4: Lz4State,
    check: RecordCheck,
}

impl RecordShape {
    /// Read the record's whole length
    pub fn total_length(&self) -> ByteCount {
        ByteCount((self.head_length + self.table_length + self.payload_length) as u64)
    }
}

impl RecordEncoder {
    /// Allocate buffers for values up to a length and stored payloads up to a length
    pub fn new(largest_value: ByteCount, largest_stored: ByteCount) -> Self {
        let chunks = chunk_count(largest_value);
        let stored = (largest_stored.0 as usize).max(CHUNK_BYTES);
        Self {
            head: touched_zeroes(chunked_head_bytes(largest_value)),
            table: touched_zeroes(page_count(chunks).0 as usize * TABLE_PAGE_BYTES),
            stored: touched_zeroes(stored),
            lz4: Lz4State::new(),
            check: RecordCheck(0),
        }
    }

    /// Read the heap these buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        ByteCount((self.head.capacity() + self.table.capacity() + self.stored.capacity()) as u64)
    }

    /// Lay a value out, or return nothing when it doesn't fit these buffers
    pub fn encode(&mut self, identity: &RecordIdentity, codec: Codec, value: &[u8]) -> Option<RecordShape> {
        if value.len() <= CHUNK_BYTES {
            return Some(self.encode_small(identity, codec, value));
        }
        self.encode_chunked(identity, codec, value)
    }

    /// Lay out a value of one chunk: a head, then the payload
    pub fn encode_small(&mut self, identity: &RecordIdentity, codec: Codec, value: &[u8]) -> RecordShape {
        let compressed = match codec {
            Codec::None => None,
            Codec::Lz4 => self.lz4.compress(value, &mut self.stored),
        };

        let (stored_codec, payload) = match compressed {
            Some(length) => (Codec::Lz4, &self.stored[..length]),
            None => (Codec::None, value),
        };
        let form = RecordForm {
            codec: stored_codec,
            is_chunked: false,
        };
        let sum = checksum(payload);
        let payload_length = payload.len();
        self.check = record_check(identity, form, sum);
        self.set_place(identity.record_key, identity.place);
        self.head[FORM_AT] = form.as_byte();
        RecordShape {
            head_length: SMALL_HEAD_BYTES,
            table_length: 0,
            payload_length,
            is_payload_stored: compressed.is_some(),
        }
    }

    /// Lay out a value of many chunks: a head, a table, then the chunks
    pub fn encode_chunked(&mut self, identity: &RecordIdentity, codec: Codec, value: &[u8]) -> Option<RecordShape> {
        let value_length = ByteCount(value.len() as u64);
        let chunks = chunk_count(value_length);
        let head_length = chunked_head_bytes(value_length);
        let table_length = table_bytes(chunks).0 as usize;
        if head_length > self.head.len() || table_length > self.table.len() {
            return None;
        }

        let payload_start = (head_length + table_length) as u64;
        let payload_length = match codec {
            Codec::None => self.table_as_written(value, payload_start),
            Codec::Lz4 => self.table_compressed(value, payload_start)?,
        };

        // a record nothing shrank in reads as one run, like a none column's
        let stored_codec = if payload_length == value.len() { Codec::None } else { codec };
        self.seal_table(chunks, payload_start + payload_length as u64);
        self.write_chunked_head(identity, stored_codec, value_length, chunks, head_length);

        Some(RecordShape {
            head_length,
            table_length,
            payload_length,
            is_payload_stored: stored_codec == Codec::Lz4,
        })
    }

    /// List chunks stored as written, and return the payload length
    pub fn table_as_written(&mut self, value: &[u8], payload_start: u64) -> usize {
        let mut offset = payload_start;
        for (index, chunk) in value.chunks(CHUNK_BYTES).enumerate() {
            self.put_entry(index, offset, chunk);
            offset += chunk.len() as u64;
        }
        value.len()
    }

    /// Compress each chunk into the stored buffer, or return nothing when they don't fit
    pub fn table_compressed(&mut self, value: &[u8], payload_start: u64) -> Option<usize> {
        let mut used = 0;
        for (index, chunk) in value.chunks(CHUNK_BYTES).enumerate() {
            let room = self.stored.get_mut(used..)?;
            let length = match self.lz4.compress(chunk, room) {
                Some(length) => length,
                None => {
                    room.get_mut(..chunk.len())?.copy_from_slice(chunk);
                    chunk.len()
                },
            };
            let sum = checksum(&self.stored[used..used + length]);
            let entry = ChunkEntry {
                stored_offset: FileOffset(payload_start + used as u64),
                value_length: ByteCount(chunk.len() as u64),
                checksum: sum,
            };
            self.write_table_entry(index, &entry);
            used += length;
        }
        Some(used)
    }

    /// Write the entry of a chunk stored as written
    pub fn put_entry(&mut self, index: usize, offset: u64, chunk: &[u8]) {
        let entry = ChunkEntry {
            stored_offset: FileOffset(offset),
            value_length: ByteCount(chunk.len() as u64),
            checksum: checksum(chunk),
        };
        self.write_table_entry(index, &entry);
    }

    /// Put an entry at its place in the table's pages
    pub fn write_table_entry(&mut self, index: usize, entry: &ChunkEntry) {
        let page = index / ENTRIES_PER_PAGE;
        let start = page * TABLE_PAGE_BYTES;
        let slot = index % ENTRIES_PER_PAGE;
        write_entry(&mut self.table[start..start + TABLE_PAGE_BYTES], slot, entry);
    }

    /// Give each full page the stored end of its last chunk
    pub fn seal_table(&mut self, chunks: ChunkCount, record_end: u64) {
        let pages = page_count(chunks).0 as usize;
        for page in 0..pages {
            if page_bytes(chunks, TablePageIndex(page as u64)) != TABLE_PAGE_BYTES {
                continue;
            }
            let next = (page + 1) * TABLE_PAGE_BYTES;
            let end = if page + 1 == pages {
                record_end
            } else {
                read_offset(&self.table[next..next + size_of::<u64>()])
            };
            let at = page * TABLE_PAGE_BYTES + PAGE_END_AT;
            self.table[at..at + size_of::<u64>()].copy_from_slice(&end.to_le_bytes());
        }
    }

    /// Write the head: form, value length, page checksums, then the check over them
    pub fn write_chunked_head(
        &mut self,
        identity: &RecordIdentity,
        codec: Codec,
        value_length: ByteCount,
        chunks: ChunkCount,
        head_length: usize,
    ) {
        let form = RecordForm {
            codec,
            is_chunked: true,
        };
        self.head[FORM_AT] = form.as_byte();
        self.head[VALUE_LENGTH_AT..PAGE_SUMS_AT].copy_from_slice(&value_length.0.to_le_bytes());

        for page in 0..page_count(chunks).0 as usize {
            let start = page * TABLE_PAGE_BYTES;
            let length = page_bytes(chunks, TablePageIndex(page as u64));
            let sum = checksum(&self.table[start..start + length]);
            let at = PAGE_SUMS_AT + page * PAGE_SUM_BYTES;
            self.head[at..at + PAGE_SUM_BYTES].copy_from_slice(&sum.0.to_le_bytes());
        }

        self.check = record_check(identity, form, checksum(&self.head[FORM_AT..head_length]));
        self.set_place(identity.record_key, identity.place);
    }

    /// Fold in the place a claim gave this write, which a retry with a new claim does again
    pub fn set_place(&mut self, record_key: RecordKey, place: WritePlace) {
        let check = place_check(self.check, record_key, place);
        self.head[..CHECK_BYTES].copy_from_slice(&check.0.to_le_bytes());
    }

    /// Point at the laid-out parts, ready for one write call
    pub fn parts<'bytes>(&'bytes self, shape: &RecordShape, value: &'bytes [u8]) -> [IoSlice<'bytes>; RECORD_PARTS] {
        let payload = if shape.is_payload_stored {
            &self.stored[..shape.payload_length]
        } else {
            value
        };
        [
            IoSlice::new(&self.head[..shape.head_length]),
            IoSlice::new(&self.table[..shape.table_length]),
            IoSlice::new(payload),
        ]
    }
}

/// Read a chunk offset from the front of a table entry
pub fn read_offset(bytes: &[u8]) -> u64 {
    let mut word = [0; size_of::<u64>()];
    word.copy_from_slice(bytes);
    u64::from_le_bytes(word)
}
