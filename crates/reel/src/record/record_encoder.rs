//! Lays a value out as a record, in pooled buffers so a put allocates nothing

use std::io::{IoSlice, Result};

use reel_core::units::{ByteCount, FileOffset, WritePlace};

use crate::io::IoFile;
use crate::pool::{Pool, touched_room, touched_zeroes};
use crate::record::chunk_table::{
    ChunkCount, ChunkEntry, ENTRIES_PER_PAGE, PAGE_END_AT, TABLE_PAGE_BYTES, TablePageIndex, page_bytes, page_count,
    table_bytes, write_entry,
};
use crate::record::codec::{CHUNK_BYTES, Codec};
use crate::record::lz4_block::Lz4State;
use crate::record::record_check::{Checksum, RecordCheck, RecordIdentity, RecordKey, checksum, place_check, record_check};
use crate::record::record_head::{
    CHECK_BYTES, FORM_AT, PAGE_SUM_BYTES, PAGE_SUMS_AT, PAYLOAD_SUM_AT, RecordForm, SMALL_HEAD_BYTES, VALUE_LENGTH_AT, chunk_count,
    chunked_head_bytes, put_piece_sums, short_sum, sums_bytes,
};

/// Bytes of one share of the compress pool
pub const SHARE_BYTES: usize = 1024 * 1024;
/// Chunks a share holds whatever they compress to
pub const CHUNKS_PER_SHARE: usize = SHARE_BYTES / CHUNK_BYTES;
/// Parts one write call can take
pub const MOST_WRITE_PARTS: usize = 1_024;
/// Runs a chunked record's payload can use, beside its head and table, and still go out in one write call
pub const MOST_RECORD_RUNS: usize = MOST_WRITE_PARTS - 2;
/// Parts of a record: head, table, piece sums and payload
pub const RECORD_PARTS: usize = 4;

/// The lengths of a laid-out record
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecordShape {
    pub head_length: usize,
    pub table_length: usize,
    pub sums_length: usize,
    pub payload_length: usize,
    pub is_payload_stored: bool,
}

/// Adjacent stored chunks that sit together in memory, so they go out as one part of a write call
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoredRun {
    /// Compressed chunks in a share
    Share { share: usize, start: usize, end: usize },
    /// Chunks stored as written, straight from the caller's value
    Value { start: usize, end: usize },
}

/// A one-chunk record stored as written, whose head fits the stack, so its writer needs no shared encoder
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlainRecord {
    check: RecordCheck,
    sum: Checksum,
}

impl PlainRecord {
    /// The form of every such record
    pub const FORM: RecordForm = RecordForm {
        codec: Codec::None,
        is_chunked: false,
    };

    /// Hash a value under its identity
    pub fn new(identity: &RecordIdentity, value: &[u8]) -> Self {
        let sum = checksum(value);
        Self { check: record_check(identity, Self::FORM, sum), sum }
    }

    /// Build the head for the place a claim gave this write
    pub fn head(&self, record_key: RecordKey, place: WritePlace) -> [u8; SMALL_HEAD_BYTES] {
        let mut head = [0; SMALL_HEAD_BYTES];
        head[..CHECK_BYTES].copy_from_slice(&place_check(self.check, record_key, place).0.to_le_bytes());
        head[FORM_AT] = Self::FORM.as_byte();
        head[PAYLOAD_SUM_AT..SMALL_HEAD_BYTES].copy_from_slice(&short_sum(self.sum));
        head
    }
}

/// The buffers one writer borrows from the pool
#[derive(Debug)]
pub struct RecordEncoder {
    head: Vec<u8>,
    table: Vec<u8>,
    sums: Vec<u8>,
    stored: Vec<u8>,
    shares: Vec<Vec<u8>>,
    runs: Vec<StoredRun>,
    lz4: Lz4State,
    check: RecordCheck,
}

impl RecordShape {
    /// Read the record's whole length
    pub fn total_length(&self) -> ByteCount {
        ByteCount((self.head_length + self.table_length + self.sums_length + self.payload_length) as u64)
    }
}

impl RecordEncoder {
    /// Allocate buffers for values up to a length, with room for a share of each of a pool's items
    pub fn new(largest_value: ByteCount, most_shares: usize) -> Self {
        let chunks = chunk_count(largest_value);
        Self {
            head: touched_zeroes(chunked_head_bytes(largest_value)),
            table: touched_zeroes(page_count(chunks).0 as usize * TABLE_PAGE_BYTES),
            sums: touched_zeroes(sums_bytes(largest_value) as usize),
            stored: touched_zeroes(CHUNK_BYTES),
            shares: touched_room(most_shares),
            runs: touched_room(MOST_RECORD_RUNS),
            lz4: Lz4State::new(),
            check: RecordCheck(0),
        }
    }

    /// Read the heap these buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        let runs = self.runs.capacity() * size_of::<StoredRun>();
        let shares = self.shares.capacity() * size_of::<Vec<u8>>();
        let buffers = self.head.capacity() + self.table.capacity() + self.sums.capacity() + self.stored.capacity();
        ByteCount((buffers + runs + shares) as u64)
    }

    /// Take the shares a value could need, all at once so two large values can't each hold what the other waits
    /// for, or return false when the pool could never hold it. A chunk that doesn't shrink uses no room in them,
    /// but only compressing tells which chunks those are
    pub fn take_shares(&mut self, pool: &Pool<Vec<u8>>, codec: Codec, value: &[u8]) -> bool {
        // a panic in an earlier write leaves its shares here
        self.give_shares(pool);
        if codec != Codec::Lz4 || value.len() <= CHUNK_BYTES {
            return true;
        }

        let count = (chunk_count(ByteCount(value.len() as u64)).0 as usize).div_ceil(CHUNKS_PER_SHARE);
        if count > self.shares.capacity() {
            return false;
        }
        pool.take_many(count, &mut self.shares);
        true
    }

    /// Give every share back
    pub fn give_shares(&mut self, pool: &Pool<Vec<u8>>) {
        pool.give_back_all(&mut self.shares);
    }

    /// Lay a value out, or return nothing when it doesn't fit these buffers
    pub fn encode(&mut self, identity: &RecordIdentity, codec: Codec, value: &[u8]) -> Option<RecordShape> {
        self.runs.clear();
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
        // the writer already has the payload's sum for the check, so the stored low bytes cost no hash
        self.head[PAYLOAD_SUM_AT..SMALL_HEAD_BYTES].copy_from_slice(&short_sum(sum));
        RecordShape {
            head_length: SMALL_HEAD_BYTES,
            table_length: 0,
            sums_length: 0,
            payload_length,
            is_payload_stored: compressed.is_some(),
        }
    }

    /// Lay out a value of many chunks: a head, a table, then the chunks. A record stored as written keeps its pieces' sums
    /// between the table and the chunks
    pub fn encode_chunked(&mut self, identity: &RecordIdentity, codec: Codec, value: &[u8]) -> Option<RecordShape> {
        let value_length = ByteCount(value.len() as u64);
        let chunks = chunk_count(value_length);
        let head_length = chunked_head_bytes(value_length);
        let table_length = table_bytes(chunks).0 as usize;
        let written_sums = sums_bytes(value_length) as usize;
        if head_length > self.head.len() || table_length > self.table.len() || written_sums > self.sums.len() {
            return None;
        }

        let front = (head_length + table_length) as u64;
        let mut sums_length = 0;
        let mut payload_length = match codec {
            Codec::None => 0,
            Codec::Lz4 => self.table_compressed(value, front)?,
        };
        // a record nothing shrank in is laid out as written, like a none column's, with its pieces' sums
        if codec == Codec::None || payload_length == value.len() {
            self.runs.clear();
            sums_length = written_sums;
            payload_length = self.table_as_written(value, front + sums_length as u64);
        }
        let stored_codec = if sums_length > 0 { Codec::None } else { codec };
        self.seal_table(chunks, front + (sums_length + payload_length) as u64);
        self.write_chunked_head(identity, stored_codec, value_length, chunks, head_length);

        Some(RecordShape {
            head_length,
            table_length,
            sums_length,
            payload_length,
            is_payload_stored: false,
        })
    }

    /// List chunks stored as written with their pieces' sums, and return the payload length
    pub fn table_as_written(&mut self, value: &[u8], payload_start: u64) -> usize {
        let mut offset = payload_start;
        let mut sums_at = 0;
        for (index, chunk) in value.chunks(CHUNK_BYTES).enumerate() {
            let length = put_piece_sums(chunk, &mut self.sums[sums_at..]);
            let entry = ChunkEntry {
                stored_offset: FileOffset(offset),
                value_length: ByteCount(chunk.len() as u64),
                checksum: checksum(&self.sums[sums_at..sums_at + length]),
            };
            self.write_table_entry(index, &entry);
            sums_at += length;
            offset += chunk.len() as u64;
        }
        value.len()
    }

    /// Compress each chunk into the shares, keep what doesn't shrink in the value, and return the payload length,
    /// or nothing when the shares run out. Past the last run but one the chunks stay as written, which takes one
    /// run however many there are, and depends only on the value
    pub fn table_compressed(&mut self, value: &[u8], payload_start: u64) -> Option<usize> {
        let mut share = 0;
        let mut used = 0;
        let mut stored = 0;
        for (index, chunk) in value.chunks(CHUNK_BYTES).enumerate() {
            let start = index * CHUNK_BYTES;
            if used + chunk.len() > SHARE_BYTES {
                share += 1;
                used = 0;
            }

            let compressed = if self.runs.len() + 1 < MOST_RECORD_RUNS {
                let room = self.shares.get_mut(share)?.get_mut(used..used + chunk.len())?;
                self.lz4.compress(chunk, room)
            } else {
                None
            };
            let (length, sum) = match compressed {
                Some(length) => {
                    let sum = checksum(&self.shares[share][used..used + length]);
                    self.add_run(StoredRun::Share {
                        share,
                        start: used,
                        end: used + length,
                    });
                    used += length;
                    (length, sum)
                },
                None => {
                    self.add_run(StoredRun::Value {
                        start,
                        end: start + chunk.len(),
                    });
                    (chunk.len(), checksum(chunk))
                },
            };

            let entry = ChunkEntry {
                stored_offset: FileOffset(payload_start + stored as u64),
                value_length: ByteCount(chunk.len() as u64),
                checksum: sum,
            };
            self.write_table_entry(index, &entry);
            stored += length;
        }
        Some(stored)
    }

    /// Count the runs of the record laid out last
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn run_count(&self) -> usize {
        self.runs.len()
    }

    /// Add a chunk's place in memory, joining the last run when it sits right after it
    pub fn add_run(&mut self, run: StoredRun) {
        match (self.runs.last_mut(), run) {
            (Some(StoredRun::Value { end, .. }), StoredRun::Value { start, end: new_end }) if *end == start => {
                *end = new_end;
            },
            (
                Some(StoredRun::Share { share, end, .. }),
                StoredRun::Share {
                    share: new_share,
                    start,
                    end: new_end,
                },
            ) if *share == new_share && *end == start => {
                *end = new_end;
            },
            (_last, run) => self.runs.push(run),
        }
    }

    /// Write a laid-out record in one call
    pub fn write_to<File: IoFile>(
        &self,
        file: &File,
        at: FileOffset,
        shape: &RecordShape,
        value: &[u8],
    ) -> Result<()> {
        self.write_range_to(file, at, shape, value, (0, shape.total_length().0 as usize))
    }

    /// Write the record's bytes from one point to another, each at its own offset from the record's start
    pub fn write_range_to<File: IoFile>(
        &self,
        file: &File,
        at: FileOffset,
        shape: &RecordShape,
        value: &[u8],
        range: (usize, usize),
    ) -> Result<()> {
        if self.runs.is_empty() {
            return write_parts_range(file, at, &self.parts(shape, value), range);
        }

        let mut parts = [IoSlice::new(&[]); MOST_WRITE_PARTS];
        parts[0] = IoSlice::new(&self.head[..shape.head_length]);
        parts[1] = IoSlice::new(&self.table[..shape.table_length]);
        for (part, run) in parts[2..].iter_mut().zip(&self.runs) {
            let bytes = match *run {
                StoredRun::Share { share, start, end } => &self.shares[share][start..end],
                StoredRun::Value { start, end } => &value[start..end],
            };
            *part = IoSlice::new(bytes);
        }
        write_parts_range(file, at, &parts[..2 + self.runs.len()], range)
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
        // a chunked record checks its payload by its page sums, so its head keeps no payload sum
        self.head[PAYLOAD_SUM_AT..SMALL_HEAD_BYTES].fill(0);
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

    /// Build a one-chunk record's head for a place, leaving the encoder as it is
    pub fn small_head(&self, record_key: RecordKey, place: WritePlace) -> [u8; SMALL_HEAD_BYTES] {
        let mut head = [0; SMALL_HEAD_BYTES];
        head[..CHECK_BYTES].copy_from_slice(&place_check(self.check, record_key, place).0.to_le_bytes());
        head[FORM_AT] = self.head[FORM_AT];
        head[PAYLOAD_SUM_AT..SMALL_HEAD_BYTES].copy_from_slice(&self.head[PAYLOAD_SUM_AT..SMALL_HEAD_BYTES]);
        head
    }

    /// Point at a one-chunk record's payload
    pub fn small_payload<'bytes>(&'bytes self, shape: &RecordShape, value: &'bytes [u8]) -> &'bytes [u8] {
        if shape.is_payload_stored { &self.stored[..shape.payload_length] } else { value }
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
            IoSlice::new(&self.sums[..shape.sums_length]),
            IoSlice::new(payload),
        ]
    }
}

/// Write the bytes of a run of parts from one point to another, with the run starting at `at`, in one write call
pub fn write_parts_range<File: IoFile>(file: &File, at: FileOffset, parts: &[IoSlice<'_>], (from, to): (usize, usize)) -> Result<()> {
    if from == 0 && to >= parts.iter().map(|part| part.len()).sum::<usize>() {
        return file.write_at(at, parts);
    }
    let mut picked = [IoSlice::new(&[]); MOST_WRITE_PARTS];
    let (mut count, mut start) = (0, 0);
    for part in parts {
        let end = start + part.len();
        let (low, high) = (from.max(start), to.min(end));
        if low < high {
            picked[count] = IoSlice::new(&part[low - start..high - start]);
            count += 1;
        }
        start = end;
    }
    file.write_at(FileOffset(at.0 + from as u64), &picked[..count])
}

/// Read a chunk offset from the front of a table entry
pub fn read_offset(bytes: &[u8]) -> u64 {
    let mut word = [0; size_of::<u64>()];
    word.copy_from_slice(bytes);
    u64::from_le_bytes(word)
}
