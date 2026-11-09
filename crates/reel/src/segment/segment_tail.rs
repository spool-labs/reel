//! The end of a sealed segment: what open reads, and what makes a segment sealed

use std::cmp::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, DeathMark, FileOffset, RowCount, WritePlace};
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::lifetime::MarkRange;
use crate::little_endian::ByteReader;
use crate::record::record_check::{Checksum, RecordKey};
use crate::segment::block_entry::{KEY_BOUND_BYTES, KEY_PREFIX_BYTES, KeyBound, KeyPrefix, SHARED_HEAD_BYTES, SharedHead};

/// Ends every sealed segment
pub const TAIL_MAGIC: u32 = 0x5245_4C54;
/// Bytes at the very end: the tail's length, the magic and the checksum
pub const TRAILER_BYTES: usize = 16;
/// Bytes of the fixed part of a tail
pub const TAIL_FIXED_BYTES: usize = 116 + MOST_MERGE_SEGMENTS * size_of::<u64>();
/// Most segments one compaction merges into one, which a tail lists as its inputs
pub const MOST_MERGE_SEGMENTS: usize = 8;
/// Bytes of one column's part of a tail
pub const COLUMN_TAIL_BYTES: usize = 72 + SHARED_HEAD_BYTES;
/// Spare bytes in one column's part of a tail
pub const COLUMN_PADDING_BYTES: usize = 4;
/// Most bytes a segment can reach, since its offsets are 32 bits
pub const MOST_SEGMENT_BYTES: ByteCount = ByteCount(u32::MAX as u64);
/// Set in a tail's input count on a segment a merge wrote, which a rebuild only walks backward
pub const OUTPUT_BIT: u32 = 1 << 31;
/// Set beside it on every output of a merge but the last: its inputs are for reference, and it stands only if that last
/// output landed
pub const PARTIAL_BIT: u32 = 1 << 30;
/// Set beside it on an output of inputs no walk had reached: it owes the forward walk they owed, and a rebuild gives it one
pub const UNWALKED_BIT: u32 = 1 << 29;
/// Most columns a store has
pub const MAX_COLUMNS: usize = 256;
/// Bytes that hold any tail
pub const MAX_TAIL_BYTES: usize = TAIL_FIXED_BYTES + MAX_COLUMNS * COLUMN_TAIL_BYTES + TRAILER_BYTES;

/// The two copies of a tail or index never share this span of the file
pub const COPY_GAP_BYTES: u64 = 64 * 1024;
/// The second tail copy ends this far before the file's end, wherever the first one starts
pub const SECOND_TAIL_DISTANCE: u64 = MAX_TAIL_BYTES as u64 + COPY_GAP_BYTES;

/// A segment's number, which a store never uses twice
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SegmentNumber(pub u64);

/// A count of row blocks
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BlockCount(pub u32);

/// The lowest and highest place in the write order over data and graves alike
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PlaceRange {
    pub lowest: WritePlace,
    pub highest: WritePlace,
}

/// One column's range and totals in one segment
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ColumnTail {
    pub column: ColumnId,
    pub shared_length: u16,
    pub shared_sum: Checksum,
    pub shared_head: SharedHead,
    pub lowest: KeyPrefix,
    pub highest: KeyPrefix,
    pub first_block: u32,
    pub block_count: BlockCount,
    pub row_count: RowCount,
    pub grave_count: RowCount,
}

/// The fixed part of a tail
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TailHead {
    pub segment: SegmentNumber,
    pub data_end: FileOffset,
    pub index_at: FileOffset,
    pub second_index_at: FileOffset,
    pub block_count: BlockCount,
    pub ranges_at: FileOffset,
    pub ranges_length: ByteCount,
    pub places: PlaceRange,
    pub place_base: WritePlace,
    pub index_sum: Checksum,
    pub ranges_sum: Checksum,
    pub column_count: u32,
    pub is_output: bool,
    pub is_partial: bool,
    pub is_unwalked: bool,
    pub marks: MarkRange,
}

/// The segments a merge output replaced, which its tail lists beside the head. Only open reads them, so the table keeps none
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TailInputs {
    pub count: u32,
    pub numbers: [SegmentNumber; MOST_MERGE_SEGMENTS],
}

impl TailInputs {
    /// List some inputs
    pub fn of(inputs: &[SegmentNumber]) -> Self {
        let mut numbers = [SegmentNumber(0); MOST_MERGE_SEGMENTS];
        numbers[..inputs.len()].copy_from_slice(inputs);
        Self { count: inputs.len() as u32, numbers }
    }

    /// Read the segments this one replaced
    pub fn inputs(&self) -> &[SegmentNumber] {
        self.numbers.get(..self.count as usize).unwrap_or_default()
    }
}

/// Writes the fixed fields of a tail in order
#[derive(Debug)]
pub struct TailCursor<'bytes> {
    bytes: &'bytes mut [u8],
    used: usize,
}

impl<'bytes> TailCursor<'bytes> {
    /// Start at the front of a buffer
    pub fn new(bytes: &'bytes mut [u8]) -> Self {
        Self { bytes, used: 0 }
    }

    /// Append raw bytes
    pub fn put(&mut self, field: &[u8]) {
        self.bytes[self.used..self.used + field.len()].copy_from_slice(field);
        self.used += field.len();
    }
}

impl ByteReader<'_> {
    /// Take a shared prefix's first bytes
    pub fn shared_head(&mut self) -> Option<SharedHead> {
        let mut head = [0; SHARED_HEAD_BYTES];
        head.copy_from_slice(self.take(SHARED_HEAD_BYTES)?);
        Some(SharedHead(head))
    }

    /// Take a key prefix
    pub fn prefix(&mut self) -> Option<KeyPrefix> {
        let mut prefix = [0; KEY_PREFIX_BYTES];
        prefix.copy_from_slice(self.take(KEY_PREFIX_BYTES)?);
        Some(KeyPrefix(prefix))
    }
}

impl TailHead {
    /// Write the fixed fields, with the inputs in their place among them
    pub fn write(&self, inputs: &TailInputs, out: &mut TailCursor) {
        out.put(&self.segment.0.to_le_bytes());
        out.put(&self.data_end.0.to_le_bytes());
        out.put(&self.index_at.0.to_le_bytes());
        out.put(&self.second_index_at.0.to_le_bytes());
        out.put(&self.block_count.0.to_le_bytes());
        out.put(&self.ranges_at.0.to_le_bytes());
        out.put(&self.ranges_length.0.to_le_bytes());
        out.put(&self.places.lowest.0.to_le_bytes());
        out.put(&self.places.highest.0.to_le_bytes());
        out.put(&self.place_base.0.to_le_bytes());
        out.put(&self.index_sum.0.to_le_bytes());
        out.put(&self.ranges_sum.0.to_le_bytes());
        out.put(&self.column_count.to_le_bytes());
        let bit = if self.is_output { OUTPUT_BIT } else { 0 };
        let partial_bit = if self.is_partial { PARTIAL_BIT } else { 0 };
        let unwalked_bit = if self.is_unwalked { UNWALKED_BIT } else { 0 };
        out.put(&(inputs.count | bit | partial_bit | unwalked_bit).to_le_bytes());
        for input in &inputs.numbers {
            out.put(&input.0.to_le_bytes());
        }
        out.put(&self.marks.lowest.0.to_le_bytes());
        out.put(&self.marks.highest.0.to_le_bytes());
    }

    /// Read the fixed fields back, and the inputs among them
    pub fn read(input: &mut ByteReader) -> Option<(Self, TailInputs)> {
        let mut head = Self {
            segment: SegmentNumber(input.word()?),
            data_end: FileOffset(input.word()?),
            index_at: FileOffset(input.word()?),
            second_index_at: FileOffset(input.word()?),
            block_count: BlockCount(input.half()?),
            ranges_at: FileOffset(input.word()?),
            ranges_length: ByteCount(input.word()?),
            places: PlaceRange {
                lowest: WritePlace(input.word()?),
                highest: WritePlace(input.word()?),
            },
            place_base: WritePlace(input.word()?),
            index_sum: Checksum(input.word()?),
            ranges_sum: Checksum(input.word()?),
            column_count: input.half()?,
            is_output: false,
            is_partial: false,
            is_unwalked: false,
            marks: MarkRange::NONE,
        };
        let mut inputs = TailInputs::default();
        let listed = input.half()?;
        inputs.count = listed & !(OUTPUT_BIT | PARTIAL_BIT | UNWALKED_BIT);
        head.is_output = listed & OUTPUT_BIT != 0;
        head.is_partial = listed & PARTIAL_BIT != 0;
        head.is_unwalked = listed & UNWALKED_BIT != 0;
        for segment in &mut inputs.numbers {
            *segment = SegmentNumber(input.word()?);
        }
        head.marks = MarkRange {
            lowest: DeathMark(input.word()?),
            highest: DeathMark(input.word()?),
        };
        Some((head, inputs))
    }
}

impl ColumnTail {
    /// Compare a key with the bytes the column's keys share, as far as the tail holds them
    pub fn order_against_shared(&self, key: &[u8]) -> Ordering {
        let known = (self.shared_length as usize).min(SHARED_HEAD_BYTES);
        key[..key.len().min(known)].cmp(&self.shared_head.0[..known])
    }

    /// Build a bound at or above every key whose bytes past the shared ones start at or below a prefix
    pub fn bound_above(&self, prefix: KeyPrefix) -> KeyBound {
        let shared = self.shared_length as usize;
        let known = shared.min(SHARED_HEAD_BYTES);
        let mut bound = [u8::MAX; KEY_BOUND_BYTES];
        bound[..known].copy_from_slice(&self.shared_head.0[..known]);
        if shared <= SHARED_HEAD_BYTES {
            bound[known..known + KEY_PREFIX_BYTES].copy_from_slice(&prefix.0);
        }
        KeyBound(bound)
    }

    /// Build the bounds of the column's keys, which hold every key it has and can be wider
    pub fn key_bounds(&self) -> (KeyBound, KeyBound) {
        let shared = self.shared_length as usize;
        let known = shared.min(SHARED_HEAD_BYTES);
        let (mut lowest, mut highest) = ([0; KEY_BOUND_BYTES], [u8::MAX; KEY_BOUND_BYTES]);
        lowest[..known].copy_from_slice(&self.shared_head.0[..known]);
        highest[..known].copy_from_slice(&self.shared_head.0[..known]);
        // past the head the prefixes stand at offsets this bound doesn't reach
        if shared <= SHARED_HEAD_BYTES {
            lowest[known..known + KEY_PREFIX_BYTES].copy_from_slice(&self.lowest.0);
            highest[known..known + KEY_PREFIX_BYTES].copy_from_slice(&self.highest.0);
        }
        (KeyBound(lowest), KeyBound(highest))
    }

    /// Write one column's fields
    pub fn write(&self, out: &mut TailCursor) {
        out.put(&[self.column.0, 0]);
        out.put(&self.shared_length.to_le_bytes());
        out.put(&self.shared_sum.0.to_le_bytes());
        out.put(&self.shared_head.0);
        out.put(&self.lowest.0);
        out.put(&self.highest.0);
        out.put(&self.first_block.to_le_bytes());
        out.put(&self.block_count.0.to_le_bytes());
        out.put(&self.row_count.0.to_le_bytes());
        out.put(&self.grave_count.0.to_le_bytes());
        out.put(&[0; COLUMN_PADDING_BYTES]);
    }

    /// Read one column's fields back
    pub fn read(input: &mut ByteReader) -> Option<Self> {
        let leading = input.take(size_of::<u32>())?;
        let tail = Self {
            column: ColumnId(leading[0]),
            shared_length: u16::from_le_bytes([leading[2], leading[3]]),
            shared_sum: Checksum(input.word()?),
            shared_head: input.shared_head()?,
            lowest: input.prefix()?,
            highest: input.prefix()?,
            first_block: input.half()?,
            block_count: BlockCount(input.half()?),
            row_count: RowCount(input.word()?),
            grave_count: RowCount(input.word()?),
        };
        input.take(COLUMN_PADDING_BYTES)?;
        Some(tail)
    }
}

/// Close a tail: its length, the magic and a keyed checksum over all of it
pub fn write_trailer(tail: &mut [u8], body_length: usize, record_key: RecordKey) -> usize {
    let total = body_length + TRAILER_BYTES;
    let length_at = body_length;
    let magic_at = length_at + size_of::<u32>();
    let sum_at = magic_at + size_of::<u32>();
    tail[length_at..magic_at].copy_from_slice(&(total as u32).to_le_bytes());
    tail[magic_at..sum_at].copy_from_slice(&TAIL_MAGIC.to_le_bytes());

    let sum = xxh3_64_with_seed(&tail[..sum_at], record_key.0);
    tail[sum_at..total].copy_from_slice(&sum.to_le_bytes());
    total
}

/// Find a whole tail at the end of some bytes, or nothing when the segment isn't sealed
pub fn find_tail(end: &[u8], record_key: RecordKey) -> Option<&[u8]> {
    let trailer_at = end.len().checked_sub(TRAILER_BYTES)?;
    let mut input = ByteReader::new(&end[trailer_at..]);
    let total = input.half()? as usize;
    if input.half()? != TAIL_MAGIC {
        return None;
    }
    let sum = input.word()?;

    let tail_at = end.len().checked_sub(total)?;
    let checked = &end[tail_at..end.len() - size_of::<u64>()];
    if xxh3_64_with_seed(checked, record_key.0) != sum {
        return None;
    }
    Some(&end[tail_at..trailer_at])
}

/// Check every column's blocks lie inside a segment's block index
pub fn columns_fit(columns: &[ColumnTail], block_lines: usize) -> bool {
    columns.iter().all(|column| column.first_block as usize + column.block_count.0 as usize <= block_lines)
}
