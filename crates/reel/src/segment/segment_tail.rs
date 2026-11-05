//! The end of a sealed segment: what open reads, and what makes a segment sealed

use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, FileOffset, RowCount, WritePlace};
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::record::record_check::{Checksum, RecordKey};
use crate::segment::block_entry::{KEY_PREFIX_BYTES, KeyPrefix};

/// Ends every sealed segment
pub const TAIL_MAGIC: u32 = 0x5245_4C54;
/// Bytes at the very end: the tail's length, the magic and the checksum
pub const TRAILER_BYTES: usize = 16;
/// Bytes of the fixed part of a tail
pub const TAIL_FIXED_BYTES: usize = 100 + MOST_MERGE_SEGMENTS * size_of::<u64>();
/// Most segments one compaction merges into one, which a tail lists as its inputs
pub const MOST_MERGE_SEGMENTS: usize = 8;
/// Bytes of one column's part of a tail
pub const COLUMN_TAIL_BYTES: usize = 72;
/// Spare bytes in one column's part of a tail
pub const COLUMN_PADDING_BYTES: usize = 4;
/// Most bytes a segment can reach, since its offsets are 32 bits
pub const MOST_SEGMENT_BYTES: ByteCount = ByteCount(u32::MAX as u64);
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
    pub input_count: u32,
    pub inputs: [SegmentNumber; MOST_MERGE_SEGMENTS],
}

/// Writes the fixed fields of a tail in order
#[derive(Debug)]
pub struct TailCursor<'bytes> {
    bytes: &'bytes mut [u8],
    used: usize,
}

/// Reads the fixed fields of a tail in order
#[derive(Clone, Copy, Debug)]
pub struct TailReader<'bytes> {
    bytes: &'bytes [u8],
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

impl<'bytes> TailReader<'bytes> {
    /// Start at the front of some bytes
    pub fn new(bytes: &'bytes [u8]) -> Self {
        Self { bytes, used: 0 }
    }

    /// Take raw bytes
    pub fn take(&mut self, length: usize) -> Option<&'bytes [u8]> {
        let field = self.bytes.get(self.used..self.used + length)?;
        self.used += length;
        Some(field)
    }

    /// Take a word
    pub fn word(&mut self) -> Option<u64> {
        let mut word = [0; size_of::<u64>()];
        word.copy_from_slice(self.take(size_of::<u64>())?);
        Some(u64::from_le_bytes(word))
    }

    /// Take a half word
    pub fn half(&mut self) -> Option<u32> {
        let mut half = [0; size_of::<u32>()];
        half.copy_from_slice(self.take(size_of::<u32>())?);
        Some(u32::from_le_bytes(half))
    }

    /// Take a key prefix
    pub fn prefix(&mut self) -> Option<KeyPrefix> {
        let mut prefix = [0; KEY_PREFIX_BYTES];
        prefix.copy_from_slice(self.take(KEY_PREFIX_BYTES)?);
        Some(KeyPrefix(prefix))
    }
}

impl TailHead {
    /// Write the fixed fields
    pub fn write(&self, out: &mut TailCursor) {
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
        out.put(&self.input_count.to_le_bytes());
        for input in &self.inputs {
            out.put(&input.0.to_le_bytes());
        }
    }

    /// Read the segments this one replaced
    pub fn inputs(&self) -> &[SegmentNumber] {
        self.inputs.get(..self.input_count as usize).unwrap_or_default()
    }

    /// Read the fixed fields back
    pub fn read(input: &mut TailReader) -> Option<Self> {
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
            input_count: input.half()?,
            inputs: [SegmentNumber(0); MOST_MERGE_SEGMENTS],
        };
        for segment in &mut head.inputs {
            *segment = SegmentNumber(input.word()?);
        }
        Some(head)
    }
}

impl ColumnTail {
    /// Write one column's fields
    pub fn write(&self, out: &mut TailCursor) {
        out.put(&[self.column.0, 0]);
        out.put(&self.shared_length.to_le_bytes());
        out.put(&self.shared_sum.0.to_le_bytes());
        out.put(&self.lowest.0);
        out.put(&self.highest.0);
        out.put(&self.first_block.to_le_bytes());
        out.put(&self.block_count.0.to_le_bytes());
        out.put(&self.row_count.0.to_le_bytes());
        out.put(&self.grave_count.0.to_le_bytes());
        out.put(&[0; COLUMN_PADDING_BYTES]);
    }

    /// Read one column's fields back
    pub fn read(input: &mut TailReader) -> Option<Self> {
        let leading = input.take(size_of::<u32>())?;
        let tail = Self {
            column: ColumnId(leading[0]),
            shared_length: u16::from_le_bytes([leading[2], leading[3]]),
            shared_sum: Checksum(input.word()?),
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
    let mut input = TailReader::new(&end[trailer_at..]);
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
