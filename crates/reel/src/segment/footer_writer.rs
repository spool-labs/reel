//! Writes a sealed segment's footer: row blocks with their filters, the block index and the tail

use std::io::{Error, IoSlice, Result};

use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, FileOffset, RowCount, WritePlace};

use crate::pool::{touched_capacity, touched_zeroes};
use crate::index::key_hash::{KeyHash, hash_key};
use crate::io::IoFile;
use crate::record::record_check::{Checksum, RecordKey, checksum};
use crate::row_kind::RowKind;
use crate::segment::block_entry::{BLOCK_ENTRY_BYTES, BlockEntry, KeyPrefix, SharedHead};
use crate::segment::block_filter::{FILTER_SUM_BYTES, add_key, filter_bytes, seal_filter};
use crate::segment::row_block::{
    BLOCK_HEADER_BYTES, BlockHeader, FooterRow, ROW_BLOCK_BYTES, RowBlockBuilder, write_block_header,
};
use crate::segment::summary::{ColumnFacts, SUMMARY_KEY_BYTES, put_bound, put_lowest};
use crate::segment::segment_tail::{
    BlockCount, COLUMN_TAIL_BYTES, COPY_GAP_BYTES, ColumnTail, MAX_COLUMNS, MAX_TAIL_BYTES, MOST_MERGE_SEGMENTS, MOST_SEGMENT_BYTES,
    PlaceRange, SECOND_TAIL_DISTANCE, SegmentNumber, TAIL_FIXED_BYTES, TailCursor, TailHead, write_trailer,
};

/// Most rows a block of the smallest rows can hold
pub const MOST_BLOCK_ROWS: usize = ROW_BLOCK_BYTES / 4;

/// What the writer knows about a column before its first row
#[derive(Clone, Copy, Debug)]
pub struct ColumnStart<'key> {
    pub column: ColumnId,
    pub lowest: &'key [u8],
    pub highest: &'key [u8],
    pub block_bytes: usize,
}

/// Where a column's whole lowest and highest keys sit in the writer's key buffer
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ColumnKeys {
    pub lowest_at: usize,
    pub highest_at: usize,
    pub end: usize,
    pub value_bytes: ByteCount,
}

/// What a finished footer adds up to
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FooterTotals {
    pub head: TailHead,
    pub file_length: ByteCount,
    pub block_count: BlockCount,
    pub row_count: RowCount,
}

/// A footer in the writing, with buffers that never grow
#[derive(Debug)]
pub struct FooterWriter {
    builder: RowBlockBuilder,
    filter: Vec<u8>,
    hashes: Vec<KeyHash>,
    entries: Vec<BlockEntry>,
    columns: Vec<ColumnTail>,
    column_keys: Vec<ColumnKeys>,
    key_bytes: Vec<u8>,
    tail: Vec<u8>,
    index: Vec<u8>,
    position: FileOffset,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    places: PlaceRange,
    place_base: WritePlace,
    row_count: RowCount,
    pub is_output: bool,
    pub is_partial: bool,
    pub is_unwalked: bool,
}

impl FooterWriter {
    /// Allocate the buffers for a segment of up to this many row blocks
    pub fn new(most_blocks: BlockCount) -> Self {
        Self {
            builder: RowBlockBuilder::new(),
            filter: touched_zeroes(filter_bytes(MOST_BLOCK_ROWS) + FILTER_SUM_BYTES),
            hashes: touched_capacity(MOST_BLOCK_ROWS, KeyHash::default()),
            entries: touched_capacity(most_blocks.0 as usize, BlockEntry::default()),
            columns: touched_capacity(MAX_COLUMNS, ColumnTail::default()),
            column_keys: touched_capacity(MAX_COLUMNS, ColumnKeys::default()),
            key_bytes: touched_capacity(MAX_COLUMNS * (2 * SUMMARY_KEY_BYTES + 1), 0),
            tail: touched_zeroes(MAX_TAIL_BYTES),
            index: touched_zeroes(ROW_BLOCK_BYTES),
            position: FileOffset(0),
            first_key: touched_capacity(ROW_BLOCK_BYTES, 0),
            last_key: touched_capacity(ROW_BLOCK_BYTES, 0),
            places: PlaceRange::default(),
            place_base: WritePlace(0),
            row_count: RowCount(0),
            is_output: false,
            is_partial: false,
            is_unwalked: false,
        }
    }

    /// Read the heap these buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        let entries = self.entries.capacity() * size_of::<BlockEntry>();
        let columns = self.columns.capacity() * size_of::<ColumnTail>();
        let hashes = self.hashes.capacity() * size_of::<KeyHash>();
        let buffers = self.filter.capacity() + self.tail.capacity() + self.index.capacity();
        let keys = self.first_key.capacity() + self.last_key.capacity();
        ByteCount(self.builder.heap_bytes().0 + (entries + columns + hashes + buffers + keys) as u64)
    }

    /// Read the block lines written so far, for the resident index
    pub fn entries(&self) -> &[BlockEntry] {
        &self.entries
    }

    /// Read the columns written so far, for the segment's tail in memory
    pub fn columns(&self) -> &[ColumnTail] {
        &self.columns
    }

    /// Read one column's facts for the summary
    pub fn column_facts(&self, index: usize) -> ColumnFacts<'_> {
        let keys = self.column_keys[index];
        let tail = &self.columns[index];
        ColumnFacts {
            column: tail.column,
            lowest: &self.key_bytes[keys.lowest_at..keys.highest_at],
            bound: &self.key_bytes[keys.highest_at..keys.end],
            row_count: tail.row_count,
            grave_count: tail.grave_count,
            dead_rows: RowCount(0),
            dead_graves: RowCount(0),
            live_bytes: keys.value_bytes,
            dead_bytes: ByteCount(0),
        }
    }

    /// Read where the next row block goes
    pub fn position(&self) -> FileOffset {
        self.position
    }

    /// Note the blocks written so far moved down the file by this many bytes
    pub fn move_down(&mut self, bytes: ByteCount) {
        for entry in &mut self.entries {
            entry.block_offset -= bytes.0 as u32;
            entry.filter_offset -= bytes.0 as u32;
        }
        self.position = FileOffset(self.position.0 - bytes.0);
    }

    /// Start a footer right after a segment's records
    pub fn start(&mut self, data_end: FileOffset, place_base: WritePlace) {
        self.place_base = place_base;
        self.builder.set_place_base(place_base);
        self.builder.clear();
        self.hashes.clear();
        self.entries.clear();
        self.columns.clear();
        self.column_keys.clear();
        self.key_bytes.clear();
        self.position = data_end;
        self.places = PlaceRange {
            lowest: WritePlace(u64::MAX),
            highest: WritePlace(0),
        };
        self.row_count = RowCount(0);
        self.is_output = false;
        self.is_partial = false;
        self.is_unwalked = false;
    }

    /// Start a column, closing the row block of the one before it
    pub fn start_column<File: IoFile>(&mut self, file: &File, start: &ColumnStart) -> Result<()> {
        self.close_block(file)?;
        self.builder.set_block_bytes(start.block_bytes);
        let shared = shared_prefix(start.lowest, start.highest);
        let lowest_at = self.key_bytes.len();
        put_lowest(start.lowest, &mut self.key_bytes);
        let highest_at = self.key_bytes.len();
        put_bound(start.highest, &mut self.key_bytes);
        self.column_keys.push(ColumnKeys {
            lowest_at,
            highest_at,
            end: self.key_bytes.len(),
            value_bytes: ByteCount(0),
        });
        self.columns.push(ColumnTail {
            column: start.column,
            shared_length: shared as u16,
            shared_sum: checksum(&start.lowest[..shared]),
            shared_head: SharedHead::of_shared(&start.lowest[..shared]),
            lowest: KeyPrefix::of_key(start.lowest, shared),
            highest: KeyPrefix::of_key(start.highest, shared),
            first_block: self.entries.len() as u32,
            block_count: BlockCount(0),
            row_count: RowCount(0),
            grave_count: RowCount(0),
        });
        Ok(())
    }

    /// End the current column at the last key it took, so its range says what it holds and nothing wider
    pub fn end_column(&mut self, last_key: &[u8]) {
        let (Some(tail), Some(keys)) = (self.columns.last_mut(), self.column_keys.last_mut()) else {
            return;
        };
        tail.highest = KeyPrefix::of_key(last_key, tail.shared_length as usize);
        self.key_bytes.truncate(keys.highest_at);
        put_bound(last_key, &mut self.key_bytes);
        keys.end = self.key_bytes.len();
    }

    /// Add the next row of the current column, in row order
    pub fn push_row<File: IoFile>(&mut self, file: &File, key: &[u8], row: &FooterRow) -> Result<()> {
        if self.hashes.len() == MOST_BLOCK_ROWS {
            self.close_block(file)?;
        }
        if !self.builder.push(key, row) {
            self.close_block(file)?;
            self.builder.push(key, row);
        }

        if self.builder.row_count().0 == 1 {
            self.first_key.clear();
            self.first_key.extend_from_slice(key);
        }
        if self.hashes.is_empty() || self.last_key != key {
            if let Some(column) = self.columns.last() {
                self.hashes.push(hash_key(column.column, key));
            }
            self.last_key.clear();
            self.last_key.extend_from_slice(key);
        }

        self.places.lowest = self.places.lowest.min(row.place);
        self.places.highest = self.places.highest.max(row.place);
        self.row_count = RowCount(self.row_count.0 + 1);
        if let Some(keys) = self.column_keys.last_mut() {
            // a range delete is no value, and the dead-byte walk counts it nowhere
            if row.kind != RowKind::RangeDelete {
                keys.value_bytes = ByteCount(keys.value_bytes.0 + row.length.0);
            }
        }
        if let Some(column) = self.columns.last_mut() {
            column.row_count = RowCount(column.row_count.0 + 1);
            if row.kind == RowKind::Grave {
                column.grave_count = RowCount(column.grave_count.0 + 1);
            }
        }
        Ok(())
    }

    /// Write the open row block with its filter
    pub fn close_block<File: IoFile>(&mut self, file: &File) -> Result<()> {
        if self.builder.is_empty() {
            return Ok(());
        }

        let row_count = self.builder.row_count();
        if self.entries.len() == self.entries.capacity() {
            return Err(Error::other("segment has more row blocks than its index holds"));
        }

        let bits = filter_bytes(self.hashes.len());
        let filter_length = bits + FILTER_SUM_BYTES;
        let filter = &mut self.filter[..filter_length];
        filter[..bits].fill(0);
        for hash in &self.hashes {
            add_key(&mut filter[..bits], *hash);
        }
        seal_filter(filter);

        let block_at = FileOffset(self.position.0 + BLOCK_HEADER_BYTES as u64);
        let block = self.builder.finish();
        let filter_at = block_at.0 + block.len() as u64;
        if filter_at + filter_length as u64 > MOST_SEGMENT_BYTES.0 {
            return Err(Error::other("footer passes the most bytes a segment can reach"));
        }
        let header = write_block_header(&BlockHeader {
            block_length: block.len() as u32,
            filter_length: filter_length as u32,
            place_base: self.place_base,
        });
        let parts = [IoSlice::new(&header), IoSlice::new(block), IoSlice::new(filter)];
        file.write_at(self.position, &parts)?;

        let shared = match self.columns.last() {
            Some(column) => column.shared_length as usize,
            None => 0,
        };
        self.entries.push(BlockEntry {
            first_key: KeyPrefix::of_key(&self.first_key, shared),
            block_offset: block_at.0 as u32,
            filter_offset: filter_at as u32,
            live_rows: row_count.0 as u32,
        });
        if let Some(column) = self.columns.last_mut() {
            column.block_count = BlockCount(column.block_count.0 + 1);
        }

        self.position = FileOffset(filter_at + filter_length as u64);
        self.builder.clear();
        self.hashes.clear();
        self.last_key.clear();
        Ok(())
    }

    /// Check a row can go in: a block it closes and the last block both fit the index
    pub fn has_block_room(&self) -> bool {
        self.entries.len() + 2 <= self.entries.capacity()
    }

    /// Write the block index, the range deletes and the tail, and return the totals
    pub fn finish<File: IoFile>(
        &mut self,
        file: &File,
        segment: SegmentNumber,
        data_end: FileOffset,
        ranges: &[u8],
        record_key: RecordKey,
        inputs: &[SegmentNumber],
    ) -> Result<FooterTotals> {
        self.close_block(file)?;

        let index_at = self.position;
        let index_sum = self.write_index(file)?;
        let index_end = self.position;
        let ranges_at = self.position;
        if !ranges.is_empty() {
            file.write_at(ranges_at, &[IoSlice::new(ranges)])?;
            self.position = FileOffset(ranges_at.0 + ranges.len() as u64);
        }

        // the second copy sits past the gap, so one bad span of the file can't take both
        let second_index_at = FileOffset(self.position.0.max(index_end.0 + COPY_GAP_BYTES));
        self.position = second_index_at;
        self.write_index(file)?;

        // the footer must be whole before a tail can say so, or a power cut could keep the tail alone
        file.sync()?;

        let mut listed = [SegmentNumber(0); MOST_MERGE_SEGMENTS];
        listed[..inputs.len()].copy_from_slice(inputs);
        let head = TailHead {
            segment,
            data_end,
            index_at,
            second_index_at,
            block_count: BlockCount(self.entries.len() as u32),
            ranges_at,
            ranges_length: ByteCount(ranges.len() as u64),
            places: self.places,
            place_base: self.place_base,
            index_sum,
            ranges_sum: checksum(ranges),
            column_count: self.columns.len() as u32,
            input_count: inputs.len() as u32,
            is_output: self.is_output,
            is_partial: self.is_partial,
            is_unwalked: self.is_unwalked,
            inputs: listed,
        };
        let total = self.write_tail(&head, record_key);
        let second_tail_at = self.position;
        let second_end = second_tail_at.0 + total as u64;
        let file_length = ByteCount(second_end + SECOND_TAIL_DISTANCE);
        let first_tail_at = FileOffset(file_length.0 - total as u64);
        file.write_at(second_tail_at, &[IoSlice::new(&self.tail[..total])])?;
        file.write_at(first_tail_at, &[IoSlice::new(&self.tail[..total])])?;
        // a power cut can keep a cut alone, so an earlier seal's tail could end the file and hide every row after it
        file.sync()?;
        file.truncate(file_length)?;
        file.sync()?;
        Ok(FooterTotals {
            head,
            file_length,
            block_count: head.block_count,
            row_count: self.row_count,
        })
    }

    /// Write the block index a buffer at a time, and return its checksum
    pub fn write_index<File: IoFile>(&mut self, file: &File) -> Result<Checksum> {
        let mut sum = Checksum(0);
        let lines_per_write = self.index.len() / BLOCK_ENTRY_BYTES;
        for lines in self.entries.chunks(lines_per_write) {
            for (line, entry) in lines.iter().enumerate() {
                entry.write(&mut self.index[line * BLOCK_ENTRY_BYTES..]);
            }
            let length = lines.len() * BLOCK_ENTRY_BYTES;
            sum = fold_sum(sum, checksum(&self.index[..length]));
            file.write_at(self.position, &[IoSlice::new(&self.index[..length])])?;
            self.position = FileOffset(self.position.0 + length as u64);
        }
        Ok(sum)
    }

    /// Lay out the tail and return its length
    pub fn write_tail(&mut self, head: &TailHead, record_key: RecordKey) -> usize {
        let mut cursor = TailCursor::new(&mut self.tail);
        head.write(&mut cursor);
        for column in &self.columns {
            column.write(&mut cursor);
        }
        let body = TAIL_FIXED_BYTES + self.columns.len() * COLUMN_TAIL_BYTES;
        write_trailer(&mut self.tail, body, record_key)
    }
}

/// Fold one buffer's checksum into a running one
pub fn fold_sum(running: Checksum, next: Checksum) -> Checksum {
    Checksum(running.0.rotate_left(1) ^ next.0)
}

/// Count the bytes two keys share at the front
pub fn shared_prefix(left: &[u8], right: &[u8]) -> usize {
    let mut shared = 0;
    for (left_byte, right_byte) in left.iter().zip(right.iter()) {
        if left_byte != right_byte {
            break;
        }
        shared += 1;
    }
    shared
}
