//! Reads a sealed segment's tail, block index and row blocks

use std::io::Result;

use reel_api::units::{ByteCount, FileOffset, WritePlace};

use crate::index::key_hash::KeyHash;
use crate::io::IoFile;
use crate::record::record_check::{Checksum, RecordKey, checksum};
use crate::segment::block_entry::{BLOCK_ENTRY_BYTES, BlockEntry, KeyPrefix};
use crate::segment::block_filter::{may_contain, open_filter};
use crate::segment::footer_writer::fold_sum;
use crate::segment::row_block::{FooterRow, ROW_BLOCK_BYTES, RowBlockWalk};
use crate::segment::segment_tail::{ColumnTail, MAX_TAIL_BYTES, TailHead, TailReader, find_tail};

/// What a footer says about one key
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FooterAnswer {
    /// The segment holds this row as the key's newest at the read point
    Found(FooterRow),
    /// The segment holds no such row
    Absent,
    /// A row block or filter failed its check
    Damaged,
}

/// One column's slice of a segment's resident index
#[derive(Clone, Copy, Debug)]
pub struct ColumnIndex<'index> {
    pub column: &'index ColumnTail,
    pub entries: &'index [BlockEntry],
    pub filter_end: FileOffset,
    pub place_base: WritePlace,
}

/// A key to find, as of a place in the write order
#[derive(Clone, Copy, Debug)]
pub struct FooterQuery<'key> {
    pub hash: KeyHash,
    pub key: &'key [u8],
    pub read_point: WritePlace,
}

/// The buffers one footer read borrows from the pool
#[derive(Debug)]
pub struct FooterReader {
    tail: Vec<u8>,
    block: Vec<u8>,
    filter: Vec<u8>,
    key: Vec<u8>,
}

impl FooterReader {
    /// Allocate the buffers
    pub fn new() -> Self {
        Self {
            tail: vec![0; MAX_TAIL_BYTES],
            block: vec![0; ROW_BLOCK_BYTES],
            filter: vec![0; ROW_BLOCK_BYTES],
            key: Vec::with_capacity(ROW_BLOCK_BYTES),
        }
    }

    /// Read the heap these buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        let total = self.tail.capacity() + self.block.capacity() + self.filter.capacity() + self.key.capacity();
        ByteCount(total as u64)
    }

    /// Read a segment's tail into columns, or return nothing when the segment isn't sealed
    pub fn read_tail<File: IoFile>(
        &mut self,
        file: &File,
        record_key: RecordKey,
        columns: &mut Vec<ColumnTail>,
    ) -> Result<Option<TailHead>> {
        let length = file.length()?.0 as usize;
        let read = length.min(MAX_TAIL_BYTES);
        let at = FileOffset((length - read) as u64);
        file.read_exact_at(at, &mut self.tail[..read])?;

        let Some(tail) = find_tail(&self.tail[..read], record_key) else {
            return Ok(None);
        };
        let mut input = TailReader::new(tail);
        let Some(head) = TailHead::read(&mut input) else {
            return Ok(None);
        };

        columns.clear();
        for _column in 0..head.column_count {
            let Some(column) = ColumnTail::read(&mut input) else {
                return Ok(None);
            };
            columns.push(column);
        }
        Ok(Some(head))
    }

    /// Read a segment's block index, and return false when it fails its check
    pub fn read_index<File: IoFile>(&mut self, file: &File, head: &TailHead, entries: &mut Vec<BlockEntry>) -> Result<bool> {
        entries.clear();
        let mut at = head.index_at;
        let mut remaining = head.block_count.0 as usize;
        let mut sum = Checksum(0);
        let lines_per_read = self.block.len() / BLOCK_ENTRY_BYTES;
        while remaining > 0 {
            let lines = remaining.min(lines_per_read);
            let length = lines * BLOCK_ENTRY_BYTES;
            file.read_exact_at(at, &mut self.block[..length])?;
            sum = fold_sum(sum, checksum(&self.block[..length]));
            for line in 0..lines {
                let Some(entry) = BlockEntry::read(&self.block[line * BLOCK_ENTRY_BYTES..]) else {
                    return Ok(false);
                };
                entries.push(entry);
            }
            at = FileOffset(at.0 + length as u64);
            remaining -= lines;
        }
        Ok(sum == head.index_sum)
    }

    /// Find a key's newest row at a read point, through its filter and one row block
    pub fn lookup<File: IoFile>(&mut self, file: &File, index: &ColumnIndex, query: &FooterQuery) -> Result<FooterAnswer> {
        let prefix = KeyPrefix::of_key(query.key, index.column.shared_length as usize);
        let first = first_candidate(index.entries, prefix);

        for position in first..index.entries.len() {
            let entry = &index.entries[position];
            if entry.first_key > prefix {
                break;
            }

            let filter_ends_at = match index.entries.get(position + 1) {
                Some(next) => next.block_at(),
                None => index.filter_end,
            };
            let filter_at = FileOffset(u64::from(entry.filter_offset));
            let filter_length = (filter_ends_at.0 - filter_at.0) as usize;
            if filter_length > self.filter.len() {
                return Ok(FooterAnswer::Damaged);
            }
            file.read_exact_at(filter_at, &mut self.filter[..filter_length])?;
            let Some(bits) = open_filter(&self.filter[..filter_length]) else {
                return Ok(FooterAnswer::Damaged);
            };
            if !may_contain(bits, query.hash) {
                continue;
            }

            match self.search_block(file, entry, index.place_base, query)? {
                FooterAnswer::Absent => {},
                answer => return Ok(answer),
            }
        }
        Ok(FooterAnswer::Absent)
    }

    /// Read one row block and find the key's newest row at a read point
    pub fn search_block<File: IoFile>(
        &mut self,
        file: &File,
        entry: &BlockEntry,
        place_base: WritePlace,
        query: &FooterQuery,
    ) -> Result<FooterAnswer> {
        let length = entry.block_length().0 as usize;
        if length > self.block.len() {
            return Ok(FooterAnswer::Damaged);
        }
        file.read_exact_at(entry.block_at(), &mut self.block[..length])?;

        let Some(mut walk) = RowBlockWalk::open(&self.block[..length], place_base) else {
            return Ok(FooterAnswer::Damaged);
        };
        let Some(mut row) = walk.seek(query.key, &mut self.key) else {
            return Ok(FooterAnswer::Absent);
        };

        loop {
            if self.key != query.key {
                return Ok(FooterAnswer::Absent);
            }
            if row.place <= query.read_point {
                return Ok(FooterAnswer::Found(row));
            }
            match walk.next_row(&mut self.key) {
                Some(next) => row = next,
                None => return Ok(FooterAnswer::Absent),
            }
        }
    }
}

impl Default for FooterReader {
    fn default() -> Self {
        Self::new()
    }
}

/// Find the first block that can hold a key: the last one starting before its prefix
pub fn first_candidate(entries: &[BlockEntry], prefix: KeyPrefix) -> usize {
    let after = entries.partition_point(|entry| entry.first_key < prefix);
    after.saturating_sub(1)
}
