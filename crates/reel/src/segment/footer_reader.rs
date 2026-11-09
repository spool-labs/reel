//! Reads a sealed segment's tail, block index and row blocks

use std::io::Result;

use reel_api::units::{ByteCount, FileOffset, WritePlace};

use crate::fixed_list::ListOut;
use crate::pool::{HollowItem, touched_capacity, touched_zeroes};
use crate::index::key_hash::KeyHash;
use crate::io::{DeviceReads, InPlace, IoFile, ReadOn, in_place};
use crate::io::device_error::{read_or_damaged, read_or_damaged_on};
use crate::record::record_check::{Checksum, RecordKey, checksum};
use crate::segment::column_directory::Candidate;
use crate::segment::block_cache::{BlockCache, BlockKey};
use crate::segment::block_entry::{BLOCK_ENTRY_BYTES, BlockEntry, KeyPrefix};
use crate::segment::filter_arena::FilterArena;
use crate::segment::block_filter::{may_contain, open_filter};
use crate::segment::footer_writer::fold_sum;
use crate::segment::row_block::{FooterRow, ROW_BLOCK_BYTES, RowBlockWalk};
use crate::segment::segment_table::BlockWindow;
use crate::segment::segment_tail::{
    ColumnTail, MAX_TAIL_BYTES, SECOND_TAIL_DISTANCE, SegmentNumber, TailHead, TailInputs, TailReader, find_tail,
};

/// Segments one page of a directory search holds
pub const CANDIDATE_PAGE: usize = 64;

/// What a filter says about one key
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilterAnswer {
    /// The block can't hold the key
    No,
    /// The block may hold the key
    Maybe,
    /// The filter failed its check
    Damaged,
}

impl FilterAnswer {
    /// Turn a filter's bit into an answer
    pub fn of(may_hold: bool) -> Self {
        if may_hold { Self::Maybe } else { Self::No }
    }
}

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

/// The caches a footer read goes through
#[derive(Clone, Copy, Debug)]
pub struct FooterCaches<'cache> {
    pub resident: &'cache FilterArena,
    pub filters: &'cache BlockCache,
    pub rows: &'cache BlockCache,
}

/// What a read of one window of block lines needs
#[derive(Clone, Copy, Debug)]
pub struct WindowRead<'window> {
    pub segment: SegmentNumber,
    pub window: &'window BlockWindow,
    pub place_base: WritePlace,
}

/// Where a found row sits in its block: its place among the block's rows, and how many the block holds
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RowAt {
    /// The row's place among the block's rows
    pub index: u32,
    /// The rows the block holds
    pub rows: u32,
}

impl RowAt {
    /// A row whose place in its block no read gave
    pub const UNKNOWN: Self = Self { index: u32::MAX, rows: 0 };

    /// Check a read gave the row's place
    pub fn is_known(self) -> bool {
        self.index < self.rows
    }

    /// Read where a walk's last row sits
    pub fn of_walk(walk: &RowBlockWalk<'_>) -> Self {
        Self { index: walk.row_index() as u32, rows: walk.rows() as u32 }
    }
}

/// The buffers one footer read borrows from the pool
#[derive(Debug)]
pub struct FooterReader {
    tail: Vec<u8>,
    block: Vec<u8>,
    filter: Vec<u8>,
    key: Vec<u8>,
    pub candidates: Vec<Candidate>,
    pub found_block: u32,
    pub found_at: RowAt,
    pub tail_inputs: TailInputs,
    /// Segments the last directory walk asked
    pub asked: u64,
}

impl FooterReader {
    /// Allocate the buffers
    pub fn new() -> Self {
        Self {
            tail: touched_zeroes(MAX_TAIL_BYTES),
            block: touched_zeroes(ROW_BLOCK_BYTES),
            filter: touched_zeroes(ROW_BLOCK_BYTES),
            key: touched_capacity(ROW_BLOCK_BYTES, 0),
            candidates: touched_capacity(CANDIDATE_PAGE, Candidate::default()),
            found_block: 0,
            found_at: RowAt::UNKNOWN,
            tail_inputs: TailInputs::default(),
            asked: 0,
        }
    }

    /// Read the heap these buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        let total = self.tail.capacity() + self.block.capacity() + self.filter.capacity() + self.key.capacity();
        ByteCount(total as u64)
    }

    /// Read a segment's tail into columns from either copy, or return nothing when neither is whole
    pub fn read_tail<File: IoFile>(
        &mut self,
        file: &File,
        record_key: RecordKey,
        columns: &mut Vec<ColumnTail>,
    ) -> Result<Option<TailHead>> {
        let length = file.length()?.0;
        if let Some(head) = self.read_tail_ending(file, FileOffset(length), record_key, columns)? {
            return Ok(Some(head));
        }

        let Some(second_end) = length.checked_sub(SECOND_TAIL_DISTANCE) else {
            return Ok(None);
        };
        self.read_tail_ending(file, FileOffset(second_end), record_key, columns)
    }

    /// Read the tail copy that ends at an offset
    pub fn read_tail_ending<File: IoFile>(
        &mut self,
        file: &File,
        end: FileOffset,
        record_key: RecordKey,
        columns: &mut Vec<ColumnTail>,
    ) -> Result<Option<TailHead>> {
        let read = (end.0 as usize).min(MAX_TAIL_BYTES);
        let at = FileOffset(end.0 - read as u64);
        // a bad sector under a copy costs that copy, as a failed check does
        if !read_or_damaged(file, at, &mut self.tail[..read])? {
            return Ok(None);
        }

        let Some(tail) = find_tail(&self.tail[..read], record_key) else {
            return Ok(None);
        };
        let mut input = TailReader::new(tail);
        let Some((head, inputs)) = TailHead::read(&mut input) else {
            return Ok(None);
        };
        self.tail_inputs = inputs;

        columns.clear();
        for _column in 0..head.column_count {
            let Some(column) = ColumnTail::read(&mut input) else {
                return Ok(None);
            };
            columns.push(column);
        }
        Ok(Some(head))
    }

    /// Read a segment's block index from either copy, and return false when neither is whole
    pub fn read_index<File: IoFile>(&mut self, file: &File, head: &TailHead, entries: &mut Vec<BlockEntry>) -> Result<bool> {
        if self.read_index_at(file, head, head.index_at, entries)? {
            return Ok(true);
        }
        self.read_index_at(file, head, head.second_index_at, entries)
    }

    /// Read one copy of a block index, and return false when it fails its check
    pub fn read_index_at<File: IoFile>(
        &mut self,
        file: &File,
        head: &TailHead,
        start: FileOffset,
        entries: &mut Vec<BlockEntry>,
    ) -> Result<bool> {
        entries.clear();
        let mut at = start;
        let mut remaining = head.block_count.0 as usize;
        let mut sum = Checksum(0);
        let lines_per_read = self.block.len() / BLOCK_ENTRY_BYTES;
        // the blocks run in order from the records' end, each with its filter before the next header
        let mut after = head.data_end.0;
        while remaining > 0 {
            let lines = remaining.min(lines_per_read);
            let length = lines * BLOCK_ENTRY_BYTES;
            if !read_or_damaged(file, at, &mut self.block[..length])? {
                return Ok(false);
            }
            sum = fold_sum(sum, checksum(&self.block[..length]));
            for line in 0..lines {
                let Some(entry) = BlockEntry::read(&self.block[line * BLOCK_ENTRY_BYTES..]) else {
                    return Ok(false);
                };
                if !entry.is_shaped(after, self.block.len()) {
                    return Ok(false);
                }
                after = u64::from(entry.filter_offset);
                entries.push(entry);
            }
            at = FileOffset(at.0 + length as u64);
            remaining -= lines;
        }
        Ok(sum == head.index_sum && after <= start.0)
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
                Some(next) => next.header_at(),
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

            match in_place(self.search_block(&InPlace, file, entry, index.place_base, query))? {
                FooterAnswer::Absent => {},
                answer => return Ok(answer),
            }
        }
        Ok(FooterAnswer::Absent)
    }

    /// Read a column's first and last key from its first and last row blocks
    pub fn edge_keys<File: IoFile>(
        &mut self,
        file: &File,
        lines: &[BlockEntry],
        place_base: WritePlace,
        lowest: &mut impl ListOut<u8>,
        highest: &mut impl ListOut<u8>,
    ) -> Result<bool> {
        let (Some(first), Some(last)) = (lines.first(), lines.last()) else {
            return Ok(false);
        };

        let length = first.block_length().0 as usize;
        if length > self.block.len() {
            return Ok(false);
        }
        if !read_or_damaged(file, first.block_at(), &mut self.block[..length])? {
            return Ok(false);
        }
        let Some(mut walk) = RowBlockWalk::open(&self.block[..length], place_base) else {
            return Ok(false);
        };
        lowest.clear();
        if walk.next_row(lowest).is_none() {
            return Ok(false);
        }

        let length = last.block_length().0 as usize;
        if length > self.block.len() {
            return Ok(false);
        }
        if !read_or_damaged(file, last.block_at(), &mut self.block[..length])? {
            return Ok(false);
        }
        let Some(mut walk) = RowBlockWalk::open(&self.block[..length], place_base) else {
            return Ok(false);
        };
        highest.clear();
        while walk.next_row(highest).is_some() {}
        Ok(true)
    }

    /// Lend the filter buffer for a read of this many bytes, or nothing when a filter that long can't be one
    pub fn filter_room(&mut self, length: usize) -> Option<&mut [u8]> {
        self.filter.get_mut(..length)
    }

    /// Search one window of block lines, through the caches
    pub async fn lookup_window<File: IoFile>(
        &mut self,
        device: &impl DeviceReads<File>,
        file: &File,
        read: &WindowRead<'_>,
        caches: &FooterCaches<'_>,
        query: &FooterQuery<'_>,
    ) -> Result<FooterAnswer> {
        for position in 0..read.window.count {
            let entry = read.window.entries[position];
            let filter_end = match read.window.entries.get(position + 1) {
                Some(next) if position + 1 < read.window.count => next.header_at().0,
                _last_line => read.window.filter_end_of_last,
            };
            match self.probe_filter(ReadOn::new(device, file), read.segment, &entry, filter_end, caches, query).await? {
                FilterAnswer::Damaged => return Ok(FooterAnswer::Damaged),
                FilterAnswer::No => continue,
                FilterAnswer::Maybe => {},
            }

            match self.search_cached(device, file, (read.segment, read.place_base), &entry, caches, query).await? {
                FooterAnswer::Absent => {},
                answer => {
                    self.found_block = entry.block_offset;
                    return Ok(answer);
                },
            }
        }
        Ok(FooterAnswer::Absent)
    }

    /// Ask a block's filter, reading and checking it once when the cache lacks it
    pub async fn probe_filter<File: IoFile>(
        &mut self,
        on: ReadOn<'_, impl DeviceReads<File>, File>,
        segment: SegmentNumber,
        entry: &BlockEntry,
        filter_end: u64,
        caches: &FooterCaches<'_>,
        query: &FooterQuery<'_>,
    ) -> Result<FilterAnswer> {
        let ReadOn { device, target: file } = on;
        let key = BlockKey {
            segment,
            offset: entry.filter_offset,
        };
        let hash = query.hash;
        if let Some(may_hold) = caches.resident.probe(segment, entry.filter_offset, hash) {
            return Ok(FilterAnswer::of(may_hold));
        }
        if let Some(may_hold) = caches.filters.with(key, |bits| may_contain(bits, hash)) {
            return Ok(FilterAnswer::of(may_hold));
        }

        let length = (filter_end - u64::from(entry.filter_offset)) as usize;
        if length > self.filter.len() {
            return Ok(FilterAnswer::Damaged);
        }
        let at = FileOffset(u64::from(entry.filter_offset));
        device.read_exact(file, at, &mut self.filter[..length]).await?;
        let Some(bits) = open_filter(&self.filter[..length]) else {
            return Ok(FilterAnswer::Damaged);
        };
        caches.filters.insert(key, bits);
        Ok(FilterAnswer::of(may_contain(bits, hash)))
    }

    /// Read and check a row block and keep it in the cache, or return nothing when it fails its check
    pub async fn load_block<File: IoFile>(
        &mut self,
        device: &impl DeviceReads<File>,
        file: &File,
        key: BlockKey,
        entry: &BlockEntry,
        rows: &BlockCache,
    ) -> Result<Option<&[u8]>> {
        let length = entry.block_length().0 as usize;
        if length > self.block.len() || !read_or_damaged_on(device, file, entry.block_at(), &mut self.block[..length]).await? {
            return Ok(None);
        }
        if RowBlockWalk::open(&self.block[..length], WritePlace(0)).is_none() {
            return Ok(None);
        }
        rows.insert(key, &self.block[..length]);
        Ok(Some(&self.block[..length]))
    }

    /// Search a row block, reading and checking it once when the cache lacks it
    pub async fn search_cached<File: IoFile>(
        &mut self,
        device: &impl DeviceReads<File>,
        file: &File,
        (segment, place_base): (SegmentNumber, WritePlace),
        entry: &BlockEntry,
        caches: &FooterCaches<'_>,
        query: &FooterQuery<'_>,
    ) -> Result<FooterAnswer> {
        let key = BlockKey {
            segment,
            offset: entry.block_offset,
        };
        let buffer = &mut self.key;
        let cached = caches.rows.with(key, |block| search_rows(block, place_base, query, buffer));
        if let Some((answer, at)) = cached {
            self.found_at = at;
            return Ok(answer);
        }

        let answer = self.search_block(device, file, entry, place_base, query).await?;
        if answer != FooterAnswer::Damaged {
            caches.rows.insert(key, &self.block[..entry.block_length().0 as usize]);
        }
        Ok(answer)
    }

    /// Read one row block and find the key's newest row at a read point
    pub async fn search_block<File: IoFile>(
        &mut self,
        device: &impl DeviceReads<File>,
        file: &File,
        entry: &BlockEntry,
        place_base: WritePlace,
        query: &FooterQuery<'_>,
    ) -> Result<FooterAnswer> {
        let length = entry.block_length().0 as usize;
        if length > self.block.len() {
            return Ok(FooterAnswer::Damaged);
        }
        device.read_exact(file, entry.block_at(), &mut self.block[..length]).await?;

        let Some(mut walk) = RowBlockWalk::open(&self.block[..length], place_base) else {
            return Ok(FooterAnswer::Damaged);
        };
        let (answer, at) = newest_at(&mut walk, query, &mut self.key);
        self.found_at = at;
        Ok(answer)
    }
}

impl Default for FooterReader {
    fn default() -> Self {
        Self::new()
    }
}

/// Find a key's newest row at a read point in a block already checked
pub fn search_rows(block: &[u8], place_base: WritePlace, query: &FooterQuery<'_>, key: &mut impl ListOut<u8>) -> (FooterAnswer, RowAt) {
    let Some(mut walk) = RowBlockWalk::open_checked(block, place_base) else {
        return (FooterAnswer::Damaged, RowAt::UNKNOWN);
    };
    newest_at(&mut walk, query, key)
}

/// Walk from a seek to the key's newest row at a read point, and say where a found row sits in the block
pub fn newest_at(walk: &mut RowBlockWalk, query: &FooterQuery<'_>, key: &mut impl ListOut<u8>) -> (FooterAnswer, RowAt) {
    let Some(mut row) = walk.seek(query.key, key) else {
        return (FooterAnswer::Absent, RowAt::UNKNOWN);
    };

    loop {
        if **key != *query.key {
            return (FooterAnswer::Absent, RowAt::UNKNOWN);
        }
        if row.place <= query.read_point {
            return (FooterAnswer::Found(row), RowAt::of_walk(walk));
        }
        match walk.next_row(key) {
            Some(next) => row = next,
            None => return (FooterAnswer::Absent, RowAt::UNKNOWN),
        }
    }
}

/// Find the first block that can hold a key: the last one starting before its prefix
pub fn first_candidate(entries: &[BlockEntry], prefix: KeyPrefix) -> usize {
    let after = entries.partition_point(|entry| entry.first_key < prefix);
    after.saturating_sub(1)
}

impl FooterReader {
    /// Allocate a reader for reads alone, with no room for a tail
    pub fn without_tail() -> Self {
        Self { tail: Vec::new(), ..Self::new() }
    }
}

impl HollowItem for FooterReader {
    fn hollow() -> Self {
        Self {
            tail: Vec::new(),
            block: Vec::new(),
            filter: Vec::new(),
            key: Vec::new(),
            candidates: Vec::new(),
            found_block: 0,
            found_at: RowAt::UNKNOWN,
            tail_inputs: TailInputs::default(),
            asked: 0,
        }
    }
}
