//! Every sealed segment's handle, tail and block index, sharded so no read takes a global lock

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use reel_api::column_id::ColumnId;
use reel_api::units::ByteCount;

use crate::segment::block_entry::{BlockEntry, KeyPrefix};
use crate::segment::footer_reader::first_candidate;
use crate::segment::segment_tail::{ColumnTail, SegmentNumber, TailHead};

/// Shards of the table
pub const TABLE_SHARDS: usize = 16;
/// Block lines one search copies out, so no lock stays held across a read
pub const WINDOW_BLOCKS: usize = 4;

/// A run of a pool
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Extent {
    pub start: u32,
    pub length: u32,
}

/// One sealed segment in the table
#[derive(Clone, Debug)]
pub struct TableEntry<File> {
    pub number: SegmentNumber,
    pub file: Option<File>,
    pub head: TailHead,
    pub columns: Extent,
    pub blocks: Extent,
    pub pin_slot: usize,
}

/// A segment held open for a read
#[derive(Debug)]
pub struct PinnedSegment<File> {
    pub file: File,
    pub head: TailHead,
    pub pin_slot: usize,
    pub shard: usize,
}

/// A few block lines around a key, with what a read of them needs
#[derive(Clone, Copy, Debug)]
pub struct BlockWindow {
    pub column: ColumnTail,
    pub entries: [BlockEntry; WINDOW_BLOCKS],
    pub count: usize,
    pub filter_end_of_last: u64,
    pub has_more: bool,
}

/// One shard: fixed pools for tails and block lines, and a fixed list of segments
#[derive(Debug)]
pub struct TableShard<File> {
    pub segments: Vec<TableEntry<File>>,
    pub columns: Vec<ColumnTail>,
    pub blocks: Vec<BlockEntry>,
    pub free_pins: Vec<usize>,
}

/// The table of a volume
#[derive(Debug)]
pub struct SegmentTable<File> {
    shards: Vec<RwLock<TableShard<File>>>,
    pins: Vec<Vec<AtomicU32>>,
}

/// How much of each pool one shard gets
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TableSizing {
    pub segments_per_shard: usize,
    pub columns_per_shard: usize,
    pub blocks_per_shard: usize,
}

impl<File: Clone> SegmentTable<File> {
    /// Allocate every pool
    pub fn new(sizing: &TableSizing) -> Self {
        let mut shards = Vec::with_capacity(TABLE_SHARDS);
        let mut pins = Vec::with_capacity(TABLE_SHARDS);
        for _shard in 0..TABLE_SHARDS {
            let mut free_pins = Vec::with_capacity(sizing.segments_per_shard);
            let mut shard_pins = Vec::with_capacity(sizing.segments_per_shard);
            for slot in 0..sizing.segments_per_shard {
                free_pins.push(slot);
                shard_pins.push(AtomicU32::new(0));
            }
            shards.push(RwLock::new(TableShard {
                segments: Vec::with_capacity(sizing.segments_per_shard),
                columns: Vec::with_capacity(sizing.columns_per_shard),
                blocks: Vec::with_capacity(sizing.blocks_per_shard),
                free_pins,
            }));
            pins.push(shard_pins);
        }
        Self { shards, pins }
    }

    /// Read a shard
    pub fn read_shard(&self, shard: usize) -> RwLockReadGuard<'_, TableShard<File>> {
        match self.shards[shard].read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Change a shard
    pub fn write_shard(&self, shard: usize) -> RwLockWriteGuard<'_, TableShard<File>> {
        match self.shards[shard].write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Read the heap every shard holds
    pub fn heap_bytes(&self) -> ByteCount {
        let mut total = 0;
        for shard in 0..TABLE_SHARDS {
            let guard = self.read_shard(shard);
            total += guard.segments.capacity() * size_of::<TableEntry<File>>();
            total += guard.columns.capacity() * size_of::<ColumnTail>();
            total += guard.blocks.capacity() * size_of::<BlockEntry>();
            total += guard.free_pins.capacity() * size_of::<usize>();
        }
        ByteCount(total as u64)
    }

    /// Install a sealed segment, or return false when its shard has no room
    pub fn install(&self, file: File, head: &TailHead, columns: &[ColumnTail], blocks: &[BlockEntry]) -> bool {
        let shard = shard_of(head.segment);
        let mut guard = self.write_shard(shard);
        let has_room = guard.columns.len() + columns.len() <= guard.columns.capacity()
            && guard.blocks.len() + blocks.len() <= guard.blocks.capacity()
            && guard.segments.len() < guard.segments.capacity();
        if !has_room {
            guard.compact();
        }
        let Some(pin_slot) = guard.free_pins.pop() else {
            return false;
        };
        if guard.columns.len() + columns.len() > guard.columns.capacity()
            || guard.blocks.len() + blocks.len() > guard.blocks.capacity()
        {
            guard.free_pins.push(pin_slot);
            return false;
        }

        let column_extent = Extent {
            start: guard.columns.len() as u32,
            length: columns.len() as u32,
        };
        guard.columns.extend_from_slice(columns);
        let block_extent = Extent {
            start: guard.blocks.len() as u32,
            length: blocks.len() as u32,
        };
        guard.blocks.extend_from_slice(blocks);
        guard.segments.push(TableEntry {
            number: head.segment,
            file: Some(file),
            head: *head,
            columns: column_extent,
            blocks: block_extent,
            pin_slot,
        });
        true
    }

    /// Hold a segment open for a read, or return nothing when it has left the table
    pub fn pin(&self, number: SegmentNumber) -> Option<PinnedSegment<File>> {
        let shard = shard_of(number);
        let guard = self.read_shard(shard);
        let entry = guard.find(number)?;
        let file = entry.file.clone()?;
        self.pins[shard][entry.pin_slot].fetch_add(1, Ordering::AcqRel);
        Some(PinnedSegment {
            file,
            head: entry.head,
            pin_slot: entry.pin_slot,
            shard,
        })
    }

    /// Let go of a pinned segment
    pub fn unpin(&self, pinned: &PinnedSegment<File>) {
        self.pins[pinned.shard][pinned.pin_slot].fetch_sub(1, Ordering::AcqRel);
    }

    /// Copy out the block lines that can hold a key, from a block onward
    pub fn block_window(&self, number: SegmentNumber, column: ColumnId, key: &[u8], skip: usize) -> Option<BlockWindow> {
        let guard = self.read_shard(shard_of(number));
        let entry = guard.find(number)?;
        let column_tail = guard.column_of(entry, column)?;
        let first = entry.blocks.start as usize + column_tail.first_block as usize;
        let lines = &guard.blocks[first..first + column_tail.block_count.0 as usize];

        let prefix = KeyPrefix::of_key(key, column_tail.shared_length as usize);
        let start = first_candidate(lines, prefix) + skip;
        let mut window = BlockWindow {
            column: *column_tail,
            entries: [BlockEntry::default(); WINDOW_BLOCKS],
            count: 0,
            filter_end_of_last: 0,
            has_more: false,
        };

        for (index, line) in lines.iter().enumerate().skip(start) {
            if line.first_key > prefix {
                break;
            }
            if window.count == WINDOW_BLOCKS {
                window.has_more = true;
                break;
            }
            window.entries[window.count] = *line;
            window.count += 1;
            window.filter_end_of_last = match lines.get(index + 1) {
                Some(next) => u64::from(next.block_offset),
                None => filter_end(entry, column_tail, &guard.blocks),
            };
        }
        Some(window)
    }
}

impl<File> TableShard<File> {
    /// Find a segment
    pub fn find(&self, number: SegmentNumber) -> Option<&TableEntry<File>> {
        self.segments.iter().find(|entry| entry.number == number)
    }

    /// Find a segment's tail of one column
    pub fn column_of(&self, entry: &TableEntry<File>, column: ColumnId) -> Option<&ColumnTail> {
        let start = entry.columns.start as usize;
        let end = start + entry.columns.length as usize;
        self.columns[start..end].iter().find(|tail| tail.column == column)
    }

    /// Close the holes retired segments left in the pools
    pub fn compact(&mut self) {
        let mut column_end = 0;
        let mut block_end = 0;
        for index in 0..self.segments.len() {
            let columns = self.segments[index].columns;
            let blocks = self.segments[index].blocks;
            let column_range = columns.start as usize..(columns.start + columns.length) as usize;
            self.columns.copy_within(column_range, column_end);
            let block_range = blocks.start as usize..(blocks.start + blocks.length) as usize;
            self.blocks.copy_within(block_range, block_end);

            self.segments[index].columns.start = column_end as u32;
            self.segments[index].blocks.start = block_end as u32;
            column_end += columns.length as usize;
            block_end += blocks.length as usize;
        }
        self.columns.truncate(column_end);
        self.blocks.truncate(block_end);
    }
}

/// Find where a column's last filter ends: at the next column's first block, or the block index
pub fn filter_end<File>(entry: &TableEntry<File>, column: &ColumnTail, blocks: &[BlockEntry]) -> u64 {
    let next = entry.blocks.start as usize + column.first_block as usize + column.block_count.0 as usize;
    let extent_end = (entry.blocks.start + entry.blocks.length) as usize;
    if next < extent_end {
        return u64::from(blocks[next].block_offset);
    }
    entry.head.index_at.0
}

/// Pick a segment's shard
pub fn shard_of(number: SegmentNumber) -> usize {
    (number.0 % TABLE_SHARDS as u64) as usize
}
