//! Every sealed segment's handle, tail and block index, sharded so no read takes a global lock

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use reel_api::column_id::ColumnId;
use reel_api::units::ByteCount;

use crate::pool::touched_capacity;
use crate::segment::block_entry::{BlockEntry, KeyPrefix};
use crate::segment::footer_reader::first_candidate;
use crate::segment::segment_tail::{ColumnTail, SegmentNumber, TailHead};

/// Shards of the table
pub const TABLE_SHARDS: usize = 16;
/// Block lines one search copies out, so no lock stays held across a read
pub const WINDOW_BLOCKS: usize = 4;
/// Picks a shard from the top of a mixed segment number
pub const SHARD_SHIFT: u32 = u64::BITS - TABLE_SHARDS.trailing_zeros();
/// Marks an empty slot in a number map
pub const NO_SLOT: u32 = u32::MAX;
/// Spreads segment numbers over a number map
pub const NUMBER_MIX: u64 = 0x9E37_79B9_7F4A_7C15;

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
    pub file: File,
    pub head: TailHead,
    pub columns: Extent,
    pub blocks: Extent,
    pub is_walked: bool,
}

/// A segment held open for a read, let go when dropped
#[derive(Debug)]
pub struct SegmentPin<'table, File> {
    pub file: File,
    pub head: TailHead,
    pub is_walked: bool,
    pin: &'table AtomicU32,
}

/// A segment taken out of the table, whose file closes once its pins are gone
#[derive(Debug)]
pub struct RetiredSegment<'table, File> {
    pub file: File,
    pub head: TailHead,
    pin: &'table AtomicU32,
}

/// One row block of a segment, for the dead-byte walk
#[derive(Clone, Copy, Debug)]
pub struct WalkBlock {
    pub column: ColumnId,
    pub entry: BlockEntry,
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

/// An open-addressed map from a segment number to its slot, of fixed size
#[derive(Clone, Debug)]
pub struct NumberMap {
    numbers: Vec<SegmentNumber>,
    slots: Vec<u32>,
    mask: usize,
}

/// One shard: fixed pools for tails and block lines, and fixed slots for segments
#[derive(Debug)]
pub struct TableShard<File> {
    pub segments: Vec<Option<TableEntry<File>>>,
    pub map: NumberMap,
    pub free_slots: Vec<u32>,
    pub columns: Vec<ColumnTail>,
    pub blocks: Vec<BlockEntry>,
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

impl<File> Drop for SegmentPin<'_, File> {
    fn drop(&mut self) {
        self.pin.fetch_sub(1, Ordering::AcqRel);
    }
}

impl<File> RetiredSegment<'_, File> {
    /// Count the reads still holding the segment
    pub fn pin_count(&self) -> u32 {
        self.pin.load(Ordering::Acquire)
    }
}

impl NumberMap {
    /// Allocate a map for a number of segments
    pub fn new(capacity: usize) -> Self {
        let size = (capacity.max(1) * 2).next_power_of_two();
        Self {
            numbers: vec![SegmentNumber(0); size],
            slots: vec![NO_SLOT; size],
            mask: size - 1,
        }
    }

    /// Find where a number sits, or the empty place it belongs
    pub fn probe(&self, number: SegmentNumber) -> usize {
        let mut index = home_of(number, self.mask);
        while self.slots[index] != NO_SLOT && self.numbers[index] != number {
            index = (index + 1) & self.mask;
        }
        index
    }

    /// Find a number's slot
    pub fn get(&self, number: SegmentNumber) -> Option<u32> {
        let slot = self.slots[self.probe(number)];
        if slot == NO_SLOT { None } else { Some(slot) }
    }

    /// Add a number
    pub fn insert(&mut self, number: SegmentNumber, slot: u32) {
        let index = self.probe(number);
        self.numbers[index] = number;
        self.slots[index] = slot;
    }

    /// Remove a number, moving later ones back so no probe breaks
    pub fn remove(&mut self, number: SegmentNumber) {
        let mut hole = self.probe(number);
        if self.slots[hole] == NO_SLOT {
            return;
        }

        let mut next = (hole + 1) & self.mask;
        while self.slots[next] != NO_SLOT {
            let home = home_of(self.numbers[next], self.mask);
            let distance_to_hole = hole.wrapping_sub(home) & self.mask;
            let distance_to_next = next.wrapping_sub(home) & self.mask;
            if distance_to_hole < distance_to_next {
                self.numbers[hole] = self.numbers[next];
                self.slots[hole] = self.slots[next];
                hole = next;
            }
            next = (next + 1) & self.mask;
        }
        self.slots[hole] = NO_SLOT;
    }
}

impl<File: Clone> SegmentTable<File> {
    /// Allocate every pool
    pub fn new(sizing: &TableSizing) -> Self {
        let mut shards = Vec::with_capacity(TABLE_SHARDS);
        let mut pins = Vec::with_capacity(TABLE_SHARDS);
        for _shard in 0..TABLE_SHARDS {
            let mut segments = Vec::with_capacity(sizing.segments_per_shard);
            let mut free_slots = Vec::with_capacity(sizing.segments_per_shard);
            let mut shard_pins = Vec::with_capacity(sizing.segments_per_shard);
            for slot in 0..sizing.segments_per_shard {
                segments.push(None);
                free_slots.push(slot as u32);
                shard_pins.push(AtomicU32::new(0));
            }
            shards.push(RwLock::new(TableShard {
                segments,
                map: NumberMap::new(sizing.segments_per_shard),
                free_slots,
                columns: touched_capacity(sizing.columns_per_shard, ColumnTail::default()),
                blocks: touched_capacity(sizing.blocks_per_shard, BlockEntry::default()),
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
            total += guard.segments.capacity() * size_of::<Option<TableEntry<File>>>();
            total += guard.columns.capacity() * size_of::<ColumnTail>();
            total += guard.blocks.capacity() * size_of::<BlockEntry>();
            total += guard.free_slots.capacity() * size_of::<u32>();
            total += guard.map.slots.capacity() * (size_of::<u32>() + size_of::<SegmentNumber>());
        }
        ByteCount(total as u64)
    }

    /// Install a sealed segment, or return false when its shard has no room
    pub fn install(&self, file: File, head: &TailHead, columns: &[ColumnTail], blocks: &[BlockEntry]) -> bool {
        let mut guard = self.write_shard(shard_of(head.segment));
        if !guard.has_room(columns, blocks) {
            guard.compact();
        }
        if !guard.has_room(columns, blocks) {
            return false;
        }
        let Some(slot) = guard.free_slots.pop() else {
            return false;
        };

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
        guard.segments[slot as usize] = Some(TableEntry {
            number: head.segment,
            file,
            head: *head,
            columns: column_extent,
            blocks: block_extent,
            is_walked: false,
        });
        guard.map.insert(head.segment, slot);
        true
    }

    /// Count the segments in the table
    pub fn segment_count(&self) -> u64 {
        let mut count = 0;
        for shard in 0..TABLE_SHARDS {
            count += self.read_shard(shard).segments.iter().flatten().count() as u64;
        }
        count
    }

    /// Check the table has a segment
    pub fn holds(&self, number: SegmentNumber) -> bool {
        self.read_shard(shard_of(number)).map.get(number).is_some()
    }

    /// Hold a segment open for a read, or return nothing when it has left the table
    pub fn pin(&self, number: SegmentNumber) -> Option<SegmentPin<'_, File>> {
        let shard = shard_of(number);
        let guard = self.read_shard(shard);
        let slot = guard.map.get(number)?;
        let entry = guard.segments[slot as usize].as_ref()?;
        let pin = &self.pins[shard][slot as usize];
        pin.fetch_add(1, Ordering::AcqRel);
        Some(SegmentPin {
            file: entry.file.clone(),
            head: entry.head,
            is_walked: entry.is_walked,
            pin,
        })
    }

    /// Take a block's dead rows off its live count
    pub fn drop_live(&self, number: SegmentNumber, block: u32, rows: u32) {
        let mut guard = self.write_shard(shard_of(number));
        let Some(slot) = guard.map.get(number) else {
            return;
        };
        let Some(start) = guard.segments[slot as usize].as_ref().map(|entry| entry.blocks.start) else {
            return;
        };
        if let Some(line) = guard.blocks.get_mut(start as usize + block as usize) {
            line.live_rows = line.live_rows.saturating_sub(rows);
        }
    }

    /// Find which of a segment's blocks starts at an offset in its file
    pub fn block_at(&self, number: SegmentNumber, offset: u32) -> Option<u32> {
        let guard = self.read_shard(shard_of(number));
        let slot = guard.map.get(number)?;
        let entry = guard.segments[slot as usize].as_ref()?;
        let first = entry.blocks.start as usize;
        let lines = guard.blocks.get(first..first + entry.blocks.length as usize)?;
        let index = lines.partition_point(|line| line.block_offset < offset);
        lines.get(index).filter(|line| line.block_offset == offset).map(|_line| index as u32)
    }

    /// Add up a segment's live rows, block by block
    pub fn live_rows(&self, number: SegmentNumber) -> Option<u64> {
        let guard = self.read_shard(shard_of(number));
        let slot = guard.map.get(number)?;
        let entry = guard.segments[slot as usize].as_ref()?;
        let first = entry.blocks.start as usize;
        let lines = guard.blocks.get(first..first + entry.blocks.length as usize)?;
        Some(lines.iter().map(|line| u64::from(line.live_rows)).sum())
    }

    /// Note a segment's walk has finished, so other walks may count against it
    pub fn mark_walked(&self, number: SegmentNumber) {
        let mut guard = self.write_shard(shard_of(number));
        let Some(slot) = guard.map.get(number) else {
            return;
        };
        if let Some(entry) = guard.segments[slot as usize].as_mut() {
            entry.is_walked = true;
        }
    }

    /// Copy out one of a segment's block lines, counted over all its columns, and the column it belongs to
    pub fn walk_block(&self, number: SegmentNumber, index: u32) -> Option<WalkBlock> {
        let guard = self.read_shard(shard_of(number));
        let slot = guard.map.get(number)?;
        let entry = guard.segments[slot as usize].as_ref()?;
        let line = guard.blocks.get(entry.blocks.start as usize + index as usize)?;
        let first = entry.columns.start as usize;
        let columns = guard.columns.get(first..first + entry.columns.length as usize)?;
        let column = columns.iter().find(|tail| index >= tail.first_block && index - tail.first_block < tail.block_count.0)?;
        Some(WalkBlock {
            column: column.column,
            entry: *line,
        })
    }

    /// Take a segment out so no new read can pin it
    pub fn retire(&self, number: SegmentNumber) -> Option<RetiredSegment<'_, File>> {
        let shard = shard_of(number);
        let mut guard = self.write_shard(shard);
        let slot = guard.map.get(number)?;
        let entry = guard.segments[slot as usize].take()?;
        guard.map.remove(number);
        guard.free_slots.push(slot);
        Some(RetiredSegment {
            file: entry.file,
            head: entry.head,
            pin: &self.pins[shard][slot as usize],
        })
    }

    /// Copy out the block lines that can hold a key, from a block onward
    pub fn block_window(&self, number: SegmentNumber, column: ColumnId, key: &[u8], skip: usize) -> Option<BlockWindow> {
        let guard = self.read_shard(shard_of(number));
        let slot = guard.map.get(number)?;
        let entry = guard.segments[slot as usize].as_ref()?;
        let column_tail = guard.column_of(entry, column)?;
        let first = entry.blocks.start as usize + column_tail.first_block as usize;
        let lines = guard.blocks.get(first..first + column_tail.block_count.0 as usize)?;

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
                Some(next) => next.header_at().0,
                None => filter_end(entry, column_tail, &guard.blocks),
            };
        }
        Some(window)
    }
}

impl<File> TableShard<File> {
    /// Check a segment's tail and block lines fit the pools
    pub fn has_room(&self, columns: &[ColumnTail], blocks: &[BlockEntry]) -> bool {
        self.columns.len() + columns.len() <= self.columns.capacity()
            && self.blocks.len() + blocks.len() <= self.blocks.capacity()
    }

    /// Find a segment's tail of one column
    pub fn column_of(&self, entry: &TableEntry<File>, column: ColumnId) -> Option<&ColumnTail> {
        let start = entry.columns.start as usize;
        let end = start + entry.columns.length as usize;
        self.columns[start..end].iter().find(|tail| tail.column == column)
    }

    /// Close the holes retired segments left in the pools, moving runs in pool order so none lands on another
    pub fn compact(&mut self) {
        let mut column_end = 0;
        let mut moved_columns = 0;
        while let Some(index) = self.next_run(moved_columns, |entry| entry.columns) {
            let Some(entry) = &mut self.segments[index] else {
                break;
            };
            let run = entry.columns;
            entry.columns.start = column_end as u32;
            let range = run.start as usize..(run.start + run.length) as usize;
            self.columns.copy_within(range, column_end);
            column_end += run.length as usize;
            moved_columns = run.start + run.length.max(1);
        }
        self.columns.truncate(column_end);

        let mut block_end = 0;
        let mut moved_blocks = 0;
        while let Some(index) = self.next_run(moved_blocks, |entry| entry.blocks) {
            let Some(entry) = &mut self.segments[index] else {
                break;
            };
            let run = entry.blocks;
            entry.blocks.start = block_end as u32;
            let range = run.start as usize..(run.start + run.length) as usize;
            self.blocks.copy_within(range, block_end);
            block_end += run.length as usize;
            moved_blocks = run.start + run.length.max(1);
        }
        self.blocks.truncate(block_end);
    }

    /// Find the segment whose run starts lowest at or past a pool position
    pub fn next_run(&self, from: u32, run_of: fn(&TableEntry<File>) -> Extent) -> Option<usize> {
        let mut best: Option<(u32, usize)> = None;
        for (index, slot) in self.segments.iter().enumerate() {
            let Some(entry) = slot else {
                continue;
            };
            let start = run_of(entry).start;
            if start >= from && best.is_none_or(|(lowest, _index)| start < lowest) {
                best = Some((start, index));
            }
        }
        best.map(|(_start, index)| index)
    }
}

/// Find where a column's last filter ends: at the next column's first block, or the block index
pub fn filter_end<File>(entry: &TableEntry<File>, column: &ColumnTail, blocks: &[BlockEntry]) -> u64 {
    let next = entry.blocks.start as usize + column.first_block as usize + column.block_count.0 as usize;
    let extent_end = (entry.blocks.start + entry.blocks.length) as usize;
    if next < extent_end {
        return blocks[next].header_at().0;
    }
    entry.head.index_at.0
}

/// Pick a segment's shard
pub fn shard_of(number: SegmentNumber) -> usize {
    (number.0.wrapping_mul(NUMBER_MIX) >> SHARD_SHIFT) as usize
}

/// Pick a number's first place in a map
pub fn home_of(number: SegmentNumber, mask: usize) -> usize {
    (number.0.wrapping_mul(NUMBER_MIX) >> 32) as usize & mask
}
