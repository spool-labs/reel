//! Every sealed segment's handle, tail and block index, sharded so no read takes a global lock

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::RwLock;

use reel_api::column_id::ColumnId;
use reel_api::direction::Direction;
use reel_api::units::{ByteCount, RootId, WritePlace};

use crate::pool::touched_capacity;
use crate::guard::Shards;
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
    pub root: RootId,
    pub head: TailHead,
    pub columns: Extent,
    pub blocks: Extent,
    pub is_walked: bool,
    pub highest_replacement: WritePlace,
    pub has_held_frees: bool,
    pub is_unproven: bool,
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
#[derive(Clone, Debug)]
pub struct RetiredSegment<File> {
    pub file: File,
    pub shard: usize,
    pub slot: u32,
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
    pub shards: Shards<TableShard<File>>,
    pins: Vec<Vec<AtomicU32>>,
    /// Each slot's walked flag, which a cursor holding the slot reads with no lock
    walked: Vec<Vec<AtomicBool>>,
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

    /// Read how many numbers the map holds with room for every probe to end
    pub fn room(&self) -> usize {
        self.slots.len() / 2
    }

    /// Empty the map
    pub fn clear(&mut self) {
        self.slots.fill(NO_SLOT);
    }

    /// Read the heap the map holds
    pub fn heap_bytes(&self) -> usize {
        self.numbers.capacity() * size_of::<SegmentNumber>() + self.slots.capacity() * size_of::<u32>()
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

    /// Move every slot past an offset back by a length, for rows that moved down over a removed one
    pub fn shift_past(&mut self, at: u32, length: u32) {
        for slot in self.slots.iter_mut().filter(|slot| **slot != NO_SLOT && **slot > at) {
            *slot -= length;
        }
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
        let mut walked = Vec::with_capacity(TABLE_SHARDS);
        for _shard in 0..TABLE_SHARDS {
            let mut segments = Vec::with_capacity(sizing.segments_per_shard);
            let mut free_slots = Vec::with_capacity(sizing.segments_per_shard);
            let mut shard_pins = Vec::with_capacity(sizing.segments_per_shard);
            let mut shard_walked = Vec::with_capacity(sizing.segments_per_shard);
            for slot in 0..sizing.segments_per_shard {
                segments.push(None);
                free_slots.push(slot as u32);
                shard_pins.push(AtomicU32::new(0));
                shard_walked.push(AtomicBool::new(false));
            }
            shards.push(RwLock::new(TableShard {
                segments,
                map: NumberMap::new(sizing.segments_per_shard),
                free_slots,
                columns: touched_capacity(sizing.columns_per_shard, ColumnTail::default()),
                blocks: touched_capacity(sizing.blocks_per_shard, BlockEntry::default()),
            }));
            pins.push(shard_pins);
            walked.push(shard_walked);
        }
        Self { shards: Shards(shards), pins, walked }
    }

    /// Read the heap every shard holds
    pub fn heap_bytes(&self) -> ByteCount {
        let mut total = 0;
        for shard in 0..TABLE_SHARDS {
            let guard = self.shards.read(shard);
            total += guard.segments.capacity() * size_of::<Option<TableEntry<File>>>();
            total += guard.columns.capacity() * size_of::<ColumnTail>();
            total += guard.blocks.capacity() * size_of::<BlockEntry>();
            total += guard.free_slots.capacity() * size_of::<u32>();
            total += guard.map.slots.capacity() * (size_of::<u32>() + size_of::<SegmentNumber>());
            total += self.pins[shard].capacity() * size_of::<AtomicU32>() + self.walked[shard].capacity() * size_of::<AtomicBool>();
        }
        ByteCount(total as u64)
    }

    /// Install a sealed segment, or return false when its shard has no room
    pub fn install(&self, file: File, root: RootId, head: &TailHead, columns: &[ColumnTail], blocks: &[BlockEntry]) -> bool {
        let mut guard = self.shards.write(shard_of(head.segment));
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
            root,
            head: *head,
            columns: column_extent,
            blocks: block_extent,
            is_walked: false,
            highest_replacement: WritePlace(0),
            has_held_frees: false,
            is_unproven: false,
        });
        self.walked[shard_of(head.segment)][slot as usize].store(false, Ordering::Release);
        guard.map.insert(head.segment, slot);
        true
    }

    /// Read how many segments the table has room for
    pub fn capacity(&self) -> usize {
        TABLE_SHARDS * self.shards.read(0).segments.len()
    }

    /// Check the fullest shard has used more than half of its segment slots, its column pool or its block pool
    pub fn is_past_half(&self) -> bool {
        (0..TABLE_SHARDS).any(|shard| {
            let shard = self.shards.read(shard);
            let used = shard.segments.len() - shard.free_slots.len();
            2 * used > shard.segments.len() || 2 * shard.columns.len() > shard.columns.capacity() || 2 * shard.blocks.len() > shard.blocks.capacity()
        })
    }

    /// Count the segments in the table
    pub fn segment_count(&self) -> u64 {
        let mut count = 0;
        for shard in 0..TABLE_SHARDS {
            count += self.shards.read(shard).segments.iter().flatten().count() as u64;
        }
        count
    }

    /// Check the table has a segment
    pub fn holds(&self, number: SegmentNumber) -> bool {
        self.shards.read(shard_of(number)).map.get(number).is_some()
    }

    /// Hold a segment open for a read, or return nothing when it has left the table
    pub fn pin(&self, number: SegmentNumber) -> Option<SegmentPin<'_, File>> {
        let shard = shard_of(number);
        let guard = self.shards.read(shard);
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

    /// Take a block's dead rows off its live count, and keep the highest place that replaced any of the segment's rows
    pub fn drop_live(&self, number: SegmentNumber, block: u32, rows: u32, replacement: WritePlace) {
        let mut guard = self.shards.write(shard_of(number));
        let Some(slot) = guard.map.get(number) else {
            return;
        };
        let Some(entry) = guard.segments[slot as usize].as_mut() else {
            return;
        };
        entry.highest_replacement = entry.highest_replacement.max(replacement);
        let start = entry.blocks.start;
        if let Some(line) = guard.blocks.get_mut(start as usize + block as usize) {
            line.live_rows = line.live_rows.saturating_sub(rows);
        }
    }

    /// Set or clear a segment's flag for large dead records no list could hold
    pub fn flag_held_frees(&self, number: SegmentNumber, is_held: bool) {
        let mut guard = self.shards.write(shard_of(number));
        let Some(slot) = guard.map.get(number) else {
            return;
        };
        if let Some(entry) = guard.segments[slot as usize].as_mut() {
            entry.has_held_frees = is_held;
        }
    }

    /// Note a confirmation couldn't prove a segment's rows dead, so only a rewrite takes it until the next open
    pub fn flag_unproven(&self, number: SegmentNumber) {
        let mut guard = self.shards.write(shard_of(number));
        let Some(slot) = guard.map.get(number) else {
            return;
        };
        if let Some(entry) = guard.segments[slot as usize].as_mut() {
            entry.is_unproven = true;
        }
    }

    /// Check a confirmation left a segment unproven
    pub fn is_unproven(&self, number: SegmentNumber) -> bool {
        let guard = self.shards.read(shard_of(number));
        let entry = guard.map.get(number).and_then(|slot| guard.segments[slot as usize].as_ref());
        entry.is_some_and(|entry| entry.is_unproven)
    }

    /// Find a segment with large dead records no list could hold
    pub fn next_held_frees(&self) -> Option<SegmentNumber> {
        (0..TABLE_SHARDS).find_map(|shard| {
            let guard = self.shards.read(shard);
            guard.segments.iter().flatten().find(|entry| entry.has_held_frees).map(|entry| entry.number)
        })
    }

    /// Find which of a segment's blocks starts at an offset in its file
    pub fn block_at(&self, number: SegmentNumber, offset: u32) -> Option<u32> {
        let guard = self.shards.read(shard_of(number));
        let slot = guard.map.get(number)?;
        let entry = guard.segments[slot as usize].as_ref()?;
        let first = entry.blocks.start as usize;
        let lines = guard.blocks.get(first..first + entry.blocks.length as usize)?;
        let index = lines.partition_point(|line| line.block_offset < offset);
        lines.get(index).filter(|line| line.block_offset == offset).map(|_line| index as u32)
    }

    /// Read the highest place any charge on a segment's rows named, which bounds the replacement of each of them
    pub fn highest_replacement(&self, number: SegmentNumber) -> Option<WritePlace> {
        let guard = self.shards.read(shard_of(number));
        let slot = guard.map.get(number)?;
        Some(guard.segments[slot as usize].as_ref()?.highest_replacement)
    }

    /// Read the highest place that replaced a segment's rows, or nothing while a block still has a live row. The place lives
    /// here alone, and it starts at zero after an open, where no cue can read below what was already written
    pub fn dead_replacement(&self, number: SegmentNumber) -> Option<WritePlace> {
        let guard = self.shards.read(shard_of(number));
        let slot = guard.map.get(number)?;
        let entry = guard.segments[slot as usize].as_ref()?;
        let first = entry.blocks.start as usize;
        let lines = guard.blocks.get(first..first + entry.blocks.length as usize)?;
        lines.iter().all(|line| line.live_rows == 0).then_some(entry.highest_replacement)
    }

    /// Add up a segment's live rows, block by block
    pub fn live_rows(&self, number: SegmentNumber) -> Option<u64> {
        let guard = self.shards.read(shard_of(number));
        let slot = guard.map.get(number)?;
        let entry = guard.segments[slot as usize].as_ref()?;
        let first = entry.blocks.start as usize;
        let lines = guard.blocks.get(first..first + entry.blocks.length as usize)?;
        Some(lines.iter().map(|line| u64::from(line.live_rows)).sum())
    }

    /// Note a segment's walk has finished, so other walks may count against it
    pub fn mark_walked(&self, number: SegmentNumber) {
        let mut guard = self.shards.write(shard_of(number));
        let Some(slot) = guard.map.get(number) else {
            return;
        };
        if let Some(entry) = guard.segments[slot as usize].as_mut() {
            entry.is_walked = true;
            self.walked[shard_of(number)][slot as usize].store(true, Ordering::Release);
        }
    }

    /// Copy out one of a segment's block lines, counted over all its columns, and the column it belongs to
    pub fn walk_block(&self, number: SegmentNumber, index: u32) -> Option<WalkBlock> {
        let guard = self.shards.read(shard_of(number));
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
    pub fn retire(&self, number: SegmentNumber) -> Option<RetiredSegment<File>> {
        let shard = shard_of(number);
        let mut guard = self.shards.write(shard);
        let slot = guard.map.get(number)?;
        let entry = guard.segments[slot as usize].take()?;
        guard.map.remove(number);
        Some(RetiredSegment {
            file: entry.file,
            shard,
            slot,
        })
    }

    /// Count the reads still holding a retired segment, which keeps its slot until `free_retired`, so the count is its own
    pub fn retired_pins(&self, retired: &RetiredSegment<File>) -> u32 {
        self.pins[retired.shard][retired.slot as usize].load(Ordering::Acquire)
    }

    /// Give a retired segment's slot back, once no read holds it
    pub fn free_retired(&self, retired: &RetiredSegment<File>) {
        self.shards.write(retired.shard).free_slots.push(retired.slot);
    }

    /// Read where a column's blocks start in a segment and how many there are
    pub fn column_blocks(&self, number: SegmentNumber, column: ColumnId) -> Option<(u32, u32)> {
        let guard = self.shards.read(shard_of(number));
        let slot = guard.map.get(number)?;
        let entry = guard.segments[slot as usize].as_ref()?;
        let tail = guard.column_of(entry, column)?;
        Some((tail.first_block, tail.block_count.0))
    }

    /// Find the block a key's rows can start in and the end of its column's blocks, in the segment's own block numbers
    pub fn blocks_from(&self, number: SegmentNumber, column: ColumnId, key: &[u8]) -> Option<(u32, u32)> {
        let guard = self.shards.read(shard_of(number));
        let slot = guard.map.get(number)?;
        let entry = guard.segments[slot as usize].as_ref()?;
        let column_tail = guard.column_of(entry, column)?;
        let first = entry.blocks.start as usize + column_tail.first_block as usize;
        let lines = guard.blocks.get(first..first + column_tail.block_count.0 as usize)?;
        let prefix = KeyPrefix::of_key(key, column_tail.shared_length as usize);
        let start = column_tail.first_block + first_candidate(lines, prefix) as u32;
        Some((start, column_tail.first_block + column_tail.block_count.0))
    }

    /// Add up each root's segments, as a caller measures them, into one number a root
    pub fn for_each_segment(&self, mut visit: impl FnMut(SegmentNumber, RootId, &File)) {
        for shard in 0..self.shards.len() {
            let guard = self.shards.read(shard);
            for entry in guard.segments.iter().flatten() {
                visit(entry.number, entry.root, &entry.file);
            }
        }
    }

    /// Read which root holds a segment
    pub fn root_of(&self, number: SegmentNumber) -> Option<RootId> {
        let guard = self.shards.read(shard_of(number));
        let slot = guard.map.get(number)?;
        guard.segments[slot as usize].as_ref().map(|entry| entry.root)
    }

    /// Copy out a segment's tail for a column
    pub fn column_tail(&self, number: SegmentNumber, column: ColumnId) -> Option<ColumnTail> {
        let guard = self.shards.read(shard_of(number));
        let slot = guard.map.get(number)?;
        let entry = guard.segments[slot as usize].as_ref()?;
        guard.column_of(entry, column).copied()
    }

    /// Copy out one block line, and the highest place that replaced any of the segment's rows
    pub fn block_line(&self, number: SegmentNumber, index: u32) -> Option<(BlockEntry, WritePlace)> {
        let guard = self.shards.read(shard_of(number));
        let slot = guard.map.get(number)?;
        let entry = guard.segments[slot as usize].as_ref()?;
        if index >= entry.blocks.length {
            return None;
        }
        let line = guard.blocks.get(entry.blocks.start as usize + index as usize)?;
        Some((*line, entry.highest_replacement))
    }

    /// Find the block a walk enters a column at, for a key known to start with the bytes the column shares. Going up that is
    /// the block its rows can start in, and going down the last block that starts at or before it
    pub fn entry_block(&self, number: SegmentNumber, tail: &ColumnTail, key: &[u8], direction: Direction) -> Option<u32> {
        let guard = self.shards.read(shard_of(number));
        let slot = guard.map.get(number)?;
        let entry = guard.segments[slot as usize].as_ref()?;
        let first = entry.blocks.start as usize + tail.first_block as usize;
        let lines = guard.blocks.get(first..first + tail.block_count.0 as usize)?;
        let prefix = KeyPrefix::of_key(key, tail.shared_length as usize);
        let at = match direction {
            Direction::Ascending => first_candidate(lines, prefix),
            Direction::Descending => lines.partition_point(|line| line.first_key <= prefix).checked_sub(1)?,
        };
        Some(tail.first_block + at as u32)
    }

    /// Copy out the block lines that can hold a key, from a block onward
    pub fn block_window(&self, number: SegmentNumber, column: ColumnId, key: &[u8], skip: usize) -> Option<BlockWindow> {
        let guard = self.shards.read(shard_of(number));
        let slot = guard.map.get(number)?;
        let entry = guard.segments[slot as usize].as_ref()?;
        guard.window_of(entry, column, key, skip)
    }

    /// Pin a segment and find its first window of blocks that could hold a key, under one take of the shard's lock. The
    /// window is nothing when the segment has no rows of the column
    pub fn pin_window(&self, number: SegmentNumber, column: ColumnId, key: &[u8]) -> Option<(SegmentPin<'_, File>, Option<BlockWindow>)>
    where
        File: Clone,
    {
        let shard = shard_of(number);
        let guard = self.shards.read(shard);
        let slot = guard.map.get(number)?;
        let entry = guard.segments[slot as usize].as_ref()?;
        let window = guard.window_of(entry, column, key, 0);
        let pin = &self.pins[shard][slot as usize];
        pin.fetch_add(1, Ordering::AcqRel);
        let segment_pin = SegmentPin { file: entry.file.clone(), head: entry.head, is_walked: entry.is_walked, pin };
        Some((segment_pin, window))
    }
}

impl<File> TableShard<File> {
    /// Find a window of a segment's blocks that could hold a key, past the first `skip` of them
    pub fn window_of(&self, entry: &TableEntry<File>, column: ColumnId, key: &[u8], skip: usize) -> Option<BlockWindow> {
        let column_tail = self.column_of(entry, column)?;
        let first = entry.blocks.start as usize + column_tail.first_block as usize;
        let lines = self.blocks.get(first..first + column_tail.block_count.0 as usize)?;

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
                None => filter_end(entry, column_tail, &self.blocks),
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

/// What a cursor learned of a segment under one take of its shard's lock
#[derive(Debug)]
pub enum RunCopy<File> {
    /// The segment left the table
    Gone,
    /// Its file and a run of the column's block lines from the first that can hold a key
    Run(HeldRun<File>),
}

/// A segment's file and a run of one column's block lines, which a cursor keeps pinned until it lets go
#[derive(Clone, Debug)]
pub struct HeldRun<File> {
    pub file: File,
    pub place_base: WritePlace,
    pub shard: usize,
    pub slot: u32,
    pub column: Option<ColumnTail>,
    pub low: KeyPrefix,
    pub count: usize,
    pub reaches_end: bool,
    pub end_filter: u64,
    /// The segment's walk had finished when the cursor last looked
    pub is_walked: bool,
    /// A prefix more lines share than a run holds, which goes to the table with no copy
    pub crowded: Option<KeyPrefix>,
}

impl<File: Clone> SegmentTable<File> {
    /// Copy a column's block lines into a run from the first that can hold a key, and pin the segment when asked, walked or not
    pub fn copy_run(&self, number: SegmentNumber, column: ColumnId, key: &[u8], run: &mut [BlockEntry], is_pinning: bool) -> RunCopy<File> {
        let shard = shard_of(number);
        let guard = self.shards.read(shard);
        let Some(slot) = guard.map.get(number) else {
            return RunCopy::Gone;
        };
        let Some(entry) = guard.segments[slot as usize].as_ref() else {
            return RunCopy::Gone;
        };
        if is_pinning {
            self.pins[shard][slot as usize].fetch_add(1, Ordering::AcqRel);
        }
        let mut held = HeldRun {
            file: entry.file.clone(),
            place_base: entry.head.place_base,
            shard,
            slot,
            column: None,
            low: KeyPrefix::default(),
            count: 0,
            reaches_end: true,
            end_filter: 0,
            is_walked: entry.is_walked,
            crowded: None,
        };
        let Some(tail) = guard.column_of(entry, column) else {
            return RunCopy::Run(held);
        };
        let first = entry.blocks.start as usize + tail.first_block as usize;
        let Some(lines) = guard.blocks.get(first..first + tail.block_count.0 as usize) else {
            return RunCopy::Run(held);
        };
        let low = KeyPrefix::of_key(key, tail.shared_length as usize);
        let start = first_candidate(lines, low);
        let count = lines.len().saturating_sub(start).min(run.len());
        run[..count].copy_from_slice(&lines[start..start + count]);
        held.column = Some(*tail);
        held.low = low;
        held.count = count;
        held.reaches_end = start + count >= lines.len();
        held.end_filter = filter_end(entry, tail, &guard.blocks);
        RunCopy::Run(held)
    }

    /// Check a held run's segment has finished its walk since the cursor last looked, with no lock
    pub fn is_walked_now(&self, held: &HeldRun<File>) -> bool {
        self.walked[held.shard][held.slot as usize].load(Ordering::Acquire)
    }

    /// Let go of the pin a cursor took on a run's segment
    pub fn release_run(&self, held: &HeldRun<File>) {
        self.pins[held.shard][held.slot as usize].fetch_sub(1, Ordering::AcqRel);
    }
}

/// Find a key's window of blocks in a copied run, as `window_of` would in the whole column, or nothing when the run can't
/// say: the key sits below the run's first key, or past its last line with more lines beyond
pub fn window_in_run(held: &HeldRun<impl Sized>, run: &[BlockEntry], key: &[u8]) -> Option<Option<BlockWindow>> {
    let Some(tail) = held.column else {
        return Some(None);
    };
    let prefix = KeyPrefix::of_key(key, tail.shared_length as usize);
    let lines = &run[..held.count];
    if prefix < held.low || (!held.reaches_end && lines.last().is_none_or(|last| last.first_key <= prefix)) {
        return None;
    }
    let start = first_candidate(lines, prefix);
    let mut window = BlockWindow {
        column: tail,
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
        window.filter_end_of_last = lines.get(index + 1).map_or(held.end_filter, |next| next.header_at().0);
    }
    Some(Some(window))
}
