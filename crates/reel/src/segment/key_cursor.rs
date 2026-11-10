//! Walked segments a walk block or merge step asks again and again, pinned once and with a run of block lines each, so a
//! row's lookup takes no shard lock

use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, WritePlace};

use crate::segment::block_entry::{BlockEntry, KeyPrefix};
use crate::segment::segment_table::{BlockWindow, HeldRun, RunCopy, SegmentTable, WINDOW_BLOCKS, window_in_run};
use crate::segment::segment_tail::SegmentNumber;

/// Segments one cursor holds
pub const CURSOR_SEGMENTS: usize = 512;
/// Block lines a cursor copies for each segment, past a window's worth so a run ends on a line past the key
pub const RUN_LINES: usize = WINDOW_BLOCKS + 2;
/// Slots in the map from a segment and column to its run, twice the segments
const MAP_SLOTS: usize = 2 * CURSOR_SEGMENTS;

/// What a cursor gives for a segment
pub enum CursorAnswer<'cursor, File> {
    /// Nothing to ask: the segment left, or its walk hasn't finished
    Skip,
    /// The cursor is full or its run can't place the key, so the row asks the table as it would without one
    Table,
    /// The segment's file, and the window of blocks that can hold the key, or nothing when it has no rows of the column
    Window {
        file: &'cursor File,
        place_base: WritePlace,
        window: Option<BlockWindow>,
    },
}

/// What one cursor did, for the report
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CursorCounts {
    pub held: u64,
    pub refills: u64,
    pub answered: u64,
    pub to_table: u64,
}

/// The cursor: runs in fixed room, a map to them, and a stamp that empties the map at each release
#[derive(Debug)]
pub struct KeyCursor<File> {
    held: Vec<HeldRun<File>>,
    lines: Vec<BlockEntry>,
    map: Vec<(SegmentNumber, ColumnId, u32, u16)>,
    stamp: u32,
    pub counts: CursorCounts,
}

impl<File: Clone> KeyCursor<File> {
    /// Allocate the fixed room
    pub fn new() -> Self {
        Self {
            held: Vec::with_capacity(CURSOR_SEGMENTS),
            lines: vec![BlockEntry::default(); CURSOR_SEGMENTS * RUN_LINES],
            map: vec![(SegmentNumber(0), ColumnId(0), 0, 0); MAP_SLOTS],
            stamp: 1,
            counts: CursorCounts::default(),
        }
    }

    /// Read the heap the room holds
    pub fn heap_bytes(&self) -> ByteCount {
        let held = self.held.capacity() * size_of::<HeldRun<File>>();
        let lines = self.lines.capacity() * size_of::<BlockEntry>();
        ByteCount((held + lines + self.map.capacity() * size_of::<(SegmentNumber, ColumnId, u32, u16)>()) as u64)
    }

    /// Find a segment's run, or the free slot it would take
    fn slot_of(&self, segment: SegmentNumber, column: ColumnId) -> (usize, Option<usize>) {
        let mut slot = (segment.0.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as usize ^ column.0 as usize;
        loop {
            slot &= MAP_SLOTS - 1;
            let (number, at_column, stamp, index) = self.map[slot];
            if stamp != self.stamp {
                return (slot, None);
            }
            if number == segment && at_column == column {
                return (slot, Some(index as usize));
            }
            slot += 1;
        }
    }

    /// Give a key's window in a walked segment, pinning the segment, walked or not, the first time it's asked. Keys come in order within a
    /// column, and a key past a run copies the run again from that key
    pub fn window(&mut self, table: &SegmentTable<File>, segment: SegmentNumber, column: ColumnId, key: &[u8]) -> CursorAnswer<'_, File> {
        let (slot, found) = self.slot_of(segment, column);
        let index = match found {
            Some(index) => {
                // a walk only ever finishes, and the slot stays the segment's while the cursor holds its pin
                if !self.held[index].is_walked {
                    if !table.is_walked_now(&self.held[index]) {
                        return CursorAnswer::Skip;
                    }
                    self.held[index].is_walked = true;
                }
                let held = &self.held[index];
                if held.crowded.is_some_and(|crowded| held.column.is_some_and(|tail| KeyPrefix::of_key(key, tail.shared_length as usize) == crowded)) {
                    self.counts.to_table += 1;
                    return CursorAnswer::Table;
                }
                let run = &self.lines[index * RUN_LINES..(index + 1) * RUN_LINES];
                if window_in_run(held, run, key).is_none() {
                    self.counts.refills += 1;
                    let run = &mut self.lines[index * RUN_LINES..(index + 1) * RUN_LINES];
                    match table.copy_run(segment, column, key, run, false) {
                        RunCopy::Run(fresh) => {
                            let held = &mut self.held[index];
                            (held.column, held.low, held.count, held.reaches_end, held.end_filter) = (fresh.column, fresh.low, fresh.count, fresh.reaches_end, fresh.end_filter);
                        },
                        RunCopy::Gone => return CursorAnswer::Skip,
                    }
                }
                index
            },
            None => {
                if self.held.len() == CURSOR_SEGMENTS {
                    self.counts.to_table += 1;
                    return CursorAnswer::Table;
                }
                let index = self.held.len();
                let run = &mut self.lines[index * RUN_LINES..(index + 1) * RUN_LINES];
                match table.copy_run(segment, column, key, run, true) {
                    RunCopy::Run(held) => {
                        let is_walked = held.is_walked;
                        self.held.push(held);
                        self.map[slot] = (segment, column, self.stamp, index as u16);
                        self.counts.held += 1;
                        if !is_walked {
                            return CursorAnswer::Skip;
                        }
                    },
                    RunCopy::Gone => return CursorAnswer::Skip,
                }
                index
            },
        };
        let run = &self.lines[index * RUN_LINES..(index + 1) * RUN_LINES];
        let Some(window) = window_in_run(&self.held[index], run, key) else {
            // many lines share the key's prefix, past what a run holds, so its later keys skip the copy
            let held = &mut self.held[index];
            held.crowded = held.column.map(|tail| KeyPrefix::of_key(key, tail.shared_length as usize));
            self.counts.to_table += 1;
            return CursorAnswer::Table;
        };
        self.counts.answered += 1;
        let held = &self.held[index];
        CursorAnswer::Window { file: &held.file, place_base: held.place_base, window }
    }

    /// Let go of every pin, at the end of a walk block or merge step
    pub fn release(&mut self, table: &SegmentTable<File>) {
        for held in &self.held {
            table.release_run(held);
        }
        self.held.clear();
        self.stamp = self.stamp.wrapping_add(1).max(1);
        if self.stamp == 1 {
            self.map.fill((SegmentNumber(0), ColumnId(0), 0, 0));
        }
    }
}

impl<File: Clone> Default for KeyCursor<File> {
    fn default() -> Self {
        Self::new()
    }
}
