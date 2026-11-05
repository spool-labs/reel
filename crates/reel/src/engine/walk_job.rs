//! The dead-byte walk, a planner unit: after a seal, count the versions each segment's rows supersede, a row block at a time
//!
//! A walk looks only at segments whose own walk has finished, so after each walk exactly the newest row of a key
//! among them is live and every other row is counted once. A segment not yet walked counts nothing until its turn,
//! which only understates. Nothing is written per row: the newest row in write order decides every read.
//!
//! Range delete rows take no part on either side, and the range-delete apply job and compaction see to rows a cover
//! hides, so neither counts here.

use std::io::{Error, ErrorKind, Result};
use std::mem;
use std::sync::atomic::AtomicU8;
use std::sync::{Arc, Mutex, OnceLock};
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, Nanoseconds, WritePlace};

use crate::config::MAX_KEY_BYTES;
use crate::engine::engine_state::EngineState;
use crate::engine::planner::UnitStep;
use crate::engine::volume::Volume;
use crate::guard::hold;
use crate::io::device_error::is_device_error;
use crate::io::job::{Job, JobStep};
use crate::io::{Io, IoFile, IoTask};
use crate::pool::{touched_capacity, touched_zeroes};
use crate::row_kind::RowKind;
use crate::segment::footer_writer::MOST_BLOCK_ROWS;
use crate::segment::row_block::{FooterRow, ROW_BLOCK_BYTES, RowBlockWalk};
use crate::segment::segment_table::{SegmentPin, WalkBlock};
use crate::segment::segment_tail::SegmentNumber;
use crate::segment::summary::DeadCount;

/// How long a walk waits after a failed read before it tries the block again
pub const WALK_RETRY: Nanoseconds = Nanoseconds(1_000_000_000);
/// A dead record at least this long gives its whole blocks back as soon as a walk counts it
pub const FREE_IN_PLACE_BYTES: ByteCount = ByteCount(64 * 1024);
/// Freed ranges start and end on a block of this many bytes, the widest block a filesystem reel runs on uses
pub const FREE_BLOCK_BYTES: u64 = 4 * 1024;
/// Most large dead records one block's walk notes, and past that a record waits for a rewrite
pub const MOST_BLOCK_FREES: usize = 64;
/// How far back a count looks for an entry to add to
pub const COUNT_SEARCH: usize = 32;

/// A worker's slot has no block
pub const SLOT_FREE: u8 = 0;
/// A worker's slot has a block to count
pub const SLOT_ASSIGNED: u8 = 1;
/// A worker's slot holds a block's counts, waiting for the planner to land them in block order
pub const SLOT_COUNTED: u8 = 2;
/// A worker's slot holds a block it couldn't count
pub const SLOT_FAILED: u8 = 3;

/// The segment a walk is on
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WalkTarget {
    pub segment: SegmentNumber,
    pub next_block: u32,
    pub block_count: u32,
    pub is_backward: bool,
    pub is_damaged: bool,
}

/// A dead record large enough to give its blocks back
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FreeRecord {
    pub segment: SegmentNumber,
    pub offset: FileOffset,
    pub length: ByteCount,
    pub place: WritePlace,
    pub replacement: WritePlace,
}

/// What counting one block found
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BlockCounts {
    pub counts: Vec<DeadCount>,
    pub frees: Vec<FreeRecord>,
}

impl BlockCounts {
    /// Allocate room for any block
    pub fn new() -> Self {
        Self {
            counts: touched_capacity(MOST_BLOCK_ROWS, DeadCount::default()),
            frees: touched_capacity(MOST_BLOCK_FREES, FreeRecord::default()),
        }
    }

    /// Read the heap they hold
    pub fn heap_bytes(&self) -> ByteCount {
        let counts = self.counts.capacity() * size_of::<DeadCount>();
        ByteCount((counts + self.frees.capacity() * size_of::<FreeRecord>()) as u64)
    }

    /// Empty the lists
    pub fn clear(&mut self) {
        self.counts.clear();
        self.frees.clear();
    }

    /// Count a row dead, and note a large record so its blocks can go
    pub fn count_dead(&mut self, segment: SegmentNumber, column: ColumnId, block: u32, row: &FooterRow, replacement: WritePlace) {
        add_count(&mut self.counts, segment, column, block, row, Some(replacement));
        if row.kind == RowKind::Value && row.length >= FREE_IN_PLACE_BYTES && self.frees.len() < self.frees.capacity() {
            self.frees.push(FreeRecord {
                segment,
                offset: row.offset,
                length: row.length,
                place: row.place,
                replacement,
            });
        }
    }
}

/// One block's counts on their way from a worker to the planner
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SlotWork {
    pub target: WalkTarget,
    pub next_block: u32,
    pub counts: BlockCounts,
}

/// A worker's place to take a block and hand its counts back. Workers only read, and the planner alone lands counts, so
/// a volume still has one walk: it keeps one target, one position and one order of landing
#[derive(Debug)]
pub struct WalkSlot<Task> {
    pub phase: AtomicU8,
    pub work: Mutex<SlotWork>,
    pub task: OnceLock<Task>,
}

impl<Task> WalkSlot<Task> {
    /// Build an empty slot with room for one block's counts
    pub fn new() -> Self {
        Self {
            phase: AtomicU8::new(SLOT_FREE),
            work: Mutex::new(SlotWork {
                counts: BlockCounts::new(),
                ..SlotWork::default()
            }),
            task: OnceLock::new(),
        }
    }

    /// Read the heap the slot's counts hold
    pub fn heap_bytes(&self) -> ByteCount {
        hold(&self.work).counts.heap_bytes()
    }
}

impl<Task> Default for WalkSlot<Task> {
    fn default() -> Self {
        Self::new()
    }
}

/// What it takes to count one row block: nothing here changes the store
pub struct BlockCounter<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
    block: Vec<u8>,
    key: Vec<u8>,
    last_key: Vec<u8>,
    pub last_column: Option<ColumnId>,
    is_next_key_unknown: bool,
}

/// The walk of one volume's sealed segments
pub struct WalkJob<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
    pub volume: usize,
    pub target: Option<WalkTarget>,
    pub retry_at: Option<Nanoseconds>,
    counter: BlockCounter<Backend>,
    counts: BlockCounts,
    waiting_frees: Vec<FreeRecord>,
    next_handed_out: u32,
    failed_slot: Option<usize>,
}

/// A worker that counts the blocks the planner hands it
pub struct WalkWorker<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
    pub volume: usize,
    pub slot: usize,
    pub counter: BlockCounter<Backend>,
}

impl<Backend: Io> BlockCounter<Backend> {
    /// Allocate the fixed buffers
    pub fn new(state: Arc<EngineState<Backend>>) -> Self {
        Self {
            state,
            block: touched_zeroes(ROW_BLOCK_BYTES),
            key: touched_capacity(MAX_KEY_BYTES as usize, 0),
            last_key: touched_capacity(MAX_KEY_BYTES as usize, 0),
            last_column: None,
            is_next_key_unknown: false,
        }
    }

    /// Read the heap the buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        ByteCount((self.block.capacity() + self.key.capacity() + self.last_key.capacity()) as u64)
    }

    /// Count a target's next block into a list, and return the position past it
    pub fn count(&mut self, volume: &Volume<Backend>, target: WalkTarget, counts: &mut BlockCounts) -> Result<u32> {
        counts.clear();

        // a damaged segment answers for nothing, so its rows count nothing and the walk moves past it
        let pin = volume.table.pin(target.segment).filter(|_pin| !target.is_damaged);
        let block = volume.table.walk_block(target.segment, target.next_block);
        let (Some(pin), Some(block)) = (pin, block) else {
            return Ok(target.block_count);
        };
        if self.last_column.is_none() && target.next_block > 0 {
            self.load_last_key(volume, &pin, target, block.column)?;
        }
        match self.count_block(volume, &pin, &block, target, counts) {
            Err(error) if is_device_error(&error) => counts.clear(),
            outcome => outcome?,
        }
        Ok(target.next_block + 1)
    }

    /// Find the last key before this block, since a key's versions can run across blocks, past blocks of range deletes
    /// alone. A block that can't be read leaves the next key unknown, and it flips nothing
    pub fn load_last_key(
        &mut self,
        volume: &Volume<Backend>,
        pin: &SegmentPin<'_, Backend::File>,
        target: WalkTarget,
        column: ColumnId,
    ) -> Result<()> {
        let mut index = target.next_block;
        while index > 0 {
            if self.state.is_stopping.load(Ordering::Acquire) {
                return Err(Error::from(ErrorKind::Interrupted));
            }
            index -= 1;
            let Some(block) = volume.table.walk_block(target.segment, index) else {
                return Ok(());
            };
            if block.column != column {
                return Ok(());
            }
            let length = block.entry.block_length().0 as usize;
            let read = pin.file.read_exact_at(block.entry.block_at(), &mut self.block[..length]);
            if let Err(error) = read {
                if !is_device_error(&error) {
                    return Err(error);
                }
                self.is_next_key_unknown = true;
                return Ok(());
            }
            let Some(mut walk) = RowBlockWalk::open(&self.block[..length], pin.head.place_base) else {
                self.is_next_key_unknown = true;
                return Ok(());
            };
            while let Some(row) = walk.next_row(&mut self.key) {
                if row.kind != RowKind::RangeDelete {
                    self.last_key.clear();
                    self.last_key.extend_from_slice(&self.key);
                    self.last_column = Some(block.column);
                }
            }
            if self.last_column.is_some() {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Count one block's rows
    pub fn count_block(
        &mut self,
        volume: &Volume<Backend>,
        pin: &SegmentPin<'_, Backend::File>,
        block: &WalkBlock,
        target: WalkTarget,
        counts: &mut BlockCounts,
    ) -> Result<()> {
        let length = block.entry.block_length().0 as usize;
        pin.file.read_exact_at(block.entry.block_at(), &mut self.block[..length])?;
        let Some(mut walk) = RowBlockWalk::open(&self.block[..length], pin.head.place_base) else {
            return Ok(());
        };
        // a key's rows run newest first, so the first one the walk meets is its top, and carrying the last key across
        // a block is enough
        let mut last_place = WritePlace(u64::MAX);

        while let Some(row) = walk.next_row(&mut self.key) {
            debug_assert!(self.last_column != Some(block.column) || self.last_key != self.key || row.place < last_place);
            last_place = row.place;
            if row.kind == RowKind::RangeDelete {
                continue;
            }
            if target.is_backward {
                add_count(&mut counts.counts, target.segment, block.column, target.next_block, &row, None);
            }
            let is_top = self.last_column != Some(block.column) || self.last_key != self.key;
            // a key that may have begun in a block no read can give has a top no walk can name
            if is_top && self.is_next_key_unknown {
                self.is_next_key_unknown = false;
                self.last_key.clear();
                self.last_key.extend_from_slice(&self.key);
                self.last_column = Some(block.column);
                continue;
            }
            if !is_top {
                // the key's top is in this segment, and its highest place is at or above that
                let replacement = pin.head.places.highest;
                counts.count_dead(target.segment, block.column, target.next_block, &row, replacement);
                continue;
            }

            self.last_key.clear();
            self.last_key.extend_from_slice(&self.key);
            self.last_column = Some(block.column);
            let found = self.state.newest_walked(volume, block.column, &self.key, target.segment);
            let newest = match found {
                Ok(newest) => newest,
                // a version a damaged block hides counts nothing, which only understates
                Err(StoreError::Corruption { .. }) => None,
                // the kind alone, since a stop comes through here during close and close allocates nothing
                Err(_error) => return Err(Error::from(ErrorKind::Interrupted)),
            };
            match newest {
                Some(found) if found.row.place > row.place => {
                    let replacement = found.row.place;
                    counts.count_dead(target.segment, block.column, target.next_block, &row, replacement);
                },
                Some(found) if !target.is_backward => {
                    if let Some(index) = volume.table.block_at(found.segment, found.block_offset) {
                        counts.count_dead(found.segment, block.column, index, &found.row, row.place);
                    }
                },
                Some(_) | None => {},
            }
        }
        Ok(())
    }
}

impl<Backend: Io> WalkJob<Backend> {
    /// Allocate the walk's fixed buffers
    pub fn new(state: Arc<EngineState<Backend>>, volume: usize) -> Self {
        Self {
            counter: BlockCounter::new(Arc::clone(&state)),
            state,
            volume,
            target: None,
            retry_at: None,
            counts: BlockCounts::new(),
            waiting_frees: touched_capacity(MOST_BLOCK_FREES, FreeRecord::default()),
            next_handed_out: 0,
            failed_slot: None,
        }
    }

    /// Read the heap the walk's buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        let waiting = self.waiting_frees.capacity() * size_of::<FreeRecord>();
        ByteCount(self.counter.heap_bytes().0 + self.counts.heap_bytes().0 + waiting as u64)
    }

    /// Pick a segment whose walk hasn't finished. Backward walks go first and newest first, so an older segment finds a
    /// newer one finished and counts against it, and then forward walks go from the lowest
    pub fn pick_target(&mut self) -> bool {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        // a seal only ends past the mark, so every row a walk counts by is published, and its slot has let go of the file
        let sealing = volume.sealing.load(Ordering::Acquire);
        let rows = hold(&volume.summary);
        let waiting = rows.rows().filter(|row| !row.facts.is_walked() && row.facts.segment.0 != sealing);
        let backward = waiting.clone().filter(|row| row.facts.is_backward).max_by_key(|row| row.facts.places.highest);
        let picked = backward.or_else(|| waiting.min_by_key(|row| row.facts.segment));
        self.target = picked.map(|row| WalkTarget {
            segment: row.facts.segment,
            next_block: row.facts.walked_blocks,
            block_count: row.facts.block_count,
            is_backward: row.facts.is_backward,
            is_damaged: row.facts.is_damaged,
        });
        self.counter.last_column = None;
        self.next_handed_out = self.target.map_or(0, |target| target.next_block);
        self.target.is_some()
    }

    /// Walk one row block here. A failure leaves the last key wherever the attempt stood, so the retry finds it again
    pub fn walk_block(&mut self, target: WalkTarget) -> Result<()> {
        let outcome = self.count_and_land(target);
        if outcome.is_err() {
            self.counter.last_column = None;
            self.note_retry();
        }
        outcome
    }

    /// Count a failed attempt, unless a stop is what failed it
    pub fn note_retry(&self) {
        if !self.state.is_stopping.load(Ordering::Acquire) {
            self.state.volumes[self.volume].walk_retries.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Count one row block, and land its counts and the new position under one hold
    pub fn count_and_land(&mut self, target: WalkTarget) -> Result<()> {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        let mut counts = mem::take(&mut self.counts);
        let outcome = self.counter.count(volume, target, &mut counts);
        let landed = outcome.and_then(|next_block| self.land(volume, target, next_block, &mut counts));
        self.counts = counts;
        landed
    }

    /// Land one block's counts and the new position under one hold. A count for a segment that left since goes nowhere, since
    /// a segment number is never used twice
    pub fn land(
        &mut self,
        volume: &Volume<Backend>,
        target: WalkTarget,
        next_block: u32,
        counts: &mut BlockCounts,
    ) -> Result<()> {
        counts.counts.sort_unstable_by_key(|count| count.segment);
        let mut rows = hold(&volume.summary);
        rows.apply_walk(target.segment, next_block, &counts.counts);
        if !target.is_backward {
            rows.note_forward(target.segment);
        }
        drop(rows);
        for count in counts.counts.iter().filter(|count| count.rows > 0) {
            volume.table.drop_live(count.segment, count.block, count.rows, count.replacement);
        }
        for free in &counts.frees {
            if !self.free_record(volume, free) && self.waiting_frees.len() < self.waiting_frees.capacity() {
                self.waiting_frees.push(*free);
            }
        }
        if next_block < target.block_count {
            self.target = Some(WalkTarget { next_block, ..target });
            return Ok(());
        }

        volume.table.mark_walked(target.segment);
        volume.wants_compaction.store(true, Ordering::Release);
        // a row dropped for room after its seal counted can leave this at zero
        let _waiting = volume.walks_waiting.fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| count.checked_sub(1));
        self.target = None;
        self.state.save_summary(volume)
    }

    /// Give a dead record's whole blocks back, or say it has to wait for a live cue or an unpublished replacement. The count
    /// moves first, so a read that took its point before this starts over, and one that takes it after has the replacement
    /// under it. A filesystem that refuses leaves the record to a rewrite
    pub fn free_record(&self, volume: &Volume<Backend>, free: &FreeRecord) -> bool {
        if !volume.can_free_blocks {
            return true;
        }
        if self.state.read_point() < free.replacement {
            return false;
        }
        self.state.cues.note_reclaim();
        if self.state.cues.has_cue_between(free.place, free.replacement) {
            return false;
        }
        let start = free.offset.0.next_multiple_of(FREE_BLOCK_BYTES);
        let end = (free.offset.0 + free.length.0) / FREE_BLOCK_BYTES * FREE_BLOCK_BYTES;
        if let (Some(pin), true) = (volume.table.pin(free.segment), start < end) {
            let length = ByteCount(end - start);
            if pin.file.free_range(FileOffset(start), length).is_ok() {
                volume.bytes_freed.fetch_add(length.0, Ordering::AcqRel);
            }
        }
        true
    }

    /// Try the records a cue held back, once no cue is left to hold them
    pub fn free_waiting(&mut self, volume: &Volume<Backend>) {
        if self.waiting_frees.is_empty() || !self.state.cues.is_empty() {
            return;
        }
        let mut waiting = mem::take(&mut self.waiting_frees);
        waiting.retain(|free| !self.free_record(volume, free));
        self.waiting_frees = waiting;
    }

    /// Give every free worker the next block. The slots bound how far ahead of the landing the workers get
    pub fn hand_out(&mut self, volume: &Volume<Backend>, target: WalkTarget) {
        self.next_handed_out = self.next_handed_out.max(target.next_block);
        for slot in &volume.walk_slots {
            if self.next_handed_out >= target.block_count {
                return;
            }
            if slot.phase.load(Ordering::Acquire) == SLOT_FREE {
                hold(&slot.work).target = WalkTarget {
                    next_block: self.next_handed_out,
                    ..target
                };
                self.next_handed_out += 1;
                assign(slot);
            }
        }
    }

    /// Land the next block once its worker has counted it, in block order, and let go of what an ended target left behind
    pub fn step_slots(&mut self, volume: &Volume<Backend>, target: WalkTarget, now: Nanoseconds) -> UnitStep {
        self.hand_out(volume, target);
        for (index, slot) in volume.walk_slots.iter().enumerate() {
            let phase = slot.phase.load(Ordering::Acquire);
            if phase != SLOT_COUNTED && phase != SLOT_FAILED {
                continue;
            }
            let mut work = hold(&slot.work);
            if work.target.segment != target.segment {
                slot.phase.store(SLOT_FREE, Ordering::Release);
                continue;
            }
            if work.target.next_block != target.next_block {
                continue;
            }
            if phase == SLOT_FAILED {
                drop(work);
                self.failed_slot = Some(index);
                self.note_retry();
                return self.wait_for_retry(now);
            }

            let next_block = work.next_block;
            let landed = self.land(volume, target, next_block, &mut work.counts);
            drop(work);
            slot.phase.store(SLOT_FREE, Ordering::Release);
            return match landed {
                Ok(()) => UnitStep::Worked,
                Err(_) => self.wait_for_retry(now),
            };
        }
        UnitStep::Idle
    }

    /// Come back after the retry wait
    pub fn wait_for_retry(&mut self, now: Nanoseconds) -> UnitStep {
        let at = Nanoseconds(now.0 + WALK_RETRY.0);
        self.retry_at = Some(at);
        UnitStep::RetryAt(at)
    }

    /// Step this volume's walk once: one row block, or nothing when no segment waits or a failed read's wait isn't over
    pub fn step_unit(&mut self, now: Nanoseconds) -> UnitStep {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        if volume.has_failed() {
            return UnitStep::Idle;
        }
        if let Some(at) = self.retry_at.filter(|at| at.0 > now.0) {
            return UnitStep::RetryAt(at);
        }
        self.retry_at = None;
        self.free_waiting(volume);
        if let Some(index) = self.failed_slot.take() {
            assign(&volume.walk_slots[index]);
        }
        if self.target.is_none() && !self.pick_target() {
            return UnitStep::Idle;
        }
        let Some(target) = self.target else {
            return UnitStep::Idle;
        };
        if !volume.walk_slots.is_empty() && !target.is_damaged {
            return self.step_slots(volume, target, now);
        }
        match self.walk_block(target) {
            Ok(()) => UnitStep::Worked,
            Err(_) => self.wait_for_retry(now),
        }
    }
}

/// Put a slot's block in its worker's hands
pub fn assign<Task: IoTask>(slot: &WalkSlot<Task>) {
    slot.phase.store(SLOT_ASSIGNED, Ordering::Release);
    if let Some(task) = slot.task.get() {
        task.wake();
    }
}

impl<Backend: Io> Job for WalkWorker<Backend> {
    fn step(&mut self) -> JobStep {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        let slot = &volume.walk_slots[self.slot];
        if slot.phase.load(Ordering::Acquire) != SLOT_ASSIGNED {
            return JobStep::Idle;
        }

        let mut work = hold(&slot.work);
        let SlotWork { target, next_block, counts } = &mut *work;
        // blocks reach a worker out of order, so each one finds the key before it
        self.counter.last_column = None;
        self.counter.is_next_key_unknown = false;
        let counted = self.counter.count(volume, *target, counts);
        let phase = match counted {
            Ok(position) => {
                *next_block = position;
                SLOT_COUNTED
            },
            Err(_) => SLOT_FAILED,
        };
        drop(work);
        slot.phase.store(phase, Ordering::Release);
        if let Some(task) = state.planner_task.get() {
            task.wake();
        }
        JobStep::Worked
    }
}

/// Add one row to a block's counts. A row with a replacement is dead, and its bytes move from live to dead. A backward walk
/// first adds every row's bytes as live, since it rebuilds them
pub fn add_count(
    counts: &mut Vec<DeadCount>,
    segment: SegmentNumber,
    column: ColumnId,
    block: u32,
    row: &FooterRow,
    replacement: Option<WritePlace>,
) {
    // keys run in order, so a repeat is near the end, and a miss only costs another entry
    let recent = counts.len().saturating_sub(COUNT_SEARCH);
    let found = counts[recent..].iter().rposition(|count| count.segment == segment && count.block == block);
    let index = match found {
        Some(index) => recent + index,
        None => {
            counts.push(DeadCount {
                segment,
                column,
                block,
                ..DeadCount::default()
            });
            counts.len() - 1
        },
    };
    let count = &mut counts[index];
    let Some(replacement) = replacement else {
        count.kept_bytes = ByteCount(count.kept_bytes.0 + row.length.0);
        return;
    };
    count.replacement = count.replacement.max(replacement);
    count.rows += 1;
    count.graves += u32::from(row.kind == RowKind::Grave);
    count.bytes = ByteCount(count.bytes.0 + row.length.0);
}
