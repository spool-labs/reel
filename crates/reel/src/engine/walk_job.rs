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
use std::sync::{Arc, Mutex};
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::maintenance_report::{JobKind, PickKind, UnitKind};
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, Nanoseconds, WritePlace};

use crate::config::{MAX_KEY_BYTES, MOST_VOLUME_SLOTS};
use crate::engine::load_state::{is_capped, pace_credit};
use crate::engine::held_bits::HeldRow;
use crate::engine::cover_apply::{ApplyStep, CoverApply};
use crate::engine::engine_state::EngineState;
use crate::engine::rewrite_job::{RewriteJob, RewriteSeat};
use crate::engine::workers::Workers;
use crate::engine::volume::{NO_SEAL, Volume};
use crate::guard::hold;
use crate::index::key_hash::{KeyHash, hash_key};
use crate::io::device_error::is_device_error;
use crate::io::job::{Job, JobStep};
use crate::io::{Io, IoFile, IoTask};
use crate::engine::admission::JobRole;
use crate::engine::block_lookups::BlockLookups;
use crate::engine::dead_bits::DeadRow;
use crate::segment::footer_reader::RowAt;
use crate::segment::key_cursor::KeyCursor;
use crate::segment::summary_orders::WalkList;
use crate::segment::summary::SegmentFacts;
use crate::fixed_list::{FixedList, ListFull, full};
use crate::region::{REGION_HEAD_BYTES, Region, list_bytes};
use crate::row_kind::RowKind;
use crate::segment::footer_writer::MOST_BLOCK_ROWS;
use crate::segment::row_block::{FooterRow, ROW_BLOCK_BYTES, RowBlockWalk};
use crate::segment::segment_table::{SegmentPin, WalkBlock};
use crate::segment::segment_tail::{MOST_MERGE_SEGMENTS, SegmentNumber};
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

/// A walk slot has no block
pub const SLOT_FREE: u8 = 0;
/// A walk slot has a block for any worker to count
pub const SLOT_ASSIGNED: u8 = 1;
/// A walk slot holds a block's counts, waiting for the planner to land them in block order
pub const SLOT_COUNTED: u8 = 2;
/// A walk slot holds a block its worker couldn't count
pub const SLOT_FAILED: u8 = 3;
/// The most inputs the rewrites can hold at once: eight for each lane, a seat a walk slot and the planner's
pub const MOST_HOLDING: usize = (MOST_VOLUME_SLOTS + 1) * MOST_MERGE_SEGMENTS;
/// A walk slot holds counts whose held rows a closed rewrite couldn't take, and its block counts again after the release
pub const SLOT_CLAIMED: u8 = 5;
/// A worker has taken a walk slot's block and is counting it
pub const SLOT_COUNTING: u8 = 4;

/// What a counted block's landing came to
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Landing {
    /// The counts landed and the walk moved past the block
    Landed,
    /// A rewrite began or ended holding its inputs after the block was counted, so it counts again
    Stale,
    /// A held row's rewrite has begun to land its bits, so the block waits for the release and counts again then
    Waiting,
}

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

/// A complete slot a charged row leaves, freed once the block's counts land
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SlotFree {
    pub column: ColumnId,
    pub hash: KeyHash,
    pub segment: SegmentNumber,
    pub block_offset: u32,
}

/// What counting one block found
#[derive(Debug, Default, Eq, PartialEq)]
pub struct BlockCounts {
    pub counts: FixedList<DeadCount>,
    pub frees: FixedList<FreeRecord>,
    pub overflowed: FixedList<SegmentNumber>,
    pub read_bytes: ByteCount,
    pub rows: u64,
    pub held: FixedList<HeldRow>,
    pub holding: FixedList<(SegmentNumber, u8)>,
    pub claims_seen: u64,
    /// A count can be thrown away and taken again, and a slot it freed early would hide the version the next count must charge
    pub slot_frees: FixedList<SlotFree>,
    /// Each charged row's place, so its bit lands with its count
    pub dead_rows: FixedList<DeadRow>,
    /// A charged row didn't fit the list, so the block's segments lose exact bits
    pub are_rows_cut: bool,
}

impl BlockCounts {
    /// Allocate room for any block
    pub fn new() -> Self {
        Self {
            counts: FixedList::on_heap(MOST_BLOCK_ROWS),
            frees: FixedList::on_heap(MOST_BLOCK_FREES),
            overflowed: FixedList::on_heap(MOST_BLOCK_FREES),
            read_bytes: ByteCount(0),
            rows: 0,
            held: FixedList::on_heap(MOST_BLOCK_ROWS),
            holding: FixedList::on_heap(MOST_HOLDING),
            claims_seen: 0,
            slot_frees: FixedList::on_heap(MOST_BLOCK_ROWS),
            dead_rows: FixedList::on_heap(2 * MOST_BLOCK_ROWS),
            are_rows_cut: false,
        }
    }

    /// Carve room for a block of up to this many rows from a walk's region: a count for each row and one for the block itself
    pub fn in_region(region: &mut Region, rows: usize) -> std::result::Result<Self, ListFull> {
        Ok(Self {
            counts: region.carve(rows + 1)?,
            frees: region.carve(MOST_BLOCK_FREES)?,
            overflowed: region.carve(MOST_BLOCK_FREES)?,
            read_bytes: ByteCount(0),
            rows: 0,
            held: region.carve(rows + 1)?,
            holding: region.carve(MOST_HOLDING)?,
            claims_seen: 0,
            slot_frees: region.carve(rows + 1)?,
            dead_rows: region.carve(2 * rows + 1)?,
            are_rows_cut: false,
        })
    }

    /// Count the region bytes room for a block of up to this many rows takes
    pub fn region_bytes(rows: usize) -> usize {
        let held = list_bytes::<HeldRow>(rows + 1) + list_bytes::<(SegmentNumber, u8)>(MOST_HOLDING) + list_bytes::<SlotFree>(rows + 1) + list_bytes::<DeadRow>(2 * rows + 1);
        list_bytes::<DeadCount>(rows + 1) + list_bytes::<FreeRecord>(MOST_BLOCK_FREES) + list_bytes::<SegmentNumber>(MOST_BLOCK_FREES) + held
    }

    /// Give the lists back, and return the most bytes they wrote
    pub fn release(&mut self) -> usize {
        let held = self.held.peak_bytes() + self.holding.peak_bytes() + self.slot_frees.peak_bytes() + self.dead_rows.peak_bytes();
        let peak = self.counts.peak_bytes() + self.frees.peak_bytes() + self.overflowed.peak_bytes() + held;
        *self = Self::default();
        peak
    }

    /// Read the heap they hold
    pub fn heap_bytes(&self) -> ByteCount {
        let counts = self.counts.capacity() * size_of::<DeadCount>() + self.slot_frees.capacity() * size_of::<SlotFree>() + self.dead_rows.capacity() * size_of::<DeadRow>();
        ByteCount((counts + self.frees.capacity() * size_of::<FreeRecord>()) as u64)
    }

    /// Empty the lists
    pub fn clear(&mut self) {
        self.counts.clear();
        self.frees.clear();
        self.overflowed.clear();
        self.read_bytes = ByteCount(0);
        self.rows = 0;
        self.held.clear();
        self.slot_frees.clear();
        self.dead_rows.clear();
        self.are_rows_cut = false;
    }

    /// Note which inputs the rewrites hold now, and the claims' count they were read at, before a block is counted
    pub fn note_claims<Backend: Io>(&mut self, volume: &Volume<Backend>) {
        self.claims_seen = volume.claims_changed.load(Ordering::Acquire);
        self.holding.clear();
        for (lane, rewrite) in volume.lanes.iter().enumerate() {
            if !rewrite.is_holding.load(Ordering::Acquire) {
                continue;
            }
            for input in hold(&rewrite.inputs).iter().filter(|input| input.0 != 0) {
                let _pushed = self.holding.push_if_room((*input, lane as u8));
            }
        }
    }

    /// Find the lane whose rewrite holds a segment
    pub fn holder(&self, segment: SegmentNumber) -> Option<u8> {
        self.holding.iter().find(|(input, _lane)| *input == segment).map(|(_input, lane)| *lane)
    }

    /// Count a row dead, and note a large record so its blocks can go
    pub fn count_dead(
        &mut self,
        segment: SegmentNumber,
        column: ColumnId,
        block: u32,
        row: &FooterRow,
        replacement: WritePlace,
        at: RowAt,
    ) -> std::result::Result<(), ListFull> {
        add_count(&mut self.counts, segment, column, block, row, Some(replacement))?;
        if !self.dead_rows.push_if_room(DeadRow::of(segment, block, at, row.length.0, row.kind)) {
            self.are_rows_cut = true;
        }
        if row.kind != RowKind::Value || row.length < FREE_IN_PLACE_BYTES {
            return Ok(());
        }
        if self.frees.room() == 0 {
            if !self.overflowed.contains(&segment) && self.overflowed.room() > 0 {
                self.overflowed.push(segment)?;
            }
            return Ok(());
        }
        self.frees.push(FreeRecord {
            segment,
            offset: row.offset,
            length: row.length,
            place: row.place,
            replacement,
        })
    }
}

/// One block's counts on their way from a worker to the planner
#[derive(Debug, Default, Eq, PartialEq)]
pub struct SlotWork {
    pub target: WalkTarget,
    pub next_block: u32,
    pub counts: BlockCounts,
}

/// A place on a volume for a block any worker takes, and for the counts it hands back. Workers only read, and the planner alone
/// lands counts, so a volume still has one walk: it keeps one target, one position and one order of landing
#[derive(Debug)]
pub struct WalkSlot {
    pub phase: AtomicU8,
    pub work: Mutex<SlotWork>,
}

impl WalkSlot {
    /// Build an empty slot with room for one block's counts
    pub fn new() -> Self {
        Self {
            phase: AtomicU8::new(SLOT_FREE),
            work: Mutex::new(SlotWork {
                ..SlotWork::default()
            }),
        }
    }

    /// Read the heap the slot's counts hold
    pub fn heap_bytes(&self) -> ByteCount {
        hold(&self.work).counts.heap_bytes()
    }
}

impl Default for WalkSlot {
    fn default() -> Self {
        Self::new()
    }
}

/// What it takes to count one row block: nothing here changes the store
pub struct BlockCounter<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
    block: FixedList<u8>,
    key: FixedList<u8>,
    last_key: FixedList<u8>,
    pub last_column: Option<ColumnId>,
    is_next_key_unknown: bool,
    pub cursor: KeyCursor<Backend::File>,
    pub lookups: BlockLookups,
    collect_key: FixedList<u8>,
    collect_last: FixedList<u8>,
}

/// The walk of one volume's sealed segments
pub struct WalkJob<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
    pub volume: usize,
    pub target: Option<WalkTarget>,
    pub retry_at: Option<Nanoseconds>,
    counter: BlockCounter<Backend>,
    counts: BlockCounts,
    waiting_frees: FixedList<FreeRecord>,
    next_handed_out: u32,
    failed_slot: Option<usize>,
    waiting_on: Option<u64>,
    claim_since: Option<Nanoseconds>,
    pub apply: CoverApply<Backend>,
    region: Option<Region>,
    block_rows: usize,
    region_need: ByteCount,
    slots_carved: u64,
}

/// A worker of the store's, which counts any volume's blocks and runs the rewrites any planner hands out
pub struct WalkWorker<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
    pub index: usize,
    pub next_volume: usize,
    pub counter: BlockCounter<Backend>,
    pub seats: Vec<Arc<Vec<RewriteSeat<Backend>>>>,
    pub rewrite: Option<RewriteJob<Backend>>,
    pub seat: (usize, usize),
}

impl<Backend: Io> BlockCounter<Backend> {
    /// Allocate the fixed buffers
    pub fn new(state: Arc<EngineState<Backend>>) -> Self {
        Self {
            state,
            block: FixedList::filled_on_heap(ROW_BLOCK_BYTES, 0),
            key: FixedList::on_heap(MAX_KEY_BYTES as usize),
            last_key: FixedList::on_heap(MAX_KEY_BYTES as usize),
            last_column: None,
            is_next_key_unknown: false,
            cursor: KeyCursor::new(),
            lookups: BlockLookups::new(),
            collect_key: FixedList::on_heap(MAX_KEY_BYTES as usize),
            collect_last: FixedList::on_heap(MAX_KEY_BYTES as usize),
        }
    }

    /// Read the heap the buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        let keys = self.block.capacity() + self.key.capacity() + self.last_key.capacity() + self.collect_key.capacity() + self.collect_last.capacity();
        ByteCount(keys as u64 + self.cursor.heap_bytes().0 + self.lookups.heap_bytes().0)
    }

    /// Count a target's next block into a list, and return the position past it
    pub fn count(&mut self, volume: &Volume<Backend>, target: WalkTarget, counts: &mut BlockCounts) -> Result<u32> {
        counts.clear();
        counts.note_claims(volume);

        // a damaged segment answers for nothing, so its rows count nothing and the walk moves past it
        let pin = volume.table.pin(target.segment).filter(|_pin| !target.is_damaged);
        let block = volume.table.walk_block(target.segment, target.next_block);
        let (Some(pin), Some(block)) = (pin, block) else {
            return Ok(target.block_count);
        };
        if self.last_column.is_none() && target.next_block > 0 {
            self.load_last_key(volume, &pin, target, block.column)?;
        }
        let counted = self.count_block(volume, &pin, &block, target, counts);
        self.cursor.release(&volume.table);
        self.state.note_cursor(mem::take(&mut self.cursor.counts), false);
        let lookups = mem::take(&mut self.lookups.counts);
        for (total, count) in self.state.block_lookups.iter().zip([lookups.blocks, lookups.candidates, lookups.whole, lookups.key_checks]) {
            total.fetch_add(count, Ordering::Relaxed);
        }
        match counted {
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
                    self.last_key.set_from(&self.key)?;
                    self.last_column = Some(block.column);
                }
            }
            if self.last_column.is_some() {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Gather the keys a block's rows look up, in the order the count asks for them, or return false when they don't fit
    pub fn collect_lookups(&mut self, block: &WalkBlock, place_base: WritePlace, length: usize) -> Result<bool> {
        self.lookups.clear();
        let Some(mut walk) = RowBlockWalk::open(&self.block[..length], place_base) else {
            return Ok(true);
        };
        self.collect_last.set_from(&self.last_key)?;
        let mut last_column = self.last_column;
        let mut is_unknown = self.is_next_key_unknown;
        while let Some(row) = walk.next_row(&mut self.collect_key) {
            if row.kind == RowKind::RangeDelete {
                continue;
            }
            let is_top = last_column != Some(block.column) || self.collect_last != self.collect_key;
            if is_top && is_unknown {
                is_unknown = false;
                self.collect_last.set_from(&self.collect_key)?;
                last_column = Some(block.column);
                continue;
            }
            if !is_top {
                continue;
            }
            self.collect_last.set_from(&self.collect_key)?;
            last_column = Some(block.column);
            if !self.lookups.add(&self.collect_key) {
                return Ok(false);
            }
        }
        Ok(true)
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
        counts.read_bytes = block.entry.block_length();
        pin.file.read_exact_at(block.entry.block_at(), &mut self.block[..length])?;
        // the block's lookups run together, one candidate at a time, and the count below takes their answers in order
        let mut answers = None;
        if self.collect_lookups(block, pin.head.place_base, length)? {
            match self.state.look_up_block(volume, block.column, target.segment, &mut self.lookups, &mut self.cursor) {
                Ok(true) => answers = Some(0),
                Ok(false) => {},
                Err(_error) => return Err(Error::from(ErrorKind::Interrupted)),
            }
        }
        let Some(mut walk) = RowBlockWalk::open(&self.block[..length], pin.head.place_base) else {
            return Ok(());
        };
        // a key's rows run newest first, so the first one the walk meets is its top, and carrying the last key across
        // a block is enough
        let mut last_place = WritePlace(u64::MAX);

        while let Some(row) = walk.next_row(&mut self.key) {
            counts.rows += 1;
            debug_assert!(self.last_column != Some(block.column) || self.last_key != self.key || row.place < last_place);
            last_place = row.place;
            if row.kind == RowKind::RangeDelete {
                continue;
            }
            if target.is_backward {
                add_count(&mut counts.counts, target.segment, block.column, target.next_block, &row, None)?;
            }
            let is_top = self.last_column != Some(block.column) || self.last_key != self.key;
            // a key that may have begun in a block no read can give has a top no walk can name
            if is_top && self.is_next_key_unknown {
                self.is_next_key_unknown = false;
                self.last_key.set_from(&self.key)?;
                self.last_column = Some(block.column);
                continue;
            }
            if !is_top {
                // the key's top is in this segment, and its highest place is at or above that
                let replacement = pin.head.places.highest;
                counts.count_dead(target.segment, block.column, target.next_block, &row, replacement, RowAt::of_walk(&walk))?;
                continue;
            }

            self.last_key.set_from(&self.key)?;
            self.last_column = Some(block.column);
            let found = match &mut answers {
                Some(next) => {
                    let lookup = self.lookups.lookups[*next];
                    *next += 1;
                    match lookup.is_damaged {
                        true => Err(StoreError::Corruption { column: block.column }),
                        false => Ok(lookup.best),
                    }
                },
                None => self.state.newest_walked_held(volume, block.column, &self.key, &[target.segment], None, &mut self.cursor),
            };
            let newest = match found {
                Ok(newest) => newest,
                // a version a damaged block hides counts nothing, which only understates
                Err(StoreError::Corruption { .. }) => None,
                // the kind alone, since a stop comes through here during close and close allocates nothing
                Err(_error) => return Err(Error::from(ErrorKind::Interrupted)),
            };
            let covered = volume.covers.newest_applied(block.column, &self.key);
            let is_covered = |place: WritePlace| covered.is_some_and(|cover| cover > place);
            match newest {
                Some(found) if found.row.place > row.place => {
                    let replacement = found.row.place;
                    counts.slot_frees.push(SlotFree { column: block.column, hash: hash_key(block.column, &self.key), segment: target.segment, block_offset: block.entry.block_offset })?;
                    counts.count_dead(target.segment, block.column, target.next_block, &row, replacement, RowAt::of_walk(&walk))?;
                },
                // a row under an applied cover is dead by it, and so is any older one a row here would find
                _ if is_covered(row.place) => {
                    let replacement = covered.unwrap_or(row.place);
                    counts.count_dead(target.segment, block.column, target.next_block, &row, replacement, RowAt::of_walk(&walk))?;
                },
                Some(found) if !target.is_backward && !is_covered(found.row.place) => {
                    if let Some(index) = volume.table.block_at(found.segment, found.block_offset) {
                        // a row of an input a rewrite holds goes to its bits, which its drain lands where the row went
                        match counts.holder(found.segment) {
                            Some(lane) => counts.held.push(HeldRow { segment: found.segment, lane, block: index, at: found.at })?,
                            None => counts.count_dead(found.segment, block.column, index, &found.row, row.place, found.at)?,
                        }
                        counts.slot_frees.push(SlotFree { column: block.column, hash: hash_key(block.column, &self.key), segment: found.segment, block_offset: found.block_offset })?;
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
            apply: CoverApply::new(Arc::clone(&state), volume),
            counter: BlockCounter::new(Arc::clone(&state)),
            state,
            volume,
            target: None,
            retry_at: None,
            counts: BlockCounts::default(),
            waiting_frees: FixedList::on_heap(MOST_BLOCK_FREES),
            next_handed_out: 0,
            failed_slot: None,
            waiting_on: None,
            claim_since: None,
            region: None,
            block_rows: 0,
            region_need: ByteCount(0),
            slots_carved: 0,
        }
    }

    /// Read the heap the walk's buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        let waiting = self.waiting_frees.capacity() * size_of::<FreeRecord>();
        ByteCount(self.counter.heap_bytes().0 + self.counts.heap_bytes().0 + self.apply.heap_bytes().0 + waiting as u64)
    }

    /// Pick a segment whose walk hasn't finished. Backward walks go first and newest first, so an older segment finds a
    /// newer one finished and counts against it, and then forward walks go from the lowest
    pub fn pick_target(&mut self) -> bool {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        // a seal only ends past the mark, so every row a walk counts by is published, and its slot has let go of the file
        let sealing = volume.sealing.load(Ordering::Acquire);
        let rows = state.hold_for_pick(self.volume, PickKind::Walk);
        let can_walk = |segment: &SegmentNumber| segment.0 != sealing && !volume.is_merging(*segment);
        let orders = rows.orders();
        // the walk lists hold only unwalked rows, and the forward one runs in segment order
        let backward = orders.walk_list(WalkList::Backward).filter(can_walk).filter_map(|segment| rows.get(segment));
        let backward = backward.max_by_key(|row| row.facts.places.highest);
        let picked = backward.or_else(|| orders.walk_list(WalkList::Unwalked).find(can_walk).and_then(|segment| rows.get(segment)));
        self.target = picked.map(|row| WalkTarget {
            segment: row.facts.segment,
            next_block: row.facts.walked_blocks,
            block_count: row.facts.block_count,
            is_backward: row.facts.is_backward,
            is_damaged: row.facts.is_damaged,
        });
        self.counter.last_column = None;
        self.next_handed_out = self.target.map_or(0, |target| target.next_block);
        volume.has_walk_target.store(self.target.is_some(), Ordering::Release);
        volume.walk_segment.store(self.target.map_or(NO_SEAL, |target| target.segment.0), Ordering::Release);
        self.target.is_some()
    }

    /// Take the target's region when it has none yet, sized from the most rows one of its blocks holds
    pub fn ensure_region(&mut self, volume: &Volume<Backend>) -> bool {
        if self.region.is_some() {
            return true;
        }
        let Some(target) = self.target else {
            return false;
        };
        let facts = hold(&volume.summary).get(target.segment).map(|row| row.facts);
        self.take_region(volume, facts.as_ref().map_or(MOST_BLOCK_ROWS, block_rows_bound))
    }

    /// Map the target's region and carve its counts, the planner's, the cover apply's and a slot's each, or say the walk waits
    pub fn take_region(&mut self, volume: &Volume<Backend>, block_rows: usize) -> bool {
        let need = REGION_HEAD_BYTES + (2 + volume.walk_slots.len()) * BlockCounts::region_bytes(block_rows);
        let Some(mut region) = self.state.admit(JobRole::Walk, ByteCount(need as u64), true) else {
            return false;
        };
        self.region_need = ByteCount(need as u64);
        let carved = self.carve(&mut region, block_rows);
        self.region = Some(region);
        if carved.is_err() {
            self.release_region(volume);
            return false;
        }
        true
    }

    /// Carve the planner's and the cover apply's counts, and leave room for a slot's each, which a slot takes at its first block
    fn carve(&mut self, region: &mut Region, block_rows: usize) -> std::result::Result<(), ListFull> {
        self.counts = BlockCounts::in_region(region, block_rows)?;
        self.apply.counts = BlockCounts::in_region(region, block_rows)?;
        self.block_rows = block_rows;
        self.slots_carved = 0;
        Ok(())
    }

    /// Give a free slot this target's counts the first time it takes one of the target's blocks
    fn carve_slot(&mut self, index: usize, work: &mut SlotWork) -> std::result::Result<(), ListFull> {
        if self.slots_carved >> index & 1 == 0 {
            let Some(region) = self.region.as_mut() else {
                return Err(full());
            };
            work.counts = BlockCounts::in_region(region, self.block_rows)?;
            self.slots_carved |= 1 << index;
        }
        Ok(())
    }

    /// Give the target's counts and region back, and count the bytes they wrote
    pub fn release_region(&mut self, volume: &Volume<Backend>) {
        let Some(region) = self.region.take() else {
            return;
        };
        let mut touched = self.counts.release() + self.apply.counts.release();
        // a slot still holding a stale count keeps its list, and the region's hold keeps the memory under it until the list goes
        for (index, slot) in volume.walk_slots.iter().enumerate() {
            if self.slots_carved >> index & 1 == 1 && slot.phase.load(Ordering::Acquire) == SLOT_FREE {
                touched += hold(&slot.work).counts.release();
            }
        }
        self.slots_carved = 0;
        drop(region);
        self.state.give_back(JobRole::Walk, self.region_need, ByteCount(touched as u64));
    }

    /// Walk one row block here. A failure leaves the last key wherever the attempt stood, so the retry finds it again
    pub fn walk_block(&mut self, target: WalkTarget) -> Result<()> {
        let state = Arc::clone(&self.state);
        if !self.ensure_region(&state.volumes[self.volume]) {
            return Err(Error::from(ErrorKind::Interrupted));
        }
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
        let started = state.io.now();
        let outcome = self.counter.count(volume, target, &mut counts);
        state.note_unit(UnitKind::WalkBlock, counts.rows, started);
        let landing = outcome.and_then(|next_block| self.land(volume, target, next_block, &mut counts));
        self.counts = counts;
        match landing? {
            Landing::Landed => self.end_claim_wait(volume, state.io.now()),
            // the next step counts the block again, and the key it remembers is this block's last, which would make the block's
            // first row look like an older version of it
            Landing::Stale => self.counter.last_column = None,
            Landing::Waiting => {
                self.counter.last_column = None;
                self.note_claim_wait(volume, started);
            },
        }
        Ok(())
    }

    /// Land one block's counts and the new position under the claim lock, so no rewrite begins or ends holding in between.
    /// A block counted before a rewrite began or ended holding counts again, and one whose held rows a closed rewrite can't
    /// take waits for its release. Every held row's bit is set or none is, so a block that waits marks nothing
    pub fn land(&mut self, volume: &Volume<Backend>, target: WalkTarget, next_block: u32, counts: &mut BlockCounts) -> Result<Landing> {
        {
            let _claim = hold(&volume.claim_lock);
            if counts.claims_seen != volume.claims_changed.load(Ordering::Acquire) {
                return Ok(Landing::Stale);
            }
            for row in counts.held.iter() {
                if hold(&volume.lanes[usize::from(row.lane)].held).reserve(row).is_err() {
                    return Ok(Landing::Waiting);
                }
            }
            for row in counts.held.iter() {
                hold(&volume.lanes[usize::from(row.lane)].held).mark(row);
            }
            self.apply_block(volume, target, next_block, counts);
            // a free on a segment a rewrite has claimed would cut a record its copy still reads
            counts.frees.retain(|free| !volume.is_taken(free.segment));
        }
        self.after_landing(volume, target, next_block, counts)?;
        Ok(Landing::Landed)
    }

    /// Apply one block's counts and the new position to the summary and the table. A count for a segment that left since goes
    /// nowhere, since a segment number is never used twice
    pub fn apply_block(&mut self, volume: &Volume<Backend>, target: WalkTarget, next_block: u32, counts: &mut BlockCounts) {
        self.state.note_moved(JobKind::DeadByteWalk, counts.read_bytes);
        // a row's bit lands with its count, and a row counted before comes back out, so a row counts once
        if counts.are_rows_cut {
            volume.dead_bits.spoil(&counts.counts);
        }
        if !volume.are_bits_lost() {
            volume.dead_bits.land(&counts.dead_rows, &mut counts.counts);
        }
        counts.counts.sort_unstable_by_key(|count| count.segment);
        let mut rows = hold(&volume.summary);
        rows.apply_walk(target.segment, next_block, &counts.counts);
        if !target.is_backward {
            rows.note_forward(target.segment);
        }
        drop(rows);
        volume.walk_blocks.fetch_add(1, Ordering::Relaxed);
        // the dead bytes a block counted pay for paced rewrites at the pace's multiplier, up to one pick's worth
        let dead_bytes = counts.counts.iter().map(|count| count.bytes.0).sum();
        let spilled = volume.pace.add(pace_credit(dead_bytes, volume.pace_per_mille()), volume.pace_cap());
        volume.pace_spilled.fetch_add(spilled, Ordering::Relaxed);
        volume.dead_counted.fetch_add(dead_bytes, Ordering::Relaxed);
        for count in counts.counts.iter().filter(|count| count.rows > 0) {
            volume.table.drop_live(count.segment, count.block, count.rows, count.replacement);
        }
        for free in counts.slot_frees.iter() {
            self.state.free_superseded_slot(volume, free.column, free.hash, free.segment, free.block_offset);
        }
    }

    /// Free what a landed block's large dead records held, and move the walk on past the block
    pub fn after_landing(
        &mut self,
        volume: &Volume<Backend>,
        target: WalkTarget,
        next_block: u32,
        counts: &mut BlockCounts,
    ) -> Result<()> {
        for segment in counts.overflowed.iter().filter(|_segment| volume.can_free_blocks) {
            volume.note_held_frees(*segment);
        }
        for free in &counts.frees {
            if self.state.free_record(volume, free) {
                continue;
            }
            if self.waiting_frees.len() == self.waiting_frees.capacity() {
                volume.note_held_frees(free.segment);
                continue;
            }
            self.waiting_frees.push(*free)?;
        }
        if next_block < target.block_count {
            self.target = Some(WalkTarget { next_block, ..target });
            return Ok(());
        }
        // the covers land before the mark, while no other walk is running to see the rows they hide
        if self.has_covers(volume, target.segment) {
            // a forward walk came first, and its apply counted what these hide
            if target.is_backward {
                self.end_apply(volume, target.segment);
                return self.finish_target(volume, target.segment);
            }
            self.apply.start(target.segment);
            volume.is_applying.store(true, Ordering::Release);
            self.target = Some(WalkTarget { next_block, ..target });
            return Ok(());
        }
        self.finish_target(volume, target.segment)
    }

    /// Check a segment's summary row says it holds covers
    pub fn has_covers(&self, volume: &Volume<Backend>, segment: SegmentNumber) -> bool {
        hold(&volume.summary).get(segment).is_some_and(|row| row.facts.has_covers)
    }

    /// Take one step of a target's covers, and end the target with the last
    pub fn step_apply(&mut self, volume: &Volume<Backend>, now: Nanoseconds) -> JobStep {
        let Some(target) = self.target else {
            return JobStep::Idle;
        };
        let started = self.state.io.now();
        let applied = self.apply.step();
        self.state.note_unit(UnitKind::CoverApply, 1, started);
        match applied {
            Ok(ApplyStep::Working) => JobStep::Worked,
            Ok(ApplyStep::Done) => {
                self.end_apply(volume, target.segment);
                match self.finish_target(volume, target.segment) {
                    Ok(()) => JobStep::Worked,
                    Err(_) => self.wait_for_retry(now),
                }
            },
            Err(_) => self.wait_for_retry(now),
        }
    }

    /// Note a segment's covers are applied
    pub fn end_apply(&self, volume: &Volume<Backend>, segment: SegmentNumber) {
        volume.is_applying.store(false, Ordering::Release);
        volume.covers.mark_segment_applied(segment);
        // an older cover inside this one could not go until now
        volume.has_kept_covers.store(true, Ordering::Release);
    }

    /// End a walk target
    pub fn finish_target(&mut self, volume: &Volume<Backend>, segment: SegmentNumber) -> Result<()> {
        volume.table.mark_walked(segment);
        // the counts just landed, so the segment may have crossed a bar, and a pick a refusal holds back looks again
        volume.walks_finished.fetch_add(1, Ordering::AcqRel);
        volume.has_walk_target.store(false, Ordering::Release);
        volume.walk_segment.store(NO_SEAL, Ordering::Release);
        volume.wants_compaction.store(true, Ordering::Release);
        // a row dropped for room after its seal counted can leave this at zero
        let _waiting = volume.walks_waiting.fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| count.checked_sub(1));
        self.target = None;
        self.release_region(volume);
        self.state.save_summary(volume)
    }

    /// Try the records a cue held back, once no cue is left to hold them
    pub fn free_waiting(&mut self, volume: &Volume<Backend>) {
        if self.waiting_frees.is_empty() || !self.state.cues.is_empty() {
            return;
        }
        let mut waiting = mem::take(&mut self.waiting_frees);
        waiting.retain(|free| !self.state.free_record(volume, free));
        self.waiting_frees = waiting;
    }

    /// Give free workers the next blocks, up to the walk cap while callers are busy. The slots bound how far ahead of the landing
    /// the workers get
    pub fn hand_out(&mut self, volume: &Volume<Backend>, target: WalkTarget) {
        self.next_handed_out = self.next_handed_out.max(target.next_block);
        // busy callers keep their cores: a few workers count blocks while they call, and every worker may once they go quiet
        let now = self.state.io.now();
        let cap = if is_capped(self.state.load_of(volume, now)) { volume.walk_cap() } else { usize::MAX };
        let mut busy = volume.walk_slots.iter().filter(|slot| slot.phase.load(Ordering::Acquire) != SLOT_FREE).count();
        for (index, slot) in volume.walk_slots.iter().enumerate() {
            if self.next_handed_out >= target.block_count || busy >= cap {
                return;
            }
            if slot.phase.load(Ordering::Acquire) == SLOT_FREE {
                let mut work = hold(&slot.work);
                // a slot with no room for this target's counts waits for the next hand-out
                if self.carve_slot(index, &mut work).is_err() {
                    continue;
                }
                work.target = WalkTarget {
                    next_block: self.next_handed_out,
                    ..target
                };
                drop(work);
                self.next_handed_out += 1;
                assign(&self.state.workers, slot);
                busy += 1;
            }
        }
    }

    /// Land the next block once its worker has counted it, in block order, and let go of what an ended target left behind
    pub fn step_slots(&mut self, volume: &Volume<Backend>, target: WalkTarget, now: Nanoseconds) -> JobStep {
        self.hand_out(volume, target);
        for (index, slot) in volume.walk_slots.iter().enumerate() {
            let phase = slot.phase.load(Ordering::Acquire);
            if phase != SLOT_COUNTED && phase != SLOT_FAILED && phase != SLOT_CLAIMED {
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

            // a block that waits on a rewrite's release counts again once the claims change
            if phase == SLOT_CLAIMED {
                drop(work);
                if self.waiting_on == Some(volume.claims_changed.load(Ordering::Acquire)) {
                    return JobStep::Idle;
                }
                self.end_claim_wait(volume, now);
                assign(&self.state.workers, slot);
                return JobStep::Worked;
            }

            let next_block = work.next_block;
            let landing = self.land(volume, target, next_block, &mut work.counts);
            drop(work);
            return match landing {
                Ok(Landing::Landed) => {
                    slot.phase.store(SLOT_FREE, Ordering::Release);
                    self.end_claim_wait(volume, now);
                    JobStep::Worked
                },
                Ok(Landing::Stale) => {
                    assign(&self.state.workers, slot);
                    JobStep::Worked
                },
                Ok(Landing::Waiting) => {
                    slot.phase.store(SLOT_CLAIMED, Ordering::Release);
                    self.note_claim_wait(volume, now);
                    JobStep::Idle
                },
                Err(_) => {
                    slot.phase.store(SLOT_FREE, Ordering::Release);
                    self.wait_for_retry(now)
                },
            };
        }
        JobStep::Idle
    }

    /// Come back after the retry wait
    pub fn wait_for_retry(&mut self, now: Nanoseconds) -> JobStep {
        let at = Nanoseconds(now.0 + WALK_RETRY.0);
        self.retry_at = Some(at);
        JobStep::IdleUntil { deadline: at }
    }

    /// Step this volume's walk once: one row block, or nothing when no segment waits or a failed read's wait isn't over
    pub fn step_unit(&mut self, now: Nanoseconds) -> JobStep {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        if volume.has_failed() {
            return JobStep::Idle;
        }
        if let Some(at) = self.retry_at.filter(|at| at.0 > now.0) {
            return JobStep::IdleUntil { deadline: at };
        }
        self.retry_at = None;
        self.free_waiting(volume);
        if let Some(index) = self.failed_slot.take() {
            assign(&self.state.workers, &volume.walk_slots[index]);
        }
        // a target holds its counts from its first step, and waits for room when admission has none
        if self.target.is_some() && !self.ensure_region(volume) {
            return self.wait_for_retry(now);
        }
        if self.apply.segment.is_some() {
            return self.step_apply(volume, now);
        }
        if self.target.is_none() && volume.is_walk_held() {
            return JobStep::Idle;
        }
        // the walk once stood aside here while any rewrite ran. A rewrite's held bits take the walk's charges on its inputs now,
        // and the count stays for the figure the hold gave
        if self.target.is_none() && volume.is_rewriting() {
            volume.walks_held.fetch_add(1, Ordering::Relaxed);
        }
        if self.target.is_none() && !self.pick_target() {
            return JobStep::Idle;
        }
        let Some(target) = self.target else {
            return JobStep::Idle;
        };
        if !self.ensure_region(volume) {
            return self.wait_for_retry(now);
        }
        if !volume.walk_slots.is_empty() && !target.is_damaged {
            return self.step_slots(volume, target, now);
        }
        // a block that waits on a rewrite's release counts again once the claims change
        if self.is_waiting(volume) {
            return JobStep::Idle;
        }
        match self.walk_block(target) {
            Ok(()) if self.is_waiting(volume) => JobStep::Idle,
            Ok(()) => JobStep::Worked,
            Err(_) => self.wait_for_retry(now),
        }
    }
}

/// Put a slot's block in any worker's hands, and wake one that sleeps
pub fn assign<Task: IoTask>(workers: &Workers<Task>, slot: &WalkSlot) {
    slot.phase.store(SLOT_ASSIGNED, Ordering::SeqCst);
    workers.wake_one();
}

impl<Backend: Io> WalkWorker<Backend> {
    /// Take a block any volume handed out and count it, looking at the volumes in turn, and say whether there was one
    pub fn count_one(&mut self) -> bool {
        let state = Arc::clone(&self.state);
        let volumes = state.volumes.len();
        for turn in 0..volumes {
            let at = (self.next_volume + turn) % volumes;
            let volume = &state.volumes[at];
            let is_taken = |slot: &&WalkSlot| {
                slot.phase.load(Ordering::SeqCst) == SLOT_ASSIGNED
                    && slot.phase.compare_exchange(SLOT_ASSIGNED, SLOT_COUNTING, Ordering::SeqCst, Ordering::SeqCst).is_ok()
            };
            let Some(slot) = volume.walk_slots.iter().find(is_taken) else {
                continue;
            };
            self.next_volume = (at + 1) % volumes;
            self.count_slot(&state, volume, slot);
            return true;
        }
        false
    }

    /// Count a taken slot's block and hand the counts back to the planner
    fn count_slot(&mut self, state: &EngineState<Backend>, volume: &Volume<Backend>, slot: &WalkSlot) {
        let cell = &state.workers.cells[self.index];
        cell.is_active.store(true, Ordering::Release);
        let mut work = hold(&slot.work);
        let SlotWork { target, next_block, counts } = &mut *work;
        // blocks reach a worker out of order, so each one finds the key before it
        self.counter.last_column = None;
        self.counter.is_next_key_unknown = false;
        let started = state.io.now();
        let counted = self.counter.count(volume, *target, counts);
        state.note_unit(UnitKind::WalkBlock, counts.rows, started);
        let phase = match counted {
            Ok(position) => {
                *next_block = position;
                SLOT_COUNTED
            },
            Err(_) => SLOT_FAILED,
        };
        drop(work);
        slot.phase.store(phase, Ordering::Release);
        cell.is_active.store(self.rewrite.is_some(), Ordering::Release);
        if let Some(task) = state.planner_task.get() {
            task.wake();
        }
    }

    /// Take a rewrite a planner started in a seat, looking at the volumes in turn, and say whether there was one
    pub fn take_seat(&mut self) -> bool {
        let volumes = self.seats.len();
        for turn in 0..volumes {
            let at = (self.next_volume + turn) % volumes;
            for (index, seat) in self.seats[at].iter().enumerate() {
                if !seat.is_waiting.load(Ordering::SeqCst) || !seat.is_waiting.swap(false, Ordering::SeqCst) {
                    continue;
                }
                let mut held = hold(&seat.job);
                if !held.as_ref().is_some_and(RewriteJob::is_running) {
                    continue;
                }
                seat.heap.store(held.as_ref().map_or(0, |job| job.heap_bytes().0), Ordering::Release);
                self.rewrite = held.take();
                self.seat = (at, index);
                self.next_volume = (at + 1) % volumes;
                self.state.workers.cells[self.index].is_active.store(true, Ordering::Release);
                return true;
            }
        }
        false
    }

    /// Run one step of the worker's rewrite, and give it back to its seat and bring the planner back when it ends, for the next
    /// pick. The worker steps a rewrite it took out of its seat with no lock held
    pub fn step_rewrite(&mut self) -> JobStep {
        let Some(job) = &mut self.rewrite else {
            return JobStep::Idle;
        };
        job.step();
        if job.is_running() {
            return JobStep::Worked;
        }
        let (volume, seat) = self.seat;
        *hold(&self.seats[volume][seat].job) = self.rewrite.take();
        self.state.workers.cells[self.index].is_active.store(false, Ordering::Release);
        if let Some(task) = self.state.planner_task.get() {
            task.wake();
        }
        JobStep::Worked
    }
}

impl<Backend: Io> Job for WalkWorker<Backend> {
    fn step(&mut self) -> JobStep {
        // a handed-out block goes first, so walk units run between a rewrite's units
        if self.count_one() {
            return JobStep::Worked;
        }
        if self.rewrite.is_some() || self.take_seat() {
            return self.step_rewrite();
        }
        let state = Arc::clone(&self.state);
        if state.workers.look_or_idle(self.index, || self.count_one() || self.take_seat()) {
            return JobStep::Worked;
        }
        JobStep::Idle
    }
}

/// Add one row to a block's counts. A row with a replacement is dead, and its bytes move from live to dead. A backward walk
/// first adds every row's bytes as live, since it rebuilds them
pub fn add_count(
    counts: &mut FixedList<DeadCount>,
    segment: SegmentNumber,
    column: ColumnId,
    block: u32,
    row: &FooterRow,
    replacement: Option<WritePlace>,
) -> std::result::Result<(), ListFull> {
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
            })?;
            counts.len() - 1
        },
    };
    let count = &mut counts[index];
    let Some(replacement) = replacement else {
        count.kept_bytes = ByteCount(count.kept_bytes.0 + row.length.0);
        return Ok(());
    };
    count.replacement = count.replacement.max(replacement);
    count.rows += 1;
    count.graves += u32::from(row.kind == RowKind::Grave);
    count.bytes = ByteCount(count.bytes.0 + row.length.0);
    Ok(())
}

/// Read the most rows one of a segment's blocks holds, or the most any block can when the fact is missing
pub fn block_rows_bound(facts: &SegmentFacts) -> usize {
    if facts.most_block_rows > 0 { facts.most_block_rows as usize } else { MOST_BLOCK_ROWS }
}

impl<Backend: Io> WalkJob<Backend> {
    /// Note the walk waits on a rewrite's release, counting a wait once however many steps it takes
    pub fn note_claim_wait(&mut self, volume: &Volume<Backend>, now: Nanoseconds) {
        self.waiting_on = Some(volume.claims_changed.load(Ordering::Acquire));
        if self.claim_since.is_none() {
            self.claim_since = Some(now);
            volume.walk_claim_waits.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// End a wait on a release, and add the time it held the walk
    pub fn end_claim_wait(&mut self, volume: &Volume<Backend>, now: Nanoseconds) {
        self.waiting_on = None;
        let Some(since) = self.claim_since.take() else {
            return;
        };
        let held = now.0.saturating_sub(since.0);
        volume.walk_claim_held.fetch_add(held, Ordering::Relaxed);
        volume.walk_claim_longest.fetch_max(held, Ordering::Relaxed);
    }

    /// Check the walk waits on a release that hasn't come
    pub fn is_waiting(&self, volume: &Volume<Backend>) -> bool {
        self.waiting_on == Some(volume.claims_changed.load(Ordering::Acquire))
    }
}
