//! Compaction, a planner unit: first the cheapest reclaim there is, unlinking a segment with nothing live in it, then a
//! rewrite of a half-dead one
//!
//! A segment goes when its row block index shows no live row, and no live cue reads below the highest place that replaced
//! its rows. The summary never overstates dead, so that takes no lookups. Damage and covers keep a segment, since their
//! rows answer for more than themselves.

use std::cell::Cell;
use std::io::{Error, ErrorKind, Result};
use std::mem;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::maintenance_report::{JobKind, PickKind, UnitKind};
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, Nanoseconds, WritePlace};

use crate::config::MAX_KEY_BYTES;
use crate::engine::complete_path::{REFILL_STEP_BLOCKS, Refill};
use crate::engine::cover_apply::is_in_range;
use crate::engine::engine_state::EngineState;
use crate::engine::rewrite_job::{PLANNER_LANE, RewriteJob, RewriteSeat, is_reclaim_due};
use crate::engine::rebuild::MOST_REBUILD_TRIES;
use crate::engine::volume::{ParkedSegment, Volume};
use crate::engine::walk_job::{FREE_IN_PLACE_BYTES, FreeRecord, WALK_RETRY};
use crate::engine::reserve_job::RESERVE_PERIOD;
use crate::guard::hold;
use crate::io::device_error::is_device_error; use crate::io::job::JobStep;
use crate::io::{InPlace, Io, IoFile, in_place};
use crate::fixed_list::FixedList;
use crate::index::complete_table::{COMPLETE_SHARDS, CompleteTable, SWEEP_STEP_BUCKETS};
use crate::row_kind::RowKind;
use crate::segment::block_entry::KeyBound;
use crate::segment::footer_reader::FooterReader;
use crate::segment::range_covers::{Cover, MOST_RANGE_COVERS};
use crate::segment::row_block::{ROW_BLOCK_BYTES, RowBlockWalk};
use crate::segment::segment_table::RetiredSegment;
use crate::segment::segment_tail::{MOST_MERGE_SEGMENTS, SegmentNumber};
use crate::segment::summary::{DeadCount, SummaryRow};
use crate::segment::summary_orders::WalkList;

/// Covers one ask of compaction looks at, which bounds what settling them costs
pub const MOST_SETTLE_LOOKS: usize = 16;
/// Settled covers' rows that can wait for their segment's note
pub const MOST_SETTLED_ROWS: usize = 1_024;
/// Segments a look at a cover's list takes at a time
pub const MOST_LISTED_LOOKS: usize = 64;
/// How long compaction waits before it looks at a parked segment's pins again
pub const PARKED_RETRY: Nanoseconds = Nanoseconds(1_000_000);

/// A pass over a segment's large rows, for dead records no list could hold
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeldPass {
    pub segment: SegmentNumber,
    pub next_block: u32,
    pub is_whole: bool,
}

/// A pass that proves a candidate's rows dead before its unlink
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfirmPass {
    pub segment: SegmentNumber,
    pub next_block: u32,
}

/// What one step of a confirmation found
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowsProof {
    /// Every row of the block is dead
    Dead,
    /// No block is left, so every row is dead
    Finished,
    /// A row stands live, so the counts are wrong
    Live,
    /// A block or a row couldn't be read, so nothing is proven
    Unread,
    /// The segment left during the pass
    Gone,
}

/// What keeps a cover in the pool
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Blocker {
    /// Nothing does
    None,
    /// An open segment may hold a row it hides
    OpenRows,
    /// This sealed segment holds a row it hides
    Segment(SegmentNumber),
}

/// Segments a sweep step keeps its gone answers for
pub const SWEEP_SEEN: usize = 256;

/// A sweep's place in a complete table
#[derive(Clone, Copy)]
pub struct Sweep {
    /// The table's column
    pub column: ColumnId,
    /// The shard the next step clears
    pub shard: usize,
    /// The bucket the next step starts at
    pub from: usize,
    /// The stale count when the sweep began, which a whole pass clears
    pub counted: u64,
}

/// The compaction of one volume
pub struct CompactJob<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
    pub volume: usize,
    pub retry_at: Option<Nanoseconds>,
    pub rewrite: RewriteJob<Backend>,
    pub seats: Arc<Vec<RewriteSeat<Backend>>>,
    handed_at: u64,
    held_pass: Option<HeldPass>,
    confirm_pass: Option<ConfirmPass>,
    should_rewrite: bool,
    is_ask_held: bool,
    refill: Option<Refill>,
    refill_key: FixedList<u8>,
    refill_below: u64,
    refill_reader: FooterReader,
    sweep: Option<Sweep>,
    last_tried: SegmentNumber,
    settle_at: usize,
    settled_rows: FixedList<DeadCount>,
    listed: FixedList<SegmentNumber>,
    pub rebuilds_tried: FixedList<SegmentNumber>,
    block: FixedList<u8>,
    key: FixedList<u8>,
    row_key: FixedList<u8>,
    end: FixedList<u8>,
    unit: Option<(UnitKind, u64)>,
}

impl<Backend: Io> CompactJob<Backend> {
    /// Allocate the job's fixed buffers
    pub fn new(state: Arc<EngineState<Backend>>, volume: usize) -> Self {
        // a mutex's first lock can allocate, and this one's could otherwise come in a close
        drop(hold(&state.volumes[volume].parked));
        let workers = state.volumes[volume].walk_slots.len();
        let seats: Vec<RewriteSeat<Backend>> =
            (1..=workers).map(|lane| RewriteSeat::new(Arc::clone(&state), volume, lane)).collect();
        Self {
            rewrite: RewriteJob::new(Arc::clone(&state), volume, PLANNER_LANE),
            seats: Arc::new(seats),
            handed_at: 0,
            should_rewrite: false,
            is_ask_held: false,
            unit: None,
            held_pass: None,
            confirm_pass: None,
            state,
            volume,
            retry_at: None,
            last_tried: SegmentNumber(0),
            settle_at: 0,
            settled_rows: FixedList::on_heap(MOST_SETTLED_ROWS),
            listed: FixedList::on_heap(MOST_LISTED_LOOKS),
            rebuilds_tried: FixedList::on_heap(MOST_REBUILD_TRIES),
            block: FixedList::filled_on_heap(ROW_BLOCK_BYTES, 0),
            key: FixedList::on_heap(MAX_KEY_BYTES as usize),
            row_key: FixedList::on_heap(MAX_KEY_BYTES as usize),
            end: FixedList::on_heap(MAX_KEY_BYTES as usize + 1),
            refill: None,
            refill_key: FixedList::on_heap(MAX_KEY_BYTES as usize),
            refill_below: u64::MAX,
            sweep: None,
            refill_reader: FooterReader::new(),
        }
    }

    /// Read the heap the job's buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        let buffers = self.block.capacity() + self.key.capacity() + self.row_key.capacity() + self.end.capacity();
        let settled = self.settled_rows.capacity() * size_of::<DeadCount>() + self.listed.capacity() * size_of::<SegmentNumber>();
        let seats: u64 = self.seats.iter().map(RewriteSeat::heap_bytes).sum();
        ByteCount((buffers + settled) as u64 + self.rewrite.heap_bytes().0 + seats)
    }

    /// Step once: try the next segment the counts call dead, or nothing when none is
    pub fn step_unit(&mut self, now: Nanoseconds) -> JobStep {
        let started = self.state.io.now();
        self.unit = None;
        let step = match self.step_work(now) {
            JobStep::Idle if self.has_parked() => JobStep::IdleUntil { deadline: Nanoseconds(now.0 + PARKED_RETRY.0) },
            step => step,
        };
        // a rewrite's step notes its own unit, the same on the planner and on a worker
        if let Some((kind, work)) = self.unit.take() {
            self.state.note_unit(kind, work, started);
        }
        step
    }

    /// Count work into the unit under way
    pub fn add_work(&mut self, work: u64) {
        if let Some(unit) = &mut self.unit {
            unit.1 += work;
        }
    }

    /// Check a file still waits for its reads, so the job has to come back with nothing else to wake it. A failed or held
    /// volume does no work, and its files stay for the next open
    pub fn has_parked(&self) -> bool {
        let volume = &self.state.volumes[self.volume];
        let is_stepping = !volume.has_failed() && !volume.is_compaction_held();
        is_stepping && volume.segments_parked.load(Ordering::Acquire) > 0
    }

    /// Check a merge beside the walk is running, or a seal or a reclaim has asked for a look at the overlap
    pub fn has_overlap_work(&self) -> bool {
        let volume = &self.state.volumes[self.volume];
        self.rewrite.is_unwalked_merge() || self.should_rewrite || volume.wants_compaction.load(Ordering::Acquire)
    }

    /// Step once: a parked file no read holds any more, then the candidate work
    pub fn step_work(&mut self, now: Nanoseconds) -> JobStep {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        if volume.has_failed() || volume.is_compaction_held() {
            return JobStep::Idle;
        }
        if let Some(at) = self.retry_at.filter(|at| at.0 > now.0) {
            return JobStep::IdleUntil { deadline: at };
        }
        self.retry_at = None;
        if volume.stray_outputs.load(Ordering::Acquire) > 0 {
            self.rewrite.clear_stray();
            if volume.stray_outputs.load(Ordering::Acquire) > 0 {
                return self.wait_for_retry(now);
            }
        }
        // every lane parks a merge's inputs, so a step clears a merge's worth that are ready or the park becomes the queue
        let mut cleared = 0;
        while cleared < MOST_MERGE_SEGMENTS as u64 && let Some(parked) = self.take_parked(volume) {
            if let Err(parked) = state.clear_parked(volume, parked) {
                park_again(volume, parked);
                self.unit = Some((UnitKind::ParkedClear, cleared));
                return self.wait_for_retry(now);
            }
            cleared += 1;
        }
        if cleared > 0 {
            self.unit = Some((UnitKind::ParkedClear, cleared));
            return JobStep::Worked;
        }
        if self.step_refill() {
            return JobStep::Worked;
        }
        if self.step_sweep() {
            return JobStep::Worked;
        }
        // a live cue may still need what it kept, and its going brings the planner back
        if state.cues.is_empty() && volume.has_kept_covers.swap(false, Ordering::AcqRel) {
            self.unit = Some((UnitKind::CoverSweep, 0));
            self.sweep_covers(volume);
            return JobStep::Worked;
        }
        if state.cues.is_empty() && volume.has_held_frees.load(Ordering::Acquire) {
            self.unit = Some((UnitKind::HeldFrees, 0));
            return match self.step_held_pass(volume) {
                Ok(()) => JobStep::Worked,
                Err(_) => JobStep::IdleUntil { deadline: Nanoseconds(now.0 + WALK_RETRY.0) },
            };
        }
        if self.confirm_pass.is_some() {
            self.unit = Some((UnitKind::UnlinkCheck, 0));
            return match self.step_confirm(volume) {
                Ok(()) => JobStep::Worked,
                Err(_) => self.wait_for_retry(now),
            };
        }
        self.hand_out_on_change(volume);
        if self.rewrite.is_running() {
            self.rewrite.step();
            return JobStep::Worked;
        }
        self.unit = Some((UnitKind::Pick, 1));
        if self.start_rebuild(volume) {
            return JobStep::Worked;
        }
        let is_asked = volume.wants_compaction.swap(false, Ordering::AcqRel);
        if is_asked {
            let has_settled = self.settle_covers(volume);
            self.note_settled(volume);
            self.force_oldest(volume);
            // a settled cover asks again, and only a worked step brings the planner back to hear it
            if has_settled {
                return JobStep::Worked;
            }
        }
        // an ask outlives the unlink steps below, as it outlived them before
        self.should_rewrite |= is_asked;
        // a whole unlink wins back the most for the least, so each step tries one ahead of the rewrites, after the covers settle.
        // A segment a cue still holds starts no pass, and the rewrites go on
        if let Some(segment) = self.pick_dead(volume) {
            if self.confirm(volume, segment).is_err() {
                return self.wait_for_retry(now);
            }
            if self.confirm_pass.is_some() {
                return JobStep::Worked;
            }
        }
        // a quiet volume with dead bytes fills its idle time with reclaiming them, and one past the bound merges back first
        let is_short = volume.is_short_of_room();
        let is_deferring = volume.is_hot(now) && !is_short;
        let is_holding_records = volume.is_ingesting(now) && !is_short;
        let is_owed = !is_deferring && volume.is_over_bound.load(Ordering::Acquire);
        let is_quiet_with_dead = !is_holding_records && !volume.is_reclaim_held() && is_reclaim_due(&hold(&volume.summary));
        let mut should_rewrite = mem::take(&mut self.should_rewrite) || is_quiet_with_dead || is_owed;
        // an ask that arriving writes held back comes due once they stop
        if is_holding_records {
            self.is_ask_held |= should_rewrite;
        } else {
            should_rewrite |= mem::take(&mut self.is_ask_held);
        }
        // workers run every rewrite, so the planner stays free to pick and walk. A merge of 1 GiB segments would hold it for minutes
        if should_rewrite && (self.hand_out() || (self.seats.is_empty() && self.rewrite.start())) {
            return JobStep::Worked;
        }
        // a look takes a few segments at a time, and the rest still count. Hot ingest looks at none
        self.should_rewrite = should_rewrite && !is_deferring && self.rewrite.has_looks_left;
        if self.should_rewrite {
            return JobStep::Worked;
        }
        if volume.is_room_refused.load(Ordering::Acquire) || volume.is_room_short.load(Ordering::Acquire) {
            return JobStep::IdleUntil { deadline: Nanoseconds(now.0 + RESERVE_PERIOD.0) };
        }
        // what hot ingest or arriving writes put off starts once the window closes
        if is_deferring || is_holding_records {
            return JobStep::IdleUntil { deadline: volume.hot_until() };
        }
        // a planner with nothing to do saves the bits that changed, so a stop after a quiet spell keeps them all
        if volume.bits_saved_at.load(Ordering::Acquire) != 0 && volume.dead_bits.is_changed.swap(false, Ordering::AcqRel) {
            let _saved = self.state.save_dead_bits(volume);
        }
        JobStep::Idle
    }

    /// Clear one step's buckets of slots that point at gone segments, once enough may, and say whether a step ran
    pub fn step_sweep(&mut self) -> bool {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        if self.sweep.is_none() {
            let on_volume = |id: usize| state.columns[id].is_some_and(|info| info.volume.0 as usize == self.volume);
            let due = (0..state.columns.len()).filter(|id| on_volume(*id)).map(|id| ColumnId(id as u8));
            let mut due = due.filter(|column| state.complete_table(*column).is_some_and(CompleteTable::is_sweep_due));
            self.sweep = due.next().and_then(|column| {
                state.complete_table(column).map(|table| Sweep {
                    column,
                    shard: 0,
                    from: 0,
                    counted: table.stale_slots(),
                })
            });
        }
        let Some(mut sweep) = self.sweep else {
            return false;
        };
        let Some(table) = state.complete_table(sweep.column) else {
            self.sweep = None;
            return false;
        };
        self.unit = Some((UnitKind::SlotSweep, SWEEP_STEP_BUCKETS as u64));
        // a step meets a few hundred segments at most, so each one's answer is kept for the step
        let seen: [Cell<u64>; SWEEP_SEEN] = [const { Cell::new(0) }; SWEEP_SEEN];
        let is_gone = |segment: u32| {
            let cell = &seen[segment as usize % SWEEP_SEEN];
            let known = cell.get();
            if known >> 1 == u64::from(segment) + 1 {
                return known & 1 == 1;
            }
            let is_gone = state.is_slot_gone(volume, segment);
            cell.set((u64::from(segment) + 1) << 1 | u64::from(is_gone));
            is_gone
        };
        if table.sweep_step(sweep.shard, sweep.from, &is_gone) {
            sweep.shard += 1;
            sweep.from = 0;
        } else {
            sweep.from += SWEEP_STEP_BUCKETS;
        }
        if sweep.shard == COMPLETE_SHARDS {
            table.end_sweep(sweep.counted);
            self.sweep = None;
        } else {
            self.sweep = Some(sweep);
        }
        true
    }

    /// Give a few row blocks past a complete table's mark their slots once it has room again, and say whether a step ran
    pub fn step_refill(&mut self) -> bool {
        let state = Arc::clone(&self.state);
        if self.refill.is_none() {
            self.refill = state.start_refill(self.volume, self.refill_below);
        }
        let Some(mut refill) = self.refill else {
            return false;
        };
        self.unit = Some((UnitKind::SlotRefill, u64::from(REFILL_STEP_BLOCKS)));
        // a failed read ends the refill with the mark standing, and a later step starts it over
        let goes_on = state.refill_step(self.volume, &mut refill, (&mut self.refill_key, &mut self.refill_reader)).unwrap_or(false);
        self.refill = goes_on.then_some(refill);
        // a pass that left the mark standing waits for the walk to free more slots before it runs again
        if !goes_on {
            let table = state.complete_table(refill.column);
            self.refill_below = table.filter(|table| table.mark().is_some()).map_or(u64::MAX, |table| table.used_slots().0);
        }
        true
    }

    /// Look for work for idle workers once something has changed that could give one a pick
    pub fn hand_out_on_change(&mut self, volume: &Volume<Backend>) {
        let left = volume.segments_rewritten.load(Ordering::Acquire) + volume.segments_unlinked.load(Ordering::Acquire);
        let changes = left + volume.seals_done.load(Ordering::Acquire);
        if changes != self.handed_at {
            self.handed_at = changes;
            let _handed = self.hand_out();
        }
    }

    /// Start a rewrite in each idle seat while picks last, waking a worker for each, and say whether one started. The planner picks
    /// and a worker runs, so more workers means more rewrites at once
    pub fn hand_out(&self) -> bool {
        let mut has_started = false;
        for seat in self.seats.iter() {
            let mut held = hold(&seat.job);
            // a worker holds its running rewrite out of the seat, and one started here waits for the worker to take it
            let Some(job) = held.as_mut().filter(|job| !job.is_running()) else {
                continue;
            };
            if !job.claim() {
                return has_started;
            }
            has_started = true;
            drop(held);
            seat.is_waiting.store(true, Ordering::SeqCst);
            self.state.workers.wake_one();
        }
        has_started
    }

    /// Come back after the retry wait
    pub fn wait_for_retry(&mut self, now: Nanoseconds) -> JobStep {
        let at = Nanoseconds(now.0 + WALK_RETRY.0);
        self.retry_at = Some(at);
        JobStep::IdleUntil { deadline: at }
    }

    /// Take a parked segment no read holds any more
    pub fn take_parked(&self, volume: &Volume<Backend>) -> Option<ParkedSegment<Backend::File>> {
        let mut parked = hold(&volume.parked);
        let slot = parked.iter_mut().find(|slot| {
            slot.as_ref().is_some_and(|parked| volume.is_unpinned(parked))
        })?;
        slot.take()
    }

    /// Free one block's large dead records for the pass over a flagged segment, or end the pass with no block left. A row
    /// goes by its key's newest row, and a cue that came back keeps the flag for its going
    pub fn step_held_pass(&mut self, volume: &Volume<Backend>) -> Result<()> {
        let pass = match self.held_pass {
            Some(pass) => pass,
            None => {
                let Some(segment) = volume.table.next_held_frees() else {
                    volume.has_held_frees.store(false, Ordering::Release);
                    return Ok(());
                };
                HeldPass { segment, next_block: 0, is_whole: true }
            },
        };
        let (pin, block) = (volume.table.pin(pass.segment), volume.table.walk_block(pass.segment, pass.next_block));
        let (Some(pin), Some(block)) = (pin, block) else {
            volume.table.flag_held_frees(pass.segment, !pass.is_whole);
            self.held_pass = None;
            return Ok(());
        };
        let length = block.entry.block_length().0 as usize;
        if length > self.block.len() {
            return Err(Error::from(ErrorKind::InvalidData));
        }
        pin.file.read_exact_at(block.entry.block_at(), &mut self.block[..length])?;
        let Some(mut walk) = RowBlockWalk::open(&self.block[..length], pin.head.place_base) else {
            return Err(Error::from(ErrorKind::InvalidData));
        };
        let mut is_whole = pass.is_whole;
        let mut rows = 0;
        while let Some(row) = walk.next_row(&mut self.key) {
            rows += 1;
            if row.kind != RowKind::Value || row.length < FREE_IN_PLACE_BYTES {
                continue;
            }
            let newest = match self.state.newest_walked(volume, block.column, &self.key, &[pass.segment], None) {
                Ok(newest) => newest,
                Err(StoreError::Corruption { .. }) => None,
                Err(_error) => return Err(Error::from(ErrorKind::Interrupted)),
            };
            if let Some(found) = newest.filter(|found| found.row.place > row.place) {
                let free = FreeRecord {
                    segment: pass.segment,
                    offset: row.offset,
                    length: row.length,
                    place: row.place,
                    replacement: found.row.place,
                };
                is_whole &= self.state.free_record(volume, &free);
            }
        }
        self.held_pass = Some(HeldPass {
            next_block: pass.next_block + 1,
            is_whole,
            ..pass
        });
        self.add_work(rows);
        Ok(())
    }

    /// Drop covers a cue kept inside newer ones, once no cue reads below those. Each cover takes its own hold of the pool,
    /// so a get waits for one at most
    pub fn sweep_covers(&mut self, volume: &Volume<Backend>) {
        let mut position = 0;
        while let Some((column, place, segment)) = volume.covers.cover_at(position, &mut self.key, &mut self.end) {
            let cover = Cover {
                column,
                start: &self.key,
                end: &self.end,
                place,
                segment,
            };
            self.state.drop_covers_inside(volume, &cover);
            position += 1;
        }
    }

    /// Take covers out of the pool that nothing older sits under any more, a few each time compaction is asked, and say whether
    /// any went
    pub fn settle_covers(&mut self, volume: &Volume<Backend>) -> bool {
        let mut has_settled = false;
        let is_stuck = volume.segments_parked.load(Ordering::Acquire) > 0 || volume.stray_outputs.load(Ordering::Acquire) > 0;
        if volume.covers.is_empty() || is_stuck {
            return false;
        }
        for _look in 0..MOST_SETTLE_LOOKS {
            let is_applied = volume.covers.is_applied_at(self.settle_at);
            let Some((column, place, segment)) = volume.covers.cover_at(self.settle_at, &mut self.key, &mut self.end) else {
                self.settle_at = 0;
                return has_settled;
            };
            if !is_applied || self.blocker(volume, column, place, segment) != Blocker::None {
                self.settle_at += 1;
                continue;
            }
            // rows under it in its own segment leave with it, at that segment's rewrite
            if self.has_row_under(volume, segment, column, place) {
                if volume.covers.mark_clear(place) {
                    volume.wants_compaction.store(true, Ordering::Release);
                }
                self.settle_at += 1;
                continue;
            }
            let cover = Cover {
                column,
                start: &self.key,
                end: &self.end,
                place,
                segment,
            };
            volume.covers.remove(&cover);
            self.note_cover_row(volume, segment, column, place);
            has_settled = true;
            volume.wants_compaction.store(true, Ordering::Release);
        }
        has_settled
    }

    /// Find a settled cover's own row, for its count to land with the note that its segment's covers are done. A cover can settle
    /// again after a crash, and only the note says this count hasn't landed. The cover's start key is in `key`
    pub fn note_cover_row(&mut self, volume: &Volume<Backend>, segment: SegmentNumber, column: ColumnId, place: WritePlace) {
        let (Some(pin), Some((first, end))) = (volume.table.pin(segment), volume.table.blocks_from(segment, column, &self.key)) else {
            return;
        };
        for index in first..end {
            let Some(block) = volume.table.walk_block(segment, index) else {
                return;
            };
            let length = block.entry.block_length().0 as usize;
            if length > self.block.len() || pin.file.read_exact_at(block.entry.block_at(), &mut self.block[..length]).is_err() {
                return;
            }
            let Some(mut walk) = RowBlockWalk::open(&self.block[..length], pin.head.place_base) else {
                return;
            };
            while let Some(row) = walk.next_row(&mut self.row_key) {
                if row.place == place && row.kind == RowKind::RangeDelete {
                    // a list that's full leaves the row live, which only keeps its segment a while longer
                    self.settled_rows.push_if_room(DeadCount {
                        segment,
                        column,
                        block: index,
                        rows: 1,
                        replacement: place,
                        ..DeadCount::default()
                    });
                    return;
                }
            }
        }
    }

    /// Mark the segments whose covers have all left the pool, so open stops loading them. Each cover has to rest on a sealed row
    /// first, since one dropped inside a newer cover rests on that one
    pub fn note_settled(&mut self, volume: &Volume<Backend>) {
        if !volume.covers.are_all_applied() {
            return;
        }
        let mut rows = self.state.hold_for_pick(self.volume, PickKind::Cover);
        let mut last = SegmentNumber(0);
        loop {
            let is_settled = |row: &SummaryRow<'_>| {
                row.facts.has_covers && row.facts.is_walked() && !volume.covers.holds_any(row.facts.segment)
            };
            let after = rows.orders().after(last).iter().filter_map(|segment| rows.get(*segment));
            let Some(segment) = after.filter(is_settled).map(|row| row.facts.segment).next() else {
                return;
            };
            rows.clear_covers(segment);
            for count in self.settled_rows.iter().filter(|count| count.segment == segment) {
                rows.apply_counts(segment, slice::from_ref(count), None);
                volume.table.drop_live(segment, count.block, count.rows, count.replacement);
            }
            self.settled_rows.retain(|count| count.segment != segment);
            last = segment;
        }
    }

    /// Find the first segment that keeps a cover in the pool: an open row or a sealed one it still hides. A cue needs no check of
    /// its own, since the rows a cue below the cover reads stay while it lives, and those keep the cover here
    pub fn blocker(&mut self, volume: &Volume<Backend>, column: ColumnId, place: WritePlace, segment: SegmentNumber) -> Blocker {
        if hold(&volume.claims).open.iter().any(|open| open.row_count.0 > 0 && open.lowest_place < place) {
            return Blocker::OpenRows;
        }
        let mut last = SegmentNumber(0);
        loop {
            let rows = self.state.hold_for_pick(self.volume, PickKind::Cover);
            let is_under = |row: &SummaryRow<'_>| {
                row.facts.segment != segment
                    && row.facts.places.lowest < place
                    && is_in_range(row, column, &self.key, &self.end)
            };
            let after = rows.orders().after(last).iter().filter_map(|segment| rows.get(*segment));
            let next = after.filter(is_under).map(|row| (row.facts.segment, row.facts.is_damaged)).next();
            drop(rows);
            let Some((other, is_damaged)) = next else {
                return self.unrowed_blocker(volume, column, place, segment);
            };
            if is_damaged || self.has_row_under(volume, other, column, place) {
                return Blocker::Segment(other);
            }
            last = other;
        }
    }

    /// Find a listed segment with no summary row that holds a row under a cover. A row given up for room leaves the summary
    /// blind to its segment, and the lists still have it
    pub fn unrowed_blocker(&mut self, volume: &Volume<Backend>, column: ColumnId, place: WritePlace, segment: SegmentNumber) -> Blocker {
        let state = Arc::clone(&self.state);
        let Some(directory) = &state.directories[column.0 as usize] else {
            return Blocker::None;
        };
        let (low, high) = (KeyBound::of_key(&self.key), KeyBound::of_key(&self.end));
        let mut skip = 0;
        loop {
            let mut listed = mem::take(&mut self.listed);
            let page = directory.overlapping(&low, &high, skip, &mut listed);
            skip += listed.len();
            let has_no_row = |other: &SegmentNumber| *other != segment && hold(&volume.summary).get(*other).is_none();
            let found = listed.iter().copied().filter(has_no_row).find(|other| self.has_row_under(volume, *other, column, place));
            self.listed = listed;
            if let Some(other) = found {
                return Blocker::Segment(other);
            }
            if !page.is_cut {
                return Blocker::None;
            }
        }
    }

    /// Check a segment holds a row older than a place inside the range in `key` and `end`. A block that won't read counts as one
    pub fn has_row_under(&mut self, volume: &Volume<Backend>, other: SegmentNumber, column: ColumnId, place: WritePlace) -> bool {
        let (Some(pin), Some((first, end))) = (volume.table.pin(other), volume.table.blocks_from(other, column, &self.key)) else {
            return false;
        };
        for index in first..end {
            let Some(block) = volume.table.walk_block(other, index) else {
                return true;
            };
            let length = block.entry.block_length().0 as usize;
            if length > self.block.len() || pin.file.read_exact_at(block.entry.block_at(), &mut self.block[..length]).is_err() {
                return true;
            }
            let Some(mut walk) = RowBlockWalk::open(&self.block[..length], pin.head.place_base) else {
                return true;
            };
            while let Some(row) = walk.next_row(&mut self.row_key) {
                if self.row_key >= self.end {
                    return false;
                }
                if self.row_key >= self.key && row.place < place {
                    return true;
                }
            }
        }
        false
    }

    /// Past half of the pool, send the oldest cover's first sealed row to a rewrite, whatever the trigger says
    pub fn force_oldest(&mut self, volume: &Volume<Backend>) {
        if volume.covers.len() * 2 <= MOST_RANGE_COVERS {
            return;
        }
        let Some((column, place, segment)) = volume.covers.oldest_applied(&mut self.key, &mut self.end) else {
            return;
        };
        if let Blocker::Segment(other) = self.blocker(volume, column, place, segment) {
            self.rewrite.forced = Some(other);
            self.should_rewrite = true;
        }
    }

    /// Pick the next segment past the last one tried whose counts say nothing in it is live
    pub fn pick_dead(&self, volume: &Volume<Backend>) -> Option<SegmentNumber> {
        if volume.is_applying.load(Ordering::Acquire) {
            return None;
        }
        let rows = self.state.hold_for_pick(self.volume, PickKind::Dead);
        let is_proven = |segment: &SegmentNumber| !volume.table.is_unproven(*segment);
        // the dead list holds the wholly dead rows in segment order
        let dead = rows.orders().walk_list(WalkList::Dead);
        let (mut after, mut all) = (dead.filter(|segment| *segment > self.last_tried), dead);
        after.find(is_proven).or_else(|| all.find(is_proven))
    }

    /// Check a candidate's counts and start the pass that proves its rows dead. A cue that stands in the way brings the planner
    /// back as it goes, and counts that haven't landed bring it back with the walk that lands them
    pub fn confirm(&mut self, volume: &Volume<Backend>, segment: SegmentNumber) -> Result<()> {
        self.last_tried = segment;
        let Some(highest_replacement) = volume.table.dead_replacement(segment) else {
            return Ok(());
        };
        if self.state.cues.holds_back(self.volume, highest_replacement) {
            return Ok(());
        }
        if !volume.can_park(1) || self.state.read_point(volume) < highest_replacement {
            return Ok(());
        }
        // a cover decides a segment wholly inside it, and no count does
        if is_under_applied_cover(volume, segment) {
            volume.wants_compaction.store(true, Ordering::Release);
            return self.unlink(volume, segment, highest_replacement);
        }
        self.confirm_pass = Some(ConfirmPass { segment, next_block: 0 });
        Ok(())
    }

    /// Prove one more block of a candidate's rows dead, and unlink it once none is left. Counts alone never unlink a segment,
    /// so a wrong count costs its space and keeps its rows
    pub fn step_confirm(&mut self, volume: &Volume<Backend>) -> Result<()> {
        let Some(pass) = self.confirm_pass else {
            return Ok(());
        };
        let proof = self.prove_block(volume, pass)?;
        if proof == RowsProof::Dead {
            self.confirm_pass = Some(ConfirmPass { next_block: pass.next_block + 1, ..pass });
            return Ok(());
        }
        self.confirm_pass = None;
        volume.wants_compaction.store(true, Ordering::Release);
        match proof {
            RowsProof::Finished => self.finish_confirm(volume, pass.segment),
            RowsProof::Live => {
                self.state.unlinks_refused.fetch_add(1, Ordering::Relaxed);
                volume.table.flag_unproven(pass.segment);
                Ok(())
            },
            RowsProof::Unread => {
                volume.table.flag_unproven(pass.segment);
                Ok(())
            },
            RowsProof::Dead | RowsProof::Gone => Ok(()),
        }
    }

    /// Unlink a segment whose rows are all proven dead, if its counts still say so
    pub fn finish_confirm(&self, volume: &Volume<Backend>, segment: SegmentNumber) -> Result<()> {
        if volume.is_applying.load(Ordering::Acquire) {
            return Ok(());
        }
        let Some(highest_replacement) = volume.table.dead_replacement(segment) else {
            return Ok(());
        };
        if self.state.cues.holds_back(self.volume, highest_replacement) {
            return Ok(());
        }
        self.unlink(volume, segment, highest_replacement)
    }

    /// Check every row of a block has a newer version in some sealed segment or lies under an applied cover, or sits in a
    /// damaged range, where a rotted replacement can't show itself
    pub fn prove_block(&mut self, volume: &Volume<Backend>, pass: ConfirmPass) -> Result<RowsProof> {
        let Some(pin) = volume.table.pin(pass.segment) else {
            return Ok(RowsProof::Gone);
        };
        let Some(block) = volume.table.walk_block(pass.segment, pass.next_block) else {
            return Ok(RowsProof::Finished);
        };
        let length = block.entry.block_length().0 as usize;
        if length > self.block.len() {
            return Ok(RowsProof::Unread);
        }
        match pin.file.read_exact_at(block.entry.block_at(), &mut self.block[..length]) {
            Ok(()) => {},
            Err(error) if is_device_error(&error) => return Ok(RowsProof::Unread),
            Err(error) => return Err(error),
        }
        let Some(mut walk) = RowBlockWalk::open(&self.block[..length], pin.head.place_base) else {
            return Ok(RowsProof::Unread);
        };
        let mut rows = 0;
        while let Some(row) = walk.next_row(&mut self.key) {
            // a settled cover's row counts dead with nothing above it
            rows += 1;
            if row.kind == RowKind::RangeDelete {
                continue;
            }
            let Ok(read) = self.state.key_read(block.column, &self.key, WritePlace(u64::MAX), false) else {
                return Ok(RowsProof::Unread);
            };
            match in_place(self.state.search_footers(&InPlace, volume, &read)) {
                Ok(newest) => {
                    let is_replaced = newest.is_some_and(|found| found.row.place > row.place);
                    let is_covered = volume.covers.newest_applied(block.column, &self.key).is_some_and(|cover| cover > row.place);
                    // damage only answers for a row it could be newer than
                    let is_hidden = volume.damage.covering(block.column, &self.key).is_some_and(|highest| highest > row.place);
                    if !is_replaced && !is_covered && !is_hidden {
                        return Ok(RowsProof::Live);
                    }
                },
                Err(StoreError::Corruption { .. }) => return Ok(RowsProof::Unread),
                Err(_error) => return Err(Error::from(ErrorKind::Interrupted)),
            }
        }
        self.add_work(rows);
        Ok(RowsProof::Dead)
    }

    /// Take the segment out of every list and save that, once its replacements are published and no cue reads below them,
    /// then park it. The file goes at a later step, once no read holds it, so a slow read never holds up the planner. A
    /// crash before it goes leaves a segment with no row, which open walks backward and finds dead again
    pub fn unlink(&self, volume: &Volume<Backend>, segment: SegmentNumber, highest_replacement: WritePlace) -> Result<()> {
        let state = &self.state;
        // a rewrite that claimed it may keep a row the cover hides, and its output would bring that row back
        if volume.is_taken(segment) || !volume.can_park(1) || state.read_point(volume) < highest_replacement {
            return Ok(());
        }
        state.cues.note_reclaim();
        if state.cues.has_cue_below(self.volume, highest_replacement) {
            return Ok(());
        }
        for directory in state.directories.iter().flatten() {
            if !directory.replace(None, &[segment]) {
                return Err(Error::from(ErrorKind::OutOfMemory));
            }
        }
        self.state.filter_arena.drop_segment(segment);
        volume.dead_bits.end(segment);
        let Some(retired) = volume.table.retire(segment) else {
            return Ok(());
        };
        let mut rows = hold(&volume.summary);
        let length = rows.get(segment).map_or(ByteCount(0), |row| row.facts.file_length);
        rows.remove(segment);
        drop(rows);
        state.note_moved(JobKind::Compaction, length);
        // the file goes only behind a saved summary, or open would find a row with no file
        let saved = state.save_summary(volume);
        park(volume, Some(retired), segment, saved.is_ok());
        saved?;
        volume.segments_unlinked.fetch_add(1, Ordering::AcqRel);
        volume.unlinked_bytes.fetch_add(length.0, Ordering::Relaxed);
        Ok(())
    }
}

/// Put back a parked segment whose file wouldn't go
pub fn park_again<Backend: Io>(volume: &Volume<Backend>, parked: ParkedSegment<Backend::File>) {
    if let Some(slot) = hold(&volume.parked).iter_mut().find(|slot| slot.is_none()) {
        *slot = Some(parked);
    }
}

/// Hold a retired segment until no read holds it. The caller has checked `Volume::can_park`, and one planner fills the park
pub fn park<Backend: Io>(
    volume: &Volume<Backend>,
    retired: Option<RetiredSegment<Backend::File>>,
    segment: SegmentNumber,
    is_saved: bool,
) {
    if let Some(slot) = hold(&volume.parked).iter_mut().find(|slot| slot.is_none()) {
        *slot = Some(ParkedSegment { retired, segment, is_saved });
        volume.segments_parked.fetch_add(1, Ordering::AcqRel);
    }
}

/// Check every column of a segment lies wholly inside an applied cover above its rows. A summary's range can only be wider
/// than the truth
pub fn is_under_applied_cover<Backend: Io>(volume: &Volume<Backend>, segment: SegmentNumber) -> bool {
    let rows = hold(&volume.summary);
    let Some(row) = rows.get(segment) else {
        return false;
    };
    let covers = volume.covers.read();
    let mut cursor = row.cursor();
    (0..row.column_count).all(|_column| {
        cursor.column().is_some_and(|facts| {
            let place = row.facts.places.highest;
            let cover = Cover { column: facts.column, start: facts.lowest, end: facts.bound, place, segment };
            !facts.bound.is_empty() && covers.is_inside_newer(&cover, true)
        })
    })
}
