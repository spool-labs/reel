//! Compaction, a planner unit: first the cheapest reclaim there is, unlinking a segment with nothing live in it, then a
//! rewrite of a half-dead one
//!
//! A segment goes when its row block index shows no live row, and no live cue reads below the highest place that replaced
//! its rows. The summary never overstates dead, so that takes no lookups. Damage and covers keep a segment, since their
//! rows answer for more than themselves.

use std::io::{Error, ErrorKind, Result};
use std::mem;
use std::slice;
use std::sync::{Arc, Mutex};
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::maintenance_report::JobKind;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, Nanoseconds, WritePlace};

use crate::config::MAX_KEY_BYTES;
use crate::engine::cover_apply::is_in_range;
use crate::engine::engine_state::EngineState;
use crate::engine::planner::UnitStep;
use crate::engine::rewrite_job::{RewriteJob, RewriteSeat};
use crate::engine::volume::{ParkedSegment, Volume};
use crate::engine::walk_job::{FREE_IN_PLACE_BYTES, FreeRecord, WALK_RETRY};
use crate::guard::hold;
use crate::io::device_error::is_device_error;
use crate::io::{Io, IoFile, IoTask};
use crate::pool::{touched_capacity, touched_zeroes};
use crate::row_kind::RowKind;
use crate::segment::block_entry::KeyBound;
use crate::segment::range_covers::{Cover, MOST_RANGE_COVERS};
use crate::segment::row_block::{ROW_BLOCK_BYTES, RowBlockWalk};
use crate::segment::segment_table::RetiredSegment;
use crate::segment::segment_tail::SegmentNumber;
use crate::segment::summary::{DeadCount, SummaryRow};

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

/// The compaction of one volume
pub struct CompactJob<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
    pub volume: usize,
    pub retry_at: Option<Nanoseconds>,
    pub rewrite: RewriteJob<Backend>,
    /// One rewrite for each of the volume's workers
    pub seats: Arc<Vec<RewriteSeat<Backend>>>,
    handed_at: u64,
    held_pass: Option<HeldPass>,
    should_rewrite: bool,
    last_tried: SegmentNumber,
    settle_at: usize,
    settled_rows: Vec<DeadCount>,
    listed: Vec<SegmentNumber>,
    block: Vec<u8>,
    key: Vec<u8>,
    row_key: Vec<u8>,
    end: Vec<u8>,
}

impl<Backend: Io> CompactJob<Backend> {
    /// Allocate the job's fixed buffers
    pub fn new(state: Arc<EngineState<Backend>>, volume: usize) -> Self {
        // a mutex's first lock can allocate, and this one's could otherwise come in a close
        drop(hold(&state.volumes[volume].parked));
        let workers = state.volumes[volume].walk_slots.len();
        let seats: Vec<RewriteSeat<Backend>> =
            (1..=workers).map(|lane| RewriteSeat { job: Mutex::new(RewriteJob::new(Arc::clone(&state), volume, lane)) }).collect();
        Self {
            rewrite: RewriteJob::new(Arc::clone(&state), volume, 0),
            seats: Arc::new(seats),
            handed_at: 0,
            should_rewrite: false,
            held_pass: None,
            state,
            volume,
            retry_at: None,
            last_tried: SegmentNumber(0),
            settle_at: 0,
            settled_rows: touched_capacity(MOST_SETTLED_ROWS, DeadCount::default()),
            listed: touched_capacity(MOST_LISTED_LOOKS, SegmentNumber(0)),
            block: touched_zeroes(ROW_BLOCK_BYTES),
            key: touched_capacity(MAX_KEY_BYTES as usize, 0),
            row_key: touched_capacity(MAX_KEY_BYTES as usize, 0),
            end: touched_capacity(MAX_KEY_BYTES as usize + 1, 0),
        }
    }

    /// Read the heap the job's buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        let buffers = self.block.capacity() + self.key.capacity() + self.row_key.capacity() + self.end.capacity();
        let settled = self.settled_rows.capacity() * size_of::<DeadCount>() + self.listed.capacity() * size_of::<SegmentNumber>();
        let seats: u64 = self.seats.iter().map(|seat| hold(&seat.job).heap_bytes().0).sum();
        ByteCount((buffers + settled) as u64 + self.rewrite.heap_bytes().0 + seats)
    }

    /// Step once: try the next segment the counts call dead, or nothing when none is
    pub fn step_unit(&mut self, now: Nanoseconds) -> UnitStep {
        match self.step_work(now) {
            UnitStep::Idle if self.has_parked() => UnitStep::RetryAt(Nanoseconds(now.0 + PARKED_RETRY.0)),
            step => step,
        }
    }

    /// Check a file still waits for its reads, so the job has to come back with nothing else to wake it. A failed or held
    /// volume does no work, and its files stay for the next open
    pub fn has_parked(&self) -> bool {
        let volume = &self.state.volumes[self.volume];
        let is_stepping = !volume.has_failed() && !volume.is_compaction_held.load(Ordering::Acquire);
        is_stepping && volume.segments_parked.load(Ordering::Acquire) > 0
    }

    /// Check a merge beside the walk is running, or a seal or a reclaim has asked for a look at the overlap
    pub fn has_overlap_work(&self) -> bool {
        let volume = &self.state.volumes[self.volume];
        self.rewrite.is_unwalked_merge() || self.should_rewrite || volume.wants_compaction.load(Ordering::Acquire)
    }

    /// Step once: a parked file no read holds any more, then the candidate work
    pub fn step_work(&mut self, now: Nanoseconds) -> UnitStep {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        if volume.has_failed() || volume.is_compaction_held.load(Ordering::Acquire) {
            return UnitStep::Idle;
        }
        if let Some(at) = self.retry_at.filter(|at| at.0 > now.0) {
            return UnitStep::RetryAt(at);
        }
        self.retry_at = None;
        if volume.stray_outputs.load(Ordering::Acquire) > 0 {
            self.rewrite.clear_stray();
            if volume.stray_outputs.load(Ordering::Acquire) > 0 {
                return self.wait_for_retry(now);
            }
        }
        if let Some(parked) = self.take_parked(volume) {
            return match state.clear_parked(volume, parked) {
                Ok(()) => UnitStep::Worked,
                Err(parked) => {
                    park_again(volume, parked);
                    self.wait_for_retry(now)
                },
            };
        }
        // a live cue may still need what it kept, and its going brings the planner back
        if state.cues.is_empty() && volume.has_kept_covers.swap(false, Ordering::AcqRel) {
            self.sweep_covers(volume);
            return UnitStep::Worked;
        }
        if state.cues.is_empty() && volume.has_held_frees.load(Ordering::Acquire) {
            return match self.step_held_pass(volume) {
                Ok(()) => UnitStep::Worked,
                Err(_) => UnitStep::RetryAt(Nanoseconds(now.0 + WALK_RETRY.0)),
            };
        }
        self.hand_out_on_change(volume);
        if self.rewrite.is_running() {
            self.rewrite.step();
            return UnitStep::Worked;
        }
        let is_asked = volume.wants_compaction.swap(false, Ordering::AcqRel);
        if is_asked {
            let has_settled = self.settle_covers(volume);
            self.note_settled(volume);
            self.force_oldest(volume);
            // a settled cover asks again, and only a worked step brings the planner back to hear it
            if has_settled {
                return UnitStep::Worked;
            }
        }
        self.should_rewrite |= is_asked;
        let candidate = if is_asked { self.pick_dead(volume) } else { None };
        let Some(segment) = candidate else {
            // an unlink wins back the most for the least, so a rewrite waits until none is left to try
            let should_rewrite = mem::take(&mut self.should_rewrite);
            // workers go first, so the planner stays free to pick and walk
            if should_rewrite && (self.hand_out(volume) || self.rewrite.start()) {
                return UnitStep::Worked;
            }
            // a look takes a few segments at a time, and the rest still count
            self.should_rewrite = should_rewrite && self.rewrite.has_looks_left;
            return if self.should_rewrite { UnitStep::Worked } else { UnitStep::Idle };
        };
        match self.confirm(volume, segment) {
            Ok(()) => UnitStep::Worked,
            Err(_) => {
                let at = Nanoseconds(now.0 + WALK_RETRY.0);
                self.retry_at = Some(at);
                UnitStep::RetryAt(at)
            },
        }
    }

    /// Look for work for idle workers once something has changed that could give one a pick
    pub fn hand_out_on_change(&mut self, volume: &Volume<Backend>) {
        let left = volume.segments_rewritten.load(Ordering::Acquire) + volume.segments_unlinked.load(Ordering::Acquire);
        let changes = left + volume.seals_done.load(Ordering::Acquire);
        if changes != self.handed_at {
            self.handed_at = changes;
            let _handed = self.hand_out(volume);
        }
    }

    /// Start a rewrite on each idle worker while picks last, and say whether one started. The planner picks and the worker runs,
    /// so more workers means more rewrites at once
    pub fn hand_out(&self, volume: &Volume<Backend>) -> bool {
        let mut has_started = false;
        for (index, seat) in self.seats.iter().enumerate() {
            let mut job = hold(&seat.job);
            if job.is_running() {
                continue;
            }
            if !job.start() {
                return has_started;
            }
            has_started = true;
            drop(job);
            if let Some(task) = volume.walk_slots[index].task.get() {
                task.wake();
            }
        }
        has_started
    }

    /// Come back after the retry wait
    pub fn wait_for_retry(&mut self, now: Nanoseconds) -> UnitStep {
        let at = Nanoseconds(now.0 + WALK_RETRY.0);
        self.retry_at = Some(at);
        UnitStep::RetryAt(at)
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
        while let Some(row) = walk.next_row(&mut self.key) {
            if row.kind != RowKind::Value || row.length < FREE_IN_PLACE_BYTES {
                continue;
            }
            let newest = match self.state.newest_walked(volume, block.column, &self.key, &[pass.segment]) {
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
                    if self.settled_rows.len() < self.settled_rows.capacity() {
                        self.settled_rows.push(DeadCount {
                            segment,
                            column,
                            block: index,
                            rows: 1,
                            replacement: place,
                            ..DeadCount::default()
                        });
                    }
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
        let mut rows = hold(&volume.summary);
        let mut last = SegmentNumber(0);
        loop {
            let is_settled = |row: &SummaryRow<'_>| {
                row.facts.has_covers && row.facts.segment > last && row.facts.is_walked() && !volume.covers.holds_any(row.facts.segment)
            };
            let Some(segment) = rows.rows().filter(is_settled).map(|row| row.facts.segment).min() else {
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
            let rows = hold(&volume.summary);
            let is_under = |row: &SummaryRow<'_>| {
                row.facts.segment > last
                    && row.facts.segment != segment
                    && row.facts.places.lowest < place
                    && is_in_range(row, column, &self.key, &self.end)
            };
            let next = rows.rows().filter(is_under).min_by_key(|row| row.facts.segment).map(|row| (row.facts.segment, row.facts.is_damaged));
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
        let rows = hold(&volume.summary);
        let dead = rows.rows().filter(is_wholly_dead);
        let after = dead.clone().filter(|row| row.facts.segment > self.last_tried).min_by_key(|row| row.facts.segment);
        let picked = after.or_else(|| dead.min_by_key(|row| row.facts.segment));
        picked.map(|row| row.facts.segment)
    }

    /// Confirm a candidate from its block counts and unlink it. A cue that stands in the way brings the planner back as it
    /// goes, and counts that haven't landed bring it back with the walk that lands them
    pub fn confirm(&mut self, volume: &Volume<Backend>, segment: SegmentNumber) -> Result<()> {
        self.last_tried = segment;
        let Some(highest_replacement) = volume.table.dead_replacement(segment) else {
            return Ok(());
        };
        if self.state.cues.holds_back(highest_replacement) {
            return Ok(());
        }
        if cfg!(debug_assertions) {
            self.check_rows(volume, segment)?;
        }
        volume.wants_compaction.store(true, Ordering::Release);
        self.unlink(volume, segment, highest_replacement)
    }

    /// Check every row of a segment the counts call dead has a newer version in some sealed segment, or sits in a damaged
    /// range, where a rotted replacement can't show itself. Only a debug build comes here, so the seed sweep proves each
    /// unlink row by row
    pub fn check_rows(&mut self, volume: &Volume<Backend>, segment: SegmentNumber) -> Result<()> {
        let Some(pin) = volume.table.pin(segment) else {
            return Ok(());
        };
        let mut index = 0;
        while let Some(block) = volume.table.walk_block(segment, index) {
            index += 1;
            let length = block.entry.block_length().0 as usize;
            if length > self.block.len() {
                return Ok(());
            }
            match pin.file.read_exact_at(block.entry.block_at(), &mut self.block[..length]) {
                Ok(()) => {},
                // a block no read can give back has nothing to check against
                Err(error) if is_device_error(&error) => return Ok(()),
                Err(error) => return Err(error),
            }
            let Some(mut walk) = RowBlockWalk::open(&self.block[..length], pin.head.place_base) else {
                return Ok(());
            };
            while let Some(row) = walk.next_row(&mut self.key) {
                // a settled cover's row counts dead with nothing above it
                if row.kind == RowKind::RangeDelete {
                    continue;
                }
                let Ok(read) = self.state.key_read(block.column, &self.key, WritePlace(u64::MAX), false) else {
                    return Ok(());
                };
                match self.state.search_footers(volume, &read) {
                    Ok(newest) => {
                        let is_replaced = newest.is_some_and(|found| found.row.place > row.place);
                        let is_covered = volume.covers.newest_over(block.column, &self.key, WritePlace(u64::MAX)).is_some_and(|cover| cover > row.place);
                        // damage only answers for a row it could be newer than
                        let is_hidden = volume.damage.covering(block.column, &self.key).is_some_and(|highest| highest > row.place);
                        debug_assert!(is_replaced || is_covered || is_hidden, "the counts call a live row dead");
                    },
                    Err(StoreError::Corruption { .. }) => return Ok(()),
                    Err(_error) => return Err(Error::from(ErrorKind::Interrupted)),
                }
            }
        }
        Ok(())
    }

    /// Take the segment out of every list and save that, once its replacements are published and no cue reads below them,
    /// then park it. The file goes at a later step, once no read holds it, so a slow read never holds up the planner. A
    /// crash before it goes leaves a segment with no row, which open walks backward and finds dead again
    pub fn unlink(&self, volume: &Volume<Backend>, segment: SegmentNumber, highest_replacement: WritePlace) -> Result<()> {
        let state = &self.state;
        if !volume.can_park(1) || state.read_point() < highest_replacement {
            return Ok(());
        }
        state.cues.note_reclaim();
        if state.cues.has_cue_below(highest_replacement) {
            return Ok(());
        }
        for directory in state.directories.iter().flatten() {
            if !directory.replace(None, &[segment]) {
                return Err(Error::from(ErrorKind::OutOfMemory));
            }
        }
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

/// Check a row's counts say nothing in its segment is live. Rows with damage or covers answer for more than their rows
pub fn is_wholly_dead(row: &SummaryRow<'_>) -> bool {
    if !row.facts.is_walked() || row.facts.is_damaged || row.facts.has_covers || row.facts.block_count == 0 {
        return false;
    }
    let mut cursor = row.cursor();
    (0..row.column_count).all(|_column| cursor.column().is_some_and(|column| column.dead_rows == column.row_count))
}
