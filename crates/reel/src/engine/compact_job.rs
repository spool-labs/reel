//! Compaction, a planner unit: first the cheapest reclaim there is, unlinking a segment with nothing live in it
//!
//! A segment goes when its row block index shows no live row, and no live cue reads below the highest place that replaced
//! its rows. The summary never overstates dead, so that takes no lookups. Damage and covers keep a segment, since their
//! rows answer for more than themselves.

use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, Nanoseconds, WritePlace};

use crate::config::MAX_KEY_BYTES;
use crate::engine::engine_state::EngineState;
use crate::engine::planner::UnitStep;
use crate::engine::volume::{ParkedSegment, Volume};
use crate::engine::walk_job::WALK_RETRY;
use crate::guard::hold;
use crate::io::device_error::is_device_error;
use crate::io::{Io, IoFile};
use crate::pool::{touched_capacity, touched_zeroes};
use crate::row_kind::RowKind;
use crate::segment::range_covers::Cover;
use crate::segment::row_block::{ROW_BLOCK_BYTES, RowBlockWalk};
use crate::segment::segment_tail::SegmentNumber;
use crate::segment::summary::SummaryRow;

/// How long compaction waits before it looks at a parked segment's pins again
pub const PARKED_RETRY: Nanoseconds = Nanoseconds(1_000_000);

/// The compaction of one volume
pub struct CompactJob<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
    pub volume: usize,
    pub retry_at: Option<Nanoseconds>,
    last_tried: SegmentNumber,
    block: Vec<u8>,
    key: Vec<u8>,
    end: Vec<u8>,
}

impl<Backend: Io> CompactJob<Backend> {
    /// Allocate the job's fixed buffers
    pub fn new(state: Arc<EngineState<Backend>>, volume: usize) -> Self {
        // a mutex's first lock can allocate, and this one's could otherwise come in a close
        drop(hold(&state.volumes[volume].parked));
        Self {
            state,
            volume,
            retry_at: None,
            last_tried: SegmentNumber(0),
            block: touched_zeroes(ROW_BLOCK_BYTES),
            key: touched_capacity(MAX_KEY_BYTES as usize, 0),
            end: touched_capacity(MAX_KEY_BYTES as usize + 1, 0),
        }
    }

    /// Read the heap the job's buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        ByteCount((self.block.capacity() + self.key.capacity() + self.end.capacity()) as u64)
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
        if let Some(parked) = self.take_parked(volume) {
            return match state.clear_parked(volume, parked) {
                Ok(()) => UnitStep::Worked,
                Err(_) => UnitStep::RetryAt(Nanoseconds(now.0 + WALK_RETRY.0)),
            };
        }
        // a live cue may still need what it kept, and its going brings the planner back
        if state.cues.is_empty() && volume.has_kept_covers.swap(false, Ordering::AcqRel) {
            self.sweep_covers(volume);
            return UnitStep::Worked;
        }
        if !volume.wants_compaction.swap(false, Ordering::AcqRel) {
            return UnitStep::Idle;
        }
        let Some(segment) = self.pick_dead(volume) else {
            return UnitStep::Idle;
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

    /// Take a parked segment no read holds any more
    pub fn take_parked(&self, volume: &Volume<Backend>) -> Option<ParkedSegment<Backend::File>> {
        let mut parked = hold(&volume.parked);
        let slot = parked.iter_mut().find(|slot| {
            slot.as_ref().is_some_and(|parked| volume.table.retired_pins(&parked.retired) == 0)
        })?;
        slot.take()
    }

    /// Check the volume has room to park another segment
    pub fn can_park(&self, volume: &Volume<Backend>) -> bool {
        hold(&volume.parked).iter().any(Option::is_none)
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

    /// Pick the next segment past the last one tried whose counts say nothing in it is live
    pub fn pick_dead(&self, volume: &Volume<Backend>) -> Option<SegmentNumber> {
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
                let Ok(read) = self.state.key_read(block.column, &self.key, WritePlace(u64::MAX), false) else {
                    return Ok(());
                };
                match self.state.search_footers(volume, &read) {
                    Ok(newest) => {
                        let is_replaced = newest.is_some_and(|found| found.row.place > row.place);
                        // damage only answers for a row it could be newer than
                        let is_hidden = volume.damage.covering(block.column, &self.key).is_some_and(|highest| highest > row.place);
                        debug_assert!(is_replaced || is_hidden, "the counts call a live row dead");
                        debug_assert!(row.kind != RowKind::RangeDelete, "the counts call a range delete dead");
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
        if !self.can_park(volume) || state.read_point() < highest_replacement {
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
        hold(&volume.summary).remove(segment);
        // the file goes only behind a saved summary, or open would find a row with no file
        let saved = state.save_summary(volume);
        let parked = ParkedSegment {
            retired,
            segment,
            is_saved: saved.is_ok(),
        };
        if let Some(slot) = hold(&volume.parked).iter_mut().find(|slot| slot.is_none()) {
            *slot = Some(parked);
            volume.segments_parked.fetch_add(1, Ordering::AcqRel);
        }
        saved?;
        volume.segments_unlinked.fetch_add(1, Ordering::AcqRel);
        Ok(())
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
