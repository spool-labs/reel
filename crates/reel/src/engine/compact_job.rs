//! Compaction, a planner unit: first the cheapest reclaim there is, unlinking a segment with nothing live in it
//!
//! Counts only say where to look. A segment goes when its own footer shows that every row has a newer version in a
//! finished segment that every live cue can see. Damage and covers keep a segment, since their rows answer for more than
//! themselves.

use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, Nanoseconds, WritePlace};

use crate::config::MAX_KEY_BYTES;
use crate::engine::engine_state::EngineState;
use crate::engine::open_segment::data_path;
use crate::engine::planner::UnitStep;
use crate::engine::volume::Volume;
use crate::engine::walk_job::WALK_RETRY;
use crate::guard::hold;
use crate::io::device_error::is_device_error;
use crate::io::{Io, IoFile};
use crate::pool::{touched_capacity, touched_zeroes};
use crate::row_kind::RowKind;
use crate::segment::range_covers::Cover;
use crate::segment::row_block::{ROW_BLOCK_BYTES, RowBlockWalk};
use crate::segment::segment_table::{SegmentPin, WalkBlock};
use crate::segment::segment_tail::SegmentNumber;
use crate::segment::summary::SummaryRow;

/// A segment being checked for anything live
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeadTarget {
    pub segment: SegmentNumber,
    pub next_block: u32,
    pub block_count: u32,
    pub highest_replacement: WritePlace,
}

/// The compaction of one volume
pub struct CompactJob<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
    pub volume: usize,
    pub target: Option<DeadTarget>,
    pub retry_at: Option<Nanoseconds>,
    last_tried: SegmentNumber,
    block: Vec<u8>,
    key: Vec<u8>,
    end: Vec<u8>,
}

impl<Backend: Io> CompactJob<Backend> {
    /// Allocate the job's fixed buffers
    pub fn new(state: Arc<EngineState<Backend>>, volume: usize) -> Self {
        Self {
            state,
            volume,
            target: None,
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

    /// Step once: a row block of a candidate's footer, or nothing when no segment looks dead
    pub fn step_unit(&mut self, now: Nanoseconds) -> UnitStep {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        if volume.has_failed() || volume.is_compaction_held.load(Ordering::Acquire) {
            return UnitStep::Idle;
        }
        if let Some(at) = self.retry_at.filter(|at| at.0 > now.0) {
            return UnitStep::RetryAt(at);
        }
        self.retry_at = None;
        // a live cue may still need what it kept, and its going brings the planner back
        if state.cues.is_empty() && volume.has_kept_covers.swap(false, Ordering::AcqRel) {
            self.sweep_covers(volume);
            return UnitStep::Worked;
        }
        if self.target.is_none() && !self.pick_dead(volume) {
            return UnitStep::Idle;
        }
        let Some(target) = self.target else {
            return UnitStep::Idle;
        };
        match self.confirm_block(volume, target) {
            Ok(()) => UnitStep::Worked,
            Err(_) => {
                let at = Nanoseconds(now.0 + WALK_RETRY.0);
                self.retry_at = Some(at);
                UnitStep::RetryAt(at)
            },
        }
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
    pub fn pick_dead(&mut self, volume: &Volume<Backend>) -> bool {
        if !volume.wants_compaction.swap(false, Ordering::AcqRel) {
            return false;
        }
        let rows = hold(&volume.summary);
        let dead = rows.rows().filter(is_wholly_dead);
        let after = dead.clone().filter(|row| row.facts.segment > self.last_tried).min_by_key(|row| row.facts.segment);
        let picked = after.or_else(|| dead.min_by_key(|row| row.facts.segment));
        self.target = picked.map(|row| DeadTarget {
            segment: row.facts.segment,
            next_block: 0,
            block_count: row.facts.block_count,
            highest_replacement: WritePlace(0),
        });
        self.target.is_some()
    }

    /// Check one row block, and unlink the segment once every block has passed
    pub fn confirm_block(&mut self, volume: &Volume<Backend>, target: DeadTarget) -> Result<()> {
        let replaced = match (volume.table.pin(target.segment), volume.table.walk_block(target.segment, target.next_block)) {
            (Some(pin), Some(block)) => self.block_replacement(volume, &pin, &block, target)?,
            _no_block => None,
        };
        let Some(highest) = replaced else {
            // the counts understate, so the next finished walk looks again
            self.last_tried = target.segment;
            self.target = None;
            return Ok(());
        };
        let target = DeadTarget {
            next_block: target.next_block + 1,
            highest_replacement: target.highest_replacement.max(highest),
            ..target
        };
        if target.next_block < target.block_count {
            self.target = Some(target);
            return Ok(());
        }

        self.last_tried = target.segment;
        self.target = None;
        volume.wants_compaction.store(true, Ordering::Release);
        self.unlink(volume, target.segment, target.highest_replacement)
    }

    /// Find the highest place that replaces a block's rows, or nothing when a row lacks a replacement every cue can see
    pub fn block_replacement(
        &mut self,
        volume: &Volume<Backend>,
        pin: &SegmentPin<'_, Backend::File>,
        block: &WalkBlock,
        target: DeadTarget,
    ) -> Result<Option<WritePlace>> {
        let mut highest = WritePlace(0);
        let length = block.entry.block_length().0 as usize;
        if length > self.block.len() {
            return Ok(None);
        }
        match pin.file.read_exact_at(block.entry.block_at(), &mut self.block[..length]) {
            Ok(()) => {},
            // a block no read can give back answers for nothing, and the segment stays for damage to deal with
            Err(error) if is_device_error(&error) => return Ok(None),
            Err(error) => return Err(error),
        }
        let Some(mut walk) = RowBlockWalk::open(&self.block[..length], pin.head.place_base) else {
            return Ok(None);
        };

        while let Some(row) = walk.next_row(&mut self.key) {
            if row.kind == RowKind::RangeDelete {
                return Ok(None);
            }
            let newest = match self.state.newest_walked(volume, block.column, &self.key, target.segment) {
                Ok(newest) => newest,
                Err(StoreError::Corruption { .. }) => return Ok(None),
                Err(_error) => return Err(Error::from(ErrorKind::Interrupted)),
            };
            let is_replaced = newest.is_some_and(|found| found.row.place > row.place);
            // a cue below the newer version still reads this one, whatever the counts say
            let is_seen = newest.is_some_and(|found| self.state.cues.holds_back(found.row.place));
            let Some(found) = newest.filter(|_found| is_replaced && !is_seen) else {
                return Ok(None);
            };
            highest = highest.max(found.row.place);
        }
        Ok(Some(highest))
    }

    /// Take the segment out of every list, save that, and remove its file, once its replacements are published and no cue
    /// reads below them. A crash before the file goes leaves a segment with no row, which open walks backward and finds
    /// dead again
    pub fn unlink(&self, volume: &Volume<Backend>, segment: SegmentNumber, highest_replacement: WritePlace) -> Result<()> {
        let state = &self.state;
        if state.read_point() < highest_replacement {
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
        while retired.pin_count() > 0 {
            // a stop leaves the file and its handle, as a crash here does
            if state.is_stopping.load(Ordering::Acquire) {
                return saved;
            }
            state.io.pause();
        }
        state.io.close_file(retired.file.clone());
        saved?;
        volume.segments_unlinked.fetch_add(1, Ordering::AcqRel);
        for root in &volume.roots {
            match state.io.remove_file(&data_path(root, segment)) {
                Ok(()) => return state.io.sync_directory(root),
                Err(error) if error.kind() == ErrorKind::NotFound => {},
                Err(error) => return Err(error),
            }
        }
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
