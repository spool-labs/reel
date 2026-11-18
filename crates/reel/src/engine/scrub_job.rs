//! The scrub: a pass over every sealed segment that checks each record's sum and record check at a set rate, so a record that
//! rotted on the disk shows as damage before a read needs it. It reads with plain reads past the record cache, and paces
//! itself, so foreground reads keep their share of the device

use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_core::column_id::ColumnId;
use reel_core::units::{Nanoseconds, WritePlace};

use crate::engine::engine_state::EngineState;
use crate::engine::read_path::newest_of;
use crate::engine::walk_job::FREE_IN_PLACE_BYTES;
use crate::guard::hold;
use crate::io::job::JobStep;
use crate::io::{InPlace, Io, in_place};
use crate::record::record_check::RecordIdentity;
use crate::record::record_reader::{RecordLocation, RecordRead};
use crate::row_kind::RowKind;
use crate::segment::row_block::RowBlockWalk;
use crate::segment::segment_tail::SegmentNumber;

/// The rate a store scrubs at when its config says nothing else, in bytes a second
pub const DEFAULT_SCRUB_BYTES_PER_SECOND: u64 = 4 * 1024 * 1024;
/// How long a scrub rests between passes
pub const SCRUB_PASS_REST: Nanoseconds = Nanoseconds(3_600_000_000_000);
/// The most a scrub that stood idle may check in one go, in seconds of its rate
const MOST_BANKED_SECONDS: u64 = 1;
const NANOS_PER_SECOND: u64 = 1_000_000_000;

/// One volume's scrub: where its pass stands, and what its rate has earned
pub struct ScrubJob<Backend: Io> {
    state: Arc<EngineState<Backend>>,
    volume: usize,
    /// Bytes a second the pass checks, the store's rate unless a caller sets its own
    pub rate: u64,
    /// The segment the pass stands in, and its next row block
    segment: Option<SegmentNumber>,
    block: u32,
    /// Bytes the rate lets the pass check, which a block's records can take below zero, and when it last earned
    allowance: i64,
    earned_at: Nanoseconds,
    /// A pass ended, and the next waits until then
    rest_until: Nanoseconds,
    block_bytes: Vec<u8>,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl<Backend: Io> ScrubJob<Backend> {
    /// Start a volume's scrub at its first segment
    pub fn new(state: Arc<EngineState<Backend>>, volume: usize) -> Self {
        Self {
            rate: state.scrub_rate,
            state,
            volume,
            segment: None,
            block: 0,
            allowance: 0,
            earned_at: Nanoseconds(0),
            rest_until: Nanoseconds(0),
            block_bytes: Vec::new(),
            key: Vec::new(),
            value: Vec::new(),
        }
    }

    /// Check one row block's records while the rate allows, or say when it allows the next
    pub fn step_unit(&mut self, now: Nanoseconds) -> JobStep {
        let rate = self.rate;
        if rate == 0 || self.state.is_read_only {
            return JobStep::Idle;
        }
        if now < self.rest_until {
            return JobStep::IdleUntil { deadline: self.rest_until };
        }
        self.earn(now, rate);
        if self.allowance <= 0 {
            let wait = (self.allowance.unsigned_abs() + 1).saturating_mul(NANOS_PER_SECOND) / rate;
            return JobStep::IdleUntil { deadline: Nanoseconds(now.0.saturating_add(wait.max(1))) };
        }
        let Some(segment) = self.segment.or_else(|| self.next_segment(None)) else {
            self.end_pass(now);
            return JobStep::IdleUntil { deadline: self.rest_until };
        };
        self.segment = Some(segment);
        if !self.check_block(segment) {
            self.block = 0;
            self.segment = self.next_segment(Some(segment));
            if self.segment.is_none() {
                self.end_pass(now);
            }
        }
        JobStep::Worked
    }

    /// Earn what the rate gives for the time since the last step, up to what a short idle banks
    fn earn(&mut self, now: Nanoseconds, rate: u64) {
        if self.earned_at.0 == 0 {
            self.earned_at = now;
            self.allowance = rate as i64;
            return;
        }
        let elapsed = now.0.saturating_sub(self.earned_at.0);
        let earned = (u128::from(elapsed) * u128::from(rate) / u128::from(NANOS_PER_SECOND)) as i64;
        let most = rate.saturating_mul(MOST_BANKED_SECONDS) as i64;
        self.allowance = self.allowance.saturating_add(earned).min(most);
        self.earned_at = now;
    }

    /// Count a finished pass and rest until the next
    fn end_pass(&mut self, now: Nanoseconds) {
        self.segment = None;
        self.block = 0;
        self.state.scrub_passes.fetch_add(1, Ordering::Relaxed);
        self.rest_until = Nanoseconds(now.0.saturating_add(SCRUB_PASS_REST.0));
    }

    /// Find the lowest sealed segment past one, or the lowest of all
    fn next_segment(&self, after: Option<SegmentNumber>) -> Option<SegmentNumber> {
        let summary = hold(&self.state.volumes[self.volume].summary);
        let numbers = summary.rows().map(|row| row.facts.segment);
        numbers.filter(|number| after.is_none_or(|after| *number > after)).min()
    }

    /// Check the records of a segment's next row block, and say whether the segment has more. A segment that left or failed
    /// its check has none
    fn check_block(&mut self, segment: SegmentNumber) -> bool {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        let Some(pin) = volume.table.pin(segment) else {
            return false;
        };
        let Some(line) = volume.table.walk_block(segment, self.block) else {
            return false;
        };
        self.block += 1;
        let Ok(info) = state.column(line.column) else {
            return true;
        };
        let bytes = &mut self.block_bytes;
        let loaded = in_place(state.visit_block(&InPlace, volume, line.column, segment, &line.entry, |block| {
            bytes.clear();
            bytes.extend_from_slice(block);
        }));
        if !matches!(loaded, Ok(Some(()))) {
            return false;
        }
        let Some(mut walk) = RowBlockWalk::open_checked(&self.block_bytes, pin.head.place_base) else {
            return false;
        };
        let counts = state.cues.counts();
        let mut checked = 0;
        while let Some(row) = walk.next_row(&mut self.key) {
            if row.kind != RowKind::Value {
                continue;
            }
            let identity = RecordIdentity { record_key: state.record_key, column: line.column, key: &self.key, kind: row.kind, place: row.place };
            let location = RecordLocation { offset: row.offset, length: row.length };
            let mut reader = state.record_reader_for(&InPlace);
            let read = in_place(reader.read_value(&InPlace, &pin.file, location, &identity, info.codec, &mut self.value));
            drop(reader);
            checked += row.length.0;
            if matches!(read, Ok(RecordRead::Valid)) {
                continue;
            }
            // a free punches only large dead records, and moves the counts first, so a failure there is no damage
            let is_large = row.length >= FREE_IN_PLACE_BYTES;
            if is_large && (!state.has_outlived(counts, true) || !self.is_newest(line.column, row.place)) {
                continue;
            }
            state.scrub_failures.fetch_add(1, Ordering::Relaxed);
            state.damage_unread(self.volume, segment);
            self.note_checked(checked);
            return false;
        }
        self.note_checked(checked);
        true
    }

    /// Charge the bytes a block's records took
    fn note_checked(&mut self, bytes: u64) {
        self.state.scrub_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.allowance = self.allowance.saturating_sub(bytes as i64);
    }

    /// Check a row is still its key's newest, since a record a free may punch is a dead one
    fn is_newest(&self, column: ColumnId, place: WritePlace) -> bool {
        let volume = &self.state.volumes[self.volume];
        let Ok(read) = self.state.key_read(column, &self.key, self.state.read_point(volume), false) else {
            return false;
        };
        let found = self.state.index.lookup(column, &self.key, read.read_point, volume.view());
        let Ok(sealed) = in_place(self.state.search_sealed(&InPlace, volume, &read)) else {
            return false;
        };
        newest_of(found.row, sealed).place() == Some(place)
    }
}
