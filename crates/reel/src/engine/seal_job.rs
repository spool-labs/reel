//! Seal: an open segment's footer, the rows it gives the spot cache on the way, and the one atomic switch to sealed

use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use reel_api::column_id::ColumnId;
use reel_api::maintenance_report::JobKind;
use reel_api::units::{ByteCount, FileOffset, Nanoseconds, RootId, RowCount, WritePlace};

use crate::pool::touched_capacity;
use crate::config::MAX_KEY_BYTES;
use crate::engine::engine_sizing::{rows_per_second, seal_point};
use crate::engine::engine_state::EngineState;
use crate::engine::open_segment::{SegmentPhase, rows_path};
use crate::engine::volume::{NO_SEAL, SealStepTimes, Volume};
use crate::guard::hold;
use crate::lifetime::MarkRange;
use crate::index::key_hash::{KeyHash, hash_key};
use crate::index::open_slots::OpenSlot;
use crate::index::seal_walk::SealWalk;
use crate::index::spot_index::SpotLocation;
use crate::io::job::{Job, JobStep};
use crate::io::{InPlace, Io, IoTask, in_place};
use crate::io::device_error::is_device_error;
use crate::record::codec::Codec;
use crate::record::record_check::RecordIdentity;
use crate::record::record_reader::{RecordLocation, RecordRead};
use crate::row_kind::RowKind;
use crate::segment::column_directory::Listing;
use crate::segment::footer_writer::{ColumnStart, FooterTotals, FooterWriter};
use crate::segment::range_covers::{Cover, put_cover};
use crate::segment::row_block::FooterRow;
use crate::segment::segment_tail::{BlockCount, SegmentNumber};
use crate::segment::damage_list::{highest_of, spans_of};
use crate::segment::summary::SegmentFacts;

/// Rows one step of a seal handles
pub const SEAL_STEP_ROWS: u64 = 1_024;
/// How long a seal waits before it tries a failed step again
pub const SEAL_RETRY: Nanoseconds = Nanoseconds(100_000_000);
/// How long a seal sleeps while writers land or the mark catches up
pub const SEAL_WAIT: Nanoseconds = Nanoseconds(100_000);
/// Row blocks one segment can have
pub const MOST_SEGMENT_BLOCKS: BlockCount = BlockCount(64 * 1024);

/// The segment a seal is working on
#[derive(Clone, Debug)]
pub struct SealTarget<File> {
    pub number: SegmentNumber,
    pub slot: OpenSlot,
    pub root: RootId,
    pub data: File,
    pub rows: File,
    pub data_end: FileOffset,
    pub row_count: RowCount,
    pub lowest_place: WritePlace,
    pub highest_place: WritePlace,
    pub marks: MarkRange,
}

/// How far a seal has gone
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SealPhase {
    /// No seal is running
    Idle,
    /// Writers of the segment are landing
    Draining,
    /// Rows are going into the footer
    Writing,
    /// The footer is durable, and the segment goes into the table
    Installing,
    /// The table has the segment, and its summary row goes to disk
    Summarizing,
    /// The table has the segment, and the rows file goes
    DiscardingRows,
    /// The switch has happened, and the write index lets go
    Cleaning,
}

/// A volume's seal job
pub struct SealJob<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
    pub volume: usize,
    pub phase: SealPhase,
    pub target: Option<SealTarget<Backend::File>>,
    pub writer: FooterWriter,
    pub walk: SealWalk,
    pub column: Option<ColumnId>,
    pub highest: Vec<u8>,
    pub ranges: Vec<u8>,
    pub has_unsaved_damage: bool,
    pub range_end: Vec<u8>,
    pub totals: Option<FooterTotals>,
    pub last_key: Vec<u8>,
    pub last_slot: Option<(KeyHash, SegmentNumber, u16)>,
    pub seal_started: Nanoseconds,
    pub step_started: Nanoseconds,
    pub rows_walked: u64,
    pub rows_cleaned: u64,
    pub last_column: Option<ColumnId>,
    pub is_in_table: bool,
    pub columns_listed: usize,
    pub is_summarized: bool,
}

impl<Backend: Io> SealJob<Backend> {
    /// Build a job with its buffers
    pub fn new(state: Arc<EngineState<Backend>>, volume: usize) -> Self {
        let ranges_room = state.volumes[volume].covers.footer_room();
        Self {
            state,
            volume,
            phase: SealPhase::Idle,
            target: None,
            writer: FooterWriter::new(MOST_SEGMENT_BLOCKS),
            walk: SealWalk::new(),
            column: None,
            highest: touched_capacity(MAX_KEY_BYTES as usize, 0),
            ranges: touched_capacity(ranges_room, 0),
            has_unsaved_damage: false,
            range_end: touched_capacity(MAX_KEY_BYTES as usize, 0),
            totals: None,
            last_key: touched_capacity(MAX_KEY_BYTES as usize, 0),
            last_slot: None,
            seal_started: Nanoseconds(0),
            step_started: Nanoseconds(0),
            rows_walked: 0,
            rows_cleaned: 0,
            last_column: None,
            is_in_table: false,
            columns_listed: 0,
            is_summarized: false,
        }
    }

    /// Read the job's volume
    pub fn volume(&self) -> &Volume<Backend> {
        &self.state.volumes[self.volume]
    }

    /// Pick a segment marked for sealing
    pub fn pick_target(&mut self) -> bool {
        let volume = &self.state.volumes[self.volume];
        let claims = hold(&volume.claims);
        for segment in &claims.open {
            if segment.phase == SegmentPhase::Sealing {
                volume.sealing.store(segment.number.0, Ordering::Release);
                let now = self.state.io.now();
                if self.seal_started.0 > 0 {
                    let interval = now.0.saturating_sub(self.seal_started.0);
                    volume.ingest_rate.store(rows_per_second(segment.row_count, interval), Ordering::Relaxed);
                }
                // the period runs from the first row to the last, so idle gaps on either side read quiet: one before the first row,
                // and one an idle seal waited out after the last. A recovered segment has none
                let last_write = if segment.quiet_since.0 > 0 { segment.quiet_since } else { now };
                let period = segment.first_row_at.map_or(0, |first| last_write.0.saturating_sub(first.0).max(1));
                volume.seal_interval.store(period, Ordering::Relaxed);
                volume.seal_started_at.store(last_write.0, Ordering::Relaxed);
                self.seal_started = now;
                self.step_started = now;
                self.rows_walked = 0;
                self.rows_cleaned = 0;
                let held = self.state.rows_held();
                let free = RowCount(self.state.index_capacity().0.saturating_sub(held.0));
                volume.ration.start(segment.row_count, held, free, now);
                self.target = Some(SealTarget {
                    number: segment.number,
                    slot: segment.slot,
                    root: segment.root,
                    data: segment.data.clone(),
                    rows: segment.rows.clone(),
                    data_end: segment.write_end,
                    row_count: segment.row_count,
                    lowest_place: segment.lowest_place,
                    highest_place: segment.highest_place,
                    marks: segment.marks,
                });
                return true;
            }
        }
        false
    }

    /// Note how long a step ran, and start the clock on the next
    pub fn end_step(&mut self, step: impl Fn(&SealStepTimes) -> &AtomicU64) {
        let now = self.state.io.now();
        let elapsed = now.0.saturating_sub(self.step_started.0);
        step(&self.volume().seal_steps).fetch_max(elapsed, Ordering::Relaxed);
        self.step_started = now;
    }

    /// End the seal in progress, and bring the planner back for the walk its segment waited on
    pub fn end_seal(&mut self) {
        if let Some(target) = &self.target {
            let elapsed = self.state.io.now().0.saturating_sub(self.seal_started.0);
            let volume = self.volume();
            let seal_rate = rows_per_second(target.row_count, elapsed);
            volume.seal_rate.store(seal_rate, Ordering::Relaxed);
            let point = seal_point(volume.segment_rows, seal_rate, volume.ingest_rate.load(Ordering::Relaxed));
            volume.seal_point.store(point.0, Ordering::Relaxed);
        }
        if let Some(target) = &self.target {
            let volume = self.volume();
            volume.rows_in_index.fetch_sub(target.row_count.0, Ordering::Relaxed);
            volume.ration.clear();
        }
        self.target = None;
        self.phase = SealPhase::Idle;
        self.volume().sealing.store(NO_SEAL, Ordering::Release);
        self.volume().seals_done.fetch_add(1, Ordering::AcqRel);
        // each seal can take a column's overlap past the bound
        self.volume().wants_compaction.store(true, Ordering::Release);
        self.volume().is_over_bound.store(true, Ordering::Release);
        if let Some(task) = self.state.planner_task.get() {
            task.wake();
        }
    }

    /// Say the seal needs a reclaim's room, and bring the planner back, which would otherwise stand aside for this seal
    pub fn note_stuck(&self) {
        if !self.volume().is_seal_stuck.swap(true, Ordering::AcqRel) {
            self.volume().seal_stops.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(task) = self.state.planner_task.get() {
            task.wake();
        }
    }

    /// Check the footer walk can start: the writers have landed, and the mark has passed the segment, so every lower place is in
    /// the index and a read that starts from here has a point at or above the segment's rows
    pub fn is_ready(&self, target: &SealTarget<Backend::File>) -> bool {
        self.is_drained(target.slot) && self.volume().order.mark() >= target.highest_place
    }

    /// Check the target's writers have all landed
    pub fn is_drained(&self, slot: OpenSlot) -> bool {
        let claims = hold(&self.volume().claims);
        for segment in &claims.open {
            if segment.slot == slot {
                return segment.in_flight == 0;
            }
        }
        true
    }

    /// Send the target's last rows out and start its footer
    pub fn start_writing(&mut self, target: &SealTarget<Backend::File>) -> Result<()> {
        self.end_step(|steps| &steps.drain);
        self.state.write_out(target.slot)?;
        self.writer.start(target.data_end, target.lowest_place);
        self.writer.set_marks(target.marks);
        // a read that began before this count can't fill the spot cache behind the walk
        self.volume().handovers.fetch_add(1, Ordering::AcqRel);
        self.last_column = None;
        self.walk.start(&self.state.index, target.slot);
        self.column = None;
        self.ranges.clear();
        self.is_in_table = false;
        self.columns_listed = 0;
        self.is_summarized = false;
        self.phase = SealPhase::Writing;
        Ok(())
    }

    /// Put a step's rows into the footer, and finish it when they run out
    pub fn write_rows(&mut self, target: &SealTarget<Backend::File>) -> Result<()> {
        self.end_step(|steps| &steps.walk_step);
        self.volume().ration.note_walked(RowCount(self.rows_walked));
        self.rows_walked += SEAL_STEP_ROWS;
        for _row in 0..SEAL_STEP_ROWS {
            let Some(shard) = self.walk.next_shard() else {
                return self.finish_footer(target);
            };

            let head = self.walk.head(shard);
            let Some(row) = head.row else {
                return Err(Error::other("write index row has an unknown kind"));
            };
            if self.column != Some(head.column) {
                self.state.index.highest_key(target.slot, head.column, &mut self.highest);
                let start = ColumnStart {
                    column: head.column,
                    lowest: head.key,
                    highest: &self.highest,
                    block_bytes: self.state.block_bytes_of(head.column),
                };
                self.writer.start_column(&target.data, &start)?;
                self.column = Some(head.column);
            }

            // the row says what the pool may have dropped: a newer cover in another segment can be lost without this one
            if row.kind == RowKind::RangeDelete {
                let identity = RecordIdentity {
                    record_key: self.state.record_key,
                    column: head.column,
                    key: head.key,
                    kind: RowKind::RangeDelete,
                    place: row.place,
                };
                let location = RecordLocation {
                    offset: row.offset,
                    length: row.length,
                };
                let end = &mut self.range_end;
                let read = self.state.record_readers.with(|reader| {
                    in_place(reader.read_value(&InPlace, &target.data, location, &identity, Codec::None, end))
                });
                let is_whole = match read {
                    Ok(read) => read == RecordRead::Valid,
                    Err(error) if is_device_error(&error) => false,
                    Err(error) => return Err(error),
                };
                if !is_whole {
                    self.state.damage_whole(self.volume, target.number);
                    self.has_unsaved_damage = true;
                    self.walk.advance(&self.state.index, shard);
                    continue;
                }
                let cover = Cover {
                    column: head.column,
                    start: head.key,
                    end,
                    place: row.place,
                    segment: target.number,
                };
                if !self.state.volumes[self.volume].covers.has_newer_in_segment(&cover) {
                    put_cover(&mut self.ranges, &cover);
                }
            }

            let footer_row = FooterRow {
                place: row.place,
                offset: row.offset,
                length: row.length,
                kind: row.kind,
            };
            self.writer.push_row(&target.data, head.key, &footer_row)?;
            self.last_slot = self.give_slot(target.number, head.column, head.key, row.kind);
            let is_new_key = self.last_column != Some(head.column) || self.last_key != head.key;
            if is_new_key {
                self.last_column = Some(head.column);
                self.last_key.clear();
                self.last_key.extend_from_slice(head.key);
                // a complete column has its slots already, and reads no spot cache
                if self.state.complete_table(head.column).is_none() {
                    let hash = hash_key(head.column, head.key);
                    if row.kind == RowKind::Value && head.is_newest {
                        let location = SpotLocation {
                            segment: target.number,
                            offset: row.offset,
                            length: row.length,
                            place: row.place,
                        };
                        self.state.spot.insert_newer(hash, &location);
                    } else {
                        self.state.spot.remove_older(hash, row.place);
                    }
                }
            }
            self.walk.advance(&self.state.index, shard);
        }
        Ok(())
    }

    /// Give a row a slot in its column's table, in the block it just went into
    pub fn give_slot(&self, number: SegmentNumber, column: ColumnId, key: &[u8], kind: RowKind) -> Option<(KeyHash, SegmentNumber, u16)> {
        let Some(table) = self.state.complete_table(column).filter(|_table| kind != RowKind::RangeDelete) else {
            return self.last_slot;
        };
        let volume = &self.state.volumes[self.volume];
        let block = self.writer.entries().len() as u16;
        // a key's versions in one block share a slot, and two keys of a tag never do
        let hash = hash_key(column, key);
        if self.last_slot != Some((hash, number, block)) {
            table.insert(hash, number, block, |segment| self.state.is_slot_gone(volume, segment));
        }
        Some((hash, number, block))
    }

    /// Finish the footer, which syncs it, then discard the rows file
    pub fn finish_footer(&mut self, target: &SealTarget<Backend::File>) -> Result<()> {
        self.end_step(|steps| &steps.walk_step);
        self.volume().ration.note_walk_end(self.step_started);
        let state = Arc::clone(&self.state);
        // the tail ends the segment's rows, so what they can no longer say has to be on disk first, and a failed save
        // holds the seal here
        if self.has_unsaved_damage {
            state.save_summary(&state.volumes[self.volume])?;
            self.has_unsaved_damage = false;
        }
        let totals = self.writer.finish(&target.data, target.number, target.data_end, &self.ranges, state.record_key, &[])?;
        self.totals = Some(totals);
        self.end_step(|steps| &steps.footer);
        self.phase = SealPhase::Installing;
        Ok(())
    }

    /// Add the segment's summary row and save the summary, once however often this retries
    pub fn summarize(&mut self, target: &SealTarget<Backend::File>) -> Result<()> {
        let Some(totals) = self.totals else {
            return Err(Error::other("seal summarizing with no footer"));
        };
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        if !self.is_summarized {
            let mut facts = SegmentFacts {
                segment: target.number,
                file_length: totals.file_length,
                data_end: target.data_end,
                row_block_end: totals.head.index_at,
                seal_mark: totals.head.index_sum,
                places: totals.head.places,
                marks: totals.head.marks,
                is_damaged: false,
                has_covers: totals.head.ranges_length.0 > 0,
                damage_place: WritePlace(0),
                block_count: self.writer.entries().len() as u32,
                walked_blocks: 0,
                is_backward: false,
                has_lost_rows: false,
                most_block_rows: totals.most_block_rows,
            };
            let writer = &self.writer;
            let columns = (0..writer.columns().len()).map(|index| writer.column_facts(index));

            // spans of a segment still open go on in its row, and only a clear ends them
            let mut rows = hold(&volume.summary);
            let is_listed = hold(&volume.damaged).contains(&target.number);
            let slots = volume.damage.slots();
            if is_listed {
                rows.remove(target.number);
            }
            match highest_of(&slots, target.number).filter(|_place| is_listed) {
                Some(place) => {
                    facts.is_damaged = true;
                    // a segment damaged before its seal lost what its rows held, and its blocks can't give that back
                    facts.has_lost_rows = true;
                    facts.damage_place = place;
                    state.note_dropped_rows(rows.push_kept(&facts, columns, spans_of(&slots, target.number)));
                },
                None => {
                    // a clean row that won't fit costs nothing until the segment is damaged, and then its columns count whole
                    if !rows.push(&facts, columns, [].into_iter()) {
                        state.note_dropped_row();
                    }
                },
            }
            drop(slots);
            // a row the summary had no room for never gets a walk, so it isn't waiting for one
            if rows.find_row(target.number).is_some() {
                volume.walks_waiting.fetch_add(1, Ordering::AcqRel);
            }
            drop(rows);
            self.is_summarized = true;
        }

        state.save_summary(volume)?;
        if let Some(task) = state.planner_task.get() {
            task.wake();
        }
        self.phase = SealPhase::DiscardingRows;
        Ok(())
    }

    /// Discard the rows file, which only runs once the table answers for the segment
    pub fn discard_rows(&mut self, target: &SealTarget<Backend::File>) -> Result<()> {
        let state = Arc::clone(&self.state);
        let root = &self.volume().roots[target.root.0 as usize];
        match state.io.remove_file(&rows_path(root, target.number)) {
            Ok(()) => {},
            Err(error) if error.kind() == ErrorKind::NotFound => {},
            Err(error) => return Err(error),
        }
        state.io.sync_directory(root)?;
        self.switch(target);
        Ok(())
    }

    /// Put the segment in the table and the directories, each part once however often this retries
    pub fn install(&mut self, target: &SealTarget<Backend::File>) -> Result<()> {
        let Some(totals) = self.totals else {
            return Err(Error::other("seal installing with no footer"));
        };
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        if !self.is_in_table {
            let installed = volume.table.install(
                target.data.clone(),
                target.root,
                &totals.head,
                self.writer.columns(),
                self.writer.entries(),
            );
            if !installed {
                self.note_stuck();
                // the rows file stays until the table has room, so a retry loses nothing
                return Err(Error::other("segment table is full"));
            }
            volume.note_sealed(totals.head.places.highest);
            state.note_moved(JobKind::Seal, totals.file_length);
            if !target.marks.is_empty() {
                volume.marked_seals.fetch_add(1, Ordering::Relaxed);
                volume.marked_seal_bytes.fetch_add(target.data_end.0, Ordering::Relaxed);
            }
            self.is_in_table = true;
        }

        while let Some(column) = self.writer.columns().get(self.columns_listed) {
            let entry = Listing::of_column(column, target.number, totals.head.places.highest);
            if let Some(directory) = &state.directories[column.column.0 as usize]
                && !directory.replace(Some(&entry), &[])
            {
                self.note_stuck();
                return Err(Error::other("column directory is full"));
            }
            self.columns_listed += 1;
        }

        volume.is_seal_stuck.store(false, Ordering::Release);
        self.phase = SealPhase::Summarizing;
        Ok(())
    }

    /// Make the one atomic store: from here the footer answers and the write index doesn't
    pub fn switch(&mut self, target: &SealTarget<Backend::File>) {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        volume.close_slot(target.slot);

        let mut claims = hold(&volume.claims);
        claims.open.retain(|segment| segment.slot != target.slot);
        *hold(&state.slot_files[target.slot.0 as usize].leaving_rows) = Some(target.rows.clone());
        drop(claims);
        if let Some(task) = self.state.planner_task.get() {
            task.wake();
        }
        self.end_step(|steps| &steps.install);
        self.phase = SealPhase::Cleaning;
    }

    /// Clean a step's rows out of the write index, and free the slot when they and the pins are gone
    pub fn clean(&mut self, target: &SealTarget<Backend::File>) -> bool {
        self.end_step(|steps| &steps.clean_step);
        self.volume().ration.note_cleaned(RowCount(self.rows_cleaned));
        self.rows_cleaned += SEAL_STEP_ROWS;
        if !self.state.index.clean_slot(target.slot, RowCount(SEAL_STEP_ROWS)) {
            return false;
        }

        let slot_file = &self.state.slot_files[target.slot.0 as usize];
        // no new pin can land once the slot is empty, so a count of zero stays zero
        slot_file.set(None);
        if slot_file.pin_count() > 0 {
            return false;
        }
        if let Some(rows) = hold(&slot_file.leaving_rows).take() {
            self.state.io.close_file(rows);
        }
        self.state.free_slot(target.slot);
        // a spare the job could not make for want of a slot goes ahead now, before the job's period
        if let Some(task) = self.state.reserve_task.get() {
            task.wake();
        }
        true
    }

    /// Read the heap the job's own buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        let keys = self.highest.capacity() + self.last_key.capacity() + self.ranges.capacity();
        ByteCount(self.writer.heap_bytes().0 + self.walk.heap_bytes().0 + keys as u64)
    }

    /// Drop an empty segment's files with no footer
    pub fn drop_empty(&mut self, target: &SealTarget<Backend::File>) -> Result<()> {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        volume.close_slot(target.slot);
        hold(&volume.claims).open.retain(|segment| segment.slot != target.slot);

        let root = &volume.roots[target.root.0 as usize];
        // a data file left with no rows and no tail reads as damage, so the rows go only once it has
        if state.remove_or_park(volume, root, target.number) {
            state.io.remove_file(&rows_path(root, target.number))?;
        }
        state.io.sync_directory(root)?;
        state.slot_files[target.slot.0 as usize].set(None);
        state.io.close_file(target.data.clone());
        state.io.close_file(target.rows.clone());
        state.free_slot(target.slot);
        Ok(())
    }

    /// Check the seal can do nothing until writers land or the mark moves
    pub fn is_waiting(&self, target: &SealTarget<Backend::File>) -> bool {
        match self.phase {
            SealPhase::Draining => !self.is_ready(target),
            SealPhase::Cleaning => self.state.slot_files[target.slot.0 as usize].pin_count() > 0,
            SealPhase::Idle
            | SealPhase::Writing
            | SealPhase::Installing
            | SealPhase::Summarizing
            | SealPhase::DiscardingRows => false,
        }
    }

    /// Decide what a failed step leaves: a retry from the footer's start, or a failed volume
    pub fn after_failure(&mut self, error: &Error) -> JobStep {
        if self.phase == SealPhase::Writing {
            self.phase = SealPhase::Draining;
        }
        let is_past_footer = matches!(
            self.phase,
            SealPhase::Installing | SealPhase::Summarizing | SealPhase::DiscardingRows
        );
        let is_retryable = error.kind() == ErrorKind::StorageFull || is_past_footer;
        if !is_retryable {
            self.volume().fail();
            return JobStep::Idle;
        }

        let now = self.state.io.now();
        JobStep::IdleUntil {
            deadline: Nanoseconds(now.0 + SEAL_RETRY.0),
        }
    }

    /// Run one bounded step of the seal in progress
    pub fn advance(&mut self, target: &SealTarget<Backend::File>) -> Result<()> {
        match self.phase {
            SealPhase::Idle => Ok(()),
            SealPhase::Draining => {
                if !self.is_ready(target) {
                    return Ok(());
                }
                if target.row_count.0 == 0 {
                    self.drop_empty(target)?;
                    self.end_seal();
                    return Ok(());
                }
                self.start_writing(target)
            },
            SealPhase::Writing => self.write_rows(target),
            SealPhase::Installing => self.install(target),
            SealPhase::Summarizing => self.summarize(target),
            SealPhase::DiscardingRows => self.discard_rows(target),
            SealPhase::Cleaning => {
                if self.clean(target) {
                    self.end_seal();
                }
                Ok(())
            },
        }
    }
}

impl<Backend: Io> Job for SealJob<Backend> {
    fn step(&mut self) -> JobStep {
        // a volume that failed makes no claim of durability until it's reopened
        if self.volume().has_failed() {
            return JobStep::Idle;
        }

        if self.phase == SealPhase::Idle {
            if !self.pick_target() {
                return JobStep::Idle;
            }
            self.phase = SealPhase::Draining;
        }

        let Some(target) = self.target.clone() else {
            self.phase = SealPhase::Idle;
            return JobStep::Worked;
        };
        // a failed step keeps the rows file and the open state, so nothing is lost by trying again
        match self.advance(&target) {
            Ok(()) if self.is_waiting(&target) => {
                let now = self.state.io.now();
                JobStep::IdleUntil {
                    deadline: Nanoseconds(now.0 + SEAL_WAIT.0),
                }
            },
            Ok(()) => JobStep::Worked,
            Err(error) => self.after_failure(&error),
        }
    }
}
