//! Seal: an open segment's footer, its hand-over, and the one atomic switch to sealed

use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, FileOffset, Nanoseconds, RootId, RowCount, WritePlace};

use crate::pool::touched_capacity;
use crate::config::MAX_KEY_BYTES;
use crate::engine::engine_state::EngineState;
use crate::engine::open_segment::{SegmentPhase, data_path, rows_path};
use crate::engine::volume::Volume;
use crate::guard::hold;
use crate::index::key_hash::hash_key;
use crate::index::open_slots::OpenSlot;
use crate::index::seal_walk::SealWalk;
use crate::index::spot_index::SpotLocation;
use crate::io::job::{Job, JobStep};
use crate::io::Io;
use crate::record::codec::Codec;
use crate::record::record_check::RecordIdentity;
use crate::record::record_reader::{RecordLocation, RecordRead};
use crate::row_kind::RowKind;
use crate::segment::column_directory::DirectoryEntry;
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
    /// The table has the segment, and every lower place has yet to reach the index
    AwaitingMark,
    /// Rows go to the spot cache
    HandingOver,
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
    pub range_end: Vec<u8>,
    pub totals: Option<FooterTotals>,
    pub last_key: Vec<u8>,
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
            range_end: touched_capacity(MAX_KEY_BYTES as usize, 0),
            totals: None,
            last_key: touched_capacity(MAX_KEY_BYTES as usize, 0),
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
                });
                return true;
            }
        }
        false
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
        self.state.write_out(target.slot)?;
        self.writer.start(target.data_end, target.lowest_place);
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
        for _row in 0..SEAL_STEP_ROWS {
            let Some(shard) = self.walk.next_shard() else {
                return self.finish_footer(target);
            };

            let head = &self.walk.heads[shard];
            let Some(row) = head.row else {
                return Err(Error::other("write index row has an unknown kind"));
            };
            if self.column != Some(head.column) {
                self.state.index.highest_key(target.slot, head.column, &mut self.highest);
                let start = ColumnStart {
                    column: head.column,
                    lowest: &head.key,
                    highest: &self.highest,
                };
                self.writer.start_column(&target.data, &start)?;
                self.column = Some(head.column);
            }

            // the row says what the pool may have dropped: a newer cover in another segment can be lost without this one
            if row.kind == RowKind::RangeDelete {
                let identity = RecordIdentity {
                    record_key: self.state.record_key,
                    column: head.column,
                    key: &head.key,
                    kind: RowKind::RangeDelete,
                    place: row.place,
                };
                let location = RecordLocation {
                    offset: row.offset,
                    length: row.length,
                };
                let end = &mut self.range_end;
                let read = self.state.record_readers.with(|reader| {
                    reader.read_value(&target.data, location, &identity, Codec::None, end)
                })?;
                if read != RecordRead::Valid {
                    return Err(Error::other("the record of a range delete failed its check"));
                }
                let cover = Cover {
                    column: head.column,
                    start: &head.key,
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
            self.writer.push_row(&target.data, &head.key, &footer_row)?;
            self.walk.advance(&self.state.index, shard);
        }
        Ok(())
    }

    /// Finish the footer, which syncs it, then discard the rows file
    pub fn finish_footer(&mut self, target: &SealTarget<Backend::File>) -> Result<()> {
        let state = Arc::clone(&self.state);
        let totals = self.writer.finish(&target.data, target.number, target.data_end, &self.ranges, state.record_key)?;
        self.totals = Some(totals);
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
                is_damaged: false,
                has_covers: totals.head.ranges_length.0 > 0,
                damage_place: WritePlace(0),
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
            drop(rows);
            self.is_summarized = true;
        }

        state.save_summary(volume)?;
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
        self.phase = SealPhase::AwaitingMark;
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
                &totals.head,
                self.writer.columns(),
                self.writer.entries(),
            );
            if !installed {
                // the rows file stays until the table has room, so a retry loses nothing
                return Err(Error::other("segment table is full"));
            }
            volume.note_sealed(totals.head.places.highest);
            self.is_in_table = true;
        }

        while let Some(column) = self.writer.columns().get(self.columns_listed) {
            let entry = DirectoryEntry {
                shared_length: column.shared_length,
                shared_sum: column.shared_sum,
                lowest: column.lowest,
                highest: column.highest,
                reach: column.highest,
                segment: target.number,
            };
            if let Some(directory) = &state.directories[column.column.0 as usize]
                && !directory.replace(Some(&entry), &[])
            {
                return Err(Error::other("column directory is full"));
            }
            self.columns_listed += 1;
        }

        self.phase = SealPhase::Summarizing;
        Ok(())
    }

    /// Start the hand-over once the mark passes the segment, so every lower place is in the index
    /// and no read that starts from here can have a read point below this segment's rows
    pub fn start_hand_over(&mut self, target: &SealTarget<Backend::File>) {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        if state.order.mark() < target.highest_place {
            return;
        }

        // a get that started before this count can't fill the spot cache with an older row
        volume.handovers.fetch_add(1, Ordering::AcqRel);
        self.walk.start(&state.index, target.slot);
        self.last_column = None;
        self.phase = SealPhase::HandingOver;
    }

    /// Give a step's rows to the spot cache: every row replaces or removes its key's slot
    pub fn hand_over(&mut self, target: &SealTarget<Backend::File>) {
        for _row in 0..SEAL_STEP_ROWS {
            let Some(shard) = self.walk.next_shard() else {
                self.switch(target);
                return;
            };
            let head = &self.walk.heads[shard];
            let is_new_key = self.last_column != Some(head.column) || self.last_key != head.key;
            if is_new_key && let Some(row) = head.row {
                let hash = hash_key(head.column, &head.key);
                let is_newest = self.state.index.is_newest_version(head.column, &head.key, row.place);
                if row.kind == RowKind::Value && is_newest {
                    let location = SpotLocation {
                        segment: target.number,
                        offset: row.offset,
                        length: row.length,
                        place: row.place,
                    };
                    self.state.spot.insert(hash, &location);
                } else {
                    self.state.spot.remove(hash);
                }
            }
            self.last_column = Some(head.column);
            self.last_key.clear();
            self.last_key.extend_from_slice(&self.walk.heads[shard].key);
            self.walk.advance(&self.state.index, shard);
        }
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
        self.phase = SealPhase::Cleaning;
    }

    /// Clean a step's rows out of the write index, and free the slot when they and the pins are gone
    pub fn clean(&mut self, target: &SealTarget<Backend::File>) -> bool {
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
        state.io.remove_file(&data_path(root, target.number))?;
        state.io.remove_file(&rows_path(root, target.number))?;
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
            SealPhase::Draining => !self.is_drained(target.slot),
            SealPhase::AwaitingMark => true,
            SealPhase::Cleaning => self.state.slot_files[target.slot.0 as usize].pin_count() > 0,
            SealPhase::Idle
            | SealPhase::Writing
            | SealPhase::Installing
            | SealPhase::Summarizing
            | SealPhase::DiscardingRows
            | SealPhase::HandingOver => false,
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
                if !self.is_drained(target.slot) {
                    return Ok(());
                }
                if target.row_count.0 == 0 {
                    self.drop_empty(target)?;
                    self.target = None;
                    self.phase = SealPhase::Idle;
                    return Ok(());
                }
                self.start_writing(target)
            },
            SealPhase::Writing => self.write_rows(target),
            SealPhase::Installing => self.install(target),
            SealPhase::Summarizing => self.summarize(target),
            SealPhase::DiscardingRows => self.discard_rows(target),
            SealPhase::AwaitingMark => {
                self.start_hand_over(target);
                Ok(())
            },
            SealPhase::HandingOver => {
                self.hand_over(target);
                Ok(())
            },
            SealPhase::Cleaning => {
                if self.clean(target) {
                    self.target = None;
                    self.phase = SealPhase::Idle;
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
