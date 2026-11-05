//! Compaction's rewrite: one segment copied to a new one, without the rows no read can reach
//!
//! Records land at the front of the output and row blocks land past the input's data end, since the records' end isn't known
//! until the last row. The blocks then move down to meet the records and the tail makes the file a segment. The tail lists
//! the input, so open removes the input however far the install got.

use std::io::{Error, ErrorKind, IoSlice, Result};
use std::mem;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, WritePlace};

use crate::config::MAX_KEY_BYTES;
use crate::engine::compact_job::park;
use crate::engine::engine_state::EngineState;
use crate::engine::open_segment::{data_path, output_path};
use crate::engine::seal_job::MOST_SEGMENT_BLOCKS;
use crate::engine::volume::{RewriteOutput, Volume};
use crate::engine::walk_job::add_count;
use crate::guard::hold;
use crate::io::{Io, IoFile};
use crate::pool::{touched_capacity, touched_zeroes};
use crate::row_kind::RowKind;
use crate::segment::column_directory::DirectoryEntry;
use crate::segment::footer_writer::{ColumnStart, FooterTotals, FooterWriter};
use crate::segment::row_block::{FooterRow, ROW_BLOCK_BYTES, RowBlockWalk};
use crate::segment::segment_table::{SegmentPin, WalkBlock};
use crate::segment::segment_tail::SegmentNumber;
use crate::segment::summary::{DeadCount, SegmentFacts, SummaryRow};

/// A segment is rewritten once this part of its rows or bytes is dead: 2 is a half, 4 a quarter
pub const REWRITE_DIVISOR: u64 = 2;
/// Bytes of one job's merge buffer, which carries records and then row blocks
pub const MERGE_BUFFER_BYTES: usize = 2 * 1024 * 1024;
/// Most row blocks of an output that can hold rows kept for a cue, past which the rewrite waits for the cue to go
pub const MOST_KEPT_BLOCKS: usize = 1_024;

/// The segment being rewritten
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RewriteTarget {
    pub segment: SegmentNumber,
    pub next_block: u32,
    pub block_count: u32,
    pub blocks_at: FileOffset,
}

/// How far a rewrite has gone
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RewritePhase {
    /// Rows of a grave-heavy segment are being decided, and nothing is copied unless one would drop
    Deciding,
    /// Rows are being decided and records copied
    Copying,
    /// Row blocks are moving down to the records' end
    Moving,
    /// The output is a whole segment and waits to replace its input
    Installing,
}

/// The rewrite of one volume
pub struct RewriteJob<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
    pub volume: usize,
    pub target: Option<RewriteTarget>,
    pub phase: RewritePhase,
    pub output: Option<RewriteOutput<Backend::File>>,
    pub writer: FooterWriter,
    pub totals: Option<FooterTotals>,
    buffer: Vec<u8>,
    used: usize,
    records_end: FileOffset,
    moved: u64,
    input_length: ByteCount,
    block: Vec<u8>,
    key: Vec<u8>,
    last_key: Vec<u8>,
    lowest: Vec<u8>,
    highest: Vec<u8>,
    last_column: Option<ColumnId>,
    output_column: Option<ColumnId>,
    top_place: WritePlace,
    newest_place: Option<WritePlace>,
    lowest_dropped: WritePlace,
    highest_replacement: WritePlace,
    dropped_rows: u64,
    kept_dead: Vec<DeadCount>,
    last_tried: SegmentNumber,
}

impl<Backend: Io> RewriteJob<Backend> {
    /// Allocate the job's fixed buffers
    pub fn new(state: Arc<EngineState<Backend>>, volume: usize) -> Self {
        // a mutex's first lock can allocate, and this one's first real use could be in a close
        drop(hold(&state.volumes[volume].partial_output));
        let keys = MAX_KEY_BYTES as usize;
        Self {
            state,
            volume,
            target: None,
            phase: RewritePhase::Copying,
            output: None,
            writer: FooterWriter::new(MOST_SEGMENT_BLOCKS),
            totals: None,
            buffer: touched_zeroes(MERGE_BUFFER_BYTES),
            used: 0,
            records_end: FileOffset(0),
            moved: 0,
            input_length: ByteCount(0),
            block: touched_zeroes(ROW_BLOCK_BYTES),
            key: touched_capacity(keys, 0),
            last_key: touched_capacity(keys, 0),
            lowest: touched_capacity(keys, 0),
            highest: touched_capacity(keys, 0),
            last_column: None,
            output_column: None,
            top_place: WritePlace(0),
            newest_place: None,
            lowest_dropped: WritePlace(u64::MAX),
            highest_replacement: WritePlace(0),
            dropped_rows: 0,
            kept_dead: touched_capacity(MOST_KEPT_BLOCKS, DeadCount::default()),
            last_tried: SegmentNumber(0),
        }
    }

    /// Read the heap the job's buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        let keys = self.key.capacity() + self.last_key.capacity() + self.lowest.capacity() + self.highest.capacity();
        let buffers = self.buffer.capacity() + self.block.capacity() + keys;
        let counts = self.kept_dead.capacity() * size_of::<DeadCount>();
        ByteCount(self.writer.heap_bytes().0 + (buffers + counts) as u64)
    }

    /// Check a rewrite is under way
    pub fn is_running(&self) -> bool {
        self.target.is_some()
    }

    /// Start on the next ready segment past the last one tried, once no walk target is in progress, since it would count rows
    /// this rewrite has decided on. The walks wait from here to the install. A grave-heavy segment is decided first, and
    /// copies nothing if no row would drop
    pub fn start(&mut self) -> bool {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        let Some((target, length, is_grave_heavy)) = self.pick(volume) else {
            return false;
        };
        if is_grave_heavy {
            self.clear(target, length);
            self.phase = RewritePhase::Deciding;
            volume.is_rewriting.store(true, Ordering::Release);
            return true;
        }
        match self.open_output(volume, target, length) {
            Ok(()) => true,
            Err(_error) => {
                self.abandon();
                false
            },
        }
    }

    /// Pick a segment and note it as tried
    pub fn pick(&mut self, volume: &Volume<Backend>) -> Option<(RewriteTarget, ByteCount, bool)> {
        if volume.stray_outputs.load(Ordering::Acquire) > 0 {
            return None;
        }
        let rows = hold(&volume.summary);
        if volume.has_walk_target.load(Ordering::Acquire) {
            return None;
        }
        let ready = rows.rows().filter(|row| row.facts.is_walked() && is_rewrite_ready(row, volume.table.freed_bytes(row.facts.segment)));
        let after = ready.clone().filter(|row| row.facts.segment > self.last_tried).min_by_key(|row| row.facts.segment);
        let picked = after.or_else(|| ready.min_by_key(|row| row.facts.segment))?;
        self.last_tried = picked.facts.segment;
        let target = RewriteTarget {
            segment: picked.facts.segment,
            next_block: 0,
            block_count: picked.facts.block_count,
            blocks_at: picked.facts.data_end,
        };
        Some((target, picked.facts.file_length, is_grave_heavy(&picked)))
    }

    /// Create the output and clear what the last rewrite left. It reserves the input's whole length, so a rewrite holds that
    /// much twice until its footer cuts the output down
    pub fn open_output(&mut self, volume: &Volume<Backend>, target: RewriteTarget, length: ByteCount) -> Result<()> {
        let state = Arc::clone(&self.state);
        let Some(pin) = volume.table.pin(target.segment) else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        let Ok(number) = state.next_segment_number() else {
            return Err(Error::from(ErrorKind::StorageFull));
        };
        let root = number.0 as usize % volume.roots.len();
        let file = state.io.create_file(&output_path(&volume.roots[root], number), length)?;
        let output = RewriteOutput { file, root, number, is_named: false };
        *hold(&volume.partial_output) = Some(output.clone());
        self.output = Some(output);

        self.writer.start(target.blocks_at, pin.head.place_base);
        self.clear(target, length);
        self.phase = RewritePhase::Copying;
        volume.is_rewriting.store(true, Ordering::Release);
        Ok(())
    }

    /// Start a pass over a target
    pub fn clear(&mut self, target: RewriteTarget, length: ByteCount) {
        self.target = Some(target);
        self.input_length = length;
        self.totals = None;
        self.used = 0;
        self.records_end = FileOffset(0);
        self.moved = 0;
        self.last_column = None;
        self.output_column = None;
        self.lowest_dropped = WritePlace(u64::MAX);
        self.highest_replacement = WritePlace(0);
        self.dropped_rows = 0;
        self.kept_dead.clear();
    }

    /// Run one bounded step, and give the rewrite up on any failure, which leaves the input as it was
    pub fn step(&mut self) {
        let Some(target) = self.target else {
            return;
        };
        let outcome = match self.phase {
            RewritePhase::Deciding | RewritePhase::Copying if target.next_block < target.block_count => self.copy_block(target),
            RewritePhase::Deciding => self.end_deciding(target),
            RewritePhase::Copying => self.finish_rows(),
            RewritePhase::Moving => self.move_blocks(target),
            RewritePhase::Installing => self.install(target),
        };
        if outcome.is_err() {
            self.abandon();
        }
    }

    /// Decide one row block of the input and copy what stays
    pub fn copy_block(&mut self, target: RewriteTarget) -> Result<()> {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        let (Some(pin), Some(block)) = (volume.table.pin(target.segment), volume.table.walk_block(target.segment, target.next_block)) else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        let length = block.entry.block_length().0 as usize;
        if length > self.block.len() {
            return Err(Error::from(ErrorKind::InvalidData));
        }
        pin.file.read_exact_at(block.entry.block_at(), &mut self.block[..length])?;

        // the walk reads the block while the rows go through the job
        let bytes = mem::take(&mut self.block);
        let copied = self.copy_rows(volume, &pin, &block, &bytes[..length], target);
        self.block = bytes;
        copied?;
        self.target = Some(RewriteTarget {
            next_block: target.next_block + 1,
            ..target
        });
        Ok(())
    }

    /// Decide each row of a block
    pub fn copy_rows(
        &mut self,
        volume: &Volume<Backend>,
        pin: &SegmentPin<'_, Backend::File>,
        block: &WalkBlock,
        bytes: &[u8],
        target: RewriteTarget,
    ) -> Result<()> {
        let Some(mut walk) = RowBlockWalk::open(bytes, pin.head.place_base) else {
            return Err(Error::from(ErrorKind::InvalidData));
        };
        while let Some(row) = walk.next_row(&mut self.key) {
            if row.kind == RowKind::RangeDelete {
                return Err(Error::from(ErrorKind::InvalidData));
            }
            let replacement = self.replacement(volume, block.column, &row, target)?;
            match replacement {
                Some(place) if self.can_drop(place, &row) => {
                    self.lowest_dropped = self.lowest_dropped.min(row.place);
                    self.highest_replacement = self.highest_replacement.max(place);
                    self.dropped_rows += 1;
                },
                None if row.kind == RowKind::Grave && self.hides_nothing(volume, block.column, &row, target)? => {
                    self.lowest_dropped = WritePlace(0);
                    self.highest_replacement = self.highest_replacement.max(row.place);
                    self.dropped_rows += 1;
                },
                _kept => self.keep(volume, pin, block.column, &row, replacement, target)?,
            }
        }
        Ok(())
    }

    /// Find the newest place above a row that replaces it: this segment's top row of the key, or a row in a finished
    /// segment. A row replaced only in an open or unfinished segment stays, and that segment's walk counts it in the output
    pub fn replacement(
        &mut self,
        volume: &Volume<Backend>,
        column: ColumnId,
        row: &FooterRow,
        target: RewriteTarget,
    ) -> Result<Option<WritePlace>> {
        let is_top = self.last_column != Some(column) || self.last_key != self.key;
        if is_top {
            self.last_column = Some(column);
            self.last_key.clear();
            self.last_key.extend_from_slice(&self.key);
            self.top_place = row.place;
            self.newest_place = match self.state.newest_walked(volume, column, &self.key, target.segment) {
                Ok(found) => found.map(|found| found.row.place),
                // a version a damaged block hides replaces nothing, and the row stays
                Err(StoreError::Corruption { .. }) => None,
                Err(_error) => return Err(Error::from(ErrorKind::Interrupted)),
            };
        }
        let own = (!is_top).then_some(self.top_place);
        Ok(own.max(self.newest_place).filter(|place| *place > row.place))
    }

    /// Check a replaced row can go: no live cue sits at or above it and below its newest replacement
    pub fn can_drop(&self, replacement: WritePlace, row: &FooterRow) -> bool {
        !self.state.cues.has_cue_between(row.place, replacement)
    }

    /// Check a grave with no newer row of its key has nothing left to hide, by this key's own rows and nothing wider. An older
    /// grave keeps it too: that one's walk count stands on this row, and a later write of the key would count it twice. It
    /// drops first, as any row with a newer one does. A file on its way out comes back at a crash, so no grave drops while
    /// one is parked. A cue below the grave can read what it covers, and damage could be hiding an older version
    pub fn hides_nothing(
        &self,
        volume: &Volume<Backend>,
        column: ColumnId,
        row: &FooterRow,
        target: RewriteTarget,
    ) -> Result<bool> {
        let state = &self.state;
        if volume.segments_parked.load(Ordering::Acquire) > 0
            || volume.stray_outputs.load(Ordering::Acquire) > 0
            || state.cues.has_cue_below(row.place)
            || volume.damage.covering(column, &self.key).is_some()
        {
            return Ok(false);
        }
        match state.has_older_row(volume, column, &self.key, row.place, target.segment) {
            Ok(has_older) => Ok(!has_older),
            Err(StoreError::Corruption { .. }) => Ok(false),
            Err(_error) => Err(Error::from(ErrorKind::Interrupted)),
        }
    }

    /// Carry a row into the output, and note it dead when a cue is all that keeps it, since no walk will count it again
    pub fn keep(
        &mut self,
        volume: &Volume<Backend>,
        pin: &SegmentPin<'_, Backend::File>,
        column: ColumnId,
        row: &FooterRow,
        replacement: Option<WritePlace>,
        target: RewriteTarget,
    ) -> Result<()> {
        if self.phase == RewritePhase::Deciding {
            return Ok(());
        }
        let Some(output) = self.output.clone() else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        if self.output_column != Some(column) {
            self.start_column(volume, pin, &output, column, target)?;
        }
        let offset = self.copy_record(&pin.file, &output.file, row)?;
        let kept = FooterRow { offset, ..*row };
        self.writer.push_row(&output.file, &self.key, &kept)?;
        if replacement.is_some() {
            if self.kept_dead.len() == self.kept_dead.capacity() {
                return Err(Error::from(ErrorKind::OutOfMemory));
            }
            add_count(&mut self.kept_dead, output.number, column, self.writer.entries().len() as u32, &kept, replacement);
        }
        Ok(())
    }

    /// Start a column in the output with the input's range, which holds every row that stays
    pub fn start_column(
        &mut self,
        volume: &Volume<Backend>,
        pin: &SegmentPin<'_, Backend::File>,
        output: &RewriteOutput<Backend::File>,
        column: ColumnId,
        target: RewriteTarget,
    ) -> Result<()> {
        let table = &volume.table;
        let edges = table.column_blocks(target.segment, column).and_then(|(first, count)| {
            let last = first + count.checked_sub(1)?;
            Some([table.walk_block(target.segment, first)?.entry, table.walk_block(target.segment, last)?.entry])
        });
        let Some(lines) = edges else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        let (lowest, highest) = (&mut self.lowest, &mut self.highest);
        let is_read = self.state.footer_readers.with(|reader| reader.edge_keys(&pin.file, &lines, pin.head.place_base, lowest, highest))?;
        if !is_read {
            return Err(Error::from(ErrorKind::InvalidData));
        }
        let start = ColumnStart {
            column,
            lowest: &self.lowest,
            highest: &self.highest,
        };
        self.writer.start_column(&output.file, &start)?;
        self.output_column = Some(column);
        Ok(())
    }

    /// Copy a record as stored, and return where it landed
    pub fn copy_record(&mut self, from: &Backend::File, to: &Backend::File, row: &FooterRow) -> Result<FileOffset> {
        let length = row.length.0 as usize;
        if length > self.buffer.len() - self.used {
            self.flush(to)?;
        }
        let at = FileOffset(self.records_end.0 + self.used as u64);
        if length <= self.buffer.len() {
            from.read_exact_at(row.offset, &mut self.buffer[self.used..self.used + length])?;
            self.used += length;
            return Ok(at);
        }

        let mut done = 0;
        while done < length {
            let part = (length - done).min(self.buffer.len());
            from.read_exact_at(FileOffset(row.offset.0 + done as u64), &mut self.buffer[..part])?;
            to.write_at(FileOffset(at.0 + done as u64), &[IoSlice::new(&self.buffer[..part])])?;
            done += part;
        }
        self.records_end = FileOffset(at.0 + length as u64);
        Ok(at)
    }

    /// Write out the records the buffer holds
    pub fn flush(&mut self, to: &Backend::File) -> Result<()> {
        if self.used > 0 {
            to.write_at(self.records_end, &[IoSlice::new(&self.buffer[..self.used])])?;
            self.records_end = FileOffset(self.records_end.0 + self.used as u64);
            self.used = 0;
        }
        Ok(())
    }

    /// End the deciding: copy from the first block if a row would drop, and leave the segment alone if none would
    pub fn end_deciding(&mut self, target: RewriteTarget) -> Result<()> {
        if self.dropped_rows == 0 {
            return Err(Error::from(ErrorKind::Interrupted));
        }
        let state = Arc::clone(&self.state);
        let target = RewriteTarget { next_block: 0, ..target };
        self.open_output(&state.volumes[self.volume], target, self.input_length)
    }

    /// End the copying. A rewrite that dropped nothing has nothing to install, and one that kept nothing has no output
    pub fn finish_rows(&mut self) -> Result<()> {
        let Some(output) = self.output.clone() else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        if self.dropped_rows == 0 {
            return Err(Error::from(ErrorKind::Interrupted));
        }
        if self.writer.columns().is_empty() {
            self.phase = RewritePhase::Installing;
            return Ok(());
        }
        self.flush(&output.file)?;
        self.writer.close_block(&output.file)?;
        self.phase = RewritePhase::Moving;
        Ok(())
    }

    /// Move a buffer of row blocks down to the records' end, and write the footer once they're all there
    pub fn move_blocks(&mut self, target: RewriteTarget) -> Result<()> {
        let Some(output) = self.output.clone() else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        let distance = target.blocks_at.0 - self.records_end.0;
        let from = target.blocks_at.0 + self.moved;
        let left = self.writer.position().0 - from;
        if distance == 0 || left == 0 {
            return self.finish_output(&output, target, distance);
        }

        let part = left.min(self.buffer.len() as u64) as usize;
        output.file.read_exact_at(FileOffset(from), &mut self.buffer[..part])?;
        output.file.write_at(FileOffset(from - distance), &[IoSlice::new(&self.buffer[..part])])?;
        self.moved += part as u64;
        Ok(())
    }

    /// Write the footer and the tail, and give the file a segment's name
    pub fn finish_output(&mut self, output: &RewriteOutput<Backend::File>, target: RewriteTarget, distance: u64) -> Result<()> {
        let state = Arc::clone(&self.state);
        let root = &state.volumes[self.volume].roots[output.root];
        self.writer.move_down(ByteCount(distance));
        let inputs = [target.segment];
        let totals = self.writer.finish(&output.file, output.number, self.records_end, &[], state.record_key, &inputs)?;
        state.io.rename_file(&output_path(root, output.number), &data_path(root, output.number))?;
        // the name has landed from here, whatever the directory's sync says
        let named = RewriteOutput { is_named: true, ..output.clone() };
        *hold(&state.volumes[self.volume].partial_output) = Some(named.clone());
        self.output = Some(named);
        state.io.sync_directory(root)?;
        self.totals = Some(totals);
        self.phase = RewritePhase::Installing;
        Ok(())
    }

    /// Swap the input for the output in every list, save that, and remove the input. The count moves before the last look:
    /// a read or a cue that takes its point after it has every replacement under that point
    pub fn install(&mut self, target: RewriteTarget) -> Result<()> {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        let Some(output) = self.output.clone() else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        // a full park clears as reads finish, and this step comes round again
        if !volume.can_park() {
            return Ok(());
        }
        if state.read_point() < self.highest_replacement {
            return Err(Error::from(ErrorKind::Interrupted));
        }
        state.cues.note_reclaim();
        if state.cues.has_cue_between(self.lowest_dropped, self.highest_replacement) {
            return Err(Error::from(ErrorKind::Interrupted));
        }
        if let Some(totals) = self.totals {
            if !volume.table.install(output.file.clone(), &totals.head, self.writer.columns(), self.writer.entries()) {
                return Err(Error::from(ErrorKind::OutOfMemory));
            }
            volume.table.mark_walked(output.number);
        }

        for column in self.writer.columns() {
            let entry = DirectoryEntry {
                shared_length: column.shared_length,
                shared_sum: column.shared_sum,
                lowest: column.lowest,
                highest: column.highest,
                reach: column.highest,
                segment: output.number,
            };
            if let Some(directory) = &state.directories[column.column.0 as usize]
                && !directory.replace(Some(&entry), &[target.segment])
            {
                volume.fail();
            }
        }
        // a column every row of which went has no entry to swap in
        for directory in state.directories.iter().flatten() {
            if !directory.replace(None, &[target.segment]) {
                volume.fail();
            }
        }

        let retired = volume.table.retire(target.segment);
        match self.totals {
            Some(totals) => self.summarize(volume, &output, &totals),
            None => {
                hold(&volume.summary).remove(target.segment);
            },
        }
        let saved = state.save_summary(volume);

        hold(&volume.partial_output).take();
        // every row dropped, so the file was never a segment
        if self.totals.is_none() {
            self.leave_output(volume, output);
        }
        self.output = None;
        self.target = None;
        volume.is_rewriting.store(false, Ordering::Release);
        volume.segments_rewritten.fetch_add(1, Ordering::AcqRel);
        volume.wants_compaction.store(true, Ordering::Release);
        if let Some(retired) = retired {
            park(volume, retired, target.segment, saved.is_ok());
        }
        saved
    }

    /// Replace the input's summary row with the output's. Its counts come from the rows it kept, and it starts walked
    pub fn summarize(&mut self, volume: &Volume<Backend>, output: &RewriteOutput<Backend::File>, totals: &FooterTotals) {
        let Some(target) = self.target else {
            return;
        };
        let block_count = self.writer.entries().len() as u32;
        let facts = SegmentFacts {
            segment: output.number,
            file_length: totals.file_length,
            data_end: self.records_end,
            row_block_end: totals.head.index_at,
            seal_mark: totals.head.index_sum,
            places: totals.head.places,
            is_damaged: false,
            has_covers: false,
            damage_place: WritePlace(0),
            block_count,
            walked_blocks: block_count,
            is_backward: false,
        };
        let writer = &self.writer;
        let columns = (0..writer.columns().len()).map(|index| writer.column_facts(index));
        let mut rows = hold(&volume.summary);
        rows.remove(target.segment);
        if !rows.push(&facts, columns, [].into_iter()) {
            self.state.note_dropped_row();
        }
        self.kept_dead.sort_unstable_by_key(|count| count.block);
        rows.apply_walk(output.number, block_count, &self.kept_dead);
        drop(rows);
        for count in self.kept_dead.iter().filter(|count| count.rows > 0) {
            volume.table.drop_live(count.segment, count.block, count.rows, count.replacement);
        }
    }

    /// Try again to remove an output a failure left behind
    pub fn clear_stray(&self) {
        let volume = &self.state.volumes[self.volume];
        let stray = hold(&volume.partial_output).take();
        if let Some(output) = stray {
            self.leave_output(volume, output);
        }
    }

    /// Remove an output, or keep it listed, as a stray that holds every grave back until it's gone
    pub fn leave_output(&self, volume: &Volume<Backend>, output: RewriteOutput<Backend::File>) {
        match self.state.remove_output(volume, output) {
            Ok(()) => volume.stray_outputs.store(0, Ordering::Release),
            Err(output) => {
                *hold(&volume.partial_output) = Some(output);
                volume.stray_outputs.store(1, Ordering::Release);
            },
        }
    }

    /// Give the rewrite up: the input stays, and the output goes whatever name it has reached
    pub fn abandon(&mut self) {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        hold(&volume.partial_output).take();
        if let Some(output) = self.output.take() {
            self.leave_output(volume, output);
        }
        self.target = None;
        volume.is_rewriting.store(false, Ordering::Release);
    }
}

/// Check a row's counts say a rewrite would win back its share of the rows or the bytes, past the bytes already freed in
/// place. Damage and covers answer for more than their rows
pub fn is_rewrite_ready(row: &SummaryRow<'_>, freed_bytes: u64) -> bool {
    if row.facts.is_damaged || row.facts.has_covers || row.facts.block_count == 0 {
        return false;
    }
    let (mut rows, mut dead_rows, mut live_graves, mut bytes, mut dead_bytes) = (0, 0, 0, 0, 0);
    let mut cursor = row.cursor();
    for _column in 0..row.column_count {
        let Some(column) = cursor.column() else {
            return false;
        };
        rows += column.row_count.0;
        dead_rows += column.dead_rows.0;
        live_graves += column.grave_count.0.saturating_sub(column.dead_graves.0);
        bytes += column.live_bytes.0 + column.dead_bytes.0;
        dead_bytes += column.dead_bytes.0;
    }
    let won_back = dead_bytes.saturating_sub(freed_bytes);
    REWRITE_DIVISOR * (dead_rows + live_graves) >= rows || (bytes > 0 && REWRITE_DIVISOR * won_back >= bytes)
}

/// Check a row's graves make up the share that would trigger a rewrite, so deciding first can save the copy
pub fn is_grave_heavy(row: &SummaryRow<'_>) -> bool {
    let (mut rows, mut graves) = (0, 0);
    let mut cursor = row.cursor();
    for _column in 0..row.column_count {
        let Some(column) = cursor.column() else {
            return false;
        };
        rows += column.row_count.0;
        graves += column.grave_count.0;
    }
    REWRITE_DIVISOR * graves >= rows
}
