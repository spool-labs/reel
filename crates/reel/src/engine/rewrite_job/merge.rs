//! Deciding a merge's rows: which go across, which drop, and where each output's records start

use std::array;
use std::io::{Error, ErrorKind, IoSlice, Result};
use std::mem;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::maintenance_report::{
    PickOutcome, UnitKind,
};
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, WritePlace};

use crate::engine::read_path::SealedRow;
use crate::engine::open_segment::output_path;
use crate::engine::seal_job::MOST_SEGMENT_BLOCKS;
use crate::engine::volume::{GRAVE_SLOT, MOST_MERGE_OUTPUTS, RewriteOutput, Volume};
use crate::engine::walk_job::add_count;
use crate::guard::hold;
use crate::io::{Io, IoFile};
use crate::row_kind::RowKind;
use crate::segment::footer_writer::ColumnStart;
use crate::index::key_hash::hash_key;
use crate::segment::row_block::FooterRow;
use crate::segment::segment_table::SegmentPin;
use crate::segment::segment_tail::{MOST_MERGE_SEGMENTS, SegmentNumber};

use crate::engine::rewrite_job::{MERGE_STEP_ROWS, MergeTarget, RewriteJob, RewritePhase};

impl<Backend: Io> RewriteJob<Backend> {
    /// Read where a merge cuts its output. An overlap merge's inputs span the same keys, so it writes outputs a segment long, and the
    /// next merge takes each whole; a record rewrite cuts into narrow ranges. No output's records reach the row blocks, which start
    /// at `blocks_at`
    pub fn merge_cut(&self, target: MergeTarget) -> u64 {
        let segment_bytes = self.state.volumes[self.volume].segment_bytes.0;
        let cut = if self.picked_as == Some(PickOutcome::Overlap) {
            target.data.0.min(segment_bytes).max(self.cut_floor)
        } else {
            (target.data.0 / MOST_MERGE_OUTPUTS as u64).max(self.cut_floor)
        };
        cut.min(target.blocks_at.0)
    }

    /// Create the data output and start every input over. It reserves the inputs' whole length, so a merge holds that much
    /// twice until its footer cuts the output down
    pub fn open_output(&mut self, volume: &Volume<Backend>, target: MergeTarget) -> Result<()> {
        self.cut_bytes = self.merge_cut(target);
        let output = self.create_output(volume, ByteCount(target.length.0.min(self.cut_bytes)), 0)?;
        self.output = Some(output);
        let base = self.lowest_place_base(volume)?;
        self.writer.start(target.blocks_at, base);
        self.writer.set_marks(target.marks);
        self.clear(target);
        self.phase = RewritePhase::Copying;
        Ok(())
    }

    /// Create an output file under a new number, listed for a close to clear away
    pub fn create_output(&self, volume: &Volume<Backend>, length: ByteCount, slot: usize) -> Result<RewriteOutput<Backend::File>> {
        let Ok(number) = self.state.next_segment_number() else {
            return Err(Error::from(ErrorKind::StorageFull));
        };
        let root = number.0 as usize % volume.roots.len();
        let file = self.state.io.create_file(&output_path(&volume.roots[root], number), length)?;
        volume.lanes[self.lane].output_floor.fetch_min(number.0, Ordering::AcqRel);
        let output = RewriteOutput { file, root, number, is_named: false };
        hold(&volume.lanes[self.lane].partial_outputs)[slot] = Some(output.clone());
        Ok(output)
    }

    /// Find the place every input's rows count from, which an output's rows can count from too
    pub fn lowest_place_base(&self, volume: &Volume<Backend>) -> Result<WritePlace> {
        let mut base = WritePlace(u64::MAX);
        for number in self.input_numbers() {
            let Some(pin) = volume.table.pin(*number) else {
                return Err(Error::from(ErrorKind::NotFound));
            };
            base = base.min(pin.head.place_base);
        }
        Ok(base)
    }

    /// Start a pass over the inputs
    pub fn clear(&mut self, target: MergeTarget) {
        self.target = Some(target);
        self.totals = None;
        self.grave_totals = None;
        self.is_drained = false;
        self.used = 0;
        self.records_end = FileOffset(0);
        self.moved = 0;
        self.edges_column = None;
        self.last_column = None;
        self.output_column = None;
        self.grave_column = None;
        self.lowest_dropped = WritePlace(u64::MAX);
        self.highest_replacement = WritePlace(0);
        self.dropped_rows = 0;
        self.dropped_graves = 0;
        self.park_waits = 0;
        self.data_slot = 0;
        self.is_cutting = false;
        self.output_rows = 0;
        self.output_last_key.clear();
        self.grave_last_key.clear();
        self.staged.clear();
        self.staged_columns.clear();
        self.staged_keys.clear();
        self.kept_dead.clear();
        for input in &mut self.inputs {
            input.rewind();
        }
    }

    /// Run one bounded step, and give the merge up on any failure, which leaves the inputs as they were
    pub fn step(&mut self) {
        // a claimed pick maps and carves here, on the thread that runs it, as a unit of its own
        if self.claimed.is_some() {
            let started = self.state.io.now();
            let need = self.admitted.map_or(0, |(admit, _bound)| admit.need.0);
            if self.begin() {
                self.state.note_unit(UnitKind::Start, need, started);
            }
            return;
        }
        let Some(target) = self.target else {
            return;
        };
        let started = self.state.io.now();
        self.unit = (UnitKind::MergeRows, 0);
        let phase = self.phase;
        let outcome = match self.phase {
            RewritePhase::Deciding | RewritePhase::Copying if !self.is_drained && !self.is_cutting => self.merge_rows(),
            RewritePhase::Deciding => self.end_deciding(target),
            RewritePhase::Placing => self.place_rows(),
            RewritePhase::Copying => self.finish_rows(),
            RewritePhase::Moving => self.move_blocks(target),
            RewritePhase::Draining => self.drain_step(),
            RewritePhase::Installing => self.install(),
        };
        // the row blocks a pass reads, so a merge's deciding and placing reads show beside its copying one
        let pass = match phase {
            RewritePhase::Deciding => 0,
            RewritePhase::Placing => 1,
            _ => 2,
        };
        let read: u64 = self.inputs.iter_mut().map(|input| mem::take(&mut input.read_bytes)).sum();
        self.state.volumes[self.volume].footer_reads[pass].fetch_add(read, Ordering::Relaxed);
        self.state.note_unit(self.unit.0, self.unit.1, started);
        if outcome.is_err() {
            // a lost or rotten block fails every merge it joins, so its segment is damage the way a get would find it
            if let Some(segment) = self.unreadable.take() {
                self.state.damage_unread(self.volume, segment);
            }
            self.abandon();
        }
    }

    /// Decide a step's rows, taking them in key order across the inputs, a key's newest first
    pub fn merge_rows(&mut self) -> Result<()> {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        let numbers = self.numbers;
        let pins: [Option<SegmentPin<'_, Backend::File>>; MOST_MERGE_SEGMENTS] =
            array::from_fn(|index| (index < self.input_count).then(|| volume.table.pin(numbers[index])).flatten());
        for (index, pin) in pins.iter().enumerate().take(self.input_count) {
            let Some(pin) = pin else {
                return Err(Error::from(ErrorKind::NotFound));
            };
            if self.inputs[index].row.is_none() && self.inputs[index].length == 0 {
                self.inputs[index].advance(volume, pin).map_err(|error| self.note_unreadable(index, error))?;
            }
        }

        for _row in 0..MERGE_STEP_ROWS {
            self.unit.1 += 1;
            let Some(index) = self.next_input() else {
                self.is_drained = true;
                return Ok(());
            };
            let Some(pin) = &pins[index] else {
                return Err(Error::from(ErrorKind::NotFound));
            };
            let input = &self.inputs[index];
            let (Some(row), column) = (input.row, input.column) else {
                return Err(Error::from(ErrorKind::NotFound));
            };
            self.key.set_from(&input.key)?;
            self.row_input = index;
            self.row_at = input.base + input.ordinal();
            // the lists are sized to the summary's rows, and a row past its input's count says the summary counted short
            if self.is_ordered && input.ordinal() >= input.block_rows {
                return Err(Error::from(ErrorKind::InvalidData));
            }
            self.decide(volume, &pins, pin, column, &row)?;
            // a cut leaves the row where it is, for the next output to take from the top
            if self.is_cutting {
                self.last_column = None;
                return Ok(());
            }
            self.inputs[index].advance(volume, pin).map_err(|error| self.note_unreadable(index, error))?;
        }
        Ok(())
    }

    /// Find the input whose row comes next: lowest column, then lowest key, then highest place
    pub fn next_input(&self) -> Option<usize> {
        let mut best: Option<usize> = None;
        for (index, input) in self.inputs[..self.input_count].iter().enumerate() {
            let Some(row) = input.row else {
                continue;
            };
            let is_first = best.is_none_or(|at| {
                let other = &self.inputs[at];
                let order = (input.column, input.key.as_slice()).cmp(&(other.column, other.key.as_slice()));
                order.then_with(|| other.row.map(|row| row.place).cmp(&Some(row.place))).is_lt()
            });
            if is_first {
                best = Some(index);
            }
        }
        best
    }

    /// Decide one row
    pub fn decide(
        &mut self,
        volume: &Volume<Backend>,
        pins: &[Option<SegmentPin<'_, Backend::File>>],
        pin: &SegmentPin<'_, Backend::File>,
        column: ColumnId,
        row: &FooterRow,
    ) -> Result<()> {
        // its cover left the pool before this segment could merge, so nothing older sits under it
        if row.kind == RowKind::RangeDelete {
            self.dropped_rows += 1;
            return Ok(());
        }
        let replacement = self.replacement(volume, column, row)?;
        match replacement {
            Some(place) if self.can_drop(place, row) => {
                self.dropped_graves += u64::from(row.kind == RowKind::Grave);
                self.lowest_dropped = self.lowest_dropped.min(row.place);
                self.highest_replacement = self.highest_replacement.max(place);
                self.dropped_rows += 1;
                Ok(())
            },
            None if row.kind == RowKind::Grave && !self.is_unwalked && self.hides_nothing(volume, column, row)? => {
                self.dropped_graves += 1;
                self.lowest_dropped = WritePlace(0);
                self.highest_replacement = self.highest_replacement.max(row.place);
                self.dropped_rows += 1;
                Ok(())
            },
            _kept if self.phase == RewritePhase::Deciding => self.mark_kept(row),
            // a grave output that's full leaves the rest of the graves as footer rows in the data output
            _kept if row.kind == RowKind::Grave && self.grave_writer.has_block_room() => {
                self.keep_grave(volume, pins, column, row, replacement)
            },
            _kept if self.is_cut_due() => {
                self.is_cutting = true;
                Ok(())
            },
            _kept => self.keep(volume, pins, pin, column, row, replacement),
        }
    }

    /// Check the output has reached its share of the merge, and another may follow it. A cut only falls before a key's first
    /// row, so a key's versions stay in one output
    pub fn is_cut_due(&self) -> bool {
        // an offset-order copy cuts where the deciding pass did, by the row's place in its input
        if self.is_ordered {
            return self.output_of(self.row_input, self.inputs[self.row_input].ordinal()) > self.data_slot;
        }
        let bytes = self.records_end.0 + self.used as u64;
        let is_full = bytes >= self.cut_bytes || self.writer.entries().len() + 2 >= MOST_SEGMENT_BLOCKS.0 as usize;
        self.is_top_row && self.output_rows > 0 && self.data_slot + 1 < MOST_MERGE_OUTPUTS && is_full
    }

    /// Read which output a row goes to: the cuts at or before its place in its input
    pub fn output_of(&self, input: usize, ordinal: u64) -> usize {
        self.cut_ordinals[..self.cuts].iter().filter(|cut| ordinal >= cut[input]).count()
    }

    /// Cut the deciding pass's output before the row in hand, at each input's place
    pub fn note_cut(&mut self) {
        for (input, at) in self.inputs[..self.input_count].iter().zip(&mut self.cut_ordinals[self.cuts]) {
            *at = input.ordinal();
        }
        self.cuts += 1;
        self.decide_bytes = 0;
        self.decide_rows = 0;
    }

    /// Find the newest place above a row that replaces it: the key's first row across the inputs, or a row in a finished
    /// segment. A row replaced only in an open or unfinished segment stays, and that segment's walk counts it in the output
    pub fn replacement(&mut self, volume: &Volume<Backend>, column: ColumnId, row: &FooterRow) -> Result<Option<WritePlace>> {
        let is_top = self.last_column != Some(column) || self.last_key != self.key;
        self.is_top_row = is_top;
        if is_top {
            self.last_column = Some(column);
            self.last_key.set_from(&self.key)?;
            self.top_place = row.place;
            // a cover whose walk has counted what it hides replaces a row as a newer row does
            self.cover_place = if self.is_unwalked { None } else { volume.covers.newest_applied(column, &self.key) };
            // an unwalked merge goes by its own rows, so it runs at the disk's rate and leaves the rest to the output's walk
            self.newest_place = match self.newest_outside(volume, column) {
                Ok(found) => found.map(|found| found.row.place),
                // a version a damaged block hides replaces nothing, and the row stays
                Err(StoreError::Corruption { .. }) => None,
                Err(_error) => return Err(Error::from(ErrorKind::Interrupted)),
            };
        }
        let own = (!is_top).then_some(self.top_place);
        Ok(own.max(self.newest_place).max(self.cover_place).filter(|place| *place > row.place))
    }

    /// Find a key's newest row in a finished segment outside the merge
    pub fn newest_outside(
        &self,
        volume: &Volume<Backend>,
        column: ColumnId,
    ) -> std::result::Result<Option<SealedRow>, StoreError> {
        if self.is_unwalked {
            return Ok(None);
        }
        self.state.newest_walked(volume, column, &self.key, &self.numbers[..self.input_count])
    }

    /// Check a replaced row can go: no live cue sits at or above it and below its newest replacement
    pub fn can_drop(&self, replacement: WritePlace, row: &FooterRow) -> bool {
        !self.state.cues.has_cue_between(self.volume, row.place, replacement)
    }

    /// Check a grave with no newer row of its key has nothing left to hide, by this key's own rows and nothing wider. An older
    /// grave keeps it too: that one's walk count stands on this row, and a later write of the key would count it twice. It
    /// drops first, as any row with a newer one does. A file on its way out comes back at a crash, so no grave drops while
    /// one is parked. A cue below the grave can read what it covers, and damage could be hiding an older version
    pub fn hides_nothing(&self, volume: &Volume<Backend>, column: ColumnId, row: &FooterRow) -> Result<bool> {
        let state = &self.state;
        if volume.segments_parked.load(Ordering::Acquire) > 0
            || volume.stray_outputs.load(Ordering::Acquire) > 0
            || state.cues.has_cue_below(self.volume, row.place)
            || volume.damage.covering(column, &self.key).is_some()
        {
            return Ok(false);
        }
        // a complete column frees a superseded row's slot, so its footers answer for older rows as any column's do
        match state.has_older_row(volume, column, &self.key, row.place, &self.numbers[..self.input_count]) {
            Ok(has_older) => Ok(!has_older),
            Err(StoreError::Corruption { .. }) => Ok(false),
            Err(_error) => Err(Error::from(ErrorKind::Interrupted)),
        }
    }

    /// Carry a row into the data output, and note it dead when a cue is all that keeps it, since no walk will count it again
    pub fn keep(
        &mut self,
        volume: &Volume<Backend>,
        pins: &[Option<SegmentPin<'_, Backend::File>>],
        pin: &SegmentPin<'_, Backend::File>,
        column: ColumnId,
        row: &FooterRow,
        replacement: Option<WritePlace>,
    ) -> Result<()> {
        let Some(output) = self.output.clone() else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        if self.output_column != Some(column) {
            if self.output_column.is_some() {
                self.writer.end_column(&self.output_last_key)?;
            }
            self.load_edges(volume, pins, column)?;
            let start = ColumnStart {
                column,
                lowest: &self.key,
                highest: &self.highest,
                block_bytes: self.state.block_bytes_of(column),
            };
            self.writer.start_column(&output.file, &start)?;
            self.output_column = Some(column);
        }
        let offset = match self.placed_offset() {
            Some(offset) => offset,
            None => {
                volume.keyed_copies.fetch_add(1, Ordering::Relaxed);
                self.copy_record(&pin.file, &output.file, row)?
            },
        };
        let kept = FooterRow { offset, ..*row };
        self.writer.push_row(&output.file, &self.key, &kept)?;
        self.output_rows += 1;
        self.output_last_key.set_from(&self.key)?;
        let block = self.writer.entries().len() as u32;
        self.give_slot(volume, output.number, column, block);
        self.note_home(output.number, block);
        self.note_kept_dead(output.number, column, block, &kept, replacement)
    }

    /// Carry a grave into the output of its own, a footer row with no bytes behind it
    pub fn keep_grave(
        &mut self,
        volume: &Volume<Backend>,
        pins: &[Option<SegmentPin<'_, Backend::File>>],
        column: ColumnId,
        row: &FooterRow,
        replacement: Option<WritePlace>,
    ) -> Result<()> {
        if self.grave_output.is_none() {
            let output = self.create_output(volume, ByteCount(0), GRAVE_SLOT)?;
            let base = self.lowest_place_base(volume)?;
            self.grave_writer.start(FileOffset(0), base);
            self.grave_output = Some(output);
        }
        let Some(output) = self.grave_output.clone() else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        if self.grave_column != Some(column) {
            if self.grave_column.is_some() {
                self.grave_writer.end_column(&self.grave_last_key)?;
            }
            self.load_edges(volume, pins, column)?;
            let start = ColumnStart {
                column,
                lowest: &self.key,
                highest: &self.highest,
                block_bytes: self.state.block_bytes_of(column),
            };
            self.grave_writer.start_column(&output.file, &start)?;
            self.grave_column = Some(column);
        }
        let kept = FooterRow {
            offset: FileOffset(0),
            length: ByteCount(0),
            ..*row
        };
        self.grave_writer.push_row(&output.file, &self.key, &kept)?;
        self.grave_last_key.set_from(&self.key)?;
        let block = self.grave_writer.entries().len() as u32;
        self.give_slot(volume, output.number, column, block);
        self.note_home(output.number, block);
        self.note_kept_dead(output.number, column, block, &kept, replacement)
    }

    /// Give a kept row a slot in its column's table, before the inputs go, so a read that meets the change finds it twice
    pub fn give_slot(&mut self, volume: &Volume<Backend>, number: SegmentNumber, column: ColumnId, block: u32) {
        if let Some(table) = self.state.complete_table(column) {
            let hash = hash_key(column, &self.key);
            if self.last_slot != Some((hash, number, block)) {
                table.insert(hash, number, block as u16, |segment| self.state.is_slot_gone(volume, segment));
                self.last_slot = Some((hash, number, block));
                if self.stale_given.0 != Some(column) {
                    self.note_stale();
                    self.stale_given.0 = Some(column);
                }
                self.stale_given.1 += 1;
            }
        }
    }

    /// Hand the slots given since the last note to their table's stale count
    pub fn note_stale(&mut self) {
        let (column, given) = std::mem::take(&mut self.stale_given);
        if let Some(table) = column.and_then(|column| self.state.complete_table(column)) {
            table.note_stale(given);
        }
    }

    /// Note a row a cue alone keeps
    pub fn note_kept_dead(
        &mut self,
        output: SegmentNumber,
        column: ColumnId,
        block: u32,
        kept: &FooterRow,
        replacement: Option<WritePlace>,
    ) -> Result<()> {
        if replacement.is_some() && !self.is_unwalked {
            if self.kept_dead.len() == self.kept_dead.capacity() {
                return Err(Error::from(ErrorKind::OutOfMemory));
            }
            add_count(&mut self.kept_dead, output, column, block, kept, replacement)?;
        }
        Ok(())
    }

    /// Read a column's range across the inputs, which holds every row that stays
    pub fn load_edges(
        &mut self,
        volume: &Volume<Backend>,
        pins: &[Option<SegmentPin<'_, Backend::File>>],
        column: ColumnId,
    ) -> Result<()> {
        if self.edges_column == Some(column) {
            return Ok(());
        }
        let table = &volume.table;
        let mut has_edges = false;
        for (index, pin) in pins.iter().enumerate().take(self.input_count) {
            let segment = self.numbers[index];
            let edges = table.column_blocks(segment, column).and_then(|(first, count)| {
                let last = first + count.checked_sub(1)?;
                Some([table.walk_block(segment, first)?.entry, table.walk_block(segment, last)?.entry])
            });
            let (Some(lines), Some(pin)) = (edges, pin) else {
                continue;
            };
            let (lowest, highest) = (&mut self.edge_lowest, &mut self.edge_highest);
            let base = pin.head.place_base;
            let is_read = self.state.job_footer_readers.with(|reader| reader.edge_keys(&pin.file, &lines, base, lowest, highest))?;
            if !is_read {
                return Err(Error::from(ErrorKind::InvalidData));
            }
            if !has_edges || self.edge_lowest < self.lowest {
                self.lowest.set_from(&self.edge_lowest)?;
            }
            if !has_edges || self.edge_highest > self.highest {
                self.highest.set_from(&self.edge_highest)?;
            }
            has_edges = true;
        }
        self.edges_column = Some(column);
        Ok(())
    }

    /// Copy a record as stored, and return where it landed
    pub fn copy_record(&mut self, from: &Backend::File, to: &Backend::File, row: &FooterRow) -> Result<FileOffset> {
        let length = row.length.0 as usize;
        if length > self.buffer.len() - self.used {
            self.flush(to)?;
        }
        let at = FileOffset(self.records_end.0 + self.used as u64);
        // the row blocks sit past this, and a key's versions kept for a cue are all that can reach them
        if self.target.is_none_or(|target| at.0 + length as u64 > target.blocks_at.0) {
            return Err(Error::from(ErrorKind::OutOfMemory));
        }
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

    /// End the deciding: copy from the first row if one would drop, and leave the segments alone if none would
    pub fn end_deciding(&mut self, target: MergeTarget) -> Result<()> {
        // several inputs becoming one win back overlap whatever they drop
        if self.dropped_rows == 0 && self.input_count == 1 {
            self.note_gave_up();
            return Err(Error::from(ErrorKind::Interrupted));
        }
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        self.open_output(volume, target)?;
        if self.is_ordered {
            // every output the deciding pass cut takes its records before the copying pass reaches it
            for slot in 1..=self.cuts {
                self.slot_outputs[slot] = Some(self.create_output(volume, ByteCount(self.cut_bytes), slot)?);
            }
            self.outputs_end = [0; MOST_MERGE_OUTPUTS];
            self.placing_input = 0;
            self.stripe.clear();
            // a pass given up partway through a copy leaves its place in the stripe, which would skip a whole input here
            self.stripe_copied = None;
            self.phase = RewritePhase::Placing;
        }
        Ok(())
    }

    /// Note a kept row of a deciding pass, for the copy to take in offset order
    pub fn mark_kept(&mut self, row: &FooterRow) -> Result<()> {
        if self.is_ordered && row.kind == RowKind::Value {
            // a cut falls only before a key's first row, so a key's versions stay in one output
            let is_full = self.decide_bytes >= self.cut_bytes || self.decide_rows >= self.cut_rows;
            if self.is_top_row && self.decide_rows > 0 && is_full && self.cuts + 1 < MOST_MERGE_OUTPUTS {
                self.note_cut();
            }
            self.decide_bytes += row.length.0;
            self.decide_rows += 1;
            let ordinal = self.row_at as usize;
            // the marks are sized to the input's rows, and a row past them says the summary counted short
            let Some(word) = self.marks.get_mut(ordinal / u64::BITS as usize) else {
                return Err(Error::from(ErrorKind::InvalidData));
            };
            *word |= 1 << (ordinal % u64::BITS as usize);
        }
        Ok(())
    }

    /// Check a row was marked
    pub fn is_marked(&self, ordinal: usize) -> bool {
        self.marks.get(ordinal / u64::BITS as usize).is_some_and(|word| word >> (ordinal % u64::BITS as usize) & 1 != 0)
    }

    /// Read where the row in hand's record landed, if a stripe copied it
    pub fn placed_offset(&self) -> Option<FileOffset> {
        let ordinal = self.row_at as usize;
        self.placed.get(ordinal).filter(|_placed| self.is_ordered && self.is_marked(ordinal)).map(|placed| FileOffset(u64::from(*placed)))
    }
}
