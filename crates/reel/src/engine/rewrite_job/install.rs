//! Finishing a merge's outputs and swapping them in for the inputs, or giving the merge up

use std::array;
use std::io::{Error, ErrorKind, IoSlice, Result};
use std::mem;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_api::maintenance_report::{
    JobKind, OTHER_MERGE, OVERLAP_MERGE, PickOutcome, RECORD_PAST_EARLY_BAR, RECORD_UNDER_EARLY_BAR, UnitKind, WriteSource,
};
use reel_api::units::{ByteCount, FileOffset, RootId, RowCount, WritePlace};

use crate::engine::dead_bits::INEXACT_KEPT;
use crate::engine::compact_job::park;
use crate::engine::engine_state::EngineState;
use crate::engine::recovery::save_cleared_spans;
use crate::engine::open_segment::{data_path, output_path};
use crate::engine::volume::{GRAVE_SLOT, GaveUp, MOST_MERGE_OUTPUTS, NO_SEAL, OutputSlots, RewriteOutput, Volume};
use crate::guard::hold;
use crate::io::device_error::is_device_error;
use crate::io::{Io, IoFile, IoTask};
use crate::fixed_list::ListFull;
use crate::segment::column_directory::Listing;
use crate::segment::footer_writer::{FooterTotals, FooterWriter};
use crate::segment::row_block::ROW_BLOCK_BYTES;
use crate::segment::segment_table::shard_of;
use crate::segment::segment_tail::{ColumnTail, MOST_MERGE_SEGMENTS, SegmentNumber};
use crate::segment::summary::{ColumnFacts, SegmentFacts, SummaryRow, SummaryRows};

use crate::engine::rewrite_job::{BAR_SCALE, EARLY_BAR, MOST_PARK_WAIT, MergeTarget, OUTGROW_STEP_BYTES, RewriteJob, RewritePhase, StagedColumn, StagedOutput, rows_bytes}; use crate::engine::rewrite_job::pick::has_disk_room;

impl<Backend: Io> RewriteJob<Backend> {
    /// Check a lone input sits in the table shard a stuck seal waits on, so moving it to an output elsewhere frees the slot
    pub fn is_leaving_stuck_shard(&self) -> bool {
        self.input_count == 1 && shard_of(self.numbers[0]) == self.state.volumes[self.volume].stuck_shard.load(Ordering::Acquire)
    }

    /// Check a merge that dropped nothing would leave its inputs' overlap as it was. One input wins back nothing unless it was
    /// cut or it leaves a stuck seal's shard. Several overlap afterwards as one chain of data outputs, which follow each other in
    /// key order, and a grave output beside them: a data and a grave output going back in as two inputs would come out as the
    /// same two again
    pub fn is_futile(&self) -> bool {
        if self.input_count == 1 {
            return !self.is_cutting && self.staged.is_empty() && !self.is_leaving_stuck_shard();
        }
        1 + usize::from(self.grave_output.is_some()) >= self.input_count
    }

    /// End the copying. Several inputs becoming fewer wins back overlap whatever it dropped, and a merge that dropped nothing
    /// and leaves as much overlap has nothing to install
    pub fn finish_rows(&mut self) -> Result<()> {
        self.unit = (UnitKind::Footer, self.writer.entries().len() as u64);
        let Some(output) = self.output.clone() else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        if self.dropped_rows == 0 && self.rebuild.is_none() && self.is_futile() {
            self.note_gave_up();
            return Err(Error::from(ErrorKind::Interrupted));
        }
        self.flush(&output.file)?;
        if self.output_column.is_some() {
            self.writer.end_column(&self.output_last_key)?;
        }
        self.writer.close_block(&output.file)?;
        self.phase = RewritePhase::Moving;
        Ok(())
    }

    /// Move a buffer of row blocks down to the records' end, and write the footers once they're all there
    pub fn move_blocks(&mut self, target: MergeTarget) -> Result<()> {
        let Some(output) = self.output.clone() else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        let distance = target.blocks_at.0 - self.records_end.0;
        let from = target.blocks_at.0 + self.moved;
        let left = self.writer.position().0 - from;
        if distance == 0 || left == 0 || self.writer.columns().is_empty() {
            self.unit = (UnitKind::Footer, self.writer.entries().len() as u64);
            return if self.is_cutting { self.cut_output(target, &output, distance) } else { self.finish_outputs(&output, distance) };
        }

        let part = left.min(self.buffer.len() as u64) as usize;
        self.unit = (UnitKind::BlockMove, (part as u64).div_ceil(ROW_BLOCK_BYTES as u64));
        output.file.read_exact_at(FileOffset(from), &mut self.buffer[..part])?;
        output.file.write_at(FileOffset(from - distance), &[IoSlice::new(&self.buffer[..part])])?;
        self.moved += part as u64;
        Ok(())
    }

    /// End an output that isn't the last, and start the next. It goes in the table now, in no list, since the next output needs
    /// the writer. Its tail marks it partial, so open keeps it only beside the last output's
    pub fn cut_output(&mut self, target: MergeTarget, output: &RewriteOutput<Backend::File>, distance: u64) -> Result<()> {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        let numbers = self.numbers;
        self.writer.move_down(ByteCount(distance));
        self.writer.is_output = true;
        self.writer.is_partial = true;
        self.writer.is_unwalked = self.is_unwalked;
        let totals = self.writer.finish(&output.file, output.number, self.records_end, &[], state.record_key, &numbers[..self.input_count])?;
        self.name_output(output, self.data_slot)?;
        if !volume.table.install(output.file.clone(), RootId(output.root as u8), &totals.head, self.writer.columns(), self.writer.entries()) {
            return Err(Error::from(ErrorKind::OutOfMemory));
        }
        volume.dead_bits.begin(output.number, self.writer.entries().len() as u32, totals.most_block_rows);
        self.stage_output(output.number, &totals)?;

        self.data_slot += 1;
        // an offset-order copy placed the next output's records already
        let next = match self.slot_outputs.get_mut(self.data_slot).and_then(Option::take) {
            Some(placed) => placed,
            None => self.create_output(volume, ByteCount(self.cut_bytes), self.data_slot)?,
        };
        self.output = Some(next);
        let base = self.lowest_place_base(volume)?;
        self.writer.start(target.blocks_at, base);
        self.writer.set_marks(target.marks);
        self.records_end = FileOffset(if self.is_ordered { self.outputs_end[self.data_slot] } else { 0 });
        self.used = 0;
        self.moved = 0;
        self.output_rows = 0;
        self.output_column = None;
        self.is_cutting = false;
        self.phase = RewritePhase::Copying;
        Ok(())
    }

    /// Swap a rebuilt segment's spans for what its lost blocks leave. Its row is gone, and what stays damaged keeps a row of its own
    pub fn end_rebuild(&mut self, volume: &Volume<Backend>) {
        let Some(segment) = self.rebuild.take() else {
            return;
        };
        volume.damage.drop_segment(segment);
        for range in self.lost_spans.drain(..) {
            volume.damage.add(&range);
        }
        if !volume.damage.holds_segment(segment) {
            hold(&volume.damaged).retain(|number| *number != segment);
            self.state.damaged_segments.fetch_sub(1, Ordering::AcqRel);
        }
        save_cleared_spans(&self.state, volume);
    }

    /// Count what a merge moved, dropped and carried, for the report, and give back the bytes it wrote past its records
    pub fn note_install(&self) -> u64 {
        let state = &self.state;
        let staged = self.staged_columns.iter().map(|column| &column.tail);
        let carried: u64 = staged.chain(self.writer.columns()).chain(self.grave_writer.columns()).map(|tail| tail.grave_count.0).sum();
        let totals = [self.totals, self.grave_totals].into_iter().flatten().map(|totals| totals.file_length.0).sum::<u64>();
        let bytes: u64 = totals + self.staged.iter().map(|staged| staged.facts.file_length.0).sum::<u64>();
        state.graves_dropped.fetch_add(self.dropped_graves, Ordering::Relaxed);
        state.graves_carried.fetch_add(carried, Ordering::Relaxed);
        state.empty_rewrites.fetch_add(u64::from(self.dropped_rows == 0), Ordering::Relaxed);
        state.note_moved(JobKind::Compaction, ByteCount(bytes));
        let mut records: u64 = [self.totals, self.grave_totals].into_iter().flatten().map(|totals| totals.head.data_end.0).sum();
        records += self.staged.iter().map(|staged| staged.facts.data_end.0).sum::<u64>();
        state.note_written(WriteSource::MergeRecords, ByteCount(records));
        state.note_written(WriteSource::MergeFooters, ByteCount(bytes.saturating_sub(records)));
        bytes.saturating_sub(records)
    }

    /// Keep what an output's summary row and list entries will need once the writer has moved on
    pub fn stage_output(&mut self, number: SegmentNumber, totals: &FooterTotals) -> std::result::Result<(), ListFull> {
        let first_column = self.staged_columns.len();
        for (index, tail) in self.writer.columns().iter().enumerate() {
            let facts = self.writer.column_facts(index);
            let lowest_at = self.staged_keys.len();
            self.staged_keys.extend_from_slice(facts.lowest)?;
            let bound_at = self.staged_keys.len();
            self.staged_keys.extend_from_slice(facts.bound)?;
            self.staged_columns.push(StagedColumn {
                tail: *tail,
                lowest_at,
                bound_at,
                end: self.staged_keys.len(),
                live_bytes: facts.live_bytes,
            })?;
        }
        let block_count = self.writer.entries().len() as u32;
        self.staged.push(StagedOutput {
            facts: output_facts(number, totals, self.records_end, block_count, self.is_unwalked),
            newest_place: totals.head.places.highest,
            first_column,
            column_count: self.staged_columns.len() - first_column,
        })
    }

    /// Make the outputs whole, the graves first. The last tail lists the inputs, and open takes that as the merge having
    /// happened, so everything that stays has to be durable by then
    pub fn finish_outputs(&mut self, output: &RewriteOutput<Backend::File>, distance: u64) -> Result<()> {
        let state = Arc::clone(&self.state);
        let numbers = self.numbers;
        let inputs = &numbers[..self.input_count];
        let has_data = !self.writer.columns().is_empty();
        if let Some(graves) = self.grave_output.clone() {
            self.grave_writer.end_column(&self.grave_last_key)?;
            self.grave_writer.close_block(&graves.file)?;
            self.grave_writer.is_output = true;
            self.grave_writer.is_unwalked = self.is_unwalked;
            self.grave_writer.is_partial = has_data;
            let totals = self.grave_writer.finish(&graves.file, graves.number, FileOffset(0), &[], state.record_key, inputs)?;
            self.grave_totals = Some(totals);
            self.name_output(&graves, GRAVE_SLOT)?;
        }
        if has_data {
            self.writer.move_down(ByteCount(distance));
            self.writer.is_output = true;
            self.writer.is_unwalked = self.is_unwalked;
            let totals = self.writer.finish(&output.file, output.number, self.records_end, &[], state.record_key, inputs)?;
            self.totals = Some(totals);
            self.name_output(output, self.data_slot)?;
        }
        self.phase = if self.is_holding { RewritePhase::Draining } else { RewritePhase::Installing };
        Ok(())
    }

    /// Give a whole output a segment's name
    pub fn name_output(&mut self, output: &RewriteOutput<Backend::File>, slot: usize) -> Result<()> {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        let root = &volume.roots[output.root];
        state.io.rename_file(&output_path(root, output.number), &data_path(root, output.number))?;
        // the name has landed from here, whatever the directory's sync says
        let named = RewriteOutput { is_named: true, ..output.clone() };
        hold(&volume.lanes[self.lane].partial_outputs)[slot] = Some(named.clone());
        if slot == GRAVE_SLOT {
            self.grave_output = Some(named);
        } else {
            self.output = Some(named);
        }
        state.io.sync_directory(root)
    }

    /// Swap the inputs for the outputs in every list, save that, and park the inputs. The count moves before the last look:
    /// a read or a cue that takes its point after it has every replacement under that point
    pub fn install(&mut self) -> Result<()> {
        let outputs = self.staged.len() + usize::from(self.output.is_some()) + usize::from(self.grave_output.is_some());
        self.unit = (UnitKind::Install, (self.input_count + outputs) as u64);
        self.install_with(|| {}, || {})
    }

    /// Install, and run some work once the grave output is listed and the inputs are not yet gone, and more once the summary
    /// without the inputs is saved and their files aren't parked yet
    pub fn install_with(&mut self, between: impl FnOnce(), before_park: impl FnOnce()) -> Result<()> {
        self.note_stale();
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        // the claim set park slots aside, so the free ones are this merge's, and a full park clears as reads let go
        if volume.free_park_slots() < self.input_count {
            // the planner clears the park, so it gets the turn this wait is for
            if let Some(task) = state.planner_task.get() {
                task.wake();
            }
            // a merge with every output written gives way only to a stop, since giving up costs the whole write
            if state.is_stopping.load(Ordering::Acquire) {
                return Err(Error::from(ErrorKind::Interrupted));
            }
            let now = state.io.now();
            let since = *self.park_wait_since.get_or_insert(now);
            if now.0.saturating_sub(since.0) > MOST_PARK_WAIT.0 && !self.is_long_wait_noted {
                volume.long_park_waits.fetch_add(1, Ordering::Relaxed);
                self.is_long_wait_noted = true;
            }
            return Ok(());
        }
        self.is_long_wait_noted = false;
        self.park_wait_since = None;
        if state.read_point(volume) < self.highest_replacement {
            return Err(Error::from(ErrorKind::Interrupted));
        }
        state.cues.note_reclaim();
        if state.cues.has_cue_between(self.volume, self.lowest_dropped, self.highest_replacement) {
            return Err(Error::from(ErrorKind::Interrupted));
        }
        let numbers = self.numbers;
        let inputs = &numbers[..self.input_count];
        self.enter_table(volume)?;
        // every output is in a list before the inputs leave theirs, so a read in between finds a row twice at one place, and
        // never not at all
        if let (Some(totals), Some(output)) = (self.grave_totals, &self.grave_output) {
            list_output(&state, volume, output.number, totals.head.places.highest, self.grave_writer.columns().iter());
        }
        for staged in &self.staged {
            let columns = self.staged_columns[staged.first_column..staged.first_column + staged.column_count].iter();
            list_output(&state, volume, staged.facts.segment, staged.newest_place, columns.map(|column| &column.tail));
        }
        if let (Some(totals), Some(output)) = (self.totals, &self.output) {
            list_output(&state, volume, output.number, totals.head.places.highest, self.writer.columns().iter());
        }
        between();
        // an unwalked merge's outputs owe a walk, and reads of finished segments leave them out until it's done
        // a walked merge marks its outputs after the lists, which only holds with no walk target running beside it
        if !self.is_unwalked {
            for staged in &self.staged {
                volume.table.mark_walked(staged.facts.segment);
            }
            for output in [self.output.as_ref(), self.grave_output.as_ref()].into_iter().flatten() {
                volume.table.mark_walked(output.number);
            }
        }
        // the inputs count as leaving before any list drops them, so no check passes over a file still on disk
        let leaving = inputs.len() as u64;
        volume.segments_leaving.fetch_add(leaving, Ordering::AcqRel);
        // a column every row of which went has no entry to swap in
        for directory in state.directories.iter().flatten() {
            if !directory.replace(None, inputs) {
                volume.fail();
            }
        }

        // a cover goes with its segment: the rows it hid there just left, and nothing older sits under it anywhere else
        for input in inputs {
            volume.covers.remove_segment(*input);
        }
        let gone = rows_bytes(&hold(&volume.summary), inputs.iter().copied());
        for input in inputs {
            self.state.filter_arena.drop_segment(*input);
            volume.dead_bits.end(*input);
        }
        let retired: [_; MOST_MERGE_SEGMENTS] = array::from_fn(|index| inputs.get(index).and_then(|number| volume.table.retire(*number)));
        let footers = self.note_install();
        if self.picked_as == Some(PickOutcome::Overlap) {
            let elapsed = state.io.now().0.saturating_sub(self.begun_at.0);
            volume.merge_interval.store((elapsed / inputs.len().max(1) as u64).max(1), Ordering::Relaxed);
        }
        volume.lanes[self.lane].rewrites_done.fetch_add(1, Ordering::AcqRel);
        self.summarize(volume, inputs);
        let staged = self.staged.iter().map(|staged| staged.facts.segment);
        let last = self.totals.and(self.output.as_ref()).map(|output| output.number);
        let graves = self.grave_totals.and(self.grave_output.as_ref()).map(|output| output.number);
        let written = rows_bytes(&hold(&volume.summary), staged.chain(last).chain(graves));
        // a record rewrite counts by what its inputs won back, against the early bar
        let kind = match self.picked_as {
            Some(PickOutcome::Record) if BAR_SCALE * gone.dead >= EARLY_BAR * gone.file => RECORD_PAST_EARLY_BAR,
            Some(PickOutcome::Record) => RECORD_UNDER_EARLY_BAR,
            Some(PickOutcome::Overlap) => OVERLAP_MERGE,
            _other => OTHER_MERGE,
        };
        volume.note_rewrite_bytes(gone, written, kind);
        self.settle_pace(volume, written.file);
        volume.rewrite_footers_by_share[kind].fetch_add(footers, Ordering::Relaxed);
        self.end_rebuild(volume);
        let saved = state.save_summary(volume);

        *hold(&volume.lanes[self.lane].partial_outputs) = Default::default();
        // a merge that kept no data never made its file a segment
        if let (None, Some(output)) = (self.totals, self.output.take()) {
            let mut slots: OutputSlots<Backend::File> = Default::default();
            slots[0] = Some(output);
            self.leave_outputs(volume, slots);
        }
        self.output = None;
        self.grave_output = None;
        self.target = None;
        volume.lanes[self.lane].output_floor.store(NO_SEAL, Ordering::Release);
        volume.lanes[self.lane].is_rewriting.store(false, Ordering::Release);
        self.release_promise(volume);
        *hold(&volume.lanes[self.lane].merging) = [SegmentNumber(0); MOST_MERGE_SEGMENTS];
        *hold(&volume.lanes[self.lane].inputs) = [SegmentNumber(0); MOST_MERGE_SEGMENTS];
        self.release_hold(volume);
        volume.segments_rewritten.fetch_add(1, Ordering::AcqRel);
        volume.early_bar_rewrites.fetch_add(self.bars_fired.early, Ordering::Relaxed);
        volume.plain_bar_rewrites.fetch_add(self.bars_fired.plain, Ordering::Relaxed);
        volume.wants_compaction.store(true, Ordering::Release);
        before_park();
        for (segment, retired) in inputs.iter().zip(retired) {
            if let Some(retired) = retired {
                park(volume, Some(retired), *segment, saved.is_ok());
            }
        }
        volume.segments_leaving.fetch_sub(leaving, Ordering::AcqRel);
        self.release_region();
        saved
    }

    /// Put the outputs in the table, both or neither. No list holds them yet, so no read can have found the first
    pub fn enter_table(&self, volume: &Volume<Backend>) -> Result<()> {
        if let (Some(totals), Some(output)) = (self.totals, &self.output)
            && !volume.table.install(output.file.clone(), RootId(output.root as u8), &totals.head, self.writer.columns(), self.writer.entries())
        {
            return Err(Error::from(ErrorKind::OutOfMemory));
        }
        if let (Some(totals), Some(output)) = (self.grave_totals, &self.grave_output)
            && !volume.table.install(output.file.clone(), RootId(output.root as u8), &totals.head, self.grave_writer.columns(), self.grave_writer.entries())
        {
            if let (Some(_totals), Some(output)) = (self.totals, &self.output)
                && let Some(retired) = volume.table.retire(output.number)
            {
                volume.table.free_retired(&retired);
            }
            return Err(Error::from(ErrorKind::OutOfMemory));
        }
        // a walk can charge an output's rows once the table has it, so its bits begin first
        if let (Some(totals), Some(output)) = (self.totals, &self.output) {
            volume.dead_bits.begin(output.number, self.writer.entries().len() as u32, totals.most_block_rows);
        }
        if let (Some(totals), Some(output)) = (self.grave_totals, &self.grave_output) {
            volume.dead_bits.begin(output.number, self.grave_writer.entries().len() as u32, totals.most_block_rows);
        }
        Ok(())
    }

    /// Replace the inputs' summary rows with the outputs'. Their counts come from the rows they kept, and they start walked
    pub fn summarize(&mut self, volume: &Volume<Backend>, inputs: &[SegmentNumber]) {
        let mut rows = hold(&volume.summary);
        for number in inputs {
            rows.remove(*number);
        }
        for staged in &self.staged {
            let columns = self.staged_columns[staged.first_column..staged.first_column + staged.column_count].iter();
            let keys = &self.staged_keys;
            let facts = columns.map(|column| ColumnFacts {
                column: column.tail.column,
                lowest: &keys[column.lowest_at..column.bound_at],
                bound: &keys[column.bound_at..column.end],
                row_count: column.tail.row_count,
                grave_count: column.tail.grave_count,
                dead_rows: RowCount(0),
                dead_graves: RowCount(0),
                live_bytes: column.live_bytes,
                dead_bytes: ByteCount(0),
            });
            if !rows.push(&staged.facts, facts, [].into_iter()) {
                self.state.note_dropped_row();
            }
        }
        if let (Some(totals), Some(output)) = (self.totals, &self.output) {
            self.push_row(&mut rows, &self.writer, output.number, &totals, self.records_end);
        }
        if let (Some(totals), Some(output)) = (self.grave_totals, &self.grave_output) {
            self.push_row(&mut rows, &self.grave_writer, output.number, &totals, FileOffset(0));
        }
        if self.is_unwalked {
            let outputs = self.staged.len() as u64 + u64::from(self.totals.is_some()) + u64::from(self.grave_totals.is_some());
            let gone = inputs.len() as u64;
            let _waiting = volume.walks_waiting.fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                Some(count.saturating_sub(gone) + outputs)
            });
            return;
        }
        self.kept_dead.sort_unstable_by_key(|count| (count.segment, count.block));
        let number = self.output.as_ref().map_or(SegmentNumber(0), |output| output.number);
        // a row a cue kept counts dead with no bit, so its output's bits stop answering
        for count in self.kept_dead.iter().filter(|count| count.rows > 0) {
            volume.dead_bits.spoil_segment(count.segment, INEXACT_KEPT);
        }
        rows.apply_walk(number, self.writer.entries().len() as u32, &self.kept_dead);
        drop(rows);
        for count in self.kept_dead.iter().filter(|count| count.rows > 0) {
            volume.table.drop_live(count.segment, count.block, count.rows, count.replacement);
        }
        // the rows the walk held land with the outputs, before the release lets a waiting walk count again
        self.land_drained(volume);
    }

    /// Add an output's summary row
    pub fn push_row(
        &self,
        rows: &mut SummaryRows,
        writer: &FooterWriter,
        number: SegmentNumber,
        totals: &FooterTotals,
        data_end: FileOffset,
    ) {
        let block_count = writer.entries().len() as u32;
        let facts = output_facts(number, totals, data_end, block_count, self.is_unwalked);
        let columns = (0..writer.columns().len()).map(|index| writer.column_facts(index));
        if !rows.push(&facts, columns, [].into_iter()) {
            self.state.note_dropped_row();
        }
    }

    /// Try again to remove outputs a failure left behind
    pub fn clear_stray(&self) {
        let volume = &self.state.volumes[self.volume];
        // a lane with no strays may be mid-rewrite, and its outputs are none of this
        for (index, lane) in volume.lanes.iter().enumerate() {
            if lane.strays.load(Ordering::Acquire) > 0 {
                let slots = mem::take(&mut *hold(&lane.partial_outputs));
                self.leave_lane_outputs(volume, index, slots);
            }
        }
    }

    /// Remove outputs, or keep them listed, as strays that hold every grave back until they're gone. The last data output goes
    /// first: its tail lists the inputs, so at a crash open removes them, and the rest of its set has to be there to take over
    pub fn leave_outputs(&self, volume: &Volume<Backend>, outputs: OutputSlots<Backend::File>) {
        self.leave_lane_outputs(volume, self.lane, outputs);
    }

    /// Remove one lane's outputs, and note what a failed remove left there
    pub fn leave_lane_outputs(&self, volume: &Volume<Backend>, lane: usize, mut outputs: OutputSlots<Backend::File>) {
        for output in outputs.iter().flatten() {
            volume.dead_bits.end(output.number);
        }
        let last = outputs[..MOST_MERGE_OUTPUTS].iter().rposition(Option::is_some);
        let mut is_held = false;
        if let Some(slot) = last {
            outputs[slot] = outputs[slot].take().and_then(|output| self.state.remove_output(volume, output).err());
            is_held = outputs[slot].is_some();
        }
        if !is_held {
            for output in &mut outputs {
                *output = output.take().and_then(|output| self.state.remove_output(volume, output).err());
            }
        }
        let strays = outputs.iter().flatten().count();
        *hold(&volume.lanes[lane].partial_outputs) = outputs;
        volume.lanes[lane].strays.store(strays as u64, Ordering::Release);
        let total = volume.lanes.iter().map(|lane| lane.strays.load(Ordering::Acquire)).sum();
        volume.stray_outputs.store(total, Ordering::Release);
    }

    /// Count what can free a row or a grave: a segment leaving, or a cue going
    pub fn changes(&self, volume: &Volume<Backend>) -> u64 {
        let left = volume.segments_rewritten.load(Ordering::Acquire) + volume.segments_unlinked.load(Ordering::Acquire);
        left + self.state.cues.cues_gone()
    }

    /// Read the segments that gave up, as they stand since the last change
    pub fn gave_up_list(&self, volume: &Volume<Backend>) -> GaveUp {
        let mut gave_up = hold(&volume.gave_up);
        gave_up.catch_up(self.changes(volume));
        *gave_up
    }

    /// List this merge's inputs as having nothing to win until something changes
    pub fn note_gave_up(&self) {
        let volume = &self.state.volumes[self.volume];
        let mut gave_up = hold(&volume.gave_up);
        gave_up.catch_up(self.changes(volume));
        for number in &self.numbers[..self.input_count] {
            gave_up.add(*number);
        }
    }

    /// Note the input a read failed on when the device lost its bytes or a row block fails its check, and pass the error on
    pub fn note_unreadable(&mut self, input: usize, error: Error) -> Error {
        self.note_lost(self.numbers[input], error)
    }

    /// Note a segment a read failed on when the device lost its bytes or they fail their check, and pass the error on. A
    /// record lost in an input fails every merge it joins as surely as a lost row block does
    pub fn note_lost(&mut self, segment: SegmentNumber, error: Error) -> Error {
        if is_device_error(&error) || error.kind() == ErrorKind::InvalidData {
            self.unreadable = Some(segment);
        }
        error
    }

    /// Give the merge up: the inputs stay, and the outputs go whatever name they have reached
    pub fn abandon(&mut self) {
        // a claimed pick that never began gives its admission back with nothing written
        self.claimed = None;
        if let Some((admit, _bound)) = self.admitted.take() {
            self.state.give_back(admit.role, admit.need, ByteCount(0));
        }
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        let given_up = self.staged.iter().map(|staged| staged.facts.file_length.0).sum::<u64>() + self.records_end.0 + self.used as u64;
        state.note_written(WriteSource::GivenUp, ByteCount(given_up));
        // what it wrote and threw away was copy all the same
        self.settle_pace(volume, given_up);
        let slots = mem::take(&mut *hold(&volume.lanes[self.lane].partial_outputs));
        // what the table holds waits for no read, since no list ever had it, and the removes below close the files
        for staged in &self.staged {
            if let Some(retired) = volume.table.retire(staged.facts.segment) {
                volume.table.free_retired(&retired);
            }
        }
        self.staged.clear();
        // the outputs an offset-order pass made ahead went with the lane's slots above, and a later pass makes its own
        self.slot_outputs = Default::default();
        self.output = None;
        self.grave_output = None;
        self.leave_outputs(volume, slots);
        self.target = None;
        volume.lanes[self.lane].output_floor.store(NO_SEAL, Ordering::Release);
        // the inputs stay, so the rows the walk held in them count there
        self.drain_to_inputs(volume);
        self.release_hold(volume);
        volume.lanes[self.lane].is_rewriting.store(false, Ordering::Release);
        self.release_promise(volume);
        *hold(&volume.lanes[self.lane].merging) = [SegmentNumber(0); MOST_MERGE_SEGMENTS];
        *hold(&volume.lanes[self.lane].inputs) = [SegmentNumber(0); MOST_MERGE_SEGMENTS];
        self.release_region();
    }
}

/// Read the bytes a segment has given back in place: its length less what its file still takes on the disk
pub fn freed_in_place<Backend: Io>(volume: &Volume<Backend>, row: &SummaryRow<'_>) -> u64 {
    let Some(pin) = volume.table.pin(row.facts.segment) else {
        return 0;
    };
    pin.file.held_bytes().map_or(0, |held| row.facts.file_length.0.saturating_sub(held.0))
}

/// Build an output's summary facts
pub fn output_facts(number: SegmentNumber, totals: &FooterTotals, data_end: FileOffset, block_count: u32, is_unwalked: bool) -> SegmentFacts {
    SegmentFacts {
        segment: number,
        file_length: totals.file_length,
        data_end,
        row_block_end: totals.head.index_at,
        seal_mark: totals.head.index_sum,
        places: totals.head.places,
        marks: totals.head.marks,
        is_damaged: false,
        has_covers: false,
        damage_place: WritePlace(0),
        block_count,
        walked_blocks: if is_unwalked { 0 } else { block_count },
        is_backward: false,
        has_lost_rows: false,
        most_block_rows: totals.most_block_rows,
    }
}

/// Put a whole output in every directory
pub fn list_output<'tail, Backend: Io>(
    state: &EngineState<Backend>,
    volume: &Volume<Backend>,
    number: SegmentNumber,
    newest_place: WritePlace,
    columns: impl Iterator<Item = &'tail ColumnTail>,
) {
    for column in columns {
        let entry = Listing::of_column(column, number, newest_place);
        if let Some(directory) = &state.directories[column.column.0 as usize]
            && !directory.replace(Some(&entry), &[])
        {
            volume.fail();
        }
    }
}

impl<Backend: Io> RewriteJob<Backend> {
    /// Give back the output room this merge claimed
    pub fn release_promise(&mut self, volume: &Volume<Backend>) {
        volume.outputs_promised.fetch_sub(self.promised.0, Ordering::AcqRel);
        volume.parks_promised.fetch_sub(self.parks_held, Ordering::AcqRel);
        self.parks_held = 0;
        self.promised = ByteCount(0);
    }
}

impl<Backend: Io> RewriteJob<Backend> {
    /// Check the merge has written past its promise, taking another step of room when the disk has it, and say whether it must
    /// give way
    pub fn outgrows_promise(&mut self) -> bool {
        // an installed or given-up merge released its promise, and the bytes it wrote went with it
        if self.target.is_none() {
            return false;
        }
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        let written = self.staged.iter().map(|staged| staged.facts.file_length.0).sum::<u64>() + self.records_end.0 + self.used as u64;
        if written <= self.promised.0 {
            return false;
        }
        let more = written - self.promised.0 + OUTGROW_STEP_BYTES;
        if !has_disk_room(&state, volume, ByteCount(more), false) {
            return true;
        }
        self.promised = ByteCount(self.promised.0 + more);
        volume.outputs_promised.fetch_add(more, Ordering::AcqRel);
        volume.promises_outgrown.fetch_add(1, Ordering::Relaxed);
        false
    }
}
