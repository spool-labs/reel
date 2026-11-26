//! Deciding a merge's rows: which go across, which drop, and where each output's records start

use std::array;
use std::io::{Error, ErrorKind, IoSlice, Result};
use std::mem;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_core::column_id::ColumnId;
use reel_core::maintenance_report::{
    PickOutcome, UnitKind,
};
use reel_core::store_error::StoreError;
use reel_core::units::{ByteCount, FileOffset, WritePlace};

use crate::engine::reports::thread_cpu;
use crate::engine::dead_bits::{BIT_SAMPLE_EVERY, DECIDED_ASKED, DECIDED_CLEAR, DECIDED_SET, DECIDED_SHORT, DECIDED_WRONG, DeadRow, INEXACT_SHORT};
use crate::segment::footer_reader::RowAt;
use crate::engine::read_path::SealedRow;
use crate::engine::open_segment::output_path;
use crate::engine::seal_job::MOST_SEGMENT_BLOCKS;
use crate::engine::volume::{GRAVE_SLOT, MOST_MERGE_OUTPUTS, RewriteOutput, Volume};
use crate::engine::walk_job::{FREE_IN_PLACE_BYTES, add_count};
use crate::guard::hold;
use crate::io::{InPlace, Io, IoFile, in_place};
use crate::record::record_check::RecordIdentity;
use crate::record::record_head::{MOST_SUMMED_PAYLOAD, SMALL_HEAD_BYTES};
use crate::record::record_reader::{RecordLocation, RecordRead, checked_payload, form_of};
use crate::row_kind::RowKind;
use crate::segment::footer_writer::ColumnStart;
use crate::index::key_hash::hash_key;
use crate::segment::row_block::FooterRow;
use crate::segment::segment_table::{SegmentPin, shard_of};
use crate::segment::segment_tail::{MOST_MERGE_SEGMENTS, SegmentNumber};

use crate::engine::rewrite_job::{MERGE_STEP_ROWS, MERGE_STEP_TIME, MOST_NUMBER_TRIES, MergeTarget, RewriteJob, RewritePhase};

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
        let output = self.create_output(volume, ByteCount(self.promised.0.max(1).min(target.length.0).min(self.cut_bytes)), 0)?;
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
        // an output that lands outside a stuck seal's shard frees the slot its input held there
        let stuck = volume.stuck_shard.load(Ordering::Acquire);
        let mut number = self.state.next_segment_number();
        for _try in 0..MOST_NUMBER_TRIES {
            match &number {
                Ok(taken) if shard_of(*taken) == stuck => number = self.state.next_segment_number(),
                _other => break,
            }
        }
        let Ok(number) = number else {
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
        self.bit_extents = [None; MOST_MERGE_SEGMENTS];
        self.bit_rows = 0;
        self.park_wait_since = None;
        self.data_slot = 0;
        self.is_cutting = false;
        self.output_rows = 0;
        self.output_last_key.clear();
        self.grave_last_key.clear();
        self.staged.clear();
        self.staged_columns.clear();
        self.staged_keys.clear();
        self.kept_dead.clear();
        self.kept_rows.clear();
        self.are_kept_rows_cut = false;
        for input in &mut self.inputs {
            input.rewind();
        }
    }

    /// Run one bounded step, and give the merge up on any failure, which leaves the inputs as they were
    pub fn step(&mut self) {
        // a claimed pick maps and carves here, on the thread that runs it, as a unit of its own
        if self.claimed.is_some() {
            let started = self.state.io.now();
            let cpu_started = thread_cpu();
            let need = self.admitted.map_or(0, |(admit, _bound)| admit.need.0);
            if self.begin() {
                self.state.note_unit(UnitKind::Start, need, started, cpu_started);
            }
            return;
        }
        let Some(target) = self.target else {
            return;
        };
        let started = self.state.io.now();
        let cpu_started = thread_cpu();
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
        // a merge that wrote past its promise takes more room before it goes on, or gives way
        // a step holds the segments outside the merge for its rows alone, so an unlink or close waits one step at most
        self.cursor.release(&self.state.volumes[self.volume].table);
        self.state.note_cursor(mem::take(&mut self.cursor.counts), true);
        let outcome = outcome.and_then(|()| if self.outgrows_promise() { Err(Error::from(ErrorKind::StorageFull)) } else { Ok(()) });
        // the row blocks a pass reads, so a merge's deciding and placing reads show beside its copying one
        let pass = match phase {
            RewritePhase::Deciding => 0,
            RewritePhase::Placing => 1,
            _ => 2,
        };
        let read: u64 = self.inputs.iter_mut().map(|input| mem::take(&mut input.read_bytes)).sum();
        self.state.volumes[self.volume].footer_reads[pass].fetch_add(read, Ordering::Relaxed);
        self.state.note_unit(self.unit.0, self.unit.1, started, cpu_started);
        if outcome.is_err() {
            // a lost or rotten block fails every merge it joins, so its segment is damage the way a get would find it
            if let Some(segment) = self.unreadable.take() {
                self.state.damage_unread(self.volume, segment);
            }
            // an asked rebuild that met a rotten record ends its ask, so the caller puts the key back and asks again while the
            // planner tries no failing merge each tick. One that stopped for any other cause keeps the ask and goes again
            if let Some(segment) = self.asked_rebuild.take()
                && outcome.as_ref().is_err_and(|error| error.kind() == ErrorKind::InvalidData)
            {
                self.state.volumes[self.volume].end_rebuild_ask(segment);
            }
            let given_up = self.staged.iter().map(|staged| staged.facts.file_length.0).sum::<u64>() + self.records_end.0 + self.used as u64;
            let kind = match outcome.as_ref().map_err(Error::kind) {
                Err(ErrorKind::Interrupted) => 0,
                Err(ErrorKind::StorageFull) => 1,
                Err(ErrorKind::NotFound) => 2,
                Err(ErrorKind::InvalidData) => 3,
                _other => 4,
            };
            let volume = &self.state.volumes[self.volume];
            volume.given_up_by_phase[phase as usize].fetch_add(given_up, Ordering::Relaxed);
            volume.given_up_by_kind[kind].fetch_add(given_up, Ordering::Relaxed);
            // a merge short of room waits a reserve period before another is picked, or a full disk retries one that can't fit
            if kind == 1 {
                volume.room_refused_at.store(self.state.io.now().0.max(1), Ordering::Release);
                volume.is_room_refused.store(true, Ordering::Release);
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

        let deadline = state.io.now().0.saturating_add(MERGE_STEP_TIME.0);
        for _row in 0..MERGE_STEP_ROWS {
            // a step past its time or a store going ends here, and the next step takes the row in hand
            if self.unit.1 > 0 && (state.io.now().0 >= deadline || state.is_stopping.load(Ordering::Acquire)) {
                return Ok(());
            }
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
            self.row_walked = pin.is_walked;
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
        let hider = replacement.map(|place| self.first_hider(volume, column, row).unwrap_or(place));
        match hider {
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
            #[cfg(feature = "test-hooks")]
            if self.is_blind_outside {
                self.newest_place = None;
                let own = (!is_top).then_some(self.top_place);
                return Ok(own.max(self.cover_place).filter(|place| *place > row.place));
            }
            self.newest_place = match self.bit_answer(volume, column, row)? {
                Some(answer) => answer,
                None => {
                    self.cursor.counts.rows_skipped += u64::from(self.is_unwalked);
                    self.cursor.counts.rows_asked += u64::from(!self.is_unwalked);
                    self.asked_outside(volume, column)?
                },
            };
        }
        let own = (!is_top).then_some(self.top_place);
        Ok(own.max(self.newest_place).max(self.cover_place).filter(|place| *place > row.place))
    }

    /// Ask the finished segments outside the merge for the key's newest place above its top row here
    pub fn asked_outside(&mut self, volume: &Volume<Backend>, column: ColumnId) -> Result<Option<WritePlace>> {
        match self.newest_outside(volume, column) {
            Ok(found) => Ok(found.map(|found| found.row.place)),
            // a version a damaged block hides replaces nothing, and the row stays
            Err(StoreError::Corruption { .. }) => Ok(None),
            Err(_error) => Err(Error::from(ErrorKind::Interrupted)),
        }
    }

    /// Answer a key's top row by its dead bit: set, a newer row replaced it at or under the input's highest replacement; clear,
    /// nothing outside did. Nothing when the row has to ask: an unwalked input or merge, bits that aren't exact, damage, a live cue
    /// that could read the row, or a grave while a segment is parked or an output stray. A value row's replacement sits in a sealed
    /// segment, and an install retires inputs only once the summary that lists its outputs is synced, so a crash leaves the
    /// replacement in one or the other. A parked file can come back holding a row a grave hid, so graves still wait
    pub fn bit_answer(&mut self, volume: &Volume<Backend>, column: ColumnId, row: &FooterRow) -> Result<Option<Option<WritePlace>>> {
        if self.is_unwalked || !self.row_walked || !volume.are_bits_answering() {
            return Ok(None);
        }
        let state = &self.state;
        let is_settled = !volume.is_parking() && volume.stray_outputs.load(Ordering::Acquire) == 0;
        let is_clean = is_settled || row.kind == RowKind::Value;
        if !is_clean || state.cues.has_cue_between(self.volume, row.place, WritePlace(u64::MAX)) {
            return Ok(None);
        }
        let spoils = volume.dead_bits.spoils.load(Ordering::Acquire);
        if spoils != self.bits_seen {
            self.bit_extents = [None; MOST_MERGE_SEGMENTS];
            self.bits_seen = spoils;
        }
        let input = self.row_input;
        let segment = self.numbers[input];
        let Some(extent) = *self.bit_extents[input].get_or_insert_with(|| volume.dead_bits.exact(segment)) else {
            return Ok(None);
        };
        let Some((block, index)) = self.inputs[input].row_place() else {
            return Ok(None);
        };
        let Some(is_dead) = volume.dead_bits.is_set(&extent, block, index) else {
            return Ok(None);
        };
        // a walk that charges a row of a held input sets the lane's bit, and leaves the table's replacement as it was
        let is_held = !is_dead && hold(&volume.lanes[self.lane].held).is_set(segment, block, index);
        let is_set = is_dead || is_held;
        // every replacement sits in a sealed segment, so the highest sealed place bounds one the table can't, after a reopen
        // reset its replacement or when a lane's held bit left it as it was
        let sealed = Some(WritePlace(volume.highest_sealed.load(Ordering::Acquire)));
        let bound = match is_held {
            true => sealed,
            false => volume.table.highest_replacement(segment).filter(|place| *place > row.place).or(sealed),
        };
        let answer = match is_set {
            true => match bound.filter(|place| *place > row.place) {
                Some(place) => Some(place),
                None => return Ok(None),
            },
            false => None,
        };
        let decided = &volume.dead_bits.decided;
        decided[if is_set { DECIDED_SET } else { DECIDED_CLEAR }].fetch_add(1, Ordering::Relaxed);
        self.bit_rows += 1;
        // the sims ask every set bit the merge decides by: its row has a newer sealed version, inputs and all, or lies under an
        // applied cover or a damaged range
        #[cfg(feature = "test-hooks")]
        if is_set && state.io.proves_dead_bits() {
            let is_replaced = state.has_newer_sealed(volume, column, &self.key, row.place, &[]).unwrap_or(true);
            // a cover reloaded at open is durable, and a get reads through it before it applies again
            let is_covered = volume.covers.newest_over(column, &self.key, WritePlace(u64::MAX)).is_some_and(|cover| cover > row.place);
            let is_hidden = volume.damage.covering(column, &self.key).is_some_and(|highest| highest > row.place);
            // a grave counts dead once nothing older is left for it to hide
            let is_bare_grave = row.kind == RowKind::Grave && !state.has_older_row(volume, column, &self.key, row.place, &[]).unwrap_or(true);
            assert!(is_replaced || is_covered || is_hidden || is_bare_grave, "a merge decides {segment:?} block {block} row {index} by its bit, and the row is live: key {:02x?}", &self.key[..]);
        }
        // one row in so many asks as well: a set bit with no newer row anywhere is wrong, a clear bit the ask finds replaced is
        // a row the walk didn't charge
        if volume.asks_every_bit() || self.bit_rows.is_multiple_of(BIT_SAMPLE_EVERY) {
            decided[DECIDED_ASKED].fetch_add(1, Ordering::Relaxed);
            let inputs = &self.numbers[..self.input_count];
            if is_set && !state.has_newer_sealed(volume, column, &self.key, row.place, inputs).unwrap_or(true) {
                decided[DECIDED_WRONG].fetch_add(1, Ordering::Relaxed);
            }
            // the walk missed a charge on this segment, so its bits stop answering and this row and every later one ask
            if !is_set && self.asked_outside(volume, column)?.is_some_and(|place| place > row.place) {
                decided[DECIDED_SHORT].fetch_add(1, Ordering::Relaxed);
                volume.dead_bits.spoil_segment(segment, INEXACT_SHORT);
                return Ok(None);
            }
        }
        Ok(Some(answer))
    }

    /// Find a key's newest row in a finished segment outside the merge, above its top row here. One below that replaces nothing
    /// the top row doesn't, so the segments with nothing above it go unasked
    pub fn newest_outside(
        &mut self,
        volume: &Volume<Backend>,
        column: ColumnId,
    ) -> std::result::Result<Option<SealedRow>, StoreError> {
        if self.is_unwalked {
            return Ok(None);
        }
        self.state.newest_walked_held(volume, column, &self.key, &self.numbers[..self.input_count], Some(self.top_place), &mut self.cursor)
    }

    /// Find the first place above a row that hides it: a newer row of the key, or the oldest applied cover over it above the
    /// row. A cue at or above that place reads the hider, so the row goes with no cue below it. The newest cover would keep the
    /// row for a cue between two covers, and the older cover leaves with this merge's inputs, which leaves the row bare
    pub fn first_hider(&self, volume: &Volume<Backend>, column: ColumnId, row: &FooterRow) -> Option<WritePlace> {
        let own = (!self.is_top_row).then_some(self.top_place);
        let cover = self.cover_place.and_then(|_newest| volume.covers.lowest_applied_above(column, &self.key, row.place));
        [own, self.newest_place, cover].into_iter().flatten().filter(|place| *place > row.place).min()
    }

    /// Check a replaced row can go: no live cue sits at or above it and below the first place that hides it
    pub fn can_drop(&self, replacement: WritePlace, row: &FooterRow) -> bool {
        !self.state.cues.has_cue_between(self.volume, row.place, replacement)
    }

    /// Check a grave with no newer row of its key has nothing left to hide, by this key's own rows and nothing wider. An older
    /// grave keeps it too: that one's walk count stands on this row, and a later write of the key would count it twice. It
    /// drops first, as any row with a newer one does. A file on its way out comes back at a crash, so no grave drops while
    /// one is parked or leaving. A cue below the grave can read what it covers, and damage could be hiding an older version
    pub fn hides_nothing(&self, volume: &Volume<Backend>, column: ColumnId, row: &FooterRow) -> Result<bool> {
        let state = &self.state;
        if volume.is_parking()
            || volume.stray_outputs.load(Ordering::Acquire) > 0
            || state.cues.has_cue_below(self.volume, row.place)
            || volume.damage.covering(column, &self.key).is_some()
        {
            return Ok(false);
        }
        // a complete column frees a superseded row's slot, so its footers answer for older rows as any column's do
        match state.has_older_row(volume, column, &self.key, row.place, &self.numbers[..self.input_count]) {
            // a segment that started leaving during the walk may hold an older row the walk missed
            Ok(has_older) => Ok(!has_older && !volume.is_parking()),
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
            // offset order copied the record as a run of bytes, so one a trusted read doesn't sum is checked here, where its key
            // is in hand. The copy just read it, so the check reads the page cache
            Some(offset) if row.kind == RowKind::Value && row.length.0 > SUMMED_RECORD_BYTES => {
                if !self.is_record_sound(&pin.file, (column, row), None) {
                    // its bytes are already across in the run, where they stay as dead space no row names
                    if self.drop_freed(column, row) {
                        return Ok(());
                    }
                    let failed = self.failed_check();
                    return Err(self.note_lost(pin.head.segment, failed));
                }
                offset
            },
            Some(offset) => offset,
            None => {
                volume.keyed_copies.fetch_add(1, Ordering::Relaxed);
                match self.copy_record(&pin.file, &output.file, (column, row)) {
                    Ok(offset) => offset,
                    Err(error) if error.kind() == ErrorKind::InvalidData && self.drop_freed(column, row) => return Ok(()),
                    Err(error) => return Err(self.note_lost(pin.head.segment, error)),
                }
            },
        };
        let kept = FooterRow { offset, ..*row };
        self.writer.push_row(&output.file, &self.key, &kept)?;
        self.output_rows += 1;
        self.output_last_key.set_from(&self.key)?;
        let at = RowAt { index: self.writer.last_row_index(), rows: self.writer.last_row_index() + 1 };
        let block = self.writer.entries().len() as u32;
        self.give_slot(volume, output.number, column, block);
        self.note_home(output.number, block, at.index);
        self.note_kept_dead(output.number, column, block, at, &kept, replacement)
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
        let at = RowAt { index: self.grave_writer.last_row_index(), rows: self.grave_writer.last_row_index() + 1 };
        let block = self.grave_writer.entries().len() as u32;
        self.give_slot(volume, output.number, column, block);
        self.note_home(output.number, block, at.index);
        self.note_kept_dead(output.number, column, block, at, &kept, replacement)
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
        at: RowAt,
        kept: &FooterRow,
        replacement: Option<WritePlace>,
    ) -> Result<()> {
        if replacement.is_some() && !self.is_unwalked {
            if self.kept_dead.len() == self.kept_dead.capacity() {
                return Err(Error::from(ErrorKind::OutOfMemory));
            }
            add_count(&mut self.kept_dead, output, column, block, kept, replacement)?;
            // the bit lands with the outputs, and a row past the list leaves the outputs' bits to stop answering
            let row = DeadRow { segment: output, block, at, length: kept.length.0, is_grave: kept.kind == RowKind::Grave };
            if self.kept_rows.push(row).is_err() {
                self.are_kept_rows_cut = true;
            }
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

    /// Copy a record as stored once it checks out against the identity its row gives, and return where it landed. A record that
    /// fails its check goes no further: the merge stops over it as unreadable, which damages its segment. This is the key-order
    /// copy. Offset order copies runs of bytes, and the copying pass checks only its records past SUMMED_RECORD_BYTES, since a
    /// trusted read's sum catches the rest
    pub fn copy_record(&mut self, from: &Backend::File, to: &Backend::File, (column, row): (ColumnId, &FooterRow)) -> Result<FileOffset> {
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
            if row.kind == RowKind::Value && !self.is_record_sound(from, (column, row), Some(&self.buffer[self.used..self.used + length])) {
                return Err(self.failed_check());
            }
            self.used += length;
            return Ok(at);
        }

        if row.kind == RowKind::Value && !self.is_record_sound(from, (column, row), None) {
            return Err(self.failed_check());
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

    /// Check a record the merge copies against its row's identity: a one-chunk record where it lies in the buffer, and any other
    /// through a reader, which checks a chunked record's stored chunks one at a time and restores nothing
    pub fn is_record_sound(&self, from: &Backend::File, (column, row): (ColumnId, &FooterRow), held: Option<&[u8]>) -> bool {
        let identity = RecordIdentity { record_key: self.state.record_key, column, key: &self.key, kind: row.kind, place: row.place };
        if let Some(record) = held
            && let Some(form) = form_of(record).filter(|form| !form.is_chunked)
        {
            return checked_payload(record, form, &identity).is_some();
        }
        let location = RecordLocation { offset: row.offset, length: row.length };
        let mut reader = self.state.record_reader_for(&InPlace);
        matches!(in_place(reader.check_stored(&InPlace, from, location, &identity)), Ok(RecordRead::Valid))
    }

    /// Drop a large record that failed its check because the walk freed it in place as dead, and say whether it went. A crash can
    /// take the walk's count of its death from the dead bits the merge decided by, and then the merge keeps it and finds its
    /// blocks punched. A key with a newer row than the record, as a get finds it, says it was superseded, and it drops as any
    /// superseded row does. A record still the newest stays a failure, which damages its segment
    pub fn drop_freed(&mut self, column: ColumnId, row: &FooterRow) -> bool {
        if row.kind != RowKind::Value || row.length < FREE_IN_PLACE_BYTES {
            return false;
        }
        let Some(newer) = self.state.newest_place(self.volume, column, &self.key).filter(|newer| *newer > row.place) else {
            return false;
        };
        self.lowest_dropped = self.lowest_dropped.min(row.place);
        self.highest_replacement = self.highest_replacement.max(newer);
        self.dropped_rows += 1;
        self.state.merge_freed_drops.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Stop the merge over a record that failed its check. A free since the merge began can have punched a record it keeps, so
    /// that stop is no damage
    pub fn failed_check(&self) -> Error {
        if !self.state.has_outlived(self.started_counts, true) {
            return Error::from(ErrorKind::Interrupted);
        }
        self.state.merge_check_failures.fetch_add(1, Ordering::Relaxed);
        Error::from(ErrorKind::InvalidData)
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
        // several inputs becoming one win back overlap whatever they drop, a lone input moves out of a stuck seal's shard, and an
        // asked rebuild clears its input's damage
        if self.dropped_rows == 0 && self.input_count == 1 && !self.is_leaving_stuck_shard() && !self.is_asked_rebuild() {
            self.note_gave_up();
            return Err(Error::from(ErrorKind::Interrupted));
        }
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        // every output the deciding pass cut takes its records before the copying pass reaches it. One is made a step, since
        // setting a large file's room aside can take seconds, and a close waits for the step
        if self.is_ordered
            && let Some(slot) = (1..=self.cuts).find(|slot| self.slot_outputs[*slot].is_none())
        {
            self.slot_outputs[slot] = Some(self.create_output(volume, ByteCount(self.cut_bytes), slot)?);
            return Ok(());
        }
        self.open_output(volume, target)?;
        if self.is_ordered {
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

/// Bytes the longest record a trusted read checks by its sum takes: a one-chunk head and MOST_SUMMED_PAYLOAD
pub const SUMMED_RECORD_BYTES: u64 = (SMALL_HEAD_BYTES + MOST_SUMMED_PAYLOAD) as u64;
