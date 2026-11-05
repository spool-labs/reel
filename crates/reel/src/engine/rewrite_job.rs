//! Compaction's merge: up to 8 sealed segments copied into a new one, without the rows no read can reach
//!
//! Records land at the front of the output and row blocks land past the inputs' data ends, since the records' end isn't
//! known until the last row. The blocks then move down to meet the records and the tail makes the file a segment. Graves that
//! stay go to an output of their own, and it is whole first: the last tail to land lists the inputs, and open removes them
//! on sight, however far the install got.

use std::array;
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
use crate::segment::segment_table::SegmentPin;
use crate::segment::segment_tail::{BlockCount, MOST_MERGE_SEGMENTS, SegmentNumber};
use crate::segment::summary::{DeadCount, SegmentFacts, SummaryRow, SummaryRows};

/// A segment is rewritten once this part of its rows or bytes is dead: 2 is a half, 4 a quarter
pub const REWRITE_DIVISOR: u64 = 2;
/// Bytes of one job's merge buffer, which carries records and then row blocks. With a row block for each input, a job's
/// merge buffers come to about 2 MiB
pub const MERGE_BUFFER_BYTES: usize = 3 * 512 * 1024;
/// Most row blocks of an output that can hold rows kept for a cue, past which the merge waits for the cue to go
pub const MOST_KEPT_BLOCKS: usize = 1_024;
/// Rows one step of a merge decides
pub const MERGE_STEP_ROWS: usize = 4_096;
/// Most row blocks the output for kept graves can have, past which the merge gives way
pub const MOST_GRAVE_BLOCKS: BlockCount = BlockCount(2_048);
/// Rounds an install waits for room to park its inputs, past which the merge gives way
pub const MOST_PARK_WAITS: u32 = 4_096;
/// Most segments of a column that overlap one key before a merge is due
pub const MOST_OVERLAP: usize = 32;
/// Most segments one look for an overlap starts from, which bounds what a look costs
pub const OVERLAP_LOOKS: usize = 16;

/// What a merge is copying
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MergeTarget {
    pub blocks_at: FileOffset,
    pub length: ByteCount,
}

/// How far a merge has gone
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RewritePhase {
    /// Rows of a grave-heavy segment are being decided, and nothing is copied unless one would drop
    Deciding,
    /// Rows are being decided and records copied
    Copying,
    /// Row blocks are moving down to the records' end
    Moving,
    /// The outputs are whole segments and wait to replace their inputs
    Installing,
}

/// One segment going into a merge, and where its rows have reached
pub struct MergeInput {
    pub segment: SegmentNumber,
    pub block_count: u32,
    next_block: u32,
    block: Vec<u8>,
    length: usize,
    position: usize,
    key: Vec<u8>,
    column: ColumnId,
    row: Option<FooterRow>,
}

impl MergeInput {
    /// Allocate the fixed buffers
    pub fn new() -> Self {
        Self {
            segment: SegmentNumber(0),
            block_count: 0,
            next_block: 0,
            block: touched_zeroes(ROW_BLOCK_BYTES),
            length: 0,
            position: 0,
            key: touched_capacity(MAX_KEY_BYTES as usize, 0),
            column: ColumnId(0),
            row: None,
        }
    }

    /// Read the heap the buffers hold
    pub fn heap_bytes(&self) -> usize {
        self.block.capacity() + self.key.capacity()
    }

    /// Go back to the first row
    pub fn rewind(&mut self) {
        self.next_block = 0;
        self.length = 0;
        self.position = 0;
        self.row = None;
    }

    /// Step to the next row, reading on through the input's blocks, and leave no row at its end
    pub fn advance<Backend: Io>(&mut self, volume: &Volume<Backend>, pin: &SegmentPin<'_, Backend::File>) -> Result<()> {
        let base = pin.head.place_base;
        loop {
            if self.length > 0 {
                let Some(mut walk) = RowBlockWalk::open_checked(&self.block[..self.length], base) else {
                    return Err(Error::from(ErrorKind::InvalidData));
                };
                walk.resume(self.position);
                self.row = walk.next_row(&mut self.key);
                self.position = walk.position();
                if self.row.is_some() {
                    return Ok(());
                }
            }
            if self.next_block >= self.block_count {
                self.row = None;
                return Ok(());
            }
            let Some(block) = volume.table.walk_block(self.segment, self.next_block) else {
                return Err(Error::from(ErrorKind::NotFound));
            };
            let length = block.entry.block_length().0 as usize;
            if length > self.block.len() {
                return Err(Error::from(ErrorKind::InvalidData));
            }
            pin.file.read_exact_at(block.entry.block_at(), &mut self.block[..length])?;
            if RowBlockWalk::open(&self.block[..length], base).is_none() {
                return Err(Error::from(ErrorKind::InvalidData));
            }
            self.next_block += 1;
            self.length = length;
            self.position = 0;
            self.column = block.column;
            self.key.clear();
        }
    }
}

impl Default for MergeInput {
    fn default() -> Self {
        Self::new()
    }
}

/// The merge of one volume
pub struct RewriteJob<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
    pub volume: usize,
    pub target: Option<MergeTarget>,
    pub phase: RewritePhase,
    pub output: Option<RewriteOutput<Backend::File>>,
    pub grave_output: Option<RewriteOutput<Backend::File>>,
    pub writer: FooterWriter,
    pub grave_writer: FooterWriter,
    pub totals: Option<FooterTotals>,
    pub grave_totals: Option<FooterTotals>,
    inputs: Vec<MergeInput>,
    numbers: [SegmentNumber; MOST_MERGE_SEGMENTS],
    input_count: usize,
    is_drained: bool,
    buffer: Vec<u8>,
    used: usize,
    records_end: FileOffset,
    moved: u64,
    key: Vec<u8>,
    last_key: Vec<u8>,
    lowest: Vec<u8>,
    highest: Vec<u8>,
    edge_lowest: Vec<u8>,
    edge_highest: Vec<u8>,
    edges_column: Option<ColumnId>,
    last_column: Option<ColumnId>,
    output_column: Option<ColumnId>,
    grave_column: Option<ColumnId>,
    top_place: WritePlace,
    newest_place: Option<WritePlace>,
    lowest_dropped: WritePlace,
    pub highest_replacement: WritePlace,
    dropped_rows: u64,
    kept_dead: Vec<DeadCount>,
    last_tried: SegmentNumber,
    park_waits: u32,
}

impl<Backend: Io> RewriteJob<Backend> {
    /// Allocate the job's fixed buffers
    pub fn new(state: Arc<EngineState<Backend>>, volume: usize) -> Self {
        // a mutex's first lock can allocate, and this one's first real use could be in a close
        drop(hold(&state.volumes[volume].partial_outputs));
        let keys = MAX_KEY_BYTES as usize;
        Self {
            state,
            volume,
            target: None,
            phase: RewritePhase::Copying,
            output: None,
            grave_output: None,
            writer: FooterWriter::new(MOST_SEGMENT_BLOCKS),
            grave_writer: FooterWriter::new(MOST_GRAVE_BLOCKS),
            totals: None,
            grave_totals: None,
            inputs: (0..MOST_MERGE_SEGMENTS).map(|_input| MergeInput::new()).collect(),
            numbers: [SegmentNumber(0); MOST_MERGE_SEGMENTS],
            input_count: 0,
            is_drained: false,
            buffer: touched_zeroes(MERGE_BUFFER_BYTES),
            used: 0,
            records_end: FileOffset(0),
            moved: 0,
            key: touched_capacity(keys, 0),
            last_key: touched_capacity(keys, 0),
            lowest: touched_capacity(keys, 0),
            highest: touched_capacity(keys, 0),
            edge_lowest: touched_capacity(keys, 0),
            edge_highest: touched_capacity(keys, 0),
            edges_column: None,
            last_column: None,
            output_column: None,
            grave_column: None,
            top_place: WritePlace(0),
            newest_place: None,
            lowest_dropped: WritePlace(u64::MAX),
            highest_replacement: WritePlace(0),
            dropped_rows: 0,
            kept_dead: touched_capacity(MOST_KEPT_BLOCKS, DeadCount::default()),
            last_tried: SegmentNumber(0),
            park_waits: 0,
        }
    }

    /// Read the heap the job's buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        let keys = self.key.capacity() + self.last_key.capacity() + self.lowest.capacity() + self.highest.capacity();
        let edges = self.edge_lowest.capacity() + self.edge_highest.capacity();
        let inputs: usize = self.inputs.iter().map(MergeInput::heap_bytes).sum();
        let counts = self.kept_dead.capacity() * size_of::<DeadCount>();
        let writers = self.writer.heap_bytes().0 + self.grave_writer.heap_bytes().0;
        ByteCount(writers + (self.buffer.capacity() + keys + edges + inputs + counts) as u64)
    }

    /// Check a merge is under way
    pub fn is_running(&self) -> bool {
        self.target.is_some()
    }

    /// Read the last segment a pick took, where the next look starts
    pub fn last_tried(&self) -> SegmentNumber {
        self.last_tried
    }

    /// Read the segments going in
    pub fn input_numbers(&self) -> &[SegmentNumber] {
        &self.numbers[..self.input_count]
    }

    /// Start on the next ready segments past the last one tried, once no walk target is in progress, since it would count rows
    /// this merge has decided on. The walks wait from here to the install. A grave-heavy segment is decided first, and copies
    /// nothing if no row would drop
    pub fn start(&mut self) -> bool {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        let Some((target, is_grave_heavy)) = self.pick(volume) else {
            return false;
        };
        self.clear(target);
        volume.is_rewriting.store(true, Ordering::Release);
        if is_grave_heavy {
            self.phase = RewritePhase::Deciding;
            return true;
        }
        match self.open_output(volume, target) {
            Ok(()) => true,
            Err(_error) => {
                self.abandon();
                false
            },
        }
    }

    /// Pick the inputs: ready segments first, and failing that a run of segments over the overlap bound
    pub fn pick(&mut self, volume: &Volume<Backend>) -> Option<(MergeTarget, bool)> {
        if volume.stray_outputs.load(Ordering::Acquire) > 0 {
            return None;
        }
        if volume.has_walk_target.load(Ordering::Acquire) {
            return None;
        }
        let room = volume.park_room().min(MOST_MERGE_SEGMENTS);
        if room == 0 {
            return None;
        }
        let rows = hold(&volume.summary);
        let is_ready = |row: &SummaryRow<'_>| {
            row.facts.is_walked() && is_rewrite_ready(row, 0) && is_rewrite_ready(row, freed_in_place(volume, row))
        };
        let ready = rows.rows().filter(is_ready);
        let after = ready.clone().filter(|row| row.facts.segment > self.last_tried).min_by_key(|row| row.facts.segment);
        if let Some(first) = after.or_else(|| ready.min_by_key(|row| row.facts.segment)) {
            let is_grave_heavy = is_grave_heavy(&first);
            let is_more = |row: &SummaryRow<'_>| is_ready(row) && row.facts.segment > first.facts.segment;
            return Some((self.gather(volume, &rows, &first, room, is_more), is_grave_heavy));
        }
        let first = overlap_start(&rows, self.last_tried)?;
        let is_more = |row: &SummaryRow<'_>| is_mergeable(row) && row.facts.segment > first.facts.segment && overlaps(&first, row);
        let target = self.gather(volume, &rows, &first, room, is_more);
        // one segment alone wins back no overlap, and the next look starts past it
        (self.input_count > 1).then_some((target, false))
    }

    /// Take a first segment and the next ones a test passes, while they fit one output, and note the last as tried
    pub fn gather(
        &mut self,
        volume: &Volume<Backend>,
        rows: &SummaryRows,
        first: &SummaryRow<'_>,
        most: usize,
        is_more: impl Fn(&SummaryRow<'_>) -> bool,
    ) -> MergeTarget {
        self.input_count = 0;
        let mut data = 0;
        let mut length = 0;
        let mut blocks = 0;
        let mut next = Some(*first);
        while let Some(row) = next {
            let facts = row.facts;
            let input = &mut self.inputs[self.input_count];
            input.segment = facts.segment;
            input.block_count = facts.block_count;
            self.numbers[self.input_count] = facts.segment;
            self.input_count += 1;
            data += facts.data_end.0;
            length += facts.file_length.0;
            blocks += facts.block_count;
            self.last_tried = facts.segment;
            if self.input_count == most {
                break;
            }
            next = rows
                .rows()
                .filter(|row| row.facts.segment > facts.segment && is_more(row))
                .min_by_key(|row| row.facts.segment)
                .filter(|row| {
                    data + row.facts.data_end.0 <= volume.segment_bytes.0
                        && blocks + row.facts.block_count <= MOST_SEGMENT_BLOCKS.0
                });
        }
        MergeTarget {
            blocks_at: FileOffset(data),
            length: ByteCount(length),
        }
    }

    /// Create the data output and start every input over. It reserves the inputs' whole length, so a merge holds that much
    /// twice until its footer cuts the output down
    pub fn open_output(&mut self, volume: &Volume<Backend>, target: MergeTarget) -> Result<()> {
        let output = self.create_output(volume, target.length, 0)?;
        self.output = Some(output);
        let base = self.lowest_place_base(volume)?;
        self.writer.start(target.blocks_at, base);
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
        let output = RewriteOutput { file, root, number, is_named: false };
        hold(&volume.partial_outputs)[slot] = Some(output.clone());
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
        self.park_waits = 0;
        self.kept_dead.clear();
        for input in &mut self.inputs {
            input.rewind();
        }
    }

    /// Run one bounded step, and give the merge up on any failure, which leaves the inputs as they were
    pub fn step(&mut self) {
        let Some(target) = self.target else {
            return;
        };
        let outcome = match self.phase {
            RewritePhase::Deciding | RewritePhase::Copying if !self.is_drained => self.merge_rows(),
            RewritePhase::Deciding => self.end_deciding(target),
            RewritePhase::Copying => self.finish_rows(),
            RewritePhase::Moving => self.move_blocks(target),
            RewritePhase::Installing => self.install(),
        };
        if outcome.is_err() {
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
                self.inputs[index].advance(volume, pin)?;
            }
        }

        for _row in 0..MERGE_STEP_ROWS {
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
            self.key.clear();
            self.key.extend_from_slice(&input.key);
            self.decide(volume, &pins, pin, column, &row)?;
            self.inputs[index].advance(volume, pin)?;
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
        if row.kind == RowKind::RangeDelete {
            return Err(Error::from(ErrorKind::InvalidData));
        }
        let replacement = self.replacement(volume, column, row)?;
        match replacement {
            Some(place) if self.can_drop(place, row) => {
                self.lowest_dropped = self.lowest_dropped.min(row.place);
                self.highest_replacement = self.highest_replacement.max(place);
                self.dropped_rows += 1;
                Ok(())
            },
            None if row.kind == RowKind::Grave && self.hides_nothing(volume, column, row)? => {
                self.lowest_dropped = WritePlace(0);
                self.highest_replacement = self.highest_replacement.max(row.place);
                self.dropped_rows += 1;
                Ok(())
            },
            _kept if self.phase == RewritePhase::Deciding => Ok(()),
            // a grave output that's full leaves the rest of the graves as footer rows in the data output
            _kept if row.kind == RowKind::Grave && self.grave_writer.has_block_room() => {
                self.keep_grave(volume, pins, column, row, replacement)
            },
            _kept => self.keep(volume, pins, pin, column, row, replacement),
        }
    }

    /// Find the newest place above a row that replaces it: the key's first row across the inputs, or a row in a finished
    /// segment. A row replaced only in an open or unfinished segment stays, and that segment's walk counts it in the output
    pub fn replacement(&mut self, volume: &Volume<Backend>, column: ColumnId, row: &FooterRow) -> Result<Option<WritePlace>> {
        let is_top = self.last_column != Some(column) || self.last_key != self.key;
        if is_top {
            self.last_column = Some(column);
            self.last_key.clear();
            self.last_key.extend_from_slice(&self.key);
            self.top_place = row.place;
            self.newest_place = match self.state.newest_walked(volume, column, &self.key, &self.numbers[..self.input_count]) {
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
    pub fn hides_nothing(&self, volume: &Volume<Backend>, column: ColumnId, row: &FooterRow) -> Result<bool> {
        let state = &self.state;
        if volume.segments_parked.load(Ordering::Acquire) > 0
            || volume.stray_outputs.load(Ordering::Acquire) > 0
            || state.cues.has_cue_below(row.place)
            || volume.damage.covering(column, &self.key).is_some()
        {
            return Ok(false);
        }
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
            self.load_edges(volume, pins, column)?;
            let start = ColumnStart {
                column,
                lowest: &self.lowest,
                highest: &self.highest,
            };
            self.writer.start_column(&output.file, &start)?;
            self.output_column = Some(column);
        }
        let offset = self.copy_record(&pin.file, &output.file, row)?;
        let kept = FooterRow { offset, ..*row };
        self.writer.push_row(&output.file, &self.key, &kept)?;
        let block = self.writer.entries().len() as u32;
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
            let output = self.create_output(volume, ByteCount(0), 1)?;
            let base = self.lowest_place_base(volume)?;
            self.grave_writer.start(FileOffset(0), base);
            self.grave_output = Some(output);
        }
        let Some(output) = self.grave_output.clone() else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        if self.grave_column != Some(column) {
            self.load_edges(volume, pins, column)?;
            let start = ColumnStart {
                column,
                lowest: &self.lowest,
                highest: &self.highest,
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
        let block = self.grave_writer.entries().len() as u32;
        self.note_kept_dead(output.number, column, block, &kept, replacement)
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
        if replacement.is_some() {
            if self.kept_dead.len() == self.kept_dead.capacity() {
                return Err(Error::from(ErrorKind::OutOfMemory));
            }
            add_count(&mut self.kept_dead, output, column, block, kept, replacement);
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
            let is_read = self.state.footer_readers.with(|reader| reader.edge_keys(&pin.file, &lines, base, lowest, highest))?;
            if !is_read {
                return Err(Error::from(ErrorKind::InvalidData));
            }
            if !has_edges || self.edge_lowest < self.lowest {
                self.lowest.clear();
                self.lowest.extend_from_slice(&self.edge_lowest);
            }
            if !has_edges || self.edge_highest > self.highest {
                self.highest.clear();
                self.highest.extend_from_slice(&self.edge_highest);
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
        if self.dropped_rows == 0 {
            return Err(Error::from(ErrorKind::Interrupted));
        }
        let state = Arc::clone(&self.state);
        self.open_output(&state.volumes[self.volume], target)
    }

    /// End the copying. Several inputs becoming one wins back overlap whatever it dropped, and one input that dropped nothing
    /// has nothing to install
    pub fn finish_rows(&mut self) -> Result<()> {
        let Some(output) = self.output.clone() else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        if self.dropped_rows == 0 && self.input_count == 1 {
            return Err(Error::from(ErrorKind::Interrupted));
        }
        self.flush(&output.file)?;
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
            return self.finish_outputs(&output, distance);
        }

        let part = left.min(self.buffer.len() as u64) as usize;
        output.file.read_exact_at(FileOffset(from), &mut self.buffer[..part])?;
        output.file.write_at(FileOffset(from - distance), &[IoSlice::new(&self.buffer[..part])])?;
        self.moved += part as u64;
        Ok(())
    }

    /// Make the outputs whole, the graves first. The last tail lists the inputs, and open takes that as the merge having
    /// happened, so everything that stays has to be durable by then
    pub fn finish_outputs(&mut self, output: &RewriteOutput<Backend::File>, distance: u64) -> Result<()> {
        let state = Arc::clone(&self.state);
        let numbers = self.numbers;
        let inputs = &numbers[..self.input_count];
        let has_data = !self.writer.columns().is_empty();
        if let Some(graves) = self.grave_output.clone() {
            self.grave_writer.close_block(&graves.file)?;
            self.grave_writer.is_output = true;
            self.grave_writer.is_grave_output = has_data;
            let totals = self.grave_writer.finish(&graves.file, graves.number, FileOffset(0), &[], state.record_key, inputs)?;
            self.grave_totals = Some(totals);
            self.name_output(&graves, 1)?;
        }
        if has_data {
            self.writer.move_down(ByteCount(distance));
            self.writer.is_output = true;
            let totals = self.writer.finish(&output.file, output.number, self.records_end, &[], state.record_key, inputs)?;
            self.totals = Some(totals);
            self.name_output(output, 0)?;
        }
        self.phase = RewritePhase::Installing;
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
        hold(&volume.partial_outputs)[slot] = Some(named.clone());
        if slot == 0 {
            self.output = Some(named);
        } else {
            self.grave_output = Some(named);
        }
        state.io.sync_directory(root)
    }

    /// Swap the inputs for the outputs in every list, save that, and park the inputs. The count moves before the last look:
    /// a read or a cue that takes its point after it has every replacement under that point
    pub fn install(&mut self) -> Result<()> {
        self.install_with(|| {})
    }

    /// Install, and run some work once the grave output is listed and the inputs are not yet gone
    pub fn install_with(&mut self, between: impl FnOnce()) -> Result<()> {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        // a full park clears as reads finish, and this step comes round again until the wait runs out
        if !volume.can_park(self.input_count) {
            self.park_waits += 1;
            if self.park_waits > MOST_PARK_WAITS {
                return Err(Error::from(ErrorKind::Interrupted));
            }
            return Ok(());
        }
        if state.read_point() < self.highest_replacement {
            return Err(Error::from(ErrorKind::Interrupted));
        }
        state.cues.note_reclaim();
        if state.cues.has_cue_between(self.lowest_dropped, self.highest_replacement) {
            return Err(Error::from(ErrorKind::Interrupted));
        }
        let numbers = self.numbers;
        let inputs = &numbers[..self.input_count];
        self.enter_table(volume)?;
        // a kept grave hides an older row outside the merge, so it's in a list before the inputs leave theirs, and until
        // then a read finds it twice at one place
        if let (Some(_totals), Some(output)) = (self.grave_totals, &self.grave_output) {
            list_output(&state, volume, output, &self.grave_writer, &[]);
        }
        between();
        if let (Some(_totals), Some(output)) = (self.totals, &self.output) {
            list_output(&state, volume, output, &self.writer, inputs);
        }
        // a column every row of which went has no entry to swap in
        for directory in state.directories.iter().flatten() {
            if !directory.replace(None, inputs) {
                volume.fail();
            }
        }

        let retired: [_; MOST_MERGE_SEGMENTS] = array::from_fn(|index| inputs.get(index).and_then(|number| volume.table.retire(*number)));
        self.summarize(volume, inputs);
        let saved = state.save_summary(volume);

        *hold(&volume.partial_outputs) = [None, None];
        // a merge that kept no data never made its file a segment
        if let (None, Some(output)) = (self.totals, self.output.take()) {
            self.leave_outputs(volume, Some(output), None);
        }
        self.output = None;
        self.grave_output = None;
        self.target = None;
        volume.is_rewriting.store(false, Ordering::Release);
        volume.segments_rewritten.fetch_add(1, Ordering::AcqRel);
        volume.wants_compaction.store(true, Ordering::Release);
        for (segment, retired) in inputs.iter().zip(retired) {
            if let Some(retired) = retired {
                park(volume, retired, *segment, saved.is_ok());
            }
        }
        saved
    }

    /// Put the outputs in the table, both or neither. No list holds them yet, so no read can have found the first
    pub fn enter_table(&self, volume: &Volume<Backend>) -> Result<()> {
        if let (Some(totals), Some(output)) = (self.totals, &self.output)
            && !volume.table.install(output.file.clone(), &totals.head, self.writer.columns(), self.writer.entries())
        {
            return Err(Error::from(ErrorKind::OutOfMemory));
        }
        if let (Some(totals), Some(output)) = (self.grave_totals, &self.grave_output)
            && !volume.table.install(output.file.clone(), &totals.head, self.grave_writer.columns(), self.grave_writer.entries())
        {
            if let (Some(_totals), Some(output)) = (self.totals, &self.output)
                && let Some(retired) = volume.table.retire(output.number)
            {
                volume.table.free_retired(&retired);
                self.state.io.close_file(retired.file);
            }
            return Err(Error::from(ErrorKind::OutOfMemory));
        }
        Ok(())
    }

    /// Replace the inputs' summary rows with the outputs'. Their counts come from the rows they kept, and they start walked
    pub fn summarize(&mut self, volume: &Volume<Backend>, inputs: &[SegmentNumber]) {
        let mut rows = hold(&volume.summary);
        for number in inputs {
            rows.remove(*number);
        }
        if let (Some(totals), Some(output)) = (self.totals, &self.output) {
            self.push_row(&mut rows, &self.writer, output.number, &totals, self.records_end);
        }
        if let (Some(totals), Some(output)) = (self.grave_totals, &self.grave_output) {
            self.push_row(&mut rows, &self.grave_writer, output.number, &totals, FileOffset(0));
        }
        self.kept_dead.sort_unstable_by_key(|count| (count.segment, count.block));
        let number = self.output.as_ref().map_or(SegmentNumber(0), |output| output.number);
        rows.apply_walk(number, self.writer.entries().len() as u32, &self.kept_dead);
        drop(rows);
        for count in self.kept_dead.iter().filter(|count| count.rows > 0) {
            volume.table.drop_live(count.segment, count.block, count.rows, count.replacement);
        }
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
        let facts = SegmentFacts {
            segment: number,
            file_length: totals.file_length,
            data_end,
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
        let columns = (0..writer.columns().len()).map(|index| writer.column_facts(index));
        if !rows.push(&facts, columns, [].into_iter()) {
            self.state.note_dropped_row();
        }
    }

    /// Try again to remove outputs a failure left behind
    pub fn clear_stray(&self) {
        let volume = &self.state.volumes[self.volume];
        let [data, graves] = mem::take(&mut *hold(&volume.partial_outputs));
        self.leave_outputs(volume, data, graves);
    }

    /// Remove outputs, or keep them listed, as strays that hold every grave back until they're gone. The data output goes
    /// first: its tail lists the inputs, so at a crash open removes them, and the grave output has to be there to take over
    pub fn leave_outputs(
        &self,
        volume: &Volume<Backend>,
        data: Option<RewriteOutput<Backend::File>>,
        graves: Option<RewriteOutput<Backend::File>>,
    ) {
        let data = data.and_then(|output| self.state.remove_output(volume, output).err());
        let graves = match data {
            Some(_) => graves,
            None => graves.and_then(|output| self.state.remove_output(volume, output).err()),
        };
        let strays = usize::from(data.is_some()) + usize::from(graves.is_some());
        *hold(&volume.partial_outputs) = [data, graves];
        volume.stray_outputs.store(strays as u64, Ordering::Release);
    }

    /// Give the merge up: the inputs stay, and the outputs go whatever name they have reached
    pub fn abandon(&mut self) {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        *hold(&volume.partial_outputs) = [None, None];
        let (data, graves) = (self.output.take(), self.grave_output.take());
        self.leave_outputs(volume, data, graves);
        self.target = None;
        volume.is_rewriting.store(false, Ordering::Release);
    }
}

/// Read the bytes a segment has given back in place: its length less what its file still takes on the disk
pub fn freed_in_place<Backend: Io>(volume: &Volume<Backend>, row: &SummaryRow<'_>) -> u64 {
    let Some(pin) = volume.table.pin(row.facts.segment) else {
        return 0;
    };
    pin.file.held_bytes().map_or(0, |held| row.facts.file_length.0.saturating_sub(held.0))
}

/// Put a whole output in every directory, in place of the segments it replaces
pub fn list_output<Backend: Io>(
    state: &EngineState<Backend>,
    volume: &Volume<Backend>,
    output: &RewriteOutput<Backend::File>,
    writer: &FooterWriter,
    removed: &[SegmentNumber],
) {
    volume.table.mark_walked(output.number);
    for column in writer.columns() {
        let entry = DirectoryEntry {
            shared_length: column.shared_length,
            shared_sum: column.shared_sum,
            lowest: column.lowest,
            highest: column.highest,
            reach: column.highest,
            segment: output.number,
        };
        if let Some(directory) = &state.directories[column.column.0 as usize]
            && !directory.replace(Some(&entry), removed)
        {
            volume.fail();
        }
    }
}

/// Check a segment can go into a merge at all. Damage and covers answer for more than their rows
pub fn is_mergeable(row: &SummaryRow<'_>) -> bool {
    row.facts.is_walked() && !row.facts.is_damaged && !row.facts.has_covers && row.facts.block_count > 0
}

/// Check a row's counts say a merge would win back its share of the rows or the bytes, past the bytes already freed in place
pub fn is_rewrite_ready(row: &SummaryRow<'_>, freed_bytes: u64) -> bool {
    if !is_mergeable(row) {
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

/// Check a row's graves make up the share that would trigger a merge, so deciding first can save the copy
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

/// Check two segments share a column and their ranges meet. A summary keeps only a key's first bytes, so a range here can only
/// be wider than the truth
pub fn overlaps(left: &SummaryRow<'_>, right: &SummaryRow<'_>) -> bool {
    let mut left_columns = left.cursor();
    for _column in 0..left.column_count {
        let Some(ours) = left_columns.column() else {
            return false;
        };
        let mut right_columns = right.cursor();
        for _column in 0..right.column_count {
            let Some(theirs) = right_columns.column() else {
                return false;
            };
            let is_before = !theirs.bound.is_empty() && theirs.bound <= ours.lowest;
            let is_after = !ours.bound.is_empty() && ours.bound <= theirs.lowest;
            if ours.column == theirs.column && !is_before && !is_after {
                return true;
            }
        }
    }
    false
}

/// Find a segment past the last one tried that overlaps more segments than the bound allows. A look starts from a few
/// segments only, so a volume of many segments pays little, and the next look starts where this one stopped
pub fn overlap_start<'rows>(rows: &'rows SummaryRows, last_tried: SegmentNumber) -> Option<SummaryRow<'rows>> {
    let mut looks = 0;
    let after = rows.rows().filter(is_mergeable).filter(|row| row.facts.segment > last_tried);
    let before = rows.rows().filter(is_mergeable).filter(|row| row.facts.segment <= last_tried);
    for row in after.chain(before) {
        if rows.rows().filter(is_mergeable).filter(|other| overlaps(&row, other)).count() > MOST_OVERLAP {
            return Some(row);
        }
        looks += 1;
        if looks == OVERLAP_LOOKS {
            return None;
        }
    }
    None
}
