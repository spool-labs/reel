//! Compaction's merge: up to 8 sealed segments copied into a new one, without the rows no read can reach
//!
//! Records land at the front of the output and row blocks land past the inputs' data ends, since the records' end isn't
//! known until the last row. The blocks then move down to meet the records and the tail makes the file a segment. Graves that
//! stay go to an output of their own, and it is whole first: the last tail to land lists the inputs, and open removes them
//! on sight, however far the install got.

use std::array;
use std::iter;
use std::io::{Error, ErrorKind, IoSlice, Result};
use std::mem;
use std::sync::{Arc, Mutex};
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::maintenance_report::JobKind;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, DeathMark, FileOffset, RootId, RowCount, WritePlace};

use crate::config::MAX_KEY_BYTES;
use crate::engine::compact_job::park;
use crate::engine::engine_state::EngineState;
use crate::engine::read_path::SealedRow;
use crate::engine::recovery::save_cleared_spans;
use crate::engine::open_segment::{data_path, output_path};
use crate::engine::seal_job::MOST_SEGMENT_BLOCKS;
use crate::engine::volume::{GRAVE_SLOT, GaveUp, MOST_MERGE_OUTPUTS, NO_SEAL, OutputSlots, RewriteOutput, Volume};
use crate::engine::walk_job::add_count;
use crate::guard::hold;
use crate::lifetime::MarkRange;
use crate::io::{Io, IoFile};
use crate::pool::{touched_capacity, touched_filled, touched_zeroes};
use crate::row_kind::RowKind;
use crate::segment::damage_list::DamagedRange;
use crate::segment::column_directory::Listing;
use crate::segment::footer_writer::{ColumnStart, FooterTotals, FooterWriter};
use crate::index::complete_table::CompleteTable;
use crate::index::key_hash::{KeyHash, hash_key};
use crate::segment::row_block::{FooterRow, ROW_BLOCK_BYTES, RowBlockWalk};
use crate::segment::segment_table::SegmentPin;
use crate::segment::segment_tail::{BlockCount, ColumnTail, MAX_COLUMNS, MOST_MERGE_SEGMENTS, SegmentNumber};
use crate::segment::summary::{ColumnFacts, DeadCount, SUMMARY_KEY_BYTES, SegmentFacts, SummaryRow, SummaryRows};

/// A bar counts in quarters of a segment's bytes
pub const BAR_QUARTERS: u64 = 4;
/// A segment is rewritten once this many quarters of its bytes are dead, the plain bar: a half
pub const PLAIN_BAR_QUARTERS: u64 = 2;
/// A marked segment waits for this many while its highest mark stands, since it dies whole soon. A rewrite there copies at
/// most a third of a byte for each byte freed, so a record freed early still comes back
pub const EARLY_BAR_QUARTERS: u64 = 3;
/// Bytes a footer row takes, about, so a rewrite weighs rows it drops beside records. A segment of graves holds rows and no records
pub const ROW_FOOTER_BYTES: u64 = 41;
/// Bytes of one job's merge buffer, which carries records and then row blocks. With a row block for each input, a job's
/// merge buffers come to about 2 MiB
pub const MERGE_BUFFER_BYTES: usize = 3 * 512 * 1024;
/// The whole of a share, for comparing what segments win back
pub const SHARE_SCALE: u64 = 1_000_000;
/// Most row blocks of an output that can hold rows kept for a cue, past which the merge waits for the cue to go
pub const MOST_KEPT_BLOCKS: usize = 1_024;
/// Rows one step of a merge decides
pub const MERGE_STEP_ROWS: usize = 4_096;
/// The widest span of a segment one stripe of a record rewrite covers
pub const MOST_STRIPE_BYTES: u64 = 256 * 1024 * 1024;
/// Rows a stripe lists at once. A span with more narrows to the half with the lowest offsets
pub const MOST_STRIPE_ROWS: usize = 65_536;
/// Most row blocks the output for kept graves can have, past which the merge gives way
pub const MOST_GRAVE_BLOCKS: BlockCount = BlockCount(2_048);
/// Rounds an install waits for room to park its inputs, past which the merge gives way
pub const MOST_PARK_WAITS: u32 = 4_096;
/// Most segments of a column that overlap one key before a merge is due
pub const MOST_OVERLAP: usize = 32;
/// Keys inside a new segment one look tries for the crowded point, which bounds what a look costs
pub const OVERLAP_PROBES: usize = 16;
/// Most segments one look for an overlap starts from, which bounds what a look costs
pub const OVERLAP_LOOKS: usize = 16;

/// A column of a finished output that isn't the last of its set, kept for its list entry and its summary row
#[derive(Clone, Copy, Debug, Default)]
pub struct StagedColumn {
    pub tail: ColumnTail,
    pub lowest_at: usize,
    pub bound_at: usize,
    pub end: usize,
    pub live_bytes: ByteCount,
}

/// A finished output that waits in the table, in no list, for its set to install
#[derive(Clone, Copy, Debug)]
pub struct StagedOutput {
    pub facts: SegmentFacts,
    pub newest_place: WritePlace,
    pub first_column: usize,
    pub column_count: usize,
}

/// A marked row a stripe will copy
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StripeRow {
    pub offset: FileOffset,
    pub length: ByteCount,
    pub ordinal: u32,
}

/// Marked segments a rewrite took, by the bar each one passed
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BarsFired {
    pub early: u64,
    pub plain: u64,
}

/// What a merge is copying
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MergeTarget {
    pub blocks_at: FileOffset,
    pub data: ByteCount,
    pub length: ByteCount,
    pub marks: MarkRange,
}

/// How far a merge has gone
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RewritePhase {
    /// Rows of a grave-heavy segment are being decided, and nothing is copied unless one would drop
    Deciding,
    /// Records go across in offset order, a stripe at a time, for one input whose rows are marked
    Placing,
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
    pub block_rows: u64,
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
            block_rows: 0,
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

    /// Check a row list this long has room for every row of the input
    pub fn row_room(&self, rows: usize) -> bool {
        self.block_rows <= rows as u64
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
    lane: usize,
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
    cover_place: Option<WritePlace>,
    lowest_dropped: WritePlace,
    pub highest_replacement: WritePlace,
    dropped_rows: u64,
    dropped_graves: u64,
    kept_dead: Vec<DeadCount>,
    last_tried: SegmentNumber,
    park_waits: u32,
    pub cut_floor: u64,
    cut_bytes: u64,
    data_slot: usize,
    is_cutting: bool,
    is_top_row: bool,
    output_rows: u64,
    output_last_key: Vec<u8>,
    grave_last_key: Vec<u8>,
    staged: Vec<StagedOutput>,
    staged_columns: Vec<StagedColumn>,
    staged_keys: Vec<u8>,
    pub forced: Option<SegmentNumber>,
    pub last_slot: Option<(KeyHash, SegmentNumber, u32)>,
    /// One bit a row of a single input, set when the deciding pass keeps its record
    marks: Vec<u64>,
    /// Where each marked row's record landed in the output, by the row's place in the input
    placed: Vec<u32>,
    stripe: Vec<StripeRow>,
    stripe_start: u64,
    stripe_end: u64,
    row_ordinal: usize,
    is_ordered: bool,
    /// Tests of a merge's cut set this, since an offset-order copy runs to one output
    pub is_key_order_held: bool,
    /// A damaged segment the next pick copies whole, and the spans its lost blocks leave
    pub rebuild: Option<SegmentNumber>,
    pub rebuild_blocks: u32,
    pub bars_fired: BarsFired,
    pub lost_spans: Vec<DamagedRange>,
    is_unwalked: bool,
    crowded_key: Vec<u8>,
    pub has_looks_left: bool,
    looked_up_to: SegmentNumber,
}

impl<Backend: Io> RewriteJob<Backend> {
    /// Allocate the job's fixed buffers
    pub fn new(state: Arc<EngineState<Backend>>, volume: usize, lane: usize) -> Self {
        let most_rows = state.volumes[volume].segment_rows.0 as usize;
        // a mutex's first lock can allocate, and this one's first real use could be in a close
        drop(hold(&state.volumes[volume].lanes[lane].partial_outputs));
        drop(hold(&state.volumes[volume].lanes[lane].merging));
        let keys = MAX_KEY_BYTES as usize;
        let cut_floor = state.volumes[volume].segment_bytes.0 / MOST_MERGE_OUTPUTS as u64;
        Self {
            state,
            volume,
            lane,
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
            cover_place: None,
            lowest_dropped: WritePlace(u64::MAX),
            highest_replacement: WritePlace(0),
            dropped_rows: 0,
            dropped_graves: 0,
            kept_dead: touched_capacity(MOST_KEPT_BLOCKS, DeadCount::default()),
            last_tried: SegmentNumber(0),
            park_waits: 0,
            cut_floor,
            cut_bytes: 0,
            data_slot: 0,
            is_cutting: false,
            is_top_row: false,
            output_rows: 0,
            output_last_key: touched_capacity(keys, 0),
            grave_last_key: touched_capacity(keys, 0),
            staged: touched_capacity(MOST_MERGE_OUTPUTS, StagedOutput { facts: SegmentFacts::default(), newest_place: WritePlace(0), first_column: 0, column_count: 0 }),
            staged_columns: touched_capacity(MOST_MERGE_OUTPUTS * MAX_COLUMNS, StagedColumn::default()),
            staged_keys: touched_capacity(MOST_MERGE_OUTPUTS * MAX_COLUMNS * (2 * SUMMARY_KEY_BYTES + 2), 0),
            forced: None,
            last_slot: None,
            marks: touched_filled(most_rows.div_ceil(u64::BITS as usize), 0),
            placed: touched_filled(most_rows, 0),
            stripe: touched_capacity(MOST_STRIPE_ROWS, StripeRow::default()),
            stripe_start: 0,
            stripe_end: 0,
            row_ordinal: 0,
            is_ordered: false,
            is_key_order_held: false,
            rebuild: None,
            rebuild_blocks: 0,
            bars_fired: BarsFired::default(),
            lost_spans: Vec::new(),
            is_unwalked: false,
            crowded_key: touched_capacity(SUMMARY_KEY_BYTES, 0),
            has_looks_left: false,
            looked_up_to: SegmentNumber(0),
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

    /// Check a merge of unwalked segments is under way, which runs ahead of the walk and needs nothing from it
    pub fn is_unwalked_merge(&self) -> bool {
        self.target.is_some() && self.is_unwalked
    }

    /// Check a merge is under way
    pub fn is_running(&self) -> bool {
        self.target.is_some()
    }

    /// Count the outputs that wait for this merge's last one
    pub fn staged_count(&self) -> usize {
        self.staged.len()
    }

    /// Read the newest segment a look for an overlap has passed
    pub fn looked_up_to(&self) -> SegmentNumber {
        self.looked_up_to
    }

    /// Read the segments going in
    pub fn input_numbers(&self) -> &[SegmentNumber] {
        &self.numbers[..self.input_count]
    }

    /// Start on a run of segments over the overlap bound, or else on the next ready segments past the last one tried. A merge
    /// of walked segments needs no walk target in progress, since it would count rows this merge has decided on, and the walks
    /// wait from here to the install. A grave-heavy segment is decided first, and copies nothing if no row would drop
    pub fn start(&mut self) -> bool {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        let Some((target, is_grave_heavy)) = self.pick(volume) else {
            return false;
        };
        self.clear(target);
        volume.lanes[self.lane].is_rewriting.store(!self.is_unwalked, Ordering::Release);
        let mut inputs = [SegmentNumber(0); MOST_MERGE_SEGMENTS];
        inputs[..self.input_count].copy_from_slice(&self.numbers[..self.input_count]);
        *hold(&volume.lanes[self.lane].inputs) = inputs;
        // one input copies in offset order, and a deciding pass says which rows go across
        self.is_ordered = self.input_count == 1 && self.rebuild.is_none() && !self.is_key_order_held && self.inputs[0].row_room(self.placed.len());
        self.marks.fill(0);
        self.row_ordinal = 0;
        if is_grave_heavy || self.is_ordered {
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

    /// Pick the inputs: a run of segments over the overlap bound first, since that comes ahead of the walk, then ready segments
    pub fn pick(&mut self, volume: &Volume<Backend>) -> Option<(MergeTarget, bool)> {
        if !volume.can_start_rewrite() {
            return None;
        }
        if volume.stray_outputs.load(Ordering::Acquire) > 0 {
            return None;
        }
        let room = volume.park_room().min(MOST_MERGE_SEGMENTS);
        if room == 0 {
            return None;
        }
        let rows = hold(&volume.summary);
        self.is_unwalked = false;
        self.bars_fired = BarsFired::default();
        // its rows counted for nothing a walk can trust, so the output goes through the forward walk they owe
        if let Some(row) = self.rebuild.and_then(|segment| rows.get(segment)) {
            self.is_unwalked = true;
            let target = self.gather(volume, &rows, &row, 1, |_row| false);
            // the row counts every block the segment had, and only the ones that read are in the table
            self.inputs[0].block_count = self.rebuild_blocks;
            return Some((target, false));
        }
        if let Some(target) = self.pick_overlap(volume, &rows, room) {
            return Some((target, false));
        }
        if volume.has_walk_target.load(Ordering::Acquire) {
            return None;
        }
        // a full pool refuses range deletes, so the segment holding the oldest cover back goes whatever the trigger says
        if let Some(row) = self.forced.take().and_then(|forced| rows.get(forced)).filter(|row| is_mergeable(volume, row)) {
            return Some((self.gather(volume, &rows, &row, 1, |_row| false), false));
        }
        let current = DeathMark(self.state.current_mark.load(Ordering::Relaxed));
        let gave_up = self.gave_up_list(volume);
        let is_ready = |row: &SummaryRow<'_>| {
            row.facts.is_walked() && !gave_up.holds(row.facts.segment) && is_rewrite_ready(volume, row, 0, current) && is_rewrite_ready(volume, row, freed_in_place(volume, row), current)
        };
        let ready = rows.rows().filter(is_ready);
        // the most won back for the bytes read goes first, past the one last tried, so a merge that gave way lets the next one up
        let after = ready.clone().filter(|row| row.facts.segment > self.last_tried).max_by_key(reclaim_share);
        if let Some(first) = after.or_else(|| ready.max_by_key(reclaim_share)) {
            let is_grave_heavy = is_grave_heavy(&first);
            let lifetime = first.facts.marks;
            let is_more = |row: &SummaryRow<'_>| is_ready(row) && row.facts.segment > first.facts.segment && row.facts.marks.shares_lifetime(lifetime);
            let target = self.gather(volume, &rows, &first, room, is_more);
            self.note_bars(&rows, current);
            return Some((target, is_grave_heavy));
        }
        None
    }

    /// Pick a run of segments over the overlap bound, all walked or all unwalked: a walked input's rows are counted and an
    /// unwalked one's aren't, so a mix leaves no rule for the output's counts
    pub fn pick_overlap(&mut self, volume: &Volume<Backend>, rows: &SummaryRows, room: usize) -> Option<MergeTarget> {
        let sealing = volume.sealing.load(Ordering::Acquire);
        let walking = volume.walk_segment.load(Ordering::Acquire);
        let can_merge = |row: &SummaryRow<'_>| {
            let segment = row.facts.segment.0;
            is_overlap_input(row) && segment != sealing && segment != walking && !volume.is_taken(row.facts.segment)
        };
        let has_bound = |column: ColumnId| self.state.complete_table(column).is_none();
        let found = overlap_start(rows, &mut self.looked_up_to, can_merge, has_bound, &mut self.crowded_key);
        let looked_up_to = self.looked_up_to;
        self.has_looks_left = rows.rows().any(|row| row.facts.segment > looked_up_to);
        let (crowded, column) = found?;
        let point = &self.crowded_key;
        let lifetime = crowded.facts.marks;
        let is_near = |row: &SummaryRow<'_>| can_merge(row) && covers_point(row, column, point) && row.facts.marks.shares_lifetime(lifetime);
        // unwalked segments go first, since they need nothing from the walk
        let is_unwalked = rows.rows().filter(|row| is_near(row) && !row.facts.is_walked()).take(2).count() == 2;
        if !is_unwalked && volume.has_walk_target.load(Ordering::Acquire) {
            // the walk goes first and asks again when it ends, and a look left here would keep the planner from it
            self.has_looks_left = false;
            return None;
        }
        let is_more = |row: &SummaryRow<'_>| is_near(row) && row.facts.is_walked() != is_unwalked;
        let target = gather_smallest(volume, rows, room, is_more, &mut self.inputs, &mut self.numbers, &mut self.input_count);
        // one segment alone wins back no overlap, and the next look starts past it
        if self.input_count < 2 {
            self.looked_up_to = crowded.facts.segment;
            return None;
        }
        self.is_unwalked = is_unwalked;
        if is_unwalked {
            let mut merging = hold(&volume.lanes[self.lane].merging);
            merging[..self.input_count].copy_from_slice(&self.numbers[..self.input_count]);
        }
        Some(target)
    }

    /// Count the marked segments picked, by the bar each one passed
    pub fn note_bars(&mut self, rows: &SummaryRows, current: DeathMark) {
        for number in &self.numbers[..self.input_count] {
            let Some(marks) = rows.get(*number).map(|row| row.facts.marks).filter(|marks| !marks.is_empty()) else {
                continue;
            };
            if bar_quarters(marks, current) == EARLY_BAR_QUARTERS {
                self.bars_fired.early += 1;
            } else {
                self.bars_fired.plain += 1;
            }
        }
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
        let mut marks = MarkRange::NONE;
        let mut next = Some(*first);
        while let Some(row) = next {
            let facts = row.facts;
            let input = &mut self.inputs[self.input_count];
            input.segment = facts.segment;
            input.block_count = facts.block_count;
            input.block_rows = rows_of(&row);
            self.numbers[self.input_count] = facts.segment;
            self.input_count += 1;
            data += facts.data_end.0;
            length += facts.file_length.0;
            blocks += facts.block_count;
            marks = marks.join(facts.marks);
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
            blocks_at: FileOffset(data.min(2 * volume.segment_bytes.0)),
            data: ByteCount(data),
            length: ByteCount(length),
            marks,
        }
    }

    /// Create the data output and start every input over. It reserves the inputs' whole length, so a merge holds that much
    /// twice until its footer cuts the output down
    pub fn open_output(&mut self, volume: &Volume<Backend>, target: MergeTarget) -> Result<()> {
        self.cut_bytes = (target.data.0 / MOST_MERGE_OUTPUTS as u64).max(self.cut_floor);
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
        let Some(target) = self.target else {
            return;
        };
        let outcome = match self.phase {
            RewritePhase::Deciding | RewritePhase::Copying if !self.is_drained && !self.is_cutting => self.merge_rows(),
            RewritePhase::Deciding => self.end_deciding(target),
            RewritePhase::Placing => self.place_rows(),
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
            // a cut leaves the row where it is, for the next output to take from the top
            if self.is_cutting {
                self.last_column = None;
                return Ok(());
            }
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
        self.row_ordinal += 1;
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
            _kept if self.phase == RewritePhase::Deciding => {
                self.mark_kept(row);
                Ok(())
            },
            // a grave output that's full leaves the rest of the graves as footer rows in the data output
            _kept if row.kind == RowKind::Grave && self.grave_writer.has_block_room() => {
                self.keep_grave(volume, pins, column, row, replacement)
            },
            _kept if self.is_top_row && self.is_cut_due() => {
                self.is_cutting = true;
                Ok(())
            },
            _kept => self.keep(volume, pins, pin, column, row, replacement),
        }
    }

    /// Check the output has reached its share of the merge, and another may follow it. A cut only falls before a key's first
    /// row, so a key's versions stay in one output
    pub fn is_cut_due(&self) -> bool {
        let bytes = self.records_end.0 + self.used as u64;
        let is_full = bytes >= self.cut_bytes || self.writer.entries().len() + 2 >= MOST_SEGMENT_BLOCKS.0 as usize;
        !self.is_ordered && self.output_rows > 0 && self.data_slot + 1 < MOST_MERGE_OUTPUTS && is_full
    }

    /// Find the newest place above a row that replaces it: the key's first row across the inputs, or a row in a finished
    /// segment. A row replaced only in an open or unfinished segment stays, and that segment's walk counts it in the output
    pub fn replacement(&mut self, volume: &Volume<Backend>, column: ColumnId, row: &FooterRow) -> Result<Option<WritePlace>> {
        let is_top = self.last_column != Some(column) || self.last_key != self.key;
        self.is_top_row = is_top;
        if is_top {
            self.last_column = Some(column);
            self.last_key.clear();
            self.last_key.extend_from_slice(&self.key);
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
        if let Some(table) = state.complete_table(column) {
            return Ok(!self.has_older_slot(volume, table, column, row));
        }
        match state.has_older_row(volume, column, &self.key, row.place, &self.numbers[..self.input_count]) {
            Ok(has_older) => Ok(!has_older),
            Err(StoreError::Corruption { .. }) => Ok(false),
            Err(_error) => Err(Error::from(ErrorKind::Interrupted)),
        }
    }

    /// Check a complete column still holds an older row of a grave's key: an open row, or any slot of its tag outside this merge,
    /// live or stale. A slot of another key can only keep the grave longer, and past the mark a row can stand with no slot at all
    pub fn has_older_slot(&self, volume: &Volume<Backend>, table: &CompleteTable, column: ColumnId, row: &FooterRow) -> bool {
        let state = &self.state;
        let point = WritePlace(row.place.0.saturating_sub(1));
        let is_own = |segment: u32| self.numbers[..self.input_count].contains(&SegmentNumber(u64::from(segment)));
        table.mark().is_some()
            || state.index.lookup(column, &self.key, point, volume.view()).row.is_some()
            || table.has_other_slot(hash_key(column, &self.key), is_own, |segment| state.is_slot_gone(volume, segment))
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
                self.writer.end_column(&self.output_last_key);
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
            None => self.copy_record(&pin.file, &output.file, row)?,
        };
        let kept = FooterRow { offset, ..*row };
        self.writer.push_row(&output.file, &self.key, &kept)?;
        self.output_rows += 1;
        self.output_last_key.clear();
        self.output_last_key.extend_from_slice(&self.key);
        let block = self.writer.entries().len() as u32;
        self.give_slot(volume, output.number, column, block);
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
                self.grave_writer.end_column(&self.grave_last_key);
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
        self.grave_last_key.clear();
        self.grave_last_key.extend_from_slice(&self.key);
        let block = self.grave_writer.entries().len() as u32;
        self.give_slot(volume, output.number, column, block);
        self.note_kept_dead(output.number, column, block, &kept, replacement)
    }

    /// Give a kept row a slot in its column's table, before the inputs go, so a read that meets the change finds it twice
    pub fn give_slot(&mut self, volume: &Volume<Backend>, number: SegmentNumber, column: ColumnId, block: u32) {
        if let Some(table) = self.state.complete_table(column) {
            let hash = hash_key(column, &self.key);
            if self.last_slot != Some((hash, number, block)) {
                table.insert(hash, number, block as u16, |segment| self.state.is_slot_gone(volume, segment));
                self.last_slot = Some((hash, number, block));
            }
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
        if self.dropped_rows == 0 {
            self.note_gave_up();
            return Err(Error::from(ErrorKind::Interrupted));
        }
        let state = Arc::clone(&self.state);
        self.open_output(&state.volumes[self.volume], target)?;
        self.row_ordinal = 0;
        if self.is_ordered {
            self.stripe.clear();
            self.stripe_start = 0;
            self.stripe_end = MOST_STRIPE_BYTES;
            self.phase = RewritePhase::Placing;
        }
        Ok(())
    }

    /// Note a kept row of a deciding pass, for the copy to take in offset order
    pub fn mark_kept(&mut self, row: &FooterRow) {
        if self.is_ordered && row.kind == RowKind::Value {
            let ordinal = self.row_ordinal - 1;
            self.marks[ordinal / u64::BITS as usize] |= 1 << (ordinal % u64::BITS as usize);
        }
    }

    /// Check a row was marked
    pub fn is_marked(&self, ordinal: usize) -> bool {
        self.marks[ordinal / u64::BITS as usize] >> (ordinal % u64::BITS as usize) & 1 != 0
    }

    /// Read where the row in hand's record landed, if a stripe copied it
    pub fn placed_offset(&self) -> Option<FileOffset> {
        let ordinal = self.row_ordinal - 1;
        (self.is_ordered && self.is_marked(ordinal)).then(|| FileOffset(u64::from(self.placed[ordinal])))
    }

    /// Walk a step of the input's rows, listing the marked ones in this stripe, and copy the stripe once the rows run out
    pub fn place_rows(&mut self) -> Result<()> {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        let Some(pin) = volume.table.pin(self.numbers[0]) else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        if self.inputs[0].row.is_none() && self.inputs[0].length == 0 {
            self.inputs[0].advance(volume, &pin)?;
        }
        for _row in 0..MERGE_STEP_ROWS {
            let Some(row) = self.inputs[0].row else {
                return self.copy_stripe(&pin.file);
            };
            let ordinal = self.row_ordinal;
            self.row_ordinal += 1;
            if self.is_marked(ordinal) && row.offset.0 >= self.stripe_start && row.offset.0 < self.stripe_end {
                self.list_stripe_row(&row, ordinal);
            }
            self.inputs[0].advance(volume, &pin)?;
        }
        Ok(())
    }

    /// List a row, narrowing the stripe to its lowest half when the list is full. The rows that leave wait for the next stripe
    pub fn list_stripe_row(&mut self, row: &FooterRow, ordinal: usize) {
        if self.stripe.len() == MOST_STRIPE_ROWS {
            self.stripe.sort_unstable_by_key(|listed| listed.offset);
            self.stripe.truncate(MOST_STRIPE_ROWS / 2);
            self.stripe_end = self.stripe.last().map_or(self.stripe_end, |listed| listed.offset.0 + 1);
            if row.offset.0 >= self.stripe_end {
                return;
            }
        }
        self.stripe.push(StripeRow { offset: row.offset, length: row.length, ordinal: ordinal as u32 });
    }

    /// Copy a stripe's records in offset order, and go round for the next stripe or on to the footer once the records run out
    pub fn copy_stripe(&mut self, from: &Backend::File) -> Result<()> {
        let Some(output) = self.output.clone() else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        self.stripe.sort_unstable_by_key(|listed| listed.offset);
        for index in 0..self.stripe.len() {
            let listed = self.stripe[index];
            let row = FooterRow { place: WritePlace(0), offset: listed.offset, length: listed.length, kind: RowKind::Value };
            let at = self.copy_record(from, &output.file, &row)?;
            self.placed[listed.ordinal as usize] = at.0 as u32;
        }
        self.stripe.clear();
        self.row_ordinal = 0;
        self.inputs[0].rewind();
        self.stripe_start = self.stripe_end;
        self.stripe_end = self.stripe_start + MOST_STRIPE_BYTES;
        if self.stripe_start >= self.inputs_data_end() {
            self.phase = RewritePhase::Copying;
        }
        Ok(())
    }

    /// Read where the input's records end
    pub fn inputs_data_end(&self) -> u64 {
        self.target.map_or(0, |target| target.data.0)
    }

    /// End the copying. Several inputs becoming one wins back overlap whatever it dropped, and one input that dropped nothing
    /// has nothing to install
    pub fn finish_rows(&mut self) -> Result<()> {
        let Some(output) = self.output.clone() else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        if self.dropped_rows == 0 && self.input_count == 1 && !self.is_cutting && self.staged.is_empty() && self.rebuild.is_none() {
            self.note_gave_up();
            return Err(Error::from(ErrorKind::Interrupted));
        }
        self.flush(&output.file)?;
        if self.output_column.is_some() {
            self.writer.end_column(&self.output_last_key);
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
            return if self.is_cutting { self.cut_output(target, &output, distance) } else { self.finish_outputs(&output, distance) };
        }

        let part = left.min(self.buffer.len() as u64) as usize;
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
        self.stage_output(output.number, &totals);

        self.data_slot += 1;
        let next = self.create_output(volume, ByteCount(self.cut_bytes), self.data_slot)?;
        self.output = Some(next);
        let base = self.lowest_place_base(volume)?;
        self.writer.start(target.blocks_at, base);
        self.writer.set_marks(target.marks);
        self.records_end = FileOffset(0);
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

    /// Count what a merge moved, dropped and carried, for the report
    pub fn note_install(&self) {
        let state = &self.state;
        let staged = self.staged_columns.iter().map(|column| &column.tail);
        let carried: u64 = staged.chain(self.writer.columns()).chain(self.grave_writer.columns()).map(|tail| tail.grave_count.0).sum();
        let totals = [self.totals, self.grave_totals].into_iter().flatten().map(|totals| totals.file_length.0).sum::<u64>();
        let bytes: u64 = totals + self.staged.iter().map(|staged| staged.facts.file_length.0).sum::<u64>();
        state.graves_dropped.fetch_add(self.dropped_graves, Ordering::Relaxed);
        state.graves_carried.fetch_add(carried, Ordering::Relaxed);
        state.empty_rewrites.fetch_add(u64::from(self.dropped_rows == 0), Ordering::Relaxed);
        state.note_moved(JobKind::Compaction, ByteCount(bytes));
    }

    /// Keep what an output's summary row and list entries will need once the writer has moved on
    pub fn stage_output(&mut self, number: SegmentNumber, totals: &FooterTotals) {
        let first_column = self.staged_columns.len();
        for (index, tail) in self.writer.columns().iter().enumerate() {
            let facts = self.writer.column_facts(index);
            let lowest_at = self.staged_keys.len();
            self.staged_keys.extend_from_slice(facts.lowest);
            let bound_at = self.staged_keys.len();
            self.staged_keys.extend_from_slice(facts.bound);
            self.staged_columns.push(StagedColumn {
                tail: *tail,
                lowest_at,
                bound_at,
                end: self.staged_keys.len(),
                live_bytes: facts.live_bytes,
            });
        }
        let block_count = self.writer.entries().len() as u32;
        self.staged.push(StagedOutput {
            facts: output_facts(number, totals, self.records_end, block_count, self.is_unwalked),
            newest_place: totals.head.places.highest,
            first_column,
            column_count: self.staged_columns.len() - first_column,
        });
    }

    /// Make the outputs whole, the graves first. The last tail lists the inputs, and open takes that as the merge having
    /// happened, so everything that stays has to be durable by then
    pub fn finish_outputs(&mut self, output: &RewriteOutput<Backend::File>, distance: u64) -> Result<()> {
        let state = Arc::clone(&self.state);
        let numbers = self.numbers;
        let inputs = &numbers[..self.input_count];
        let has_data = !self.writer.columns().is_empty();
        if let Some(graves) = self.grave_output.clone() {
            self.grave_writer.end_column(&self.grave_last_key);
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
        let retired: [_; MOST_MERGE_SEGMENTS] = array::from_fn(|index| inputs.get(index).and_then(|number| volume.table.retire(*number)));
        self.note_install();
        volume.lanes[self.lane].rewrites_done.fetch_add(1, Ordering::AcqRel);
        self.summarize(volume, inputs);
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
        *hold(&volume.lanes[self.lane].merging) = [SegmentNumber(0); MOST_MERGE_SEGMENTS];
        *hold(&volume.lanes[self.lane].inputs) = [SegmentNumber(0); MOST_MERGE_SEGMENTS];
        volume.segments_rewritten.fetch_add(1, Ordering::AcqRel);
        volume.early_bar_rewrites.fetch_add(self.bars_fired.early, Ordering::Relaxed);
        volume.plain_bar_rewrites.fetch_add(self.bars_fired.plain, Ordering::Relaxed);
        volume.wants_compaction.store(true, Ordering::Release);
        for (segment, retired) in inputs.iter().zip(retired) {
            if let Some(retired) = retired {
                park(volume, Some(retired), *segment, saved.is_ok());
            }
        }
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

    /// Give the merge up: the inputs stay, and the outputs go whatever name they have reached
    pub fn abandon(&mut self) {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        let slots = mem::take(&mut *hold(&volume.lanes[self.lane].partial_outputs));
        // what the table holds waits for no read, since no list ever had it, and the removes below close the files
        for staged in &self.staged {
            if let Some(retired) = volume.table.retire(staged.facts.segment) {
                volume.table.free_retired(&retired);
            }
        }
        self.staged.clear();
        self.output = None;
        self.grave_output = None;
        self.leave_outputs(volume, slots);
        self.target = None;
        volume.lanes[self.lane].output_floor.store(NO_SEAL, Ordering::Release);
        volume.lanes[self.lane].is_rewriting.store(false, Ordering::Release);
        *hold(&volume.lanes[self.lane].merging) = [SegmentNumber(0); MOST_MERGE_SEGMENTS];
        *hold(&volume.lanes[self.lane].inputs) = [SegmentNumber(0); MOST_MERGE_SEGMENTS];
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

/// Check a segment's covers have all left the pool, with every cover still there in a sealed segment: a cover dropped inside a
/// newer one rests on that one, which a crash could lose until its segment seals
pub fn has_settled_covers<Backend: Io>(volume: &Volume<Backend>, row: &SummaryRow<'_>) -> bool {
    row.facts.has_covers && volume.covers.are_all_applied() && !volume.covers.holds_segment(row.facts.segment)
}

/// Check a segment can go into a merge at all. Damage and covers answer for more than their rows
pub fn is_mergeable<Backend: Io>(volume: &Volume<Backend>, row: &SummaryRow<'_>) -> bool {
    let is_clear = !row.facts.has_covers || has_settled_covers(volume, row);
    row.facts.is_walked() && !row.facts.is_damaged && is_clear && row.facts.block_count > 0 && !volume.is_taken(row.facts.segment)
}

/// Check a segment can go into an overlap merge: walked, or with no walk begun and a forward one owed. A half-walked segment's
/// rows are half counted
pub fn is_overlap_input(row: &SummaryRow<'_>) -> bool {
    let is_unwalked = row.facts.walked_blocks == 0 && !row.facts.is_backward;
    (row.facts.is_walked() || is_unwalked) && !row.facts.is_damaged && !row.facts.has_covers && row.facts.block_count > 0
}

/// Check a row's counts say a merge would win back its share of the rows or the bytes, past the bytes already freed in place
pub fn is_rewrite_ready<Backend: Io>(volume: &Volume<Backend>, row: &SummaryRow<'_>, freed_bytes: u64, current: DeathMark) -> bool {
    if !is_mergeable(volume, row) {
        return false;
    }
    // a settled cover's row only leaves at a rewrite
    if row.facts.has_covers {
        return true;
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
    let won_back = dead_bytes.saturating_sub(freed_bytes) + (dead_rows + live_graves) * ROW_FOOTER_BYTES;
    BAR_QUARTERS * won_back >= bar_quarters(row.facts.marks, current) * (bytes + rows * ROW_FOOTER_BYTES)
}

/// Read what a rewrite of a segment wins back, as a share of its record and footer bytes
pub fn reclaim_share(row: &SummaryRow<'_>) -> u64 {
    let (mut rows, mut dead_rows, mut bytes, mut dead_bytes) = (0, 0, 0, 0);
    let mut cursor = row.cursor();
    for _column in 0..row.column_count {
        let Some(column) = cursor.column() else {
            break;
        };
        rows += column.row_count.0;
        dead_rows += column.dead_rows.0 + column.grave_count.0.saturating_sub(column.dead_graves.0);
        bytes += column.live_bytes.0 + column.dead_bytes.0;
        dead_bytes += column.dead_bytes.0;
    }
    SHARE_SCALE * (dead_bytes + dead_rows * ROW_FOOTER_BYTES) / (bytes + rows * ROW_FOOTER_BYTES).max(1)
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
    BAR_QUARTERS * graves >= PLAIN_BAR_QUARTERS * rows
}

/// Take the smallest segments a test passes, whole segments included, since the output is cut to size. Small ones first, so a
/// merge rewrites as little as it takes to bring a crowded point down
pub fn gather_smallest<Backend: Io>(
    volume: &Volume<Backend>,
    rows: &SummaryRows,
    most: usize,
    is_more: impl Fn(&SummaryRow<'_>) -> bool,
    inputs: &mut [MergeInput],
    numbers: &mut [SegmentNumber; MOST_MERGE_SEGMENTS],
    input_count: &mut usize,
) -> MergeTarget {
    *input_count = 0;
    let (mut data, mut length, mut blocks) = (0, 0, 0);
    let mut marks = MarkRange::NONE;
    while *input_count < most {
        let taken = &numbers[..*input_count];
        let fits = |row: &SummaryRow<'_>| {
            let is_inside = data + row.facts.data_end.0 <= MOST_MERGE_OUTPUTS as u64 * volume.segment_bytes.0;
            is_inside && blocks + row.facts.block_count <= MOST_MERGE_OUTPUTS as u32 * MOST_SEGMENT_BLOCKS.0 && !taken.contains(&row.facts.segment)
        };
        let Some(row) = rows.rows().filter(|row| is_more(row) && fits(row)).min_by_key(|row| row.facts.data_end) else {
            break;
        };
        let facts = row.facts;
        inputs[*input_count].segment = facts.segment;
        inputs[*input_count].block_count = facts.block_count;
        numbers[*input_count] = facts.segment;
        *input_count += 1;
        data += facts.data_end.0;
        length += facts.file_length.0;
        blocks += facts.block_count;
        marks = marks.join(facts.marks);
    }
    MergeTarget {
        blocks_at: FileOffset(data.min(2 * volume.segment_bytes.0)),
        data: ByteCount(data),
        length: ByteCount(length),
        marks,
    }
}

/// Check a segment's range for a column holds a key. A summary keeps only a key's first bytes, so this can only be wider than the
/// truth
pub fn covers_point(row: &SummaryRow<'_>, column: ColumnId, point: &[u8]) -> bool {
    let mut cursor = row.cursor();
    for _column in 0..row.column_count {
        let Some(facts) = cursor.column() else {
            return false;
        };
        if facts.column == column {
            return facts.lowest <= point && (facts.bound.is_empty() || point < facts.bound);
        }
    }
    false
}

/// Check two segments share a column and their ranges meet. A summary keeps only a key's first bytes, so a range here can only
/// be wider than the truth
pub fn overlaps(left: &SummaryRow<'_>, right: &SummaryRow<'_>) -> bool {
    let (mut left_columns, mut right_columns) = (left.cursor(), right.cursor());
    let (mut left_left, mut right_left) = (left.column_count, right.column_count);
    let (mut ours, mut theirs) = (left_columns.column(), right_columns.column());
    // a row keeps its columns in order, so the two lists meet in one pass
    while let (Some(our), Some(their)) = (&ours, &theirs) {
        if our.column == their.column {
            let is_before = !their.bound.is_empty() && their.bound <= our.lowest;
            let is_after = !our.bound.is_empty() && our.bound <= their.lowest;
            if !is_before && !is_after {
                return true;
            }
        }
        if our.column <= their.column {
            left_left -= 1;
            ours = if left_left > 0 { left_columns.column() } else { None };
        } else {
            right_left -= 1;
            theirs = if right_left > 0 { right_columns.column() } else { None };
        }
    }
    false
}

/// Find a point more segments cover than the bound allows, in a segment no look has passed, and fill in the key. Overlap only
/// grows when a segment arrives, so each one takes one look, and with none new a look costs nothing. It rises at a segment's
/// lowest key, so the points worth trying are the new segment's own and those of a few that begin inside it
pub fn overlap_start<'rows>(
    rows: &'rows SummaryRows,
    looked_up_to: &mut SegmentNumber,
    can_merge: impl Fn(&SummaryRow<'_>) -> bool,
    has_bound: impl Fn(ColumnId) -> bool,
    point: &mut Vec<u8>,
) -> Option<(SummaryRow<'rows>, ColumnId)> {
    let is_crowded = |column: ColumnId, key: &[u8]| {
        rows.rows().filter(|row| can_merge(row) && covers_point(row, column, key)).take(MOST_OVERLAP + 1).count() > MOST_OVERLAP
    };
    for _look in 0..OVERLAP_LOOKS {
        let row = rows.rows().filter(|row| row.facts.segment > *looked_up_to).min_by_key(|row| row.facts.segment)?;
        let mut columns = row.cursor();
        for _column in 0..row.column_count {
            let facts = columns.column()?;
            if !has_bound(facts.column) {
                continue;
            }
            let inside = rows.rows().filter_map(|other| lowest_inside(&other, facts.column, facts.lowest, facts.bound));
            let found = iter::once(facts.lowest).chain(inside.take(OVERLAP_PROBES)).find(|key| is_crowded(facts.column, key));
            if let Some(key) = found.filter(|_key| can_merge(&row)) {
                point.clear();
                point.extend_from_slice(key);
                return Some((row, facts.column));
            }
        }
        *looked_up_to = row.facts.segment;
    }
    None
}

/// Read a segment's lowest key for a column when it falls inside a range
pub fn lowest_inside<'row>(row: &SummaryRow<'row>, column: ColumnId, lowest: &[u8], bound: &[u8]) -> Option<&'row [u8]> {
    let mut cursor = row.cursor();
    for _column in 0..row.column_count {
        let facts = cursor.column()?;
        if facts.column == column {
            let is_inside = facts.lowest > lowest && (bound.is_empty() || facts.lowest < bound);
            return is_inside.then_some(facts.lowest);
        }
    }
    None
}

/// A rewrite a worker runs. The planner starts one here and the worker steps it, and the lock only ever waits on one step
pub struct RewriteSeat<Backend: Io> {
    pub job: Mutex<RewriteJob<Backend>>,
}

/// Count the rows a summary row's columns hold
pub fn rows_of(row: &SummaryRow<'_>) -> u64 {
    let mut rows = 0;
    let mut cursor = row.cursor();
    for _column in 0..row.column_count {
        if let Some(column) = cursor.column() {
            rows += column.row_count.0;
        }
    }
    rows
}

/// Read a segment's bar in quarters: the early bar while its highest mark stands, and the plain one after that or with no marks
pub fn bar_quarters(marks: MarkRange, current: DeathMark) -> u64 {
    if marks.highest().is_some_and(|highest| current <= highest) {
        EARLY_BAR_QUARTERS
    } else {
        PLAIN_BAR_QUARTERS
    }
}
