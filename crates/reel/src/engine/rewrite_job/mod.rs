//! Compaction's merge: up to 8 sealed segments copied into a new one, without the rows no read can reach
//!
//! Records land at the front of the output and row blocks land past the inputs' data ends, since the records' end isn't
//! known until the last row. The blocks then move down to meet the records and the tail makes the file a segment. Graves that
//! stay go to an output of their own, and it is whole first: the last tail to land lists the inputs, and open removes them
//! on sight, however far the install got.

use std::iter;
use std::io::{Error, ErrorKind, Result};
use std::mem;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use reel_api::column_id::ColumnId;
use reel_api::maintenance_report::{
    PickOutcome, UnitKind,
};
use reel_api::units::{ByteCount, FileOffset, Nanoseconds, WritePlace};

use crate::config::MAX_KEY_BYTES;
use crate::engine::engine_state::EngineState;
use crate::engine::seal_job::MOST_SEGMENT_BLOCKS;
use crate::engine::volume::{MOST_MERGE_OUTPUTS, RewriteOutput, RowBytes, Volume};
use crate::guard::hold;
use crate::lifetime::MarkRange;
use crate::io::{Io, IoFile};
use crate::fixed_list::{FixedList, ListFull};
use crate::region::{LIST_ALIGN_ROOM, REGION_HEAD_BYTES, Region, list_bytes};
use crate::engine::held_bits::HeldBits;
use crate::engine::admission::{Admit, JobRole, PICK_HALVINGS};
use crate::engine::dead_bits::BitsExtent;
use crate::segment::key_cursor::KeyCursor;
use crate::segment::damage_list::DamagedRange;
use crate::segment::footer_writer::{FooterTotals, FooterWriter};
use crate::index::key_hash::KeyHash;
use crate::segment::row_block::{BLOCK_TRAILER_BYTES, FooterRow, ROW_BLOCK_BYTES, ROW_ROOM_NUMBERS, RowBlockWalk};
use crate::segment::segment_table::SegmentPin;
use crate::segment::segment_tail::{BlockCount, ColumnTail, MAX_COLUMNS, MOST_MERGE_SEGMENTS, SegmentNumber};
use crate::segment::summary::{DeadCount, SUMMARY_KEY_BYTES, SegmentFacts, SummaryRow, SummaryRows};

mod held;
mod install;
mod merge;
mod pick;
mod place;

pub use pick::is_reclaim_due;
#[cfg(any(test, feature = "test-hooks"))]
pub use install::freed_in_place;
#[cfg(any(test, feature = "test-hooks"))]
pub use pick::{CROWD_ANSWERS, CrowdAnswer, CrowdAnswers, CrowdStamp, column_range, covers_point, has_settled_covers, is_crowded, is_mergeable, is_overlap_input, overlap_bound, overlaps};

/// A bar counts in quarters of a segment's bytes
pub const BAR_QUARTERS: u64 = 4;
/// A segment is rewritten once this many quarters of its bytes are dead, the plain bar: a half
pub const PLAIN_BAR_QUARTERS: u64 = 2;
/// A marked segment waits for this many while its highest mark stands, since it dies whole soon. A rewrite there copies at
/// most a third of a byte for each byte freed, so a record freed early still comes back
pub const EARLY_BAR_QUARTERS: u64 = 3;
/// A quiet volume with dead bytes rewrites any segment that wins a byte back, highest share first
pub const QUIET_BAR_QUARTERS: u64 = 0;
/// Bytes of one job's merge buffer, which carries records and then row blocks. With a row block for each input, a job's
/// merge buffers come to about 2 MiB
pub const MERGE_BUFFER_BYTES: usize = 3 * 512 * 1024;
/// Most row blocks of an output that can hold rows kept for a cue, past which the merge waits for the cue to go
pub const MOST_KEPT_BLOCKS: usize = 1_024;
/// A row the merge dropped, which a held bit lands nowhere for
pub const NO_HOME: u32 = u32::MAX;
/// Bits of a row's home that give its block in the output, past which the bits give the output
pub const HOME_BLOCK_BITS: u32 = 28;
/// Rows one step of a merge decides
pub const MERGE_STEP_ROWS: usize = 4_096;
/// The longest a step of a merge decides rows for. A row can ask every overlapping segment's footers, so rows alone can run a
/// step past a minute, and a close waits for the step
pub const MERGE_STEP_TIME: Nanoseconds = Nanoseconds(20_000_000);
/// Bytes a stripe copy step takes across before it stops, a buffer's worth, past which only the run in hand finishes
pub const COPY_STEP_BYTES: u64 = MERGE_BUFFER_BYTES as u64;
/// Bytes each output's stage holds before it goes out in one write
pub const STAGE_BYTES: usize = 192 * 1024;
/// The most bytes between two records the offset-order copy reads through to take both in one read, past which the second
/// starts a read of its own
pub const SPAN_GAP_BYTES: u64 = 4 * 1024;
/// Bytes one staged column's kept lowest key and bound take, each up to a summary key and a byte past it
pub const STAGED_COLUMN_KEY_BYTES: usize = 2 * SUMMARY_KEY_BYTES + 2;
/// Keys a running rewrite holds a key's length of: the row's, the last, two pairs of edges and each output's last
pub const RUN_KEYS: usize = 8;
/// Most row blocks the output for kept graves can have, past which the merge gives way
pub const MOST_GRAVE_BLOCKS: BlockCount = BlockCount(2_048);
/// How long an install waits for room to park its inputs before the report counts it as long
pub const MOST_PARK_WAIT: Nanoseconds = Nanoseconds(10_000_000_000);
/// Most segments of a column that overlap one key before a merge is due, for records up to the base size
pub const MOST_OVERLAP: usize = 32;
/// Records this size or smaller keep the bound at MOST_OVERLAP, and larger ones raise it in step, so large values seldom merge
pub const OVERLAP_BASE_RECORD_BYTES: u64 = 4 * 1024;
/// Room a merge takes at a time once it has written past its promise
pub const OUTGROW_STEP_BYTES: u64 = 16 * 1024 * 1024;
/// The longest an overlap look holds the summary before it stops for the next pick
pub const MOST_LOOK_TIME: Nanoseconds = Nanoseconds(2_000_000);
/// The planner's own lane. Worker lanes count up from one
pub const PLANNER_LANE: usize = 0;
/// Keys inside a new segment one look tries for the crowded point, which bounds what a look costs
pub const OVERLAP_PROBES: usize = 16;
/// Most segments one look for an overlap starts from, which bounds what a look costs
pub const OVERLAP_LOOKS: usize = 16;
/// Most rows of one share bucket a pick looks at before it moves down to the next
pub const BUCKET_LOOKS: usize = 64;
/// Most rows a merge looks along the segment order for each input past its first
pub const GATHER_LOOKS: usize = 64;
/// Most segments one directory listing of an overlap look holds
pub const OVERLAP_LISTED: usize = 4 * (MOST_OVERLAP + 1);
/// Most listings a count of the segments over one key pages through
pub const CROWD_PAGES: usize = 4;
/// Numbers an output skips looking for one outside a stuck seal's shard, which one in 16 lands in
pub const MOST_NUMBER_TRIES: usize = 64;

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
    pub offset: u32,
    pub length: u32,
    pub ordinal: u32,
}

impl StripeRow {
    /// Read where the record starts in its input
    pub fn start(&self) -> u64 {
        u64::from(self.offset)
    }

    /// Read where the record ends in its input
    pub fn end(&self) -> u64 {
        u64::from(self.offset) + u64::from(self.length)
    }
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
    /// The rows the walk held in the inputs land on the outputs, a held block at a time
    Draining,
    /// The outputs are whole segments and wait to replace their inputs
    Installing,
}

/// One segment going into a merge, and where its rows have reached
pub struct MergeInput {
    pub segment: SegmentNumber,
    pub block_count: u32,
    pub block_rows: u64,
    pub most_block_rows: u32,
    pub data_end: u64,
    pub base: u64,
    rows_seen: u64,
    pub read_bytes: u64,
    next_block: u32,
    block: FixedList<u8>,
    length: usize,
    position: usize,
    key: FixedList<u8>,
    column: ColumnId,
    row: Option<FooterRow>,
    block_firsts: FixedList<u32>,
}

impl MergeInput {
    /// Make an input with no buffers, which its job carves from its region at the start
    pub fn new() -> Self {
        Self {
            segment: SegmentNumber(0),
            block_count: 0,
            block_rows: 0,
            most_block_rows: 0,
            data_end: 0,
            base: 0,
            rows_seen: 0,
            read_bytes: 0,
            next_block: 0,
            block: FixedList::empty(),
            length: 0,
            position: 0,
            key: FixedList::empty(),
            column: ColumnId(0),
            row: None,
            block_firsts: FixedList::empty(),
        }
    }

    /// Read the heap the buffers hold
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn heap_bytes(&self) -> usize {
        self.block.capacity() + self.key.capacity()
    }

    /// Carve the input's block and key from its job's region
    pub fn carve(&mut self, region: &mut Region) -> std::result::Result<(), ListFull> {
        self.block = region.carve_filled(ROW_BLOCK_BYTES, 0)?;
        self.key = region.carve(MAX_KEY_BYTES as usize)?;
        Ok(())
    }

    /// Count the region bytes an input takes
    pub fn region_bytes() -> usize {
        list_bytes::<u8>(ROW_BLOCK_BYTES) + list_bytes::<u8>(MAX_KEY_BYTES as usize)
    }

    /// Give the block and key back, and return the most bytes they wrote
    pub fn release(&mut self) -> usize {
        let peak = self.block.peak_bytes() + self.key.peak_bytes() + self.block_firsts.peak_bytes();
        self.block = FixedList::empty();
        self.key = FixedList::empty();
        self.block_firsts = FixedList::empty();
        peak
    }

    /// Read the ordinal of a block's first row, which the deciding pass noted as it read the block
    pub fn block_first(&self, block: u32) -> Option<u64> {
        self.block_firsts.get(block as usize).map(|first| u64::from(*first))
    }

    /// Check a row list this long has room for every row of the input
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn row_room(&self, rows: usize) -> bool {
        self.block_rows <= rows as u64
    }

    /// Read the row in hand's place among the input's rows, or the count of them once they've run out
    pub fn ordinal(&self) -> u64 {
        if self.row.is_some() { self.rows_seen - 1 } else { self.rows_seen }
    }

    /// Read the block of the row in hand and its place among the block's rows
    pub fn row_place(&self) -> Option<(u32, u32)> {
        self.row?;
        let block = self.next_block.checked_sub(1)?;
        let first = u64::from(*self.block_firsts.get(block as usize)?);
        Some((block, u32::try_from(self.ordinal().checked_sub(first)?).ok()?))
    }

    /// Go back to the first row
    pub fn rewind(&mut self) {
        self.rows_seen = 0;
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
                    self.rows_seen += 1;
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
            self.read_bytes += length as u64;
            if RowBlockWalk::open(&self.block[..length], base).is_none() {
                return Err(Error::from(ErrorKind::InvalidData));
            }
            // the drain finds a held row's ordinal from its block's first one and its place in the block
            if let Some(first) = self.block_firsts.get_mut(self.next_block as usize) {
                *first = self.rows_seen as u32;
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
    promised: ByteCount,
    parks_held: usize,
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
    buffer: FixedList<u8>,
    used: usize,
    records_end: FileOffset,
    moved: u64,
    key: FixedList<u8>,
    last_key: FixedList<u8>,
    lowest: FixedList<u8>,
    highest: FixedList<u8>,
    edge_lowest: FixedList<u8>,
    edge_highest: FixedList<u8>,
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
    kept_dead: FixedList<DeadCount>,
    park_wait_since: Option<Nanoseconds>, is_long_wait_noted: bool,
    pub cut_floor: u64,
    cut_bytes: u64,
    data_slot: usize,
    is_cutting: bool,
    is_top_row: bool,
    output_rows: u64,
    output_last_key: FixedList<u8>,
    grave_last_key: FixedList<u8>,
    staged: FixedList<StagedOutput>,
    staged_columns: FixedList<StagedColumn>,
    staged_keys: FixedList<u8>,
    pub forced: Option<SegmentNumber>,
    /// A need admission let in for a pick, with its bound, which the job maps and carves at its first step
    admitted: Option<(Admit, RewriteBound)>,
    /// A claimed pick waiting for its first step: its target, and whether it decides its graves first
    claimed: Option<(MergeTarget, bool)>,
    /// An input with a row block the device lost or that fails its check, which the give-up damages so no pick takes it again
    unreadable: Option<SegmentNumber>,
    /// The need the region in hand was admitted with, which goes back at the job's end
    region_need: ByteCount,
    /// The forced segment a pick took, which goes back when admission turns the pick away
    forced_pick: Option<SegmentNumber>,
    /// The mapping this run's lists are carved from, held from an admitted start to the install or the abandon
    region: Option<Region>,
    /// Holds the segments outside the merge that a step asks for newer rows
    pub cursor: KeyCursor<Backend::File>,
    /// Tests set this low to fill the grave output
    pub most_grave_blocks: BlockCount,
    pub last_slot: Option<(KeyHash, SegmentNumber, u32)>,
    /// Slots given in one column since the last count went to its table, each leaving an input's slot behind
    stale_given: (Option<ColumnId>, u64),
    /// One bit a row of the inputs, each input's rows from its base, set when the deciding pass keeps its record
    marks: FixedList<u64>,
    /// Where each marked row's record landed in its output, by the row's base and place in its input
    placed: FixedList<u32>,
    /// The input being placed's kept records, sorted by offset before they go across
    stripe: FixedList<StripeRow>,
    /// Each output's records on their way out, a buffer an output
    stages: [FixedList<u8>; MOST_MERGE_OUTPUTS],
    stripe_copied: Option<usize>,
    unit: (UnitKind, u64),
    /// The input the row in hand came from
    row_input: usize,
    /// The row in hand comes from an input whose walk had finished when the step pinned it
    row_walked: bool,
    /// Each input's exact bits, read once a merge, and the volume's spoils when they were read
    bit_extents: [Option<Option<BitsExtent>>; MOST_MERGE_SEGMENTS],
    bits_seen: u64,
    /// Rows a bit decided this merge, so one in so many is asked as well
    bit_rows: u64,
    /// The row in hand's place among every input's rows, its input's base and its place in that input
    row_at: u64,
    is_ordered: bool,
    /// The deciding pass's cuts, each input's place there, a row at or past it going to a later output
    cut_ordinals: [[u64; MOST_MERGE_SEGMENTS]; MOST_MERGE_OUTPUTS],
    /// Cuts the deciding pass made
    cuts: usize,
    /// Kept bytes and rows the deciding pass has given the output it's on
    decide_bytes: u64,
    decide_rows: u64,
    /// The most kept rows one output takes, so its blocks fit the writer
    cut_rows: u64,
    /// The outputs past the first an offset-order copy places records in before the copying pass reaches them
    slot_outputs: [Option<RewriteOutput<Backend::File>>; MOST_MERGE_OUTPUTS],
    /// Where each output's records end
    outputs_end: [u64; MOST_MERGE_OUTPUTS],
    /// The input the placing pass walks
    placing_input: usize,
    /// The pick's bound took the offset-order lists
    is_ordered_bound: bool,
    /// Tests of a merge's cut set this, since an offset-order copy runs to one output
    pub is_key_order_held: bool,
    /// A damaged segment the next pick copies whole, and the spans its lost blocks leave
    pub rebuild: Option<SegmentNumber>,
    pub rebuild_blocks: u32,
    pub bars_fired: BarsFired,
    pub lost_spans: Vec<DamagedRange>,
    is_unwalked: bool,
    crowded_key: FixedList<u8>,
    listed: FixedList<SegmentNumber>,
    crowd: FixedList<SegmentNumber>,
    pub crowd_answers: CrowdAnswers,
    pub has_looks_left: bool,
    pub picked_as: Option<PickOutcome>,
    /// The overlap look's current pass began at the oldest segment, with no merge started since
    is_whole_pass: bool,
    /// When the job began, for the merge time hot ingest is judged by
    begun_at: Nanoseconds,
    held_words: FixedList<u64>,
    row_home: FixedList<u32>,
    drained: FixedList<DeadCount>,
    held: HeldBits,
    drain_input: usize,
    drain_block: u32,
    home_outputs: [SegmentNumber; MOST_MERGE_OUTPUTS + 1],
    home_count: usize,
    is_holding: bool,
    looked_up_to: SegmentNumber,
}

impl<Backend: Io> RewriteJob<Backend> {
    /// Allocate the job's fixed buffers
    pub fn new(state: Arc<EngineState<Backend>>, volume: usize, lane: usize) -> Self {
        // a mutex's first lock can allocate, and this one's first real use could be in a close
        drop(hold(&state.volumes[volume].lanes[lane].partial_outputs));
        drop(hold(&state.volumes[volume].lanes[lane].merging));
        drop(hold(&state.volumes[volume].claim_lock));
        let cut_floor = state.volumes[volume].segment_bytes.0 / MOST_MERGE_OUTPUTS as u64;
        Self {
            state,
            volume,
            lane,
            target: None,
            phase: RewritePhase::Copying,
            output: None,
            grave_output: None,
            writer: FooterWriter::empty(),
            grave_writer: FooterWriter::empty(),
            totals: None,
            grave_totals: None,
            inputs: (0..MOST_MERGE_SEGMENTS).map(|_input| MergeInput::new()).collect(),
            numbers: [SegmentNumber(0); MOST_MERGE_SEGMENTS],
            input_count: 0,
            is_drained: false,
            buffer: FixedList::empty(),
            used: 0,
            records_end: FileOffset(0),
            moved: 0,
            key: FixedList::empty(),
            last_key: FixedList::empty(),
            lowest: FixedList::empty(),
            highest: FixedList::empty(),
            edge_lowest: FixedList::empty(),
            edge_highest: FixedList::empty(),
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
            kept_dead: FixedList::empty(),
            park_wait_since: None, is_long_wait_noted: false,
            cut_floor,
            cut_bytes: 0,
            data_slot: 0,
            is_cutting: false,
            is_top_row: false,
            output_rows: 0,
            output_last_key: FixedList::empty(),
            grave_last_key: FixedList::empty(),
            staged: FixedList::empty(),
            staged_columns: FixedList::empty(),
            staged_keys: FixedList::empty(),
            forced: None,
            admitted: None,
            claimed: None,
            unreadable: None,
            region_need: ByteCount(0),
            forced_pick: None,
            region: None,
            cursor: KeyCursor::new(),
            most_grave_blocks: MOST_GRAVE_BLOCKS,
            last_slot: None,
            stale_given: (None, 0),
            marks: FixedList::empty(),
            placed: FixedList::empty(),
            stripe: FixedList::empty(),
            stages: Default::default(),
            stripe_copied: None,
            unit: (UnitKind::MergeRows, 0),
            row_input: 0,
            row_walked: false,
            bit_extents: [None; MOST_MERGE_SEGMENTS],
            bits_seen: 0,
            bit_rows: 0,
            row_at: 0,
            is_ordered: false,
            cut_ordinals: [[0; MOST_MERGE_SEGMENTS]; MOST_MERGE_OUTPUTS],
            cuts: 0,
            decide_bytes: 0,
            decide_rows: 0,
            cut_rows: u64::MAX,
            slot_outputs: Default::default(),
            outputs_end: [0; MOST_MERGE_OUTPUTS],
            placing_input: 0,
            is_ordered_bound: false,
            is_key_order_held: false,
            rebuild: None,
            rebuild_blocks: 0,
            bars_fired: BarsFired::default(),
            lost_spans: Vec::new(),
            is_unwalked: false,
            crowded_key: FixedList::on_heap(SUMMARY_KEY_BYTES),
            listed: FixedList::on_heap(OVERLAP_LISTED),
            crowd: FixedList::on_heap(OVERLAP_LISTED),
            crowd_answers: CrowdAnswers::new(),
            has_looks_left: false,
            picked_as: None,
            promised: ByteCount(0),
            parks_held: 0,
            is_whole_pass: true,
            begun_at: Nanoseconds(0),
            held_words: FixedList::empty(),
            row_home: FixedList::empty(),
            drained: FixedList::empty(),
            held: HeldBits::default(),
            drain_input: 0,
            drain_block: 0,
            home_outputs: [SegmentNumber(0); MOST_MERGE_OUTPUTS + 1],
            home_count: 0,
            is_holding: false,
            looked_up_to: SegmentNumber(0),
        }
    }

    /// Read the heap the job's buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        let inputs = self.inputs.capacity() * size_of::<MergeInput>();
        let lists = self.listed.held_bytes() + self.crowd.held_bytes() + self.crowd_answers.answers.capacity() * size_of::<CrowdAnswer>();
        ByteCount((self.crowded_key.held_bytes() + lists + inputs) as u64 + self.cursor.heap_bytes().0)
    }

    /// Read the bytes the run's region maps, nothing between runs
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn region_bytes(&self) -> ByteCount {
        self.region.as_ref().map_or(ByteCount(0), Region::mapped_bytes)
    }

    /// Work out what the picked inputs hold, from their summary rows
    pub fn bound(&self, volume: &Volume<Backend>) -> RewriteBound {
        let rows = hold(&volume.summary);
        let mut bound = RewriteBound { inputs: self.input_count, block_bytes: ROW_BLOCK_BYTES, most_grave_blocks: self.most_grave_blocks, ..RewriteBound::default() };
        for number in self.input_numbers() {
            let Some(row) = rows.get(*number) else {
                continue;
            };
            bound.rows += rows_of(&row);
            bound.most_input_rows = bound.most_input_rows.max(rows_of(&row));
            bound.columns += usize::from(row.column_count);
            let mut cursor = row.cursor();
            for _column in 0..row.column_count {
                if let Some(column) = cursor.column() {
                    bound.block_bytes = bound.block_bytes.min(self.state.block_bytes_of(column.column));
                }
            }
        }
        bound.columns = bound.columns.min(MAX_COLUMNS);
        bound.is_ordered = self.rebuild.is_none() && !self.is_key_order_held;
        // the walk charges only walked segments, so an unwalked merge holds nothing
        bound.is_holding = !self.is_unwalked && self.rebuild.is_none();
        if bound.is_holding {
            for number in self.input_numbers() {
                if let Some(facts) = rows.get(*number).map(|row| row.facts) {
                    bound.blocks += u64::from(facts.block_count);
                    bound.held_words += HeldBits::words_for(iter::once((facts.block_count, facts.most_block_rows)));
                }
            }
        }
        bound
    }

    /// Carve every list of the run from its region
    pub fn attach(&mut self, mut region: Region, bound: &RewriteBound) -> std::result::Result<(), ListFull> {
        let carved = self.carve(&mut region, bound);
        self.is_ordered_bound = bound.is_ordered;
        self.cut_rows = (MOST_SEGMENT_BLOCKS.0 as u64 - 2) * fewest_block_rows(bound.block_bytes);
        let mut base = 0;
        for input in &mut self.inputs[..bound.inputs] {
            input.base = base;
            base += input.block_rows;
        }
        self.region = Some(region);
        carved
    }

    /// Carve the lists in the order the bound counts them
    fn carve(&mut self, region: &mut Region, bound: &RewriteBound) -> std::result::Result<(), ListFull> {
        let keys = MAX_KEY_BYTES as usize;
        self.writer = FooterWriter::in_region(region, bound.output_blocks())?;
        self.grave_writer = FooterWriter::in_region(region, bound.grave_blocks())?;
        for input in &mut self.inputs[..bound.inputs] {
            input.carve(region)?;
        }
        self.buffer = region.carve_filled(MERGE_BUFFER_BYTES, 0)?;
        self.kept_dead = region.carve(MOST_KEPT_BLOCKS)?;
        for key in [&mut self.key, &mut self.last_key, &mut self.lowest, &mut self.highest] {
            *key = region.carve(keys)?;
        }
        for key in [&mut self.edge_lowest, &mut self.edge_highest, &mut self.output_last_key, &mut self.grave_last_key] {
            *key = region.carve(keys)?;
        }
        self.staged = region.carve(MOST_MERGE_OUTPUTS)?;
        self.staged_columns = region.carve(MOST_MERGE_OUTPUTS * bound.columns)?;
        self.staged_keys = region.carve(MOST_MERGE_OUTPUTS * bound.columns * STAGED_COLUMN_KEY_BYTES)?;
        let rows = bound.ordered_rows();
        self.marks = region.carve_filled(rows.div_ceil(u64::BITS as usize), 0)?;
        self.placed = region.carve_filled(rows, 0)?;
        self.stripe = region.carve(bound.ordered_input_rows())?;
        if bound.is_holding {
            for input in &mut self.inputs[..bound.inputs] {
                input.block_firsts = region.carve_filled(input.block_count as usize, 0)?;
            }
            self.held_words = region.carve_filled(bound.held_words, 0)?;
            self.row_home = region.carve_filled(bound.rows as usize, NO_HOME)?;
            self.drained = region.carve(bound.drain_room())?;
        }
        for stage in &mut self.stages[..bound.stages()] {
            *stage = region.carve(STAGE_BYTES)?;
        }
        Ok(())
    }

    /// Give the run's lists and region back, and count the bytes they wrote
    pub fn release_region(&mut self) {
        let Some(region) = self.region.take() else {
            return;
        };
        let (_held, mut touched) = self.writer.list_bytes();
        touched += self.grave_writer.list_bytes().1;
        self.writer = FooterWriter::empty();
        self.grave_writer = FooterWriter::empty();
        for input in &mut self.inputs {
            touched += input.release();
        }
        for key in [&mut self.buffer, &mut self.key, &mut self.last_key, &mut self.lowest, &mut self.highest, &mut self.staged_keys] {
            touched += mem::take(key).peak_bytes();
        }
        for key in [&mut self.edge_lowest, &mut self.edge_highest, &mut self.output_last_key, &mut self.grave_last_key] {
            touched += mem::take(key).peak_bytes();
        }
        touched += mem::take(&mut self.kept_dead).peak_bytes() + mem::take(&mut self.staged).peak_bytes();
        touched += mem::take(&mut self.staged_columns).peak_bytes() + mem::take(&mut self.marks).peak_bytes();
        touched += mem::take(&mut self.placed).peak_bytes() + mem::take(&mut self.stripe).peak_bytes();
        touched += mem::take(&mut self.held_words).peak_bytes() + mem::take(&mut self.row_home).peak_bytes();
        touched += mem::take(&mut self.drained).peak_bytes();
        for stage in &mut self.stages {
            touched += mem::take(stage).peak_bytes();
        }
        drop(region);
        self.state.give_back(JobRole::Rewrite, self.region_need, ByteCount(touched as u64));
    }

    /// Pick, and halve the inputs while the need is past what admission gives, up to a few times before the lane waits
    pub fn pick_admitted(&mut self, volume: &Volume<Backend>) -> Option<(MergeTarget, bool)> {
        let mut most = MOST_MERGE_SEGMENTS;
        for _halving in 0..=PICK_HALVINGS {
            let picked = self.pick_within(volume, most)?;
            let Some(output) = self.disk_room_for(volume, picked.0.length) else {
                self.unpick(volume);
                volume.is_room_refused.store(true, Ordering::Release);
                volume.note_pick(PickOutcome::Refused);
                // fewer inputs write a smaller output, so a short disk tries a smaller merge before it gives up
                if self.input_count < 2 {
                    self.has_looks_left = false;
                    return None;
                }
                most = self.input_count / 2;
                continue;
            };
            let mut bound = self.bound(volume);
            // offset order first, and the same pick in key order when its lists don't fit, which the floor alone admits
            let mut admit = None;
            if bound.is_ordered {
                admit = self.state.decide_admit(JobRole::Rewrite, bound.region_bytes(), false);
            }
            if admit.is_none() {
                bound.is_ordered = false;
                admit = self.state.decide_admit(JobRole::Rewrite, bound.region_bytes(), true);
            }
            if let Some(admit) = admit {
                // the job maps and carves on its own thread, at its first step
                self.admitted = Some((admit, bound));
                self.promised = output;
                volume.outputs_promised.fetch_add(output.0, Ordering::AcqRel);
                self.parks_held = self.input_count;
                volume.parks_promised.fetch_add(self.input_count, Ordering::AcqRel);
                let in_flight = volume.lanes.iter().filter(|lane| lane.is_rewriting.load(Ordering::Acquire)).count() as u64 + 1;
                volume.most_in_flight.fetch_max(in_flight, Ordering::Relaxed);
                volume.is_room_refused.store(false, Ordering::Release);
                self.forced_pick = None;
                return Some(picked);
            }
            self.unpick(volume);
            if self.input_count < 2 {
                return None;
            }
            most = self.input_count / 2;
        }
        None
    }

    /// Put back what a pick took when admission turned it away
    pub fn unpick(&mut self, volume: &Volume<Backend>) {
        if self.is_unwalked {
            *hold(&volume.lanes[self.lane].merging) = [SegmentNumber(0); MOST_MERGE_SEGMENTS];
        }
        if let Some(forced) = self.forced_pick.take() {
            self.forced = Some(forced);
        }
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
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn staged_count(&self) -> usize {
        self.staged.len()
    }

    /// Read the newest segment a look for an overlap has passed
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn looked_up_to(&self) -> SegmentNumber {
        self.looked_up_to
    }

    /// Read the segments going in
    pub fn input_numbers(&self) -> &[SegmentNumber] {
        &self.numbers[..self.input_count]
    }

}

/// A rewrite a worker runs. The planner starts one in the seat, and the worker takes it out to step it and puts it back once
/// it ends, so the lock is held for a swap and never across a step. While the worker holds the job, the seat keeps its heap
pub struct RewriteSeat<Backend: Io> {
    pub job: Mutex<Option<RewriteJob<Backend>>>,
    pub heap: AtomicU64,
    pub is_waiting: AtomicBool,
}

impl<Backend: Io> RewriteSeat<Backend> {
    /// Seat an idle rewrite for a lane
    pub fn new(state: Arc<EngineState<Backend>>, volume: usize, lane: usize) -> Self {
        Self { job: Mutex::new(Some(RewriteJob::new(state, volume, lane))), heap: AtomicU64::new(0), is_waiting: AtomicBool::new(false) }
    }

    /// Read the heap of the seat's job, wherever it is
    pub fn heap_bytes(&self) -> u64 {
        hold(&self.job).as_ref().map_or(self.heap.load(Ordering::Acquire), |job| job.heap_bytes().0)
    }

    /// Check the seat's rewrite is running, which it is while a worker holds it
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn is_busy(&self) -> bool {
        hold(&self.job).as_ref().is_none_or(RewriteJob::is_running)
    }
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


/// Add up the file bytes and the dead bytes of some segments' rows
pub fn rows_bytes(rows: &SummaryRows, segments: impl Iterator<Item = SegmentNumber>) -> RowBytes {
    let mut bytes = RowBytes::default();
    for row in segments.filter_map(|segment| rows.get(segment)) {
        bytes.file += row.facts.file_length.0;
        let mut cursor = row.cursor();
        for _column in 0..row.column_count {
            let Some(facts) = cursor.column() else {
                break;
            };
            bytes.dead += facts.dead_bytes.0;
        }
    }
    bytes
}

/// What a pick's inputs hold, which bounds every list its run carves
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RewriteBound {
    /// Inputs picked
    pub inputs: usize,
    /// Rows the inputs hold in all, every row an output can take
    pub rows: u64,
    /// Rows the largest input holds, every row the placing pass lists at once
    pub most_input_rows: u64,
    /// Columns the inputs hold in all, the most an output can hold
    pub columns: usize,
    /// The smallest block any of those columns writes
    pub block_bytes: usize,
    /// One input copies in offset order, with marks, places and a stripe for its rows
    pub is_ordered: bool,
    /// The most blocks the grave output takes
    pub most_grave_blocks: BlockCount,
    /// Whether the merge holds its inputs, so the walk's charges on them go to bits that its install lands on the outputs
    pub is_holding: bool,
    /// Blocks the inputs hold in all, each with the ordinal of its first row
    pub blocks: u64,
    /// Words the held bits take
    pub held_words: usize,
}

impl RewriteBound {
    /// Count the most row blocks one output can write
    pub fn output_blocks(&self) -> BlockCount {
        BlockCount(blocks_for_rows(self.rows, self.block_bytes, self.columns).min(MOST_SEGMENT_BLOCKS.0))
    }

    /// Count the most row blocks the grave output can write
    pub fn grave_blocks(&self) -> BlockCount {
        BlockCount(blocks_for_rows(self.rows, self.block_bytes, self.columns).min(self.most_grave_blocks.0))
    }

    /// Count the rows the placing pass lists at once, none for a key-order copy
    pub fn ordered_input_rows(&self) -> usize {
        if self.is_ordered { self.most_input_rows as usize } else { 0 }
    }

    /// Count the stages an offset-order copy carves, one an output it can cut into
    pub fn stages(&self) -> usize {
        if self.is_ordered { MOST_MERGE_OUTPUTS } else { 0 }
    }

    /// Count the rows the offset-order copy marks and places, none for a key-order copy
    pub fn ordered_rows(&self) -> usize {
        if self.is_ordered { self.rows as usize } else { 0 }
    }

    /// Count the dead counts a drain can land, an entry a block of every output
    pub fn drain_room(&self) -> usize {
        if !self.is_holding {
            return 0;
        }
        let blocks = blocks_for_rows(self.rows, self.block_bytes, self.columns) as usize;
        blocks + MOST_MERGE_OUTPUTS + self.grave_blocks().0 as usize
    }

    /// Count the region bytes the held bits, the blocks' first ordinals, the rows' homes and the drain's counts take
    pub fn holding_bytes(&self) -> usize {
        if !self.is_holding {
            return 0;
        }
        let firsts = self.blocks as usize * size_of::<u32>() + self.inputs * LIST_ALIGN_ROOM;
        list_bytes::<u64>(self.held_words) + firsts + list_bytes::<u32>(self.rows as usize) + list_bytes::<DeadCount>(self.drain_room())
    }

    /// Count the region bytes every list of the run takes, each with its alignment room
    pub fn region_bytes(&self) -> ByteCount {
        let writers = FooterWriter::region_bytes(self.output_blocks()) + FooterWriter::region_bytes(self.grave_blocks());
        let inputs = self.inputs * MergeInput::region_bytes();
        let keys = RUN_KEYS * list_bytes::<u8>(MAX_KEY_BYTES as usize);
        let buffers = list_bytes::<u8>(MERGE_BUFFER_BYTES) + list_bytes::<DeadCount>(MOST_KEPT_BLOCKS);
        let staged = list_bytes::<StagedOutput>(MOST_MERGE_OUTPUTS)
            + list_bytes::<StagedColumn>(MOST_MERGE_OUTPUTS * self.columns)
            + list_bytes::<u8>(MOST_MERGE_OUTPUTS * self.columns * STAGED_COLUMN_KEY_BYTES);
        let rows = self.ordered_rows();
        let ordered = list_bytes::<u64>(rows.div_ceil(u64::BITS as usize))
            + list_bytes::<u32>(rows)
            + list_bytes::<StripeRow>(self.ordered_input_rows())
            + self.stages() * list_bytes::<u8>(STAGE_BYTES);
        ByteCount((REGION_HEAD_BYTES + writers + inputs + keys + buffers + staged + ordered + self.holding_bytes()) as u64)
    }
}

/// Count the most row blocks these rows can fill. A block closes only when the next row can't fit, so each one a writer
/// closes before its column or output ends holds at least the fewest rows of the widest key, and each column and output
/// leaves one block short
pub fn blocks_for_rows(rows: u64, block_bytes: usize, columns: usize) -> u32 {
    let blocks = rows.div_ceil(fewest_block_rows(block_bytes)) + columns as u64 + 1;
    u32::try_from(blocks).unwrap_or(u32::MAX)
}

/// Count the fewest rows a block a writer closes before its column or output ends can hold, at the widest key
pub fn fewest_block_rows(block_bytes: usize) -> u64 {
    let widest_row = ROW_ROOM_NUMBERS + MAX_KEY_BYTES as usize + size_of::<u16>();
    (block_bytes.saturating_sub(BLOCK_TRAILER_BYTES) / widest_row).saturating_sub(1).max(1) as u64
}

