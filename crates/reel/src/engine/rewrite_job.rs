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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use reel_api::column_id::ColumnId;
use reel_api::maintenance_report::{JobKind, PickKind, PickOutcome, SPAN_RUN_BUCKETS, UnitKind};
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, DeathMark, FileOffset, RootId, RowCount, WritePlace};

use crate::config::MAX_KEY_BYTES;
use crate::engine::compact_job::park;
use crate::engine::engine_state::EngineState;
use crate::engine::read_path::SealedRow;
use crate::engine::recovery::save_cleared_spans;
use crate::engine::open_segment::{data_path, output_path};
use crate::engine::seal_job::MOST_SEGMENT_BLOCKS;
use crate::engine::volume::{GRAVE_SLOT, GaveUp, MOST_MERGE_OUTPUTS, NO_SEAL, OutputSlots, RewriteOutput, RowBytes, Volume};
use crate::engine::walk_job::add_count;
use crate::guard::hold;
use crate::lifetime::MarkRange;
use crate::io::{Io, IoFile};
use crate::fixed_list::{FixedList, ListFull};
use crate::region::{REGION_HEAD_BYTES, Region, list_bytes};
use crate::engine::admission::{Admit, JobRole, PICK_HALVINGS};
use crate::row_kind::RowKind;
use crate::segment::damage_list::DamagedRange;
use crate::segment::block_entry::KeyBound;
use crate::segment::column_directory::{ColumnDirectory, Listing};
use crate::segment::footer_writer::{ColumnStart, FooterTotals, FooterWriter};
use crate::index::complete_table::CompleteTable;
use crate::index::key_hash::{KeyHash, hash_key};
use crate::segment::row_block::{BLOCK_TRAILER_BYTES, FooterRow, ROW_BLOCK_BYTES, ROW_ROOM_NUMBERS, RowBlockWalk};
use crate::segment::segment_table::SegmentPin;
use crate::segment::segment_tail::{BlockCount, ColumnTail, MAX_COLUMNS, MOST_MERGE_SEGMENTS, SegmentNumber};
use crate::segment::summary::{ColumnFacts, DeadCount, SUMMARY_KEY_BYTES, SegmentFacts, SummaryRow, SummaryRows};
use crate::segment::summary_orders::{COVERS_LIST, ROW_FOOTER_BYTES, SHARE_BUCKETS, SHARE_SCALE, reclaim_share, share_bucket};

/// A bar counts in quarters of a segment's bytes
pub const BAR_QUARTERS: u64 = 4;
/// A segment is rewritten once this many quarters of its bytes are dead, the plain bar: a half
pub const PLAIN_BAR_QUARTERS: u64 = 2;
/// A marked segment waits for this many while its highest mark stands, since it dies whole soon. A rewrite there copies at
/// most a third of a byte for each byte freed, so a record freed early still comes back
pub const EARLY_BAR_QUARTERS: u64 = 3;
/// Bytes of one job's merge buffer, which carries records and then row blocks. With a row block for each input, a job's
/// merge buffers come to about 2 MiB
pub const MERGE_BUFFER_BYTES: usize = 3 * 512 * 1024;
/// Most row blocks of an output that can hold rows kept for a cue, past which the merge waits for the cue to go
pub const MOST_KEPT_BLOCKS: usize = 1_024;
/// Rows one step of a merge decides
pub const MERGE_STEP_ROWS: usize = 4_096;
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
/// Rounds an install waits for room to park its inputs, past which the merge gives way
pub const MOST_PARK_WAITS: u32 = 4_096;
/// Most segments of a column that overlap one key before a merge is due
pub const MOST_OVERLAP: usize = 32;
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
    /// The outputs are whole segments and wait to replace their inputs
    Installing,
}

/// One segment going into a merge, and where its rows have reached
pub struct MergeInput {
    pub segment: SegmentNumber,
    pub block_count: u32,
    pub block_rows: u64,
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
}

impl MergeInput {
    /// Make an input with no buffers, which its job carves from its region at the start
    pub fn new() -> Self {
        Self {
            segment: SegmentNumber(0),
            block_count: 0,
            block_rows: 0,
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
        }
    }

    /// Read the heap the buffers hold
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
        let peak = self.block.peak_bytes() + self.key.peak_bytes();
        self.block = FixedList::empty();
        self.key = FixedList::empty();
        peak
    }

    /// Check a row list this long has room for every row of the input
    pub fn row_room(&self, rows: usize) -> bool {
        self.block_rows <= rows as u64
    }

    /// Read the row in hand's place among the input's rows, or the count of them once they've run out
    pub fn ordinal(&self) -> u64 {
        if self.row.is_some() { self.rows_seen - 1 } else { self.rows_seen }
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
    park_waits: u32,
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
    /// The need the region in hand was admitted with, which goes back at the job's end
    region_need: ByteCount,
    /// The forced segment a pick took, which goes back when admission turns the pick away
    forced_pick: Option<SegmentNumber>,
    /// The mapping this run's lists are carved from, held from an admitted start to the install or the abandon
    region: Option<Region>,
    /// Tests set this low to fill the grave output
    pub most_grave_blocks: BlockCount,
    pub last_slot: Option<(KeyHash, SegmentNumber, u32)>,
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
    pub has_looks_left: bool,
    looked_up_to: SegmentNumber,
}

impl<Backend: Io> RewriteJob<Backend> {
    /// Allocate the job's fixed buffers
    pub fn new(state: Arc<EngineState<Backend>>, volume: usize, lane: usize) -> Self {
        // a mutex's first lock can allocate, and this one's first real use could be in a close
        drop(hold(&state.volumes[volume].lanes[lane].partial_outputs));
        drop(hold(&state.volumes[volume].lanes[lane].merging));
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
            park_waits: 0,
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
            region_need: ByteCount(0),
            forced_pick: None,
            region: None,
            most_grave_blocks: MOST_GRAVE_BLOCKS,
            last_slot: None,
            marks: FixedList::empty(),
            placed: FixedList::empty(),
            stripe: FixedList::empty(),
            stages: Default::default(),
            stripe_copied: None,
            unit: (UnitKind::MergeRows, 0),
            row_input: 0,
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
            has_looks_left: false,
            looked_up_to: SegmentNumber(0),
        }
    }

    /// Read the heap the job's buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        let inputs = self.inputs.capacity() * size_of::<MergeInput>();
        let lists = self.listed.held_bytes() + self.crowd.held_bytes();
        ByteCount((self.crowded_key.held_bytes() + lists + inputs) as u64)
    }

    /// Read the bytes the run's region maps, nothing between runs
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
        self.claim() && self.begin()
    }

    /// Pick, take admission's word on the pick's need and claim the inputs, so no other lane picks them. The job maps its region
    /// and opens its output at its first step, on whichever thread steps it, which keeps a populate off the planner
    pub fn claim(&mut self) -> bool {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        let Some((target, is_grave_heavy)) = self.pick_admitted(volume) else {
            return false;
        };
        self.clear(target);
        volume.lanes[self.lane].is_rewriting.store(!self.is_unwalked, Ordering::Release);
        let mut inputs = [SegmentNumber(0); MOST_MERGE_SEGMENTS];
        inputs[..self.input_count].copy_from_slice(&self.numbers[..self.input_count]);
        *hold(&volume.lanes[self.lane].inputs) = inputs;
        self.claimed = Some((target, is_grave_heavy));
        true
    }

    /// Check a claimed pick waits for its first step to map its region
    pub fn is_claimed(&self) -> bool {
        self.claimed.is_some()
    }

    /// Map the claimed pick's region, carve its lists and set the job going, or give the merge up and say so
    pub fn begin(&mut self) -> bool {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        let (Some((target, is_grave_heavy)), Some((admit, bound))) = (self.claimed.take(), self.admitted.take()) else {
            return false;
        };
        let Some(region) = self.state.map_admitted(&admit) else {
            self.abandon();
            return false;
        };
        self.region_need = admit.need;
        if self.attach(region, &bound).is_err() {
            // the bound came out short of its own carving, a bug the full count shows
            self.abandon();
            return false;
        }
        self.clear(target);
        // the inputs copy in offset order when their lists fit, and a deciding pass says which rows go across and to which output
        let rows: u64 = self.inputs[..self.input_count].iter().map(|input| input.block_rows).sum();
        self.is_ordered = self.is_ordered_bound && rows as usize <= self.placed.len();
        if self.is_ordered {
            volume.offset_order_copies.fetch_add(1, Ordering::Relaxed);
            self.cut_bytes = (target.data.0 / MOST_MERGE_OUTPUTS as u64).max(self.cut_floor);
        } else {
            volume.key_order_copies.fetch_add(1, Ordering::Relaxed);
        }
        self.marks.fill(0);
        self.cuts = 0;
        self.decide_bytes = 0;
        self.decide_rows = 0;
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
        self.pick_within(volume, MOST_MERGE_SEGMENTS)
    }

    /// Pick up to this many inputs
    pub fn pick_within(&mut self, volume: &Volume<Backend>, most: usize) -> Option<(MergeTarget, bool)> {
        let room = volume.park_room().min(most);
        if !volume.can_start_rewrite() || volume.stray_outputs.load(Ordering::Acquire) > 0 || room == 0 {
            volume.note_pick(PickOutcome::Refused);
            return None;
        }
        let state = Arc::clone(&self.state);
        let kind = if self.lane == PLANNER_LANE { PickKind::Overlap } else { PickKind::Rewrite };
        let mut rows = state.hold_for_pick(self.volume, kind);
        self.is_unwalked = false;
        self.bars_fired = BarsFired::default();
        // its rows counted for nothing a walk can trust, so the output goes through the forward walk they owe
        if let Some(row) = self.rebuild.and_then(|segment| rows.get(segment)) {
            self.is_unwalked = true;
            let target = self.gather(volume, &rows, &row, 1, |_row| false);
            // the row counts every block the segment had, and only the ones that read are in the table
            self.inputs[0].block_count = self.rebuild_blocks;
            volume.note_pick(PickOutcome::Rebuild);
            return Some((target, false));
        }
        // an overlap merge copies records, so one at a time on the planner keeps ingest at its line
        if self.lane == PLANNER_LANE
            && let Some(target) = self.pick_overlap(volume, &rows, room)
        {
            volume.note_pick(PickOutcome::Overlap);
            return Some((target, false));
        }
        if volume.has_walk_target.load(Ordering::Acquire) {
            volume.note_pick(PickOutcome::WalkTarget);
            return None;
        }
        // a full pool refuses range deletes, so the segment holding the oldest cover back goes whatever the trigger says
        if let Some(row) = self.forced.take().and_then(|forced| rows.get(forced)).filter(|row| is_mergeable(volume, row)) {
            self.forced_pick = Some(row.facts.segment);
            let target = self.gather(volume, &rows, &row, 1, |_row| false);
            volume.note_pick(PickOutcome::Forced);
            return Some((target, false));
        }
        let current = DeathMark(self.state.current_mark.load(Ordering::Relaxed));
        let gave_up = self.gave_up_list(volume);
        let is_ready = |row: &SummaryRow<'_>| {
            row.facts.is_walked() && !gave_up.holds(row.facts.segment) && is_rewrite_ready(volume, row, 0, current) && is_rewrite_ready(volume, row, freed_in_place(volume, row), current)
        };
        // the most won back for the bytes read goes first, and a picked row moves to its bucket's end, so a merge that gave way
        // lets the next one up
        if let Some(first) = first_ready(&rows, is_ready) {
            let is_grave_heavy = is_grave_heavy(&first);
            let (segment, lifetime) = (first.facts.segment, first.facts.marks);
            let is_more = |row: &SummaryRow<'_>| is_ready(row) && row.facts.segment > segment && row.facts.marks.shares_lifetime(lifetime);
            let target = self.gather(volume, &rows, &first, room, is_more);
            self.note_bars(&rows, current);
            rows.rotate(segment);
            volume.note_pick(PickOutcome::Record);
            return Some((target, is_grave_heavy));
        }
        volume.note_pick(PickOutcome::Nothing);
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
        let state = Arc::clone(&self.state);
        // a column with a complete table has no overlap bound
        let directory_of = |column: ColumnId| state.complete_table(column).is_none().then(|| state.directories[column.0 as usize].as_ref()).flatten();
        let lists = (&mut self.listed, &mut self.crowd);
        let found = overlap_start(rows, directory_of, &mut self.looked_up_to, can_merge, &mut self.crowded_key, lists);
        self.has_looks_left = !rows.orders().after(self.looked_up_to).is_empty();
        let Some((crowded, column)) = found else {
            volume.note_pick(PickOutcome::OverlapMiss);
            return None;
        };
        let point = &self.crowded_key;
        let lifetime = crowded.facts.marks;
        let is_near = |row: &SummaryRow<'_>| can_merge(row) && covers_point(row, column, point) && row.facts.marks.shares_lifetime(lifetime);
        // the segments over the point are the merge's candidates, and the directory lists them
        if let Some(directory) = directory_of(column) {
            directory.overlapping(&KeyBound::of_key(point), &KeyBound::past_prefix(point), 0, &mut self.listed);
        }
        let near = self.listed.iter().filter_map(|segment| rows.get(*segment)).filter(is_near);
        // unwalked segments go first, since they need nothing from the walk
        let is_unwalked = near.filter(|row| !row.facts.is_walked()).take(2).count() == 2;
        if !is_unwalked && volume.has_walk_target.load(Ordering::Acquire) {
            // the walk goes first and asks again when it ends, and a look left here would keep the planner from it
            self.has_looks_left = false;
            return None;
        }
        let is_more = |row: &SummaryRow<'_>| is_near(row) && row.facts.is_walked() != is_unwalked;
        let target = gather_smallest(volume, (rows, &self.listed), room, is_more, &mut self.inputs, &mut self.numbers, &mut self.input_count);
        // one segment alone wins back no overlap, and the next look starts past it
        if self.input_count < 2 {
            self.looked_up_to = crowded.facts.segment;
            volume.note_pick(PickOutcome::OverlapLone);
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

    /// Take a first segment and the next ones a test passes, while they fit one output
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
            input.data_end = facts.data_end.0;
            self.numbers[self.input_count] = facts.segment;
            self.input_count += 1;
            data += facts.data_end.0;
            length += facts.file_length.0;
            blocks += facts.block_count;
            marks = marks.join(facts.marks);
            if self.input_count == most {
                break;
            }
            // the next row up that passes, within a few looks along the segment order
            let after = rows.orders().after(facts.segment).iter().take(GATHER_LOOKS);
            next = after
                .filter_map(|segment| rows.get(*segment))
                .find(|row| is_more(row))
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

    /// Walk a step of an input's rows, listing its marked ones, and once its rows run out copy them in offset order a step at a
    /// time, then go on to the next input, and to the copying pass once every input's records are across
    pub fn place_rows(&mut self) -> Result<()> {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        let input = self.placing_input;
        if input >= self.input_count {
            return self.end_placing();
        }
        let Some(pin) = volume.table.pin(self.numbers[input]) else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        if self.stripe_copied.is_some() {
            return self.copy_stripe(&pin.file);
        }
        if self.inputs[input].row.is_none() && self.inputs[input].length == 0 {
            self.inputs[input].advance(volume, &pin)?;
        }
        for _row in 0..MERGE_STEP_ROWS {
            self.unit.1 += 1;
            let Some(row) = self.inputs[input].row else {
                // the input's kept records go in offset order, whichever output each one belongs to
                self.stripe.sort_unstable_by_key(|listed| listed.offset);
                self.stripe_copied = Some(0);
                return self.copy_stripe(&pin.file);
            };
            let at = (self.inputs[input].base + self.inputs[input].ordinal()) as usize;
            if self.is_marked(at) {
                // a record's place and length fit four bytes each, as the places the copy keeps do
                let (Ok(offset), Ok(length)) = (u32::try_from(row.offset.0), u32::try_from(row.length.0)) else {
                    return Err(Error::from(ErrorKind::InvalidData));
                };
                self.stripe.push(StripeRow { offset, length, ordinal: at as u32 })?;
            }
            self.inputs[input].advance(volume, &pin)?;
        }
        Ok(())
    }

    /// Copy a step of an input's listed records in offset order, a run to a read, staging each into its output's buffer, and once
    /// they're all across go on to the next input
    pub fn copy_stripe(&mut self, from: &Backend::File) -> Result<()> {
        // a step takes runs until a buffer's bytes are across, and a run ends only at a gap or the buffer's edge
        let end = self.stripe.len();
        let (mut lone, mut runs, mut run_records, mut gaps, mut copied) = (0, 0, 0, 0, 0);
        let mut index = self.stripe_copied.unwrap_or(0);
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        while index < end && copied < COPY_STEP_BYTES {
            let last = self.run_end(index, end);
            copied += self.stripe[index..=last].iter().map(|listed| u64::from(listed.length)).sum::<u64>();
            gaps += self.copy_run(from, index, last)?;
            let records = (last + 1 - index) as u64;
            if records == 1 {
                lone += 1;
            } else {
                runs += 1;
                run_records += records;
            }
            volume.span_runs[(records.ilog2() as usize).min(SPAN_RUN_BUCKETS - 1)].fetch_add(1, Ordering::Relaxed);
            index = last + 1;
        }
        self.unit = (UnitKind::StripeCopy, copied);
        volume.lone_copies.fetch_add(lone, Ordering::Relaxed);
        volume.span_reads.fetch_add(runs, Ordering::Relaxed);
        volume.span_records.fetch_add(run_records, Ordering::Relaxed);
        volume.span_gap_bytes.fetch_add(gaps, Ordering::Relaxed);
        if index < end {
            self.stripe_copied = Some(index);
            return Ok(());
        }
        self.stripe_copied = None;
        self.stripe.clear();
        self.placing_input += 1;
        Ok(())
    }

    /// Read an output's file: the first is the output in hand, and the later ones wait in their slots
    pub fn slot_file(&self, slot: usize) -> Result<Backend::File> {
        let output = if slot == 0 { self.output.as_ref() } else { self.slot_outputs.get(slot).and_then(Option::as_ref) };
        output.map(|output| output.file.clone()).ok_or_else(|| Error::from(ErrorKind::NotFound))
    }

    /// End the placing: write out every output's staged records, and start the copying pass on the first output
    pub fn end_placing(&mut self) -> Result<()> {
        for output in 0..=self.cuts {
            self.flush_stage(output)?;
        }
        self.records_end = FileOffset(self.outputs_end[0]);
        self.used = 0;
        for input in &mut self.inputs[..self.input_count] {
            input.rewind();
        }
        self.phase = RewritePhase::Copying;
        Ok(())
    }

    /// Find the last listed record a run from this one takes: each next one starts within the gap bound of the one before, and
    /// the run's bytes fit the read buffer
    pub fn run_end(&self, index: usize, end: usize) -> usize {
        let start = self.stripe[index].start();
        let mut span_end = self.stripe[index].end();
        let mut last = index;
        while last + 1 < end {
            let next = self.stripe[last + 1];
            let next_end = next.end();
            if next.start() > span_end + SPAN_GAP_BYTES || next_end - start > self.buffer.len() as u64 {
                break;
            }
            span_end = next_end;
            last += 1;
        }
        last
    }

    /// Copy a run of listed records with one read into the buffer, staging each into its output's buffer, and return the gap bytes
    /// the read took. A record past the buffer goes across alone, in parts
    pub fn copy_run(&mut self, from: &Backend::File, index: usize, last: usize) -> Result<u64> {
        let start = self.stripe[index].start();
        let span = (self.stripe[last].end() - start) as usize;
        if span > self.buffer.len() {
            return self.copy_large(from, index).map(|()| 0);
        }
        from.read_exact_at(FileOffset(start), &mut self.buffer[..span])?;
        let mut kept = 0;
        for position in index..=last {
            let listed = self.stripe[position];
            let output = self.listed_output(listed.ordinal);
            let at = self.stage(output, (listed.start() - start) as usize, listed.length as usize)?;
            self.place(listed.ordinal, at)?;
            kept += u64::from(listed.length);
        }
        Ok(span as u64 - kept)
    }

    /// Read which output a listed record of the input being placed goes to
    pub fn listed_output(&self, ordinal: u32) -> usize {
        let input = self.placing_input;
        self.output_of(input, u64::from(ordinal) - self.inputs[input].base)
    }

    /// Put a record from the read buffer in its output's stage, writing the stage out first when it lacks the room, and return
    /// where the record lands in the output
    pub fn stage(&mut self, output: usize, read: usize, length: usize) -> Result<FileOffset> {
        if length > self.stages[output].room() {
            self.flush_stage(output)?;
        }
        let at = self.outputs_end[output] + self.stages[output].len() as u64;
        // the row blocks sit past this, and a key's versions kept for a cue are all that can reach them
        if self.target.is_none_or(|target| at + length as u64 > target.blocks_at.0) {
            return Err(Error::from(ErrorKind::OutOfMemory));
        }
        if length > self.stages[output].capacity() {
            let file = self.slot_file(output)?;
            file.write_at(FileOffset(at), &[IoSlice::new(&self.buffer[read..read + length])])?;
            self.outputs_end[output] += length as u64;
            return Ok(FileOffset(at));
        }
        self.stages[output].extend_from_slice(&self.buffer[read..read + length])?;
        Ok(FileOffset(at))
    }

    /// Write an output's staged records out at its records' end
    pub fn flush_stage(&mut self, output: usize) -> Result<()> {
        if self.stages[output].is_empty() {
            return Ok(());
        }
        let file = self.slot_file(output)?;
        file.write_at(FileOffset(self.outputs_end[output]), &[IoSlice::new(&self.stages[output])])?;
        self.outputs_end[output] += self.stages[output].len() as u64;
        self.stages[output].clear();
        Ok(())
    }

    /// Copy one listed record past the read buffer to its output in parts, after the records staged before it
    pub fn copy_large(&mut self, from: &Backend::File, index: usize) -> Result<()> {
        let listed = self.stripe[index];
        let output = self.listed_output(listed.ordinal);
        self.flush_stage(output)?;
        let at = self.outputs_end[output];
        let length = u64::from(listed.length);
        if self.target.is_none_or(|target| at + length > target.blocks_at.0) {
            return Err(Error::from(ErrorKind::OutOfMemory));
        }
        let file = self.slot_file(output)?;
        let mut done = 0;
        while done < length {
            let part = (length - done).min(self.buffer.len() as u64) as usize;
            from.read_exact_at(FileOffset(listed.start() + done), &mut self.buffer[..part])?;
            file.write_at(FileOffset(at + done), &[IoSlice::new(&self.buffer[..part])])?;
            done += part as u64;
        }
        self.outputs_end[output] += length;
        self.place(listed.ordinal, FileOffset(at))
    }

    /// Note where a listed record landed in the output
    pub fn place(&mut self, ordinal: u32, at: FileOffset) -> Result<()> {
        let Some(placed) = self.placed.get_mut(ordinal as usize) else {
            return Err(Error::from(ErrorKind::InvalidData));
        };
        *placed = at.0 as u32;
        Ok(())
    }

    /// Read where the input's records end
    pub fn inputs_data_end(&self) -> u64 {
        self.target.map_or(0, |target| target.data.0)
    }

    /// End the copying. Several inputs becoming one wins back overlap whatever it dropped, and one input that dropped nothing
    /// has nothing to install
    pub fn finish_rows(&mut self) -> Result<()> {
        self.unit = (UnitKind::Footer, self.writer.entries().len() as u64);
        let Some(output) = self.output.clone() else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        if self.dropped_rows == 0 && self.input_count == 1 && !self.is_cutting && self.staged.is_empty() && self.rebuild.is_none() {
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
        let outputs = self.staged.len() + usize::from(self.output.is_some()) + usize::from(self.grave_output.is_some());
        self.unit = (UnitKind::Install, (self.input_count + outputs) as u64);
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
        }
        let retired: [_; MOST_MERGE_SEGMENTS] = array::from_fn(|index| inputs.get(index).and_then(|number| volume.table.retire(*number)));
        self.note_install();
        volume.lanes[self.lane].rewrites_done.fetch_add(1, Ordering::AcqRel);
        self.summarize(volume, inputs);
        let staged = self.staged.iter().map(|staged| staged.facts.segment);
        let last = self.totals.and(self.output.as_ref()).map(|output| output.number);
        let graves = self.grave_totals.and(self.grave_output.as_ref()).map(|output| output.number);
        let written = rows_bytes(&hold(&volume.summary), staged.chain(last).chain(graves));
        volume.note_rewrite_bytes(gone, written);
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
        // a claimed pick that never began gives its admission back with nothing written
        self.claimed = None;
        if let Some((admit, _bound)) = self.admitted.take() {
            self.state.give_back(admit.role, admit.need, ByteCount(0));
        }
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
    (rows, candidates): (&SummaryRows, &[SegmentNumber]),
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
        let listed = candidates.iter().filter_map(|segment| rows.get(*segment));
        let Some(row) = listed.filter(|row| is_more(row) && fits(row)).min_by_key(|row| row.facts.data_end) else {
            break;
        };
        let facts = row.facts;
        inputs[*input_count].segment = facts.segment;
        inputs[*input_count].block_count = facts.block_count;
        inputs[*input_count].block_rows = rows_of(&row);
        inputs[*input_count].data_end = facts.data_end.0;
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
pub fn overlap_start<'rows, 'state>(
    rows: &'rows SummaryRows,
    directory_of: impl Fn(ColumnId) -> Option<&'state ColumnDirectory>,
    looked_up_to: &mut SegmentNumber,
    can_merge: impl Fn(&SummaryRow<'_>) -> bool,
    point: &mut FixedList<u8>,
    lists: (&mut FixedList<SegmentNumber>, &mut FixedList<SegmentNumber>),
) -> Option<(SummaryRow<'rows>, ColumnId)> {
    let (listed, crowd) = lists;
    for _look in 0..OVERLAP_LOOKS {
        let segment = *rows.orders().after(*looked_up_to).first()?;
        let row = rows.get(segment)?;
        let mut columns = row.cursor();
        for _column in 0..row.column_count {
            let facts = columns.column()?;
            let Some(directory) = directory_of(facts.column) else {
                continue;
            };
            let high = if facts.bound.is_empty() { KeyBound::HIGHEST } else { KeyBound::past_prefix(facts.bound) };
            directory.starting_inside(&KeyBound::of_key(facts.lowest), &high, listed);
            let others = listed.iter().filter_map(|other| rows.get(*other));
            let inside = others.filter_map(|other| lowest_inside(&other, facts.column, facts.lowest, facts.bound));
            let mut probes = iter::once(facts.lowest).chain(inside.take(OVERLAP_PROBES));
            let found = probes.find(|key| is_crowded(rows, directory, &can_merge, (facts.column, key), crowd));
            if let Some(key) = found.filter(|_key| can_merge(&row)) {
                point.set_from(key).ok()?;
                return Some((row, facts.column));
            }
        }
        *looked_up_to = segment;
    }
    None
}

/// Check more than the bound's mergeable segments hold a key, counting the directory's segments over it against their rows
pub fn is_crowded(
    rows: &SummaryRows,
    directory: &ColumnDirectory,
    can_merge: impl Fn(&SummaryRow<'_>) -> bool,
    (column, key): (ColumnId, &[u8]),
    crowd: &mut FixedList<SegmentNumber>,
) -> bool {
    // a summary row keeps a key cut short, so the directory is asked for every key that starts with it
    let (low, high) = (KeyBound::of_key(key), KeyBound::past_prefix(key));
    let (mut covering, mut skip) = (0, 0);
    for _page in 0..CROWD_PAGES {
        let page = directory.overlapping(&low, &high, skip, crowd);
        skip += crowd.len();
        let over = crowd.iter().filter_map(|segment| rows.get(*segment));
        covering += over.filter(|row| can_merge(row) && covers_point(row, column, key)).count();
        if covering > MOST_OVERLAP {
            return true;
        }
        if !page.is_cut {
            return false;
        }
    }
    false
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

/// Read a segment's bar in quarters: the early bar while its highest mark stands, and the plain one after that or with no marks
pub fn bar_quarters(marks: MarkRange, current: DeathMark) -> u64 {
    if marks.highest().is_some_and(|highest| current <= highest) {
        EARLY_BAR_QUARTERS
    } else {
        PLAIN_BAR_QUARTERS
    }
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
    /// Columns the inputs hold in all, the most an output can carry
    pub columns: usize,
    /// The smallest block any of those columns writes
    pub block_bytes: usize,
    /// One input copies in offset order, with marks, places and a stripe for its rows
    pub is_ordered: bool,
    /// The most blocks the grave output takes
    pub most_grave_blocks: BlockCount,
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
        ByteCount((REGION_HEAD_BYTES + writers + inputs + keys + buffers + staged + ordered) as u64)
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

/// Find the ready row that wins most back in the highest share bucket that has one, down to the least share the plain bar lets
/// in, and then among the rows with covers. A pick looks at no more than the first few rows of a bucket, and of equal shares
/// takes the last, which leaves the rows before it to other lanes
pub fn first_ready<'rows>(rows: &'rows SummaryRows, is_ready: impl Fn(&SummaryRow<'_>) -> bool) -> Option<SummaryRow<'rows>> {
    let least = share_bucket(SHARE_SCALE * PLAIN_BAR_QUARTERS / BAR_QUARTERS);
    let mut lists = (least..SHARE_BUCKETS).rev().chain(iter::once(COVERS_LIST));
    lists.find_map(|list| {
        let looked = rows.orders().share_list(list).take(BUCKET_LOOKS).filter_map(|segment| rows.get(segment));
        looked.filter(|row| is_ready(row)).max_by_key(reclaim_share)
    })
}
