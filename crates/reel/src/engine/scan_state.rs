//! One scan's place: a source for each sealed segment it stands in, a cursor for each write index shard, and its last key

use std::mem;

use reel_api::column_id::ColumnId;
use reel_api::direction::Direction;
use reel_api::units::{ByteCount, FileOffset, Nanoseconds, RowCount, WritePlace};

use crate::index::shard_cursor::{ShardCursor, TreeCursor};
use crate::index::sorted_keys::{KeyBatch, KeyEntry, MOST_KEY_BYTES};
use crate::engine::read_path::{Located, SealedRow};
use crate::index::open_slots::OpenSlots;
use crate::index::write_index::SHARD_COUNT;
use crate::pool::{touched_capacity, touched_filled};
use crate::row_kind::RowKind;
use crate::segment::block_entry::{BlockEntry, KeyBound};
use crate::prefetch::{LINE_BYTES, fetch_span};
use crate::segment::block_cache::MOST_MANY_BLOCKS;
use crate::segment::row_block::{FooterRow, ROW_BLOCK_BYTES, RowBlockWalk};
use crate::segment::segment_tail::{ColumnTail, SegmentNumber};

/// The most scans a store holds at once
pub const MOST_SCANS: usize = 32;
/// The most segments a scan keeps a place in, twice the overlap bound's worth of room. A key past that many goes by seeks
pub const MOST_SCAN_SOURCES: usize = 64;
/// The sources that keep their block in their own buffer, so a step reads it with no cache lookup. The rest go through the cache
pub const HELD_SOURCE_BLOCKS: usize = 8;
/// A generation no directory has, so a scan seats itself at its next step
pub const NO_GENERATION: u64 = u64::MAX;
/// A leaf of the scan's tree with no source
const NO_SOURCE: u16 = u16::MAX;

/// Where a scan stops
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanLimit<'key> {
    /// At the end of the column
    None,
    /// Below this key going up, and at it going down
    Key(&'key [u8]),
    /// Past every key with this prefix
    Prefix(&'key [u8]),
}

/// What a scan walks
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScanRange<'key> {
    pub column: ColumnId,
    pub start: &'key [u8],
    pub is_start_inclusive: bool,
    pub limit: ScanLimit<'key>,
    pub direction: Direction,
    /// The rows the caller takes, when it says, so the walk locates and reads no more than that a batch. A hint: a walk goes on
    /// past it when asked
    pub wanted: Option<RowCount>,
}

/// What lets a counting walk take a block by its line: the lowest bound of anything else the walk knows of, and the rows so far
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockTake {
    pub gate: KeyBound,
    pub rows: u64,
}

/// One sealed segment's place in a scan. The source holds its key and row, and reads its block through the cache
#[derive(Clone, Debug)]
pub struct SegmentSource {
    pub segment: SegmentNumber,
    pub place_base: WritePlace,
    pub line: BlockEntry,
    pub block: u32,
    pub first_block: u32,
    pub end_block: u32,
    pub position: usize,
    pub run: usize,
    pub rows_left: usize,
    pub is_at_block_end: bool,
    pub key: Vec<u8>,
    pub row: FooterRow,
    pub tail: ColumnTail,
    pub is_countable: bool,
    /// The block this source stands in, copied out of the cache, and where it came from
    pub held: Vec<u8>,
    pub held_at: Option<(SegmentNumber, u32)>,
    /// Blocks this source copied into its buffer, and rows it read through the cache, since the walk started
    pub fills: u64,
    pub cache_steps: u64,
}

/// One write index shard's next key
#[derive(Clone, Debug)]
pub struct ShardHead {
    pub cursor: ShardCursor,
    pub key: Vec<u8>,
    pub has_key: bool,
    pub is_placed: bool,
    /// Every place at or below this was in the index when the head was placed
    pub placed_at: WritePlace,
}

/// The most keys a scan locates ahead of its caller at one point, and the bytes of records it reads for them in one go
pub const BATCH_KEYS: usize = 32;
/// The most keys a batch of a scan told it wants more than one batch locates, inside the same bytes of records
pub const SCAN_BATCH_KEYS: usize = 256;
/// The most runs one round of a batch's reads sends before it waits
pub const MOST_BATCH_RUNS: usize = 32;
pub const BATCH_BYTES: usize = 64 * 1024;
/// The largest record a batch takes: a 4 KiB value with its key and header fits, so a store of them scans in batches
pub const MOST_BATCH_RECORD: usize = 8 * 1024;
/// Records in one segment this close join into one read
pub const JOIN_GAP: u64 = 4 * 1024;

/// Count the keys a scan's next batch locates: the told rows it still wants, the one asked for now among them, up to
/// SCAN_BATCH_KEYS. A scan told no count starts at BATCH_KEYS and doubles with the rows it has taken, so a long one reaches
/// SCAN_BATCH_KEYS and a short one locates no more than twice its rows and BATCH_KEYS past what it takes
pub fn batch_room(wanted: u64, asked: u64) -> usize {
    if wanted == u64::MAX {
        let taken = usize::try_from(asked).map_or(SCAN_BATCH_KEYS, |taken| taken.min(SCAN_BATCH_KEYS));
        return taken.saturating_add(1).next_power_of_two().clamp(BATCH_KEYS, SCAN_BATCH_KEYS);
    }
    usize::try_from(wanted.saturating_sub(asked).saturating_add(1)).map_or(SCAN_BATCH_KEYS, |left| left.min(SCAN_BATCH_KEYS))
}

/// A batch entry that holds nothing, which the batch's room is written with at open
pub const NO_BATCH_ENTRY: BatchEntry = BatchEntry { key_at: 0, key_length: 0, located: Located::Absent, record: None, is_value: false };

/// One key a scan located ahead, and where its record sits in the batch's buffer once read
#[derive(Clone, Copy, Debug)]
pub struct BatchEntry {
    pub key_at: usize,
    pub key_length: usize,
    pub located: Located,
    pub record: Option<(usize, usize)>,
    /// The span in the batch's buffer holds the value itself, from the record cache, where it would hold the record
    pub is_value: bool,
}

/// Keys a scan located ahead at one point, answered in order. Each answer stands as of that point, so a write that lands
/// while the batch lasts shows from the next batch on
#[derive(Clone, Debug)]
pub struct ScanBatch {
    pub entries: Vec<BatchEntry>,
    pub next: usize,
    pub point: WritePlace,
    pub keys: Vec<u8>,
    pub records: Vec<u8>,
    /// Each entry's sealed row for the batch's read, and the span its record landed at
    pub rows: Vec<Option<SealedRow>>,
    pub spans: Vec<Option<(usize, usize)>>,
}

impl ScanBatch {
    /// Allocate a batch's buffers
    pub fn new(widest_key: usize) -> Self {
        Self {
            entries: touched_capacity(SCAN_BATCH_KEYS, NO_BATCH_ENTRY),
            next: 0,
            point: WritePlace(0),
            keys: touched_capacity(SCAN_BATCH_KEYS * widest_key, 0),
            records: touched_capacity(BATCH_BYTES, 0),
            rows: touched_capacity(SCAN_BATCH_KEYS, None),
            spans: touched_capacity(SCAN_BATCH_KEYS, None),
        }
    }

    /// Drop every key the batch holds
    pub fn clear(&mut self) {
        self.entries.clear();
        self.next = 0;
        self.keys.clear();
        self.records.clear();
        self.rows.clear();
        self.spans.clear();
    }

    /// Check the batch has a key left to answer
    pub fn has_next(&self) -> bool {
        self.next < self.entries.len()
    }

    /// Read an entry's key
    pub fn key_of(&self, entry: &BatchEntry) -> &[u8] {
        &self.keys[entry.key_at..entry.key_at + entry.key_length]
    }

    /// Read the heap the batch holds
    pub fn heap_bytes(&self) -> usize {
        let rows = self.rows.capacity() * size_of::<Option<SealedRow>>() + self.spans.capacity() * size_of::<Option<(usize, usize)>>();
        self.entries.capacity() * size_of::<BatchEntry>() + self.keys.capacity() + self.records.capacity() + rows
    }
}

/// One scan's fixed buffers and place, out of the store's pool for as long as the scan lives
#[derive(Clone, Debug)]
pub struct ScanState {
    pub column: ColumnId,
    pub direction: Direction,
    pub position: Vec<u8>,
    pub is_inclusive: bool,
    pub limit: Vec<u8>,
    pub has_limit: bool,
    pub point: Option<WritePlace>,
    pub generation: u64,
    pub frontier: Option<KeyBound>,
    pub sources: Vec<SegmentSource>,
    pub source_count: usize,
    /// A loser tree over the sources: each inner node keeps the loser of its match, and `winner` the closest source
    pub losers: Vec<u16>,
    pub winners: Vec<u16>,
    pub winner: u16,
    pub width: usize,
    pub is_tree_stale: bool,
    pub spill: SegmentSource,
    pub is_spilled: bool,
    pub heads: Vec<ShardHead>,
    pub closest_head: Option<usize>,
    /// The oldest place any head was placed at, and the view they were placed in
    pub heads_mark: WritePlace,
    pub heads_view: OpenSlots,
    pub key: Vec<u8>,
    pub found: Vec<SegmentNumber>,
    pub origin: Vec<u8>,
    pub out_key: Vec<u8>,
    pub is_counting: bool,
    pub counted: u64,
    pub taken: u64,
    /// Index lookups this walk made, for keys a head showed or that came in under it
    pub lookups: u64,
    /// Keys answered from a record the batch read in a joined run
    pub batched: u64,
    pub window: ScanWindows,
    /// A sorted column's sealed keys from its index, graves among them, the next at `sorted_next`
    pub sorted: KeyBatch,
    pub sorted_next: usize,
    /// The index holds nothing past the batch
    pub is_sorted_done: bool,
    /// The walk takes its sealed keys from the column's sorted index, and seats no segment
    pub is_on_index: bool,
    /// The rows the caller said it wants, and the rows it has asked for with a value
    pub wanted: u64,
    pub asked: u64,
    pub batch: ScanBatch,
}

impl SegmentSource {
    /// Allocate a source's key buffer, and a block buffer of this many bytes
    pub fn new(widest_key: usize, held_bytes: usize) -> Self {
        Self {
            segment: SegmentNumber(0),
            place_base: WritePlace(0),
            line: BlockEntry::default(),
            block: 0,
            first_block: 0,
            end_block: 0,
            position: 0,
            run: 0,
            rows_left: 0,
            is_at_block_end: true,
            key: touched_capacity(widest_key, 0),
            row: FooterRow {
                place: WritePlace(0),
                offset: FileOffset(0),
                length: ByteCount(0),
                kind: RowKind::Value,
            },
            tail: ColumnTail::default(),
            is_countable: false,
            held: touched_capacity(held_bytes, 0),
            held_at: None,
            fills: 0,
            cache_steps: 0,
        }
    }

    /// Stand at the start of a block's rows, or past its end for a walk going down
    pub fn enter_block(&mut self, line: BlockEntry) {
        self.line = line;
        self.position = 0;
        self.is_at_block_end = true;
        self.key.clear();
    }

    /// Read the next row of a block, or nothing at its end
    pub fn next_in_block(&mut self, block: &[u8]) -> Option<FooterRow> {
        let mut walk = RowBlockWalk::open_checked(block, self.place_base)?;
        walk.resume(self.position);
        let row = walk.next_row(&mut self.key)?;
        self.position = walk.position();
        Some(row)
    }

    /// Read the row before this one in a block, or nothing at its start
    pub fn previous_in_block(&mut self, block: &[u8]) -> Option<FooterRow> {
        let mut walk = RowBlockWalk::open_checked(block, self.place_base)?;
        if self.is_at_block_end {
            self.is_at_block_end = false;
            self.run = walk.restart_count().checked_sub(1)?;
            self.rows_left = walk.rows_at_restart(self.run);
        }
        if self.rows_left == 0 {
            self.run = self.run.checked_sub(1)?;
            self.rows_left = walk.rows_at_restart(self.run);
        }
        self.rows_left -= 1;
        walk.row_in_run(self.run, self.rows_left, &mut self.key)
    }

    /// Step to the next row in the walk's direction in the held block
    pub fn step_held(&mut self, direction: Direction) -> Option<FooterRow> {
        let held = mem::take(&mut self.held);
        let row = match direction {
            Direction::Ascending => self.next_in_block(&held),
            Direction::Descending => self.previous_in_block(&held),
        };
        self.held = held;
        row
    }

    /// Find a block's first row past a key going up, or nothing when its rows end first
    pub fn seek_up(&mut self, block: &[u8], target: &[u8], is_inclusive: bool) -> Option<FooterRow> {
        let mut walk = RowBlockWalk::open_checked(block, self.place_base)?;
        let mut row = walk.seek(target, &mut self.key)?;
        while !is_inclusive && self.key == target {
            row = walk.next_row(&mut self.key)?;
        }
        self.position = walk.position();
        Some(row)
    }

    /// Find a block's first row past a key going down, or nothing when every row stands above it
    pub fn seek_down(&mut self, block: &[u8], target: &[u8], is_inclusive: bool) -> Option<FooterRow> {
        let mut walk = RowBlockWalk::open_checked(block, self.place_base)?;
        let (run, rows) = walk.seek_back(target, is_inclusive, &mut self.key)?;
        self.is_at_block_end = false;
        self.run = run;
        self.rows_left = rows - 1;
        walk.row_in_run(run, self.rows_left, &mut self.key)
    }
}

/// How often a volume weighs whether the record cache can hold most of its live records for scans
pub const SCAN_CACHE_WEIGH_EVERY: Nanoseconds = Nanoseconds(1_000_000_000);
/// The cache holds most of a volume when it holds all but one of these parts of its live records
pub const SCAN_CACHE_PARTS: u64 = 4;

/// The most sources one batch seats together
pub const SEAT_BATCH: usize = MOST_MANY_BLOCKS;
/// Bytes a restart's row takes past its offset with a short key, so a probe asks for the line its row runs into as well
pub const RESTART_ROW_BYTES: usize = 24;

/// Seat several sources in their cached blocks at once: one binary-search probe a round for every source, each asking the CPU
/// for its next probe's row, so their cache misses wait together where one source's would wait after another's. A source finds
/// the row `seek_up` or `seek_down` would. Nothing for a source with no block, or no row past the key in it, which then seats
/// alone
pub fn seek_together(
    sources: &mut [SegmentSource],
    blocks: &[Option<&[u8]>],
    target: &[u8],
    is_inclusive: bool,
    direction: Direction,
    rows: &mut [Option<FooterRow>],
) {
    // the search keeps the last restart below the key going up, and the last one at or before it going down
    let is_low = |key: &[u8]| key < target || (direction == Direction::Descending && is_inclusive && key == target);
    let count = sources.len().min(blocks.len()).min(rows.len()).min(SEAT_BATCH);
    let mut walks: [Option<RowBlockWalk<'_>>; SEAT_BATCH] = [const { None }; SEAT_BATCH];
    let mut bounds = [(0_usize, 0_usize); SEAT_BATCH];
    for block in blocks.iter().take(count).flatten() {
        // the counts and the restart offsets sit at the block's end
        fetch_span(block, block.len().saturating_sub(2 * LINE_BYTES), 2 * LINE_BYTES);
    }
    for ((block, source), (walk, bound)) in blocks.iter().zip(sources.iter()).zip(walks.iter_mut().zip(&mut bounds)).take(count) {
        let Some(opened) = block.and_then(|block| RowBlockWalk::open_checked(block, source.place_base)) else {
            continue;
        };
        if opened.restart_count() == 0 {
            continue;
        }
        // going down, a block whose first row already stands past the key holds nothing for this seat
        if direction == Direction::Descending && !opened.restart_key(0).is_some_and(is_low) {
            continue;
        }
        *bound = (0, opened.restart_count());
        fetch_span(opened.block_rows(), opened.restart_at(bound.1 / 2), RESTART_ROW_BYTES);
        *walk = Some(opened);
    }
    let mut is_searching = true;
    while is_searching {
        is_searching = false;
        for (walk, bound) in walks.iter_mut().zip(&mut bounds).take(count) {
            let Some(searching) = walk.as_ref() else {
                continue;
            };
            let (low, high) = *bound;
            if low + 1 >= high {
                continue;
            }
            is_searching = true;
            let middle = (low + high) / 2;
            let Some(is_below) = searching.restart_key(middle).map(is_low) else {
                *walk = None;
                continue;
            };
            *bound = if is_below { (middle, high) } else { (low, middle) };
            let next = if bound.0 + 1 < bound.1 { (bound.0 + bound.1) / 2 } else { bound.0 };
            fetch_span(searching.block_rows(), searching.restart_at(next), RESTART_ROW_BYTES);
        }
    }
    // a walk from its restart reads up to a run of rows, which every source asks for before any walks
    for (walk, bound) in walks.iter().zip(&bounds).take(count) {
        let Some(found) = walk.as_ref() else {
            continue;
        };
        let start = found.restart_at(bound.0);
        let end = if bound.0 + 1 < found.restart_count() { found.restart_at(bound.0 + 1) + RESTART_ROW_BYTES } else { found.block_rows().len() };
        fetch_span(found.block_rows(), start, end.saturating_sub(start));
    }
    for ((walk, bound), (source, row)) in walks.iter_mut().zip(&bounds).zip(sources.iter_mut().zip(rows.iter_mut())).take(count) {
        let Some(found) = walk.as_mut() else {
            continue;
        };
        *row = match direction {
            Direction::Ascending => finish_up(found, bound.0, source, target, is_inclusive),
            Direction::Descending => finish_down(found, bound.0, source, is_low),
        };
    }
}

/// Walk up from the restart a search found to the source's first row past the key, as `seek_up` does
fn finish_up(walk: &mut RowBlockWalk<'_>, restart: usize, source: &mut SegmentSource, target: &[u8], is_inclusive: bool) -> Option<FooterRow> {
    let mut row = walk.seek_from(restart, target, &mut source.key)?;
    while !is_inclusive && source.key == target {
        row = walk.next_row(&mut source.key)?;
    }
    source.position = walk.position();
    Some(row)
}

/// Count the run's rows at or before the key from the restart a search found, and stand on the last, as `seek_down` does
fn finish_down(walk: &mut RowBlockWalk<'_>, restart: usize, source: &mut SegmentSource, is_low: impl Fn(&[u8]) -> bool) -> Option<FooterRow> {
    walk.key_at_restart(restart, &mut source.key)?;
    let mut rows = 1;
    while rows < walk.rows_at_restart(restart) {
        walk.next_row(&mut source.key)?;
        if !is_low(&source.key) {
            break;
        }
        rows += 1;
    }
    source.is_at_block_end = false;
    source.run = restart;
    source.rows_left = rows - 1;
    walk.row_in_run(restart, rows - 1, &mut source.key)
}

impl ShardHead {
    /// Allocate a head's cursor and key buffer
    pub fn new(widest_key: usize, open_slots: usize) -> Self {
        Self {
            cursor: ShardCursor::new(open_slots),
            key: touched_capacity(widest_key, 0),
            has_key: false,
            is_placed: false,
            placed_at: WritePlace(0),
        }
    }
}

impl ScanState {
    /// Allocate a scan's buffers
    pub fn new(widest_key: usize, open_slots: usize) -> Self {
        Self {
            column: ColumnId(0),
            direction: Direction::Ascending,
            position: touched_capacity(widest_key, 0),
            is_inclusive: true,
            limit: touched_capacity(widest_key, 0),
            has_limit: false,
            point: None,
            generation: NO_GENERATION,
            frontier: None,
            sources: (0..MOST_SCAN_SOURCES).map(|index| SegmentSource::new(widest_key, if index < HELD_SOURCE_BLOCKS { ROW_BLOCK_BYTES } else { 0 })).collect(),
            source_count: 0,
            losers: touched_filled(MOST_SCAN_SOURCES, NO_SOURCE),
            winners: touched_filled(MOST_SCAN_SOURCES, NO_SOURCE),
            winner: NO_SOURCE,
            width: 1,
            is_tree_stale: true,
            spill: SegmentSource::new(widest_key, 0),
            is_spilled: false,
            heads: (0..SHARD_COUNT).map(|_head| ShardHead::new(widest_key, open_slots)).collect(),
            closest_head: None,
            heads_mark: WritePlace(0),
            heads_view: OpenSlots(0),
            key: touched_capacity(widest_key, 0),
            found: touched_capacity(MOST_SCAN_SOURCES, SegmentNumber(0)),
            origin: touched_capacity(widest_key, 0),
            out_key: touched_capacity(widest_key, 0),
            is_counting: false,
            counted: 0,
            taken: 0,
            lookups: 0,
            batched: 0,
            window: ScanWindows::new(),
            batch: ScanBatch::new(widest_key),
            sorted: sorted_batch(widest_key),
            sorted_next: 0,
            is_sorted_done: false,
            is_on_index: false,
            wanted: u64::MAX,
            asked: 0,
        }
    }

    /// Start a walk in these buffers
    pub fn start(&mut self, range: &ScanRange, point: Option<WritePlace>) {
        self.column = range.column;
        self.direction = range.direction;
        self.position.clear();
        self.position.extend_from_slice(range.start);
        self.is_inclusive = range.is_start_inclusive;
        self.origin.clear();
        self.origin.extend_from_slice(range.start);
        self.is_counting = false;
        self.counted = 0;
        self.taken = 0;
        self.lookups = 0;
        self.batched = 0;
        for source in &mut self.sources {
            source.fills = 0;
            source.cache_steps = 0;
        }
        self.forget_held();
        self.window.clear();
        self.batch.clear();
        self.forget_sorted();
        self.is_on_index = false;
        self.wanted = range.wanted.map_or(u64::MAX, |rows| rows.0);
        self.asked = 0;
        self.limit.clear();
        self.has_limit = match range.limit {
            ScanLimit::None => false,
            ScanLimit::Key(key) => {
                self.limit.extend_from_slice(key);
                true
            },
            ScanLimit::Prefix(prefix) => {
                self.limit.extend_from_slice(prefix);
                next_prefix(&mut self.limit)
            },
        };
        self.point = point;
        self.generation = NO_GENERATION;
    }

    /// Start a count over from its first key, stepping every row from here on
    pub fn restart_count(&mut self) {
        self.position.clear();
        self.position.extend_from_slice(&self.origin);
        self.is_inclusive = true;
        self.is_counting = false;
        self.counted = 0;
        self.taken = 0;
    }

    /// Find the lowest bound of anything but one source: another source, a shard's next key, the next segment or the end
    pub fn gate_without(&self, index: usize) -> KeyBound {
        let sources = self.sources[..self.source_count].iter().enumerate().filter(|(other, _source)| *other != index);
        let keys = sources.map(|(_other, source)| source.key.as_slice());
        let heads = self.heads.iter().filter(|head| head.has_key).map(|head| head.key.as_slice());
        let lowest = keys.chain(heads).map(KeyBound::of_key).min().unwrap_or(KeyBound::HIGHEST);
        let limit = if self.has_limit { KeyBound::of_key(&self.limit) } else { KeyBound::HIGHEST };
        lowest.min(limit).min(self.frontier.unwrap_or(KeyBound::HIGHEST))
    }

    /// Drop every place, for a list that changed, and find them again from the last key
    pub fn reseat(&mut self, generation: u64) {
        self.generation = generation;
        self.source_count = 0;
        self.is_tree_stale = true;
        self.is_spilled = false;
        self.frontier = Some(KeyBound::of_key(&self.position));
        self.forget_sorted();
        self.forget_held();
        for head in &mut self.heads {
            head.cursor.reset();
            head.is_placed = false;
            head.has_key = false;
        }
    }

    /// Drop the index's batch, so the next step reads it again from the walk's position
    pub fn forget_sorted(&mut self) {
        self.sorted.clear();
        self.sorted_next = 0;
        self.is_sorted_done = false;
    }

    /// Read the index batch's next key the walk hasn't passed, if the batch holds one
    pub fn sorted_key(&self) -> Option<&[u8]> {
        (self.sorted_next < self.sorted.len()).then(|| self.sorted.key(self.sorted_next))
    }

    /// Check a key lies past the walk's position, the position itself in when the walk hasn't passed it
    pub fn is_ahead(&self, key: &[u8]) -> bool {
        key == self.position.as_slice() && self.is_inclusive || self.is_before(&self.position, key)
    }

    /// Drop what each source held, so its next block comes through the cache
    pub fn forget_held(&mut self) {
        for source in &mut self.sources {
            source.held_at = None;
        }
    }

    /// Check a segment already has a place here
    pub fn holds(&self, segment: SegmentNumber) -> bool {
        self.sources[..self.source_count].iter().any(|source| source.segment == segment)
    }

    /// Check one key comes before another in the walk's direction
    pub fn is_before(&self, key: &[u8], other: &[u8]) -> bool {
        match self.direction {
            Direction::Ascending => key < other,
            Direction::Descending => key > other,
        }
    }

    /// Find the key the walk reaches next among the places it holds
    pub fn closest_key(&mut self) -> Option<&[u8]> {
        self.settle_tree();
        let source = self.closest_source().map(|index| self.sources[index].key.as_slice());
        let head = self.closest_head.map(|head| self.heads[head].key.as_slice());
        match (source, head) {
            (Some(source), Some(head)) => Some(if self.is_before(head, source) { head } else { source }),
            (source, head) => source.or(head),
        }
    }

    /// Read the source the tree has closest, once it's settled
    pub fn closest_source(&self) -> Option<usize> {
        (self.winner != NO_SOURCE).then_some(self.winner as usize)
    }

    /// Check one leaf of the tree comes before another, where an empty leaf comes after every source
    pub fn beats(&self, leaf: u16, other: u16) -> bool {
        leaf != NO_SOURCE && (other == NO_SOURCE || self.is_before(&self.sources[leaf as usize].key, &self.sources[other as usize].key))
    }

    /// Build the tree again over the sources, once a source came or went
    pub fn settle_tree(&mut self) {
        if !self.is_tree_stale {
            return;
        }
        self.is_tree_stale = false;
        self.width = self.source_count.next_power_of_two();
        let width = self.width;
        let leaf = |index: usize, count: usize| if index < count { index as u16 } else { NO_SOURCE };
        for node in (1..width).rev() {
            let (left, right) = (2 * node, 2 * node + 1);
            let left = if left >= width { leaf(left - width, self.source_count) } else { self.winners[left] };
            let right = if right >= width { leaf(right - width, self.source_count) } else { self.winners[right] };
            let (winner, loser) = if self.beats(right, left) { (right, left) } else { (left, right) };
            self.winners[node] = winner;
            self.losers[node] = loser;
        }
        self.winner = if width == 1 { leaf(0, self.source_count) } else { self.winners[1] };
    }

    /// Play a source's matches again from its leaf, after it moved to a later key
    pub fn replay(&mut self, index: usize) {
        if self.is_tree_stale {
            return;
        }
        let mut current = index as u16;
        let mut node = (self.width + index) / 2;
        while node >= 1 {
            if self.beats(self.losers[node], current) {
                mem::swap(&mut self.losers[node], &mut current);
            }
            node /= 2;
        }
        self.winner = current;
    }

    /// Check the index holds no row of a key at a read point: no head shows it, and nothing at or below the point came in
    /// since the heads were placed
    pub fn is_off_the_heads(&self, key: &[u8], read_point: WritePlace) -> bool {
        read_point <= self.heads_mark && self.closest_head.is_none_or(|head| self.heads[head].key != key)
    }

    /// Find the closest of the heads, once any has moved
    pub fn find_closest_head(&mut self) {
        let mut closest: Option<usize> = None;
        for (index, head) in self.heads.iter().enumerate() {
            if head.has_key && closest.is_none_or(|best| self.is_before(&head.key, &self.heads[best].key)) {
                closest = Some(index);
            }
        }
        self.closest_head = closest;
        self.heads_mark = self.heads.iter().map(|head| head.placed_at).min().unwrap_or(WritePlace(0));
    }

    /// Check a key lies past the walk's far end
    pub fn is_past_limit(&self, key: &[u8]) -> bool {
        self.has_limit
            && match self.direction {
                Direction::Ascending => key >= self.limit.as_slice(),
                Direction::Descending => key < self.limit.as_slice(),
            }
    }

    /// Read the far end of the walk as a bound
    pub fn far_bound(&self) -> KeyBound {
        match (self.direction, self.has_limit) {
            (Direction::Ascending | Direction::Descending, true) => KeyBound::of_key(&self.limit),
            (Direction::Ascending, false) => KeyBound::HIGHEST,
            (Direction::Descending, false) => KeyBound::of_key(&[]),
        }
    }

    /// Check no key at or past a bound can be inside the walk's far end
    pub fn is_bound_past_limit(&self, bound: &KeyBound) -> bool {
        self.has_limit
            && match self.direction {
                Direction::Ascending => *bound > KeyBound::of_key(&self.limit),
                Direction::Descending => *bound < KeyBound::of_key(&self.limit),
            }
    }

    /// Stand past a key, so a head that showed it looks again
    pub fn pass(&mut self, key: &[u8]) {
        self.position.clear();
        self.position.extend_from_slice(key);
        self.is_inclusive = false;
        // the closest head stands at or before every other, so a key that isn't it is on none of them
        if self.closest_head.is_none_or(|head| self.heads[head].key != key) {
            return;
        }
        for head in &mut self.heads {
            if head.has_key && head.key == key {
                head.is_placed = false;
            }
        }
    }

    /// Take a source out of the walk, keeping its buffer
    pub fn drop_source(&mut self, index: usize) {
        self.source_count -= 1;
        self.sources.swap(index, self.source_count);
        self.is_tree_stale = true;
    }

    /// Read the heap these buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        let source = size_of::<SegmentSource>() as u64;
        let keys: usize = self.sources.iter().map(|source| source.key.capacity() + source.held.capacity()).sum();
        let heads: usize = self.heads.iter().map(|head| head.key.capacity() + head.cursor.trees.capacity() * size_of::<TreeCursor>()).sum();
        let own = self.position.capacity() + self.limit.capacity() + self.key.capacity() + self.spill.key.capacity() + self.origin.capacity() + self.out_key.capacity();
        let found = self.found.capacity() * size_of::<SegmentNumber>() + (self.losers.capacity() + self.winners.capacity()) * size_of::<u16>();
        let head = self.heads.capacity() * size_of::<ShardHead>();
        let batches = self.batch.heap_bytes() + self.sorted.heap_bytes();
        ByteCount(self.sources.capacity() as u64 * source + (keys + heads + own + found + head + self.window.heap_bytes() + batches) as u64)
    }
}

/// Build the range from a key to the column's end in a direction
pub fn from_key(column: ColumnId, start: &[u8], direction: Direction) -> ScanRange<'_> {
    ScanRange {
        column,
        start,
        is_start_inclusive: true,
        limit: ScanLimit::None,
        direction,
        wanted: None,
    }
}

/// Build the range [start, end) in a direction
pub fn in_range<'key>(column: ColumnId, start: &'key [u8], end: &'key [u8], direction: Direction) -> ScanRange<'key> {
    let (first, is_start_inclusive, limit) = match direction {
        Direction::Ascending => (start, true, end),
        Direction::Descending => (end, false, start),
    };
    ScanRange {
        column,
        start: first,
        is_start_inclusive,
        limit: ScanLimit::Key(limit),
        direction,
        wanted: None,
    }
}

/// Build the range of keys under a prefix
pub fn under_prefix(column: ColumnId, prefix: &[u8]) -> ScanRange<'_> {
    ScanRange {
        column,
        start: prefix,
        is_start_inclusive: true,
        limit: ScanLimit::Prefix(prefix),
        direction: Direction::Ascending,
        wanted: None,
    }
}

impl<'key> ScanRange<'key> {
    /// Say how many rows the caller takes from this range
    pub fn wanting(self, rows: RowCount) -> Self {
        Self { wanted: Some(rows), ..self }
    }
}

/// Turn a prefix into the lowest key past all its keys, and say whether there is one
pub fn next_prefix(prefix: &mut Vec<u8>) -> bool {
    while prefix.last() == Some(&u8::MAX) {
        prefix.pop();
    }
    let Some(last) = prefix.last_mut() else {
        return false;
    };
    *last += 1;
    true
}

/// Plan the heap one scan holds
pub fn planned_scan_bytes(widest_key: u64, open_slots: usize) -> u64 {
    let keys = (MOST_SCAN_SOURCES + SHARD_COUNT + 6) as u64 * widest_key;
    let sources = (MOST_SCAN_SOURCES + 1) * size_of::<SegmentSource>();
    let heads = SHARD_COUNT * (size_of::<ShardHead>() + open_slots * size_of::<TreeCursor>());
    let held = HELD_SOURCE_BLOCKS * ROW_BLOCK_BYTES;
    let tree = 2 * MOST_SCAN_SOURCES * size_of::<u16>();
    let sorted = SCAN_BATCH_KEYS * ((widest_key as usize).clamp(1, MOST_KEY_BYTES) + size_of::<KeyEntry>());
    let rows = SCAN_BATCH_KEYS * (size_of::<Option<SealedRow>>() + size_of::<Option<(usize, usize)>>());
    let batch = SCAN_BATCH_KEYS * (widest_key as usize + size_of::<BatchEntry>()) + BATCH_BYTES + sorted + rows;
    keys + (sources + heads + held + tree + batch + MOST_SCAN_SOURCES * size_of::<SegmentNumber>() + SCAN_WINDOWS * SCAN_WINDOW_BYTES) as u64
}


/// Bytes a scan's window reads at its widest
pub const SCAN_WINDOW_BYTES: usize = 16 * 1024;
/// Windows a scan keeps, so rows interleaved across segments each keep their stretch
pub const SCAN_WINDOWS: usize = 4;
/// Bytes a window reads until rows show they lie side by side
pub const LEAST_WINDOW_BYTES: usize = 4 * 1024;
/// The largest record a window takes, so a big value goes through its own read
pub const MOST_WINDOW_RECORD: usize = LEAST_WINDOW_BYTES;

/// A stretch of one segment's records a scan has read. Rows that lie side by side come from it with no read call each, and
/// its reach grows only while they do, so a walk over scattered rows reads what it would have read anyway
#[derive(Clone, Debug)]
pub struct ScanWindow {
    bytes: Vec<u8>,
    segment: Option<SegmentNumber>,
    start: u64,
    length: usize,
    reach: usize,
}

impl ScanWindow {
    /// Allocate the window's bytes
    pub fn new() -> Self {
        Self {
            bytes: touched_filled(SCAN_WINDOW_BYTES, 0),
            segment: None,
            start: 0,
            length: 0,
            reach: LEAST_WINDOW_BYTES,
        }
    }

    /// Read the heap the window holds
    pub fn heap_bytes(&self) -> usize {
        self.bytes.capacity()
    }

    /// Forget what the window holds
    pub fn clear(&mut self) {
        self.segment = None;
        self.length = 0;
        self.reach = LEAST_WINDOW_BYTES;
    }

    /// Find a record's bytes, when the window holds them whole
    pub fn holding(&self, segment: SegmentNumber, offset: u64, length: usize) -> Option<&[u8]> {
        if self.segment != Some(segment) {
            return None;
        }
        let at = offset.checked_sub(self.start)? as usize;
        self.bytes.get(at..at + length).filter(|_record| at + length <= self.length)
    }

    /// Check a record lies just past the window in the walk's direction, which is what earns a longer read
    pub fn is_next_to(&self, segment: SegmentNumber, offset: u64, length: usize, direction: Direction) -> bool {
        if self.segment != Some(segment) {
            return false;
        }
        let span = self.length as u64;
        match direction {
            Direction::Ascending => {
                let end = self.start + span;
                offset >= end && offset < end + span
            },
            Direction::Descending => {
                let record_end = offset + length as u64;
                record_end <= self.start && record_end + span > self.start
            },
        }
    }

    /// Work out what to read for a record, and give the bytes to read into
    pub fn plan(
        &mut self,
        segment: SegmentNumber,
        (offset, length): (u64, usize),
        direction: Direction,
        data_end: u64,
    ) -> (FileOffset, &mut [u8]) {
        self.reach = if self.is_next_to(segment, offset, length, direction) {
            (self.reach * 2).min(SCAN_WINDOW_BYTES)
        } else {
            LEAST_WINDOW_BYTES
        };
        let reach = self.reach as u64;
        let (start, end) = match direction {
            Direction::Ascending => (offset, (offset + reach).min(data_end)),
            Direction::Descending => {
                let end = offset + length as u64;
                (end.saturating_sub(reach), end)
            },
        };
        self.segment = Some(segment);
        self.start = start;
        self.length = end.saturating_sub(start) as usize;
        (FileOffset(start), &mut self.bytes[..self.length])
    }
}

impl Default for ScanWindow {
    fn default() -> Self {
        Self::new()
    }
}

/// A scan's windows, one a segment, the least recently read giving way to a new segment
#[derive(Clone, Debug)]
pub struct ScanWindows {
    windows: [ScanWindow; SCAN_WINDOWS],
    used: [u64; SCAN_WINDOWS],
    clock: u64,
}

impl ScanWindows {
    /// Allocate every window's bytes
    pub fn new() -> Self {
        Self {
            windows: std::array::from_fn(|_window| ScanWindow::new()),
            used: [0; SCAN_WINDOWS],
            clock: 0,
        }
    }

    /// Read the heap the windows hold
    pub fn heap_bytes(&self) -> usize {
        self.windows.iter().map(ScanWindow::heap_bytes).sum()
    }

    /// Forget what every window holds
    pub fn clear(&mut self) {
        for window in &mut self.windows {
            window.clear();
        }
    }

    /// Find a record's bytes in the window that holds its segment
    pub fn holding(&self, segment: SegmentNumber, offset: u64, length: usize) -> Option<&[u8]> {
        self.windows.iter().find_map(|window| window.holding(segment, offset, length))
    }

    /// Plan a read for a record in its segment's window, or in the least recently read one
    pub fn plan(&mut self, segment: SegmentNumber, record: (u64, usize), direction: Direction, data_end: u64) -> (FileOffset, &mut [u8]) {
        let own = self.windows.iter().position(|window| window.segment == Some(segment));
        let oldest = (0..SCAN_WINDOWS).min_by_key(|index| self.used[*index]).unwrap_or(0);
        let index = own.unwrap_or(oldest);
        self.clock += 1;
        self.used[index] = self.clock;
        self.windows[index].plan(segment, record, direction, data_end)
    }
}

impl Default for ScanWindows {
    fn default() -> Self {
        Self::new()
    }
}

/// Allocate a scan's batch of sorted index entries, for the widest key a sorted column can have
pub fn sorted_batch(widest_key: usize) -> KeyBatch {
    let mut batch = KeyBatch::new(widest_key.clamp(1, MOST_KEY_BYTES), SCAN_BATCH_KEYS);
    batch.is_with_graves = true;
    batch
}
