//! One scan's place: a source for each sealed segment it stands in, a cursor for each write index shard, and its last key

use std::mem;

use reel_api::column_id::ColumnId;
use reel_api::direction::Direction;
use reel_api::units::{ByteCount, FileOffset, WritePlace};

use crate::index::shard_cursor::{ShardCursor, TreeCursor};
use crate::engine::read_path::Located;
use crate::index::open_slots::OpenSlots;
use crate::index::write_index::SHARD_COUNT;
use crate::pool::{touched_capacity, touched_filled};
use crate::row_kind::RowKind;
use crate::segment::block_entry::{BlockEntry, KeyBound};
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
pub const BATCH_BYTES: usize = 64 * 1024;
/// The largest record a batch takes: a 4 KiB value with its key and header fits, so a store of them scans in batches
pub const MOST_BATCH_RECORD: usize = 8 * 1024;
/// Records in one segment this close join into one read
pub const JOIN_GAP: u64 = 4 * 1024;

/// One key a scan located ahead, and where its record sits in the batch's buffer once read
#[derive(Clone, Copy, Debug)]
pub struct BatchEntry {
    pub key_at: usize,
    pub key_length: usize,
    pub located: Located,
    pub record: Option<(usize, usize)>,
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
    pub order: Vec<u16>,
}

impl ScanBatch {
    /// Allocate a batch's buffers
    pub fn new(widest_key: usize) -> Self {
        Self {
            entries: Vec::with_capacity(BATCH_KEYS),
            next: 0,
            point: WritePlace(0),
            keys: touched_capacity(BATCH_KEYS * widest_key, 0),
            records: touched_capacity(BATCH_BYTES, 0),
            order: touched_capacity(BATCH_KEYS, 0),
        }
    }

    /// Drop every key the batch holds
    pub fn clear(&mut self) {
        self.entries.clear();
        self.next = 0;
        self.keys.clear();
        self.records.clear();
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
        self.entries.capacity() * size_of::<BatchEntry>() + self.keys.capacity() + self.records.capacity() + self.order.capacity() * size_of::<u16>()
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
        self.forget_held();
        for head in &mut self.heads {
            head.cursor.reset();
            head.is_placed = false;
            head.has_key = false;
        }
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
        ByteCount(self.sources.capacity() as u64 * source + (keys + heads + own + found + head + self.window.heap_bytes() + self.batch.heap_bytes()) as u64)
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
    let batch = BATCH_KEYS * (widest_key as usize + size_of::<BatchEntry>() + size_of::<u16>()) + BATCH_BYTES;
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
