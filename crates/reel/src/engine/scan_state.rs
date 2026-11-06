//! One scan's place: a source for each sealed segment it stands in, a cursor for each write index shard, and its last key

use reel_api::column_id::ColumnId;
use reel_api::direction::Direction;
use reel_api::units::{ByteCount, FileOffset, WritePlace};

use crate::index::shard_cursor::{ShardCursor, TreeCursor};
use crate::index::write_index::SHARD_COUNT;
use crate::pool::touched_capacity;
use crate::row_kind::RowKind;
use crate::segment::block_entry::{BlockEntry, KeyBound};
use crate::segment::row_block::{FooterRow, RowBlockWalk};
use crate::segment::segment_tail::SegmentNumber;

/// The most scans a store holds at once
pub const MOST_SCANS: usize = 32;
/// The most segments a scan keeps a place in, twice the overlap bound's worth of room. A key past that many goes by seeks
pub const MOST_SCAN_SOURCES: usize = 64;
/// A generation no directory has, so a scan seats itself at its next step
pub const NO_GENERATION: u64 = u64::MAX;

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
}

/// One write index shard's next key
#[derive(Clone, Debug)]
pub struct ShardHead {
    pub cursor: ShardCursor,
    pub key: Vec<u8>,
    pub has_key: bool,
    pub is_placed: bool,
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
    pub spill: SegmentSource,
    pub is_spilled: bool,
    pub heads: Vec<ShardHead>,
    pub key: Vec<u8>,
    pub found: Vec<SegmentNumber>,
}

impl SegmentSource {
    /// Allocate a source's key buffer
    pub fn new(widest_key: usize) -> Self {
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
            sources: (0..MOST_SCAN_SOURCES).map(|_source| SegmentSource::new(widest_key)).collect(),
            source_count: 0,
            spill: SegmentSource::new(widest_key),
            is_spilled: false,
            heads: (0..SHARD_COUNT).map(|_head| ShardHead::new(widest_key, open_slots)).collect(),
            key: touched_capacity(widest_key, 0),
            found: touched_capacity(MOST_SCAN_SOURCES, SegmentNumber(0)),
        }
    }

    /// Start a walk in these buffers
    pub fn start(&mut self, range: &ScanRange, point: Option<WritePlace>) {
        self.column = range.column;
        self.direction = range.direction;
        self.position.clear();
        self.position.extend_from_slice(range.start);
        self.is_inclusive = range.is_start_inclusive;
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

    /// Drop every place, for a list that changed, and find them again from the last key
    pub fn reseat(&mut self, generation: u64) {
        self.generation = generation;
        self.source_count = 0;
        self.is_spilled = false;
        self.frontier = Some(KeyBound::of_key(&self.position));
        for head in &mut self.heads {
            head.cursor.reset();
            head.is_placed = false;
            head.has_key = false;
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
    pub fn closest_key(&self) -> Option<&[u8]> {
        let sources = self.sources[..self.source_count].iter().map(|source| source.key.as_slice());
        let heads = self.heads.iter().filter(|head| head.has_key).map(|head| head.key.as_slice());
        sources.chain(heads).reduce(|best, key| if self.is_before(key, best) { key } else { best })
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
    }

    /// Read the heap these buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        let source = size_of::<SegmentSource>() as u64;
        let keys: usize = self.sources.iter().map(|source| source.key.capacity()).sum();
        let heads: usize = self.heads.iter().map(|head| head.key.capacity() + head.cursor.trees.capacity() * size_of::<TreeCursor>()).sum();
        let own = self.position.capacity() + self.limit.capacity() + self.key.capacity() + self.spill.key.capacity();
        let found = self.found.capacity() * size_of::<SegmentNumber>();
        let head = self.heads.capacity() * size_of::<ShardHead>();
        ByteCount(self.sources.capacity() as u64 * source + (keys + heads + own + found + head) as u64)
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
    let keys = (MOST_SCAN_SOURCES + SHARD_COUNT + 4) as u64 * widest_key;
    let sources = (MOST_SCAN_SOURCES + 1) * size_of::<SegmentSource>();
    let heads = SHARD_COUNT * (size_of::<ShardHead>() + open_slots * size_of::<TreeCursor>());
    keys + (sources + heads + MOST_SCAN_SOURCES * size_of::<SegmentNumber>()) as u64
}

