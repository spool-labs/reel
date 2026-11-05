//! Pending range deletes: a fixed pool per volume, in key order so a get searches it

use std::cmp::Ordering as KeyOrder;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, WritePlace};

use crate::little_endian::read_word;
use crate::pool::{touched_filled, touched_room, touched_zeroes};
use crate::segment::segment_tail::SegmentNumber;

/// Most pending range deletes a volume holds. A delete past it is refused, and maintenance makes room
pub const MOST_RANGE_COVERS: usize = 4_096;
/// Covers in one block of the order, each block keeping the cover that reaches furthest, so a search passes a block
/// that ends before its key in one step
pub const COVER_BLOCK: usize = 64;
/// Covers a search looks at beside the ones that hold its key, however they nest. A search visits every cover that
/// holds its key, so the worst case is a full pool nested over one key: 4,096 steps, 17.7 us on a macOS laptop
pub const MOST_SEARCH_STEPS: usize = 200;
/// Bytes a cover takes in a footer beside its keys: column, two key lengths and its place
pub const COVER_RECORD_BYTES: usize = 1 + 2 + 2 + size_of::<u64>();
/// Bytes a cover takes in the pool beside its keys
pub const COVER_SLOT_BYTES: usize = size_of::<CoverSlot>() + 3 * size_of::<u16>();

/// One range delete
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Cover<'key> {
    pub column: ColumnId,
    pub start: &'key [u8],
    pub end: &'key [u8],
    pub place: WritePlace,
    pub segment: SegmentNumber,
}

/// A cover's fields, with its keys in the arena
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CoverSlot {
    pub column: ColumnId,
    pub start_length: u16,
    pub end_length: u16,
    pub place: WritePlace,
    pub segment: SegmentNumber,
}

/// The pool behind the lock
#[derive(Debug)]
pub struct CoverPool {
    slots: Vec<CoverSlot>,
    free: Vec<u16>,
    order: Vec<u16>,
    reach: Vec<u16>,
    block_reach: Vec<u16>,
    keys: Vec<u8>,
    widest_key: usize,
    held: usize,
}

/// A volume's covers
#[derive(Debug)]
pub struct RangeCovers {
    count: AtomicUsize,
    pool: RwLock<CoverPool>,
    longest_walk: AtomicUsize,
}

impl CoverPool {
    /// Allocate and touch a pool for keys up to a width
    pub fn new(widest_key: usize) -> Self {
        let mut free = touched_room(MOST_RANGE_COVERS);
        for slot in (0..MOST_RANGE_COVERS as u16).rev() {
            free.push(slot);
        }
        Self {
            slots: touched_filled(MOST_RANGE_COVERS, CoverSlot::default()),
            free,
            order: touched_room(MOST_RANGE_COVERS),
            reach: touched_room(MOST_RANGE_COVERS),
            block_reach: touched_room(MOST_RANGE_COVERS / COVER_BLOCK + 1),
            keys: touched_zeroes(MOST_RANGE_COVERS * 2 * widest_key),
            widest_key,
            held: 0,
        }
    }

    /// Read a slot's first key
    pub fn start(&self, slot: u16) -> &[u8] {
        let at = slot as usize * 2 * self.widest_key;
        &self.keys[at..at + self.slots[slot as usize].start_length as usize]
    }

    /// Read a slot's end key
    pub fn end(&self, slot: u16) -> &[u8] {
        let at = (slot as usize * 2 + 1) * self.widest_key;
        &self.keys[at..at + self.slots[slot as usize].end_length as usize]
    }

    /// Order a position's cover against a column and key
    pub fn compare(&self, position: usize, column: ColumnId, key: &[u8]) -> KeyOrder {
        let slot = self.order[position];
        self.slots[slot as usize].column.cmp(&column).then_with(|| self.start(slot).cmp(key))
    }

    /// Find the first position past every cover that starts at or before a key
    pub fn past_start(&self, column: ColumnId, key: &[u8]) -> usize {
        let mut low = 0;
        let mut high = self.order.len();
        while low < high {
            let middle = (low + high) / 2;
            if self.compare(middle, column, key) == KeyOrder::Greater {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        low
    }

    /// Find the first position whose cover starts at or after a key
    pub fn at_start(&self, column: ColumnId, key: &[u8]) -> usize {
        let mut low = 0;
        let mut high = self.order.len();
        while low < high {
            let middle = (low + high) / 2;
            if self.compare(middle, column, key) == KeyOrder::Less {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        low
    }

    /// Find the newest cover over a key a reader at a place can see. The walk back stops where no earlier cover
    /// reaches the key, and steps count the covers it looked at
    pub fn newest_over(
        &self,
        column: ColumnId,
        key: &[u8],
        read_point: WritePlace,
        steps: &mut usize,
    ) -> Option<WritePlace> {
        let mut newest = None;
        let mut position = self.past_start(column, key);
        while position > 0 {
            let slot = self.order[position - 1];
            *steps += 1;
            if self.slots[slot as usize].column != column || self.end(self.reach[position - 1]) <= key {
                break;
            }
            if position.is_multiple_of(COVER_BLOCK) && self.end(self.block_reach[position / COVER_BLOCK - 1]) <= key {
                position -= COVER_BLOCK;
                continue;
            }
            position -= 1;

            let place = self.slots[slot as usize].place;
            if key < self.end(slot) && place <= read_point {
                newest = newest.max(Some(place));
            }
        }
        newest
    }

    /// Check a cover at this place or newer already holds this one, as one found again at open can be
    pub fn is_inside_newer(&self, cover: &Cover) -> bool {
        let mut position = self.past_start(cover.column, cover.start);
        while position > 0 {
            position -= 1;
            let slot = self.order[position];
            if self.slots[slot as usize].column != cover.column || self.end(self.reach[position]) <= cover.start {
                return false;
            }
            if self.slots[slot as usize].place >= cover.place && cover.end <= self.end(slot) {
                return true;
            }
        }
        false
    }

    /// Check a newer cover in the same segment holds this one, so the footer that keeps the newer needs no other
    pub fn has_newer_in_segment(&self, cover: &Cover) -> bool {
        let mut position = self.past_start(cover.column, cover.start);
        while position > 0 {
            position -= 1;
            let slot = self.order[position];
            let found = self.slots[slot as usize];
            if found.column != cover.column || self.end(self.reach[position]) <= cover.start {
                return false;
            }
            if found.place > cover.place && found.segment == cover.segment && cover.end <= self.end(slot) {
                return true;
            }
        }
        false
    }

    /// Check an older cover lies wholly inside a new one
    pub fn has_inside(&self, cover: &Cover) -> bool {
        let mut position = self.at_start(cover.column, cover.start);
        while position < self.order.len() {
            let slot = self.order[position];
            if self.slots[slot as usize].column != cover.column || self.start(slot) >= cover.end {
                return false;
            }
            if self.end(slot) <= cover.end && self.slots[slot as usize].place < cover.place {
                return true;
            }
            position += 1;
        }
        false
    }

    /// Drop every older cover wholly inside a new one, whose place and range say all they said
    pub fn drop_inside(&mut self, cover: &Cover) {
        let mut position = self.at_start(cover.column, cover.start);
        let first = position;
        let mut has_dropped = false;
        while position < self.order.len() {
            let slot = self.order[position];
            if self.slots[slot as usize].column != cover.column || self.start(slot) >= cover.end {
                break;
            }
            if self.end(slot) <= cover.end && self.slots[slot as usize].place < cover.place {
                self.order.remove(position);
                self.reach.remove(position);
                self.free.push(slot);
                has_dropped = true;
            } else {
                position += 1;
            }
        }
        if has_dropped {
            self.rebuild_reach(first);
            self.rebuild_blocks(first);
        }
    }

    /// Set the reach of every block from the one holding a position on
    pub fn rebuild_blocks(&mut self, from: usize) {
        let first = from / COVER_BLOCK;
        self.block_reach.truncate(first);
        for block in first..self.order.len().div_ceil(COVER_BLOCK) {
            let end = ((block + 1) * COVER_BLOCK).min(self.order.len());
            let mut furthest = self.order[block * COVER_BLOCK];
            for position in block * COVER_BLOCK..end {
                if self.end(self.order[position]) > self.end(furthest) {
                    furthest = self.order[position];
                }
            }
            self.block_reach.push(furthest);
        }
    }

    /// Set every reach of a column from a position on, after covers left it
    pub fn rebuild_reach(&mut self, from: usize) {
        for position in from..self.order.len() {
            self.reach[position] = self.reach_at(position, self.order[position]);
        }
    }

    /// Pick whichever reaches further at a position: its own cover, or the one before it in the column
    pub fn reach_at(&self, position: usize, slot: u16) -> u16 {
        let Some(before) = position.checked_sub(1) else {
            return slot;
        };
        let earlier = self.reach[before];
        let is_same_column = self.slots[self.order[before] as usize].column == self.slots[slot as usize].column;
        if is_same_column && self.end(earlier) > self.end(slot) { earlier } else { slot }
    }

    /// Put a cover in its place in the order, or return false when no slot is left or its keys don't fit
    pub fn insert(&mut self, cover: &Cover) -> bool {
        if cover.start.len() > self.widest_key || cover.end.len() > self.widest_key {
            return false;
        }
        let Some(slot) = self.free.pop() else {
            return false;
        };

        self.slots[slot as usize] = CoverSlot {
            column: cover.column,
            start_length: cover.start.len() as u16,
            end_length: cover.end.len() as u16,
            place: cover.place,
            segment: cover.segment,
        };
        let start_at = slot as usize * 2 * self.widest_key;
        self.keys[start_at..start_at + cover.start.len()].copy_from_slice(cover.start);
        let end_at = start_at + self.widest_key;
        self.keys[end_at..end_at + cover.end.len()].copy_from_slice(cover.end);

        let position = self.past_start(cover.column, cover.start);
        self.order.insert(position, slot);
        self.reach.insert(position, slot);
        self.reach[position] = self.reach_at(position, slot);
        self.rebuild_blocks(position);
        // a later position only changes while this cover reaches past what it had
        for later in position + 1..self.order.len() {
            let other = self.order[later];
            if self.slots[other as usize].column != cover.column || self.end(self.reach[later]) >= cover.end {
                break;
            }
            self.reach[later] = slot;
        }
        true
    }
}

impl RangeCovers {
    /// Allocate a pool for keys up to a width
    pub fn new(widest_key: usize) -> Self {
        Self {
            count: AtomicUsize::new(0),
            pool: RwLock::new(CoverPool::new(widest_key)),
            longest_walk: AtomicUsize::new(0),
        }
    }

    /// Read the pool
    pub fn read(&self) -> RwLockReadGuard<'_, CoverPool> {
        match self.pool.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Change the pool
    pub fn write(&self) -> RwLockWriteGuard<'_, CoverPool> {
        match self.pool.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Count the pending covers
    pub fn len(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    /// Check no cover is pending
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Count the bytes every cover would take in a footer
    pub fn footer_room(&self) -> usize {
        MOST_RANGE_COVERS * (COVER_RECORD_BYTES + 2 * self.read().widest_key)
    }

    /// Read the heap the pool holds
    pub fn heap_bytes(&self) -> ByteCount {
        let pool = self.read();
        let slots = pool.slots.capacity() * size_of::<CoverSlot>();
        let lists = pool.free.capacity() + pool.order.capacity() + pool.reach.capacity() + pool.block_reach.capacity();
        let lists = lists * size_of::<u16>();
        ByteCount((slots + lists + pool.keys.capacity()) as u64)
    }

    /// Find the newest cover over a key a reader at a place can see, which costs one load with none pending
    pub fn newest_over(&self, column: ColumnId, key: &[u8], read_point: WritePlace) -> Option<WritePlace> {
        if self.is_empty() {
            return None;
        }
        let mut steps = 0;
        let newest = self.read().newest_over(column, key, read_point, &mut steps);
        self.note_walk(steps);
        newest
    }

    /// Keep the longest walk a search has made, for a report to show
    pub fn note_walk(&self, steps: usize) {
        if steps > self.longest_walk.load(Ordering::Relaxed) {
            self.longest_walk.fetch_max(steps, Ordering::Relaxed);
        }
    }

    /// Read the longest walk a search has made
    pub fn longest_walk(&self) -> usize {
        self.longest_walk.load(Ordering::Relaxed)
    }

    /// Count the covers a search for a key looks at
    pub fn search_steps(&self, column: ColumnId, key: &[u8]) -> usize {
        let mut steps = 0;
        let _newest = self.read().newest_over(column, key, WritePlace(u64::MAX), &mut steps);
        steps
    }

    /// Check a newer cover in the same segment holds this one
    pub fn has_newer_in_segment(&self, cover: &Cover) -> bool {
        self.read().has_newer_in_segment(cover)
    }

    /// Hold slots for covers about to land, or hold nothing when the pool can't take them
    pub fn hold(&self, count: usize) -> bool {
        let mut pool = self.write();
        if pool.free.len() < pool.held + count {
            return false;
        }
        pool.held += count;
        true
    }

    /// Give back slots held for covers that won't land
    pub fn release(&self, count: usize) {
        self.write().held -= count;
    }

    /// Put in a cover a hold made room for. Older covers inside it stay until `drop_inside`, once it's published
    pub fn add_held(&self, cover: &Cover) {
        let mut pool = self.write();
        pool.held -= 1;
        pool.insert(cover);
        let count = pool.order.len();
        drop(pool);
        self.count.store(count, Ordering::Release);
    }

    /// Check an older cover lies wholly inside a new one
    pub fn has_inside(&self, cover: &Cover) -> bool {
        self.read().has_inside(cover)
    }

    /// Copy out the cover at a position in key order, or return false past the end
    pub fn cover_at(&self, position: usize, start: &mut Vec<u8>, end: &mut Vec<u8>) -> Option<(ColumnId, WritePlace, SegmentNumber)> {
        let pool = self.read();
        let slot = *pool.order.get(position)?;
        start.clear();
        start.extend_from_slice(pool.start(slot));
        end.clear();
        end.extend_from_slice(pool.end(slot));
        let fields = &pool.slots[slot as usize];
        Some((fields.column, fields.place, fields.segment))
    }

    /// Drop the older covers inside a published one. A get reads its read point under the lock, so none can see
    /// a place below the newer cover once they've gone
    pub fn drop_inside(&self, cover: &Cover) {
        let mut pool = self.write();
        pool.drop_inside(cover);
        let count = pool.order.len();
        drop(pool);
        self.count.store(count, Ordering::Release);
    }

    /// Put in a cover open found, or return false when the pool has no slot for it
    pub fn add(&self, cover: &Cover) -> bool {
        let mut pool = self.write();
        pool.drop_inside(cover);
        // no writer runs at open, so nothing above this cover can still be unpublished
        let has_room = pool.is_inside_newer(cover) || pool.insert(cover);
        let count = pool.order.len();
        drop(pool);
        self.count.store(count, Ordering::Release);
        has_room
    }

    /// Find the newest cover over a key at the newest read point, reading that point under the lock
    pub fn newest_now(
        &self,
        column: ColumnId,
        key: &[u8],
        read_point: impl Fn() -> WritePlace,
    ) -> (Option<WritePlace>, WritePlace) {
        if self.is_empty() {
            return (None, read_point());
        }
        let pool = self.read();
        let point = read_point();
        let mut steps = 0;
        let newest = pool.newest_over(column, key, point, &mut steps);
        drop(pool);
        self.note_walk(steps);
        (newest, point)
    }
}

/// Add a cover to a footer's block
pub fn put_cover(bytes: &mut Vec<u8>, cover: &Cover) {
    bytes.push(cover.column.0);
    bytes.extend_from_slice(&(cover.start.len() as u16).to_le_bytes());
    bytes.extend_from_slice(&(cover.end.len() as u16).to_le_bytes());
    bytes.extend_from_slice(&cover.place.0.to_le_bytes());
    bytes.extend_from_slice(cover.start);
    bytes.extend_from_slice(cover.end);
}

/// Read the covers of a footer's block, or return false when it's cut short
pub fn read_covers(mut bytes: &[u8], segment: SegmentNumber, mut each: impl FnMut(&Cover)) -> bool {
    while !bytes.is_empty() {
        let Some(head) = bytes.get(..COVER_RECORD_BYTES) else {
            return false;
        };
        let start_length = u16::from_le_bytes([head[1], head[2]]) as usize;
        let end_length = u16::from_le_bytes([head[3], head[4]]) as usize;
        let Some(place) = read_word(head, 5) else {
            return false;
        };
        let keys_end = COVER_RECORD_BYTES + start_length + end_length;
        let Some(keys) = bytes.get(COVER_RECORD_BYTES..keys_end) else {
            return false;
        };
        each(&Cover {
            column: ColumnId(head[0]),
            start: &keys[..start_length],
            end: &keys[start_length..],
            place: WritePlace(place),
            segment,
        });
        bytes = &bytes[keys_end..];
    }
    true
}
