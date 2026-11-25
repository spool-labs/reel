//! A cache of records' values in reel's own memory, so a hot get reads no file and makes no syscall
//!
//! Each shard writes values into a fixed arena as a log and evicts from its tail. A value read while it sits in the oldest
//! quarter of the log moves back to the head, so a hot record stays, and a value goes in only on its second miss, so a scan
//! leaves the hot set in place. A record never changes where it sits, so an entry can't go stale: a merge that moves a record
//! leaves the old entry to age out.

use std::io::Result;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use reel_core::units::{ByteCount, FileOffset};

use crate::engine::caller_activity::CallerCounts;
use crate::guard::hold;
use crate::pool::touched_filled;
use crate::pool_room::PoolRoom;
use crate::segment::segment_tail::SegmentNumber;

/// Shards of the cache, each behind its own lock
pub const CACHE_SHARDS: usize = 64;
/// The largest value the cache holds. Bigger values stay in the page cache alone
pub const MOST_CACHED_BYTES: usize = 16 * 1024;
/// Bytes of the header in front of each key and value in the arena
pub const ENTRY_HEADER_BYTES: usize = 24;
/// The longest key the cache holds a value for
pub const MOST_CACHED_KEY_BYTES: usize = 1024;
/// Bits of the header's length word the key's length takes, under the value's
pub const KEY_LENGTH_BITS: u32 = 16;
/// The key's length in the header's length word
pub const KEY_LENGTH_MASK: u64 = (1 << KEY_LENGTH_BITS) - 1;
/// Arena bytes a shard plans an index slot for
pub const BYTES_PER_INDEX_SLOT: usize = 48;
/// Index slots a shard plans for each mean value its arena holds, a margin past the load at which it evicts
pub const SLOTS_PER_VALUE: usize = 4;
/// The fewest admission counts a shard keeps
pub const LEAST_SEEN_COUNTS: usize = 64;
/// A segment number no record has, which marks an empty index slot or a skip in the arena
pub const NO_RECORD: u64 = u64::MAX;
/// A value read in the last part in this many of the log moves back to the head
pub const PROMOTE_PART: u64 = 4;
/// The smallest arena a shard gets, so a tiny budget still holds a few small values
pub const LEAST_SHARD_BYTES: usize = 4 * 1024;

/// Where one cached value sits
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheEntry {
    /// The record's segment, or `NO_RECORD` for an empty slot
    pub segment: u64,
    /// The record's offset in its segment
    pub offset: u64,
    /// The key's first byte, as a position in the log, with the value right after it
    pub at: u64,
    /// The key's length
    pub key_length: u16,
    /// The value's length
    pub length: u32,
}

/// One shard: the arena, its log positions, the index and the admission counts. The arena grows into its room until the log
/// first wraps, and keeps its size from then on
#[derive(Debug)]
pub struct CacheShard {
    room: Arc<PoolRoom>,
    /// The arena bytes the shard plans for, which it grows to
    share: usize,
    /// The arena bytes the log runs over now
    capacity: usize,
    is_wrapped: bool,
    head: u64,
    tail: u64,
    slots: Vec<CacheEntry>,
    used: usize,
    seen: Vec<u8>,
}

/// What a cache read found
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheLookup {
    /// The value is in the buffer
    Hit,
    /// The cache lacks it, and the value goes in once read when this miss was its second
    Miss { is_admitted: bool },
}

/// The cache over every shard
#[derive(Debug)]
pub struct RecordCache {
    shards: Vec<Mutex<CacheShard>>,
    /// Reads the cache answered, in a slot for each thread
    pub hits: CallerCounts,
    /// Reads it missed
    pub misses: CallerCounts,
    /// Values it took in on a miss
    pub admits: CallerCounts,
    /// The shard a give-back starts from, which moves each time
    next_to_give: AtomicUsize,
}

/// Mix a record's place into a hash
pub fn place_hash(segment: u64, offset: u64) -> u64 {
    let mut hash = segment ^ offset.rotate_left(29);
    hash = (hash ^ (hash >> 33)).wrapping_mul(0xFF51_AFD7_ED55_8CCD);
    hash = (hash ^ (hash >> 33)).wrapping_mul(0xC4CE_B9FE_1A85_EC53);
    hash ^ (hash >> 33)
}

/// Round a length up to whole words, so every header starts aligned
pub fn padded(length: usize) -> usize {
    length.next_multiple_of(8)
}

impl CacheEntry {
    /// Build an empty slot
    pub const fn empty() -> Self {
        Self { segment: NO_RECORD, offset: 0, at: 0, key_length: 0, length: 0 }
    }
}

impl CacheShard {
    /// Allocate and touch a shard whose arena, index and admission counts together take some bytes. A store of wide records
    /// plans its index for the values its arena holds and keeps a count a value, so a first miss is soon forgotten and only a
    /// record missed twice close together comes in. Small records, or a store with none on disk, plan a slot for each stretch
    ///
    /// A whole shard brings its arena into memory at once, and any other starts at the room's first step and grows into the rest
    pub fn growing(bytes: usize, mean_record: Option<u64>, is_whole: bool) -> Result<Self> {
        let entry = mean_record.map_or(0, |bytes| padded(ENTRY_HEADER_BYTES + bytes as usize));
        let stretch = (entry / SLOTS_PER_VALUE).max(BYTES_PER_INDEX_SLOT);
        let is_wide = stretch > BYTES_PER_INDEX_SLOT;
        let seen_per_slot = if is_wide { 0 } else { 1 };
        // the index and its counts come out of the shard's bytes, one slot planned for each stretch of arena
        let slot_cost = size_of::<CacheEntry>() + seen_per_slot;
        let planned = bytes / (stretch + slot_cost);
        let slots = if planned < 16 { 16 } else { 1 << planned.ilog2() };
        let values = bytes / entry.max(1);
        let seen = if is_wide { values.next_power_of_two().max(LEAST_SEEN_COUNTS) } else { slots };
        let arena = bytes.saturating_sub(slots * size_of::<CacheEntry>() + seen).max(LEAST_SHARD_BYTES) / 8 * 8;
        let room = PoolRoom::reserve(ByteCount(arena as u64), ByteCount(if is_whole { arena as u64 } else { 0 }))?;
        let mut shard = Self {
            room: Arc::new(room),
            share: arena,
            capacity: 0,
            is_wrapped: false,
            head: 0,
            tail: 0,
            slots: touched_filled(slots, CacheEntry::empty()),
            used: 0,
            seen: touched_filled(seen, 0),
        };
        shard.follow_room();
        Ok(shard)
    }

    /// Run the log over every arena byte in memory, until the log first wraps
    fn follow_room(&mut self) {
        if !self.is_wrapped {
            self.capacity = (self.room.committed().min(self.share).min(self.room.cap()) / 8 * 8).max(ENTRY_HEADER_BYTES);
        }
    }

    /// Check the room can still grow, so a full log waits for its next part
    fn can_grow(&self) -> bool {
        self.capacity < self.share && self.room.can_grow()
    }

    /// Read the arena's bytes the log runs over
    fn arena(&self) -> &[u8] {
        // SAFETY: the capacity lies under the room's limit, and the growing job only writes past it
        unsafe { std::slice::from_raw_parts(self.room.base(), self.capacity) }
    }

    /// Write the arena's bytes the log runs over
    fn arena_mut(&mut self) -> &mut [u8] {
        // SAFETY: as in arena, and the shard's own borrow is unique
        unsafe { std::slice::from_raw_parts_mut(self.room.base(), self.capacity) }
    }

    /// Drop every value and start the log over within this many bytes, so the room past them can go back
    fn start_over(&mut self, bytes: usize) {
        self.slots.fill(CacheEntry::empty());
        self.used = 0;
        self.head = 0;
        self.tail = 0;
        self.capacity = (bytes.min(self.share) / 8 * 8).max(ENTRY_HEADER_BYTES);
        // a log started over grows again once its cap rises
        self.is_wrapped = false;
        self.room.note_used(0);
    }

    /// Read the arena's room, which the growing job raises
    pub fn room(&self) -> &Arc<PoolRoom> {
        &self.room
    }

    /// Read the heap the shard holds: the arena in memory, the index and the counts
    pub fn heap_bytes(&self) -> usize {
        self.room.committed().min(self.room.reserved()) + self.slots.len() * size_of::<CacheEntry>() + self.seen.len()
    }

    /// Find a record's index slot
    fn find(&self, segment: u64, offset: u64) -> Option<usize> {
        let mask = self.slots.len() - 1;
        let mut at = place_hash(segment, offset) as usize & mask;
        for _step in 0..self.slots.len() {
            let entry = &self.slots[at];
            if entry.segment == NO_RECORD {
                return None;
            }
            if entry.segment == segment && entry.offset == offset {
                return Some(at);
            }
            at = (at + 1) & mask;
        }
        None
    }

    /// Take a slot out of the index and close the gap behind it, so every probe still reaches its record
    fn remove_slot(&mut self, mut gap: usize) {
        let mask = self.slots.len() - 1;
        let mut next = (gap + 1) & mask;
        while self.slots[next].segment != NO_RECORD {
            let home = place_hash(self.slots[next].segment, self.slots[next].offset) as usize & mask;
            // a slot moves into the gap when its home sits at or before the gap along its probe
            if (next.wrapping_sub(home) & mask) >= (next.wrapping_sub(gap) & mask) {
                self.slots[gap] = self.slots[next];
                gap = next;
            }
            next = (next + 1) & mask;
        }
        self.slots[gap] = CacheEntry::empty();
        self.used -= 1;
    }

    /// Put an entry in the index, which has room since the caller made it
    fn add_slot(&mut self, entry: CacheEntry) {
        let mask = self.slots.len() - 1;
        let mut at = place_hash(entry.segment, entry.offset) as usize & mask;
        while self.slots[at].segment != NO_RECORD {
            at = (at + 1) & mask;
        }
        self.slots[at] = entry;
        self.used += 1;
    }

    /// Read a word of the arena
    fn word(&self, at: usize) -> u64 {
        let mut bytes = [0; 8];
        bytes.copy_from_slice(&self.arena()[at..at + 8]);
        u64::from_le_bytes(bytes)
    }

    /// Write a word of the arena
    fn set_word(&mut self, at: usize, word: u64) {
        self.arena_mut()[at..at + 8].copy_from_slice(&word.to_le_bytes());
    }

    /// Drop the oldest thing in the log: a skip at the arena's end, or a value and its index slot
    fn evict_one(&mut self) {
        let capacity = self.capacity as u64;
        let physical = (self.tail % capacity) as usize;
        let left = self.capacity - physical;
        if left < ENTRY_HEADER_BYTES || self.word(physical) == NO_RECORD {
            self.tail += left as u64;
            return;
        }
        let (segment, offset) = (self.word(physical), self.word(physical + 8));
        let lengths = self.word(physical + 16);
        let at = self.tail + ENTRY_HEADER_BYTES as u64;
        // a value that moved to the head left this copy behind, and its slot points at the newer one
        if let Some(slot) = self.find(segment, offset)
            && self.slots[slot].at == at
        {
            self.remove_slot(slot);
        }
        let stored = (lengths & KEY_LENGTH_MASK) + (lengths >> KEY_LENGTH_BITS);
        self.tail += (ENTRY_HEADER_BYTES + padded(stored as usize)) as u64;
    }

    /// Make room at the head for an entry of some bytes, skipping the arena's end when the entry won't fit before it, and say
    /// whether it did. Until the log first wraps it evicts nothing: a full log that can still grow takes nothing in until its
    /// next part comes, and one that can't grows no further and wraps
    fn make_room(&mut self, need: usize) -> bool {
        self.follow_room();
        if need.saturating_mul(4) > self.capacity {
            return false;
        }
        let is_index_full = self.used * 4 >= self.slots.len() * 3;
        let is_arena_full = self.head + need as u64 > self.capacity as u64;
        if !self.is_wrapped && (is_arena_full || is_index_full) {
            if is_arena_full && !is_index_full && self.can_grow() {
                return false;
            }
            // the log keeps this size from here, so the job grows it no further
            self.is_wrapped = true;
            self.room.note_used(0);
        }
        let capacity = self.capacity as u64;
        let physical = (self.head % capacity) as usize;
        let left = self.capacity - physical;
        let skip = if left < need { left } else { 0 };
        while capacity - (self.head - self.tail) < (skip + need) as u64 || self.used * 4 >= self.slots.len() * 3 {
            self.evict_one();
        }
        if skip > 0 {
            if skip >= ENTRY_HEADER_BYTES {
                self.set_word(physical, NO_RECORD);
            }
            self.head += skip as u64;
        }
        true
    }

    /// Write a key and its value at the head and list them
    fn append(&mut self, segment: u64, offset: u64, key: &[u8], value: &[u8]) {
        let need = ENTRY_HEADER_BYTES + padded(key.len() + value.len());
        if !self.make_room(need) {
            return;
        }
        let physical = (self.head % self.capacity as u64) as usize;
        self.set_word(physical, segment);
        self.set_word(physical + 8, offset);
        self.set_word(physical + 16, (value.len() as u64) << KEY_LENGTH_BITS | key.len() as u64);
        let start = physical + ENTRY_HEADER_BYTES;
        self.arena_mut()[start..start + key.len()].copy_from_slice(key);
        self.arena_mut()[start + key.len()..start + key.len() + value.len()].copy_from_slice(value);
        self.add_slot(CacheEntry {
            segment,
            offset,
            at: self.head + ENTRY_HEADER_BYTES as u64,
            key_length: key.len() as u16,
            length: value.len() as u32,
        });
        self.head += need as u64;
        if !self.is_wrapped {
            self.room.note_used(self.head as usize);
        }
    }

    /// Copy a cached value into a buffer, or note a whole read's miss
    pub fn read(&mut self, segment: u64, offset: u64, key: &[u8], start: usize, length: Option<usize>, out: &mut Vec<u8>) -> CacheLookup {
        self.read_onto(segment, offset, key, start, length, out, 0)
    }

    /// Copy a cached value into a buffer from a place in it on, keeping what comes before, or note a whole read's miss
    #[allow(clippy::too_many_arguments, reason = "a record's place and key, the part of its value, and where in the buffer it goes")]
    pub fn read_onto(&mut self, segment: u64, offset: u64, key: &[u8], start: usize, length: Option<usize>, out: &mut Vec<u8>, at: usize) -> CacheLookup {
        let Some((slot, entry)) = self.find(segment, offset).map(|slot| (slot, self.slots[slot])) else {
            return self.note_miss(segment, offset, length);
        };
        let capacity = self.capacity as u64;
        let physical = (entry.at % capacity) as usize;
        let key_length = usize::from(entry.key_length);
        // a place another key's lookup led to answers nothing
        if self.arena()[physical..physical + key_length] != *key {
            return self.note_miss(segment, offset, length);
        }
        let value_at = physical + key_length;
        let end = entry.length as usize;
        let from = start.min(end);
        let to = length.map_or(end, |length| (from + length).min(end));
        out.truncate(at);
        out.extend_from_slice(&self.arena()[value_at + from..value_at + to]);
        // a whole read near the tail moves to the head from the caller's copy, since making room can evict the source
        if length.is_none() && from == 0 && entry.at - self.tail < capacity / PROMOTE_PART {
            self.remove_slot(slot);
            self.append(segment, offset, key, &out[at..]);
        }
        CacheLookup::Hit
    }

    /// Note a whole read's miss, and say whether it is the record's second
    fn note_miss(&mut self, segment: u64, offset: u64, length: Option<usize>) -> CacheLookup {
        if length.is_some() {
            return CacheLookup::Miss { is_admitted: false };
        }
        let hash = place_hash(segment, offset);
        let count = hash as usize & (self.seen.len() - 1);
        let print = (hash >> 24) as u8 | 1;
        let is_admitted = self.seen[count] == print;
        self.seen[count] = if is_admitted { 0 } else { print };
        CacheLookup::Miss { is_admitted }
    }

    /// Take in a key and its value that a read admitted on its miss
    pub fn admit(&mut self, segment: u64, offset: u64, key: &[u8], value: &[u8]) {
        let stored = key.len() + value.len();
        if value.len() > MOST_CACHED_BYTES || key.len() > MOST_CACHED_KEY_BYTES || ENTRY_HEADER_BYTES + padded(stored) > self.share / 4 {
            return;
        }
        if self.find(segment, offset).is_some() {
            return;
        }
        self.append(segment, offset, key, value);
    }

    /// Count the values the shard holds
    pub fn value_count(&self) -> usize {
        self.used
    }
}

impl RecordCache {
    /// Read the least a record cache holds, whatever share it gets
    pub const fn least_bytes() -> u64 {
        (CACHE_SHARDS * 2 * LEAST_SHARD_BYTES) as u64
    }

    /// Build a cache of some bytes across every shard, all of it in memory
    pub fn new(bytes: ByteCount, mean_record: Option<u64>) -> Result<Self> {
        Self::growing(bytes, mean_record, true)
    }

    /// Build a cache whose arenas are whole in memory, or start small and grow into the rest
    pub fn growing(bytes: ByteCount, mean_record: Option<u64>, is_whole: bool) -> Result<Self> {
        let shard_bytes = (bytes.0 as usize / CACHE_SHARDS).max(2 * LEAST_SHARD_BYTES);
        let mut shards = Vec::with_capacity(CACHE_SHARDS);
        for _shard in 0..CACHE_SHARDS {
            shards.push(Mutex::new(CacheShard::growing(shard_bytes, mean_record, is_whole)?));
        }
        // a lock can build its state on first use, so each takes it here, at open
        for shard in &shards {
            drop(hold(shard));
        }
        Ok(Self { shards, hits: CallerCounts::default(), misses: CallerCounts::default(), admits: CallerCounts::default(), next_to_give: AtomicUsize::new(0) })
    }

    /// Build a cache whose arenas start at an eighth of their share
    pub fn fresh(bytes: ByteCount, mean_record: Option<u64>) -> Result<Self> {
        Self::growing(bytes, mean_record, false)
    }

    /// Give back at least this many bytes, a whole shard at a time: each one taken drops what it holds and keeps one extent, and its
    /// cap lifts with the others once the store holds well under its target. Returns the bytes given back
    pub fn give_back(&self, bytes: usize) -> usize {
        let count = self.shards.len();
        let first = self.next_to_give.fetch_add(1, Ordering::Relaxed);
        let mut given = 0;
        // whole shards go one at a time, from a start that moves each time, so a small cut empties a sixty-fourth of the cache and
        // the same keys don't always pay
        for turn in 0..count {
            if given >= bytes {
                break;
            }
            let shard = &self.shards[(first + turn) % count];
            let room = Arc::clone(hold(shard).room());
            if room.committed() <= room.extent() {
                continue;
            }
            room.set_cap(room.extent());
            given += self.shorten_shard(shard);
        }
        given
    }

    /// Shorten one shard to its cap, and return the bytes given back
    fn shorten_shard(&self, shard: &Mutex<CacheShard>) -> usize {
        let mut held = hold(shard);
        let cap = held.room.cap();
        if held.capacity <= cap {
            return 0;
        }
        held.start_over(cap);
        let room = Arc::clone(&held.room);
        drop(held);
        room.release_above(cap)
    }

    /// Read every shard's arena room, for the job that grows them
    pub fn rooms(&self) -> Vec<Arc<PoolRoom>> {
        self.shards.iter().map(|shard| hold(shard).room().clone()).collect()
    }

    /// Read the bytes the cache grows to: every arena's share, the index and the counts
    pub fn share_bytes(&self) -> ByteCount {
        let shards = self.shards.iter().map(|shard| {
            let shard = hold(shard);
            shard.share + shard.slots.len() * size_of::<CacheEntry>() + shard.seen.len()
        });
        ByteCount(shards.sum::<usize>() as u64)
    }

    /// Pick a record's shard
    fn shard(&self, segment: SegmentNumber, offset: FileOffset) -> &Mutex<CacheShard> {
        let hash = place_hash(segment.0, offset.0);
        &self.shards[(hash >> 58) as usize % CACHE_SHARDS]
    }

    /// Copy a key's cached value into a buffer, or note the miss
    pub fn get(&self, segment: SegmentNumber, offset: FileOffset, key: &[u8], out: &mut Vec<u8>) -> CacheLookup {
        let lookup = hold(self.shard(segment, offset)).read(segment.0, offset.0, key, 0, None, out);
        self.count(lookup);
        lookup
    }

    /// Add a key's cached value onto the end of a buffer, or note the miss and leave the buffer as it was
    pub fn get_onto(&self, segment: SegmentNumber, offset: FileOffset, key: &[u8], out: &mut Vec<u8>) -> CacheLookup {
        let at = out.len();
        let lookup = hold(self.shard(segment, offset)).read_onto(segment.0, offset.0, key, 0, None, out, at);
        self.count(lookup);
        lookup
    }

    /// Copy a window of a key's cached value into a buffer, and say whether the cache held it
    pub fn get_window(&self, segment: SegmentNumber, offset: FileOffset, key: &[u8], start: ByteCount, length: ByteCount, out: &mut Vec<u8>) -> CacheLookup {
        let lookup = hold(self.shard(segment, offset)).read(segment.0, offset.0, key, start.0 as usize, Some(length.0 as usize), out);
        self.count(lookup);
        lookup
    }

    /// Take in a value its read's miss admitted
    pub fn admit(&self, segment: SegmentNumber, offset: FileOffset, key: &[u8], value: &[u8]) {
        hold(self.shard(segment, offset)).admit(segment.0, offset.0, key, value);
        self.admits.note();
    }

    /// Count a lookup as a hit or a miss
    fn count(&self, lookup: CacheLookup) {
        match lookup {
            CacheLookup::Hit => self.hits.note(),
            CacheLookup::Miss { .. } => self.misses.note(),
        }
    }

    /// Read the heap the cache holds
    pub fn heap_bytes(&self) -> ByteCount {
        ByteCount(self.shards.iter().map(|shard| hold(shard).heap_bytes()).sum::<usize>() as u64)
    }

    /// Count the index slots every shard planned, which shows the layout its mean gave
    pub fn index_slots(&self) -> usize {
        self.shards.iter().map(|shard| hold(shard).slots.len()).sum()
    }

    /// Count the values the cache holds
    pub fn value_count(&self) -> usize {
        self.shards.iter().map(|shard| hold(shard).value_count()).sum()
    }
}
