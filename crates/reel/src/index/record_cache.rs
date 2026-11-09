//! A cache of records' values in reel's own memory, so a hot get reads no file and makes no syscall
//!
//! Each shard writes values into a fixed arena as a log and evicts from its tail. A value read while it sits in the oldest
//! quarter of the log moves back to the head, so a hot record stays, and a value goes in only on its second miss, so a scan
//! leaves the hot set in place. A record never changes where it sits, so an entry can't go stale: a merge that moves a record
//! leaves the old entry to age out.

use std::sync::Mutex;

use reel_api::units::{ByteCount, FileOffset};

use crate::guard::hold;
use crate::pool::touched_filled;
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

/// One shard: the arena, its log positions, the index and the admission counts
#[derive(Debug)]
pub struct CacheShard {
    arena: Vec<u8>,
    head: u64,
    tail: u64,
    slots: Vec<CacheEntry>,
    used: usize,
    seen: Vec<u8>,
}

/// The cache over every shard
#[derive(Debug)]
pub struct RecordCache {
    shards: Vec<Mutex<CacheShard>>,
    shard_bytes: usize,
}

/// Mix a record's place into a hash
pub fn place_hash(segment: u64, offset: u64) -> u64 {
    let mixed = (segment ^ offset.rotate_left(29)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    mixed ^ (mixed >> 31)
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
    /// Allocate and touch a shard with an arena of some bytes
    pub fn new(bytes: usize) -> Self {
        let slots = (bytes / BYTES_PER_INDEX_SLOT).next_power_of_two().max(16);
        Self {
            arena: touched_filled(bytes, 0),
            head: 0,
            tail: 0,
            slots: touched_filled(slots, CacheEntry::empty()),
            used: 0,
            seen: touched_filled(slots, 0),
        }
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
        bytes.copy_from_slice(&self.arena[at..at + 8]);
        u64::from_le_bytes(bytes)
    }

    /// Write a word of the arena
    fn set_word(&mut self, at: usize, word: u64) {
        self.arena[at..at + 8].copy_from_slice(&word.to_le_bytes());
    }

    /// Drop the oldest thing in the log: a skip at the arena's end, or a value and its index slot
    fn evict_one(&mut self) {
        let capacity = self.arena.len() as u64;
        let physical = (self.tail % capacity) as usize;
        let left = self.arena.len() - physical;
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

    /// Make room at the head for an entry of some bytes, skipping the arena's end when the entry won't fit before it
    fn make_room(&mut self, need: usize) {
        let capacity = self.arena.len() as u64;
        let physical = (self.head % capacity) as usize;
        let left = self.arena.len() - physical;
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
    }

    /// Write a key and its value at the head and list them
    fn append(&mut self, segment: u64, offset: u64, key: &[u8], value: &[u8]) {
        let need = ENTRY_HEADER_BYTES + padded(key.len() + value.len());
        self.make_room(need);
        let physical = (self.head % self.arena.len() as u64) as usize;
        self.set_word(physical, segment);
        self.set_word(physical + 8, offset);
        self.set_word(physical + 16, (value.len() as u64) << KEY_LENGTH_BITS | key.len() as u64);
        let start = physical + ENTRY_HEADER_BYTES;
        self.arena[start..start + key.len()].copy_from_slice(key);
        self.arena[start + key.len()..start + key.len() + value.len()].copy_from_slice(value);
        self.add_slot(CacheEntry {
            segment,
            offset,
            at: self.head + ENTRY_HEADER_BYTES as u64,
            key_length: key.len() as u16,
            length: value.len() as u32,
        });
        self.head += need as u64;
    }

    /// Copy a cached value's bytes from a start into a buffer when the record there is this key's, and move it back to the head
    /// when it sits near the tail
    pub fn read(&mut self, segment: u64, offset: u64, key: &[u8], start: usize, length: Option<usize>, out: &mut Vec<u8>) -> bool {
        let Some(slot) = self.find(segment, offset) else {
            return false;
        };
        let entry = self.slots[slot];
        let capacity = self.arena.len() as u64;
        let physical = (entry.at % capacity) as usize;
        let key_length = usize::from(entry.key_length);
        // a place another key's lookup led to answers nothing
        if self.arena[physical..physical + key_length] != *key {
            return false;
        }
        let value_at = physical + key_length;
        let end = entry.length as usize;
        let from = start.min(end);
        let to = length.map_or(end, |length| (from + length).min(end));
        out.clear();
        out.extend_from_slice(&self.arena[value_at + from..value_at + to]);
        // the oldest part of the log goes first, and a value read there moves to the head and stays
        if entry.at - self.tail < capacity / PROMOTE_PART {
            let mut moved = [0u8; MOST_CACHED_BYTES];
            moved[..end].copy_from_slice(&self.arena[value_at..value_at + end]);
            self.remove_slot(slot);
            self.append(segment, offset, key, &moved[..end]);
        }
        true
    }

    /// Take a key and its value in when this is its second miss, and note its first
    pub fn offer(&mut self, segment: u64, offset: u64, key: &[u8], value: &[u8]) {
        let stored = key.len() + value.len();
        if value.len() > MOST_CACHED_BYTES || key.len() > MOST_CACHED_KEY_BYTES || ENTRY_HEADER_BYTES + padded(stored) > self.arena.len() / 4 {
            return;
        }
        if self.find(segment, offset).is_some() {
            return;
        }
        let count = place_hash(segment, offset) as usize & (self.seen.len() - 1);
        if self.seen[count] == 0 {
            self.seen[count] = 1;
            return;
        }
        self.seen[count] = 0;
        self.append(segment, offset, key, value);
    }

    /// Count the values the shard holds
    pub fn value_count(&self) -> usize {
        self.used
    }
}

impl RecordCache {
    /// Allocate and touch a cache of some bytes across every shard
    pub fn new(bytes: ByteCount) -> Self {
        let shard_bytes = (bytes.0 as usize / CACHE_SHARDS).max(LEAST_SHARD_BYTES).next_multiple_of(8);
        let shards: Vec<_> = (0..CACHE_SHARDS).map(|_shard| Mutex::new(CacheShard::new(shard_bytes))).collect();
        // a lock can build its state on first use, so each takes it here, at open
        for shard in &shards {
            drop(hold(shard));
        }
        Self { shards, shard_bytes }
    }

    /// Pick a record's shard
    fn shard(&self, segment: SegmentNumber, offset: FileOffset) -> &Mutex<CacheShard> {
        let hash = place_hash(segment.0, offset.0);
        &self.shards[(hash >> 58) as usize % CACHE_SHARDS]
    }

    /// Copy a key's cached value into a buffer, and say whether the cache held it
    pub fn get(&self, segment: SegmentNumber, offset: FileOffset, key: &[u8], out: &mut Vec<u8>) -> bool {
        hold(self.shard(segment, offset)).read(segment.0, offset.0, key, 0, None, out)
    }

    /// Copy a window of a key's cached value into a buffer, and say whether the cache held it
    pub fn get_window(&self, segment: SegmentNumber, offset: FileOffset, key: &[u8], start: ByteCount, length: ByteCount, out: &mut Vec<u8>) -> bool {
        hold(self.shard(segment, offset)).read(segment.0, offset.0, key, start.0 as usize, Some(length.0 as usize), out)
    }

    /// Offer a key's value a read just checked
    pub fn offer(&self, segment: SegmentNumber, offset: FileOffset, key: &[u8], value: &[u8]) {
        hold(self.shard(segment, offset)).offer(segment.0, offset.0, key, value);
    }

    /// Read the heap the cache holds
    pub fn heap_bytes(&self) -> ByteCount {
        let slots = self.shards.first().map_or(0, |shard| {
            let shard = hold(shard);
            shard.slots.len() * size_of::<CacheEntry>() + shard.seen.len()
        });
        ByteCount(((self.shard_bytes + slots) * CACHE_SHARDS) as u64)
    }

    /// Count the values the cache holds
    pub fn value_count(&self) -> usize {
        self.shards.iter().map(|shard| hold(shard).value_count()).sum()
    }
}
