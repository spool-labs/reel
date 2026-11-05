//! A cache of sealed live keys' locations, one slot per key, cold keys out first

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use reel_api::units::{ByteCount, FileOffset, WritePlace};

use crate::index::key_hash::KeyHash;
use crate::index::write_index::{SHARD_COUNT, shard_of};
use crate::segment::segment_tail::SegmentNumber;

/// Slots a key can land in
pub const BUCKET_SLOTS: usize = 4;
/// Marks an unused slot, which no segment number reaches
pub const NO_SEGMENT: u32 = u32::MAX;
/// Bits a tag starts above, clear of the bucket bits and the shard bits
pub const TAG_SHIFT: u32 = 28;
/// Bits in one word of the clock
pub const CLOCK_WORD_BITS: usize = u64::BITS as usize;

/// One key's location in a sealed segment
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SpotLocation {
    pub segment: SegmentNumber,
    pub offset: FileOffset,
    pub length: ByteCount,
    pub place: WritePlace,
}

/// One slot: a tag of the key's hash and where the record sits
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SpotSlot {
    pub tag: u32,
    pub segment: u32,
    pub offset: u32,
    pub length: u32,
    pub place: u64,
}

/// A shard's slots, fixed at open
#[derive(Debug)]
pub struct SpotShard {
    slots: Vec<SpotSlot>,
    bucket_mask: usize,
}

/// The cache, sharded by a hash of the key
#[derive(Debug)]
pub struct SpotIndex {
    shards: Vec<RwLock<SpotShard>>,
    clocks: Vec<Vec<AtomicU64>>,
}

impl SpotSlot {
    /// Build an unused slot
    pub fn empty() -> Self {
        Self {
            tag: 0,
            segment: NO_SEGMENT,
            offset: 0,
            length: 0,
            place: 0,
        }
    }

    /// Check whether the slot holds a key
    pub fn is_used(&self) -> bool {
        self.segment != NO_SEGMENT
    }

    /// Read the location back
    pub fn location(&self) -> SpotLocation {
        SpotLocation {
            segment: SegmentNumber(u64::from(self.segment)),
            offset: FileOffset(u64::from(self.offset)),
            length: ByteCount(u64::from(self.length)),
            place: WritePlace(self.place),
        }
    }
}

impl SpotShard {
    /// Allocate and touch a whole number of buckets
    pub fn new(buckets: usize) -> Self {
        Self {
            slots: vec![SpotSlot::empty(); buckets * BUCKET_SLOTS],
            bucket_mask: buckets - 1,
        }
    }

    /// Find the first slot of a key's bucket
    pub fn bucket_of(&self, hash: KeyHash) -> usize {
        (hash.0 as usize & self.bucket_mask) * BUCKET_SLOTS
    }

    /// Find the slot holding a key
    pub fn find(&self, hash: KeyHash) -> Option<usize> {
        let tag = tag_of(hash);
        let bucket = self.bucket_of(hash);
        for index in bucket..bucket + BUCKET_SLOTS {
            let slot = &self.slots[index];
            if slot.is_used() && slot.tag == tag {
                return Some(index);
            }
        }
        None
    }
}

impl SpotIndex {
    /// Allocate and touch slots that fill a share of the budget
    pub fn new(share: ByteCount) -> Self {
        let per_shard = share.0 as usize / SHARD_COUNT;
        let slot_bytes = size_of::<SpotSlot>() + 1;
        let fitting = (per_shard / (slot_bytes * BUCKET_SLOTS)).max(1);
        let buckets = if fitting.is_power_of_two() {
            fitting
        } else {
            fitting.next_power_of_two() / 2
        };

        let mut shards = Vec::with_capacity(SHARD_COUNT);
        let mut clocks = Vec::with_capacity(SHARD_COUNT);
        for _shard in 0..SHARD_COUNT {
            shards.push(RwLock::new(SpotShard::new(buckets)));
            let words = (buckets * BUCKET_SLOTS).div_ceil(CLOCK_WORD_BITS);
            let mut clock = Vec::with_capacity(words);
            for _word in 0..words {
                clock.push(AtomicU64::new(0));
            }
            clocks.push(clock);
        }
        Self { shards, clocks }
    }

    /// Read a shard
    pub fn read_shard(&self, index: usize) -> RwLockReadGuard<'_, SpotShard> {
        match self.shards[index].read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Change a shard
    pub fn write_shard(&self, index: usize) -> RwLockWriteGuard<'_, SpotShard> {
        match self.shards[index].write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Read the heap the cache holds
    pub fn heap_bytes(&self) -> ByteCount {
        let mut total = 0;
        for index in 0..SHARD_COUNT {
            total += self.read_shard(index).slots.capacity() * size_of::<SpotSlot>();
            total += self.clocks[index].capacity() * size_of::<AtomicU64>();
        }
        ByteCount(total as u64)
    }

    /// Find a key's location, and mark it as recently used
    pub fn lookup(&self, hash: KeyHash) -> Option<SpotLocation> {
        let shard = shard_of(hash);
        let guard = self.read_shard(shard);
        let index = guard.find(hash)?;
        self.touch(shard, index);
        Some(guard.slots[index].location())
    }

    /// Set the clock bit of a slot
    pub fn touch(&self, shard: usize, index: usize) {
        let word = &self.clocks[shard][index / CLOCK_WORD_BITS];
        let bit = 1 << (index % CLOCK_WORD_BITS);
        if word.load(Ordering::Relaxed) & bit == 0 {
            word.fetch_or(bit, Ordering::Relaxed);
        }
    }

    /// Check the clock bit of a slot
    pub fn is_touched(&self, shard: usize, index: usize) -> bool {
        let word = &self.clocks[shard][index / CLOCK_WORD_BITS];
        word.load(Ordering::Relaxed) & (1 << (index % CLOCK_WORD_BITS)) != 0
    }

    /// Clear the clock bit of a slot
    pub fn clear_touch(&self, shard: usize, index: usize) {
        let word = &self.clocks[shard][index / CLOCK_WORD_BITS];
        word.fetch_and(!(1 << (index % CLOCK_WORD_BITS)), Ordering::Relaxed);
    }

    /// Point a key at a new location, taking the slot of a cold key when its bucket is full
    pub fn insert(&self, hash: KeyHash, location: &SpotLocation) {
        let shard = shard_of(hash);
        let mut guard = self.write_shard(shard);
        let found = guard.find(hash);

        // a slot keeps 32 bits of the number, and an older location must never stay behind
        let Ok(segment) = u32::try_from(location.segment.0) else {
            if let Some(index) = found {
                guard.slots[index] = SpotSlot::empty();
            }
            return;
        };

        let index = match found {
            Some(index) => index,
            None => self.pick_slot(shard, &guard, hash),
        };
        guard.slots[index] = SpotSlot {
            tag: tag_of(hash),
            segment,
            offset: location.offset.0 as u32,
            length: location.length.0 as u32,
            place: location.place.0,
        };
    }

    /// Pick an empty slot, then a cold one, and age the bucket only when every key in it is hot
    pub fn pick_slot(&self, shard: usize, guard: &SpotShard, hash: KeyHash) -> usize {
        let bucket = guard.bucket_of(hash);
        for index in bucket..bucket + BUCKET_SLOTS {
            if !guard.slots[index].is_used() {
                return index;
            }
        }

        for index in bucket..bucket + BUCKET_SLOTS {
            if !self.is_touched(shard, index) {
                return index;
            }
        }

        for index in bucket..bucket + BUCKET_SLOTS {
            self.clear_touch(shard, index);
        }
        bucket + tag_of(hash) as usize % BUCKET_SLOTS
    }

    /// Forget a key
    pub fn remove(&self, hash: KeyHash) {
        let mut guard = self.write_shard(shard_of(hash));
        if let Some(index) = guard.find(hash) {
            guard.slots[index] = SpotSlot::empty();
            self.clear_touch(shard_of(hash), index);
        }
    }
}

/// Pick the bits that tell keys in one bucket apart
pub fn tag_of(hash: KeyHash) -> u32 {
    (hash.0 >> TAG_SHIFT) as u32
}
