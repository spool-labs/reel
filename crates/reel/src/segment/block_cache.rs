//! One store-wide cache of checked filter and row blocks, cold blocks out first

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use reel_api::units::ByteCount;

use crate::pool::touched_filled;
use crate::segment::segment_tail::SegmentNumber;

/// Shards of a cache
pub const CACHE_SHARDS: usize = 16;
/// Slots a block can land in
pub const CACHE_WAYS: usize = 4;
/// Marks an unused slot
pub const NO_BLOCK: BlockKey = BlockKey {
    segment: SegmentNumber(u64::MAX),
    offset: u32::MAX,
};
/// Spreads block offsets over the buckets
pub const OFFSET_MIX: u64 = 0x9E37_79B9_7F4A_7C15;

/// A block's place in the store
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BlockKey {
    pub segment: SegmentNumber,
    pub offset: u32,
}

/// One shard's slots
#[derive(Debug)]
pub struct CacheShard {
    keys: Vec<BlockKey>,
    lengths: Vec<u32>,
    bytes: Vec<u8>,
}

/// A cache of blocks up to a fixed size
#[derive(Debug)]
pub struct BlockCache {
    shards: Vec<RwLock<CacheShard>>,
    touched: Vec<Vec<AtomicU64>>,
    slot_bytes: usize,
    buckets: usize,
}

impl BlockCache {
    /// Allocate and touch slots that fill a share of the budget
    pub fn new(share: ByteCount, slot_bytes: usize) -> Self {
        let per_shard = share.0 as usize / CACHE_SHARDS;
        let buckets = (per_shard / (slot_bytes * CACHE_WAYS)).max(1);
        let slots = buckets * CACHE_WAYS;

        let mut shards = Vec::with_capacity(CACHE_SHARDS);
        let mut touched = Vec::with_capacity(CACHE_SHARDS);
        for _shard in 0..CACHE_SHARDS {
            shards.push(RwLock::new(CacheShard {
                keys: vec![NO_BLOCK; slots],
                lengths: touched_filled(slots, 0),
                bytes: vec![1; slots * slot_bytes],
            }));
            let mut bits = Vec::with_capacity(slots);
            for _slot in 0..slots {
                bits.push(AtomicU64::new(0));
            }
            touched.push(bits);
        }
        Self {
            shards,
            touched,
            slot_bytes,
            buckets,
        }
    }

    /// Read the most bytes a block can have
    pub fn slot_bytes(&self) -> usize {
        self.slot_bytes
    }

    /// Read a shard
    pub fn read_shard(&self, index: usize) -> RwLockReadGuard<'_, CacheShard> {
        match self.shards[index].read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Change a shard
    pub fn write_shard(&self, index: usize) -> RwLockWriteGuard<'_, CacheShard> {
        match self.shards[index].write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Empty the cache
    pub fn clear(&self) {
        for index in 0..self.shards.len() {
            let mut guard = self.write_shard(index);
            guard.keys.fill(NO_BLOCK);
        }
    }

    /// Read the heap the cache holds
    pub fn heap_bytes(&self) -> ByteCount {
        let mut total = 0;
        for shard in 0..CACHE_SHARDS {
            let guard = self.read_shard(shard);
            total += guard.bytes.capacity() + guard.keys.capacity() * size_of::<BlockKey>();
            total += guard.lengths.capacity() * size_of::<u32>();
            total += self.touched[shard].capacity() * size_of::<AtomicU64>();
        }
        ByteCount(total as u64)
    }

    /// Find a block's shard and first slot
    pub fn place_of(&self, key: BlockKey) -> (usize, usize) {
        let mixed = key.segment.0.wrapping_add(u64::from(key.offset)).wrapping_mul(OFFSET_MIX);
        let shard = (mixed >> 60) as usize % CACHE_SHARDS;
        let bucket = (mixed >> 20) as usize % self.buckets;
        (shard, bucket * CACHE_WAYS)
    }

    /// Run a function over a cached block, with no lock held past it
    pub fn with<Answer>(&self, key: BlockKey, visit: impl FnOnce(&[u8]) -> Answer) -> Option<Answer> {
        let (shard, first) = self.place_of(key);
        let guard = self.read_shard(shard);
        for slot in first..first + CACHE_WAYS {
            if guard.keys[slot] == key {
                self.touched[shard][slot].store(1, Ordering::Relaxed);
                let start = slot * self.slot_bytes;
                let end = start + guard.lengths[slot] as usize;
                return Some(visit(&guard.bytes[start..end]));
            }
        }
        None
    }

    /// Keep a checked block, in place of a cold one when its bucket is full
    pub fn insert(&self, key: BlockKey, block: &[u8]) {
        if block.len() > self.slot_bytes {
            return;
        }

        let (shard, first) = self.place_of(key);
        let mut guard = self.write_shard(shard);
        let slot = self.pick_slot(shard, &guard, first, key);
        guard.keys[slot] = key;
        guard.lengths[slot] = block.len() as u32;
        let start = slot * self.slot_bytes;
        guard.bytes[start..start + block.len()].copy_from_slice(block);
    }

    /// Pick the block's own slot, an empty one, a cold one, or age the bucket
    pub fn pick_slot(&self, shard: usize, guard: &CacheShard, first: usize, key: BlockKey) -> usize {
        for slot in first..first + CACHE_WAYS {
            if guard.keys[slot] == key || guard.keys[slot] == NO_BLOCK {
                return slot;
            }
        }
        for slot in first..first + CACHE_WAYS {
            if self.touched[shard][slot].load(Ordering::Relaxed) == 0 {
                return slot;
            }
        }
        for slot in first..first + CACHE_WAYS {
            self.touched[shard][slot].store(0, Ordering::Relaxed);
        }
        first
    }
}
