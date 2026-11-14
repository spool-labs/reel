//! One store-wide cache of checked filter and row blocks, cold blocks out first

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{RwLock, RwLockReadGuard};

use reel_api::units::ByteCount;

use crate::pool::touched_filled;
use crate::guard::Shards;
use crate::segment::segment_tail::SegmentNumber;

/// Shards of a cache
pub const CACHE_SHARDS: usize = 16;
/// Slots a block can land in
pub const CACHE_WAYS: usize = 4;
/// The most blocks one visit over several takes
pub const MOST_MANY_BLOCKS: usize = 16;
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
    pub shards: Shards<CacheShard>,
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
                keys: touched_filled(slots, NO_BLOCK),
                lengths: touched_filled(slots, 0),
                bytes: touched_filled(slots * slot_bytes, 1),
            }));
            let mut bits = Vec::with_capacity(slots);
            for _slot in 0..slots {
                bits.push(AtomicU64::new(0));
            }
            touched.push(bits);
        }
        Self {
            shards: Shards(shards),
            touched,
            slot_bytes,
            buckets,
        }
    }

    /// Read the most bytes a block can have
    pub fn slot_bytes(&self) -> usize {
        self.slot_bytes
    }

    /// Empty the cache
    pub fn clear(&self) {
        for index in 0..self.shards.len() {
            let mut guard = self.shards.write(index);
            guard.keys.fill(NO_BLOCK);
        }
    }

    /// Read the heap the cache holds
    pub fn heap_bytes(&self) -> ByteCount {
        let mut total = 0;
        for shard in 0..CACHE_SHARDS {
            let guard = self.shards.read(shard);
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
        let guard = self.shards.read(shard);
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

    /// Run a function over several cached blocks at once, with no lock held past it. Each shard's read lock is taken once, in
    /// shard order, so two callers with writers waiting between them can't hold one lock each and wait for the other's. A block
    /// the cache doesn't hold comes as nothing
    pub fn with_many<Answer>(&self, keys: &[BlockKey], visit: impl FnOnce(&[Option<&[u8]>]) -> Answer) -> Answer {
        let count = keys.len().min(MOST_MANY_BLOCKS);
        let mut wanted = [false; CACHE_SHARDS];
        let mut places = [(0_usize, 0_usize); MOST_MANY_BLOCKS];
        for (key, place) in keys.iter().zip(&mut places).take(count) {
            *place = self.place_of(*key);
            wanted[place.0] = true;
        }
        let mut guards: [Option<RwLockReadGuard<'_, CacheShard>>; CACHE_SHARDS] = [const { None }; CACHE_SHARDS];
        for (shard, guard) in guards.iter_mut().enumerate() {
            if wanted[shard] {
                *guard = Some(self.shards.read(shard));
            }
        }
        let mut blocks: [Option<&[u8]>; MOST_MANY_BLOCKS] = [None; MOST_MANY_BLOCKS];
        for ((key, &(shard, first)), block) in keys.iter().zip(&places).zip(&mut blocks).take(count) {
            let Some(guard) = guards[shard].as_ref() else {
                continue;
            };
            if let Some(slot) = (first..first + CACHE_WAYS).find(|slot| guard.keys[*slot] == *key) {
                self.touched[shard][slot].store(1, Ordering::Relaxed);
                let start = slot * self.slot_bytes;
                *block = Some(&guard.bytes[start..start + guard.lengths[slot] as usize]);
            }
        }
        visit(&blocks[..count])
    }

    /// Keep a checked block, in place of a cold one when its bucket is full
    pub fn insert(&self, key: BlockKey, block: &[u8]) {
        if block.len() > self.slot_bytes {
            return;
        }

        let (shard, first) = self.place_of(key);
        let mut guard = self.shards.write(shard);
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
