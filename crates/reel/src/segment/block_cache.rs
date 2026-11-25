//! One store-wide cache of checked filter and row blocks, cold blocks out first

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{RwLock, RwLockReadGuard};

use reel_core::units::ByteCount;

use crate::pool::touched_filled;
use crate::pool_room::{give_back_buffer, take_back_buffer};
use crate::guard::Shards;
use crate::segment::segment_tail::SegmentNumber;

/// The fewest shards a cache has
pub const CACHE_SHARDS: usize = 16;
/// The most shards a cache has. A visit over several blocks holds a shard for each, so a large cache splits finer and a visit
/// holds a small share of it
pub const MOST_CACHE_SHARDS: usize = 256;
/// Buckets a shard keeps at least before a cache splits into more shards
pub const LEAST_SHARD_BUCKETS: usize = 16;
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
    /// Cleared while the reserve job has the shard's pages given back. An absent shard holds no block and keeps none
    is_present: bool,
}

/// A cache of blocks up to a fixed size
#[derive(Debug)]
pub struct BlockCache {
    pub shards: Shards<CacheShard>,
    /// Shards whose pages are given back
    absent: AtomicUsize,
    touched: Vec<Vec<AtomicU64>>,
    /// Inserts that found their shard locked and kept nothing, since a reader would otherwise queue behind the wait
    skipped: AtomicU64,
    slot_bytes: usize,
    buckets: usize,
}

impl BlockCache {
    /// Allocate and touch slots that fill a share of the budget
    pub fn new(share: ByteCount, slot_bytes: usize) -> Self {
        let fits = share.0 as usize / (slot_bytes * CACHE_WAYS * LEAST_SHARD_BUCKETS);
        let shard_count = (1_usize << fits.max(1).ilog2()).clamp(CACHE_SHARDS, MOST_CACHE_SHARDS);
        let per_shard = share.0 as usize / shard_count;
        let buckets = (per_shard / (slot_bytes * CACHE_WAYS)).max(1);
        let slots = buckets * CACHE_WAYS;

        let mut shards = Vec::with_capacity(shard_count);
        let mut touched = Vec::with_capacity(shard_count);
        for _shard in 0..shard_count {
            shards.push(RwLock::new(CacheShard {
                keys: touched_filled(slots, NO_BLOCK),
                lengths: touched_filled(slots, 0),
                bytes: touched_filled(slots * slot_bytes, 1),
                is_present: true,
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
            skipped: AtomicU64::new(0),
            absent: AtomicUsize::new(0),
            slot_bytes,
            buckets,
        }
    }

    /// Count the inserts that found their shard locked
    pub fn skipped_inserts(&self) -> u64 {
        self.skipped.load(Ordering::Relaxed)
    }

    /// Read the most bytes a block can have
    pub fn slot_bytes(&self) -> usize {
        self.slot_bytes
    }

    /// Empty the cache
    pub fn clear(&self) {
        for index in 0..self.shards.len() {
            let mut guard = self.shards.write(index);
            if guard.is_present {
                guard.keys.fill(NO_BLOCK);
            }
        }
    }

    /// Read the heap the cache holds
    pub fn heap_bytes(&self) -> ByteCount {
        let mut total = 0;
        for shard in 0..self.shards.len() {
            let guard = self.shards.read(shard);
            if guard.is_present {
                total += guard.bytes.capacity() + guard.keys.capacity() * size_of::<BlockKey>();
            total += guard.lengths.capacity() * size_of::<u32>();
            }
            total += self.touched[shard].capacity() * size_of::<AtomicU64>();
        }
        ByteCount(total as u64)
    }

    /// Find a block's shard and first slot
    pub fn place_of(&self, key: BlockKey) -> (usize, usize) {
        let mixed = key.segment.0.wrapping_add(u64::from(key.offset)).wrapping_mul(OFFSET_MIX);
        let shard = (mixed >> 56) as usize % self.shards.len();
        let bucket = (mixed >> 20) as usize % self.buckets;
        (shard, bucket * CACHE_WAYS)
    }

    /// Run a function over a cached block, with no lock held past it
    pub fn with<Answer>(&self, key: BlockKey, visit: impl FnOnce(&[u8]) -> Answer) -> Option<Answer> {
        let (shard, first) = self.place_of(key);
        let guard = self.shards.read(shard);
        if !guard.is_present {
            return None;
        }
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
        let mut places = [(0_usize, 0_usize); MOST_MANY_BLOCKS];
        // the shards the keys fall in, each once, in shard order
        let mut wanted = [0_usize; MOST_MANY_BLOCKS];
        let mut wanted_count = 0;
        for (key, place) in keys.iter().zip(&mut places).take(count) {
            *place = self.place_of(*key);
            if !wanted[..wanted_count].contains(&place.0) {
                wanted[wanted_count] = place.0;
                wanted_count += 1;
            }
        }
        let wanted = &mut wanted[..wanted_count];
        wanted.sort_unstable();
        let mut guards: [Option<RwLockReadGuard<'_, CacheShard>>; MOST_MANY_BLOCKS] = [const { None }; MOST_MANY_BLOCKS];
        for (shard, guard) in wanted.iter().zip(&mut guards) {
            *guard = Some(self.shards.read(*shard));
        }
        let mut blocks: [Option<&[u8]>; MOST_MANY_BLOCKS] = [None; MOST_MANY_BLOCKS];
        for ((key, &(shard, first)), block) in keys.iter().zip(&places).zip(&mut blocks).take(count) {
            let Some(guard) = wanted.iter().position(|held| *held == shard).and_then(|at| guards[at].as_ref()).filter(|guard| guard.is_present) else {
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
        // a cache can go without a block, so an insert never waits for a shard a visit holds
        let Some(mut guard) = self.shards.try_write(shard) else {
            self.skipped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if !guard.is_present {
            return;
        }
        let slot = self.pick_slot(shard, &guard, first, key);
        guard.keys[slot] = key;
        guard.lengths[slot] = block.len() as u32;
        let start = slot * self.slot_bytes;
        guard.bytes[start..start + block.len()].copy_from_slice(block);
    }

    /// Give one shard's pages back to the kernel, and return the bytes given. Its blocks read as missing until it comes back
    pub fn give_back_shard(&self, shard: usize) -> usize {
        let mut guard = self.shards.write(shard);
        if !guard.is_present {
            return 0;
        }
        guard.is_present = false;
        self.absent.fetch_add(1, Ordering::AcqRel);
        let keys = guard.keys.len() * size_of::<BlockKey>();
        let lengths = guard.lengths.len() * size_of::<u32>();
        let bytes = guard.bytes.len();
        give_back_buffer(guard.keys.as_mut_ptr().cast(), keys) + give_back_buffer(guard.lengths.as_mut_ptr().cast(), lengths) + give_back_buffer(guard.bytes.as_mut_ptr(), bytes)
    }

    /// Bring a shard given back into memory again with every slot empty, touching each page here so no caller faults one in, and
    /// say whether it was absent
    pub fn bring_back_shard(&self, shard: usize) -> bool {
        let mut guard = self.shards.write(shard);
        if guard.is_present {
            return false;
        }
        let (keys, lengths, bytes) = (guard.keys.len() * size_of::<BlockKey>(), guard.lengths.len() * size_of::<u32>(), guard.bytes.len());
        take_back_buffer(guard.keys.as_mut_ptr().cast(), keys);
        take_back_buffer(guard.lengths.as_mut_ptr().cast(), lengths);
        take_back_buffer(guard.bytes.as_mut_ptr(), bytes);
        guard.keys.fill(NO_BLOCK);
        guard.lengths.fill(0);
        guard.bytes.fill(1);
        for bit in &self.touched[shard] {
            bit.store(0, Ordering::Relaxed);
        }
        guard.is_present = true;
        self.absent.fetch_sub(1, Ordering::AcqRel);
        true
    }

    /// Count the shards whose pages are given back
    pub fn absent_shards(&self) -> usize {
        self.absent.load(Ordering::Acquire)
    }

    /// Read the bytes one shard's slots take
    pub fn shard_bytes(&self) -> usize {
        let guard = self.shards.read(0);
        guard.keys.len() * size_of::<BlockKey>() + guard.lengths.len() * size_of::<u32>() + guard.bytes.len()
    }

    /// Count the shards
    pub fn shard_count(&self) -> usize {
        self.shards.len()
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
