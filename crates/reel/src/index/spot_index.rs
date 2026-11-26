//! A cache of sealed live keys' locations, one slot per key, cold keys out first

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::RwLock;

use reel_core::units::{ByteCount, FileOffset, WritePlace};

use crate::pool::touched_filled;
use crate::pool_room::{give_back_buffer, take_back_buffer, whole_pages};
use crate::index::key_hash::KeyHash;
use crate::guard::Shards;
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

// a slot stays under the 28 bytes a cached key may cost
const _: () = assert!(size_of::<SpotSlot>() < 28);

/// A shard's slots, fixed at open
#[derive(Debug)]
pub struct SpotShard {
    slots: Vec<SpotSlot>,
    bucket_mask: usize,
    /// Cleared while the reserve job has the slots' pages given back. An absent shard finds nothing and keeps nothing
    is_present: bool,
}

/// The cache, sharded by a hash of the key
#[derive(Debug)]
pub struct SpotIndex {
    pub shards: Shards<SpotShard>,
    clocks: Vec<Vec<AtomicU64>>,
    /// Shards whose pages are given back
    absent: AtomicUsize,
    /// The shards keys spread over: all of them, or fewer once a rebalance lowered the cache's ceiling by whole shards
    live_shards: AtomicUsize,
    /// Each shard's presence, read with no lock so a reader misses an absent shard without waiting on a fill
    present: Vec<AtomicBool>,
    /// Bytes the shards given back hold no more
    given: AtomicU64,
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
            slots: touched_filled(buckets * BUCKET_SLOTS, SpotSlot::empty()),
            bucket_mask: buckets - 1,
            is_present: true,
        }
    }

    /// Find the first slot of a key's bucket
    pub fn bucket_of(&self, hash: KeyHash) -> usize {
        (hash.0 as usize & self.bucket_mask) * BUCKET_SLOTS
    }

    /// Find the slot holding a key
    pub fn find(&self, hash: KeyHash) -> Option<usize> {
        if !self.is_present {
            return None;
        }
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
        Self { shards: Shards(shards), clocks, absent: AtomicUsize::new(0), live_shards: AtomicUsize::new(SHARD_COUNT), present: (0..SHARD_COUNT).map(|_shard| AtomicBool::new(true)).collect(), given: AtomicU64::new(0) }
    }

    /// Read the heap the cache holds
    pub fn heap_bytes(&self) -> ByteCount {
        let mut total = 0;
        for index in 0..SHARD_COUNT {
            total += self.shards.read(index).slots.capacity() * size_of::<SpotSlot>();
            total += self.clocks[index].capacity() * size_of::<AtomicU64>();
        }
        ByteCount((total as u64).saturating_sub(self.given_bytes()))
    }

    /// Find a key's location, and mark it as recently used
    pub fn lookup(&self, hash: KeyHash) -> Option<SpotLocation> {
        let shard = self.shard_for(hash);
        if !self.is_present(shard) {
            return None;
        }
        let guard = self.shards.read(shard);
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

    /// Fill a slot from a read, unless a seal has started handing rows over since the read began
    pub fn insert_from_read(&self, hash: KeyHash, location: &SpotLocation, is_current: impl Fn() -> bool) {
        let shard = self.shard_for(hash);
        if !self.is_present(shard) {
            return;
        }
        let mut guard = self.shards.write(shard);
        if is_current() && !holds_newer(&guard, hash, location.place) {
            self.store(shard, &mut guard, hash, location);
        }
    }

    /// Point a key at a new location, taking the slot of a cold key when its bucket is full
    pub fn insert(&self, hash: KeyHash, location: &SpotLocation) {
        let shard = self.shard_for(hash);
        if !self.is_present(shard) {
            return;
        }
        let mut guard = self.shards.write(shard);
        self.store(shard, &mut guard, hash, location);
    }

    /// Point a key at a location, unless its slot holds a newer place
    pub fn insert_newer(&self, hash: KeyHash, location: &SpotLocation) {
        let shard = self.shard_for(hash);
        if !self.is_present(shard) {
            return;
        }
        let mut guard = self.shards.write(shard);
        if !holds_newer(&guard, hash, location.place) {
            self.store(shard, &mut guard, hash, location);
        }
    }

    /// Forget a key, unless its slot holds a newer place than this one
    pub fn remove_older(&self, hash: KeyHash, place: WritePlace) {
        let shard = self.shard_for(hash);
        if !self.is_present(shard) {
            return;
        }
        let mut guard = self.shards.write(shard);
        if let Some(index) = guard.find(hash).filter(|index| guard.slots[*index].place < place.0) {
            guard.slots[index] = SpotSlot::empty();
            self.clear_touch(shard, index);
        }
    }

    /// Write a location into a key's slot under the shard's lock
    pub fn store(&self, shard: usize, guard: &mut SpotShard, hash: KeyHash, location: &SpotLocation) {
        if !guard.is_present {
            return;
        }
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
            None => self.pick_slot(shard, guard, hash),
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

    /// Give one shard's slot pages back to the kernel, and return the bytes given. Its keys read as missing until it comes back
    pub fn give_back_shard(&self, shard: usize) -> usize {
        // a shard with no whole page gives nothing, and stays
        let mut guard = self.shards.write(shard);
        let bytes = guard.slots.len() * size_of::<SpotSlot>();
        if !guard.is_present || whole_pages(guard.slots.as_mut_ptr().cast(), bytes).is_none() {
            return 0;
        }
        // a reader that loads the flag false misses with no lock taken
        self.present[shard].store(false, Ordering::Release);
        guard.is_present = false;
        self.absent.fetch_add(1, Ordering::AcqRel);
        let given = give_back_buffer(guard.slots.as_mut_ptr().cast(), bytes);
        self.given.fetch_add(given as u64, Ordering::AcqRel);
        given
    }

    /// Bring a shard given back into memory again with every slot empty, and say whether it was absent. Readers load the flag false
    /// for the whole fill and miss with no lock taken, so the fill holds up nobody
    pub fn bring_back_shard(&self, shard: usize) -> bool {
        let mut guard = self.shards.write(shard);
        if guard.is_present {
            return false;
        }
        let bytes = guard.slots.len() * size_of::<SpotSlot>();
        take_back_buffer(guard.slots.as_mut_ptr().cast(), bytes);
        guard.slots.fill(SpotSlot::empty());
        for word in &self.clocks[shard] {
            word.store(0, Ordering::Relaxed);
        }
        let given = whole_pages(guard.slots.as_mut_ptr().cast(), bytes).map_or(0, |(_first, length)| length);
        self.given.fetch_sub(given as u64, Ordering::AcqRel);
        guard.is_present = true;
        self.present[shard].store(true, Ordering::Release);
        self.absent.fetch_sub(1, Ordering::AcqRel);
        true
    }

    /// Find a key's shard among the live ones
    fn shard_for(&self, hash: KeyHash) -> usize {
        shard_of(hash) % self.live_shards.load(Ordering::Acquire).max(1)
    }

    /// Spread keys over the first shards alone, the rest given back above a lowered ceiling, so every key keeps a place a reopen
    /// at the smaller share would give it. A key whose shard moves misses until it is cached again. Its old slot keeps a tag a lookup
    /// of another key matches only as two keys' tags in one bucket can today, and a spot hit is checked against the record it reads
    pub fn set_live_shards(&self, shards: usize) {
        self.live_shards.store(shards.clamp(1, SHARD_COUNT), Ordering::Release);
    }

    /// Read the bytes the shards given back hold no more
    pub fn given_bytes(&self) -> u64 {
        self.given.load(Ordering::Acquire)
    }

    /// Check a shard is present, with no lock taken
    fn is_present(&self, shard: usize) -> bool {
        self.present[shard].load(Ordering::Acquire)
    }

    /// Count the shards whose pages are given back
    pub fn absent_shards(&self) -> usize {
        self.absent.load(Ordering::Acquire)
    }

    /// Read the bytes one shard's slots take
    pub fn shard_bytes(&self) -> usize {
        self.shards.read(0).slots.len() * size_of::<SpotSlot>()
    }

    /// Count the shards
    pub fn shard_count(&self) -> usize {
        SHARD_COUNT
    }

    /// Forget a key
    pub fn remove(&self, hash: KeyHash) {
        let shard = self.shard_for(hash);
        if !self.is_present(shard) {
            return;
        }
        let mut guard = self.shards.write(shard);
        if let Some(index) = guard.find(hash) {
            guard.slots[index] = SpotSlot::empty();
            self.clear_touch(shard, index);
        }
    }
}

/// Pick the bits that tell keys in one bucket apart
pub fn tag_of(hash: KeyHash) -> u32 {
    (hash.0 >> TAG_SHIFT) as u32
}

/// Check a key's slot holds a place newer than this one
pub fn holds_newer(guard: &SpotShard, hash: KeyHash, place: WritePlace) -> bool {
    guard.find(hash).is_some_and(|index| guard.slots[index].place > place.0)
}
