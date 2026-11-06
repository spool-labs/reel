//! A complete column's table: a slot for each sealed row of the column, so a get finds its row block with no footer search
//!
//! A slot holds a tag, a segment and a row block, and no key, so a match is a candidate that the block's key settles. A slot
//! sits in one of two home buckets, and the second comes from the first and the tag, so a slot can move with no key to hand.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use reel_api::units::{ByteCount, RowCount};

use crate::index::key_hash::KeyHash;
use crate::pool::touched_filled;
use crate::segment::segment_tail::SegmentNumber;

/// Bytes of a slot on disk
pub const SLOT_BYTES: usize = 8;
/// Slots in a bucket
pub const BUCKET_SLOTS: usize = 4;
/// Shards of a table, each behind its own lock
pub const COMPLETE_SHARDS: usize = 64;
/// A table this many tenths full holds its rows
pub const LOAD_TENTHS: u64 = 9;
/// One slot in this many sits in a shard's overflow area
pub const OVERFLOW_DIVISOR: usize = 64;
/// The smallest overflow area a shard gets
pub const LEAST_OVERFLOW: usize = 8;
/// Moves an insert makes between homes before it gives a slot to the overflow area
pub const MOST_MOVES: usize = 64;
/// Slots one tag can hold in its homes and the overflow area before it folds, so a get's candidates fit a fixed list
pub const MOST_TAG_SLOTS: usize = 32;
/// The bit of a slot's tag that marks a superseded row
pub const STALE_BIT: u16 = 0x8000;
/// The bits of a slot's tag that come from the key
pub const TAG_MASK: u16 = !STALE_BIT;
/// A column with no mark serves every key from its table
pub const NO_MARK: u64 = u64::MAX;
/// The segment an unused slot holds
pub const NO_SEGMENT: u32 = u32::MAX;
/// The segment of a slot that says the footers answer for its tag, once the tag's list has filled
pub const STICKY_SEGMENT: u32 = u32::MAX - 1;
/// Spreads a tag over the buckets
pub const TAG_MIX: u64 = 0x9E37_79B9_7F4A_7C15;

/// Where one row of a complete column sits
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompleteSlot {
    pub segment: u32,
    pub block: u16,
    pub tag: u16,
}

/// The slots a key's tag matches
#[derive(Clone, Copy, Debug)]
pub struct TagMatches {
    slots: [CompleteSlot; MOST_TAG_SLOTS],
    count: usize,
}

/// A key's place in a table
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Homes {
    pub first: usize,
    pub second: usize,
    pub tag: u16,
}

/// One shard's slots, fixed at open
#[derive(Debug)]
pub struct CompleteShard {
    slots: Vec<CompleteSlot>,
    overflow: Vec<CompleteSlot>,
    buckets: usize,
    overflow_used: usize,
}

/// A complete column's table
#[derive(Debug)]
pub struct CompleteTable {
    shards: Vec<RwLock<CompleteShard>>,
    mark: AtomicU64,
    used: AtomicU64,
    past_capacity: AtomicU64,
}

impl CompleteSlot {
    /// Build an unused slot
    pub fn empty() -> Self {
        Self {
            segment: NO_SEGMENT,
            block: 0,
            tag: 0,
        }
    }

    /// Check a slot holds a row
    pub fn is_used(&self) -> bool {
        self.segment != NO_SEGMENT
    }

    /// Check the slot's row has been superseded
    pub fn is_stale(&self) -> bool {
        self.tag & STALE_BIT != 0
    }

    /// Check the slot stands for rows it no longer locates
    pub fn is_sticky(&self) -> bool {
        self.segment == STICKY_SEGMENT
    }

    /// Read the bits that came from the key
    pub fn key_tag(&self) -> u16 {
        self.tag & TAG_MASK
    }

    /// Read the slot's segment
    pub fn segment_number(&self) -> SegmentNumber {
        SegmentNumber(u64::from(self.segment))
    }

    /// Write a slot out
    pub fn to_bytes(self) -> [u8; SLOT_BYTES] {
        let mut bytes = [0; SLOT_BYTES];
        bytes[..4].copy_from_slice(&self.segment.to_le_bytes());
        bytes[4..6].copy_from_slice(&self.block.to_le_bytes());
        bytes[6..].copy_from_slice(&self.tag.to_le_bytes());
        bytes
    }

    /// Read a slot back
    pub fn from_bytes(bytes: [u8; SLOT_BYTES]) -> Self {
        Self {
            segment: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            block: u16::from_le_bytes([bytes[4], bytes[5]]),
            tag: u16::from_le_bytes([bytes[6], bytes[7]]),
        }
    }
}

impl TagMatches {
    /// Start with no matches
    pub fn new() -> Self {
        Self {
            slots: [CompleteSlot::empty(); MOST_TAG_SLOTS],
            count: 0,
        }
    }

    /// Read the matches
    pub fn slots(&self) -> &[CompleteSlot] {
        &self.slots[..self.count]
    }

    /// Add a match. A table holds no more of a tag than this list does
    pub fn push(&mut self, slot: CompleteSlot) {
        if self.count < MOST_TAG_SLOTS {
            self.slots[self.count] = slot;
            self.count += 1;
        }
    }
}

impl Default for TagMatches {
    fn default() -> Self {
        Self::new()
    }
}

impl CompleteShard {
    /// Allocate and touch a shard's share of the slots
    pub fn new(slots: usize) -> Self {
        let buckets = slots.div_ceil(BUCKET_SLOTS).max(1);
        let overflow = (slots / OVERFLOW_DIVISOR).max(LEAST_OVERFLOW);
        Self {
            slots: touched_filled(buckets * BUCKET_SLOTS, CompleteSlot::empty()),
            overflow: touched_filled(overflow, CompleteSlot::empty()),
            buckets,
            overflow_used: 0,
        }
    }

    /// Work out a key's homes
    pub fn homes(&self, hash: KeyHash) -> Homes {
        let tag = tag_of(hash);
        let first = (((hash.0 & 0xFFFF_FFFF) * self.buckets as u64) >> 32) as usize;
        Homes {
            first,
            second: self.other_home(first, tag),
            tag,
        }
    }

    /// Work out a slot's other home from the one it sits in
    pub fn other_home(&self, bucket: usize, tag: u16) -> usize {
        let spread = (u64::from(tag & TAG_MASK).wrapping_mul(TAG_MIX) >> 32) as usize % self.buckets;
        (spread + self.buckets - bucket) % self.buckets
    }

    /// Call on every used slot with a tag, in the homes and then the overflow area
    pub fn visit_tag(&self, homes: &Homes, mut visit: impl FnMut(&CompleteSlot)) {
        let mut look = |slots: &[CompleteSlot]| {
            for slot in slots {
                if slot.is_used() && slot.key_tag() == homes.tag {
                    visit(slot);
                }
            }
        };
        look(self.bucket(homes.first));
        if homes.second != homes.first {
            look(self.bucket(homes.second));
        }
        if self.overflow_used > 0 {
            let mut at = self.overflow_start(homes.tag);
            for _step in 0..self.overflow.len() {
                let slot = &self.overflow[at];
                if !slot.is_used() {
                    break;
                }
                look(std::slice::from_ref(slot));
                at = (at + 1) % self.overflow.len();
            }
        }
    }

    /// Change every used slot with a tag in a key's homes
    pub fn change_homes(&mut self, homes: &Homes, mut change: impl FnMut(&mut CompleteSlot)) {
        let tag = homes.tag;
        let mut buckets = [Some(homes.first), Some(homes.second)];
        if homes.first == homes.second {
            buckets[1] = None;
        }
        for bucket in buckets.into_iter().flatten() {
            for slot in &mut self.slots[bucket * BUCKET_SLOTS..(bucket + 1) * BUCKET_SLOTS] {
                if slot.is_used() && slot.key_tag() == tag {
                    change(slot);
                }
            }
        }
    }

    /// Change every slot with a tag in its run of the overflow area, leaving each one used so the run stays whole
    pub fn change_overflow(&mut self, tag: u16, mut change: impl FnMut(&mut CompleteSlot)) {
        if self.overflow_used == 0 {
            return;
        }
        let mut at = self.overflow_start(tag);
        for _step in 0..self.overflow.len() {
            let slot = &mut self.overflow[at];
            if !slot.is_used() {
                return;
            }
            if slot.key_tag() == tag {
                change(slot);
            }
            at = (at + 1) % self.overflow.len();
        }
    }

    /// Take a tag's slots out of its run of the overflow area, and say how many went
    pub fn fold_overflow(&mut self, tag: u16) -> u64 {
        let mut folded = 0;
        if self.overflow_used == 0 {
            return folded;
        }
        let mut at = self.overflow_start(tag);
        for _step in 0..self.overflow.len() {
            let slot = self.overflow[at];
            if !slot.is_used() {
                break;
            }
            if slot.key_tag() == tag {
                // a later slot of the run can shift into this position, so the look stays here
                self.clear_overflow(at);
                folded += 1;
                continue;
            }
            at = (at + 1) % self.overflow.len();
        }
        folded
    }

    /// Read a bucket's slots
    pub fn bucket(&self, bucket: usize) -> &[CompleteSlot] {
        &self.slots[bucket * BUCKET_SLOTS..(bucket + 1) * BUCKET_SLOTS]
    }

    /// Work out where a tag's run starts in the overflow area
    pub fn overflow_start(&self, tag: u16) -> usize {
        (tag & TAG_MASK) as usize % self.overflow.len()
    }

    /// Count the overflow area's slots
    pub fn overflow_used(&self) -> usize {
        self.overflow_used
    }

    /// Check the overflow area can take a slot and still end every run with an unused slot
    pub fn has_overflow_room(&self) -> bool {
        self.overflow_used + 1 < self.overflow.len()
    }

    /// Put a slot in a free place in a bucket, clearing slots of gone segments on the way, and say how many it cleared
    pub fn fill_bucket(&mut self, bucket: usize, slot: CompleteSlot, is_gone: &impl Fn(u32) -> bool) -> Option<u64> {
        let mut freed = 0;
        let mut is_placed = false;
        for held in &mut self.slots[bucket * BUCKET_SLOTS..(bucket + 1) * BUCKET_SLOTS] {
            if held.is_used() && !held.is_sticky() && is_gone(held.segment) {
                *held = CompleteSlot::empty();
                freed += 1;
            }
            if !is_placed && !held.is_used() {
                *held = slot;
                is_placed = true;
            }
        }
        is_placed.then_some(freed)
    }

    /// Put a slot in the overflow area, which has been checked for room
    pub fn fill_overflow(&mut self, slot: CompleteSlot) {
        let mut at = self.overflow_start(slot.tag);
        while self.overflow[at].is_used() {
            at = (at + 1) % self.overflow.len();
        }
        self.overflow[at] = slot;
        self.overflow_used += 1;
    }

    /// Take a slot out of the overflow area and close the gap, so every run stays whole
    pub fn clear_overflow(&mut self, at: usize) {
        let length = self.overflow.len();
        let mut gap = at;
        let mut next = (at + 1) % length;
        while self.overflow[next].is_used() {
            let start = self.overflow_start(self.overflow[next].tag);
            // a slot can fill the gap only if its run reaches back that far
            let distance_to_gap = (gap + length - start) % length;
            let distance_to_slot = (next + length - start) % length;
            if distance_to_gap <= distance_to_slot {
                self.overflow[gap] = self.overflow[next];
                gap = next;
            }
            next = (next + 1) % length;
        }
        self.overflow[gap] = CompleteSlot::empty();
        self.overflow_used -= 1;
    }
}

impl CompleteTable {
    /// Allocate and touch a table with room for these rows
    pub fn new(rows: RowCount) -> Self {
        let slots = (rows.0 * 10 / LOAD_TENTHS) as usize;
        let per_shard = slots.div_ceil(COMPLETE_SHARDS);
        Self {
            shards: (0..COMPLETE_SHARDS).map(|_shard| RwLock::new(CompleteShard::new(per_shard))).collect(),
            mark: AtomicU64::new(NO_MARK),
            used: AtomicU64::new(0),
            past_capacity: AtomicU64::new(0),
        }
    }

    /// Read a shard
    pub fn read_shard(&self, index: usize) -> RwLockReadGuard<'_, CompleteShard> {
        match self.shards[index].read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Change a shard
    pub fn write_shard(&self, index: usize) -> RwLockWriteGuard<'_, CompleteShard> {
        match self.shards[index].write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Read the heap the table holds
    pub fn heap_bytes(&self) -> ByteCount {
        let slots: usize = (0..COMPLETE_SHARDS)
            .map(|index| {
                let shard = self.read_shard(index);
                shard.slots.capacity() + shard.overflow.capacity()
            })
            .sum();
        ByteCount((slots * size_of::<CompleteSlot>()) as u64)
    }

    /// Read how many slots hold a row
    pub fn used_slots(&self) -> RowCount {
        RowCount(self.used.load(Ordering::Acquire))
    }

    /// Read how many rows found no room
    pub fn past_capacity(&self) -> RowCount {
        RowCount(self.past_capacity.load(Ordering::Acquire))
    }

    /// Read the segment a get's footer search starts from, if rows have gone past the table
    pub fn mark(&self) -> Option<SegmentNumber> {
        match self.mark.load(Ordering::Acquire) {
            NO_MARK => None,
            mark => Some(SegmentNumber(mark)),
        }
    }

    /// Count every slot the table has room for
    pub fn slot_count(&self) -> usize {
        let (slots, overflow) = self.shard_sizes();
        (slots + overflow) * COMPLETE_SHARDS
    }

    /// Read how many slots and overflow slots each shard has
    pub fn shard_sizes(&self) -> (usize, usize) {
        let shard = self.read_shard(0);
        (shard.slots.len(), shard.overflow.len())
    }

    /// Call on a shard's slots, then its overflow area, used or not
    pub fn visit_slots(&self, shard: usize, mut visit: impl FnMut(&CompleteSlot)) {
        let guard = self.read_shard(shard);
        guard.slots.iter().chain(guard.overflow.iter()).for_each(&mut visit);
    }

    /// Read the length of a part of a shard: its slots, then its overflow area
    pub fn part_length(&self, shard: usize, part: usize) -> usize {
        let guard = self.read_shard(shard);
        if part == 0 { guard.slots.len() } else { guard.overflow.len() }
    }

    /// Fill a part of a shard from an image's bytes, starting at a slot
    pub fn fill_part(&self, shard: usize, part: usize, from: usize, bytes: &[u8]) {
        let mut guard = self.write_shard(shard);
        let shard = &mut *guard;
        let slots = if part == 0 { &mut shard.slots } else { &mut shard.overflow };
        for (slot, stored) in slots[from..].iter_mut().zip(bytes.chunks_exact(SLOT_BYTES)) {
            let mut word = [0; SLOT_BYTES];
            word.copy_from_slice(stored);
            *slot = CompleteSlot::from_bytes(word);
        }
    }

    /// Finish a fill from an image: count the overflow slots and take the image's count of used slots
    pub fn finish_fill(&self, used: u64) {
        for index in 0..COMPLETE_SHARDS {
            let mut shard = self.write_shard(index);
            shard.overflow_used = shard.overflow.iter().filter(|slot| slot.is_used()).count();
        }
        self.used.store(used, Ordering::Release);
    }

    /// Empty the table
    pub fn clear(&self) {
        for index in 0..COMPLETE_SHARDS {
            let mut shard = self.write_shard(index);
            shard.slots.fill(CompleteSlot::empty());
            shard.overflow.fill(CompleteSlot::empty());
            shard.overflow_used = 0;
        }
        self.used.store(0, Ordering::Release);
    }

    /// Fill a list with the slots a key's tag matches
    pub fn find(&self, hash: KeyHash, matches: &mut TagMatches) {
        let shard = self.read_shard(shard_of(hash));
        let homes = shard.homes(hash);
        shard.visit_tag(&homes, |slot| matches.push(*slot));
    }

    /// Mark one row's slot superseded. A get skips it, and it stays until its segment goes, so a grave can see an older row stands.
    /// Two keys of a tag in one block hold a slot each, so this marks one
    pub fn mark_stale(&self, hash: KeyHash, segment: SegmentNumber, block: u16) {
        let mut shard = self.write_shard(shard_of(hash));
        let homes = shard.homes(hash);
        let mut is_marked = false;
        let mut mark = |slot: &mut CompleteSlot| {
            if !is_marked && !slot.is_stale() && u64::from(slot.segment) == segment.0 && slot.block == block {
                slot.tag |= STALE_BIT;
                is_marked = true;
            }
        };
        shard.change_homes(&homes, &mut mark);
        shard.change_overflow(homes.tag, mark);
    }

    /// Check a key's tag has a slot past the ones it is asked to leave out. A tag another key shares can only keep a grave longer
    pub fn has_other_slot(&self, hash: KeyHash, is_own: impl Fn(u32) -> bool, is_gone: impl Fn(u32) -> bool) -> bool {
        let shard = self.read_shard(shard_of(hash));
        let homes = shard.homes(hash);
        let mut has_other = false;
        shard.visit_tag(&homes, |slot| has_other |= slot.is_sticky() || (!is_own(slot.segment) && !is_gone(slot.segment)));
        has_other
    }

    /// Give a tag whose list is full one sticky slot in place of the rest. Its readers go by the footers from here on, so a key
    /// written into many segments costs that tag and no other
    pub fn fold_tag(&self, shard: &mut CompleteShard, homes: &Homes) {
        let mut folded = 0;
        shard.change_homes(homes, |slot| {
            *slot = CompleteSlot::empty();
            folded += 1;
        });
        folded += shard.fold_overflow(homes.tag);
        let sticky = CompleteSlot {
            segment: STICKY_SEGMENT,
            block: 0,
            tag: homes.tag | STALE_BIT,
        };
        // every key of the tag walks this run, whatever its homes, and a full list held most of its slots here, so there's room
        shard.fill_overflow(sticky);
        self.used.fetch_sub(folded - 1, Ordering::AcqRel);
    }

    /// Give a sealed row a slot. A row with no room marks the column at its segment, and a get searches the footers from there
    pub fn insert(&self, hash: KeyHash, segment: SegmentNumber, block: u16, is_gone: impl Fn(u32) -> bool) {
        let is_placed = u32::try_from(segment.0).is_ok_and(|number| number < STICKY_SEGMENT && self.place(hash, number, block, &is_gone));
        if !is_placed {
            self.past_capacity.fetch_add(1, Ordering::AcqRel);
            self.mark.fetch_min(segment.0, Ordering::AcqRel);
        }
    }

    /// Find a slot a place: a home, then a home by moving another slot on, then the overflow area
    pub fn place(&self, hash: KeyHash, segment: u32, block: u16, is_gone: &impl Fn(u32) -> bool) -> bool {
        let mut shard = self.write_shard(shard_of(hash));
        let homes = shard.homes(hash);
        let (mut tag_slots, mut has_sticky) = (0, false);
        shard.visit_tag(&homes, |slot| {
            tag_slots += 1;
            has_sticky |= slot.is_sticky();
        });
        if has_sticky {
            return true;
        }
        if tag_slots >= MOST_TAG_SLOTS {
            self.fold_tag(&mut shard, &homes);
            return true;
        }

        let slot = CompleteSlot { segment, block, tag: homes.tag };
        for bucket in [homes.first, homes.second] {
            if let Some(freed) = shard.fill_bucket(bucket, slot, is_gone) {
                self.used.fetch_add(1, Ordering::AcqRel);
                self.used.fetch_sub(freed, Ordering::AcqRel);
                return true;
            }
        }
        // a slot moved out has to land, so moves start only with room for the last one
        if !shard.has_overflow_room() {
            return false;
        }

        let mut moving = slot;
        let mut bucket = homes.first;
        for step in 0..MOST_MOVES {
            let at = bucket * BUCKET_SLOTS + (usize::from(moving.key_tag()) + step) % BUCKET_SLOTS;
            std::mem::swap(&mut shard.slots[at], &mut moving);
            bucket = shard.other_home(bucket, moving.tag);
            if let Some(freed) = shard.fill_bucket(bucket, moving, is_gone) {
                self.used.fetch_add(1, Ordering::AcqRel);
                self.used.fetch_sub(freed, Ordering::AcqRel);
                return true;
            }
        }
        shard.fill_overflow(moving);
        self.used.fetch_add(1, Ordering::AcqRel);
        true
    }
}

/// Read the tag a key's slots carry
pub fn tag_of(hash: KeyHash) -> u16 {
    (hash.0 >> 32) as u16 & TAG_MASK
}

/// Find the shard a key's slots sit in
pub fn shard_of(hash: KeyHash) -> usize {
    (hash.0 >> 48) as usize % COMPLETE_SHARDS
}
