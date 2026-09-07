//! One slab beneath the caches that hold what this volume issued
//!
//! Every key these caches take is a number the reel allocated: a segment, a small
//! column, a block index. None is chosen by a caller and none needs defending, so
//! the key is packed into one word and mixed with the multiply the fd cache already
//! hashes segment ids with, rather than run through SipHash as a tuple. What the
//! tenants differ in is what an entry weighs, which is a number the caller passes.
//!
//! Three things fall out of owning the structure. Eviction is clock over the slots
//! with a hot bit, which is the policy the fd cache documented and could not run off
//! a hash map's iteration order. Retiring a segment walks that segment's own chain
//! rather than the whole cache. And the lock is one mutex per shard rather than one
//! write lock every insert of every tenant queues on.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::format::column::ColumnId;
use crate::format::loc::SegmentId;
use crate::sync::checked::lock;
use crate::sync::checked::Mutex;

/// Shards a hold splits into, each under its own mutex
const SHARDS: usize = 16;

/// Entries a shard has to fit before splitting into one more of them is worth it
const PER_SHARD: usize = 8;

/// The empty table slot and the end of a chain
const NONE: u32 = u32::MAX;

/// The multiplier, 2^64 divided by the golden ratio
const MIX: u64 = 0x9e37_79b9_7f4a_7c15;

/// Bits the block index takes, the low end of the packed key
const BLOCK_BITS: u32 = 24;

/// Block indices a key can name, past which an entry is not held
pub const MAX_BLOCK: usize = (1 << BLOCK_BITS) - 1;

/// One cache key packed into a word: segment, column, block
pub fn hold_key(segment: SegmentId, column: ColumnId, block: usize) -> u64 {
    (u64::from(segment.as_u32()) << 32)
        | (u64::from(column.as_u8()) << BLOCK_BITS)
        | (block as u64 & MAX_BLOCK as u64)
}

/// The key of a whole segment, for a tenant holding one entry per segment
pub fn segment_key(segment: SegmentId) -> u64 {
    u64::from(segment.as_u32()) << 32
}

/// The segment a packed key names
fn segment_of(key: u64) -> u32 {
    (key >> 32) as u32
}

/// Spread a key issued in order across the table's buckets
fn mix(key: u64) -> u64 {
    let mixed = key.wrapping_mul(MIX);
    mixed ^ (mixed >> 32)
}

/// One held entry, or a vacancy on the free list
struct Slot<V> {
    /// The packed key, meaningless while the slot is vacant
    key: u64,

    /// What is held, and none for a vacancy
    val: Option<V>,

    /// What the entry weighs against its shard's budget
    weight: usize,

    /// Whether the entry was read since the hand last passed it
    hot: bool,

    /// Next entry of the same segment, or the next vacancy while free
    next: u32,
}

/// What one shard holds, behind its own mutex
struct Inner<V> {
    /// The entries themselves, vacancies threaded onto the free list
    slots: Vec<Slot<V>>,

    /// Ends of the free list, taken from the head and given back at the tail
    ///
    /// In order rather than newest first, so a slot reused stands where the entry
    /// that freed it stood and the hand still meets entries in the order they
    /// arrived. Taken from the newest end, clock would give up a fresh entry ahead
    /// of ones put in before it.
    free: u32,
    free_tail: u32,

    /// Open addressed, a power of two, holding slot indices
    table: Vec<u32>,

    /// Head of each segment's chain, hashed on the segment alone
    chains: Vec<u32>,

    /// Entries the shard is holding
    filled: usize,

    /// The slot the hand is standing on, which is the next one it looks at
    hand: usize,

    /// What the held entries weigh
    bytes: usize,
}

/// One shard, under its own mutex
struct Shard<V> {
    inner: Mutex<Inner<V>>,
}

/// A bounded cache over keys this volume issued
///
/// The budget is the whole hold's rather than a shard's, so what one entry may
/// weigh does not shrink with the shard count and the bound stays what the caller
/// asked for. A shard that has to make room gives up one of its own.
pub struct Hold<V> {
    budget: usize,
    bytes: AtomicUsize,
    shards: Box<[Shard<V>]>,
}

/// Shards a budget is worth splitting into, given what an entry typically weighs
///
/// A cache too small to give every shard a working set of its own is left whole,
/// which is what a test-sized bound and the fd cache's smallest settings ask for.
fn shards_for(budget: usize, typical: usize) -> usize {
    let mut shards = SHARDS;
    while shards > 1 && budget / shards < typical.max(1) * PER_SHARD {
        shards /= 2;
    }
    shards
}

impl<V: Clone> Hold<V> {
    /// A hold weighing at most this many bytes, split by what an entry weighs
    pub fn new(budget: usize, typical: usize) -> Hold<V> {
        let shards = shards_for(budget, typical);
        let mut built = Vec::with_capacity(shards);
        for _ in 0..shards {
            built.push(Shard {
                inner: Mutex::new(Inner::new()),
            });
        }
        Hold {
            budget,
            bytes: AtomicUsize::new(0),
            shards: built.into_boxed_slice(),
        }
    }

    /// What one entry may weigh and still be taken in
    ///
    /// One weighing more than the whole hold is turned away rather than emptying
    /// it: the caller keeps what it just read either way.
    pub fn share(&self) -> usize {
        self.budget
    }

    fn shard_of(&self, key: u64) -> &Shard<V> {
        let at = (mix(key) >> 32) as usize % self.shards.len();
        &self.shards[at]
    }

    /// What is held under this key, marked hot for the hand that next passes it
    pub fn get(&self, key: u64) -> Option<V> {
        let shard = self.shard_of(key);
        let mut inner = lock(&shard.inner);
        let at = inner.find(key)?;
        inner.slots[at].hot = true;
        inner.slots[at].val.clone()
    }

    /// Hold a value, giving up cold entries until it fits
    ///
    /// A key already held is left as it stands, which is what the caches this
    /// replaces did: two readers racing on the same block both hold a copy and
    /// only the first one's is kept.
    pub fn insert(&self, key: u64, value: V, weight: usize) {
        if weight > self.budget {
            return;
        }
        let shard = self.shard_of(key);
        let mut inner = lock(&shard.inner);
        if inner.find(key).is_some() {
            return;
        }
        while self.bytes.load(Ordering::Relaxed) + weight > self.budget {
            // A shard with nothing of its own left turns the entry away rather
            // than taking the hold past what it was given.
            match inner.evict() {
                Some(freed) => {
                    self.bytes.fetch_sub(freed, Ordering::Relaxed);
                }
                None => return,
            }
        }
        inner.put(key, value, weight);
        self.bytes.fetch_add(weight, Ordering::Relaxed);
    }

    /// Take a key out and hand back what it held
    pub fn take(&self, key: u64) -> Option<V> {
        let shard = self.shard_of(key);
        let mut inner = lock(&shard.inner);
        let at = inner.find(key)?;
        let held = inner.slots[at].val.clone();
        let freed = inner.drop_slot(at);
        self.bytes.fetch_sub(freed, Ordering::Relaxed);
        held
    }

    /// Give up every entry of one segment, by walking that segment's chain
    pub fn forget(&self, segment: SegmentId) {
        let number = segment.as_u32();
        for shard in self.shards.iter() {
            let mut inner = lock(&shard.inner);
            let freed = inner.forget(number);
            self.bytes.fetch_sub(freed, Ordering::Relaxed);
        }
    }

    /// Give up everything, for a reader rebuilding its view of the volume
    pub fn clear(&self) {
        for shard in self.shards.iter() {
            *lock(&shard.inner) = Inner::new();
        }
        self.bytes.store(0, Ordering::Relaxed);
    }

    /// What the held entries weigh
    pub fn bytes(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }

    /// Entries held across every shard
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| lock(&shard.inner).filled)
            .sum()
    }

    /// Whether the hold has nothing in it
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Table slots per entry held, so a probe run stays short
const SPREAD: usize = 2;

impl<V> Inner<V> {
    fn new() -> Inner<V> {
        Inner {
            slots: Vec::new(),
            free: NONE,
            free_tail: NONE,
            table: vec![NONE; 16],
            chains: vec![NONE; 16],
            filled: 0,
            hand: 0,
            bytes: 0,
        }
    }

    /// The slot a key is held in, found by probing from where it mixes to
    fn find(&self, key: u64) -> Option<usize> {
        let mask = self.table.len() - 1;
        let mut at = mix(key) as usize & mask;
        loop {
            match self.table[at] {
                NONE => return None,
                slot if self.slots[slot as usize].key == key => return Some(slot as usize),
                _ => at = (at + 1) & mask,
            }
        }
    }

    /// Where in the table a held key sits
    fn table_at(&self, key: u64) -> usize {
        let mask = self.table.len() - 1;
        let mut at = mix(key) as usize & mask;
        loop {
            let slot = self.table[at];
            if slot != NONE && self.slots[slot as usize].key == key {
                return at;
            }
            at = (at + 1) & mask;
        }
    }

    /// Take a key out of the table, shifting the run behind it back over the hole
    fn table_remove(&mut self, key: u64) {
        let mask = self.table.len() - 1;
        let mut hole = self.table_at(key);
        self.table[hole] = NONE;
        let mut probe = (hole + 1) & mask;
        loop {
            let slot = self.table[probe];
            if slot == NONE {
                return;
            }
            let home = mix(self.slots[slot as usize].key) as usize & mask;
            // The entry moves back only if the hole is no further from its home
            // than where it sits, which is what keeps every run contiguous.
            if (probe.wrapping_sub(hole)) & mask <= (probe.wrapping_sub(home)) & mask {
                self.table[hole] = slot;
                self.table[probe] = NONE;
                hole = probe;
            }
            probe = (probe + 1) & mask;
        }
    }

    fn table_insert(&mut self, slot: u32) {
        let mask = self.table.len() - 1;
        let mut at = mix(self.slots[slot as usize].key) as usize & mask;
        while self.table[at] != NONE {
            at = (at + 1) & mask;
        }
        self.table[at] = slot;
    }

    /// Double the table and the chains, rehashing what is held
    fn regrow(&mut self) {
        self.table = vec![NONE; self.table.len() * 2];
        self.chains = vec![NONE; self.chains.len() * 2];
        let held: Vec<u32> = (0..self.slots.len() as u32)
            .filter(|slot| self.slots[*slot as usize].val.is_some())
            .collect();
        for slot in held {
            self.table_insert(slot);
            self.chain_link(slot);
        }
    }

    fn chain_at(&self, key: u64) -> usize {
        mix(segment_key(SegmentId(segment_of(key)))) as usize & (self.chains.len() - 1)
    }

    fn chain_link(&mut self, slot: u32) {
        let bucket = self.chain_at(self.slots[slot as usize].key);
        self.slots[slot as usize].next = self.chains[bucket];
        self.chains[bucket] = slot;
    }

    fn chain_unlink(&mut self, slot: u32) {
        let bucket = self.chain_at(self.slots[slot as usize].key);
        let mut at = self.chains[bucket];
        if at == slot {
            self.chains[bucket] = self.slots[slot as usize].next;
            return;
        }
        while at != NONE {
            let next = self.slots[at as usize].next;
            if next == slot {
                self.slots[at as usize].next = self.slots[slot as usize].next;
                return;
            }
            at = next;
        }
    }

    /// Put a value in a vacant slot and link it into the table and its chain
    fn put(&mut self, key: u64, value: V, weight: usize) {
        if (self.filled + 1) * SPREAD >= self.table.len() {
            self.regrow();
        }
        let slot = match self.free {
            NONE => {
                self.slots.push(Slot {
                    key,
                    val: None,
                    weight: 0,
                    hot: false,
                    next: NONE,
                });
                (self.slots.len() - 1) as u32
            }
            free => {
                self.free = self.slots[free as usize].next;
                if self.free == NONE {
                    self.free_tail = NONE;
                }
                free
            }
        };
        let held = &mut self.slots[slot as usize];
        held.key = key;
        held.val = Some(value);
        held.weight = weight;
        // Entering cold, so a block read once on the way past leaves on the next
        // sweep and one read again stays.
        held.hot = false;
        self.bytes += weight;
        self.filled += 1;
        self.table_insert(slot);
        self.chain_link(slot);
    }

    /// Give a slot back, taking it out of the table and its chain
    fn drop_slot(&mut self, at: usize) -> usize {
        self.chain_unlink(at as u32);
        self.free_slot(at)
    }

    /// Give a slot back with its chain already rewired around it
    fn free_slot(&mut self, at: usize) -> usize {
        let key = self.slots[at].key;
        self.table_remove(key);
        let held = &mut self.slots[at];
        held.val = None;
        self.bytes -= held.weight;
        held.hot = false;
        let freed = std::mem::take(&mut self.slots[at].weight);
        self.slots[at].next = NONE;
        match self.free_tail {
            NONE => self.free = at as u32,
            tail => self.slots[tail as usize].next = at as u32,
        }
        self.free_tail = at as u32;
        self.filled -= 1;
        freed
    }

    /// Give up one entry the hand passed twice without a read in between
    ///
    /// A sweep where everything is hot clears every bit on the way, so the second
    /// time around takes the first entry it reaches and making room always does.
    fn evict(&mut self) -> Option<usize> {
        if self.filled == 0 {
            return None;
        }
        let slots = self.slots.len();
        for _ in 0..(2 * slots) {
            let at = self.hand;
            self.hand = (self.hand + 1) % slots;
            if self.slots[at].val.is_none() {
                continue;
            }
            if self.slots[at].hot {
                self.slots[at].hot = false;
                continue;
            }
            return Some(self.drop_slot(at));
        }
        None
    }

    /// Unlink every entry of one segment, walking that segment's chain alone
    ///
    /// The chain is rebuilt in the one pass that finds them, so retiring a segment
    /// costs its own entries rather than its entries times the chain they sit in.
    fn forget(&mut self, segment: u32) -> usize {
        let bucket = mix(segment_key(SegmentId(segment))) as usize & (self.chains.len() - 1);
        let mut at = self.chains[bucket];
        let mut kept = NONE;
        let mut going = Vec::new();
        while at != NONE {
            let slot = &self.slots[at as usize];
            let next = slot.next;
            match slot.val.is_some() && segment_of(slot.key) == segment {
                true => going.push(at as usize),
                false => {
                    self.slots[at as usize].next = kept;
                    kept = at;
                }
            }
            at = next;
        }
        self.chains[bucket] = kept;
        let mut freed = 0;
        for at in going {
            freed += self.free_slot(at);
        }
        freed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key_of(segment: u32, column: u8, block: usize) -> u64 {
        hold_key(SegmentId(segment), ColumnId(column), block)
    }

    // a packed key names one segment, column and block and nothing else
    #[test]
    fn keys_pack_apart() {
        assert_ne!(key_of(1, 0, 0), key_of(0, 1, 0));
        assert_ne!(key_of(0, 1, 0), key_of(0, 0, 1));
        assert_eq!(segment_of(key_of(9, 3, 77)), 9);
        assert_eq!(key_of(9, 3, 77), key_of(9, 3, 77));
        // The block field is the low end, so a segment's blocks share a chain.
        assert_eq!(segment_of(key_of(4, 255, MAX_BLOCK)), 4);
    }

    // what goes in comes back, and what is taken out does not
    #[test]
    fn holds_and_gives_up() {
        let hold: Hold<u64> = Hold::new(4_096, 16);
        hold.insert(key_of(1, 0, 0), 7, 16);

        assert_eq!(hold.get(key_of(1, 0, 0)), Some(7));
        assert_eq!(hold.get(key_of(1, 0, 1)), None);
        assert_eq!(hold.len(), 1);
        assert_eq!(hold.bytes(), 16);

        assert_eq!(hold.take(key_of(1, 0, 0)), Some(7));
        assert_eq!(hold.get(key_of(1, 0, 0)), None);
        assert_eq!(hold.bytes(), 0);
        assert!(hold.is_empty());
    }

    // an entry heavier than a shard is turned away rather than emptying one
    #[test]
    fn refuses_what_it_cannot_hold() {
        let hold: Hold<u64> = Hold::new(64, 64);
        hold.insert(key_of(1, 0, 0), 7, 65);

        assert!(hold.is_empty());
    }

    // the hand gives up what was not read since it last passed
    #[test]
    fn clock_keeps_what_is_read() {
        // One shard, so the eviction order is the one being checked.
        let hold: Hold<u64> = Hold::new(4, 4);
        for block in 0..4 {
            hold.insert(key_of(1, 0, block), block as u64, 1);
        }
        assert_eq!(hold.len(), 4);

        // Block two is read, so the sweep passes it and takes a cold one.
        assert_eq!(hold.get(key_of(1, 0, 2)), Some(2));
        hold.insert(key_of(1, 0, 4), 4, 1);

        assert_eq!(hold.len(), 4);
        assert_eq!(hold.get(key_of(1, 0, 2)), Some(2), "a read entry stayed");
        assert_eq!(hold.get(key_of(1, 0, 4)), Some(4), "the new entry landed");
    }

    // retiring a segment takes its entries and leaves the others standing
    #[test]
    fn forget_takes_one_segment() {
        let hold: Hold<u64> = Hold::new(64_000, 16);
        for segment in 1..4u32 {
            for block in 0..8 {
                hold.insert(key_of(segment, 0, block), u64::from(segment), 16);
            }
        }
        assert_eq!(hold.len(), 24);

        hold.forget(SegmentId(2));

        assert_eq!(hold.len(), 16);
        assert_eq!(hold.bytes(), 16 * 16);
        for block in 0..8 {
            assert_eq!(hold.get(key_of(2, 0, block)), None);
            assert_eq!(hold.get(key_of(1, 0, block)), Some(1));
            assert_eq!(hold.get(key_of(3, 0, block)), Some(3));
        }
    }

    // the table grows and everything held is still found afterwards
    #[test]
    fn regrows_without_losing_a_key() {
        let hold: Hold<u64> = Hold::new(1 << 20, 16);
        for block in 0..2_000 {
            hold.insert(key_of(1, 0, block), block as u64, 16);
        }
        for block in 0..2_000 {
            assert_eq!(hold.get(key_of(1, 0, block)), Some(block as u64));
        }
        assert_eq!(hold.len(), 2_000);

        // Taking half out leaves the other half findable through the shifted runs.
        for block in (0..2_000).step_by(2) {
            assert_eq!(hold.take(key_of(1, 0, block)), Some(block as u64));
        }
        for block in (1..2_000).step_by(2) {
            assert_eq!(hold.get(key_of(1, 0, block)), Some(block as u64));
        }
        assert_eq!(hold.len(), 1_000);
    }

    // a budget too small to give every shard a working set stays one shard
    #[test]
    fn small_budgets_stay_whole() {
        assert_eq!(shards_for(64, 64), 1);
        assert_eq!(shards_for(1 << 20, 4_096), 16);
        assert_eq!(shards_for(4, 1), 1);
    }
}
