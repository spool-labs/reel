//! A shard's table from a key to its newest row, whatever open segment holds it

use reel_core::column_id::ColumnId;
use reel_core::units::ByteCount;

use std::io::Result;

use crate::growing_list::GrowingList;
use crate::index::entry_slab::{EntryAddress, EntrySlab, NO_ENTRY};
use crate::index::key_hash::KeyHash;

/// The table refuses a new key past this share of its slots, in eighths
pub const LOAD_LIMIT_EIGHTHS: usize = 7;
/// The share above counts in eighths
pub const LOAD_LIMIT_SCALE: usize = 8;

/// One slot: the low bits of the key's hash and the newest row of the key
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TableSlot {
    pub tag: u32,
    pub head: EntryAddress,
}

/// An open-addressed table that grows: its slots live in one of two buffers, each reserved for the most slots the shard can use
/// and in memory only as far as the size in force. A grow rehashes the live buffer into the other at twice the size and gives the
/// old one's pages back, so nothing allocates and a probe never meets a page out of memory
#[derive(Debug)]
pub struct KeyTable {
    buffers: [GrowingList<TableSlot>; 2],
    live: usize,
    mask: usize,
    used: usize,
    /// The slots it starts with and shrinks back to
    first: usize,
    /// The most slots it can grow to
    most: usize,
    /// Rehashes so far, each a pass over every key under the shard's write lock
    rehashes: u64,
}

/// The most slots a table grows to: a grow into more rehashes enough keys to hold a shard's lock past a millisecond, so a larger
/// table starts whole. Measured on the Mac: the last grow into 2^19 slots held 0.3 to 0.6 ms, into 2^21 slots 1.2 to 2.7 ms
pub const MOST_GROWN_SLOTS: usize = 1 << 19;

/// A slot no key holds
const EMPTY_SLOT: TableSlot = TableSlot { tag: 0, head: NO_ENTRY };

impl KeyTable {
    /// Allocate a table with all of its most slots in memory
    pub fn new(capacity: usize) -> Result<Self> {
        Self::growing(capacity, capacity)
    }

    /// Reserve a table for this many slots with its first ones in memory, each rounded up to a power of two
    pub fn growing(capacity: usize, first: usize) -> Result<Self> {
        let most = capacity.next_power_of_two();
        let first = if most > MOST_GROWN_SLOTS { most } else { first.clamp(1, most).next_power_of_two() };
        let mut buffers = [GrowingList::reserved(most)?, GrowingList::reserved(most)?];
        fill_empty(&mut buffers[0], first)?;
        Ok(Self { buffers, live: 0, mask: first - 1, used: 0, first, most, rehashes: 0 })
    }

    #[inline]
    fn slots(&self) -> &[TableSlot] {
        &self.buffers[self.live]
    }

    fn slots_mut(&mut self) -> &mut [TableSlot] {
        &mut self.buffers[self.live]
    }

    /// Count the rehashes the table has run
    pub fn rehashes(&self) -> u64 {
        self.rehashes
    }

    /// Count the slots in force
    pub fn size(&self) -> usize {
        self.mask + 1
    }

    /// Check the table is past half the keys its size takes, which the reserve job grows ahead of
    pub fn wants_to_grow(&self) -> bool {
        self.size() < self.most && 2 * self.used * LOAD_LIMIT_SCALE >= self.size() * LOAD_LIMIT_EIGHTHS
    }

    /// Check the table can still grow
    pub fn can_grow(&self) -> bool {
        self.size() < self.most
    }

    /// Rehash every key into the other buffer at twice the size, and give the old buffer's pages back. The caller holds the shard's
    /// write lock, so no probe is under way
    pub fn grow(&mut self) -> Result<bool> {
        self.grow_to(self.size() * 2)
    }

    /// Rehash every key into the other buffer at the smallest size past this one that takes this many keys, up to the most, in
    /// one pass. The caller holds the shard's write lock
    pub fn grow_for(&mut self, rows: u64) -> Result<bool> {
        let mut size = self.size() * 2;
        while size < self.most && !Self::takes(size, self.used as u64 + rows) {
            size *= 2;
        }
        self.grow_to(size.min(self.most))
    }

    fn takes(size: usize, keys: u64) -> bool {
        keys.saturating_sub(1) * (LOAD_LIMIT_SCALE as u64) < (size * LOAD_LIMIT_EIGHTHS) as u64
    }

    fn grow_to(&mut self, size: usize) -> Result<bool> {
        if !self.can_grow() || size <= self.size() {
            return Ok(false);
        }
        let other = 1 - self.live;
        fill_empty(&mut self.buffers[other], size)?;
        let mask = size - 1;
        let (live, target) = if other == 1 {
            let (low, high) = self.buffers.split_at_mut(1);
            (&low[0], &mut high[0])
        } else {
            let (low, high) = self.buffers.split_at_mut(1);
            (&high[0], &mut low[0])
        };
        for slot in live.iter().filter(|slot| slot.head != NO_ENTRY) {
            let mut index = slot.tag as usize & mask;
            while target[index].head != NO_ENTRY {
                index = (index + 1) & mask;
            }
            target[index] = *slot;
        }
        let old = self.live;
        self.live = other;
        self.mask = mask;
        self.rehashes += 1;
        self.buffers[old].clear();
        self.buffers[old].give_back_past(0);
        Ok(true)
    }

    /// Go back to the first size once no key is in the table, and give the pages past it back
    pub fn shrink_if_empty(&mut self) -> Result<bool> {
        if self.used != 0 || self.size() == self.first {
            return Ok(false);
        }
        let first = self.first;
        let other = 1 - self.live;
        fill_empty(&mut self.buffers[other], first)?;
        let old = self.live;
        self.live = other;
        self.mask = first - 1;
        self.buffers[old].clear();
        self.buffers[old].give_back_past(0);
        Ok(true)
    }

    /// Read the heap this table holds: both buffers' pages in memory
    pub fn heap_bytes(&self) -> ByteCount {
        ByteCount((self.buffers[0].ready_bytes() + self.buffers[1].ready_bytes()) as u64)
    }

    /// Check a new key can go in, which also keeps every probe finite
    pub fn has_room(&self) -> bool {
        self.has_room_for(1)
    }

    /// Check this many new keys can go in
    pub fn has_room_for(&self, rows: u64) -> bool {
        Self::takes(self.size(), self.used as u64 + rows)
    }

    /// Count the keys an empty table takes once grown to its most
    pub fn most_rows(&self) -> u64 {
        (self.most * LOAD_LIMIT_EIGHTHS / LOAD_LIMIT_SCALE) as u64
    }

    /// Find the slot of a key, or the empty slot where it belongs
    pub fn probe(&self, hash: KeyHash, column: ColumnId, key: &[u8], slab: &EntrySlab) -> usize {
        let tag = hash.0 as u32;
        let mut index = tag as usize & self.mask;
        // the slots in force, bound once for the whole probe
        let slots = self.slots();
        loop {
            let slot = slots[index];
            if slot.head == NO_ENTRY {
                return index;
            }
            if slot.tag == tag && slab.column(slot.head) == column && slab.key(slot.head) == key {
                return index;
            }
            index = (index + 1) & self.mask;
        }
    }

    /// Find the slot whose newest row is this one, which needs no key compare
    pub fn find_head(&self, hash: KeyHash, head: EntryAddress) -> usize {
        let mut index = hash.0 as u32 as usize & self.mask;
        let slots = self.slots();
        loop {
            let slot = slots[index];
            if slot.head == head || slot.head == NO_ENTRY {
                return index;
            }
            index = (index + 1) & self.mask;
        }
    }

    /// Read the tag in a key's home slot, so a later probe of that slot finds it cached
    pub fn home_tag(&self, hash: KeyHash) -> u32 {
        self.slots()[hash.0 as u32 as usize & self.mask].tag
    }

    /// Read the newest row at a slot
    pub fn head(&self, index: usize) -> EntryAddress {
        self.slots()[index].head
    }

    /// Point a slot at a row
    pub fn set(&mut self, index: usize, hash: KeyHash, head: EntryAddress) {
        if self.slots()[index].head == NO_ENTRY {
            self.used += 1;
        }
        self.slots_mut()[index] = TableSlot {
            tag: hash.0 as u32,
            head,
        };
    }

    /// Empty a slot, moving later rows back so no probe breaks
    pub fn remove(&mut self, index: usize) {
        self.used -= 1;
        let mut hole = index;
        let mut next = (hole + 1) & self.mask;
        loop {
            let slot = self.slots()[next];
            if slot.head == NO_ENTRY {
                break;
            }
            let home = slot.tag as usize & self.mask;
            let distance_to_hole = hole.wrapping_sub(home) & self.mask;
            let distance_to_next = next.wrapping_sub(home) & self.mask;
            if distance_to_hole < distance_to_next {
                self.slots_mut()[hole] = slot;
                hole = next;
            }
            next = (next + 1) & self.mask;
        }
        self.slots_mut()[hole] = EMPTY_SLOT;
    }
}

/// Bring a buffer's first slots into memory, every one empty
fn fill_empty(buffer: &mut GrowingList<TableSlot>, size: usize) -> Result<()> {
    buffer.clear();
    buffer.make_ready(size)?;
    let is_in = buffer.extend_from(std::iter::repeat_n(EMPTY_SLOT, size));
    debug_assert!(is_in);
    Ok(())
}
