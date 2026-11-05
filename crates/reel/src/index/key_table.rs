//! A shard's table from a key to its newest row, whatever open segment holds it

use reel_api::column_id::ColumnId;
use reel_api::units::ByteCount;

use crate::index::entry_slab::{EntryAddress, EntrySlab, NO_ENTRY};
use crate::index::key_hash::KeyHash;

/// One slot: the low bits of the key's hash and the newest row of the key
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TableSlot {
    pub tag: u32,
    pub head: EntryAddress,
}

/// An open-addressed table of fixed size
#[derive(Debug)]
pub struct KeyTable {
    slots: Vec<TableSlot>,
    mask: usize,
}

impl KeyTable {
    /// Allocate and touch a table of at least this many slots
    pub fn new(capacity: usize) -> Self {
        let size = capacity.next_power_of_two();
        let empty = TableSlot {
            tag: 0,
            head: NO_ENTRY,
        };
        Self {
            slots: vec![empty; size],
            mask: size - 1,
        }
    }

    /// Read the heap this table holds
    pub fn heap_bytes(&self) -> ByteCount {
        ByteCount((self.slots.capacity() * size_of::<TableSlot>()) as u64)
    }

    /// Find the slot of a key, or the empty slot where it belongs
    pub fn probe(&self, hash: KeyHash, column: ColumnId, key: &[u8], slab: &EntrySlab) -> usize {
        let tag = hash.0 as u32;
        let mut index = tag as usize & self.mask;
        loop {
            let slot = self.slots[index];
            if slot.head == NO_ENTRY {
                return index;
            }
            if slot.tag == tag && slab.column(slot.head) == column && slab.key(slot.head) == key {
                return index;
            }
            index = (index + 1) & self.mask;
        }
    }

    /// Read the newest row at a slot
    pub fn head(&self, index: usize) -> EntryAddress {
        self.slots[index].head
    }

    /// Point a slot at a row
    pub fn set(&mut self, index: usize, hash: KeyHash, head: EntryAddress) {
        self.slots[index] = TableSlot {
            tag: hash.0 as u32,
            head,
        };
    }

    /// Empty a slot, moving later rows back so no probe breaks
    pub fn remove(&mut self, index: usize) {
        let mut hole = index;
        let mut next = (hole + 1) & self.mask;
        loop {
            let slot = self.slots[next];
            if slot.head == NO_ENTRY {
                break;
            }
            let home = slot.tag as usize & self.mask;
            let distance_to_hole = hole.wrapping_sub(home) & self.mask;
            let distance_to_next = next.wrapping_sub(home) & self.mask;
            if distance_to_hole < distance_to_next {
                self.slots[hole] = slot;
                hole = next;
            }
            next = (next + 1) & self.mask;
        }
        self.slots[hole] = TableSlot {
            tag: 0,
            head: NO_ENTRY,
        };
    }
}
