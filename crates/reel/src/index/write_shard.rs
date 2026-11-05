//! One shard of the write index: a tree per open segment and one table over all of them

use std::cmp::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::direction::Direction;
use reel_api::units::{ByteCount, RowCount, WritePlace};

use crate::index::entry_slab::{EntryAddress, EntrySlab, NO_ENTRY, PageCount};
use crate::index::key_hash::{KeyHash, hash_key};
use crate::index::key_table::KeyTable;
use crate::index::node_pool::{NodeCount, NodePool};
use crate::index::open_slots::{OpenSlot, OpenSlots};
use crate::index::seek_target::SeekTarget;
use crate::index::write_row::{WriteLookup, WriteRow};
use crate::index::write_tree::{MAX_NEW_NODES, TreePosition, WriteTree, entry_at, step_forward};
use crate::row_kind::RowKind;

/// Sizes of one shard's pools, fixed at open
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardSizing {
    pub pages: PageCount,
    pub nodes: NodeCount,
    pub table_slots: usize,
    pub open_slots: usize,
}

/// Where a walk goes next
#[derive(Clone, Copy, Debug)]
pub struct WalkBound<'key> {
    pub column: ColumnId,
    pub key: &'key [u8],
    pub is_inclusive: bool,
    pub direction: Direction,
}

/// A shard's pools and indexes
#[derive(Debug)]
pub struct WriteShard {
    pub nodes: NodePool,
    pub slab: EntrySlab,
    pub table: KeyTable,
    pub trees: Vec<WriteTree>,
}

impl WriteShard {
    /// Allocate and touch every pool
    pub fn new(sizing: &ShardSizing) -> Self {
        Self {
            nodes: NodePool::new(sizing.nodes),
            slab: EntrySlab::new(sizing.pages),
            table: KeyTable::new(sizing.table_slots),
            trees: vec![WriteTree::empty(); sizing.open_slots],
        }
    }

    /// Read the heap this shard holds
    pub fn heap_bytes(&self) -> ByteCount {
        let trees = self.trees.capacity() * size_of::<WriteTree>();
        let total = self.nodes.heap_bytes().0 + self.slab.heap_bytes().0 + self.table.heap_bytes().0 + trees as u64;
        ByteCount(total)
    }

    /// Add a row, or return false when a pool ran out
    pub fn insert(&mut self, hash: KeyHash, column: ColumnId, key: &[u8], row: &WriteRow) -> bool {
        let tree = &mut self.trees[row.slot.0 as usize];
        if self.nodes.free_count < MAX_NEW_NODES || !self.slab.has_room(&tree.pages, key.len()) {
            return false;
        }
        let Some(address) = self.slab.store(&mut tree.pages, column, key, row) else {
            return false;
        };

        let target = SeekTarget::new(column, key, row.place);
        tree.link(&mut self.nodes, &self.slab, &target, address);
        self.link_version(hash, column, key, address, row.place);
        true
    }

    /// Put a row in its key's chain, which stays newest first whatever order writers finish in
    pub fn link_version(
        &mut self,
        hash: KeyHash,
        column: ColumnId,
        key: &[u8],
        address: EntryAddress,
        place: WritePlace,
    ) {
        let index = self.table.probe(hash, column, key, &self.slab);
        let head = self.table.head(index);
        if head == NO_ENTRY || self.slab.place(head) < place {
            self.slab.set_older(address, head);
            self.table.set(index, hash, address);
            return;
        }

        let mut newer = head;
        loop {
            let older = self.slab.older(newer);
            if older == NO_ENTRY || self.slab.place(older) < place {
                self.slab.set_older(address, older);
                self.slab.set_older(newer, address);
                return;
            }
            newer = older;
        }
    }

    /// Find the newest row a reader at a place can see, counting only open slots in its view
    pub fn lookup(
        &self,
        hash: KeyHash,
        column: ColumnId,
        key: &[u8],
        read_point: WritePlace,
        view: OpenSlots,
    ) -> WriteLookup {
        let head = self.table.head(self.table.probe(hash, column, key, &self.slab));
        self.walk_versions(head, read_point, view)
    }

    /// Walk a key's versions, newest first, for the first one the reader counts
    pub fn walk_versions(&self, head: EntryAddress, read_point: WritePlace, view: OpenSlots) -> WriteLookup {
        let mut skipped_sealed = false;
        let mut entry = head;
        while entry != NO_ENTRY {
            if self.slab.place(entry) <= read_point {
                if !view.contains(self.slab.slot(entry)) {
                    skipped_sealed = true;
                } else {
                    return WriteLookup {
                        row: self.slab.row(entry),
                        should_ask_footers: skipped_sealed || self.slab.has_newer_sealed(entry),
                    };
                }
            }
            entry = self.slab.older(entry);
        }

        WriteLookup {
            row: None,
            should_ask_footers: true,
        }
    }

    /// Check whether a key's newest row is a visible grave in an open slot
    pub fn is_newest_grave(
        &self,
        hash: KeyHash,
        column: ColumnId,
        key: &[u8],
        visible: WritePlace,
        view: OpenSlots,
    ) -> bool {
        let head = self.table.head(self.table.probe(hash, column, key, &self.slab));
        if head == NO_ENTRY || self.slab.has_newer_sealed(head) {
            return false;
        }
        let Some(row) = self.slab.row(head) else {
            return false;
        };
        row.kind == RowKind::Grave && row.place <= visible && view.contains(row.slot)
    }

    /// Take up to a limit of a closed slot's rows out of the table, and free the slot at the end
    pub fn clean_slot(&mut self, slot: OpenSlot, limit: RowCount) -> bool {
        let tree = self.trees[slot.0 as usize];
        let mut position = match tree.cleanup {
            Some(position) => Some(position),
            None => tree.first_position(&self.nodes),
        };

        for _row in 0..limit.0 {
            let Some(current) = position else {
                break;
            };
            self.unlink(entry_at(&self.nodes, current));
            position = step_forward(&self.nodes, current);
        }

        match position {
            Some(position) => {
                self.trees[slot.0 as usize].cleanup = Some(position);
                false
            },
            None => {
                let tree = &mut self.trees[slot.0 as usize];
                tree.clear(&mut self.nodes, &mut self.slab);
                true
            },
        }
    }

    /// Take one row out of its key's chain, and leave a mark on the next older version
    pub fn unlink(&mut self, address: EntryAddress) {
        let column = self.slab.column(address);
        let hash = hash_key(column, self.slab.key(address));
        let index = self.table.probe(hash, column, self.slab.key(address), &self.slab);
        let older = self.slab.older(address);
        if older != NO_ENTRY {
            self.slab.mark_newer_sealed(older);
        }

        let head = self.table.head(index);
        if head == address {
            if older == NO_ENTRY {
                self.table.remove(index);
            } else {
                self.table.set(index, hash, older);
            }
            return;
        }

        let mut newer = head;
        while newer != NO_ENTRY {
            let next = self.slab.older(newer);
            if next == address {
                self.slab.set_older(newer, older);
                return;
            }
            newer = next;
        }
    }

    /// Find the next key past a bound in any open tree of the view, and copy it out
    pub fn next_key(&self, bound: &WalkBound, view: OpenSlots, key: &mut Vec<u8>) -> bool {
        let mut best: Option<EntryAddress> = None;
        for (index, tree) in self.trees.iter().enumerate() {
            if !view.contains(OpenSlot(index as u8)) {
                continue;
            }
            let Some(position) = seek_bound(tree, &self.nodes, &self.slab, bound) else {
                continue;
            };
            let entry = entry_at(&self.nodes, position);
            if self.slab.column(entry) != bound.column {
                continue;
            }
            best = Some(self.closer(best, entry, bound.direction));
        }

        match best {
            Some(entry) => {
                key.clear();
                key.extend_from_slice(self.slab.key(entry));
                true
            },
            None => false,
        }
    }

    /// Pick whichever row comes first in a walk's direction
    pub fn closer(&self, best: Option<EntryAddress>, entry: EntryAddress, direction: Direction) -> EntryAddress {
        let Some(current) = best else {
            return entry;
        };
        let order = self.slab.key(entry).cmp(self.slab.key(current));
        let is_closer = match direction {
            Direction::Ascending => order == Ordering::Less,
            Direction::Descending => order == Ordering::Greater,
        };
        if is_closer { entry } else { current }
    }
}

/// Find a tree's first row past a walk's bound
pub fn seek_bound(tree: &WriteTree, nodes: &NodePool, slab: &EntrySlab, bound: &WalkBound) -> Option<TreePosition> {
    match (bound.direction, bound.is_inclusive) {
        // versions of a key sort newest first, so the lowest place sits after them all
        (Direction::Ascending, true) => tree.seek(nodes, slab, &SeekTarget::new(bound.column, bound.key, WritePlace(u64::MAX))),
        (Direction::Ascending, false) => tree.seek(nodes, slab, &SeekTarget::new(bound.column, bound.key, WritePlace(0))),
        (Direction::Descending, true) => tree.seek_before(nodes, slab, &SeekTarget::new(bound.column, bound.key, WritePlace(0))),
        (Direction::Descending, false) => {
            tree.seek_before(nodes, slab, &SeekTarget::new(bound.column, bound.key, WritePlace(u64::MAX)))
        },
    }
}
