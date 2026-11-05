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
use crate::index::shard_cursor::{MAX_CURSOR_STEPS, ShardCursor, TreeCursor};
use crate::index::write_row::{WriteLookup, WriteRow};
use crate::index::write_tree::{MAX_NEW_NODES, TreePosition, WriteTree, entry_at, step_back, step_forward};
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
        let has_pools = self.nodes.free_count >= MAX_NEW_NODES && self.table.has_room();
        if !has_pools || !self.slab.has_room(&tree.pages, key.len()) {
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
            if head != NO_ENTRY {
                self.slab.set_newer(head, address);
            }
            self.table.set(index, hash, address);
            return;
        }

        // only writers still in flight land behind the head, so this walk stays short
        let mut newer = head;
        loop {
            let older = self.slab.older(newer);
            if older == NO_ENTRY || self.slab.place(older) < place {
                self.slab.set_older(address, older);
                self.slab.set_newer(address, newer);
                self.slab.set_older(newer, address);
                if older != NO_ENTRY {
                    self.slab.set_newer(older, address);
                }
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
        if head == NO_ENTRY {
            return WriteLookup {
                row: None,
                should_ask_footers: true,
            };
        }

        if self.slab.place(head) <= read_point && view.contains(self.slab.slot(head)) {
            return WriteLookup {
                row: self.slab.row(head),
                should_ask_footers: self.slab.has_newer_sealed(head),
            };
        }
        self.search_trees(column, key, read_point, view)
    }

    /// Find a key's newest version at a read point with one seek per tree, whatever the versions
    pub fn search_trees(
        &self,
        column: ColumnId,
        key: &[u8],
        read_point: WritePlace,
        view: OpenSlots,
    ) -> WriteLookup {
        let target = SeekTarget::new(column, key, read_point);
        let mut newest_open: Option<EntryAddress> = None;
        let mut newest_closed = WritePlace(0);

        for (index, tree) in self.trees.iter().enumerate() {
            let Some(position) = tree.seek(&self.nodes, &self.slab, &target) else {
                continue;
            };
            let entry = entry_at(&self.nodes, position);
            if self.slab.column(entry) != column || self.slab.key(entry) != key {
                continue;
            }

            let place = self.slab.place(entry);
            if !view.contains(OpenSlot(index as u8)) {
                newest_closed = newest_closed.max(place);
            } else if newest_open.is_none_or(|best| self.slab.place(best) < place) {
                newest_open = Some(entry);
            }
        }

        let Some(entry) = newest_open else {
            return WriteLookup {
                row: None,
                should_ask_footers: true,
            };
        };
        WriteLookup {
            row: self.slab.row(entry),
            should_ask_footers: newest_closed > self.slab.place(entry) || self.slab.has_newer_sealed(entry),
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
        let older = self.slab.older(address);
        let newer = self.slab.newer(address);
        if older != NO_ENTRY {
            self.slab.mark_newer_sealed(older);
            self.slab.set_newer(older, newer);
        }
        if newer != NO_ENTRY {
            self.slab.set_older(newer, older);
            return;
        }

        let column = self.slab.column(address);
        let hash = hash_key(column, self.slab.key(address));
        let index = self.table.probe(hash, column, self.slab.key(address), &self.slab);
        if older == NO_ENTRY {
            self.table.remove(index);
        } else {
            self.table.set(index, hash, older);
        }
    }

    /// Find the next key past a bound in the view's open trees, stepping where a cursor still stands
    pub fn next_key(
        &self,
        cursor: &mut ShardCursor,
        bound: &WalkBound,
        view: OpenSlots,
        key: &mut Vec<u8>,
    ) -> bool {
        let mut best: Option<EntryAddress> = None;
        for (index, tree) in self.trees.iter().enumerate() {
            if !view.contains(OpenSlot(index as u8)) {
                continue;
            }
            let Some(entry) = self.place_cursor(&mut cursor.trees[index], tree, bound) else {
                continue;
            };
            if self.slab.column(entry) == bound.column {
                best = Some(self.closer(best, entry, bound.direction));
            }
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

    /// Bring one tree's cursor to its first row past the bound
    pub fn place_cursor(&self, cursor: &mut TreeCursor, tree: &WriteTree, bound: &WalkBound) -> Option<EntryAddress> {
        if cursor.is_placed && self.is_cursor_standing(cursor) {
            let mut position = cursor.position?;
            for _step in 0..MAX_CURSOR_STEPS {
                let entry = entry_at(&self.nodes, position);
                if self.is_past_bound(entry, bound) {
                    cursor.position = Some(position);
                    cursor.version = self.nodes.node(position.leaf).version;
                    return Some(entry);
                }
                let next = match bound.direction {
                    Direction::Ascending => step_forward(&self.nodes, position),
                    Direction::Descending => step_back(&self.nodes, position),
                };
                let Some(next) = next else {
                    cursor.position = None;
                    return None;
                };
                position = next;
            }
        }

        let position = seek_bound(tree, &self.nodes, &self.slab, bound);
        cursor.is_placed = true;
        cursor.position = position;
        let position = position?;
        cursor.version = self.nodes.node(position.leaf).version;
        Some(entry_at(&self.nodes, position))
    }

    /// Check a cursor's leaf hasn't changed since it stood there
    pub fn is_cursor_standing(&self, cursor: &TreeCursor) -> bool {
        match cursor.position {
            Some(position) => self.nodes.node(position.leaf).version == cursor.version,
            None => true,
        }
    }

    /// Check a row lies past a walk's bound
    pub fn is_past_bound(&self, entry: EntryAddress, bound: &WalkBound) -> bool {
        let order = self
            .slab
            .column(entry)
            .cmp(&bound.column)
            .then_with(|| self.slab.key(entry).cmp(bound.key));
        match (bound.direction, bound.is_inclusive) {
            (Direction::Ascending, true) => order != Ordering::Less,
            (Direction::Ascending, false) => order == Ordering::Greater,
            (Direction::Descending, true) => order != Ordering::Greater,
            (Direction::Descending, false) => order == Ordering::Less,
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
