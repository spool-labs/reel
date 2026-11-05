//! One shard of the write index: a tree per open segment and one table over all of them

use std::cmp::Ordering;
use std::hint;

use reel_api::column_id::ColumnId;
use reel_api::direction::Direction;
use reel_api::units::{ByteCount, RowCount, WritePlace};

use crate::index::entry_slab::{ENTRY_ALIGNMENT, EntryAddress, EntrySlab, NO_ENTRY, PAGE_BYTES, PageCount, entry_size};
use crate::index::key_hash::{KeyHash, hash_key};
use crate::index::key_table::KeyTable;
use crate::index::node_pool::{NODE_FANOUT, NodeCount, NodePool};
use crate::index::open_slots::{OpenSlot, OpenSlots};
use crate::index::seek_target::SeekTarget;
use crate::index::shard_cursor::{MAX_CURSOR_STEPS, ShardCursor, TreeCursor};
use crate::index::write_row::{WriteLookup, WriteRow};
use crate::index::write_tree::{MAX_NEW_NODES, TreePosition, WriteTree, entry_at, step_back, step_forward};
use crate::row_kind::RowKind;

/// A cleanup takes this many rows at a time, so their cold reads overlap
pub const CLEAN_RUN_ROWS: usize = 16;

/// Sizes of one shard's pools, fixed at open
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardSizing {
    pub pages: PageCount,
    pub nodes: NodeCount,
    pub table_slots: usize,
    pub open_slots: usize,
}

/// What a batch asks of one shard
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ShardNeed {
    pub rows: u64,
    pub entry_bytes: u64,
    pub largest_entry: u64,
}

/// Room batches hold in a shard until their rows are in
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ShardHold {
    pub rows: u64,
    pub pages: PageCount,
}

impl ShardNeed {
    /// Count one more row of this key length
    pub fn add(&mut self, key_length: usize) {
        let size = entry_size(key_length) as u64;
        self.rows += 1;
        self.entry_bytes += size;
        self.largest_entry = self.largest_entry.max(size);
    }

    /// Count the pages these rows can take: a page stops short only of an entry that doesn't fit
    pub fn pages(&self) -> PageCount {
        if self.rows == 0 {
            return PageCount(0);
        }
        let filled = PAGE_BYTES as u64 + ENTRY_ALIGNMENT as u64 - self.largest_entry;
        PageCount(self.entry_bytes.div_ceil(filled) as u32)
    }
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
    pub held: ShardHold,
}

impl WriteShard {
    /// Allocate and touch every pool
    pub fn new(sizing: &ShardSizing) -> Self {
        Self {
            nodes: NodePool::new(sizing.nodes),
            slab: EntrySlab::new(sizing.pages),
            table: KeyTable::new(sizing.table_slots),
            trees: vec![WriteTree::empty(); sizing.open_slots],
            held: ShardHold::default(),
        }
    }

    /// Read the heap this shard holds
    pub fn heap_bytes(&self) -> ByteCount {
        let trees = self.trees.capacity() * size_of::<WriteTree>();
        let total = self.nodes.heap_bytes().0 + self.slab.heap_bytes().0 + self.table.heap_bytes().0 + trees as u64;
        ByteCount(total)
    }

    /// Check the pools can take one more row of this key length in a slot
    pub fn has_room(&self, slot: OpenSlot, key_length: usize) -> bool {
        let tree = &self.trees[slot.0 as usize];
        self.nodes.free_count >= MAX_NEW_NODES
            && self.table.has_room_for(self.held.rows + 1)
            && self.slab.has_room(&tree.pages, key_length, self.held.pages)
            && (self.held.rows == 0 || self.rows_fit_nodes(self.held.rows + 1))
    }

    /// Count the rows this shard's nodes can always hold: every node but a root is at least half full
    pub fn node_row_limit(&self) -> u64 {
        let spare = self.trees.len() * MAX_NEW_NODES.0 as usize;
        let usable = self.nodes.nodes.len().saturating_sub(spare) as u64;
        usable * (NODE_FANOUT as u64 / 2 - 1)
    }

    /// Check the nodes can take this many more rows beside the rows every tree holds
    pub fn rows_fit_nodes(&self, rows: u64) -> bool {
        let held: u64 = self.trees.iter().map(|tree| tree.row_count.0).sum();
        held + rows <= self.node_row_limit()
    }

    /// Hold room for a batch's rows, or hold nothing
    pub fn hold_room(&mut self, need: &ShardNeed) -> bool {
        if need.rows == 0 {
            return true;
        }
        let rows = self.held.rows + need.rows;
        let pages = PageCount(self.held.pages.0 + need.pages().0);
        if !self.table.has_room_for(rows) || !self.slab.has_free_pages(pages) || !self.rows_fit_nodes(rows) {
            return false;
        }
        self.held = ShardHold { rows, pages };
        true
    }

    /// Give back what a batch held
    pub fn release_room(&mut self, need: &ShardNeed) {
        self.held.rows -= need.rows;
        self.held.pages = PageCount(self.held.pages.0 - need.pages().0);
    }

    /// Check an empty shard could hold a batch's rows
    pub fn could_hold(&self, need: &ShardNeed) -> bool {
        need.rows <= self.table.most_rows()
            && need.rows <= self.node_row_limit()
            && need.pages() <= self.slab.total_pages()
    }

    /// Add a row, or return false when a pool ran out
    pub fn insert(&mut self, hash: KeyHash, column: ColumnId, key: &[u8], row: &WriteRow) -> bool {
        if !self.has_room(row.slot, key.len()) {
            return false;
        }

        self.link_row(hash, column, key, row, false)
    }

    /// Add a row of a batch that holds its room
    pub fn insert_held(&mut self, hash: KeyHash, column: ColumnId, key: &[u8], row: &WriteRow) -> bool {
        self.link_row(hash, column, key, row, true)
    }

    /// Store a row and link it, keeping a batch's rows on their own pages so a put beside it can't use up their room
    pub fn link_row(&mut self, hash: KeyHash, column: ColumnId, key: &[u8], row: &WriteRow, is_held: bool) -> bool {
        let tree = &mut self.trees[row.slot.0 as usize];
        tree.segment = row.segment;
        let chain = if is_held { &mut tree.batch_pages } else { &mut tree.pages };
        let Some(address) = self.slab.store(chain, column, key, row) else {
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
                row: self.row_of(head),
                should_ask_footers: self.slab.has_newer_sealed(head),
            };
        }
        self.search_trees(column, key, read_point, view)
    }

    /// Read a row with the segment of the tree that holds it
    pub fn row_of(&self, entry: EntryAddress) -> Option<WriteRow> {
        let mut row = self.slab.row(entry)?;
        row.segment = self.trees[row.slot.0 as usize].segment;
        Some(row)
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
            row: self.row_of(entry),
            should_ask_footers: newest_closed > self.slab.place(entry) || self.slab.has_newer_sealed(entry),
        }
    }

    /// Find a slot's last row in a column
    pub fn last_of_column(&self, slot: OpenSlot, column: ColumnId) -> Option<EntryAddress> {
        let tree = &self.trees[slot.0 as usize];
        let position = match column.0.checked_add(1) {
            Some(next) => {
                let target = SeekTarget::new(ColumnId(next), &[], WritePlace(u64::MAX));
                tree.seek_before(&self.nodes, &self.slab, &target)?
            },
            None => tree.last_position(&self.nodes)?,
        };
        let entry = entry_at(&self.nodes, position);
        if self.slab.column(entry) == column {
            return Some(entry);
        }
        None
    }

    /// Check no newer version of a key is in any open segment or has sealed away
    pub fn is_newest_version(&self, hash: KeyHash, column: ColumnId, key: &[u8], place: WritePlace) -> bool {
        let head = self.table.head(self.table.probe(hash, column, key, &self.slab));
        head != NO_ENTRY && self.slab.place(head) == place && !self.slab.has_newer_sealed(head)
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

        let mut left = limit.0;
        while left > 0 && position.is_some() {
            let mut entries = [NO_ENTRY; CLEAN_RUN_ROWS];
            let mut count = 0;
            while count < CLEAN_RUN_ROWS
                && (count as u64) < left
                && let Some(current) = position
            {
                entries[count] = entry_at(&self.nodes, current);
                position = step_forward(&self.nodes, current);
                count += 1;
            }
            self.unlink_run(&entries[..count]);
            left -= count as u64;
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

    /// Take a run of rows out of their chains. Each row's cold reads come first, side by side, so they overlap
    pub fn unlink_run(&mut self, entries: &[EntryAddress]) {
        let mut hashes = [KeyHash::default(); CLEAN_RUN_ROWS];
        let mut touched = 0;
        for (hash, &entry) in hashes.iter_mut().zip(entries) {
            *hash = hash_key(self.slab.column(entry), self.slab.key(entry));
            touched ^= self.table.home_tag(*hash);
        }
        hint::black_box(touched);
        for (&entry, &hash) in entries.iter().zip(&hashes) {
            self.unlink(entry, hash);
        }
    }

    /// Take one row out of its key's chain, and leave a mark on the next older version
    pub fn unlink(&mut self, address: EntryAddress, hash: KeyHash) {
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

        let index = self.table.find_head(hash, address);
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
