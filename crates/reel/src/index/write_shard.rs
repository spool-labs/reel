//! One shard of the write index: a tree per open segment and one table over all of them

use std::cmp::Ordering;
use std::hint;
use std::io::Result;

use reel_core::column_id::ColumnId;
use reel_core::direction::Direction;
use reel_core::units::{ByteCount, RowCount, WritePlace};

use crate::index::entry_slab::{ENTRY_ALIGNMENT, EntryAddress, EntrySlab, MOST_INLINE_VALUE, NO_ENTRY, PAGE_BYTES, PageCount, PageIndex, entry_size, inline_entry_size};
use crate::index::key_hash::{KeyHash, hash_key};
use crate::index::key_table::{KeyTable, TableSlot};
use crate::index::write_index::ShardCounts;
use crate::index::node_pool::{NO_NODE, NODE_FANOUT, NODE_PAGE_BYTES, NODES_PER_PAGE, NodeCount, NodeIndex, NodePool};
use crate::index::open_slots::{MAX_OPEN_SLOTS, OpenSlot, OpenSlots};
use crate::index::seek_target::SeekTarget;
use crate::index::shard_cursor::{MAX_CURSOR_STEPS, ShardCursor, TreeCursor};
use crate::index::write_row::{WriteLookup, WriteRow};
use crate::index::write_tree::{
    MAX_NEW_NODES, TreePosition, WriteTree, child_index, entry_at, lower_bound, normalize, step_back, step_forward,
};
use crate::prefetch::fetch_lines;
use crate::row_kind::RowKind;

/// A cleanup takes this many rows at a time, so their cold reads overlap
pub const CLEAN_RUN_ROWS: usize = 16;

/// The part of a shard's slab kept values may take: shard_for_rows adds a quarter to the pages rows need, a fifth of the whole
pub const KEPT_SHARE_DIVISOR: u64 = 5;

/// Sizes of one shard's pools, fixed at open
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardSizing {
    pub pages: PageCount,
    pub nodes: NodeCount,
    pub table_slots: usize,
    pub open_slots: usize,
    /// Buckets of the shard's open counts, so a scan can pass a shard with no open key near it
    pub buckets: usize,
}

impl ShardSizing {
    /// Read the heap a shard of this sizing allocates, as heap_bytes reads it once built
    pub fn heap_bytes(&self) -> ByteCount {
        let pages = u64::from(self.pages.0) * (PAGE_BYTES + size_of::<PageIndex>()) as u64;
        // nodes sit several to a page, and the pool keeps a list of the pages it took
        let node_pages = u64::from(self.nodes.0.div_ceil(NODES_PER_PAGE));
        let nodes = node_pages * (NODE_PAGE_BYTES + size_of::<u32>()) as u64;
        // the table lives in a mapping of its own, whole system pages of it
        let table = (self.table_slots.next_power_of_two() * size_of::<TableSlot>()).next_multiple_of(crate::region::page_bytes()) as u64;
        let trees = (self.open_slots * size_of::<WriteTree>()) as u64;
        ByteCount(pages + nodes + table + trees + self.buckets.max(1).next_power_of_two() as u64)
    }
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
    /// The rows a batch found the key table short of, which the reserve job grows the table to fit
    pub wanted_rows: u64,
}

impl WriteShard {
    /// Build every pool with all of it in memory
    pub fn new(sizing: &ShardSizing) -> Result<Self> {
        Self::growing(sizing, sizing.pages)
    }

    /// Build every pool, with the slab's first pages in memory and the rest reserved for it to grow into
    pub fn growing(sizing: &ShardSizing, first_pages: PageCount) -> Result<Self> {
        // the nodes start in the slab's proportion, past the spare every tree keeps for its splits, and the job grows both
        let first_nodes = (u64::from(sizing.nodes.0) * u64::from(first_pages.0)).div_ceil(u64::from(sizing.pages.0.max(1))) as u32;
        let spare = MAX_NEW_NODES.0 * sizing.open_slots as u32;
        Ok(Self {
            nodes: NodePool::growing(sizing.nodes, NodeCount(first_nodes + spare))?,
            slab: EntrySlab::growing(sizing.pages, first_pages)?,
            // the table starts in the slab's proportion too, and the reserve job grows it ahead of its keys
            table: KeyTable::growing(sizing.table_slots, (sizing.table_slots * first_pages.0 as usize).div_ceil(sizing.pages.0.max(1) as usize))?,
            trees: vec![WriteTree::empty(); sizing.open_slots],
            held: ShardHold::default(),
            wanted_rows: 0,
        })
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
        self.has_nodes()
            && self.table.has_room_for(self.held.rows + 1)
            && self.slab.has_room(&tree.pages, key_length, self.held.pages)
            && (self.held.rows == 0 || self.rows_fit_nodes(self.held.rows + 1))
    }

    /// Check the slab is what lacks room and it can still grow, so a writer waits for its next part instead of sealing early
    pub fn waits_for_slab(&self) -> bool {
        let room = self.slab.room();
        let is_slab_growing = self.slab.total_pages() < self.slab.most_pages() && room.can_grow();
        // a writer short of nodes in memory waits for the job's next step as it does for the slab's, and seals early only once
        // neither can grow
        let are_nodes_short = !self.has_nodes() || !self.rows_fit_nodes(self.held.rows + 1);
        let is_nodes_growing = are_nodes_short && self.nodes.can_grow();
        // a table short of slots waits for the job's grow too
        let is_table_short = !self.table.has_room_for(self.held.rows + 1);
        let is_table_growing = is_table_short && self.table.can_grow();
        // every pool that is short can grow, and one of them is growing
        let can_wait = (!is_table_short || is_table_growing) && (!are_nodes_short || is_nodes_growing);
        can_wait && (is_table_growing || is_nodes_growing || is_slab_growing)
    }

    /// Check enough nodes are in memory for a row's splits, and ask the job for more when not
    pub fn has_nodes(&self) -> bool {
        let has = self.nodes.free_count() >= MAX_NEW_NODES;
        if !has {
            self.nodes.note_short();
        }
        has
    }

    /// Count the rows this many nodes can always hold beside the trees' spares: every node but a root is at least half full
    fn rows_for_nodes(&self, nodes: NodeCount) -> u64 {
        let spare = self.trees.len() * MAX_NEW_NODES.0 as usize;
        let usable = (nodes.0 as usize).saturating_sub(spare) as u64;
        usable * (NODE_FANOUT as u64 / 2 - 1)
    }

    /// Count the rows the shard's nodes can hold once the pool has grown to its most
    pub fn node_row_limit(&self) -> u64 {
        self.rows_for_nodes(self.nodes.capacity())
    }

    /// Check the nodes in memory can take this many more rows beside the rows every tree holds, so a row or a batch never runs out
    /// of nodes midway, and ask the job for more when they can't
    pub fn rows_fit_nodes(&self, rows: u64) -> bool {
        let held: u64 = self.trees.iter().map(|tree| tree.row_count.0).sum();
        let fits = held + rows <= self.rows_for_nodes(self.nodes.in_memory());
        if !fits {
            self.nodes.note_short();
        }
        fits
    }

    /// Hold room for a batch's rows, or hold nothing
    pub fn hold_room(&mut self, need: &ShardNeed) -> bool {
        if need.rows == 0 {
            return true;
        }
        let rows = self.held.rows + need.rows;
        let pages = PageCount(self.held.pages.0 + need.pages().0);
        if !self.table.has_room_for(rows) {
            // a batch that fails holds nothing, so the job learns its rows here
            self.wanted_rows = self.wanted_rows.max(rows);
            return false;
        }
        if !self.slab.has_free_pages(pages) || !self.rows_fit_nodes(rows) {
            return false;
        }
        self.held = ShardHold { rows, pages };
        true
    }

    /// Say whether a batch's room in this shard is short of nodes the job can still bring in: Some(true) when only nodes are short
    /// and they can grow, Some(false) when the shard holds it already, and None when another pool is short or the nodes can't grow
    pub fn waits_for_nodes(&self, need: &ShardNeed) -> Option<bool> {
        let rows = self.held.rows + need.rows;
        let pages = PageCount(self.held.pages.0 + need.pages().0);
        if !self.slab.has_free_pages(pages) {
            return None;
        }
        // a table or a node pool short of what the job can still bring in waits for it, and either that can't grow seals
        let is_table_short = !self.table.has_room_for(rows);
        let are_nodes_short = !self.rows_fit_nodes(rows);
        if (is_table_short && !self.table.can_grow()) || (are_nodes_short && !self.nodes.can_grow()) {
            return None;
        }
        Some(is_table_short || are_nodes_short)
    }

    /// Grow the key table when it is past half the keys its size takes or a writer found it short, and say whether it grew. The
    /// reserve job runs this under the shard's write lock
    pub fn grow_table(&mut self) -> Result<bool> {
        if !self.wants_table_grown() {
            return Ok(false);
        }
        // a batch's rows past one doubling take more, all in one rehash
        let is_grown = self.table.grow_for(self.table_rows_wanted())?;
        if self.table.has_room_for(self.wanted_rows) || !self.table.can_grow() {
            self.wanted_rows = 0;
        }
        Ok(is_grown)
    }

    /// Check the key table is past half the keys its size takes, or a writer or a batch found it short
    pub fn wants_table_grown(&self) -> bool {
        self.table.wants_to_grow() || (self.table.can_grow() && !self.table.has_room_for(self.table_rows_wanted()))
    }

    fn table_rows_wanted(&self) -> u64 {
        self.wanted_rows.max(self.held.rows + 1)
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
            && need.pages() <= self.slab.most_pages()
    }

    /// Add a row, or return false when a pool ran out
    pub fn insert(&mut self, hash: KeyHash, column: ColumnId, key: &[u8], row: &WriteRow, value: Option<&[u8]>, is_counted: bool) -> bool {
        if !self.has_room(row.slot, key.len()) {
            return false;
        }

        let inline = value.filter(|value| self.has_inline_room(row.slot, key.len(), value.len()));
        self.link_row(hash, column, key, row, inline, (false, is_counted))
    }

    /// Check a row could keep a value of this length after its key: a short one, while the shard's kept values stay inside the
    /// share of the slab sized for them, so rows keep every page they were sized for whatever their keys' widths. The row must fit
    /// its page, or the plain row would open a page too, so a kept value never strands a page's tail a plain row would have used
    pub fn has_inline_room(&self, slot: OpenSlot, key_length: usize, value_length: usize) -> bool {
        if value_length > MOST_INLINE_VALUE {
            return false;
        }
        let size = inline_entry_size(key_length, value_length);
        let plain = entry_size(key_length);
        let pages = &self.trees[slot.0 as usize].pages;
        let is_tail_kept = pages.used + size <= PAGE_BYTES || pages.used + plain > PAGE_BYTES;
        let kept: u64 = self.trees.iter().map(|tree| tree.kept_bytes).sum();
        is_tail_kept && kept + (size - plain) as u64 <= self.kept_room() && self.slab.has_room_for(pages, size, self.held.pages)
    }

    /// Count the slab bytes kept values may take: the share shard_for_rows adds for them, less a page for each open slot's
    /// partly used tail
    pub fn kept_room(&self) -> u64 {
        let total = u64::from(self.slab.most_pages().0) * PAGE_BYTES as u64;
        (total / KEPT_SHARE_DIVISOR).saturating_sub((self.trees.len() * PAGE_BYTES) as u64)
    }

    /// Add a row of a batch that holds its room
    pub fn insert_held(&mut self, hash: KeyHash, column: ColumnId, key: &[u8], row: &WriteRow, is_counted: bool) -> bool {
        self.link_row(hash, column, key, row, None, (true, is_counted))
    }

    /// Store a row and link it, keeping a batch's rows on their own pages so a put beside it can't use up their room
    pub fn link_row(&mut self, hash: KeyHash, column: ColumnId, key: &[u8], row: &WriteRow, inline: Option<&[u8]>, (is_held, is_counted): (bool, bool)) -> bool {
        let tree = &mut self.trees[row.slot.0 as usize];
        tree.segment = row.segment;
        let chain = if is_held { &mut tree.batch_pages } else { &mut tree.pages };
        let Some(address) = self.slab.store(chain, column, key, row, inline) else {
            return false;
        };
        if let Some(value) = inline {
            tree.kept_bytes += (inline_entry_size(key.len(), value.len()) - entry_size(key.len())) as u64;
        }
        // a row no count holds reads as entered, so a clean counts nothing out for it
        if !is_counted {
            self.slab.mark_entered(address);
        }

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

    /// Find the newest row a reader at a place can see, as lookup does, and copy the value it keeps onto the end of the buffer when
    /// it is the key's newest row and keeps one. Say whether the value went in
    pub fn lookup_value(
        &self,
        hash: KeyHash,
        (column, key): (ColumnId, &[u8]),
        read_point: WritePlace,
        view: OpenSlots,
        out: &mut Vec<u8>,
    ) -> (WriteLookup, bool) {
        let head = self.table.head(self.table.probe(hash, column, key, &self.slab));
        if head == NO_ENTRY {
            return (WriteLookup { row: None, should_ask_footers: true }, false);
        }
        if self.slab.place(head) <= read_point && view.contains(self.slab.slot(head)) {
            let found = WriteLookup { row: self.row_of(head), should_ask_footers: self.slab.has_newer_sealed(head) };
            let Some(value) = self.slab.inline_value(head) else {
                return (found, false);
            };
            out.extend_from_slice(value);
            return (found, true);
        }
        (self.search_trees(column, key, read_point, view), false)
    }

    /// Find the newest row a reader at a place can see from one of the key's versions in a tree, as lookup does, with no hash and no
    /// table probe: a version's newer links lead to the head the table would name
    pub fn lookup_from(&self, entry: EntryAddress, (column, key): (ColumnId, &[u8]), read_point: WritePlace, view: OpenSlots) -> WriteLookup {
        let mut head = entry;
        loop {
            let newer = self.slab.newer(head);
            if newer == NO_ENTRY {
                break;
            }
            head = newer;
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
    pub fn clean_slot(&mut self, slot: OpenSlot, limit: RowCount, counts: &ShardCounts<'_>) -> bool {
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
            self.unlink_run(&entries[..count], counts);
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
    pub fn unlink_run(&mut self, entries: &[EntryAddress], counts: &ShardCounts<'_>) {
        let mut hashes = [KeyHash::default(); CLEAN_RUN_ROWS];
        let mut touched = 0;
        for (hash, &entry) in hashes.iter_mut().zip(entries) {
            *hash = hash_key(self.slab.column(entry), self.slab.key(entry));
            // a row entered in its sorted index left the counts then
            if !self.slab.is_entered(entry) {
                counts.remove(self.slab.column(entry), self.slab.key(entry));
            }
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
        match self.next_entry(cursor, bound, view) {
            Some(entry) => {
                key.clear();
                key.extend_from_slice(self.slab.key(entry));
                true
            },
            None => false,
        }
    }

    /// Find the entry of the next key past a bound in the view's open trees, stepping where a cursor still stands
    pub fn next_entry(&self, cursor: &mut ShardCursor, bound: &WalkBound, view: OpenSlots) -> Option<EntryAddress> {
        let mut best: Option<EntryAddress> = None;
        for (index, tree) in self.trees.iter().enumerate() {
            if !view.contains(OpenSlot(index as u8)) {
                continue;
            }
            let Some(entry) = self.place_cursor(cursor.tree(index), tree, bound) else {
                continue;
            };
            if self.slab.column(entry) == bound.column {
                best = Some(self.closer(best, entry, bound.direction));
            }
        }
        best
    }

    /// Answer a bound outside a tree's span with no descent: when the tree's last row in the walk's direction isn't past the
    /// bound no row is, and when its first row is past the bound that row is the answer. Nothing when the bound falls inside
    pub fn edge_of(&self, tree: &WriteTree, bound: &WalkBound) -> Option<Option<TreePosition>> {
        let (near, far) = match bound.direction {
            Direction::Ascending => (tree.first_position(&self.nodes), tree.last_position(&self.nodes)),
            Direction::Descending => (tree.last_position(&self.nodes), tree.first_position(&self.nodes)),
        };
        let (Some(near), Some(far)) = (near, far) else {
            return Some(None);
        };
        if !self.is_past_bound(entry_at(&self.nodes, far), bound) {
            return Some(None);
        }
        self.is_past_bound(entry_at(&self.nodes, near), bound).then_some(Some(near))
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
                    cursor.pool_resets = self.nodes.resets();
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

        let position = self.edge_of(tree, bound).unwrap_or_else(|| seek_bound(tree, &self.nodes, &self.slab, bound));
        self.stand(cursor, position)
    }

    /// Stand a cursor on a position, or past the tree's end, and give the row there
    pub fn stand(&self, cursor: &mut TreeCursor, position: Option<TreePosition>) -> Option<EntryAddress> {
        cursor.is_placed = true;
        cursor.position = position;
        let position = position?;
        cursor.version = self.nodes.node(position.leaf).version;
        cursor.pool_resets = self.nodes.resets();
        Some(entry_at(&self.nodes, position))
    }

    /// Check a cursor's leaf hasn't changed since it stood there
    pub fn is_cursor_standing(&self, cursor: &TreeCursor) -> bool {
        match cursor.position {
            // the pool gives its pages back only at a reset, and the room protects an extent it gave back, so a cursor from before
            // the last reset stands no more with no read of its leaf. A leaf since is on a page the pool holds, and its version tells
            // a reused node from the one the cursor stood on
            Some(position) => cursor.pool_resets == self.nodes.resets() && self.nodes.node(position.leaf).version == cursor.version,
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

/// The most shards whose heads one batch places together: about as many cache misses as can be in flight, and few enough shard
/// locks that writers wait for no more than a short batch
pub const HEAD_GROUP: usize = 4;

/// One shard's head a batch places: the shard under its read lock, the walk's cursor in it, and where its key goes
pub struct HeadPlace<'place> {
    pub shard: &'place WriteShard,
    pub cursor: &'place mut ShardCursor,
    pub key: &'place mut Vec<u8>,
    pub has_key: &'place mut bool,
}

/// Place several shards' heads on the next key past a bound at once, each with the answer `next_key` gives. A tree a cursor
/// still stands in steps on as `place_cursor` does, and a bound outside a tree's span needs no descent. Every other tree
/// descends with the rest a level a round, each asking the CPU for its next node, so their cache misses wait together
pub fn place_heads_together(heads: &mut [Option<HeadPlace<'_>>], bound: &WalkBound, view: OpenSlots) -> usize {
    // the targets `seek_bound` searches for, and whether the answer is the row before it
    let (place, is_before) = match (bound.direction, bound.is_inclusive) {
        (Direction::Ascending, true) => (WritePlace(u64::MAX), false),
        (Direction::Ascending, false) => (WritePlace(0), false),
        (Direction::Descending, true) => (WritePlace(0), true),
        (Direction::Descending, false) => (WritePlace(u64::MAX), true),
    };
    let target = SeekTarget::new(bound.column, bound.key, place);
    // each head keeps only its closest entry in the bound's column, so a tree's answer folds in as it comes, and a head's
    // equal keys copy the same bytes whatever order they come in
    let mut best = [None; HEAD_GROUP];
    let fold = |shard: &WriteShard, best: &mut Option<EntryAddress>, entry: Option<EntryAddress>| {
        if let Some(entry) = entry.filter(|entry| shard.slab.column(*entry) == bound.column) {
            *best = Some(shard.closer(*best, entry, bound.direction));
        }
    };
    let mut descents = [NO_DESCENT; HEAD_GROUP * MAX_OPEN_SLOTS];
    let mut count = 0;
    for (head, (place, best)) in heads.iter_mut().zip(&mut best).enumerate().take(HEAD_GROUP) {
        let Some(place) = place.as_mut() else {
            continue;
        };
        let shard = place.shard;
        for (index, tree) in shard.trees.iter().enumerate() {
            if !view.contains(OpenSlot(index as u8)) {
                continue;
            }
            let cursor = place.cursor.tree(index);
            if cursor.is_placed && shard.is_cursor_standing(cursor) {
                fold(shard, best, shard.place_cursor(cursor, tree, bound));
                continue;
            }
            if let Some(edge) = shard.edge_of(tree, bound) {
                fold(shard, best, shard.stand(cursor, edge));
                continue;
            }
            fetch_lines(shard.nodes.node(tree.root), NODE_HEAD_LINES);
            descents[count] = Descent { head: head as u8, tree: index as u8, node: tree.root };
            count += 1;
        }
    }
    let mut is_descending = true;
    while is_descending {
        is_descending = false;
        for descent in descents.iter_mut().take(count) {
            let Some(shard) = heads[usize::from(descent.head)].as_ref().map(|place| place.shard) else {
                continue;
            };
            let standing = shard.nodes.node(descent.node);
            if standing.is_leaf {
                continue;
            }
            is_descending = true;
            descent.node = standing.children[child_index(&shard.nodes, &shard.slab, descent.node, &target) as usize];
            fetch_lines(shard.nodes.node(descent.node), NODE_HEAD_LINES);
        }
    }
    for descent in descents.iter().take(count) {
        let Some(place) = heads[usize::from(descent.head)].as_mut() else {
            continue;
        };
        let shard = place.shard;
        let index = usize::from(descent.tree);
        let row = lower_bound(&shard.nodes, &shard.slab, descent.node, &target);
        let mut position = normalize(&shard.nodes, TreePosition { leaf: descent.node, index: row });
        if is_before {
            position = match position {
                Some(found) => step_back(&shard.nodes, found),
                None => shard.trees[index].last_position(&shard.nodes),
            };
        }
        let entry = shard.stand(place.cursor.tree(index), position);
        fold(shard, &mut best[usize::from(descent.head)], entry);
    }
    for (place, best) in heads.iter_mut().zip(best).take(HEAD_GROUP) {
        let Some(place) = place.as_mut() else {
            continue;
        };
        *place.has_key = best.is_some();
        if let Some(entry) = best {
            place.key.clear();
            place.key.extend_from_slice(place.shard.slab.key(entry));
        }
    }
    count
}

/// One tree a head descends: the head, the tree's slot and the node it stands on
#[derive(Clone, Copy, Debug)]
pub struct Descent {
    pub head: u8,
    pub tree: u8,
    pub node: NodeIndex,
}

/// A descent that stands on no tree, which the list starts from
pub const NO_DESCENT: Descent = Descent { head: 0, tree: 0, node: NO_NODE };

/// Lines of a tree node that hold its header and the row prefixes a descent searches
pub const NODE_HEAD_LINES: usize = 5;
