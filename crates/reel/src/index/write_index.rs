//! Keys, locations and graves of the open segments, sharded by a hash of the key

use std::sync::RwLock;
use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};

use reel_core::column_id::ColumnId;
use reel_core::units::{ByteCount, RowCount, WritePlace};

use crate::index::key_hash::{KeyHash, hash_key};
use crate::engine::caller_activity::PaddedCount;
use crate::guard::Shards;
use crate::index::open_counts::OpenCounts;
use crate::index::open_slots::{OpenSlot, OpenSlots};
use crate::index::shard_cursor::ShardCursor;
use crate::index::write_row::{WriteLookup, WriteRow};
use crate::index::write_shard::{ShardNeed, ShardSizing, WalkBound, WriteShard};
use crate::index::entry_slab::{EntryAddress, NO_ENTRY};
use crate::index::sorted_keys::{Floors, KeyEntry, MOST_KEY_BYTES, SortedKeys, Upserted};
use crate::pool::{touched_capacity, touched_from};
use crate::index::seek_target::SeekTarget;
use crate::index::write_tree::{entry_at, step_forward};
use crate::row_kind::RowKind;

/// Shards of the write index
pub const SHARD_COUNT: usize = 16;
/// Picks a shard from the top of the hash, since the table uses the bottom
pub const SHARD_SHIFT: u32 = u64::BITS - SHARD_COUNT.trailing_zeros();

/// The write index of a store
#[derive(Debug)]
pub struct WriteIndex {
    pub shards: Shards<WriteShard>,
    /// Each shard's open counts, read with no lock. A row of a sorted column leaves them once its index holds it
    pub counts: Vec<OpenCounts>,
    /// Where each column's open rows count: PLAIN_COUNTS, SORTED_COUNTS or NO_COUNTS
    pub classes: [AtomicU8; COLUMN_SLOTS],
    /// Each shard's open rows of each sorted column its index doesn't hold yet. At 0 a walk of the column needs no head there
    pub column_open: Vec<[AtomicU32; COLUMN_SLOTS]>,
    /// Each shard's count of changes to its rows, which goes up under its write lock
    pub changes: [PaddedCount; SHARD_COUNT],
}

impl WriteIndex {
    /// Allocate and touch every shard
    pub fn new(sizing: &ShardSizing) -> Self {
        let mut shards = Vec::with_capacity(SHARD_COUNT);
        for _shard in 0..SHARD_COUNT {
            shards.push(RwLock::new(WriteShard::new(sizing)));
        }
        let counts = (0..SHARD_COUNT).map(|_shard| OpenCounts::new(sizing.buckets)).collect();
        let classes = std::array::from_fn(|_column| AtomicU8::new(PLAIN_COUNTS));
        let column_open = touched_from(SHARD_COUNT, || std::array::from_fn(|_column| AtomicU32::new(0)));
        Self { shards: Shards(shards), counts, classes, column_open, changes: Default::default() }
    }

    /// Read the heap every shard holds
    pub fn heap_bytes(&self) -> ByteCount {
        let mut total = 0;
        for index in 0..SHARD_COUNT {
            total += self.shards.read(index).heap_bytes().0;
            total += self.counts[index].heap_bytes().0;
        }
        ByteCount(total)
    }

    /// Count a column's rows in the sorted counts, before any row of it comes in
    pub fn mark_sorted(&self, column: ColumnId) {
        self.classes[column.0 as usize].store(SORTED_COUNTS, Ordering::Release);
    }

    /// Read the open counts a column's rows go in, or nothing for a column that counts none
    pub fn counts_for(&self, shard: usize, column: ColumnId) -> Option<&OpenCounts> {
        self.shard_counts(shard).of(column)
    }

    /// Read both of a shard's open counts, for a clean that counts out rows of every column
    pub fn shard_counts(&self, shard: usize) -> ShardCounts<'_> {
        ShardCounts { buckets: &self.counts[shard], classes: &self.classes, column_open: &self.column_open[shard] }
    }

    /// Stop counting a sorted column whose index can't take its open rows, which would hold every other sorted column's buckets
    /// open. Its walks go off the index, where no head stands on a bound, so its rows need no count. Every shard is held at once
    /// while its rows leave the counts, so no insert counts one in between
    pub fn stop_counting(&self, column: ColumnId) {
        let mut guards: Vec<_> = (0..SHARD_COUNT).map(|shard| self.shards.write(shard)).collect();
        if self.classes[column.0 as usize].swap(NO_COUNTS, Ordering::AcqRel) != SORTED_COUNTS {
            return;
        }
        for (shard, guard) in guards.iter_mut().enumerate() {
            for slot in 0..guard.trees.len() {
                let mut position = guard.trees[slot].first_position(&guard.nodes);
                while let Some(at) = position {
                    let address = entry_at(&guard.nodes, at);
                    position = step_forward(&guard.nodes, at);
                    if guard.slab.column(address) == column && !guard.slab.is_entered(address) {
                        self.counts[shard].remove(guard.slab.key(address));
                        guard.slab.mark_entered(address);
                    }
                }
            }
            self.column_open[shard][column.0 as usize].store(0, Ordering::Release);
        }
    }

    /// Read whether a column's rows count in the sorted counts
    pub fn is_sorted_counted(&self, column: ColumnId) -> bool {
        self.classes[column.0 as usize].load(Ordering::Acquire) == SORTED_COUNTS
    }

    /// Add a row, keeping a short value with it while its shard has room to spare, or return false when the shard ran out of room
    pub fn insert(&self, column: ColumnId, key: &[u8], row: &WriteRow, value: Option<&[u8]>) -> bool {
        let hash = hash_key(column, key);
        let shard = shard_of(hash);
        let mut guard = self.shards.write(shard);
        let counts = self.shard_counts(shard);
        let is_in = guard.insert(hash, column, key, row, value, counts.of(column).is_some());
        if is_in {
            counts.add(column, key);
        }
        self.count_change(shard, is_in);
        is_in
    }

    /// Add a row of a batch that holds its room
    pub fn insert_held(&self, column: ColumnId, key: &[u8], row: &WriteRow) -> bool {
        let hash = hash_key(column, key);
        let shard = shard_of(hash);
        let mut guard = self.shards.write(shard);
        let counts = self.shard_counts(shard);
        let is_in = guard.insert_held(hash, column, key, row, counts.of(column).is_some());
        if is_in {
            counts.add(column, key);
        }
        self.count_change(shard, is_in);
        is_in
    }

    /// Hold room for a batch in every shard it touches, or hold nothing
    pub fn hold_room(&self, needs: &[ShardNeed; SHARD_COUNT]) -> bool {
        for (index, need) in needs.iter().enumerate() {
            if !self.shards.write(index).hold_room(need) {
                self.release_room(&needs[..index]);
                return false;
            }
        }
        true
    }

    /// Give back the room a batch held, in shards from the first
    pub fn release_room(&self, needs: &[ShardNeed]) {
        for (index, need) in needs.iter().enumerate() {
            self.shards.write(index).release_room(need);
        }
    }

    /// Check empty shards could hold a batch
    pub fn could_hold(&self, needs: &[ShardNeed; SHARD_COUNT]) -> bool {
        needs.iter().enumerate().all(|(index, need)| self.shards.read(index).could_hold(need))
    }

    /// Count a change to a shard's rows while its write lock is held
    pub fn count_change(&self, shard: usize, is_changed: bool) {
        if is_changed {
            self.changes[shard].0.fetch_add(1, Ordering::Release);
        }
    }

    /// Read a shard's count of changes
    pub fn change_count(&self, shard: usize) -> u64 {
        self.changes[shard].0.load(Ordering::Acquire)
    }

    /// Find the newest row a reader at a place can see
    pub fn lookup(&self, column: ColumnId, key: &[u8], read_point: WritePlace, view: OpenSlots) -> WriteLookup {
        let hash = hash_key(column, key);
        self.shards.read(shard_of(hash)).lookup(hash, column, key, read_point, view)
    }

    /// Find a key's newest row at a read point, and copy the value it keeps onto the end of the buffer when it keeps one. Say
    /// whether the value went in
    pub fn lookup_value(&self, column: ColumnId, key: &[u8], read_point: WritePlace, view: OpenSlots, out: &mut Vec<u8>) -> (WriteLookup, bool) {
        let hash = hash_key(column, key);
        self.shards.read(shard_of(hash)).lookup_value(hash, (column, key), read_point, view, out)
    }

    /// Check whether a key's newest row is a visible grave, so a delete can write nothing
    pub fn is_newest_grave(&self, column: ColumnId, key: &[u8], visible: WritePlace, view: OpenSlots) -> bool {
        let hash = hash_key(column, key);
        self.shards.read(shard_of(hash)).is_newest_grave(hash, column, key, visible, view)
    }

    /// Clean up to a limit of a closed slot's rows in each shard, and say whether all are gone
    pub fn clean_slot(&self, slot: OpenSlot, limit: RowCount) -> bool {
        let mut is_clean = true;
        for index in 0..SHARD_COUNT {
            let mut guard = self.shards.write(index);
            is_clean &= guard.clean_slot(slot, limit, &self.shard_counts(index));
            self.count_change(index, true);
        }
        is_clean
    }

    /// Check a key's shard could take a row in a slot right now
    pub fn has_room(&self, column: ColumnId, key: &[u8], slot: OpenSlot) -> bool {
        let hash = hash_key(column, key);
        self.shards.read(shard_of(hash)).has_room(slot, key.len())
    }

    /// Find a slot's highest key in a column, in any shard
    pub fn highest_key(&self, slot: OpenSlot, column: ColumnId, highest: &mut Vec<u8>) {
        highest.clear();
        for shard in 0..SHARD_COUNT {
            let guard = self.shards.read(shard);
            let Some(entry) = guard.last_of_column(slot, column) else {
                continue;
            };
            let key = guard.slab.key(entry);
            if key > highest.as_slice() {
                highest.clear();
                highest.extend_from_slice(key);
            }
        }
    }

    /// Find the key a walk reaches next in one shard
    pub fn next_key(
        &self,
        shard: usize,
        cursor: &mut ShardCursor,
        bound: &WalkBound,
        view: OpenSlots,
        key: &mut Vec<u8>,
    ) -> bool {
        self.shards.read(shard).next_key(cursor, bound, view, key)
    }
}

/// Pick a key's shard
pub fn shard_of(hash: KeyHash) -> usize {
    (hash.0 >> SHARD_SHIFT) as usize
}

/// Most rows one run of entering puts into a sorted index under a shard's write lock
pub const ENTER_RUN_ROWS: usize = 256;

impl WriteIndex {
    /// Enter one shard's open rows of sorted columns into their indexes in key order, a run at a time under the shard's write
    /// lock, and count each out of the open counts once its index holds it. A row its index can't take stays counted, so a walk
    /// places a real head over it. Slots with no floors are left alone. Says how many rows went in
    pub fn enter_shard<'index>(
        &self,
        shard: usize,
        floors: &[Option<Floors>],
        index_of: impl Fn(ColumnId) -> Option<&'index SortedKeys>,
        run: &mut EnterRun,
    ) -> u64 {
        let mut guard = self.shards.write(shard);
        let mut entered = 0;
        for slot in 0..guard.trees.len() {
            let Some(slot_floors) = floors.get(slot).copied().flatten() else {
                continue;
            };
            let segment = guard.trees[slot].segment;
            let mut position = guard.trees[slot].first_position(&guard.nodes);
            let mut column: Option<ColumnId> = None;
            // a tree holds a key's rows newest first, and only the newest goes in, as a seal gives one row a key. The older ones are
            // covered once it's in
            let mut newest: Option<(EntryAddress, Newest)> = None;
            while let Some(at) = position {
                let address = entry_at(&guard.nodes, at);
                position = step_forward(&guard.nodes, at);
                let row_column = guard.slab.column(address);
                let is_same_key = newest.is_some_and(|(first, _state)| guard.slab.column(first) == row_column && guard.slab.key(first) == guard.slab.key(address));
                if is_same_key {
                    match newest.map(|(_first, state)| state) {
                        Some(Newest::In) if !guard.slab.is_entered(address) => {
                            guard.slab.mark_entered(address);
                            self.shard_counts(shard).remove(guard.slab.column(address), guard.slab.key(address));
                            entered += 1;
                        },
                        // past the run's room an older row stays counted, which only places a real head over it
                        Some(Newest::InRun(row)) if run.older.len() < ENTER_RUN_ROWS => run.older.push((row, address)),
                        _other => {},
                    }
                    continue;
                }
                if column != Some(row_column) || run.entries.len() == ENTER_RUN_ROWS {
                    let had_rows = !run.entries.is_empty();
                    entered += self.enter_run(&mut guard, shard, column.and_then(&index_of), slot_floors, run);
                    column = Some(row_column);
                    // gets of keys in this shard wait on its lock, so the lock goes between runs, and the walk finds this key
                    // again after, since a write in between can move the tree's nodes
                    if had_rows {
                        run.resume.clear();
                        run.resume.extend_from_slice(guard.slab.key(address));
                        drop(guard);
                        guard = self.shards.write(shard);
                        if guard.trees[slot].segment != segment {
                            break;
                        }
                        let target = SeekTarget::new(row_column, &run.resume, WritePlace(u64::MAX));
                        position = guard.trees[slot].seek(&guard.nodes, &guard.slab, &target);
                        newest = None;
                        continue;
                    }
                }
                newest = Some((address, Newest::Out));
                let Some(index) = index_of(row_column) else {
                    continue;
                };
                if guard.slab.is_entered(address) {
                    newest = Some((address, Newest::In));
                    continue;
                }
                let (Some(row), key) = (guard.slab.row(address), guard.slab.key(address)) else {
                    continue;
                };
                if row.kind == RowKind::RangeDelete || key.len() != index.width() {
                    continue;
                }
                newest = Some((address, Newest::InRun(run.entries.len())));
                run.keys.extend_from_slice(key);
                run.entries.push(KeyEntry { place: row.place, segment, offset: row.offset, length: row.length, is_grave: row.kind == RowKind::Grave });
                run.addresses.push(address);
            }
            entered += self.enter_run(&mut guard, shard, column.and_then(&index_of), slot_floors, run);
        }
        entered
    }

    /// Put one run into its index, then mark and count out the rows the index holds now
    fn enter_run(&self, guard: &mut WriteShard, shard: usize, index: Option<&SortedKeys>, floors: Floors, run: &mut EnterRun) -> u64 {
        let mut entered = 0;
        if let Some(index) = index.filter(|_index| !run.entries.is_empty()) {
            let mut is_in = [false; ENTER_RUN_ROWS];
            index.upsert_many(&run.keys, &run.entries, floors, |row, upserted| {
                is_in[row] = matches!(upserted, Upserted::Added | Upserted::Replaced | Upserted::Kept);
            });
            // a walk that sees a row counted out reads its index batch again, and finds the row there
            index.note_entered();
            for (&address, is_in) in run.addresses.iter().zip(is_in) {
                if is_in {
                    guard.slab.mark_entered(address);
                    self.shard_counts(shard).remove(guard.slab.column(address), guard.slab.key(address));
                    entered += 1;
                }
            }
            for &(row, address) in &run.older {
                if is_in[row] && !guard.slab.is_entered(address) {
                    guard.slab.mark_entered(address);
                    self.shard_counts(shard).remove(guard.slab.column(address), guard.slab.key(address));
                    entered += 1;
                }
            }
        }
        run.keys.clear();
        run.entries.clear();
        run.addresses.clear();
        run.older.clear();
        entered
    }
}

/// The rows one run of entering gathers, in buffers sized at open so an entering allocates nothing
#[derive(Debug)]
pub struct EnterRun {
    keys: Vec<u8>,
    entries: Vec<KeyEntry>,
    addresses: Vec<EntryAddress>,
    /// Older rows of a run's keys, by the run row of their newest, which cover them once it's in
    older: Vec<(usize, EntryAddress)>,
    /// The key a walk stopped at to let its lock go, which it seeks again after
    resume: Vec<u8>,
}

/// What became of a key's newest row in a slot's tree
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Newest {
    /// It was in its index already
    In,
    /// It waits in the run at this row
    InRun(usize),
    /// It stays out: no index, or a row an index can't hold
    Out,
}

impl EnterRun {
    /// Allocate and touch room for a run of the widest keys a sorted index takes
    pub fn new() -> Self {
        Self {
            keys: touched_capacity(ENTER_RUN_ROWS * MOST_KEY_BYTES, 0),
            entries: touched_capacity(ENTER_RUN_ROWS, KeyEntry::default()),
            addresses: touched_capacity(ENTER_RUN_ROWS, NO_ENTRY),
            older: touched_capacity(ENTER_RUN_ROWS, (0, NO_ENTRY)),
            resume: touched_capacity(MOST_KEY_BYTES, 0),
        }
    }
}

impl Default for EnterRun {
    fn default() -> Self {
        Self::new()
    }
}

/// Every column id a store can hold
pub const COLUMN_SLOTS: usize = u8::MAX as usize + 1;

/// A shard's two open counts, and which columns go in which
#[derive(Clone, Copy, Debug)]
pub struct ShardCounts<'counts> {
    pub buckets: &'counts OpenCounts,
    pub classes: &'counts [AtomicU8; COLUMN_SLOTS],
    pub column_open: &'counts [AtomicU32; COLUMN_SLOTS],
}

impl<'counts> ShardCounts<'counts> {
    /// Read the counts a column's rows go in, or nothing for a column that counts none
    pub fn of(self, column: ColumnId) -> Option<&'counts OpenCounts> {
        match self.classes[column.0 as usize].load(Ordering::Acquire) {
            PLAIN_COUNTS | SORTED_COUNTS => Some(self.buckets),
            _none => None,
        }
    }

    /// Count an open row in, under its shard's write lock
    pub fn add(self, column: ColumnId, key: &[u8]) {
        let Some(counts) = self.of(column) else {
            return;
        };
        counts.add(key);
        if self.classes[column.0 as usize].load(Ordering::Acquire) == SORTED_COUNTS {
            self.column_open[column.0 as usize].fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Count an open row out, under its shard's write lock
    pub fn remove(self, column: ColumnId, key: &[u8]) {
        let Some(counts) = self.of(column) else {
            return;
        };
        counts.remove(key);
        if self.classes[column.0 as usize].load(Ordering::Acquire) == SORTED_COUNTS {
            let open = &self.column_open[column.0 as usize];
            let _was = open.fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| Some(count.saturating_sub(1)));
        }
    }

    /// Check a sorted column has no open row its index lacks in this shard
    pub fn is_column_quiet(self, column: ColumnId) -> bool {
        self.classes[column.0 as usize].load(Ordering::Acquire) == SORTED_COUNTS && self.column_open[column.0 as usize].load(Ordering::Acquire) == 0
    }
}

/// A column whose open rows count in the buckets
pub const PLAIN_COUNTS: u8 = 0;
/// A sorted column whose open rows count in the buckets and its own count until its index holds them
pub const SORTED_COUNTS: u8 = 1;
/// A sorted column whose index can't take its open rows, which no count holds
pub const NO_COUNTS: u8 = 2;
