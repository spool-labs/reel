//! Keys, locations and graves of the open segments, sharded by a hash of the key

use std::sync::RwLock;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};

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
use crate::index::entry_slab::{EntryAddress, NO_ENTRY, PageCount};
use crate::pool_room::PoolRoom;
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
    /// Times a batch found a key table short of more rows than any batch before it, so a waiting batch wakes the job only then
    pub wanted_raises: AtomicU64,
    /// Each shard's open counts, read with no lock. A row of a sorted column leaves them once its index holds it
    pub counts: Vec<OpenCounts>,
    /// Where each column's open rows count: PLAIN_COUNTS, SORTED_COUNTS or NO_COUNTS
    pub classes: [AtomicU8; COLUMN_SLOTS],
    /// Each column's count of counting switches: odd while a stop or a start runs. A reader of the counts with no lock compares it
    /// before and after, so a switch between its reads shows
    pub class_changes: [AtomicU32; COLUMN_SLOTS],
    /// Each shard's open rows of each sorted column its index doesn't hold yet. At 0 a walk of the column needs no head there
    pub column_open: Vec<[AtomicU32; COLUMN_SLOTS]>,
    /// Each shard's count of changes to its rows, which goes up under its write lock
    pub changes: [PaddedCount; SHARD_COUNT],
    /// Work a test runs each time an entering lets a shard go partway through a slot's rows
    #[cfg(feature = "test-hooks")]
    pub between_enter_holds: std::sync::Mutex<Option<EnterHook>>,
    /// Shard locks batches took to add their rows, for a test that a batch takes a few and not one a row
    #[cfg(feature = "test-hooks")]
    pub batch_locks: AtomicU64,
}

/// A test's work between an entering's holds of a shard, given the shard
#[cfg(feature = "test-hooks")]
#[derive(Clone)]
pub struct EnterHook(pub std::sync::Arc<dyn Fn(usize) + Send + Sync>);

#[cfg(feature = "test-hooks")]
impl std::fmt::Debug for EnterHook {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("EnterHook")
    }
}

impl WriteIndex {
    /// Build every shard with its slab's first pages in memory
    pub fn new(sizing: &ShardSizing, first_pages: PageCount) -> std::io::Result<Self> {
        let mut shards = Vec::with_capacity(SHARD_COUNT);
        for _shard in 0..SHARD_COUNT {
            shards.push(RwLock::new(WriteShard::growing(sizing, first_pages)?));
        }
        let counts = (0..SHARD_COUNT).map(|_shard| OpenCounts::new(sizing.buckets)).collect();
        let classes = std::array::from_fn(|_column| AtomicU8::new(PLAIN_COUNTS));
        let column_open = touched_from(SHARD_COUNT, || std::array::from_fn(|_column| AtomicU32::new(0)));
        Ok(Self { shards: Shards(shards), wanted_raises: AtomicU64::new(0), counts, classes, class_changes: std::array::from_fn(|_column| AtomicU32::new(0)), column_open, changes: Default::default(),
            #[cfg(feature = "test-hooks")]
            between_enter_holds: std::sync::Mutex::new(None),
            #[cfg(feature = "test-hooks")]
            batch_locks: AtomicU64::new(0),
        })
    }

    /// Read every shard's slab room, for the job that grows them
    pub fn rooms(&self) -> Vec<std::sync::Arc<PoolRoom>> {
        // each shard's slab and node pool grow on the job
        (0..SHARD_COUNT)
            .flat_map(|index| {
                let shard = self.shards.read(index);
                [shard.slab.room().clone(), shard.nodes.room().clone()]
            })
            .collect()
    }

    /// Count a reserve job step for every shard's node pool, giving back the pages of one that stayed free, or of every free one
    /// when pressed. The job runs this on the thread that grows the pools, before it gives empty extents back
    pub fn step_node_pools(&self, is_pressed: bool) -> bool {
        let mut is_given = false;
        for index in 0..SHARD_COUNT {
            let mut shard = self.shards.write(index);
            if shard.nodes.step_idle(is_pressed) {
                is_given = true;
                // a shard whose nodes went back holds no key, so its table goes back to its first size too
                let _shrunk = shard.table.shrink_if_empty();
            }
        }
        is_given
    }

    /// Grow every shard's key table that is past half its keys or that a writer found short, ahead of the writers. The reserve job
    /// runs this on its own thread. Say whether any grew
    pub fn grow_key_tables(&self) -> bool {
        let mut is_grown = false;
        for index in 0..SHARD_COUNT {
            // a look under the read lock first, so a step that grows nothing takes no write lock
            let wants = {
                self.shards.read(index).wants_table_grown()
            };
            if wants {
                is_grown |= self.shards.write(index).grow_table().unwrap_or(false);
            }
        }
        is_grown
    }

    /// Read the bytes every shard's key table holds in memory, which it maps apart from the allocator
    pub fn key_tables_in_memory(&self) -> u64 {
        (0..SHARD_COUNT).map(|index| self.shards.read(index).table.heap_bytes().0).sum()
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
        ShardCounts { buckets: &self.counts[shard], classes: &self.classes, class_changes: &self.class_changes, column_open: &self.column_open[shard] }
    }

    /// Stop counting a sorted column whose index can't take its open rows, which would hold every other sorted column's buckets
    /// open. Its walks go off the index, where no head stands on a bound, so its rows need no count. Every shard is held at once
    /// while its rows leave the counts, so no insert counts one in between
    pub fn stop_counting(&self, column: ColumnId) {
        let mut guards: Vec<_> = (0..SHARD_COUNT).map(|shard| self.shards.write(shard)).collect();
        if !self.is_sorted_counted(column) {
            return;
        }
        self.class_changes[column.0 as usize].fetch_add(1, Ordering::AcqRel);
        self.classes[column.0 as usize].store(NO_COUNTS, Ordering::Release);
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
        self.class_changes[column.0 as usize].fetch_add(1, Ordering::AcqRel);
    }

    /// Count a sorted column again once a refill put every sealed row back in its index. Every shard is held at once while each
    /// entered open row of the column leaves the entered state and counts in, and the entering puts them back in the index. A walk
    /// reads the counts with no lock, so every shard's counts fill before the column reads as counted: a walk that saw it counted
    /// with a shard not yet filled would stand that shard's head past the end and skip its open rows
    pub fn start_counting(&self, column: ColumnId) {
        let mut guards: Vec<_> = (0..SHARD_COUNT).map(|shard| self.shards.write(shard)).collect();
        if self.is_sorted_counted(column) {
            return;
        }
        self.class_changes[column.0 as usize].fetch_add(1, Ordering::AcqRel);
        for (shard, guard) in guards.iter_mut().enumerate() {
            let mut open = 0;
            for slot in 0..guard.trees.len() {
                let mut position = guard.trees[slot].first_position(&guard.nodes);
                while let Some(at) = position {
                    let address = entry_at(&guard.nodes, at);
                    position = step_forward(&guard.nodes, at);
                    if guard.slab.column(address) == column && guard.slab.is_entered(address) {
                        guard.slab.clear_entered(address);
                        self.counts[shard].add(guard.slab.key(address));
                        open += 1;
                    }
                }
            }
            self.column_open[shard][column.0 as usize].store(open, Ordering::Release);
        }
        self.classes[column.0 as usize].store(SORTED_COUNTS, Ordering::Release);
        self.class_changes[column.0 as usize].fetch_add(1, Ordering::AcqRel);
        for shard in 0..SHARD_COUNT {
            self.count_change(shard, true);
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
        #[cfg(feature = "test-hooks")]
        self.batch_locks.fetch_add(1, Ordering::Relaxed);
        let counts = self.shard_counts(shard);
        let is_in = guard.insert_held(hash, column, key, row, counts.of(column).is_some());
        if is_in {
            counts.add(column, key);
        }
        self.count_change(shard, is_in);
        is_in
    }

    /// Add a batch's rows that hold their room, a chunk at a time, taking each shard's lock once for the chunk's rows in it, so
    /// a batch takes a few locks and not one a row. A key's rows stay in batch order, since a key keeps to one shard
    pub fn insert_held_rows<'rows>(&self, rows: impl Iterator<Item = (ColumnId, &'rows [u8], WriteRow)>) {
        let mut rows = rows.peekable();
        let mut hashes = [KeyHash::default(); HELD_CHUNK_ROWS];
        let mut keys: [(ColumnId, &[u8]); HELD_CHUNK_ROWS] = [(ColumnId(0), &[]); HELD_CHUNK_ROWS];
        let mut held: [Option<WriteRow>; HELD_CHUNK_ROWS] = [None; HELD_CHUNK_ROWS];
        while rows.peek().is_some() {
            let mut count = 0;
            let mut shards = 0_u32;
            for (column, key, row) in rows.by_ref().take(HELD_CHUNK_ROWS) {
                let hash = hash_key(column, key);
                hashes[count] = hash;
                keys[count] = (column, key);
                held[count] = Some(row);
                shards |= 1 << shard_of(hash);
                count += 1;
            }
            while shards != 0 {
                let shard = shards.trailing_zeros() as usize;
                shards &= shards - 1;
                let mut guard = self.shards.write(shard);
                #[cfg(feature = "test-hooks")]
                self.batch_locks.fetch_add(1, Ordering::Relaxed);
                let counts = self.shard_counts(shard);
                let mut is_changed = false;
                for at in (0..count).filter(|&at| shard_of(hashes[at]) == shard) {
                    let ((column, key), Some(row)) = (keys[at], held[at].as_ref()) else {
                        continue;
                    };
                    let is_in = guard.insert_held(hashes[at], column, key, row, counts.of(column).is_some());
                    debug_assert!(is_in, "a batch's row found no room its batch held");
                    if is_in {
                        counts.add(column, key);
                    }
                    is_changed |= is_in;
                }
                self.count_change(shard, is_changed);
            }
        }
    }

    /// Hold room for a batch in every shard it touches, or hold nothing
    pub fn hold_room(&self, needs: &[ShardNeed; SHARD_COUNT]) -> bool {
        for (index, need) in needs.iter().enumerate() {
            let mut shard = self.shards.write(index);
            let wanted = shard.wanted_rows;
            if !shard.hold_room(need) {
                if shard.wanted_rows > wanted {
                    self.wanted_raises.fetch_add(1, Ordering::AcqRel);
                }
                drop(shard);
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

    /// Check a batch's room is short only of nodes the job can still bring in, so it waits for them instead of sealing early
    pub fn waits_for_nodes(&self, needs: &[ShardNeed; SHARD_COUNT]) -> bool {
        let mut is_short = false;
        for (index, need) in needs.iter().enumerate().filter(|(_index, need)| need.rows > 0) {
            match self.shards.read(index).waits_for_nodes(need) {
                Some(true) => is_short = true,
                Some(false) => {},
                None => return false,
            }
        }
        is_short
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

    /// Check a key's shard waits only for its slab to grow
    pub fn waits_for_slab(&self, column: ColumnId, key: &[u8]) -> bool {
        self.shards.read(shard_of(hash_key(column, key))).waits_for_slab()
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
/// Rows of a batch grouped by shard at a time, so each shard's lock is taken once for them
pub const HELD_CHUNK_ROWS: usize = 128;
/// Rows an entering walks under one hold of a shard's lock, so the shard's writers and a seal's walk wait no longer than that
pub const ENTER_HOLD_ROWS: usize = 4096;

impl WriteIndex {
    /// Enter one shard's open rows of sorted columns into their indexes in key order, a run at a time under the shard's write
    /// lock and never more than ENTER_HOLD_ROWS rows to a hold, and count each out of the open counts once its index holds it. A row its index can't take stays counted, so a walk
    /// places a real head over it. Slots with no floors are left alone. Says how many rows went in
    pub fn enter_shard<'index>(
        &self,
        shard: usize,
        floors: &[Option<Floors>],
        index_of: impl Fn(ColumnId) -> Option<&'index SortedKeys>,
        run: &mut EnterRun,
    ) -> u64 {
        let mut entered = 0;
        for slot in 0..floors.len() {
            let Some(slot_floors) = floors.get(slot).copied().flatten() else {
                continue;
            };
            // the walk lets the lock go every ENTER_HOLD_ROWS rows, at a key's first row, and picks up past the last whole key. A row
            // that came in behind it waits for the next entering, and stays counted till then
            let mut resume: Option<ColumnId> = None;
            let mut segment_seen = None;
            loop {
                let mut guard = self.shards.write(shard);
                let Some(tree) = guard.trees.get(slot) else {
                    break;
                };
                let segment = tree.segment;
                // a slot sealed and opened again between holds holds another segment's rows, which the next entering takes
                if *segment_seen.get_or_insert(segment) != segment {
                    break;
                }
                let mut position = match resume {
                    None => tree.first_position(&guard.nodes),
                    Some(column) => tree.seek(&guard.nodes, &guard.slab, &SeekTarget::new(column, &run.resume, WritePlace(0))),
                };
                let mut column: Option<ColumnId> = None;
                // a tree holds a key's rows newest first, and only the newest goes in, as a seal gives one row a key. The older ones
                // are covered once it's in
                let mut newest: Option<(EntryAddress, Newest)> = None;
                let mut walked = 0;
                let mut is_whole = true;
                while let Some(at) = position {
                    let address = entry_at(&guard.nodes, at);
                    let row_column = guard.slab.column(address);
                    let is_same_key = newest.is_some_and(|(first, _state)| guard.slab.column(first) == row_column && guard.slab.key(first) == guard.slab.key(address));
                    if !is_same_key && walked >= ENTER_HOLD_ROWS
                        && let Some((first, _state)) = newest
                    {
                        run.resume.clear();
                        run.resume.extend_from_slice(guard.slab.key(first));
                        resume = Some(guard.slab.column(first));
                        is_whole = false;
                        break;
                    }
                    position = step_forward(&guard.nodes, at);
                    walked += 1;
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
                        entered += self.enter_run(&mut guard, shard, column.and_then(&index_of), slot_floors, run);
                        column = Some(row_column);
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
                // the run goes in before the lock does, since its addresses hold only while the shard is held
                entered += self.enter_run(&mut guard, shard, column.and_then(&index_of), slot_floors, run);
                if is_whole {
                    break;
                }
                drop(guard);
                #[cfg(feature = "test-hooks")]
                if let Some(EnterHook(work)) = crate::guard::hold(&self.between_enter_holds).clone() {
                    work(shard);
                }
            }
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
    /// The last whole key a walk took before it let its shard go, which the next hold seeks past
    resume: Vec<u8>,
    entries: Vec<KeyEntry>,
    addresses: Vec<EntryAddress>,
    /// Older rows of a run's keys, by the run row of their newest, which cover them once it's in
    older: Vec<(usize, EntryAddress)>,
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
            resume: touched_capacity(MOST_KEY_BYTES, 0),
            entries: touched_capacity(ENTER_RUN_ROWS, KeyEntry::default()),
            addresses: touched_capacity(ENTER_RUN_ROWS, NO_ENTRY),
            older: touched_capacity(ENTER_RUN_ROWS, (0, NO_ENTRY)),
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
    pub class_changes: &'counts [AtomicU32; COLUMN_SLOTS],
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
        let before = self.class_change(column);
        let is_quiet = self.classes[column.0 as usize].load(Ordering::Acquire) == SORTED_COUNTS && self.column_open[column.0 as usize].load(Ordering::Acquire) == 0;
        is_quiet && self.is_class_steady(column, before)
    }

    /// Read a column's count of counting switches, which a reader of the counts takes before it reads them
    pub fn class_change(self, column: ColumnId) -> u32 {
        self.class_changes[column.0 as usize].load(Ordering::Acquire)
    }

    /// Check no counting switch ran or began since the count a reader took, so the counts it read in between hold
    pub fn is_class_steady(self, column: ColumnId, before: u32) -> bool {
        before.is_multiple_of(2) && self.class_change(column) == before
    }
}

/// A column whose open rows count in the buckets
pub const PLAIN_COUNTS: u8 = 0;
/// A sorted column whose open rows count in the buckets and its own count until its index holds them
pub const SORTED_COUNTS: u8 = 1;
/// A sorted column whose index can't take its open rows, which no count holds
pub const NO_COUNTS: u8 = 2;
