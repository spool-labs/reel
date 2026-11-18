//! Keys, locations and graves of the open segments, sharded by a hash of the key

use std::sync::RwLock;
use std::sync::atomic::Ordering;

use reel_core::column_id::ColumnId;
use reel_core::units::{ByteCount, RowCount, WritePlace};

use crate::index::key_hash::{KeyHash, hash_key};
use crate::engine::caller_activity::PaddedCount;
use crate::guard::Shards;
use crate::index::open_slots::{OpenSlot, OpenSlots};
use crate::index::shard_cursor::ShardCursor;
use crate::index::write_row::{WriteLookup, WriteRow};
use crate::index::write_shard::{ShardNeed, ShardSizing, WalkBound, WriteShard};

/// Shards of the write index
pub const SHARD_COUNT: usize = 16;
/// Picks a shard from the top of the hash, since the table uses the bottom
pub const SHARD_SHIFT: u32 = u64::BITS - SHARD_COUNT.trailing_zeros();

/// The write index of a store
#[derive(Debug)]
pub struct WriteIndex {
    pub shards: Shards<WriteShard>,
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
        Self { shards: Shards(shards), changes: Default::default() }
    }

    /// Read the heap every shard holds
    pub fn heap_bytes(&self) -> ByteCount {
        let mut total = 0;
        for index in 0..SHARD_COUNT {
            total += self.shards.read(index).heap_bytes().0;
        }
        ByteCount(total)
    }

    /// Add a row, or return false when its shard ran out of room
    pub fn insert(&self, column: ColumnId, key: &[u8], row: &WriteRow) -> bool {
        let hash = hash_key(column, key);
        let shard = shard_of(hash);
        let mut guard = self.shards.write(shard);
        let is_in = guard.insert(hash, column, key, row);
        self.count_change(shard, is_in);
        is_in
    }

    /// Add a row of a batch that holds its room
    pub fn insert_held(&self, column: ColumnId, key: &[u8], row: &WriteRow) -> bool {
        let hash = hash_key(column, key);
        let shard = shard_of(hash);
        let mut guard = self.shards.write(shard);
        let is_in = guard.insert_held(hash, column, key, row);
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
            is_clean &= guard.clean_slot(slot, limit);
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
