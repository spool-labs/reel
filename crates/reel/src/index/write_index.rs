//! Keys, locations and graves of the open segments, sharded by a hash of the key

use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, RowCount, WritePlace};

use crate::index::key_hash::{KeyHash, hash_key};
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
    shards: Vec<RwLock<WriteShard>>,
}

impl WriteIndex {
    /// Allocate and touch every shard
    pub fn new(sizing: &ShardSizing) -> Self {
        let mut shards = Vec::with_capacity(SHARD_COUNT);
        for _shard in 0..SHARD_COUNT {
            shards.push(RwLock::new(WriteShard::new(sizing)));
        }
        Self { shards }
    }

    /// Read a shard
    pub fn read_shard(&self, index: usize) -> RwLockReadGuard<'_, WriteShard> {
        match self.shards[index].read() {
            Ok(guard) => guard,
            Err(poisoned) => crate::guard::recover(poisoned),
        }
    }

    /// Change a shard
    pub fn write_shard(&self, index: usize) -> RwLockWriteGuard<'_, WriteShard> {
        match self.shards[index].write() {
            Ok(guard) => guard,
            Err(poisoned) => crate::guard::recover(poisoned),
        }
    }

    /// Read the heap every shard holds
    pub fn heap_bytes(&self) -> ByteCount {
        let mut total = 0;
        for index in 0..SHARD_COUNT {
            total += self.read_shard(index).heap_bytes().0;
        }
        ByteCount(total)
    }

    /// Add a row, or return false when its shard ran out of room
    pub fn insert(&self, column: ColumnId, key: &[u8], row: &WriteRow) -> bool {
        let hash = hash_key(column, key);
        self.write_shard(shard_of(hash)).insert(hash, column, key, row)
    }

    /// Add a row of a batch that holds its room
    pub fn insert_held(&self, column: ColumnId, key: &[u8], row: &WriteRow) -> bool {
        let hash = hash_key(column, key);
        self.write_shard(shard_of(hash)).insert_held(hash, column, key, row)
    }

    /// Hold room for a batch in every shard it touches, or hold nothing
    pub fn hold_room(&self, needs: &[ShardNeed; SHARD_COUNT]) -> bool {
        for (index, need) in needs.iter().enumerate() {
            if !self.write_shard(index).hold_room(need) {
                self.release_room(&needs[..index]);
                return false;
            }
        }
        true
    }

    /// Give back the room a batch held, in shards from the first
    pub fn release_room(&self, needs: &[ShardNeed]) {
        for (index, need) in needs.iter().enumerate() {
            self.write_shard(index).release_room(need);
        }
    }

    /// Check empty shards could hold a batch
    pub fn could_hold(&self, needs: &[ShardNeed; SHARD_COUNT]) -> bool {
        needs.iter().enumerate().all(|(index, need)| self.read_shard(index).could_hold(need))
    }

    /// Find the newest row a reader at a place can see
    pub fn lookup(&self, column: ColumnId, key: &[u8], read_point: WritePlace, view: OpenSlots) -> WriteLookup {
        let hash = hash_key(column, key);
        self.read_shard(shard_of(hash)).lookup(hash, column, key, read_point, view)
    }

    /// Check whether a key's newest row is a visible grave, so a delete can write nothing
    pub fn is_newest_grave(&self, column: ColumnId, key: &[u8], visible: WritePlace, view: OpenSlots) -> bool {
        let hash = hash_key(column, key);
        self.read_shard(shard_of(hash)).is_newest_grave(hash, column, key, visible, view)
    }

    /// Clean up to a limit of a closed slot's rows in each shard, and say whether all are gone
    pub fn clean_slot(&self, slot: OpenSlot, limit: RowCount) -> bool {
        let mut is_clean = true;
        for index in 0..SHARD_COUNT {
            is_clean &= self.write_shard(index).clean_slot(slot, limit);
        }
        is_clean
    }

    /// Check a key's shard could take a row in a slot right now
    pub fn has_room(&self, column: ColumnId, key: &[u8], slot: OpenSlot) -> bool {
        let hash = hash_key(column, key);
        self.read_shard(shard_of(hash)).has_room(slot, key.len())
    }

    /// Find a slot's highest key in a column, in any shard
    pub fn highest_key(&self, slot: OpenSlot, column: ColumnId, highest: &mut Vec<u8>) {
        highest.clear();
        for shard in 0..SHARD_COUNT {
            let guard = self.read_shard(shard);
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
        self.read_shard(shard).next_key(cursor, bound, view, key)
    }
}

/// Pick a key's shard
pub fn shard_of(hash: KeyHash) -> usize {
    (hash.0 >> SHARD_SHIFT) as usize
}
