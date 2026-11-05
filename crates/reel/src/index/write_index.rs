//! Keys, locations and graves of the open segments, sharded by a hash of the key

use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, RowCount, WritePlace};

use crate::index::key_hash::{KeyHash, hash_key};
use crate::index::open_slots::{OpenSlot, OpenSlots};
use crate::index::write_row::{WriteLookup, WriteRow};
use crate::index::write_shard::{ShardSizing, WalkBound, WriteShard};

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
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Change a shard
    pub fn write_shard(&self, index: usize) -> RwLockWriteGuard<'_, WriteShard> {
        match self.shards[index].write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
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

    /// Find the key a walk reaches next in one shard
    pub fn next_key(&self, shard: usize, bound: &WalkBound, view: OpenSlots, key: &mut Vec<u8>) -> bool {
        self.read_shard(shard).next_key(bound, view, key)
    }
}

/// Pick a key's shard
pub fn shard_of(hash: KeyHash) -> usize {
    (hash.0 >> SHARD_SHIFT) as usize
}
