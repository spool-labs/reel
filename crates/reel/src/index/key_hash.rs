//! The hash every in-memory index shards by

use reel_core::column_id::ColumnId;
use xxhash_rust::xxh3::xxh3_64_with_seed;

/// A key's hash within its column
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct KeyHash(pub u64);

/// Hash a key, with its column as the seed
pub fn hash_key(column: ColumnId, key: &[u8]) -> KeyHash {
    KeyHash(xxh3_64_with_seed(key, u64::from(column.0)))
}
