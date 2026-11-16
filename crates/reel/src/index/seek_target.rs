//! A position in row order: key ascending, then newest version first

use std::cmp::Ordering;

use reel_core::column_id::ColumnId;
use reel_core::units::WritePlace;

/// Bytes of a key that fit a prefix beside the column
pub const PREFIX_KEY_BYTES: usize = 7;
/// Bits the column takes at the top of a prefix
pub const PREFIX_COLUMN_SHIFT: u32 = 56;

/// The row order's leading bytes, packed so a word compare orders them
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RowPrefix(pub u64);

/// A column, a key and a place to compare rows against
#[derive(Clone, Copy, Debug)]
pub struct SeekTarget<'key> {
    pub column: ColumnId,
    pub key: &'key [u8],
    pub place: WritePlace,
    pub prefix: RowPrefix,
}

impl<'key> SeekTarget<'key> {
    /// Build a target from its parts
    pub fn new(column: ColumnId, key: &'key [u8], place: WritePlace) -> Self {
        Self {
            column,
            key,
            place,
            prefix: row_prefix(column, key),
        }
    }
}

/// Pack a column and its key's leading bytes
pub fn row_prefix(column: ColumnId, key: &[u8]) -> RowPrefix {
    let mut bytes = [0; PREFIX_KEY_BYTES + 1];
    let length = key.len().min(PREFIX_KEY_BYTES);
    bytes[1..=length].copy_from_slice(&key[..length]);
    let word = u64::from_be_bytes(bytes);
    RowPrefix(word | (u64::from(column.0) << PREFIX_COLUMN_SHIFT))
}

/// Compare two rows: key ascending, then newest version first
pub fn compare_rows(
    left_column: ColumnId,
    left_key: &[u8],
    left_place: WritePlace,
    right: &SeekTarget,
) -> Ordering {
    let by_key = left_column
        .cmp(&right.column)
        .then_with(|| left_key.cmp(right.key));
    by_key.then_with(|| right.place.cmp(&left_place))
}
