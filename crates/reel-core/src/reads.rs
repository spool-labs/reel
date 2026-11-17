//! Every read, on a store and on a cue

use std::future::Future;

use crate::column_id::ColumnId;
use crate::direction::Direction;
use crate::scan::{KeyScan, Scan};
use crate::store_error::StoreError;
use crate::units::{ByteCount, RowCount};
use crate::value_list::ValueList;

/// A window into one value
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ValueWindow {
    pub offset: ByteCount,
    pub length: ByteCount,
}

/// The reads a store and a cue share
pub trait Reads: Send + Sync {
    /// A walk over keys and values
    type Rows: Scan;
    /// A walk over keys alone
    type Keys: KeyScan;

    /// Fill the buffer with the newest value, and say whether the key has one
    fn get(&self, column: ColumnId, key: &[u8], buf: &mut Vec<u8>) -> Result<bool, StoreError>;

    /// Answer every key at one point in the write order, with a bad record as that key's answer
    fn get_many(&self, column: ColumnId, keys: &[&[u8]], values: &mut ValueList) -> Result<(), StoreError>;

    /// Fill the buffer with a checked window of the value, and say whether the key has one
    fn get_range(
        &self,
        column: ColumnId,
        key: &[u8],
        window: ValueWindow,
        buf: &mut Vec<u8>,
    ) -> Result<bool, StoreError>;

    /// Check whether the key has a value, without reading the value
    fn contains(&self, column: ColumnId, key: &[u8]) -> Result<bool, StoreError>;

    /// Walk rows from a key. The start can be any length, a prefix included, and compares byte by byte with the keys
    fn iter_from(&self, column: ColumnId, start: &[u8], direction: Direction) -> Result<Self::Rows, StoreError>;

    /// Walk rows in [start, end). Either bound can be any length and compares byte by byte with the keys
    fn iter_range(
        &self,
        column: ColumnId,
        start: &[u8],
        end: &[u8],
        direction: Direction,
    ) -> Result<Self::Rows, StoreError>;

    /// Walk rows under a prefix
    fn iter_prefix(&self, column: ColumnId, prefix: &[u8]) -> Result<Self::Rows, StoreError>;

    /// Walk keys from a key
    fn iter_keys_from(&self, column: ColumnId, start: &[u8], direction: Direction) -> Result<Self::Keys, StoreError>;

    /// Walk keys in [start, end)
    fn iter_keys_range(
        &self,
        column: ColumnId,
        start: &[u8],
        end: &[u8],
        direction: Direction,
    ) -> Result<Self::Keys, StoreError>;

    /// Walk keys under a prefix
    fn iter_keys_prefix(&self, column: ColumnId, prefix: &[u8]) -> Result<Self::Keys, StoreError>;

    /// Count the keys under a prefix, reading no values
    fn count_prefix(&self, column: ColumnId, prefix: &[u8]) -> Result<RowCount, StoreError>;

    /// Add up the bytes the records under a prefix take on disk, each head and its coding included, reading no values
    fn bytes_prefix(&self, column: ColumnId, prefix: &[u8]) -> Result<ByteCount, StoreError>;

    /// Run a get without blocking the caller's runtime thread
    fn get_async(
        &self,
        column: ColumnId,
        key: &[u8],
        buf: &mut Vec<u8>,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;

    /// Run a batched get without blocking the caller's runtime thread
    fn get_many_async(
        &self,
        column: ColumnId,
        keys: &[&[u8]],
        values: &mut ValueList,
    ) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// Run a window read without blocking the caller's runtime thread
    fn get_range_async(
        &self,
        column: ColumnId,
        key: &[u8],
        window: ValueWindow,
        buf: &mut Vec<u8>,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send;
}
