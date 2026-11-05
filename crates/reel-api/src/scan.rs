//! Walks over keys in bytewise order

use std::future::Future;

use crate::store_error::StoreError;
use crate::value_list::ValueList;

/// A walk over keys and values
pub trait Scan: Send {
    /// Fill the buffers with the next row, and say whether there was one
    fn next_row(&mut self, key: &mut Vec<u8>, value: &mut Vec<u8>) -> Result<bool, StoreError>;

    /// Stream the next block of keys, and say whether there was one
    fn next_block(&mut self, keys: &mut ValueList) -> impl Future<Output = Result<bool, StoreError>> + Send;
}

/// A walk over keys alone
pub trait KeyScan: Send {
    /// Fill the buffer with the next key, and say whether there was one
    fn next_key(&mut self, key: &mut Vec<u8>) -> Result<bool, StoreError>;
}
