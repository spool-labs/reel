//! Walks over keys in bytewise order

use std::future::Future;

use crate::store_error::StoreError;
use crate::value_list::ValueList;

/// What one step of a walk found
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanStep {
    /// The buffers hold the next row
    Found,
    /// The key buffer holds a key whose record failed its check, and the walk can go on
    Corrupt,
    /// The walk is over
    End,
}

/// A walk over keys and values
pub trait Scan: Send {
    /// Fill the buffers with the next row
    fn next_row(&mut self, key: &mut Vec<u8>, value: &mut Vec<u8>) -> Result<ScanStep, StoreError>;

    /// Stream the next block of keys, and say whether the walk has more
    fn next_block(&mut self, keys: &mut ValueList) -> impl Future<Output = Result<bool, StoreError>> + Send;
}

/// A walk over keys alone
pub trait KeyScan: Send {
    /// Fill the buffer with the next key
    fn next_key(&mut self, key: &mut Vec<u8>) -> Result<ScanStep, StoreError>;

    /// Stream the next block of keys, and say whether the walk has more
    fn next_key_block(&mut self, keys: &mut ValueList) -> impl Future<Output = Result<bool, StoreError>> + Send;
}
