//! A thin typed key-value store abstraction
//!
//! Byte-oriented access through the `Store` trait, typed access through
//! `TypedStore` and `Column`. Backends implement `Store`.

pub mod batch;
mod column;
mod error;
pub mod store;
mod typed;
pub mod value;

pub use batch::{BatchOp, WriteBatch};
pub use column::Column;
pub use error::Error;
pub use store::{
    directory_size_bytes, range_of, CfDiskUsage, Direction, DiskVolume, KeyValue, Store, StoreIter,
    StoreVolume,
};
pub use typed::TypedStore;
pub use value::Value;

pub type Result<T> = std::result::Result<T, Error>;
