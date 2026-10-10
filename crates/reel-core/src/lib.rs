//! A thin key-value store abstraction behind the `Store` trait

pub mod batch;
mod error;
pub mod store;
pub mod value;

pub use batch::{BatchOp, WriteBatch};
pub use error::Error;
pub use store::{
    directory_size_bytes, range_of, CfDiskUsage, Direction, DiskVolume, KeyValue, Store, StoreIter,
    StoreVolume,
};
pub use value::{ReadBlock, Value};

pub type Result<T> = std::result::Result<T, Error>;
