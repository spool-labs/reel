//! Reel is an embedded key-value store for the Tapedrive storage node
//!
//! The store and what opening one takes are the crate's surface. The modules behind it are the crate's own, and the sims and
//! benches reach them through the test-hooks feature
//!
//! A caller can tell a scan how many rows it will take, and the scan then locates and reads only that many at a time:
//!
//! ```no_run
//! use reel::{ReelStore, PlainIo, StoreConfig, from_key};
//! use reel_core::column_id::ColumnId;
//! use reel_core::direction::Direction;
//! use reel_core::units::RowCount;
//!
//! fn first_ten(store: &ReelStore<PlainIo>, start: &[u8]) {
//!     let range = from_key(ColumnId(1), start, Direction::Ascending).wanting(RowCount(10));
//!     let _scan = store.iter(&range);
//! }
//! # let _config: Option<StoreConfig> = None;
//! ```

/// Declare modules the crate keeps to itself, which the test-hooks feature opens to the sims and benches
macro_rules! inner_modules {
    ($($name:ident),* $(,)?) => {
        $(
            #[cfg(feature = "test-hooks")]
            pub mod $name;
            #[cfg(not(feature = "test-hooks"))]
            pub(crate) mod $name;
        )*
    };
}

inner_modules!(
    config,
    engine,
    fixed_list,
    guard,
    index,
    io,
    lease,
    lifetime,
    little_endian,
    record,
    region,
    pool,
    prefetch,
    row_kind,
    rows,
    segment,
    superblock,
    write_order,
);

pub use config::{ColumnConfig, DeviceClass, KeyShape, NO_WORKERS, StoreConfig, SyncPolicy, ThreadCounts, VolumeConfig};
pub use engine::reel_store::ReelStore;
pub use io::Io;
pub use io::plain::PlainIo;
pub use record::codec::Codec;
pub use engine::cue::Cue;
pub use engine::scan_state::{ScanLimit, ScanRange, from_key, in_range};
pub use engine::store_scan::StoreScan;
