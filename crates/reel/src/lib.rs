//! Reel is an embedded key-value store for the Tapedrive storage node
//!
//! The store and what opening one takes are the crate's surface. The modules behind it are the crate's own, and the sims and
//! benches reach them through the test-hooks feature

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
    row_kind,
    rows,
    segment,
    superblock,
    write_order,
);

pub use config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, ThreadCounts, VolumeConfig};
pub use engine::reel_store::ReelStore;
pub use io::Io;
pub use io::plain::PlainIo;
pub use record::codec::Codec;
