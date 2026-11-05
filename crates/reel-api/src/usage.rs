//! Space reports for columns, volumes and roots

use std::path::PathBuf;

use crate::units::ByteCount;

/// Live and on-disk bytes of one column on one root
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ColumnRootUsage {
    pub root: PathBuf,
    pub live: ByteCount,
    pub on_disk: ByteCount,
}

/// Live and on-disk bytes of one column
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ColumnUsage {
    pub live: ByteCount,
    pub on_disk: ByteCount,
    pub roots: Vec<ColumnRootUsage>,
}

/// Used and free bytes of one root
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RootUsage {
    pub root: PathBuf,
    pub used: ByteCount,
    pub free: ByteCount,
}

/// Used and free bytes of one volume
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VolumeUsage {
    pub volume: String,
    pub used: ByteCount,
    pub free: ByteCount,
    pub roots: Vec<RootUsage>,
}
