//! Space reports, filled into buffers the caller reuses

use crate::units::{ByteCount, RootId, VolumeId};

/// Live and on-disk bytes of one column on one root
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ColumnRootUsage {
    pub root: RootId,
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
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RootUsage {
    pub volume: VolumeId,
    pub root: RootId,
    pub used: ByteCount,
    pub free: ByteCount,
}

/// Used and free bytes of one volume
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VolumeUsage {
    pub volume: VolumeId,
    pub used: ByteCount,
    pub free: ByteCount,
}

/// Every volume and root
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UsageReport {
    pub volumes: Vec<VolumeUsage>,
    pub roots: Vec<RootUsage>,
}

impl ColumnUsage {
    /// Forget the readings and keep the buffer
    pub fn clear(&mut self) {
        self.live = ByteCount::default();
        self.on_disk = ByteCount::default();
        self.roots.clear();
    }
}

impl UsageReport {
    /// Forget the readings and keep the buffers
    pub fn clear(&mut self) {
        self.volumes.clear();
        self.roots.clear();
    }
}
