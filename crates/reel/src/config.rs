//! What a store is opened with: one memory budget, volumes and columns

use std::path::PathBuf;

use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, VolumeId};

use crate::record::codec::Codec;

/// The memory budget when the caller doesn't set one
pub const DEFAULT_MEMORY_BUDGET: ByteCount = ByteCount(512 * 1024 * 1024);
/// The longest key a column can declare
pub const MAX_KEY_BYTES: u16 = 1_056;

/// How a column's keys are sized
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum KeyShape {
    /// Every key has this width
    Fixed { width: u16 },
    /// Keys run up to this width
    Variable { most: u16 },
}

/// When a volume's writes reach the disk
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SyncPolicy {
    /// Every write
    EveryWrite,
    /// Every so many bytes written
    EveryBytes(ByteCount),
    /// Never, for benches
    Never,
}

/// One column
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnConfig {
    pub id: ColumnId,
    pub name: String,
    pub key_shape: KeyShape,
    pub codec: Codec,
    pub volume: VolumeId,
}

/// One volume
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VolumeConfig {
    pub name: String,
    pub roots: Vec<PathBuf>,
    pub sync: SyncPolicy,
}

/// A whole store
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreConfig {
    pub memory_budget: ByteCount,
    pub volumes: Vec<VolumeConfig>,
    pub columns: Vec<ColumnConfig>,
}

impl KeyShape {
    /// Read the widest key this shape takes
    pub fn widest(self) -> usize {
        match self {
            KeyShape::Fixed { width } => width as usize,
            KeyShape::Variable { most } => most as usize,
        }
    }

    /// Check a key's width
    pub fn accepts(self, length: usize) -> bool {
        match self {
            KeyShape::Fixed { width } => length == width as usize,
            KeyShape::Variable { most } => length <= most as usize,
        }
    }

    /// Check the declared width is one a column may have
    pub fn is_valid(self) -> bool {
        match self {
            KeyShape::Fixed { width } => width <= MAX_KEY_BYTES,
            KeyShape::Variable { most } => most <= MAX_KEY_BYTES,
        }
    }
}

impl StoreConfig {
    /// Read the widest key a volume's columns take
    pub fn widest_key(&self, volume: usize) -> usize {
        let columns = self.columns.iter().filter(|column| column.volume.0 as usize == volume);
        columns.map(|column| column.key_shape.widest()).max().unwrap_or(0)
    }
}
