//! What a store is opened with: one memory budget, volumes and columns

use std::path::PathBuf;

use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, RowCount, VolumeId};

use crate::record::codec::Codec;
use crate::record::record_head::SMALL_HEAD_BYTES;
use crate::segment::footer_writer::SMALLEST_ROW_BYTES;

/// The fewest bytes a row with its record takes when its column declares no value: the smallest record and stored row
pub const FORMAT_ROW_FLOOR: u64 = (SMALL_HEAD_BYTES + SMALLEST_ROW_BYTES) as u64;

/// The most walk slots and rewrite seats a volume has, which the 64-bit mask of slots carved from a walk target holds
pub const MOST_VOLUME_SLOTS: usize = 64;
/// The most maintenance workers a store runs, past any host's online CPUs
pub const MOST_WORKERS: usize = 1_024;

/// Count the maintenance workers a store runs when no test sets the count: one an online CPU, which every volume shares
pub fn default_workers() -> usize {
    let cpus = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    cpus.clamp(1, MOST_WORKERS)
}
/// Async read threads a store runs by default
pub const DEFAULT_READ_THREADS: usize = 4;
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

/// What a volume sits on, which every rule for spinning disks keys on
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum DeviceClass {
    /// Ask the kernel about the device under the first root. A device it can't say about counts as solid
    #[default]
    Detect,
    /// A spinning disk, or anything with one under it
    Spinning,
    /// Anything with no seek to speak of
    Solid,
}

/// One column
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnConfig {
    pub id: ColumnId,
    pub name: String,
    pub key_shape: KeyShape,
    pub codec: Codec,
    pub volume: VolumeId,
    /// The rows a complete table holds, for a column that declares one. A get on it reads no footers
    pub complete_rows: Option<RowCount>,
    /// The rows a sorted index holds, for a column of fixed-width keys that declares one. A get on it reads no footers, and a
    /// scan steps one sorted list. A column declares this or a complete table, never both
    pub sorted_rows: Option<RowCount>,
    /// The fewest value bytes a record of this column holds, when it declares them, which raises its volume's row floor
    pub least_value: Option<ByteCount>,
}

/// One volume
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VolumeConfig {
    pub name: String,
    pub roots: Vec<PathBuf>,
    pub sync: SyncPolicy,
    pub device: DeviceClass,
}

/// How many threads a store runs for maintenance and async reads. Each one's memory comes out of the budget, sized and touched at
/// open, and a count the budget can't hold is refused there. One seal thread a volume runs whatever these say
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ThreadCounts {
    /// Workers the store runs for every volume, a hook only the sims and the bench reach through the test-hooks feature, refused
    /// when the budget can't hold it. A store's count is otherwise one an online CPU, as many as the budget holds
    #[cfg(feature = "test-hooks")]
    pub maintenance_workers: Option<usize>,
    /// Threads that serve the async read forms
    pub read_threads: usize,
}

/// A planner alone on each volume, with no workers beside it
#[cfg(feature = "test-hooks")]
pub const NO_WORKERS: ThreadCounts = ThreadCounts {
    maintenance_workers: Some(0),
    read_threads: DEFAULT_READ_THREADS,
};

impl Default for ThreadCounts {
    fn default() -> Self {
        Self {
            #[cfg(feature = "test-hooks")]
            maintenance_workers: None,
            read_threads: DEFAULT_READ_THREADS,
        }
    }
}

impl ThreadCounts {
    /// Read the worker count a test set, or nothing for a store that counts its CPUs
    pub fn set_workers(&self) -> Option<usize> {
        #[cfg(feature = "test-hooks")]
        return self.maintenance_workers;
        #[cfg(not(feature = "test-hooks"))]
        None
    }
}

/// A whole store
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreConfig {
    pub memory_budget: ByteCount,
    pub threads: ThreadCounts,
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

    /// Read the fewest bytes a row of a volume takes with its record: the smallest record and stored row, past the least value
    /// its columns declare. A column that declares none brings the floor down to the format's own
    pub fn row_floor(&self, volume: usize) -> u64 {
        let columns = self.columns.iter().filter(|column| column.volume.0 as usize == volume);
        let least = columns.map(|column| column.least_value.map_or(0, |bytes| bytes.0)).min().unwrap_or(0);
        FORMAT_ROW_FLOOR + least
    }
}
