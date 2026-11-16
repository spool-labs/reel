//! What a store is opened with: one memory budget, volumes and columns

use std::path::PathBuf;

use reel_core::column_id::ColumnId;
use reel_core::units::{ByteCount, RowCount, VolumeId};

use crate::engine::engine_sizing::OPEN_PER_ROOT;
use crate::lifetime::MOST_LIFETIME_GROUPS;
use crate::record::codec::{CHUNK_BYTES, Codec};
use crate::segment::segment_tail::MOST_SEGMENT_BYTES;
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

/// The index a column keeps over its sealed segments
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ColumnIndex {
    /// A sorted index on a column of fixed-width keys, and none on one of variable width
    #[default]
    ByKeys,
    /// None: gets and scans read the footers of the sealed segments
    Plain,
    /// A sorted index, sized at each open from the keys it held. A get reads no footers, and a scan steps one sorted list. It
    /// takes keys of one fixed width
    Sorted,
    /// A complete table of `least_rows` rows by hash, for a column read only by key. A get reads no footers
    Complete,
}

/// One column
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnConfig {
    pub id: ColumnId,
    pub name: String,
    pub key_shape: KeyShape,
    pub codec: Codec,
    pub volume: VolumeId,
    /// The index the column keeps, by default sorted when its keys have one fixed width
    pub index: ColumnIndex,
    /// For a sorted index the fewest rows it asks room for at open, for a load the operator knows is coming. For a complete
    /// table the rows it holds, which it needs
    pub least_rows: Option<RowCount>,
    /// The fewest value bytes a record of this column holds, when it declares them, which raises its volume's row floor
    pub least_value: Option<ByteCount>,
}

/// The smallest segment a volume may set: a one-chunk record with its head and the window it lands through
pub const LEAST_SEGMENT_BYTES: ByteCount = ByteCount(2 * CHUNK_BYTES as u64);
/// The largest segment a volume may set, with room for its footer under the format's four-byte offsets
pub const MOST_SET_SEGMENT_BYTES: ByteCount = ByteCount(MOST_SEGMENT_BYTES.0 / 2);

/// One volume
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VolumeConfig {
    pub name: String,
    pub roots: Vec<PathBuf>,
    pub sync: SyncPolicy,
    pub device: DeviceClass,
    /// The most segments this volume writes into at once, lifetime groups among them. None runs OPEN_PER_ROOT a root and
    /// MOST_LIFETIME_GROUPS groups. Each one held open takes write queues and read windows out of the budget, so a small store
    /// sets 1
    pub open_segments: Option<usize>,
    /// The bytes each segment of this volume takes before it seals. None sizes them from the budget. A set size is at least
    /// LEAST_SEGMENT_BYTES, which holds a whole one-chunk record with its head and the record window it lands through, and at
    /// most MOST_SET_SEGMENT_BYTES, which leaves a segment's footer room under the format's four-byte offsets. The reservation
    /// step then follows it, an eighth of the segment down to a page. A store whose every volume sets one caps its write index at
    /// three segments' worth of the smallest rows, and a batch can hold no more rows than that: 60,491 at 256 KiB segments
    pub segment_bytes: Option<ByteCount>,
    /// The lifetime groups marked writes may hold open at once. None takes them from open_segments. Some(0) runs none, for a
    /// writer that never marks a row: its slots are its open segments alone, and a marked write is refused with NoLifetimeGroups.
    /// A store that held marked rows and comes back with none seals its marked open segments at open, and their rows keep their
    /// marks
    pub lifetime_groups: Option<usize>,
}

impl ColumnConfig {
    /// Build a column on the first volume with no codec and the index its keys take by default: sorted on fixed-width keys,
    /// none on variable ones
    pub fn new(id: ColumnId, name: impl Into<String>, key_shape: KeyShape) -> Self {
        Self { id, name: name.into(), key_shape, codec: Codec::None, volume: VolumeId(0), index: ColumnIndex::ByKeys, least_rows: None, least_value: None }
    }

    /// Check the column keeps a sorted index: asked for, or by default on keys of one fixed width
    pub fn is_sorted(&self) -> bool {
        match self.index {
            ColumnIndex::Sorted => true,
            ColumnIndex::ByKeys => matches!(self.key_shape, KeyShape::Fixed { .. }),
            ColumnIndex::Plain | ColumnIndex::Complete => false,
        }
    }

    /// Read the rows a complete table holds, for a column that keeps one
    pub fn complete_rows(&self) -> Option<RowCount> {
        if self.index == ColumnIndex::Complete { self.least_rows } else { None }
    }

    /// Read the fewest rows a sorted index asks room for, for a column that keeps one
    pub fn sorted_least(&self) -> Option<RowCount> {
        if self.is_sorted() { self.least_rows } else { None }
    }
}

impl VolumeConfig {
    /// Build a volume on these roots, with the device under the first root asked about
    pub fn new(name: impl Into<String>, roots: Vec<PathBuf>, sync: SyncPolicy) -> Self {
        Self { name: name.into(), roots, sync, device: DeviceClass::Detect, open_segments: None, segment_bytes: None, lifetime_groups: None }
    }

    /// Count the segments this volume's unmarked writes may take at once
    pub fn most_open(&self) -> usize {
        self.open_segments.map_or(self.roots.len() * OPEN_PER_ROOT, |most| most.max(1))
    }

    /// Count the lifetime groups this volume's marked writes may hold open at once
    pub fn most_groups(&self) -> usize {
        match self.lifetime_groups {
            Some(groups) => groups.min(MOST_LIFETIME_GROUPS),
            None => self.open_segments.map_or(MOST_LIFETIME_GROUPS, |most| most.clamp(1, MOST_LIFETIME_GROUPS)),
        }
    }
}

/// How many threads a store runs for maintenance and async reads. Each one's memory comes out of the budget, sized and touched at
/// open, and a count the budget can't hold is refused there. One seal thread a volume runs whatever these say
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ThreadCounts {
    /// Workers the store runs for every volume, refused when the budget can't hold them. None runs one an online CPU, as many as
    /// the budget holds, so a budget at its floor runs none, and 0 leaves each volume's planner alone
    pub maintenance_workers: Option<usize>,
    /// Threads that serve the async read forms. 0 runs none, for a caller that uses only the blocking forms: the async ones then
    /// return NoAsyncReads, and the store holds no read slot and no ring
    pub read_threads: usize,
}

/// A planner alone on each volume, with no workers beside it
pub const NO_WORKERS: ThreadCounts = ThreadCounts {
    maintenance_workers: Some(0),
    read_threads: DEFAULT_READ_THREADS,
};

impl Default for ThreadCounts {
    fn default() -> Self {
        Self {
            maintenance_workers: None,
            read_threads: DEFAULT_READ_THREADS,
        }
    }
}

impl ThreadCounts {
    /// Read the worker count the caller set, or nothing for a store that counts its CPUs
    pub fn set_workers(&self) -> Option<usize> {
        self.maintenance_workers
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
