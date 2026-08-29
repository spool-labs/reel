//! Reel store configuration and load-time validation

use std::num::NonZeroU32;
use std::time::Duration;

#[cfg(feature = "serde")]
use serde::de::Error as SerdeError;
#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer};

use crate::units::ByteCount;

use crate::error::{ReelError, Result};

const DEFAULT_SEGMENT_GIB: u64 = 1;
const DEFAULT_ALLOC_CHUNK_MIB: u64 = 64;
const DEFAULT_COMPACT_DEAD_RATIO: f64 = 0.50;
const DEFAULT_MERGE_DEAD_RATIO: f64 = 0.50;
const DEFAULT_SCRUB_MBPS: u64 = 64;

/// Sealed descriptors the reader cache holds, the one number every volume runs
pub const DEFAULT_FD_CACHE: u64 = 256;

const DEFAULT_FOOTER_CACHE_MIB: u64 = 64;
const DEFAULT_FILTER_BITS: u8 = 10;

/// Tails one volume will open however wide the machine is
///
/// Each tail is an open segment reserving its whole size up front, so tails
/// multiply what a volume claims before it has written anything.
const MAX_AUTO_TAILS: usize = 8;

pub(crate) const KIB: u64 = 1024;
pub(crate) const MIB: u64 = KIB * 1024;
pub(crate) const GIB: u64 = MIB * 1024;
/// Only ever reached by a written unit, so it goes with the parser
#[cfg(feature = "serde")]
pub(crate) const TIB: u64 = GIB * 1024;

/// Durability sync cadence for group commit drains
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncPolicy {
    /// Never sync on the hot path, and a seal still syncs, so only the tail is at risk
    Never,
    /// Sync once this many bytes accumulate across a drain
    Bytes(ByteCount),
    /// Sync after every put
    EveryPut,
}

/// Where a volume keeps the index of the segments it has sealed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum IndexResidency {
    /// Every live key in memory, one io per read and a footprint per key
    Resident,

    /// Sealed keys stay in their footers, memory holds what it takes to find them
    Paged,

    /// Recent sealed keys stay in memory, older ones go to their footers
    Hot(HotIndex),
}

impl IndexResidency {
    /// Whether sealed keys ever leave the map on this volume
    pub fn pages(&self) -> bool {
        !matches!(self, IndexResidency::Resident)
    }
}

/// When a hot index gives a sealed segment's keys up
///
/// Both limits are ceilings rather than targets: a segment goes over once it has
/// been sealed longer than the age, and the oldest go early while the maps weigh
/// more than the budget.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub struct HotIndex {
    /// Seconds a sealed segment's keys stay resident before they are handed over
    pub after_secs: u64,

    /// Resident index bytes past which the oldest segments are handed over early
    #[cfg_attr(feature = "serde", serde(deserialize_with = "deserialize_bytes"))]
    pub budget: ByteCount,
}

impl HotIndex {
    /// How long a sealed segment's keys stay resident
    pub fn after(&self) -> Duration {
        Duration::from_secs(self.after_secs)
    }
}

/// Whether a new segment reserves space per step or is pre-written whole
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum Preallocate {
    /// Reserve ahead of the write head in allocation chunks
    Chunk,
    /// Pre-write the whole segment at creation
    Full,
}

/// Compaction rate limit, unpaced unless a cap is named
///
/// The cap is device traffic, read plus write, and not reclaimed space: a pass
/// reads the segment it retires and writes the survivors, so an operator sizing
/// this against a reclaim deadline should divide.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompactRate {
    /// Unpaced: the pass runs at device speed while there is debt to drain
    Auto,
    /// Cap at this many megabytes per second, held inside a pass as well as between passes
    Mbps(u64),
}

/// Append tails a volume may run, automatic or a fixed count.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThreadBudget {
    /// Take the machine's parallelism as the cap
    Auto,
    /// Never fan out past this many threads
    Fixed(NonZeroU32),
}

impl ThreadBudget {
    /// Build a budget from a count, where zero means take the machine's own
    pub fn threads(count: u32) -> ThreadBudget {
        match NonZeroU32::new(count) {
            Some(count) => ThreadBudget::Fixed(count),
            None => ThreadBudget::Auto,
        }
    }

    /// The cap in threads, resolved against the machine for an automatic budget
    pub fn resolve(self) -> usize {
        match self {
            ThreadBudget::Auto => std::thread::available_parallelism()
                .map(|width| width.get())
                .unwrap_or(1),
            ThreadBudget::Fixed(count) => count.get() as usize,
        }
    }

    /// The cap resolved for append tails, which cost disk rather than only cpu
    ///
    /// A tail costs a whole reserved segment, so an automatic budget stops well
    /// short of a wide machine's core count and a named number is taken as given.
    pub fn resolve_tails(self) -> usize {
        match self {
            ThreadBudget::Auto => self.resolve().clamp(1, MAX_AUTO_TAILS),
            ThreadBudget::Fixed(count) => count.get() as usize,
        }
    }
}

/// The tier one volume serves
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum VolumeClass {
    /// The ingest surface: tails, fresh segments, and the hot tier
    #[default]
    Fast,

    /// Bytes at rest: what compaction demotes once it has aged
    Capacity,
}

/// One volume root past the reel's own, with the tier it serves
///
/// A class describes the volume, not how the volume was built: a stripe handed
/// over as one entry and a raw drive are the same to the engine. A dead entry
/// stays in the list, since the placement table is indexed by list order.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize))]
pub struct VolumeSpec {
    /// Directory the volume is mounted at
    pub path: std::path::PathBuf,

    /// The tier, fast unless the entry says otherwise
    #[cfg_attr(feature = "serde", serde(default))]
    pub class: VolumeClass,

    /// The operator's word that this drive is dead, taking it out of every scan and draw
    #[cfg_attr(feature = "serde", serde(default))]
    pub dead: bool,
}

impl VolumeSpec {
    /// A fast volume at this root
    pub fn fast(path: impl Into<std::path::PathBuf>) -> VolumeSpec {
        VolumeSpec {
            path: path.into(),
            class: VolumeClass::Fast,
            dead: false,
        }
    }

    /// A capacity volume at this root
    pub fn capacity(path: impl Into<std::path::PathBuf>) -> VolumeSpec {
        VolumeSpec {
            path: path.into(),
            class: VolumeClass::Capacity,
            dead: false,
        }
    }

    /// The same volume with the operator's word that the drive is dead
    pub fn declared_dead(mut self) -> VolumeSpec {
        self.dead = true;
        self
    }
}

/// How a thread waits for a ring completion that has not arrived
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum RingWait {
    /// Spin while the ring holds only writes and sleep once it holds a read
    Auto,
    /// Spin, then yield, and keep asking, holding a core for the whole wait
    Spin,
    /// Sleep in the kernel until a completion is ready, at a syscall in and a wakeup out
    Kernel,
}

/// When the kernel runs the completion work a ring owes its owning thread
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum TaskRun {
    /// Hold it until the thread enters asking for completions, on a kernel from 6.1
    ///
    /// A ring is one thread's here, which is the promise this mode is built on: no
    /// interrupt, no work run on a transition the thread made for something else,
    /// and the completions land in a batch at the one place that wants them.
    Deferred,

    /// Run it at the next kernel exit rather than interrupting for it, from 5.19
    Cooperative,

    /// Interrupt the thread for every completion, which is what a ring does unasked
    Interrupt,
}

/// Ring tunables
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct RingTuning {
    /// Hand the kernel a pool of aligned buffers, which is what puts direct data ops on the ring
    pub registered_buffers: bool,

    /// How a thread waits on a completion that has not landed yet
    pub wait: RingWait,

    /// When the kernel runs this ring's completion work, stepped down where refused
    pub taskrun: TaskRun,
}

impl Default for RingTuning {
    fn default() -> Self {
        Self {
            registered_buffers: true,
            wait: RingWait::Auto,
            taskrun: TaskRun::Deferred,
        }
    }
}

impl TaskRun {
    /// This mode and the ones below it, for a kernel that refuses the one asked for
    ///
    /// Deferred wants 6.1 and cooperative 5.19, and a refusal comes back from the
    /// setup as one errno with nothing in it to say which flag was the problem, so
    /// the answer is to try the next one down rather than to read the version.
    pub fn and_below(self) -> &'static [TaskRun] {
        match self {
            TaskRun::Deferred => &[TaskRun::Deferred, TaskRun::Cooperative, TaskRun::Interrupt],
            TaskRun::Cooperative => &[TaskRun::Cooperative, TaskRun::Interrupt],
            TaskRun::Interrupt => &[TaskRun::Interrupt],
        }
    }

    /// Whether a thread has to ask the kernel before it reads its own queue
    ///
    /// Both modes that are not the default set `IORING_SQ_TASKRUN` when work is
    /// waiting, so a peek is a flag read; only under deferred is the ask the one
    /// thing that makes a completion appear.
    pub fn is_asked_for(self) -> bool {
        !matches!(self, TaskRun::Interrupt)
    }
}

impl IoBackend {
    /// Whether this backend's descriptors bypass the page cache
    pub fn is_direct(self) -> bool {
        matches!(self, IoBackend::UringDirect)
    }
}

/// Selected file I/O backend
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum IoBackend {
    /// Synchronous POSIX backend, the portable floor
    #[default]
    Posix,
    /// Buffered ring backend on Linux, submitting through the page cache
    Uring,
    /// Direct ring backend with registered buffers on Linux
    UringDirect,
}

/// How a ranged read of a large record reaches the device
///
/// Linux is the only platform where the direct open flag means anything, so off it
/// every arm is the same buffered read with a wider span.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum RangedReads {
    /// Read the window through the page cache
    Cached,
    /// Ask the cache without blocking, and go around it when it cannot answer
    Probed,
    /// Go around the cache without asking
    Direct,
}

/// How an awaited whole-record read reaches its bytes
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum PointReads {
    /// Read through the driver, one op per read whatever the cache holds
    Queued,
    /// Ask the cache without blocking, and queue only the reads it cannot answer
    Probed,
}

/// Whether a column may hold its resident keys in anything but the tree
///
/// Nothing on disk depends on the choice and a reopen rebuilds the maps either
/// way, so the tree is also the way back off the other shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum ShardShapes {
    /// Every column's keys in the ordered tree
    Tree,
    /// Each column takes the shape its own declaration asks for
    Declared,
}

/// Where a sealed segment's fence over its blocks lives, off unless asked for
///
/// A fence is one lead per block of a partition's rows. Without it a blocked search
/// pays a block read per halving; with it the halvings happen over the leads and the
/// search reads one block.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum FenceResidency {
    /// No fence: a search binary searches the blocks themselves
    Off,

    /// Every lead in memory, so a search reads one block and nothing else
    Resident,

    /// The sampled level in memory, so a search reads one page of leads and one block
    Paged,
}

/// Where a record that fails its checksum can be fetched again from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum RepairPath {
    /// Another copy exists, so a corrupt record becomes a miss and a refetch
    Peers,
    /// Nothing else holds these bytes, so corruption is an error, never a miss
    None,
}

/// Load-time settings for one reel bulk volume
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Deserialize))]
#[cfg_attr(feature = "serde", serde(default))]
pub struct ReelConfig {
    /// Target size of each segment file before it seals
    #[cfg_attr(feature = "serde", serde(deserialize_with = "deserialize_bytes"))]
    pub segment_bytes: ByteCount,

    /// Bytes reserved ahead of the write head per allocation step
    #[cfg_attr(feature = "serde", serde(deserialize_with = "deserialize_bytes"))]
    pub alloc_chunk: ByteCount,

    /// Whether a new segment reserves per step or is pre-written whole
    pub preallocate: Preallocate,

    /// Where the index of a sealed segment lives, in memory or in its footer
    pub index: IndexResidency,

    /// Whether a column's declared map shape is honoured, or the tree serves them all
    pub shard_shapes: ShardShapes,

    /// Durability sync cadence parsed from the sync bytes scalar
    #[cfg_attr(
        feature = "serde",
        serde(rename = "sync_bytes", deserialize_with = "deserialize_sync")
    )]
    pub sync: SyncPolicy,

    /// Dead fraction at which a sealed segment is rewritten
    pub compact_dead_ratio: f64,

    /// Compaction rate limit, automatic or a fixed cap
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "deserialize_compact_rate")
    )]
    pub compact_mbps: CompactRate,

    /// Background scrub rate over sealed segments, off when set to zero
    pub scrub_mbps: u64,

    /// Whether each read verifies the record checksum
    pub verify_reads: bool,

    /// Where a record that fails its checksum can be fetched again from
    pub repair: RepairPath,

    /// Smallest record served from a read-only mapping of its segment file, unset maps nothing
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "deserialize_optional_bytes")
    )]
    pub map_above: Option<ByteCount>,

    /// Which plane a window of a large record is read on
    pub ranged_reads: RangedReads,

    /// Rewrite a segment into key order once it seals, then drop the log copy
    pub rewrite_on_seal: bool,

    /// Collapse the volume's sorted runs into one, on a caller's pass and on the tick
    pub merge_sorted_runs: bool,

    /// Dead share of the standing sorted runs at which the tick collapses them
    pub merge_dead_ratio: f64,

    /// Whether an awaited whole-record read asks the page cache before it queues
    pub point_reads: PointReads,

    /// Byte budget for the values the index carries beside its entries, zero for all-resident
    #[cfg_attr(feature = "serde", serde(deserialize_with = "deserialize_bytes"))]
    pub carried_budget: ByteCount,

    /// Append tails the volume runs, which is how many files it appends into
    #[cfg_attr(
        feature = "serde",
        serde(deserialize_with = "deserialize_thread_budget")
    )]
    pub active_tails: ThreadBudget,

    /// Extra volume roots the reel places segments across, past its own fast root
    #[cfg_attr(feature = "serde", serde(default))]
    pub volumes: Vec<VolumeSpec>,

    /// File backend selected for this volume
    pub io_backend: IoBackend,

    /// Ring tunables, which only mean anything under a ring backend
    pub uring: RingTuning,

    /// Bits per key a seal spends on each column's filter, zero for no filter
    pub filter_bits: u8,

    /// Where a sealed segment's fence over its blocks lives, off unless asked for
    pub fence: FenceResidency,

    /// Bytes of sealed-footer state a paged volume keeps at once
    #[cfg_attr(feature = "serde", serde(deserialize_with = "deserialize_bytes"))]
    pub footer_cache: ByteCount,

    /// Write the resident index down at a cue, and read it back at the next open
    pub index_checkpoint: bool,
}

/// A floor every record clears, for a volume that wants the mapping outright
pub const MAP_EVERYTHING: Option<ByteCount> = Some(ByteCount::from_bytes(0));

impl ReelConfig {
    /// Whether a read of this many payload bytes is served from a mapping
    pub fn maps(&self, len: usize) -> bool {
        match self.map_above {
            Some(floor) => len as u64 >= floor.to_bytes(),
            None => false,
        }
    }
}

impl Default for ReelConfig {
    fn default() -> Self {
        Self {
            segment_bytes: ByteCount::gb(DEFAULT_SEGMENT_GIB),
            alloc_chunk: ByteCount::mb(DEFAULT_ALLOC_CHUNK_MIB),
            preallocate: Preallocate::Full,
            index: IndexResidency::Resident,
            shard_shapes: ShardShapes::Tree,
            sync: SyncPolicy::Never,
            compact_dead_ratio: DEFAULT_COMPACT_DEAD_RATIO,
            compact_mbps: CompactRate::Auto,
            scrub_mbps: DEFAULT_SCRUB_MBPS,
            verify_reads: false,
            repair: RepairPath::Peers,
            map_above: None,
            ranged_reads: RangedReads::Cached,
            rewrite_on_seal: false,
            merge_sorted_runs: false,
            merge_dead_ratio: DEFAULT_MERGE_DEAD_RATIO,
            point_reads: PointReads::Queued,
            carried_budget: ByteCount::from_bytes(0),
            footer_cache: ByteCount::mb(DEFAULT_FOOTER_CACHE_MIB),
            active_tails: ThreadBudget::Auto,
            volumes: Vec::new(),
            io_backend: IoBackend::default(),
            uring: RingTuning::default(),
            filter_bits: DEFAULT_FILTER_BITS,
            fence: FenceResidency::Off,
            index_checkpoint: false,
        }
    }
}

impl ReelConfig {
    /// Append tails this volume runs, floored at one per fast volume
    ///
    /// A named count is taken as a minimum rather than as given, and capacity
    /// volumes take no tail, since nothing fresh is ever placed on one.
    pub fn tail_count(&self) -> usize {
        let fast = 1 + self
            .volumes
            .iter()
            .filter(|volume| volume.class == VolumeClass::Fast && !volume.dead)
            .count();
        self.active_tails.resolve_tails().max(fast)
    }

    /// Bits per key this volume's seals actually spend on filters
    ///
    /// Nothing on a resident index, whatever the knob says: such a column answers
    /// every key from its map and never searches a footer.
    pub fn seal_filter_bits(&self) -> u8 {
        match self.index.pages() {
            true => self.filter_bits,
            false => 0,
        }
    }

    /// Whether this volume's seals write a fence over each partition's blocks
    ///
    /// Nothing on a resident index, for the reason the filters are nothing there:
    /// the leads would be bytes written and never read.
    pub fn seal_fences(&self) -> bool {
        self.index.pages() && self.fence != FenceResidency::Off
    }

    /// Reject settings the on-disk types or the engine cannot represent
    pub fn validate(&self) -> Result<()> {
        let segment = self.segment_bytes.to_bytes();
        if segment == 0 {
            return Err(ReelError::Config(
                "segment_bytes must be non-zero".to_string(),
            ));
        }
        if segment > u32::MAX as u64 {
            return Err(ReelError::Config(
                "segment_bytes must fit a u32 offset, under four gibibytes".to_string(),
            ));
        }

        let alloc = self.alloc_chunk.to_bytes();
        if alloc == 0 {
            return Err(ReelError::Config(
                "alloc_chunk must be non-zero".to_string(),
            ));
        }
        if alloc > segment {
            return Err(ReelError::Config(
                "alloc_chunk must not exceed segment_bytes".to_string(),
            ));
        }

        if let SyncPolicy::Bytes(threshold) = self.sync {
            if threshold.to_bytes() == 0 {
                return Err(ReelError::Config(
                    "sync_bytes threshold must be non-zero".to_string(),
                ));
            }
        }

        if !(0.0..=1.0).contains(&self.compact_dead_ratio) {
            return Err(ReelError::Config(
                "compact_dead_ratio must be between zero and one".to_string(),
            ));
        }

        if !(0.0..=1.0).contains(&self.merge_dead_ratio) {
            return Err(ReelError::Config(
                "merge_dead_ratio must be between zero and one".to_string(),
            ));
        }

        if self.map_above.is_some() && self.io_backend == IoBackend::UringDirect {
            return Err(ReelError::Config(
                "map_above and a direct volume contradict each other: a mapping reads the page cache a direct volume bypasses".to_string(),
            ));
        }

        if self.ranged_reads != RangedReads::Cached {
            if self.io_backend == IoBackend::UringDirect {
                return Err(ReelError::Config(
                    "ranged_reads and a direct volume contradict each other: a direct volume already reads around the page cache everywhere".to_string(),
                ));
            }
            if self.io_backend == IoBackend::Uring {
                return Err(ReelError::Config(
                    "ranged_reads needs the posix backend: a driver has one backend, and a buffered ring serves the read itself, so the direct descriptor would name a file the reader never consults".to_string(),
                ));
            }
            if self.map_above.is_some() {
                return Err(ReelError::Config(
                    "ranged_reads and map_above contradict each other: the mapping answers the window before the route is reached".to_string(),
                ));
            }
        }

        // map_above is not refused here: the mapping serves the blocking door and
        // the probe the awaited one, which never maps
        if self.point_reads == PointReads::Probed && self.io_backend == IoBackend::UringDirect {
            return Err(ReelError::Config(
                "point_reads and a direct volume contradict each other: a direct volume holds no page cache to ask".to_string(),
            ));
        }

        if self.merge_sorted_runs && !self.rewrite_on_seal {
            return Err(ReelError::Config(
                "merge_sorted_runs needs rewrite_on_seal: a volume that does not seal by rewriting produces no sorted runs, and a merge over segments that only look sorted would rewrite the volume for nothing".to_string(),
            ));
        }

        Ok(())
    }
}

/// The two written forms a knob accepts: a bare number, or a number with a unit
///
/// Only a deserializer ever produces one, so it and every text parser below it
/// belong to the feature.
#[cfg(feature = "serde")]
#[derive(Deserialize)]
#[serde(untagged)]
enum Scalar {
    Int(u64),
    Text(String),
}

#[cfg(feature = "serde")]
fn deserialize_bytes<'de, Deser>(
    deserializer: Deser,
) -> std::result::Result<ByteCount, Deser::Error>
where
    Deser: Deserializer<'de>,
{
    let scalar = Scalar::deserialize(deserializer)?;
    let bytes = match scalar {
        Scalar::Int(value) => value,
        Scalar::Text(text) => parse_byte_size(&text).map_err(SerdeError::custom)?,
    };
    Ok(ByteCount::from_bytes(bytes))
}

/// A byte count that may be absent, for knobs whose default is to do nothing
#[cfg(feature = "serde")]
fn deserialize_optional_bytes<'de, Deser>(
    deserializer: Deser,
) -> std::result::Result<Option<ByteCount>, Deser::Error>
where
    Deser: Deserializer<'de>,
{
    let scalar = Option::<Scalar>::deserialize(deserializer)?;
    let Some(scalar) = scalar else {
        return Ok(None);
    };
    let bytes = match scalar {
        Scalar::Int(value) => value,
        Scalar::Text(text) => parse_byte_size(&text).map_err(SerdeError::custom)?,
    };
    Ok(Some(ByteCount::from_bytes(bytes)))
}

#[cfg(feature = "serde")]
fn deserialize_sync<'de, Deser>(
    deserializer: Deser,
) -> std::result::Result<SyncPolicy, Deser::Error>
where
    Deser: Deserializer<'de>,
{
    let scalar = Scalar::deserialize(deserializer)?;
    match scalar {
        Scalar::Int(0) => Ok(SyncPolicy::EveryPut),
        Scalar::Int(value) => Ok(SyncPolicy::Bytes(ByteCount::from_bytes(value))),
        Scalar::Text(text) => parse_sync_text(&text).map_err(SerdeError::custom),
    }
}

#[cfg(feature = "serde")]
fn deserialize_compact_rate<'de, Deser>(
    deserializer: Deser,
) -> std::result::Result<CompactRate, Deser::Error>
where
    Deser: Deserializer<'de>,
{
    let scalar = Scalar::deserialize(deserializer)?;
    match scalar {
        Scalar::Int(value) => Ok(CompactRate::Mbps(value)),
        Scalar::Text(text) => parse_compact_rate(&text).map_err(SerdeError::custom),
    }
}

#[cfg(feature = "serde")]
fn deserialize_thread_budget<'de, Deser>(
    deserializer: Deser,
) -> std::result::Result<ThreadBudget, Deser::Error>
where
    Deser: Deserializer<'de>,
{
    let scalar = Scalar::deserialize(deserializer)?;
    match scalar {
        Scalar::Int(value) => {
            let count = u32::try_from(value)
                .map_err(|_| SerdeError::custom(format!("active_tails `{value}` is too large")))?;
            Ok(ThreadBudget::threads(count))
        }
        Scalar::Text(text) => parse_thread_budget(&text).map_err(SerdeError::custom),
    }
}

#[cfg(feature = "serde")]
fn parse_thread_budget(text: &str) -> std::result::Result<ThreadBudget, String> {
    let trimmed = text.trim();
    if trimmed.eq_ignore_ascii_case("auto") {
        return Ok(ThreadBudget::Auto);
    }

    let count: u32 = trimmed
        .parse()
        .map_err(|error| format!("active_tails `{text}` is not auto or a number: {error}"))?;
    Ok(ThreadBudget::threads(count))
}

#[cfg(feature = "serde")]
fn parse_sync_text(text: &str) -> std::result::Result<SyncPolicy, String> {
    let trimmed = text.trim();
    if trimmed.eq_ignore_ascii_case("never") {
        return Ok(SyncPolicy::Never);
    }

    let bytes = parse_byte_size(trimmed)?;
    if bytes == 0 {
        Ok(SyncPolicy::EveryPut)
    } else {
        Ok(SyncPolicy::Bytes(ByteCount::from_bytes(bytes)))
    }
}

#[cfg(feature = "serde")]
fn parse_compact_rate(text: &str) -> std::result::Result<CompactRate, String> {
    let trimmed = text.trim();
    if trimmed.eq_ignore_ascii_case("auto") {
        return Ok(CompactRate::Auto);
    }

    let value: u64 = trimmed
        .parse()
        .map_err(|error| format!("compact_mbps `{text}` is not auto or a number: {error}"))?;
    Ok(CompactRate::Mbps(value))
}

#[cfg(feature = "serde")]
fn parse_byte_size(spec: &str) -> std::result::Result<u64, String> {
    let trimmed = spec.trim();
    if trimmed.is_empty() {
        return Err("empty size".to_string());
    }

    let split = trimmed
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (number_part, unit_part) = trimmed.split_at(split);
    if number_part.is_empty() {
        return Err(format!("size `{spec}` has no leading number"));
    }

    let number: u64 = number_part
        .parse()
        .map_err(|error| format!("size `{spec}` has an invalid number: {error}"))?;
    let multiplier = byte_multiplier(unit_part.trim())?;

    number
        .checked_mul(multiplier)
        .ok_or_else(|| format!("size `{spec}` overflows"))
}

#[cfg(feature = "serde")]
fn byte_multiplier(unit: &str) -> std::result::Result<u64, String> {
    match unit.to_ascii_lowercase().as_str() {
        "" | "b" => Ok(1),
        "k" | "kb" | "kib" => Ok(KIB),
        "m" | "mb" | "mib" => Ok(MIB),
        "g" | "gb" | "gib" => Ok(GIB),
        "t" | "tb" | "tib" => Ok(TIB),
        other => Err(format!("unknown size unit `{other}`")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // a mode the kernel refuses has somewhere to step down to, and the floor has not
    #[test]
    fn a_completion_mode_steps_down_to_one_every_kernel_takes() {
        assert_eq!(
            TaskRun::Deferred.and_below(),
            [TaskRun::Deferred, TaskRun::Cooperative, TaskRun::Interrupt],
        );
        assert_eq!(
            TaskRun::Cooperative.and_below(),
            [TaskRun::Cooperative, TaskRun::Interrupt],
        );
        assert_eq!(
            TaskRun::Interrupt.and_below(),
            [TaskRun::Interrupt],
            "the floor asks for nothing, so it has nowhere to fall to",
        );
        for mode in TaskRun::Deferred.and_below() {
            assert_eq!(
                mode.and_below().last(),
                Some(&TaskRun::Interrupt),
                "a mode stepped down without reaching the one a ring runs unasked",
            );
        }
    }

    // the two modes that hold their work are the two a thread has to ask
    #[test]
    fn a_held_completion_is_one_the_thread_asks_for() {
        assert!(TaskRun::Deferred.is_asked_for());
        assert!(TaskRun::Cooperative.is_asked_for());
        assert!(
            !TaskRun::Interrupt.is_asked_for(),
            "a ring that interrupts for a completion has posted it already",
        );
    }

    // the floor is asked about the record, so one volume answers both ways
    #[test]
    fn a_floor_maps_the_large_record_and_not_the_small() {
        let config = ReelConfig {
            map_above: Some(ByteCount::mb(2)),
            ..ReelConfig::default()
        };

        assert!(
            !config.maps(64 * 1024),
            "a small record pays the whole window"
        );
        assert!(config.maps(4 * 1024 * 1024), "a large one dwarfs it");
    }

    // unset maps nothing, which is what a volume without peers wants
    #[test]
    fn no_floor_maps_nothing() {
        let config = ReelConfig::default();

        assert!(!config.maps(usize::MAX));
    }

    // a default config passes validation
    #[test]
    fn default_ok() {
        assert!(ReelConfig::default().validate().is_ok());
    }

    // a segment past a u32 offset is rejected
    #[test]
    fn segment_too_large() {
        let config = ReelConfig {
            segment_bytes: ByteCount::gb(4),
            ..ReelConfig::default()
        };

        assert!(config.validate().is_err());
    }

    // an alloc chunk larger than the segment is rejected
    #[test]
    fn alloc_over_segment() {
        let config = ReelConfig {
            segment_bytes: ByteCount::gb(1),
            alloc_chunk: ByteCount::gb(2),
            ..ReelConfig::default()
        };

        assert!(config.validate().is_err());
    }

    // a dead ratio outside the unit interval is rejected
    #[test]
    fn ratio_out_of_range() {
        let config = ReelConfig {
            compact_dead_ratio: 1.5,
            ..ReelConfig::default()
        };

        assert!(config.validate().is_err());
    }

    // a merge ratio outside the unit interval is rejected
    #[test]
    fn merge_ratio_out_of_range() {
        let config = ReelConfig {
            merge_dead_ratio: -0.5,
            ..ReelConfig::default()
        };

        assert!(config.validate().is_err());
    }

    // an automatic tail budget resolves to at least one and stops short of a wide machine
    #[test]
    fn auto_tails_are_bounded() {
        let auto = ThreadBudget::Auto.resolve_tails();

        assert!(auto >= 1);
        assert!(auto <= MAX_AUTO_TAILS);
        assert_eq!(ThreadBudget::threads(0).resolve_tails(), auto);
        assert_eq!(
            ThreadBudget::threads(32).resolve_tails(),
            32,
            "a named count is taken as given"
        );
    }

    // an every-put policy still validates
    #[test]
    fn every_put_ok() {
        let config = ReelConfig {
            sync: SyncPolicy::EveryPut,
            ..ReelConfig::default()
        };

        assert!(config.validate().is_ok());
    }

    // human byte sizes parse to their byte counts
    #[test]
    fn parses_sizes() {
        assert_eq!(parse_byte_size("1 GiB").expect("gib"), GIB);
        assert_eq!(parse_byte_size("64 MiB").expect("mib"), 64 * MIB);
        assert_eq!(parse_byte_size("1024").expect("bytes"), 1024);
        assert!(parse_byte_size("3 QiB").is_err());
    }

    // sync scalars map to the right policy
    #[test]
    fn sync_text() {
        assert_eq!(parse_sync_text("never").expect("never"), SyncPolicy::Never);
        assert_eq!(
            parse_sync_text("64 MiB").expect("bytes"),
            SyncPolicy::Bytes(ByteCount::mb(64)),
        );
        assert_eq!(parse_sync_text("0").expect("zero"), SyncPolicy::EveryPut);
    }

    // compact rate parses auto and a fixed cap
    #[test]
    fn compact_rate() {
        assert_eq!(parse_compact_rate("auto").expect("auto"), CompactRate::Auto);
        assert_eq!(
            parse_compact_rate("20").expect("num"),
            CompactRate::Mbps(20)
        );
        assert!(parse_compact_rate("fast").is_err());
    }

    // a thread budget parses auto, a cap, and zero as taking the machine's own
    #[test]
    fn thread_budget() {
        assert_eq!(
            parse_thread_budget("auto").expect("auto"),
            ThreadBudget::Auto
        );
        assert_eq!(parse_thread_budget("0").expect("zero"), ThreadBudget::Auto);
        assert_eq!(
            parse_thread_budget("2").expect("two"),
            ThreadBudget::Fixed(NonZeroU32::new(2).expect("two")),
        );
        assert!(parse_thread_budget("many").is_err());
        assert_eq!(ThreadBudget::threads(3).resolve(), 3);
        assert!(ThreadBudget::Auto.resolve() >= 1);
    }

    // io backend parses the shipped floor and the ring arms
    #[test]
    fn io_backend() {
        let posix: ReelConfig = serde_json::from_str(r#"{"io_backend":"posix"}"#).expect("posix");
        let ring: ReelConfig = serde_json::from_str(r#"{"io_backend":"uring"}"#).expect("uring");

        assert_eq!(posix.io_backend, IoBackend::Posix);
        assert_eq!(ring.io_backend, IoBackend::Uring);
        assert_eq!(IoBackend::default(), IoBackend::Posix);
    }

    // a non-default config deserializes every knob to its chosen value
    #[test]
    fn deserializes_json() {
        let raw = r#"{
            "segment_bytes": "512 MiB",
            "alloc_chunk": "32 MiB",
            "preallocate": "full",
            "sync_bytes": 0,
            "compact_dead_ratio": 0.25,
            "compact_mbps": 200,
            "scrub_mbps": 0,
            "verify_reads": false,
            "active_tails": 4,
            "io_backend": "uring_direct"
        }"#;

        let config: ReelConfig = serde_json::from_str(raw).expect("config");

        assert_eq!(config.segment_bytes, ByteCount::mb(512));
        assert_eq!(config.preallocate, Preallocate::Full);
        assert_eq!(config.sync, SyncPolicy::EveryPut);
        assert_eq!(config.compact_mbps, CompactRate::Mbps(200));
        assert_eq!(config.active_tails, ThreadBudget::threads(4));
        assert_eq!(config.io_backend, IoBackend::UringDirect);
        assert!(config.validate().is_ok());
    }

    // the full spec config block deserializes to the defaults and validates
    #[test]
    fn deserializes_spec_block() {
        let raw = r#"{
            "segment_bytes": "1 GiB",
            "alloc_chunk": "64 MiB",
            "preallocate": "full",
            "sync_bytes": "never",
            "compact_dead_ratio": 0.50,
            "compact_mbps": "auto",
            "scrub_mbps": 64,
            "verify_reads": false,
            "repair": "peers",
            "active_tails": "auto",
            "io_backend": "posix"
        }"#;

        let config: ReelConfig = serde_json::from_str(raw).expect("spec config");

        assert_eq!(config, ReelConfig::default());
        assert!(config.validate().is_ok());
    }

    // missing fields fall back to defaults
    #[test]
    fn deserializes_partial() {
        let config: ReelConfig = serde_json::from_str("{}").expect("empty config");

        assert_eq!(config, ReelConfig::default());
    }
}
