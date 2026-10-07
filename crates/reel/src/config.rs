//! Reel store configuration and load-time validation

use std::num::NonZeroU32;
use std::sync::OnceLock;

#[cfg(feature = "serde")]
use serde::Deserialize;

use crate::units::ByteCount;

use crate::error::{ReelError, Result};

const DEFAULT_SEGMENT_GIB: u64 = 1;
const DEFAULT_ALLOC_CHUNK_MIB: u64 = 64;
const DEFAULT_COMPACT_DEAD_RATIO: f64 = 0.50;
const DEFAULT_SCRUB_MBPS: u64 = 64;

/// Sealed descriptors the reader cache holds, unless the open-file limit says fewer
///
/// A store with more sealed segments than this drops a handle on every miss and remaps
/// the segment on the next read, so the default sits well above a volume's segment count.
pub const DEFAULT_FD_CACHE: u64 = 4096;

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
        // One ask per process skips repeat cgroup reads and keeps the tail layout fixed
        static WIDTH: OnceLock<usize> = OnceLock::new();
        match self {
            ThreadBudget::Auto => *WIDTH.get_or_init(|| {
                std::thread::available_parallelism()
                    .map(|width| width.get())
                    .unwrap_or(1)
            }),
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
pub struct VolumeSpec {
    /// Directory the volume is mounted at
    pub path: std::path::PathBuf,

    /// The tier, fast unless the entry says otherwise
    pub class: VolumeClass,

    /// The operator's word that this drive is dead, taking it out of every scan and draw
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

/// When the kernel runs the completion work a ring owes its owning thread
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskRun {
    /// Hold it until the thread enters asking for completions, on a kernel from 6.1
    ///
    /// Rests on a ring belonging to one thread, which is the promise this backend keeps.
    Deferred,

    /// Run it at the next kernel exit rather than interrupting for it, from 5.19
    Cooperative,

    /// Interrupt the thread for every completion, which is what a ring does unasked
    Interrupt,
}

/// Ring tunables
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RingTuning {
    /// Hand the kernel a pool of aligned buffers, which is what puts direct data ops on the ring
    pub registered_buffers: bool,

    /// When the kernel runs this ring's completion work, stepped down where refused
    pub taskrun: TaskRun,
}

impl Default for RingTuning {
    fn default() -> Self {
        Self {
            registered_buffers: true,
            taskrun: TaskRun::Deferred,
        }
    }
}

impl TaskRun {
    /// This mode and the ones below it, for a kernel that refuses the one asked for
    ///
    /// Deferred wants 6.1 and cooperative 5.19, and a refusal comes back as one errno with
    /// nothing naming the flag, so the answer is to try the next one down.
    pub fn and_below(self) -> &'static [TaskRun] {
        match self {
            TaskRun::Deferred => &[TaskRun::Deferred, TaskRun::Cooperative, TaskRun::Interrupt],
            TaskRun::Cooperative => &[TaskRun::Cooperative, TaskRun::Interrupt],
            TaskRun::Interrupt => &[TaskRun::Interrupt],
        }
    }

    /// Whether a thread has to ask the kernel before it reads its own queue
    ///
    /// Both non-default modes set `IORING_SQ_TASKRUN` when work is waiting, so a peek is a
    /// flag read; only under deferred is the ask what makes a completion appear.
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
pub enum PointReads {
    /// Read through the driver, one op per read whatever the cache holds
    Queued,
    /// Ask the cache without blocking, and queue only the reads it cannot answer
    Probed,
}

/// Where a sealed segment's fence over its blocks lives, off unless asked for
///
/// A fence is one lead per block of a partition's rows. Without it a blocked search
/// pays a block read per halving; with it the halvings happen over the leads and the
/// search reads one block.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
pub enum RepairPath {
    /// Another copy exists, so a corrupt record becomes a miss and a refetch
    Peers,
    /// Nothing else holds these bytes, so corruption is an error, never a miss
    None,
}

/// Load-time settings for one reel bulk volume
#[derive(Clone, Debug, PartialEq)]
pub struct ReelConfig {
    /// Target size of each segment file before it seals
    pub segment_bytes: ByteCount,

    /// Bytes reserved ahead of the write head per allocation step
    pub alloc_chunk: ByteCount,

    /// Whether a new segment reserves per step or is pre-written whole
    pub preallocate: Preallocate,

    /// Durability sync cadence for group commit drains
    pub sync: SyncPolicy,

    /// Dead fraction at which a sealed segment is rewritten
    pub compact_dead_ratio: f64,

    /// Compaction rate limit, automatic or a fixed cap
    pub compact_mbps: CompactRate,

    /// Background scrub rate over sealed segments, off when set to zero
    pub scrub_mbps: u64,

    /// Whether each read verifies the record checksum
    pub verify_reads: bool,

    /// Where a record that fails its checksum can be fetched again from
    pub repair: RepairPath,

    /// Smallest record served from a read-only mapping of its segment file, unset maps nothing
    pub map_above: Option<ByteCount>,

    /// Which plane a window of a large record is read on
    pub ranged_reads: RangedReads,

    /// Whether an awaited whole-record read asks the page cache before it queues
    pub point_reads: PointReads,

    /// Append tails the volume runs, which is how many files it appends into
    pub active_tails: ThreadBudget,

    /// Extra volume roots the reel places segments across, past its own fast root
    pub volumes: Vec<VolumeSpec>,

    /// File backend selected for this volume
    pub io_backend: IoBackend,

    /// Ring tunables, which only mean anything under a ring backend
    pub uring: RingTuning,

    /// Bits per key a seal spends on each column's filter, zero for no filter
    pub filter_bits: u8,

    /// Where a sealed segment's fence over its blocks lives, off unless asked for
    pub fence: FenceResidency,

    /// Bytes of sealed-footer state the volume keeps at once
    pub footer_cache: ByteCount,
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

    /// Whether reads into an open tail go through its mapping, whatever `map_above` says
    pub fn maps_tails(&self) -> bool {
        self.io_backend != IoBackend::UringDirect && self.ranged_reads == RangedReads::Cached
    }
}

impl Default for ReelConfig {
    fn default() -> Self {
        Self {
            segment_bytes: ByteCount::gb(DEFAULT_SEGMENT_GIB),
            alloc_chunk: ByteCount::mb(DEFAULT_ALLOC_CHUNK_MIB),
            preallocate: Preallocate::Full,
            sync: SyncPolicy::Never,
            compact_dead_ratio: DEFAULT_COMPACT_DEAD_RATIO,
            compact_mbps: CompactRate::Auto,
            scrub_mbps: DEFAULT_SCRUB_MBPS,
            verify_reads: false,
            repair: RepairPath::Peers,
            map_above: None,
            ranged_reads: RangedReads::Cached,
            point_reads: PointReads::Queued,
            footer_cache: ByteCount::mb(DEFAULT_FOOTER_CACHE_MIB),
            active_tails: ThreadBudget::Auto,
            volumes: Vec::new(),
            io_backend: IoBackend::default(),
            uring: RingTuning::default(),
            filter_bits: DEFAULT_FILTER_BITS,
            fence: FenceResidency::Off,
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

    /// How many compaction passes run at once, one for every two tails
    pub fn compact_passes(&self) -> usize {
        // One bit in a word leases each reserved tail, which caps the passes at the word's width
        self.tail_count().div_ceil(2).min(64)
    }

    /// Whether this volume's seals write a fence over each partition's blocks
    pub fn seal_fences(&self) -> bool {
        self.fence != FenceResidency::Off
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

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    // io backend parses the shipped floor and the ring arms
    #[test]
    fn io_backend() {
        let posix: IoBackend = serde_json::from_str(r#""posix""#).expect("posix");
        let ring: IoBackend = serde_json::from_str(r#""uring""#).expect("uring");
        let direct: IoBackend = serde_json::from_str(r#""uring_direct""#).expect("uring_direct");

        assert_eq!(posix, IoBackend::Posix);
        assert_eq!(ring, IoBackend::Uring);
        assert_eq!(direct, IoBackend::UringDirect);
        assert_eq!(IoBackend::default(), IoBackend::Posix);
    }
}
