//! Reel store configuration and load-time validation

use std::num::NonZeroU32;
use std::sync::OnceLock;

#[cfg(feature = "serde")]
use serde::Deserialize;

use crate::units::ByteCount;

use crate::error::{ReelError, Result};

const DEFAULT_SEGMENT_GIB: u64 = 1;
const DEFAULT_COMPACT_DEAD_RATIO: f64 = 0.50;
const DEFAULT_SCRUB_MBPS: u64 = 64;

/// The reader cache holds this many sealed descriptors, unless the open-file limit is lower
pub const DEFAULT_FD_CACHE: u64 = 4096;

const DEFAULT_FOOTER_CACHE_MIB: u64 = 64;
const DEFAULT_FILTER_BITS: u8 = 10;

/// An automatic budget opens at most this many tails per volume, however wide the machine
const MAX_AUTO_TAILS: usize = 8;

pub(crate) const KIB: u64 = 1024;
pub(crate) const MIB: u64 = KIB * 1024;
pub(crate) const GIB: u64 = MIB * 1024;

/// Durability sync cadence for group commit drains
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncPolicy {
    /// Never sync on the hot path, and a seal still syncs, so only the tail is at risk
    Never,
    /// Sync once this many bytes accumulate across a drain, and zero syncs every write
    Bytes(ByteCount),
}

/// Compaction rate limit, unpaced unless a cap is set
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompactRate {
    /// Unpaced: the pass runs at device speed while there is debt to drain
    Auto,
    /// Cap read plus write traffic at this many megabytes per second, inside and between passes
    Mbps(u64),
}

/// How many append tails a volume may run, automatic or a fixed count
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

    /// The cap resolved for append tails, held to `MAX_AUTO_TAILS` for an automatic budget
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VolumeSpec {
    /// The volume's mount directory
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
    /// Hold it until the owning thread asks for completions, on a kernel from 6.1
    Deferred,

    /// Run it at the next kernel exit, on a kernel from 5.19
    Cooperative,

    /// Interrupt the thread for every completion, the ring's default
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
    pub fn and_below(self) -> &'static [TaskRun] {
        match self {
            TaskRun::Deferred => &[TaskRun::Deferred, TaskRun::Cooperative, TaskRun::Interrupt],
            TaskRun::Cooperative => &[TaskRun::Cooperative, TaskRun::Interrupt],
            TaskRun::Interrupt => &[TaskRun::Interrupt],
        }
    }

    /// Whether a thread has to ask the kernel before it reads its own queue
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

/// Where a record that fails its checksum can be fetched again from
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepairPath {
    /// Another copy exists, so a corrupt record becomes a miss and a refetch
    Peers,
    /// Nothing else holds these bytes, so corruption is an error
    None,
}

/// Load-time settings for one reel bulk volume
#[derive(Clone, Debug, PartialEq)]
pub struct ReelConfig {
    /// Target size of each segment file before it seals
    pub segment_bytes: ByteCount,

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

    /// Records this size and up are served from a read-only mapping, unset maps nothing
    pub map_above: Option<ByteCount>,

    /// How many append tails the volume runs, one file each
    pub active_tails: ThreadBudget,

    /// Extra volume roots for segments, past the reel's own fast root
    pub volumes: Vec<VolumeSpec>,

    /// The volume's file backend
    pub io_backend: IoBackend,

    /// Ring tunables, which only mean anything under a ring backend
    pub uring: RingTuning,

    /// Filter bits per key for each column at seal, zero for no filter
    pub filter_bits: u8,

    /// How many bytes of sealed-footer state the volume keeps at once
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
        self.io_backend != IoBackend::UringDirect
    }
}

impl Default for ReelConfig {
    fn default() -> Self {
        Self {
            segment_bytes: ByteCount::gb(DEFAULT_SEGMENT_GIB),
            sync: SyncPolicy::Never,
            compact_dead_ratio: DEFAULT_COMPACT_DEAD_RATIO,
            compact_mbps: CompactRate::Auto,
            scrub_mbps: DEFAULT_SCRUB_MBPS,
            verify_reads: false,
            repair: RepairPath::Peers,
            map_above: None,
            footer_cache: ByteCount::mb(DEFAULT_FOOTER_CACHE_MIB),
            active_tails: ThreadBudget::Auto,
            volumes: Vec::new(),
            io_backend: IoBackend::default(),
            uring: RingTuning::default(),
            filter_bits: DEFAULT_FILTER_BITS,
        }
    }
}

impl ReelConfig {
    /// How many append tails this volume runs, floored at one per fast volume
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

        assert_eq!(ThreadBudget::threads(3).resolve(), 3);
        assert!(ThreadBudget::Auto.resolve() >= 1);
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
