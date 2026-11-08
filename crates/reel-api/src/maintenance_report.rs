//! Each maintenance job's state and the store's reclaim counts

use crate::column_id::ColumnId;
use crate::units::{ByteCount, Nanoseconds, RowCount, SegmentCount, VolumeId};

/// Every kind of job, in the order a report lists them
pub const JOB_KINDS: [JobKind; 7] = [
    JobKind::RowWriteOut,
    JobKind::Seal,
    JobKind::DeadByteWalk,
    JobKind::RangeDeleteApply,
    JobKind::Compaction,
    JobKind::KeyRunMerge,
    JobKind::SpotCacheUpkeep,
];

/// A kind of maintenance job
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum JobKind {
    /// Row groups reaching the kernel
    RowWriteOut,
    /// Open segments becoming sealed ones
    Seal,
    /// Counting superseded versions after a seal
    DeadByteWalk,
    /// Unlinking segments a range delete covers
    RangeDeleteApply,
    /// Merging and rewriting sealed segments
    Compaction,
    /// Merging key runs
    KeyRunMerge,
    /// Keeping the spot cache in its budget
    SpotCacheUpkeep,
}

impl JobKind {
    /// Read this kind's place in `JOB_KINDS`
    pub const fn index(self) -> usize {
        self as usize
    }
}

/// What a job is doing
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum JobState {
    /// Nothing to do
    Idle,
    /// Working
    Running,
    /// Moved nothing for an hour while work stands
    Stalled,
    /// Ended by close
    Stopped,
}

/// One job's state and totals
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobReport {
    pub kind: JobKind,
    pub state: JobState,
    pub bytes_moved: ByteCount,
    pub last_run: Option<Nanoseconds>,
}

/// One volume's readings a node roll watches
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VolumeReading {
    pub volume: VolumeId,
    pub segment_count: SegmentCount,
    pub used_bytes: ByteCount,
    pub is_spinning: bool,
    /// The class came from the config, where a false here means it was detected
    pub is_device_set: bool,
    /// Rewrites each lane has finished: the planner's, then a worker's each
    pub rewrites_by_lane: [u64; MOST_REPORTED_LANES],
    /// Lifetime groups open now
    pub lifetime_groups: usize,
    /// Segments of marked records sealed since open
    pub marked_seals: u64,
    /// Record bytes those segments held at seal
    pub marked_seal_bytes: ByteCount,
    /// Marked batches that held a delete, which went to the unmarked segments with their marks dropped
    pub marks_dropped: u64,
    /// Marked segments rewritten at the early bar, while their highest mark stood
    pub early_bar_rewrites: u64,
    /// Marked segments rewritten at the plain bar, once the current mark had passed their highest
    pub plain_bar_rewrites: u64,
    pub longest_seal_steps: SealSteps,
    /// Record rewrites that copied in key order, reading records at random
    pub key_order_copies: u64,
}

/// Lanes a volume reading has room for. Workers past this show in the last one
pub const MOST_REPORTED_LANES: usize = 16;

/// How a complete column stands. A read-only view has no table, and reads through the footers
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompleteReading {
    pub column: ColumnId,
    pub has_table: bool,
    pub used_slots: RowCount,
    pub rows_past_capacity: RowCount,
}

/// How a column that declares a least value stands: the values written under it since open, which land as any other and size
/// nothing
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LeastValueReading {
    pub column: ColumnId,
    pub least_value: ByteCount,
    pub values_under_least: u64,
}

/// Every job, the reclaim counts and the readings a node roll stops on, in buffers the caller reuses
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaintenanceReport {
    pub jobs: [JobReport; JOB_KINDS.len()],
    pub volumes: Vec<VolumeReading>,
    pub complete_columns: Vec<CompleteReading>,
    pub least_value_columns: Vec<LeastValueReading>,
    pub graves_at_open: RowCount,
    pub deletes_since_open: RowCount,
    pub graves_dropped: RowCount,
    pub graves_carried: RowCount,
    pub segments_rewritten: SegmentCount,
    pub segments_unlinked: SegmentCount,
    pub rewrites_reclaiming_nothing: SegmentCount,
    pub corruption_errors: RowCount,
    pub damaged_segments: SegmentCount,
    pub summary_rows_dropped: RowCount,
    pub refused_reads: RowCount,
    pub reclaim_retries: RowCount,
    pub segments_parked: SegmentCount,
    pub empties_left: SegmentCount,
    pub stray_outputs: SegmentCount,
    /// How long the filter arena took to fill after open, which is the store's restart window
    pub arena_fill: Option<Nanoseconds>,
    /// Async reads complete through the ring, and through the blocking pool when this is false
    pub has_ring: bool,
}

impl JobReport {
    /// Start an idle job with nothing moved
    pub fn new(kind: JobKind) -> Self {
        Self {
            kind,
            state: JobState::Idle,
            bytes_moved: ByteCount::default(),
            last_run: None,
        }
    }
}

impl MaintenanceReport {
    /// Forget the volume readings and keep the buffer
    pub fn clear(&mut self) {
        self.volumes.clear();
        self.complete_columns.clear();
        self.least_value_columns.clear();
    }

    /// Start a report with one idle entry per kind of job
    pub fn new() -> Self {
        Self {
            jobs: JOB_KINDS.map(JobReport::new),
            volumes: Vec::new(),
            complete_columns: Vec::new(),
            least_value_columns: Vec::new(),
            graves_at_open: RowCount::default(),
            deletes_since_open: RowCount::default(),
            graves_dropped: RowCount::default(),
            graves_carried: RowCount::default(),
            segments_rewritten: SegmentCount::default(),
            segments_unlinked: SegmentCount::default(),
            rewrites_reclaiming_nothing: SegmentCount::default(),
            corruption_errors: RowCount::default(),
            damaged_segments: SegmentCount::default(),
            summary_rows_dropped: RowCount::default(),
            refused_reads: RowCount::default(),
            reclaim_retries: RowCount::default(),
            segments_parked: SegmentCount::default(),
            empties_left: SegmentCount::default(),
            stray_outputs: SegmentCount::default(),
            arena_fill: None,
            has_ring: false,
        }
    }
}

impl Default for MaintenanceReport {
    fn default() -> Self {
        Self::new()
    }
}

/// The longest each step of a seal has run since open, each a stretch with no room coming back on its own
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SealSteps {
    /// Waiting for the segment's writers to land
    pub drain: Nanoseconds,
    /// One step of the walk
    pub walk_step: Nanoseconds,
    /// Writing and syncing the footer
    pub footer: Nanoseconds,
    /// Listing the segment and saving its summary
    pub install: Nanoseconds,
    /// One step of cleaning the write index
    pub clean_step: Nanoseconds,
}
