//! Each maintenance job's state and the store's reclaim counts

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
}

/// Every job, the reclaim counts and the readings a node roll stops on, in buffers the caller reuses
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaintenanceReport {
    pub jobs: [JobReport; JOB_KINDS.len()],
    pub volumes: Vec<VolumeReading>,
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
    }

    /// Start a report with one idle entry per kind of job
    pub fn new() -> Self {
        Self {
            jobs: JOB_KINDS.map(JobReport::new),
            volumes: Vec::new(),
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
        }
    }
}

impl Default for MaintenanceReport {
    fn default() -> Self {
        Self::new()
    }
}
