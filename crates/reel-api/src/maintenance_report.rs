//! Each maintenance job's state and the store's reclaim counts

use crate::units::{ByteCount, Nanoseconds, RowCount, SegmentCount};

/// How many kinds of job the engine runs
pub const JOB_KINDS: usize = 7;

/// A kind of maintenance job
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum JobKind {
    /// Row groups reaching the kernel
    #[default]
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

/// What a job is doing
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum JobState {
    /// Nothing to do
    #[default]
    Idle,
    /// Working
    Running,
    /// Moved nothing for an hour while work stands
    Stalled,
    /// Ended by close
    Stopped,
}

/// One job's state and totals
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct JobReport {
    pub kind: JobKind,
    pub state: JobState,
    pub bytes_moved: ByteCount,
    pub last_run: Option<Nanoseconds>,
    pub cpu_time: Nanoseconds,
}

/// Every job and the reclaim counts
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MaintenanceReport {
    pub jobs: [JobReport; JOB_KINDS],
    pub graves_dropped: RowCount,
    pub graves_carried: RowCount,
    pub segments_rewritten: SegmentCount,
    pub segments_unlinked: SegmentCount,
}
