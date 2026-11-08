//! What each kind of job has moved and when, which `maintenance_report` reads

use std::sync::atomic::{AtomicU64, Ordering};

use reel_api::maintenance_report::{JOB_KINDS, UNIT_KINDS, UnitReading};
use reel_api::units::{ByteCount, Nanoseconds};

/// A job with work standing that moved nothing for this long shows as stalled, so a loop never reads as activity
pub const STALL_AFTER: Nanoseconds = Nanoseconds(3_600_000_000_000);
/// A clock reading no job ran at
pub const NEVER: u64 = 0;

/// One kind of job's totals
#[derive(Debug, Default)]
pub struct JobTally {
    bytes_moved: AtomicU64,
    last_run: AtomicU64,
}

/// Every kind's totals, in the order of `JOB_KINDS`
pub type JobTallies = [JobTally; JOB_KINDS.len()];

impl JobTally {
    /// Count bytes a job moved
    pub fn note_moved(&self, bytes: ByteCount, now: Nanoseconds) {
        self.bytes_moved.fetch_add(bytes.0, Ordering::Relaxed);
        self.last_run.store(now.0.max(1), Ordering::Relaxed);
    }

    /// Read the bytes moved so far
    pub fn bytes_moved(&self) -> ByteCount {
        ByteCount(self.bytes_moved.load(Ordering::Relaxed))
    }

    /// Read when the job last moved anything
    pub fn last_run(&self) -> Option<Nanoseconds> {
        let last = self.last_run.load(Ordering::Relaxed);
        (last != NEVER).then_some(Nanoseconds(last))
    }
}

/// The largest unit of one kind so far, in work and in wall time, and the sums over every unit
#[derive(Debug, Default)]
pub struct UnitTally {
    work: AtomicU64,
    time: AtomicU64,
    work_sum: AtomicU64,
    time_sum: AtomicU64,
}

/// Every kind's tally, in the order of `UNIT_KINDS`
pub type UnitTallies = [UnitTally; UNIT_KINDS.len()];

impl UnitTally {
    /// Note one unit's work and how long it ran
    pub fn note(&self, work: u64, time: Nanoseconds) {
        self.work.fetch_max(work, Ordering::Relaxed);
        self.time.fetch_max(time.0, Ordering::Relaxed);
        self.work_sum.fetch_add(work, Ordering::Relaxed);
        self.time_sum.fetch_add(time.0, Ordering::Relaxed);
    }

    /// Read the largest work and the longest time so far, and the sums
    pub fn reading(&self) -> UnitReading {
        UnitReading {
            work: self.work.load(Ordering::Relaxed),
            time: Nanoseconds(self.time.load(Ordering::Relaxed)),
            work_sum: self.work_sum.load(Ordering::Relaxed),
            time_sum: Nanoseconds(self.time_sum.load(Ordering::Relaxed)),
        }
    }
}
