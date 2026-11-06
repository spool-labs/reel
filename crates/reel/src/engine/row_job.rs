//! Row write-out: every group reaches the kernel within 100 ms of its puts returning

use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_api::units::{ByteCount, Nanoseconds};

use crate::engine::engine_state::EngineState;
use crate::engine::open_segment::{OpenSegment, SegmentPhase};
use crate::guard::hold;
use crate::index::open_slots::OpenSlot;
use crate::io::Io;
use crate::io::job::{Job, JobStep};

/// How long rows wait in memory, well inside the 100 ms a kill can cost
pub const WRITE_OUT_PERIOD: Nanoseconds = Nanoseconds(50_000_000);
/// An open segment this large seals once it sits with no write
pub const IDLE_SEAL_BYTES: ByteCount = ByteCount(64 * 1024 * 1024);
/// How long a large open segment sits before it seals
pub const IDLE_SEAL_AFTER: Nanoseconds = Nanoseconds(3_000_000_000);

/// The job that sends waiting rows to the kernel
pub struct RowJob<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
    pub last_sent: Nanoseconds,
}

impl<Backend: Io> RowJob<Backend> {
    /// Send every waiting group, and say whether there were any
    pub fn send_all(&self) -> bool {
        let mut has_sent = false;
        for slot in 0..self.state.queues.len() {
            let slot = OpenSlot(slot as u8);
            if !self.state.has_waiting_rows(slot) {
                continue;
            }
            has_sent = true;

            // a failed group stays in place for the next pass, and only a full disk can pass by itself
            if let Err(error) = self.state.write_out_if_free(slot)
                && error.kind() != ErrorKind::StorageFull
            {
                self.fail_volume_of(slot);
            }
        }
        has_sent
    }

    /// Fail the volume a slot belongs to, which then refuses writes
    pub fn fail_volume_of(&self, slot: OpenSlot) {
        for volume in &self.state.volumes {
            let holds_slot = hold(&volume.claims).open.iter().any(|segment| segment.slot == slot);
            if holds_slot {
                volume.fail();
            }
        }
    }

    /// Seal what has sat with no write long enough, and say when to look at what hasn't yet. A segment under the size stays,
    /// since sealing it would only leave a small one to merge
    pub fn seal_idle_segments(&self) -> Option<Nanoseconds> {
        let now = self.state.io.now();
        let due = Nanoseconds(self.last_sent.0 + IDLE_SEAL_AFTER.0);
        let mut deadline = None;
        for volume in &self.state.volumes {
            let is_large = |segment: &OpenSegment<Backend::File>| {
                segment.phase == SegmentPhase::Taking && segment.write_end.0 >= IDLE_SEAL_BYTES.0
            };
            if !hold(&volume.claims).open.iter().any(is_large) {
                continue;
            }
            if now.0 >= due.0 {
                self.state.seal_fullest(volume);
            } else {
                deadline = Some(due);
            }
        }
        deadline
    }

    /// Check any slot has rows waiting
    pub fn has_rows(&self) -> bool {
        for slot in 0..self.state.queues.len() {
            if self.state.has_waiting_rows(OpenSlot(slot as u8)) {
                return true;
            }
        }
        false
    }
}

impl<Backend: Io> Job for RowJob<Backend> {
    fn step(&mut self) -> JobStep {
        if self.send_all() {
            let now = self.state.io.now();
            self.last_sent = now;
            return JobStep::IdleUntil {
                deadline: Nanoseconds(now.0 + WRITE_OUT_PERIOD.0),
            };
        }

        // a row that lands after this store wakes the job, and one that landed before it is seen here
        self.state.row_job_idle.store(true, Ordering::Release);
        if self.has_rows() {
            self.state.row_job_idle.store(false, Ordering::Release);
            return JobStep::Worked;
        }
        match self.seal_idle_segments() {
            Some(deadline) => JobStep::IdleUntil { deadline },
            None => JobStep::Idle,
        }
    }
}
