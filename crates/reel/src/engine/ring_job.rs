//! The job that takes a ring's completions and wakes the reads waiting on them

use std::sync::Arc;

use reel_api::units::Nanoseconds;

use crate::engine::engine_state::EngineState;
use crate::io::job::{Job, JobStep};
use crate::io::ring::{RING_READS, ReadRing, RingDriver};
use crate::io::Io;
use crate::pool::Pool;
use crate::record::record_reader::RecordReader;
use crate::segment::footer_reader::FooterReader;

/// How long a reaper waits after its driver fails a look
pub const RING_RETRY: Nanoseconds = Nanoseconds(10_000_000);

/// A job that reaps one ring
pub struct RingReaper<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
}

impl<Backend: Io> Job for RingReaper<Backend> {
    fn step(&mut self) -> JobStep {
        let Some(ring) = &self.state.ring else {
            return JobStep::Finished;
        };
        match ring.driver.reap(&mut |slot, answer| ring.finish(slot, answer)) {
            Ok(false) => JobStep::Idle,
            Ok(true) => JobStep::Worked,
            // a kernel that refuses the wait now may take it later, and a spin would cost a core until then
            Err(_) => JobStep::IdleUntil { deadline: Nanoseconds(self.state.io.now().0 + RING_RETRY.0) },
        }
    }
}

impl<Backend: Io> EngineState<Backend> {
    /// Give the store a ring and the buffers its reads hold while they wait
    pub fn attach_ring(&mut self, driver: Option<Backend::Ring>) {
        let Some(driver) = driver else {
            return;
        };
        self.ring = Some(ReadRing::new(driver));
        self.ring_record_readers = Pool::new((0..RING_READS).map(|_reader| RecordReader::new()).collect());
        self.ring_footer_readers = Pool::new((0..RING_READS).map(|_reader| FooterReader::without_tail()).collect());
    }

    /// Refuse new ring reads, let the ones in flight land, and bring the reaper out of its wait. The kernel answers every read
    /// it took, a dying device included, and the errors a wait can return pass, so this ends
    pub fn drain_ring(&self) {
        let Some(ring) = &self.ring else {
            return;
        };
        ring.close();
        while !ring.is_drained() {
            self.io.pause();
        }
        ring.driver.poke();
    }
}
