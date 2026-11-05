//! A job the simulated scheduler steps

use std::io::Result;
use std::sync::{Arc, Mutex};

use reel_api::units::{Nanoseconds, ProcessId};

use reel::guard::hold;
use reel::io::IoTask;
use reel::io::job::Job;

/// A job's place in the scheduler
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct JobIndex(pub usize);

/// A count of jobs
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct JobCount(pub u64);

/// What a job waits for before it can run again
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobWait {
    /// Nothing, so it can run
    Nothing,
    /// A wake
    Wake,
    /// A wake or a clock reading
    Clock { deadline: Nanoseconds },
}

/// One job and how far it has gone
pub struct JobSlot {
    pub name: &'static str,
    pub process: ProcessId,
    pub work: Option<Box<dyn Job>>,
    pub wait: JobWait,
    pub is_woken: bool,
    pub is_stopped: bool,
}

/// The handle of a job the scheduler owns, good only for the process that started it
pub struct SimulatedTask {
    pub slots: Arc<Mutex<Vec<JobSlot>>>,
    pub index: JobIndex,
    pub process: ProcessId,
}

impl JobSlot {
    /// Check whether the job can run at a clock reading
    pub fn is_runnable(&self, now: Nanoseconds) -> bool {
        if self.work.is_none() {
            return false;
        }
        match self.wait {
            JobWait::Nothing => true,
            JobWait::Wake => self.is_woken,
            JobWait::Clock { deadline } => self.is_woken || deadline <= now,
        }
    }
}

impl IoTask for SimulatedTask {
    fn wake(&self) {
        let mut slots = hold(&self.slots);
        if let Some(slot) = slots.get_mut(self.index.0)
            && slot.process == self.process
        {
            slot.is_woken = true;
        }
    }

    fn stop(&self) -> Result<()> {
        let mut slots = hold(&self.slots);
        if let Some(slot) = slots.get_mut(self.index.0)
            && slot.process == self.process
        {
            slot.is_stopped = true;
            slot.work = None;
        }
        Ok(())
    }
}
