//! A job the simulated scheduler steps

use std::io::Result;
use std::sync::{Arc, Mutex};

use crate::guard::hold;
use crate::io::IoTask;
use crate::io::job::Job;

/// One job and how far it has gone
pub struct JobSlot {
    pub name: &'static str,
    pub work: Option<Box<dyn Job>>,
    pub is_woken: bool,
    pub is_finished: bool,
}

/// The handle of a job the scheduler owns
pub struct SimulatedTask {
    pub slots: Arc<Mutex<Vec<JobSlot>>>,
    pub index: usize,
}

impl IoTask for SimulatedTask {
    fn wake(&self) {
        if let Some(slot) = hold(&self.slots).get_mut(self.index) {
            slot.is_woken = true;
        }
    }

    fn stop(&self) -> Result<()> {
        if let Some(slot) = hold(&self.slots).get_mut(self.index) {
            slot.is_finished = true;
            slot.work = None;
        }
        Ok(())
    }
}
