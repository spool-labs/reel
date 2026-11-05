//! A job running on its own named thread

use std::io::{Error, Result};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use reel_api::units::Nanoseconds;

use crate::guard::hold;
use crate::io::job::{Job, JobStep};
use crate::io::{Io, IoTask};

/// What the job's thread is told
#[derive(Debug, Default)]
pub struct TaskFlags {
    pub is_woken: bool,
    pub is_stopping: bool,
}

/// How a handle reaches the job's thread
#[derive(Debug, Default)]
pub struct TaskSignal {
    pub flags: Mutex<TaskFlags>,
    pub changed: Condvar,
}

/// The handle of a job on its own thread
#[derive(Debug)]
pub struct PlainTask {
    signal: Arc<TaskSignal>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl TaskFlags {
    /// Check whether the job has to keep sleeping
    pub fn is_quiet(&self) -> bool {
        !self.is_woken && !self.is_stopping
    }
}

impl TaskSignal {
    /// Check whether the job should end
    pub fn is_stopping(&self) -> bool {
        hold(&self.flags).is_stopping
    }

    /// Sleep until a wake or a stop
    pub fn wait(&self) {
        let flags = hold(&self.flags);
        let mut flags = match self.changed.wait_while(flags, |flags| flags.is_quiet()) {
            Ok(flags) => flags,
            Err(poisoned) => poisoned.into_inner(),
        };
        flags.is_woken = false;
    }

    /// Sleep until a wake, a stop or the time running out
    pub fn wait_for(&self, limit: Duration) {
        let flags = hold(&self.flags);
        let mut flags = match self.changed.wait_timeout_while(flags, limit, |flags| flags.is_quiet()) {
            Ok(outcome) => outcome.0,
            Err(poisoned) => poisoned.into_inner().0,
        };
        flags.is_woken = false;
    }
}

impl PlainTask {
    /// Wrap a started thread
    pub fn new(signal: Arc<TaskSignal>, thread: JoinHandle<()>) -> Self {
        let task = Self {
            signal,
            thread: Mutex::new(Some(thread)),
        };
        // some platforms set a lock up at its first use, and that's a job for open
        drop(hold(&task.thread));
        task
    }
}

impl Drop for PlainTask {
    fn drop(&mut self) {
        // a drop has no caller to tell, and a clean close has already stopped the job
        let _stopped = self.stop();
    }
}

impl IoTask for PlainTask {
    fn wake(&self) {
        hold(&self.signal.flags).is_woken = true;
        self.signal.changed.notify_all();
    }

    fn stop(&self) -> Result<()> {
        hold(&self.signal.flags).is_stopping = true;
        self.signal.changed.notify_all();

        let mut slot = hold(&self.thread);
        // a job stopping itself ends after this step, and can't wait for its own thread
        if let Some(thread) = slot.as_ref()
            && thread.thread().id() == thread::current().id()
        {
            return Ok(());
        }

        let thread = slot.take();
        drop(slot);
        if let Some(thread) = thread {
            return match thread.join() {
                Ok(()) => Ok(()),
                Err(_panic) => Err(Error::other("job thread panicked")),
            };
        }
        Ok(())
    }
}

/// Step a job until it finishes or its handle stops it
pub fn run_job<Work: Job, Backend: Io>(mut job: Work, signal: &TaskSignal, io: &Backend) {
    loop {
        if signal.is_stopping() {
            return;
        }
        match job.step() {
            JobStep::Worked => {},
            JobStep::Idle => signal.wait(),
            JobStep::IdleUntil { deadline } => signal.wait_for(time_left(deadline, io.now())),
            JobStep::Finished => return,
        }
    }
}

/// Measure the time from a clock reading to a deadline
pub fn time_left(deadline: Nanoseconds, now: Nanoseconds) -> Duration {
    Duration::from_nanos(deadline.0.saturating_sub(now.0))
}
