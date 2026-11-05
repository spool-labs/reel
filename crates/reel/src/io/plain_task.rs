//! A job running on its own named thread

use std::io::{Error, Result};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::guard::hold;
use crate::io::IoTask;
use crate::io::job::{Job, JobStep};

/// How long an idle job sleeps before it looks for work again
pub const IDLE_WAIT: Duration = Duration::from_millis(10);

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

impl TaskSignal {
    /// Check whether the job should end
    pub fn is_stopping(&self) -> bool {
        hold(&self.flags).is_stopping
    }

    /// Sleep until a wake, a stop or the idle wait running out
    pub fn wait(&self) {
        let mut flags = hold(&self.flags);
        if !flags.is_woken && !flags.is_stopping {
            flags = match self.changed.wait_timeout(flags, IDLE_WAIT) {
                Ok(outcome) => outcome.0,
                Err(poisoned) => poisoned.into_inner().0,
            };
        }
        flags.is_woken = false;
    }
}

impl PlainTask {
    /// Wrap a started thread
    pub fn new(signal: Arc<TaskSignal>, thread: JoinHandle<()>) -> Self {
        Self {
            signal,
            thread: Mutex::new(Some(thread)),
        }
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

        let thread = hold(&self.thread).take();
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
pub fn run_job<Work: Job>(mut job: Work, signal: &TaskSignal) {
    loop {
        if signal.is_stopping() {
            return;
        }
        match job.step() {
            JobStep::Worked => {},
            JobStep::Idle => signal.wait(),
            JobStep::Finished => return,
        }
    }
}
