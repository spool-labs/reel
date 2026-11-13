//! The store shape a planner-facing sim opens by default: a planner alone on each volume, or workers beside it when the run sets
//! the workers pass's variable, so one test body checks both shapes. The budget stays the test's own, since a larger one moves
//! the table's room and every count a test reads off it

use std::env;

use reel::config::{NO_WORKERS, ThreadCounts};
use reel::engine::compact_job::CompactJob;
use reel::engine::rewrite_job::RewriteJob;
use reel::guard::hold;
use reel_sim::simulated_io::SimulatedIo;

/// A run that sets this variable opens the planner-facing sims with that many workers on each volume
pub const WORKERS_VARIABLE: &str = "REEL_SIM_WORKERS";

/// Read the worker count the run asks for, none when the variable is unset. A value that isn't a count stops the run, or a
/// typo would pass for a run with no workers
pub fn workers() -> usize {
    match env::var(WORKERS_VARIABLE) {
        Ok(count) => count.parse().unwrap_or_else(|_error| panic!("{WORKERS_VARIABLE} is {count:?}, which isn't a count")),
        Err(_unset) => 0,
    }
}

/// Read the thread counts a sim's default store opens with
pub fn threads() -> ThreadCounts {
    ThreadCounts { maintenance_workers: Some(workers()), ..NO_WORKERS }
}

/// Check any of a job's rewrites passes a test: the planner's own lane, or a seat a worker would run, which no worker steps
/// while a test drives the job by hand
pub fn any_rewrite(job: &CompactJob<SimulatedIo>, test: impl Fn(&RewriteJob<SimulatedIo>) -> bool) -> bool {
    test(&job.rewrite) || job.seats.iter().any(|seat| hold(&seat.job).as_ref().is_some_and(&test))
}

/// Check a rewrite runs on the planner's lane or in a seat
pub fn is_rewriting(job: &CompactJob<SimulatedIo>) -> bool {
    any_rewrite(job, RewriteJob::is_running)
}

/// Step every running rewrite of a job to its end, the seats' as well
pub fn finish_rewrites(job: &mut CompactJob<SimulatedIo>) {
    while job.rewrite.is_running() {
        job.rewrite.step();
    }
    for seat in job.seats.iter() {
        if let Some(seated) = hold(&seat.job).as_mut() {
            while seated.is_running() {
                seated.step();
            }
        }
    }
}

/// Give up every running rewrite of a job, the seats' as well
pub fn abandon_rewrites(job: &mut CompactJob<SimulatedIo>) {
    job.rewrite.abandon();
    for seat in job.seats.iter() {
        if let Some(seated) = hold(&seat.job).as_mut().filter(|seated| seated.is_running()) {
            seated.abandon();
        }
    }
}
