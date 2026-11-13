//! The store shape a planner-facing sim opens by default: a planner alone on each volume, or workers beside it when the run sets
//! the workers pass's variable, so one test body checks both shapes. The budget stays the test's own, since a larger one moves
//! the table's room and every count a test reads off it

use std::env;

use reel::config::{NO_WORKERS, ThreadCounts};

/// A run that sets this variable opens the planner-facing sims with that many workers on each volume
pub const WORKERS_VARIABLE: &str = "REEL_SIM_WORKERS";

/// Read the worker count the run asks for, none when the variable is unset
pub fn workers() -> usize {
    env::var(WORKERS_VARIABLE).ok().and_then(|count| count.parse().ok()).unwrap_or(0)
}

/// Read the thread counts a sim's default store opens with
pub fn threads() -> ThreadCounts {
    ThreadCounts { maintenance_workers: Some(workers()), ..NO_WORKERS }
}
