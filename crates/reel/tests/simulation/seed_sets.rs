//! The seeds a harness runs: its fixed set, or a long sweep when the environment asks for one

use std::env;

/// How many seeds a sweep runs, and with this unset every harness runs its fixed set
const SWEEP_COUNT: &str = "REEL_SWEEP_SEEDS";
/// The first seed of a sweep, zero when unset, so each night can cover new ones
const SWEEP_FIRST: &str = "REEL_SWEEP_FIRST";

fn read_number(name: &str) -> Option<u64> {
    env::var(name).ok()?.parse().ok()
}

// the seeds to run, each printed as it starts, so a failure shows the seed that replays it
pub fn seeds(fixed: impl IntoIterator<Item = u64>) -> impl Iterator<Item = u64> {
    let picked: Vec<u64> = match read_number(SWEEP_COUNT) {
        Some(count) => {
            let first = read_number(SWEEP_FIRST).unwrap_or(0);
            (first..first + count).collect()
        },
        None => fixed.into_iter().collect(),
    };
    picked.into_iter().inspect(|seed| println!("seed {seed}"))
}
