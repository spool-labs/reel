//! Runs the simulation harness over a long run of seeds, with every check a debug build has

use crate::ci::{Variable, check_lines, run_cargo_with, workspace_root};
use crate::error::TaskError;
use crate::summary::normalize_target;

/// The harness a sweep runs
pub const SWEEP_HARNESS: &str = "simulation";
/// Tells the harness how many seeds to run
pub const SEED_COUNT_VARIABLE: &str = "REEL_SWEEP_SEEDS";
/// Tells the harness which seed to start at
pub const FIRST_SEED_VARIABLE: &str = "REEL_SWEEP_FIRST";
/// Keeps debug assertions in the optimized build a sweep runs
pub const ASSERTIONS_VARIABLE: &str = "CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS";
/// Keeps overflow checks in the optimized build a sweep runs
pub const OVERFLOW_VARIABLE: &str = "CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS";
/// Cargo arguments for a sweep, which keeps going after a failure
pub const SWEEP_ARGUMENTS: [&str; 10] = [
    "test",
    "--package",
    "tape-reel",
    "--test",
    SWEEP_HARNESS,
    "--locked",
    "--release",
    "--no-fail-fast",
    "--features",
    "test-hooks",
];

/// A count of seeds
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SeedCount(pub u64);

/// One seed of a simulation
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SeedNumber(pub u64);

/// The seeds one sweep covers
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SweepRange {
    pub count: SeedCount,
    pub first: SeedNumber,
}

/// Run the harness over a range of seeds, and fail when any seed fails or the harness is cut short
pub fn run_sweep(arguments: &[String]) -> Result<(), TaskError> {
    let range = parse_range(arguments)?;
    let root = workspace_root()?;
    println!("sweep of {} seeds from {}", range.count.0, range.first.0);

    let run = run_cargo_with(&root, &SWEEP_ARGUMENTS, &sweep_variables(&range))?;
    let mut failures: Vec<String> = Vec::new();
    if !run.is_success {
        failures.push("a seed failed, and the last `seed` line a failed test printed is the one to replay".to_string());
    }
    check_lines(&run.lines, &[normalize_target(SWEEP_HARNESS)], &mut failures);

    if failures.is_empty() {
        println!("sweep passed");
        return Ok(());
    }
    Err(TaskError::Failed { failures })
}

/// Read a sweep's seed count and its first seed, which is zero when left out
pub fn parse_range(arguments: &[String]) -> Result<SweepRange, TaskError> {
    let Some(count) = arguments.first() else {
        return Err(TaskError::MissingSeedCount);
    };
    let first = match arguments.get(1) {
        Some(first) => parse_number(first)?,
        None => 0,
    };
    Ok(SweepRange {
        count: SeedCount(parse_number(count)?),
        first: SeedNumber(first),
    })
}

/// Read one whole number from an argument
pub fn parse_number(argument: &str) -> Result<u64, TaskError> {
    let parsed: Result<u64, _> = argument.parse();
    parsed.map_err(|_unreadable| TaskError::BadNumber {
        argument: argument.to_string(),
    })
}

/// List what a sweep sets in the harness's environment
pub fn sweep_variables(range: &SweepRange) -> [Variable; 4] {
    [
        variable(SEED_COUNT_VARIABLE, range.count.0.to_string()),
        variable(FIRST_SEED_VARIABLE, range.first.0.to_string()),
        variable(ASSERTIONS_VARIABLE, true.to_string()),
        variable(OVERFLOW_VARIABLE, true.to_string()),
    ]
}

/// Build one environment variable
pub fn variable(name: &str, value: String) -> Variable {
    Variable {
        name: name.to_string(),
        value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(words: &[&str]) -> Vec<String> {
        let mut arguments = Vec::new();
        for word in words {
            arguments.push(word.to_string());
        }
        arguments
    }

    // a count alone starts the sweep at seed zero
    #[test]
    fn count_only() {
        let range = parse_range(&arguments(&["64"])).expect("range");

        assert_eq!(range.count, SeedCount(64));
        assert_eq!(range.first, SeedNumber(0));
    }

    // a second number moves the first seed
    #[test]
    fn first_seed() {
        let range = parse_range(&arguments(&["64", "1280"])).expect("range");

        assert_eq!(range.first, SeedNumber(1_280));
    }

    // no count and a word in place of a number are both refused
    #[test]
    fn bad_arguments() {
        assert!(parse_range(&arguments(&[])).is_err());
        assert!(parse_range(&arguments(&["many"])).is_err());
        assert!(parse_range(&arguments(&["64", "soon"])).is_err());
    }

    // the harness gets its count and first seed by the names it reads
    #[test]
    fn harness_variables() {
        let range = SweepRange {
            count: SeedCount(8),
            first: SeedNumber(40),
        };

        let variables = sweep_variables(&range);

        assert_eq!(variables[0], variable(SEED_COUNT_VARIABLE, "8".to_string()));
        assert_eq!(variables[1], variable(FIRST_SEED_VARIABLE, "40".to_string()));
    }
}
