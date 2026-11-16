//! Lints and tests the workspace, and fails when any harness is cut short

use std::env;
use std::fs;
use std::io::{BufRead, BufReader, Error as IoError, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, Sender};
use std::thread::{self, JoinHandle};

use crate::error::TaskError;
use crate::summary::{normalize_target, parse_summary, parse_target};

/// File listing the test targets every change must run
pub const HARNESS_LIST: &str = "ci/harnesses";
/// Marks a comment line in the harness list
pub const COMMENT_PREFIX: char = '#';
/// Byte that ends a line of output
pub const LINE_END: u8 = b'\n';
/// Cargo arguments for the lint pass
pub const CLIPPY_ARGUMENTS: [&str; 9] = ["clippy", "--workspace", "--all-targets", "--locked", "--features", TEST_HOOKS, "--", "-D", "warnings"];
/// Cargo arguments for the lint pass on the build a node takes, with no test hooks. It names the published crates alone, since a
/// workspace build unifies features, and the sims crate turns the test hooks on for every member
pub const NODE_CLIPPY_ARGUMENTS: [&str; 10] = ["clippy", "-p", "tape-reel", "-p", "tape-reel-core", "--lib", "--locked", "--", "-D", "warnings"];
/// Cargo arguments for the outside caller's test, in a call of its own with no features, so the test hooks never reach it
pub const PUBLIC_API_ARGUMENTS: [&str; 4] = ["test", "--locked", "-p", "tape-reel-public-api"];
/// Cargo arguments for the test pass, which keeps going after a failure
pub const TEST_ARGUMENTS: [&str; 8] = ["test", "--workspace", "--locked", "--no-fail-fast", "--profile", CI_PROFILE, "--features", TEST_HOOKS];
/// Cargo arguments for the pick gate: every kind of pick at 100,000 segments a volume, timed in a release build against its line
pub const PICK_GATE_ARGUMENTS: [&str; 12] =
    ["test", "--release", "--locked", "-p", "tape-reel", "--features", TEST_HOOKS, "--test", "simulation", "picks::", "--", "--nocapture"];
/// The profile the CI's tests build in: optimized, with debug assertions and overflow checks
pub const CI_PROFILE: &str = "ci";
/// The feature that opens the crate's inner modules to the sims and benches, which no node build turns on
pub const TEST_HOOKS: &str = "tape-reel/test-hooks";
/// Cargo arguments for the workers pass: the planner-facing sims again, with a worker beside each planner as a node runs
pub const WORKERS_PASS_ARGUMENTS: [&str; 15] = [
    "test",
    "--locked",
    "--profile",
    CI_PROFILE,
    "-p",
    "tape-reel",
    "--features",
    TEST_HOOKS,
    "--test",
    "simulation",
    "--",
    "rewrite::",
    "compaction::",
    "dead_walk::",
    "reports::",
];
/// The sims read this variable for the worker count their default store opens with, in tests/simulation/shape.rs
pub const WORKERS_VARIABLE: &str = "REEL_SIM_WORKERS";
/// Workers the pass opens beside each planner: one fits every sim's budget and takes the seats' path
pub const PASS_WORKERS: &str = "1";
/// Tests the workers pass runs at least: the four modules held 211 when the pass began
pub const LEAST_PASS_TESTS: u64 = 200;

/// What one cargo run printed and how it ended
#[derive(Debug, Default)]
pub struct CargoRun {
    pub is_success: bool,
    pub lines: Vec<String>,
}

/// One environment variable a cargo run is given
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Variable {
    pub name: String,
    pub value: String,
}

/// Run every check and every harness, and report every failure at the end
pub fn run_ci() -> Result<(), TaskError> {
    let root = workspace_root()?;
    let harnesses = read_harnesses(&root)?;
    let mut failures: Vec<String> = Vec::new();

    let clippy = run_cargo(&root, &CLIPPY_ARGUMENTS)?;
    if !clippy.is_success {
        failures.push("clippy reported problems".to_string());
    }

    let node = run_cargo(&root, &NODE_CLIPPY_ARGUMENTS)?;
    if !node.is_success {
        failures.push("the build without test hooks has problems".to_string());
    }

    let public = run_cargo(&root, &PUBLIC_API_ARGUMENTS)?;
    if !public.is_success {
        failures.push("an outside caller can't build or run a store through the public surface".to_string());
    }

    // the workers pass runs the same binary beside the test pass, and waits on cargo's build lock until it's built
    let workers = [Variable { name: WORKERS_VARIABLE.to_string(), value: PASS_WORKERS.to_string() }];
    let (tests, pass) = thread::scope(|scope| {
        let pass = scope.spawn(|| run_cargo_with(&root, &WORKERS_PASS_ARGUMENTS, &workers));
        let tests = run_cargo(&root, &TEST_ARGUMENTS);
        (tests, pass.join())
    });
    let tests = tests?;
    let Ok(pass) = pass else {
        return Err(TaskError::Failed { failures: vec!["the workers pass stopped with a panic".to_string()] });
    };
    let pass = pass?;
    if !tests.is_success {
        failures.push("a test failed or a test binary stopped early".to_string());
    }
    check_lines(&tests.lines, &harnesses, &mut failures);

    if !pass.is_success {
        failures.push("a planner-facing sim failed with a worker beside the planner".to_string());
    }
    check_workers_pass(&pass.lines, &mut failures);

    let picks = run_cargo(&root, &PICK_GATE_ARGUMENTS)?;
    if !picks.is_success {
        failures.push("a pick held the summary past its line at 100,000 segments".to_string());
    }

    if failures.is_empty() {
        println!("ci passed");
        return Ok(());
    }
    Err(TaskError::Failed { failures })
}

/// Find the workspace root from this crate's own folder
pub fn workspace_root() -> Result<PathBuf, TaskError> {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    match manifest.parent() {
        Some(root) => Ok(root.to_path_buf()),
        None => Err(TaskError::MissingRoot),
    }
}

/// Read the names of the test targets every change must run
pub fn read_harnesses(root: &Path) -> Result<Vec<String>, TaskError> {
    let path = root.join(HARNESS_LIST);
    let text = fs::read_to_string(&path).map_err(|source| TaskError::Io {
        action: format!("reading {HARNESS_LIST}"),
        source,
    })?;

    let mut harnesses = Vec::new();
    for line in text.lines() {
        let name = line.trim();
        if name.is_empty() || name.starts_with(COMMENT_PREFIX) {
            continue;
        }
        harnesses.push(normalize_target(name));
    }
    Ok(harnesses)
}

/// Run cargo in the workspace, echo its output and keep every line
pub fn run_cargo(root: &Path, arguments: &[&str]) -> Result<CargoRun, TaskError> {
    run_cargo_with(root, arguments, &[])
}

/// Run cargo with extra environment variables, echo its output and keep every line
pub fn run_cargo_with(root: &Path, arguments: &[&str], variables: &[Variable]) -> Result<CargoRun, TaskError> {
    let cargo = env::var("CARGO").unwrap_or_else(|_missing| "cargo".to_string());
    println!("cargo {}", arguments.join(" "));

    let mut command = Command::new(cargo);
    for variable in variables {
        command.env(&variable.name, &variable.value);
    }
    let mut child = command
        .args(arguments)
        .current_dir(root)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|source| cargo_error(arguments, source))?;

    let (tx, rx) = mpsc::channel();
    let mut readers: Vec<JoinHandle<()>> = Vec::new();
    if let Some(pipe) = child.stdout.take() {
        let lines = tx.clone();
        readers.push(thread::spawn(move || copy_lines(pipe, &lines)));
    }
    if let Some(pipe) = child.stderr.take() {
        let lines = tx.clone();
        readers.push(thread::spawn(move || copy_lines(pipe, &lines)));
    }
    drop(tx);

    let lines: Vec<String> = rx.iter().collect();
    for reader in readers {
        // a reader ends when its pipe closes, and cargo's exit status carries the outcome
        let _joined = reader.join();
    }
    let status = child.wait().map_err(|source| cargo_error(arguments, source))?;

    Ok(CargoRun {
        is_success: status.success(),
        lines,
    })
}

/// Build the error for a cargo process that could not run
pub fn cargo_error(arguments: &[&str], source: IoError) -> TaskError {
    TaskError::Io {
        action: format!("running cargo {}", arguments.join(" ")),
        source,
    }
}

/// Echo each line of a pipe and send it on until the pipe closes
pub fn copy_lines<Pipe: Read>(pipe: Pipe, tx: &Sender<String>) {
    let mut reader = BufReader::new(pipe);
    let mut bytes: Vec<u8> = Vec::new();
    loop {
        bytes.clear();
        match reader.read_until(LINE_END, &mut bytes) {
            Ok(0) => return,
            Ok(_length) => {},
            Err(_closed) => return,
        }
        let line = String::from_utf8_lossy(&bytes).trim_end().to_string();
        println!("{line}");
        // the receiver drains until every reader is done, so a send can only fail after the run is over
        let _sent = tx.send(line);
    }
}

/// Add a failure for every skipped test and every harness that did not run
pub fn check_lines(lines: &[String], harnesses: &[String], failures: &mut Vec<String>) {
    let mut ignored = 0;
    let mut filtered = 0;
    let mut started: Vec<String> = Vec::new();
    for line in lines {
        if let Some(summary) = parse_summary(line) {
            ignored += summary.ignored.0;
            filtered += summary.filtered.0;
        }
        if let Some(target) = parse_target(line) {
            started.push(target);
        }
    }

    if ignored > 0 {
        failures.push(format!("{ignored} tests ignored"));
    }
    if filtered > 0 {
        failures.push(format!("{filtered} tests filtered out"));
    }
    for harness in harnesses {
        if !started.contains(harness) {
            failures.push(format!("harness `{harness}` did not run"));
        }
    }
}

/// Check the workers pass ran its tests: a filter that matches nothing after a rename passes with no test run
pub fn check_workers_pass(lines: &[String], failures: &mut Vec<String>) {
    let passed: u64 = lines.iter().filter_map(|line| parse_summary(line)).map(|summary| summary.passed.0).sum();
    if passed < LEAST_PASS_TESTS {
        failures.push(format!("the workers pass ran {passed} tests, under its floor of {LEAST_PASS_TESTS}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // a run with every harness started and nothing skipped adds no failure
    #[test]
    fn clean_run() {
        let lines = vec![
            "     Running tests/simulation.rs (target/debug/deps/simulation-0f3a9c21d4e5b677)".to_string(),
            "test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s".to_string(),
        ];
        let harnesses = vec!["simulation".to_string()];
        let mut failures = Vec::new();

        check_lines(&lines, &harnesses, &mut failures);

        assert!(failures.is_empty());
    }

    // a workers pass whose filter matched nothing passes with no test run, and the floor fails it
    #[test]
    fn empty_workers_pass() {
        let lines = vec!["test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 790 filtered out; finished in 0.00s".to_string()];
        let mut failures = Vec::new();

        check_workers_pass(&lines, &mut failures);

        assert_eq!(failures.len(), 1);
    }

    // a workers pass at its full count adds no failure
    #[test]
    fn full_workers_pass() {
        let lines = vec!["test result: ok. 215 passed; 0 failed; 0 ignored; 0 measured; 579 filtered out; finished in 230.00s".to_string()];
        let mut failures = Vec::new();

        check_workers_pass(&lines, &mut failures);

        assert!(failures.is_empty());
    }

    // a listed harness that cargo never started is a failure
    #[test]
    fn missing_harness() {
        let harnesses = vec!["simulation".to_string()];
        let mut failures = Vec::new();

        check_lines(&[], &harnesses, &mut failures);

        assert_eq!(failures.len(), 1);
    }

    // an ignored test is a failure
    #[test]
    fn ignored_test() {
        let lines =
            vec!["test result: ok. 4 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.01s".to_string()];
        let mut failures = Vec::new();

        check_lines(&lines, &[], &mut failures);

        assert_eq!(failures.len(), 1);
    }
}
