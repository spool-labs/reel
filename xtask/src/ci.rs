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
pub const CLIPPY_ARGUMENTS: [&str; 7] = ["clippy", "--workspace", "--all-targets", "--locked", "--", "-D", "warnings"];
/// Cargo arguments for the test pass, which keeps going after a failure
pub const TEST_ARGUMENTS: [&str; 4] = ["test", "--workspace", "--locked", "--no-fail-fast"];

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

    let tests = run_cargo(&root, &TEST_ARGUMENTS)?;
    if !tests.is_success {
        failures.push("a test failed or a test binary stopped early".to_string());
    }
    check_lines(&tests.lines, &harnesses, &mut failures);

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
