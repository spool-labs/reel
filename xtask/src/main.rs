//! Runs the workspace's own build tasks

pub mod ci;
pub mod error;
pub mod summary;
pub mod sweep;

use std::env;
use std::process::ExitCode;

use crate::ci::run_ci;
use crate::error::TaskError;
use crate::sweep::run_sweep;

/// Subcommand that runs every check and every harness
pub const CI_COMMAND: &str = "ci";
/// Subcommand that runs the simulation over a long run of seeds
pub const SWEEP_COMMAND: &str = "sweep";
/// Arguments before a subcommand's own: the program and the subcommand
pub const LEADING_ARGUMENTS: usize = 2;

fn main() -> ExitCode {
    let arguments: Vec<String> = env::args().collect();
    let command = arguments.get(1);
    let outcome = match command.map(String::as_str) {
        Some(CI_COMMAND) => run_ci(),
        Some(SWEEP_COMMAND) => run_sweep(arguments.get(LEADING_ARGUMENTS..).unwrap_or_default()),
        Some(other) => Err(TaskError::UnknownCommand {
            command: other.to_string(),
        }),
        None => Err(TaskError::MissingCommand),
    };

    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("xtask: {error}");
            ExitCode::FAILURE
        },
    }
}
