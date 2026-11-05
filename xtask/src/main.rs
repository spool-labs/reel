//! Runs the workspace's own build tasks

pub mod ci;
pub mod error;
pub mod summary;

use std::env;
use std::process::ExitCode;

use crate::ci::run_ci;
use crate::error::TaskError;

/// Subcommand that runs every check and every harness
pub const CI_COMMAND: &str = "ci";

fn main() -> ExitCode {
    let command = env::args().nth(1);
    let outcome = match command.as_deref() {
        Some(CI_COMMAND) => run_ci(),
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
