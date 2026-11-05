//! Errors a build task can end with

use std::fmt::{Display, Formatter, Result as FormatResult};
use std::io::Error as IoError;

/// Reason a build task failed
#[derive(Debug)]
pub enum TaskError {
    /// No subcommand was given
    MissingCommand,
    /// The subcommand is not one this tool runs
    UnknownCommand { command: String },
    /// A sweep was asked for with no seed count
    MissingSeedCount,
    /// An argument is not the number it has to be
    BadNumber { argument: String },
    /// The workspace root could not be found
    MissingRoot,
    /// A file or process could not be used
    Io { action: String, source: IoError },
    /// One or more checks failed
    Failed { failures: Vec<String> },
}

impl Display for TaskError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FormatResult {
        match self {
            TaskError::MissingCommand => write!(formatter, "no command given, try `ci`"),
            TaskError::UnknownCommand { command } => write!(formatter, "unknown command `{command}`"),
            TaskError::MissingSeedCount => write!(formatter, "sweep needs a seed count: `sweep <count> [first]`"),
            TaskError::BadNumber { argument } => write!(formatter, "`{argument}` is not a whole number"),
            TaskError::MissingRoot => write!(formatter, "workspace root not found"),
            TaskError::Io { action, source } => write!(formatter, "{action}: {source}"),
            TaskError::Failed { failures } => write_failures(formatter, failures),
        }
    }
}

/// Write one line per failed check
pub fn write_failures(formatter: &mut Formatter<'_>, failures: &[String]) -> FormatResult {
    write!(formatter, "{} failed", failures.len())?;
    for failure in failures {
        write!(formatter, "\n  {failure}")?;
    }
    Ok(())
}
