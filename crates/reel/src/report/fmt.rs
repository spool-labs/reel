//! How a figure reads once it is a cell
//!
//! One place for it, because a report that formats bytes two ways in one screen
//! reads as two reports. Everything here is the reader's units rather than the
//! engine's: a count of bytes is only a figure until it says which figure.

use std::path::Path;

/// Bytes in the largest unit that leaves a figure worth reading
pub fn bytes(count: u64) -> String {
    match count {
        count if count >= 1 << 30 => format!("{:.1} GiB", count as f64 / (1u64 << 30) as f64),
        count if count >= 1 << 20 => format!("{:.1} MiB", count as f64 / (1u64 << 20) as f64),
        count if count >= 1 << 10 => format!("{:.1} KiB", count as f64 / (1u64 << 10) as f64),
        count => format!("{count} B"),
    }
}

/// A fraction as the percentage a reader compares in
pub fn pct(fraction: f64) -> String {
    format!("{:.1}%", fraction * 100.0)
}

/// A count with the word for what is being counted, singular where it is one
pub fn plural(count: u64, one: &str, many: &str) -> String {
    match count {
        1 => format!("1 {one}"),
        count => format!("{count} {many}"),
    }
}

/// A figure this open could answer, or a dash where it could not
///
/// A cell the open cannot fill is not a zero, and printing one would be a wrong
/// answer where there is no answer.
pub fn answered<Figure>(value: Option<Figure>) -> String
where
    Figure: std::fmt::Display,
{
    match value {
        Some(value) => value.to_string(),
        None => "-".to_string(),
    }
}

/// A byte count the machine could not tell us
pub fn maybe_bytes(count: Option<u64>) -> String {
    match count {
        Some(count) => bytes(count),
        None => "unknown".to_string(),
    }
}

/// The name a volume goes by in a heading, which is its directory rather than
/// its whole path
///
/// The full path is a fact and stays in the facts; a heading that opens with
/// sixty characters of temporary directory buries the sentence it introduces.
pub fn volume_name(path: &str) -> String {
    match Path::new(path).file_name() {
        Some(name) if !name.is_empty() => name.to_string_lossy().to_string(),
        _ => path.to_string(),
    }
}
