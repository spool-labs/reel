//! Shared formatting for report figures, so every report uses the same units

use std::path::Path;

/// Bytes in the largest binary unit that fits
pub fn bytes(count: u64) -> String {
    match count {
        count if count >= 1 << 30 => format!("{:.1} GiB", count as f64 / (1u64 << 30) as f64),
        count if count >= 1 << 20 => format!("{:.1} MiB", count as f64 / (1u64 << 20) as f64),
        count if count >= 1 << 10 => format!("{:.1} KiB", count as f64 / (1u64 << 10) as f64),
        count => format!("{count} B"),
    }
}

/// A fraction as a percentage with one decimal
pub fn pct(fraction: f64) -> String {
    format!("{:.1}%", fraction * 100.0)
}

/// A count with its singular or plural noun
pub fn plural(count: u64, one: &str, many: &str) -> String {
    match count {
        1 => format!("1 {one}"),
        count => format!("{count} {many}"),
    }
}

/// A byte count that may be unknown
pub fn maybe_bytes(count: Option<u64>) -> String {
    match count {
        Some(count) => bytes(count),
        None => "unknown".to_string(),
    }
}

/// A volume's directory name for a heading, falling back to the whole path
pub fn volume_name(path: &str) -> String {
    match Path::new(path).file_name() {
        Some(name) if !name.is_empty() => name.to_string_lossy().to_string(),
        _ => path.to_string(),
    }
}
