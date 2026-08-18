//! Sealed segments standing over each column
//!
//! What a lookup narrows its search with, and what a read at an older sequence
//! number needs to find a version the map no longer holds.

use crate::engine::ReelStore;

/// One column's standing sealed segments
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct SpanRow {
    /// The column's name, as the volume was opened with it
    pub column: String,

    /// Sealed segments covering keys of the column
    pub sealed_segments: usize,
}

/// The sealed segments standing over each of a volume's columns
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct SpansReport {
    /// The volume's root directory
    pub volume: String,

    /// Whether the open leaves sealed keys in their footers
    pub is_paged: bool,

    /// The columns the volume was opened over
    pub columns: Vec<SpanRow>,
}

/// Ask an open volume what each column has sealed over it
///
/// A resident open resolves the sealed keys instead of leaving spans over them,
/// so every count comes back zero there.
pub fn spans(engine: &ReelStore) -> SpansReport {
    let index = engine.index();
    let mut columns = Vec::new();
    for spec in engine.columns() {
        columns.push(SpanRow {
            column: spec.name.to_string(),
            sealed_segments: index.sealed_spans(spec.id),
        });
    }

    SpansReport {
        volume: engine.root().display().to_string(),
        is_paged: engine.config().index.pages(),
        columns,
    }
}
