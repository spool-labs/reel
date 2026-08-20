//! Sealed segments standing over each column
//!
//! What a lookup narrows its search with, and what a read at an older sequence
//! number needs to find a version the map no longer holds.

use crate::engine::ReelStore;
use crate::report::caveat::{self, Caveat};
use crate::report::doc::{Column, Doc, Row, Table, Tone};
use crate::report::fmt;
use crate::report::render::Report;

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

    /// What stands between these figures and what a reader would take them for
    pub caveats: Vec<Caveat>,
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

    let is_paged = engine.config().index.pages();
    let mut caveats = Vec::new();
    if columns.is_empty() {
        caveats.push(
            Caveat::new("no columns declared, so no spans are counted")
                .fix("pass --column NAME:ID for each column the volume was written with"),
        );
    }
    if !is_paged {
        caveats.push(
            Caveat::new("sealed spans stand only over a paged open, so this one counts none")
                .fix("--paged"),
        );
    }

    SpansReport {
        volume: engine.root().display().to_string(),
        is_paged,
        columns,
        caveats,
    }
}

impl Report for SpansReport {
    fn doc(&self) -> Doc {
        let widest = self.columns.iter().map(|row| row.sealed_segments).max();
        let total: usize = self.columns.iter().map(|row| row.sealed_segments).sum();

        let mut table = Table::new([Column::left("column"), Column::right("sealed segments")]);
        for row in &self.columns {
            let cells = Row::new([row.column.clone(), row.sealed_segments.to_string()]);
            // The deepest column is the one a lookup pays the most for, so it is
            // the row worth pointing at.
            table = table.row(match Some(row.sealed_segments) == widest && total > 0 {
                true => cells.note("deepest search"),
                false => cells,
            });
        }

        Doc::new()
            .head(fmt::volume_name(&self.volume))
            .head("spans")
            .head(match self.is_paged {
                true => "paged",
                false => "resident",
            })
            .verdict(
                match self.caveats.is_empty() {
                    true => Tone::Plain,
                    false => Tone::Warn,
                },
                match widest {
                    Some(_) => fmt::plural(total as u64, "sealed segment", "sealed segments"),
                    None => "nothing counted".to_string(),
                },
                match widest {
                    // The deepest column is what a lookup pays for, so it is
                    // the figure the verdict carries rather than the total.
                    Some(widest) => format!(
                        "standing over {}, deepest {widest}",
                        fmt::plural(self.columns.len() as u64, "column", "columns"),
                    ),
                    None => "no columns were declared to count them over".to_string(),
                },
            )
            .facts([("volume".to_string(), self.volume.clone())])
            .table(table)
            .notes("not counted", Tone::Warn, caveat::notes(&self.caveats))
            .footer(["machine-readable: -o json"])
    }
}
