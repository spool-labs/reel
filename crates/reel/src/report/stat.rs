//! What each column holds and what the segments weigh, live against dead

use crate::engine::ReelStore;
use crate::report::caveat::{self, Caveat};
use crate::report::doc::{Column, Doc, Row, Table, Tone};
use crate::report::fmt;
use crate::report::render::Report;

/// What one column holds, as far as this open can say
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct StatColumn {
    /// The column's name, as the volume was opened with it
    pub column: String,

    /// The identifier its records are stamped with
    pub id: u8,

    /// Sealed segments a search has to consider, absent on a resident open
    pub runs: Option<usize>,

    /// Live records in the column, absent on a paged open
    pub records: Option<u64>,

    /// Live bytes in the column, absent on a paged open
    pub bytes: Option<u64>,
}

/// The operator numbers for a volume
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct StatReport {
    /// The volume's root directory
    pub volume: String,

    /// The sequence number the volume has reached
    pub sequence: u64,

    /// Segments the volume holds
    pub segments: usize,

    /// Bytes a read can still reach, summed over the segments
    pub live_bytes: u64,

    /// Bytes waiting to be reclaimed, summed over the segments
    pub dead_bytes: u64,

    /// Bytes cue points are holding back from the dead figure
    pub tombstone_bytes: u64,

    /// Dead over live and dead together
    pub dead_share: f64,

    /// Cue points held open in this process
    pub held_cues: usize,

    /// Whether the open leaves sealed keys in their footers
    pub is_paged: bool,

    /// Sealed segments this open attributed no bytes to
    pub born_segments: usize,

    /// The columns the volume was opened over
    pub columns: Vec<StatColumn>,

    /// What stands between these figures and what a reader would take them for
    pub caveats: Vec<Caveat>,
}

/// Ask an open volume for the operator numbers
///
/// A paged index holds the tails' keys and no others, so its record and byte
/// counts would be a fraction of the column presented as the whole. Those cells
/// go unanswered there, and the sealed spans go unanswered on a resident open.
/// Neither is a zero.
pub fn stat(engine: &ReelStore) -> StatReport {
    let index = engine.index();
    let is_paged = engine.config().index.pages();
    let segments = index.segments_snapshot();

    let mut live = 0u64;
    let mut dead = 0u64;
    let mut held = 0u64;
    for (_, bytes) in &segments {
        live += bytes.live;
        dead += bytes.dead;
        held += bytes.held;
    }

    let mut columns = Vec::new();
    for spec in engine.columns() {
        let totals = index.column_totals(spec.id);
        columns.push(StatColumn {
            column: spec.name.to_string(),
            id: spec.id.as_u8(),
            runs: is_paged.then(|| index.sealed_spans(spec.id)),
            records: totals.map(|totals| totals.count),
            bytes: totals.map(|totals| totals.bytes.to_bytes()),
        });
    }

    let born_segments = engine.born_segments();
    StatReport {
        volume: engine.root().display().to_string(),
        sequence: engine.sequence().as_u64(),
        segments: segments.len(),
        live_bytes: live,
        dead_bytes: dead,
        tombstone_bytes: held,
        dead_share: match live + dead {
            0 => 0.0,
            total => dead as f64 / total as f64,
        },
        held_cues: engine.cue_points().held().len(),
        is_paged,
        born_segments,
        caveats: caveats(&columns, is_paged, born_segments),
        columns,
    }
}

/// What a reader has to know before taking any of these figures for a total
fn caveats(columns: &[StatColumn], is_paged: bool, born_segments: usize) -> Vec<Caveat> {
    let mut caveats = Vec::new();
    if columns.is_empty() {
        caveats.push(
            Caveat::new("no columns declared, so the per-column numbers count nothing")
                .fix("pass --column NAME:ID for each column the volume was written with"),
        );
    }
    // Every sealed segment is born under a paged open, and its bytes are in no
    // counter, so the live and dead figures are floors rather than the volume's
    // totals. Saying so is the difference between a floor and a wrong number.
    if born_segments > 0 {
        caveats.push(
            Caveat::new(format!(
                "{born_segments} sealed segments carry no attributed bytes, so the live, dead \
                 and share figures are floors"
            ))
            .fix("a resident open attributes them all: drop --paged"),
        );
    }
    if !columns.is_empty() {
        caveats.push(match is_paged {
            true => Caveat::new(
                "a paged open holds only the tails' keys, so record and byte counts go unanswered",
            )
            .fix("drop --paged"),
            false => Caveat::new("runs stand only over a paged open, so this one counts none")
                .fix("--paged"),
        });
    }
    caveats
}

impl Report for StatReport {
    fn doc(&self) -> Doc {
        let stored = self.live_bytes + self.dead_bytes;
        Doc::new()
            .head(fmt::volume_name(&self.volume))
            .head("stat")
            .head(format!("seq {}", self.sequence))
            .head(fmt::plural(self.segments as u64, "segment", "segments"))
            .head(match self.is_paged {
                true => "paged",
                false => "resident",
            })
            .verdict(
                match self.born_segments > 0 {
                    true => Tone::Warn,
                    false => Tone::Plain,
                },
                format!("{} live", fmt::bytes(self.live_bytes)),
                match stored {
                    0 => "nothing written here yet".to_string(),
                    _ => format!(
                        "{} dead of {} stored ({})",
                        fmt::bytes(self.dead_bytes),
                        fmt::bytes(stored),
                        fmt::pct(self.dead_share),
                    ),
                },
            )
            .facts([
                ("volume".to_string(), self.volume.clone()),
                (
                    "tombstone bytes".to_string(),
                    fmt::bytes(self.tombstone_bytes),
                ),
                ("held cue points".to_string(), self.held_cues.to_string()),
            ])
            .table(self.column_table())
            .notes("not counted", Tone::Warn, caveat::notes(&self.caveats))
            .footer([
                "segment weights: reel <volume> cue",
                "machine-readable: -o json",
            ])
    }
}

impl StatReport {
    fn column_table(&self) -> Table {
        let mut table = Table::new([
            Column::left("column"),
            Column::right("id"),
            Column::right("runs"),
            Column::right("records"),
            Column::right("bytes"),
        ]);
        for row in &self.columns {
            table = table.row(Row::new([
                row.column.clone(),
                row.id.to_string(),
                fmt::answered(row.runs),
                fmt::answered(row.records),
                fmt::answered(row.bytes.map(fmt::bytes)),
            ]));
        }
        table
    }
}
