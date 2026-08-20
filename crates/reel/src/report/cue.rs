//! Where a volume's sequence stands and what it is holding to get there

use crate::engine::ReelStore;
use crate::report::caveat::{self, Caveat};
use crate::report::doc::{Column, Doc, Row, Table, Tone};
use crate::report::fmt;
use crate::report::render::Report;

/// One segment's weight, live against what is waiting to be reclaimed
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct SegmentRow {
    /// Segment number, which is also the order it was written in
    pub segment: u32,

    /// Bytes of it a read can still reach
    pub live: u64,

    /// Bytes superseded or deleted, reclaimed by erasing the whole file
    pub dead: u64,

    /// Bytes a cue point is holding back from the dead figure
    pub held: u64,

    /// Dead over the segment's whole weight, the order compaction picks in
    pub dead_fraction: f64,
}

/// What one column has standing over it, as far as this open can say
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ColumnRow {
    /// The column's name, as the volume was opened with it
    pub column: String,

    /// The identifier its records are stamped with
    pub id: u8,

    /// Sealed segments covering keys of the column, absent on a resident open
    pub sealed_segments: Option<usize>,
}

/// A cue point some part of this process is holding open
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct HeldCue {
    /// The sequence number the cue stands at
    pub at: u64,

    /// How many holders are keeping it open
    pub holders: usize,
}

/// Where the volume stands and what it is holding to get there
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CueReport {
    /// The volume's root directory
    pub volume: String,

    /// The sequence number the volume has reached
    pub sequence: u64,

    /// The oldest sequence a read can still reach back to
    pub floor: Option<u64>,

    /// Segments the volume holds, before the listing is truncated
    pub total_segments: usize,

    /// Bytes a read can still reach, summed over the segments
    pub live_bytes: u64,

    /// Dead bytes store wide, which is what compaction has to work through
    pub dead_bytes: u64,

    /// Dead over live and dead together, the order compaction picks in
    pub dead_share: f64,

    /// Whether the open leaves sealed keys in their footers
    pub is_paged: bool,

    /// Sealed segments a rebuild left uncounted, so the totals read as a floor
    pub born_segments: usize,

    /// Range deletes still standing over the volume
    pub standing_covers: u64,

    /// Whether a cover is still owed the sweep that resolves it
    pub sweep_owed: bool,

    /// Tombstones the index is carrying
    pub graves: u64,

    /// Cue points held open in this process
    pub held: Vec<HeldCue>,

    /// Segments, fullest of dead first, truncated to the limit asked for
    pub segments: Vec<SegmentRow>,

    /// The columns the volume was opened over
    pub columns: Vec<ColumnRow>,

    /// What stands between these figures and what a reader would take them for
    pub caveats: Vec<Caveat>,
}

/// Ask an open volume where its sequence stands and what its segments weigh
///
/// Fullest of dead first, since that is the order compaction picks in. The per
/// column figure is what a paged open leaves standing, so a resident open
/// answers nothing there rather than answering none. A limit of zero asks for
/// the whole listing, since nobody runs a report to be shown no rows.
pub fn cue(engine: &ReelStore, limit: usize) -> CueReport {
    let index = engine.index();
    let is_paged = engine.config().index.pages();

    // Both totals are summed here, out of the one snapshot, and neither is asked
    // of the engine again: a second walk of the same rows is a second instant,
    // and every verb here is built to read a volume something else is writing.
    // Two instants would let the share and the head disagree with the table
    // beneath them. Summed as the rows are built, so the listing can be sorted
    // and cut afterwards without the totals following it down.
    let mut live_bytes = 0u64;
    let mut dead_bytes = 0u64;
    let mut segments: Vec<SegmentRow> = Vec::new();
    for (segment, bytes) in index.segments_snapshot() {
        let total = bytes.live + bytes.dead;
        live_bytes += bytes.live;
        dead_bytes += bytes.dead;
        segments.push(SegmentRow {
            segment: segment.as_u32(),
            live: bytes.live,
            dead: bytes.dead,
            held: bytes.held,
            dead_fraction: match total {
                0 => 0.0,
                total => bytes.dead as f64 / total as f64,
            },
        });
    }
    let total_segments = segments.len();
    segments.sort_by(|a, b| b.dead_fraction.total_cmp(&a.dead_fraction));
    if limit > 0 {
        segments.truncate(limit);
    }

    let mut columns = Vec::new();
    for spec in engine.columns() {
        columns.push(ColumnRow {
            column: spec.name.to_string(),
            id: spec.id.as_u8(),
            sealed_segments: is_paged.then(|| index.sealed_spans(spec.id)),
        });
    }

    let cues = engine.cue_points();
    let mut held = Vec::new();
    for (at, holders) in cues.held() {
        held.push(HeldCue {
            at: at.as_u64(),
            holders,
        });
    }

    let born_segments = engine.born_segments();
    let sweep_owed = index.has_pending_covers();

    CueReport {
        volume: engine.root().display().to_string(),
        sequence: engine.sequence().as_u64(),
        floor: cues.floor().map(|at| at.as_u64()),
        total_segments,
        live_bytes,
        dead_bytes,
        dead_share: match live_bytes + dead_bytes {
            0 => 0.0,
            total => dead_bytes as f64 / total as f64,
        },
        is_paged,
        born_segments,
        standing_covers: index.cover_count(),
        sweep_owed,
        graves: index.grave_count(),
        held,
        caveats: caveats(&columns, is_paged, sweep_owed, born_segments),
        segments,
        columns,
    }
}

/// What a reader has to know before taking any of these figures for a total
fn caveats(
    columns: &[ColumnRow],
    is_paged: bool,
    sweep_owed: bool,
    born_segments: usize,
) -> Vec<Caveat> {
    let mut caveats = Vec::new();
    match columns.is_empty() {
        true => caveats.push(
            Caveat::new("no columns declared, so sealed spans and standing covers count nothing")
                .fix("pass --column NAME:ID for each column the volume was written with"),
        ),
        false if !is_paged => caveats.push(
            Caveat::new("sealed spans stand only over a paged open, so this one counts none")
                .fix("--paged"),
        ),
        false => {}
    }
    if sweep_owed {
        caveats.push(Caveat::new(
            "a cover is still owed its sweep, so the counters read as a floor",
        ));
    }
    if born_segments > 0 {
        caveats.push(Caveat::new(format!(
            "{born_segments} sealed segments a rebuild left uncounted, so the totals are a floor",
        )));
    }
    caveats
}

/// A segment is worth compacting once dead outweighs live in it
const CROWDED: f64 = 0.5;

impl Report for CueReport {
    fn doc(&self) -> Doc {
        Doc::new()
            .head(fmt::volume_name(&self.volume))
            .head(format!("seq {}", self.sequence))
            .head(fmt::plural(
                self.total_segments as u64,
                "segment",
                "segments",
            ))
            .head(fmt::bytes(self.live_bytes + self.dead_bytes))
            .head(match self.is_paged {
                true => "paged",
                false => "resident",
            })
            .head("read-only")
            .verdict(self.tone(), self.headline(), self.detail())
            .facts(self.facts())
            .table(self.segment_table())
            .table(self.column_table())
            .notes("not counted", Tone::Warn, caveat::notes(&self.caveats))
            .footer([
                "per-column figures: reel <volume> --column NAME:ID stat",
                "machine-readable: -o json",
            ])
    }
}

impl CueReport {
    /// A figure standing on a floor is not the figure, and reads as a caveat
    fn tone(&self) -> Tone {
        match self.sweep_owed || self.born_segments > 0 {
            true => Tone::Warn,
            false => Tone::Plain,
        }
    }

    /// The one line a reader came for: what compaction still has to work through
    fn headline(&self) -> String {
        match self.total_segments {
            0 => "empty".to_string(),
            _ => format!("{} dead", fmt::pct(self.dead_share)),
        }
    }

    fn detail(&self) -> String {
        match self.total_segments {
            0 => "nothing written here yet".to_string(),
            _ => format!(
                "{} of {} waiting on compaction",
                fmt::bytes(self.dead_bytes),
                fmt::bytes(self.live_bytes + self.dead_bytes),
            ),
        }
    }

    fn facts(&self) -> Vec<(String, String)> {
        vec![
            ("volume".to_string(), self.volume.clone()),
            (
                "reaches back to".to_string(),
                match self.floor {
                    Some(at) => format!("sequence {at}"),
                    None => "nothing older than the sequence above".to_string(),
                },
            ),
            (
                "standing covers".to_string(),
                match self.sweep_owed {
                    true => format!("{}, one still owed its sweep", self.standing_covers),
                    false => self.standing_covers.to_string(),
                },
            ),
            ("graves".to_string(), self.graves.to_string()),
            // Cue points live in the process that took them, so a tool looking
            // in from outside sees none even while a writer holds several.
            (
                "cue points".to_string(),
                match self.held.is_empty() {
                    true => "none held in this process".to_string(),
                    false => self
                        .held
                        .iter()
                        .map(|row| format!("{} held by {}", row.at, row.holders))
                        .collect::<Vec<String>>()
                        .join(", "),
                },
            ),
        ]
    }

    fn segment_table(&self) -> Table {
        let mut table = Table::new([
            Column::left("segment"),
            Column::right("live"),
            Column::right("dead"),
            Column::right("held"),
            Column::right("dead%"),
        ]);
        for (at, row) in self.segments.iter().enumerate() {
            let cells = Row::new([
                row.segment.to_string(),
                fmt::bytes(row.live),
                fmt::bytes(row.dead),
                fmt::bytes(row.held),
                fmt::pct(row.dead_fraction),
            ]);
            table = table.row(match row.dead_fraction >= CROWDED {
                true => cells.note(match at {
                    0 => "compaction's next pick",
                    _ => "over half dead",
                }),
                false => cells,
            });
        }
        table.caption(match self.segments.len() == self.total_segments {
            true => format!(
                "all {}",
                fmt::plural(self.total_segments as u64, "segment", "segments")
            ),
            false => format!(
                "{} of {} segments, fullest of dead first — --limit 0 for all",
                self.segments.len(),
                self.total_segments,
            ),
        })
    }

    fn column_table(&self) -> Table {
        let mut table = Table::new([
            Column::left("column"),
            Column::right("id"),
            Column::right("sealed segments"),
        ]);
        for row in &self.columns {
            table = table.row(Row::new([
                row.column.clone(),
                row.id.to_string(),
                fmt::answered(row.sealed_segments),
            ]));
        }
        table
    }
}
