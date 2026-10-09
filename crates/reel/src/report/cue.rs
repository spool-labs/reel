//! Where a volume's sequence stands and what it is holding to get there

use crate::engine::ReelStore;
use crate::report::caveat::{self, Caveat};
use crate::report::doc::{Column, Doc, Row, Table, Tone};
use crate::report::fmt;
use crate::report::render::Report;

/// One segment's live and dead bytes
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct SegmentRow {
    /// Segment number, which is also the order it was written in
    pub segment: u32,

    /// Bytes a read can still reach
    pub live: u64,

    /// Bytes superseded or deleted, reclaimed by erasing the whole file
    pub dead: u64,

    /// Bytes a cue point is holding back from the dead figure
    pub held: u64,

    /// Dead bytes over the segment's total, which orders compaction
    pub dead_fraction: f64,
}

/// One column the volume was opened over
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ColumnRow {
    /// The column's name, as the volume was opened with it
    pub column: String,

    /// The identifier its records are stamped with
    pub id: u8,

    /// Sealed segments covering keys of the column
    pub sealed_segments: usize,
}

/// A cue point some part of this process is holding open
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct HeldCue {
    /// The cue's sequence number
    pub at: u64,

    /// How many holders are keeping it open
    pub holders: usize,
}

/// The volume's sequence, segment weights and held cue points
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct CueReport {
    /// The volume's root directory
    pub volume: String,

    /// The sequence number the volume has reached
    pub sequence: u64,

    /// The oldest sequence a read can still reach back to
    pub floor: Option<u64>,

    /// Segments the volume holds, counted before the listing is truncated
    pub total_segments: usize,

    /// Bytes a read can still reach, summed over the segments
    pub live_bytes: u64,

    /// Dead bytes across the volume, which compaction has to work through
    pub dead_bytes: u64,

    /// Dead bytes over live and dead together
    pub dead_share: f64,

    /// Range deletes still standing over the volume
    pub standing_covers: u64,

    /// Whether a cover still waits for the sweep that resolves it
    pub sweep_owed: bool,

    /// Tombstones the index holds
    pub graves: u64,

    /// Cue points held open in this process
    pub held: Vec<HeldCue>,

    /// Segments, highest dead fraction first, truncated to the limit asked for
    pub segments: Vec<SegmentRow>,

    /// The columns the volume was opened over
    pub columns: Vec<ColumnRow>,

    /// What the counts leave unaccounted for
    pub caveats: Vec<Caveat>,
}

/// Report where an open volume's sequence stands and what its segments weigh
pub fn cue(engine: &ReelStore, limit: usize) -> CueReport {
    let index = engine.index();

    // Totals come from the same snapshot as the rows, so they agree with the table
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
            sealed_segments: index.sealed_spans(spec.id),
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
        standing_covers: index.cover_count(),
        sweep_owed,
        graves: index.grave_count(),
        held,
        caveats: caveats(&columns, sweep_owed, live_bytes + dead_bytes > 0),
        segments,
        columns,
    }
}

fn caveats(columns: &[ColumnRow], sweep_owed: bool, holds_bytes: bool) -> Vec<Caveat> {
    let mut caveats = Vec::new();
    if columns.is_empty() {
        caveats.push(
            Caveat::new("no columns declared, so sealed spans and standing covers count nothing")
                .fix("pass --column NAME:ID for each column the volume was written with"),
        );
    }
    if sweep_owed {
        caveats.push(Caveat::new(
            "a cover is still owed its sweep, so the counters read as a floor",
        ));
    }
    if holds_bytes {
        caveats.push(caveat::seal_tally());
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
    /// A count that waits on a sweep is a floor, so it reads as a warning
    fn tone(&self) -> Tone {
        match self.sweep_owed {
            true => Tone::Warn,
            false => Tone::Plain,
        }
    }

    /// The headline: what compaction still has to work through
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
            // Cue points live in the process that took them, so another process sees none
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
                row.sealed_segments.to_string(),
            ]));
        }
        table
    }
}
