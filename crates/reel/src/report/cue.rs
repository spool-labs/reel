//! Where a volume's sequence stands and what it is holding to get there

use crate::engine::ReelStore;

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

    /// Dead bytes store wide, which is what compaction has to work through
    pub dead_bytes: u64,

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
}

/// Ask an open volume where its sequence stands and what its segments weigh
///
/// Fullest of dead first, since that is the order compaction picks in. The per
/// column figure is what a paged open leaves standing, so a resident open
/// answers nothing there rather than answering none.
pub fn cue(engine: &ReelStore, limit: usize) -> CueReport {
    let index = engine.index();
    let is_paged = engine.config().index.pages();

    let mut segments: Vec<SegmentRow> = Vec::new();
    for (segment, bytes) in index.segments_snapshot() {
        let total = bytes.live + bytes.dead;
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
    segments.truncate(limit);

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

    CueReport {
        volume: engine.root().display().to_string(),
        sequence: engine.sequence().as_u64(),
        floor: cues.floor().map(|at| at.as_u64()),
        total_segments,
        dead_bytes: engine.dead_bytes().to_bytes(),
        born_segments: engine.born_segments(),
        standing_covers: index.cover_count(),
        sweep_owed: index.has_pending_covers(),
        graves: index.grave_count(),
        held,
        segments,
        columns,
    }
}
