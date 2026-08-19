//! What each column holds and what the segments weigh, live against dead

use crate::engine::ReelStore;

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

    /// Sealed segments this open attributed no bytes to
    pub born_segments: usize,

    /// The columns the volume was opened over
    pub columns: Vec<StatColumn>,
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
        born_segments: engine.born_segments(),
        columns,
    }
}
