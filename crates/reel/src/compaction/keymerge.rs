//! Key-run merges: the walk's runs collapsed into one key run, no record moved
//!
//! A merge reads the rows of the runs it takes, a data segment's footer or an earlier
//! key run, and writes the newest row of each key with the place its record lies. The
//! records stay where they were written, so neither FastForward nor the map is touched,
//! and nothing goes down but keys and places.

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::compaction::compactor::{Compactor, PassClaim};
use crate::error::{ReelError, Result};
use crate::format::column::ColumnId;
use crate::format::footer::{FooterPartition, SegmentFooter, VARYING_WIDTH};
use crate::format::loc::{Loc, SegmentId};
use crate::index::keyrun::{row_in, KeyRun, RunColumn, RunRow, RunWriter};
use crate::index::map::ReelIndex;
use crate::reel::Reel;

/// What one key-run merge did
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct KeyMergeReport {
    /// Runs the merge read: data segments and key runs together
    pub runs_merged: u64,

    /// Data segments the new run covers that no run did before
    pub segments_covered: u64,

    /// Rows the new run holds
    pub rows_written: u64,

    /// Rows passed over for a newer row of the same key
    pub rows_shadowed: u64,
}

/// One run a merge reads
enum Source {
    /// A data segment's own footer
    Footer {
        segment: SegmentId,
        footer: Arc<SegmentFooter>,
    },

    /// An earlier key run
    Keys(Arc<KeyRun>),
}

/// Where a merge stands in one source's rows for the column being merged
enum Cursor<'a> {
    Footer {
        segment: SegmentId,
        rows: &'a FooterPartition,
        at: usize,
    },
    Keys {
        run: &'a KeyRun,
        column: &'a RunColumn,
        at: u64,
        block: Option<u32>,
        first: u64,
        buf: Vec<u8>,
    },
}

impl Cursor<'_> {
    /// The key the cursor stands on, nothing once it is past its rows
    fn key(&self) -> Option<&[u8]> {
        match self {
            Cursor::Footer { rows, at, .. } => rows.key_at(*at),
            Cursor::Keys { column, at, first, buf, .. } => {
                (*at < column.rows()).then(|| crate::index::keyrun::key_in(buf, column, (*at - *first) as usize))
            }
        }
    }

    /// Read the block the cursor stands in, when it is not the one held
    fn reach(&mut self) -> Result<()> {
        if let Cursor::Keys { run, column, at, block, first, buf } = self {
            if *at >= column.rows() {
                return Ok(());
            }
            let wanted = (*at / u64::from(column.block_rows())) as u32;
            if *block != Some(wanted) {
                *buf = run.read_block(column, wanted, std::mem::take(buf))?;
                *block = Some(wanted);
                *first = column.block_span(wanted).0;
            }
        }
        Ok(())
    }

    /// The row the cursor stands on, with the place its record lies
    fn row(&self) -> Result<RunRow> {
        match self {
            Cursor::Footer { segment, rows, at } => {
                let row = rows.row_at(*at)?;
                Ok(RunRow {
                    lsn: row.lsn,
                    loc: Loc::new(*segment, row.offset, row.len),
                    flags: row.flags,
                })
            }
            Cursor::Keys { column, at, first, buf, .. } => Ok(row_in(buf, column, (*at - *first) as usize)?.1),
        }
    }

    /// Step to the next row
    fn advance(&mut self) -> Result<()> {
        match self {
            Cursor::Footer { at, .. } => *at += 1,
            Cursor::Keys { at, .. } => *at += 1,
        }
        self.reach()
    }
}

/// Merge segments and key runs into one key run, which then answers for every segment they covered
///
/// Each segment is claimed for the length of the pass, so no rewrite moves its records
/// while the merge is writing down where they lie. A column whose keys vary in width is
/// one key runs do not take, and a merge meeting one gives up and leaves its runs as
/// they were.
pub fn merge_into_key_run(
    compactor: &Compactor,
    reel: &Reel,
    index: &ReelIndex,
    segments: &[SegmentId],
    runs: &[Arc<KeyRun>],
) -> Result<KeyMergeReport> {
    let shared = reel.shared();
    let mut claims: Vec<PassClaim<'_>> = Vec::new();
    let mut sources = Vec::new();
    for segment in segments {
        let Some(claim) = compactor.claim(*segment) else {
            continue;
        };
        // Chosen from a list taken before the claim, so a rewrite can have retired it
        // in between, and a run covering a segment that is gone names records nowhere.
        if !index.holds_sealed(*segment) {
            continue;
        }
        let Some(footer) = shared.footer_of(*segment)? else {
            continue;
        };
        claims.push(claim);
        sources.push(Source::Footer {
            segment: *segment,
            footer,
        });
    }
    sources.extend(runs.iter().map(|run| Source::Keys(Arc::clone(run))));
    if sources.len() < 2 {
        return Ok(KeyMergeReport::default());
    }

    let mut columns: BTreeSet<(ColumnId, u16)> = BTreeSet::new();
    for source in &sources {
        match source {
            Source::Footer { footer, .. } => {
                columns.extend(footer.partitions.iter().filter(|rows| !rows.is_empty()).map(|rows| (rows.column, rows.key_width)))
            }
            Source::Keys(run) => columns.extend(run.columns().iter().map(|column| (column.column, column.key_width))),
        }
    }
    if columns.iter().any(|(_, width)| *width == VARYING_WIDTH) {
        return Err(ReelError::Rejected("key runs take fixed-width columns alone".to_string()));
    }
    let mut widths = columns.iter().map(|(column, _)| *column).collect::<Vec<_>>();
    widths.dedup();
    if widths.len() != columns.len() {
        return Err(ReelError::Rejected("a column comes in two key widths across the runs".to_string()));
    }

    let root = shared.volumes.roots()[0].clone();
    let id = index.key_runs().draw_id();
    let mut writer = RunWriter::create(&shared.driver, &root, id)?;
    let mut report = KeyMergeReport {
        runs_merged: sources.len() as u64,
        ..KeyMergeReport::default()
    };
    let written = (|| -> Result<()> {
        for (column, width) in &columns {
            writer.begin_column(*column, *width)?;
            merge_column(&sources, *column, &mut writer, &mut report)?;
        }
        Ok(())
    })();
    if let Err(error) = written {
        writer.abandon();
        return Err(error);
    }

    // Only what was merged: a segment a claim turned away keeps its footer in the walk,
    // and covering it would hide rows no run holds.
    let mut covered: BTreeSet<SegmentId> = sources
        .iter()
        .filter_map(|source| match source {
            Source::Footer { segment, .. } => Some(*segment),
            Source::Keys(_) => None,
        })
        .collect();
    for run in runs {
        covered.extend(run.covered.iter().copied());
    }
    let covered: Vec<SegmentId> = covered.into_iter().collect();
    let before = index.key_runs().covered();
    report.segments_covered = covered.iter().filter(|segment| !before.contains(segment)).count() as u64;
    report.rows_written = writer.rows();
    let path = writer.finish(&covered)?;
    let run = Arc::new(KeyRun::open(&shared.driver, &path, id)?);
    let merged: Vec<u64> = runs.iter().map(|run| run.id).collect();
    for retired in index.key_runs().install(run, &merged) {
        retired.retire();
    }
    drop(claims);
    Ok(report)
}

/// Write one column's newest row of each key across every source, in key order
fn merge_column(
    sources: &[Source],
    column: ColumnId,
    writer: &mut RunWriter<'_>,
    report: &mut KeyMergeReport,
) -> Result<()> {
    let mut cursors: Vec<Cursor<'_>> = Vec::with_capacity(sources.len());
    for source in sources {
        match source {
            Source::Footer { segment, footer } => {
                if let Some(rows) = footer.partition(column) {
                    cursors.push(Cursor::Footer {
                        segment: *segment,
                        rows,
                        at: 0,
                    });
                }
            }
            Source::Keys(run) => {
                if let Some(held) = run.column(column) {
                    let mut cursor = Cursor::Keys {
                        run,
                        column: held,
                        at: 0,
                        block: None,
                        first: 0,
                        buf: Vec::new(),
                    };
                    cursor.reach()?;
                    cursors.push(cursor);
                }
            }
        }
    }
    // A heap of the cursors still standing on rows, least key on top, so a row costs a
    // few comparisons however many runs the merge reads.
    let mut heap: Vec<usize> = (0..cursors.len()).filter(|at| cursors[*at].key().is_some()).collect();
    for at in (0..heap.len() / 2).rev() {
        sift_down(&mut heap, &cursors, at);
    }
    let mut key = Vec::new();
    while let Some(&top) = heap.first() {
        key.clear();
        key.extend_from_slice(cursors[top].key().unwrap_or_default());
        // Every row of the key goes, and the one with the highest sequence number stays.
        let mut newest: Option<RunRow> = None;
        let mut seen = 0u64;
        while let Some(&top) = heap.first() {
            if cursors[top].key() != Some(key.as_slice()) {
                break;
            }
            let row = cursors[top].row()?;
            seen += 1;
            if newest.is_none_or(|held| held.lsn < row.lsn) {
                newest = Some(row);
            }
            cursors[top].advance()?;
            if cursors[top].key().is_none() {
                heap.swap_remove(0);
            }
            sift_down(&mut heap, &cursors, 0);
        }
        report.rows_shadowed += seen.saturating_sub(1);
        if let Some(row) = newest {
            writer.push(&key, row)?;
        }
    }
    Ok(())
}

/// Whether cursor `a` stands on a lesser key than cursor `b`, ties to the earlier cursor
fn is_before(cursors: &[Cursor<'_>], a: usize, b: usize) -> bool {
    match (cursors[a].key(), cursors[b].key()) {
        (Some(left), Some(right)) => (left, a) < (right, b),
        (Some(_), None) => true,
        (None, _) => false,
    }
}

/// Sink the cursor at `at` until both below it stand on greater keys
fn sift_down(heap: &mut [usize], cursors: &[Cursor<'_>], mut at: usize) {
    loop {
        let left = 2 * at + 1;
        if left >= heap.len() {
            return;
        }
        let right = left + 1;
        let child = match right < heap.len() && is_before(cursors, heap[right], heap[left]) {
            true => right,
            false => left,
        };
        if !is_before(cursors, heap[child], heap[at]) {
            return;
        }
        heap.swap(at, child);
        at = child;
    }
}
