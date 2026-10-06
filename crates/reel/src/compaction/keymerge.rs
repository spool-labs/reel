//! Key-run merges: the walk's runs collapsed into one key run, no record moved
//! Only keys and places go down, so neither the spot index nor the map sees a merge

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use crate::compaction::compactor::{Compactor, PassClaim};
use crate::error::{ReelError, Result};
use crate::format::column::ColumnId;
use crate::format::footer::{FooterPartition, SegmentFooter};
use crate::format::loc::{Loc, SegmentId};
use crate::index::keyrun::{key_in, row_in, KeyRun, RunColumn, RunRow, RunWriter};
use crate::index::map::ReelIndex;
use crate::reel::Reel;

/// What one key-run merge did
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MergeReport {
    /// The merge read this many runs, data segments and key runs together
    pub runs_merged: u64,

    /// The new run covers this many data segments that no run covered before
    pub segments_covered: u64,

    /// The new run holds this many rows
    pub rows_written: u64,

    /// A newer row of the same key shadowed this many rows
    pub rows_shadowed: u64,
}

/// One input run of a merge
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
        rows: &'a [u8],
        column: &'a RunColumn,
        at: u64,
    },
}

impl Cursor<'_> {
    /// The cursor's current key, nothing once it is past its rows
    fn key(&self) -> Option<&[u8]> {
        match self {
            Cursor::Footer { rows, at, .. } => rows.key_at(*at),
            Cursor::Keys { rows, column, at } => {
                (*at < column.rows()).then(|| key_in(rows, column, *at as usize))
            }
        }
    }

    /// The cursor's current row, with the place its record lies
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
            Cursor::Keys { rows, column, at } => Ok(row_in(rows, column, *at as usize)?.1),
        }
    }

    /// Step to the next row
    fn advance(&mut self) {
        match self {
            Cursor::Footer { at, .. } => *at += 1,
            Cursor::Keys { at, .. } => *at += 1,
        }
    }
}

/// Merge segments and key runs into one key run, which then answers for every segment they covered
pub fn merge_into_key_run(
    compactor: &Compactor,
    reel: &Reel,
    index: &ReelIndex,
    segments: &[SegmentId],
    runs: &[Arc<KeyRun>],
) -> Result<MergeReport> {
    let shared = reel.shared();
    let mut claims: Vec<PassClaim<'_>> = Vec::new();
    let mut sources = Vec::new();
    for segment in segments {
        let Some(claim) = compactor.claim(*segment) else {
            continue;
        };
        // The list predates the claim, so a rewrite may have retired this segment since
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
    // Claim the runs' segments too, so no rewrite moves a record while the merge picks rows
    let mut claimed: HashSet<SegmentId> = claims.iter().map(PassClaim::segment).collect();
    for run in runs {
        for segment in &run.covered {
            if !index.holds_sealed(*segment) || !claimed.insert(*segment) {
                continue;
            }
            let Some(claim) = compactor.claim(*segment) else {
                return Ok(MergeReport::default());
            };
            claims.push(claim);
        }
    }
    sources.extend(runs.iter().map(|run| Source::Keys(Arc::clone(run))));
    if sources.len() < 2 {
        return Ok(MergeReport::default());
    }
    // Taken under the claims so it holds for the whole merge
    let standing: HashSet<SegmentId> = index
        .segments_snapshot()
        .into_iter()
        .map(|(segment, _)| segment)
        .collect();

    let mut columns: BTreeSet<(ColumnId, u16)> = BTreeSet::new();
    for source in &sources {
        match source {
            Source::Footer { footer, .. } => columns.extend(
                footer
                    .partitions
                    .iter()
                    .filter(|rows| !rows.is_empty())
                    .map(|rows| (rows.column, rows.key_width)),
            ),
            Source::Keys(run) => columns.extend(
                run.columns()
                    .iter()
                    .map(|column| (column.column, column.key_width)),
            ),
        }
    }
    let mut widths = columns
        .iter()
        .map(|(column, _)| *column)
        .collect::<Vec<_>>();
    widths.dedup();
    if widths.len() != columns.len() {
        return Err(ReelError::Rejected(
            "a column comes in two key widths across the runs".to_string(),
        ));
    }

    let root = shared.volumes.roots()[0].clone();
    let id = index.key_runs().draw_id();
    let mut writer = RunWriter::create(&shared.driver, &root, id)?;
    let mut report = MergeReport {
        runs_merged: sources.len() as u64,
        ..MergeReport::default()
    };
    let written = (|| -> Result<()> {
        for (column, width) in &columns {
            writer.begin_column(*column, *width)?;
            merge_column(&sources, *column, &standing, &mut writer, &mut report)?;
        }
        Ok(())
    })();
    if let Err(error) = written {
        writer.abandon();
        return Err(error);
    }

    // Covering a segment the merge skipped would hide its rows from the walk
    let mut covered: BTreeSet<SegmentId> = sources
        .iter()
        .filter_map(|source| match source {
            Source::Footer { segment, .. } => Some(*segment),
            Source::Keys(_) => None,
        })
        .collect();
    for run in runs {
        covered.extend(
            run.covered
                .iter()
                .copied()
                .filter(|segment| standing.contains(segment)),
        );
    }
    let covered: Vec<SegmentId> = covered.into_iter().collect();
    let before = index.key_runs().covered();
    report.segments_covered = covered
        .iter()
        .filter(|segment| !before.contains(segment))
        .count() as u64;
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
    standing: &HashSet<SegmentId>,
    writer: &mut RunWriter<'_>,
    report: &mut MergeReport,
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
                    cursors.push(Cursor::Keys {
                        rows: run.rows(held),
                        column: held,
                        at: 0,
                    });
                }
            }
        }
    }
    // A heap keeps the least key on top, so a row costs a few comparisons however many runs merge
    let mut heap: Vec<usize> = (0..cursors.len())
        .filter(|at| cursors[*at].key().is_some())
        .collect();
    for at in (0..heap.len() / 2).rev() {
        sift_down(&mut heap, &cursors, at);
    }
    let mut key = Vec::new();
    while let Some(&top) = heap.first() {
        key.clear();
        key.extend_from_slice(cursors[top].key().unwrap_or_default());
        // Only rows in standing segments count, since the rest point at moved or dropped records
        let mut newest: Option<RunRow> = None;
        let mut seen = 0u64;
        while let Some(&top) = heap.first() {
            if cursors[top].key() != Some(key.as_slice()) {
                break;
            }
            let row = cursors[top].row()?;
            seen += 1;
            if standing.contains(&row.loc.segment) && newest.is_none_or(|held| held.lsn < row.lsn) {
                newest = Some(row);
            }
            cursors[top].advance();
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
