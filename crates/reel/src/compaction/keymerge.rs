//! Key-run merges: the walk's runs collapsed into one key run, no record moved
//! Only keys and places go down, so neither the spot index nor the map sees a merge

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use crate::compaction::compactor::{Compactor, PassClaim};
use crate::error::{ReelError, Result};
use crate::format::column::ColumnId;
use crate::format::footer::{FooterPartition, SegmentFooter};
use crate::format::loc::SegmentId;
use crate::index::keyrun::{
    FooterRows, KeyRun, RowReader, RunColumn, RunPointer, RunRow, RunWriter,
};
use crate::index::map::ReelIndex;
use crate::index::paged::FooterSource;
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
        column: &'a RunColumn,
        rows: FooterRows,
        at: u64,
        current: Option<(Vec<u8>, RunRow, u32)>,
    },
}

impl<'a> Cursor<'a> {
    /// A cursor on a key run's column, standing on its first row that reads
    fn keys(
        footers: &Arc<dyn FooterSource>,
        run: &Arc<KeyRun>,
        column: &'a RunColumn,
        standing: &HashSet<SegmentId>,
    ) -> Result<Cursor<'a>> {
        let rows = FooterRows::new(
            Arc::clone(footers),
            Arc::clone(run),
            column.column,
            &|segment| standing.contains(&segment),
        );
        let mut cursor = Cursor::Keys {
            column,
            rows,
            at: 0,
            current: None,
        };
        cursor.settle()?;
        Ok(cursor)
    }

    /// Read the key run cursor's row at `at`, stepping past rows into segments gone
    fn settle(&mut self) -> Result<()> {
        let Cursor::Keys {
            column,
            rows,
            at,
            current,
        } = self
        else {
            return Ok(());
        };
        let mut key = current.take().map(|(key, _, _)| key).unwrap_or_default();
        while *at < column.rows() {
            let pointer = rows.run().pointer(column, *at);
            if let Some((found, row)) = rows.read(pointer)? {
                key.clear();
                key.extend_from_slice(found);
                *current = Some((key, row, pointer.row));
                return Ok(());
            }
            *at += 1;
        }
        Ok(())
    }

    /// The cursor's current key, nothing once it is past its rows
    fn key(&self) -> Option<&[u8]> {
        match self {
            Cursor::Footer { rows, at, .. } => rows.key_at(*at),
            Cursor::Keys { current, .. } => current.as_ref().map(|(key, _, _)| key.as_slice()),
        }
    }

    /// The cursor's current row and its place in its segment's footer partition
    fn row(&self) -> Result<(RunRow, u32)> {
        match self {
            Cursor::Footer { segment, rows, at } => {
                Ok((RunRow::of(*segment, rows.row_at(*at)?), *at as u32))
            }
            Cursor::Keys { current, .. } => current
                .as_ref()
                .map(|(_, row, place)| (*row, *place))
                .ok_or_else(|| {
                    ReelError::Corruption("a spent key run cursor was read".to_string())
                }),
        }
    }

    fn advance(&mut self) -> Result<()> {
        match self {
            Cursor::Footer { at, .. } => {
                *at += 1;
                Ok(())
            }
            Cursor::Keys { at, .. } => {
                *at += 1;
                self.settle()
            }
        }
    }
}

/// Merge segments and key runs into one key run, which then answers for their segments
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

    // A covered segment the merge skipped would hide its rows from the walk
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

    let root = shared.volumes.roots()[0].clone();
    let id = index.key_runs().draw_id();
    let mut writer = RunWriter::create(&shared.driver, &root, id)?;
    let mut report = MergeReport {
        runs_merged: sources.len() as u64,
        ..MergeReport::default()
    };
    let footers: Arc<dyn FooterSource> = Arc::clone(shared) as Arc<dyn FooterSource>;
    let written = (|| -> Result<()> {
        for (column, width) in &columns {
            writer.begin_column(*column, *width)?;
            merge_column(
                &footers,
                &sources,
                *column,
                &standing,
                &covered,
                &mut writer,
                &mut report,
            )?;
        }
        Ok(())
    })();
    if let Err(error) = written {
        writer.abandon();
        return Err(error);
    }
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
    footers: &Arc<dyn FooterSource>,
    sources: &[Source],
    column: ColumnId,
    standing: &HashSet<SegmentId>,
    covered: &[SegmentId],
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
                    cursors.push(Cursor::keys(footers, run, held, standing)?);
                }
            }
        }
    }
    // A heap keeps the least key on top, so each row costs a few comparisons
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
        let mut newest: Option<(RunRow, u32)> = None;
        let mut seen = 0u64;
        while let Some(&top) = heap.first() {
            if cursors[top].key() != Some(key.as_slice()) {
                break;
            }
            let (row, place) = cursors[top].row()?;
            seen += 1;
            if standing.contains(&row.loc.segment)
                && newest.is_none_or(|(held, _)| held.lsn < row.lsn)
            {
                newest = Some((row, place));
            }
            cursors[top].advance()?;
            if cursors[top].key().is_none() {
                heap.swap_remove(0);
            }
            sift_down(&mut heap, &cursors, 0);
        }
        report.rows_shadowed += seen.saturating_sub(1);
        if let Some((row, place)) = newest {
            let at = covered.binary_search(&row.loc.segment).map_err(|_| {
                ReelError::Corruption(format!(
                    "a merged row points at segment {}, which the run does not cover",
                    row.loc.segment.as_u32()
                ))
            })?;
            writer.push(
                &key,
                RunPointer {
                    covered: at as u32,
                    row: place,
                },
            )?;
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
