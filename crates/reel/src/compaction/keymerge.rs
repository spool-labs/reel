//! Key-run merges: the walk's runs collapsed into one key run, no record moved
//! Only keys and places go down, so neither the spot index nor the map sees a merge

use std::collections::{BTreeSet, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::compaction::compactor::{Compactor, PassClaim};
use crate::error::{ReelError, Result};
use crate::format::column::ColumnId;
use crate::format::footer::{FooterFind, FooterPartition, SegmentFooter};
use crate::format::loc::SegmentId;
use crate::format::lsn::Lsn;
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

    /// The run left out this many deleted keys that nothing older can still reach
    pub graves_dropped: u64,
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
    stop: &AtomicBool,
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

    let mut columns: BTreeSet<ColumnId> = BTreeSet::new();
    for source in &sources {
        match source {
            Source::Footer { footer, .. } => columns.extend(
                footer
                    .partitions
                    .iter()
                    .filter(|rows| !rows.is_empty())
                    .map(|rows| rows.column),
            ),
            Source::Keys(run) => columns.extend(run.columns().iter().map(|column| column.column)),
        }
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
    let footers: Arc<dyn FooterSource> = Arc::clone(shared) as Arc<dyn FooterSource>;
    // The standing segments the run leaves out, each with the oldest record it holds
    let mut outside = Vec::new();
    for segment in &standing {
        if covered.binary_search(segment).is_err() {
            outside.push(Outside {
                segment: *segment,
                oldest: index.min_lsn_of(*segment),
                is_sealed: index.holds_sealed(*segment),
            });
        }
    }
    let graves = Graves {
        footers: Arc::clone(&footers),
        outside,
        settled: shared.settled_below(),
    };

    let root = shared.volumes.roots()[0].clone();
    let id = index.key_runs().draw_id();
    let mut writer = RunWriter::create(&shared.driver, &root, id)?;
    let mut report = MergeReport {
        runs_merged: sources.len() as u64,
        ..MergeReport::default()
    };
    let written = (|| -> Result<bool> {
        for column in &columns {
            writer.begin_column(*column)?;
            let finished = merge_column(
                &footers,
                &sources,
                *column,
                &standing,
                &covered,
                &graves,
                &mut writer,
                &mut report,
                stop,
            )?;
            if !finished {
                return Ok(false);
            }
        }
        Ok(true)
    })();
    match written {
        Ok(true) => {}
        // A stopped merge leaves the runs as they stood, and the next open merges them again
        Ok(false) => {
            writer.abandon();
            return Ok(MergeReport::default());
        }
        Err(error) => {
            writer.abandon();
            return Err(error);
        }
    }
    let before = index.key_runs().covered();
    report.segments_covered = covered
        .iter()
        .filter(|segment| !before.contains(segment))
        .count() as u64;
    report.rows_written = writer.rows();
    let path = writer.finish(&covered)?;
    // A run the open refuses answers nothing, and the next merge writes a fresh one
    let run = Arc::new(KeyRun::open_or_unlink(&shared.driver, &path, id)?);
    let merged: Vec<u64> = runs.iter().map(|run| run.id).collect();
    for retired in index.key_runs().install(run, &merged) {
        retired.retire();
    }
    drop(claims);
    Ok(report)
}

/// Write one column's newest row of each key across every source, in key order, false once stopped
#[allow(clippy::too_many_arguments)]
fn merge_column(
    footers: &Arc<dyn FooterSource>,
    sources: &[Source],
    column: ColumnId,
    standing: &HashSet<SegmentId>,
    covered: &[SegmentId],
    graves: &Graves,
    writer: &mut RunWriter<'_>,
    report: &mut MergeReport,
    stop: &AtomicBool,
) -> Result<bool> {
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
    let mut window = Window::default();
    while let Some(&top) = heap.first() {
        if stop.load(Ordering::Relaxed) {
            return Ok(false);
        }
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
            // A copy keeps its record's number, so on a tie the newer segment holds the copy
            if standing.contains(&row.loc.segment)
                && newest.is_none_or(|(held, _)| {
                    (held.lsn, held.loc.segment) < (row.lsn, row.loc.segment)
                })
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
            window.push(&key, row, place);
            if window.bytes >= WINDOW_BYTES {
                window.flush(column, covered, graves, writer, report)?;
            }
        }
    }
    window.flush(column, covered, graves, writer, report)?;
    Ok(true)
}

/// A merge holds this many bytes of rows before it asks the segments left out about its graves
const WINDOW_BYTES: usize = 64 << 20;

/// Rows a merge has picked and not yet written, in key order
#[derive(Default)]
struct Window {
    keys: Vec<u8>,
    rows: Vec<(usize, RunRow, u32)>,
    bytes: usize,
}

impl Window {
    fn push(&mut self, key: &[u8], row: RunRow, place: u32) {
        self.rows.push((self.keys.len(), row, place));
        self.keys.extend_from_slice(key);
        self.bytes += key.len() + std::mem::size_of::<(usize, RunRow, u32)>();
    }

    fn key(&self, at: usize) -> &[u8] {
        let start = self.rows[at].0;
        let end = self.rows.get(at + 1).map_or(self.keys.len(), |next| next.0);
        &self.keys[start..end]
    }

    /// Drop the graves that hide nothing, write the rest, and start empty
    fn flush(
        &mut self,
        column: ColumnId,
        covered: &[SegmentId],
        graves: &Graves,
        writer: &mut RunWriter<'_>,
        report: &mut MergeReport,
    ) -> Result<()> {
        let kept = graves.hiding(column, self)?;
        for at in 0..self.rows.len() {
            let (_, row, place) = self.rows[at];
            if row.flags.is_tombstone() && !kept[at] {
                report.graves_dropped += 1;
                continue;
            }
            let pointer = covered.binary_search(&row.loc.segment).map_err(|_| {
                ReelError::Corruption(format!(
                    "a merged row points at segment {}, which the run does not cover",
                    row.loc.segment.as_u32()
                ))
            })?;
            writer.push(
                self.key(at),
                RunPointer {
                    covered: pointer as u32,
                    row: place,
                },
            )?;
        }
        self.keys.clear();
        self.rows.clear();
        self.bytes = 0;
        Ok(())
    }
}

/// A standing segment a merge leaves out, with the oldest record it holds
struct Outside {
    /// The segment
    segment: SegmentId,

    /// The lowest sequence number it holds, nothing for one it holds no record of
    oldest: Option<Lsn>,

    /// Whether it sealed, so its footer can be asked
    is_sealed: bool,
}

/// What a merge asks before it leaves a deleted key out of its run
struct Graves {
    /// Where the footers of the segments left out are read
    footers: Arc<dyn FooterSource>,

    /// Every standing segment the run does not cover
    outside: Vec<Outside>,

    /// Sequence numbers below this one have landed in a segment
    settled: Lsn,
}

impl Graves {
    /// The window's deletes that may still hide an older version of their key outside the run
    ///
    /// Each segment left out is read once for the whole window, so a merge costs one footer
    /// read per segment and window, however many graves it holds
    fn hiding(&self, column: ColumnId, window: &Window) -> Result<Vec<bool>> {
        let mut kept = vec![false; window.rows.len()];
        let mut open: Vec<usize> = Vec::new();
        for (at, (_, row, _)) in window.rows.iter().enumerate() {
            if !row.flags.is_tombstone() {
                continue;
            }
            match row.lsn >= self.settled {
                true => kept[at] = true,
                false => open.push(at),
            }
        }
        for outside in &self.outside {
            if open.is_empty() {
                break;
            }
            let Some(oldest) = outside.oldest else {
                continue;
            };
            let lsn = |at: usize| window.rows[at].1.lsn;
            if open.iter().all(|at| oldest >= lsn(*at)) {
                continue;
            }
            // A segment with no footer yet cannot be asked, so it keeps the delete
            if !outside.is_sealed {
                open.retain(|at| {
                    kept[*at] = oldest < lsn(*at);
                    !kept[*at]
                });
                continue;
            }
            let Some(footer) = self.footers.footer_once(outside.segment)? else {
                continue;
            };
            let Some(partition) = footer.partition(column) else {
                continue;
            };
            let mut failed = None;
            open.retain(|at| {
                if oldest >= lsn(*at) || failed.is_some() {
                    return true;
                }
                match partition.lookup(window.key(*at)) {
                    Ok(FooterFind::Found(row)) => kept[*at] = row.lsn < lsn(*at),
                    Ok(_) => {}
                    Err(error) => failed = Some(error),
                }
                !kept[*at]
            });
            if let Some(error) = failed {
                return Err(error);
            }
        }
        Ok(kept)
    }
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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::format::column::RecordKey;
    use crate::format::footer::FooterEntry;
    use crate::format::record::Flags;
    use crate::io::fault::FaultPlan;
    use crate::io::sim_backend::SimIo;
    use crate::reel::segment::IoDriver;

    const COLUMN: ColumnId = ColumnId(1);

    /// The merge reads no footer through the source when every input is a segment's own
    struct NoFooters;

    impl FooterSource for NoFooters {
        fn footer(&self, _segment: SegmentId) -> Result<Option<Arc<SegmentFooter>>> {
            Ok(None)
        }
    }

    /// A sealed segment holding one key at one sequence number
    fn sealed(segment: SegmentId, offset: u32) -> Source {
        let key = RecordKey::from_bytes(COLUMN, &[7; 8]).expect("key");
        let row = FooterEntry::new(key, Lsn(5), offset, 100, Flags::DATA);
        let mut footer = SegmentFooter::build(vec![row]);
        let _ = footer.pack(0);
        Source::Footer {
            segment,
            footer: Arc::new(footer),
        }
    }

    // a source and its copy tie on the record's number, and the run points at the copy
    #[test]
    fn copy_wins() {
        let sim = SimIo::new(FaultPlan::new(1));
        let driver = Arc::new(IoDriver::new(Arc::new(sim)));
        let footers: Arc<dyn FooterSource> = Arc::new(NoFooters);
        let covered = [SegmentId(2), SegmentId(19)];
        let sources = [sealed(covered[0], 100), sealed(covered[1], 900)];
        let standing: HashSet<SegmentId> = covered.iter().copied().collect();
        let graves = Graves {
            footers: Arc::clone(&footers),
            outside: Vec::new(),
            settled: Lsn(u64::MAX),
        };
        let mut writer = RunWriter::create(&driver, Path::new("/runs"), 1).expect("create");
        writer.begin_column(COLUMN).expect("column");
        let mut report = MergeReport::default();
        merge_column(
            &footers,
            &sources,
            COLUMN,
            &standing,
            &covered,
            &graves,
            &mut writer,
            &mut report,
            &AtomicBool::new(false),
        )
        .expect("merge");
        let path = writer.finish(&covered).expect("finish");
        let run = KeyRun::open(&driver, &path, 1).expect("open");
        let column = run.column(COLUMN).expect("column");
        assert_eq!(column.rows(), 1);
        assert_eq!(
            run.segment_of(run.pointer(column, 0)),
            Some(covered[1]),
            "the run points at the source"
        );
    }

    /// Footers of the segments a merge leaves out, counting every read
    struct CountingFooters {
        held: Vec<(SegmentId, Arc<SegmentFooter>)>,
        reads: std::sync::atomic::AtomicUsize,
    }

    impl FooterSource for CountingFooters {
        fn footer(&self, segment: SegmentId) -> Result<Option<Arc<SegmentFooter>>> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            Ok(self
                .held
                .iter()
                .find(|(held, _)| *held == segment)
                .map(|(_, footer)| Arc::clone(footer)))
        }
    }

    fn footer_of(rows: Vec<FooterEntry>) -> Arc<SegmentFooter> {
        let mut footer = SegmentFooter::build(rows);
        let _ = footer.pack(0);
        Arc::new(footer)
    }

    fn key(n: u32) -> RecordKey {
        RecordKey::from_bytes(COLUMN, &n.to_be_bytes()).expect("key")
    }

    // each segment left out is read once for all the graves, and only a grave over an older row stays
    #[test]
    fn graves_read_each_outside_segment_once() {
        let sim = SimIo::new(FaultPlan::new(1));
        let driver = Arc::new(IoDriver::new(Arc::new(sim)));
        let merged = SegmentId(40);
        let deletes: Vec<FooterEntry> = (0..1_000u32)
            .map(|n| FooterEntry::new(key(n), Lsn(100), n * 16, 0, Flags::TOMBSTONE))
            .collect();
        let sources = [Source::Footer {
            segment: merged,
            footer: footer_of(deletes),
        }];
        // Only key 3 has an older row outside the run
        let outside: Vec<SegmentId> = (1..=20).map(SegmentId).collect();
        let held = outside
            .iter()
            .map(|segment| {
                let at = if segment.as_u32() == 7 { 3 } else { 5_000 + segment.as_u32() };
                (*segment, footer_of(vec![FooterEntry::new(key(at), Lsn(5), 0, 100, Flags::DATA)]))
            })
            .collect();
        let counting = Arc::new(CountingFooters {
            held,
            reads: std::sync::atomic::AtomicUsize::new(0),
        });
        let footers: Arc<dyn FooterSource> = Arc::clone(&counting) as Arc<dyn FooterSource>;
        let graves = Graves {
            footers: Arc::clone(&footers),
            outside: outside
                .iter()
                .map(|segment| Outside {
                    segment: *segment,
                    oldest: Some(Lsn(5)),
                    is_sealed: true,
                })
                .collect(),
            settled: Lsn(u64::MAX),
        };
        let covered = [merged];
        let standing: HashSet<SegmentId> = covered.iter().copied().collect();
        let mut writer = RunWriter::create(&driver, Path::new("/runs"), 1).expect("create");
        writer.begin_column(COLUMN).expect("column");
        let mut report = MergeReport::default();
        let finished = merge_column(
            &footers,
            &sources,
            COLUMN,
            &standing,
            &covered,
            &graves,
            &mut writer,
            &mut report,
            &AtomicBool::new(false),
        )
        .expect("merge");
        assert!(finished);
        assert_eq!(counting.reads.load(Ordering::Relaxed), outside.len());
        assert_eq!(report.graves_dropped, 999);
        let path = writer.finish(&covered).expect("finish");
        let run = KeyRun::open(&driver, &path, 1).expect("open");
        assert_eq!(run.column(COLUMN).expect("column").rows(), 1, "only key 3's grave stays");
    }
}
