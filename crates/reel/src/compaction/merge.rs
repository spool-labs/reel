//! The k-way merge of a volume's sorted runs into one
//!
//! Sealing by rewriting leaves one sorted run a segment, and a paged read has to ask
//! every standing run because a segment number is not a version. This pass reads the
//! runs together in key order, keeps the newest version of every key, and retires the
//! sources once its own output is sealed. The maintenance tick runs it on a volume that
//! armed it, when the standing stack has gone dead enough to be worth collapsing, and a
//! caller may drive a pass of its own whatever the stack holds.
//!
//! Every tombstone is carried forward whatever it shadows, so no merge can resurrect a
//! deleted key, and a run holding a version an open cue point can still read is not
//! selected at all. The rows come out of the sources' footers, held parsed for the
//! length of the pass, so a volume past what its footers weigh is more than this shape
//! can merge.

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::append::Appender;
use crate::config::RepairPath;
use crate::error::{ReelError, Result};
use crate::format::column::{Codec, ColumnId, RecordKey};
use crate::format::footer::{FooterEntry, FooterPartition, FooterRow, SegmentFooter, NO_RECORD};
use crate::format::loc::SegmentId;
use crate::format::lsn::Lsn;
use crate::format::record::Flags;
use crate::index::map::{KeyRepoint, ReelIndex};
use crate::reel::segment::{SegmentHandle, SegmentReader};
use crate::reel::{Reel, ReelShared};
use crate::sync::rendezvous;

use crate::compaction::compactor::{
    footer_bound, is_missing, read_payload, segment_len, source_handle, Compactor, PassClaim,
    RecordScan, SourceRecord,
};
use crate::compaction::pressure::PassPace;

/// Repoints one hold of the publish barrier takes
///
/// A reader waiting on the barrier pays the whole hold, so the cap is on the hold
/// rather than on the pass.
const REPOINT_BATCH: usize = 4096;

/// Sorted runs below which there is nothing for a merge to collapse
///
/// One run already answers a read in one look, and merging it with itself would
/// rewrite the volume to produce what it started with.
const RUNS_WORTH_MERGING: usize = 2;

/// What one merge pass did
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MergeReport {
    /// Sorted runs the pass read together
    pub runs_merged: u64,

    /// Sources retired, once the output was sealed and the repoints published
    pub sources_retired: u64,

    /// Sources left standing because something in them failed its checksum
    pub sources_kept_by_rot: u64,

    /// Segments the merge wrote, every one of them holding nothing else
    pub segments_written: u64,

    /// Rows written into the output, one per key the merge kept
    pub rows_written: u64,

    /// Rows written as a value in the row itself, with no record behind them
    pub rows_listed: u64,

    /// Tombstones carried forward, which a merge never drops
    pub tombstones_kept: u64,

    /// Rows a newer version shadowed, read and not written
    pub rows_shadowed: u64,

    /// Resident entries repointed at the copies
    pub entries_repointed: u64,

    /// Holds of the publish barrier the repoints took
    pub repoint_batches: u64,

    /// Bytes the pass bought back from the sources
    pub bytes_read: u64,

    /// Bytes the pass wrote into its output
    pub bytes_written: u64,
}

/// One sealed sorted run a pass is reading, and the file behind it
struct MergeSource {
    /// The segment the run is
    segment: SegmentId,

    /// Held for the length of the pass, so the file outlives the reads
    handle: SegmentHandle,

    /// The rows themselves, which is what the merge walks
    footer: Arc<SegmentFooter>,

    /// Where this segment's record region ends
    region_end: u64,

    /// Whether something in it would not verify, which keeps it standing
    is_rotted: bool,
}

impl MergeSource {
    /// The partition holding one column's rows, or nothing where it holds none
    fn partition_of(&self, column: ColumnId) -> Option<&FooterPartition> {
        self.footer
            .partitions
            .iter()
            .find(|partition| partition.column == column)
    }
}

/// The row a merge chose for one key, and the run it came out of
struct Winner {
    /// Which source the row is in
    at: usize,

    /// Which row of that source's partition for the column
    row: usize,

    /// The sequence number that won it
    lsn: Lsn,
}

/// Everything a pass accumulates while it writes
struct MergeState {
    /// What the pass will report when it is done
    report: MergeReport,

    /// Moved records waiting for a hold of the publish barrier
    pending: Vec<KeyRepoint>,

    /// Segments the output landed in, which are the ones whose spans get noted
    written: BTreeSet<SegmentId>,
}

/// Merge the volume's sorted runs into one, in a pass the plane or a caller drives
///
/// Refused on a volume that did not arm it, since a volume that does not seal by
/// rewriting has no sorted runs and a merge over it would quietly do nothing.
pub fn merge_once(
    compactor: &Compactor,
    reel: &Reel,
    index: &ReelIndex,
    cue_floor: Option<Lsn>,
) -> Result<MergeReport> {
    let shared = reel.shared();
    if !shared.config.merge_sorted_runs {
        return Err(ReelError::Rejected(
            "this volume did not arm merge_sorted_runs, so it holds no sorted runs a merge could collapse"
                .to_string(),
        ));
    }
    // an unswept cover spans rows a merge reads as live, and retiring their segments
    // would take them out from under the release pass
    if index.has_pending_covers() {
        return Ok(MergeReport::default());
    }

    // The claim stops a compaction pass retiring a run out from under the merge reading
    // it, and dropping it stops a failed pass hiding those runs for ever.
    let mut claims = Vec::new();
    let mut sources = select_runs(compactor, reel, index, cue_floor, &mut claims)?;
    if sources.len() < RUNS_WORTH_MERGING {
        return Ok(MergeReport::default());
    }

    let mut state = MergeState {
        report: MergeReport {
            runs_merged: sources.len() as u64,
            ..MergeReport::default()
        },
        pending: Vec::with_capacity(REPOINT_BATCH),
        written: BTreeSet::new(),
    };
    // created where the charged reads begin, so the footers selection parsed are off
    // the gate's books
    let mut pace = compactor.pace();

    let writer = Appender::open_for_merge(Arc::clone(shared), merge_tail_index(shared))?;
    let walked = write_merged(reel, index, &writer, &mut sources, &mut state, &mut pace);
    rendezvous::at("merge/written");
    // the writer finishes whichever way the walk left it, since only a seal that lands
    // takes the hold off its segments
    let finished = writer.finish();
    drop(writer);
    walked?;
    finished?;
    state.report.segments_written = state.written.len() as u64;

    // nothing is repointed at a listed row: the output's own spans are what a search
    // follows to reach it, and they go down before any source is retired
    let is_output_sealed = note_output_spans(shared, index, &state)?;
    rendezvous::at("merge/sealed");

    // The sources are the authority until the output can answer for what it took from
    // them: a segment with no footer answers for nothing no repoint names, which is
    // every listed row and every tombstone it carried.
    if is_output_sealed && !index.has_pending_covers() {
        retire_sources(compactor, shared, index, sources, &mut state);
    }
    drop(claims);
    // the tail past the last step is left standing on the gate rather than slept off here
    pace.settle(
        state
            .report
            .bytes_read
            .saturating_add(state.report.bytes_written),
    );
    compactor.note_merged_runs(state.report.runs_merged);
    Ok(state.report)
}

/// Tell the index what every output segment holds, and say whether all of them sealed
///
/// A rolled segment whose seal the device refused is parked for the maintenance tick
/// rather than reported, so the footers are what the pass believes rather than the seal
/// call. A segment without one holds rows nothing can reach: a repointed key still
/// reads, but a listed row and a carried tombstone are reachable only through a footer.
fn note_output_spans(
    shared: &Arc<ReelShared>,
    index: &ReelIndex,
    state: &MergeState,
) -> Result<bool> {
    let mut is_sealed = true;
    for segment in &state.written {
        let Some(footer) = shared.footer_of(*segment)? else {
            tracing::warn!(
                "a merge's output segment {} has no footer, so its sources stand for the next pass",
                segment.as_u32(),
            );
            is_sealed = false;
            continue;
        };
        index.note_spans(*segment, &footer)?;
    }
    Ok(is_sealed)
}

/// Where a merge's writer sits in the tail numbering, past every tail the reel opened
///
/// Past the reserved tails, so draws are unpinned and the output lands on whichever
/// volume has room.
fn merge_tail_index(shared: &ReelShared) -> u64 {
    shared.config.tail_count() as u64 + 1
}

/// The dead share of the standing sorted runs, or nothing where there is no stack
///
/// Summed across the runs together rather than read per segment: what a merge collapses
/// is the stack, and one run's own share says nothing about how much of the stack a
/// newer run shadows. Nothing where fewer runs stand than a merge could collapse, which
/// is the one case where a stack at any share is worth no pass.
///
/// A run a pass would pass over is left out here too, or a volume whose whole debt sits
/// in one no merge may touch would ask for a pass every tick and get an empty one.
pub fn sorted_run_dead_ratio(
    compactor: &Compactor,
    reel: &Reel,
    index: &ReelIndex,
    cue_floor: Option<Lsn>,
) -> Result<Option<f64>> {
    let shared = reel.shared();
    let owed = shared.pending_seals();
    let mut runs = 0usize;
    let mut dead = 0u64;
    let mut total = 0u64;
    for (segment, bytes) in index.segments_snapshot() {
        if shared.is_held(segment) || owed.contains(&segment) || bytes.total() == 0 {
            continue;
        }
        if compactor.is_rot_pinned(segment) {
            continue;
        }
        if cue_floor.is_some_and(|floor| {
            index
                .min_lsn_of(segment)
                .is_some_and(|oldest| oldest <= floor)
        }) {
            continue;
        }
        // the dead share moves as records die, while whether a segment is a run at all
        // was settled by its footer, so only the first is derived per tick
        let Some(facts) = compactor.facts_of(shared, segment)? else {
            continue;
        };
        if !facts.is_sorted_run {
            continue;
        }
        runs += 1;
        dead = dead.saturating_add(bytes.dead);
        total = total.saturating_add(bytes.total());
    }
    if runs < RUNS_WORTH_MERGING || total == 0 {
        return Ok(None);
    }
    Ok(Some(dead as f64 / total as f64))
}

/// The sealed sorted runs a merge may read
///
/// Every exclusion here is a way to lose data or to answer a read wrong: a segment a
/// tail still holds is being written into, one whose spans the index has not been told
/// about answers every liveness question with nothing, one an older reader can still
/// see holds versions a merge would drop, and one left standing for rot holds bytes no
/// rewrite may stamp a fresh checksum over.
fn select_runs<'compactor>(
    compactor: &'compactor Compactor,
    reel: &Reel,
    index: &ReelIndex,
    cue_floor: Option<Lsn>,
    claims: &mut Vec<PassClaim<'compactor>>,
) -> Result<Vec<MergeSource>> {
    let shared = reel.shared();
    let owed = shared.pending_seals();
    let mut sources = Vec::new();
    for (segment, bytes) in index.segments_snapshot() {
        if shared.is_held(segment) || owed.contains(&segment) || bytes.total() == 0 {
            continue;
        }
        if compactor.is_rot_pinned(segment) {
            continue;
        }
        if cue_floor.is_some_and(|floor| {
            index
                .min_lsn_of(segment)
                .is_some_and(|oldest| oldest <= floor)
        }) {
            continue;
        }
        // a segment whose records are not in the order its rows are in is not a run,
        // which its footer settled at the seal
        let Some(facts) = compactor.facts_of(shared, segment)? else {
            continue;
        };
        if !facts.is_sorted_run {
            continue;
        }
        // the rows themselves, which the walk needs whatever the facts say
        let Some(footer) = shared.footer_of(segment)? else {
            continue;
        };
        let handle = match source_handle(shared, segment) {
            Ok(handle) => handle,
            Err(error) if is_missing(&error) => continue,
            Err(error) => return Err(error),
        };
        let Some(file_len) = segment_len(shared, &handle)? else {
            continue;
        };
        // claimed under the walk that chose it, or a pass beside this one retires it mid-merge
        let Some(claim) = compactor.claim(segment) else {
            continue;
        };
        claims.push(claim);
        let region_end = footer_bound(shared, &handle, file_len)?;
        sources.push(MergeSource {
            segment,
            handle,
            footer,
            region_end,
            is_rotted: false,
        });
    }
    // the queue above was read before the walk, so a segment that sealed inside it is
    // absent from that copy while its hold is already gone
    let owed = shared.pending_seals();
    sources.retain(|source| !owed.contains(&source.segment));
    claims.retain(|claim| {
        sources
            .iter()
            .any(|source| source.segment == claim.segment())
    });
    Ok(sources)
}

/// Walk every column's runs together and write the winners through the merge's writer
fn write_merged(
    reel: &Reel,
    index: &ReelIndex,
    writer: &Appender,
    sources: &mut [MergeSource],
    state: &mut MergeState,
    pace: &mut PassPace<'_>,
) -> Result<()> {
    let shared = reel.shared();
    let mut readers: Vec<SegmentReader<'_>> = sources
        .iter()
        .map(|source| SegmentReader::new(&shared.driver, source.handle.file(), source.region_end))
        .collect();

    // Ascending, because that is the order a footer writes its partitions in, and an
    // output whose columns arrived out of that order reads as an unsorted run.
    let mut columns: Vec<ColumnId> = index.columns().iter().map(|spec| spec.id).collect();
    columns.sort_unstable();

    let mut walked = Ok(());
    for column in columns {
        walked = merge_column(
            reel,
            index,
            writer,
            sources,
            &mut readers,
            state,
            column,
            pace,
        );
        if walked.is_err() {
            break;
        }
    }
    if walked.is_ok() {
        walked = flush_repoints(index, state);
    }
    // counted from the reader, so a refill over dead bytes is charged like any other
    state.report.bytes_read = readers.iter().map(SegmentReader::read_bytes).sum();
    walked
}

/// The k-way walk over one column's runs
///
/// Rows within a partition are ordered by key and then by sequence number, so a run of
/// equal keys inside one source ends on that source's newest version of the key, and
/// the winner across sources is the highest of those. Every cursor standing on the key
/// is advanced past it, whether it won or not.
///
/// Metered a key at a time, so the stretch a paced volume's gate cannot interrupt is one
/// record read plus one record written.
#[allow(clippy::too_many_arguments)]
fn merge_column(
    reel: &Reel,
    index: &ReelIndex,
    writer: &Appender,
    sources: &mut [MergeSource],
    readers: &mut [SegmentReader<'_>],
    state: &mut MergeState,
    column: ColumnId,
    pace: &mut PassPace<'_>,
) -> Result<()> {
    let mut cursors: Vec<usize> = vec![0; sources.len()];
    loop {
        let Some(key) = least_key(sources, &cursors, column) else {
            return Ok(());
        };
        let winner = take_key(sources, &mut cursors, column, &key, &mut state.report);
        let Some(winner) = winner else {
            continue;
        };
        let key = RecordKey::from_bytes(column, &key)?;
        emit_row(reel, index, writer, sources, readers, state, &key, &winner)?;
        pace.reached(moved_bytes(readers, state));
        if state.pending.len() >= REPOINT_BATCH {
            flush_repoints(index, state)?;
        }
    }
}

/// Device traffic the pass has run up so far, which is what the gate is charged for
///
/// A running total rather than a delta, since the meter takes one, and read off the
/// readers rather than summed from the rows kept.
fn moved_bytes(readers: &[SegmentReader<'_>], state: &MergeState) -> u64 {
    let read: u64 = readers.iter().map(SegmentReader::read_bytes).sum();
    read.saturating_add(state.report.bytes_written)
}

/// The lowest key any cursor is standing on, copied out so the cursors can move
fn least_key(sources: &[MergeSource], cursors: &[usize], column: ColumnId) -> Option<Vec<u8>> {
    let mut least: Option<&[u8]> = None;
    for (at, source) in sources.iter().enumerate() {
        let Some(partition) = source.partition_of(column) else {
            continue;
        };
        let Some(key) = partition.key_at(cursors[at]) else {
            continue;
        };
        if least.is_none_or(|held| key < held) {
            least = Some(key);
        }
    }
    least.map(<[u8]>::to_vec)
}

/// Advance every cursor past one key, handing back the newest row it stood on
fn take_key(
    sources: &[MergeSource],
    cursors: &mut [usize],
    column: ColumnId,
    key: &[u8],
    report: &mut MergeReport,
) -> Option<Winner> {
    let mut winner: Option<Winner> = None;
    let mut seen = 0u64;
    for (at, source) in sources.iter().enumerate() {
        let Some(partition) = source.partition_of(column) else {
            continue;
        };
        while partition.key_at(cursors[at]) == Some(key) {
            let row = cursors[at];
            cursors[at] += 1;
            let Ok(found) = partition.row_at(row) else {
                continue;
            };
            seen += 1;
            if winner.as_ref().is_none_or(|held| found.lsn > held.lsn) {
                winner = Some(Winner {
                    at,
                    row,
                    lsn: found.lsn,
                });
            }
        }
    }
    report.rows_shadowed += seen.saturating_sub(1);
    winner
}

/// Write one key's winning row into the output, whichever shape that row has
#[allow(clippy::too_many_arguments)]
fn emit_row(
    reel: &Reel,
    index: &ReelIndex,
    writer: &Appender,
    sources: &mut [MergeSource],
    readers: &mut [SegmentReader<'_>],
    state: &mut MergeState,
    key: &RecordKey,
    winner: &Winner,
) -> Result<()> {
    let row = row_of(sources, key.column, winner)?;

    // A tombstone is written whatever the index says, since the index is what it is
    // there to survive: a pruned grave reads as nothing here, and dropping the record
    // on that is how a merge resurrects the version underneath it.
    if row.is_tombstone() || row.is_range_tombstone() {
        return carry_tombstone(index, writer, sources, readers, state, key, winner);
    }

    // A data row is written only while the volume still answers with it: a row written
    // past a version an open tail, an unselected run or a standing cover holds plants a
    // version nothing points at and books a key the volume has already counted.
    let live = index.get(key)?;
    if live.is_none_or(|entry| entry.lsn != row.lsn) {
        state.report.rows_shadowed += 1;
        return Ok(());
    }

    if row.stands_alone() {
        return relist(index, writer, sources, state, key, winner);
    }
    copy_record(reel, index, writer, sources, readers, state, key, winner)
}

/// The row one winner names, read back out of the source it came from
fn row_of(sources: &[MergeSource], column: ColumnId, winner: &Winner) -> Result<FooterRow> {
    sources[winner.at]
        .partition_of(column)
        .ok_or_else(|| {
            ReelError::Corruption("a merge lost the partition it was walking".to_string())
        })?
        .row_at(winner.row)
}

/// Carry a row that holds its own value into the output, still holding it
///
/// A scan walks records and a standing row has none, so a merge that passed over these
/// would retire their source and take the only copy of the value with it. Nothing is
/// repointed, since the source keeps answering until it retires.
fn relist(
    index: &ReelIndex,
    writer: &Appender,
    sources: &mut [MergeSource],
    state: &mut MergeState,
    key: &RecordKey,
    winner: &Winner,
) -> Result<()> {
    if !index.residency().pages() {
        return Err(ReelError::Corruption(format!(
            "segment {} holds a row carrying its own value on a volume whose index does not page",
            sources[winner.at].segment.as_u32(),
        )));
    }
    let row = row_of(sources, key.column, winner)?;
    let carry = index
        .spec(key.column)
        .map(|spec| spec.row_carry_width())
        .unwrap_or(0);
    // a row that will not verify is the only copy of its value, so its source keeps
    // standing the way a rotted record's does
    let carried = sources[winner.at]
        .partition_of(key.column)
        .and_then(|partition| partition.carried_at(winner.row).ok().flatten())
        .and_then(|value| {
            FooterEntry::standing_alone(key.clone(), row.lsn, row.flags, carry, value)
        });
    let Some(entry) = carried else {
        sources[winner.at].is_rotted = true;
        return Ok(());
    };

    let landed = writer.list_carried_row(&entry)?;
    index.note_listed(key, sources[winner.at].segment, row.len);
    state.written.insert(landed);
    state.report.rows_written += 1;
    state.report.rows_listed += 1;
    Ok(())
}

/// Carry one tombstone forward, whatever it shadows
///
/// Dropping one is how a merge resurrects a deleted key, and the only thing that could
/// say a grave is finished is a floor below every reader on the volume, which a merge
/// neither holds nor is asked to work out.
fn carry_tombstone(
    index: &ReelIndex,
    writer: &Appender,
    sources: &mut [MergeSource],
    readers: &mut [SegmentReader<'_>],
    state: &mut MergeState,
    key: &RecordKey,
    winner: &Winner,
) -> Result<()> {
    let row = row_of(sources, key.column, winner)?;
    let carried = match row.is_range_tombstone() {
        false => writer.append_carried_tombstone(key.clone(), row.lsn)?,
        true => {
            // the payload is the exclusive end, and a range delete that lost it would
            // come back covering a different span
            let Some(record) = read_source_record(readers, winner, row.offset)? else {
                sources[winner.at].is_rotted = true;
                return Ok(());
            };
            let end = read_payload(&mut readers[winner.at], &record)?;
            if !record.header.verify(&end) {
                sources[winner.at].is_rotted = true;
                return Ok(());
            }
            writer.append_carried_range(key.clone(), row.lsn, end)?
        }
    };
    // the output has to know it holds this, or it seals with no row and the delete is lost
    index.hold(key, row.lsn, carried.loc);
    state.written.insert(carried.loc.segment);
    state.report.rows_written += 1;
    state.report.tombstones_kept += 1;
    state.report.bytes_written += u64::from(carried.loc.len);
    Ok(())
}

/// Copy one live record into the output, or put its value in a row and write none
#[allow(clippy::too_many_arguments)]
fn copy_record(
    reel: &Reel,
    index: &ReelIndex,
    writer: &Appender,
    sources: &mut [MergeSource],
    readers: &mut [SegmentReader<'_>],
    state: &mut MergeState,
    key: &RecordKey,
    winner: &Winner,
) -> Result<()> {
    let row = row_of(sources, key.column, winner)?;
    let Some(record) = read_source_record(readers, winner, row.offset)? else {
        sources[winner.at].is_rotted = true;
        return Ok(());
    };
    let payload = read_payload(&mut readers[winner.at], &record)?;
    let segment = sources[winner.at].segment;

    if !record.header.verify(&payload) {
        // With peers the eviction turns the miss into a repair enqueue and the source
        // may still retire. A sole copy keeps its bytes where they are: rewriting them
        // would stamp a fresh checksum over rot and serve it as good.
        if reel.shared().config.repair == RepairPath::Peers {
            index.evict_at(key, record.loc(segment))?;
            return Ok(());
        }
        sources[winner.at].is_rotted = true;
        tracing::warn!(
            "a record in segment {} fails its checksum on a sole copy, so a merge leaves its run standing",
            segment.as_u32(),
        );
        return Ok(());
    }

    // a value the output's rows can hold goes into a row and nowhere else
    if let Some(entry) = carried_row(index, key, record.header.lsn, &payload) {
        let landed = writer.list_carried_row(&entry)?;
        index.note_listed(key, segment, record.header.length);
        state.written.insert(landed);
        state.report.rows_written += 1;
        state.report.rows_listed += 1;
        return Ok(());
    }

    let committed =
        writer.append_copy(key.clone(), record.header.lsn, payload, record.header.codec)?;
    state.written.insert(committed.loc.segment);
    state.report.rows_written += 1;
    state.report.bytes_written += record.span();
    state.pending.push(KeyRepoint {
        key: key.clone(),
        to: committed.loc,
        lsn: record.header.lsn,
    });
    Ok(())
}

/// A row that can be this value's only home, when everything about it allows one
///
/// Every condition is a way to lose the value rather than a preference: the volume has
/// to page and the key must be gone from the resident map, since a resident entry would
/// keep naming a record about to be unlinked, the record has to be uncompressed, and
/// the row has to be able to hold the value at all.
fn carried_row(
    index: &ReelIndex,
    key: &RecordKey,
    lsn: Lsn,
    payload: &[u8],
) -> Option<FooterEntry> {
    if !index.residency().pages() {
        return None;
    }
    if index.codec_of(key.column) != Codec::None {
        return None;
    }
    if index
        .column(key.column)
        .and_then(|column| column.entry_or_grave(key.as_slice()))
        .is_some()
    {
        return None;
    }
    let carry = index.spec(key.column)?.row_carry_width();
    FooterEntry::standing_alone(key.clone(), lsn, Flags::DATA, carry, payload)
}

/// The record one row names, or nothing where the records disagree with the footer
fn read_source_record(
    readers: &mut [SegmentReader<'_>],
    winner: &Winner,
    offset: u32,
) -> Result<Option<SourceRecord>> {
    if offset == NO_RECORD {
        return Ok(None);
    }
    let found = RecordScan::resuming(&mut readers[winner.at], u64::from(offset)).next_record()?;
    Ok(found.filter(|record| record.offset == offset))
}

/// Publish the repoints a run of copies is owed, under one hold of the barrier
fn flush_repoints(index: &ReelIndex, state: &mut MergeState) -> Result<()> {
    if state.pending.is_empty() {
        return Ok(());
    }
    rendezvous::at("merge/repoint");
    let moved = index.repoint_batch(&state.pending)?;
    state.pending.clear();
    state.report.entries_repointed += moved;
    state.report.repoint_batches += 1;
    Ok(())
}

/// Retire every source the pass finished with, and leave the rotted ones standing
///
/// The index stops naming a segment before its file goes, not after: a paged read
/// chooses its segment from a footer search, and the other order kept offering one
/// whose file had already been unlinked.
fn retire_sources(
    compactor: &Compactor,
    shared: &Arc<ReelShared>,
    index: &ReelIndex,
    sources: Vec<MergeSource>,
    state: &mut MergeState,
) {
    for source in sources {
        if source.is_rotted {
            // pinned at the dead bytes left behind, so a later pass does not read the
            // whole file to meet the same checksum miss
            compactor.pin_rot(source.segment, index.segment_bytes(source.segment).dead);
            state.report.sources_kept_by_rot += 1;
            continue;
        }
        rendezvous::at("merge/retire");
        index.forget_segment(source.segment);
        compactor.forget_facts(source.segment);
        shared.fd_cache.remove(source.segment);
        shared.footers.forget(source.segment);
        source.handle.mark_doomed();
        state.report.sources_retired += 1;
    }
}
