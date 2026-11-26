//! Settling the summary at open against the segments found, and the damage a lost row or tail leaves

use std::collections::{BTreeMap, BTreeSet};
use std::io::Result;
use std::mem;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use reel_core::column_id::ColumnId;
use reel_core::units::{ByteCount, RowCount, WritePlace};

use crate::engine::engine_state::EngineState;
use crate::engine::volume::Volume;
use crate::guard::hold;
use crate::io::{Io, IoFile};
use crate::record::record_check::Checksum;
use crate::segment::damage_list::{DamagedRange, highest_of, spans_of};
use crate::segment::footer_reader::FooterReader;
use crate::segment::segment_tail::SegmentNumber;
use crate::segment::summary::{
    ColumnFacts, LostRows, SUMMARY_KEY_BYTES, SegmentFacts, SpanFacts,
    SummaryPaths,
    SummaryRow, SummaryRows, put_bound, put_lowest,
    read_summary,
};

use crate::engine::recovery::{FoundDamaged, FoundOpen, FoundSealed, OwnedColumn, RootScan, SummaryFound};

/// Mark an open segment's columns damaged whole when rot took a group a sync had covered, ranked at the
/// counter at open: the group's keys can't be known. The row keeps where the rot was, in its seal mark, so
/// the rot an earlier open ranked keeps its rank and clears, and new rot takes this open's counter
pub fn damage_open<Backend: Io>(
    state: &EngineState<Backend>,
    found: &FoundOpen<Backend::File>,
    rot_sum: Checksum,
) -> Result<()> {
    let volume = &state.volumes[found.volume];
    let is_known = hold(&volume.summary).get(found.number).is_some_and(|row| row.facts.seal_mark == rot_sum);
    if is_known {
        return Ok(());
    }

    let counter = state.places.last_given();
    let facts = SegmentFacts {
        segment: found.number,
        seal_mark: rot_sum,
        is_damaged: true,
        has_lost_rows: true,
        damage_place: counter,
        ..SegmentFacts::default()
    };
    let ranges = whole_columns(state, found.volume, found.number, counter);

    volume.damage.drop_segment(found.number);
    for range in &ranges {
        volume.damage.add(range);
    }
    let mut damaged = hold(&volume.damaged);
    if !damaged.contains(&found.number) {
        damaged.push(found.number);
    }
    drop(damaged);
    let mut rows = hold(&volume.summary);
    rows.remove(found.number);
    let dropped = rows.push_kept(&facts, [].into_iter(), ranges.iter().map(span_of));
    drop(rows);
    state.note_dropped_rows(dropped);
    state.save_summary(volume)
}

/// Read each volume's summary, which open reads after the tails
pub fn read_summaries<Backend: Io>(
    io: &Backend,
    roots: &[(usize, Vec<PathBuf>)],
) -> Result<Vec<SummaryFound>> {
    let mut found = Vec::with_capacity(roots.len());
    for (_volume, paths) in roots {
        let summary_paths = SummaryPaths::new(&paths[0]);
        let mut rows = SummaryRows::new(ByteCount(0));
        // the superblock's limit keeps the write order, so open has no use for the summary's mark
        let mut lost = LostRows::default();
        let _mark = read_summary(io, &summary_paths, &mut rows, &mut lost)?;
        found.push(SummaryFound {
            rows,
            paths: summary_paths,
            lost,
        });
    }
    Ok(found)
}

/// Bring each volume's summary in line with what open found, in one walk of its rows
pub fn settle_summaries<Backend: Io>(state: &EngineState<Backend>, scan: &mut RootScan<Backend::File>) -> Result<()> {
    let mut reader = FooterReader::new();
    for (index, volume) in state.volumes.iter().enumerate() {
        let mut rows = hold(&volume.summary);
        mark_lost_rows(state, scan, index, &mut rows);
        let listed = rows.offsets();
        sort_empties(state, scan, index, &listed)?;

        // rows of segments no root holds stay behind here
        let mut settled = SummaryRows::new(ByteCount(0));
        settled.make_room(rows.byte_room());
        let mut held = BTreeSet::new();
        for found in &scan.sealed {
            if found.volume == index {
                held.insert(found.head.segment);
                settle_sealed(state, volume, found, &rows, &listed, &mut settled, &mut reader)?;
            }
        }
        for found in &scan.damaged {
            if found.volume == index {
                held.insert(found.number);
                let row = listed.get(&found.number).and_then(|at| rows.row_at(*at));
                settle_damaged(state, volume, found, row, &mut settled);
            }
        }
        let open: BTreeSet<SegmentNumber> =
            scan.scanned.iter().filter(|segment| segment.found.volume == index).map(|segment| segment.found.number).collect();
        let cleared: BTreeSet<SegmentNumber> = scan.cleared.iter().filter(|(volume, _number)| *volume == index).map(|(_volume, number)| *number).collect();
        carry_rows(volume, &rows, &listed, (&held, &open, &cleared), &mut settled);
        settled.highest_forward = held.last().map_or(SegmentNumber(0), |highest| rows.highest_forward.min(*highest));
        let index_room = rows.index_room();
        rows.settle_from(settled);
        rows.make_index(index_room);
        drop(rows);
        state.save_summary(volume)?;
    }
    Ok(())
}

/// Keep the rows no sealed or damaged segment took: an open segment's, and a gone segment's spans, which stay until the caller
/// clears them or an asked rebuild's output that dropped the segment cleared them
pub fn carry_rows<Backend: Io>(
    volume: &Volume<Backend>,
    rows: &SummaryRows,
    listed: &BTreeMap<SegmentNumber, usize>,
    (held, open, cleared): (&BTreeSet<SegmentNumber>, &BTreeSet<SegmentNumber>, &BTreeSet<SegmentNumber>),
    settled: &mut SummaryRows,
) {
    for (number, at) in listed {
        let Some(row) = rows.row_at(*at) else {
            continue;
        };
        if held.contains(number) || cleared.contains(number) || !row.facts.is_damaged || (!open.contains(number) && row.span_count == 0) {
            continue;
        }

        for range in row_ranges(&row, *number, &mut Vec::new()) {
            volume.damage.add(&range);
        }
        hold(&volume.damaged).push(*number);
        // the room was sized at open from every row it found, and a row with spans has to stay
        settled.make_room_for(rows, *at);
        let is_copied = settled.copy_row(rows, *at);
        debug_assert!(is_copied);
    }
}

/// Build a whole-column range for every column of a volume in a segment
pub fn whole_columns<Backend: Io>(
    state: &EngineState<Backend>,
    volume: usize,
    segment: SegmentNumber,
    counter: WritePlace,
) -> Vec<DamagedRange> {
    let mut ranges = Vec::new();
    for (id, info) in state.columns.iter().enumerate() {
        if info.is_some_and(|info| info.volume.0 as usize == volume) {
            ranges.push(DamagedRange {
                column: ColumnId(id as u8),
                segment,
                highest_place: counter,
                start: Vec::new(),
                end: None,
            });
        }
    }
    ranges
}

/// Damage a segment's columns whole at the counter, keeping its row's facts when it has a row
pub fn widen_row<Backend: Io>(
    state: &EngineState<Backend>,
    index: usize,
    rows: &mut SummaryRows,
    number: SegmentNumber,
    counter: WritePlace,
) {
    let ranges = whole_columns(state, index, number, counter);
    let mut facts = SegmentFacts {
        segment: number,
        ..SegmentFacts::default()
    };
    let mut columns = Vec::new();
    if let Some(row) = rows.get(number) {
        facts = row.facts;
        let _spans = row_ranges(&row, number, &mut columns);
    }
    facts.is_damaged = true;
    facts.damage_place = counter;
    rows.remove(number);
    state.note_dropped_rows(rows.push_kept(&facts, columns.iter().map(column_of), ranges.iter().map(span_of)));
}

/// Damage one record's key in a segment's row at its place, keeping the row's facts and the spans a damaged row already holds. A
/// clean row's spans start from this one alone, since its columns stay readable
pub fn point_row<Backend: Io>(state: &EngineState<Backend>, rows: &mut SummaryRows, number: SegmentNumber, range: &DamagedRange) {
    let mut facts = SegmentFacts {
        segment: number,
        ..SegmentFacts::default()
    };
    let mut columns = Vec::new();
    let mut ranges = Vec::new();
    if let Some(row) = rows.get(number) {
        facts = row.facts;
        let spans = row_ranges(&row, number, &mut columns);
        if row.facts.is_damaged {
            ranges = spans;
        }
    }
    // a row keeps one damaged place for all its spans, the highest any of them needs
    facts.damage_place = if facts.is_damaged { facts.damage_place.max(range.highest_place) } else { range.highest_place };
    facts.is_damaged = true;
    ranges.push(range.clone());
    rows.remove(number);
    state.note_dropped_rows(rows.push_kept(&facts, columns.iter().map(column_of), ranges.iter().map(span_of)));
}

/// Damage whole what a row the newer summary copy lost could have spoken for: an older row may lack the
/// newer row's spans, and a segment with no row may have had some. Older rows keep their facts
pub fn mark_lost_rows<Backend: Io>(
    state: &EngineState<Backend>,
    scan: &RootScan<Backend::File>,
    index: usize,
    rows: &mut SummaryRows,
) {
    let lost = mem::take(&mut *hold(&state.volumes[index].lost_rows));
    let counter = state.places.last_given();
    for number in &lost.unread_covers {
        widen_row(state, index, rows, *number, counter);
    }
    // a row from the older copy can sit behind counts the newer rows already hold, so only a backward walk is safe
    for number in &lost.older {
        rows.restart_backward(*number);
    }
    if !lost.has_lost_row {
        return;
    }

    for number in lost.older {
        widen_row(state, index, rows, number, counter);
    }

    let sealed = scan.sealed.iter().filter(|found| found.volume == index).map(|found| found.head.segment);
    let open = scan.scanned.iter().filter(|segment| segment.found.volume == index).map(|segment| segment.found.number);
    for number in sealed.chain(open) {
        if rows.get(number).is_some() {
            continue;
        }
        let facts = SegmentFacts {
            segment: number,
            is_damaged: true,
            damage_place: counter,
            ..SegmentFacts::default()
        };
        let ranges = whole_columns(state, index, number, counter);
        state.note_dropped_rows(rows.push_kept(&facts, [].into_iter(), ranges.iter().map(span_of)));
    }
}

/// Unlink the files that never took a record, and treat one the summary lists as damaged
pub fn sort_empties<Backend: Io>(
    state: &EngineState<Backend>,
    scan: &mut RootScan<Backend::File>,
    volume: usize,
    listed: &BTreeMap<SegmentNumber, usize>,
) -> Result<()> {
    let mut index = 0;
    while index < scan.empty.len() {
        if scan.empty[index].volume != volume {
            index += 1;
            continue;
        }
        let found = scan.empty.swap_remove(index);
        if listed.contains_key(&found.number) {
            // a zeroed first sector on a sealed segment reads like this, and the row says what it is
            scan.damaged.push(FoundDamaged {
                volume,
                path: found.root.clone(),
                number: found.number,
                file_length: found.file_length,
                head: None,
                columns: Vec::new(),
            });
            continue;
        }
        let _is_gone = state.remove_or_park(&state.volumes[volume], &found.root, found.number);
    }
    Ok(())
}

/// Copy a sealed segment's row when it matches the tail, and build it again when it doesn't
pub fn settle_sealed<Backend: Io>(
    state: &EngineState<Backend>,
    volume: &Volume<Backend>,
    found: &FoundSealed<Backend::File>,
    rows: &SummaryRows,
    listed: &BTreeMap<SegmentNumber, usize>,
    settled: &mut SummaryRows,
    reader: &mut FooterReader,
) -> Result<()> {
    let facts = SegmentFacts {
        segment: found.head.segment,
        file_length: found.file.length()?,
        data_end: found.head.data_end,
        row_block_end: found.head.index_at,
        seal_mark: found.head.index_sum,
        places: found.head.places,
        marks: found.head.marks,
        is_damaged: false,
        has_covers: found.head.ranges_length.0 > 0,
        damage_place: WritePlace(0),
        block_count: found.head.block_count.0,
        walked_blocks: 0,
        // no saved count can hold a walk of a segment above the number, one at or below it may be held already, and an output
        // holds rows its input's walk already counted
        is_backward: found.head.segment <= rows.highest_forward || (found.head.is_output && !found.head.is_unwalked),
        has_lost_rows: false,
        most_block_rows: found.entries.iter().map(|entry| entry.live_rows).max().unwrap_or(0),
    };

    if let Some(at) = listed.get(&facts.segment)
        && let Some(row) = rows.row_at(*at)
        && is_same_segment(&row.facts, &facts)
    {
        // spans outlive the fault that made them, and only clear_damage ends them
        if row.facts.is_damaged {
            for range in row_ranges(&row, facts.segment, &mut Vec::new()) {
                volume.damage.add(&range);
            }
            hold(&volume.damaged).push(facts.segment);
        }
        // the room was sized at open from every row it found, and a row with spans has to stay
        settled.make_room_for(rows, *at);
        let is_copied = settled.copy_row(rows, *at);
        debug_assert!(is_copied);
        return Ok(());
    }

    let mut keys = Vec::with_capacity(found.columns.len());
    for column in &found.columns {
        let first = column.first_block as usize;
        let lines = found.entries.get(first..first + column.block_count.0 as usize).unwrap_or_default();
        let mut lowest = Vec::new();
        let mut highest = Vec::new();
        if !reader.edge_keys(&found.file, lines, found.head.place_base, &mut lowest, &mut highest)? {
            // a block that can't be read gets the widest range, which can only ask too much of a read
            keys.push((Vec::new(), Vec::new()));
            continue;
        }
        let mut kept_lowest = Vec::with_capacity(SUMMARY_KEY_BYTES);
        let mut bound = Vec::with_capacity(SUMMARY_KEY_BYTES + 1);
        put_lowest(&lowest, &mut kept_lowest)?;
        put_bound(&highest, &mut bound)?;
        keys.push((kept_lowest, bound));
    }

    // the blocks give no byte counts. A forward row spreads the data over its columns by rows, so live and dead add up to the data
    // end as a seal's counts do and the walk moves bytes to dead from there. A backward walk rebuilds live from 0
    let total_rows = found.columns.iter().map(|column| column.row_count.0).sum::<u64>().max(1);
    let data_bytes = if facts.is_backward { 0 } else { facts.data_end.0 };
    let columns = found.columns.iter().zip(keys.iter()).map(|(column, edges)| ColumnFacts {
        column: column.column,
        lowest: &edges.0,
        bound: &edges.1,
        row_count: column.row_count,
        grave_count: column.grave_count,
        dead_rows: RowCount(0),
        dead_graves: RowCount(0),
        live_bytes: ByteCount((u128::from(data_bytes) * u128::from(column.row_count.0) / u128::from(total_rows)) as u64),
        dead_bytes: ByteCount(0),
    });
    let mut ranges = Vec::new();
    let mut facts = facts;
    // a seal that crashed before its row went in leaves the open segment's row, and its spans go on
    if let Some(row) = listed.get(&facts.segment).and_then(|at| rows.row_at(*at))
        && is_open_row(&row)
    {
        ranges = row_ranges(&row, facts.segment, &mut Vec::new());
        facts.is_damaged = true;
        facts.damage_place = row.facts.damage_place;
        facts.has_lost_rows = row.facts.has_lost_rows;
        for range in &ranges {
            volume.damage.add(range);
        }
        hold(&volume.damaged).push(facts.segment);
    }
    if ranges.is_empty() {
        if !settled.push(&facts, columns, [].into_iter()) {
            state.note_dropped_row();
        }
    } else {
        state.note_dropped_rows(settled.push_kept(&facts, columns, ranges.iter().map(span_of)));
    }
    Ok(())
}

/// Turn a damaged segment's summary row, or what's left of it, into ranges reads must respect
pub fn settle_damaged<Backend: Io>(
    state: &EngineState<Backend>,
    volume: &Volume<Backend>,
    found: &FoundDamaged,
    row: Option<SummaryRow>,
    settled: &mut SummaryRows,
) {
    let mut ranges = Vec::new();
    let mut facts = SegmentFacts {
        segment: found.number,
        file_length: found.file_length,
        is_damaged: true,
        ..SegmentFacts::default()
    };
    let mut columns: Vec<OwnedColumn> = Vec::new();

    // a row can give the keys a segment held, and never the ranges its lost footer hid, so those go whole
    match row.filter(|row| is_row_trusted(row, found) && !row.facts.has_covers) {
        Some(row) => {
            facts = row.facts;
            facts.is_damaged = true;
            ranges = row_ranges(&row, found.number, &mut columns);
            // damage from before the tails went ranks below any row that landed after it, and the lost footer hid those too.
            // Once ranked at the highest place this stops, so a span a caller cleared stays cleared
            if row.facts.damage_place < row.facts.places.highest {
                facts.damage_place = row.facts.places.highest;
                if row.facts.is_damaged {
                    ranges.extend(column_ranges(&columns, found.number, row.facts.places.highest));
                }
            }
        },
        None => fallback_ranges(state, found, &mut facts, &mut ranges),
    }

    for range in &ranges {
        volume.damage.add(range);
    }
    hold(&volume.damaged).push(found.number);
    let column_facts = columns.iter().map(column_of);
    state.note_dropped_rows(settled.push_kept(&facts, column_facts, ranges.iter().map(span_of)));
}

/// Check a row can speak for a damaged segment: its length always, and the tail's facts when one reads
pub fn is_row_trusted(row: &SummaryRow, found: &FoundDamaged) -> bool {
    if row.facts.file_length != found.file_length {
        return false;
    }
    match &found.head {
        Some(head) => row.facts.seal_mark == head.index_sum && row.facts.places == head.places,
        None => true,
    }
}

/// Read the damaged ranges a row gives, and copy its columns out
pub fn row_ranges(row: &SummaryRow, segment: SegmentNumber, columns: &mut Vec<OwnedColumn>) -> Vec<DamagedRange> {
    let mut ranges = Vec::new();
    let highest_place = match row.facts.is_damaged {
        true => row.facts.damage_place,
        false => row.facts.places.highest,
    };
    let mut cursor = row.cursor();
    for _column in 0..row.column_count {
        let Some(column) = cursor.column() else {
            break;
        };
        columns.push(OwnedColumn {
            column: column.column,
            lowest: column.lowest.to_vec(),
            bound: column.bound.to_vec(),
            row_count: column.row_count,
            grave_count: column.grave_count,
            dead_rows: column.dead_rows,
            dead_graves: column.dead_graves,
            live_bytes: column.live_bytes,
            dead_bytes: column.dead_bytes,
        });
    }

    if row.facts.is_damaged {
        // the spans a row keeps are all that's left, since a caller may have cleared the rest
        for _span in 0..row.span_count {
            let Some(span) = cursor.span() else {
                break;
            };
            ranges.push(DamagedRange {
                column: span.column,
                segment,
                highest_place,
                start: span.start.to_vec(),
                end: span.end.map(<[u8]>::to_vec),
            });
        }
        return ranges;
    }

    ranges.extend(column_ranges(columns, segment, highest_place));
    ranges
}

/// Cover each of a row's columns from its lowest key to its bound
pub fn column_ranges<'columns>(
    columns: &'columns [OwnedColumn],
    segment: SegmentNumber,
    highest_place: WritePlace,
) -> impl Iterator<Item = DamagedRange> + 'columns {
    columns.iter().map(move |column| DamagedRange {
        column: column.column,
        segment,
        highest_place,
        start: column.lowest.clone(),
        end: (!column.bound.is_empty()).then(|| column.bound.clone()),
    })
}

/// Cover whole columns when no row can speak for a damaged segment
pub fn fallback_ranges<Backend: Io>(
    state: &EngineState<Backend>,
    found: &FoundDamaged,
    facts: &mut SegmentFacts,
    ranges: &mut Vec<DamagedRange>,
) {
    let highest_place = match &found.head {
        Some(head) => {
            facts.places = head.places;
            facts.data_end = head.data_end;
            facts.row_block_end = head.index_at;
            facts.seal_mark = head.index_sum;
            facts.damage_place = head.places.highest;
            head.places.highest
        },
        None => {
            // every row a read can find sits at or below the counter at open, and every new write above it
            let counter = state.places.last_given();
            facts.places.highest = counter;
            facts.damage_place = counter;
            counter
        },
    };

    let mut add = |column: ColumnId| {
        ranges.push(DamagedRange {
            column,
            segment: found.number,
            highest_place,
            start: Vec::new(),
            end: None,
        });
    };
    if found.head.is_some() {
        for column in &found.columns {
            add(column.column);
        }
        return;
    }
    for (id, info) in state.columns.iter().enumerate() {
        if info.is_some_and(|info| info.volume.0 as usize == found.volume) {
            add(ColumnId(id as u8));
        }
    }
}

/// Check a row and a tail describe the same segment, whatever its row says about damage or the walk
pub fn is_same_segment(row: &SegmentFacts, tail: &SegmentFacts) -> bool {
    let without_damage = SegmentFacts {
        is_damaged: false,
        damage_place: WritePlace(0),
        // a settled segment's row drops the flag its tail keeps for good
        has_covers: tail.has_covers,
        walked_blocks: tail.walked_blocks,
        is_backward: tail.is_backward,
        has_lost_rows: false,
        ..*row
    };
    without_damage == *tail
}

/// Check a row was written for a segment still open, which has no length to speak of
pub fn is_open_row(row: &SummaryRow) -> bool {
    row.facts.is_damaged && row.facts.file_length.0 == 0
}

/// Read a range as the span a row keeps
pub fn span_of(range: &DamagedRange) -> SpanFacts<'_> {
    SpanFacts {
        column: range.column,
        start: &range.start,
        end: range.end.as_deref(),
    }
}

/// Read an owned column as the facts a row keeps
pub fn column_of(column: &OwnedColumn) -> ColumnFacts<'_> {
    ColumnFacts {
        column: column.column,
        lowest: &column.lowest,
        bound: &column.bound,
        row_count: column.row_count,
        grave_count: column.grave_count,
        dead_rows: column.dead_rows,
        dead_graves: column.dead_graves,
        live_bytes: column.live_bytes,
        dead_bytes: column.dead_bytes,
    }
}

/// Write what's left of every damaged segment's spans into its row, in place, which never fails: a row with
/// spans stays, and a segment with no row gets one that speaks for its spans alone
pub fn save_cleared_spans<Backend: Io>(state: &EngineState<Backend>, volume: &Volume<Backend>) {
    let mut rows = hold(&volume.summary);
    let damaged = hold(&volume.damaged);
    let slots = volume.damage.slots();
    for number in damaged.iter() {
        let spans = spans_of(&slots, *number);
        match rows.rewrite_spans(*number, spans.clone()) {
            Some(dropped) => state.note_dropped_rows(dropped),
            None => {
                let facts = SegmentFacts {
                    segment: *number,
                    is_damaged: true,
                    damage_place: highest_of(&slots, *number).unwrap_or(WritePlace(0)),
                    ..SegmentFacts::default()
                };
                state.note_dropped_rows(rows.push_kept(&facts, [].into_iter(), spans));
            },
        }
    }
}

impl<Backend: Io> EngineState<Backend> {
    /// Damage a segment's columns whole at the counter, when a seal finds a cover it can't read. The row it leaves is an
    /// open segment's, which open carries over to the sealed segment, and the seal saves it before its tail is durable
    pub fn damage_whole(&self, volume: usize, segment: SegmentNumber) {
        // a claim in flight stands above the volume's mark, and its row has to land above this damage
        let counter = self.volumes[volume].order.mark();
        let ranges = whole_columns(self, volume, segment, counter);
        let target = &self.volumes[volume];
        for range in &ranges {
            target.damage.add(range);
        }
        let mut damaged = hold(&target.damaged);
        if !damaged.contains(&segment) {
            damaged.push(segment);
            self.damaged_segments.fetch_add(1, Ordering::AcqRel);
        }
        drop(damaged);

        let facts = SegmentFacts {
            segment,
            is_damaged: true,
            damage_place: counter,
            ..SegmentFacts::default()
        };
        let mut rows = hold(&target.summary);
        rows.remove(segment);
        let dropped = rows.push_kept(&facts, [].into_iter(), ranges.iter().map(span_of));
        drop(rows);
        self.note_dropped_rows(dropped);
    }
}
