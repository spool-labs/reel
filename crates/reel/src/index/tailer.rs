//! Following the log from a reader that does not own it

use std::collections::HashMap;
use std::path::Path;

use crate::error::Result;
use crate::format::footer::SegmentFooter;
use crate::format::journal::read_groups;
use crate::format::loc::SegmentId;
use crate::format::lsn::Lsn;
use crate::index::entry::RangeCover;
use crate::index::map::ReelIndex;
use crate::index::recovery::{
    footer_records, journal_records, read_footer, read_rows, WalkedRecord, SEALED,
};
use crate::index::tbtreemap::{TBTreeMap, NODE_WIDTH};
use crate::io::op::FileId;
use crate::reel::segment::{read_segment_header, IoDriver};
use crate::reel::{segment_file_name, segment_number};

/// A pass keeps a range delete for this many sequence numbers after it moves past it
const COVER_WINDOW: u64 = 1 << 20;

/// A reader holding more covers than this says so, and the caller rebuilds
const MAX_COVERS: usize = 4096;

/// How far a reader has consumed the log, and what it still has to remember
#[derive(Debug, Default)]
pub struct LogCursor {
    /// How far the last pass read in each segment
    positions: TBTreeMap<SegmentId, NODE_WIDTH, u64>,

    /// Range deletes still held against records older than themselves
    ranges: Vec<RangeCover>,
}

impl LogCursor {
    /// A cursor that has consumed nothing, which reads the volume from its start
    pub fn new() -> LogCursor {
        LogCursor::default()
    }

    /// How many segments the cursor tracks a position in
    pub fn len(&self) -> usize {
        self.positions.len()
    }

    /// Whether the cursor has consumed nothing at all
    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }

    /// How many range deletes the reader still holds against older records
    pub fn range_count(&self) -> usize {
        self.ranges.len()
    }

    /// Start from where a rebuild left every segment it read
    pub fn start_from(&mut self, consumed: &HashMap<SegmentId, u64>) {
        for (segment, offset) in consumed {
            self.positions.insert(*segment, *offset);
        }
    }

    /// Forget a segment the volume no longer has
    fn retire(&mut self, segment: SegmentId) {
        // Packed, since retirement drains the low end, where a bare removal leaves empty leaves
        self.positions.remove_packed(&segment);
    }

    /// Drop the covers too far below the highest sequence number to hide anything still arriving
    fn prune_covers(&mut self, highest_lsn: Lsn) {
        let floor = highest_lsn.as_u64().saturating_sub(COVER_WINDOW);
        if floor == 0 {
            return;
        }
        self.ranges.retain(|cover| cover.lsn.as_u64() > floor);
    }

    /// Whether the reader is holding more covers than it is willing to test per record
    fn is_saturated(&self) -> bool {
        self.ranges.len() > MAX_COVERS
    }
}

/// What one catch-up pass found
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CaughtUp {
    /// Records the pass applied to the index
    pub applied: u64,

    /// Records the pass read and declined as stale or covered
    pub skipped: u64,

    /// Segments the volume has retired since the last pass, so a reader drops those descriptors
    pub retired: Vec<SegmentId>,

    /// Highest sequence number the pass saw
    pub highest_lsn: Lsn,

    /// Whether the reader holds more range deletes than it can keep testing, so it should rebuild
    pub is_saturated: bool,

    /// Versions still counted in retired segments after the pass, which only a rebuild settles
    pub lost: u64,
}

/// Advance a reader's index to what the volume holds now
pub fn catch_up(
    driver: &IoDriver,
    reel_dir: &Path,
    index: &ReelIndex,
    cursor: &mut LogCursor,
) -> Result<CaughtUp> {
    let listing = driver.list_or_empty(reel_dir)?;
    let mut present: TBTreeMap<SegmentId, NODE_WIDTH, u64> = TBTreeMap::new();
    for entry in listing {
        if let Some(number) = segment_number(&entry.name) {
            present.insert(SegmentId(number), entry.len);
        }
    }

    let gone: Vec<SegmentId> = cursor
        .positions
        .iter()
        .map(|(segment, _)| *segment)
        .filter(|segment| !present.contains_key(segment))
        .collect();

    let mut found: Vec<WalkedRecord> = Vec::new();
    let mut sealed: Vec<(SegmentId, SegmentFooter)> = Vec::new();
    for (segment, file_len) in present.iter() {
        let from = cursor.positions.get(segment).copied().unwrap_or(0);
        if from == SEALED {
            continue;
        }
        let path = reel_dir.join(segment_file_name(*segment));
        let file = driver.open(&path, false)?;
        let followed = follow_segment(driver, file, *segment, *file_len, from);
        driver.close(file)?;
        let (records, next, footer) = followed?;
        cursor.positions.insert(*segment, next);
        found.extend(records);
        sealed.extend(footer.map(|footer| (*segment, footer)));
    }

    // The index's guards assume sequence order, and kept ranges cover what one pass's sort cannot
    found.sort_by_key(|record| record.lsn);
    // A follower serves reads throughout, so the pass publishes under the index's barrier
    let applied = index.publish_pass(|| apply_all(index, cursor, found, &gone));
    // Retired after the pass applies, so its writes can book the versions these segments held
    for segment in &gone {
        cursor.retire(*segment);
        index.forget_segment(*segment);
    }
    // A counted slot left in a retired segment holds a version no record moved or booked
    let lost = index.forget_retired_slots();
    let mut result = applied?;
    result.retired = gone;
    result.lost = lost;

    // A segment that sealed since the open gets its spans, so the graves its tombstones left can go
    for (segment, footer) in &sealed {
        if let Err(error) = index.note_spans(*segment, footer) {
            tracing::warn!(
                "reel segment {} sealed with a footer that holds no key range: {error}",
                segment.as_u32()
            );
        }
    }

    // Prune after the pass, since the highest sequence number seen decides it
    cursor.prune_covers(result.highest_lsn);
    result.is_saturated = cursor.is_saturated();
    Ok(result)
}

/// What a follower has not read of one segment, where it reads next, and its footer once sealed
fn follow_segment(
    driver: &IoDriver,
    file: FileId,
    segment: SegmentId,
    file_len: u64,
    from: u64,
) -> Result<(Vec<WalkedRecord>, u64, Option<SegmentFooter>)> {
    // The sequence guard turns down the footer rows a pass through the journal already applied
    if let Some(footer) = read_footer(driver, file, file_len)? {
        let records = footer_records(driver, file, segment, &footer)?;
        return Ok((records, SEALED, Some(footer)));
    }
    let Some(rows_at) = read_segment_header(driver, file)?
        .map(|header| header.rows_at)
        .filter(|rows_at| *rows_at > 0 && file_len >= *rows_at)
    else {
        return Ok((Vec::new(), from, None));
    };
    let bytes = read_rows(driver, file, rows_at, file_len, from)?;
    let (groups, valid) = read_groups(&bytes);
    Ok((journal_records(segment, groups), from + valid as u64, None))
}

fn apply_all(
    index: &ReelIndex,
    cursor: &mut LogCursor,
    found: Vec<WalkedRecord>,
    retired: &[SegmentId],
) -> Result<CaughtUp> {
    let mut result = CaughtUp::default();
    for record in found {
        if record.lsn > result.highest_lsn {
            result.highest_lsn = record.lsn;
        }
        if apply_one(index, cursor, &record, retired)? {
            result.applied += 1;
        } else {
            result.skipped += 1;
        }
    }
    Ok(result)
}

/// Apply one record, and report whether it changed anything
fn apply_one(
    index: &ReelIndex,
    cursor: &mut LogCursor,
    record: &WalkedRecord,
    retired: &[SegmentId],
) -> Result<bool> {
    if record.flags.is_range_tombstone() {
        let cover = RangeCover {
            start: record.key.clone(),
            end: record.range_end.clone(),
            lsn: record.lsn,
        };
        index.remove_range(
            &record.key,
            cover.end.as_ref().map(|end| end.as_slice()),
            record.lsn,
            record.loc,
        )?;
        cursor.ranges.push(cover);
        return Ok(true);
    }

    if record.flags.is_tombstone() {
        return index.remove(&record.key, record.lsn, record.loc);
    }

    // A record a range delete covered must stay gone, and the index would take it as fresh
    if cursor
        .ranges
        .iter()
        .any(|cover| cover.covers(&record.key, record.lsn))
    {
        return Ok(false);
    }

    // A relocation is the same version in a new place, so the key follows it
    if record.flags.is_relocated() {
        let from = index.retired_source(&record.key, retired, record.loc.len);
        if index.repoint(&record.key, from, record.loc, record.lsn)? {
            return Ok(true);
        }
    }

    index.insert(&record.key, record.loc, record.lsn)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::format::column::{Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, RecordKey};
    use crate::format::loc::Loc;
    use crate::format::record::Flags;

    const RECORDS: ColumnId = ColumnId(1);

    const COLUMNS: ColumnSet = &[ColumnSpec {
        id: RECORDS,
        name: "records",
        key_width: KeyWidth::Fixed(8),
        shard_bytes: 1,
        purge_mark: None,
        codec: Codec::None,
    }];

    fn key(byte: u8) -> RecordKey {
        RecordKey::from_bytes(RECORDS, &[byte; 8]).expect("key")
    }

    fn data(byte: u8, lsn: u64, segment: u32) -> WalkedRecord {
        WalkedRecord {
            key: key(byte),
            lsn: Lsn(lsn),
            loc: Loc::new(SegmentId(segment), 0, 100),
            flags: Flags::DATA,
            range_end: None,
        }
    }

    fn index() -> ReelIndex {
        ReelIndex::new(COLUMNS).expect("index")
    }

    // an older version arriving after a newer one is refused by the index guard
    #[test]
    fn stale_put_is_refused() {
        let index = index();
        let mut cursor = LogCursor::new();

        assert!(apply_one(&index, &mut cursor, &data(1, 5, 2), &[]).expect("apply"));
        assert!(!apply_one(&index, &mut cursor, &data(1, 3, 1), &[]).expect("apply"));
        assert_eq!(
            index.get(&key(1)).expect("read").expect("present").lsn,
            Lsn(5)
        );
    }

    // a key a range delete covered does not come back on a later pass
    #[test]
    fn a_covered_key_stays_deleted() {
        let index = index();
        let mut cursor = LogCursor::new();
        let range = WalkedRecord {
            key: key(0),
            lsn: Lsn(10),
            loc: Loc::new(SegmentId(1), 0, 0),
            flags: Flags::RANGE_TOMBSTONE,
            range_end: None,
        };

        assert!(apply_one(&index, &mut cursor, &range, &[]).expect("apply"));
        assert_eq!(cursor.range_count(), 1);

        assert!(
            !apply_one(&index, &mut cursor, &data(4, 9, 2), &[]).expect("apply"),
            "older than the delete"
        );
        assert!(!index.contains(&key(4)).expect("read"));

        assert!(
            apply_one(&index, &mut cursor, &data(4, 11, 2), &[]).expect("apply"),
            "newer than the delete"
        );
        assert!(index.contains(&key(4)).expect("read"));
    }

    fn range(byte: u8, lsn: u64) -> WalkedRecord {
        WalkedRecord {
            key: key(byte),
            lsn: Lsn(lsn),
            loc: Loc::new(SegmentId(1), 0, 0),
            flags: Flags::RANGE_TOMBSTONE,
            range_end: None,
        }
    }

    // the covers a reader holds do not grow with every range delete it ever reads
    #[test]
    fn covers_are_pruned_behind_the_window() {
        let index = index();
        let mut cursor = LogCursor::new();
        for lsn in [1u64, 2, 3] {
            apply_one(&index, &mut cursor, &range(lsn as u8, lsn), &[]).expect("apply");
        }
        apply_one(&index, &mut cursor, &range(9, COVER_WINDOW + 10), &[]).expect("apply");
        assert_eq!(cursor.range_count(), 4);

        cursor.prune_covers(Lsn(COVER_WINDOW + 10));

        assert_eq!(
            cursor.range_count(),
            1,
            "only the cover inside the window is kept"
        );
    }

    // a cover still inside the window survives, since something older may yet arrive
    #[test]
    fn a_recent_cover_is_kept() {
        let index = index();
        let mut cursor = LogCursor::new();
        apply_one(&index, &mut cursor, &range(1, COVER_WINDOW), &[]).expect("apply");

        cursor.prune_covers(Lsn(COVER_WINDOW + 1));

        assert_eq!(cursor.range_count(), 1);
        assert!(
            !apply_one(&index, &mut cursor, &data(1, COVER_WINDOW - 1, 2), &[]).expect("apply")
        );
    }

    // a pass that has seen nothing far enough along prunes nothing
    #[test]
    fn an_early_pass_prunes_nothing() {
        let index = index();
        let mut cursor = LogCursor::new();
        apply_one(&index, &mut cursor, &range(1, 5), &[]).expect("apply");

        cursor.prune_covers(Lsn(7));

        assert_eq!(cursor.range_count(), 1);
    }

    // a reader past the ceiling reports saturation
    #[test]
    fn too_many_covers_saturates() {
        let index = index();
        let mut cursor = LogCursor::new();

        for at in 0..=MAX_COVERS as u64 {
            apply_one(&index, &mut cursor, &range(1, COVER_WINDOW + at + 1), &[]).expect("apply");
        }

        assert!(
            cursor.is_saturated(),
            "the reader is holding more than it will test"
        );
    }

    // a relocation repoints the key to its new place
    #[test]
    fn a_relocation_repoints() {
        let index = index();
        let mut cursor = LogCursor::new();
        apply_one(&index, &mut cursor, &data(1, 5, 1), &[]).expect("apply");

        let moved = WalkedRecord {
            flags: Flags::DATA.relocated(),
            loc: Loc::new(SegmentId(9), 4096, 100),
            ..data(1, 5, 9)
        };
        assert!(apply_one(&index, &mut cursor, &moved, &[]).expect("apply"));

        let entry = index.get(&key(1)).expect("read").expect("present");
        assert_eq!(entry.loc.segment, SegmentId(9));
        assert_eq!(entry.lsn, Lsn(5), "a relocation is the same version");
    }

    // a tombstone drops the key and an older one changes nothing
    #[test]
    fn tombstones_follow_their_order() {
        let index = index();
        let mut cursor = LogCursor::new();
        apply_one(&index, &mut cursor, &data(1, 5, 1), &[]).expect("apply");

        let stale = WalkedRecord {
            flags: Flags::TOMBSTONE,
            ..data(1, 4, 1)
        };
        let fresh = WalkedRecord {
            flags: Flags::TOMBSTONE,
            ..data(1, 6, 1)
        };

        assert!(!apply_one(&index, &mut cursor, &stale, &[]).expect("apply"));
        assert!(index.contains(&key(1)).expect("read"));
        assert!(apply_one(&index, &mut cursor, &fresh, &[]).expect("apply"));
        assert!(!index.contains(&key(1)).expect("read"));
    }

    // a cursor forgets a segment the volume no longer holds
    #[test]
    fn a_retired_segment_leaves_the_cursor() {
        let mut cursor = LogCursor::new();
        cursor.positions.insert(SegmentId(1), 4096);
        cursor.positions.insert(SegmentId(2), 8192);

        cursor.retire(SegmentId(1));

        assert_eq!(cursor.len(), 1);
        assert!(cursor.positions.contains_key(&SegmentId(2)));
    }
}
