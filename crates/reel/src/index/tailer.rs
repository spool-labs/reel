//! Following the log from a reader that does not own it
//!
//! A read-only open holds an index nothing advances, so a reader does what recovery
//! does from where it last stopped. Ordering is the whole difficulty: a
//! segment-by-segment read is not in sequence order, and range tombstones are not
//! guarded by sequence number the way puts and point tombstones are, so the reader
//! keeps the ranges it has seen and tests later records against them.

use std::collections::HashMap;
use std::path::Path;

use crate::error::Result;
use crate::format::loc::SegmentId;
use crate::format::lsn::Lsn;
use crate::index::entry::RangeCover;
use crate::index::map::ReelIndex;
use crate::index::recovery::{walk_records, WalkedRecord};
use crate::index::tbtreemap::{TBTreeMap, NODE_WIDTH};
use crate::reel::segment::{IoDriver, SegmentReader};
use crate::reel::{segment_file_name, segment_number};

/// Sequence numbers a range delete is kept for once the pass has moved past it
///
/// A cover is for a later pass delivering something older, which happens when a
/// record drew its sequence number before the cover but had not landed where the
/// reader had read to. The admission budget bounds how far apart those can be.
const COVER_WINDOW: u64 = 1 << 20;

/// Covers a reader holds before it stops trusting the list at all
///
/// Quietly dropping a cover would let a deleted key come back, so a reader past
/// this says so and the caller rebuilds instead, which needs no covers at all.
const MAX_COVERS: usize = 4096;

/// How far a reader has consumed the log, and what it still has to remember
///
/// The positions are per segment because that is where a walk resumes. The ranges
/// outlive any one pass, since a record old enough to hide may arrive much later.
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

    /// Segments the cursor is tracking a position in
    pub fn len(&self) -> usize {
        self.positions.len()
    }

    /// Whether the cursor has consumed nothing at all
    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }

    /// Range deletes the reader is still holding against older records
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
        // Packed, since the segment numbers climb and retirement drains the low
        // end, where the bare removal would leave the emptied leaves behind.
        self.positions.remove_packed(&segment);
    }

    /// Drop the covers nothing older than can still arrive
    ///
    /// No writer can hold a record unlanded a whole window, so a cover that far
    /// below the highest sequence number seen has outlived what it could hide.
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

    /// Segments the volume has retired since the last pass, named so a reader can
    /// drop exactly those descriptors
    pub retired: Vec<SegmentId>,

    /// Highest sequence number the pass saw
    pub highest_lsn: Lsn,

    /// Whether the reader holds more range deletes than it can keep testing, so it
    /// should rebuild rather than keep following
    pub is_saturated: bool,
}

/// Advance a reader's index to what the volume holds now
///
/// The walk starts where the last pass stopped in every segment still present,
/// picks up the segments that have appeared and drops the ones that have gone.
/// Applying in sequence order is what makes a pass match the writer's own.
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
    for segment in &gone {
        cursor.retire(*segment);
        index.forget_segment(*segment);
    }

    let mut found: Vec<WalkedRecord> = Vec::new();
    for (segment, file_len) in present.iter() {
        let from = cursor.positions.get(segment).copied().unwrap_or(0);
        if from >= *file_len {
            continue;
        }
        let file = driver.open(&reel_dir.join(segment_file_name(*segment)), false)?;
        let mut reader = SegmentReader::new(driver, file, *file_len);
        let walked = walk_records(&mut reader, *segment, from, *file_len);
        driver.close(file)?;
        let walked = walked?;
        cursor.positions.insert(*segment, walked.next_offset);
        found.extend(walked.records);
    }

    // The index's guards assume sequence order; across passes the retained ranges
    // cover what sorting one pass cannot.
    found.sort_by_key(|record| record.lsn);
    // A follower serves reads throughout, so the pass publishes under the index's own
    // barrier, with every device read it needed already done above.
    let mut result = index.publish_pass(|| apply_all(index, cursor, found, gone))?;

    // After the pass, since the highest sequence number seen is what decides it.
    cursor.prune_covers(result.highest_lsn);
    result.is_saturated = cursor.is_saturated();
    Ok(result)
}

fn apply_all(
    index: &ReelIndex,
    cursor: &mut LogCursor,
    found: Vec<WalkedRecord>,
    retired: Vec<SegmentId>,
) -> Result<CaughtUp> {
    let mut result = CaughtUp {
        retired,
        ..CaughtUp::default()
    };
    for record in found {
        if record.lsn > result.highest_lsn {
            result.highest_lsn = record.lsn;
        }
        if apply_one(index, cursor, &record)? {
            result.applied += 1;
        } else {
            result.skipped += 1;
        }
    }
    Ok(result)
}

/// Apply one record, and report whether it changed anything
fn apply_one(index: &ReelIndex, cursor: &mut LogCursor, record: &WalkedRecord) -> Result<bool> {
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

    // A record a range delete already covered must not come back, and nothing in
    // the index would refuse it: the key is absent, so the put looks fresh.
    if cursor
        .ranges
        .iter()
        .any(|cover| cover.covers(&record.key, record.lsn))
    {
        return Ok(false);
    }

    // A relocation is the same version in a new place, so taking it for a stale
    // write would leave the reader on the segment compaction is about to unlink.
    if record.flags.is_relocated() && index.repoint(&record.key, record.loc, record.lsn)? {
        return Ok(true);
    }

    // The replay holds headers rather than payloads, so a carrying column starts
    // cold here and warms on its first read.
    index.insert(&record.key, record.loc, record.lsn, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::format::column::{
        Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape, RecordKey,
    };
    use crate::format::loc::Loc;
    use crate::format::record::Flags;

    const RECORDS: ColumnId = ColumnId(1);

    const COLUMNS: ColumnSet = &[ColumnSpec {
        id: RECORDS,
        name: "records",
        key_width: KeyWidth::Fixed(8),
        shard_bytes: 1,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
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
        ReelIndex::new(
            COLUMNS,
            crate::config::IndexResidency::Resident,
            crate::config::ShardShapes::Tree,
        )
        .expect("index")
    }

    // an older version arriving after a newer one is refused by the index guard
    #[test]
    fn stale_put_is_refused() {
        let index = index();
        let mut cursor = LogCursor::new();

        assert!(apply_one(&index, &mut cursor, &data(1, 5, 2)).expect("apply"));
        assert!(!apply_one(&index, &mut cursor, &data(1, 3, 1)).expect("apply"));
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

        assert!(apply_one(&index, &mut cursor, &range).expect("apply"));
        assert_eq!(cursor.range_count(), 1);

        assert!(
            !apply_one(&index, &mut cursor, &data(4, 9, 2)).expect("apply"),
            "older than the delete"
        );
        assert!(!index.contains(&key(4)).expect("read"));

        assert!(
            apply_one(&index, &mut cursor, &data(4, 11, 2)).expect("apply"),
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
            apply_one(&index, &mut cursor, &range(lsn as u8, lsn)).expect("apply");
        }
        apply_one(&index, &mut cursor, &range(9, COVER_WINDOW + 10)).expect("apply");
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
        apply_one(&index, &mut cursor, &range(1, COVER_WINDOW)).expect("apply");

        cursor.prune_covers(Lsn(COVER_WINDOW + 1));

        assert_eq!(cursor.range_count(), 1);
        assert!(!apply_one(&index, &mut cursor, &data(1, COVER_WINDOW - 1, 2)).expect("apply"));
    }

    // a pass that has seen nothing far enough along prunes nothing
    #[test]
    fn an_early_pass_prunes_nothing() {
        let index = index();
        let mut cursor = LogCursor::new();
        apply_one(&index, &mut cursor, &range(1, 5)).expect("apply");

        cursor.prune_covers(Lsn(7));

        assert_eq!(cursor.range_count(), 1);
    }

    // a reader past the ceiling says so rather than quietly dropping a cover
    #[test]
    fn too_many_covers_saturates() {
        let index = index();
        let mut cursor = LogCursor::new();

        for at in 0..=MAX_COVERS as u64 {
            apply_one(&index, &mut cursor, &range(1, COVER_WINDOW + at + 1)).expect("apply");
        }

        assert!(
            cursor.is_saturated(),
            "the reader is holding more than it will test"
        );
    }

    // a relocation repoints the key rather than being taken for a stale write
    #[test]
    fn a_relocation_repoints() {
        let index = index();
        let mut cursor = LogCursor::new();
        apply_one(&index, &mut cursor, &data(1, 5, 1)).expect("apply");

        let moved = WalkedRecord {
            flags: Flags::DATA.relocated(),
            loc: Loc::new(SegmentId(9), 4096, 100),
            ..data(1, 5, 9)
        };
        assert!(apply_one(&index, &mut cursor, &moved).expect("apply"));

        let entry = index.get(&key(1)).expect("read").expect("present");
        assert_eq!(entry.loc.segment, SegmentId(9));
        assert_eq!(entry.lsn, Lsn(5), "a relocation is the same version");
    }

    // a tombstone drops the key and an older one changes nothing
    #[test]
    fn tombstones_follow_their_order() {
        let index = index();
        let mut cursor = LogCursor::new();
        apply_one(&index, &mut cursor, &data(1, 5, 1)).expect("apply");

        let stale = WalkedRecord {
            flags: Flags::TOMBSTONE,
            ..data(1, 4, 1)
        };
        let fresh = WalkedRecord {
            flags: Flags::TOMBSTONE,
            ..data(1, 6, 1)
        };

        assert!(!apply_one(&index, &mut cursor, &stale).expect("apply"));
        assert!(index.contains(&key(1)).expect("read"));
        assert!(apply_one(&index, &mut cursor, &fresh).expect("apply"));
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
