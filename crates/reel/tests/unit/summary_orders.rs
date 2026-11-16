use std::iter;

use reel::lifetime::MarkRange;
use reel::record::record_check::Checksum;
use reel::segment::segment_tail::{PlaceRange, SegmentNumber};
use reel::segment::summary::{ColumnFacts, DeadCount, SegmentFacts, SummaryRows};
use reel::segment::summary_orders::{COVERS_LIST, SHARE_SCALE, WalkList, reclaim_share, share_bucket};
use reel_core::column_id::ColumnId;
use reel_core::units::{ByteCount, DeathMark, FileOffset, RowCount, WritePlace};

const COLUMN: ColumnId = ColumnId(1);
const BLOCKS: u32 = 4;
const ROOM: usize = 16;

fn facts(number: u64, walked_blocks: u32) -> SegmentFacts {
    SegmentFacts {
        has_covers: false,
        segment: SegmentNumber(number),
        file_length: ByteCount(1_000),
        data_end: FileOffset(800),
        row_block_end: FileOffset(900),
        seal_mark: Checksum(7),
        places: PlaceRange { lowest: WritePlace(number * 10), highest: WritePlace(number * 10 + 9) },
        is_damaged: false,
        damage_place: WritePlace(0),
        block_count: BLOCKS,
        walked_blocks,
        is_backward: false,
        has_lost_rows: false,
        marks: MarkRange { lowest: DeathMark(1), highest: DeathMark(1) },
        most_block_rows: 10,
    }
}

// a column of 100 rows of 10 bytes each, a number of them dead
fn column(dead: u64) -> ColumnFacts<'static> {
    ColumnFacts {
        column: COLUMN,
        lowest: b"a",
        bound: b"z",
        row_count: RowCount(100),
        grave_count: RowCount(0),
        dead_rows: RowCount(dead),
        dead_graves: RowCount(0),
        live_bytes: ByteCount(10 * (100 - dead)),
        dead_bytes: ByteCount(10 * dead),
    }
}

fn rows_with(added: &[(SegmentFacts, u64)]) -> SummaryRows {
    let mut rows = SummaryRows::new(ByteCount(64 * 1024));
    rows.make_index(ROOM);
    for (facts, dead) in added {
        assert!(rows.push(facts, iter::once(column(*dead)), iter::empty()));
    }
    rows
}

fn walk_list(rows: &SummaryRows, list: WalkList) -> Vec<u64> {
    rows.orders().walk_list(list).map(|segment| segment.0).collect()
}

fn share_list(rows: &SummaryRows, list: usize) -> Vec<u64> {
    rows.orders().share_list(list).map(|segment| segment.0).collect()
}

fn bucket_of(rows: &SummaryRows, number: u64) -> usize {
    share_bucket(reclaim_share(&rows.get(SegmentNumber(number)).expect("a row")))
}

// each row lands on the lists its walk and counts call for
#[test]
fn rows_land_on_their_lists() {
    let mut backward = facts(4, 0);
    backward.is_backward = true;
    let mut damaged = facts(5, BLOCKS);
    damaged.is_damaged = true;
    let mut covered = facts(6, BLOCKS);
    covered.has_covers = true;
    let rows = rows_with(&[(facts(1, 0), 0), (facts(2, BLOCKS), 90), (facts(3, BLOCKS), 100), (backward, 0), (damaged, 90), (covered, 10)]);

    assert_eq!(walk_list(&rows, WalkList::Unwalked), [1]);
    assert_eq!(walk_list(&rows, WalkList::Backward), [4]);
    assert_eq!(walk_list(&rows, WalkList::Dead), [3]);
    assert_eq!(rows.orders().walk_count(WalkList::Unwalked), 1);
    assert_eq!(share_list(&rows, bucket_of(&rows, 2)), [2]);
    assert_eq!(share_list(&rows, bucket_of(&rows, 3)), [3]);
    assert_eq!(share_list(&rows, COVERS_LIST), [6]);
    let listed: usize = (0..=COVERS_LIST).map(|list| share_list(&rows, list).len()).sum();
    assert_eq!(listed, 3, "an unwalked or damaged row sat on a share list");
    assert_eq!(rows.orders().present().iter().map(|segment| segment.0).collect::<Vec<_>>(), [1, 2, 3, 4, 5, 6]);
}

// a walk that finishes moves a row off the unwalked list and into the bucket its counts reach, and more dead moves it up
#[test]
fn counts_move_a_row() {
    let mut rows = rows_with(&[(facts(1, 0), 0)]);
    let count = |rows: u32, bytes: u64| DeadCount { segment: SegmentNumber(1), column: COLUMN, block: 0, rows, bytes: ByteCount(bytes), ..DeadCount::default() };
    rows.apply_walk(SegmentNumber(1), BLOCKS, &[count(50, 500)]);
    assert!(walk_list(&rows, WalkList::Unwalked).is_empty());
    let half = bucket_of(&rows, 1);
    assert_eq!(share_list(&rows, half), [1]);
    rows.apply_counts(SegmentNumber(1), &[count(50, 500)], None);
    let whole = bucket_of(&rows, 1);
    assert!(whole > half);
    assert_eq!(share_list(&rows, whole), [1]);
    assert!(share_list(&rows, half).is_empty());
    assert_eq!(walk_list(&rows, WalkList::Dead), [1]);
    assert_eq!(whole, share_bucket(SHARE_SCALE));
}

// a removed row leaves every list and the order, and the rows past it still read
#[test]
fn removed_rows_leave() {
    let mut rows = rows_with(&[(facts(1, 0), 0), (facts(2, BLOCKS), 90), (facts(3, 0), 0)]);
    let bucket = bucket_of(&rows, 2);
    rows.remove(SegmentNumber(2));
    rows.remove(SegmentNumber(1));
    assert!(share_list(&rows, bucket).is_empty());
    assert_eq!(walk_list(&rows, WalkList::Unwalked), [3]);
    assert_eq!(rows.orders().present(), [SegmentNumber(3)]);
    assert_eq!(rows.get(SegmentNumber(3)).expect("a row").facts.segment, SegmentNumber(3));
    assert!(rows.get(SegmentNumber(2)).is_none());
}

// rows added out of order still walk in segment order, and the order answers what lies past and below a segment
#[test]
fn walk_lists_keep_segment_order() {
    let rows = rows_with(&[(facts(5, 0), 0), (facts(2, 0), 0), (facts(9, 0), 0), (facts(7, 0), 0)]);
    assert_eq!(walk_list(&rows, WalkList::Unwalked), [2, 5, 7, 9]);
    assert_eq!(rows.orders().after(SegmentNumber(5)), [SegmentNumber(7), SegmentNumber(9)]);
    assert_eq!(rows.orders().below(SegmentNumber(7)), [SegmentNumber(2), SegmentNumber(5)]);
}

// a picked row moves to its bucket's end, so the next pick there starts with another
#[test]
fn rotation_moves_a_row_last() {
    let mut rows = rows_with(&[(facts(1, BLOCKS), 90), (facts(2, BLOCKS), 90), (facts(3, BLOCKS), 90)]);
    let bucket = bucket_of(&rows, 1);
    assert_eq!(share_list(&rows, bucket), [1, 2, 3]);
    rows.rotate(SegmentNumber(1));
    assert_eq!(share_list(&rows, bucket), [2, 3, 1]);
}

// rows past the room the orders were sized for are counted as left out, which the report shows
#[test]
fn full_orders_say_so() {
    let mut rows = SummaryRows::new(ByteCount(64 * 1024));
    rows.make_index(2);
    for number in 1..=4 {
        assert!(rows.push(&facts(number, 0), iter::once(column(0)), iter::empty()));
    }
    assert_eq!(rows.orders().left_out(), 2);
    assert_eq!(rows.orders().len(), 2);
}
