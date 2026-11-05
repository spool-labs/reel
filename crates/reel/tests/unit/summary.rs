use reel::segment::segment_tail::{PlaceRange, SegmentNumber};
use reel::record::record_check::Checksum;
use reel::segment::summary::*;
use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, FileOffset, RowCount, WritePlace};

const ROOM: ByteCount = ByteCount(4_096);

fn facts(number: u64) -> SegmentFacts {
    SegmentFacts {
        segment: SegmentNumber(number),
        file_length: ByteCount(1_000),
        data_end: FileOffset(100),
        row_block_end: FileOffset(800),
        seal_mark: Checksum(7),
        places: PlaceRange {
            lowest: WritePlace(3),
            highest: WritePlace(9),
        },
        is_damaged: false,
    }
}

fn column() -> ColumnFacts<'static> {
    ColumnFacts {
        column: ColumnId(2),
        lowest: b"apple",
        highest: b"pear",
        row_count: RowCount(10),
        grave_count: RowCount(1),
    }
}

fn span() -> SpanFacts<'static> {
    SpanFacts {
        column: ColumnId(2),
        start: b"cherry",
        end: None,
    }
}

fn filled() -> SummaryRows {
    let mut rows = SummaryRows::new(ROOM);
    assert!(rows.push(&facts(1), [column()].into_iter(), [span()].into_iter()));
    assert!(rows.push(&facts(2), [column()].into_iter(), [].into_iter()));
    rows
}

// a row reads back field for field
#[test]
fn row_round() {
    let rows = filled();

    let row = rows.get(SegmentNumber(1)).expect("row");
    let mut cursor = row.cursor();

    assert_eq!(row.facts, facts(1));
    assert_eq!(cursor.column(), Some(column()));
    assert_eq!(cursor.span(), Some(span()));
    assert!(rows.get(SegmentNumber(3)).is_none());
}

// one rotted row is lost alone
#[test]
fn row_rot() {
    let rows = filled();
    let mut bytes = rows.bytes().to_vec();

    bytes[20] ^= 1;
    let (length, first) = read_row(&bytes).expect("row");
    let (_, second) = read_row(&bytes[length..]).expect("row");

    assert!(first.is_none());
    assert_eq!(second.expect("row").facts, facts(2));
}

// a row goes without disturbing the one after it
#[test]
fn row_remove() {
    let mut rows = filled();

    rows.remove(SegmentNumber(1));

    assert!(rows.get(SegmentNumber(1)).is_none());
    assert_eq!(rows.get(SegmentNumber(2)).expect("row").facts, facts(2));
}

// rows past the room are refused
#[test]
fn row_room() {
    let mut rows = SummaryRows::new(ByteCount(16));

    assert!(!rows.push(&facts(1), [column()].into_iter(), [].into_iter()));
}

// a row fails the test and leaves, and the others stay whole
#[test]
fn row_keep() {
    let mut rows = filled();

    rows.keep_where(|row| row.facts.segment == SegmentNumber(2));

    assert!(rows.get(SegmentNumber(1)).is_none());
    assert_eq!(rows.get(SegmentNumber(2)).expect("row").facts, facts(2));
}
