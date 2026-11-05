use reel::segment::segment_tail::{PlaceRange, SegmentNumber};
use reel::record::record_check::Checksum;
use reel::segment::summary::*;
use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, FileOffset, RowCount, WritePlace};

const ROOM: ByteCount = ByteCount(4_096);

fn facts(number: u64) -> SegmentFacts {
    SegmentFacts {
        has_covers: false,
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
        damage_place: WritePlace(0),
        block_count: 7,
        walked_blocks: 3,
        is_backward: false,
    }
}

fn column() -> ColumnFacts<'static> {
    ColumnFacts {
        column: ColumnId(2),
        lowest: b"apple",
        bound: b"pear",
        row_count: RowCount(10),
        grave_count: RowCount(1),
        dead_rows: RowCount(0),
        dead_graves: RowCount(0),
        live_bytes: ByteCount(50),
        dead_bytes: ByteCount(0),
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

// one walk finds where every row starts
#[test]
fn row_offsets() {
    let rows = filled();

    let offsets = rows.offsets();

    assert_eq!(offsets.len(), 2);
    assert_eq!(rows.row_at(offsets[&SegmentNumber(2)]).expect("row").facts, facts(2));
}

// a row copies across as it stands
#[test]
fn row_copy() {
    let rows = filled();
    let offsets = rows.offsets();
    let mut copied = SummaryRows::new(ROOM);

    assert!(copied.copy_row(&rows, offsets[&SegmentNumber(1)]));

    assert_eq!(copied.get(SegmentNumber(1)).expect("row").facts, facts(1));
    assert!(copied.get(SegmentNumber(2)).is_none());
}

// rot in a row's length costs that row alone: the walk finds the next head
#[test]
fn head_rot() {
    let rows = filled();
    let mut bytes = rows.bytes().to_vec();

    bytes[5] ^= 1;
    let (length, first) = read_row(&bytes).expect("a row follows");
    let (_, second) = read_row(&bytes[length..]).expect("row");

    assert!(first.is_none());
    assert_eq!(second.expect("row").facts, facts(2));
}

// a short key's bound sits right above it, and a long key's above every key sharing its first bytes
#[test]
fn key_bound() {
    let mut bound = Vec::new();
    put_bound(b"pear", &mut bound);
    assert_eq!(bound, b"pear\0");

    let long = [7; 2 * SUMMARY_KEY_BYTES];
    bound.clear();
    put_bound(&long, &mut bound);
    let mut above = long[..SUMMARY_KEY_BYTES].to_vec();
    above[SUMMARY_KEY_BYTES - 1] += 1;
    assert_eq!(bound, above);
    assert!(long.as_slice() < bound.as_slice());
}

// a key of top bytes has nothing above it, so the range runs to the column's end
#[test]
fn no_bound() {
    let mut bound = Vec::new();

    put_bound(&[u8::MAX; SUMMARY_KEY_BYTES], &mut bound);

    assert!(bound.is_empty());
}

// a lowest key keeps its first bytes, which sort at or below it
#[test]
fn key_prefix() {
    let long = [7; 2 * SUMMARY_KEY_BYTES];
    let mut lowest = Vec::new();

    put_lowest(&long, &mut lowest);

    assert_eq!(lowest.len(), SUMMARY_KEY_BYTES);
    assert!(lowest.as_slice() <= long.as_slice());
}

// a row that must stay in rows with no room takes the room it needs, from nothing
#[test]
fn row_grows() {
    let mut rows = SummaryRows::new(ByteCount(0));

    let dropped = rows.push_kept(&facts(1), [column()].into_iter(), [span()].into_iter());

    assert_eq!(dropped, 0);
    assert_eq!(rows.get(SegmentNumber(1)).expect("row").facts, facts(1));
}

// spans rewritten in place leave the row and the rows after it whole
#[test]
fn span_rewrite() {
    let mut rows = filled();
    let longer = SpanFacts {
        column: ColumnId(2),
        start: b"a long start key",
        end: Some(b"a long end key"),
    };

    assert_eq!(rows.rewrite_spans(SegmentNumber(1), [longer, span()].into_iter()), Some(0));

    let row = rows.get(SegmentNumber(1)).expect("row");
    let mut cursor = row.cursor();
    assert_eq!(row.span_count, 2);
    assert_eq!(cursor.column(), Some(column()));
    assert_eq!(cursor.span(), Some(longer));
    assert_eq!(cursor.span(), Some(span()));
    assert_eq!(rows.get(SegmentNumber(2)).expect("row").facts, facts(2));

    assert_eq!(rows.rewrite_spans(SegmentNumber(1), [].into_iter()), Some(0));
    assert_eq!(rows.get(SegmentNumber(1)).expect("row").span_count, 0);
    assert_eq!(rows.get(SegmentNumber(2)).expect("row").facts, facts(2));
    assert_eq!(rows.rewrite_spans(SegmentNumber(3), [].into_iter()), None);
}

// a walk step lands its counts and its position together, and leaves other rows alone
#[test]
fn walk_counts() {
    let mut rows = SummaryRows::new(ROOM);
    for number in [SegmentNumber(1), SegmentNumber(2), SegmentNumber(3)] {
        assert!(rows.push(&facts(number.0), [column()].into_iter(), [].into_iter()));
    }
    let count = DeadCount {
        segment: SegmentNumber(1),
        column: ColumnId(2),
        block: 3,
        rows: 2,
        graves: 1,
        bytes: ByteCount(30),
        kept_bytes: ByteCount(0),
    };

    rows.apply_walk(SegmentNumber(3), 5, &[count]);

    assert!(!rows.has_bad_row());
    let counted = rows.get(SegmentNumber(1)).expect("a row");
    let facts_of = counted.cursor().column().expect("a column");
    assert_eq!((facts_of.dead_rows, facts_of.dead_graves), (RowCount(2), RowCount(1)));
    assert_eq!((facts_of.live_bytes, facts_of.dead_bytes), (ByteCount(20), ByteCount(30)));
    assert_eq!(counted.facts.walked_blocks, 3);
    assert_eq!(counted.dead_blocks().collect::<Vec<_>>(), [(3, 2)]);
    assert_eq!(rows.get(SegmentNumber(3)).expect("a row").facts.walked_blocks, 5);
    let other = rows.get(SegmentNumber(2)).expect("a row");
    assert_eq!(other.cursor().column().expect("a column").dead_rows, RowCount(0));
}

// a walk step finds its rows through the lookup after rows moved, and a block past the row's own is refused
#[test]
fn row_lookup() {
    let mut rows = SummaryRows::new(ROOM);
    rows.make_index(4);
    for number in 1..=3 {
        assert!(rows.push(&facts(number), [column()].into_iter(), [].into_iter()));
    }
    rows.remove(SegmentNumber(1));
    assert!(rows.get(SegmentNumber(1)).is_none());
    assert!(rows.rewrite_spans(SegmentNumber(2), [span()].into_iter()).is_some());
    let count = DeadCount {
        segment: SegmentNumber(3),
        column: ColumnId(2),
        block: 1,
        rows: 1,
        graves: 0,
        bytes: ByteCount(5),
        kept_bytes: ByteCount(0),
    };

    rows.apply_walk(SegmentNumber(3), 4, &[count]);

    assert!(!rows.has_bad_row());
    let counted = rows.get(SegmentNumber(3)).expect("a row");
    assert_eq!(counted.facts.walked_blocks, 4);
    assert_eq!(counted.dead_blocks().collect::<Vec<_>>(), [(1, 1)]);
    assert_eq!(rows.get(SegmentNumber(2)).expect("a row").span_count, 1);
}

// a lookup too small for the rows gives way to walking them
#[test]
fn lookup_full() {
    let mut rows = SummaryRows::new(ROOM);
    rows.make_index(1);
    for number in 1..=5 {
        assert!(rows.push(&facts(number), [column()].into_iter(), [].into_iter()));
    }

    assert_eq!(rows.get(SegmentNumber(5)).expect("a row").facts.segment, SegmentNumber(5));
}
