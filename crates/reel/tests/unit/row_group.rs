use reel::lifetime::MarkRange;
use reel::record::record_check::RecordKey;
use reel::row_kind::RowKind;
use reel::rows::row_group::*;
use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, DeathMark, FileOffset, RowCount, WritePlace};

const RECORD_KEY: RecordKey = RecordKey(11);
const CAPACITY: ByteCount = ByteCount(4_096);
const SYNCED: FileOffset = FileOffset(512);

fn row(key: &[u8], place: u64, kind: RowKind) -> RowEntry<'_> {
    RowEntry {
        column: ColumnId(4),
        key,
        place: WritePlace(place),
        offset: FileOffset(place * 100),
        length: ByteCount(50),
        kind,
    }
}

// a group reads back with its rows in order and its sync mark
#[test]
fn group_round() {
    let mut group = RowGroupBuffer::new(CAPACITY);
    assert!(group.push(&row(b"first", 1, RowKind::Value)));
    assert!(group.push(&row(b"", 2, RowKind::Grave)));
    assert!(group.push(&row(b"third", 3, RowKind::RangeDelete)));

    let bytes = group.finish(SYNCED, MarkRange::NONE, RECORD_KEY);
    let parsed = parse_group(bytes, RECORD_KEY).expect("whole group");
    let rows: Vec<RowEntry> = parsed.walk().collect();

    assert_eq!(parsed.row_count, RowCount(3));
    assert_eq!(parsed.synced_through, SYNCED);
    assert_eq!(parsed.total_length, ByteCount(bytes.len() as u64));
    assert_eq!(rows[0], row(b"first", 1, RowKind::Value));
    assert_eq!(rows[1], row(b"", 2, RowKind::Grave));
    assert_eq!(rows[2], row(b"third", 3, RowKind::RangeDelete));
}

// a flipped byte, a cut group and a foreign store's group are all refused
#[test]
fn torn_group() {
    let mut group = RowGroupBuffer::new(CAPACITY);
    assert!(group.push(&row(b"key", 1, RowKind::Value)));
    let mut bytes = group.finish(SYNCED, MarkRange::NONE, RECORD_KEY).to_vec();

    assert!(parse_group(&bytes[..bytes.len() - 1], RECORD_KEY).is_none());
    assert!(parse_group(&bytes, RecordKey(12)).is_none());
    bytes[GROUP_HEADER_BYTES] ^= 1;
    assert!(parse_group(&bytes, RECORD_KEY).is_none());
    assert!(parse_group(&[0; 64], RECORD_KEY).is_none());
}

// a group refuses a row past its capacity and takes rows again after a clear
#[test]
fn group_capacity() {
    // the third row has the largest offset, so a group sized for it holds the first and not a second
    let one = encoded_size(&row(b"", 3, RowKind::Value));
    let mut group = RowGroupBuffer::new(ByteCount((GROUP_OVERHEAD_BYTES + one) as u64));

    assert!(group.push(&row(b"", 1, RowKind::Value)));
    assert!(!group.push(&row(b"", 2, RowKind::Value)));

    group.clear();
    assert!(group.is_empty());
    assert!(group.push(&row(b"", 3, RowKind::Value)));
}

// a group keeps the marks so far, which is how an open segment finds its group after a crash
#[test]
fn group_keeps_marks() {
    let marks = MarkRange { lowest: DeathMark(40), highest: DeathMark(70) };
    let mut group = RowGroupBuffer::new(CAPACITY);
    assert!(group.push(&row(b"first", 1, RowKind::Value)));
    let bytes = group.finish(SYNCED, marks, RECORD_KEY);
    assert_eq!(parse_group(bytes, RECORD_KEY).expect("whole group").marks, marks);
}

// rows take their numbers as varints: small numbers take a byte or two, the largest still round trip, and a row never takes
// more than the room a claim holds for it
#[test]
fn rows_take_varints() {
    let key = b"key-000001";
    let small = RowEntry { column: ColumnId(1), key, place: WritePlace(300), offset: FileOffset(4_096), length: ByteCount(120), kind: RowKind::Value };
    let largest = RowEntry { place: WritePlace(u64::MAX), offset: FileOffset(u64::MAX), length: ByteCount(u64::MAX), ..small };
    assert!(encoded_size(&small) <= ROW_FIXED_BYTES + 1 + 2 + 2 + 1 + key.len());
    assert_eq!(encoded_size(&largest), row_size(key.len()) - (KEY_LENGTH_VARINT_BYTES - 1));
    for row in [small, largest] {
        assert!(encoded_size(&row) <= row_size(key.len()));
        let mut group = RowGroupBuffer::new(CAPACITY);
        assert!(group.push(&row));
        assert_eq!(group.row_bytes(), encoded_size(&row));
        let parsed: Vec<RowEntry> = group.walk().collect();
        assert_eq!(parsed, vec![row]);
    }
    for number in [0, 1, 127, 128, 16_383, 16_384, u64::MAX] {
        assert_eq!(varint_size(number), (number.max(1).ilog2() / 7 + 1) as usize, "{number}");
    }
}

// a walk over bytes that end inside a row's numbers stops before that row, so a piece cut in two carries it to the next
#[test]
fn walk_stops_inside_a_varint() {
    let row = RowEntry { column: ColumnId(1), key: b"key", place: WritePlace(1 << 40), offset: FileOffset(1 << 30), length: ByteCount(9), kind: RowKind::Value };
    let mut group = RowGroupBuffer::new(CAPACITY);
    assert!(group.push(&row) && group.push(&row));
    let rows = group.rows();
    let first = encoded_size(&row);
    for cut in first..2 * first {
        let mut walk = RowWalk::over(&rows[..cut]);
        assert_eq!(walk.next(), Some(row), "cut at {cut}");
        assert_eq!(walk.next(), None, "cut at {cut}");
        assert_eq!(walk.left(), cut - first);
    }
}
