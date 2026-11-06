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
    let mut group = RowGroupBuffer::new(ByteCount(GROUP_OVERHEAD_BYTES as u64 + 32));

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
