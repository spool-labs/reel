use reel::record::record_check::{Checksum, RecordKey};
use reel::segment::block_entry::KeyPrefix;
use reel::segment::segment_tail::*;
use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, FileOffset, RowCount, WritePlace};

const RECORD_KEY: RecordKey = RecordKey(31);

fn head() -> TailHead {
    TailHead {
        segment: SegmentNumber(9),
        data_end: FileOffset(1_000),
        index_at: FileOffset(9_000),
        second_index_at: FileOffset(9_200),
        block_count: BlockCount(4),
        ranges_at: FileOffset(9_500),
        ranges_length: ByteCount(20),
        places: PlaceRange {
            lowest: WritePlace(3),
            highest: WritePlace(30),
        },
        place_base: WritePlace(3),
        index_sum: Checksum(8),
        ranges_sum: Checksum(9),
        column_count: 2,
        input_count: 1,
        is_output: true,
        is_grave_output: false,
        is_unwalked: false,
        inputs: [SegmentNumber(5); MOST_MERGE_SEGMENTS],
    }
}

fn column(id: u8) -> ColumnTail {
    ColumnTail {
        column: ColumnId(id),
        shared_length: 5,
        shared_sum: Checksum(6),
        lowest: KeyPrefix::of_key(b"low", 0),
        highest: KeyPrefix::of_key(b"high", 0),
        first_block: 1,
        block_count: BlockCount(2),
        row_count: RowCount(100),
        grave_count: RowCount(7),
    }
}

fn built() -> Vec<u8> {
    let mut bytes = vec![0; MAX_TAIL_BYTES];
    let mut cursor = TailCursor::new(&mut bytes);
    head().write(&mut cursor);
    column(1).write(&mut cursor);
    column(2).write(&mut cursor);
    let body = TAIL_FIXED_BYTES + 2 * COLUMN_TAIL_BYTES;
    let total = write_trailer(&mut bytes, body, RECORD_KEY);
    bytes.truncate(total);
    bytes
}

// a tail reads back field for field from the end of its file
#[test]
fn tail_round() {
    let mut file = vec![7; 500];
    file.extend_from_slice(&built());

    let tail = find_tail(&file, RECORD_KEY).expect("whole tail");
    let mut input = TailReader::new(tail);

    assert_eq!(TailHead::read(&mut input), Some(head()));
    assert_eq!(ColumnTail::read(&mut input), Some(column(1)));
    assert_eq!(ColumnTail::read(&mut input), Some(column(2)));
}

// a flipped byte, a cut tail, another store's tail and plain data all read as not sealed
#[test]
fn no_tail() {
    let mut bytes = built();

    assert!(find_tail(&bytes[..bytes.len() - 1], RECORD_KEY).is_none());
    assert!(find_tail(&bytes, RecordKey(32)).is_none());
    assert!(find_tail(&[0; 4_096], RECORD_KEY).is_none());
    bytes[20] ^= 1;
    assert!(find_tail(&bytes, RECORD_KEY).is_none());
}

// a column whose blocks run past the index doesn't fit
#[test]
fn column_range() {
    let column = column(1);
    let lines = column.first_block as usize + column.block_count.0 as usize;

    assert!(columns_fit(&[column], lines));
    assert!(!columns_fit(&[column], lines - 1));
}
