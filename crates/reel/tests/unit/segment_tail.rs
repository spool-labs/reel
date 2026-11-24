use reel::lifetime::MarkRange;
use reel::record::record_check::{Checksum, RecordKey};
use reel::segment::block_entry::{KeyBound, KeyPrefix, SHARED_HEAD_BYTES, SharedHead};
use reel::little_endian::ByteReader;
use reel::segment::segment_tail::*;
use reel_core::column_id::ColumnId;
use reel_core::units::{ByteCount, DeathMark, FileOffset, RowCount, WritePlace};

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
        is_output: true,
        is_partial: false,
        is_unwalked: false,
        clears_damage: true,
        marks: MarkRange { lowest: DeathMark(40), highest: DeathMark(70) },
    }
}

fn inputs() -> TailInputs {
    TailInputs::of(&[SegmentNumber(5)])
}

fn column(id: u8) -> ColumnTail {
    ColumnTail {
        column: ColumnId(id),
        shared_length: 5,
        shared_sum: Checksum(6),
        shared_head: SharedHead::of_shared(b"share"),
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
    head().write(&inputs(), &mut cursor);
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
    let mut input = ByteReader::new(tail);

    assert_eq!(TailHead::read(&mut input), Some((head(), inputs())));
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

// a column's bounds hold its keys: the shared bytes, then the prefixes, and whatever the tail can't say stays open
#[test]
fn key_bounds() {
    let tail = |shared: &[u8], lowest: &[u8], highest: &[u8]| ColumnTail {
        shared_length: shared.len() as u16,
        shared_head: SharedHead::of_shared(shared),
        lowest: KeyPrefix::of_key(lowest, 0),
        highest: KeyPrefix::of_key(highest, 0),
        ..ColumnTail::default()
    };
    let (lowest, highest) = tail(b"tape", b"a", b"c").key_bounds();
    assert_eq!(lowest, KeyBound::of_key(b"tapea"));
    assert!(highest > KeyBound::of_key(b"tapec"));
    assert!(highest < KeyBound::of_key(b"taped"));

    let long = [7; SHARED_HEAD_BYTES + 2];
    let (lowest, highest) = tail(&long, b"a", b"c").key_bounds();
    assert_eq!(lowest, KeyBound::of_key(&long[..SHARED_HEAD_BYTES]));
    let mut above = [7; SHARED_HEAD_BYTES + 3];
    above[SHARED_HEAD_BYTES] = u8::MAX;
    assert!(highest >= KeyBound::of_key(&above));
}
