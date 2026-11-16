use reel::segment::damage_list::*;
use reel::segment::segment_tail::SegmentNumber;
use reel_core::column_id::ColumnId;
use reel_core::units::WritePlace;

const COLUMN: ColumnId = ColumnId(1);

fn range(start: &[u8], end: Option<&[u8]>, place: u64) -> DamagedRange {
    DamagedRange {
        column: COLUMN,
        segment: SegmentNumber(9),
        highest_place: WritePlace(place),
        start: start.to_vec(),
        end: end.map(<[u8]>::to_vec),
    }
}

// a range holds keys from its start up to its end, in its own column, and the highest place wins
#[test]
fn range_cover() {
    let list = DamageList::new();
    assert_eq!(list.covering(COLUMN, b"m"), None);

    list.add(&range(b"c", Some(b"p"), 5));
    list.add(&range(b"m", None, 9));

    assert_eq!(list.covering(COLUMN, b"b"), None);
    assert_eq!(list.covering(COLUMN, b"c"), Some(WritePlace(5)));
    assert_eq!(list.covering(COLUMN, b"m"), Some(WritePlace(9)));
    assert_eq!(list.covering(COLUMN, b"z"), Some(WritePlace(9)));
    assert_eq!(list.covering(ColumnId(2), b"m"), None);
}

// clearing the middle leaves both ends, and clearing the rest leaves nothing
#[test]
fn range_clear() {
    let list = DamageList::new();
    list.add(&range(b"c", Some(b"p"), 5));

    list.clear(COLUMN, b"f", Some(b"k")).expect("clear");
    assert_eq!(list.len(), 2);
    assert_eq!(list.covering(COLUMN, b"d"), Some(WritePlace(5)));
    assert_eq!(list.covering(COLUMN, b"g"), None);
    assert_eq!(list.covering(COLUMN, b"m"), Some(WritePlace(5)));

    list.clear(COLUMN, b"a", None).expect("clear");
    assert!(list.is_empty());
}

// a span clear of a whole column range leaves the part past its end
#[test]
fn open_ended() {
    let list = DamageList::new();
    list.add(&range(b"", None, 5));

    list.clear(COLUMN, b"", Some(b"m")).expect("clear");

    assert_eq!(list.covering(COLUMN, b"a"), None);
    assert_eq!(list.covering(COLUMN, b"z"), Some(WritePlace(5)));
}

fn numbered(number: u64, column: ColumnId, segment: u64) -> DamagedRange {
    DamagedRange {
        column,
        segment: SegmentNumber(segment),
        highest_place: WritePlace(5),
        start: format!("{number:06}").into_bytes(),
        end: Some(format!("{number:06}z").into_bytes()),
    }
}

fn full_list() -> DamageList {
    let list = DamageList::new();
    for number in 0..MOST_DAMAGED_SPANS as u64 {
        list.add(&numbered(number, COLUMN, 9));
    }
    assert_eq!(list.len(), MOST_DAMAGED_SPANS);
    list
}

// an add on a full pool widens the segment's spans in that column to the whole column
#[test]
fn full_add() {
    let list = full_list();
    assert_eq!(list.covering(COLUMN, b"zzz"), None);

    list.add(&numbered(999_999, COLUMN, 9));

    assert_eq!(list.len(), 1);
    assert_eq!(list.covering(COLUMN, b"zzz"), Some(WritePlace(5)));
    assert_eq!(list.covering(COLUMN, b""), Some(WritePlace(5)));
}

// an add on a full pool for a column with no span frees one, and its keys are covered
#[test]
fn full_other() {
    let list = full_list();

    list.add(&numbered(1, ColumnId(2), 8));

    assert_eq!(list.covering(ColumnId(2), b"000001"), Some(WritePlace(5)));
    assert_eq!(list.covering(COLUMN, b"000001"), Some(WritePlace(5)));
}

// a clear that would split a span on a full pool says so and clears nothing
#[test]
fn full_split() {
    let list = full_list();
    let spans = list.len();

    let cleared = list.clear(COLUMN, b"000005a", Some(b"000005b"));

    assert_eq!(cleared, Err(PoolFull));
    assert_eq!(list.len(), spans);
    assert_eq!(list.covering(COLUMN, b"000005a"), Some(WritePlace(5)));
}

// a clear of a whole span or of a span's start needs no new span, so it works on a full pool
#[test]
fn full_clear() {
    let list = full_list();

    list.clear(COLUMN, b"000007", Some(b"000007z")).expect("clear");
    assert_eq!(list.covering(COLUMN, b"000007a"), None);

    let list = full_list();
    list.clear(COLUMN, b"000007", Some(b"000007m")).expect("clear");
    assert_eq!(list.covering(COLUMN, b"000007a"), None);
    assert_eq!(list.covering(COLUMN, b"000007n"), Some(WritePlace(5)));
}

// a spare copy brings the spans back
#[test]
fn spare_copy() {
    let list = full_list();
    list.keep_spare();
    list.clear(COLUMN, b"", None).expect("clear");
    assert!(list.is_empty());

    list.restore_spare();

    assert_eq!(list.len(), MOST_DAMAGED_SPANS);
}

// dropping a segment's spans keeps the cover a widened span gave another segment, since open adds its whole columns back
#[test]
fn drop_widened() {
    let list = full_list();
    list.add(&numbered(1, COLUMN, 8));
    assert_eq!(list.covering(COLUMN, b"zzz"), Some(WritePlace(5)));

    list.drop_segment(SegmentNumber(9));
    list.add(&range(b"", None, 7));

    assert_eq!(list.covering(COLUMN, b"000001"), Some(WritePlace(7)));
    assert_eq!(list.covering(COLUMN, b"zzz"), Some(WritePlace(7)));
}
