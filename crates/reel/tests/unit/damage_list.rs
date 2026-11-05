use reel::segment::damage_list::*;
use reel::segment::segment_tail::SegmentNumber;
use reel_api::column_id::ColumnId;
use reel_api::units::WritePlace;

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

    list.add(range(b"c", Some(b"p"), 5));
    list.add(range(b"m", None, 9));

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
    list.add(range(b"c", Some(b"p"), 5));

    list.clear(COLUMN, b"f", Some(b"k"));
    assert_eq!(list.len(), 2);
    assert_eq!(list.covering(COLUMN, b"d"), Some(WritePlace(5)));
    assert_eq!(list.covering(COLUMN, b"g"), None);
    assert_eq!(list.covering(COLUMN, b"m"), Some(WritePlace(5)));

    list.clear(COLUMN, b"a", None);
    assert!(list.is_empty());
}

// a span clear of a whole column range leaves the part past its end
#[test]
fn open_ended() {
    let list = DamageList::new();
    list.add(range(b"", None, 5));

    list.clear(COLUMN, b"", Some(b"m"));

    assert_eq!(list.covering(COLUMN, b"a"), None);
    assert_eq!(list.covering(COLUMN, b"z"), Some(WritePlace(5)));
}
