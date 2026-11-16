use reel::segment::range_covers::*;
use reel::segment::segment_tail::SegmentNumber;
use reel_core::column_id::ColumnId;
use reel_core::units::WritePlace;

const COLUMN: ColumnId = ColumnId(1);
const KEY_BYTES: usize = 16;
const NESTED: usize = 2_000;

fn cover<'key>(start: &'key [u8], end: &'key [u8], place: u64) -> Cover<'key> {
    Cover {
        column: COLUMN,
        start,
        end,
        place: WritePlace(place),
        segment: SegmentNumber(1),
    }
}

fn number(value: usize) -> Vec<u8> {
    format!("{value:08}").into_bytes()
}

// an older cover that lands after an unpublished newer one stays, so a reader between the two still finds it
#[test]
fn older_lands_late() {
    let covers = RangeCovers::new(KEY_BYTES);
    assert!(covers.hold(2));

    covers.add_held(&cover(b"a", b"zz", 12));
    covers.add_held(&cover(b"a", b"z", 10));

    assert_eq!(covers.newest_over(COLUMN, b"m", WritePlace(10)), Some(WritePlace(10)));
    assert_eq!(covers.newest_over(COLUMN, b"m", WritePlace(12)), Some(WritePlace(12)));
    covers.drop_inside(&cover(b"a", b"zz", 12));
    assert_eq!(covers.len(), 1);
}

// a cover found twice at open takes one slot
#[test]
fn found_twice() {
    let covers = RangeCovers::new(KEY_BYTES);

    assert!(covers.add(&cover(b"a", b"b", 5), false));
    assert!(covers.add(&cover(b"a", b"b", 5), false));

    assert_eq!(covers.len(), 1);
}

// keys past the arena's width are refused, whatever a rotted footer says
#[test]
fn wide_keys() {
    let covers = RangeCovers::new(KEY_BYTES);
    let wide = [b'k'; KEY_BYTES + 1];

    assert!(!covers.add(&cover(&wide, b"z", 5), false));

    assert!(covers.is_empty());
}

// keys that are prefixes of each other sort apart, and a range ends exactly at its end
#[test]
fn prefix_keys() {
    let covers = RangeCovers::new(KEY_BYTES);
    assert!(covers.add(&cover(b"ab", b"abc", 5), false));

    let point = WritePlace(9);
    assert_eq!(covers.newest_over(COLUMN, b"a", point), None);
    assert_eq!(covers.newest_over(COLUMN, b"ab", point), Some(WritePlace(5)));
    assert_eq!(covers.newest_over(COLUMN, b"ab\0", point), Some(WritePlace(5)));
    assert_eq!(covers.newest_over(COLUMN, b"abc", point), None);
}

// the walk over covers nested inside an old wide one: the worst case the search has
#[test]
fn nested_cost() {
    let covers = RangeCovers::new(KEY_BYTES);
    let (first, last) = (number(0), number(100_000));
    assert!(covers.add(&cover(&first, &last, 1), false));
    for inner in 0..NESTED {
        let (start, end) = (number(10 * inner + 1), number(10 * inner + 2));
        assert!(covers.add(&cover(&start, &end, inner as u64 + 2), false));
    }

    let steps = covers.search_steps(COLUMN, &number(10 * NESTED));

    assert!(steps <= MOST_SEARCH_STEPS, "{steps} steps");
    assert_eq!(covers.newest_over(COLUMN, &number(10 * NESTED), WritePlace(u64::MAX)), Some(WritePlace(1)));
}
