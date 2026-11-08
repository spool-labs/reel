use reel::record::record_check::checksum;
use reel::segment::block_entry::{KeyBound, KeyPrefix};
use reel::segment::column_directory::*;
use reel::segment::footer_writer::shared_prefix;
use reel::segment::segment_tail::SegmentNumber;
use reel_api::units::WritePlace;

const CAPACITY: usize = 64;
const MANY: u64 = 1_000;

fn entry(number: u64, lowest: &[u8], highest: &[u8]) -> Listing {
    let shared = shared_prefix(lowest, highest);
    Listing {
        entry: DirectoryEntry {
            shared_length: shared as u16,
            shared_sum: checksum(&lowest[..shared]),
            lowest: KeyPrefix::of_key(lowest, shared),
            highest: KeyPrefix::of_key(highest, shared),
            reach: 0,
            segment: SegmentNumber(number),
            newest_place: WritePlace(number),
        },
        range: RangeEntry {
            lowest: KeyBound::of_key(lowest),
            highest: KeyBound::of_key(highest),
            reach: 0,
            segment: SegmentNumber(number),
        },
    }
}

fn found(directory: &ColumnDirectory, key: &[u8]) -> Vec<u64> {
    let mut out = Vec::with_capacity(CAPACITY);
    let page = directory.candidates(key, 0, &mut out);
    assert!(!page.is_cut);
    let mut numbers = Vec::new();
    for candidate in out {
        numbers.push(candidate.segment.0);
    }
    numbers.sort_unstable();
    numbers
}

// ordered ranges give one segment, overlaps give each of them, and a gap gives none
#[test]
fn range_search() {
    let directory = ColumnDirectory::new(CAPACITY);
    assert!(directory.replace(Some(&entry(1, b"a", b"c")), &[]));
    assert!(directory.replace(Some(&entry(2, b"d", b"f")), &[]));
    assert!(directory.replace(Some(&entry(3, b"b", b"e")), &[]));
    assert!(directory.replace(Some(&entry(4, b"m", b"p")), &[]));

    assert_eq!(found(&directory, b"a"), vec![1]);
    assert_eq!(found(&directory, b"c"), vec![1, 3]);
    assert_eq!(found(&directory, b"e"), vec![2, 3]);
    assert_eq!(found(&directory, b"g"), Vec::<u64>::new());
    assert_eq!(found(&directory, b"n"), vec![4]);
}

// a wide old segment stays in reach of keys past the narrow ones after it
#[test]
fn wide_segment() {
    let directory = ColumnDirectory::new(CAPACITY);
    assert!(directory.replace(Some(&entry(1, b"a", b"z")), &[]));
    assert!(directory.replace(Some(&entry(2, b"b", b"c")), &[]));
    assert!(directory.replace(Some(&entry(3, b"d", b"e")), &[]));

    assert_eq!(found(&directory, b"m"), vec![1]);
}

// keys sharing a long prefix still rule out the segments of other ranges
#[test]
fn long_prefix() {
    let directory = ColumnDirectory::new(CAPACITY);
    assert!(directory.replace(Some(&entry(1, b"address-of-one-tape-0001", b"address-of-one-tape-0100")), &[]));
    assert!(directory.replace(Some(&entry(2, b"address-of-one-tape-0101", b"address-of-one-tape-0200")), &[]));
    assert!(directory.replace(Some(&entry(3, b"address-of-one-tape-0201", b"address-of-one-tape-0300")), &[]));

    assert_eq!(found(&directory, b"address-of-one-tape-0150"), vec![2]);
    assert_eq!(found(&directory, b"address-of-one-tape-0250"), vec![3]);
    assert_eq!(found(&directory, b"address-of-another-0150"), Vec::<u64>::new());
}

// a merge swaps its inputs for its output in one publish
#[test]
fn merge_swap() {
    let directory = ColumnDirectory::new(CAPACITY);
    assert!(directory.replace(Some(&entry(1, b"a", b"c")), &[]));
    assert!(directory.replace(Some(&entry(2, b"b", b"d")), &[]));

    assert!(directory.replace(Some(&entry(3, b"a", b"d")), &[SegmentNumber(1), SegmentNumber(2)]));

    assert_eq!(found(&directory, b"b"), vec![3]);
}

// a small buffer gets every overlapping segment page by page
#[test]
fn cut_pages() {
    let directory = ColumnDirectory::new(CAPACITY);
    for number in 0..10 {
        assert!(directory.replace(Some(&entry(number, b"a", b"z")), &[]));
    }

    let mut out = Vec::with_capacity(4);
    let mut numbers = Vec::new();
    let mut skip = 0;
    loop {
        let page = directory.candidates(b"m", skip, &mut out);
        for candidate in &out {
            numbers.push(candidate.segment.0);
        }
        skip += out.len();
        if !page.is_cut {
            break;
        }
    }

    numbers.sort_unstable();
    assert_eq!(numbers, (0..10).collect::<Vec<u64>>());
}

// a wide segment among many narrow ones finds just the two that hold a key
#[test]
fn wide_among_many() {
    let directory = ColumnDirectory::new(MANY as usize + 1);
    assert!(directory.replace(Some(&entry(MANY, b"a0000", b"z9999")), &[]));
    for number in 0..MANY {
        let lowest = format!("k{number:04}0");
        let highest = format!("k{number:04}9");
        assert!(directory.replace(Some(&entry(number, lowest.as_bytes(), highest.as_bytes())), &[]));
    }

    let key = b"k05005";
    let narrow = found(&directory, key);

    assert_eq!(narrow, vec![500, MANY]);
}

// a full directory refuses a segment and keeps its list
#[test]
fn full_directory() {
    let directory = ColumnDirectory::new(1);

    assert!(directory.replace(Some(&entry(1, b"a", b"c")), &[]));
    assert!(!directory.replace(Some(&entry(2, b"d", b"f")), &[]));

    assert_eq!(found(&directory, b"b"), vec![1]);
}

fn meeting(directory: &ColumnDirectory, low: &[u8], high: &[u8]) -> Vec<u64> {
    let mut out = Vec::with_capacity(CAPACITY);
    let page = directory.overlapping(&KeyBound::of_key(low), &KeyBound::of_key(high), 0, &mut out);
    assert!(!page.is_cut);
    out.into_iter().map(|segment| segment.0).collect()
}

// a range finds every segment it meets, lowest first, and a gap finds none
#[test]
fn overlapping_ranges() {
    let directory = ColumnDirectory::new(CAPACITY);
    assert!(directory.replace(Some(&entry(1, b"a", b"c")), &[]));
    assert!(directory.replace(Some(&entry(2, b"d", b"f")), &[]));
    assert!(directory.replace(Some(&entry(3, b"b", b"z")), &[]));
    assert!(directory.replace(Some(&entry(4, b"m", b"p")), &[]));

    assert_eq!(meeting(&directory, b"a", b"z"), vec![1, 3, 2, 4]);
    assert_eq!(meeting(&directory, b"c", b"d"), vec![1, 3, 2]);
    assert_eq!(meeting(&directory, b"g", b"h"), vec![3]);
    assert_eq!(meeting(&directory, b"n", b"n"), vec![3, 4]);
    assert!(directory.replace(None, &[SegmentNumber(3)]));
    assert_eq!(meeting(&directory, b"g", b"h"), Vec::<u64>::new());
    assert_eq!(meeting(&directory, b"o", b"zz"), vec![4]);
}

// a page takes what fits, and the next one goes on from it
#[test]
fn overlapping_pages() {
    let directory = ColumnDirectory::new(CAPACITY);
    for number in 0..10 {
        assert!(directory.replace(Some(&entry(number, &[number as u8], &[number as u8, 1])), &[]));
    }
    let (low, high) = (KeyBound::of_key(&[]), KeyBound::HIGHEST);
    let mut out = Vec::with_capacity(4);
    let first = directory.overlapping(&low, &high, 0, &mut out);
    assert!(first.is_cut);
    assert_eq!(out, [0, 1, 2, 3].map(SegmentNumber));
    let last = directory.overlapping(&low, &high, 8, &mut out);
    assert!(!last.is_cut);
    assert_eq!(out, [8, 9].map(SegmentNumber));
}

// the walk's next stop is the nearest start above and the nearest end below, whatever stands wide around it
#[test]
fn next_stops() {
    let directory = ColumnDirectory::new(CAPACITY);
    assert!(directory.replace(Some(&entry(1, b"a", b"z")), &[]));
    assert!(directory.replace(Some(&entry(2, b"c", b"d")), &[]));
    assert!(directory.replace(Some(&entry(3, b"f", b"g")), &[]));
    assert!(directory.replace(Some(&entry(4, b"m", b"n")), &[]));

    assert_eq!(directory.next_above(&KeyBound::of_key(b"d")), Some(KeyBound::of_key(b"f")));
    assert_eq!(directory.next_above(&KeyBound::of_key(b"m")), None);
    assert_eq!(directory.next_below(&KeyBound::of_key(b"m")), Some(KeyBound::of_key(b"g")));
    assert_eq!(directory.next_below(&KeyBound::of_key(b"zz")), Some(KeyBound::of_key(b"z")));
    assert_eq!(directory.next_below(&KeyBound::of_key(b"d")), None);
}
