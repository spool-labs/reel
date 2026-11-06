use reel::segment::filter_arena::*;
use reel::segment::segment_tail::SegmentNumber;
use reel_api::units::ByteCount;

const SHARE: ByteCount = ByteCount(4_096);
const SEGMENTS: usize = 8;
const FIRST: SegmentNumber = SegmentNumber(1);
const SECOND: SegmentNumber = SegmentNumber(2);

fn arena() -> FilterArena {
    FilterArena::new(SHARE, SEGMENTS)
}

fn stored(arena: &FilterArena, segment: SegmentNumber, offset: u32) -> Option<Vec<u8>> {
    arena.with_filter(segment, offset, <[u8]>::to_vec)
}

// a block's bits come back by its offset, and an offset it never held finds nothing
#[test]
fn bits_by_offset() {
    let arena = arena();
    assert!(arena.begin(FIRST, 3, 9));
    arena.add(FIRST, 100, b"aaa");
    arena.add(FIRST, 200, b"bb");
    arena.add(FIRST, 300, b"cccc");
    assert_eq!(stored(&arena, FIRST, 200), Some(b"bb".to_vec()));
    assert_eq!(stored(&arena, FIRST, 300), Some(b"cccc".to_vec()));
    assert_eq!(stored(&arena, FIRST, 100), Some(b"aaa".to_vec()));
    assert_eq!(stored(&arena, FIRST, 150), None);
    assert_eq!(stored(&arena, SECOND, 100), None);
}

// an extent answers only once it's whole, so a half-filled one sends a read to the file
#[test]
fn shows_when_whole() {
    let arena = arena();
    assert!(arena.begin(FIRST, 2, 4));
    arena.add(FIRST, 100, b"aa");
    assert!(arena.holds(FIRST));
    assert_eq!(stored(&arena, FIRST, 100), None);
    arena.add(FIRST, 200, b"bb");
    assert_eq!(stored(&arena, FIRST, 100), Some(b"aa".to_vec()));
}

// a segment that doesn't fit stays out, and what's there keeps answering
#[test]
fn refuses_past_the_share() {
    let arena = arena();
    assert!(arena.begin(FIRST, 1, 3_000));
    arena.add(FIRST, 100, &[7; 3_000]);
    assert!(!arena.begin(SECOND, 1, 3_000));
    assert!(!arena.holds(SECOND));
    assert_eq!(stored(&arena, FIRST, 100).map(|bits| bits.len()), Some(3_000));
}

// a segment's going gives its room back, and spans side by side join for a larger one
#[test]
fn room_comes_back() {
    let arena = arena();
    assert!(arena.begin(FIRST, 1, 1_500));
    assert!(arena.begin(SECOND, 1, 1_500));
    arena.drop_segment(FIRST);
    arena.drop_segment(SECOND);
    assert!(!arena.holds(FIRST));
    assert!(arena.begin(SegmentNumber(3), 1, 3_500));
}

// the table of segments has its own limit, past which a segment stays out
#[test]
fn segment_limit() {
    let arena = FilterArena::new(SHARE, 1);
    assert!(arena.begin(FIRST, 1, 1));
    assert!(!arena.begin(SECOND, 1, 1));
}

// a segment with no filters takes a line in the table and no bytes
#[test]
fn empty_extent() {
    let arena = FilterArena::new(SHARE, SEGMENTS);
    assert!(arena.begin(FIRST, 0, 0));
    assert!(arena.holds(FIRST));
    arena.drop_segment(FIRST);
    assert!(!arena.holds(FIRST));
}

// a share too small for its tables holds no segment, and refuses one with no harm
#[test]
fn tiny_share() {
    let arena = FilterArena::new(ByteCount(64), SEGMENTS);
    assert!(!arena.begin(FIRST, 1, 1));
    assert!(arena.heap_bytes().0 < 1_024);
}

// the arena's whole heap stays inside its share, tables included
#[test]
fn heap_inside_the_share() {
    let share = ByteCount(1024 * 1024);
    assert!(FilterArena::new(share, 1_000_000).heap_bytes() <= share);
}
