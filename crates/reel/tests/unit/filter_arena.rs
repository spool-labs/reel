use std::hint;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use reel::index::key_hash::KeyHash;
use reel::segment::filter_arena::*;
use reel::segment::segment_tail::SegmentNumber;
use reel_core::units::ByteCount;

const SHARE: ByteCount = ByteCount(4_096);
const SEGMENTS: usize = 8;
const FIRST: SegmentNumber = SegmentNumber(1);
const SECOND: SegmentNumber = SegmentNumber(2);

fn arena() -> FilterArena {
    FilterArena::new(SHARE, SEGMENTS)
}

fn stored(arena: &FilterArena, segment: SegmentNumber, offset: u32) -> Option<Vec<u8>> {
    arena.copy_filter(segment, offset)
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

const LOW_PATTERN: u8 = 0x5A;
const HIGH_PATTERN: u8 = 0xA5;
const PATTERN_BYTES: usize = 1_024;
const RACE_OFFSET: u32 = 100;
const RACE_ROUNDS: u64 = 20_000;
const RACE_READERS: usize = 3;
const RACE_SETTLE: Duration = Duration::from_millis(1);

fn fill(arena: &FilterArena, segment: SegmentNumber, pattern: u8) {
    assert!(arena.begin(segment, 1, PATTERN_BYTES as u32));
    arena.add(segment, RACE_OFFSET, &[pattern; PATTERN_BYTES]);
}

// a reader racing a drop and a reload of the same room only ever answers from the segment it asked for, or not at all
#[test]
fn race_answers_from_the_asked_segment() {
    let arena = FilterArena::new(ByteCount(64 * 1024), SEGMENTS);
    let is_done = AtomicBool::new(false);
    let answered = AtomicU64::new(0);
    fill(&arena, FIRST, LOW_PATTERN);
    thread::scope(|scope| {
        let readers: Vec<_> = (0..RACE_READERS)
            .map(|_reader| {
                scope.spawn(|| {
                    let (mut answers, mut misses) = (0_u64, 0_u64);
                    while !is_done.load(Ordering::Acquire) {
                        match arena.copy_filter(FIRST, RACE_OFFSET) {
                            Some(bits) => {
                                assert!(bits.iter().all(|byte| *byte == LOW_PATTERN), "a probe read another segment's bytes");
                                answers += 1;
                                answered.fetch_add(1, Ordering::Relaxed);
                            },
                            None => misses += 1,
                        }
                    }
                    (answers, misses)
                })
            })
            .collect();
        for _round in 0..RACE_ROUNDS {
            arena.drop_segment(FIRST);
            fill(&arena, SECOND, HIGH_PATTERN);
            arena.drop_segment(SECOND);
            fill(&arena, FIRST, LOW_PATTERN);
            // a writer that never stops leaves a slow reader no whole window, and the race needs both outcomes
            let (seen, start) = (answered.load(Ordering::Relaxed), Instant::now());
            while answered.load(Ordering::Relaxed) == seen && start.elapsed() < RACE_SETTLE {
                hint::spin_loop();
            }
        }
        is_done.store(true, Ordering::Release);
        let (answers, misses) = readers.into_iter().map(|reader| reader.join().expect("join")).fold((0, 0), |sum, one| (sum.0 + one.0, sum.1 + one.1));
        assert!(answers > 0 && misses > 0, "{answers} answers and {misses} misses");
    });
}

// a filling extent answers nothing, and a probe for a block it doesn't hold is no verdict
#[test]
fn probe_says_nothing_it_cant_prove() {
    let arena = arena();
    let hash = KeyHash(7);
    assert!(arena.begin(FIRST, 2, 8));
    arena.add(FIRST, 100, &[0xFF; 4]);
    assert_eq!(arena.probe(FIRST, 100, hash), None);
    arena.add(FIRST, 200, &[0; 4]);
    assert_eq!(arena.probe(FIRST, 100, hash), Some(true));
    assert_eq!(arena.probe(FIRST, 200, hash), Some(false));
    assert_eq!(arena.probe(FIRST, 150, hash), None);
    assert_eq!(arena.probe(SECOND, 100, hash), None);
}
