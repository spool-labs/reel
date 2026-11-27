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
    FilterArena::new(SHARE, SHARE, SEGMENTS).expect("arena")
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
    let arena = FilterArena::new(SHARE, SHARE, 1).expect("arena");
    assert!(arena.begin(FIRST, 1, 1));
    assert!(!arena.begin(SECOND, 1, 1));
}

// a segment with no filters takes a line in the table and no bytes
#[test]
fn empty_extent() {
    let arena = FilterArena::new(SHARE, SHARE, SEGMENTS).expect("arena");
    assert!(arena.begin(FIRST, 0, 0));
    assert!(arena.holds(FIRST));
    arena.drop_segment(FIRST);
    assert!(!arena.holds(FIRST));
}

// a share too small for its tables holds no segment, and refuses one with no harm
#[test]
fn tiny_share() {
    let arena = FilterArena::new(ByteCount(64), ByteCount(64), SEGMENTS).expect("arena");
    assert!(!arena.begin(FIRST, 1, 1));
    assert!(arena.heap_bytes().0 < 1_024);
}

// the arena's whole heap stays inside its share, tables included
#[test]
fn heap_inside_the_share() {
    let share = ByteCount(1024 * 1024);
    assert!(FilterArena::new(share, share, 1_000_000).expect("arena").heap_bytes() <= share);
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
    let arena = FilterArena::new(ByteCount(64 * 1024), ByteCount(64 * 1024), SEGMENTS).expect("arena");
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

const GROW_ROUNDS: usize = 40;
const GROW_SEGMENTS: u64 = 200;
const GROW_BLOCK_BYTES: usize = 256;
const GROW_PAGES: usize = 64;
const GROW_OFFSET: u32 = 9;

// readers probe the newest segment while the arena grows for each one that doesn't fit, and read only words in memory: every
// answer is the asked segment's, and on Linux no reader faults once it has warmed up
#[test]
fn probes_race_grows() {
    let page = reel::region::page_bytes();
    let ceiling = ByteCount((GROW_PAGES * page) as u64);
    let mut grew = 0;
    for _round in 0..GROW_ROUNDS {
        let arena = FilterArena::new(ceiling, ByteCount(0), GROW_SEGMENTS as usize).expect("arena");
        let newest = AtomicU64::new(0);
        let is_done = AtomicBool::new(false);
        thread::scope(|scope| {
            let readers: Vec<_> = (0..RACE_READERS)
                .map(|_reader| {
                    scope.spawn(|| {
                        let mut faults = None;
                        while !is_done.load(Ordering::Acquire) {
                            let number = newest.load(Ordering::Acquire);
                            if number == 0 {
                                continue;
                            }
                            // the first probe brings in the reader's own pages, and the count starts after it
                            if faults.is_none() {
                                let _first = arena.probe(SegmentNumber(number), GROW_OFFSET, KeyHash(number));
                                faults = Some(reel::region::thread_faults());
                            }
                            // every block is all ones, so a probe that answers can only say maybe
                            assert_ne!(arena.probe(SegmentNumber(number), GROW_OFFSET, KeyHash(number)), Some(false), "a probe read words it doesn't own");
                        }
                        reel::region::thread_faults().saturating_sub(faults.unwrap_or_else(reel::region::thread_faults))
                    })
                })
                .collect();
            for number in 1..=GROW_SEGMENTS {
                let segment = SegmentNumber(number);
                if !arena.begin(segment, 1, GROW_BLOCK_BYTES as u32) {
                    assert!(arena.is_short(), "a refused segment asked for no room");
                    assert!(arena.grow_step(&mut u64::MAX.clone()).expect("grow"), "the arena didn't grow under its ceiling");
                    grew += 1;
                    assert!(arena.begin(segment, 1, GROW_BLOCK_BYTES as u32), "a segment didn't fit after a grow");
                }
                arena.add(segment, GROW_OFFSET, &[0xFF; GROW_BLOCK_BYTES]);
                newest.store(number, Ordering::Release);
            }
            is_done.store(true, Ordering::Release);
            for reader in readers {
                let faults = reader.join().expect("join");
                // a probe's stack and code come in on its first pass, and nothing after
                assert!(faults <= READER_WARM_FAULTS, "a reader took {faults} faults beside the grows");
            }
        });
        assert!(arena.ready_bytes() <= ceiling.0);
    }
    assert!(grew >= GROW_ROUNDS, "the arena grew {grew} times");
}

const READER_WARM_FAULTS: u64 = 8;

// an arena at its ceiling grows no more and stops asking, so its segment stays with the block cache
#[test]
fn grows_stop_at_the_ceiling() {
    let page = reel::region::page_bytes();
    let arena = FilterArena::new(ByteCount(page as u64), ByteCount(0), SEGMENTS).expect("arena");
    assert_eq!(arena.ready_bytes(), 0);
    assert!(!arena.begin(FIRST, 1, 8));
    assert!(arena.is_short());
    let mut none_left = 0;
    assert!(!arena.grow_step(&mut none_left).expect("grow"), "a grow passed the room left");
    assert!(arena.is_short(), "a grow held short stopped asking");
    assert!(arena.grow_step(&mut u64::MAX.clone()).expect("grow"));
    assert!(arena.begin(FIRST, 1, 8));
    assert!(!arena.begin(SECOND, 1, page as u32));
    assert!(!arena.grow_step(&mut u64::MAX.clone()).expect("grow"), "an arena grew past its ceiling");
    assert!(!arena.is_short(), "an arena at its ceiling kept asking");
}

const CAP_HELD_BYTES: u64 = 32 * 1024 * 1024;

// an arena that holds a lot grows by what a refused segment lacked and at most ARENA_STEP_BYTES past it, so one grow on the reserve
// job never doubles a large arena at once
#[test]
fn a_grow_takes_at_most_a_step() {
    let arena = FilterArena::new(ByteCount(4 * CAP_HELD_BYTES), ByteCount(CAP_HELD_BYTES), SEGMENTS).expect("arena");
    let held = arena.ready_bytes();
    assert!(held >= CAP_HELD_BYTES - 4096);
    // one segment takes nearly all the floor, and the next small one finds no room
    assert!(arena.begin(FIRST, 1, (held - 64) as u32));
    assert!(!arena.begin(SECOND, 1, 64));
    let grow = arena.next_grow().expect("a grow");
    assert!(grow <= (ARENA_STEP_BYTES + 4 * 4096) as u64, "one grow of {grow} bytes passed the step");
    assert!(arena.grow_step(&mut u64::MAX.clone()).expect("grow"));
    assert!(arena.begin(SECOND, 1, 64), "the segment didn't fit after a grow");
}
