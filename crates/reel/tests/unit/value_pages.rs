//! A sorted index's value pages hand back what a leaf put in, and nothing once its chain went back

use reel::index::value_pages::{HeldChain, MOST_HELD_VALUE, ValuePages};

const LEAF: u32 = 7;
const ENTRIES: usize = 100;

fn value(index: usize, length: usize) -> Vec<u8> {
    (0..length).map(|at| (index * 31 + at) as u8).collect()
}

// every entry a leaf filled reads back as it went in, across as many pages as its values take, and an unfilled one reads nothing
#[test]
fn fills_read_back() {
    let pages = ValuePages::new(16);
    let mut chain = HeldChain::NONE;
    for index in (0..ENTRIES).step_by(2) {
        chain = pages.fill((0, LEAF), chain, (index, ENTRIES), &value(index, 100)).expect("a fill");
    }
    for index in 0..ENTRIES {
        let mut out = Vec::new();
        let is_held = pages.read(chain, index, &mut out);
        assert_eq!(is_held, index % 2 == 0, "entry {index}");
        if is_held {
            assert_eq!(out, value(index, 100), "entry {index}");
        }
    }
    assert!(pages.free_pages() < 15, "fifty values of 100 bytes fit one page");
    assert_eq!(pages.held.load(std::sync::atomic::Ordering::Relaxed), (ENTRIES / 2) as u64);
}

// a value over the cap is never held, and the leaf keeps the chain it had
#[test]
fn long_values_stay_out() {
    let pages = ValuePages::new(4);
    assert_eq!(pages.fill((0, LEAF), HeldChain::NONE, (0, ENTRIES), &value(0, MOST_HELD_VALUE + 1)), None);
    assert_eq!(pages.free_pages(), 4);
}

// a dropped chain reads nothing, even through a reader that kept the chain, and its pages go back to the pool
#[test]
fn a_dropped_chain_reads_nothing() {
    let pages = ValuePages::new(8);
    let chain = pages.fill((0, LEAF), HeldChain::NONE, (3, ENTRIES), &value(3, 500)).expect("a fill");
    pages.drop_chain(0, chain);
    let mut out = Vec::new();
    assert!(!pages.read(chain, 3, &mut out));
    assert!(out.is_empty());
    assert_eq!(pages.free_pages(), 8);
    // the same pages filled for another leaf don't answer for the old chain
    let other = pages.fill((0, LEAF + 1), HeldChain::NONE, (3, ENTRIES), &value(9, 500)).expect("a fill");
    assert!(!pages.read(chain, 3, &mut out));
    assert!(pages.read(other, 3, &mut out));
    assert_eq!(out, value(9, 500));
}

// a full pool takes no more values, and a dropped chain gives its pages back for the next fill
#[test]
fn a_full_pool_fills_again_once_a_chain_drops() {
    let pages = ValuePages::new(2);
    let mut first = HeldChain::NONE;
    let mut second = HeldChain::NONE;
    for index in 0..3 {
        first = pages.fill((0, LEAF), first, (index, ENTRIES), &value(index, 1_000)).expect("a fill");
        second = pages.fill((0, LEAF + 1), second, (index, ENTRIES), &value(index, 1_000)).expect("a fill");
    }
    assert_eq!(pages.fill((0, LEAF + 2), HeldChain::NONE, (0, ENTRIES), &value(2, 100)), None);
    let mut out = Vec::new();
    assert!(pages.read(second, 0, &mut out), "a full pool lost a held value");
    pages.drop_chain(0, first);
    let third = pages.fill((0, LEAF + 2), HeldChain::NONE, (0, ENTRIES), &value(2, 100)).expect("a fill");
    out.clear();
    assert!(pages.read(third, 0, &mut out));
    assert_eq!(out, value(2, 100));
}

// a fill of an entry that already holds a value keeps the first
#[test]
fn a_second_fill_keeps_the_first() {
    let pages = ValuePages::new(4);
    let chain = pages.fill((0, LEAF), HeldChain::NONE, (5, ENTRIES), b"first").expect("a fill");
    let again = pages.fill((0, LEAF), chain, (5, ENTRIES), b"second").expect("a fill");
    assert_eq!(chain, again);
    let mut out = Vec::new();
    assert!(pages.read(chain, 5, &mut out));
    assert_eq!(out, b"first");
}

mod in_an_index {
    use reel::index::sorted_keys::{Floors, KeyEntry, SortedKeys};
    use reel::segment::segment_tail::SegmentNumber;
    use reel_core::units::{ByteCount, FileOffset, RowCount, WritePlace};

    const KEYS: u64 = 4_000;
    const WIDTH: usize = 16;

    fn key(number: u64) -> Vec<u8> {
        number.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes().iter().chain(&number.to_be_bytes()).copied().collect()
    }

    fn row(number: u64, version: u64) -> (WritePlace, SegmentNumber, FileOffset) {
        (WritePlace(number * 10 + version), SegmentNumber(version + 1), FileOffset(number * 64))
    }

    fn entry((place, segment, offset): (WritePlace, SegmentNumber, FileOffset)) -> KeyEntry {
        KeyEntry { place, segment, offset, length: ByteCount(100), is_grave: false }
    }

    fn value(number: u64, version: u64) -> Vec<u8> {
        format!("value-{number}-{version}").into_bytes()
    }

    // a pool far smaller than the keys fills until it's full, and every held answer is the value of the row the entry names: an entry
    // replaced since its fill, or a leaf split under it, answers nothing until filled again
    #[test]
    fn held_answers_are_always_the_entrys_row() {
        let index = SortedKeys::new(WIDTH, RowCount(KEYS * 2)).holding_values(std::sync::Arc::new(reel::index::value_pages::ValuePages::new(8)), (0, true));
        for number in 0..KEYS {
            index.upsert(&key(number), entry(row(number, 0)), Floors::at(WritePlace(0)));
        }
        let mut answered = 0;
        let mut newest = vec![0_u64; KEYS as usize];
        for round in 0..3_u64 {
            for number in 0..KEYS {
                let version = newest[number as usize];
                index.hold_value(&key(number), row(number, version), &value(number, version));
            }
            let check = |newest: &[u64], answered: &mut u64| {
                for number in 0..KEYS {
                    let version = newest[number as usize];
                    let mut out = Vec::new();
                    // the entry's own row answers its own value or nothing, and an older row never answers
                    if index.held_value(&key(number), row(number, version), &mut out) {
                        assert_eq!(out, value(number, version), "key {number} round {round}");
                        *answered += 1;
                    }
                    out.clear();
                    if version > 0 {
                        assert!(!index.held_value(&key(number), row(number, version - 1), &mut out), "key {number} answered an older row");
                    }
                }
            };
            check(&newest, &mut answered);
            // a third of the keys get a newer row, which drops their leaves' values
            for number in (0..KEYS).filter(|number| number % 3 == round) {
                newest[number as usize] += 1;
                index.upsert(&key(number), entry(row(number, newest[number as usize])), Floors::at(WritePlace(0)));
            }
            check(&newest, &mut answered);
        }
        assert!(answered > 0, "nothing was held");
    }
}

// two indexes share a pool with four pages each: one walking every leaf takes its own four and half the other's, the other
// still takes the half it's owed, and a page given back goes to the one still under its floor
#[test]
fn a_sharer_never_takes_another_sharer_floor() {
    let pages = ValuePages::shared(8, &[4, 4]);
    let mut walked = Vec::new();
    for leaf in 0..8 {
        if let Some(chain) = pages.fill((0, LEAF + leaf), HeldChain::NONE, (0, ENTRIES), &value(leaf as usize, 100)) {
            walked.push(chain);
        }
    }
    assert_eq!(walked.len(), 6, "the walking index took past half the other's share");
    let owed: Vec<HeldChain> = (0..2)
        .map(|leaf| pages.fill((1, LEAF + 100 + leaf), HeldChain::NONE, (0, ENTRIES), &value(leaf as usize, 100)).expect("the other's share"))
        .collect();
    assert_eq!(owed.len(), 2);
    assert_eq!(pages.fill((1, LEAF + 200), HeldChain::NONE, (0, ENTRIES), &value(0, 100)), None, "a full pool gave a page");
    pages.drop_chain(1, owed[0]);
    assert!(pages.fill((0, LEAF + 300), HeldChain::NONE, (0, ENTRIES), &value(0, 100)).is_none(), "a page the other is owed went to the walker");
    assert!(pages.fill((1, LEAF + 301), HeldChain::NONE, (0, ENTRIES), &value(0, 100)).is_some(), "the owed page didn't come back");
}

// three indexes share a pool with four pages each: a walk of the first takes what leaves the others half their floors, and each
// of the others still gets its half, whichever goes first
#[test]
fn every_sharer_keeps_half_its_floor() {
    let pages = ValuePages::shared(12, &[4, 4, 4]);
    let take_all = |sharer: usize, base: u32| (0..12).filter(|leaf| pages.fill((sharer, base + leaf), HeldChain::NONE, (0, ENTRIES), &value(*leaf as usize, 100)).is_some()).count();
    assert_eq!(take_all(0, LEAF), 8, "the walk took past the others' halves");
    assert_eq!(take_all(1, LEAF + 100), 2, "the second index missed its half");
    assert_eq!(take_all(2, LEAF + 200), 2, "the third index missed its half");
    assert!(!pages.could_take(0) && !pages.could_take(1) && !pages.could_take(2), "a full pool said it could give a page");
}
