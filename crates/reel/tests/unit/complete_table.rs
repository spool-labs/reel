//! A complete table holds every row it takes, and says when it can't

use reel::index::complete_table::{CompleteShard, CompleteSlot, CompleteTable, Homes, MOST_TAG_SLOTS, STALE_BIT, TagMatches, shard_of, tag_of};
use reel::index::key_hash::KeyHash;
use reel::segment::segment_tail::SegmentNumber;
use reel_api::units::RowCount;

const ROWS: u64 = 100_000;
const OVERFLOW_SHARD_SLOTS: usize = 64;

fn hash_of(number: u64) -> KeyHash {
    let mut mixed = number.wrapping_add(0x9E37_79B9_7F4A_7C15);
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    KeyHash(mixed ^ (mixed >> 31))
}

fn matches_of(table: &CompleteTable, hash: KeyHash) -> TagMatches {
    let mut matches = TagMatches::new();
    table.find(hash, &mut matches);
    matches
}

fn has_row(table: &CompleteTable, hash: KeyHash, segment: u32, block: u16) -> bool {
    matches_of(table, hash).slots().iter().any(|slot| slot.segment == segment && slot.block == block)
}

// every row of a table at capacity stays findable, with no move losing one
#[test]
fn holds_its_capacity() {
    let table = CompleteTable::new(RowCount(ROWS));
    for number in 0..ROWS {
        table.insert(hash_of(number), SegmentNumber(number % 1_000), (number % 60_000) as u16, |_segment| false);
    }
    assert_eq!(table.past_capacity(), RowCount(0));
    assert_eq!(table.mark(), None);
    assert_eq!(table.used_slots(), RowCount(ROWS));
    for number in 0..ROWS {
        assert!(has_row(&table, hash_of(number), (number % 1_000) as u32, (number % 60_000) as u16), "row {number}");
    }
}

// a table too small marks the first segment that found no room, and every row it did take stays findable
#[test]
fn full_marks_the_segment() {
    let table = CompleteTable::new(RowCount(1_000));
    let mut first_past = None;
    for number in 0..ROWS {
        let before = table.past_capacity();
        table.insert(hash_of(number), SegmentNumber(number), 7, |_segment| false);
        if table.past_capacity() != before {
            first_past.get_or_insert(number);
        } else {
            assert!(has_row(&table, hash_of(number), number as u32, 7));
        }
    }
    assert_eq!(table.mark(), first_past.map(SegmentNumber));
    assert!(table.past_capacity().0 > 0);
    assert_eq!(table.used_slots().0 + table.past_capacity().0, ROWS);
}

// one key's slots fill its homes and go on in the overflow area, up to the most a get can list
#[test]
fn one_key_many_slots() {
    let table = CompleteTable::new(RowCount(ROWS));
    let hash = hash_of(1);
    for segment in 0..MOST_TAG_SLOTS as u64 {
        table.insert(hash, SegmentNumber(segment), 1, |_segment| false);
    }
    assert_eq!(table.past_capacity(), RowCount(0));
    assert_eq!(matches_of(&table, hash).slots().len(), MOST_TAG_SLOTS);
    table.insert(hash, SegmentNumber(MOST_TAG_SLOTS as u64), 1, |_segment| false);
    assert_eq!(table.mark(), Some(SegmentNumber(MOST_TAG_SLOTS as u64)));
}

// a gone segment's slots clear when an insert visits their bucket
#[test]
fn gone_slots_clear_on_a_visit() {
    let table = CompleteTable::new(RowCount(ROWS));
    let hash = hash_of(2);
    for segment in 0..4 {
        table.insert(hash, SegmentNumber(segment), 1, |_segment| false);
    }
    table.insert(hash, SegmentNumber(9), 1, |segment| segment < 4);
    let matches = matches_of(&table, hash);
    assert!(matches.slots().iter().all(|slot| slot.segment == 9 || slot.segment >= 4 || !slot.is_used()));
    assert!(has_row(&table, hash, 9, 1));
}

// a key's tag and shard come from bits that don't overlap
#[test]
fn tag_and_shard() {
    let hash = KeyHash(0xABCD_1234_0000_0000);
    assert_eq!(tag_of(hash), 0x1234);
    assert_eq!(shard_of(hash), 0xABCD % 64);
}

fn slot_of(tag: u16, is_stale: bool) -> CompleteSlot {
    CompleteSlot {
        segment: u32::from(tag),
        block: 0,
        tag: if is_stale { tag | STALE_BIT } else { tag },
    }
}

fn tag_count(shard: &CompleteShard, tag: u16) -> usize {
    let homes = Homes { first: 0, second: 0, tag };
    let mut count = 0;
    shard.visit_tag(&homes, |_slot| count += 1);
    count
}

// a slot whose run wrapped past the end of the overflow area stays in reach when a stale slot before it goes
#[test]
fn fold_keeps_a_wrapped_run() {
    let mut shard = CompleteShard::new(OVERFLOW_SHARD_SLOTS);
    for slot in [slot_of(5, true), slot_of(13, false), slot_of(7, false), slot_of(6, false)] {
        shard.fill_overflow(slot);
    }
    assert_eq!(tag_count(&shard, 6), 1);
    assert_eq!(shard.fold_overflow(5), (1, false));
    for tag in [13, 7, 6] {
        assert_eq!(tag_count(&shard, tag), 1, "tag {tag}");
    }
    assert_eq!(shard.overflow_used(), 3);
}

// a run of one tag with stale and live slots keeps the live ones, and a slot that shifts into a cleared position is looked at
#[test]
fn fold_clears_a_whole_run() {
    let mut shard = CompleteShard::new(OVERFLOW_SHARD_SLOTS);
    for is_stale in [true, true, false, true] {
        shard.fill_overflow(slot_of(3, is_stale));
    }
    assert_eq!(shard.fold_overflow(3), (3, false));
    assert_eq!(tag_count(&shard, 3), 1);
    assert_eq!(shard.overflow_used(), 1);
}
