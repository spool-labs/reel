//! A complete table holds every row it takes, and says when it can't

use reel::index::complete_table::{CompleteTable, MOST_TAG_SLOTS, TagMatches, shard_of, tag_of};
use reel::index::key_hash::KeyHash;
use reel::segment::segment_tail::SegmentNumber;
use reel_api::units::RowCount;

const ROWS: u64 = 100_000;

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
