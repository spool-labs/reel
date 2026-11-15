//! A complete table holds every row it takes, and says when it can't

use std::time::{Duration, Instant};

use reel::index::complete_table::{
    BUCKET_SLOTS, CompleteShard, CompleteSlot, CompleteTable, Homes, MOST_RUN_SLOTS, MOST_TAG_SLOTS, OVERFLOW_DIVISOR, STALE_BIT, TagMatches, shard_of, tag_of,
};
use reel::index::key_hash::KeyHash;
use reel::segment::segment_tail::SegmentNumber;
use reel_api::units::RowCount;

const ROWS: u64 = 100_000;
const OVERFLOW_SHARD_SLOTS: usize = 64;
const CLIFF_ROOM: u64 = 6_000_000;
const CLIFF_KEYS: u64 = 1_000_000;
const CLIFF_WRITES: u64 = 6_000_000;
const CLIFF_SEGMENT_ROWS: u64 = 250_000;
const CLIFF_READS: u64 = 100_000;
const MOST_FIND_TIME: Duration = Duration::from_micros(30);

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

// one key's slots fill its homes and go on in the overflow area, and one past the most a get can list folds them into a sticky slot
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
    assert_eq!(table.mark(), None);
    let matches = matches_of(&table, hash);
    assert_eq!(matches.slots().len(), 1);
    assert!(matches.slots()[0].is_sticky());
    assert_eq!(table.used_slots(), RowCount(1));
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

// a slot whose run wrapped past the end of the overflow area stays in reach when a slot before it goes
#[test]
fn fold_keeps_a_wrapped_run() {
    let mut shard = CompleteShard::new(OVERFLOW_SHARD_SLOTS);
    for slot in [slot_of(5, true), slot_of(13, false), slot_of(7, false), slot_of(6, false)] {
        shard.fill_overflow(slot);
    }
    assert_eq!(tag_count(&shard, 6), 1);
    assert_eq!(shard.fold_overflow(5), 1);
    for tag in [13, 7, 6] {
        assert_eq!(tag_count(&shard, tag), 1, "tag {tag}");
    }
    assert_eq!(shard.overflow_used(), 3);
}

// a whole run of one tag goes, looking again wherever a slot shifts into a cleared position
#[test]
fn fold_clears_a_whole_run() {
    let mut shard = CompleteShard::new(OVERFLOW_SHARD_SLOTS);
    for is_stale in [true, true, false, true] {
        shard.fill_overflow(slot_of(3, is_stale));
    }
    shard.fill_overflow(slot_of(4, false));
    assert_eq!(shard.fold_overflow(3), 4);
    assert_eq!(tag_count(&shard, 3), 0);
    assert_eq!(tag_count(&shard, 4), 1);
    assert_eq!(shard.overflow_used(), 1);
}

// a fold takes slots of the tag that belong to a key with other homes, and that key finds the sticky slot in the run they share
#[test]
fn fold_covers_a_key_with_other_homes() {
    let table = CompleteTable::new(RowCount(ROWS));
    let hot = KeyHash(0x0000_1234_0000_0000);
    let other = KeyHash(0x0000_1234_4000_0000);
    {
        let mut shard = table.shards.write(shard_of(other));
        let (hot_homes, other_homes) = (shard.homes(hot), shard.homes(other));
        assert!(![hot_homes.first, hot_homes.second].contains(&other_homes.first));
        assert!(![hot_homes.first, hot_homes.second].contains(&other_homes.second));
        shard.fill_overflow(CompleteSlot { segment: 7, block: 3, tag: tag_of(other) });
    }
    assert!(has_row(&table, other, 7, 3), "before the fold");
    for segment in 100..101 + MOST_TAG_SLOTS as u64 {
        table.insert(hot, SegmentNumber(segment), 1, |_segment| false);
    }
    let matches = matches_of(&table, other);
    assert!(matches.slots().iter().any(|slot| slot.is_sticky() || (slot.segment == 7 && slot.block == 3)));
}

// slots of gone segments leave the overflow area, so they count toward no fold and hold no room
#[test]
fn gone_overflow_slots_leave_the_count() {
    let table = CompleteTable::new(RowCount(ROWS));
    let hash = hash_of(3);
    for segment in 0..20 {
        table.insert(hash, SegmentNumber(segment), 1, |_segment| false);
    }
    for segment in 100..124 {
        table.insert(hash, SegmentNumber(segment), 1, |segment| segment < 100);
    }
    let matches = matches_of(&table, hash);
    let gone = matches.slots().iter().filter(|slot| slot.segment < 100).count();
    let sticky = matches.slots().iter().filter(|slot| slot.is_sticky()).count();
    assert_eq!((gone, sticky), (0, 0));
    assert_eq!(table.used_slots(), RowCount(24));
}

// keys with many versions fill the overflow area long before the table, and a full area costs a get a bounded walk
#[test]
fn full_overflow_stays_cheap() {
    let table = CompleteTable::new(RowCount(CLIFF_ROOM));
    let mut random = 0x2545_F491_4F6C_DD1D_u64;
    for write in 0..CLIFF_WRITES {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        table.insert(hash_of(random % CLIFF_KEYS), SegmentNumber(write / CLIFF_SEGMENT_ROWS), (write % CLIFF_SEGMENT_ROWS / 100) as u16, |_segment| false);
    }
    assert!(table.past_capacity().0 > 0, "the area should have filled");
    let started = Instant::now();
    for number in 0..CLIFF_READS {
        let _matches = matches_of(&table, hash_of(number));
    }
    let each = started.elapsed() / CLIFF_READS as u32;
    assert!(each < MOST_FIND_TIME, "{each:?} a find");
}

// a slot past its run's reach never lands, so a run can't grow across the area
#[test]
fn run_stops_at_its_reach() {
    let mut shard = CompleteShard::new(OVERFLOW_SHARD_SLOTS * OVERFLOW_DIVISOR);
    for _slot in 0..MOST_RUN_SLOTS {
        assert!(shard.fill_overflow(slot_of(3, false)));
    }
    assert!(!shard.fill_overflow(slot_of(3, false)));
    assert_eq!(shard.overflow_used(), MOST_RUN_SLOTS);
    assert_eq!(tag_count(&shard, 3), MOST_RUN_SLOTS);
}

// the budget's figure for a complete table is the heap the built table holds
#[test]
fn planned_bytes_match_the_built_table() {
    for rows in [1, 1_000, 100_000, 2_000_000] {
        let table = CompleteTable::new(RowCount(rows));
        assert_eq!(reel::index::complete_table::planned_table_bytes(RowCount(rows)), table.heap_bytes(), "{rows} rows");
    }
}

// two keys of one tag share both homes or neither, whatever the bucket count, so a key's free meets another key's slot in its
// homes only where its give saw that slot and shared it
#[test]
fn keys_of_a_tag_share_both_homes_or_none() {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut next = move || {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut mixed = state;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        mixed ^ (mixed >> 31)
    };
    for buckets in [1, 2, 3, 7, 64, 1_000] {
        let shard = CompleteShard::new(buckets * BUCKET_SLOTS);
        let mut by_tag: std::collections::BTreeMap<u16, Vec<[usize; 2]>> = std::collections::BTreeMap::new();
        for _key in 0..20_000 {
            // few tags, so every tag gets many keys
            let hash = KeyHash((next() & 0xFFFF_FFFF) | (u64::from(next() as u16 % 8) << 32));
            let homes = shard.homes(hash);
            let mut pair = [homes.first, homes.second];
            pair.sort_unstable();
            by_tag.entry(homes.tag).or_default().push(pair);
        }
        for (tag, pairs) in &by_tag {
            for pair in pairs {
                for other in pairs {
                    let shares_one = pair.iter().any(|home| other.contains(home));
                    assert!(!shares_one || pair == other, "{buckets} buckets, tag {tag}: {pair:?} and {other:?}");
                }
            }
        }
    }
}

// two keys of one shard and tag: the same high bits, and low bits that put the first home where asked
fn same_tag_pair(other_low: u64) -> (KeyHash, KeyHash) {
    (KeyHash(0x0005_1234_0000_0000), KeyHash(0x0005_1234_0000_0000 | other_low))
}

// a free that never proved its slot keeps a guarded one: two frees of one key's row, its own slot in its homes and another
// key's identical slot in the run, take the first and keep the second
#[test]
fn an_unproved_second_free_keeps_a_run_slot() {
    let table = CompleteTable::new(RowCount(ROWS));
    let (first, second) = same_tag_pair(0x8000_0000);
    let shard = shard_of(first);
    let (first_homes, second_homes) = (table.shards.read(shard).homes(first), table.shards.read(shard).homes(second));
    assert!(![second_homes.first, second_homes.second].contains(&first_homes.first), "{first_homes:?} {second_homes:?}");
    table.insert(first, SegmentNumber(7), 3, |_segment| false);
    assert!(table.shards.write(shard).fill_overflow(CompleteSlot { segment: 7, block: 3, tag: tag_of(second) }));
    table.free_superseded(first, SegmentNumber(7), 3, (table.shares(), false));
    table.free_superseded(first, SegmentNumber(7), 3, (table.shares(), false));
    assert!(has_row(&table, second, 7, 3), "a second free took the other key's run slot");
    assert_eq!(table.frees_kept(), 1);
}

// a share that lands between a free's count and its look keeps the slot: the free read the count before the other key's row
// took the slot, so the slot it meets is guarded and unproved
#[test]
fn a_share_after_the_count_keeps_the_slot() {
    let table = CompleteTable::new(RowCount(ROWS));
    let (first, second) = same_tag_pair(1);
    let shard = shard_of(first);
    assert_eq!(table.shards.read(shard).homes(first).first, table.shards.read(shard).homes(second).first);
    table.insert(first, SegmentNumber(7), 3, |_segment| false);
    let seen = table.shares();
    assert_eq!(table.slot_guard(first, SegmentNumber(7), 3), Some(false));
    table.insert(second, SegmentNumber(7), 3, |_segment| false);
    assert_eq!(table.shares(), seen + 1, "the second key's row took the first key's slot");
    table.free_superseded(first, SegmentNumber(7), 3, (seen, false));
    assert!(has_row(&table, second, 7, 3), "the free took a slot the second key shares");
    assert_eq!(table.frees_kept(), 1);
}
