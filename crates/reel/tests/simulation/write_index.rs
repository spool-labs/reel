//! The write index against a plain model, with the seal switch stepped by hand

use std::collections::{BTreeMap, BTreeSet};

use crate::seed_sets::seeds;
use reel::index::entry_slab::PageCount;
use reel::index::node_pool::NodeCount;
use reel::index::open_slots::{OpenSlot, OpenSlots};
use reel::index::seal_walk::{SealWalk, WALK_RUN_ROWS};
use reel::index::shard_cursor::ShardCursor;
use reel::index::write_index::{SHARD_COUNT, WriteIndex, shard_of};
use reel::index::write_row::WriteRow;
use reel::index::key_hash::hash_key;
use reel::index::write_shard::{ShardHold, ShardNeed, ShardSizing, WalkBound, WriteShard};
use reel_sim::seeded_random::{Seed, SeededRandom};
use reel::row_kind::RowKind;
use reel::segment::segment_tail::SegmentNumber;
use reel_core::column_id::ColumnId;
use reel_core::direction::Direction;
use reel_core::units::{ByteCount, FileOffset, RowCount, WritePlace};

const SEED_COUNT: u64 = 16;
const COLUMN: ColumnId = ColumnId(5);
const SEALED: OpenSlot = OpenSlot(0);
const STAYING: OpenSlot = OpenSlot(1);
const SLOTS: u64 = 3;
const KEY_SPACE: u64 = 300;
const ROWS: u64 = 4_000;
const CLEAN_ALL: RowCount = RowCount(u64::MAX);
const SIZING: ShardSizing = ShardSizing {
    pages: PageCount(192),
    nodes: NodeCount(512),
    table_slots: 2_048,
    open_slots: 4,
    buckets: 4_096,
};
const TIGHT: ShardSizing = ShardSizing {
    pages: PageCount(8),
    nodes: NodeCount(64),
    table_slots: 8_192,
    open_slots: 4,
    buckets: 4_096,
};
const SMALL_TABLE: ShardSizing = ShardSizing {
    pages: PageCount(256),
    nodes: NodeCount(512),
    table_slots: 64,
    open_slots: 4,
    buckets: 4_096,
};
const LATEST: WritePlace = WritePlace(u64::MAX - 1);
const MANY_VERSIONS: u64 = 3_000;
const KEY_SEARCH: u64 = 4_000;
const CLEAN_VERSIONS: u64 = 40;
const MOST_EXTRA_KEY_BYTES: u64 = 90;
const LONG_KEY_BYTES: usize = 2_000;
const NEED_ROWS: u64 = 3_000;
const BATCH_SIZING: ShardSizing = ShardSizing {
    pages: PageCount(256),
    nodes: NodeCount(256),
    table_slots: 8_192,
    open_slots: 4,
    buckets: 4_096,
};

fn row(place: u64, slot: OpenSlot, kind: RowKind) -> WriteRow {
    WriteRow {
        place: WritePlace(place),
        offset: FileOffset(place * 10),
        length: ByteCount(place),
        kind,
        slot,
        segment: SegmentNumber(1),
        has_newer_sealed: false,
    }
}

fn both() -> OpenSlots {
    OpenSlots::only(SEALED).with(STAYING)
}

fn after_seal() -> OpenSlots {
    OpenSlots::only(STAYING)
}

fn key_of(number: u64) -> Vec<u8> {
    format!("key-{number:05}").into_bytes()
}

fn add(index: &WriteIndex, key: &[u8], written: WriteRow) {
    assert!(index.insert(COLUMN, key, &written, None));
}

fn drained(walk: &mut SealWalk, index: &WriteIndex) -> Vec<(Vec<u8>, u64)> {
    let mut rows = Vec::new();
    while let Some(shard) = walk.next_shard() {
        let head = walk.head(shard);
        let place = head.row.expect("row").place.0;
        rows.push((head.key.to_vec(), place));
        walk.advance(index, shard);
    }
    rows
}

fn next_across_shards(
    index: &WriteIndex,
    cursors: &mut [ShardCursor],
    bound: &WalkBound,
    view: OpenSlots,
) -> Option<Vec<u8>> {
    let mut best: Option<Vec<u8>> = None;
    let mut found = Vec::new();
    for (shard, cursor) in cursors.iter_mut().enumerate() {
        if !index.next_key(shard, cursor, bound, view, &mut found) {
            continue;
        }
        let is_closer = match (&best, bound.direction) {
            (None, _direction) => true,
            (Some(current), Direction::Ascending) => found < *current,
            (Some(current), Direction::Descending) => found > *current,
        };
        if is_closer {
            best = Some(found.clone());
        }
    }
    best
}

// lookups match a plain model at every read point, whatever order rows land in
#[test]
fn model_lookups() {
    for seed in seeds(0..SEED_COUNT) {
        let mut random = SeededRandom::new(Seed(seed));
        let index = WriteIndex::new(&SIZING, SIZING.pages).expect("build index");
        let mut model: BTreeMap<Vec<u8>, BTreeMap<u64, RowKind>> = BTreeMap::new();
        let view = OpenSlots::only(OpenSlot(0)).with(OpenSlot(1)).with(OpenSlot(2));

        let mut places: Vec<u64> = (1..=ROWS).collect();
        for at in 0..places.len() {
            let swap = (at + random.below(8) as usize).min(places.len() - 1);
            places.swap(at, swap);
        }

        for place in places {
            let key = key_of(random.below(KEY_SPACE));
            let slot = OpenSlot(random.below(SLOTS) as u8);
            let kind = if random.one_in(4) { RowKind::Grave } else { RowKind::Value };
            add(&index, &key, row(place, slot, kind));
            model.entry(key).or_default().insert(place, kind);
        }

        for (key, versions) in &model {
            for _check in 0..4 {
                let read_point = WritePlace(random.below(ROWS + 2));
                let expected = versions.range(..=read_point.0).next_back();
                let found = index.lookup(COLUMN, key, read_point, view);

                match expected {
                    Some((place, kind)) => {
                        let answer = found.row.expect("visible row");
                        assert_eq!(answer.place, WritePlace(*place));
                        assert_eq!(answer.kind, *kind);
                        assert!(!found.should_ask_footers);
                    },
                    None => assert!(found.row.is_none()),
                }
            }
        }
    }
}

// keys arriving in order and in reverse keep every row in order through splits
#[test]
fn tree_order() {
    for is_reversed in [false, true] {
        let index = WriteIndex::new(&SIZING, SIZING.pages).expect("build index");
        let mut expected = Vec::new();
        for number in 0..ROWS {
            let written = if is_reversed { ROWS - number } else { number + 1 };
            let key = key_of(written);
            add(&index, &key, row(number + 1, SEALED, RowKind::Value));
            expected.push((key, number + 1));
        }
        expected.sort();

        let mut walk = SealWalk::new();
        walk.start(&index, SEALED);

        assert_eq!(drained(&mut walk, &index), expected);
    }
}

// a seal walk returns each slot's rows once: key ascending, then newest first
#[test]
fn seal_order() {
    let mut random = SeededRandom::new(Seed(1));
    let index = WriteIndex::new(&SIZING, SIZING.pages).expect("build index");
    let mut expected: Vec<(Vec<u8>, u64)> = Vec::new();

    for place in 1..=ROWS {
        let key = key_of(random.below(KEY_SPACE));
        let slot = if random.one_in(2) { SEALED } else { STAYING };
        add(&index, &key, row(place, slot, RowKind::Value));
        if slot == SEALED {
            expected.push((key, place));
        }
    }
    expected.sort_by(|left, right| left.0.cmp(&right.0).then(right.1.cmp(&left.1)));

    let mut walk = SealWalk::new();
    walk.start(&index, SEALED);

    assert_eq!(drained(&mut walk, &index), expected);
}

// a walk holds its order with shards that have no rows, that run out early, and that outlast a run
#[test]
fn walk_gaps() {
    let wanted = |shard: usize| match shard % 4 {
        0 => 0,
        1 => 1,
        2 => WALK_RUN_ROWS,
        _ => 2 * WALK_RUN_ROWS + 1,
    };
    let index = WriteIndex::new(&SIZING, SIZING.pages).expect("build index");
    let mut counts = [0; SHARD_COUNT];
    let mut expected = Vec::new();
    for number in 0..KEY_SEARCH {
        let key = key_of(number);
        let shard = shard_of(hash_key(COLUMN, &key));
        let place = number + 1;
        if counts[shard] < wanted(shard) {
            counts[shard] += 1;
            add(&index, &key, row(place, SEALED, RowKind::Value));
            expected.push((key, place));
        } else {
            add(&index, &key, row(place, STAYING, RowKind::Value));
        }
        if (0..SHARD_COUNT).all(|shard| counts[shard] == wanted(shard)) {
            break;
        }
    }
    assert!((0..SHARD_COUNT).all(|shard| counts[shard] == wanted(shard)));
    expected.sort();

    let mut walk = SealWalk::new();
    walk.start(&index, SEALED);

    assert_eq!(drained(&mut walk, &index), expected);
}

// after the switch a closed slot answers nothing, before any cleanup has run
#[test]
fn switch_before_cleanup() {
    let index = WriteIndex::new(&SIZING, SIZING.pages).expect("build index");
    add(&index, b"sealed-only", row(3, SEALED, RowKind::Value));

    let before = index.lookup(COLUMN, b"sealed-only", LATEST, both());
    assert_eq!(before.row.expect("open row").place, WritePlace(3));
    assert!(!before.should_ask_footers);

    let after = index.lookup(COLUMN, b"sealed-only", LATEST, after_seal());
    assert_eq!(after.row, None);
    assert!(after.should_ask_footers);
}

// a closed slot's grave stops answering at the switch
#[test]
fn grave_at_switch() {
    let index = WriteIndex::new(&SIZING, SIZING.pages).expect("build index");
    add(&index, b"gone", row(3, SEALED, RowKind::Grave));

    let after = index.lookup(COLUMN, b"gone", LATEST, after_seal());

    assert_eq!(after.row, None);
    assert!(after.should_ask_footers);
}

// part way through cleanup no reader can tell which rows are gone
#[test]
fn switch_mid_cleanup() {
    let index = WriteIndex::new(&SIZING, SIZING.pages).expect("build index");
    for number in 0..ROWS {
        add(&index, &key_of(number), row(number + 1, SEALED, RowKind::Value));
    }
    add(&index, b"staying", row(ROWS + 1, STAYING, RowKind::Value));

    let mut is_clean = false;
    while !is_clean {
        is_clean = index.clean_slot(SEALED, RowCount(7));

        for number in (0..ROWS).step_by(97) {
            let found = index.lookup(COLUMN, &key_of(number), LATEST, after_seal());
            assert_eq!(found.row, None);
            assert!(found.should_ask_footers);
        }
        let staying = index.lookup(COLUMN, b"staying", LATEST, after_seal());
        assert_eq!(staying.row.expect("open row").place, WritePlace(ROWS + 1));
        assert!(!staying.should_ask_footers);
    }
}

// a cleanup run through many versions of a key keeps its chain: the table slot goes with the last row, and a newer
// row in another slot stays
#[test]
fn clean_versions() {
    let index = WriteIndex::new(&SIZING, SIZING.pages).expect("build index");
    for place in 1..=CLEAN_VERSIONS {
        add(&index, b"gone", row(place, SEALED, RowKind::Value));
        add(&index, b"kept", row(CLEAN_VERSIONS + place, SEALED, RowKind::Value));
    }
    let newest = 2 * CLEAN_VERSIONS + 1;
    add(&index, b"kept", row(newest, STAYING, RowKind::Value));

    let mut is_clean = false;
    while !is_clean {
        is_clean = index.clean_slot(SEALED, RowCount(5));
    }

    assert_eq!(index.lookup(COLUMN, b"gone", LATEST, both()).row, None);
    let kept = index.lookup(COLUMN, b"kept", LATEST, both());
    assert_eq!(kept.row.expect("open row").place, WritePlace(newest));
    add(&index, b"gone", row(newest + 1, STAYING, RowKind::Value));
    let back = index.lookup(COLUMN, b"gone", LATEST, both());
    assert_eq!(back.row.expect("open row").place, WritePlace(newest + 1));
}

// a view loaded before the switch either finds the row or goes to the footers
#[test]
fn view_before_switch() {
    let index = WriteIndex::new(&SIZING, SIZING.pages).expect("build index");
    add(&index, b"key", row(3, SEALED, RowKind::Value));

    let early = index.lookup(COLUMN, b"key", LATEST, both());
    assert_eq!(early.row.expect("open row").place, WritePlace(3));

    assert!(index.clean_slot(SEALED, CLEAN_ALL));

    let late = index.lookup(COLUMN, b"key", LATEST, both());
    assert_eq!(late.row, None);
    assert!(late.should_ask_footers);
}

// an older row in a segment that stays open can't hide a newer sealed one
#[test]
fn stale_version() {
    let index = WriteIndex::new(&SIZING, SIZING.pages).expect("build index");
    add(&index, b"key", row(5, STAYING, RowKind::Value));
    add(&index, b"key", row(10, SEALED, RowKind::Value));

    let at_switch = index.lookup(COLUMN, b"key", LATEST, after_seal());
    assert_eq!(at_switch.row.expect("older row").place, WritePlace(5));
    assert!(at_switch.should_ask_footers);

    assert!(index.clean_slot(SEALED, CLEAN_ALL));

    let cleaned = index.lookup(COLUMN, b"key", LATEST, after_seal());
    assert_eq!(cleaned.row.expect("older row").place, WritePlace(5));
    assert!(cleaned.should_ask_footers);
}

// a newer write over a stale row needs no footers again
#[test]
fn newer_after_stale() {
    let index = WriteIndex::new(&SIZING, SIZING.pages).expect("build index");
    add(&index, b"key", row(5, STAYING, RowKind::Value));
    add(&index, b"key", row(10, SEALED, RowKind::Value));
    assert!(index.clean_slot(SEALED, CLEAN_ALL));

    add(&index, b"key", row(20, STAYING, RowKind::Value));

    let found = index.lookup(COLUMN, b"key", LATEST, after_seal());
    assert_eq!(found.row.expect("newest row").place, WritePlace(20));
    assert!(!found.should_ask_footers);
}

// a cue below a sealed row still reads the older row and checks the footers
#[test]
fn cue_stale() {
    let index = WriteIndex::new(&SIZING, SIZING.pages).expect("build index");
    add(&index, b"key", row(5, STAYING, RowKind::Value));
    add(&index, b"key", row(10, SEALED, RowKind::Value));
    assert!(index.clean_slot(SEALED, CLEAN_ALL));

    let found = index.lookup(COLUMN, b"key", WritePlace(7), after_seal());

    assert_eq!(found.row.expect("older row").place, WritePlace(5));
}

// a slot takes rows again once cleanup has freed it
#[test]
fn slot_reuse() {
    let index = WriteIndex::new(&TIGHT, TIGHT.pages).expect("build index");
    for round in 0..4 {
        for number in 0..ROWS / 4 {
            let place = round * ROWS + number + 1;
            add(&index, &key_of(number), row(place, SEALED, RowKind::Value));
        }
        assert!(index.clean_slot(SEALED, CLEAN_ALL));
    }

    let found = index.lookup(COLUMN, &key_of(1), LATEST, both());
    assert_eq!(found.row, None);
}

// a full shard refuses a row and keeps every row it took
#[test]
fn full_shard() {
    let index = WriteIndex::new(&TIGHT, TIGHT.pages).expect("build index");
    let mut taken = Vec::new();
    for number in 0..ROWS * 8 {
        let key = key_of(number);
        if !index.insert(COLUMN, &key, &row(number + 1, SEALED, RowKind::Value), None) {
            break;
        }
        taken.push((key, number + 1));
    }

    assert!((taken.len() as u64) < ROWS * 8);
    for (key, place) in taken {
        let found = index.lookup(COLUMN, &key, LATEST, both());
        assert_eq!(found.row.expect("row").place, WritePlace(place));
    }
}

// the delete shortcut needs a visible grave as the key's newest row in an open slot
#[test]
fn grave_shortcut() {
    let index = WriteIndex::new(&SIZING, SIZING.pages).expect("build index");
    add(&index, b"grave", row(3, SEALED, RowKind::Grave));
    add(&index, b"value", row(4, SEALED, RowKind::Value));
    add(&index, b"revived", row(5, SEALED, RowKind::Grave));
    add(&index, b"revived", row(6, SEALED, RowKind::Value));

    assert!(index.is_newest_grave(COLUMN, b"grave", LATEST, both()));
    assert!(!index.is_newest_grave(COLUMN, b"value", LATEST, both()));
    assert!(!index.is_newest_grave(COLUMN, b"revived", LATEST, both()));
    assert!(!index.is_newest_grave(COLUMN, b"absent", LATEST, both()));

    assert!(!index.is_newest_grave(COLUMN, b"grave", WritePlace(2), both()));
    assert!(!index.is_newest_grave(COLUMN, b"grave", LATEST, after_seal()));
}

// a stale grave in a segment that stays open can't stand in for a sealed newer write
#[test]
fn shortcut_stale() {
    let index = WriteIndex::new(&SIZING, SIZING.pages).expect("build index");
    add(&index, b"key", row(5, STAYING, RowKind::Grave));
    add(&index, b"key", row(10, SEALED, RowKind::Value));
    assert!(index.clean_slot(SEALED, CLEAN_ALL));

    assert!(!index.is_newest_grave(COLUMN, b"key", LATEST, after_seal()));
}

// walks reach every key once in both directions and stay in their column
#[test]
fn walk_keys() {
    let index = WriteIndex::new(&SIZING, SIZING.pages).expect("build index");
    let mut expected: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut random = SeededRandom::new(Seed(2));
    for place in 1..=ROWS {
        let key = key_of(random.below(KEY_SPACE));
        let slot = if random.one_in(2) { SEALED } else { STAYING };
        add(&index, &key, row(place, slot, RowKind::Value));
        expected.insert(key);
    }
    let other = ColumnId(COLUMN.0 + 1);
    assert!(index.insert(other, b"key-99999", &row(ROWS + 1, SEALED, RowKind::Value), None));

    let mut cursors = vec![ShardCursor::new(SIZING.open_slots); SHARD_COUNT];
    let mut walked = Vec::new();
    let mut last: Vec<u8> = Vec::new();
    let mut is_inclusive = true;
    loop {
        let bound = WalkBound {
            column: COLUMN,
            key: &last,
            is_inclusive,
            direction: Direction::Ascending,
        };
        let Some(next) = next_across_shards(&index, &mut cursors, &bound, both()) else {
            break;
        };
        walked.push(next.clone());
        last = next;
        is_inclusive = false;
    }
    let ascending: Vec<Vec<u8>> = expected.iter().cloned().collect();
    assert_eq!(walked, ascending);

    let mut cursors = vec![ShardCursor::new(SIZING.open_slots); SHARD_COUNT];
    let mut walked = Vec::new();
    let mut last = b"z".to_vec();
    loop {
        let bound = WalkBound {
            column: COLUMN,
            key: &last,
            is_inclusive: false,
            direction: Direction::Descending,
        };
        let Some(next) = next_across_shards(&index, &mut cursors, &bound, both()) else {
            break;
        };
        walked.push(next.clone());
        last = next;
    }
    let descending: Vec<Vec<u8>> = expected.iter().rev().cloned().collect();
    assert_eq!(walked, descending);
}

// a fresh cursor finds the first key past any bound, below, inside or above each tree's span and in either direction, with
// other columns' rows on both sides, as the plain model does
#[test]
fn bound_at_every_edge() {
    for seed in seeds(0..SEED_COUNT) {
        let mut random = SeededRandom::new(Seed(seed));
        let index = WriteIndex::new(&SIZING, SIZING.pages).expect("build index");
        let mut expected: BTreeSet<Vec<u8>> = BTreeSet::new();
        for place in 1..=ROWS / 4 {
            let key = key_of(KEY_SPACE / 4 + random.below(KEY_SPACE / 2));
            let slot = if random.one_in(2) { SEALED } else { STAYING };
            add(&index, &key, row(place, slot, RowKind::Value));
            expected.insert(key);
        }
        for (other, place) in [(ColumnId(COLUMN.0 - 1), ROWS + 1), (ColumnId(COLUMN.0 + 1), ROWS + 2)] {
            assert!(index.insert(other, &key_of(KEY_SPACE / 2), &row(place, SEALED, RowKind::Value), None));
        }
        for _ask in 0..KEY_SPACE {
            let start = key_of(random.below(KEY_SPACE));
            let is_inclusive = random.one_in(2);
            for direction in [Direction::Ascending, Direction::Descending] {
                let bound = WalkBound { column: COLUMN, key: &start, is_inclusive, direction };
                let mut cursors = vec![ShardCursor::new(SIZING.open_slots); SHARD_COUNT];
                let found = next_across_shards(&index, &mut cursors, &bound, both());
                let want = match (direction, is_inclusive) {
                    (Direction::Ascending, true) => expected.range(start.clone()..).next(),
                    (Direction::Ascending, false) => expected.range((std::ops::Bound::Excluded(start.clone()), std::ops::Bound::Unbounded)).next(),
                    (Direction::Descending, true) => expected.range(..=start.clone()).next_back(),
                    (Direction::Descending, false) => expected.range(..start.clone()).next_back(),
                };
                assert_eq!(found.as_ref(), want, "seed {seed} {direction:?} from {start:?} inclusive {is_inclusive}");
            }
        }
    }
}

// a shard refuses a row when its table is near full, and never spins in a probe
#[test]
fn table_limit() {
    let index = WriteIndex::new(&SMALL_TABLE, SMALL_TABLE.pages).expect("build index");
    let mut taken = 0;
    for number in 0..ROWS {
        if index.insert(COLUMN, &key_of(number), &row(number + 1, SEALED, RowKind::Value), None) {
            taken += 1;
        }
    }

    assert!(taken < ROWS);
    let absent = index.lookup(COLUMN, b"absent", LATEST, both());
    assert_eq!(absent.row, None);
}

// cleaning a middle segment keeps the versions on both sides of it
#[test]
fn middle_clean() {
    let middle = OpenSlot(2);
    let view = both().with(middle);
    let index = WriteIndex::new(&SIZING, SIZING.pages).expect("build index");
    add(&index, b"key", row(1, STAYING, RowKind::Value));
    add(&index, b"key", row(2, middle, RowKind::Value));
    add(&index, b"key", row(3, SEALED, RowKind::Value));

    assert!(index.clean_slot(middle, CLEAN_ALL));

    let without_middle = view.without(middle);
    let newest = index.lookup(COLUMN, b"key", LATEST, without_middle);
    assert_eq!(newest.row.expect("newest").place, WritePlace(3));
    assert!(!newest.should_ask_footers);
    let oldest = index.lookup(COLUMN, b"key", WritePlace(2), without_middle);
    assert_eq!(oldest.row.expect("oldest").place, WritePlace(1));
    assert!(oldest.should_ask_footers);
}

// a cue far behind a much rewritten key finds its version among all the newer ones
#[test]
fn cue_versions() {
    let index = WriteIndex::new(&SIZING, SIZING.pages).expect("build index");
    for place in 1..=MANY_VERSIONS {
        let slot = if place % 2 == 0 { SEALED } else { STAYING };
        add(&index, b"key", row(place, slot, RowKind::Value));
    }

    for read_point in [1, 2, 77, MANY_VERSIONS - 1, MANY_VERSIONS] {
        let found = index.lookup(COLUMN, b"key", WritePlace(read_point), both());
        assert_eq!(found.row.expect("version").place, WritePlace(read_point));
        assert!(!found.should_ask_footers);
    }
}

// a much rewritten key cleans out of one slot and keeps its versions in the other
#[test]
fn hot_key_clean() {
    let index = WriteIndex::new(&SIZING, SIZING.pages).expect("build index");
    for place in 1..=MANY_VERSIONS {
        let slot = if place % 2 == 0 { SEALED } else { STAYING };
        add(&index, b"key", row(place, slot, RowKind::Value));
    }

    assert!(index.clean_slot(SEALED, CLEAN_ALL));

    let found = index.lookup(COLUMN, b"key", LATEST, after_seal());
    assert_eq!(found.row.expect("row").place, WritePlace(MANY_VERSIONS - 1));
    assert!(found.should_ask_footers);
}

// a walk keeps going through rows written under it, with no key twice and none lost
#[test]
fn walk_under_writes() {
    let index = WriteIndex::new(&SIZING, SIZING.pages).expect("build index");
    let mut expected: BTreeSet<Vec<u8>> = BTreeSet::new();
    for number in 0..KEY_SPACE {
        let key = key_of(number * 2);
        add(&index, &key, row(number + 1, SEALED, RowKind::Value));
        expected.insert(key);
    }

    let mut cursors = vec![ShardCursor::new(SIZING.open_slots); SHARD_COUNT];
    let mut walked = Vec::new();
    let mut last: Vec<u8> = Vec::new();
    let mut is_inclusive = true;
    let mut place = KEY_SPACE;
    loop {
        let bound = WalkBound {
            column: COLUMN,
            key: &last,
            is_inclusive,
            direction: Direction::Ascending,
        };
        let Some(next) = next_across_shards(&index, &mut cursors, &bound, both()) else {
            break;
        };
        walked.push(next.clone());
        last = next;
        is_inclusive = false;

        place += 1;
        add(&index, &key_of(place % KEY_SPACE * 2 + 1), row(place, STAYING, RowKind::Value));
    }

    let mut previous: Option<&Vec<u8>> = None;
    for key in &walked {
        assert!(previous.is_none_or(|earlier| earlier < key));
        previous = Some(key);
    }
    for key in &expected {
        assert!(walked.contains(key));
    }
}

fn held_row(place: u64) -> WriteRow {
    row(place, STAYING, RowKind::Value)
}

fn random_keys(seed: u64, count: u64) -> Vec<Vec<u8>> {
    let mut random = SeededRandom::new(Seed(seed));
    (0..count)
        .map(|number| {
            let mut key = format!("{:016x}-{number}", random.next_word()).into_bytes();
            key.resize(key.len() + random.below(MOST_EXTRA_KEY_BYTES) as usize, b'k');
            key
        })
        .collect()
}

// a batch's need covers the pages its rows take, whatever its key lengths
#[test]
fn need_pages() {
    for seed in seeds([1, 2, 3]) {
        let keys = random_keys(seed, NEED_ROWS);
        let mut need = ShardNeed::default();
        for key in &keys {
            need.add(key.len());
        }
        let mut shard = WriteShard::new(&BATCH_SIZING).expect("build shard");
        assert!(shard.hold_room(&need), "seed {seed}");

        for (place, key) in keys.iter().enumerate() {
            let hash = hash_key(COLUMN, key);
            assert!(shard.insert_held(hash, COLUMN, key, &held_row(place as u64 + 1), true), "seed {seed}");
        }

        assert!(shard.trees[STAYING.0 as usize].batch_pages.pages <= need.pages(), "seed {seed}");
    }
}

// rows up to the limit the nodes promise always go in, falling keys being the order that leaves every node half full
#[test]
fn node_limit() {
    let mut shard = WriteShard::new(&BATCH_SIZING).expect("build shard");
    let limit = shard.node_row_limit();
    let keys: Vec<Vec<u8>> = (0..limit).rev().map(key_of).collect();
    let mut need = ShardNeed::default();
    for key in &keys {
        need.add(key.len());
    }
    assert!(shard.hold_room(&need));

    for (place, key) in keys.iter().enumerate() {
        let hash = hash_key(COLUMN, key);
        assert!(shard.insert_held(hash, COLUMN, key, &held_row(place as u64 + 1), true));
    }

    assert_eq!(shard.trees[STAYING.0 as usize].row_count, RowCount(limit));
    for key in &keys {
        let hash = hash_key(COLUMN, key);
        assert!(shard.lookup(hash, COLUMN, key, LATEST, both()).row.is_some());
    }
}

// a put beside a hold can't take the pages held, and gets them once they're given back
#[test]
fn hold_blocks() {
    let mut shard = WriteShard::new(&TIGHT).expect("build shard");
    let key = vec![b'k'; LONG_KEY_BYTES];
    let mut need = ShardNeed::default();
    while need.pages() < shard.slab.total_pages() {
        need.add(key.len());
    }
    assert!(shard.hold_room(&need));

    assert!(!shard.has_room(SEALED, key.len()));

    shard.release_room(&need);
    assert!(shard.has_room(SEALED, key.len()));
}

// a hold that a later shard can't take leaves nothing held in the shards before it
#[test]
fn hold_whole() {
    let index = WriteIndex::new(&TIGHT, TIGHT.pages).expect("build index");
    let key = key_of(1);
    let mut needs = [ShardNeed::default(); SHARD_COUNT];
    needs[0].add(key.len());
    while needs[SHARD_COUNT - 1].pages() <= index.shards.read(SHARD_COUNT - 1).slab.total_pages() {
        needs[SHARD_COUNT - 1].add(key.len());
    }

    assert!(!index.hold_room(&needs));

    assert_eq!(index.shards.read(0).held, ShardHold::default());
}

use reel::engine::engine_sizing::shard_for_rows;
use reel::index::entry_slab::MOST_INLINE_VALUE;

const PLANNED_ROWS: u64 = 10_000;
const SHORT_KEY_BYTES: usize = 8;

// rows of short keys that keep the longest values, then rows of wider keys with none: the kept values stay in their own share
// of the slab, so the shard still takes every row it was planned for, whatever the later keys' widths
#[test]
fn kept_values_leave_planned_rows_their_room() {
    let sizing = shard_for_rows(PLANNED_ROWS * SHARD_COUNT as u64, 4);
    for wide in [64_usize, 99] {
        let mut shard = WriteShard::new(&sizing).expect("build shard");
        let value = vec![3_u8; MOST_INLINE_VALUE];
        let mut rows = 0_u64;
        loop {
            let (key, kept) = if rows < PLANNED_ROWS / 2 {
                (rows.to_be_bytes()[8 - SHORT_KEY_BYTES..].to_vec(), Some(value.as_slice()))
            } else {
                let mut key = vec![0_u8; wide];
                key[..8].copy_from_slice(&rows.to_be_bytes());
                (key, None)
            };
            let row = row(rows + 1, SEALED, RowKind::Value);
            if !shard.insert(hash_key(COLUMN, &key), COLUMN, &key, &row, kept, true) {
                break;
            }
            rows += 1;
        }
        let kept: u64 = shard.trees.iter().map(|tree| tree.kept_bytes).sum();
        assert!(kept > 0, "no row kept its value");
        assert!(kept <= shard.kept_room(), "kept values took {kept} B of {} B", shard.kept_room());
        assert!(rows >= PLANNED_ROWS, "{wide} B keys: the shard took {rows} of {PLANNED_ROWS} planned rows");
    }
}

// a cursor stood after a shard's node pool handed its pages back once still stands on its leaf
#[test]
fn a_cursor_stands_after_a_pool_reset() {
    use reel::index::shard_cursor::TreeCursor;
    let index = WriteIndex::new(&SIZING, SIZING.pages).expect("index");
    // stand a fresh cursor on the slot's first row in every shard, and count the cursors that stand right after
    let standing = |index: &WriteIndex| {
        let (mut placed, mut stood) = (0, 0);
        for shard in 0..SHARD_COUNT {
            let guard = index.shards.read(shard);
            let tree = &guard.trees[STAYING.0 as usize];
            let mut cursor = TreeCursor::default();
            let bound = WalkBound { column: COLUMN, key: b"", is_inclusive: true, direction: Direction::Ascending };
            if guard.place_cursor(&mut cursor, tree, &bound).is_some() {
                placed += 1;
                stood += usize::from(guard.is_cursor_standing(&cursor));
            }
        }
        (placed, stood)
    };
    for number in 0..400 {
        add(&index, &key_of(number), row(number + 1, STAYING, RowKind::Value));
    }
    let before = standing(&index);
    // the slot's rows go, as after a seal, and the reserve job hands every free pool's pages back
    assert!(index.clean_slot(STAYING, CLEAN_ALL));
    assert!(index.step_node_pools(true), "no pool handed its pages back");
    let resets: Vec<u32> = (0..SHARD_COUNT).map(|shard| index.shards.read(shard).nodes.resets()).collect();
    for number in 0..400 {
        add(&index, &key_of(number), row(1_000 + number, STAYING, RowKind::Value));
    }
    let after = standing(&index);
    println!("cursors placed and standing before the hand-back: {before:?}, after it: {after:?}, pool resets {:?}", &resets[..4]);
    assert_eq!(before.0, before.1);
    assert_eq!(after.0, after.1, "a cursor stood after the pool's hand-back reads as not standing, so every step descends");
}
