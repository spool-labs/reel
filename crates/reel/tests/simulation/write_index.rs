//! The write index against a plain model, with the seal switch stepped by hand

use std::collections::{BTreeMap, BTreeSet};

use reel::index::entry_slab::PageCount;
use reel::index::node_pool::NodeCount;
use reel::index::open_slots::{OpenSlot, OpenSlots};
use reel::index::seal_walk::SealWalk;
use reel::index::shard_cursor::ShardCursor;
use reel::index::write_index::{SHARD_COUNT, WriteIndex};
use reel::index::write_row::WriteRow;
use reel::index::write_shard::{ShardSizing, WalkBound};
use reel::io::simulated::seeded_random::{Seed, SeededRandom};
use reel::row_kind::RowKind;
use reel::segment::segment_tail::SegmentNumber;
use reel_api::column_id::ColumnId;
use reel_api::direction::Direction;
use reel_api::units::{ByteCount, FileOffset, RowCount, WritePlace};

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
};
const TIGHT: ShardSizing = ShardSizing {
    pages: PageCount(8),
    nodes: NodeCount(64),
    table_slots: 8_192,
    open_slots: 4,
};
const SMALL_TABLE: ShardSizing = ShardSizing {
    pages: PageCount(256),
    nodes: NodeCount(512),
    table_slots: 64,
    open_slots: 4,
};
const LATEST: WritePlace = WritePlace(u64::MAX - 1);
const MANY_VERSIONS: u64 = 3_000;

fn row(place: u64, slot: OpenSlot, kind: RowKind) -> WriteRow {
    WriteRow {
        place: WritePlace(place),
        offset: FileOffset(place * 10),
        length: ByteCount(place),
        kind,
        slot,
        segment: SegmentNumber(1),
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
    assert!(index.insert(COLUMN, key, &written));
}

fn drained(walk: &mut SealWalk, index: &WriteIndex) -> Vec<(Vec<u8>, u64)> {
    let mut rows = Vec::new();
    while let Some(shard) = walk.next_shard() {
        let head = &walk.heads[shard];
        let place = head.row.expect("row").place.0;
        rows.push((head.key.clone(), place));
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
    for seed in 0..SEED_COUNT {
        let mut random = SeededRandom::new(Seed(seed));
        let index = WriteIndex::new(&SIZING);
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
        let index = WriteIndex::new(&SIZING);
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
    let index = WriteIndex::new(&SIZING);
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

// after the switch a closed slot answers nothing, before any cleanup has run
#[test]
fn switch_before_cleanup() {
    let index = WriteIndex::new(&SIZING);
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
    let index = WriteIndex::new(&SIZING);
    add(&index, b"gone", row(3, SEALED, RowKind::Grave));

    let after = index.lookup(COLUMN, b"gone", LATEST, after_seal());

    assert_eq!(after.row, None);
    assert!(after.should_ask_footers);
}

// part way through cleanup no reader can tell which rows are gone
#[test]
fn switch_mid_cleanup() {
    let index = WriteIndex::new(&SIZING);
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

// a view loaded before the switch either finds the row or goes to the footers
#[test]
fn view_before_switch() {
    let index = WriteIndex::new(&SIZING);
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
    let index = WriteIndex::new(&SIZING);
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
    let index = WriteIndex::new(&SIZING);
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
    let index = WriteIndex::new(&SIZING);
    add(&index, b"key", row(5, STAYING, RowKind::Value));
    add(&index, b"key", row(10, SEALED, RowKind::Value));
    assert!(index.clean_slot(SEALED, CLEAN_ALL));

    let found = index.lookup(COLUMN, b"key", WritePlace(7), after_seal());

    assert_eq!(found.row.expect("older row").place, WritePlace(5));
}

// a slot takes rows again once cleanup has freed it
#[test]
fn slot_reuse() {
    let index = WriteIndex::new(&TIGHT);
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
    let index = WriteIndex::new(&TIGHT);
    let mut taken = Vec::new();
    for number in 0..ROWS * 8 {
        let key = key_of(number);
        if !index.insert(COLUMN, &key, &row(number + 1, SEALED, RowKind::Value)) {
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
    let index = WriteIndex::new(&SIZING);
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
    let index = WriteIndex::new(&SIZING);
    add(&index, b"key", row(5, STAYING, RowKind::Grave));
    add(&index, b"key", row(10, SEALED, RowKind::Value));
    assert!(index.clean_slot(SEALED, CLEAN_ALL));

    assert!(!index.is_newest_grave(COLUMN, b"key", LATEST, after_seal()));
}

// walks reach every key once in both directions and stay in their column
#[test]
fn walk_keys() {
    let index = WriteIndex::new(&SIZING);
    let mut expected: BTreeSet<Vec<u8>> = BTreeSet::new();
    let mut random = SeededRandom::new(Seed(2));
    for place in 1..=ROWS {
        let key = key_of(random.below(KEY_SPACE));
        let slot = if random.one_in(2) { SEALED } else { STAYING };
        add(&index, &key, row(place, slot, RowKind::Value));
        expected.insert(key);
    }
    let other = ColumnId(COLUMN.0 + 1);
    assert!(index.insert(other, b"key-99999", &row(ROWS + 1, SEALED, RowKind::Value)));

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

// a shard refuses a row when its table is near full, and never spins in a probe
#[test]
fn table_limit() {
    let index = WriteIndex::new(&SMALL_TABLE);
    let mut taken = 0;
    for number in 0..ROWS {
        if index.insert(COLUMN, &key_of(number), &row(number + 1, SEALED, RowKind::Value)) {
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
    let index = WriteIndex::new(&SIZING);
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
    let index = WriteIndex::new(&SIZING);
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
    let index = WriteIndex::new(&SIZING);
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
    let index = WriteIndex::new(&SIZING);
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
