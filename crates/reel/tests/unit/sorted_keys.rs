//! A sorted index answers every get and scan as a model of newest rows does, through splits, drops, moves and range removes

use std::collections::BTreeMap;

use reel::index::sorted_keys::{Floors, KeyBatch, KeyEntry, MOST_LOOKED, ScanFrom, SortedKeys, Upserted};
use reel::segment::segment_tail::SegmentNumber;
use reel_api::units::{ByteCount, FileOffset, RowCount, WritePlace};

const SEEDS: u64 = 40;
const ROWS: u64 = 30_000;
const KEYS: u64 = 4_000;
const WINDOW: usize = 64;
const BATCH: usize = 37;
const DEEP_SEEDS: u64 = 4;
const DEEP_KEYS: u64 = 40_000;
const DEEP_ROWS: u64 = 200_000;

struct Mixer(u64);

impl Mixer {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut mixed = self.0;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        mixed ^ (mixed >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

#[derive(Clone, Copy, Debug)]
struct Row {
    key: u64,
    entry: KeyEntry,
}

fn key_bytes(width: usize, key: u64) -> Vec<u8> {
    // a few first bytes, so parts hold many keys and some hold none
    let mut bytes = vec![0; width];
    bytes[0] = (key % 5) as u8 * 50;
    let tail = key.to_be_bytes();
    let length = (width - 1).min(8);
    bytes[width - length..].copy_from_slice(&tail[8 - length..]);
    bytes
}

fn entry(place: u64, is_grave: bool) -> KeyEntry {
    KeyEntry {
        place: WritePlace(place),
        segment: SegmentNumber(place / 1000),
        offset: FileOffset(place * 7 % 1_000_000),
        length: ByteCount(if is_grave { 0 } else { place % 999 + 1 }),
        is_grave,
    }
}

// rows with rising places, delivered out of order inside a window, as segments seal out of place order
fn rows_of(mixer: &mut Mixer, count: u64, keys: u64) -> Vec<Row> {
    let mut rows: Vec<Row> = (1..=count)
        .map(|place| Row {
            key: mixer.below(keys),
            entry: entry(place, mixer.below(4) == 0),
        })
        .collect();
    for start in (0..rows.len()).step_by(WINDOW) {
        let end = (start + WINDOW).min(rows.len());
        for at in start..end {
            let other = start + mixer.below((end - start) as u64) as usize;
            rows.swap(at, other);
        }
    }
    rows
}

// the lowest place a row not yet delivered has
fn floors_of(rows: &[Row]) -> Vec<WritePlace> {
    let mut floors = vec![WritePlace(u64::MAX); rows.len()];
    let mut lowest = u64::MAX;
    for at in (0..rows.len()).rev() {
        floors[at] = WritePlace(lowest);
        lowest = lowest.min(rows[at].entry.place.0);
    }
    floors
}

fn newest(model: &mut BTreeMap<Vec<u8>, KeyEntry>, key: Vec<u8>, entry: KeyEntry) {
    let held = model.entry(key).or_insert(entry);
    if entry.place > held.place {
        *held = entry;
    }
}

fn live(model: &BTreeMap<Vec<u8>, KeyEntry>) -> Vec<(Vec<u8>, KeyEntry)> {
    model.iter().filter(|(_key, entry)| !entry.is_grave).map(|(key, entry)| (key.clone(), *entry)).collect()
}

fn assert_matches(index: &SortedKeys, model: &BTreeMap<Vec<u8>, KeyEntry>, width: usize, keys: u64) {
    assert_eq!(index.check(), Ok(()));
    for key in 0..keys {
        let bytes = key_bytes(width, key);
        let found = index.find(&bytes).filter(|entry| !entry.is_grave);
        let expected = model.get(&bytes).filter(|entry| !entry.is_grave);
        assert_eq!(found, expected.copied(), "key {key}");
    }
    let mut seen = Vec::new();
    index.visit(|key, entry| {
        if !entry.is_grave {
            seen.push((key.to_vec(), *entry));
        }
    });
    assert_eq!(seen, live(model));
}

fn scan(index: &SortedKeys, width: usize, is_descending: bool, start: Option<(Vec<u8>, bool)>) -> Vec<(Vec<u8>, KeyEntry)> {
    let mut batch = KeyBatch::new(width, BATCH);
    let mut out: Vec<(Vec<u8>, KeyEntry)> = Vec::new();
    let mut from = start;
    loop {
        let scan_from = match &from {
            None => ScanFrom::Edge,
            Some((key, is_inclusive)) => ScanFrom::Key { key, is_inclusive: *is_inclusive },
        };
        let is_done = if is_descending { index.read_before(scan_from, BATCH, &mut batch) } else { index.read_after(scan_from, BATCH, &mut batch) };
        for at in 0..batch.len() {
            out.push((batch.key(at).to_vec(), batch.entries[at]));
        }
        if is_done {
            return out;
        }
        from = Some((batch.resume_key().to_vec(), false));
    }
}

fn run_seed(seed: u64, width: usize, is_many: bool, keys: u64, row_count: u64) {
    let mut mixer = Mixer(seed);
    let index = SortedKeys::new(width, RowCount(keys));
    let rows = rows_of(&mut mixer, row_count, keys);
    let floors = floors_of(&rows);
    let mut model = BTreeMap::new();
    if is_many {
        // runs in key order, as a seal hands them over, each under the floor of its last row
        for (start, chunk) in rows.chunks(WINDOW).enumerate() {
            let mut sorted = chunk.to_vec();
            sorted.sort_by(|left, right| key_bytes(width, left.key).cmp(&key_bytes(width, right.key)).then(right.entry.place.cmp(&left.entry.place)));
            sorted.dedup_by_key(|row| row.key);
            let keys: Vec<u8> = sorted.iter().flat_map(|row| key_bytes(width, row.key)).collect();
            let entries: Vec<KeyEntry> = sorted.iter().map(|row| row.entry).collect();
            let floor = floors[(start * WINDOW + chunk.len() - 1).min(rows.len() - 1)];
            index.upsert_many(&keys, &entries, Floors::at(floor), |_row, upserted| assert_ne!(upserted, Upserted::NoRoom));
            for row in chunk {
                newest(&mut model, key_bytes(width, row.key), row.entry);
            }
        }
    } else {
        for (at, row) in rows.iter().enumerate() {
            assert_ne!(index.upsert(&key_bytes(width, row.key), row.entry, Floors::at(floors[at])), Upserted::NoRoom);
            newest(&mut model, key_bytes(width, row.key), row.entry);
        }
    }
    assert_matches(&index, &model, width, keys);

    let expected = live(&model);
    assert_eq!(scan(&index, width, false, None), expected);
    let mut reversed = expected.clone();
    reversed.reverse();
    assert_eq!(scan(&index, width, true, None), reversed);
    for _probe in 0..20 {
        let start = key_bytes(width, mixer.below(keys));
        let is_inclusive = mixer.below(2) == 0;
        let after: Vec<_> = expected.iter().filter(|(key, _entry)| if is_inclusive { *key >= start } else { *key > start }).cloned().collect();
        assert_eq!(scan(&index, width, false, Some((start.clone(), is_inclusive))), after);
        let mut before: Vec<_> = expected.iter().filter(|(key, _entry)| if is_inclusive { *key <= start } else { *key < start }).cloned().collect();
        before.reverse();
        assert_eq!(scan(&index, width, true, Some((start, is_inclusive))), before);
    }

    // a merge moves the entries whose place it kept, and no other
    for (key, entry) in model.iter_mut() {
        let segment = SegmentNumber(entry.segment.0 + 1_000);
        let is_kept = mixer.below(2) == 0;
        let place = if is_kept { entry.place } else { WritePlace(entry.place.0 + 1) };
        let is_held = index.find(key) == Some(*entry);
        let moved = index.move_entry(key, place, segment, FileOffset(5));
        assert_eq!(moved, is_kept && is_held);
        if moved {
            entry.segment = segment;
            entry.offset = FileOffset(5);
        }
    }
    assert_matches(&index, &model, width, keys);

    // a cover takes out the older entries in its range
    for _cover in 0..6 {
        let (low, high) = (mixer.below(keys), mixer.below(keys));
        let (start, end) = (key_bytes(width, low.min(high)), key_bytes(width, low.max(high)));
        let below = WritePlace(mixer.below(row_count));
        let is_open = mixer.below(4) == 0;
        let end = (!is_open).then_some(end);
        index.remove_range(&start, end.as_deref(), below);
        model.retain(|key, entry| *key < start || end.as_ref().is_some_and(|end| key >= end) || entry.place >= below);
        assert_matches(&index, &model, width, keys);
    }

    if keys == DEEP_KEYS {
        assert!(index.deepest() >= 3, "{} levels", index.deepest());
    }

    // graves past the floor drop every entry, and every page goes back
    for key in 0..keys {
        index.upsert(&key_bytes(width, key), entry(row_count + 1 + key, true), Floors::at(WritePlace(u64::MAX)));
    }
    assert_eq!(index.check(), Ok(()));
    assert_eq!(index.entry_count(), RowCount(0));
    assert_eq!(index.free_pages(), index.page_count());
}

#[test]
fn matches_the_model_one_row_at_a_time() {
    for seed in 0..SEEDS {
        for width in [3, 16, 17, 64] {
            run_seed(seed, width, false, KEYS, ROWS);
        }
    }
}

#[test]
fn matches_the_model_in_runs() {
    for seed in 0..SEEDS {
        for width in [3, 16, 17, 64] {
            run_seed(seed + 1_000, width, true, KEYS, ROWS);
        }
    }
}

// a grave sealed before an older value of its key keeps the value out, until the floor passes it
#[test]
fn a_grave_holds_back_an_older_value() {
    let index = SortedKeys::new(16, RowCount(100));
    let key = [9; 16];
    assert_eq!(index.upsert(&key, entry(20, true), Floors::at(WritePlace(10))), Upserted::Added);
    assert_eq!(index.upsert(&key, entry(10, false), Floors::at(WritePlace(11))), Upserted::Kept);
    assert!(index.find(&key).is_some_and(|entry| entry.is_grave));
    assert_eq!(index.upsert(&key, entry(30, true), Floors::at(WritePlace(40))), Upserted::Dropped);
    assert_eq!(index.find(&key), None);
    assert_eq!(index.check(), Ok(()));
}

// keys in order fill each leaf whole
#[test]
fn keys_in_order_fill_their_leaves() {
    let rows = 50_000;
    let index = SortedKeys::new(16, RowCount(rows));
    for key in 0..rows {
        let mut bytes = [0; 16];
        bytes[8..].copy_from_slice(&key.to_be_bytes());
        assert_eq!(index.upsert(&bytes, entry(key + 1, false), Floors::at(WritePlace(0))), Upserted::Added);
    }
    assert_eq!(index.check(), Ok(()));
    let used = index.page_count() - index.free_pages();
    let leaves = rows.div_ceil(index.leaf_capacity() as u64) as u32;
    assert!(used <= leaves + leaves.div_ceil(64) + 2, "{used} pages for {leaves} full leaves");
}

// a pool with no page left marks the row's segment, and a clear after a refill takes the mark away
#[test]
fn past_capacity_sets_a_mark() {
    let index = SortedKeys::new(16, RowCount(1));
    let mut no_room = None;
    for key in 0..1_000_000u64 {
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&key.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes());
        let mut row = entry(key + 1, false);
        row.segment = SegmentNumber(7);
        if index.upsert(&bytes, row, Floors::at(WritePlace(0))) == Upserted::NoRoom {
            no_room = Some(key);
            break;
        }
    }
    assert!(no_room.is_some());
    assert_eq!(index.check(), Ok(()));
    assert_eq!(index.mark(), Some(SegmentNumber(7)));
    assert_eq!(index.past_capacity(), RowCount(1));
    assert!(index.clear_mark(SegmentNumber(7), RowCount(1)));
    assert_eq!(index.mark(), None);
}

// wide keys and many rows reach three levels, so inner pages split and empty
#[test]
fn matches_the_model_three_levels_deep() {
    for seed in 0..DEEP_SEEDS {
        run_seed(seed + 2_000, 64, seed % 2 == 0, DEEP_KEYS, DEEP_ROWS);
    }
}

fn ordered_key(key: u64) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes[8..].copy_from_slice(&key.to_be_bytes());
    bytes
}

// a full leaf keeps its graves over the floor when it makes room, and drops the ones under it
#[test]
fn a_full_leaf_drops_only_graves_under_the_floor() {
    let index = SortedKeys::new(16, RowCount(10_000));
    let capacity = index.leaf_capacity() as u64;
    for key in 0..capacity {
        assert_eq!(index.upsert(&ordered_key(key), entry(1_000 + key, true), Floors::at(WritePlace(0))), Upserted::Added);
    }
    assert_eq!(index.upsert(&ordered_key(capacity), entry(5_000, false), Floors::at(WritePlace(0))), Upserted::Added);
    for key in 0..capacity {
        assert_eq!(index.upsert(&ordered_key(key), entry(key + 1, false), Floors::at(WritePlace(0))), Upserted::Kept, "key {key}");
    }
    assert_eq!(index.check(), Ok(()));

    let index = SortedKeys::new(16, RowCount(10_000));
    for key in 0..capacity {
        index.upsert(&ordered_key(key), entry(key + 1, true), Floors::at(WritePlace(0)));
    }
    assert_eq!(index.upsert(&ordered_key(capacity), entry(5_000, false), Floors::at(WritePlace(capacity + 1))), Upserted::Added);
    assert_eq!(index.entry_count(), RowCount(1));
    assert_eq!(index.page_count() - index.free_pages(), 1);
    assert_eq!(index.check(), Ok(()));
}

// a root left with one child folds into it
#[test]
fn a_root_with_one_child_folds() {
    let index = SortedKeys::new(16, RowCount(10_000));
    let capacity = index.leaf_capacity() as u64;
    for key in 0..2 * capacity {
        index.upsert(&ordered_key(key), entry(key + 1, false), Floors::at(WritePlace(0)));
    }
    assert_eq!(index.deepest(), 2);
    assert_eq!(index.remove_range(&ordered_key(0), Some(&ordered_key(capacity)), WritePlace(u64::MAX)), capacity);
    assert_eq!(index.check(), Ok(()));
    assert_eq!(index.deepest(), 1);
}

// a scan over a run of graves stops after a fixed count of entries and goes on from the last one it looked at
#[test]
fn a_scan_over_graves_stops_and_goes_on() {
    let index = SortedKeys::new(16, RowCount(10_000));
    let graves = 1_000;
    for key in 0..graves {
        index.upsert(&ordered_key(key), entry(key + 1, true), Floors::at(WritePlace(0)));
    }
    index.upsert(&ordered_key(graves), entry(graves + 1, false), Floors::at(WritePlace(0)));
    let mut batch = KeyBatch::new(16, 10);
    let mut from: Option<Vec<u8>> = None;
    let mut calls = 0;
    loop {
        let scan_from = match &from {
            None => ScanFrom::Edge,
            Some(key) => ScanFrom::Key { key: key.as_slice(), is_inclusive: false },
        };
        let is_done = index.read_after(scan_from, 10, &mut batch);
        calls += 1;
        if is_done {
            break;
        }
        assert!(batch.is_empty(), "graves come before the value");
        from = Some(batch.resume_key().to_vec());
    }
    assert_eq!(batch.len(), 1);
    assert_eq!(calls, (graves + 1).div_ceil(MOST_LOOKED as u64) as usize);
}

// two versions of a key in one run, newest first as a seal hands them over, leave one entry
#[test]
fn a_run_with_two_versions_of_a_key_keeps_one_entry() {
    let index = SortedKeys::new(16, RowCount(10_000));
    index.upsert(&ordered_key(0), entry(1, false), Floors::at(WritePlace(0)));
    let mut keys = Vec::new();
    let mut entries = Vec::new();
    for key in 1..50u64 {
        keys.extend_from_slice(&ordered_key(key));
        entries.push(entry(1_000 + key, false));
        keys.extend_from_slice(&ordered_key(key));
        entries.push(entry(key, false));
    }
    index.upsert_many(&keys, &entries, Floors::at(WritePlace(0)), |_row, _upserted| {});
    assert_eq!(index.check(), Ok(()));
    assert_eq!(index.entry_count(), RowCount(50));
    for key in 1..50u64 {
        assert_eq!(index.find(&ordered_key(key)).map(|entry| entry.place), Some(WritePlace(1_000 + key)));
    }
}

// a start of another width takes the stored key it packs to only where that key sorts on the scan's side of the start, either
// flag, both ways
#[test]
fn starts_of_another_width() {
    let index = SortedKeys::new(4, RowCount(100));
    let stored = [[1, 2, 0, 0], [1, 2, 3, 4], [1, 2, 3, 5]];
    for (place, key) in stored.iter().enumerate() {
        index.upsert(key, entry(place as u64 + 1, false), Floors::at(WritePlace(0)));
    }
    let scan = |start: &[u8], is_inclusive: bool, is_ascending: bool| {
        let mut batch = KeyBatch::new(4, 10);
        let from = ScanFrom::Key { key: start, is_inclusive };
        let is_done = if is_ascending { index.read_after(from, 10, &mut batch) } else { index.read_before(from, 10, &mut batch) };
        assert!(is_done);
        (0..batch.len()).map(|at| batch.key(at).to_vec()).collect::<Vec<_>>()
    };
    for is_inclusive in [false, true] {
        // [1, 2] sorts below [1, 2, 0, 0], so going up takes it and going down doesn't
        assert_eq!(scan(&[1, 2], is_inclusive, true), stored.iter().map(|key| key.to_vec()).collect::<Vec<_>>());
        assert_eq!(scan(&[1, 2], is_inclusive, false), Vec::<Vec<u8>>::new());
        // [1, 2, 3, 4, 9] sorts above [1, 2, 3, 4], so going up skips it and going down takes it
        assert_eq!(scan(&[1, 2, 3, 4, 9], is_inclusive, true), vec![vec![1, 2, 3, 5]]);
        assert_eq!(scan(&[1, 2, 3, 4, 9], is_inclusive, false), vec![vec![1, 2, 3, 4], vec![1, 2, 0, 0]]);
    }
    assert_eq!(scan(&[1, 2, 3, 4], false, true), vec![vec![1, 2, 3, 5]]);
    assert_eq!(scan(&[1, 2, 3, 4], true, false), vec![vec![1, 2, 3, 4], vec![1, 2, 0, 0]]);
}

// a width that fills no whole word packs a long start's extra bytes into the last word's padding, and orders it all the same
#[test]
fn starts_past_a_part_word() {
    let index = SortedKeys::new(6, RowCount(100));
    let stored = [[1, 2, 3, 4, 5, 6], [1, 2, 3, 4, 5, 7]];
    for (place, key) in stored.iter().enumerate() {
        index.upsert(key, entry(place as u64 + 1, false), Floors::at(WritePlace(0)));
    }
    let scan = |start: &[u8], is_ascending: bool| {
        let mut batch = KeyBatch::new(6, 10);
        let from = ScanFrom::Key { key: start, is_inclusive: true };
        let is_done = if is_ascending { index.read_after(from, 10, &mut batch) } else { index.read_before(from, 10, &mut batch) };
        assert!(is_done);
        (0..batch.len()).map(|at| batch.key(at).to_vec()).collect::<Vec<_>>()
    };
    // both seven-byte starts sort above [1, 2, 3, 4, 5, 6] and below [1, 2, 3, 4, 5, 7]
    for start in [[1, 2, 3, 4, 5, 6, 0], [1, 2, 3, 4, 5, 6, 9]] {
        assert_eq!(scan(&start, true), vec![vec![1, 2, 3, 4, 5, 7]]);
        assert_eq!(scan(&start, false), vec![vec![1, 2, 3, 4, 5, 6]]);
    }
}

// the budget's figure for a sorted index is the heap the built index holds
#[test]
fn planned_bytes_match_the_built_index() {
    for (width, rows) in [(8, 1_000), (16, 100_000), (17, 2_000_000), (64, 300_000)] {
        let index = SortedKeys::new(width, RowCount(rows));
        assert_eq!(reel::index::sorted_keys::planned_index_bytes(width, RowCount(rows)), index.heap_bytes(), "{width} B keys, {rows} rows");
    }
}
