//! Scans: keys in order in both directions, both ends honored, and a walk that outlives whatever the jobs do under it

use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use reel::config::{ColumnConfig, ColumnIndex, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::engine::read_path::MOST_READ_TRIES;
use reel::engine::scan_state::{MOST_SCAN_SOURCES, MOST_WINDOW_RECORD};
use reel::engine::store_scan::StoreScan;
use reel::engine::walk_job::FREE_IN_PLACE_BYTES;
use reel::guard::hold;
use reel::io::{Io, IoFile};
use reel::record::codec::Codec;
use reel_core::column_id::ColumnId;
use reel_core::direction::Direction;
use reel_core::scan::ScanStep;
use reel_core::store_error::StoreError;
use reel_core::units::{ByteCount, FileOffset, Nanoseconds, VolumeId};
use reel_sim::file_image::BitMask;
use reel_sim::seeded_random::{Seed, SeededRandom};
use reel_sim::simulated_io::SimulatedIo;

use crate::seed_sets::seeds;

const ROOT: &str = "/disk/scans";
const COLUMN: ColumnId = ColumnId(1);
const OTHER_COLUMN: ColumnId = ColumnId(2);
const BUDGET: ByteCount = ByteCount(68 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 400_000;
const KEYS: u64 = 200;
const DEAD_KEYS: u64 = 14_000;
const HOT_VERSIONS: u64 = 6_000;
const LONG_SHARED: usize = 70;
const LONG_GROUP_AT: usize = 55;
const LONG_KEYS: u64 = 9_000;
const RANDOM_KEYS: u64 = 300;
const RANDOM_ROUNDS: u64 = 400;
const FIXED_SEEDS: [u64; 6] = [0, 1, 2, 3, 4, 5];
const HOOK_STEPS: u64 = 20_000;
const WINDOW_KEYS: u64 = 2_000;
const BAD_RECORD_AT: u64 = 4_000;

type Store = ReelStore<SimulatedIo>;
type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn open(io: &SimulatedIo) -> Store {
    let column = |id, name: &str| ColumnConfig {
        id,
        name: name.to_string(),
        key_shape: KeyShape::Variable { most: 96 },
        codec: Codec::None,
        volume: VolumeId(0),
        index: ColumnIndex::Plain,
        least_rows: None,
        least_value: None,
    };
    let config = StoreConfig {
        memory_budget: BUDGET,
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "scans".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(), open_segments: None, segment_bytes: None, lifetime_groups: None,
        }],
        columns: vec![column(COLUMN, "column"), column(OTHER_COLUMN, "other")],
    };
    ReelStore::open(io.clone(), &config).expect("open")
}

fn settle(io: &SimulatedIo) {
    for _step in 0..STEPS {
        if io.step_any().is_none() {
            io.advance_clock(TICK);
            if io.step_any().is_none() {
                return;
            }
        }
    }
    panic!("jobs never went quiet");
}

fn seal(store: &Store, io: &SimulatedIo) {
    store.seal_now();
    settle(io);
}

fn hold_compaction(store: &Store, is_held: bool) {
    store.state.volumes[0].holds.compaction.store(is_held, Ordering::Release);
    store.state.ask_compaction();
}

fn key(number: u64) -> Vec<u8> {
    format!("key-{number:05}").into_bytes()
}

fn put(store: &Store, model: &mut Model, number: u64, value: &[u8]) {
    store.put(COLUMN, &key(number), value).expect("put");
    model.insert(key(number), value.to_vec());
}

fn rows(mut scan: StoreScan<SimulatedIo>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let (mut key, mut value, mut found) = (Vec::new(), Vec::new(), Vec::new());
    loop {
        match scan.next_row(&mut key, &mut value).expect("step") {
            ScanStep::Found => found.push((key.clone(), value.clone())),
            ScanStep::Corrupt => panic!("a corrupt record"),
            ScanStep::End => return found,
        }
    }
}

fn keys(mut scan: StoreScan<SimulatedIo>) -> Vec<Vec<u8>> {
    let (mut key, mut found) = (Vec::new(), Vec::new());
    loop {
        match scan.next_key(&mut key).expect("step") {
            ScanStep::Found => found.push(key.clone()),
            ScanStep::Corrupt => panic!("a corrupt record"),
            ScanStep::End => return found,
        }
    }
}

fn forward(model: &Model) -> Vec<(Vec<u8>, Vec<u8>)> {
    model.iter().map(|(key, value)| (key.clone(), value.clone())).collect()
}

fn backward(model: &Model) -> Vec<(Vec<u8>, Vec<u8>)> {
    model.iter().rev().map(|(key, value)| (key.clone(), value.clone())).collect()
}

fn check_whole(store: &Store, model: &Model) {
    assert_eq!(rows(store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan")), forward(model));
    assert_eq!(rows(store.iter_from(COLUMN, b"zzz", Direction::Descending).expect("scan")), backward(model));
}

// an empty store has nothing to walk, in either direction
#[test]
fn empty_store() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io);
    check_whole(&store, &Model::new());
    store.close().expect("close");
}

// open rows alone come out in order, both ways, with a key's newest value
#[test]
fn open_rows() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io);
    let mut model = Model::new();
    for number in (0..KEYS).rev() {
        put(&store, &mut model, number, b"first");
    }
    put(&store, &mut model, 7, b"second");
    store.delete(COLUMN, &key(9)).expect("delete");
    model.remove(&key(9));
    check_whole(&store, &model);
    store.close().expect("close");
}

// sealed rows and open rows merge, a newer row or grave in either place wins, and a key comes once
#[test]
fn sealed_and_open() {
    let io = SimulatedIo::new(Seed(3));
    let store = open(&io);
    hold_compaction(&store, true);
    let mut model = Model::new();
    for number in 0..KEYS {
        put(&store, &mut model, number, b"first");
    }
    seal(&store, &io);
    for number in (0..KEYS).step_by(3) {
        put(&store, &mut model, number, b"second");
    }
    seal(&store, &io);
    for number in (0..KEYS).step_by(5) {
        store.delete(COLUMN, &key(number)).expect("delete");
        model.remove(&key(number));
    }
    seal(&store, &io);
    for number in (0..KEYS).step_by(7) {
        put(&store, &mut model, number, b"third");
    }
    check_whole(&store, &model);
    store.close().expect("close");
}

// both ends hold in both directions, a start between keys starts at the next one, and the end key stays out
#[test]
fn ends_honored() {
    let io = SimulatedIo::new(Seed(4));
    let store = open(&io);
    hold_compaction(&store, true);
    let mut model = Model::new();
    for number in 0..KEYS {
        put(&store, &mut model, number, b"value");
        if number == KEYS / 2 {
            seal(&store, &io);
        }
    }
    let (low, high) = (key(50), key(150));
    let expected: Vec<Vec<u8>> = model.range(low.clone()..high.clone()).map(|(key, _value)| key.clone()).collect();
    let mut reversed = expected.clone();
    reversed.reverse();
    assert_eq!(keys(store.iter_range(COLUMN, &low, &high, Direction::Ascending).expect("scan")), expected);
    assert_eq!(keys(store.iter_range(COLUMN, &low, &high, Direction::Descending).expect("scan")), reversed);

    let between = b"key-00050x";
    let up: Vec<Vec<u8>> = model.range::<[u8], _>((Bound::Included(&between[..]), Bound::Unbounded)).map(|(key, _value)| key.clone()).collect();
    assert_eq!(keys(store.iter_from(COLUMN, between, Direction::Ascending).expect("scan")), up);
    let down: Vec<Vec<u8>> = model.range::<[u8], _>((Bound::Unbounded, Bound::Included(&between[..]))).rev().map(|(key, _value)| key.clone()).collect();
    assert_eq!(keys(store.iter_from(COLUMN, between, Direction::Descending).expect("scan")), down);
    assert!(keys(store.iter_range(COLUMN, &high, &low, Direction::Ascending).expect("scan")).is_empty());
    assert!(keys(store.iter_range(COLUMN, &low, &low, Direction::Descending).expect("scan")).is_empty());
    store.close().expect("close");
}

// a prefix walk stops at the prefix's end, and a prefix of all high bytes runs to the column's end
#[test]
fn prefix_walk() {
    let io = SimulatedIo::new(Seed(5));
    let store = open(&io);
    for name in [&b"a"[..], b"ab", b"ab\xff", b"ac", &[0xff, 0xff], &[0xff, 0xff, 1], &[0xff, 0xfe]] {
        store.put(COLUMN, name, b"value").expect("put");
    }
    seal(&store, &io);
    store.put(COLUMN, b"ab1", b"value").expect("put");
    let expected: [&[u8]; 3] = [b"ab", b"ab1", b"ab\xff"];
    assert_eq!(keys(store.iter_prefix(COLUMN, b"ab").expect("scan")), expected);
    let high: [&[u8]; 2] = [&[0xff, 0xff], &[0xff, 0xff, 1]];
    assert_eq!(keys(store.iter_prefix(COLUMN, &[0xff, 0xff]).expect("scan")), high);
    store.close().expect("close");
}

// a column's keys stay out of another's walk, sealed or open
#[test]
fn columns_apart() {
    let io = SimulatedIo::new(Seed(6));
    let store = open(&io);
    hold_compaction(&store, true);
    let mut model = Model::new();
    for number in 0..KEYS {
        put(&store, &mut model, number, b"value");
        store.put(OTHER_COLUMN, &key(number + 1), b"other").expect("put");
    }
    seal(&store, &io);
    put(&store, &mut model, KEYS, b"value");
    store.put(OTHER_COLUMN, &key(KEYS + 5), b"other").expect("put");
    check_whole(&store, &model);
    assert_eq!(keys(store.iter_from(OTHER_COLUMN, b"", Direction::Ascending).expect("scan")).len(), KEYS as usize + 1);
    store.close().expect("close");
}

// a range delete hides what it covers from a walk at once, and a row written after it stays
#[test]
fn range_delete_hides() {
    let io = SimulatedIo::new(Seed(7));
    let store = open(&io);
    hold_compaction(&store, true);
    let mut model = Model::new();
    for number in 0..KEYS {
        put(&store, &mut model, number, b"value");
    }
    seal(&store, &io);
    store.delete_range(COLUMN, &key(20), &key(120)).expect("delete");
    model.retain(|name, _value| name.as_slice() < key(20).as_slice() || name.as_slice() >= key(120).as_slice());
    put(&store, &mut model, 60, b"newer");
    check_whole(&store, &model);
    seal(&store, &io);
    check_whole(&store, &model);
    store.close().expect("close");
}

// a walk at a cue sees that point, whatever is written, deleted or merged later
#[test]
fn cue_walk() {
    let io = SimulatedIo::new(Seed(8));
    let store = open(&io);
    let mut model = Model::new();
    for number in 0..KEYS {
        put(&store, &mut model, number, b"value");
    }
    seal(&store, &io);
    let cue = store.cue().expect("cue");
    let expected = forward(&model);

    for number in (0..KEYS).step_by(2) {
        store.put(COLUMN, &key(number), b"newer").expect("put");
    }
    for number in (1..KEYS).step_by(4) {
        store.delete(COLUMN, &key(number)).expect("delete");
    }
    store.delete_range(COLUMN, &key(10), &key(40)).expect("delete");
    store.put(COLUMN, b"key-new", b"newer").expect("put");
    seal(&store, &io);
    assert_eq!(rows(cue.iter_from(COLUMN, b"", Direction::Ascending).expect("scan")), expected);
    let mut reversed = expected.clone();
    reversed.reverse();
    assert_eq!(rows(cue.iter_from(COLUMN, b"zzz", Direction::Descending).expect("scan")), reversed);
    drop(cue);
    store.close().expect("close");
}

// a walk at a cue keeps reading after the cue goes, since it holds a slot of its own
#[test]
fn cue_walk_outlives_the_cue() {
    let io = SimulatedIo::new(Seed(9));
    let store = open(&io);
    let mut model = Model::new();
    for number in 0..KEYS {
        put(&store, &mut model, number, b"value");
    }
    seal(&store, &io);
    let cue = store.cue().expect("cue");
    let scan = cue.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
    drop(cue);
    store.delete_range(COLUMN, b"", b"zzz").expect("delete");
    seal(&store, &io);
    assert_eq!(rows(scan), forward(&model));
    store.close().expect("close");
}

// a walk goes on from its last key through a seal, a merge and an unlink, with no key twice and none missed
#[test]
fn outlives_the_jobs() {
    for direction in [Direction::Ascending, Direction::Descending] {
        let io = SimulatedIo::new(Seed(10));
        let store = open(&io);
        let mut model = Model::new();
        for number in 0..KEYS {
            put(&store, &mut model, number, b"value");
            if number % 40 == 0 {
                seal(&store, &io);
            }
        }
        let start: &[u8] = if direction == Direction::Ascending { b"" } else { b"zzz" };
        let mut scan = store.iter_from(COLUMN, start, direction).expect("scan");
        let (mut name, mut value, mut seen) = (Vec::new(), Vec::new(), Vec::new());
        for step in 0..KEYS {
            assert_eq!(scan.next_row(&mut name, &mut value).expect("step"), ScanStep::Found);
            seen.push(name.clone());
            if step % 30 == 0 {
                for number in 0..KEYS {
                    store.put(COLUMN, &key(number), b"value").expect("put");
                }
                seal(&store, &io);
            }
        }
        assert_eq!(scan.next_row(&mut name, &mut value).expect("step"), ScanStep::End);
        let mut expected: Vec<Vec<u8>> = model.keys().cloned().collect();
        if direction == Direction::Descending {
            expected.reverse();
        }
        assert_eq!(seen, expected);
        drop(scan);
        store.close().expect("close");
    }
}

// more segments over a key than a scan has places goes by seeks, with the same answer
#[test]
fn more_segments_than_sources() {
    let io = SimulatedIo::new(Seed(11));
    let store = open(&io);
    hold_compaction(&store, true);
    let mut model = Model::new();
    let count = MOST_SCAN_SOURCES as u64 + 8;
    for round in 0..count {
        put(&store, &mut model, round, b"first");
        put(&store, &mut model, count + round, b"edge");
        put(&store, &mut model, 3 * count, b"last");
        put(&store, &mut model, count / 2, format!("value-{round}").as_bytes());
        seal(&store, &io);
    }
    check_whole(&store, &model);
    store.close().expect("close");
}

// the pool's scans run out, and one that ends hands its place to the next
#[test]
fn scans_run_out() {
    let io = SimulatedIo::new(Seed(12));
    let store = open(&io);
    let mut scans = Vec::new();
    for _scan in 0..store.state.read_pool.scan_count {
        scans.push(store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan"));
    }
    let refused = store.iter_from(COLUMN, b"", Direction::Ascending);
    assert!(matches!(refused, Err(StoreError::ScansFull { limit }) if limit == store.state.read_pool.scan_count as u64));
    scans.pop();
    assert!(store.iter_from(COLUMN, b"", Direction::Ascending).is_ok());
    drop(scans);
    store.close().expect("close");
}

// a walk on a closed store says so
#[test]
fn closed_store() {
    let io = SimulatedIo::new(Seed(13));
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");
    let state = std::sync::Arc::clone(&store.state);
    let mut scan = StoreScan::new(state, &reel::engine::scan_state::from_key(COLUMN, b"", Direction::Ascending)).expect("scan");
    store.close().expect("close");
    let (mut name, mut value) = (Vec::new(), Vec::new());
    assert!(matches!(scan.next_row(&mut name, &mut value), Err(StoreError::Closed)));
}

// random writes, seals and merges match a model at every look, in both directions and over random ranges
#[test]
fn random_against_a_model() {
    for seed in seeds(FIXED_SEEDS) {
        let io = SimulatedIo::new(Seed(seed));
        let store = open(&io);
        let mut random = SeededRandom::new(Seed(seed));
        let mut model = Model::new();
        for round in 0..RANDOM_ROUNDS {
            let number = random.below(RANDOM_KEYS);
            match random.below(10) {
                0..=5 => put(&store, &mut model, number, format!("value-{round}").as_bytes()),
                6 | 7 => {
                    store.delete(COLUMN, &key(number)).expect("delete");
                    model.remove(&key(number));
                },
                8 => {
                    let (start, end) = (key(number), key(number + random.below(20)));
                    store.delete_range(COLUMN, &start, &end).expect("delete");
                    model.retain(|name, _value| *name < start || *name >= end);
                },
                _not_written => {
                    seal(&store, &io);
                },
            }
            if round % 20 == 0 {
                check_whole(&store, &model);
                let (low, high) = (key(number), key(number + random.below(RANDOM_KEYS)));
                let found = keys(store.iter_range(COLUMN, &low, &high, Direction::Descending).expect("scan"));
                let expected: Vec<Vec<u8>> = model.range(low.clone()..high.clone()).rev().map(|(key, _value)| key.clone()).collect();
                assert_eq!(found, expected);
            }
        }
        settle(&io);
        check_whole(&store, &model);
        store.close().expect("close");
        let store = open(&io);
        settle(&io);
        check_whole(&store, &model);
        store.close().expect("close");
    }
}

// a block with no live row is crossed unread, so a segment every row of which was replaced costs a walk nothing
#[test]
fn dead_blocks_unread() {
    let io = SimulatedIo::new(Seed(14));
    let store = open(&io);
    hold_compaction(&store, true);
    for number in 0..DEAD_KEYS {
        store.put(COLUMN, &key(number), b"first").expect("put");
    }
    seal(&store, &io);
    for number in 0..DEAD_KEYS {
        store.put(COLUMN, &key(number), b"second").expect("put");
    }
    let newer = hold(&store.state.volumes[0].claims).open[0].number;
    seal(&store, &io);
    store.close().expect("close");
    drop(store);

    let store = open(&io);
    hold_compaction(&store, true);
    settle(&io);
    let blocks = hold(&store.state.volumes[0].summary).get(newer).expect("a row").facts.block_count;
    assert!(blocks > 1);
    let before = io.read_count();
    assert_eq!(keys(store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan")).len(), DEAD_KEYS as usize);
    let up = io.read_count().0 - before.0;
    assert_eq!(up, u64::from(blocks));
    store.close().expect("close");
}

// a cue below the replacements still reads the blocks they emptied
#[test]
fn dead_blocks_for_a_cue() {
    let io = SimulatedIo::new(Seed(15));
    let store = open(&io);
    hold_compaction(&store, true);
    let mut model = Model::new();
    for number in 0..DEAD_KEYS {
        put(&store, &mut model, number, b"first");
    }
    seal(&store, &io);
    let cue = store.cue().expect("cue");
    for number in 0..DEAD_KEYS {
        store.put(COLUMN, &key(number), b"second").expect("put");
    }
    seal(&store, &io);
    assert_eq!(rows(cue.iter_from(COLUMN, b"", Direction::Ascending).expect("scan")), forward(&model));
    assert_eq!(rows(cue.iter_from(COLUMN, b"zzz", Direction::Descending).expect("scan")), backward(&model));
    drop(cue);
    store.close().expect("close");
}

// a key whose versions run across row blocks comes once with its newest value, and a cue finds the one it can see
#[test]
fn versions_across_blocks() {
    let io = SimulatedIo::new(Seed(16));
    let store = open(&io);
    hold_compaction(&store, true);
    let (hot, mut model, mut seen) = (b"key-hot".to_vec(), Model::new(), Model::new());
    put(&store, &mut model, 1, b"before");
    store.put(COLUMN, b"key-z", b"after").expect("put");
    model.insert(b"key-z".to_vec(), b"after".to_vec());
    let mut cue = None;
    for version in 0..HOT_VERSIONS {
        let value = format!("value-{version}").into_bytes();
        store.put(COLUMN, &hot, &value).expect("put");
        model.insert(hot.clone(), value);
        if version == HOT_VERSIONS / 2 {
            seen = model.clone();
            cue = Some(store.cue().expect("cue"));
        }
    }
    seal(&store, &io);
    check_whole(&store, &model);
    let cue = cue.expect("a cue");
    assert_eq!(rows(cue.iter_from(COLUMN, b"", Direction::Ascending).expect("scan")), forward(&seen));
    assert_eq!(rows(cue.iter_from(COLUMN, b"zzz", Direction::Descending).expect("scan")), backward(&seen));
    drop(cue);
    store.close().expect("close");
}

fn long_key(group: u8, number: u64) -> Vec<u8> {
    let mut name = vec![b'x'; LONG_SHARED];
    name[LONG_GROUP_AT] = group;
    name.extend_from_slice(format!("{number:04}").as_bytes());
    name
}

// keys that share more than a tail holds still find their segments and places, from a key below, inside, between and above
#[test]
fn long_shared_prefixes() {
    let io = SimulatedIo::new(Seed(17));
    let store = open(&io);
    hold_compaction(&store, true);
    let mut model = Model::new();
    for group in [b'c', b'e', b'g'] {
        for number in 0..LONG_KEYS {
            let name = long_key(group, number);
            store.put(COLUMN, &name, b"value").expect("put");
            model.insert(name, b"value".to_vec());
        }
        seal(&store, &io);
    }
    check_whole(&store, &model);
    let starts = [long_key(b'a', 0), long_key(b'd', 0), long_key(b'e', 7), long_key(b'e', LONG_KEYS), long_key(b'z', 0)];
    for start in &starts {
        let up: Vec<Vec<u8>> = model.range(start.clone()..).map(|(name, _value)| name.clone()).collect();
        assert_eq!(keys(store.iter_from(COLUMN, start, Direction::Ascending).expect("scan")), up);
        let down: Vec<Vec<u8>> = model.range(..=start.clone()).rev().map(|(name, _value)| name.clone()).collect();
        assert_eq!(keys(store.iter_from(COLUMN, start, Direction::Descending).expect("scan")), down);
    }
    store.close().expect("close");
}

// a record that fails its check comes as that key's answer, and the walk goes on to the next
#[test]
fn corrupt_record_goes_on() {
    let io = SimulatedIo::new(Seed(18));
    let store = open(&io);
    hold_compaction(&store, true);
    store.put(COLUMN, b"a", b"first value").expect("put");
    store.put(COLUMN, b"b", b"rotting value").expect("put");
    store.put(COLUMN, b"c", b"third value").expect("put");
    seal(&store, &io);
    store.close().expect("close");
    drop(store);

    rot_value(&io, b"rotting");

    let store = open(&io);
    hold_compaction(&store, true);
    let errors = store.state.corruption_errors.load(Ordering::Relaxed);
    let mut scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
    let (mut name, mut value) = (Vec::new(), Vec::new());
    assert_eq!(scan.next_row(&mut name, &mut value).expect("step"), ScanStep::Found);
    assert_eq!(scan.next_row(&mut name, &mut value).expect("step"), ScanStep::Corrupt);
    assert_eq!(name, b"b");
    assert_eq!(scan.next_row(&mut name, &mut value).expect("step"), ScanStep::Found);
    assert_eq!((name.as_slice(), value.as_slice()), (&b"c"[..], &b"third value"[..]));
    assert_eq!(scan.next_row(&mut name, &mut value).expect("step"), ScanStep::End);
    drop(scan);
    // the scan keeps its count of corrupt steps and adds it to the store's total when it goes back
    assert_eq!(store.state.corruption_errors.load(Ordering::Relaxed) - errors, 1, "the corrupt step went uncounted");
    store.close().expect("close");
}

// a start outside a segment's shared bytes takes the column from its near end, however many blocks it has
#[test]
fn start_outside_a_segment() {
    let io = SimulatedIo::new(Seed(19));
    let store = open(&io);
    hold_compaction(&store, true);
    let mut model = Model::new();
    for number in 0..DEAD_KEYS {
        put(&store, &mut model, number, b"v");
    }
    seal(&store, &io);
    let (all, none) = (model.len(), 0);
    for (start, up, down) in [(&b"a"[..], all, none), (b"kex-99999", all, none), (b"kez", none, all), (b"zzz", none, all)] {
        assert_eq!(keys(store.iter_from(COLUMN, start, Direction::Ascending).expect("scan")).len(), up);
        assert_eq!(keys(store.iter_from(COLUMN, start, Direction::Descending).expect("scan")).len(), down);
    }
    store.close().expect("close");
}

// run jobs until they go quiet, from inside a read the scan is making
fn step_jobs(io: &SimulatedIo) {
    for _step in 0..HOOK_STEPS {
        if io.step_any().is_none() {
            io.advance_clock(TICK);
            if io.step_any().is_none() {
                return;
            }
        }
    }
}

// a seal that lands inside a step takes a key out of the index, and the step seeks again and finds it in the new segment
#[test]
fn seal_inside_a_step() {
    let io = SimulatedIo::new(Seed(20));
    let store = open(&io);
    store.put(COLUMN, b"a0", b"value").expect("put");
    store.put(COLUMN, b"c", b"value").expect("put");
    seal(&store, &io);
    store.close().expect("close");
    drop(store);

    let store = open(&io);
    settle(&io);
    store.put(COLUMN, b"b", b"value").expect("put");
    let (state, disk, is_done) = (Arc::clone(&store.state), io.clone(), AtomicBool::new(false));
    io.on_every_read(Some(Arc::new(move || {
        if !is_done.swap(true, Ordering::AcqRel) {
            state.seal_fullest(&state.volumes[0]);
            step_jobs(&disk);
        }
    })));
    let found = keys(store.iter_from(COLUMN, b"a1", Direction::Ascending).expect("scan"));
    io.on_every_read(None);
    let expected: [&[u8]; 2] = [b"b", b"c"];
    assert_eq!(found, expected);
    store.close().expect("close");
}

// a record freed under a walk's read fails its check, and the walk reads the row that replaced it
#[test]
fn freed_under_a_read() {
    let io = SimulatedIo::new(Seed(21));
    let store = open(&io);
    let (older, newer) = (vec![1; FREE_IN_PLACE_BYTES.0 as usize], vec![2; FREE_IN_PLACE_BYTES.0 as usize]);
    store.put(COLUMN, b"key", &older).expect("put");
    seal(&store, &io);
    assert_eq!(keys(store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan")).len(), 1);

    let frees_before = store.state.cues.counts().frees;
    let (state, disk, is_done, value) = (Arc::clone(&store.state), io.clone(), AtomicBool::new(false), newer.clone());
    io.on_every_read(Some(Arc::new(move || {
        if !is_done.swap(true, Ordering::AcqRel) {
            state.write_value(COLUMN, b"key", &value, None).expect("put");
            state.seal_fullest(&state.volumes[0]);
            step_jobs(&disk);
        }
    })));
    let found = rows(store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan"));
    io.on_every_read(None);
    assert!(store.state.cues.counts().frees > frees_before);
    assert_eq!(found, vec![(b"key".to_vec(), newer)]);
    store.close().expect("close");
}

// a bad record under a walk that frees keep crossing gets its answer under a pin, so no job's rate can starve the walk
#[test]
fn crossed_every_try() {
    let io = SimulatedIo::new(Seed(22));
    let store = open(&io);
    store.put(COLUMN, b"key", b"rotting value").expect("put");
    seal(&store, &io);
    store.close().expect("close");
    drop(store);
    rot_value(&io, b"rotting");

    let store = open(&io);
    settle(&io);
    let (state, reads) = (Arc::clone(&store.state), Arc::new(AtomicU64::new(0)));
    let counted = Arc::clone(&reads);
    io.on_every_read(Some(Arc::new(move || {
        state.cues.note_free();
        counted.fetch_add(1, Ordering::Relaxed);
    })));
    let mut scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
    let (mut name, mut value) = (Vec::new(), Vec::new());
    assert_eq!(scan.next_row(&mut name, &mut value).expect("step"), ScanStep::Corrupt);
    io.on_every_read(None);
    assert!(reads.load(Ordering::Relaxed) > u64::from(MOST_READ_TRIES));
    drop(scan);
    store.close().expect("close");
}

fn rot_value(io: &SimulatedIo, needle: &[u8]) {
    let listing = io.list_directory(ROOT.as_ref()).expect("list");
    let path = listing.into_iter().find(|path| path.extension().is_some_and(|name| name == "data")).expect("a data file");
    let file = io.open_read_only(&path).expect("open");
    let mut bytes = vec![0; file.length().expect("length").0 as usize];
    file.read_exact_at(FileOffset(0), &mut bytes).expect("read");
    io.close_file(file);
    let at = bytes.windows(needle.len()).position(|window| window == needle).expect("the value");
    io.rot(&path, FileOffset(at as u64), BitMask(1)).expect("rot");
}

fn walk_values(store: &Store, direction: Direction) -> Vec<(Vec<u8>, Vec<u8>)> {
    let start: &[u8] = match direction {
        Direction::Ascending => b"",
        Direction::Descending => b"\xff",
    };
    let mut scan = store.iter_from(COLUMN, start, direction).expect("scan");
    let (mut name, mut value, mut rows) = (Vec::new(), Vec::new(), Vec::new());
    loop {
        match scan.next_row(&mut name, &mut value).expect("step") {
            ScanStep::Found => rows.push((name.clone(), value.clone())),
            ScanStep::Corrupt => panic!("a row failed its check"),
            ScanStep::End => return rows,
        }
    }
}

fn sealed_in_order(io: &SimulatedIo) -> Store {
    let store = open(io);
    for number in 0..WINDOW_KEYS {
        store.put(COLUMN, &key(number), format!("value-{number:06}").as_bytes()).expect("put");
    }
    seal(&store, io);
    store.close().expect("close");
    drop(store);
    open(io)
}

// rows side by side come out of a window, far fewer reads than rows, in both directions
#[test]
fn window_saves_reads() {
    for direction in [Direction::Ascending, Direction::Descending] {
        let io = SimulatedIo::new(Seed(70));
        let store = sealed_in_order(&io);
        let before = io.read_count().0;
        let rows = walk_values(&store, direction);
        let reads = io.read_count().0 - before;

        assert_eq!(rows.len() as u64, WINDOW_KEYS);
        for (index, (name, value)) in rows.iter().enumerate() {
            let number = match direction {
                Direction::Ascending => index as u64,
                Direction::Descending => WINDOW_KEYS - 1 - index as u64,
            };
            assert_eq!(*name, key(number));
            assert_eq!(*value, format!("value-{number:06}").into_bytes());
        }
        assert!(reads < WINDOW_KEYS / 4, "{reads} reads for {WINDOW_KEYS} rows going {direction:?}");
        store.close().expect("close");
    }
}

// a value too large for the window still reads back through its own read
#[test]
fn window_passes_a_large_value() {
    let io = SimulatedIo::new(Seed(71));
    let store = open(&io);
    let large = vec![9; MOST_WINDOW_RECORD * 3];
    store.put(COLUMN, b"a", b"small").expect("put");
    store.put(COLUMN, b"b", &large).expect("put");
    store.put(COLUMN, b"c", b"small").expect("put");
    seal(&store, &io);
    let rows = walk_values(&store, Direction::Ascending);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[1].1, large);
    store.close().expect("close");
}

// a bad sector under a neighbour leaves this row to its own read, so the window never fails a row that reads
#[test]
fn window_read_fails_only_the_bad_row() {
    let io = SimulatedIo::new(Seed(72));
    let store = sealed_in_order(&io);
    let path = io
        .list_directory(Path::new(ROOT))
        .expect("list")
        .into_iter()
        .find(|path| path.extension().is_some_and(|name| name == "data"))
        .expect("a segment");
    io.bad_sector(&path, FileOffset(BAD_RECORD_AT)).expect("bad");

    let mut scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
    let (mut name, mut value, mut found, mut corrupt) = (Vec::new(), Vec::new(), 0, 0);
    loop {
        match scan.next_row(&mut name, &mut value).expect("step") {
            ScanStep::Found => found += 1,
            ScanStep::Corrupt => corrupt += 1,
            ScanStep::End => break,
        }
    }
    drop(scan);
    assert_eq!(found + corrupt, WINDOW_KEYS);
    assert!(corrupt > 0 && corrupt < WINDOW_KEYS / 10, "{corrupt} rows lost to one sector");
    store.close().expect("close");
}

const SPREAD_STRIDE: u64 = 787;
const SPREAD_ROWS: u64 = 2_000;

fn lz4_config() -> StoreConfig {
    StoreConfig {
        memory_budget: ByteCount(128 * 1024 * 1024),
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "scans".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(), open_segments: None, segment_bytes: None, lifetime_groups: None,
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Variable { most: 96 },
            codec: Codec::Lz4,
            volume: VolumeId(0),
            index: ColumnIndex::Plain,
            least_rows: None,
            least_value: None,
        }],
    }
}

// a scan of a reopened lz4 column, with its rows written in the order a stride gives: its rows and read calls
fn lz4_scan(stride: u64) -> (u64, u64) {
    let io = SimulatedIo::new(Seed(73));
    let config = lz4_config();
    let store = ReelStore::open(io.clone(), &config).expect("open");
    for step in 0..SPREAD_ROWS {
        let number = step * stride % SPREAD_ROWS;
        let value = vec![b'a' + (number % 20) as u8; 300];
        store.put(COLUMN, &key(number), &value).expect("put");
    }
    seal(&store, &io);
    store.close().expect("close");
    drop(store);
    let store = ReelStore::open(io.clone(), &config).expect("open");

    let before = io.read_count().0;
    let mut scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
    let (mut name, mut value, mut rows) = (Vec::new(), Vec::new(), 0);
    while scan.next_row(&mut name, &mut value).expect("step") == ScanStep::Found {
        assert_eq!(value.len(), 300);
        rows += 1;
    }
    drop(scan);
    let reads = io.read_count().0 - before;
    store.close().expect("close");
    (rows, reads)
}

// a compressed column's scan costs no more reads than rows, with the rows scattered through the segment
#[test]
fn compressed_scan_reads_once_a_row() {
    let (rows, reads) = lz4_scan(SPREAD_STRIDE);
    assert_eq!(rows, SPREAD_ROWS);
    assert!(reads < SPREAD_ROWS + SPREAD_ROWS / 4, "{reads} reads for {rows} rows");
}

// a compressed column's rows side by side come out of the window with a read call for many
#[test]
fn compressed_rows_share_reads() {
    let (rows, reads) = lz4_scan(1);
    assert_eq!(rows, SPREAD_ROWS);
    assert!(reads < SPREAD_ROWS / 4, "{reads} reads for {rows} rows");
}

// rows that alternate between two segments each come from their own segment's window
#[test]
fn interleaved_rows_keep_their_windows() {
    let io = SimulatedIo::new(Seed(72));
    let store = open(&io);
    for parity in [0, 1] {
        for number in (parity..WINDOW_KEYS).step_by(2) {
            store.put(COLUMN, &key(number), format!("value-{number:06}").as_bytes()).expect("put");
        }
        seal(&store, &io);
    }
    store.close().expect("close");
    drop(store);
    let store = open(&io);

    let before = io.read_count().0;
    let rows = walk_values(&store, Direction::Ascending);
    let reads = io.read_count().0 - before;
    assert_eq!(rows.len() as u64, WINDOW_KEYS);
    assert!(reads < WINDOW_KEYS / 4, "{reads} reads for {WINDOW_KEYS} rows across two segments");
    store.close().expect("close");
}

// scans over more overlapping segments than one batch seats, and over open trees in every shard, give every key the model
// holds in order, either way, with deletes and overwrites between seals and a seal between two steps of a walk. The seats
// whose blocks the cache holds go together, and so do the heads' tree descents
#[test]
fn seats_together_agree() {
    for seed in seeds(0..TOGETHER_SEEDS) {
        let io = SimulatedIo::new(Seed(seed));
        let store = open(&io);
        hold_compaction(&store, true);
        let mut random = SeededRandom::new(Seed(seed));
        let mut model = Model::new();
        for _round in 0..TOGETHER_SEALS {
            churn(&store, &mut model, &mut random);
            seal(&store, &io);
        }
        // the last round stays open, so the heads have trees to descend
        churn(&store, &mut model, &mut random);
        let (seats, heads) = (&store.state.seats_together, &store.state.heads_together);
        let (seats_before, heads_before) = (seats.load(Ordering::Relaxed), heads.load(Ordering::Relaxed));
        for _start in 0..TOGETHER_STARTS {
            let start = key(random.below(TOGETHER_KEY_SPACE + 10));
            let pair = |(name, value): (&Vec<u8>, &Vec<u8>)| (name.clone(), value.clone());
            let up: Vec<(Vec<u8>, Vec<u8>)> = model.range(start.clone()..).take(TOGETHER_STEPS).map(pair).collect();
            let down: Vec<(Vec<u8>, Vec<u8>)> = model.range(..=start.clone()).rev().take(TOGETHER_STEPS).map(pair).collect();
            // the second walk each way finds the blocks the first put in the cache
            for (direction, want) in [(Direction::Ascending, &up), (Direction::Descending, &down), (Direction::Ascending, &up), (Direction::Descending, &down)] {
                assert_eq!(&walk_from(&store, &start, direction, None), want, "seed {seed} {direction:?} from {start:?}");
            }
        }
        assert!(seats.load(Ordering::Relaxed) > seats_before, "seed {seed}: no seat went together");
        assert!(heads.load(Ordering::Relaxed) > heads_before, "seed {seed}: no head descended together");

        // a seal halfway through a walk moves the open rows into a segment the walk has to seat as it goes
        let start = key(random.below(TOGETHER_KEY_SPACE));
        let pair = |(name, value): (&Vec<u8>, &Vec<u8>)| (name.clone(), value.clone());
        let want: Vec<(Vec<u8>, Vec<u8>)> = model.range(start.clone()..).take(TOGETHER_STEPS).map(pair).collect();
        let found = walk_from(&store, &start, Direction::Ascending, Some(&|| seal(&store, &io)));
        assert_eq!(found, want, "seed {seed} sealed under the walk from {start:?}");
        store.close().expect("close");
    }
}

// a list one past a batch's room seats in two even batches, so every segment over the key seats together and none alone
#[test]
fn seats_split_evenly() {
    let io = SimulatedIo::new(Seed(61));
    let store = open(&io);
    hold_compaction(&store, true);
    let segments = reel::engine::scan_state::SEAT_BATCH as u64 + 1;
    for round in 0..segments {
        // keys of their own, so every segment keeps live rows across the whole range
        for number in [round, EVEN_KEYS / 2 + round, EVEN_KEYS - 1 - round] {
            store.put(COLUMN, &key(number), format!("{round}").as_bytes()).expect("put");
        }
        seal(&store, &io);
    }
    // every segment starts below the walk's first key, so one list holds them all
    let start = key(EVEN_KEYS / 2);
    // the first walk puts the blocks in the cache, and the second seats every segment from it
    let _warm = walk_from(&store, &start, Direction::Ascending, None);
    let (seats, together) = (&store.state.scan_seats, &store.state.seats_together);
    let (seats_before, together_before) = (seats.load(Ordering::Relaxed), together.load(Ordering::Relaxed));
    let found = walk_from(&store, &start, Direction::Ascending, None);
    assert_eq!(found.len(), TOGETHER_STEPS);
    assert_eq!(seats.load(Ordering::Relaxed) - seats_before, segments);
    assert_eq!(together.load(Ordering::Relaxed) - together_before, segments, "a seat went alone");
    store.close().expect("close");
}

// a scan over a column the record cache can hold takes the values it holds from it, keeps what it read, and gives the model's
// rows, and a column too large for the cache leaves the cache to gets
#[test]
fn scans_use_the_cache_that_holds_the_column() {
    for (seed, is_small) in [(51, true), (52, false)] {
        let io = SimulatedIo::new(Seed(seed));
        let store = open(&io);
        hold_compaction(&store, true);
        let room = store.state.record_cache.heap_bytes().0;
        // a small column takes a tenth of the cache, and a large one twice the cache
        let (count, value_bytes) = if is_small { (room / 10 / CACHE_TEST_VALUE, CACHE_TEST_VALUE) } else { (room * 2 / CACHE_TEST_VALUE, CACHE_TEST_VALUE) };
        let mut model = Model::new();
        for number in 0..count {
            let value = vec![number as u8; value_bytes as usize];
            store.put(COLUMN, &key(number), &value).expect("put");
            model.insert(key(number), value);
        }
        seal(&store, &io);
        io.advance_clock(Nanoseconds(2_000_000_000));
        let before = store.state.scan_cache_hits.load(Ordering::Relaxed);
        for _walk in 0..3 {
            let start = key(count / 3);
            let want: Vec<(Vec<u8>, Vec<u8>)> = model.range(start.clone()..).take(TOGETHER_STEPS).map(|(name, value)| (name.clone(), value.clone())).collect();
            assert_eq!(walk_from(&store, &start, Direction::Ascending, None), want, "seed {seed}");
        }
        let hits = store.state.scan_cache_hits.load(Ordering::Relaxed) - before;
        assert_eq!(store.state.volumes[0].scans_use_cache.load(Ordering::Relaxed), is_small, "seed {seed}: {count} values of {value_bytes} B against {room} B of cache");
        assert_eq!(hits > 0, is_small, "seed {seed}: {hits} values came from the cache");
        store.close().expect("close");
    }
}

/// Bytes of each value the cache sims write
const CACHE_TEST_VALUE: u64 = 400;

// put and delete a round of keys the model follows
fn churn(store: &Store, model: &mut Model, random: &mut SeededRandom) {
    for _put in 0..TOGETHER_PUTS {
        let name = key(random.below(TOGETHER_KEY_SPACE));
        if random.one_in(6) {
            store.delete(COLUMN, &name).expect("delete");
            model.remove(&name);
        } else {
            let value = random.below(u64::MAX).to_le_bytes().to_vec();
            store.put(COLUMN, &name, &value).expect("put");
            model.insert(name, value);
        }
    }
}

// walk a number of rows from a key, running something halfway when asked
fn walk_from(store: &Store, start: &[u8], direction: Direction, halfway: Option<&dyn Fn()>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut scan = store.iter_from(COLUMN, start, direction).expect("scan");
    let (mut name, mut value, mut found) = (Vec::new(), Vec::new(), Vec::new());
    while found.len() < TOGETHER_STEPS && scan.next_row(&mut name, &mut value).expect("step") == ScanStep::Found {
        found.push((name.clone(), value.clone()));
        if found.len() == TOGETHER_STEPS / 2
            && let Some(halfway) = halfway
        {
            halfway();
        }
    }
    found
}

const EVEN_KEYS: u64 = 1_000;
const TOGETHER_SEEDS: u64 = 6;
const TOGETHER_SEALS: u64 = 24;
const TOGETHER_PUTS: u64 = 80;
const TOGETHER_KEY_SPACE: u64 = 2_000;
const TOGETHER_STARTS: u64 = 40;
const TOGETHER_STEPS: usize = 25;

// step a walk to its end or a number of keys, and return what it found
fn step_keys(store: &Store, scan: &mut reel::engine::scan_state::ScanState, most: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    let (mut key, mut value, mut found) = (Vec::new(), Vec::new(), Vec::new());
    while (found.len() as u64) < most {
        match store.state.step_scan(scan, &mut key, reel::engine::scan_path::ScanWant::Value(&mut value)).expect("step") {
            ScanStep::Found => found.push((key.clone(), value.clone())),
            ScanStep::End => break,
            ScanStep::Corrupt => panic!("a corrupt row"),
        }
    }
    found
}

// a walk asks the index only for the keys its heads show, and still sees a row that comes in under it
#[test]
fn index_asked_for_its_own_keys() {
    let io = SimulatedIo::new(Seed(40));
    let store = open(&io);
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), b"sealed").expect("put");
    }
    seal(&store, &io);
    let open_keys = [3, 90, 150];
    for number in open_keys {
        store.put(COLUMN, &key(number), b"open").expect("put");
    }

    let range = reel::engine::scan_state::from_key(COLUMN, b"", Direction::Ascending);
    let mut scan = store.state.take_scan(&range, None).expect("scan");
    let found = step_keys(&store, &mut scan, u64::MAX);
    assert_eq!(found.len() as u64, KEYS);
    assert_eq!(scan.lookups, open_keys.len() as u64);
    assert!(found.iter().all(|(found, value)| value == if open_keys.iter().any(|number| key(*number) == *found) { b"open".as_slice() } else { b"sealed".as_slice() }));
    store.state.give_back_scan(scan);

    let mut scan = store.state.take_scan(&range, None).expect("scan");
    let first = step_keys(&store, &mut scan, 10);
    assert_eq!(first.len(), 10);
    store.delete(COLUMN, &key(50)).expect("delete");
    store.put(COLUMN, &key(60), b"new").expect("put");
    let rest = step_keys(&store, &mut scan, u64::MAX);
    assert_eq!(rest.len() as u64, KEYS - 11);
    assert!(rest.iter().all(|(found, _value)| *found != key(50)));
    assert_eq!(rest.iter().find(|(found, _value)| *found == key(60)).expect("key 60").1, b"new");
    store.state.give_back_scan(scan);
    store.close().expect("close");
}

const HELD_KEYS: u64 = 2_000;
const HELD_SEGMENTS: u64 = reel::engine::scan_state::HELD_SOURCE_BLOCKS as u64 + 3;

// a walk reads each block of a held source once into its own buffer and steps its rows there, and the sources past the
// held ones go through the cache, both ways
#[test]
fn sources_hold_their_blocks() {
    let io = SimulatedIo::new(Seed(41));
    let store = open(&io);
    for number in 0..HELD_KEYS {
        store.put(COLUMN, &key(number), &[7; 60]).expect("put");
    }
    seal(&store, &io);
    let volume = &store.state.volumes[0];
    let segment = hold(&volume.summary).rows().map(|row| row.facts.segment).next().expect("a segment");
    let (_first, blocks) = volume.table.column_blocks(segment, COLUMN).expect("the column");
    assert!(blocks > 1);
    for direction in [Direction::Ascending, Direction::Descending] {
        let start: &[u8] = if direction == Direction::Ascending { b"" } else { b"zzz" };
        let range = reel::engine::scan_state::from_key(COLUMN, start, direction);
        let mut scan = store.state.take_scan(&range, None).expect("scan");
        let found = step_keys(&store, &mut scan, u64::MAX);
        assert_eq!(found.len() as u64, HELD_KEYS);
        assert!(found.windows(2).all(|pair| (pair[0].0 < pair[1].0) == (direction == Direction::Ascending)));
        assert_eq!(scan.sources.iter().map(|source| source.fills).sum::<u64>(), u64::from(blocks));
        assert_eq!(scan.sources.iter().map(|source| source.cache_steps).sum::<u64>(), 0);
        store.state.give_back_scan(scan);
    }

    // more segments than hold their blocks, each spanning the keys, so the walk stands in all of them at once
    hold_compaction(&store, true);
    for segment in 0..HELD_SEGMENTS {
        store.put(COLUMN, &key(segment), b"later").expect("put");
        store.put(COLUMN, &key(HELD_KEYS - 1 - segment), b"later").expect("put");
        seal(&store, &io);
    }
    let range = reel::engine::scan_state::from_key(COLUMN, b"", Direction::Ascending);
    let mut scan = store.state.take_scan(&range, None).expect("scan");
    let found = step_keys(&store, &mut scan, u64::MAX);
    assert_eq!(found.len() as u64, HELD_KEYS);
    assert_eq!(found.iter().filter(|(_key, value)| value == b"later").count() as u64, 2 * HELD_SEGMENTS);
    assert!(found.windows(2).all(|pair| pair[0].0 < pair[1].0));
    assert!(scan.sources.iter().map(|source| source.cache_steps).sum::<u64>() > 0);
    store.state.give_back_scan(scan);
    store.close().expect("close");
}

// the old answer from three table calls: where a key enters the column, by its shared bytes and the block lines
fn entry_by_parts(store: &Store, segment: reel::segment::segment_tail::SegmentNumber, probe: &[u8], direction: Direction) -> Option<(u32, reel::segment::block_entry::BlockEntry)> {
    let table = &store.state.volumes[0].table;
    let tail = table.column_tail(segment, COLUMN).expect("the column");
    let end = tail.first_block + tail.block_count.0;
    let start = match (tail.order_against_shared(probe), direction) {
        (std::cmp::Ordering::Equal, direction) => table.entry_block(segment, &tail, probe, direction),
        (std::cmp::Ordering::Less, Direction::Ascending) => Some(tail.first_block),
        (std::cmp::Ordering::Greater, Direction::Descending) => end.checked_sub(1),
        _other => None,
    }?;
    table.block_line(segment, start).map(|(line, _replacement)| (start, line))
}

// one table call finds the same entry block as the three it replaced, for every key, the gaps and both ends, both ways
#[test]
fn seat_line_matches_its_parts() {
    use reel::segment::segment_table::Seat;
    let io = SimulatedIo::new(Seed(42));
    let store = open(&io);
    for number in 0..HELD_KEYS {
        store.put(COLUMN, &key(2 * number), &[7; 60]).expect("put");
    }
    seal(&store, &io);
    let volume = &store.state.volumes[0];
    let segment = hold(&volume.summary).rows().map(|row| row.facts.segment).next().expect("a segment");
    let mut probes: Vec<Vec<u8>> = (0..2 * HELD_KEYS + 1).map(key).collect();
    probes.extend([b"a".to_vec(), b"zzz".to_vec(), Vec::new()]);
    for probe in &probes {
        for direction in [Direction::Ascending, Direction::Descending] {
            let Seat::Line(seat) = volume.table.seat_line(segment, COLUMN, probe, direction) else {
                panic!("no seat");
            };
            assert!(seat.is_settled);
            let start = seat.start.map(|(start, line)| (start, line.block_offset));
            let parts = entry_by_parts(&store, segment, probe, direction).map(|(start, line)| (start, line.block_offset));
            assert_eq!(start, parts, "{} {direction:?}", String::from_utf8_lossy(probe));
        }
    }
    assert!(matches!(volume.table.seat_line(segment, OTHER_COLUMN, b"key", Direction::Ascending), Seat::NoColumn));
    store.close().expect("close");

    // keys that share more bytes than a tail holds take a row to place a key that matches them
    let io = SimulatedIo::new(Seed(43));
    let store = open(&io);
    let shared = vec![b's'; LONG_SHARED];
    for number in 0..KEYS {
        let mut long = shared.clone();
        long.extend_from_slice(&key(number));
        store.put(COLUMN, &long, b"value").expect("put");
    }
    seal(&store, &io);
    let volume = &store.state.volumes[0];
    let segment = hold(&volume.summary).rows().map(|row| row.facts.segment).next().expect("a segment");
    let Seat::Line(seat) = volume.table.seat_line(segment, COLUMN, &shared, Direction::Ascending) else {
        panic!("no seat");
    };
    assert!(!seat.is_settled);
    store.close().expect("close");
}

const BATCH_TEST_KEYS: u64 = 500;

fn batch_value(number: u64) -> Vec<u8> {
    format!("first-{number}").into_bytes()
}

// a walk for values reads its small sealed records in batches, answers a batch as of the point it located it, and seeks
// again from the key it stands on when the batch's segment leaves under it
#[test]
fn scans_read_values_in_batches() {
    // an untold walk's first batch takes FIRST_UNTOLD_KEYS keys
    let batch_keys = reel::engine::scan_state::FIRST_UNTOLD_KEYS as u64;
    let io = SimulatedIo::new(Seed(44));
    let store = open(&io);
    hold_compaction(&store, true);
    for number in 0..BATCH_TEST_KEYS {
        store.put(COLUMN, &key(number), &batch_value(number)).expect("put");
    }
    seal(&store, &io);
    let segment = hold(&store.state.volumes[0].summary).rows().map(|row| row.facts.segment).next().expect("a segment");
    let range = reel::engine::scan_state::from_key(COLUMN, b"", Direction::Ascending);

    let mut scan = store.state.take_scan(&range, None).expect("scan");
    let found = step_keys(&store, &mut scan, u64::MAX);
    assert_eq!(found.len() as u64, BATCH_TEST_KEYS);
    assert!(found.iter().enumerate().all(|(number, (found, value))| *found == key(number as u64) && *value == batch_value(number as u64)));
    assert_eq!(scan.batched, BATCH_TEST_KEYS);
    store.state.give_back_scan(scan);

    // a write inside the batch shows from the next batch on, and one past it shows at once
    let mut scan = store.state.take_scan(&range, None).expect("scan");
    assert_eq!(step_keys(&store, &mut scan, 1).len(), 1);
    store.put(COLUMN, &key(5), b"second").expect("put");
    store.put(COLUMN, &key(batch_keys + 5), b"second").expect("put");
    let rest = step_keys(&store, &mut scan, u64::MAX);
    assert_eq!(rest.len() as u64, BATCH_TEST_KEYS - 1);
    assert_eq!(rest[4], (key(5), batch_value(5)));
    assert_eq!(rest[batch_keys as usize + 4], (key(batch_keys + 5), b"second".to_vec()));
    store.state.give_back_scan(scan);

    // the segments leave while a batch stands in them: the records it read, key 5's open one among them, answer as of its point
    let mut scan = store.state.take_scan(&range, None).expect("scan");
    let first = step_keys(&store, &mut scan, 1);
    for number in 0..BATCH_TEST_KEYS {
        store.put(COLUMN, &key(number), b"third").expect("put");
    }
    hold_compaction(&store, false);
    seal(&store, &io);
    assert!(!store.state.volumes[0].table.holds(segment));
    let rest = step_keys(&store, &mut scan, u64::MAX);
    let keys: Vec<Vec<u8>> = first.iter().chain(&rest).map(|(found, _value)| found.clone()).collect();
    assert_eq!(keys, (0..BATCH_TEST_KEYS).map(key).collect::<Vec<_>>());
    let held = batch_keys as usize - 1;
    assert!(rest[..held].iter().all(|(_key, value)| value != b"third"));
    assert!(rest[held..].iter().all(|(_key, value)| value == b"third"));
    store.state.give_back_scan(scan);
    store.close().expect("close");
}

const LARGE_VALUE_BYTES: usize = 10_000;

// a large record takes its own read when its key is answered, so a batch whose segment left seeks again from that key
#[test]
fn moved_batch_seeks_again() {
    let io = SimulatedIo::new(Seed(45));
    let store = open(&io);
    hold_compaction(&store, true);
    for number in 0..BATCH_TEST_KEYS {
        store.put(COLUMN, &key(number), &[1; LARGE_VALUE_BYTES]).expect("put");
    }
    seal(&store, &io);
    let segment = hold(&store.state.volumes[0].summary).rows().map(|row| row.facts.segment).next().expect("a segment");
    let range = reel::engine::scan_state::from_key(COLUMN, b"", Direction::Ascending);
    let mut scan = store.state.take_scan(&range, None).expect("scan");
    let first = step_keys(&store, &mut scan, 1);
    for number in 0..BATCH_TEST_KEYS {
        store.put(COLUMN, &key(number), b"newer").expect("put");
    }
    hold_compaction(&store, false);
    seal(&store, &io);
    assert!(!store.state.volumes[0].table.holds(segment));
    let rest = step_keys(&store, &mut scan, u64::MAX);
    let keys: Vec<Vec<u8>> = first.iter().chain(&rest).map(|(found, _value)| found.clone()).collect();
    assert_eq!(keys, (0..BATCH_TEST_KEYS).map(key).collect::<Vec<_>>());
    assert!(rest.iter().all(|(_key, value)| value == b"newer"));
    store.state.give_back_scan(scan);
    store.close().expect("close");
}

const WIDE_BATCH_KEYS: u64 = 100;
const WIDE_BATCH_VALUE_BYTES: usize = 5_000;

fn wide_batch_value(number: u64) -> Vec<u8> {
    let mut value = format!("wide-{number}-").into_bytes();
    value.resize(WIDE_BATCH_VALUE_BYTES, (number % 251) as u8);
    value
}

// a record past 4 KiB and under the batch's most still reads in the batch, so a store of 4 KiB values scans in batches
#[test]
fn wide_records_read_in_batches() {
    let io = SimulatedIo::new(Seed(45));
    let store = open(&io);
    hold_compaction(&store, true);
    for number in 0..WIDE_BATCH_KEYS {
        store.put(COLUMN, &key(number), &wide_batch_value(number)).expect("put");
    }
    seal(&store, &io);
    let range = reel::engine::scan_state::from_key(COLUMN, b"", Direction::Ascending);
    let mut scan = store.state.take_scan(&range, None).expect("scan");
    let found = step_keys(&store, &mut scan, u64::MAX);
    assert_eq!(found.len() as u64, WIDE_BATCH_KEYS);
    assert!(found.iter().enumerate().all(|(number, (found, value))| *found == key(number as u64) && *value == wide_batch_value(number as u64)));
    assert_eq!(scan.batched, WIDE_BATCH_KEYS, "wide records took their own reads");
    store.state.give_back_scan(scan);
    store.close().expect("close");
}


// collect a key walk's keys one step at a time
fn stepped_keys(store: &Store, start: &[u8]) -> Vec<Vec<u8>> {
    let mut scan = store.iter_from(COLUMN, start, Direction::Ascending).expect("scan");
    let (mut keys, mut key) = (Vec::new(), Vec::new());
    while scan.next_key(&mut key).expect("step") == ScanStep::Found {
        keys.push(key.clone());
    }
    keys
}

// a drain lends the same keys a key walk steps through, over sealed, open, overwritten and deleted rows, and stops where the
// closure says, with the walk going on from the next key
#[test]
fn drained_keys_match_stepped_keys() {
    let io = SimulatedIo::new(Seed(4_950));
    let store = open(&io);
    let key = |number: u64| format!("key-{number:05}").into_bytes();
    for number in (0..3_000).filter(|number| number % 2 == 0) {
        store.put(COLUMN, &key(number), b"sealed").expect("put");
    }
    seal(&store, &io);
    for number in (0..3_000).filter(|number| number % 2 == 1) {
        store.put(COLUMN, &key(number), b"open").expect("put");
    }
    for number in (0..3_000).filter(|number| number % 13 == 0) {
        store.delete(COLUMN, &key(number)).expect("delete");
    }
    for start in [&b""[..], b"key-01", b"key-02999", b"zzz"] {
        let stepped = stepped_keys(&store, start);
        let mut drained = Vec::new();
        let mut scan = store.iter_from(COLUMN, start, Direction::Ascending).expect("scan");
        let mut buffer = Vec::new();
        assert_eq!(scan.drain_keys(&mut buffer, |name| {
            drained.push(name.to_vec());
            true
        }).expect("drain"), ScanStep::End);
        assert_eq!(drained, stepped, "start {start:?}");
    }

    // stop after 100 keys, then step on: the walk goes on from the 101st
    let stepped = stepped_keys(&store, b"");
    let mut scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
    let (mut taken, mut buffer) = (Vec::new(), Vec::new());
    assert_eq!(scan.drain_keys(&mut buffer, |name| {
        taken.push(name.to_vec());
        taken.len() < 100
    }).expect("drain"), ScanStep::Found);
    assert_eq!(scan.next_key(&mut buffer).expect("step"), ScanStep::Found);
    taken.push(buffer.clone());
    assert_eq!(taken.as_slice(), &stepped[..101]);
    store.close().expect("close");
}
