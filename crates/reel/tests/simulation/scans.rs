//! Scans: keys in order in both directions, both ends honored, and a walk that outlives whatever the jobs do under it

use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::engine::read_path::MOST_READ_TRIES;
use reel::engine::scan_state::{MOST_SCAN_SOURCES, MOST_SCANS};
use reel::engine::store_scan::StoreScan;
use reel::engine::walk_job::FREE_IN_PLACE_BYTES;
use reel::guard::hold;
use reel::io::{Io, IoFile};
use reel::record::codec::Codec;
use reel_api::column_id::ColumnId;
use reel_api::direction::Direction;
use reel_api::scan::ScanStep;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, Nanoseconds, VolumeId};
use reel_sim::file_image::BitMask;
use reel_sim::seeded_random::{Seed, SeededRandom};
use reel_sim::simulated_io::SimulatedIo;

use crate::seed_sets::seeds;

const ROOT: &str = "/disk/scans";
const COLUMN: ColumnId = ColumnId(1);
const OTHER_COLUMN: ColumnId = ColumnId(2);
const BUDGET: ByteCount = ByteCount(64 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 400_000;
const KEYS: u64 = 200;
const DEAD_KEYS: u64 = 20_000;
const HOT_VERSIONS: u64 = 6_000;
const LONG_SHARED: usize = 70;
const LONG_GROUP_AT: usize = 55;
const LONG_KEYS: u64 = 9_000;
const RANDOM_KEYS: u64 = 300;
const RANDOM_ROUNDS: u64 = 400;
const FIXED_SEEDS: [u64; 6] = [0, 1, 2, 3, 4, 5];
const HOOK_STEPS: u64 = 20_000;

type Store = ReelStore<SimulatedIo>;
type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn open(io: &SimulatedIo) -> Store {
    let column = |id, name: &str| ColumnConfig {
        id,
        name: name.to_string(),
        key_shape: KeyShape::Variable { most: 96 },
        codec: Codec::None,
        volume: VolumeId(0),
        complete_rows: None,
    };
    let config = StoreConfig {
        memory_budget: BUDGET,
        threads: Default::default(),
        volumes: vec![VolumeConfig {
            name: "scans".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
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
    store.state.volumes[0].is_compaction_held.store(is_held, Ordering::Release);
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
    for _scan in 0..MOST_SCANS {
        scans.push(store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan"));
    }
    let refused = store.iter_from(COLUMN, b"", Direction::Ascending);
    assert!(matches!(refused, Err(StoreError::ScansFull { limit }) if limit == MOST_SCANS as u64));
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
    let mut scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
    let (mut name, mut value) = (Vec::new(), Vec::new());
    assert_eq!(scan.next_row(&mut name, &mut value).expect("step"), ScanStep::Found);
    assert_eq!(scan.next_row(&mut name, &mut value).expect("step"), ScanStep::Corrupt);
    assert_eq!(name, b"b");
    assert_eq!(scan.next_row(&mut name, &mut value).expect("step"), ScanStep::Found);
    assert_eq!((name.as_slice(), value.as_slice()), (&b"c"[..], &b"third value"[..]));
    assert_eq!(scan.next_row(&mut name, &mut value).expect("step"), ScanStep::End);
    drop(scan);
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
            state.write_value(COLUMN, b"key", &value).expect("put");
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
