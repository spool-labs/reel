//! The whole engine on the simulated disk, checked against a plain model of versions

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::seed_sets::seeds;
use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel_sim::seeded_random::{Seed, SeededRandom};
use reel_sim::simulated_disk::OperationCount;
use reel_sim::simulated_io::SimulatedIo;
use reel::record::codec::Codec;
use reel_api::column_id::ColumnId;
use reel_api::reads::ValueWindow;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, Nanoseconds, VolumeId};

const ROOT: &str = "/disk/meta";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(32 * 1024 * 1024);
const KEY_SPACE: u64 = 400;
const OPERATIONS: u64 = 3_000;
const SEED_COUNT: u64 = 8;
const MANY_ROWS: u64 = 30_000;
const SETTLE_STEPS: u64 = 2_000_000;
const KILL_WINDOW: Nanoseconds = Nanoseconds(100_000_000);
const TICK: Nanoseconds = Nanoseconds(5_000_000);
const MOST_VALUE_BYTES: u64 = 300;
const SETTLE_TICK: Nanoseconds = Nanoseconds(1_000_000);
const SEAL_ROWS: u64 = 200;
const SEAL_STEPS: u64 = 1_000;
const SEAL_CRASH_POINTS: u64 = 14;
const SEAL_CRASH_SEEDS: u64 = 3;
const FAILED_OPEN_POINTS: u64 = 40;

type Store = ReelStore<SimulatedIo>;

/// What a crash can do to writes no sync covered
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Crash {
    /// The kernel keeps what reached it
    Kill,
    /// Only what a sync covered is sure to remain
    PowerCut,
}

/// One key's versions in write order: a value, or nothing for a delete
#[derive(Default)]
struct Versions {
    history: Vec<Option<Vec<u8>>>,
    survives_kill: usize,
    survives_power_cut: usize,
}

/// A plain map of versions, with the writes a crash may lose
#[derive(Default)]
struct Model {
    keys: BTreeMap<Vec<u8>, Versions>,
    returned: Vec<(Vec<u8>, usize, Nanoseconds)>,
}

impl Model {
    fn record(&mut self, key: &[u8], value: Option<Vec<u8>>, now: Nanoseconds) {
        let versions = self.keys.entry(key.to_vec()).or_default();
        versions.history.push(value);
        self.returned.push((key.to_vec(), versions.history.len(), now));
    }

    fn newest(&self, key: &[u8]) -> Option<&Vec<u8>> {
        let versions = self.keys.get(key)?;
        versions.history.last()?.as_ref()
    }

    /// Every write that returned before a time must survive any later crash
    fn settle_before(&mut self, time: Nanoseconds) {
        let mut kept = Vec::new();
        for (key, count, returned_at) in self.returned.drain(..) {
            if returned_at <= time {
                if let Some(versions) = self.keys.get_mut(&key) {
                    versions.survives_kill = versions.survives_kill.max(count);
                }
            } else {
                kept.push((key, count, returned_at));
            }
        }
        self.returned = kept;
    }

    /// Every write so far must survive, as after a flush
    fn settle_all(&mut self) {
        for versions in self.keys.values_mut() {
            versions.survives_kill = versions.history.len();
            versions.survives_power_cut = versions.history.len();
        }
        self.returned.clear();
    }

    /// Check what a key reads after a crash, and take it as the truth from here
    fn accept_crash(&mut self, key: &[u8], found: Option<Vec<u8>>, crash: Crash) {
        let Some(versions) = self.keys.get_mut(key) else {
            assert_eq!(found, None);
            return;
        };

        let mut accepted = None;
        let floor = match crash {
            Crash::Kill => versions.survives_kill.max(versions.survives_power_cut),
            Crash::PowerCut => versions.survives_power_cut,
        };
        let first = floor.max(1) - 1;
        for index in first..versions.history.len() {
            if versions.history[index] == found {
                accepted = Some(index);
            }
        }
        let is_nothing_written = floor == 0 && found.is_none();
        match accepted {
            Some(index) => versions.history.truncate(index + 1),
            None if is_nothing_written => versions.history.clear(),
            None => panic!("key {key:?} read {found:?}, which no surviving write left"),
        }
        versions.survives_kill = versions.history.len();
        versions.survives_power_cut = versions.survives_power_cut.min(versions.history.len());
    }
}

fn config(sync: SyncPolicy) -> StoreConfig {
    StoreConfig {
        memory_budget: BUDGET,
        volumes: vec![VolumeConfig {
            name: "meta".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync,
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "track".to_string(),
            key_shape: KeyShape::Variable { most: 64 },
            codec: Codec::None,
            volume: VolumeId(0),
        }],
    }
}

fn open(io: &SimulatedIo) -> Store {
    ReelStore::open(io.clone(), &config(SyncPolicy::Never)).expect("open")
}

fn key_of(number: u64) -> Vec<u8> {
    format!("key-{number:08}").into_bytes()
}

fn value_of(random: &mut SeededRandom, place: u64) -> Vec<u8> {
    let length = random.below(MOST_VALUE_BYTES) as usize;
    let mut value = place.to_le_bytes().to_vec();
    value.resize(length + value.len(), place as u8);
    value
}

fn read(store: &Store, key: &[u8]) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    match store.get(COLUMN, key, &mut buf).expect("get") {
        true => Some(buf),
        false => None,
    }
}

fn settle(io: &SimulatedIo) {
    for _step in 0..SETTLE_STEPS {
        if io.step_any().is_none() {
            // a job waiting a moment for a writer or the mark runs again after this
            io.advance_clock(SETTLE_TICK);
            if io.step_any().is_none() {
                return;
            }
        }
    }
    panic!("jobs never went quiet");
}

// a value reads back, a new one replaces it, and a delete hides it
#[test]
fn put_get() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io);

    assert_eq!(read(&store, b"key"), None);
    store.put(COLUMN, b"key", b"first").expect("put");
    assert_eq!(read(&store, b"key"), Some(b"first".to_vec()));
    store.put(COLUMN, b"key", b"second").expect("put");
    assert_eq!(read(&store, b"key"), Some(b"second".to_vec()));
    store.delete(COLUMN, b"key").expect("delete");
    assert_eq!(read(&store, b"key"), None);
    assert!(!store.contains(COLUMN, b"key").expect("contains"));

    store.close().expect("close");
    assert_eq!(io.open_handle_count(), 0);
    assert_eq!(io.handle_faults(), OperationCount(0));
}

// a wrong key, an unknown column and a closed store are refused
#[test]
fn refusals() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io);

    let long_key = vec![0; 65];
    let wrong_width = store.put(COLUMN, &long_key, b"value");
    assert!(matches!(wrong_width, Err(StoreError::KeyWidth { .. })));
    let unknown = store.put(ColumnId(9), b"key", b"value");
    assert!(matches!(unknown, Err(StoreError::UnknownColumn { .. })));

    store.close().expect("close");
    assert!(matches!(store.put(COLUMN, b"key", b"value"), Err(StoreError::Closed)));
}

// a second writer is refused, and it learns the process that holds the store
#[test]
fn second_writer() {
    let io = SimulatedIo::new(Seed(3));
    let store = open(&io);

    let second = ReelStore::open(io.clone(), &config(SyncPolicy::Never));

    assert!(matches!(second, Err(StoreError::Locked { .. })));
    store.close().expect("close");
}

// a clean close and open keeps every write, with no job left running
#[test]
fn close_open() {
    let io = SimulatedIo::new(Seed(4));
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");
    store.put(COLUMN, b"gone", b"value").expect("put");
    store.delete(COLUMN, b"gone").expect("delete");
    store.close().expect("close");
    drop(store);

    let store = open(&io);
    assert_eq!(read(&store, b"key"), Some(b"value".to_vec()));
    assert_eq!(read(&store, b"gone"), None);
    store.close().expect("close");
}

// rows pass through seals into footers, and every key still reads its newest value
#[test]
fn through_seals() {
    let io = SimulatedIo::new(Seed(5));
    let store = open(&io);
    let mut random = SeededRandom::new(Seed(55));
    let mut model = Model::default();

    for place in 0..MANY_ROWS {
        let key = key_of(place % (KEY_SPACE * 20));
        let value = value_of(&mut random, place);
        store.put(COLUMN, &key, &value).expect("put");
        model.record(&key, Some(value), Nanoseconds(0));
        io.step_any();
    }
    settle(&io);

    for key in model.keys.keys() {
        assert_eq!(read(&store, key).as_ref(), model.newest(key));
    }
    store.close().expect("close");
    assert_eq!(io.open_handle_count(), 0);
    assert_eq!(io.handle_faults(), OperationCount(0));

    drop(store);
    let store = open(&io);
    settle(&io);
    for key in model.keys.keys() {
        assert_eq!(read(&store, key).as_ref(), model.newest(key));
    }
    store.close().expect("close");
}

// a window of a value reads back from the write index and from a footer
#[test]
fn value_window() {
    let io = SimulatedIo::new(Seed(6));
    let store = open(&io);
    store.put(COLUMN, b"key", b"0123456789").expect("put");

    let window = ValueWindow {
        offset: ByteCount(3),
        length: ByteCount(4),
    };
    let mut buf = Vec::new();

    assert!(store.get_range(COLUMN, b"key", window, &mut buf).expect("window"));
    assert_eq!(buf, b"3456");
    store.close().expect("close");
}

// random writes, kills, power cuts and reopens never lose a write that had to survive
#[test]
fn model_crashes() {
    for seed in seeds(0..SEED_COUNT) {
        run_crashes(Seed(seed));
    }
}

fn run_crashes(seed: Seed) {
    let io = SimulatedIo::new(seed);
    let mut random = SeededRandom::new(seed);
    let mut store = open(&io);
    let mut model = Model::default();
    let mut clock = Nanoseconds(0);

    for place in 0..OPERATIONS {
        let key = key_of(random.below(KEY_SPACE));
        match random.below(100) {
            0..60 => {
                let value = value_of(&mut random, place);
                store.put(COLUMN, &key, &value).expect("put");
                model.record(&key, Some(value), clock);
            },
            60..80 => {
                store.delete(COLUMN, &key).expect("delete");
                model.record(&key, None, clock);
            },
            80..97 => assert_eq!(read(&store, &key).as_ref(), model.newest(&key)),
            97 => {
                store.flush().expect("flush");
                model.settle_all();
            },
            98 => {
                // jobs run ahead of the clock, so every group is written inside its window
                settle(&io);
                io.advance_clock(KILL_WINDOW);
                clock = Nanoseconds(clock.0 + KILL_WINDOW.0);
                settle(&io);
                model.settle_before(Nanoseconds(clock.0 - KILL_WINDOW.0));
                io.kill();
                drop(store);
                store = open(&io);
                reconcile(&store, &mut model, Crash::Kill);
            },
            _power => {
                io.power_cut();
                drop(store);
                store = open(&io);
                reconcile(&store, &mut model, Crash::PowerCut);
            },
        }

        if random.one_in(4) {
            io.step_any();
        }
        if random.one_in(8) {
            io.advance_clock(TICK);
            clock = Nanoseconds(clock.0 + TICK.0);
        }
    }

    store.close().expect("close");
    assert_eq!(io.open_handle_count(), 0);
    assert_eq!(io.handle_faults(), OperationCount(0));
}

fn reconcile(store: &Store, model: &mut Model, crash: Crash) {
    let mut keys = Vec::with_capacity(model.keys.len());
    for key in model.keys.keys() {
        keys.push(key.clone());
    }
    for key in keys {
        let found = read(store, &key);
        model.accept_crash(&key, found, crash);
    }
    model.returned.clear();
}

fn fill_sealable(store: &Store, rows: u64) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut expected = BTreeMap::new();
    for number in 0..rows {
        let key = key_of(number);
        let value = format!("value-{number}").into_bytes();
        store.put(COLUMN, &key, &value).expect("put");
        expected.insert(key, value);
    }
    expected
}

fn check_all(store: &Store, expected: &BTreeMap<Vec<u8>, Vec<u8>>) {
    for (key, value) in expected {
        assert_eq!(read(store, key).as_ref(), Some(value));
    }
}

// a crash at any operation of a seal loses no flushed write, through two reopens
#[test]
fn seal_crashes() {
    for crash_at in 0..SEAL_CRASH_POINTS {
        for seed in seeds(0..SEAL_CRASH_SEEDS) {
            for crash in [Crash::Kill, Crash::PowerCut] {
                crash_during_seal(OperationCount(crash_at), Seed(seed), crash);
            }
        }
    }
}

fn crash_during_seal(crash_at: OperationCount, seed: Seed, crash: Crash) {
    let io = SimulatedIo::new(seed);
    let store = open(&io);
    let expected = fill_sealable(&store, SEAL_ROWS);
    store.flush().expect("flush");

    store.seal_now();
    io.arm_crash(crash_at);
    for _step in 0..SEAL_STEPS {
        if io.step_any().is_none() || io.is_crashed() {
            break;
        }
    }

    match crash {
        Crash::Kill => io.kill(),
        Crash::PowerCut => io.power_cut(),
    }
    drop(store);

    let store = open(&io);
    check_all(&store, &expected);
    settle(&io);
    check_all(&store, &expected);

    io.kill();
    drop(store);
    let store = open(&io);
    check_all(&store, &expected);
    store.close().expect("close");
    assert_eq!(io.open_handle_count(), 0);
    assert_eq!(io.handle_faults(), OperationCount(0));
}

// every step of a seal answers the same, for keys in the sealing segment and for keys written beside it
#[test]
fn gets_through_seal() {
    let io = SimulatedIo::new(Seed(7));
    let store = open(&io);
    let mut expected = fill_sealable(&store, SEAL_ROWS);
    store.put(COLUMN, b"gone-later", b"old").expect("put");

    store.seal_now();
    store.put(COLUMN, &key_of(1), b"newer-beside-the-seal").expect("put");
    expected.insert(key_of(1), b"newer-beside-the-seal".to_vec());
    store.delete(COLUMN, b"gone-later").expect("delete");

    let mut steps = 0;
    loop {
        check_all(&store, &expected);
        assert_eq!(read(&store, b"gone-later"), None);
        steps += 1;
        if io.step_any().is_none() {
            break;
        }
    }

    assert!(steps > 3);
    store.close().expect("close");
}

// a sealed grave takes its key out of the spot cache, so a cached value can't come back
#[test]
fn spot_grave() {
    let io = SimulatedIo::new(Seed(8));
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");
    store.seal_now();
    settle(&io);
    assert_eq!(read(&store, b"key"), Some(b"value".to_vec()));
    assert_eq!(read(&store, b"key"), Some(b"value".to_vec()));

    store.delete(COLUMN, b"key").expect("delete");
    store.seal_now();
    settle(&io);

    assert_eq!(read(&store, b"key"), None);
    store.close().expect("close");
}

// a get that started before a hand-over can't put its older row in the spot cache
#[test]
fn spot_fence() {
    let io = SimulatedIo::new(Seed(9));
    let store = open(&io);
    store.put(COLUMN, b"key", b"older").expect("put");
    store.seal_now();
    settle(&io);

    let volume = &store.state.volumes[0];
    let before = volume.handover_count();
    let point = store.state.read_point();
    let hit = store.state.key_read(COLUMN, b"key", point, false).expect("key");
    let older = store.state.search_footers(volume, &hit).expect("footers").expect("row");

    store.put(COLUMN, b"key", b"newer").expect("put");
    store.seal_now();
    settle(&io);
    store.state.spot.remove(hit.hash);
    store.state.fill_spot(volume, &hit, &older, before);

    assert_eq!(read(&store, b"key"), Some(b"newer".to_vec()));
    store.close().expect("close");
}

// an open that fails at any step leaves no handle behind, so a caller can try again
#[test]
fn failed_open() {
    for crash_at in 0..FAILED_OPEN_POINTS {
        let io = SimulatedIo::new(Seed(crash_at));
        let store = open(&io);
        fill_sealable(&store, SEAL_ROWS);
        store.seal_now();
        settle(&io);
        fill_sealable(&store, SEAL_ROWS);
        store.flush().expect("flush");
        io.kill();
        drop(store);

        io.arm_crash(OperationCount(crash_at));
        let opened = ReelStore::open(io.clone(), &config(SyncPolicy::Never));
        if opened.is_err() {
            assert_eq!(io.open_handle_count(), 0, "crash at {crash_at}");
        }
        io.kill();
        drop(opened);

        let store = open(&io);
        store.close().expect("close");
        assert_eq!(io.handle_faults(), OperationCount(0), "crash at {crash_at}");
    }
}

// a close with a failed volume says so, since a write that returned there may not be durable
#[test]
fn failed_close() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");
    io.fail_syncs(OperationCount(1));
    assert!(store.flush().is_err());

    assert!(store.close().is_err());
}
