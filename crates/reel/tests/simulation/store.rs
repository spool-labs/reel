//! The whole engine on the simulated disk, checked against a plain model of versions

use crate::done_flag::SetsOnDrop;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use crate::seed_sets::seeds;
use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::open_segment::{RECORD_WINDOW_BYTES, SegmentPhase, rows_path};
use reel::engine::engine_sizing::OPEN_PER_ROOT;
use reel::engine::reserve_job::RESERVE_PERIOD;
use reel::engine::write_path::{ClaimRequest, SPARE_REQUEST, WRITES_PER_OPEN, WindowSwitch};
use reel::engine::read_path::ReadAt;
use reel::engine::reel_store::ReelStore;
use reel::guard::hold;
use reel::io::{InPlace, Io, IoFile, in_place};
use reel_sim::seeded_random::{Seed, SeededRandom};
use reel_sim::simulated_disk::OperationCount;
use reel_sim::simulated_io::SimulatedIo;
use reel::record::codec::Codec;
use reel_api::column_id::ColumnId;
use reel_api::reads::ValueWindow;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, Nanoseconds, RootId, RowCount, VolumeId};
use reel_api::value_list::{ValueAnswer, ValueList};
use reel_api::write_batch::WriteBatch;

const ROOT: &str = "/disk/meta";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(61 * 1024 * 1024);
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
const QUIET_ROWS: u64 = 100;
const WINDOW_ROWS: u64 = 500;
const WINDOW_VALUE: usize = 300;
const SEND_CRASH_POINTS: u64 = 8;
const LARGE_ENOUGH: usize = 100_000;
const RACE_RECORD_BYTES: usize = 4_000;
const BATCH_ROUNDS: u64 = 20_000;
const RACE_ATTEMPTS: u64 = 20_000_000;
const JOB_STEPS: u64 = 200;
const SEAL_TICK: Nanoseconds = Nanoseconds(1_000_000);
const ONE_ROW: ClaimRequest = ClaimRequest {
    bytes: ByteCount(1),
    rows: RowCount(1),
    is_alone: false,
    has_records: true,
    covers: 0,
    mark: None,
    has_deletes: false,
};

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
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "meta".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync,
            device: Default::default(),
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "track".to_string(),
            key_shape: KeyShape::Variable { most: 64 },
            codec: Codec::None,
            volume: VolumeId(0),
            complete_rows: None,
            least_value: None,
        }],
    }
}

fn open(io: &SimulatedIo) -> Store {
    ReelStore::open(io.clone(), &config(SyncPolicy::Never)).expect("open")
}

fn settle_jobs(io: &SimulatedIo) {
    for _step in 0..JOB_STEPS {
        if io.step_any().is_none() {
            return;
        }
    }
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
    let point = store.state.read_point(&store.state.volumes[0]);
    let hit = store.state.key_read(COLUMN, b"key", point, false).expect("key");
    let older = in_place(store.state.search_footers(&InPlace, volume, &hit)).expect("footers").expect("row");

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

// a put or a get with nobody waiting wakes nobody, since a wake is a system call whether anyone listens or not
#[test]
fn quiet_put() {
    let io = SimulatedIo::new(Seed(90));
    let store = open(&io);
    let mut buf = Vec::new();

    for number in 0..QUIET_ROWS {
        store.put(COLUMN, &number.to_le_bytes(), &[1; LARGE_ENOUGH]).expect("put");
        store.put(COLUMN, &number.to_le_bytes(), b"value").expect("put");
        assert!(store.get(COLUMN, &number.to_le_bytes(), &mut buf).expect("get"));
    }

    let state = &store.state;
    assert_eq!(state.volumes[0].order.wakes(), 0);
    assert_eq!(state.encoders.wakes() + state.stored_shares.wakes() + state.record_readers.wakes(), 0);
    store.close().expect("close");
}

// an owned value lands like any other, and the store keeps nothing of the buffer
#[test]
fn owned_value() {
    let io = SimulatedIo::new(Seed(91));
    let store = open(&io);
    let value = vec![7; LARGE_ENOUGH];

    store.put_owned(COLUMN, b"key", value.clone()).expect("put");

    let mut buf = Vec::new();
    assert!(store.get(COLUMN, b"key", &mut buf).expect("get"));
    assert!(buf == value);
    store.close().expect("close");
}

// a small put makes no write call of its own, and its record reads back before and after its window goes out
#[test]
fn held_record() {
    let io = SimulatedIo::new(Seed(92));
    let store = open(&io);
    store.put(COLUMN, b"first", b"value").expect("put");
    store.flush().expect("flush");

    let before = io.write_calls();
    store.put(COLUMN, b"key", b"value").expect("put");

    assert_eq!(io.write_calls(), before);
    let mut buf = Vec::new();
    assert!(store.get(COLUMN, b"key", &mut buf).expect("get"));
    assert_eq!(buf, b"value");

    store.flush().expect("flush");
    assert!(io.write_calls() > before);
    assert!(store.get(COLUMN, b"key", &mut buf).expect("get"));
    assert_eq!(buf, b"value");
    store.close().expect("close");
}

// a put that asks for durability never returns from the window
#[test]
fn durable_put() {
    let io = SimulatedIo::new(Seed(93));
    let store = ReelStore::open(io.clone(), &config(SyncPolicy::EveryWrite)).expect("open");
    store.put(COLUMN, b"key", b"value").expect("put");
    io.power_cut();
    drop(store);

    let store = ReelStore::open(io.clone(), &config(SyncPolicy::EveryWrite)).expect("open");
    let mut buf = Vec::new();
    assert!(store.get(COLUMN, b"key", &mut buf).expect("get"));
    store.close().expect("close");
}

// a kill with records still in the window loses them and their rows together, so no row lists bytes never written
#[test]
fn window_killed() {
    let io = SimulatedIo::new(Seed(94));
    let store = open(&io);
    store.put(COLUMN, b"older", b"value").expect("put");
    store.flush().expect("flush");
    store.put(COLUMN, b"younger", b"value").expect("put");
    io.kill();
    drop(store);

    let store = open(&io);

    let mut buf = Vec::new();
    assert!(store.get(COLUMN, b"older", &mut buf).expect("get"));
    assert!(!store.get(COLUMN, b"younger", &mut buf).expect("get"));
    store.close().expect("close");
}

// records past one window roll to the next range of the file, and every one reads back after a reopen
#[test]
fn window_rolls() {
    let io = SimulatedIo::new(Seed(95));
    let store = open(&io);
    let value = vec![9; WINDOW_VALUE];
    for number in 0..WINDOW_ROWS {
        store.put(COLUMN, &number.to_le_bytes(), &value).expect("put");
    }
    let mut buf = Vec::new();
    for number in 0..WINDOW_ROWS {
        assert!(store.get(COLUMN, &number.to_le_bytes(), &mut buf).expect("get"));
        assert_eq!(buf, value);
    }
    store.flush().expect("flush");
    io.kill();
    drop(store);

    let store = open(&io);
    for number in 0..WINDOW_ROWS {
        assert!(store.get(COLUMN, &number.to_le_bytes(), &mut buf).expect("get"));
        assert_eq!(buf, value);
    }
    store.close().expect("close");
}

// a get of a held record takes no queue lock, so a writer holding it for as long as it likes can't slow a read
#[test]
fn held_read_free() {
    let io = SimulatedIo::new(Seed(96));
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");
    let slot = hold(&store.state.volumes[0].claims).open[0].slot;

    let guard = hold(&store.state.queues[slot.0 as usize]);
    let mut buf = Vec::new();
    assert!(store.get(COLUMN, b"key", &mut buf).expect("get"));
    drop(guard);

    assert_eq!(buf, b"value");
    store.close().expect("close");
}

// a window read of a held record cuts it like a read from the file
#[test]
fn held_window() {
    let io = SimulatedIo::new(Seed(99));
    let store = open(&io);
    store.put(COLUMN, b"key", b"0123456789").expect("put");
    let window = ValueWindow {
        offset: ByteCount(2),
        length: ByteCount(3),
    };

    let mut buf = Vec::new();
    assert!(store.get_range(COLUMN, b"key", window, &mut buf).expect("window"));

    assert_eq!(buf, b"234");
    store.close().expect("close");
}

// a segment marked to seal gets no new range, so a window can't land under its footer
#[test]
fn no_range_sealing() {
    let io = SimulatedIo::new(Seed(101));
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");
    let volume = &store.state.volumes[0];
    let slot = {
        let mut claims = hold(&volume.claims);
        claims.open[0].phase = SegmentPhase::Sealing;
        claims.open[0].slot
    };
    let before = hold(&volume.claims).open[0].write_end;

    store.state.write_out(slot).expect("write out");
    assert_eq!(store.state.switch_window(volume, slot, RECORD_WINDOW_BYTES), WindowSwitch::HasRoom);

    assert_eq!(hold(&volume.claims).open[0].write_end, before);
    store.close().expect("close");
}

// rows never go out ahead of records a failed write kept back, and both arrive once writes work again
#[test]
fn window_retry() {
    let io = SimulatedIo::new(Seed(102));
    let store = open(&io);
    store.put(COLUMN, b"first", b"value").expect("put");
    store.flush().expect("flush");
    store.put(COLUMN, b"second", b"value").expect("put");
    let slot = hold(&store.state.volumes[0].claims).open[0].slot;
    let rows_before = rows_length(&io);

    io.fail_writes_after(OperationCount(0));
    assert!(store.state.write_out(slot).is_err());
    store.put(COLUMN, b"third", b"value").expect("put");

    assert_eq!(rows_length(&io), rows_before);
    let mut buf = Vec::new();
    assert!(store.get(COLUMN, b"second", &mut buf).expect("get"));
    store.flush().expect("flush");
    io.kill();
    drop(store);
    let store = open(&io);
    assert!(store.get(COLUMN, b"second", &mut buf).expect("get"));
    assert!(store.get(COLUMN, b"third", &mut buf).expect("get"));
    store.close().expect("close");
}

fn rows_length(io: &SimulatedIo) -> u64 {
    let mut total = 0;
    for path in io.list_directory(Path::new(ROOT)).expect("list") {
        if path.extension().is_some_and(|extension| extension == "rows") {
            let file = io.open_read_only(&path).expect("open");
            total += file.length().expect("length").0;
            io.close_file(file);
        }
    }
    total
}

// a crash at any step of a send leaves no row listing bytes that never went out
#[test]
fn send_crash() {
    for crash_at in 0..SEND_CRASH_POINTS {
        for is_power_cut in [false, true] {
            let io = SimulatedIo::new(Seed(crash_at));
            let store = open(&io);
            store.put(COLUMN, b"older", b"value").expect("put");
            store.flush().expect("flush");
            store.put(COLUMN, b"key", b"value").expect("put");

            io.arm_crash(OperationCount(crash_at));
            let flushed = store.flush().is_ok() && !io.is_crashed();
            if is_power_cut {
                io.power_cut();
            } else {
                io.kill();
            }
            drop(store);

            let store = open(&io);
            let mut buf = Vec::new();
            assert!(store.get(COLUMN, b"older", &mut buf).expect("get"), "crash at {crash_at}");
            let found = store.get(COLUMN, b"key", &mut buf).expect("get");
            assert!(found || !flushed, "crash at {crash_at}");
            store.close().expect("close");
        }
    }
}

// a half-full segment asks for the next one, a claim takes it with no sync of its own, and close removes a spare
#[test]
fn segment_ahead() {
    let io = SimulatedIo::new(Seed(97));
    let store = open(&io);
    let volume = &store.state.volumes[0];
    for number in 0..volume.segment_rows.0 / 2 + 1 {
        store.put(COLUMN, &number.to_le_bytes(), b"v").expect("put");
    }
    for _step in 0..SETTLE_STEPS {
        if hold(&volume.claims).prepared.is_some() {
            break;
        }
        if io.step_any().is_none() {
            io.advance_clock(SETTLE_TICK);
        }
    }
    assert!(hold(&volume.claims).prepared.is_some());

    let spare = hold(&volume.claims).prepared.as_ref().map(|segment| segment.number);
    let before = io.write_calls();
    store.seal_now();
    store.put(COLUMN, b"after", b"value").expect("put");

    assert_eq!(hold(&volume.claims).open.iter().find(|segment| segment.phase == SegmentPhase::Taking).map(|segment| segment.number), spare);
    assert_eq!(io.write_calls(), before);
    store.close().expect("close");
}

// a spare no claim took leaves with close, and the next open finds nothing of it
#[test]
fn spare_closed() {
    let io = SimulatedIo::new(Seed(98));
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");
    hold(&store.state.volumes[0].claims).wants_prepared = true;
    store.state.prepare_segments();
    let spare = hold(&store.state.volumes[0].claims).prepared.as_ref().map(|segment| segment.number);
    assert!(spare.is_some());

    store.close().expect("close");
    drop(store);

    let listing = io.list_directory(Path::new(ROOT)).expect("list");
    let spare_rows = spare.map(|number| rows_path(Path::new(ROOT), number).to_path_buf());
    assert!(!listing.iter().any(|path| Some(path) == spare_rows.as_ref()));
    let store = open(&io);
    let mut buf = Vec::new();
    assert!(store.get(COLUMN, b"key", &mut buf).expect("get"));
    store.close().expect("close");
}

// a window that fills moves the slot to the next range with no write call of its own in the way: a writer lands in
// the new window while the old one waits, and both read back
#[test]
fn window_switches() {
    let io = SimulatedIo::new(Seed(103));
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");
    let volume = &store.state.volumes[0];
    let slot = hold(&volume.claims).open[0].slot;
    let index = slot.0 as usize;
    let live = hold(&store.state.queues[index]).live_window;
    let before = hold(&volume.claims).open[0].write_end;

    assert_eq!(store.state.switch_window(volume, slot, RECORD_WINDOW_BYTES), WindowSwitch::Switched);

    assert_eq!(hold(&store.state.queues[index]).live_window, 1 - live);
    assert!(hold(&volume.claims).open[0].write_end > before);
    assert!(!store.state.windows[index][live].is_empty());
    assert!(store.state.try_window(slot, [b"h", b"v"], 1).is_some());
    hold(&store.state.queues[index]).reserved -= 1;
    let mut buf = Vec::new();
    assert!(store.get(COLUMN, b"key", &mut buf).expect("get"));
    assert_eq!(buf, b"value");
    store.close().expect("close");
}

// a window waiting to go keeps its range and its records: the live one can't move onto it
#[test]
fn window_waits() {
    let io = SimulatedIo::new(Seed(104));
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");
    let volume = &store.state.volumes[0];
    let slot = hold(&volume.claims).open[0].slot;
    assert_eq!(store.state.switch_window(volume, slot, RECORD_WINDOW_BYTES), WindowSwitch::Switched);
    let before = hold(&volume.claims).open[0].write_end;

    assert_eq!(store.state.switch_window(volume, slot, RECORD_WINDOW_BYTES + 1), WindowSwitch::OtherBusy);

    assert_eq!(hold(&volume.claims).open[0].write_end, before);
    store.close().expect("close");
}

// a writer switching windows during a send loses nothing: a record pushed before a send is in the file when it returns
#[test]
fn window_race() {
    let io = SimulatedIo::new(Seed(105));
    io.keep_real_time();
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");
    let volume = &store.state.volumes[0];
    let slot = hold(&volume.claims).open[0].slot;
    let index = slot.0 as usize;
    let pushed = Mutex::new(Vec::new());
    let is_done = AtomicBool::new(false);
    let fill = [0; RACE_RECORD_BYTES];

    thread::scope(|scope| {
        scope.spawn(|| {
            let _done = SetsOnDrop(&is_done);
            let mut number = 0u64;
            // bounded, so a failed check in the sender ends the test with nobody left to send
            for _attempt in 0..RACE_ATTEMPTS {
                let bytes = number.to_le_bytes();
                if let Some(offset) = store.state.try_window(slot, [&bytes, &fill], 1) {
                    hold(&store.state.queues[index]).reserved -= 1;
                    hold(&pushed).push((offset, number));
                    number += 1;
                } else if store.state.switch_window(volume, slot, RACE_RECORD_BYTES + bytes.len()) == WindowSwitch::NoRoom {
                    break;
                }
            }
        });

        let mut checked = 0;
        while !is_done.load(Ordering::Acquire) {
            let seen = hold(&pushed).len();
            store.state.take_writing_turn(slot);
            store.state.send_windows(index).expect("send");
            hold(&store.state.queues[index]).is_writing = false;

            let file = store.state.slot_files[index].file().expect("a data file");
            let records = hold(&pushed)[checked..seen].to_vec();
            for (offset, number) in records {
                let mut bytes = [0; size_of::<u64>()];
                file.read_at(offset, &mut bytes).expect("read");
                assert_eq!(u64::from_le_bytes(bytes), number);
            }
            checked = seen;
        }
    });
    store.close().expect("close");
}

// a writer behind a long sync pauses with nothing held, and sends the kept window once the turn frees
#[test]
fn window_pauses() {
    let io = SimulatedIo::new(Seed(106));
    io.keep_real_time();
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");
    let slot = hold(&store.state.volumes[0].claims).open[0].slot;
    let index = slot.0 as usize;
    let volume = &store.state.volumes[0];
    assert_eq!(store.state.switch_window(volume, slot, RECORD_WINDOW_BYTES), WindowSwitch::Switched);
    hold(&store.state.queues[index]).is_writing = true;
    let start = io.now();

    thread::scope(|scope| {
        let waiter = scope.spawn(|| store.state.wait_for_kept_window(slot));
        while io.now() == start {
            thread::yield_now();
        }
        assert!(!waiter.is_finished());
        hold(&store.state.queues[index]).is_writing = false;
        waiter.join().expect("join").expect("send");
    });

    assert!(!hold(&store.state.queues[index]).has_kept_window);
    store.close().expect("close");
}

// covers that fit nowhere seal one segment at a time, and the others keep taking writes
#[test]
fn covers_seal_one() {
    let io = SimulatedIo::new(Seed(107));
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");
    let volume = &store.state.volumes[0];
    hold(&volume.claims).wants_prepared = true;
    store.state.prepare_segments();

    let mut claims = hold(&volume.claims);
    let spare = claims.prepared.take().expect("a spare");
    volume.open_slot(spare.slot);
    claims.open.push(spare);
    store.state.seal_most_covers(&mut claims, volume);
    store.state.seal_most_covers(&mut claims, volume);

    let sealing = claims.open.iter().filter(|segment| segment.phase == SegmentPhase::Sealing).count();
    assert_eq!(sealing, 1);
    drop(claims);
    store.close().expect("close");
}

// eight writers in flight reach every segment a root may have open, in a few job steps inside the job's period
#[test]
fn spares_spread() {
    let io = SimulatedIo::new(Seed(108));
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");
    let volume = &store.state.volumes[0];
    let start = io.clock_reading();

    let mut held = Vec::new();
    for _writer in 0..OPEN_PER_ROOT as u32 * WRITES_PER_OPEN {
        held.push(store.state.claim(volume, &ONE_ROW).expect("claim"));
        settle_jobs(&io);
    }

    let claims = hold(&volume.claims);
    let taking = claims.open.iter().filter(|segment| segment.phase == SegmentPhase::Taking).count();
    assert_eq!(taking, OPEN_PER_ROOT);
    assert!(io.clock_reading().0 - start.0 < RESERVE_PERIOD.0);
    drop(claims);
    drop(held);
    store.close().expect("close");
}

// a spare made while a second ask waits leaves nothing asked, so the claim that takes it wakes the job
#[test]
fn spare_asks_again() {
    let io = SimulatedIo::new(Seed(109));
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");
    let volume = &store.state.volumes[0];
    settle_jobs(&io);

    hold(&volume.claims).is_creating = true;
    let created = store.state.create_segment(volume, RootId(0), &SPARE_REQUEST);
    store.state.ask_for_segment(&mut hold(&volume.claims));
    store.state.finish_create(volume, created);
    settle_jobs(&io);

    {
        let mut claims = hold(&volume.claims);
        let spare = claims.prepared.take().expect("a spare");
        volume.open_slot(spare.slot);
        claims.open.push(spare);
        store.state.ask_for_segment(&mut claims);
    }
    settle_jobs(&io);

    assert!(hold(&volume.claims).prepared.is_some());
    store.close().expect("close");
}

// a slot a seal frees goes to the spare the job couldn't make, well before the job's period
#[test]
fn spare_after_seal() {
    let io = SimulatedIo::new(Seed(110));
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");
    let volume = &store.state.volumes[0];
    let mut taken = Vec::new();
    while let Some(slot) = store.state.take_slot() {
        taken.push(slot);
    }
    hold(&volume.claims).wants_prepared = true;
    store.state.prepare_segments();
    assert!(hold(&volume.claims).prepared.is_none());
    let start = io.clock_reading();

    {
        let mut claims = hold(&volume.claims);
        store.state.mark_fullest(&mut claims, volume);
    }
    for _step in 0..SEAL_STEPS {
        io.advance_clock(SEAL_TICK);
        settle_jobs(&io);
        if hold(&volume.claims).prepared.is_some() {
            break;
        }
    }

    assert!(hold(&volume.claims).prepared.is_some());
    assert!(io.clock_reading().0 - start.0 < RESERVE_PERIOD.0);
    for slot in taken {
        store.state.free_slot(slot);
    }
    store.close().expect("close");
}

// every key gets its answer in order: a value, nothing, or a bad record
#[test]
fn many_answers() {
    let io = SimulatedIo::new(Seed(118));
    let store = open(&io);
    store.put(COLUMN, b"first", b"one").expect("put");
    store.put(COLUMN, b"third", b"three").expect("put");
    store.seal_now();
    settle(&io);
    store.put(COLUMN, b"fourth", b"four").expect("put");

    let mut values = ValueList::new();
    let keys: [&[u8]; 4] = [b"first", b"second", b"third", b"fourth"];
    store.get_many(COLUMN, &keys, &mut values).expect("get_many");

    assert_eq!(values.len(), 4);
    assert_eq!(values.answer(0), Some(ValueAnswer::Found(b"one")));
    assert_eq!(values.answer(1), Some(ValueAnswer::Missing));
    assert_eq!(values.answer(2), Some(ValueAnswer::Found(b"three")));
    assert_eq!(values.answer(3), Some(ValueAnswer::Found(b"four")));
    store.close().expect("close");
}

// a read at one point sees what was there then, and leaves nothing older in the spot cache for a later get
#[test]
fn many_old_point() {
    let io = SimulatedIo::new(Seed(119));
    let store = open(&io);
    store.state.volumes[0].is_compaction_held.store(true, Ordering::Release);
    store.put(COLUMN, b"key", b"older").expect("put");
    store.seal_now();
    settle(&io);
    let point = store.state.read_point(&store.state.volumes[0]);
    store.put(COLUMN, b"key", b"newer").expect("put");
    store.seal_now();
    settle(&io);

    let mut values = ValueList::new();
    let mut scratch = Vec::new();
    let handovers = store.state.volumes[0].handover_count();
    in_place(store.state.read_keys(&InPlace, COLUMN, &[b"key"], ReadAt { point, handovers }, &mut values, &mut scratch)).expect("read");

    assert_eq!(values.answer(0), Some(ValueAnswer::Found(b"older")));
    let mut buf = Vec::new();
    assert!(store.get(COLUMN, b"key", &mut buf).expect("get"));
    assert_eq!(buf, b"newer");
    store.close().expect("close");
}

// a batch is seen whole or not at all, while a writer rewrites it
#[test]
fn many_batch_whole() {
    let io = SimulatedIo::new(Seed(120));
    io.keep_real_time();
    let store = open(&io);
    let is_done = AtomicBool::new(false);
    let keys: [&[u8]; 3] = [b"a", b"b", b"c"];

    thread::scope(|scope| {
        scope.spawn(|| {
            let _done = SetsOnDrop(&is_done);
            for number in 0..BATCH_ROUNDS {
                let mut batch = WriteBatch::new();
                for key in keys {
                    batch.put(COLUMN, key, &number.to_le_bytes());
                }
                store.write_batch(&batch).expect("batch");
            }
        });
        let mut values = ValueList::new();
        while !is_done.load(Ordering::Acquire) {
            store.get_many(COLUMN, &keys, &mut values).expect("get_many");
            assert_eq!(values.answer(0), values.answer(1));
            assert_eq!(values.answer(1), values.answer(2));
        }
    });
    store.close().expect("close");
}
