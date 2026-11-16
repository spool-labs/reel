//! The whole engine on the simulated disk, checked against a plain model of versions

use crate::done_flag::SetsOnDrop;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use crate::seed_sets::seeds;
use reel::config::{ColumnConfig, ColumnIndex, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::open_segment::{RECORD_WINDOW_BYTES, SegmentPhase, reserve_step, rows_path};
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
use reel_core::column_id::ColumnId;
use reel_core::reads::ValueWindow;
use reel_core::store_error::StoreError;
use reel_core::units::{ByteCount, Nanoseconds, RootId, RowCount, VolumeId};
use reel_core::value_list::{ReadBuffers, ValueAnswer, ValueList};
use reel_core::write_batch::WriteBatch;
use reel_sim::wait::wait_until;

const ROOT: &str = "/disk/meta";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(63 * 1024 * 1024);
const ROOM_BUDGET: ByteCount = ByteCount(400 * 1024 * 1024);
const ROOM_VALUE: usize = 1024 * 1024;
const ROOM_VALUES: u64 = 100;
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
const MANY_KEYS: u64 = 150;
const MANY_ROUNDS: u64 = 3;
const MANY_ABSENT: u64 = 10;
/// Past the largest record a batch reads, so some records read on their own
const MANY_WIDEST: u64 = 12 * 1024;
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
            device: Default::default(), open_segments: None, segment_bytes: None, lifetime_groups: None,
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "track".to_string(),
            key_shape: KeyShape::Variable { most: 64 },
            codec: Codec::None,
            volume: VolumeId(0),
            index: ColumnIndex::Plain,
            least_rows: None,
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
        wait_until(|| io.now() != start, "the clock never moved");
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
    while let Some(slot) = store.state.take_slot(0) {
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
    store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
    store.put(COLUMN, b"key", b"older").expect("put");
    store.seal_now();
    settle(&io);
    let point = store.state.read_point(&store.state.volumes[0]);
    store.put(COLUMN, b"key", b"newer").expect("put");
    store.seal_now();
    settle(&io);

    let mut values = ValueList::new();
    let mut buffers = ReadBuffers::default();
    let handovers = store.state.volumes[0].handover_count();
    in_place(store.state.read_keys(&InPlace, COLUMN, &[b"key"], ReadAt { point, handovers }, &mut values, &mut buffers)).expect("read");

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

// get_many gives every key the answer get gives it over batches of sealed, open, deleted, cached and wide records, in any
// order and with keys that were never written, and it reads the small sealed ones together
#[test]
fn many_agrees_over_batches() {
    for seed in seeds(0..SEED_COUNT) {
        let io = SimulatedIo::new(Seed(seed));
        let store = open(&io);
        let mut random = SeededRandom::new(Seed(seed));
        let mut model = BTreeMap::new();
        for round in 0..MANY_ROUNDS {
            for number in 0..MANY_KEYS {
                let key = key_of(number);
                match random.below(10) {
                    0..7 => {
                        let mut value = (round * MANY_KEYS + number).to_le_bytes().to_vec();
                        value.resize(value.len() + random.below(MANY_WIDEST) as usize, number as u8);
                        store.put(COLUMN, &key, &value).expect("put");
                        model.insert(key, value);
                    },
                    7 => {
                        store.delete(COLUMN, &key).expect("delete");
                        model.remove(&key);
                    },
                    _ => {},
                }
            }
            // the last round stays open
            if round + 1 < MANY_ROUNDS {
                store.seal_now();
                settle(&io);
            }
        }
        // a third of the keys go through get first, so the spot and record caches hold them
        for number in 0..MANY_KEYS {
            if random.below(3) == 0 {
                read(&store, &key_of(number));
            }
        }
        let mut keys: Vec<Vec<u8>> = (0..MANY_KEYS).map(key_of).chain((0..MANY_ABSENT).map(|number| format!("absent-{number}").into_bytes())).collect();
        for index in (1..keys.len()).rev() {
            keys.swap(index, random.below(index as u64 + 1) as usize);
        }
        let asked: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();

        let before = store.state.batched_gets.load(Ordering::Relaxed);
        let mut values = ValueList::new();
        store.get_many(COLUMN, &asked, &mut values).expect("get_many");

        assert_eq!(values.len(), asked.len());
        for (index, key) in asked.iter().enumerate() {
            let want = model.get(*key).map(Vec::as_slice);
            assert_eq!(read(&store, key).as_deref(), want, "seed {seed}: get of {key:?}");
            let answer = values.answer(index).expect("an answer");
            match want {
                Some(value) => assert_eq!(answer, ValueAnswer::Found(value), "seed {seed}: key {key:?}"),
                None => assert_eq!(answer, ValueAnswer::Missing, "seed {seed}: key {key:?}"),
            }
        }
        assert!(store.state.batched_gets.load(Ordering::Relaxed) > before, "seed {seed}: no key came from a batch read");
        store.close().expect("close");
    }
}

// an open segment holds a step of disk at create and gains the next step as its writes pass it, so a store never holds a whole
// segment of disk ahead of what it wrote, and a spare holds a step alone
#[test]
fn room_grows_a_step_at_a_time() {
    let io = SimulatedIo::new(Seed(61));
    let mut config = config(SyncPolicy::Never);
    config.memory_budget = ROOM_BUDGET;
    let store = ReelStore::open(io.clone(), &config).expect("open");
    let volume = &store.state.volumes[0];
    let step = reserve_step(volume.segment_bytes);
    assert!(volume.segment_bytes.0 > 2 * step, "a segment of {} B takes no step", volume.segment_bytes.0);
    let value = vec![7; ROOM_VALUE];
    let held = || {
        let claims = hold(&volume.claims);
        let segment = claims.open.iter().find(|segment| segment.phase == SegmentPhase::Taking).expect("an open segment");
        let spare = claims.prepared.as_ref().map(|spare| spare.data.length().expect("length").0);
        (segment.data.length().expect("length").0, segment.write_end.0, spare)
    };

    store.put(COLUMN, b"first", &value).expect("put");
    let (length, _written, _spare) = held();
    assert_eq!(length, step);

    for number in 0..ROOM_VALUES {
        store.put(COLUMN, format!("key-{number:04}").as_bytes(), &value).expect("put");
    }
    settle(&io);
    let (length, written, spare) = held();
    assert!(written > step, "the writes stayed inside the first step");
    // the job takes the next step once the writes come within half a step of the room's end
    assert!(length >= written && length < written + step * 3 / 2, "{length} B held for {written} B written");
    assert!(length < volume.segment_bytes.0, "a whole segment held");
    assert!(spare.is_none_or(|spare| spare <= step), "a spare holds {spare:?} B");
    store.close().expect("close");
}


// a quiet open segment gives its room ahead back after the idle seal's quiet and takes it again at its next write, and the report
// shows the room apart from what was written
#[test]
fn quiet_room_goes_back() {
    let io = SimulatedIo::new(Seed(63));
    let mut config = config(SyncPolicy::Never);
    config.memory_budget = ROOM_BUDGET;
    let store = ReelStore::open(io.clone(), &config).expect("open");
    let volume = &store.state.volumes[0];
    let held = || {
        let claims = hold(&volume.claims);
        let segment = claims.open.iter().find(|segment| segment.phase == SegmentPhase::Taking && segment.row_count.0 > 0).expect("an open segment");
        (segment.data.length().expect("length").0, segment.write_end.0)
    };
    let preallocated = || {
        let mut report = reel_core::maintenance_report::MaintenanceReport::default();
        store.maintenance_report(&mut report);
        report.volumes[0].preallocated.0
    };

    store.put(COLUMN, b"first", &[7; ROOM_VALUE]).expect("put");
    let (length, written) = held();
    assert!(length > written + reel::engine::row_job::GIVE_BACK_LEAST);
    assert!(preallocated() >= length - written);

    settle(&io);
    io.advance_clock(Nanoseconds(reel::engine::row_job::IDLE_SEAL_AFTER.0 + SETTLE_TICK.0));
    settle(&io);
    let (length, written) = held();
    assert_eq!(length, written);
    assert_eq!(volume.rooms_given_back.load(Ordering::Relaxed), 1);
    assert_eq!(read(&store, b"first"), Some(vec![7; ROOM_VALUE]));

    store.put(COLUMN, b"second", &[8; ROOM_VALUE]).expect("put");
    let (length, written) = held();
    assert!(length > written, "the next write took no room");
    assert_eq!(read(&store, b"second"), Some(vec![8; ROOM_VALUE]));
    store.close().expect("close");
}

// a small store steps its room ahead by a share of its segment, short of the 64 MiB a large one takes
#[test]
fn small_store_steps_less() {
    let io = SimulatedIo::new(Seed(64));
    let store = ReelStore::open(io.clone(), &config(SyncPolicy::Never)).expect("open");
    let volume = &store.state.volumes[0];
    let step = reserve_step(volume.segment_bytes);
    assert!(step < reel::engine::open_segment::RESERVE_STEP_BYTES, "a segment of {} B steps a whole 64 MiB", volume.segment_bytes.0);
    store.put(COLUMN, b"first", b"value").expect("put");
    let claims = hold(&volume.claims);
    let segment = claims.open.iter().find(|segment| segment.row_count.0 > 0).expect("an open segment");
    assert!(segment.data.length().expect("length").0 <= step);
    drop(claims);
    store.close().expect("close");
}

// room a segment asked for before its seal never grows the sealed file, where it once buried the tails and the next open lost the
// segment as damaged
#[test]
fn room_skips_a_sealing_segment() {
    let io = SimulatedIo::new(Seed(62));
    let mut config = config(SyncPolicy::Never);
    config.memory_budget = ROOM_BUDGET;
    let store = ReelStore::open(io.clone(), &config).expect("open");
    let value = vec![7; ROOM_VALUE];
    io.hold_jobs("reel-reserve", true);
    // a write that ends within half a step of the room asks the held job for more, and the seal has to start with that ask out
    let is_asked = || hold(&store.state.volumes[0].claims).open.iter().any(|segment| segment.is_reserve_asked);
    let mut written = 0;
    while written < ROOM_VALUES || !is_asked() {
        store.put(COLUMN, format!("key-{written:04}").as_bytes(), &value).expect("put");
        written += 1;
        assert!(written < 4 * ROOM_VALUES, "no write asked for room");
    }
    store.seal_now();
    // step until the seal has cut the file to its tails while the segment still sits among the open ones, then hold the seal
    // there and let the room job look
    let is_cut = || {
        let claims = hold(&store.state.volumes[0].claims);
        claims.open.iter().any(|segment| segment.is_reserve_asked && segment.data.length().expect("length").0 < segment.reserved_end.0)
    };
    while !is_cut() {
        assert!(io.step_any().is_some(), "the seal ended before it cut the file");
    }
    io.hold_jobs("reel-seal", true);
    io.hold_jobs("reel-reserve", false);
    io.wake_jobs("reel-reserve");
    settle(&io);
    io.hold_jobs("reel-seal", false);
    io.wake_jobs("reel-seal");
    settle(&io);
    store.close().expect("close");
    drop(store);

    let store = ReelStore::open(io.clone(), &config).expect("open");
    assert_eq!(store.damaged_segments(), 0);
    for number in 0..written {
        assert_eq!(read(&store, format!("key-{number:04}").as_bytes()), Some(value.clone()), "key {number}");
    }
    store.close().expect("close");
}

// read each open segment's number and rows
fn open_rows(store: &Store) -> Vec<(u64, u64)> {
    hold(&store.state.volumes[0].claims).open.iter().map(|open| (open.number.0, open.row_count.0)).collect()
}

// a clean close leaves the open segment as it stood, and the reopen fills it on from where it stopped with nothing copied
#[test]
fn reopen_keeps_the_open_segment() {
    let io = SimulatedIo::new(Seed(4_101));
    let store = open(&io);
    store.put(COLUMN, b"first", b"value").expect("put");
    assert_eq!(open_rows(&store).iter().map(|(_number, rows)| rows).sum::<u64>(), 1, "the first put counted rows it never wrote");
    // the claim the window missed took no bytes, so it left no gap
    assert_eq!(store.state.volumes[0].unlanded_bytes.load(Ordering::Acquire), 0, "a window miss left a gap");
    for number in 0..QUIET_ROWS {
        store.put(COLUMN, format!("key-{number:04}").as_bytes(), b"value").expect("put");
    }
    let segments = store.state.volumes[0].table.segment_count();
    let before = open_rows(&store);
    assert!(before.iter().any(|(_number, rows)| *rows > 0), "no row went to an open segment");
    store.close().expect("close");
    drop(store);

    let store = open(&io);
    assert_eq!(store.state.volumes[0].table.segment_count(), segments, "the reopen sealed or copied a segment");
    assert_eq!(open_rows(&store), before, "the reopen moved the open segment");
    store.put(COLUMN, b"after", b"value").expect("put");
    let after = open_rows(&store);
    let rows = |open: &[(u64, u64)]| open.iter().map(|(_number, rows)| rows).sum::<u64>();
    assert_eq!(after.iter().map(|(number, _rows)| *number).collect::<Vec<_>>(), before.iter().map(|(number, _rows)| *number).collect::<Vec<_>>());
    assert_eq!(rows(&after), rows(&before) + 1, "the new row went somewhere else");
    for number in 0..QUIET_ROWS {
        assert_eq!(read(&store, format!("key-{number:04}").as_bytes()), Some(b"value".to_vec()));
    }
    assert_eq!(read(&store, b"after"), Some(b"value".to_vec()));
    store.close().expect("close");
}

// a volume set to one open segment takes every writer into it, and its store's least budget is lower
#[test]
fn one_open_segment_takes_every_writer() {
    let io = SimulatedIo::new(Seed(110));
    let mut one = config(SyncPolicy::Never);
    one.volumes[0].open_segments = Some(1);
    let store = ReelStore::open(io.clone(), &one).expect("open");
    store.put(COLUMN, b"key", b"value").expect("put");
    let volume = &store.state.volumes[0];
    let mut held = Vec::new();
    for _writer in 0..OPEN_PER_ROOT as u32 * WRITES_PER_OPEN {
        held.push(store.state.claim(volume, &ONE_ROW).expect("claim"));
        settle_jobs(&io);
    }
    let claims = hold(&volume.claims);
    let taking = claims.open.iter().filter(|segment| segment.phase == SegmentPhase::Taking).count();
    assert_eq!(taking, 1);
    drop(claims);
    drop(held);
    let (least, slots) = (store.memory_report().least_budget, store.memory_report().open_slots);
    store.close().expect("close");
    let full = ReelStore::open(SimulatedIo::new(Seed(110)), &config(SyncPolicy::Never)).expect("open");
    println!("one open segment: {slots} slots, least {least:?}; default: {} slots, least {:?}", full.memory_report().open_slots, full.memory_report().least_budget);
    assert!(slots < full.memory_report().open_slots && least < full.memory_report().least_budget);
    full.close().expect("close");
}

// puts, deletes and marked puts on a volume of one open segment, sealed, closed and killed between rounds, read back as a model
// holds them
#[test]
fn one_open_segment_matches_the_model() {
    one_open_segment_runs(None, None);
}

// the same on segments of 256 KiB, which fill and seal many times a seed
#[test]
fn small_segments_match_the_model() {
    one_open_segment_runs(Some(ByteCount(256 * 1024)), None);
}

// the same at 256 MiB, where the set size caps the write index, with keys of a few bytes and empty values so a segment's rows reach
// the cap: it seals by rows sooner and goes on taking writes
#[test]
fn a_capped_index_matches_the_model() {
    one_open_segment_runs(Some(ByteCount(256 * 1024)), Some(ByteCount(256 * 1024 * 1024)));
}

// the same capped index with the volume's default open segments, so several take writes at once under the cap
#[test]
fn a_capped_index_with_open_segments_matches_the_model() {
    one_open_segment_runs_with(Some(ByteCount(256 * 1024)), Some(ByteCount(256 * 1024 * 1024)), None);
}

// run the one-open-segment model over the seeds, on segments of this size or the budget's
fn one_open_segment_runs(segment_bytes: Option<ByteCount>, budget: Option<ByteCount>) {
    one_open_segment_runs_with(segment_bytes, budget, Some(1));
}

// run the model over the seeds with this many open segments, or the volume's default
fn one_open_segment_runs_with(segment_bytes: Option<ByteCount>, budget: Option<ByteCount>, open_segments: Option<usize>) {
    for seed in seeds(0..8) {
        let io = SimulatedIo::new(Seed(300 + seed));
        let mut random = SeededRandom::new(Seed(400 + seed));
        let mut one = config(SyncPolicy::Never);
        one.volumes[0].open_segments = open_segments;
        one.volumes[0].segment_bytes = segment_bytes;
        if let Some(budget) = budget {
            one.memory_budget = budget;
        }
        let mut store = ReelStore::open(io.clone(), &one).expect("open");
        let mut model = BTreeMap::new();
        for round in 0..24 {
            for write in 0..200 {
                let key = format!("key-{}", random.below(400)).into_bytes();
                match random.below(8) {
                    0 => {
                        store.delete(COLUMN, &key).expect("delete");
                        model.remove(&key);
                    },
                    1 => {
                        let value = format!("{seed}-{round}-{write}-marked").into_bytes();
                        let mark = reel_core::units::DeathMark(1 + random.below(6) * 1_000_000);
                        store.put_marked(COLUMN, &key, &value, mark).expect("put");
                        model.insert(key, value);
                    },
                    _ => {
                        let value = format!("{seed}-{round}-{write}").into_bytes();
                        store.put(COLUMN, &key, &value).expect("put");
                        model.insert(key, value);
                    },
                }
            }
            match random.below(4) {
                0 => {
                    store.seal_now();
                    settle(&io);
                },
                1 => {
                    store.close().expect("close");
                    drop(store);
                    store = ReelStore::open(io.clone(), &one).expect("open");
                },
                2 => {
                    store.flush().expect("flush");
                    io.kill();
                    drop(store);
                    store = ReelStore::open(io.clone(), &one).expect("open");
                },
                _ => settle(&io),
            }
            for number in 0..400 {
                let key = format!("key-{number}").into_bytes();
                assert_eq!(read(&store, &key), model.get(&key).cloned(), "seed {seed} round {round} key {number}");
            }
        }
        store.close().expect("close");
    }
}

// a column that declares a large least value doesn't shrink the capped write index: its rows can be smaller than it, and a
// delete has no value, so batches of empty values and of deletes land as on a store that declares none
#[test]
fn a_declared_least_value_leaves_the_cap() {
    let budget = ByteCount(256 * 1024 * 1024);
    let shaped = |least_value: Option<ByteCount>| {
        let mut config = config(SyncPolicy::Never);
        config.memory_budget = budget;
        config.volumes[0].segment_bytes = Some(ByteCount(256 * 1024));
        config.columns[0].least_value = least_value;
        config
    };
    let plain_rows = {
        let io = SimulatedIo::new(Seed(120));
        let store = ReelStore::open(io, &shaped(None)).expect("open");
        let rows = store.state.sizing.segment_rows;
        store.close().expect("close");
        rows
    };
    let declared = shaped(Some(ByteCount(64 * 1024)));
    let io = SimulatedIo::new(Seed(121));
    let store = ReelStore::open(io.clone(), &declared).expect("open");
    let mut puts = WriteBatch::new();
    for number in 0..1_000_u32 {
        puts.put(COLUMN, format!("empty-{number:04}").as_bytes(), b"");
    }
    store.write_batch(&puts).expect("a batch of empty values");
    let mut deletes = WriteBatch::new();
    for number in 0..500_u32 {
        deletes.delete(COLUMN, format!("empty-{:04}", number * 2).as_bytes());
    }
    store.write_batch(&deletes).expect("a batch of deletes");
    assert_eq!(store.state.sizing.segment_rows, plain_rows, "a declared least value moved the cap");
    store.close().expect("close");
    drop(store);
    let store = ReelStore::open(io, &declared).expect("reopen");
    // the even keys were deleted and the odd ones keep their empty values
    for number in 0..1_000_u32 {
        let wanted = (number % 2 == 1).then(Vec::new);
        assert_eq!(read(&store, format!("empty-{number:04}").as_bytes()), wanted, "key {number}");
    }
    store.close().expect("close");
}

// a store of 256 KiB segments takes 8 KiB record windows, so a finished segment of a few KB of rows holds one small window of data
#[test]
fn a_small_segment_holds_a_small_window() {
    let io = SimulatedIo::new(Seed(150));
    let mut small = config(SyncPolicy::Never);
    small.volumes[0].segment_bytes = Some(ByteCount(256 * 1024));
    let store = ReelStore::open(io.clone(), &small).expect("open");
    for number in 0..64_u32 {
        store.put(COLUMN, format!("key-{number:03}").as_bytes(), &[3_u8; 64]).expect("put");
    }
    std::thread::scope(|scope| {
        let finishing = scope.spawn(|| store.finish());
        while !finishing.is_finished() {
            if io.step_any().is_none() {
                io.advance_clock(Nanoseconds(1_000_000));
            }
        }
        finishing.join().expect("thread").expect("finish");
    });
    let store = ReelStore::open(io.clone(), &small).expect("reopen");
    for row in hold(&store.state.volumes[0].summary).rows() {
        let facts = &row.facts;
        assert_eq!(facts.data_end, reel_core::units::FileOffset(8 * 1024), "segment {:?}: {:?}", facts.segment, facts.file_length);
    }
    store.close().expect("close");
}

// a store of a 256 KiB volume and a 64 MiB one: each volume's writes take its own window, so the small one's data region steps
// 8 KiB a window and the large one's 64 KiB
fn two_volumes() -> StoreConfig {
    let mut two = config(SyncPolicy::Never);
    two.memory_budget = ByteCount(128 * 1024 * 1024);
    two.volumes[0].segment_bytes = Some(ByteCount(256 * 1024));
    two.volumes.push(VolumeConfig::new("large", vec![PathBuf::from("/disk/large")], SyncPolicy::Never));
    two.volumes[1].segment_bytes = Some(ByteCount(64 * 1024 * 1024));
    two.columns.push(ColumnConfig { volume: VolumeId(1), ..ColumnConfig::new(ColumnId(2), "large", KeyShape::Variable { most: 64 }) });
    two
}

#[test]
fn each_volume_takes_its_own_window() {
    let io = SimulatedIo::new(Seed(151));
    let store = ReelStore::open(io.clone(), &two_volumes()).expect("open");
    for number in 0..64_u32 {
        store.put(COLUMN, format!("key-{number:03}").as_bytes(), &[3_u8; 64]).expect("put");
        store.put(ColumnId(2), format!("key-{number:03}").as_bytes(), &[3_u8; 64]).expect("put");
    }
    std::thread::scope(|scope| {
        let finishing = scope.spawn(|| store.finish());
        while !finishing.is_finished() {
            if io.step_any().is_none() {
                io.advance_clock(Nanoseconds(1_000_000));
            }
        }
        finishing.join().expect("thread").expect("finish");
    });
    let store = ReelStore::open(io.clone(), &two_volumes()).expect("reopen");
    for (volume, window) in [(0, 8 * 1024_u64), (1, 64 * 1024)] {
        let rows = hold(&store.state.volumes[volume].summary);
        let ends: Vec<u64> = rows.rows().map(|row| row.facts.data_end.0).collect();
        assert_eq!(ends, vec![window], "volume {volume}");
    }
    store.close().expect("close");
}

// puts, deletes and seals over both volumes, closed and killed between rounds, read back as a model holds them
#[test]
fn two_volume_windows_match_the_model() {
    for seed in seeds(0..8) {
        let io = SimulatedIo::new(Seed(500 + seed));
        let mut random = SeededRandom::new(Seed(600 + seed));
        let mut store = ReelStore::open(io.clone(), &two_volumes()).expect("open");
        let mut model = BTreeMap::new();
        for round in 0..16 {
            for write in 0..150 {
                let column = if random.below(2) == 0 { COLUMN } else { ColumnId(2) };
                let key = (column.0, format!("key-{}", random.below(300)).into_bytes());
                if random.below(6) == 0 {
                    store.delete(column, &key.1).expect("delete");
                    model.remove(&key);
                } else {
                    let value = vec![(round + write) as u8; 1 + random.below(5_000) as usize];
                    store.put(column, &key.1, &value).expect("put");
                    model.insert(key, value);
                }
            }
            match random.below(4) {
                0 => {
                    store.seal_now();
                    settle(&io);
                },
                1 => {
                    store.close().expect("close");
                    drop(store);
                    store = ReelStore::open(io.clone(), &two_volumes()).expect("open");
                },
                2 => {
                    store.flush().expect("flush");
                    io.kill();
                    drop(store);
                    store = ReelStore::open(io.clone(), &two_volumes()).expect("open");
                },
                _ => settle(&io),
            }
            for column in [COLUMN, ColumnId(2)] {
                for number in 0..300 {
                    let key = (column.0, format!("key-{number}").into_bytes());
                    let mut value = Vec::new();
                    let found = store.get(column, &key.1, &mut value).expect("get").then_some(value);
                    assert_eq!(found.as_ref(), model.get(&key), "seed {seed} round {round} column {column:?} key {number}");
                }
            }
        }
        store.close().expect("close");
    }
}
