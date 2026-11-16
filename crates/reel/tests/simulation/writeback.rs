//! An open segment's bytes start for the device during ingest, so a seal's sync finds little left to send

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::thread;

use reel::config::{ColumnConfig, ColumnIndex, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::engine::reserve_job::WRITEBACK_STEP_BYTES;
use reel::engine::row_job::WRITE_OUT_PERIOD;
use reel::guard::hold;
use reel::record::codec::Codec;
use reel_core::column_id::ColumnId;
use reel_core::units::{ByteCount, Nanoseconds, VolumeId};
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_io::SimulatedIo;
use reel_sim::wait::wait_until;

const ROOT: &str = "/disk/writeback";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(256 * 1024 * 1024);
const VALUE_BYTES: u64 = 4_000;
const STEPS: u64 = 200_000;
const TICK: Nanoseconds = Nanoseconds(1_000_000);

type Store = ReelStore<SimulatedIo>;

fn open(io: &SimulatedIo) -> Store {
    open_with(io, SyncPolicy::Never)
}

fn open_with(io: &SimulatedIo, sync: SyncPolicy) -> Store {
    let config = StoreConfig {
        memory_budget: BUDGET,
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "writeback".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync,
            device: Default::default(), open_segments: None, segment_bytes: None, lifetime_groups: None,
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Variable { most: 64 },
            codec: Codec::None,
            volume: VolumeId(0),
            index: ColumnIndex::Plain,
            least_rows: None,
            least_value: None,
        }],
    };
    ReelStore::open(io.clone(), &config).expect("open")
}

fn fill(store: &Store, bytes: u64) {
    let value = vec![7; VALUE_BYTES as usize];
    for number in 0..bytes / VALUE_BYTES {
        store.put(COLUMN, format!("key-{number:06}").as_bytes(), &value).expect("put");
    }
}

// run the jobs for two write-out periods, far inside the reserve job's own period
fn two_periods(io: &SimulatedIo) {
    let mut left = 2 * WRITE_OUT_PERIOD.0 / TICK.0;
    for _step in 0..STEPS {
        if io.step_any().is_none() {
            if left == 0 {
                return;
            }
            left -= 1;
            io.advance_clock(TICK);
        }
    }
}

// small records in one segment start for the device within a write-out or two, with no rollover to wake the job
#[test]
fn starts_during_ingest() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io);
    two_periods(&io);
    fill(&store, 3 * WRITEBACK_STEP_BYTES);
    two_periods(&io);
    assert!(io.writeback_bytes().0 >= 2 * WRITEBACK_STEP_BYTES, "{:?}", io.writeback_bytes());
    store.close().expect("close");
}

// a few bytes wait for the seal's sync, so a quiet store makes no calls
#[test]
fn small_writes_wait() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io);
    fill(&store, WRITEBACK_STEP_BYTES / 2);
    two_periods(&io);
    assert_eq!(io.writeback_bytes().0, 0);
    store.close().expect("close");
}

// a range starts once: a look with nothing new sends nothing more
#[test]
fn starts_a_range_once() {
    let io = SimulatedIo::new(Seed(3));
    let store = open(&io);
    fill(&store, 2 * WRITEBACK_STEP_BYTES);
    two_periods(&io);
    let started = io.writeback_bytes();
    assert!(started.0 > 0);
    store.put(COLUMN, b"small", b"value").expect("put");
    two_periods(&io);
    assert_eq!(io.writeback_bytes(), started);
    store.close().expect("close");
}


// a writer that runs past its segment's room takes more with the claims lock let go, so other writers go on
#[test]
fn room_step_frees_the_claims() {
    let io = SimulatedIo::new(Seed(6));
    let store = open(&io);
    fill(&store, VALUE_BYTES);
    let volume = &store.state.volumes[0];
    {
        let mut claims = hold(&volume.claims);
        let segment = claims.open.first_mut().expect("an open segment");
        segment.reserved_end = segment.write_end;
    }
    io.hold_reserves(true);
    thread::scope(|scope| {
        let writer = scope.spawn(|| store.put(COLUMN, b"past-the-room", &vec![3; 4 * VALUE_BYTES as usize]));
        wait_until(|| io.held_reserves() > 0, "the writer never reached its room step");
        assert!(volume.claims.try_lock().is_ok(), "the room step held the claims");
        io.hold_reserves(false);
        writer.join().expect("join").expect("put");
    });
    let mut value = Vec::new();
    assert!(store.get(COLUMN, b"past-the-room", &mut value).expect("get"));
    store.close().expect("close");
}

// a writer that crosses the cadence limit hands the sync to the row job, and one at twice the limit syncs itself
#[test]
fn cadence_syncs_at_the_limit() {
    let io = SimulatedIo::new(Seed(5));
    let store = open_with(&io, SyncPolicy::EveryBytes(ByteCount(10 * VALUE_BYTES)));
    let volume = &store.state.volumes[0];
    let synced = || hold(&volume.sync).completed;
    let start = synced();
    fill(&store, 9 * VALUE_BYTES);
    assert_eq!(synced(), start);
    fill(&store, 2 * VALUE_BYTES);
    assert_eq!(synced(), start, "the writer synced at the limit");
    assert!(volume.is_sync_due.load(Ordering::Acquire));
    two_periods(&io);
    assert_eq!(synced(), start + 1, "the row job never ran the sync");

    // with the row job not stepping, the writer that reaches twice the limit syncs itself
    fill(&store, 21 * VALUE_BYTES);
    assert!(synced() >= start + 2);
    store.close().expect("close");
}

const SYNC_CRASH_POINTS: u64 = 80;
const SYNC_KEYS: u64 = 200;

// a sync sends its rows with the turn held, gives the turn back for the fdatasyncs and writes its mark after: a crash at any
// step keeps every row a flush that returned covered, and every row an earlier flush covered
#[test]
fn sync_crash_points() {
    for crash_at in 0..SYNC_CRASH_POINTS {
        let io = SimulatedIo::new(Seed(crash_at));
        let store = open(&io);
        for number in 0..SYNC_KEYS {
            store.put(COLUMN, format!("first-{number:04}").as_bytes(), b"one").expect("put");
        }
        store.flush().expect("flush");
        for number in 0..SYNC_KEYS {
            store.put(COLUMN, format!("second-{number:04}").as_bytes(), b"two").expect("put");
        }
        io.arm_crash(reel_sim::simulated_disk::OperationCount(crash_at));
        let is_flushed = store.flush().is_ok() && !io.is_crashed();
        io.power_cut();
        drop(store);

        let store = open(&io);
        let mut buf = Vec::new();
        for number in 0..SYNC_KEYS {
            assert!(store.get(COLUMN, format!("first-{number:04}").as_bytes(), &mut buf).expect("get"), "crash at {crash_at}: a row the first flush covered went");
            if is_flushed {
                assert!(store.get(COLUMN, format!("second-{number:04}").as_bytes(), &mut buf).expect("get"), "crash at {crash_at}: a flushed row went");
            }
        }
        store.close().expect("close");
    }
}
