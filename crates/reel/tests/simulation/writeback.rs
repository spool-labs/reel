//! An open segment's bytes start for the device during ingest, so a seal's sync finds little left to send

use std::path::PathBuf;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::engine::reserve_job::WRITEBACK_STEP_BYTES;
use reel::engine::row_job::WRITE_OUT_PERIOD;
use reel::record::codec::Codec;
use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, Nanoseconds, VolumeId};
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_io::SimulatedIo;

const ROOT: &str = "/disk/writeback";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(256 * 1024 * 1024);
const VALUE_BYTES: u64 = 4_000;
const STEPS: u64 = 200_000;
const TICK: Nanoseconds = Nanoseconds(1_000_000);

type Store = ReelStore<SimulatedIo>;

fn open(io: &SimulatedIo) -> Store {
    let config = StoreConfig {
        memory_budget: BUDGET,
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "writeback".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(),
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Variable { most: 64 },
            codec: Codec::None,
            volume: VolumeId(0),
            complete_rows: None,
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
