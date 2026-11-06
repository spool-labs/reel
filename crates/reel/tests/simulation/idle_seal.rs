//! A large open segment seals once it sits with no write, and a small one stays open for the writes to come

use std::path::PathBuf;
use std::sync::atomic::Ordering;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::engine::row_job::{IDLE_SEAL_AFTER, IDLE_SEAL_BYTES};
use reel::record::codec::Codec;
use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, Nanoseconds, VolumeId};
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_io::SimulatedIo;

const ROOT: &str = "/disk/idle";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(512 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(10_000_000);
const STEPS: u64 = 400_000;
const VALUE_BYTES: usize = 1024 * 1024;

type Store = ReelStore<SimulatedIo>;

fn open(io: &SimulatedIo) -> Store {
    let config = StoreConfig {
        memory_budget: BUDGET,
        threads: Default::default(),
        volumes: vec![VolumeConfig {
            name: "idle".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Variable { most: 64 },
            codec: Codec::None,
            volume: VolumeId(0),
            complete_rows: None,
        }],
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

fn put_bytes(store: &Store, bytes: u64) {
    let value = vec![7; VALUE_BYTES];
    for number in 0..bytes.div_ceil(VALUE_BYTES as u64) {
        store.put(COLUMN, format!("key-{number:04}").as_bytes(), &value).expect("put");
    }
}

fn seals(store: &Store) -> u64 {
    store.state.volumes[0].seals_done.load(Ordering::Acquire)
}

// a segment above the size seals a few seconds after its last write, with no ask
#[test]
fn large_seals() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io);
    put_bytes(&store, IDLE_SEAL_BYTES.0);
    settle(&io);
    assert_eq!(seals(&store), 0);
    io.advance_clock(IDLE_SEAL_AFTER);
    settle(&io);
    assert_eq!(seals(&store), 1);
    store.close().expect("close");
}

// a segment under the size stays open however long it sits
#[test]
fn small_stays() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io);
    put_bytes(&store, IDLE_SEAL_BYTES.0 / 2);
    settle(&io);
    io.advance_clock(Nanoseconds(IDLE_SEAL_AFTER.0 * 10));
    settle(&io);
    assert_eq!(seals(&store), 0);
    store.close().expect("close");
}

// writes that keep coming hold the seal off
#[test]
fn writes_hold_it_off() {
    let io = SimulatedIo::new(Seed(3));
    let store = open(&io);
    put_bytes(&store, IDLE_SEAL_BYTES.0);
    for _tick in 0..4 {
        settle(&io);
        io.advance_clock(Nanoseconds(IDLE_SEAL_AFTER.0 / 2));
        store.put(COLUMN, b"more", b"value").expect("put");
    }
    settle(&io);
    assert_eq!(seals(&store), 0);
    store.close().expect("close");
}
