//! A large open segment seals once it sits with no write, one that holds a delete seals at any size, and a small one stays open

use std::path::PathBuf;
use std::sync::atomic::Ordering;

use reel::config::{ColumnConfig, ColumnIndex, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::engine::row_job::{DELETE_SEAL_AFTER, IDLE_SEAL_AFTER, IDLE_SEAL_BYTES};
use reel::guard::hold;
use reel::record::codec::Codec;
use reel_core::column_id::ColumnId;
use reel_core::units::{ByteCount, DeathMark, Nanoseconds, VolumeId};
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
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "idle".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(), open_segments: None, segment_bytes: None,
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

// a segment that holds a delete seals a few seconds after its last write, whatever its size
#[test]
fn delete_seals_small() {
    let io = SimulatedIo::new(Seed(4));
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");
    store.delete(COLUMN, b"key").expect("delete");
    settle(&io);
    assert_eq!(seals(&store), 0);
    io.advance_clock(IDLE_SEAL_AFTER);
    settle(&io);
    assert_eq!(seals(&store), 1);
    store.close().expect("close");
}

fn trickle(store: &Store, io: &SimulatedIo) {
    let second = Nanoseconds(1_000_000_000);
    for number in 0..DELETE_SEAL_AFTER.0 / second.0 + 2 {
        store.put(COLUMN, format!("trickle-{number:03}").as_bytes(), b"value").expect("put");
        settle(io);
        io.advance_clock(second);
    }
    settle(io);
}

// a trickle of writes can't hold a delete's segment open past the bound
#[test]
fn delete_ages_out() {
    let io = SimulatedIo::new(Seed(5));
    let store = open(&io);
    store.delete(COLUMN, b"key").expect("delete");
    settle(&io);
    trickle(&store, &io);
    assert_eq!(seals(&store), 1);
    store.close().expect("close");
}

// the same trickle with no delete leaves the segment open
#[test]
fn trickle_stays_open() {
    let io = SimulatedIo::new(Seed(6));
    let store = open(&io);
    trickle(&store, &io);
    assert_eq!(seals(&store), 0);
    store.close().expect("close");
}

// a quiet segment seals beside a busy one, which store-wide idle would never reach
#[test]
fn quiet_beside_busy() {
    let io = SimulatedIo::new(Seed(7));
    let store = open(&io);
    store.delete(COLUMN, b"key").expect("delete");
    settle(&io);
    let holding_deletes = |store: &Store| hold(&store.state.volumes[0].claims).open.iter().filter(|segment| segment.has_deletes).count();
    assert_eq!(holding_deletes(&store), 1);
    let second = Nanoseconds(1_000_000_000);
    for number in 0..IDLE_SEAL_AFTER.0 / second.0 + 2 {
        store.put_marked(COLUMN, format!("marked-{number:03}").as_bytes(), b"value", DeathMark(1_000)).expect("put");
        settle(&io);
        io.advance_clock(second);
    }
    settle(&io);
    assert_eq!(holding_deletes(&store), 0);
    store.close().expect("close");
}

fn open_bytes(store: &Store) -> u64 {
    hold(&store.state.volumes[0].claims).open.iter().map(|open| open.write_end.0).sum()
}

fn reads_back(store: &Store, bytes: u64) {
    let mut value = Vec::new();
    for number in 0..bytes.div_ceil(VALUE_BYTES as u64) {
        assert!(store.get(COLUMN, format!("key-{number:04}").as_bytes(), &mut value).expect("get"), "key {number}");
        assert_eq!(value.len(), VALUE_BYTES);
    }
}

// a small segment that came back at open seals once it sits idle, after a close or a kill, and its rows read from the footer
#[test]
fn replayed_small_seals() {
    for is_killed in [false, true] {
        let io = SimulatedIo::new(Seed(20));
        let store = open(&io);
        put_bytes(&store, IDLE_SEAL_BYTES.0 / 2);
        settle(&io);
        if is_killed {
            store.flush().expect("flush");
            io.kill();
        } else {
            store.close().expect("close");
        }
        drop(store);

        let store = open(&io);
        assert!(open_bytes(&store) > 0, "the segment came back open");
        settle(&io);
        io.advance_clock(IDLE_SEAL_AFTER);
        settle(&io);
        assert_eq!(seals(&store), 1);
        assert_eq!(open_bytes(&store), 0);
        reads_back(&store, IDLE_SEAL_BYTES.0 / 2);
        store.close().expect("close");
    }
}

// a segment that came back at open and took a write since stays open under the size
#[test]
fn replayed_then_written_stays() {
    let io = SimulatedIo::new(Seed(21));
    let store = open(&io);
    put_bytes(&store, IDLE_SEAL_BYTES.0 / 2);
    settle(&io);
    store.close().expect("close");
    drop(store);

    let store = open(&io);
    store.put(COLUMN, b"after-open", b"value").expect("put");
    settle(&io);
    io.advance_clock(Nanoseconds(IDLE_SEAL_AFTER.0 * 10));
    settle(&io);
    assert_eq!(seals(&store), 0);
    reads_back(&store, IDLE_SEAL_BYTES.0 / 2);
    store.close().expect("close");
}
