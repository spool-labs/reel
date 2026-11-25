//! Cues: a read view holds one point in the write order, whatever comes after it

use std::path::PathBuf;

use reel::config::{ColumnConfig, ColumnIndex, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::cue::MOST_CUES;
use reel::engine::reel_store::ReelStore;
use reel::record::codec::Codec;
use reel_core::column_id::ColumnId;
use reel_core::reads::ValueWindow;
use reel_core::store_error::StoreError;
use reel_core::units::{ByteCount, Nanoseconds, VolumeId};
use reel_core::value_list::{ValueAnswer, ValueList};
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_io::SimulatedIo;

const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(64 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 20_000;

type Store = ReelStore<SimulatedIo>;

fn open(io: &SimulatedIo) -> Store {
    let config = StoreConfig {
        memory_budget: BUDGET,
        max_memory: None,
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "cues".to_string(),
            roots: vec![PathBuf::from("/disk/cues")],
            sync: SyncPolicy::Never,
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

fn read(store: &Store, key: &[u8]) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    store.get(COLUMN, key, &mut buf).expect("get").then_some(buf)
}

// a cue sees what was there when it was taken, through overwrites, deletes and seals, and a plain get sees the newest
#[test]
fn frozen_view() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io);
    store.put(COLUMN, b"kept", b"older").expect("put");
    store.put(COLUMN, b"gone", b"value").expect("put");
    seal(&store, &io);
    let cue = store.cue().expect("cue");

    store.put(COLUMN, b"kept", b"newer").expect("put");
    store.delete(COLUMN, b"gone").expect("delete");
    store.put(COLUMN, b"later", b"value").expect("put");
    seal(&store, &io);

    let mut buf = Vec::new();
    assert!(cue.get(COLUMN, b"kept", &mut buf).expect("get"));
    assert_eq!(buf, b"older");
    assert!(cue.contains(COLUMN, b"gone").expect("contains"));
    assert!(!cue.get(COLUMN, b"later", &mut buf).expect("get"));
    assert_eq!(read(&store, b"kept"), Some(b"newer".to_vec()));
    assert_eq!(read(&store, b"gone"), None);
    drop(cue);
    store.close().expect("close");
}

// a cue reads a window and a batch of keys at its own point
#[test]
fn all_reads() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io);
    store.put(COLUMN, b"key", b"0123456789").expect("put");
    let cue = store.cue().expect("cue");
    store.put(COLUMN, b"key", b"abcdefghij").expect("put");

    let mut buf = Vec::new();
    let window = ValueWindow {
        offset: ByteCount(2),
        length: ByteCount(3),
    };
    assert!(cue.get_range(COLUMN, b"key", window, &mut buf).expect("get_range"));
    assert_eq!(buf, b"234");

    let mut values = ValueList::new();
    let keys: [&[u8]; 2] = [b"key", b"missing"];
    cue.get_many(COLUMN, &keys, &mut values).expect("get_many");
    assert_eq!(values.answer(0), Some(ValueAnswer::Found(b"0123456789")));
    assert_eq!(values.answer(1), Some(ValueAnswer::Missing));
    drop(cue);
    store.close().expect("close");
}

// a cover inside a newer one stays while a cue reads below the newer one, or what it hid would come back for the cue
#[test]
fn keeps_cover() {
    let io = SimulatedIo::new(Seed(3));
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");
    seal(&store, &io);
    store.delete_range(COLUMN, b"kex", b"kez").expect("delete");
    let cue = store.cue().expect("cue");

    store.delete_range(COLUMN, b"a", b"z").expect("delete");

    let mut buf = Vec::new();
    assert!(!cue.get(COLUMN, b"key", &mut buf).expect("get"));
    assert_eq!(store.state.volumes[0].covers.len(), 2);
    drop(cue);
    store.delete_range(COLUMN, b"a", b"zz").expect("delete");
    assert_eq!(store.state.volumes[0].covers.len(), 1);
    store.close().expect("close");
}

// the table holds a fixed number of cues, a full one says so, and a cue that goes frees its slot
#[test]
fn full_table() {
    let io = SimulatedIo::new(Seed(4));
    let store = open(&io);
    let mut cues = Vec::new();
    for _cue in 0..MOST_CUES {
        cues.push(store.cue().expect("cue"));
    }

    assert!(matches!(store.cue(), Err(StoreError::CuesFull { .. })));
    cues.pop();
    assert!(store.cue().is_ok());
    drop(cues);
    assert!(store.state.cues.is_empty());
    store.close().expect("close");
}

// a cue outlives nothing: once the store has closed its reads say so, and its files are never touched
#[test]
fn closed_store() {
    let io = SimulatedIo::new(Seed(5));
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");
    let cue = store.cue().expect("cue");
    store.close().expect("close");

    let mut buf = Vec::new();
    assert!(matches!(cue.get(COLUMN, b"key", &mut buf), Err(StoreError::Closed)));
    assert!(matches!(cue.contains(COLUMN, b"key"), Err(StoreError::Closed)));
    let mut values = ValueList::new();
    assert!(matches!(cue.get_many(COLUMN, &[b"key"], &mut values), Err(StoreError::Closed)));
}

// a cover kept for a cue goes once the cue has
#[test]
fn kept_cover_goes() {
    let io = SimulatedIo::new(Seed(6));
    let store = open(&io);
    store.delete_range(COLUMN, b"kex", b"kez").expect("delete");
    let cue = store.cue().expect("cue");
    store.delete_range(COLUMN, b"a", b"z").expect("delete");
    assert_eq!(store.state.volumes[0].covers.len(), 2);

    drop(cue);
    settle(&io);

    assert_eq!(store.state.volumes[0].covers.len(), 1);
    store.close().expect("close");
}

// a cover with nothing inside it asks no read to start over
#[test]
fn plain_cover_counts_nothing() {
    let io = SimulatedIo::new(Seed(7));
    let store = open(&io);
    store.delete_range(COLUMN, b"a", b"b").expect("delete");
    store.delete_range(COLUMN, b"c", b"d").expect("delete");

    assert_eq!(store.state.cues.counts().reclaims, 0);
    store.close().expect("close");
}

// a reclaim that lands between reading the count and taking the slot costs the slot, and the next try takes a newer point
#[test]
fn pin_lost() {
    let io = SimulatedIo::new(Seed(8));
    let store = open(&io);
    let state = &store.state;
    let counts = state.cues.counts();
    let slot = state.cues.take(0, state.read_point(&state.volumes[0])).expect("a slot");

    state.cues.note_reclaim();

    assert!(!state.pin_holds(slot, counts));
    assert!(state.cues.is_empty());
    let slot = state.cues.take(0, state.read_point(&state.volumes[0])).expect("a slot");
    assert!(state.pin_holds(slot, state.cues.counts()));
    assert_eq!(state.cues.len(), 1);
    state.cues.release(slot);
    store.close().expect("close");
}

// a free that lands between reading the counts and taking the slot costs the slot too
#[test]
fn pin_lost_to_a_free() {
    let io = SimulatedIo::new(Seed(9));
    let store = open(&io);
    let state = &store.state;
    let counts = state.cues.counts();
    let slot = state.cues.take(0, state.read_point(&state.volumes[0])).expect("a slot");

    state.cues.note_free();

    assert!(!state.pin_holds(slot, counts));
    assert!(state.cues.is_empty());
    store.close().expect("close");
}

// a reclaim between a get_many's count and its pin costs the pin, and the slot goes back
#[test]
fn many_pin_lost() {
    let io = SimulatedIo::new(Seed(9));
    let store = open(&io);
    let state = &store.state;
    let counts = state.cues.counts();
    let point = state.read_point(&state.volumes[0]);

    state.cues.note_reclaim();

    assert_eq!(state.pin_many(&state.volumes[0], point, counts, 0), None);
    assert!(state.cues.is_empty());
    store.close().expect("close");
}
