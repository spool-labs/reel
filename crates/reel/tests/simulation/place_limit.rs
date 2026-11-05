//! The write order's limit: every new write lands above every place ever handed out, whatever was lost

use std::path::{Path, PathBuf};
use std::sync::Arc;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::engine_state::PLACE_RUN;
use reel::engine::reel_store::ReelStore;
use reel::engine::reserve_job::RESERVE_PERIOD;
use reel::io::Io;
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_disk::OperationCount;
use reel_sim::simulated_io::SimulatedIo;
use reel::record::codec::Codec;
use reel::superblock::{SUPERBLOCK_NAME, Superblock, SuperblockPaths};
use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, RowCount, VolumeId, WritePlace};

const FIRST_ROOT: &str = "/disk/limit-one";
const SECOND_ROOT: &str = "/disk/limit-two";
const ROOTS: [&str; 2] = [FIRST_ROOT, SECOND_ROOT];
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(8 * 1024 * 1024);
const KEY: &[u8] = b"key";
const VALUE: &[u8] = b"value";
const CRASH_POINTS: u64 = 16;

type Store = ReelStore<SimulatedIo>;

fn config() -> StoreConfig {
    StoreConfig {
        memory_budget: BUDGET,
        volumes: vec![VolumeConfig {
            name: "meta".to_string(),
            roots: ROOTS.iter().map(PathBuf::from).collect(),
            sync: SyncPolicy::Never,
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
    ReelStore::open(io.clone(), &config()).expect("open")
}

fn superblock_of(io: &SimulatedIo, root: &str) -> Superblock {
    Superblock::read(io, Path::new(root)).expect("read").expect("superblock")
}

fn limit_of(io: &SimulatedIo, root: &str) -> WritePlace {
    superblock_of(io, root).place_limit
}

fn holds(store: &Store, key: &[u8]) -> bool {
    let mut buf = Vec::new();
    store.get(COLUMN, key, &mut buf).expect("get")
}

// a fresh store starts at the bottom of its first run, and every root saved the run first
#[test]
fn first_run() {
    let io = SimulatedIo::new(Seed(1));

    let store = open(&io);

    assert_eq!(store.state.read_point(), WritePlace(0));
    for root in ROOTS {
        assert_eq!(limit_of(&io, root), WritePlace(PLACE_RUN));
    }
    store.close().expect("close");
}

// open starts at the limit the last open saved, and saves the next limit before any write
#[test]
fn open_start() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io);
    store.put(COLUMN, KEY, VALUE).expect("put");
    store.close().expect("close");
    drop(store);
    let saved = limit_of(&io, FIRST_ROOT);

    let store = open(&io);

    assert_eq!(store.state.read_point(), saved);
    assert!(holds(&store, KEY));
    for root in ROOTS {
        assert_eq!(limit_of(&io, root), WritePlace(saved.0 + PLACE_RUN));
    }
    store.close().expect("close");
}

// the highest limit wins, whatever the revisions say, so a stale root can't pull the counter back
#[test]
fn stale_root() {
    let io = SimulatedIo::new(Seed(3));
    let store = open(&io);
    store.close().expect("close");
    drop(store);
    let saved = limit_of(&io, FIRST_ROOT);

    let mut stale = superblock_of(&io, SECOND_ROOT);
    stale.place_limit = WritePlace(1);
    stale.revision += 5;
    stale.write(&io, &SuperblockPaths::new(Path::new(SECOND_ROOT)), &mut Vec::new()).expect("write");

    let store = open(&io);

    assert_eq!(store.state.read_point(), saved);
    store.close().expect("close");
}

// a root that lost its superblock leaves the other root's limit standing
#[test]
fn lost_root() {
    let io = SimulatedIo::new(Seed(4));
    let store = open(&io);
    store.close().expect("close");
    drop(store);
    let saved = limit_of(&io, SECOND_ROOT);
    io.remove_file(&Path::new(FIRST_ROOT).join(SUPERBLOCK_NAME)).expect("remove");

    let store = open(&io);

    assert_eq!(store.state.read_point(), saved);
    store.close().expect("close");
}

// a claim past the limit waits for every root to hold the next one
#[test]
fn run_raise() {
    let io = SimulatedIo::new(Seed(5));
    let store = open(&io);
    store.put(COLUMN, b"before", VALUE).expect("put");
    let saved = limit_of(&io, FIRST_ROOT);
    store.state.order.set_limit(store.state.read_point());

    store.put(COLUMN, KEY, VALUE).expect("put");

    assert!(holds(&store, KEY));
    for root in ROOTS {
        assert_eq!(limit_of(&io, root), WritePlace(saved.0 + PLACE_RUN));
    }
    store.close().expect("close");
}

// a raise that can't reach a root fails that root's volume and hands out no place, and the next open goes on
#[test]
fn raise_fails() {
    let io = SimulatedIo::new(Seed(6));
    let store = open(&io);
    store.put(COLUMN, b"before", VALUE).expect("put");
    let mark = store.state.read_point();
    store.state.order.set_limit(mark);

    io.fail_syncs(OperationCount(1));
    assert!(store.put(COLUMN, KEY, VALUE).is_err());

    assert_eq!(store.state.read_point(), mark);
    assert!(!holds(&store, KEY));
    assert!(store.close().is_err());
    drop(store);

    let store = open(&io);
    assert!(store.state.read_point() >= mark);
    store.put(COLUMN, KEY, VALUE).expect("put");
    assert!(holds(&store, KEY));
    store.close().expect("close");
}

// the job saves the next limit while half a run is left, so no writer waits on a root's sync
#[test]
fn job_raises() {
    let io = SimulatedIo::new(Seed(7));
    let store = open(&io);
    let saved = limit_of(&io, FIRST_ROOT);
    store.state.order.set_limit(WritePlace(store.state.read_point().0 + PLACE_RUN / 2 - 1));

    io.advance_clock(RESERVE_PERIOD);
    while io.step_any().is_some() {}

    assert_eq!(limit_of(&io, FIRST_ROOT), WritePlace(saved.0 + PLACE_RUN));
    store.close().expect("close");
}

// a crash at any step of a raise leaves a limit at or above every place handed out
#[test]
fn raise_crash() {
    for crash_at in 0..CRASH_POINTS {
        let io = SimulatedIo::new(Seed(100 + crash_at));
        let store = open(&io);
        store.put(COLUMN, b"before", VALUE).expect("put");
        let mark = store.state.read_point();
        store.state.order.set_limit(mark);

        io.arm_crash(OperationCount(crash_at));
        let written = store.put(COLUMN, KEY, VALUE);
        let handed_out = WritePlace(mark.0 + u64::from(written.is_ok()));
        io.power_cut();
        drop(store);

        let store = open(&io);
        assert!(store.state.read_point() >= handed_out, "crash at {crash_at}");
        store.put(COLUMN, b"after", VALUE).expect("put");
        assert!(holds(&store, b"after"));
        store.close().expect("close");
    }
}

// a limit no root took stays unpublished, so no place goes out above what a root saved
#[test]
fn no_root() {
    let io = SimulatedIo::new(Seed(8));
    let store = open(&io);
    store.put(COLUMN, b"before", VALUE).expect("put");
    let mark = store.state.read_point();
    store.state.order.set_limit(mark);

    io.fail_syncs(OperationCount(1));
    assert!(!store.state.reserve_ahead());

    assert_eq!(store.state.order.try_claim(RowCount(1)), None);
    assert!(store.close().is_err());
}

// a second save that starts during the first steps aside, so the limit moves once
#[test]
fn one_saver() {
    let io = SimulatedIo::new(Seed(9));
    let store = open(&io);
    let saved = limit_of(&io, FIRST_ROOT);
    let state = Arc::clone(&store.state);
    let during = Arc::clone(&store.state);
    io.after_next_sync(Box::new(move || {
        assert!(during.reserve_ahead());
    }));

    assert!(state.reserve_ahead());

    assert_eq!(limit_of(&io, FIRST_ROOT), WritePlace(saved.0 + PLACE_RUN));
    store.close().expect("close");
}

// open starts past the places it replays, whatever the superblock says
#[test]
fn replay_past() {
    let io = SimulatedIo::new(Seed(10));
    let store = open(&io);
    store.put(COLUMN, KEY, VALUE).expect("put");
    store.put(COLUMN, b"second", VALUE).expect("put");
    let highest = store.state.read_point();
    store.close().expect("close");
    drop(store);

    for root in ROOTS {
        let mut stale = superblock_of(&io, root);
        stale.place_limit = WritePlace(0);
        stale.write(&io, &SuperblockPaths::new(Path::new(root)), &mut Vec::new()).expect("write");
    }
    let store = open(&io);

    assert!(store.state.read_point() >= highest);
    store.put(COLUMN, KEY, b"newer").expect("put");
    let mut buf = Vec::new();
    store.get(COLUMN, KEY, &mut buf).expect("get");
    assert_eq!(buf, b"newer");
    store.close().expect("close");
}
