//! A landed mark a volume: a write waits for its own volume alone, and a cue still holds one moment across them

use std::path::PathBuf;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::engine::write_path::ClaimRequest;
use reel::record::codec::Codec;
use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, Nanoseconds, RowCount, VolumeId};
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_io::SimulatedIo;

const BULK_ROOT: &str = "/disk/bulk";
const META_ROOT: &str = "/disk/meta";
const BULK: ColumnId = ColumnId(1);
const META: ColumnId = ColumnId(2);
const BUDGET: ByteCount = ByteCount(128 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 200_000;
const ONE_ROW: ClaimRequest = ClaimRequest {
    bytes: ByteCount(0),
    rows: RowCount(1),
    is_alone: false,
    has_records: false,
    covers: 0,
    mark: None,
    has_deletes: false,
};

type Store2 = ReelStore<SimulatedIo>;

fn volume(name: &str, root: &str) -> VolumeConfig {
    VolumeConfig {
        name: name.to_string(),
        roots: vec![PathBuf::from(root)],
        sync: SyncPolicy::Never,
        device: Default::default(),
    }
}

fn column(id: ColumnId, name: &str, volume: u8) -> ColumnConfig {
    ColumnConfig {
        id,
        name: name.to_string(),
        key_shape: KeyShape::Variable { most: 64 },
        codec: Codec::None,
        volume: VolumeId(volume),
        complete_rows: None,
    }
}

fn open(io: &SimulatedIo) -> Store2 {
    let config = StoreConfig {
        memory_budget: BUDGET,
        threads: reel::config::NO_WORKERS,
        volumes: vec![volume("bulk", BULK_ROOT), volume("meta", META_ROOT)],
        columns: vec![column(BULK, "bulk", 0), column(META, "meta", 1)],
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

fn read(store: &Store2, column: ColumnId, key: &[u8]) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    store.get(column, key, &mut buf).expect("get").then_some(buf)
}

// a put on one volume returns and shows while a write on another is still in flight
#[test]
fn put_passes_a_write_in_flight() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io);
    let bulk = &store.state.volumes[0];
    let claim = store.state.claim(bulk, &ONE_ROW).expect("claim");

    store.put(META, b"key", b"value").expect("put");
    assert_eq!(read(&store, META, b"key"), Some(b"value".to_vec()));

    drop(claim);
    store.close().expect("close");
}

// a write held up on a volume keeps its later writes from its own reads, and a cue keeps the same cut
#[test]
fn cue_holds_each_volume_at_its_mark() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io);
    let bulk = &store.state.volumes[0];
    store.put(BULK, b"before", b"value").expect("put");
    let bulk_mark = bulk.order.mark();
    let claim = store.state.claim(bulk, &ONE_ROW).expect("claim");
    store.put(META, b"key", b"value").expect("put");

    let cue = store.cue().expect("cue");
    assert_eq!(cue.point_of(BULK).expect("point"), bulk_mark);
    assert!(cue.point_of(META).expect("point") > claim.first_place());
    let mut buf = Vec::new();
    assert!(cue.get(META, b"key", &mut buf).expect("get"));
    assert!(cue.get(BULK, b"before", &mut buf).expect("get"));

    drop(claim);
    store.put(META, b"later", b"value").expect("put");
    assert!(!cue.get(META, b"later", &mut buf).expect("get"));
    drop(cue);
    store.close().expect("close");
}

// a write that returned before another began is in every cue that holds the later one
#[test]
fn cut_keeps_write_order() {
    let io = SimulatedIo::new(Seed(3));
    let store = open(&io);
    let mut buf = Vec::new();
    for number in 0..200_u64 {
        let value = number.to_le_bytes();
        store.put(META, b"first", &value).expect("put");
        let between = store.cue().expect("cue");
        store.put(BULK, b"second", &value).expect("put");
        let after = store.cue().expect("cue");

        assert!(between.get(META, b"first", &mut buf).expect("get"));
        assert_eq!(buf, value);
        if number > 0 {
            assert!(between.get(BULK, b"second", &mut buf).expect("get"));
            assert_eq!(buf, (number - 1).to_le_bytes());
        }
        assert!(after.get(BULK, b"second", &mut buf).expect("get"));
        assert_eq!(buf, value);
        assert!(after.get(META, b"first", &mut buf).expect("get"));
        assert_eq!(buf, value);
    }
    store.close().expect("close");
}

// a cue over a volume nothing has written reads nothing there and everything elsewhere
#[test]
fn cue_on_a_volume_never_written() {
    let io = SimulatedIo::new(Seed(4));
    let store = open(&io);
    store.put(META, b"key", b"value").expect("put");
    let cue = store.cue().expect("cue");
    let mut buf = Vec::new();
    assert!(cue.get(META, b"key", &mut buf).expect("get"));
    assert!(!cue.get(BULK, b"key", &mut buf).expect("get"));
    drop(cue);
    store.close().expect("close");
}

// a crash with rows on one volume restarts the count and every mark above them, the volume with nothing included
#[test]
fn open_lifts_every_mark() {
    let io = SimulatedIo::new(Seed(5));
    let store = open(&io);
    for number in 0..50_u64 {
        store.put(META, &number.to_le_bytes(), b"value").expect("put");
    }
    store.flush().expect("flush");
    settle(&io);
    let highest = store.state.read_point(&store.state.volumes[1]);
    io.power_cut();
    drop(store);

    let store = open(&io);
    assert!(store.state.places.last_given() >= highest);
    for volume in &store.state.volumes {
        assert!(volume.order.mark() >= highest);
    }
    let cue = store.cue().expect("cue");
    let mut buf = Vec::new();
    assert!(cue.get(META, &0_u64.to_le_bytes(), &mut buf).expect("get"));
    drop(cue);
    store.put(BULK, b"key", b"value").expect("put");
    assert!(store.state.read_point(&store.state.volumes[0]) > highest);
    store.close().expect("close");
}

// a seal on one volume starts and lands while a write on another stands in flight
#[test]
fn seal_passes_a_write_in_flight() {
    let io = SimulatedIo::new(Seed(6));
    let store = open(&io);
    store.put(META, b"key", b"value").expect("put");
    let claim = store.state.claim(&store.state.volumes[0], &ONE_ROW).expect("claim");
    store.state.seal_fullest(&store.state.volumes[1]);
    settle(&io);
    let seals = store.state.volumes[1].seals_done.load(std::sync::atomic::Ordering::Acquire);
    assert_eq!(seals, 1);
    assert_eq!(read(&store, META, b"key"), Some(b"value".to_vec()));
    drop(claim);
    store.close().expect("close");
}

// a place stands once across the volumes, so nothing two volumes write shares one
#[test]
fn places_stay_store_wide() {
    let io = SimulatedIo::new(Seed(7));
    let store = open(&io);
    store.put(BULK, b"key", b"value").expect("put");
    let bulk = store.state.volumes[0].order.mark();
    store.put(META, b"key", b"value").expect("put");
    let meta = store.state.volumes[1].order.mark();
    assert!(meta > bulk);
    store.close().expect("close");
}
