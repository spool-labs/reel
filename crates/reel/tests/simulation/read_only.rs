//! A read-only open beside a writer: no lock, no write, a whole view of what it found, and a fresh one when asked

use std::path::PathBuf;
use std::sync::atomic::Ordering;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::io::Io;
use reel::record::codec::Codec;
use reel_api::column_id::ColumnId;
use reel_api::direction::Direction;
use reel_api::scan::ScanStep;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, Nanoseconds, RowCount, VolumeId};
use reel_api::write_batch::WriteBatch;
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_disk::OperationCount;
use reel_sim::simulated_io::SimulatedIo;

const ROOT: &str = "/disk/read-only";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(64 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 400_000;
const KEYS: u64 = 300;
const BATCH_KEYS: u64 = 50;

type Store = ReelStore<SimulatedIo>;

fn config() -> StoreConfig {
    StoreConfig {
        memory_budget: BUDGET,
        volumes: vec![VolumeConfig {
            name: "read-only".to_string(),
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
    }
}

fn open(io: &SimulatedIo) -> Store {
    ReelStore::open(io.clone(), &config()).expect("open")
}

fn view(io: &SimulatedIo) -> Store {
    ReelStore::open_read_only(io.clone(), &config()).expect("open read-only")
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

// the rows reach the file on the job's clock, and a view reads the file
fn land_rows(io: &SimulatedIo) {
    io.advance_clock(Nanoseconds(TICK.0 * 200));
    settle(io);
}

fn seal(store: &Store, io: &SimulatedIo) {
    store.seal_now();
    settle(io);
}

fn key(number: u64) -> Vec<u8> {
    format!("key-{number:04}").into_bytes()
}

fn read(store: &Store, name: &[u8]) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    store.get(COLUMN, name, &mut buf).expect("get").then_some(buf)
}

fn keys(store: &Store) -> Vec<Vec<u8>> {
    let mut scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
    let (mut name, mut found) = (Vec::new(), Vec::new());
    while scan.next_key(&mut name).expect("step") == ScanStep::Found {
        found.push(name.clone());
    }
    found
}

fn put_keys(store: &Store, value: &[u8]) {
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), value).expect("put");
    }
}

// a view sees sealed rows and the rows a writer has out so far, and later writes wait for a refresh
#[test]
fn sees_then_refreshes() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io);
    put_keys(&store, b"sealed");
    seal(&store, &io);
    store.put(COLUMN, &key(1), b"open").expect("put");
    store.delete(COLUMN, &key(2)).expect("delete");
    land_rows(&io);

    let mut viewer = view(&io);
    assert_eq!(read(&viewer, &key(0)), Some(b"sealed".to_vec()));
    assert_eq!(read(&viewer, &key(1)), Some(b"open".to_vec()));
    assert_eq!(read(&viewer, &key(2)), None);
    assert_eq!(keys(&viewer).len(), KEYS as usize - 1);

    store.put(COLUMN, &key(1), b"newer").expect("put");
    store.delete(COLUMN, &key(3)).expect("delete");
    seal(&store, &io);
    land_rows(&io);
    assert_eq!(read(&viewer, &key(1)), Some(b"open".to_vec()));
    viewer.refresh().expect("refresh");
    assert_eq!(read(&viewer, &key(1)), Some(b"newer".to_vec()));
    assert_eq!(read(&viewer, &key(3)), None);
    assert_eq!(viewer.count_prefix(COLUMN, b"key-").expect("count"), RowCount(KEYS - 2));
    viewer.close().expect("close");
    store.close().expect("close");
}

// a view refuses writes, holds no lock against the writer, and leaves the disk as it found it
#[test]
fn changes_nothing() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io);
    put_keys(&store, b"value");
    land_rows(&io);
    let before = io.list_directory(ROOT.as_ref()).expect("list");
    let writes = io.write_count();

    let viewer = view(&io);
    assert!(matches!(viewer.put(COLUMN, b"a", b"b"), Err(StoreError::ReadOnly)));
    assert!(matches!(viewer.delete(COLUMN, b"a"), Err(StoreError::ReadOnly)));
    assert!(matches!(viewer.delete_range(COLUMN, b"a", b"b"), Err(StoreError::ReadOnly)));
    assert!(matches!(viewer.write_batch(&WriteBatch::new()), Err(StoreError::ReadOnly)));
    assert!(matches!(viewer.flush(), Err(StoreError::ReadOnly)));
    assert_eq!(keys(&viewer).len(), KEYS as usize);
    viewer.close().expect("close");

    assert_eq!(io.write_count(), writes);
    assert_eq!(io.list_directory(ROOT.as_ref()).expect("list"), before);
    store.put(COLUMN, b"after", b"value").expect("put");
    store.close().expect("close");
}

// a store that has never been written has nothing to view
#[test]
fn nothing_to_view() {
    let io = SimulatedIo::new(Seed(3));
    assert!(ReelStore::open_read_only(io.clone(), &config()).is_err());
}

// a segment the writer unlinks stays readable through the file the view holds
#[test]
fn unlink_under_a_view() {
    let io = SimulatedIo::new(Seed(4));
    let store = open(&io);
    put_keys(&store, b"older");
    seal(&store, &io);
    let viewer = view(&io);

    put_keys(&store, b"newer");
    seal(&store, &io);
    settle(&io);
    assert!(store.state.volumes[0].segments_unlinked.load(Ordering::Acquire) > 0);
    assert_eq!(read(&viewer, &key(5)), Some(b"older".to_vec()));
    assert_eq!(keys(&viewer).len(), KEYS as usize);
    viewer.close().expect("close");
    store.close().expect("close");
}

// a batch in the open rows comes whole, and one in a torn last group not at all
#[test]
fn batches_whole() {
    let io = SimulatedIo::new(Seed(5));
    let store = open(&io);
    let mut batch = WriteBatch::new();
    for number in 0..BATCH_KEYS {
        batch.put(COLUMN, &key(number), b"batch");
    }
    store.write_batch(&batch).expect("batch");
    land_rows(&io);
    let viewer = view(&io);
    assert_eq!(keys(&viewer).len(), BATCH_KEYS as usize);
    viewer.close().expect("close");

    // rows that haven't reached the file yet are simply absent
    let mut later = WriteBatch::new();
    for number in BATCH_KEYS..BATCH_KEYS * 2 {
        later.put(COLUMN, &key(number), b"batch");
    }
    store.write_batch(&later).expect("batch");
    let viewer = view(&io);
    let seen = keys(&viewer).len();
    assert!(seen == BATCH_KEYS as usize || seen == 2 * BATCH_KEYS as usize, "{seen} keys");
    viewer.close().expect("close");
    store.close().expect("close");
}

// a merge's output and its inputs both on disk read as one set of versions
#[test]
fn output_beside_its_inputs() {
    let io = SimulatedIo::new(Seed(6));
    let store = open(&io);
    put_keys(&store, b"first");
    seal(&store, &io);
    put_keys(&store, b"second");
    store.put(COLUMN, b"zz", b"value").expect("put");
    // a remove that fails keeps the replaced files on disk beside what replaced them
    io.fail_removes(OperationCount(u64::MAX));
    seal(&store, &io);
    settle(&io);

    let viewer = view(&io);
    let found = keys(&viewer);
    assert_eq!(found.len(), KEYS as usize + 1);
    assert_eq!(read(&viewer, &key(7)), Some(b"second".to_vec()));
    assert_eq!(viewer.count_prefix(COLUMN, b"key-").expect("count"), RowCount(KEYS));
    viewer.close().expect("close");
    io.fail_removes(OperationCount(0));
    store.close().expect("close");
}

// a range delete and a damaged span reach a view as they reach the writer
#[test]
fn covers_reach_a_view() {
    let io = SimulatedIo::new(Seed(7));
    let store = open(&io);
    put_keys(&store, b"value");
    seal(&store, &io);
    store.delete_range(COLUMN, &key(10), &key(20)).expect("delete");
    land_rows(&io);
    let viewer = view(&io);
    assert_eq!(read(&viewer, &key(15)), None);
    assert_eq!(keys(&viewer).len(), KEYS as usize - 10);
    viewer.close().expect("close");

    seal(&store, &io);
    let viewer = view(&io);
    assert_eq!(read(&viewer, &key(15)), None);
    viewer.close().expect("close");
    store.close().expect("close");
}
