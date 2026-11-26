//! A read-only open beside a writer: no lock, no write, a whole view of what it found, and a fresh one when asked

use std::path::PathBuf;
use std::sync::atomic::Ordering;

use reel::engine::engine_sizing::shard_for_rows;
use reel::guard::hold;
use reel::index::write_index::SHARD_COUNT;
use reel::config::{ColumnConfig, ColumnIndex, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::open_segment::data_path;
use reel::engine::reel_store::ReelStore;
use reel::io::{InPlace, Io, in_place};
use reel::record::codec::{CHUNK_BYTES, Codec};
use reel::record::record_head::SMALL_HEAD_BYTES;
use reel_core::column_id::ColumnId;
use reel_core::direction::Direction;
use reel_core::scan::ScanStep;
use reel_core::store_error::StoreError;
use reel_core::units::{ByteCount, FileOffset, Nanoseconds, RowCount, VolumeId};
use reel_core::write_batch::WriteBatch;
use reel_sim::file_image::BitMask;
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
        max_memory: None,
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "read-only".to_string(),
            roots: vec![PathBuf::from(ROOT)],
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
    // the refused calls touched no write buffer, and a view holds none, starts no job that would, and keeps no spare group
    let state = &viewer.state;
    assert!(state.encoders.grab().is_none() && state.batch_rows.grab().is_none() && state.batch_records.grab().is_none());
    assert!(state.row_task.get().is_none() && state.reserve_task.get().is_none() && state.planner_task.get().is_none());
    assert!(state.seal_tasks.iter().all(|task| task.get().is_none()));
    assert!(state.spare_groups.iter().all(|group| hold(group).is_none()));
    assert_eq!(viewer.memory_report().maintenance_workers, 0);
    assert!(viewer.memory_report().is_read_only && !store.memory_report().is_read_only);
    // a reader fills no record window, so its slots hold none
    assert_eq!(viewer.memory_report().queues, ByteCount(0));
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

// a view taken at any step of the seal of a range delete's segment hides every key the delete covers: it holds the delete's
// row and its cover together, or neither
#[test]
fn view_mid_seal_keeps_covers() {
    let io = SimulatedIo::new(Seed(31));
    let store = open(&io);
    for number in 0..SPAN_KEYS {
        store.put(COLUMN, format!("span-{number:06}").as_bytes(), b"value").expect("put");
    }
    seal(&store, &io);
    store.delete_range(COLUMN, b"span-000000", format!("span-{SPAN_KEYS:06}").as_bytes()).expect("delete");
    store.put(COLUMN, b"tail", b"value").expect("put");
    store.flush().expect("flush");
    land_rows(&io);
    store.seal_now();
    let mut buf = Vec::new();
    let mut views = 0;
    for _step in 0..STEPS {
        let view = view(&io);
        for number in 0..SPAN_KEYS {
            let key = format!("span-{number:06}");
            assert!(!view.get(COLUMN, key.as_bytes(), &mut buf).expect("get"), "{key} came back in a view after {views} steps");
        }
        view.close().expect("close");
        views += 1;
        if io.step_any().is_none() {
            break;
        }
    }
    assert!(views > 1);
    store.close().expect("close");
}

const SPAN_KEYS: u64 = 100;

// a view of an lz4 column decodes values under a chunk, over one and at the largest stored size with no encoder and no stored
// share, by get and by a full scan
#[test]
fn reads_compressed_values() {
    let io = SimulatedIo::new(Seed(31));
    let mut config = config();
    config.memory_budget = ByteCount(LZ4_BUDGET);
    config.threads = crate::shape::threads();
    config.columns[0].codec = Codec::Lz4;
    let store = ReelStore::open(io.clone(), &config).expect("open");
    let largest = store.state.sizing.lz4_stored_limit.0 as usize;
    let lengths = [1_000, CHUNK_BYTES - 1, CHUNK_BYTES + 1_000, 5 * CHUNK_BYTES + 17, largest];
    let value_of = |length: usize| (0..length).map(|at| ((at * 7 + length) % 251) as u8).collect::<Vec<u8>>();
    let mut written = Vec::new();
    for (number, length) in lengths.into_iter().enumerate() {
        let name = key(number as u64);
        store.put(COLUMN, &name, &value_of(length)).expect("put");
        written.push((name, value_of(length)));
    }
    // the finish waits on seals that only the stepped jobs run, so it runs on its own thread while this one steps them
    std::thread::scope(|scope| {
        let finishing = scope.spawn(|| store.finish());
        while !finishing.is_finished() {
            if io.step_any().is_none() {
                io.advance_clock(TICK);
            }
        }
        finishing.join().expect("the finish thread").expect("finish");
    });
    drop(store);

    let viewer = ReelStore::open_read_only(io.clone(), &config).expect("open read-only");
    let state = &viewer.state;
    assert!(state.encoders.grab().is_none() && state.stored_shares.grab().is_none(), "a view holds an encoder or a stored share");
    for (name, value) in &written {
        assert_eq!(read(&viewer, name).as_deref(), Some(value.as_slice()), "{} bytes", value.len());
    }
    let mut scan = viewer.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
    let (mut name, mut value, mut seen) = (Vec::new(), Vec::new(), 0);
    while scan.next_row(&mut name, &mut value).expect("row") == ScanStep::Found {
        assert_eq!((&name, &value), (&written[seen].0, &written[seen].1), "row {seen}");
        seen += 1;
    }
    assert_eq!(seen, written.len());
    drop(scan);
    viewer.close().expect("close");
}

// a budget that holds the lz4 stored share and the largest stored value
const LZ4_BUDGET: u64 = 320 * 1024 * 1024;

// run a finish on its own thread while this one steps the jobs it waits on
fn finish_stepped(store: &Store, io: &SimulatedIo) {
    std::thread::scope(|scope| {
        let finishing = scope.spawn(|| store.finish());
        while !finishing.is_finished() {
            if io.step_any().is_none() {
                io.advance_clock(TICK);
            }
        }
        finishing.join().expect("the finish thread").expect("finish");
    });
}

// the write index a view holds for no open rows: the least each shard takes
fn least_index(viewer: &Store) -> ByteCount {
    let shard = shard_for_rows(0, viewer.state.sizing.shard.open_slots);
    ByteCount(shard.heap_bytes().0 * SHARD_COUNT as u64)
}

// a view of a finished store holds the least write index, and its record cache has what a writer's index would
#[test]
fn a_finished_store_is_viewed_with_the_least_index() {
    let io = SimulatedIo::new(Seed(32));
    let store = open(&io);
    put_keys(&store, b"value");
    finish_stepped(&store, &io);
    // a writer's pools grow with what it holds, so its plan is what a viewer's least index stays under
    let writer_index = ByteCount(store.state.sizing.shard.heap_bytes().0 * SHARD_COUNT as u64);
    drop(store);
    let viewer = view(&io);
    let memory = viewer.memory_report();
    assert_eq!(memory.write_index, least_index(&viewer), "{memory:?}");
    assert!(memory.write_index < writer_index, "{memory:?}");
    assert_eq!(keys(&viewer).len(), KEYS as usize);
    viewer.close().expect("close");
}

// a view beside a writer sizes its index from the open rows at each load, so a refresh after the writer added rows holds more,
// and every row reads
#[test]
fn a_refresh_sizes_the_index_from_the_open_rows() {
    let io = SimulatedIo::new(Seed(33));
    let store = open(&io);
    put_keys(&store, b"first");
    land_rows(&io);
    let mut viewer = view(&io);
    let before = viewer.memory_report().write_index;
    for number in KEYS..GROWN_KEYS {
        store.put(COLUMN, &key(number), b"later").expect("put");
    }
    land_rows(&io);
    viewer.refresh().expect("refresh");
    let after = viewer.memory_report().write_index;
    assert!(after > before, "the index stayed at {before:?}, now {after:?}");
    for number in 0..GROWN_KEYS {
        let wanted: &[u8] = if number < KEYS { b"first" } else { b"later" };
        assert_eq!(read(&viewer, &key(number)).as_deref(), Some(wanted), "key {number}");
    }
    viewer.close().expect("close");
    store.close().expect("close");
}

// a budget too small for a writer's open rows refuses the view at open and at refresh, as it did before the index followed the rows
#[test]
fn open_rows_past_the_budget_refuse_the_view() {
    let io = SimulatedIo::new(Seed(34));
    let mut big = config();
    big.memory_budget = ByteCount(WRITER_BUDGET);
    let store = ReelStore::open(io.clone(), &big).expect("open");
    let small = config();
    store.put(COLUMN, &key(0), b"one").expect("put");
    land_rows(&io);
    let mut viewer = ReelStore::open_read_only(io.clone(), &small).expect("a view of one open row");
    for number in 1..MANY_KEYS {
        store.put(COLUMN, &key(number), b"many").expect("put");
    }
    land_rows(&io);
    let is_too_many = |outcome: Result<(), StoreError>| matches!(outcome, Err(StoreError::Io { source, .. }) if source.to_string().contains("open rows don't fit"));
    let refreshed = viewer.refresh();
    assert!(is_too_many(refreshed), "a refresh took the open rows in");
    assert!(is_too_many(ReelStore::open_read_only(io.clone(), &small).map(|_view| ())), "an open took the open rows in");
    store.close().expect("close");
}

// keys a writer has out once it has added more past the first ones
const GROWN_KEYS: u64 = 20_000;
// open rows past what a view's write index holds at BUDGET, from a writer that holds them all unsealed
const MANY_KEYS: u64 = 400_000;
const WRITER_BUDGET: u64 = 768 * 1024 * 1024;

// a reader set to no lifetime group beside a writer with a marked open segment reads it and leaves it: the recovery seal of a
// marked group is the writer's, and a reader writes nothing and seals nothing
#[test]
fn a_reader_with_no_groups_seals_nothing() {
    let io = SimulatedIo::new(Seed(35));
    let store = open(&io);
    store.put_marked(COLUMN, b"marked", b"value", reel_core::units::DeathMark(1_000_000)).expect("put");
    land_rows(&io);
    let before = io.list_directory(ROOT.as_ref()).expect("list");
    let writes = io.write_count();
    let mut none = config();
    none.volumes[0].lifetime_groups = Some(0);
    let viewer = ReelStore::open_read_only(io.clone(), &none).expect("open read-only");
    assert_eq!(read(&viewer, b"marked"), Some(b"value".to_vec()));
    viewer.close().expect("close");
    assert_eq!(io.write_count(), writes, "the reader wrote");
    assert_eq!(io.list_directory(ROOT.as_ref()).expect("list"), before, "the reader changed the files");
    store.close().expect("close");
}

// a reader's counts can't see its writer's frees, so it checks every record it reads, and bytes changed under it report the
// failure
#[test]
fn a_reader_checks_a_record_changed_under_it() {
    let io = SimulatedIo::new(Seed(90));
    let writer = open(&io);
    writer.put(COLUMN, &key(1), b"value").expect("put");
    seal(&writer, &io);
    land_rows(&io);
    let viewer = view(&io);
    let volume = &viewer.state.volumes[0];
    let name = key(1);
    let read = viewer.state.key_read(COLUMN, &name, viewer.state.read_point(volume), false).expect("read");
    let row = in_place(viewer.state.search_sealed(&InPlace, volume, &read)).expect("search").expect("a sealed row");
    let path = data_path(&volume.roots[0], row.segment);
    io.rot(&path, FileOffset(row.row.offset.0 + SMALL_HEAD_BYTES as u64), BitMask(1)).expect("rot");
    let mut buf = Vec::new();
    let answer = viewer.get(COLUMN, &key(1), &mut buf);
    assert!(matches!(answer, Err(StoreError::Corruption { .. })), "the reader took changed bytes: {answer:?}");
    viewer.close().expect("close");
    writer.close().expect("close");
}

// a view trusts no record: its scan over open rows a writer left checks every one
#[test]
fn a_view_scan_trusts_no_record() {
    let io = SimulatedIo::new(Seed(77));
    let store = open(&io);
    for number in 0..50_u32 {
        store.put(COLUMN, format!("aaa-{number:04}").as_bytes(), b"open value").expect("put");
    }
    for number in 0..200_u32 {
        store.put(COLUMN, format!("zzz-{number:04}").as_bytes(), &[7_u8; 1_000]).expect("put");
    }
    store.flush().expect("flush");

    let viewer = view(&io);
    let mut scan = viewer.iter_from(COLUMN, b"a", Direction::Ascending).expect("scan");
    let (mut name, mut value, mut found) = (Vec::new(), Vec::new(), 0);
    while scan.next_row(&mut name, &mut value).expect("step") == ScanStep::Found {
        found += 1;
    }
    assert_eq!(found, 250);
    assert_eq!(viewer.state.scan_trusted_records.load(Ordering::Relaxed), 0, "a view trusted a record");
    drop(scan);
    drop(viewer);
    store.close().expect("close");
}
