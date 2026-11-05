//! A range delete: one record, a key written after it survives, and it holds through seals and opens

use std::path::{Path, PathBuf};
use std::thread;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::open_segment::rows_path;
use reel::engine::reel_store::ReelStore;
use reel::engine::write_path::ClaimRequest;
use reel::io::{Io, IoFile};
use reel::record::codec::Codec;
use reel::segment::range_covers::MOST_RANGE_COVERS;
use reel::segment::segment_tail::{SECOND_TAIL_DISTANCE, SegmentNumber};
use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, Nanoseconds, RowCount, VolumeId, WritePlace};
use reel_api::write_batch::WriteBatch;
use reel_sim::file_image::BitMask;
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_disk::OperationCount;
use reel_sim::simulated_io::SimulatedIo;

const ROOT: &str = "/disk/ranges";
const COLUMN: ColumnId = ColumnId(1);
const OTHER: ColumnId = ColumnId(2);
const BUDGET: ByteCount = ByteCount(48 * 1024 * 1024);
const KEYS: u64 = 100;
const SEARCH_STEPS: usize = 4;
const CRASH_POINTS: u64 = 10;
const TICK: Nanoseconds = Nanoseconds(1_000_000);

type Store = ReelStore<SimulatedIo>;

fn config() -> StoreConfig {
    let column = |id: ColumnId, name: &str| ColumnConfig {
        id,
        name: name.to_string(),
        key_shape: KeyShape::Variable { most: 64 },
        codec: Codec::None,
        volume: VolumeId(0),
    };
    StoreConfig {
        memory_budget: BUDGET,
        volumes: vec![VolumeConfig {
            name: "ranges".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
        }],
        columns: vec![column(COLUMN, "column"), column(OTHER, "other")],
    }
}

fn open(io: &SimulatedIo) -> Store {
    ReelStore::open(io.clone(), &config()).expect("open")
}

fn key_of(number: u64) -> Vec<u8> {
    format!("key-{number:06}").into_bytes()
}

fn read(store: &Store, column: ColumnId, key: &[u8]) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    store.get(column, key, &mut buf).expect("get").then_some(buf)
}

fn fill(store: &Store) {
    for number in 0..KEYS {
        store.put(COLUMN, &key_of(number), b"older").expect("put");
        store.put(OTHER, &key_of(number), b"older").expect("put");
    }
}

fn seal(store: &Store, io: &SimulatedIo) {
    let before = store.state.volumes[0].table.segment_count();
    store.seal_now();
    while store.state.volumes[0].table.segment_count() == before {
        if io.step_any().is_none() {
            io.advance_clock(TICK);
        }
    }
}

fn assert_deleted(store: &Store) {
    for number in 0..KEYS {
        let expected = (!(10..20).contains(&number)).then(|| b"older".to_vec());
        assert_eq!(read(store, COLUMN, &key_of(number)), expected, "key {number}");
        assert_eq!(read(store, OTHER, &key_of(number)), Some(b"older".to_vec()), "other column, key {number}");
    }
}

// the range goes, its end stays, and the column beside it isn't touched
#[test]
fn removes_range() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io);
    fill(&store);

    store.delete_range(COLUMN, &key_of(10), &key_of(20)).expect("delete");

    assert_deleted(&store);
    assert!(!store.contains(COLUMN, &key_of(15)).expect("contains"));
    assert!(store.contains(COLUMN, &key_of(20)).expect("contains"));
    store.close().expect("close");
}

// sealed rows under a cover stay hidden, and a reopen brings the cover back from the footer
#[test]
fn sealed_rows() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io);
    fill(&store);
    seal(&store, &io);

    store.delete_range(COLUMN, &key_of(10), &key_of(20)).expect("delete");

    assert_deleted(&store);
    seal(&store, &io);
    assert_deleted(&store);
    store.close().expect("close");
    drop(store);

    let store = open(&io);
    assert_deleted(&store);
    assert_eq!(store.state.volumes[0].covers.len(), 1);
    store.close().expect("close");
}

// a key written after a range delete survives it, also through a seal and a reopen
#[test]
fn later_write() {
    let io = SimulatedIo::new(Seed(3));
    let store = open(&io);
    fill(&store);
    store.delete_range(COLUMN, &key_of(10), &key_of(20)).expect("delete");
    store.put(COLUMN, &key_of(15), b"newer").expect("put");
    store.put(COLUMN, &key_of(10), b"newer").expect("put");

    assert_eq!(read(&store, COLUMN, &key_of(15)), Some(b"newer".to_vec()));
    assert_eq!(read(&store, COLUMN, &key_of(10)), Some(b"newer".to_vec()));
    assert_eq!(read(&store, COLUMN, &key_of(11)), None);
    seal(&store, &io);
    store.close().expect("close");
    drop(store);

    let store = open(&io);
    assert_eq!(read(&store, COLUMN, &key_of(15)), Some(b"newer".to_vec()));
    assert_eq!(read(&store, COLUMN, &key_of(10)), Some(b"newer".to_vec()));
    assert_eq!(read(&store, COLUMN, &key_of(11)), None);
    store.close().expect("close");
}

// an open rebuilds a cover from an open segment's rows, after a kill
#[test]
fn open_rows() {
    let io = SimulatedIo::new(Seed(4));
    let store = open(&io);
    fill(&store);
    store.delete_range(COLUMN, &key_of(10), &key_of(20)).expect("delete");
    store.flush().expect("flush");
    io.kill();
    drop(store);

    let store = open(&io);

    assert_deleted(&store);
    store.close().expect("close");
}

// a batch applies a range delete in batch order with what's around it
#[test]
fn batch_order() {
    let io = SimulatedIo::new(Seed(5));
    let store = open(&io);
    fill(&store);
    let mut batch = WriteBatch::new();
    batch.put(COLUMN, &key_of(11), b"before");
    batch.delete_range(COLUMN, &key_of(10), &key_of(20));
    batch.put(COLUMN, &key_of(12), b"after");

    store.write_batch(&batch).expect("batch");

    assert_eq!(read(&store, COLUMN, &key_of(11)), None);
    assert_eq!(read(&store, COLUMN, &key_of(12)), Some(b"after".to_vec()));
    assert_eq!(read(&store, COLUMN, &key_of(13)), None);
    store.flush().expect("flush");
    io.kill();
    drop(store);

    let store = open(&io);
    assert_eq!(read(&store, COLUMN, &key_of(11)), None);
    assert_eq!(read(&store, COLUMN, &key_of(12)), Some(b"after".to_vec()));
    store.close().expect("close");
}

// a crash leaves the delete whole or absent, and what it hid never comes back once a flush covered it
#[test]
fn crash_points() {
    for crash_at in 0..CRASH_POINTS {
        for is_power_cut in [false, true] {
            let io = SimulatedIo::new(Seed(crash_at));
            let store = open(&io);
            fill(&store);
            store.flush().expect("flush");

            io.arm_crash(OperationCount(crash_at));
            let flushed = store.delete_range(COLUMN, &key_of(10), &key_of(20)).is_ok() && store.flush().is_ok();
            let is_flushed = flushed && !io.is_crashed();
            if is_power_cut {
                io.power_cut();
            } else {
                io.kill();
            }
            drop(store);

            let store = open(&io);
            let found: Vec<_> = (10..20).map(|number| read(&store, COLUMN, &key_of(number))).collect();
            assert!(found.iter().all(|value| *value == found[0]), "crash at {crash_at}: {found:?}");
            assert!(found[0].is_none() || !is_flushed, "crash at {crash_at}");
            store.close().expect("close");
        }
    }
}

// a cover inside a newer one is dropped, and what it hid stays hidden
#[test]
fn inside_newer() {
    let io = SimulatedIo::new(Seed(6));
    let store = open(&io);
    fill(&store);
    store.delete_range(COLUMN, &key_of(12), &key_of(15)).expect("delete");
    store.delete_range(COLUMN, &key_of(10), &key_of(20)).expect("delete");

    assert_eq!(store.state.volumes[0].covers.len(), 1);
    assert_deleted(&store);
    store.close().expect("close");
}

// a cover that only overlaps a newer one stays, and each hides its own keys
#[test]
fn overlapping() {
    let io = SimulatedIo::new(Seed(7));
    let store = open(&io);
    fill(&store);
    store.delete_range(COLUMN, &key_of(10), &key_of(15)).expect("delete");
    store.delete_range(COLUMN, &key_of(13), &key_of(20)).expect("delete");

    assert_eq!(store.state.volumes[0].covers.len(), 2);
    assert_deleted(&store);
    store.close().expect("close");
}

// an older cover wider than a newer one inside it still answers for keys past the newer one
#[test]
fn wider_older() {
    let io = SimulatedIo::new(Seed(8));
    let store = open(&io);
    fill(&store);
    store.delete_range(COLUMN, &key_of(10), &key_of(20)).expect("delete");
    store.delete_range(COLUMN, &key_of(12), &key_of(14)).expect("delete");

    assert_eq!(store.state.volumes[0].covers.len(), 2);
    assert_deleted(&store);
    store.close().expect("close");
}

// a full pool refuses with nothing written, and a get still answers right
#[test]
fn full_pool() {
    let io = SimulatedIo::new(Seed(9));
    let store = open(&io);
    fill(&store);
    for number in 0..MOST_RANGE_COVERS as u64 {
        let start = 1_000_000 + 2 * number;
        store.delete_range(COLUMN, &key_of(start), &key_of(start + 1)).expect("delete");
    }
    let before = store.state.read_point();

    let refused = store.delete_range(COLUMN, &key_of(10), &key_of(20));

    assert!(matches!(refused, Err(StoreError::RangeDeletesFull { .. })));
    assert_eq!(store.state.read_point(), before);
    assert_eq!(read(&store, COLUMN, &key_of(15)), Some(b"older".to_vec()));
    store.close().expect("close");
}

// a search over a full pool looks at a handful of covers, however many there are
#[test]
fn search_cost() {
    let io = SimulatedIo::new(Seed(10));
    let store = open(&io);
    let covers = &store.state.volumes[0].covers;
    assert_eq!(covers.search_steps(COLUMN, &key_of(1)), 0);
    store.delete_range(COLUMN, &key_of(10), &key_of(11)).expect("delete");
    assert!(covers.search_steps(COLUMN, &key_of(10)) <= SEARCH_STEPS);

    for number in 1..MOST_RANGE_COVERS as u64 {
        let start = 2 * number + 10;
        store.delete_range(COLUMN, &key_of(start), &key_of(start + 1)).expect("delete");
    }

    assert_eq!(covers.len(), MOST_RANGE_COVERS);
    for number in [0, 1, 2_000, 4_000, 9_000] {
        assert!(covers.search_steps(COLUMN, &key_of(number)) <= SEARCH_STEPS, "key {number}");
    }
    store.close().expect("close");
}

// the pool comes back the same from the footers
#[test]
fn pool_reopens() {
    let io = SimulatedIo::new(Seed(11));
    let store = open(&io);
    fill(&store);
    store.delete_range(COLUMN, &key_of(10), &key_of(15)).expect("delete");
    store.delete_range(COLUMN, &key_of(13), &key_of(20)).expect("delete");
    store.delete_range(OTHER, &key_of(90), &key_of(95)).expect("delete");
    seal(&store, &io);
    store.delete_range(COLUMN, &key_of(50), &key_of(51)).expect("delete");
    seal(&store, &io);
    let before = store.state.volumes[0].covers.len();
    store.close().expect("close");
    drop(store);

    let store = open(&io);

    assert_eq!(store.state.volumes[0].covers.len(), before);
    assert_eq!(read(&store, COLUMN, &key_of(14)), None);
    assert_eq!(read(&store, OTHER, &key_of(92)), None);
    assert_eq!(read(&store, COLUMN, &key_of(50)), None);
    assert_eq!(read(&store, COLUMN, &key_of(51)), Some(b"older".to_vec()));
    store.close().expect("close");
}

// a rotted block of covers can't say what it hid, so the segment's columns refuse
#[test]
fn rotted_block() {
    let io = SimulatedIo::new(Seed(12));
    let store = open(&io);
    fill(&store);
    store.delete_range(COLUMN, &key_of(10), &key_of(20)).expect("delete");
    seal(&store, &io);
    store.close().expect("close");
    drop(store);
    let path = data_of(&io);
    let file = io.open_read_only(&path).expect("open");
    let mut bytes = vec![0; file.length().expect("length").0 as usize];
    file.read_exact_at(FileOffset(0), &mut bytes).expect("read");
    io.close_file(file);
    // only the block of covers holds the two keys side by side
    let pair = [key_of(10), key_of(20)].concat();
    let at = bytes.windows(pair.len()).position(|window| window == pair).expect("the block");
    io.rot(&path, FileOffset(at as u64), BitMask(1)).expect("rot");

    let store = open(&io);

    let mut buf = Vec::new();
    assert!(matches!(store.get(COLUMN, &key_of(15), &mut buf), Err(StoreError::Corruption { .. })));
    store.close().expect("close");
}

fn data_of(io: &SimulatedIo) -> PathBuf {
    let listing = io.list_directory(Path::new(ROOT)).expect("list");
    listing.into_iter().find(|path| path.extension().is_some_and(|extension| extension == "data")).expect("a data file")
}

// a newer cover drops an older one from the pool, but the older one's footer keeps it: when a crash takes the newer
// one, which sat in another segment, the delete a flush covered still holds
#[test]
fn older_outlives_newer() {
    let io = SimulatedIo::new(Seed(13));
    let store = open(&io);
    fill(&store);
    store.delete_range(COLUMN, &key_of(10), &key_of(20)).expect("delete");
    store.flush().expect("flush");

    let volume = &store.state.volumes[0];
    let alone = ClaimRequest {
        bytes: ByteCount(1),
        rows: RowCount(1),
        is_alone: true,
    };
    let claim = store.state.claim(volume, &alone).expect("claim");
    let (older_slot, older_segment) = (claim.slot, claim.segment);
    thread::scope(|scope| {
        let writer = scope.spawn(|| store.delete_range(COLUMN, &key_of(5), &key_of(30)));
        while store.state.index.lookup(COLUMN, &key_of(5), WritePlace(u64::MAX), volume.view()).row.is_none_or(|row| row.segment == older_segment) {
            thread::yield_now();
        }
        drop(claim);
        writer.join().expect("join").expect("delete");
    });
    assert_eq!(volume.covers.len(), 1);

    let newer_rows = rows_of_other(&io, older_segment);
    let before = volume.table.segment_count();
    store.state.seal_slot(volume, older_slot);
    while volume.table.segment_count() == before {
        if io.step_any().is_none() {
            io.advance_clock(TICK);
        }
    }
    io.kill();
    drop(store);
    io.rot(&newer_rows, FileOffset(0), BitMask(1)).expect("rot");

    let store = open(&io);

    assert_eq!(read(&store, COLUMN, &key_of(15)), None);
    store.close().expect("close");
}

// the rows file of the open segment that isn't this one
fn rows_of_other(io: &SimulatedIo, segment: SegmentNumber) -> PathBuf {
    let own = rows_path(Path::new(ROOT), segment).to_path_buf();
    let listing = io.list_directory(Path::new(ROOT)).expect("list");
    let mut rows = listing.into_iter().filter(|path| path.extension().is_some_and(|extension| extension == "rows"));
    rows.find(|path| *path != own).expect("a second rows file")
}

// a segment that loses both tails loses the covers its footer kept, so even keys past its own rows refuse
#[test]
fn lost_footer() {
    let io = SimulatedIo::new(Seed(14));
    let store = open(&io);
    store.put(COLUMN, &key_of(50), b"older").expect("put");
    seal(&store, &io);
    store.delete_range(COLUMN, &key_of(10), &key_of(90)).expect("delete");
    seal(&store, &io);
    // the seal drops the rows file last, and a tail with rows beside it reads as open
    while has_rows_file(&io) {
        if io.step_any().is_none() {
            io.advance_clock(TICK);
        }
    }
    store.close().expect("close");
    drop(store);

    let pair = [key_of(10), key_of(90)].concat();
    let mut holder = None;
    for path in io.list_directory(Path::new(ROOT)).expect("list") {
        if path.extension().is_none_or(|extension| extension != "data") {
            continue;
        }
        let file = io.open_read_only(&path).expect("open");
        let mut bytes = vec![0; file.length().expect("length").0 as usize];
        file.read_exact_at(FileOffset(0), &mut bytes).expect("read");
        io.close_file(file);
        if bytes.windows(pair.len()).any(|window| window == pair) {
            holder = Some((path, ByteCount(bytes.len() as u64)));
        }
    }
    let (newest, length) = holder.expect("the footer");
    let newest = &newest;
    io.rot(newest, FileOffset(length.0 - 3), BitMask(1)).expect("rot");
    io.rot(newest, FileOffset(length.0 - SECOND_TAIL_DISTANCE - 3), BitMask(1)).expect("rot");

    let store = open(&io);

    let mut buf = Vec::new();
    assert!(matches!(store.get(COLUMN, &key_of(50), &mut buf), Err(StoreError::Corruption { .. })));
    store.close().expect("close");
}

// a cover's record the disk lost refuses the column, and never lets what it hid come back
#[test]
fn lost_record() {
    let io = SimulatedIo::new(Seed(15));
    let store = open(&io);
    fill(&store);
    store.flush().expect("flush");
    store.delete_range(COLUMN, &key_of(10), &key_of(20)).expect("delete");
    store.flush().expect("flush");
    io.kill();
    drop(store);
    let path = data_of(&io);
    let file = io.open_read_only(&path).expect("open");
    let mut bytes = vec![0; file.length().expect("length").0 as usize];
    file.read_exact_at(FileOffset(0), &mut bytes).expect("read");
    io.close_file(file);
    let end = key_of(20);
    let at = bytes.windows(end.len()).rposition(|window| window == end).expect("the record");
    io.rot(&path, FileOffset(at as u64), BitMask(1)).expect("rot");

    let store = open(&io);

    let mut buf = Vec::new();
    assert!(matches!(store.get(COLUMN, &key_of(15), &mut buf), Err(StoreError::Corruption { .. })));
    store.close().expect("close");
}

fn has_rows_file(io: &SimulatedIo) -> bool {
    let listing = io.list_directory(Path::new(ROOT)).expect("list");
    listing.iter().any(|path| path.extension().is_some_and(|extension| extension == "rows"))
}
