//! A range delete: one record, a key written after it survives, and it holds through seals and opens

use std::path::{Path, PathBuf};
use std::thread;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::open_segment::{data_path, rows_path};
use reel::engine::recovery::copy_row;
use reel::engine::reel_store::ReelStore;
use reel::engine::write_path::ClaimRequest;
use reel::guard::hold;
use reel::io::{Io, IoFile};
use reel::record::codec::Codec;
use reel::record::record_head::plain_length;
use reel::row_kind::RowKind;
use reel::rows::row_group::RowEntry;
use reel::segment::range_covers::{Cover, MOST_RANGE_COVERS};
use reel::segment::segment_tail::{SECOND_TAIL_DISTANCE, SegmentNumber};
use reel_api::column_id::ColumnId;
use reel_api::reads::ValueWindow;
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
const BUDGET: ByteCount = ByteCount(60 * 1024 * 1024);
const KEYS: u64 = 100;
const SEARCH_STEPS: usize = 4;
const CRASH_POINTS: u64 = 10;
const LARGE_BUDGET: ByteCount = ByteCount(128 * 1024 * 1024);
const REPEATS: u64 = 5_000;
const CENTRE: u64 = 500_000;
const SPARE_STEPS: u64 = 10_000;
const ROT_CRASH_POINTS: u64 = 40;
const ROT_SEAL_STEPS: u64 = 400;
const BATCH_COVERS: u64 = 100;
const COVER_SURPLUS: u64 = 10;
const TICK: Nanoseconds = Nanoseconds(1_000_000);

type Store = ReelStore<SimulatedIo>;

fn config() -> StoreConfig {
    config_at(BUDGET)
}

fn config_at(budget: ByteCount) -> StoreConfig {
    let column = |id: ColumnId, name: &str| ColumnConfig {
        id,
        name: name.to_string(),
        key_shape: KeyShape::Variable { most: 64 },
        codec: Codec::None,
        volume: VolumeId(0),
        complete_rows: None,
    };
    StoreConfig {
        memory_budget: budget,
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "ranges".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(),
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
    io.keep_real_time();
    let store = open(&io);
    fill(&store);
    store.delete_range(COLUMN, &key_of(10), &key_of(20)).expect("delete");
    store.flush().expect("flush");

    let volume = &store.state.volumes[0];
    let alone = ClaimRequest {
        bytes: ByteCount(1),
        rows: RowCount(1),
        is_alone: true,
        has_records: true,
        covers: 0,
        mark: None,
        has_deletes: false,
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

// covers that keep dropping out of the pool still count against their segment, so a seal's buffer holds them all
#[test]
fn cover_cap() {
    let io = SimulatedIo::new(Seed(16));
    let store = open(&io);
    for number in 0..MOST_RANGE_COVERS as u64 + COVER_SURPLUS {
        store.delete_range(COLUMN, &key_of(100), &key_of(200 + number)).expect("delete");
    }

    assert_eq!(store.state.volumes[0].covers.len(), 1);
    let claims = hold(&store.state.volumes[0].claims);
    assert!(claims.open.iter().all(|segment| segment.cover_rows <= MOST_RANGE_COVERS));
    drop(claims);
    store.close().expect("close");
}

// a range with nothing in it deletes nothing and takes no slot, in a batch as in the single call
#[test]
fn empty_ranges() {
    let io = SimulatedIo::new(Seed(17));
    let store = open(&io);
    store.put(COLUMN, b"k", b"value").expect("put");
    store.delete_range(COLUMN, b"k", b"k").expect("delete");
    store.delete_range(COLUMN, b"m", b"k").expect("delete");
    let mut batch = WriteBatch::new();
    batch.delete_range(COLUMN, b"k", b"k");
    batch.delete_range(COLUMN, b"m", b"k");
    batch.put(COLUMN, b"after", b"value");

    store.write_batch(&batch).expect("batch");

    assert_eq!(read(&store, COLUMN, b"k"), Some(b"value".to_vec()));
    assert_eq!(read(&store, COLUMN, b"after"), Some(b"value".to_vec()));
    assert_eq!(store.state.volumes[0].covers.len(), 0);
    store.close().expect("close");
    drop(store);
    let store = open(&io);
    assert_eq!(read(&store, COLUMN, b"k"), Some(b"value".to_vec()));
    store.close().expect("close");
}

// every batch that fails gives back the slots it held, however early it fails
#[test]
fn hold_released() {
    let io = SimulatedIo::new(Seed(18));
    let store = open(&io);
    let huge = vec![1; store.state.volumes[0].segment_bytes.0 as usize / 2 + 1];
    let mut batch = WriteBatch::new();
    batch.delete_range(COLUMN, &key_of(1), &key_of(2));
    batch.put(COLUMN, b"first", &huge);
    batch.put(COLUMN, b"second", &huge);

    for _try in 0..MOST_RANGE_COVERS + 1 {
        assert!(matches!(store.write_batch(&batch), Err(StoreError::BatchTooLarge { .. })));
    }

    store.delete_range(COLUMN, &key_of(1), &key_of(2)).expect("delete");
    store.close().expect("close");
}

// a batch on a full pool is refused with nothing written
#[test]
fn full_batch() {
    let io = SimulatedIo::new(Seed(19));
    let store = open(&io);
    for number in 0..MOST_RANGE_COVERS as u64 {
        let start = 1_000_000 + 2 * number;
        store.delete_range(COLUMN, &key_of(start), &key_of(start + 1)).expect("delete");
    }
    let mut batch = WriteBatch::new();
    batch.put(COLUMN, b"key", b"value");
    batch.delete_range(COLUMN, &key_of(10), &key_of(20));

    assert!(matches!(store.write_batch(&batch), Err(StoreError::RangeDeletesFull { .. })));

    assert_eq!(read(&store, COLUMN, b"key"), None);
    store.close().expect("close");
}

// a grave and a range delete over one key, in either order, leave it gone until a later write
#[test]
fn grave_and_range() {
    let io = SimulatedIo::new(Seed(20));
    let store = open(&io);
    fill(&store);
    store.delete(COLUMN, &key_of(12)).expect("delete");
    store.delete_range(COLUMN, &key_of(10), &key_of(20)).expect("delete");
    store.delete_range(COLUMN, &key_of(30), &key_of(40)).expect("delete");
    store.delete(COLUMN, &key_of(32)).expect("delete");

    assert_eq!(read(&store, COLUMN, &key_of(12)), None);
    assert_eq!(read(&store, COLUMN, &key_of(32)), None);
    seal(&store, &io);
    assert_eq!(read(&store, COLUMN, &key_of(12)), None);
    assert_eq!(read(&store, COLUMN, &key_of(32)), None);
    store.close().expect("close");
}

// a window read and contains both answer as a get does under a cover
#[test]
fn other_reads() {
    let io = SimulatedIo::new(Seed(21));
    let store = open(&io);
    fill(&store);
    store.delete_range(COLUMN, &key_of(10), &key_of(20)).expect("delete");
    let mut buf = Vec::new();
    let window = ValueWindow {
        offset: ByteCount(0),
        length: ByteCount(2),
    };

    assert!(!store.get_range(COLUMN, &key_of(15), window, &mut buf).expect("window"));
    assert!(!store.contains(COLUMN, &key_of(15)).expect("contains"));
    assert!(store.get_range(COLUMN, &key_of(25), window, &mut buf).expect("window"));
    store.close().expect("close");
}

// a store written at one budget, with more rows than a smaller budget can rebuild, and a range delete among them
fn past_small_budget(seed: u64) -> SimulatedIo {
    let limit_io = SimulatedIo::new(Seed(seed));
    let limit_store = ReelStore::open(limit_io.clone(), &config_at(BUDGET)).expect("open");
    let limit = limit_store.state.volumes[0].segment_rows.0;
    limit_store.close().expect("close");

    let io = SimulatedIo::new(Seed(seed));
    let store = ReelStore::open(io.clone(), &config_at(LARGE_BUDGET)).expect("open");
    fill(&store);
    store.delete_range(COLUMN, &key_of(10), &key_of(20)).expect("delete");
    for number in 0..limit + 1 {
        store.put(OTHER, &key_of(1_000 + number), b"v").expect("put");
    }
    store.flush().expect("flush");
    io.kill();
    drop(store);
    io
}

// a range delete copied at open keeps hiding what it hid
#[test]
fn copied_cover() {
    let io = past_small_budget(22);

    let store = open(&io);

    assert_eq!(read(&store, COLUMN, &key_of(15)), None);
    assert_eq!(read(&store, COLUMN, &key_of(25)), Some(b"older".to_vec()));
    store.close().expect("close");
    drop(store);
    let store = open(&io);
    assert_eq!(read(&store, COLUMN, &key_of(15)), None);
    store.close().expect("close");
}

// a cover the copy can't read refuses the column, and never shows what it hid
#[test]
fn copied_rot() {
    let io = past_small_budget(23);
    let path = data_of(&io);
    let file = io.open_read_only(&path).expect("open");
    let mut bytes = vec![0; file.length().expect("length").0 as usize];
    file.read_exact_at(FileOffset(0), &mut bytes).expect("read");
    io.close_file(file);
    let end = key_of(20);
    // the put of key 20 has its key in the rows file, so the data file holds these bytes in the cover's record alone
    let at = bytes.windows(end.len()).position(|window| window == end).expect("the record");
    io.rot(&path, FileOffset(at as u64), BitMask(1)).expect("rot");

    let store = open(&io);

    let mut buf = Vec::new();
    assert!(matches!(store.get(COLUMN, &key_of(15), &mut buf), Err(StoreError::Corruption { .. })));
    store.close().expect("close");
}

// a seal that can't read a cover's record finishes, and the columns refuse
#[test]
fn seal_bad_cover() {
    let io = SimulatedIo::new(Seed(24));
    let store = open(&io);
    fill(&store);
    store.delete_range(COLUMN, &key_of(10), &key_of(20)).expect("delete");
    store.flush().expect("flush");
    let path = data_of(&io);
    let file = io.open_read_only(&path).expect("open");
    let mut bytes = vec![0; file.length().expect("length").0 as usize];
    file.read_exact_at(FileOffset(0), &mut bytes).expect("read");
    io.close_file(file);
    let end = key_of(20);
    let at = bytes.windows(end.len()).position(|window| window == end).expect("the record");
    io.rot(&path, FileOffset(at as u64), BitMask(1)).expect("rot");

    seal(&store, &io);

    let mut buf = Vec::new();
    assert!(matches!(store.get(COLUMN, &key_of(15), &mut buf), Err(StoreError::Corruption { .. })));
    store.close().expect("close");
}

// one range deleted 5,000 times in a row stays one cover, and each segment seals inside its buffer
#[test]
fn many_repeats() {
    let io = SimulatedIo::new(Seed(25));
    let store = open(&io);
    fill(&store);
    for _repeat in 0..REPEATS {
        store.delete_range(COLUMN, &key_of(10), &key_of(20)).expect("delete");
    }

    assert_eq!(store.state.volumes[0].covers.len(), 1);
    seal(&store, &io);
    assert_deleted(&store);
    store.close().expect("close");
    drop(store);
    let store = open(&io);
    assert_deleted(&store);
    store.close().expect("close");
}

// every slot at its cover limit seals, so range deletes go on. Each start key differs, or the index fills first and its
// wait seals the segments, and each cover holds the one before, so the pool stays at one
#[test]
fn every_slot_capped() {
    let io = SimulatedIo::new(Seed(26));
    let store = ReelStore::open(io.clone(), &config_at(LARGE_BUDGET)).expect("open");
    let slots = store.state.sizing.shard.open_slots as u64;
    for number in 0..(MOST_RANGE_COVERS as u64 + 1) * (slots + 1) {
        store.delete_range(COLUMN, &key_of(CENTRE - number), &key_of(CENTRE + 1 + number)).expect("delete");
    }

    assert_eq!(store.state.volumes[0].covers.len(), 1);
    store.close().expect("close");
}

// batches that fit no segment's cover room still land: the fullest seals though none is at the limit
#[test]
fn batches_past_room() {
    let io = SimulatedIo::new(Seed(27));
    let store = ReelStore::open(io.clone(), &config_at(LARGE_BUDGET)).expect("open");
    let slots = store.state.sizing.shard.open_slots as u64;
    let mut number = 0;
    for _batch in 0..(MOST_RANGE_COVERS as u64 / BATCH_COVERS + 1) * (slots + 1) {
        let mut batch = WriteBatch::new();
        for _repeat in 0..BATCH_COVERS {
            batch.delete_range(COLUMN, &key_of(CENTRE - number), &key_of(CENTRE + 1 + number));
            number += 1;
        }
        store.write_batch(&batch).expect("batch");
    }

    assert_eq!(store.state.volumes[0].covers.len(), 1);
    store.close().expect("close");
}

// a spare a crash left behind is gone after open, and the data around it is whole
#[test]
fn spare_crash() {
    for is_power_cut in [false, true] {
        let io = SimulatedIo::new(Seed(28));
        let store = open(&io);
        fill(&store);
        store.flush().expect("flush");
        hold(&store.state.volumes[0].claims).wants_prepared = true;
        store.state.prepare_segments();
        let spare = hold(&store.state.volumes[0].claims).prepared.as_ref().map(|segment| segment.number).expect("a spare");
        if is_power_cut {
            io.power_cut();
        } else {
            io.kill();
        }
        drop(store);

        let store = open(&io);

        let spare_rows = rows_path(Path::new(ROOT), spare).to_path_buf();
        // open seals what holds no row away with its jobs
        for _step in 0..SPARE_STEPS {
            if !io.list_directory(Path::new(ROOT)).expect("list").contains(&spare_rows) {
                break;
            }
            if io.step_any().is_none() {
                io.advance_clock(TICK);
            }
        }
        let listing = io.list_directory(Path::new(ROOT)).expect("list");
        assert!(!listing.contains(&spare_rows));
        for number in 0..KEYS {
            assert_eq!(read(&store, COLUMN, &key_of(number)), Some(b"older".to_vec()));
        }
        store.close().expect("close");
    }
}

// a cover that rots before its seal can't go quietly: at every crash point the older value stays hidden or the read
// refuses
#[test]
fn rotted_cover_seal() {
    for crash_at in 0..ROT_CRASH_POINTS {
        for is_power_cut in [false, true] {
            let io = SimulatedIo::new(Seed(31));
            let store = open(&io);
            store.put(COLUMN, b"key", b"older").expect("put");
            store.flush().expect("flush");
            seal(&store, &io);
            store.put(COLUMN, b"aaa", b"beside").expect("put");
            store.delete_range(COLUMN, b"key", b"kez").expect("delete");
            store.flush().expect("flush");
            // rot under a mark no power cut can lose: without one a synced last group reads as a torn end, which the
            // spec accepts
            store.flush().expect("flush");

            let volume = &store.state.volumes[0];
            let claims = hold(&volume.claims);
            let segment = claims.open.iter().find(|segment| segment.cover_rows > 0).expect("a cover");
            let path = data_path(&volume.roots[segment.root.0 as usize], segment.number).to_path_buf();
            let end = FileOffset(segment.write_end.0 - 1);
            drop(claims);
            io.rot(&path, end, BitMask(1)).expect("rot");

            store.seal_now();
            io.arm_crash(OperationCount(crash_at));
            for _step in 0..ROT_SEAL_STEPS {
                if io.is_crashed() {
                    break;
                }
                if io.step_any().is_none() {
                    io.advance_clock(TICK);
                }
            }
            if is_power_cut {
                io.power_cut();
            } else {
                io.kill();
            }
            drop(store);

            let store = open(&io);
            let mut buf = Vec::new();
            let found = store.get(COLUMN, b"key", &mut buf);
            assert!(!matches!(found, Ok(true)), "crash at {crash_at} read {buf:?}");
            store.close().expect("close");
        }
    }
}

// a copied cover with no slot refuses its columns for this open, and the next open decides again from the rows
#[test]
fn copied_cover_full() {
    let io = SimulatedIo::new(Seed(32));
    let store = open(&io);
    store.put(COLUMN, b"key", b"older").expect("put");
    seal(&store, &io);
    store.delete_range(COLUMN, b"key", b"kez").expect("delete");
    store.flush().expect("flush");

    let volume = &store.state.volumes[0];
    let length = plain_length(ByteCount(b"kez".len() as u64));
    let (number, data, offset) = {
        let claims = hold(&volume.claims);
        let segment = claims.open.iter().find(|segment| segment.cover_rows > 0).expect("a cover");
        (segment.number, segment.data.clone(), FileOffset(segment.write_end.0 - length.0))
    };
    let row = RowEntry {
        column: COLUMN,
        key: b"key",
        place: store.state.order.mark(),
        offset,
        length,
        kind: RowKind::RangeDelete,
    };
    // at a real open a copied cover isn't in the pool yet, so the test takes it out
    volume.covers.drop_inside(&Cover {
        column: COLUMN,
        start: b"a",
        end: b"z",
        place: WritePlace(row.place.0 + 1),
        segment: number,
    });
    for filler in 0..MOST_RANGE_COVERS as u64 {
        let (start, end) = (key_of(filler * 2), key_of(filler * 2 + 1));
        let cover = Cover {
            column: OTHER,
            start: &start,
            end: &end,
            place: WritePlace(1),
            segment: number,
        };
        if !volume.covers.add(&cover, false) {
            break;
        }
    }

    copy_row(&store.state, 0, number, &data, &row, &mut Vec::new()).expect("copy");

    let mut buf = Vec::new();
    assert!(matches!(store.get(COLUMN, b"key", &mut buf), Err(StoreError::Corruption { .. })));
    store.close().expect("close");
    drop(store);

    let store = open(&io);
    assert_eq!(read(&store, COLUMN, b"key"), None);
    store.close().expect("close");
}
