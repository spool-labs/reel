//! A batch lands whole or not at all: to a read beside it and after any crash

use std::path::{Path, PathBuf};
use std::thread;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::batch_path::MOST_GROUP_ROW_BYTES;
use reel::engine::reel_store::ReelStore;
use reel::engine::write_path::ClaimRequest;
use reel::io::{Io, IoFile};
use reel::record::codec::Codec;
use reel::rows::row_group::{RowEntry, parse_group};
use reel::superblock::Superblock;
use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, RowCount, VolumeId, WritePlace};
use reel_api::write_batch::WriteBatch;
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_disk::OperationCount;
use reel_sim::simulated_io::SimulatedIo;

const ROOT: &str = "/disk/batch-meta";
const SECOND_ROOT: &str = "/disk/batch-bulk";
const PLAIN: ColumnId = ColumnId(1);
const PACKED: ColumnId = ColumnId(2);
const OTHER_VOLUME: ColumnId = ColumnId(3);
const BUDGET: ByteCount = ByteCount(128 * 1024 * 1024);
const BATCH_KEYS: u64 = 5;
const CRASH_POINTS: u64 = 12;
const LARGE_VALUE_BYTES: usize = 200_000;
const FILLER_ROWS: u64 = 300;
const ROOM_ROWS: u64 = 2_000;

type Store = ReelStore<SimulatedIo>;

fn config() -> StoreConfig {
    let volume = |name: &str, root: &str| VolumeConfig {
        name: name.to_string(),
        roots: vec![PathBuf::from(root)],
        sync: SyncPolicy::Never,
    };
    let column = |id: ColumnId, name: &str, codec: Codec, volume: u8| ColumnConfig {
        id,
        name: name.to_string(),
        key_shape: KeyShape::Variable { most: 64 },
        codec,
        volume: VolumeId(volume),
    };
    StoreConfig {
        memory_budget: BUDGET,
        volumes: vec![volume("meta", ROOT), volume("bulk", SECOND_ROOT)],
        columns: vec![
            column(PLAIN, "plain", Codec::None, 0),
            column(PACKED, "packed", Codec::Lz4, 0),
            column(OTHER_VOLUME, "other", Codec::None, 1),
        ],
    }
}

fn open(io: &SimulatedIo) -> Store {
    ReelStore::open(io.clone(), &config()).expect("open")
}

fn read(store: &Store, column: ColumnId, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
    let mut buf = Vec::new();
    Ok(store.get(column, key, &mut buf)?.then_some(buf))
}

fn key_of(number: u64) -> Vec<u8> {
    format!("key-{number:06}").into_bytes()
}

// one value under every key, so a read of them all shows whether the batch came back whole
fn batch_of(value: &[u8]) -> WriteBatch {
    let mut batch = WriteBatch::new();
    for number in 0..BATCH_KEYS {
        batch.put(PLAIN, &key_of(number), value);
    }
    batch
}

fn values(store: &Store) -> Vec<Option<Vec<u8>>> {
    (0..BATCH_KEYS).map(|number| read(store, PLAIN, &key_of(number)).expect("get")).collect()
}

fn assert_whole(store: &Store, value: Option<&[u8]>) {
    assert_eq!(values(store), vec![value.map(<[u8]>::to_vec); BATCH_KEYS as usize]);
}

// every group of the volume's rows files, each as its rows
fn groups(io: &SimulatedIo) -> Vec<Vec<(WritePlace, Vec<u8>)>> {
    let superblock = Superblock::read(io, Path::new(ROOT)).expect("read").expect("superblock");
    let mut groups = Vec::new();
    for path in io.list_directory(Path::new(ROOT)).expect("list") {
        if path.extension().is_none_or(|extension| extension != "rows") {
            continue;
        }
        let file = io.open_read_only(&path).expect("open");
        let mut bytes = vec![0; file.length().expect("length").0 as usize];
        file.read_exact_at(FileOffset(0), &mut bytes).expect("read");
        io.close_file(file);

        let mut at = 0;
        while let Some(group) = bytes.get(at..).and_then(|rest| parse_group(rest, superblock.record_key)) {
            groups.push(group.walk().map(|row: RowEntry| (row.place, row.key.to_vec())).collect());
            at += group.total_length.0 as usize;
        }
    }
    groups
}

// a later operation on a key wins, whichever way round
#[test]
fn in_order() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io);
    store.put(PLAIN, b"gone", b"value").expect("put");
    let mut batch = WriteBatch::new();
    batch.put(PLAIN, b"twice", b"first");
    batch.put(PLAIN, b"twice", b"second");
    batch.put(PLAIN, b"back", b"first");
    batch.delete(PLAIN, b"back");
    batch.delete(PLAIN, b"gone");
    batch.delete(PLAIN, b"again");
    batch.put(PLAIN, b"again", b"value");

    store.write_batch(&batch).expect("batch");

    assert_eq!(read(&store, PLAIN, b"twice").expect("get"), Some(b"second".to_vec()));
    assert_eq!(read(&store, PLAIN, b"back").expect("get"), None);
    assert_eq!(read(&store, PLAIN, b"gone").expect("get"), None);
    assert_eq!(read(&store, PLAIN, b"again").expect("get"), Some(b"value".to_vec()));
    store.close().expect("close");
}

// every codec and record form reads back, also after an open rebuilt the rows
#[test]
fn all_forms() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io);
    let large: Vec<u8> = (0..LARGE_VALUE_BYTES).map(|index| ((index * 31) >> 3) as u8).collect();
    let packed = vec![b'x'; LARGE_VALUE_BYTES];
    let mut batch = WriteBatch::new();
    batch.put(PLAIN, b"small", b"value");
    batch.put(PLAIN, b"large", &large);
    batch.put(PACKED, b"small", b"value");
    batch.put(PACKED, b"large", &packed);

    store.write_batch(&batch).expect("batch");

    store.flush().expect("flush");
    io.kill();
    drop(store);

    let store = open(&io);
    assert_eq!(read(&store, PLAIN, b"small").expect("get"), Some(b"value".to_vec()));
    assert_eq!(read(&store, PLAIN, b"large").expect("get"), Some(large));
    assert_eq!(read(&store, PACKED, b"small").expect("get"), Some(b"value".to_vec()));
    assert_eq!(read(&store, PACKED, b"large").expect("get"), Some(packed));
    store.close().expect("close");
}

// a batch is one claim: its rows take consecutive places and sit in one group
#[test]
fn one_claim() {
    let io = SimulatedIo::new(Seed(3));
    let store = open(&io);
    store.put(PLAIN, b"before", b"value").expect("put");
    store.flush().expect("flush");
    let mark = store.state.read_point();

    store.write_batch(&batch_of(b"value")).expect("batch");
    store.flush().expect("flush");

    let rows: Vec<Vec<(WritePlace, Vec<u8>)>> =
        groups(&io).into_iter().filter(|group| group.iter().any(|row| row.1 == key_of(0))).collect();
    assert_eq!(rows.len(), 1);
    let places: Vec<u64> = rows[0].iter().map(|row| row.0.0).collect();
    let expected: Vec<u64> = (1..=BATCH_KEYS).map(|step| mark.0 + step).collect();
    assert_eq!(places, expected);
    store.close().expect("close");
}

// a read beside a batch sees none of it until the mark passes all of it, though its rows are in the index
#[test]
fn seen_whole() {
    let io = SimulatedIo::new(Seed(4));
    let store = open(&io);
    let volume = &store.state.volumes[0];
    let request = ClaimRequest {
        bytes: ByteCount(0),
        rows: RowCount(1),
    };
    let earlier = store.state.claim(volume, &request).expect("claim");

    thread::scope(|scope| {
        let writer = scope.spawn(|| store.write_batch(&batch_of(b"value")));
        let last = key_of(BATCH_KEYS - 1);
        while store.state.index.lookup(PLAIN, &last, WritePlace(u64::MAX), volume.view()).row.is_none() {
            thread::yield_now();
        }

        assert_whole(&store, None);

        drop(earlier);
        writer.join().expect("join").expect("batch");
    });
    assert_whole(&store, Some(b"value"));
    store.close().expect("close");
}

// a crash at any step leaves all of a batch or none of it, and a batch that returned before a flush stays
#[test]
fn crash_whole() {
    for crash_at in 0..CRASH_POINTS {
        for is_power_cut in [false, true] {
            let io = SimulatedIo::new(Seed(crash_at));
            let store = open(&io);
            store.write_batch(&batch_of(b"older")).expect("batch");
            store.flush().expect("flush");

            io.arm_crash(OperationCount(crash_at));
            let written = store.write_batch(&batch_of(b"newer"));
            let flushed = written.is_ok() && store.flush().is_ok() && !io.is_crashed();
            if is_power_cut {
                io.power_cut();
            } else {
                io.kill();
            }
            drop(store);

            let store = open(&io);
            let found = values(&store);
            assert!(found.iter().all(|value| *value == found[0]), "crash at {crash_at}: {found:?}");
            let is_expected = found[0] == Some(b"newer".to_vec()) || (!flushed && found[0] == Some(b"older".to_vec()));
            assert!(is_expected, "crash at {crash_at}: {found:?}");
            store.close().expect("close");
        }
    }
}

// a record that fails to write leaves nothing of the batch for a read, then or after a reopen
#[test]
fn failed_write() {
    let io = SimulatedIo::new(Seed(5));
    let store = open(&io);
    store.write_batch(&batch_of(b"older")).expect("batch");
    io.fail_writes(OperationCount(1));

    assert!(store.write_batch(&batch_of(b"newer")).is_err());

    io.fail_writes(OperationCount(0));
    assert_whole(&store, Some(b"older"));
    store.close().expect("close");
    drop(store);
    let store = open(&io);
    assert_whole(&store, Some(b"older"));
    store.close().expect("close");
}

// a batch across volumes is refused whole
#[test]
fn across_volumes() {
    let io = SimulatedIo::new(Seed(6));
    let store = open(&io);
    let mut batch = WriteBatch::new();
    batch.put(PLAIN, b"key", b"value");
    batch.put(OTHER_VOLUME, b"key", b"value");

    assert!(matches!(store.write_batch(&batch), Err(StoreError::BatchAcrossVolumes)));

    assert_eq!(read(&store, PLAIN, b"key").expect("get"), None);
    store.close().expect("close");
}

// rows past one group are refused whole, and a batch at the limit lands
#[test]
fn group_limit() {
    let io = SimulatedIo::new(Seed(7));
    let store = open(&io);
    let mut batch = WriteBatch::new();
    let mut number = 0;
    while batch_row_bytes(&batch) + ROW_BYTES <= MOST_GROUP_ROW_BYTES {
        batch.delete(PLAIN, &key_of(number));
        number += 1;
    }
    store.put(PLAIN, &key_of(0), b"value").expect("put");
    store.write_batch(&batch).expect("batch");
    assert_eq!(read(&store, PLAIN, &key_of(0)).expect("get"), None);

    store.put(PLAIN, &key_of(0), b"value").expect("put");
    batch.delete(PLAIN, &key_of(number));

    assert!(matches!(store.write_batch(&batch), Err(StoreError::BatchTooLarge { .. })));

    assert_eq!(read(&store, PLAIN, &key_of(0)).expect("get"), Some(b"value".to_vec()));
    store.close().expect("close");
}

const ROW_BYTES: usize = reel::rows::row_group::ROW_HEADER_BYTES + "key-000000".len();

fn batch_row_bytes(batch: &WriteBatch) -> usize {
    batch.len() * ROW_BYTES
}

// values past a segment are refused whole
#[test]
fn segment_limit() {
    let io = SimulatedIo::new(Seed(8));
    let store = open(&io);
    let value = vec![1; store.state.volumes[0].segment_bytes.0 as usize / 2 + 1];
    let mut batch = WriteBatch::new();
    batch.put(PLAIN, b"first", &value);
    batch.put(PLAIN, b"second", &value);

    assert!(matches!(store.write_batch(&batch), Err(StoreError::BatchTooLarge { .. })));

    assert_eq!(read(&store, PLAIN, b"first").expect("get"), None);
    store.close().expect("close");
}

// an empty batch changes nothing and takes no place
#[test]
fn empty_batch() {
    let io = SimulatedIo::new(Seed(9));
    let store = open(&io);
    let mark = store.state.read_point();

    store.write_batch(&WriteBatch::new()).expect("batch");

    assert_eq!(store.state.read_point(), mark);
    store.close().expect("close");
}

// batches beside puts in the same segment keep to their rows when the index fills and seals make room
#[test]
fn seals_make_room() {
    let io = SimulatedIo::new(Seed(10));
    let store = open(&io);
    for round in 0..ROOM_ROWS / FILLER_ROWS {
        let mut batch = WriteBatch::new();
        for number in 0..FILLER_ROWS {
            batch.put(PLAIN, &key_of(round * FILLER_ROWS + number), b"value");
            store.put(PACKED, &key_of(round * FILLER_ROWS + number), b"value").expect("put");
        }
        store.write_batch(&batch).expect("batch");
        store.seal_now();
        for _step in 0..FILLER_ROWS {
            if io.step_any().is_none() {
                break;
            }
        }
    }
    for number in 0..ROOM_ROWS / FILLER_ROWS * FILLER_ROWS {
        assert_eq!(read(&store, PLAIN, &key_of(number)).expect("get"), Some(b"value".to_vec()));
    }
    store.close().expect("close");
}
