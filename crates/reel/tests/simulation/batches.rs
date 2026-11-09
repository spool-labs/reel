//! A batch lands whole or not at all: to a read beside it and after any crash

use std::io::IoSlice;
use std::path::{Path, PathBuf};
use std::thread;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::engine::batch_path::MOST_BUFFERED_ROW_BYTES;
use reel::engine::open_segment::{SegmentPhase, data_path};
use reel::engine::recovery::walk_rows;
use reel::engine::write_path::{ClaimRequest, LandOutcome};
use reel::guard::hold;
use reel::index::key_hash::hash_key;
use reel::index::open_slots::OpenSlot;
use reel::index::write_index::shard_of;
use reel::index::write_row::WriteRow;
use reel::io::{Io, IoFile};
use reel::lifetime::MarkRange;
use reel::record::codec::Codec;
use reel::record::record_check::RecordKey;
use reel::row_kind::RowKind;
use reel::rows::row_group::{
    GROUP_OVERHEAD_BYTES, GroupHead, ROW_HEADER_BYTES, ROW_KIND_AT, RowEntry, RowGroupBuffer, parse_group,
};
use reel::superblock::Superblock;
use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, Nanoseconds, RowCount, VolumeId, WritePlace};
use reel_api::write_batch::WriteBatch;
use reel_sim::file_image::BitMask;
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_disk::OperationCount;
use reel_sim::simulated_io::SimulatedIo;
use xxhash_rust::xxh3::xxh3_64_with_seed;

const ROOT: &str = "/disk/batch-meta";
const SECOND_ROOT: &str = "/disk/batch-bulk";
const PLAIN: ColumnId = ColumnId(1);
const PACKED: ColumnId = ColumnId(2);
const OTHER_VOLUME: ColumnId = ColumnId(3);
const BUDGET: ByteCount = ByteCount(176 * 1024 * 1024);
const BATCH_KEYS: u64 = 5;
const CRASH_POINTS: u64 = 12;
const LARGE_VALUE_BYTES: usize = 200_000;
const FILLER_ROWS: u64 = 300;
const ROOM_ROWS: u64 = 2_000;
const LONG_ROWS: u64 = 18_000;
const LONG_CRASH_POINTS: u64 = 6;
const LONG_PIECES: u64 = 3;
const GATHERED_ROWS: u64 = 100;
const SMALL_VALUE: usize = 100;
const SPLIT_ROWS: u64 = 200;
const SPLIT_VALUE: usize = 5_000;
const LEFT_ON_DISK: u64 = 4_096;
const SMALL_DISK: ByteCount = ByteCount(288 * 1024 * 1024);
const PAST_FIRST_PIECE: u64 = 300_000;
const LONG_READ_POINTS: u64 = 40;
const GROUP_BYTES: u64 = 4_096;
const SMALL_BUFFER: usize = 16;
const UNKNOWN_KIND: u8 = 0xEE;
const GROUP_DEADLINE: Nanoseconds = Nanoseconds(100_000_000);
const GROUP_TICK: Nanoseconds = Nanoseconds(1_000_000);

type Store = ReelStore<SimulatedIo>;

fn config() -> StoreConfig {
    let volume = |name: &str, root: &str| VolumeConfig {
        name: name.to_string(),
        roots: vec![PathBuf::from(root)],
        sync: SyncPolicy::Never,
        device: Default::default(),
    };
    let column = |id: ColumnId, name: &str, codec: Codec, volume: u8| ColumnConfig {
        id,
        name: name.to_string(),
        key_shape: KeyShape::Variable { most: 64 },
        codec,
        volume: VolumeId(volume),
        complete_rows: None,
        least_value: None,
    };
    StoreConfig {
        memory_budget: BUDGET,
        threads: reel::config::NO_WORKERS,
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
    let mark = store.state.read_point(&store.state.volumes[0]);

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
    io.keep_real_time();
    let store = open(&io);
    let volume = &store.state.volumes[0];
    let request = ClaimRequest {
        bytes: ByteCount(0),
        rows: RowCount(1),
        is_alone: false,
        has_records: false,
        covers: 0,
        mark: None,
        has_deletes: false,
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

// rows past what a segment takes are refused whole
#[test]
fn row_limit() {
    let io = SimulatedIo::new(Seed(7));
    let store = open(&io);
    store.put(PLAIN, &key_of(0), b"value").expect("put");
    let mut batch = WriteBatch::new();
    for number in 0..=store.state.volumes[0].segment_rows.0 {
        batch.delete(PLAIN, &key_of(number));
    }

    assert!(matches!(store.write_batch(&batch), Err(StoreError::BatchTooManyRows { .. })));

    assert_eq!(read(&store, PLAIN, &key_of(0)).expect("get"), Some(b"value".to_vec()));
    store.close().expect("close");
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
    let mark = store.state.read_point(&store.state.volumes[0]);

    store.write_batch(&WriteBatch::new()).expect("batch");

    assert_eq!(store.state.read_point(&store.state.volumes[0]), mark);
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

// a batch of graves with rows past two buffers, so its group goes out in three pieces
fn long_batch(value: Option<&[u8]>) -> WriteBatch {
    let mut batch = WriteBatch::new();
    for number in 0..LONG_ROWS {
        match value {
            Some(value) => batch.put(PLAIN, &key_of(number), value),
            None => batch.delete(PLAIN, &key_of(number)),
        }
    }
    batch
}

fn long_values(store: &Store) -> Vec<Option<Vec<u8>>> {
    (0..LONG_ROWS).map(|number| read(store, PLAIN, &key_of(number)).expect("get")).collect()
}

fn assert_all(found: &[Option<Vec<u8>>], value: Option<&[u8]>) {
    assert!(found.iter().all(|found| found.as_deref() == value), "{:?}", found.iter().find(|found| found.as_deref() != value));
}

// a group past the buffer is one group with consecutive places, and a walk in pieces brings every row back
#[test]
fn long_group() {
    let io = SimulatedIo::new(Seed(11));
    let store = open(&io);
    store.put(PLAIN, b"before", b"value").expect("put");
    store.flush().expect("flush");
    let mark = store.state.read_point(&store.state.volumes[0]);

    store.write_batch(&long_batch(Some(b"value"))).expect("batch");

    assert_all(&long_values(&store), Some(b"value"));
    store.flush().expect("flush");
    let rows: Vec<Vec<(WritePlace, Vec<u8>)>> =
        groups(&io).into_iter().filter(|group| group.iter().any(|row| row.1 == key_of(0))).collect();
    assert_eq!(rows.len(), 1);
    let places: Vec<u64> = rows[0].iter().map(|row| row.0.0).collect();
    let expected: Vec<u64> = (1..=LONG_ROWS).map(|step| mark.0 + step).collect();
    assert_eq!(places, expected);

    io.kill();
    drop(store);
    let store = open(&io);
    assert_all(&long_values(&store), Some(b"value"));
    assert_eq!(read(&store, PLAIN, b"before").expect("get"), Some(b"value".to_vec()));
    store.close().expect("close");
}

// a crash at any step of a long group leaves all of it or none, and a later reopen agrees
#[test]
fn long_crash() {
    for crash_at in 0..LONG_CRASH_POINTS {
        for is_power_cut in [false, true] {
            let io = SimulatedIo::new(Seed(crash_at));
            let store = open(&io);
            store.write_batch(&long_batch(Some(b"older"))).expect("batch");
            store.flush().expect("flush");

            io.arm_crash(OperationCount(crash_at));
            let written = store.write_batch(&long_batch(None));
            let flushed = written.is_ok() && store.flush().is_ok() && !io.is_crashed();
            if is_power_cut {
                io.power_cut();
            } else {
                io.kill();
            }
            drop(store);

            let store = open(&io);
            let found = long_values(&store);
            assert!(found.iter().all(|value| *value == found[0]), "crash at {crash_at}");
            assert!(found[0].is_none() || !flushed, "crash at {crash_at}");
            store.close().expect("close");
        }
    }
}

// a rows write that fails at any piece or at the closing sum shows nothing, stops the volume as the row job would,
// and leaves no group for open to bring back
#[test]
fn long_failure() {
    for writes_before_failure in 0..=LONG_PIECES {
        let io = SimulatedIo::new(Seed(12));
        let store = open(&io);
        store.write_batch(&long_batch(Some(b"older"))).expect("batch");
        store.flush().expect("flush");

        io.fail_writes_after(OperationCount(writes_before_failure));
        let written = store.write_batch(&long_batch(None));

        assert!(matches!(written, Err(StoreError::Io { action: "writing rows", .. })), "{writes_before_failure} writes");
        assert_all(&long_values(&store), Some(b"older"));
        assert!(matches!(store.put(PLAIN, b"after", b"value"), Err(StoreError::VolumeFailed { .. })));
        io.kill();
        drop(store);

        let store = open(&io);
        assert_all(&long_values(&store), Some(b"older"));
        store.close().expect("close");
    }
}

// a full disk mid-group is the volume's own error, as on the row job's path, and the volume goes on
#[test]
fn long_full() {
    let io = SimulatedIo::new(Seed(18));
    let store = open(&io);
    store.write_batch(&long_batch(Some(b"older"))).expect("batch");
    store.flush().expect("flush");

    io.fill_writes(OperationCount(1));
    let written = store.write_batch(&long_batch(None));

    assert!(matches!(written, Err(StoreError::VolumeFull { .. })));
    assert_all(&long_values(&store), Some(b"older"));
    store.put(PLAIN, b"after", b"value").expect("put");
    store.flush().expect("flush");
    store.close().expect("close");
}

// a group its root has no room for is refused before a piece goes out
#[test]
fn long_room() {
    let io = SimulatedIo::new(Seed(19));
    io.add_disk(Path::new(ROOT), SMALL_DISK);
    let store = open(&io);
    store.put(PLAIN, b"first", b"value").expect("put");
    let free = io.free_space(Path::new(ROOT)).expect("free");
    let filler = io.create_file(&Path::new(ROOT).join("filler"), ByteCount(free.0 - LEFT_ON_DISK)).expect("create");
    let before = io.read_totals();

    assert!(matches!(store.write_batch(&long_batch(None)), Err(StoreError::VolumeFull { .. })));

    assert_eq!(io.read_totals(), before);
    io.close_file(filler);
    store.close().expect("close");
}

// rows of a long batch seal like any others
#[test]
fn long_seal() {
    let io = SimulatedIo::new(Seed(13));
    let store = open(&io);
    store.write_batch(&long_batch(Some(b"value"))).expect("batch");
    let before = store.state.volumes[0].table.segment_count();
    store.seal_now();
    while store.state.volumes[0].table.segment_count() == before {
        assert!(io.step_any().is_some());
    }

    assert_all(&long_values(&store), Some(b"value"));
    store.close().expect("close");
}

// a batch shares its group with puts beside it, and a record the crash took drops every row of the group
#[test]
fn shared_group() {
    let io = SimulatedIo::new(Seed(14));
    let store = open(&io);
    store.write_batch(&batch_of(b"older")).expect("batch");
    store.put(PLAIN, b"beside", b"older").expect("put");
    store.flush().expect("flush");

    store.put(PLAIN, b"beside", b"newer").expect("put");
    store.write_batch(&batch_of(b"newer")).expect("batch");
    store.put(PLAIN, b"after", b"newer").expect("put");
    let (slot, _data) = open_data(&store);
    store.state.write_out(slot).expect("write out");
    let (data, lost) = last_record(&io, &key_of(2));
    io.kill();
    drop(store);
    io.rot(&data, lost, BitMask(1)).expect("rot");

    let store = open(&io);

    assert_whole(&store, Some(b"older"));
    assert_eq!(read(&store, PLAIN, b"beside").expect("get"), Some(b"older".to_vec()));
    assert_eq!(read(&store, PLAIN, b"after").expect("get"), None);
    store.close().expect("close");
}

// the slot and data file of the one open segment
fn open_data(store: &Store) -> (OpenSlot, PathBuf) {
    let claims = hold(&store.state.volumes[0].claims);
    let segment = &claims.open[0];
    (segment.slot, data_path(Path::new(ROOT), segment.number).to_path_buf())
}

// where the newest row of a key says its record sits, in which data file
fn last_record(io: &SimulatedIo, key: &[u8]) -> (PathBuf, FileOffset) {
    let superblock = Superblock::read(io, Path::new(ROOT)).expect("read").expect("superblock");
    let mut found = None;
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
            for row in group.walk() {
                if row.key == key {
                    found = Some((path.with_extension("data"), row.offset));
                }
            }
            at += group.total_length.0 as usize;
        }
    }
    found.expect("a row")
}

// a long batch has its segment alone: a claim beside it opens another, and the segment takes claims again after
#[test]
fn alone_claim() {
    let io = SimulatedIo::new(Seed(15));
    let store = open(&io);
    let volume = &store.state.volumes[0];
    let bytes = ByteCount(1);
    let long = ClaimRequest {
        bytes,
        rows: RowCount(1),
        is_alone: true,
        has_records: true,
        covers: 0,
        mark: None,
        has_deletes: false,
    };
    let short = ClaimRequest { is_alone: false, ..long };

    let alone = store.state.claim(volume, &long).expect("claim");
    let beside = store.state.claim(volume, &short).expect("claim");
    assert_ne!(alone.segment, beside.segment);
    let segment = alone.segment;
    drop(beside);
    drop(alone);

    let open: Vec<_> = hold(&volume.claims).open.iter().map(|open| (open.number, open.phase)).collect();
    assert!(open.contains(&(segment, SegmentPhase::Taking)));
    store.close().expect("close");
}

// rows of a claim already in flight on the segment go out ahead of a long group, so they never wait behind its turn
#[test]
fn rows_go_first() {
    let io = SimulatedIo::new(Seed(16));
    io.keep_real_time();
    let store = open(&io);
    store.put(PLAIN, b"first", b"value").expect("put");
    let volume = &store.state.volumes[0];
    let request = ClaimRequest {
        bytes: ByteCount(0),
        rows: RowCount(1),
        is_alone: false,
        has_records: false,
        covers: 0,
        mark: None,
        has_deletes: false,
    };
    let earlier = store.state.claim(volume, &request).expect("claim");
    let place = earlier.first_place();

    thread::scope(|scope| {
        let writer = scope.spawn(|| store.write_batch(&long_batch(None)));
        while !writer.is_finished() && !hold(&volume.claims).open.iter().any(|open| open.phase == SegmentPhase::Alone) {
            thread::yield_now();
        }
        let row = WriteRow {
            place,
            offset: earlier.offset,
            length: ByteCount(0),
            kind: RowKind::Grave,
            slot: earlier.slot,
            segment: earlier.segment,
            has_newer_sealed: false,
        };
        assert_eq!(store.state.land_row(PLAIN, b"beside", &row).expect("land"), LandOutcome::Landed);
        earlier.land();
        writer.join().expect("join").expect("batch");
    });
    store.flush().expect("flush");

    let rows = groups(&io);
    let beside = rows.iter().position(|group| group.iter().any(|row| row.1 == b"beside")).expect("a group");
    let long = rows.iter().position(|group| group.iter().any(|row| row.1 == key_of(0))).expect("a group");
    assert!(beside < long);
    store.close().expect("close");
}

// a put beside a long batch that holds its writing turn reaches the kernel inside the rule's 100 ms, though it can't
// return until the mark passes the batch, and rows already waiting on the batch's own slot hold nothing back
#[test]
fn turn_spares_puts() {
    let io = SimulatedIo::new(Seed(17));
    io.keep_real_time();
    let store = open(&io);
    store.put(PLAIN, b"before", b"value").expect("put");
    let volume = &store.state.volumes[0];
    let long = ClaimRequest {
        bytes: ByteCount(1),
        rows: RowCount(1),
        is_alone: true,
        has_records: true,
        covers: 0,
        mark: None,
        has_deletes: false,
    };
    let claim = store.state.claim(volume, &long).expect("claim");
    let slot = claim.slot;
    store.state.take_writing_turn(slot);

    thread::scope(|scope| {
        let writer = scope.spawn(|| store.put(PLAIN, b"beside", b"value"));
        // the row is in the index once the put has landed it, and the rule counts from there
        while store.state.index.lookup(PLAIN, b"beside", WritePlace(u64::MAX), volume.view()).row.is_none() {
            thread::yield_now();
        }
        let start = io.clock_reading();
        while !groups(&io).iter().flatten().any(|row| row.1 == b"beside") {
            assert!(io.clock_reading().0 - start.0 <= GROUP_DEADLINE.0, "the put's group waited behind the turn");
            io.step_any();
            io.advance_clock(GROUP_TICK);
        }

        hold(&store.state.queues[slot.0 as usize]).is_writing = false;
        drop(claim);
        writer.join().expect("join").expect("put");
    });
    store.close().expect("close");
}

// rows that exactly fill the buffer and rows one byte past it each land as one group, whichever way they went out
#[test]
fn path_boundary() {
    let row_bytes = ROW_HEADER_BYTES + key_of(0).len();
    let plain_rows = (MOST_BUFFERED_ROW_BYTES / row_bytes - 1) as u64;
    let exact_key = row_bytes + MOST_BUFFERED_ROW_BYTES % row_bytes - ROW_HEADER_BYTES;
    for key_length in [exact_key, exact_key + 1] {
        let io = SimulatedIo::new(Seed(20));
        let store = open(&io);
        let last = vec![b'p'; key_length];
        let mut batch = WriteBatch::new();
        for number in 0..plain_rows {
            batch.put(PLAIN, &key_of(number), b"value");
        }
        batch.put(PLAIN, &last, b"value");

        store.write_batch(&batch).expect("batch");
        store.flush().expect("flush");

        let rows = groups(&io);
        let group = rows.iter().find(|group| group.iter().any(|row| row.1 == last)).expect("a group");
        assert_eq!(group.len() as u64, plain_rows + 1, "key of {key_length} bytes");
        io.kill();
        drop(store);
        let store = open(&io);
        assert_eq!(read(&store, PLAIN, &last).expect("get"), Some(b"value".to_vec()));
        assert_eq!(read(&store, PLAIN, &key_of(0)).expect("get"), Some(b"value".to_vec()));
        store.close().expect("close");
    }
}

// a batch whose rows all fall in one shard is refused whole when that shard could never hold them
#[test]
fn shard_limit() {
    let io = SimulatedIo::new(Seed(21));
    let store = open(&io);
    let limit = store.state.index.shards.read(0).node_row_limit();
    let mut batch = WriteBatch::new();
    let mut number = 0;
    while (batch.len() as u64) <= limit {
        if shard_of(hash_key(PLAIN, &key_of(number))) == 0 {
            batch.delete(PLAIN, &key_of(number));
        }
        number += 1;
    }

    assert!(matches!(store.write_batch(&batch), Err(StoreError::BatchTooLarge { .. })));

    store.close().expect("close");
}

// a later operation on a key wins in a long batch too, also after open walks the group in pieces
#[test]
fn long_twice() {
    let io = SimulatedIo::new(Seed(22));
    let store = open(&io);
    let mut batch = long_batch(Some(b"first"));
    batch.put(PLAIN, &key_of(0), b"second");
    batch.delete(PLAIN, &key_of(1));
    batch.delete(PLAIN, &key_of(LONG_ROWS - 1));
    batch.put(PLAIN, &key_of(LONG_ROWS - 1), b"second");

    store.write_batch(&batch).expect("batch");

    assert_twice(&store);
    store.flush().expect("flush");
    io.kill();
    drop(store);
    let store = open(&io);
    assert_twice(&store);
    store.close().expect("close");
}

fn assert_twice(store: &Store) {
    assert_eq!(read(store, PLAIN, &key_of(0)).expect("get"), Some(b"second".to_vec()));
    assert_eq!(read(store, PLAIN, &key_of(1)).expect("get"), None);
    assert_eq!(read(store, PLAIN, &key_of(2)).expect("get"), Some(b"first".to_vec()));
    assert_eq!(read(store, PLAIN, &key_of(LONG_ROWS - 1)).expect("get"), Some(b"second".to_vec()));
}

// a crash that keeps the rows and takes one record drops the long group whole
#[test]
fn long_lost_record() {
    let io = SimulatedIo::new(Seed(23));
    let store = open(&io);
    store.write_batch(&long_batch(Some(b"older"))).expect("batch");
    store.flush().expect("flush");

    store.write_batch(&long_batch(Some(b"newer"))).expect("batch");
    let (data, lost) = last_record(&io, &key_of(LONG_ROWS - 1));
    io.kill();
    drop(store);
    io.rot(&data, lost, BitMask(1)).expect("rot");

    let store = open(&io);

    assert_all(&long_values(&store), Some(b"older"));
    store.close().expect("close");
}

fn long_group_start(io: &SimulatedIo, rows: &Path) -> u64 {
    let superblock = Superblock::read(io, Path::new(ROOT)).expect("read").expect("superblock");
    let file = io.open_read_only(rows).expect("open");
    let mut bytes = vec![0; file.length().expect("length").0 as usize];
    file.read_exact_at(FileOffset(0), &mut bytes).expect("read");
    io.close_file(file);
    let mut at = 0;
    while let Some(group) = bytes.get(at..).and_then(|rest| parse_group(rest, superblock.record_key)) {
        if group.row_count.0 >= LONG_ROWS {
            return at as u64;
        }
        at += group.total_length.0 as usize;
    }
    panic!("no long group");
}

fn rows_path_of(io: &SimulatedIo) -> PathBuf {
    let listing = io.list_directory(Path::new(ROOT)).expect("list");
    listing.into_iter().find(|path| path.extension().is_some_and(|extension| extension == "rows")).expect("a rows file")
}

// rot inside a long group a later mark covers is damage, and never the older value
#[test]
fn long_rot() {
    let io = SimulatedIo::new(Seed(24));
    let store = open(&io);
    store.write_batch(&batch_of(b"older")).expect("batch");
    store.flush().expect("flush");
    store.write_batch(&long_batch(Some(b"newer"))).expect("batch");
    store.flush().expect("flush");
    io.kill();
    drop(store);
    let rows = rows_path_of(&io);
    let start = long_group_start(&io, &rows);
    io.rot(&rows, FileOffset(start + PAST_FIRST_PIECE), BitMask(1)).expect("rot");

    let store = open(&io);

    assert_eq!(store.damaged_segments(), 1);
    assert!(matches!(read(&store, PLAIN, &key_of(0)), Err(StoreError::Corruption { .. })));
    store.close().expect("close");
}

// a bad sector in a long group no sync covered is damage: a device error is never a torn end
#[test]
fn long_bad_sector() {
    let io = SimulatedIo::new(Seed(25));
    let store = open(&io);
    store.write_batch(&batch_of(b"older")).expect("batch");
    store.flush().expect("flush");
    store.write_batch(&long_batch(Some(b"newer"))).expect("batch");
    io.kill();
    drop(store);
    let rows = rows_path_of(&io);
    let start = long_group_start(&io, &rows);
    io.bad_sector(&rows, FileOffset(start + PAST_FIRST_PIECE)).expect("bad");

    let store = open(&io);

    assert_eq!(store.damaged_segments(), 1);
    assert!(matches!(read(&store, PLAIN, &key_of(0)), Err(StoreError::Corruption { .. })));
    store.close().expect("close");
}

// a device error at any read of an open shows a long group whole or refuses it, and never fails on a later piece
#[test]
fn long_read() {
    for reads in 0..LONG_READ_POINTS {
        let io = SimulatedIo::new(Seed(26));
        let store = open(&io);
        store.write_batch(&long_batch(Some(b"value"))).expect("batch");
        store.flush().expect("flush");
        io.kill();
        drop(store);

        io.fail_reads_after(OperationCount(reads));
        let Ok(store) = ReelStore::open(io.clone(), &config()) else {
            continue;
        };
        let answers: Vec<_> = (0..LONG_ROWS).map(|number| read(&store, PLAIN, &key_of(number))).collect();
        let shown = answers.iter().filter(|answer| matches!(answer, Ok(Some(_)))).count() as u64;
        assert!(shown == 0 || shown == LONG_ROWS || answers.iter().any(Result::is_err), "{reads} reads: {shown} rows");
        store.close().expect("close");
    }
}

// a batch past the room left in a segment's rows takes a new segment, so no segment grows past what open can rebuild
#[test]
fn long_new_segment() {
    let io = SimulatedIo::new(Seed(27));
    let store = open(&io);
    let volume = &store.state.volumes[0];
    let limit = volume.segment_rows.0;
    let mut first = WriteBatch::new();
    for number in 0..limit - 1 {
        first.delete(PACKED, &key_of(number));
    }
    store.write_batch(&first).expect("batch");

    store.write_batch(&long_batch(Some(b"value"))).expect("batch");

    let open_rows: Vec<u64> = hold(&volume.claims).open.iter().map(|segment| segment.row_count.0).collect();
    assert!(open_rows.iter().all(|rows| *rows <= limit), "{open_rows:?}");
    assert_all(&long_values(&store), Some(b"value"));
    store.close().expect("close");
}

// a seal asked for while a long batch has its segment waits for the batch, and every row comes through
#[test]
fn long_seal_beside() {
    let io = SimulatedIo::new(Seed(28));
    io.keep_real_time();
    let store = open(&io);
    let volume = &store.state.volumes[0];
    let request = ClaimRequest {
        bytes: ByteCount(0),
        rows: RowCount(1),
        is_alone: false,
        has_records: false,
        covers: 0,
        mark: None,
        has_deletes: false,
    };
    let earlier = store.state.claim(volume, &request).expect("claim");

    thread::scope(|scope| {
        let writer = scope.spawn(|| store.write_batch(&long_batch(Some(b"value"))));
        while !writer.is_finished() && !hold(&volume.claims).open.iter().any(|open| open.phase == SegmentPhase::Alone) {
            thread::yield_now();
        }
        store.seal_now();
        drop(earlier);
        writer.join().expect("join").expect("batch");
    });

    let phases: Vec<SegmentPhase> = hold(&volume.claims).open.iter().map(|open| open.phase).collect();
    assert!(!phases.contains(&SegmentPhase::Alone));
    while io.step_any().is_some() {}
    assert_all(&long_values(&store), Some(b"value"));
    store.close().expect("close");
}

// a long batch sealed and reopened reads from its footer
#[test]
fn long_reopen() {
    let io = SimulatedIo::new(Seed(29));
    let store = open(&io);
    store.write_batch(&long_batch(Some(b"value"))).expect("batch");
    store.close().expect("close");
    drop(store);

    let store = open(&io);

    assert_all(&long_values(&store), Some(b"value"));
    store.close().expect("close");
}

// a group that passes its check with a row no walk can read is an error, and never a walk that spins
#[test]
fn unread_row() {
    let io = SimulatedIo::new(Seed(30));
    let path = Path::new(ROOT).join("walk");
    let record_key = RecordKey(1);
    let mut row = vec![0; ROW_HEADER_BYTES];
    row[ROW_KIND_AT] = UNKNOWN_KIND;
    let total = GROUP_OVERHEAD_BYTES + row.len();
    let mut stage = RowGroupBuffer::new(ByteCount(GROUP_BYTES));
    let head = GroupHead {
        row_count: RowCount(1),
        total_length: ByteCount(total as u64),
        synced_through: FileOffset(0),
        marks: MarkRange::NONE,
    };
    stage.set_head(&head);
    let mut bytes = stage.with_head().to_vec();
    bytes.extend_from_slice(&row);
    let sum = xxh3_64_with_seed(&bytes, record_key.0);
    bytes.extend_from_slice(&sum.to_le_bytes());
    io.create_directory(Path::new(ROOT)).expect("create");
    let file = io.create_file(&path, ByteCount(0)).expect("create");
    file.write_at(FileOffset(0), &[IoSlice::new(&bytes)]).expect("write");

    let mut buffer = [0; SMALL_BUFFER];
    let walked = walk_rows(&file, FileOffset(0), &head, &mut buffer, |_row| Ok(true));

    assert!(walked.is_err());
    io.close_file(file);
}

// small records share a write call, and a chunked record between them goes out on its own at the right offset
#[test]
fn few_writes() {
    let io = SimulatedIo::new(Seed(31));
    let store = open(&io);
    store.put(PLAIN, b"first", b"value").expect("put");
    let mut batch = WriteBatch::new();
    for number in 0..GATHERED_ROWS {
        batch.put(PLAIN, &key_of(number), &[number as u8; SMALL_VALUE]);
    }

    let before = io.write_calls();
    store.write_batch(&batch).expect("batch");
    let calls = io.write_calls().0 - before.0;

    assert_eq!(calls, 1);
    for number in 0..GATHERED_ROWS {
        assert_eq!(read(&store, PLAIN, &key_of(number)).expect("get"), Some(vec![number as u8; SMALL_VALUE]));
    }
    store.close().expect("close");
}

// records past one buffer, and a chunked one between small ones, keep their offsets across the write calls
#[test]
fn split_writes() {
    let io = SimulatedIo::new(Seed(32));
    let store = open(&io);
    let large: Vec<u8> = (0..LARGE_VALUE_BYTES).map(|index| ((index * 7) >> 2) as u8).collect();
    let mut batch = WriteBatch::new();
    for number in 0..SPLIT_ROWS {
        batch.put(PLAIN, &key_of(number), &[number as u8; SPLIT_VALUE]);
        if number == SPLIT_ROWS / 2 {
            batch.put(PLAIN, b"large", &large);
        }
    }

    store.write_batch(&batch).expect("batch");
    store.flush().expect("flush");
    io.kill();
    drop(store);

    let store = open(&io);
    assert_eq!(read(&store, PLAIN, b"large").expect("get"), Some(large));
    for number in 0..SPLIT_ROWS {
        assert_eq!(read(&store, PLAIN, &key_of(number)).expect("get"), Some(vec![number as u8; SPLIT_VALUE]));
    }
    store.close().expect("close");
}
