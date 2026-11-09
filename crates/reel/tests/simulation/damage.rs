//! Rot in a sealed segment: one copy lost costs nothing, and reads past a damaged range refuse to answer

use std::collections::BTreeMap;
use std::io::IoSlice;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::open_segment::{SegmentPhase, data_path};
use reel::engine::reel_store::ReelStore;
use reel::guard::hold;
use reel_sim::file_image::BitMask;
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_disk::OperationCount;
use reel_sim::simulated_io::SimulatedIo;
use reel::io::{Io, IoFile, IoTask};
use reel::record::codec::Codec;
use reel::record::record_check::Checksum;
use reel::segment::damage_list::{DamagedRange, MOST_DAMAGED_SPANS};
use reel::segment::footer_reader::FooterReader;
use reel::segment::row_block::{BLOCK_HEADER_BYTES, BlockHeader, read_block_header};
use reel::segment::segment_tail::{SECOND_TAIL_DISTANCE, SegmentNumber, TailHead};
use reel::segment::summary::{
    FORWARD_UNKNOWN, LostRows, ROW_HEAD_BYTES, SUMMARY_HEADER_BYTES, SegmentFacts, SummaryPaths, SummaryRows, read_row, read_summary,
    copy_ends, read_copy, write_copy, write_summary,
};
use reel::rows::row_group::parse_group;
use reel::superblock::{Superblock, SuperblockPaths};
use reel_api::column_id::ColumnId;
use reel_api::direction::Direction;
use reel_api::scan::ScanStep;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, Nanoseconds, VolumeId, WritePlace};
use reel_api::value_list::{ValueAnswer, ValueList};

const ROOT: &str = "/disk/damage";
const ORDERED: ColumnId = ColumnId(1);
const RANDOM: ColumnId = ColumnId(2);
const COLUMNS: [ColumnId; 2] = [ORDERED, RANDOM];
const BUDGET: ByteCount = ByteCount(63 * 1024 * 1024);
const SETTLE_STEPS: u64 = 2_000_000;
const SETTLE_TICK: Nanoseconds = Nanoseconds(1_000_000);
const ROW_PERIOD: Nanoseconds = Nanoseconds(60_000_000);
const KEY_COUNT: u64 = 300;
const REBUILD_KEYS: u64 = 20_000;
const REBUILD_LOST_BLOCKS: u64 = 6;
const REBUILD_BUDGET: ByteCount = ByteCount(192 * 1024 * 1024);
const SPREAD: u64 = 0x9E37_79B9_7F4A_7C15;
const FLIP: BitMask = BitMask(1);
const OLDEST: usize = 0;
const MIDDLE: usize = 1;
const NEWEST: usize = 2;
const SECTOR_BYTES: u64 = 512;
const SECTOR_KEYS: u64 = 40;
const MARK_AT: u64 = 9;
const TRAILER_MARK_BACK: u64 = 23;
const SAVE_CRASH_POINTS: u64 = 16;
const SEGMENTS: usize = 3;
const SEAL_CRASH_POINTS: u64 = 24;
const BUSY_WRITES: u32 = 1_000;
const STALE_TAIL_SEED: u64 = 0;
const STALE_TAIL_SECOND_POINTS: u64 = 3;
const SEAL_STEPS: u64 = 1_000;
const CLEAR_CRASH_POINTS: u64 = 24;
const SEAL_DATA_SYNCS: u64 = 6;
const LONG_KEY_BYTES: usize = 100;
const SECTOR_ROWS: u64 = 40;
const GROUPS_BACK: u64 = 150;
const WIDE_COLUMNS: u8 = 200;
const WIDE_SEGMENTS: u64 = 90;
const WIDE_BUDGET: ByteCount = ByteCount(145 * 1024 * 1024);
// what the summary plans at the least budget, spans included
const PLANNED_ROOM_AT_LEAST: u64 = 1_400_000;
const LEAST_BUDGET: ByteCount = ByteCount(63 * 1024 * 1024);
// a segment lost with its row counts every stage a store held at open
const STAGE_LOST: usize = NEWEST;
// a stage for writes after the open
const AFTER_OPEN: usize = usize::MAX;

type Store = ReelStore<SimulatedIo>;

/// The newest version of a key, and which segment holds it
#[derive(Clone, Debug)]
struct Entry {
    value: Option<Vec<u8>>,
    stage: usize,
}

type Model = BTreeMap<(ColumnId, Vec<u8>), Entry>;

/// A store with three sealed segments, closed
struct Built {
    io: SimulatedIo,
    model: Model,
    files: Vec<PathBuf>,
    heads: Vec<TailHead>,
}

/// Which keys a damaged segment's ranges hold
#[derive(Clone, Copy, Debug)]
enum Cover {
    /// Keys between the segment's lowest and highest in each column
    Edges(usize),
    /// Every key of the segment's columns
    Whole(usize),
}

fn config() -> StoreConfig {
    sized_config(BUDGET)
}

fn sized_config(budget: ByteCount) -> StoreConfig {
    StoreConfig {
        memory_budget: budget,
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "meta".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(),
        }],
        columns: vec![
            ColumnConfig {
                id: ORDERED,
                name: "ordered".to_string(),
                key_shape: KeyShape::Variable { most: 128 },
                codec: Codec::None,
                volume: VolumeId(0),
                complete_rows: None,
                least_value: None,
            },
            ColumnConfig {
                id: RANDOM,
                name: "random".to_string(),
                key_shape: KeyShape::Variable { most: 128 },
                codec: Codec::None,
                volume: VolumeId(0),
                complete_rows: None,
                least_value: None,
            },
        ],
    }
}

fn open(io: &SimulatedIo) -> Store {
    ReelStore::open(io.clone(), &config()).expect("open")
}

fn settle(io: &SimulatedIo) {
    for _step in 0..SETTLE_STEPS {
        if io.step_any().is_none() {
            io.advance_clock(SETTLE_TICK);
            if io.step_any().is_none() {
                return;
            }
        }
    }
    panic!("jobs never went quiet");
}

fn key_of(column: ColumnId, number: u64) -> Vec<u8> {
    if column == ORDERED {
        return format!("key-{number:08}").into_bytes();
    }
    format!("{:016x}", number.wrapping_mul(SPREAD)).into_bytes()
}

// the numbers a stage's segment holds in each column
fn numbers_of(stage: usize) -> Vec<u64> {
    if stage == OLDEST {
        return (0..KEY_COUNT).collect();
    }
    if stage == MIDDLE {
        return (100..200).collect();
    }
    (150..170).chain(250..260).collect()
}

fn write(store: &Store, model: &mut Model, column: ColumnId, number: u64, stage: usize, is_grave: bool) {
    let key = key_of(column, number);
    let value = (!is_grave).then(|| format!("{number}-from-{stage}").into_bytes());
    match &value {
        Some(value) => store.put(column, &key, value).expect("put"),
        None => store.delete(column, &key).expect("delete"),
    }
    model.insert((column, key), Entry { value, stage });
}

fn seal(store: &Store, io: &SimulatedIo) {
    store.seal_now();
    settle(io);
}

fn data_files(io: &SimulatedIo) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for path in io.list_directory(&PathBuf::from(ROOT)).expect("list") {
        if path.extension().is_some_and(|extension| extension == "data") {
            files.push(path);
        }
    }
    files.sort();
    files
}

fn read_head(io: &SimulatedIo, path: &Path) -> TailHead {
    let superblock = Superblock::read(io, &PathBuf::from(ROOT)).expect("read").expect("superblock");
    let file = io.open_read_only(path).expect("open");
    let mut columns = Vec::new();
    let head = FooterReader::new().read_tail(&file, superblock.record_key, &mut columns).expect("tail");
    io.close_file(file);
    head.expect("sealed")
}

// the oldest segment holds every key, and the two after it hold newer versions and graves of some
fn build(seed: u64) -> Built {
    let io = SimulatedIo::new(Seed(seed));
    let store = open(&io);
    // reclaim would take the dead rows these segments are built to hold
    store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
    let mut model = Model::new();
    for column in COLUMNS {
        for number in numbers_of(OLDEST) {
            write(&store, &mut model, column, number, OLDEST, false);
        }
    }
    seal(&store, &io);
    for column in COLUMNS {
        for number in numbers_of(MIDDLE) {
            write(&store, &mut model, column, number, MIDDLE, number.is_multiple_of(5));
        }
    }
    seal(&store, &io);
    for column in COLUMNS {
        for number in numbers_of(NEWEST) {
            write(&store, &mut model, column, number, NEWEST, number.is_multiple_of(4));
        }
    }
    seal(&store, &io);
    store.close().expect("close");
    drop(store);

    let files = data_files(&io);
    assert_eq!(files.len(), SEGMENTS);
    let heads = files.iter().map(|path| read_head(&io, path)).collect();
    Built { io, model, files, heads }
}

fn read(store: &Store, column: ColumnId, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
    let mut buf = Vec::new();
    Ok(store.get(column, key, &mut buf)?.then_some(buf))
}

fn rot_first_tail(built: &Built, stage: usize) {
    let length = file_length(&built.io, &built.files[stage]);
    built.io.rot(&built.files[stage], FileOffset(length.0 - 3), FLIP).expect("rot");
}

fn rot_second_tail(built: &Built, stage: usize) {
    let length = file_length(&built.io, &built.files[stage]);
    built.io.rot(&built.files[stage], FileOffset(length.0 - SECOND_TAIL_DISTANCE - 3), FLIP).expect("rot");
}

fn rot_indexes(built: &Built, stage: usize) {
    let head = &built.heads[stage];
    built.io.rot(&built.files[stage], FileOffset(head.index_at.0 + 2), FLIP).expect("rot");
    built.io.rot(&built.files[stage], FileOffset(head.second_index_at.0 + 2), FLIP).expect("rot");
}

// rot a row in both copies, the double failure that loses it
fn rot_row(built: &Built, segment: SegmentNumber) {
    let paths = SummaryPaths::new(Path::new(ROOT));
    let at = row_offset(&built.io, &paths, segment);
    let offset = FileOffset((at + ROW_HEAD_BYTES + 2) as u64);
    built.io.rot(&paths.current, offset, FLIP).expect("rot");
    built.io.rot(&paths.second, offset, FLIP).expect("rot");
}

fn file_length(io: &SimulatedIo, path: &Path) -> ByteCount {
    let file = io.open_read_only(path).expect("open");
    let length = file_length_of(&file);
    io.close_file(file);
    length
}

fn file_length_of(file: &impl IoFile) -> ByteCount {
    file.length().expect("length")
}

// the keys a segment holds in a column, at its lowest and highest
fn edges(column: ColumnId, stage: usize) -> (Vec<u8>, Vec<u8>) {
    let mut keys: Vec<Vec<u8>> = numbers_of(stage).into_iter().map(|number| key_of(column, number)).collect();
    keys.sort();
    (keys[0].clone(), keys[keys.len() - 1].clone())
}

fn is_inside(cover: Cover, column: ColumnId, key: &[u8]) -> bool {
    match cover {
        Cover::Whole(_stage) => true,
        Cover::Edges(stage) => {
            let (lowest, highest) = edges(column, stage);
            key >= lowest.as_slice() && key <= highest.as_slice()
        },
    }
}

fn stage_of(cover: Cover) -> usize {
    match cover {
        Cover::Edges(stage) | Cover::Whole(stage) => stage,
    }
}

fn assert_match(store: &Store, model: &Model) {
    for ((column, key), entry) in model {
        assert_eq!(read(store, *column, key).expect("get"), entry.value, "key {key:?}");
    }
}

// a read inside the ranges answers only from a segment above the damaged one, and every other read matches
fn assert_guarded(store: &Store, built: &Built, cover: Cover) -> u64 {
    let mut refused = 0;
    for ((column, key), entry) in &built.model {
        let got = read(store, *column, key);
        if is_inside(cover, *column, key) && entry.stage <= stage_of(cover) {
            assert!(matches!(got, Err(StoreError::Corruption { .. })), "key {key:?} answered {got:?}");
            refused += 1;
        } else {
            assert_eq!(got.expect("get"), entry.value, "key {key:?}");
        }
    }
    refused
}

// rot in one tail copy costs nothing
#[test]
fn first_tail() {
    let built = build(1);
    rot_first_tail(&built, MIDDLE);

    let store = open(&built.io);

    assert_eq!(store.damaged_segments(), 0);
    assert_match(&store, &built.model);
    store.close().expect("close");
}

// rot in the other tail copy costs nothing
#[test]
fn second_tail() {
    let built = build(2);
    rot_second_tail(&built, MIDDLE);

    let store = open(&built.io);

    assert_eq!(store.damaged_segments(), 0);
    assert_match(&store, &built.model);
    store.close().expect("close");
}

// rot in one block index copy costs nothing
#[test]
fn one_index() {
    let built = build(3);
    let head = &built.heads[MIDDLE];
    built.io.rot(&built.files[MIDDLE], FileOffset(head.index_at.0 + 2), FLIP).expect("rot");

    let store = open(&built.io);

    assert_eq!(store.damaged_segments(), 0);
    assert_match(&store, &built.model);
    store.close().expect("close");
}

// both tails lost: the summary row gives the ranges, and keys outside them keep serving
#[test]
fn both_tails() {
    let built = build(4);
    rot_first_tail(&built, MIDDLE);
    rot_second_tail(&built, MIDDLE);

    let store = open(&built.io);

    assert_eq!(store.damaged_segments(), 1);
    assert!(assert_guarded(&store, &built, Cover::Edges(MIDDLE)) > 0);
    store.close().expect("close");
}

// a walk serves up to a damaged range and stops there with an error, and one that never reaches it ends clean
#[test]
fn scan_reaches_damage() {
    let built = build(41);
    rot_first_tail(&built, MIDDLE);
    rot_second_tail(&built, MIDDLE);
    let store = open(&built.io);
    let (lowest, highest) = edges(ORDERED, MIDDLE);

    for (start, direction) in [(&b""[..], Direction::Ascending), (&b"zzz"[..], Direction::Descending)] {
        let mut scan = store.iter_from(ORDERED, start, direction).expect("scan");
        let (mut key, mut value, mut found) = (Vec::new(), Vec::new(), 0);
        loop {
            match scan.next_row(&mut key, &mut value) {
                Ok(ScanStep::Found) => {
                    assert!(key < lowest || key > highest, "key {key:?} came from inside");
                    found += 1;
                },
                Err(StoreError::Corruption { .. }) => break,
                other => panic!("walk ended {other:?}"),
            }
        }
        assert!(found > 0);
        assert!(matches!(scan.next_row(&mut key, &mut value), Err(StoreError::Corruption { .. })));
    }

    let (start, end) = (key_of(ORDERED, 270), key_of(ORDERED, KEY_COUNT));
    let mut scan = store.iter_range(ORDERED, &start, &end, Direction::Ascending).expect("scan");
    let (mut key, mut value, mut found) = (Vec::new(), Vec::new(), 0);
    while scan.next_row(&mut key, &mut value).expect("step") == ScanStep::Found {
        found += 1;
    }
    assert_eq!(found, KEY_COUNT - 270);
    drop(scan);
    store.close().expect("close");
}

// both index copies lost: the same ranges, and the tail still reads
#[test]
fn both_indexes() {
    let built = build(5);
    rot_indexes(&built, MIDDLE);

    let store = open(&built.io);

    assert_eq!(store.damaged_segments(), 1);
    assert!(assert_guarded(&store, &built, Cover::Edges(MIDDLE)) > 0);
    store.close().expect("close");
    assert_eq!(built.io.open_handle_count(), 0);
}

// a summary row lost alone comes back from the tail, and reads never notice
#[test]
fn lost_row() {
    let built = build(6);
    let segment = built.heads[MIDDLE].segment;
    rot_row(&built, segment);

    let store = open(&built.io);

    assert_eq!(store.damaged_segments(), 0);
    assert!(hold(&store.state.volumes[0].summary).get(segment).is_some());
    assert_match(&store, &built.model);
    store.close().expect("close");
}

// the row comes back with its ranges, so tails lost after that still leave keys outside serving
#[test]
fn rebuilt_row() {
    let built = build(7);
    rot_row(&built, built.heads[MIDDLE].segment);
    let store = open(&built.io);
    store.close().expect("close");
    drop(store);

    rot_first_tail(&built, MIDDLE);
    rot_second_tail(&built, MIDDLE);
    let store = open(&built.io);

    assert!(assert_guarded(&store, &built, Cover::Edges(MIDDLE)) > 0);
    store.close().expect("close");
}

// tails and row lost: the damage covers the columns whole, at the place counter at open
#[test]
fn nothing_left() {
    let built = build(8);
    rot_row(&built, built.heads[MIDDLE].segment);
    rot_first_tail(&built, MIDDLE);
    rot_second_tail(&built, MIDDLE);

    let store = open(&built.io);

    assert_eq!(store.damaged_segments(), 1);
    assert!(assert_guarded(&store, &built, Cover::Whole(STAGE_LOST)) > 0);
    store.close().expect("close");
}

// indexes and row lost: the tail still knows the place, and the columns count whole
#[test]
fn only_tail() {
    let built = build(9);
    rot_row(&built, built.heads[MIDDLE].segment);
    rot_indexes(&built, MIDDLE);

    let store = open(&built.io);

    assert_eq!(store.damaged_segments(), 1);
    assert!(assert_guarded(&store, &built, Cover::Whole(MIDDLE)) > 0);
    store.close().expect("close");
}

// a rotted row block costs its own keys and nothing else
#[test]
fn one_block() {
    let built = build(10);
    let at = built.heads[MIDDLE].data_end.0 + BLOCK_HEADER_BYTES as u64 + 5;
    built.io.rot(&built.files[MIDDLE], FileOffset(at), FLIP).expect("rot");

    let store = open(&built.io);

    let mut refused = 0;
    for ((column, key), entry) in &built.model {
        match read(&store, *column, key) {
            Ok(value) => assert_eq!(value, entry.value, "key {key:?}"),
            Err(StoreError::Corruption { .. }) => refused += 1,
            Err(error) => panic!("key {key:?} failed: {error}"),
        }
    }
    assert!(refused > 0);
    store.close().expect("close");
}

// a merge that meets a rotten row block damages its segment and goes quiet, where it once picked the same inputs forever
#[test]
fn merge_meets_rot() {
    let built = build(12);
    let at = built.heads[MIDDLE].data_end.0 + BLOCK_HEADER_BYTES as u64 + 5;
    built.io.rot(&built.files[MIDDLE], FileOffset(at), FLIP).expect("rot");
    assert_merge_damages(&built);
}

// the same for a block the device can't read at all
#[test]
fn merge_meets_bad_sector() {
    let built = build(13);
    let at = built.heads[MIDDLE].data_end.0 + BLOCK_HEADER_BYTES as u64 + 5;
    built.io.bad_sector(&built.files[MIDDLE], FileOffset(at)).expect("bad sector");
    assert_merge_damages(&built);
}

fn assert_merge_damages(built: &Built) {
    let store = open(&built.io);
    hold_rebuilds(&store);
    settle(&built.io);
    assert_eq!(store.damaged_segments(), 1);
    for ((column, key), entry) in &built.model {
        match read(&store, *column, key) {
            Ok(value) => assert_eq!(value, entry.value, "key {key:?}"),
            Err(StoreError::Corruption { .. }) => {},
            Err(error) => panic!("key {key:?} failed: {error}"),
        }
    }
    store.close().expect("close");
}

// a put or delete inside a range lands above it and serves, also after another open
#[test]
fn write_above() {
    let mut built = build(11);
    rot_first_tail(&built, NEWEST);
    rot_second_tail(&built, NEWEST);

    let store = open(&built.io);
    hold_rebuilds(&store);
    let mut model = built.model.clone();
    write(&store, &mut model, ORDERED, 155, NEWEST + 1, false);
    write(&store, &mut model, ORDERED, 156, NEWEST + 1, true);
    built.model = model;
    let guarded = assert_guarded(&store, &built, Cover::Edges(NEWEST));
    assert!(guarded > 0);
    seal(&store, &built.io);
    store.close().expect("close");
    drop(store);

    let store = open(&built.io);
    hold_rebuilds(&store);
    assert_eq!(assert_guarded(&store, &built, Cover::Edges(NEWEST)), guarded);
    store.close().expect("close");
}

// ranges stay through opens until the caller clears them, and a clear lasts
#[test]
fn clear_lasts() {
    let built = build(12);
    rot_first_tail(&built, MIDDLE);
    rot_second_tail(&built, MIDDLE);

    let store = open(&built.io);
    store.close().expect("close");
    drop(store);

    let store = open(&built.io);
    assert!(assert_guarded(&store, &built, Cover::Edges(MIDDLE)) > 0);
    let mut repaired = built.model.clone();
    for column in COLUMNS {
        let (lowest, highest) = edges(column, MIDDLE);
        for ((held_column, key), entry) in &built.model {
            if *held_column == column && key >= &lowest && key <= &highest {
                let value = b"repaired".to_vec();
                store.put(column, key, &value).expect("put");
                repaired.insert((column, key.clone()), Entry { value: Some(value), stage: entry.stage });
            }
        }
        store.clear_damage(column, b"", None).expect("clear");
    }
    assert_match(&store, &repaired);
    seal(&store, &built.io);
    store.close().expect("close");
    drop(store);

    let store = open(&built.io);
    assert_match(&store, &repaired);
    store.close().expect("close");
}

// clearing part of a range keeps the rest, across an open
#[test]
fn clear_part() {
    let built = build(13);
    rot_first_tail(&built, MIDDLE);
    rot_second_tail(&built, MIDDLE);
    let store = open(&built.io);

    let split = key_of(ORDERED, 130);
    store.clear_damage(ORDERED, b"", Some(&split)).expect("clear");
    store.close().expect("close");
    drop(store);

    let store = open(&built.io);
    let cleared = key_of(ORDERED, 110);
    let refused = key_of(ORDERED, 131);
    assert!(read(&store, ORDERED, &cleared).is_ok());
    assert!(matches!(read(&store, ORDERED, &refused), Err(StoreError::Corruption { .. })));
    store.close().expect("close");
}

// spans outlive the fault: a segment that reads again keeps them until a clear
#[test]
fn span_stays() {
    let built = build(14);
    rot_indexes(&built, MIDDLE);
    let store = open(&built.io);
    assert_eq!(store.damaged_segments(), 1);
    store.close().expect("close");
    drop(store);

    rot_indexes(&built, MIDDLE);
    let store = open(&built.io);

    assert_eq!(store.damaged_segments(), 1);
    assert!(assert_guarded(&store, &built, Cover::Edges(MIDDLE)) > 0);
    store.close().expect("close");
}

// a file whose length moved keeps no row: the damage covers the columns whole
#[test]
fn moved_length() {
    let built = build(15);
    let file = built.io.open_file(&built.files[MIDDLE]).expect("open");
    let end = FileOffset(file_length_of(&file).0);
    file.write_at(end, &[IoSlice::new(b"junk")]).expect("write");
    file.sync().expect("sync");
    built.io.close_file(file);

    let store = open(&built.io);

    assert_eq!(store.damaged_segments(), 1);
    assert!(assert_guarded(&store, &built, Cover::Whole(STAGE_LOST)) > 0);
    store.close().expect("close");
}

// the counter at open stands above a segment lost with its row, and writes above it serve across opens
#[test]
fn lost_write() {
    let mut built = build(16);
    rot_row(&built, built.heads[NEWEST].segment);
    rot_first_tail(&built, NEWEST);
    rot_second_tail(&built, NEWEST);

    let store = open(&built.io);
    assert!(store.state.read_point(&store.state.volumes[0]) >= built.heads[NEWEST].places.highest);
    let mut model = built.model.clone();
    write(&store, &mut model, ORDERED, 155, AFTER_OPEN, false);
    write(&store, &mut model, RANDOM, 156, AFTER_OPEN, true);
    built.model = model;
    let refused = assert_guarded(&store, &built, Cover::Whole(STAGE_LOST));
    assert!(refused > 0);
    seal(&store, &built.io);
    store.close().expect("close");
    drop(store);

    let store = open(&built.io);
    assert_eq!(assert_guarded(&store, &built, Cover::Whole(STAGE_LOST)), refused);
    store.close().expect("close");
}

// close saves the summary with the mark it's current to
#[test]
fn close_mark() {
    let built = build(17);
    let store = open(&built.io);
    store.put(ORDERED, b"last", b"value").expect("put");
    let mark = store.state.read_point(&store.state.volumes[0]);
    store.close().expect("close");
    drop(store);

    let mut rows = SummaryRows::new(ByteCount(0));
    let saved = read_summary(&built.io, &SummaryPaths::new(Path::new(ROOT)), &mut rows, &mut LostRows::default()).expect("read");

    assert_eq!(saved, Some(mark));
}

// rot in a record the last sync covered reads as corruption, and never as the older version under it
#[test]
fn flushed_rot() {
    let io = SimulatedIo::new(Seed(18));
    let store = open(&io);
    let key = key_of(ORDERED, 1);
    store.put(ORDERED, &key, b"older").expect("put");
    seal(&store, &io);
    store.put(ORDERED, &key, b"newer").expect("put");
    store.flush().expect("flush");
    io.kill();
    drop(store);

    let files = data_files(&io);
    io.rot(&files[files.len() - 1], FileOffset(3), FLIP).expect("rot");
    let store = open(&io);

    assert!(matches!(read(&store, ORDERED, &key), Err(StoreError::Corruption { .. })));
    store.close().expect("close");
}

// a sync that found no new rows still covers what the row job wrote out before it
#[test]
fn later_sync() {
    let io = SimulatedIo::new(Seed(19));
    let store = open(&io);
    let key = key_of(ORDERED, 1);
    store.put(ORDERED, &key, b"older").expect("put");
    seal(&store, &io);
    store.put(ORDERED, &key, b"newer").expect("put");
    settle(&io);
    store.flush().expect("flush");
    io.kill();
    drop(store);

    let files = data_files(&io);
    io.rot(&files[files.len() - 1], FileOffset(3), FLIP).expect("rot");
    let store = open(&io);

    assert!(matches!(read(&store, ORDERED, &key), Err(StoreError::Corruption { .. })));
    store.close().expect("close");
}

fn rot_length(built: &Built, segment: SegmentNumber) {
    let paths = SummaryPaths::new(Path::new(ROOT));
    let at = FileOffset((row_offset(&built.io, &paths, segment) + 5) as u64);
    built.io.rot(&paths.current, at, FLIP).expect("rot");
    built.io.rot(&paths.second, at, FLIP).expect("rot");
}

fn summary_rows(io: &SimulatedIo) -> (SummaryPaths, SummaryRows, WritePlace) {
    let paths = SummaryPaths::new(Path::new(ROOT));
    let mut rows = SummaryRows::new(ByteCount(0));
    let mark = read_summary(io, &paths, &mut rows, &mut LostRows::default()).expect("read").expect("a summary");
    rows.make_room(ByteCount(rows.bytes().len() as u64 * 2));
    (paths, rows, mark)
}

fn row_offset(io: &SimulatedIo, paths: &SummaryPaths, segment: SegmentNumber) -> usize {
    let file = io.open_read_only(&paths.current).expect("open");
    let mut bytes = vec![0; file_length_of(&file).0 as usize];
    file.read_exact_at(FileOffset(0), &mut bytes).expect("read");
    io.close_file(file);

    let mut at = SUMMARY_HEADER_BYTES;
    loop {
        let (length, row) = read_row(&bytes[at..]).expect("the row is in the summary");
        if row.expect("row").facts.segment == segment {
            return at;
        }
        at += length;
    }
}

fn assert_refused_or_match(store: &Store, model: &Model) -> u64 {
    let mut refused = 0;
    for ((column, key), entry) in model {
        match read(store, *column, key) {
            Ok(value) => assert_eq!(value, entry.value, "key {key:?}"),
            Err(StoreError::Corruption { .. }) => refused += 1,
            Err(error) => panic!("key {key:?} failed: {error}"),
        }
    }
    refused
}

// rot in a row's length field costs that row alone, so the next segment's ranges still come from its row
#[test]
fn row_length() {
    let built = build(20);
    rot_length(&built, built.heads[MIDDLE].segment);
    rot_first_tail(&built, NEWEST);
    rot_second_tail(&built, NEWEST);

    let store = open(&built.io);

    assert!(assert_guarded(&store, &built, Cover::Edges(NEWEST)) > 0);
    store.close().expect("close");
}

// a bad sector under a record reads as corruption of that record
#[test]
fn bad_record() {
    let built = build(21);
    built.io.bad_sector(&built.files[MIDDLE], FileOffset(3)).expect("bad");

    let store = open(&built.io);

    assert!(assert_refused_or_match(&store, &built.model) > 0);
    store.close().expect("close");
}

// a bad sector under one tail copy costs nothing
#[test]
fn bad_tail() {
    let built = build(22);
    let length = file_length(&built.io, &built.files[MIDDLE]);
    built.io.bad_sector(&built.files[MIDDLE], FileOffset(length.0 - 3)).expect("bad");

    let store = open(&built.io);

    assert_eq!(store.damaged_segments(), 0);
    assert_match(&store, &built.model);
    store.close().expect("close");
}

// bad sectors under both tail copies damage the segment, and the store opens
#[test]
fn bad_tails() {
    let built = build(23);
    let length = file_length(&built.io, &built.files[MIDDLE]);
    built.io.bad_sector(&built.files[MIDDLE], FileOffset(length.0 - 3)).expect("bad");
    let second = FileOffset(length.0 - SECOND_TAIL_DISTANCE - 3);
    built.io.bad_sector(&built.files[MIDDLE], second).expect("bad");

    let store = open(&built.io);

    assert_eq!(store.damaged_segments(), 1);
    assert!(assert_guarded(&store, &built, Cover::Edges(MIDDLE)) > 0);
    store.close().expect("close");
}

// a bad sector under a row block costs that block's keys and leaves the store up
#[test]
fn bad_block() {
    let built = build(24);
    let at = built.heads[MIDDLE].data_end.0 + BLOCK_HEADER_BYTES as u64 + 5;
    built.io.bad_sector(&built.files[MIDDLE], FileOffset(at)).expect("bad");

    let store = open(&built.io);

    assert!(assert_refused_or_match(&store, &built.model) > 0);
    store.close().expect("close");
}

// bad sectors under both copies cost their rows, which come back from the tails
#[test]
fn bad_summary() {
    let built = build(25);
    let paths = SummaryPaths::new(Path::new(ROOT));
    built.io.bad_sector(&paths.current, FileOffset(SUMMARY_HEADER_BYTES as u64 + 3)).expect("bad");
    built.io.bad_sector(&paths.second, FileOffset(SUMMARY_HEADER_BYTES as u64 + 3)).expect("bad");

    let store = open(&built.io);

    assert_eq!(hold(&store.state.volumes[0].summary).rows().count(), SEGMENTS);
    assert_match(&store, &built.model);
    store.close().expect("close");
}

// a sealed segment with a zeroed first head and two bad tails stays, since the summary says what it is
#[test]
fn zero_head() {
    let built = build(26);
    let file = built.io.open_file(&built.files[MIDDLE]).expect("open");
    file.write_at(FileOffset(0), &[IoSlice::new(&[0; 512])]).expect("write");
    file.sync().expect("sync");
    built.io.close_file(file);
    rot_first_tail(&built, MIDDLE);
    rot_second_tail(&built, MIDDLE);

    let store = open(&built.io);

    assert_eq!(data_files(&built.io).len(), SEGMENTS);
    assert_eq!(store.damaged_segments(), 1);
    assert!(assert_guarded(&store, &built, Cover::Edges(MIDDLE)) > 0);
    store.close().expect("close");
}

// a row whose seal mark disagrees with the tail speaks for nothing, and the columns count whole
#[test]
fn wrong_mark() {
    let built = build(27);
    let (paths, mut rows, mark) = summary_rows(&built.io);
    let segment = built.heads[MIDDLE].segment;
    let mut facts = rows.get(segment).expect("row").facts;
    facts.seal_mark = Checksum(facts.seal_mark.0 + 1);
    rows.remove(segment);
    assert!(rows.push(&facts, [].into_iter(), [].into_iter()));
    write_summary(&built.io, &paths, mark, rows.highest_forward, rows.bytes()).expect("write");
    rot_indexes(&built, MIDDLE);

    let store = open(&built.io);

    assert!(assert_guarded(&store, &built, Cover::Whole(MIDDLE)) > 0);
    store.close().expect("close");
}

// a row of a segment no root holds leaves at open
#[test]
fn gone_row() {
    let built = build(28);
    let (paths, mut rows, mark) = summary_rows(&built.io);
    let gone = SegmentNumber(built.heads[NEWEST].segment.0 + 1_000);
    let mut facts = rows.get(built.heads[NEWEST].segment).expect("row").facts;
    facts.segment = gone;
    assert!(rows.push(&facts, [].into_iter(), [].into_iter()));
    write_summary(&built.io, &paths, mark, rows.highest_forward, rows.bytes()).expect("write");

    let store = open(&built.io);

    assert!(hold(&store.state.volumes[0].summary).get(gone).is_none());
    store.close().expect("close");
}

// contains refuses inside a range the way get does
#[test]
fn key_contains() {
    let built = build(29);
    rot_first_tail(&built, MIDDLE);
    rot_second_tail(&built, MIDDLE);

    let store = open(&built.io);

    let inside = key_of(ORDERED, 110);
    let outside = key_of(ORDERED, 10);
    assert!(matches!(store.contains(ORDERED, &inside), Err(StoreError::Corruption { .. })));
    assert!(store.contains(ORDERED, &outside).expect("contains"));
    store.close().expect("close");
}

// a store with no summary file builds one from its tails
#[test]
fn no_summary() {
    let built = build(30);
    let paths = SummaryPaths::new(Path::new(ROOT));
    built.io.remove_file(&paths.current).expect("remove");
    built.io.remove_file(&paths.second).expect("remove");

    let store = open(&built.io);

    assert_eq!(hold(&store.state.volumes[0].summary).rows().count(), SEGMENTS);
    assert_match(&store, &built.model);
    store.close().expect("close");
}

// a full summary loses the row, counts it, and the seal goes through
#[test]
fn full_summary() {
    let built = build(31);
    let store = open(&built.io);
    let facts = hold(&store.state.volumes[0].summary).get(built.heads[NEWEST].segment).expect("row").facts;
    while hold(&store.state.volumes[0].summary).push(&facts, [].into_iter(), [].into_iter()) {}

    store.put(ORDERED, b"after", b"value").expect("put");
    seal(&store, &built.io);

    assert_eq!(store.dropped_summary_rows(), 1);
    assert_eq!(read(&store, ORDERED, b"after").expect("get"), Some(b"value".to_vec()));
    assert_match(&store, &built.model);
    store.close().expect("close");
}

// long keys sharing their first bytes keep a row of bounded size, and a range that only widens
#[test]
fn long_keys() {
    let io = SimulatedIo::new(Seed(32));
    let store = open(&io);
    let shared = "s".repeat(LONG_KEY_BYTES);
    let key = |number: u64| format!("{shared}{number:04}").into_bytes();
    let mut model = Model::new();
    for number in 0..10 {
        store.put(ORDERED, &key(number), b"value").expect("put");
        model.insert((ORDERED, key(number)), Entry { value: Some(b"value".to_vec()), stage: MIDDLE });
    }
    seal(&store, &io);
    let row_length = hold(&store.state.volumes[0].summary).bytes().len();
    assert!(row_length < 256, "row took {row_length} B");
    store.close().expect("close");
    drop(store);

    let files = data_files(&io);
    let length = file_length(&io, &files[0]);
    io.rot(&files[0], FileOffset(length.0 - 3), FLIP).expect("rot");
    io.rot(&files[0], FileOffset(length.0 - SECOND_TAIL_DISTANCE - 3), FLIP).expect("rot");

    let store = open(&io);

    for number in 0..10 {
        assert!(matches!(read(&store, ORDERED, &key(number)), Err(StoreError::Corruption { .. })));
    }
    assert_eq!(read(&store, ORDERED, b"a").expect("get"), None);
    assert_eq!(read(&store, ORDERED, b"t").expect("get"), None);
    store.close().expect("close");
}

// a crash at any step of a seal leaves a row for every sealed segment once open is through
#[test]
fn seal_crash() {
    for crash_at in 0..SEAL_CRASH_POINTS {
        for is_power_cut in [false, true] {
            let io = SimulatedIo::new(Seed(crash_at));
            let store = open(&io);
            store.put(ORDERED, b"key", b"value").expect("put");
            store.flush().expect("flush");
            store.seal_now();
            io.arm_crash(OperationCount(crash_at));
            for _step in 0..SEAL_STEPS {
                if io.step_any().is_none() || io.is_crashed() {
                    break;
                }
            }
            if is_power_cut {
                io.power_cut();
            } else {
                io.kill();
            }
            drop(store);

            let store = open(&io);
            let rows = hold(&store.state.volumes[0].summary).rows().count() as u64;
            let sealed = store.state.volumes[0].table.segment_count();
            assert_eq!(rows, sealed, "crash at {crash_at}");
            assert_eq!(read(&store, ORDERED, b"key").expect("get"), Some(b"value".to_vec()));
            store.close().expect("close");
        }
    }
}

// a crash inside a clear leaves the spans or the clear, and never a store that can't open
#[test]
fn clear_crash() {
    for crash_at in 0..CLEAR_CRASH_POINTS {
        let built = build(33);
        rot_first_tail(&built, MIDDLE);
        rot_second_tail(&built, MIDDLE);
        let store = open(&built.io);

        built.io.arm_crash(OperationCount(crash_at));
        let cleared = store.clear_damage(ORDERED, b"", None);
        built.io.power_cut();
        drop(store);

        let store = open(&built.io);
        let inside = key_of(ORDERED, 110);
        match read(&store, ORDERED, &inside) {
            Ok(_value) => {},
            Err(StoreError::Corruption { .. }) => assert!(cleared.is_err(), "crash at {crash_at}"),
            Err(error) => panic!("crash at {crash_at}: {error}"),
        }
        store.clear_damage(ORDERED, b"", None).expect("clear");
        assert!(read(&store, ORDERED, &inside).is_ok());
        store.close().expect("close");
    }
}

fn rows_files(io: &SimulatedIo) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for path in io.list_directory(&PathBuf::from(ROOT)).expect("list") {
        if path.extension().is_some_and(|extension| extension == "rows") {
            files.push(path);
        }
    }
    files
}

// two flushed groups in an open segment, with rot in the first one
fn rotted_rows(seed: u64) -> SimulatedIo {
    let io = SimulatedIo::new(Seed(seed));
    let store = open(&io);
    store.put(ORDERED, b"older", b"value").expect("put");
    store.flush().expect("flush");
    store.put(ORDERED, b"later", b"value").expect("put");
    store.flush().expect("flush");
    io.kill();
    drop(store);

    let files = rows_files(&io);
    assert_eq!(files.len(), 1);
    io.rot(&files[0], FileOffset(20), FLIP).expect("rot");
    io
}

// rot in a group a later sync mark covers keeps the later groups and damages the columns whole
#[test]
fn rows_rot() {
    let io = rotted_rows(34);

    let store = open(&io);

    assert_eq!(store.damaged_segments(), 1);
    assert!(matches!(read(&store, ORDERED, b"later"), Err(StoreError::Corruption { .. })));
    assert!(matches!(read(&store, RANDOM, b"never"), Err(StoreError::Corruption { .. })));
    store.put(ORDERED, b"new", b"value").expect("put");
    assert_eq!(read(&store, ORDERED, b"new").expect("get"), Some(b"value".to_vec()));

    store.clear_damage(ORDERED, b"", None).expect("clear");
    assert_eq!(read(&store, ORDERED, b"later").expect("get"), Some(b"value".to_vec()));
    assert_eq!(read(&store, ORDERED, b"older").expect("get"), None);
    store.close().expect("close");
}

// write to one open segment, by making every other look busy
fn write_in(store: &Store, number: SegmentNumber, write: impl FnOnce()) {
    let volume = &store.state.volumes[0];
    let busy = |in_flight: u32| {
        for segment in hold(&volume.claims).open.iter_mut().filter(|segment| segment.number != number) {
            segment.in_flight = in_flight;
        }
    };
    busy(BUSY_WRITES);
    write();
    busy(0);
}

fn seal_one(store: &Store, io: &SimulatedIo, number: SegmentNumber) {
    let volume = &store.state.volumes[0];
    hold(&volume.claims).open.iter_mut().find(|segment| segment.number == number).expect("open").phase = SegmentPhase::Sealing;
    if let Some(task) = store.state.seal_tasks[0].get() {
        task.wake();
    }
    settle(io);
}

// a segment damaged at open goes on taking rows above that rank, and when its tails go the lost footer hid those rows too, so a
// grave that ranks above the old damage can't answer for the key
#[test]
fn lost_tails_raise_the_rank() {
    let io = rotted_rows(37);
    let store = open(&io);
    store.put(ORDERED, b"warm", b"value").expect("put");
    let volume = &store.state.volumes[0];
    let damaged = hold(&volume.claims).open[0].number;
    hold(&volume.claims).wants_prepared = true;
    store.state.prepare_segments();
    settle(&io);
    let other = {
        let mut claims = hold(&volume.claims);
        let spare = claims.prepared.take().expect("a spare");
        let number = spare.number;
        volume.open_slot(spare.slot);
        claims.open.push(spare);
        number
    };
    write_in(&store, other, || store.delete(ORDERED, b"later").expect("delete"));
    write_in(&store, damaged, || store.put(ORDERED, b"later", b"newest").expect("put"));
    seal_one(&store, &io, other);
    seal_one(&store, &io, damaged);
    let highest = hold(&volume.summary).get(damaged).expect("a row").facts.places.highest;
    let path = data_path(Path::new(ROOT), damaged);
    store.close().expect("close");
    drop(store);
    let length = file_length(&io, &path);
    io.rot(&path, FileOffset(length.0 - 3), FLIP).expect("rot");
    io.rot(&path, FileOffset(length.0 - SECOND_TAIL_DISTANCE - 3), FLIP).expect("rot");

    let store = open(&io);
    assert_eq!(store.state.volumes[0].damage.covering(ORDERED, b"later"), Some(highest));
    assert!(matches!(read(&store, ORDERED, b"later"), Err(StoreError::Corruption { .. })));
    store.close().expect("close");
}

// a segment that lost rows and then loses both tails stays damaged, since its blocks can't say what the rows file held
#[test]
fn lost_rows_outlive_a_rebuild() {
    let io = rotted_rows(36);
    let store = open(&io);
    store.seal_now();
    settle(&io);
    store.close().expect("close");
    drop(store);
    for path in data_files(&io) {
        let length = file_length(&io, &path);
        if length.0 > SECOND_TAIL_DISTANCE + 3 {
            io.rot(&path, FileOffset(length.0 - 3), FLIP).expect("rot");
            io.rot(&path, FileOffset(length.0 - SECOND_TAIL_DISTANCE - 3), FLIP).expect("rot");
        }
    }

    let store = open(&io);
    settle(&io);
    assert!(matches!(read(&store, ORDERED, b"older"), Err(StoreError::Corruption { .. })));
    store.close().expect("close");
}

// the rank from the first open holds through opens and a seal, and so do a put and a clear
#[test]
fn rows_stay() {
    let io = rotted_rows(35);
    let store = open(&io);
    store.put(ORDERED, b"new", b"value").expect("put");
    store.clear_damage(RANDOM, b"", None).expect("clear");
    store.flush().expect("flush");
    store.close().expect("close");
    drop(store);

    let store = open(&io);
    assert_eq!(read(&store, ORDERED, b"new").expect("get"), Some(b"value".to_vec()));
    assert!(matches!(read(&store, ORDERED, b"later"), Err(StoreError::Corruption { .. })));
    assert_eq!(read(&store, RANDOM, b"never").expect("get"), None);
    seal(&store, &io);
    store.close().expect("close");
    drop(store);

    let store = open(&io);
    assert_eq!(rows_files(&io).len(), 0);
    assert_eq!(read(&store, ORDERED, b"new").expect("get"), Some(b"value".to_vec()));
    assert!(matches!(read(&store, ORDERED, b"later"), Err(StoreError::Corruption { .. })));
    assert_eq!(read(&store, RANDOM, b"never").expect("get"), None);
    store.close().expect("close");
}

// a synced last group that rots while a power cut took its mark reads as a torn end, which the spec accepts
#[test]
fn torn_end() {
    let io = SimulatedIo::new(Seed(36));
    let store = open(&io);
    store.put(ORDERED, b"older", b"value").expect("put");
    store.flush().expect("flush");
    io.kill();
    drop(store);
    let files = rows_files(&io);
    let length = file_length(&io, &files[0]);
    io.rot(&files[0], FileOffset(20), FLIP).expect("rot");
    io.rot(&files[0], FileOffset(length.0 - 3), FLIP).expect("rot");

    let store = open(&io);

    assert_eq!(store.damaged_segments(), 0);
    assert_eq!(read(&store, ORDERED, b"older").expect("get"), None);
    store.close().expect("close");
}

// a covered last group that rots with its mark group present reads as damage
#[test]
fn covered_last() {
    let io = SimulatedIo::new(Seed(37));
    let store = open(&io);
    store.put(ORDERED, b"older", b"value").expect("put");
    store.flush().expect("flush");
    io.kill();
    drop(store);
    io.rot(&rows_files(&io)[0], FileOffset(20), FLIP).expect("rot");

    let store = open(&io);

    assert_eq!(store.damaged_segments(), 1);
    assert!(matches!(read(&store, ORDERED, b"older"), Err(StoreError::Corruption { .. })));
    store.close().expect("close");
}

// a rows sync that fails drops a delete from the disk, and what open shows stays through a power cut
#[test]
fn rows_sync() {
    let io = SimulatedIo::new(Seed(38));
    let store = open(&io);
    store.put(ORDERED, b"key", b"older").expect("put");
    seal(&store, &io);
    store.delete(ORDERED, b"key").expect("delete");
    io.fail_file_sync("rows", OperationCount(0));
    assert!(store.flush().is_err());
    io.kill();
    drop(store);

    let store = open(&io);
    assert_eq!(read(&store, ORDERED, b"key").expect("get"), None);
    store.flush().expect("flush");
    io.power_cut();
    drop(store);

    let store = open(&io);
    assert_eq!(read(&store, ORDERED, b"key").expect("get"), None);
    store.close().expect("close");
}

// a tail sync that fails leaves the file's length, so a tail from an earlier seal can never end the file
#[test]
fn tail_then_cut() {
    let io = SimulatedIo::new(Seed(39));
    let store = open(&io);
    store.put(ORDERED, b"key", b"value").expect("put");
    store.flush().expect("flush");
    let before = data_lengths(&io);

    io.fail_file_sync("data", OperationCount(1));
    seal(&store, &io);

    assert_eq!(data_lengths(&io), before);
    io.kill();
    drop(store);
    let store = open(&io);
    assert_eq!(read(&store, ORDERED, b"key").expect("get"), Some(b"value".to_vec()));
    store.close().expect("close");
}

// run until an armed crash fires or the jobs go quiet, and say whether it fired
fn run_to_crash(io: &SimulatedIo) -> bool {
    for _step in 0..SETTLE_STEPS {
        if io.is_crashed() {
            return true;
        }
        if io.step_any().is_none() {
            io.advance_clock(SETTLE_TICK);
            if io.step_any().is_none() {
                return io.is_crashed();
            }
        }
    }
    io.is_crashed()
}

// one seal stopped at one point, rows flushed past what it wrote, and a power cut in a second seal
fn stale_tail_run(seed: u64, first: u64, second: u64, before_open: impl Fn(&SimulatedIo)) -> bool {
    let io = SimulatedIo::new(Seed(seed));
    let store = open(&io);
    store.put(ORDERED, b"first", b"value").expect("put");
    io.advance_clock(ROW_PERIOD);
    settle(&io);
    // a failed sync forgets the room set aside, so the file's durable end follows its writes
    io.fail_file_sync("data", OperationCount(0));
    let _failed = store.flush();
    io.kill();
    drop(store);

    let store = open(&io);
    store.flush().expect("flush");
    io.arm_crash(OperationCount(first));
    store.seal_now();
    if !run_to_crash(&io) {
        io.disarm_crash();
        store.close().expect("close");
        return false;
    }
    io.kill();
    drop(store);

    let store = open(&io);
    store.put(ORDERED, b"kept", b"value").expect("put");
    store.flush().expect("flush");
    store.put(ORDERED, b"later", b"value").expect("put");
    io.arm_crash(OperationCount(second));
    store.seal_now();
    let _fired = run_to_crash(&io);
    io.disarm_crash();
    io.power_cut();
    drop(store);
    before_open(&io);

    let store = open(&io);
    let kept = read(&store, ORDERED, b"kept");
    assert_eq!(kept.expect("get"), Some(b"value".to_vec()), "seed {seed}, crashes at {first} and {second}");
    store.close().expect("close");
    true
}

// a segment that lost both tails comes back whole from its row blocks, through an open too
#[test]
fn rebuild_restores() {
    let built = build(42);
    rot_first_tail(&built, MIDDLE);
    rot_second_tail(&built, MIDDLE);

    let store = open(&built.io);
    assert_eq!(store.damaged_segments(), 1);
    settle(&built.io);
    assert_eq!(store.damaged_segments(), 0);
    assert_match(&store, &built.model);
    store.close().expect("close");
    drop(store);

    let store = open(&built.io);
    assert_eq!(store.damaged_segments(), 0);
    assert_match(&store, &built.model);
    store.close().expect("close");
}

// a row block that fails its check leaves its span damaged, and every other key serves
#[test]
fn rebuild_keeps_a_lost_block_damaged() {
    let built = build(43);
    rot_first_tail(&built, MIDDLE);
    rot_second_tail(&built, MIDDLE);
    let at = built.heads[MIDDLE].data_end.0 + BLOCK_HEADER_BYTES as u64 + 3;
    built.io.rot(&built.files[MIDDLE], FileOffset(at), FLIP).expect("rot");

    let store = open(&built.io);
    settle(&built.io);
    assert_eq!(store.damaged_segments(), 1);
    let mut refused = 0;
    for ((column, key), entry) in &built.model {
        match read(&store, *column, key) {
            Ok(found) => assert_eq!(found, entry.value, "key {key:?}"),
            Err(StoreError::Corruption { .. }) => refused += 1,
            Err(error) => panic!("key {key:?} failed: {error}"),
        }
    }
    assert!(refused > 0);
    for ((column, key), entry) in &built.model {
        if read(&store, *column, key).is_err() {
            match &entry.value {
                Some(value) => store.put(*column, key, value).expect("put"),
                None => store.delete(*column, key).expect("delete"),
            }
        }
    }
    store.clear_damage(ORDERED, b"", None).expect("clear");
    store.clear_damage(RANDOM, b"", None).expect("clear");
    assert_match(&store, &built.model);
    store.close().expect("close");
}

// a lost block costs its own span and nothing past it, and no key in it reads older, whichever block it was
#[test]
fn rebuild_never_reads_older() {
    for lost in 0..REBUILD_LOST_BLOCKS {
        let io = SimulatedIo::new(Seed(44));
        let store = ReelStore::open(io.clone(), &sized_config(REBUILD_BUDGET)).expect("open");
        hold_rebuilds(&store);
        store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
        let mut model = Model::new();
        for stage in [OLDEST, MIDDLE] {
            for column in COLUMNS {
                for number in 0..REBUILD_KEYS {
                    write(&store, &mut model, column, number, stage, false);
                }
            }
            seal(&store, &io);
        }
        store.close().expect("close");
        drop(store);

        let files = data_files(&io);
        assert_eq!(files.len(), 2);
        let heads: Vec<TailHead> = files.iter().map(|path| read_head(&io, path)).collect();
        let built = Built { io, model, files, heads };
        rot_first_tail(&built, MIDDLE);
        rot_second_tail(&built, MIDDLE);
        let mut at = built.heads[MIDDLE].data_end.0;
        for _block in 0..lost {
            let header = block_header(&built, at);
            at += BLOCK_HEADER_BYTES as u64 + u64::from(header.block_length) + u64::from(header.filter_length);
        }
        built.io.rot(&built.files[MIDDLE], FileOffset(at + BLOCK_HEADER_BYTES as u64 + 3), FLIP).expect("rot");

        let store = ReelStore::open(built.io.clone(), &sized_config(REBUILD_BUDGET)).expect("open");
        settle(&built.io);
        assert!(store.state.volumes[0].segments_rewritten.load(Ordering::Acquire) > 0, "block {lost}");
        let mut refused = 0;
        for ((column, key), entry) in &built.model {
            match read(&store, *column, key) {
                Ok(found) => assert_eq!(found, entry.value, "block {lost}, key {key:?}"),
                Err(StoreError::Corruption { .. }) => refused += 1,
                Err(error) => panic!("key {key:?} failed: {error}"),
            }
        }
        assert!(refused > 0 && refused < REBUILD_KEYS / 2, "block {lost}: {refused} refused");
        store.close().expect("close");
    }
}

fn block_header(built: &Built, at: u64) -> BlockHeader {
    let file = built.io.open_read_only(&built.files[OLDEST]).expect("open");
    let mut bytes = [0; BLOCK_HEADER_BYTES];
    file.read_exact_at(FileOffset(at), &mut bytes).expect("read");
    built.io.close_file(file);
    read_block_header(&bytes).expect("a header")
}

// the family of `tail_then_cut`: a tail from a seal that never finished, read as final. It stops speaking once rows go on past it, so a power cut that brings it back to the file's
// end hides none of them
#[test]
fn stale_tail_hides_no_rows() {
    let mut runs = 0;
    for first in 0..SEAL_CRASH_POINTS {
        for second in 0..STALE_TAIL_SECOND_POINTS {
            runs += u64::from(stale_tail_run(STALE_TAIL_SEED, first, second, |_io| {}));
        }
    }
    assert!(runs > 0);
}

// rot both index copies of every segment that still reads a tail
fn rot_tailed_indexes(io: &SimulatedIo) {
    let superblock = Superblock::read(io, &PathBuf::from(ROOT)).expect("read").expect("superblock");
    for path in data_files(io) {
        let file = io.open_read_only(&path).expect("open");
        let mut columns = Vec::new();
        let head = FooterReader::new().read_tail(&file, superblock.record_key, &mut columns).expect("tail");
        io.close_file(file);
        if let Some(head) = head {
            io.rot(&path, FileOffset(head.index_at.0 + 2), FLIP).expect("rot");
            io.rot(&path, FileOffset(head.second_index_at.0 + 2), FLIP).expect("rot");
        }
    }
}

// a stale tail over a footer that reads as damaged still stops speaking, so the rows file comes back whole
#[test]
fn stale_tail_over_rot() {
    let mut runs = 0;
    for first in 0..SEAL_CRASH_POINTS {
        for second in 0..STALE_TAIL_SECOND_POINTS {
            runs += u64::from(stale_tail_run(STALE_TAIL_SEED, first, second, rot_tailed_indexes));
        }
    }
    assert!(runs > 0);
}

fn hold_rebuilds(store: &Store) {
    for volume in &store.state.volumes {
        volume.holds.rebuild.store(true, Ordering::Release);
    }
}

fn data_lengths(io: &SimulatedIo) -> Vec<ByteCount> {
    let lengths = data_files(io).into_iter().map(|path| {
        let file = io.open_read_only(&path).expect("open");
        let length = file_length_of(&file);
        io.close_file(file);
        length
    });
    lengths.collect()
}

// a seal stopped at any of its data syncs loses no flushed write through a reopen and a power cut
#[test]
fn tail_sync() {
    for skip in 0..SEAL_DATA_SYNCS {
        let io = SimulatedIo::new(Seed(skip));
        let store = open(&io);
        store.put(ORDERED, b"key", b"value").expect("put");
        store.flush().expect("flush");
        store.seal_now();
        io.fail_file_sync("data", OperationCount(skip));
        settle(&io);
        io.kill();
        drop(store);

        let store = open(&io);
        store.flush().expect("flush");
        io.power_cut();
        drop(store);

        let store = open(&io);
        assert_eq!(read(&store, ORDERED, b"key").expect("get"), Some(b"value".to_vec()), "skip {skip}");
        store.close().expect("close");
    }
}

fn wide_config(budget: ByteCount) -> StoreConfig {
    let mut config = config();
    config.memory_budget = budget;
    config.columns = (1..=WIDE_COLUMNS)
        .map(|id| ColumnConfig {
            id: ColumnId(id),
            name: format!("column-{id}"),
            key_shape: KeyShape::Variable { most: 128 },
            codec: Codec::None,
            volume: VolumeId(0),
            complete_rows: None,
            least_value: None,
        })
        .collect();
    config
}

// more rows than the plan and no summary: every row comes back, so open drops none and no rank moves
#[test]
fn rows_fit() {
    let io = SimulatedIo::new(Seed(39));
    let store = ReelStore::open(io.clone(), &wide_config(WIDE_BUDGET)).expect("open");
    for segment in 0..WIDE_SEGMENTS {
        for id in 1..=WIDE_COLUMNS {
            // a key of its own a segment, so compaction leaves every one
            store.put(ColumnId(id), &key_of_segment(segment), b"value").expect("put");
        }
        seal(&store, &io);
    }
    store.close().expect("close");
    drop(store);
    let paths = SummaryPaths::new(Path::new(ROOT));
    io.remove_file(&paths.current).expect("remove");
    io.remove_file(&paths.second).expect("remove");

    let store = ReelStore::open(io.clone(), &wide_config(LEAST_BUDGET)).expect("open");

    let rows = hold(&store.state.volumes[0].summary);
    assert!(rows.bytes().len() as u64 > PLANNED_ROOM_AT_LEAST);
    assert_eq!(rows.rows().count() as u64, WIDE_SEGMENTS);
    drop(rows);
    assert_eq!(store.dropped_summary_rows(), 0);
    store.close().expect("close");
}

// a row lost in one copy comes from the other, so a segment that loses its tails keeps its ranges
#[test]
fn one_copy() {
    let built = build(40);
    let paths = SummaryPaths::new(Path::new(ROOT));
    let at = row_offset(&built.io, &paths, built.heads[MIDDLE].segment);
    built.io.rot(&paths.current, FileOffset((at + ROW_HEAD_BYTES + 2) as u64), FLIP).expect("rot");
    rot_first_tail(&built, MIDDLE);
    rot_second_tail(&built, MIDDLE);

    let store = open(&built.io);

    assert!(assert_guarded(&store, &built, Cover::Edges(MIDDLE)) > 0);
    store.close().expect("close");
}

// a bad sector under a covered group keeps the later groups: the search goes on sector by sector
#[test]
fn bad_rows() {
    let io = SimulatedIo::new(Seed(41));
    let store = open(&io);
    for number in 0..SECTOR_ROWS {
        store.put(ORDERED, &key_of(ORDERED, number), b"value").expect("put");
    }
    store.flush().expect("flush");
    store.put(ORDERED, b"later", b"value").expect("put");
    store.flush().expect("flush");
    io.kill();
    drop(store);
    io.bad_sector(&rows_files(&io)[0], FileOffset(20)).expect("bad");

    let store = open(&io);

    assert_eq!(store.damaged_segments(), 1);
    assert!(matches!(read(&store, ORDERED, b"later"), Err(StoreError::Corruption { .. })));
    store.clear_damage(ORDERED, b"", None).expect("clear");
    assert_eq!(read(&store, ORDERED, b"later").expect("get"), Some(b"value".to_vec()));
    store.close().expect("close");
}

// rot found at a later open takes that open's counter, also over spans a caller cleared
#[test]
fn new_rot() {
    let io = rotted_rows(42);
    let store = open(&io);
    store.clear_damage(ORDERED, b"", None).expect("clear");
    store.put(ORDERED, b"key", b"older").expect("put");
    store.flush().expect("flush");
    store.put(ORDERED, b"key", b"newer").expect("put");
    store.flush().expect("flush");
    store.put(ORDERED, b"other", b"value").expect("put");
    store.flush().expect("flush");
    let file = &rows_files(&io)[0];
    let length = file_length(&io, file);
    store.close().expect("close");
    drop(store);

    // the group of the newer put sits a few groups back from the end
    io.rot(file, FileOffset(length.0 - GROUPS_BACK), FLIP).expect("rot");
    let store = open(&io);

    assert!(matches!(read(&store, ORDERED, b"key"), Err(StoreError::Corruption { .. })));
    store.close().expect("close");
}

// a seal keeps a damaged open segment's spans when the summary is full, giving up a clean row for them
#[test]
fn seal_spans() {
    let io = rotted_rows(43);
    let store = open(&io);
    // long keys in both columns make the sealed row bigger than the open row it replaces
    for column in COLUMNS {
        store.put(column, &[b'k'; 100], b"value").expect("put");
        store.put(column, &[b'z'; 100], b"value").expect("put");
    }
    let volume = &store.state.volumes[0];
    let spans = hold(&volume.damaged)[0];
    let clean = SegmentFacts {
        segment: SegmentNumber(spans.0 + 1_000),
        ..SegmentFacts::default()
    };
    while hold(&volume.summary).push(&clean, [].into_iter(), [].into_iter()) {}

    seal(&store, &io);
    store.close().expect("close");
    drop(store);

    let store = open(&io);
    assert_eq!(store.damaged_segments(), 1);
    assert!(matches!(read(&store, ORDERED, b"later"), Err(StoreError::Corruption { .. })));
    store.close().expect("close");
}

// a clear works when a damaged segment has no row, and when a split outgrows the room
#[test]
fn clear_grows() {
    let built = build(44);
    rot_first_tail(&built, MIDDLE);
    rot_second_tail(&built, MIDDLE);
    let store = open(&built.io);
    let volume = &store.state.volumes[0];
    hold(&volume.summary).remove(built.heads[MIDDLE].segment);
    let heap = hold(&volume.summary).heap_bytes();
    let clean = SegmentFacts {
        segment: SegmentNumber(1),
        is_damaged: true,
        ..SegmentFacts::default()
    };
    while hold(&volume.summary).push(&clean, [].into_iter(), [].into_iter()) {}

    let split = key_of(ORDERED, 130);
    let end = key_of(ORDERED, 131);
    store.clear_damage(ORDERED, &split, Some(&end)).expect("clear");

    assert!(hold(&volume.summary).heap_bytes() >= heap);
    assert!(read(&store, ORDERED, &split).is_ok());
    assert!(matches!(read(&store, ORDERED, &end), Err(StoreError::Corruption { .. })));
    store.close().expect("close");
}

// what open shows stays through a power cut right after it, with no flush from the caller
#[test]
fn open_covers() {
    let io = SimulatedIo::new(Seed(45));
    let store = open(&io);
    store.put(ORDERED, b"key", b"older").expect("put");
    seal(&store, &io);
    store.delete(ORDERED, b"key").expect("delete");
    io.advance_clock(ROW_PERIOD);
    settle(&io);
    io.kill();
    drop(store);

    let store = open(&io);
    assert_eq!(read(&store, ORDERED, b"key").expect("get"), None);
    io.power_cut();
    drop(store);

    let store = open(&io);
    assert_eq!(read(&store, ORDERED, b"key").expect("get"), None);
    store.close().expect("close");
}

// where each group of a rows file starts
fn group_starts(io: &SimulatedIo, path: &Path) -> Vec<u64> {
    let superblock = Superblock::read(io, Path::new(ROOT)).expect("read").expect("superblock");
    let file = io.open_read_only(path).expect("open");
    let mut bytes = vec![0; file_length_of(&file).0 as usize];
    file.read_exact_at(FileOffset(0), &mut bytes).expect("read");
    io.close_file(file);

    let mut starts = Vec::new();
    let mut at = 0;
    while let Some(group) = bytes.get(at..).and_then(|rest| parse_group(rest, superblock.record_key)) {
        starts.push(at as u64);
        at += group.total_length.0 as usize;
    }
    starts
}

// three flushed writes, each in its group with a mark group after it
fn three_groups(seed: u64) -> (SimulatedIo, Vec<u64>) {
    let io = SimulatedIo::new(Seed(seed));
    let store = open(&io);
    for key in [b"first", b"midst", b"third"] {
        store.put(ORDERED, key, b"value").expect("put");
        store.flush().expect("flush");
    }
    io.kill();
    drop(store);
    let starts = group_starts(&io, &rows_files(&io)[0]);
    (io, starts)
}

fn reopen(store: Store) {
    store.close().expect("close");
}

// rot that grows past the region an earlier open ranked takes the new open's counter
#[test]
fn grown_rot() {
    let (io, starts) = three_groups(46);
    assert!(starts.len() >= 6);
    let rows = rows_files(&io)[0].clone();
    io.rot(&rows, FileOffset(starts[2] + 20), FLIP).expect("rot");

    let store = open(&io);
    store.clear_damage(ORDERED, b"", None).expect("clear");
    assert_eq!(read(&store, ORDERED, b"third").expect("get"), Some(b"value".to_vec()));
    reopen(store);

    // the next group goes bad too, so the region starts where it did and ends further on
    io.rot(&rows, FileOffset(starts[3] + 20), FLIP).expect("rot");
    let store = open(&io);

    assert!(matches!(read(&store, ORDERED, b"third"), Err(StoreError::Corruption { .. })));
    store.close().expect("close");
}

// a second bad region takes the new open's counter, though the first one is known
#[test]
fn second_region() {
    let (io, starts) = three_groups(47);
    let rows = rows_files(&io)[0].clone();
    io.rot(&rows, FileOffset(starts[0] + 20), FLIP).expect("rot");

    let store = open(&io);
    store.clear_damage(ORDERED, b"", None).expect("clear");
    reopen(store);

    io.rot(&rows, FileOffset(starts[2] + 20), FLIP).expect("rot");
    let store = open(&io);

    assert!(matches!(read(&store, ORDERED, b"third"), Err(StoreError::Corruption { .. })));
    store.close().expect("close");
}

// rot in the first group is new rot, whatever row the segment already has
#[test]
fn first_region() {
    let (io, starts) = three_groups(48);
    let number = SegmentNumber(data_number(&rows_files(&io)[0]));
    let paths = SummaryPaths::new(Path::new(ROOT));
    let mut rows = SummaryRows::new(ByteCount(4_096));
    let facts = SegmentFacts {
        segment: number,
        is_damaged: true,
        ..SegmentFacts::default()
    };
    assert!(rows.push(&facts, [].into_iter(), [].into_iter()));
    write_summary(&io, &paths, WritePlace(0), rows.highest_forward, rows.bytes()).expect("write");
    io.rot(&rows_files(&io)[0], FileOffset(starts[0] + 20), FLIP).expect("rot");

    let store = open(&io);

    assert!(matches!(read(&store, ORDERED, b"third"), Err(StoreError::Corruption { .. })));
    store.close().expect("close");
}

fn data_number(path: &Path) -> u64 {
    let stem = path.file_stem().expect("stem").to_string_lossy().to_string();
    u64::from_str_radix(&stem, 16).expect("number")
}

// a rank at the counter stands above every place open found, also when the roots saved a stale limit
#[test]
fn stale_limit() {
    let built = build(49);
    rot_row(&built, built.heads[MIDDLE].segment);
    rot_first_tail(&built, MIDDLE);
    rot_second_tail(&built, MIDDLE);
    let mut stale = Superblock::read(&built.io, Path::new(ROOT)).expect("read").expect("superblock");
    stale.place_limit = WritePlace(0);
    stale.write(&built.io, &SuperblockPaths::new(Path::new(ROOT)), &mut Vec::new()).expect("write");

    let store = open(&built.io);

    assert!(assert_guarded(&store, &built, Cover::Whole(STAGE_LOST)) > 0);
    store.close().expect("close");
}

// a bad sector in the middle or at the end of a covered group, or two of them, keeps the later groups
#[test]
fn bad_spots() {
    for spots in [&[600_u64][..], &[1_100], &[20, 600]] {
        let io = SimulatedIo::new(Seed(50));
        let store = open(&io);
        for number in 0..SECTOR_ROWS * 2 {
            store.put(ORDERED, &key_of(ORDERED, number), b"value").expect("put");
        }
        store.flush().expect("flush");
        store.put(ORDERED, b"later", b"value").expect("put");
        store.flush().expect("flush");
        io.kill();
        drop(store);
        let rows = rows_files(&io)[0].clone();
        assert!(group_starts(&io, &rows)[1] > 1_200);
        for spot in spots {
            io.bad_sector(&rows, FileOffset(*spot)).expect("bad");
        }

        let store = open(&io);

        assert_eq!(store.damaged_segments(), 1, "spots {spots:?}");
        store.clear_damage(ORDERED, b"", None).expect("clear");
        assert_eq!(read(&store, ORDERED, b"later").expect("get"), Some(b"value".to_vec()), "spots {spots:?}");
        store.close().expect("close");
    }
}

// a clear whose save fails says so and changes nothing, and the same clear then goes through
#[test]
fn clear_save() {
    let built = build(51);
    rot_first_tail(&built, MIDDLE);
    rot_second_tail(&built, MIDDLE);
    let store = open(&built.io);
    let inside = key_of(ORDERED, 110);

    built.io.fail_file_sync("new", OperationCount(0));
    assert!(store.clear_damage(ORDERED, b"", None).is_err());
    assert!(matches!(read(&store, ORDERED, &inside), Err(StoreError::Corruption { .. })));

    store.clear_damage(ORDERED, b"", None).expect("clear");
    assert!(read(&store, ORDERED, &inside).is_ok());
    store.close().expect("close");
}

// a clear that would split a span on a full pool comes back as its own error
#[test]
fn clear_full() {
    let built = build(52);
    rot_first_tail(&built, MIDDLE);
    rot_second_tail(&built, MIDDLE);
    let store = open(&built.io);
    let damage = &store.state.volumes[0].damage;
    let segment = built.heads[MIDDLE].segment;
    let mut number = 0;
    while damage.len() < MOST_DAMAGED_SPANS {
        damage.add(&DamagedRange {
            column: RANDOM,
            segment,
            highest_place: WritePlace(1),
            start: format!("{number:06}").into_bytes(),
            end: Some(format!("{number:06}z").into_bytes()),
        });
        number += 1;
    }

    let split = store.clear_damage(RANDOM, b"000002a", Some(b"000002b"));

    assert!(matches!(split, Err(StoreError::DamagePoolFull { .. })));
    store.close().expect("close");
}

// a read the process gets wrong is no damage: open fails, saves nothing, and the next open is clean
#[test]
fn not_damage() {
    let built = build(53);
    built.io.fail_reads(OperationCount(1));
    assert!(ReelStore::open(built.io.clone(), &config()).is_err());
    built.io.fail_reads(OperationCount(0));

    let store = open(&built.io);

    assert_eq!(store.damaged_segments(), 0);
    assert_match(&store, &built.model);
    store.close().expect("close");
}

// write the newer copy alone, as a crash between the two copies leaves it
fn newer_copy(io: &SimulatedIo, paths: &SummaryPaths, mark: WritePlace, rows: &[u8]) {
    write_copy(io, paths, &paths.current, &copy_ends(WritePlace(mark.0 + 1), FORWARD_UNKNOWN, rows), rows).expect("write");
}

fn assert_all_refused(store: &Store, built: &Built) {
    for (column, key) in built.model.keys() {
        let got = read(store, *column, key);
        assert!(matches!(got, Err(StoreError::Corruption { .. })), "key {key:?} answered {got:?}");
    }
}

// rot in the newer copy's row, head or body, keeps the older row's facts and damages the columns at the counter
#[test]
fn newer_rot() {
    for stage in [OLDEST, MIDDLE, NEWEST] {
        for rot_at in [1, ROW_HEAD_BYTES + 2] {
            let built = build(60 + stage as u64);
            let (paths, rows, mark) = summary_rows(&built.io);
            let segment = built.heads[stage].segment;
            let older = rows.get(segment).expect("row").facts;
            newer_copy(&built.io, &paths, mark, rows.bytes());
            let at = row_offset(&built.io, &paths, segment);
            built.io.rot(&paths.current, FileOffset((at + rot_at) as u64), FLIP).expect("rot");

            let store = open(&built.io);

            assert_eq!(store.damaged_segments(), 1, "stage {stage}, rot at {rot_at}");
            assert_all_refused(&store, &built);
            let summary = hold(&store.state.volumes[0].summary);
            let facts = summary.get(segment).expect("row").facts;
            assert_eq!((facts.places, facts.seal_mark), (older.places, older.seal_mark));
            assert_eq!(facts.damage_place, store.state.volumes[0].order.mark());
            drop(summary);
            store.close().expect("close");
        }
    }
}

// the row the newer copy lost may be the first the segment had, so no row is no proof of no spans
#[test]
fn only_newer() {
    let built = build(63);
    let (paths, mut rows, mark) = summary_rows(&built.io);
    let segment = built.heads[MIDDLE].segment;
    let full = rows.bytes().to_vec();
    rows.remove(segment);
    write_summary(&built.io, &paths, mark, rows.highest_forward, rows.bytes()).expect("write");
    newer_copy(&built.io, &paths, mark, &full);
    let at = row_offset(&built.io, &paths, segment);
    built.io.rot(&paths.current, FileOffset((at + ROW_HEAD_BYTES + 2) as u64), FLIP).expect("rot");

    let store = open(&built.io);

    assert_eq!(store.damaged_segments(), 1);
    assert_all_refused(&store, &built);
    assert_eq!(hold(&store.state.volumes[0].summary).get(segment).expect("row").facts.places, built.heads[MIDDLE].places);
    store.close().expect("close");
}

// a segment the newer copy lacks takes the older row with its walk started over, and two marks alone damage nothing
#[test]
fn only_older() {
    let built = build(64);
    let (paths, mut rows, mark) = summary_rows(&built.io);
    let segment = built.heads[MIDDLE].segment;
    let older = rows.get(segment).expect("row").facts;
    rows.remove(segment);
    newer_copy(&built.io, &paths, mark, rows.bytes());

    let store = open(&built.io);

    assert_eq!(store.damaged_segments(), 0);
    let taken = hold(&store.state.volumes[0].summary).get(segment).expect("row").facts;
    assert!(taken.is_backward);
    assert_eq!(taken.walked_blocks, 0);
    assert_eq!(SegmentFacts { is_backward: older.is_backward, walked_blocks: older.walked_blocks, ..taken }, older);
    assert_match(&store, &built.model);
    store.close().expect("close");
}

// rows that reach a second sector: the last group runs over the sector edge and its mark group sits past it
fn sector_end(seed: u64) -> (SimulatedIo, PathBuf, ByteCount) {
    let io = SimulatedIo::new(Seed(seed));
    let store = open(&io);
    store.put(ORDERED, b"older", b"first").expect("put");
    store.flush().expect("flush");
    store.put(ORDERED, b"older", b"second").expect("put");
    for number in 0..SECTOR_KEYS {
        store.put(ORDERED, &key_of(ORDERED, number), b"value").expect("put");
    }
    store.flush().expect("flush");
    io.kill();
    drop(store);

    let rows = rows_files(&io)[0].clone();
    let length = file_length(&io, &rows);
    let starts = group_starts(&io, &rows);
    let (last, mark) = (starts[starts.len() - 2], starts[starts.len() - 1]);
    assert!(last / SECTOR_BYTES < (length.0 - 1) / SECTOR_BYTES, "groups at {starts:?} of {length:?}");
    assert_eq!(mark / SECTOR_BYTES, (length.0 - 1) / SECTOR_BYTES, "groups at {starts:?} of {length:?}");
    (io, rows, length)
}

// one bad sector over the last group's end and its mark group refuses, through later opens, and never shows the older value
#[test]
fn bad_end() {
    let (io, rows, length) = sector_end(65);
    io.bad_sector(&rows, FileOffset(length.0 - 1)).expect("bad");

    for _open in 0..2 {
        let store = open(&io);

        assert_eq!(store.damaged_segments(), 1);
        assert!(matches!(read(&store, ORDERED, b"older"), Err(StoreError::Corruption { .. })));
        store.close().expect("close");
    }
}

// the same two groups with readable bytes that fail their check cut as a torn end
#[test]
fn rot_end() {
    let (io, rows, length) = sector_end(66);
    let starts = group_starts(&io, &rows);
    io.rot(&rows, FileOffset(starts[starts.len() - 2] + 20), FLIP).expect("rot");
    io.rot(&rows, FileOffset(length.0 - 3), FLIP).expect("rot");

    let store = open(&io);

    assert_eq!(store.damaged_segments(), 0);
    assert_eq!(read(&store, ORDERED, b"older").expect("get"), Some(b"first".to_vec()));
    store.close().expect("close");
}

// an open segment's row may be the only trace of a tail open cut, so losing it damages the segment again
#[test]
fn open_lost() {
    let (io, rows_file, length) = sector_end(67);
    io.bad_sector(&rows_file, FileOffset(length.0 - 1)).expect("bad");
    open(&io).close().expect("close");

    let (paths, mut rows, mark) = summary_rows(&io);
    let (segment, full) = (rows.rows().next().expect("row").facts.segment, rows.bytes().to_vec());
    rows.remove(segment);
    write_summary(&io, &paths, mark, rows.highest_forward, rows.bytes()).expect("write");
    newer_copy(&io, &paths, mark, &full);
    let at = row_offset(&io, &paths, segment);
    io.rot(&paths.current, FileOffset((at + ROW_HEAD_BYTES + 2) as u64), FLIP).expect("rot");

    let store = open(&io);

    assert_eq!(store.damaged_segments(), 1);
    assert!(matches!(read(&store, ORDERED, b"older"), Err(StoreError::Corruption { .. })));
    store.close().expect("close");
}

// rot that grows replaces the spans of the open before it, so open after open the pool holds one span per column
#[test]
fn ranked_again() {
    let (io, starts) = three_groups(68);
    let rows = rows_files(&io)[0].clone();
    for start in &starts[2..] {
        io.rot(&rows, FileOffset(start + 20), FLIP).expect("rot");

        let store = open(&io);

        assert_eq!(store.state.volumes[0].damage.len(), COLUMNS.len());
        store.close().expect("close");
    }
}

// a clear holds once open has cut the bad tail, since nothing is left there to read as new rot
#[test]
fn end_cleared() {
    let (io, rows, length) = sector_end(69);
    io.bad_sector(&rows, FileOffset(length.0 - 1)).expect("bad");
    let store = open(&io);
    store.clear_damage(ORDERED, b"", None).expect("clear");
    store.clear_damage(RANDOM, b"", None).expect("clear");
    assert_eq!(read(&store, ORDERED, b"older").expect("get"), Some(b"first".to_vec()));
    reopen(store);

    let store = open(&io);

    assert_eq!(store.state.volumes[0].damage.len(), 0);
    assert_eq!(read(&store, ORDERED, b"older").expect("get"), Some(b"first".to_vec()));
    store.close().expect("close");
}

// a bad sector over the newer header after a crash between the copies still compares marks, through the trailer
#[test]
fn trailer_mark() {
    let built = build(70);
    let (paths, rows, mark) = summary_rows(&built.io);
    newer_copy(&built.io, &paths, mark, rows.bytes());
    built.io.bad_sector(&paths.current, FileOffset(0)).expect("bad");

    let store = open(&built.io);

    assert!(store.damaged_segments() > 0);
    assert_all_refused(&store, &built);
    store.close().expect("close");
}

// a bad header with equal marks costs nothing, whether it took the rows beside it or not
#[test]
fn equal_trailer() {
    for is_bad_sector in [false, true] {
        let built = build(71);
        let paths = SummaryPaths::new(Path::new(ROOT));
        if is_bad_sector {
            built.io.bad_sector(&paths.current, FileOffset(0)).expect("bad");
        } else {
            built.io.rot(&paths.current, FileOffset(MARK_AT), FLIP).expect("rot");
        }

        let store = open(&built.io);

        assert_eq!(store.damaged_segments(), 0);
        assert_match(&store, &built.model);
        store.close().expect("close");
    }
}

// a copy with no mark at either end leaves the other's rows as they are, and a row it does hold still counts
#[test]
fn no_marks() {
    for is_newer in [true, false] {
        let built = build(72);
        let (paths, rows, mark) = summary_rows(&built.io);
        newer_copy(&built.io, &paths, mark, rows.bytes());
        let (markless, other) = if is_newer { (&paths.current, &paths.second) } else { (&paths.second, &paths.current) };
        built.io.rot(markless, FileOffset(MARK_AT), FLIP).expect("rot");
        let length = file_length(&built.io, markless);
        built.io.rot(markless, FileOffset(length.0 - TRAILER_MARK_BACK), FLIP).expect("rot");
        let at = row_offset(&built.io, &paths, built.heads[MIDDLE].segment);
        built.io.rot(other, FileOffset((at + ROW_HEAD_BYTES + 2) as u64), FLIP).expect("rot");

        let store = open(&built.io);

        assert_eq!(store.damaged_segments(), 0);
        assert!(hold(&store.state.volumes[0].summary).get(built.heads[MIDDLE].segment).is_some());
        assert_match(&store, &built.model);
        store.close().expect("close");
    }
}

// a crash at any step of a save leaves every copy with one mark at both ends, since a copy lands by rename
#[test]
fn save_crash() {
    for crash_at in 0..SAVE_CRASH_POINTS {
        let io = SimulatedIo::new(Seed(crash_at));
        let store = open(&io);
        store.put(ORDERED, b"key", b"value").expect("put");
        store.close().expect("close");
        drop(store);
        let (paths, rows, mark) = summary_rows(&io);

        io.arm_crash(OperationCount(crash_at));
        let _saved = write_summary(&io, &paths, WritePlace(mark.0 + 1), rows.highest_forward, rows.bytes());
        io.power_cut();

        for path in [&paths.current, &paths.second] {
            let copy = read_copy(&io, path).expect("read").expect("a copy");
            assert!(copy.header_mark.is_some(), "crash at {crash_at}");
            assert_eq!(copy.header_mark, copy.trailer_mark, "crash at {crash_at}");
        }
    }
}

fn key_of_segment(segment: u64) -> Vec<u8> {
    let mut key = vec![b'k'; 40];
    key[..8].copy_from_slice(&segment.to_le_bytes());
    key
}

// get_many gives each key the answer a get gives it, so a bad record or a damaged span is that key's answer alone
fn assert_many_agrees(store: &Store, built: &Built) -> u64 {
    let mut refused = 0;
    for column in COLUMNS {
        let keys: Vec<&[u8]> = built.model.keys().filter(|(owner, _key)| *owner == column).map(|(_owner, key)| key.as_slice()).collect();
        let mut values = ValueList::new();
        store.get_many(column, &keys, &mut values).expect("get_many");
        for (index, key) in keys.iter().enumerate() {
            let answer = values.answer(index).expect("an answer");
            match read(store, column, key) {
                Ok(Some(value)) => assert_eq!(answer, ValueAnswer::Found(&value)),
                Ok(None) => assert_eq!(answer, ValueAnswer::Missing),
                Err(StoreError::Corruption { .. }) => {
                    assert_eq!(answer, ValueAnswer::Corrupt);
                    refused += 1;
                },
                Err(error) => panic!("get: {error}"),
            }
        }
    }
    refused
}

// a record no read can give back is that key's answer, and the keys around it still answer
#[test]
fn many_bad_record() {
    let built = build(61);
    built.io.bad_sector(&built.files[MIDDLE], FileOffset(3)).expect("bad");

    let store = open(&built.io);

    assert!(assert_many_agrees(&store, &built) > 0);
    store.close().expect("close");
}

// a key inside a damaged span answers the same way
#[test]
fn many_damaged_span() {
    let built = build(62);
    rot_first_tail(&built, MIDDLE);
    rot_second_tail(&built, MIDDLE);

    let store = open(&built.io);

    assert!(assert_many_agrees(&store, &built) > 0);
    store.close().expect("close");
}
