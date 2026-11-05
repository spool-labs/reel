//! Rot in a sealed segment: one copy lost costs nothing, and reads past a damaged range refuse to answer

use std::collections::BTreeMap;
use std::io::IoSlice;
use std::path::{Path, PathBuf};

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::guard::hold;
use reel::io::simulated::file_image::BitMask;
use reel::io::simulated::seeded_random::Seed;
use reel::io::simulated::simulated_io::SimulatedIo;
use reel::io::{Io, IoFile};
use reel::record::codec::Codec;
use reel::segment::footer_reader::FooterReader;
use reel::segment::row_block::BLOCK_HEADER_BYTES;
use reel::segment::segment_tail::{SECOND_TAIL_DISTANCE, SegmentNumber, TailHead};
use reel::segment::summary::{
    ROW_LENGTH_BYTES, SUMMARY_HEADER_BYTES, SummaryPaths, SummaryRows, read_row, read_summary,
};
use reel::superblock::Superblock;
use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, Nanoseconds, VolumeId};

const ROOT: &str = "/disk/damage";
const ORDERED: ColumnId = ColumnId(1);
const RANDOM: ColumnId = ColumnId(2);
const COLUMNS: [ColumnId; 2] = [ORDERED, RANDOM];
const BUDGET: ByteCount = ByteCount(8 * 1024 * 1024);
const SETTLE_STEPS: u64 = 2_000_000;
const SETTLE_TICK: Nanoseconds = Nanoseconds(1_000_000);
const KEY_COUNT: u64 = 300;
const SPREAD: u64 = 0x9E37_79B9_7F4A_7C15;
const FLIP: BitMask = BitMask(1);
const OLDEST: usize = 0;
const MIDDLE: usize = 1;
const NEWEST: usize = 2;
const SEGMENTS: usize = 3;
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
    StoreConfig {
        memory_budget: BUDGET,
        volumes: vec![VolumeConfig {
            name: "meta".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
        }],
        columns: vec![
            ColumnConfig {
                id: ORDERED,
                name: "ordered".to_string(),
                key_shape: KeyShape::Variable { most: 64 },
                codec: Codec::None,
                volume: VolumeId(0),
            },
            ColumnConfig {
                id: RANDOM,
                name: "random".to_string(),
                key_shape: KeyShape::Variable { most: 64 },
                codec: Codec::None,
                volume: VolumeId(0),
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

fn rot_row(built: &Built, segment: SegmentNumber) {
    let paths = SummaryPaths::new(Path::new(ROOT));
    let file = built.io.open_read_only(&paths.current).expect("open");
    let mut bytes = vec![0; file_length_of(&file).0 as usize];
    file.read_exact_at(FileOffset(0), &mut bytes).expect("read");
    built.io.close_file(file);

    let mut at = SUMMARY_HEADER_BYTES;
    loop {
        let (length, row) = read_row(&bytes[at..]).expect("the row is in the summary");
        if row.expect("row").facts.segment == segment {
            built.io.rot(&paths.current, FileOffset((at + ROW_LENGTH_BYTES + 2) as u64), FLIP).expect("rot");
            return;
        }
        at += length;
    }
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

// both index copies lost: the same ranges, and the tail still reads
#[test]
fn both_indexes() {
    let built = build(5);
    rot_indexes(&built, MIDDLE);

    let store = open(&built.io);

    assert_eq!(store.damaged_segments(), 1);
    assert!(assert_guarded(&store, &built, Cover::Edges(MIDDLE)) > 0);
    store.close().expect("close");
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

// a put or delete inside a range lands above it and serves, also after another open
#[test]
fn write_above() {
    let mut built = build(11);
    rot_first_tail(&built, NEWEST);
    rot_second_tail(&built, NEWEST);

    let store = open(&built.io);
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
    assert!(store.state.read_point() >= built.heads[NEWEST].places.highest);
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
    let mark = store.state.read_point();
    store.close().expect("close");
    drop(store);

    let mut rows = SummaryRows::new(ByteCount(0));
    let saved = read_summary(&built.io, &SummaryPaths::new(Path::new(ROOT)), &mut rows).expect("read");

    assert_eq!(saved, Some(mark));
}
