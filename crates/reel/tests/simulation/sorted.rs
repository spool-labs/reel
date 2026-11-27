//! Sorted columns: an index holds each sealed key's newest place, so a get reads no footers, through seals, merges, covers and opens

use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::seed_sets::seeds;

use reel::config::{ColumnConfig, ColumnIndex, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::compact_job::CompactJob;
use reel::engine::open_segment::SegmentPhase;
use reel::engine::reel_store::ReelStore;
use reel::engine::rewrite_job::RewritePhase;
use reel::guard::hold;
use reel::index::sorted_keys::KeyEntry;
use reel::record::codec::Codec;
use reel::engine::store_scan::StoreScan;
use reel::{ScanRange, from_key};
use reel_core::column_id::ColumnId;
use reel_core::direction::Direction;
use reel_core::scan::ScanStep;
use reel_core::maintenance_report::MaintenanceReport;
use reel::segment::segment_tail::SegmentNumber;
use reel_core::units::{ByteCount, DeathMark, Nanoseconds, RowCount, VolumeId, WritePlace};
use reel_core::write_batch::WriteBatch;
use reel_sim::seeded_random::{Seed, SeededRandom};
use reel_sim::simulated_io::SimulatedIo;

const ROOT: &str = "/disk/sorted";
const COLUMN: ColumnId = ColumnId(1);
const WIDTH: u16 = 16;
const BUDGET: ByteCount = ByteCount(128 * 1024 * 1024);
const HELD_BUDGET: ByteCount = ByteCount(1024 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 400_000;
const KEYS: u64 = 2_000;
const CAPACITY: RowCount = RowCount(100_000);
const SMALL_CAPACITY: RowCount = RowCount(300);
const CHURN_SEEDS: u64 = 6;
const CHURN_ROUNDS: u64 = 8;
const CHURN_WRITES: u64 = 600;
const CHURN_KEYS: u64 = 1_500;
const CHURN_CAPACITY: RowCount = RowCount(900);
const EXTRA_KEYS: u64 = 32_000;
const COVER_ONE_IN: u64 = 40;
const MARK: DeathMark = DeathMark(1_000_000);
const SCAN_STARTS: u64 = 12;
const WANTED_ROWS: u64 = 10;
const AHEAD: usize = 500;
const SCAN_TAKE: usize = 40;
const TOLD_COUNTS: [usize; 3] = [100, 256, 300];
const MERGE_KEYS: u64 = 15_000;
const CUE_ROUNDS: u64 = 3;
const COVERED: usize = 40;
const LATE_KEYS: u64 = 100_000;

type Store = ReelStore<SimulatedIo>;
type Model = BTreeMap<Vec<u8>, Vec<u8>>;
type Rows = Vec<(Vec<u8>, Vec<u8>)>;

thread_local! {
    // the budget a store these sims open runs at, which a full one raises so the leaves hold values
    static SIM_BUDGET: std::cell::Cell<ByteCount> = const { std::cell::Cell::new(BUDGET) };
    // the codec the column declares, which a compressed run sets so scans restore what their leaves hold
    static SIM_CODEC: std::cell::Cell<Codec> = const { std::cell::Cell::new(Codec::None) };
}

// every store these sims open gives its index exactly the rows it declared, so a small one fills and marks
fn config(capacity: RowCount) -> StoreConfig {
    reel::engine::engine_sizing::EXACT_INDEX_ROWS.with(|exact| exact.set(true));
    StoreConfig {
        memory_budget: SIM_BUDGET.with(std::cell::Cell::get),
        max_memory: None,
        threads: crate::shape::threads(),
        volumes: vec![VolumeConfig {
            name: "sorted".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(), open_segments: None, segment_bytes: None, lifetime_groups: None,
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Fixed { width: WIDTH },
            codec: SIM_CODEC.with(std::cell::Cell::get),
            volume: VolumeId(0),
            index: ColumnIndex::Sorted,
            least_rows: Some(capacity),
            least_value: None,
        }],
    }
}

fn open(io: &SimulatedIo, capacity: RowCount) -> Store {
    ReelStore::open(io.clone(), &config(capacity)).expect("open")
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
    panic!("jobs never settled");
}

// the volume's count of open rows, which lets a scan's heads skip the write index, is the rows its open segments hold
fn check_open_rows(store: &Store) {
    let volume = &store.state.volumes[0];
    let open: u64 = hold(&volume.claims).open.iter().map(|open| open.row_count.0).sum();
    assert_eq!(volume.rows_in_index.load(Ordering::Acquire), open, "the open row count drifted");
}

fn cool(store: &Store, io: &SimulatedIo) {
    let until = store.state.quiet_at(io.clock_reading());
    io.advance_clock(Nanoseconds(until.0.saturating_sub(io.clock_reading().0)));
    settle(io);
    check_open_rows(store);
}

fn seal(store: &Store, io: &SimulatedIo) {
    store.seal_now();
    settle(io);
    cool(store, io);
}

// every open segment, the fullest first, so nothing stays in the write index
fn seal_all(store: &Store, io: &SimulatedIo) {
    while hold(&store.state.volumes[0].claims).open.iter().any(|open| open.row_count.0 > 0) {
        seal(store, io);
    }
}

fn key(number: u64) -> Vec<u8> {
    number.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes().iter().chain(&number.to_be_bytes()).copied().collect()
}

// a key whose first byte is the same for every number, so every key falls in one part of the index and a small count fills it
fn one_part_key(number: u64) -> Vec<u8> {
    let mut name = key(number);
    name[0] = 0x42;
    name
}

// a key in the one part past every churn key there, numbered in order
fn extra_key(number: u64) -> Vec<u8> {
    let mut name = vec![0xFF; WIDTH as usize - size_of::<u64>()];
    name[0] = 0x42;
    name.extend_from_slice(&number.to_be_bytes());
    name
}

fn read(store: &Store, name: &[u8]) -> Option<Vec<u8>> {
    let mut value = Vec::new();
    store.get(COLUMN, name, &mut value).expect("get").then_some(value)
}

fn report(store: &Store) -> MaintenanceReport {
    let mut report = MaintenanceReport::default();
    store.maintenance_report(&mut report);
    report
}

fn entry(store: &Store, name: &[u8]) -> Option<KeyEntry> {
    store.state.sorted_index(COLUMN).expect("an index").find(name)
}

// every get matches the model, and with every row sealed the index's live entries are the model's keys, each in a pinned segment
fn check(store: &Store, model: &Model, written: &[Vec<u8>]) {
    for name in written {
        assert_eq!(read(store, name).as_ref(), model.get(name), "{name:?}");
    }
    let index = store.state.sorted_index(COLUMN).expect("an index");
    assert_eq!(index.check(), Ok(()));
}

fn check_entries(store: &Store, model: &Model) {
    let index = store.state.sorted_index(COLUMN).expect("an index");
    let mut live = Vec::new();
    index.visit(|name, entry| {
        if !entry.is_grave {
            live.push(name.to_vec());
            assert!(store.state.volumes[0].table.pin(entry.segment).is_some(), "an entry points at a gone segment");
        }
    });
    assert_eq!(live, model.keys().cloned().collect::<Vec<_>>());
}

fn rows(mut scan: StoreScan<SimulatedIo>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let (mut key, mut value, mut found) = (Vec::new(), Vec::new(), Vec::new());
    loop {
        match scan.next_row(&mut key, &mut value).expect("step") {
            ScanStep::Found => found.push((key.clone(), value.clone())),
            ScanStep::Corrupt => panic!("a corrupt record"),
            ScanStep::End => return found,
        }
    }
}

// the first rows of a scan, read no further
fn first_rows(mut scan: StoreScan<SimulatedIo>, most: usize) -> Rows {
    let (mut key, mut value, mut found) = (Vec::new(), Vec::new(), Vec::new());
    while found.len() < most && scan.next_row(&mut key, &mut value).expect("step") == ScanStep::Found {
        found.push((key.clone(), value.clone()));
    }
    found
}

// a start among the model's keys with room rows past it in the walk's direction, or any churn key while the model is too small
fn pick_start(names: &[&Vec<u8>], random: &mut SeededRandom, room: usize, direction: Direction) -> Vec<u8> {
    if names.len() <= room {
        return key(random.below(CHURN_KEYS));
    }
    let at = random.below((names.len() - room) as u64) as usize;
    match direction {
        Direction::Ascending => names[at].clone(),
        Direction::Descending => names[at + room].clone(),
    }
}

// whole walks both ways and walks from keys of every width, against the model
fn check_scans(store: &Store, model: &Model, random: &mut SeededRandom) {
    let forward: Vec<(Vec<u8>, Vec<u8>)> = model.iter().map(|(key, value)| (key.clone(), value.clone())).collect();
    let names: Vec<&Vec<u8>> = forward.iter().map(|(name, _value)| name).collect();
    assert_eq!(rows(store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan")), forward);
    let top = vec![0xFF; WIDTH as usize + 1];
    let mut backward = forward.clone();
    backward.reverse();
    assert_eq!(rows(store.iter_from(COLUMN, &top, Direction::Descending).expect("scan")), backward);
    for _start in 0..SCAN_STARTS {
        let mut start = pick_start(&names, random, 0, Direction::Ascending);
        // a start a byte short or a byte long sorts beside the stored keys of its leading bytes
        match random.below(3) {
            0 => {
                start.pop();
            },
            1 => start.push(random.below(256) as u8),
            _ => {},
        }
        let up: Vec<_> = forward.iter().filter(|(name, _value)| *name >= start).take(SCAN_TAKE).cloned().collect();
        let found = first_rows(store.iter_from(COLUMN, &start, Direction::Ascending).expect("scan"), SCAN_TAKE);
        assert_eq!(found, up, "up from {start:?}");
        let down: Vec<_> = backward.iter().filter(|(name, _value)| *name <= start).take(SCAN_TAKE).cloned().collect();
        let found = first_rows(store.iter_from(COLUMN, &start, Direction::Descending).expect("scan"), SCAN_TAKE);
        assert_eq!(found, down, "down from {start:?}");
    }
    // one untold walk from a random key reads to the end, so its batch grows through every size
    let start = pick_start(&names, random, 0, Direction::Ascending);
    let up: Rows = forward.iter().filter(|(name, _value)| *name >= start).cloned().collect();
    assert_eq!(rows(store.iter_from(COLUMN, &start, Direction::Ascending).expect("scan")), up, "whole walk up from {start:?}");
    // a scan told its count takes that many rows, and one read on past the count returns the rows after them. Each starts with
    // that many rows ahead of it, so a scan that comes back short fails
    for told in TOLD_COUNTS {
        for direction in [Direction::Ascending, Direction::Descending] {
            let start = pick_start(&names, random, told + SCAN_TAKE, direction);
            let expected: Rows = match direction {
                Direction::Ascending => forward.iter().filter(|(name, _value)| *name >= start).take(told + SCAN_TAKE).cloned().collect(),
                Direction::Descending => backward.iter().filter(|(name, _value)| *name <= start).take(told + SCAN_TAKE).cloned().collect(),
            };
            assert!(names.len() <= told + SCAN_TAKE || expected.len() == told + SCAN_TAKE, "a start with too few rows ahead");
            let range = ScanRange { wanted: Some(RowCount(told as u64)), ..from_key(COLUMN, &start, direction) };
            let mut scan = store.iter(&range).expect("scan");
            let (mut name, mut value, mut found) = (Vec::new(), Vec::new(), Vec::new());
            let mut take = |found: &mut Rows, most: usize| {
                while found.len() < most && scan.next_row(&mut name, &mut value).expect("step") == ScanStep::Found {
                    found.push((name.clone(), value.clone()));
                }
            };
            take(&mut found, told);
            assert_eq!(found, expected.iter().take(told).cloned().collect::<Rows>(), "told {told} {direction:?} from {start:?}");
            take(&mut found, told + SCAN_TAKE);
            assert_eq!(found, expected, "past told {told} {direction:?} from {start:?}");
        }
    }
}

// sealed keys read back through the index, with overwrites and deletes across seals, through a clean reopen and a kill
#[test]
fn reads_through_the_index() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io, CAPACITY);
    let mut model = Model::new();
    let written: Vec<Vec<u8>> = (0..KEYS).map(key).collect();
    for number in 0..KEYS {
        let value = format!("first-{number}").into_bytes();
        store.put(COLUMN, &key(number), &value).expect("put");
        model.insert(key(number), value);
    }
    seal_all(&store, &io);
    for number in (0..KEYS).step_by(3) {
        let value = format!("second-{number}").into_bytes();
        store.put(COLUMN, &key(number), &value).expect("put");
        model.insert(key(number), value);
    }
    for number in (1..KEYS).step_by(7) {
        store.delete(COLUMN, &key(number)).expect("delete");
        model.remove(&key(number));
    }
    seal_all(&store, &io);
    let before = report(&store).sorted_answers;
    check(&store, &model, &written);
    assert!(report(&store).sorted_answers >= before + KEYS, "gets went by the footers");
    check_entries(&store, &model);
    // with no cover a count takes whole blocks by their lines and steps nothing through the index
    let scan_keys = store.state.sorted_scan_keys.load(Ordering::Relaxed);
    assert_eq!(store.count_prefix(COLUMN, b"").expect("count").0, model.len() as u64);
    assert_eq!(store.state.sorted_scan_keys.load(Ordering::Relaxed), scan_keys, "a count stepped the index");
    assert_eq!(read(&store, &[7; WIDTH as usize]), None);
    // with no cover a walk locates the index's entries in runs, and keys written since and left open stand between them
    let mut random = SeededRandom::new(Seed(11));
    let runs = store.state.sorted_runs.load(Ordering::Relaxed);
    check_scans(&store, &model, &mut random);
    assert!(store.state.sorted_runs.load(Ordering::Relaxed) > runs, "no batch took a run from the index");
    for number in (0..KEYS).step_by(11) {
        let value = format!("open-{number}").into_bytes();
        store.put(COLUMN, &key(number), &value).expect("put");
        model.insert(key(number), value);
    }
    for number in (5..KEYS).step_by(13) {
        store.delete(COLUMN, &key(number)).expect("delete");
        model.remove(&key(number));
    }
    store.put(COLUMN, &key(KEYS + 1), b"newest").expect("put");
    model.insert(key(KEYS + 1), b"newest".to_vec());
    check_scans(&store, &model, &mut random);
    // a range delete still open hides sealed keys the index holds, so a walk takes those keys a step at a time
    let mut names: Vec<Vec<u8>> = model.keys().cloned().collect();
    names.sort();
    let (start, end) = (names[names.len() / 4].clone(), names[names.len() / 2].clone());
    store.delete_range(COLUMN, &start, &end).expect("delete range");
    model.retain(|name, _value| *name < start || *name >= end);
    check_scans(&store, &model, &mut random);
    seal_all(&store, &io);
    store.close().expect("close");

    let store = open(&io, CAPACITY);
    check(&store, &model, &written);
    check_entries(&store, &model);
    store.flush().expect("flush");
    io.kill();
    drop(store);

    let store = open(&io, CAPACITY);
    check(&store, &model, &written);
    store.close().expect("close");
}

// a grave sealed before an older value of its key in another open segment keeps the key deleted once that segment seals too,
// and leaves the key no value entry
#[test]
fn grave_sealed_first_keeps_the_key_deleted() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io, CAPACITY);
    let name = key(5);
    store.put_marked(COLUMN, &name, b"oldest", MARK).expect("put");
    store.put(COLUMN, &name, b"newer").expect("put");
    store.delete(COLUMN, &name).expect("delete");
    let volume = &store.state.volumes[0];
    let filled = hold(&volume.claims).open.iter().filter(|open| open.row_count.0 > 0).count();
    assert_eq!(filled, 2, "the marked put went to a segment of its own");

    // the unmarked segment holds the newer value and the grave, and seals while the marked one stays open
    {
        let mut claims = hold(&volume.claims);
        let unmarked = claims.open.iter().position(|open| open.row_count.0 == 2).expect("the unmarked segment");
        store.state.seal_at(&mut claims, volume, unmarked);
    }
    settle(&io);
    cool(&store, &io);
    assert!(hold(&volume.claims).open.iter().any(|open| open.row_count.0 == 1), "the marked segment is still open");
    assert!(entry(&store, &name).is_some_and(|entry| entry.is_grave), "the grave stays while an older row is open");
    assert_eq!(read(&store, &name), None);

    seal_all(&store, &io);
    assert_eq!(read(&store, &name), None);
    assert!(entry(&store, &name).is_none_or(|entry| entry.is_grave));
    assert_eq!(store.state.sorted_index(COLUMN).expect("an index").check(), Ok(()));
    store.close().expect("close");
    let store = open(&io, CAPACITY);
    assert_eq!(read(&store, &name), None);
    store.close().expect("close");
}

// a cover sealed while an older value under it sits in another open segment: that value gets no entry when it seals, the key
// reads absent with no read refused, and merges leave no entry behind
#[test]
fn cover_over_an_open_older_row() {
    let io = SimulatedIo::new(Seed(3));
    let store = open(&io, CAPACITY);
    let inside = key(9);
    store.put_marked(COLUMN, &inside, b"under", MARK).expect("put");
    let (start, end) = (vec![0; WIDTH as usize], vec![0xFF; WIDTH as usize]);
    store.put(COLUMN, &key(10), b"before").expect("put");
    store.delete_range(COLUMN, &start, &end).expect("delete range");
    let volume = &store.state.volumes[0];
    {
        let mut claims = hold(&volume.claims);
        let unmarked = claims.open.iter().position(|open| open.row_count.0 == 2).expect("the cover's segment");
        store.state.seal_at(&mut claims, volume, unmarked);
    }
    settle(&io);
    cool(&store, &io);
    assert_eq!(entry(&store, &key(10)), None, "the cover took the older entry out");
    seal_all(&store, &io);
    assert_eq!(entry(&store, &inside), None, "a value under a newer cover gets no entry");
    assert_eq!(read(&store, &inside), None);
    for _round in 0..4 {
        seal_all(&store, &io);
        cool(&store, &io);
    }
    assert_eq!(read(&store, &inside), None);
    assert_eq!(read(&store, &key(10)), None);
    assert_eq!(store.state.sorted_index(COLUMN).expect("an index").check(), Ok(()));
    store.close().expect("close");
    let store = open(&io, CAPACITY);
    assert_eq!(entry(&store, &inside), None, "open takes out what a live cover hides");
    assert_eq!(read(&store, &inside), None);
    store.close().expect("close");
}

// past capacity every key still reads, through the footers past the mark, and the report says how many rows had no room
#[test]
fn past_capacity_stays_right() {
    let io = SimulatedIo::new(Seed(4));
    let store = open(&io, SMALL_CAPACITY);
    let mut model = Model::new();
    let written: Vec<Vec<u8>> = (0..KEYS * 20).map(key).collect();
    for round in 0..3 {
        for number in 0..KEYS * 20 {
            // two keys in three take a new row a round, so each segment keeps live rows and a merge has some to carry
            if round > 0 && number % 3 == round % 3 {
                continue;
            }
            let value = format!("{round}-{number}").into_bytes();
            store.put(COLUMN, &key(number), &value).expect("put");
            model.insert(key(number), value);
        }
        seal_all(&store, &io);
    }
    for number in (0..KEYS * 20).step_by(5) {
        store.delete(COLUMN, &key(number)).expect("delete");
        model.remove(&key(number));
    }
    seal_all(&store, &io);
    assert!(store.state.sorted_index(COLUMN).expect("an index").mark().is_some());
    let reading = report(&store).sorted_columns.iter().find(|reading| reading.column == COLUMN).copied().expect("a reading");
    assert!(reading.rows_past_capacity.0 > 0 && reading.is_marked && reading.capacity == SMALL_CAPACITY, "{reading:?}");
    let before = report(&store).sorted_footer_asks;
    check(&store, &model, &written);
    assert!(report(&store).sorted_footer_asks > before, "reads past the mark asked the footers");
    store.close().expect("close");
}

// random puts, deletes and range deletes over many seals, merges, reopens and kills read as a model does
#[test]
fn churn_matches_the_model() {
    for seed in seeds(0..CHURN_SEEDS) {
        let marks = churn(seed, Arm { capacity: CAPACITY, key_of: key, extra: 0, is_sized: false });
        assert_eq!(marks, (false, true), "seed {seed}");
    }
}

// the same churn with every key in one part of the index, so leaves fill and split under it
#[test]
fn churn_in_one_part_matches_the_model() {
    for seed in seeds(0..CHURN_SEEDS) {
        let marks = churn(seed, Arm { capacity: CAPACITY, key_of: one_part_key, extra: 0, is_sized: false });
        assert_eq!(marks, (false, true), "seed {seed}");
    }
}

// the same churn on a full budget, whose index leaves keep the values reads take, so gets and scans answer from them
#[test]
fn churn_with_held_values_matches_the_model() {
    SIM_BUDGET.with(|budget| budget.set(HELD_BUDGET));
    let (mut fills, mut held_rows) = (0, 0);
    for seed in seeds(0..CHURN_SEEDS) {
        HELD_SEEN.with(|seen| seen.set((0, 0)));
        churn(seed, Arm { capacity: CAPACITY, key_of: key, extra: 0, is_sized: false });
        let (seed_fills, seed_rows) = HELD_SEEN.with(std::cell::Cell::get);
        fills += seed_fills;
        held_rows += seed_rows;
    }
    SIM_BUDGET.with(|budget| budget.set(BUDGET));
    println!("held values: {fills} fills, {held_rows} scan rows from leaves");
    assert!(fills > 0 && held_rows > 0, "no value was held and served");
}

thread_local! {
    // value fills and scan rows answered from leaves, summed over a seed's stores before each close
    static HELD_SEEN: std::cell::Cell<(u64, u64)> = const { std::cell::Cell::new((0, 0)) };
}

// compressed values a scan restored and held, the most any store of a run saw
static RESTORED_SEEN: AtomicU64 = AtomicU64::new(0);

// a scan over a compressed column restores each sealed value once and its leaf holds what a get would read, so a second walk
// answers from the leaves, and walks and gets give the values written
#[test]
fn compressed_values_restore_once_into_their_leaves() {
    SIM_BUDGET.with(|budget| budget.set(HELD_BUDGET));
    SIM_CODEC.with(|codec| codec.set(Codec::Lz4));
    let io = SimulatedIo::new(Seed(5_103));
    let store = open(&io, CAPACITY);
    let value = |number: u64| format!("{number:08}-").repeat(30).into_bytes();
    let mut model = BTreeMap::new();
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), &value(number)).expect("put");
        model.insert(key(number), value(number));
    }
    seal_all(&store, &io);
    let first = walk_rows(&mut store.iter_from(COLUMN, &[], Direction::Ascending).expect("scan"));
    let restored = store.state.scan_restored_values.load(Ordering::Relaxed);
    let held = report(&store).scan_held_values;
    let second = walk_rows(&mut store.iter_from(COLUMN, &[], Direction::Ascending).expect("scan"));
    let held_after = report(&store).scan_held_values;
    SIM_CODEC.with(|codec| codec.set(Codec::None));
    SIM_BUDGET.with(|budget| budget.set(BUDGET));
    assert!(first == model && second == model, "a walk gave other values");
    for number in 0..KEYS {
        assert_eq!(read(&store, &key(number)), Some(value(number)), "key {number}");
    }
    assert!(restored > 0, "no compressed value was restored");
    assert!(held_after > held, "the second walk took nothing from the leaves");
    store.close().expect("close");
}

// opens that resized an index in the sized churn, over every seed
static SIZED_RESIZES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

// the same churn with the index sized by the store at every open, from the rows on disk after a clean close and after a
// kill
#[test]
fn churn_sized_by_the_store_matches_the_model() {
    // a small headroom, so each pool follows the keys its index held and opens resize it
    reel::engine::engine_sizing::TEST_INDEX_HEADROOM.with(|headroom| headroom.set(Some(64 * 1024)));
    for seed in seeds(0..CHURN_SEEDS) {
        churn(seed, Arm { capacity: CAPACITY, key_of: key, extra: 0, is_sized: true });
    }
    let resized = SIZED_RESIZES.load(Ordering::Relaxed);
    println!("{resized} opens resized an index");
    assert!(resized > 0, "no open resized an index");
}

// the same churn on an index with room for fewer rows than a seed writes: extra keys put first take it past capacity, and a
// range delete of them halfway through lets merges and the refill free room
#[test]
fn churn_past_capacity_matches_the_model() {
    let (mut marked, mut cleared) = (0, 0);
    for seed in seeds(0..CHURN_SEEDS) {
        let (was_marked, is_cleared) = churn(seed, Arm { capacity: CHURN_CAPACITY, key_of: one_part_key, extra: EXTRA_KEYS, is_sized: false });
        marked += u64::from(was_marked);
        cleared += u64::from(was_marked && is_cleared);
    }
    println!("churn past capacity: {marked} seeds marked, {cleared} cleared");
    assert!(marked > 0, "no seed went past capacity");
}

// how one churn runs: the index's declared rows, the keys it writes, and the keys put before the rounds and deleted halfway
struct Arm {
    capacity: RowCount,
    key_of: fn(u64) -> Vec<u8>,
    extra: u64,
    /// The store sizes the index from the keys it held at each open, in place of the declared rows
    is_sized: bool,
}

// run one seed's churn, and say whether the index was ever marked and whether it ends with no mark
fn churn(seed: u64, Arm { capacity, key_of, extra, is_sized }: Arm) -> (bool, bool) {
    let open_arm = |io: &SimulatedIo| if is_sized { open_sized(io) } else { open(io, capacity) };
    // a store that sizes its index: got stays under asked, and the pool moves
    let mut resized = 0;
    let mut sized_check = |store: &Store, before: RowCount| {
        if !is_sized {
            return;
        }
        let reading = sorted_reading(store);
        assert!(reading.got_bytes <= reading.asked_bytes, "seed {seed}: {reading:?}");
        resized += u64::from(reading.capacity != before);
    };
    let is_marked = |store: &Store| store.state.sorted_index(COLUMN).expect("an index").mark().is_some();
    let mut was_marked = false;
    {
        let io = SimulatedIo::new(Seed(100 + seed));
        let mut random = SeededRandom::new(Seed(200 + seed));
        let mut store = open_arm(&io);
        let mut model = Model::new();
        let written: Vec<Vec<u8>> = (0..CHURN_KEYS).map(key_of).collect();
        // the extra keys sort together past every churn key of their part, so one range delete takes them all
        let (first_extra, last_extra) = (extra_key(0), extra_key(extra));
        for number in 0..extra {
            store.put(COLUMN, &extra_key(number), b"extra").expect("put");
            model.insert(extra_key(number), b"extra".to_vec());
        }
        if extra > 0 {
            seal_all(&store, &io);
        }
        let mut sorted_names = written.clone();
        sorted_names.sort();
        for round in 0..CHURN_ROUNDS {
            if extra > 0 && round == CHURN_ROUNDS / 2 {
                store.delete_range(COLUMN, &first_extra, &last_extra).expect("delete range");
                model.retain(|name, _value| *name < first_extra || *name >= last_extra);
            }
            for write in 0..CHURN_WRITES {
                let name = key_of(random.below(CHURN_KEYS));
                match random.below(COVER_ONE_IN) {
                    0 => {
                        let at = random.below(CHURN_KEYS) as usize;
                        let until = (at + random.below(40) as usize).min(sorted_names.len() - 1);
                        let (start, end) = (sorted_names[at].clone(), sorted_names[until].clone());
                        store.delete_range(COLUMN, &start, &end).expect("delete range");
                        model.retain(|name, _value| *name < start || *name >= end);
                    },
                    1..=6 => {
                        store.delete(COLUMN, &name).expect("delete");
                        model.remove(&name);
                    },
                    _ => {
                        let value = format!("{seed}-{round}-{write}").into_bytes();
                        if random.below(4) == 0 {
                            store.put_marked(COLUMN, &name, &value, MARK).expect("put");
                        } else {
                            store.put(COLUMN, &name, &value).expect("put");
                        }
                        model.insert(name, value);
                    },
                }
            }
            match random.below(4) {
                0 => seal(&store, &io),
                1 => seal_all(&store, &io),
                2 => {
                    let before = sorted_reading(&store).capacity;
                    store.close().expect("close");
                    drop(store);
                    store = open_arm(&io);
                    sized_check(&store, before);
                },
                _ => {
                    store.flush().expect("flush");
                    let before = sorted_reading(&store).capacity;
                    io.kill();
                    drop(store);
                    store = open_arm(&io);
                    sized_check(&store, before);
                },
            }
            was_marked |= is_marked(&store);
            check(&store, &model, &written);
            check_scans(&store, &model, &mut random);
            note_held(&store);
            // a count keeps to the segments and takes nothing from the index, and counts what the model holds
            let prefix = [random.below(256) as u8];
            let scan_keys = store.state.sorted_scan_keys.load(Ordering::Relaxed);
            let counted = store.count_prefix(COLUMN, &prefix).expect("count");
            assert_eq!(counted.0, model.keys().filter(|name| name.starts_with(&prefix)).count() as u64);
            // with no cover and no damage a count takes whole blocks, and otherwise it steps keys as any walk does
            let volume = &store.state.volumes[0];
            if volume.covers.is_empty() && volume.damage.is_empty() {
                assert_eq!(store.state.sorted_scan_keys.load(Ordering::Relaxed), scan_keys, "a count stepped the index");
            }
        }
        seal_all(&store, &io);
        for _round in 0..3 {
            cool(&store, &io);
        }
        check(&store, &model, &written);
        was_marked |= is_marked(&store);
        // a marked index lacks the rows past its capacity, and with no mark it holds every live key
        let is_cleared = !is_marked(&store);
        if is_cleared {
            check_entries(&store, &model);
        }
        // a seed whose rounds all end in a reopen seals nothing until here, so the walks run once more over sealed keys
        check_scans(&store, &model, &mut random);
        note_held(&store);
        // with a mark standing every get and scan reads the footers, so only an index with no mark answers on its own
        if is_cleared {
            assert!(store.state.sorted_answers.load(Ordering::Relaxed) > 0);
            assert!(store.state.sorted_scan_keys.load(Ordering::Relaxed) > 0, "no scan key came from the index");
        }
        store.close().expect("close");
        if is_sized {
            println!("seed {seed}: {resized} opens resized the index");
            SIZED_RESIZES.fetch_add(resized, Ordering::Relaxed);
        }
        (was_marked, is_cleared)
    }
}

// add a store's value fills and scan rows from leaves to the seed's count, once before a store goes
fn note_held(store: &Store) {
    let report = report(store);
    HELD_SEEN.with(|seen| {
        let (fills, rows) = seen.get();
        seen.set((fills.max(report.value_fills), rows.max(report.scan_held_values)));
    });
    RESTORED_SEEN.fetch_max(store.state.scan_restored_values.load(Ordering::Relaxed), Ordering::Relaxed);
}

// seal every open segment with rows but one
fn seal_all_but(store: &Store, io: &SimulatedIo, kept: SegmentNumber) {
    let volume = &store.state.volumes[0];
    loop {
        let mut claims = hold(&volume.claims);
        let Some(at) = claims.open.iter().position(|open| open.number != kept && open.row_count.0 > 0 && open.phase == SegmentPhase::Taking) else {
            return;
        };
        store.state.seal_at(&mut claims, volume, at);
        drop(claims);
        settle(io);
        cool(store, io);
    }
}

// with a mark standing, graves keep their entries. A value past the mark has no entry, and a grave of its key in an older open
// segment, below the mark, would leave the footers past the mark that value as the newest row if it dropped
#[test]
fn graves_stay_while_a_mark_stands() {
    let io = SimulatedIo::new(Seed(5));
    let store = open(&io, SMALL_CAPACITY);
    // merges would carry the late value and its grave past the mark together, so the reads here meet the segments as sealed
    store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
    let index = || store.state.sorted_index(COLUMN).expect("an index");
    let volume = &store.state.volumes[0];
    // the anchor has its entry before its marked segment opens, so that segment's seal needs no room for it
    store.put(COLUMN, &key(u64::MAX), b"first").expect("put");
    seal_all(&store, &io);
    store.put_marked(COLUMN, &key(u64::MAX), b"anchor", MARK).expect("put");
    let older = hold(&volume.claims).open.iter().find(|open| open.row_count.0 == 1).map(|open| open.number).expect("the marked segment");
    let mut filled = 0;
    while index().mark().is_none() {
        for number in filled..filled + KEYS {
            store.put(COLUMN, &key(number), b"filler").expect("put");
        }
        filled += KEYS;
        seal_all_but(&store, &io, older);
    }
    assert!(index().mark().is_some_and(|mark| mark > older));
    let late = key(u64::MAX / 2);
    store.put(COLUMN, &late, b"past the mark").expect("put");
    seal_all_but(&store, &io, older);
    assert_eq!(entry(&store, &late), None, "the late value found no room");
    for number in 0..filled {
        store.delete(COLUMN, &key(number)).expect("delete");
    }
    seal_all_but(&store, &io, older);
    let mut batch = WriteBatch::new();
    batch.set_mark(MARK);
    batch.delete(COLUMN, &late);
    store.write_batch(&batch).expect("batch");
    seal_all(&store, &io);
    assert!(index().mark().is_some_and(|mark| mark > older), "the mark stays past the older segment");
    assert_eq!(read(&store, &late), None);
    for number in (0..filled).step_by(97) {
        assert_eq!(read(&store, &key(number)), None);
    }
    assert_eq!(index().check(), Ok(()));
    store.close().expect("close");
}

// merges move each kept row's entry to the listed output before the inputs go: mid-copy and after, every get answers from the
// index with no footer ask, a deleted key's grave among them
#[test]
fn merges_keep_gets_on_the_index() {
    let io = SimulatedIo::new(Seed(6));
    let store = open(&io, CAPACITY);
    let volume = &store.state.volumes[0];
    volume.holds.compaction.store(true, Ordering::Release);
    volume.holds.reclaim.store(true, Ordering::Release);
    let mut model = Model::new();
    let written: Vec<Vec<u8>> = (0..MERGE_KEYS).map(key).collect();
    for number in 0..MERGE_KEYS {
        store.put(COLUMN, &key(number), b"first").expect("put");
        model.insert(key(number), b"first".to_vec());
    }
    seal(&store, &io);
    // two keys in three take a newer row and one in five a grave, so the first segment is mostly dead and a merge carries the rest
    for number in (0..MERGE_KEYS).filter(|number| number % 3 != 2) {
        store.put(COLUMN, &key(number), b"second").expect("put");
        model.insert(key(number), b"second".to_vec());
    }
    for number in (0..MERGE_KEYS).step_by(5) {
        store.delete(COLUMN, &key(number)).expect("delete");
        model.remove(&key(number));
    }
    seal(&store, &io);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    while !job.rewrite.start() {
        assert!(job.rewrite.has_looks_left(), "no merge to run");
    }
    while job.rewrite.phase != RewritePhase::Copying {
        job.rewrite.step();
    }
    job.rewrite.step();
    assert!(job.rewrite.is_running());
    // part of the rows are in an output that isn't listed yet, and the entries still point at the inputs
    let before = report(&store);
    check(&store, &model, &written);
    assert_eq!(report(&store).sorted_footer_asks, before.sorted_footer_asks, "a get mid-copy asked the footers");
    while job.rewrite.is_running() {
        job.rewrite.step();
    }
    assert!(volume.segments_rewritten.load(Ordering::Acquire) > 0, "no merge ran");
    let before = report(&store);
    check(&store, &model, &written);
    let after = report(&store);
    assert_eq!(after.sorted_footer_asks, before.sorted_footer_asks, "a get after the merge asked the footers");
    assert!(after.sorted_answers >= before.sorted_answers + MERGE_KEYS);
    check_entries(&store, &model);
    store.close().expect("close");
}

// an entry whose segment is gone: a grave answers absent from the index, and a value goes to the footers for its row, in a get
// and in a scan
#[test]
fn entries_whose_segment_left() {
    let io = SimulatedIo::new(Seed(8));
    let store = open(&io, CAPACITY);
    let (deleted, kept) = (key(1), key(2));
    store.put(COLUMN, &deleted, b"value").expect("put");
    store.put(COLUMN, &kept, b"value").expect("put");
    seal_all(&store, &io);
    // an older row stays open beside the grave's segment, so the floor keeps the grave as an entry
    store.put_marked(COLUMN, &key(3), b"open", MARK).expect("put");
    store.delete(COLUMN, &deleted).expect("delete");
    let volume = &store.state.volumes[0];
    {
        let mut claims = hold(&volume.claims);
        let unmarked = claims.open.iter().position(|open| open.ceiling.is_none() && open.row_count.0 > 0).expect("the grave's segment");
        store.state.seal_at(&mut claims, volume, unmarked);
    }
    settle(&io);
    let index = store.state.sorted_index(COLUMN).expect("an index");
    // a number an entry can hold and no segment has
    let gone = SegmentNumber(u64::from(u32::MAX - 2));
    assert!(store.state.volumes[0].table.pin(gone).is_none());
    for name in [&deleted, &kept] {
        let held = index.find(name).expect("an entry");
        assert!(index.move_entry(name, held.place, gone, held.offset));
    }
    assert!(entry(&store, &deleted).is_some_and(|entry| entry.is_grave));

    let before = report(&store).sorted_footer_asks;
    assert_eq!(read(&store, &deleted), None);
    assert_eq!(report(&store).sorted_footer_asks, before, "the grave asked the footers");
    assert_eq!(read(&store, &kept).as_deref(), Some(&b"value"[..]));
    assert_eq!(report(&store).sorted_footer_asks, before + 1, "the value didn't ask the footers");

    let found = rows(store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan"));
    assert_eq!(found, vec![(kept.clone(), b"value".to_vec()), (key(3), b"open".to_vec())]);
    assert!(report(&store).sorted_footer_asks > before + 1, "the scan's value didn't ask the footers");
    store.close().expect("close");
}

// a walk told how many rows it wants locates no more than that a batch, and gives the same rows
#[test]
fn a_wanted_count_caps_the_batch() {
    let io = SimulatedIo::new(Seed(9));
    let store = open(&io, CAPACITY);
    let mut model = Model::new();
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), format!("{number}").as_bytes()).expect("put");
        model.insert(key(number), format!("{number}").into_bytes());
    }
    seal_all(&store, &io);
    let range = reel::engine::scan_state::from_key(COLUMN, b"", Direction::Ascending);
    for wanted in [1, WANTED_ROWS, 1_000] {
        let mut scan = store.state.take_scan(&range.wanting(RowCount(wanted)), None).expect("scan");
        let (mut name, mut value) = (Vec::new(), Vec::new());
        let mut found = Vec::new();
        for _row in 0..wanted.min(KEYS) {
            let step = store.state.step_scan(&mut scan, &mut name, reel::engine::scan_path::ScanWant::Value(&mut value)).expect("step");
            assert_eq!(step, ScanStep::Found);
            assert!(scan.batch.entries.len() as u64 <= wanted, "{} located for {wanted} wanted", scan.batch.entries.len());
            assert!(scan.sorted.len() as u64 <= wanted, "{} index entries for {wanted} wanted", scan.sorted.len());
            found.push((name.clone(), value.clone()));
        }
        let expected: Vec<_> = model.iter().take(wanted.min(KEYS) as usize).map(|(name, value)| (name.clone(), value.clone())).collect();
        assert_eq!(found, expected);
        store.state.give_back_scan(scan);
        // the store's own walk takes the count on its range and gives the same rows
        let walked: Vec<_> = rows(store.iter(&range.wanting(RowCount(wanted))).expect("scan")).into_iter().take(wanted.min(KEYS) as usize).collect();
        assert_eq!(walked, expected);
    }
    store.close().expect("close");
}

// a write that lands between two batches of one walk shows from the next batch: an overwrite of a sealed key ahead of the walk
// reads new, and a delete of one leaves it out, while the walk still takes its sealed keys in runs
#[test]
fn writes_between_batches_show() {
    let io = SimulatedIo::new(Seed(10));
    let store = open(&io, CAPACITY);
    let mut model = Model::new();
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), format!("{number}").as_bytes()).expect("put");
        model.insert(key(number), format!("{number}").into_bytes());
    }
    seal_all(&store, &io);
    // a segment stays open before the walk, so the writes under it land in the view the heads were placed in
    store.put(COLUMN, &key(KEYS + 5), b"open").expect("put");
    model.insert(key(KEYS + 5), b"open".to_vec());
    let names: Vec<Vec<u8>> = model.keys().cloned().collect();
    let range = reel::engine::scan_state::from_key(COLUMN, b"", Direction::Ascending);
    let mut scan = store.state.take_scan(&range, None).expect("scan");
    let (mut name, mut value) = (Vec::new(), Vec::new());
    let mut found = Vec::new();
    let step = |scan: &mut reel::engine::scan_state::ScanState, name: &mut Vec<u8>, value: &mut Vec<u8>| {
        store.state.step_scan(scan, name, reel::engine::scan_path::ScanWant::Value(value)).expect("step")
    };
    assert_eq!(step(&mut scan, &mut name, &mut value), ScanStep::Found);
    found.push((name.clone(), value.clone()));
    // two sealed keys well past the first batch change under the walk
    let (changed, deleted) = (names[AHEAD].clone(), names[AHEAD + 1].clone());
    store.put(COLUMN, &changed, b"changed").expect("put");
    store.delete(COLUMN, &deleted).expect("delete");
    model.insert(changed, b"changed".to_vec());
    model.remove(&deleted);
    let runs = store.state.sorted_runs.load(Ordering::Relaxed);
    while step(&mut scan, &mut name, &mut value) == ScanStep::Found {
        found.push((name.clone(), value.clone()));
    }
    store.state.give_back_scan(scan);
    let expected: Vec<(Vec<u8>, Vec<u8>)> = model.iter().map(|(name, value)| (name.clone(), value.clone())).collect();
    assert_eq!(found, expected);
    assert!(store.state.sorted_runs.load(Ordering::Relaxed) > runs, "the later batches took no run");
    store.close().expect("close");
}

// an index declared past the budget opens with what the cap gives it, and the report says it asked for more than it got
#[test]
fn an_index_past_the_budget_is_capped() {
    let io = SimulatedIo::new(Seed(12));
    let rows = RowCount(50_000_000);
    let store = ReelStore::open(io.clone(), &config(rows)).expect("open");
    let reading = report(&store).sorted_columns.iter().find(|reading| reading.column == COLUMN).copied().expect("a reading");
    assert!(reading.asked_bytes > reading.got_bytes, "{reading:?}");
    assert!(reading.capacity < rows, "{reading:?}");
    store.close().expect("close");
}
// a store whose only column is sorted reads at a cue behind the newest sealed rows through the footers, while gets at the tip go
// by the index
#[test]
fn an_all_sorted_store_reads_at_a_cue() {
    let io = SimulatedIo::new(Seed(13));
    let store = open(&io, CAPACITY);
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), b"first").expect("put");
    }
    seal_all(&store, &io);
    let cue = store.cue().expect("cue");
    for round in 0..CUE_ROUNDS {
        for number in (0..KEYS).step_by(2) {
            store.put(COLUMN, &key(number), format!("round-{round}").as_bytes()).expect("put");
        }
        seal_all(&store, &io);
    }
    let latest = format!("round-{}", CUE_ROUNDS - 1).into_bytes();
    let (answers, asks) = (report(&store).sorted_answers, report(&store).sorted_footer_asks);
    let mut value = Vec::new();
    for number in 0..KEYS {
        let newest: &[u8] = if number % 2 == 0 { &latest } else { b"first" };
        assert_eq!(read(&store, &key(number)).as_deref(), Some(newest));
        assert!(cue.get(COLUMN, &key(number), &mut value).expect("get"));
        assert_eq!(value, b"first");
    }
    assert!(report(&store).sorted_answers >= answers + KEYS, "gets at the tip went by the footers");
    assert!(report(&store).sorted_footer_asks >= asks + KEYS, "gets at the cue went by the index");
    drop(cue);
    store.close().expect("close");
}

// take one row of a walk from the start, make a change, then take the rest. Return the rows and the first batch's last key
fn walk_around(store: &Store, change: impl FnOnce(&reel::engine::scan_state::ScanState)) -> (Rows, Vec<u8>) {
    let range = reel::engine::scan_state::from_key(COLUMN, b"", Direction::Ascending);
    let mut scan = store.state.take_scan(&range, None).expect("scan");
    let (mut name, mut value) = (Vec::new(), Vec::new());
    let mut found = Vec::new();
    let mut step = |scan: &mut reel::engine::scan_state::ScanState, found: &mut Vec<(Vec<u8>, Vec<u8>)>| {
        let step = store.state.step_scan(scan, &mut name, reel::engine::scan_path::ScanWant::Value(&mut value)).expect("step");
        if step == ScanStep::Found {
            found.push((name.clone(), value.clone()));
        }
        step
    };
    assert_eq!(step(&mut scan, &mut found), ScanStep::Found);
    let last = scan.batch.entries.last().expect("a located batch");
    let batch_end = scan.batch_key(last).to_vec();
    change(&scan);
    while step(&mut scan, &mut found) == ScanStep::Found {}
    store.state.give_back_scan(scan);
    (found, batch_end)
}

// a batch answers as of the point it was located, and the change shows from the next batch on
fn walked(before: &Model, after: &Model, batch_end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let located = before.range(..=batch_end.to_vec());
    let later = after.range(batch_end.to_vec()..).filter(|(name, _value)| name.as_slice() != batch_end);
    located.chain(later).map(|(name, value)| (name.clone(), value.clone())).collect()
}

// a range delete that seals and applies under a walk hides its sealed keys ahead of the walk from the next batch on
#[test]
fn a_cover_applied_under_a_walk() {
    let io = SimulatedIo::new(Seed(14));
    let store = open(&io, CAPACITY);
    let mut model = Model::new();
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), format!("{number}").as_bytes()).expect("put");
        model.insert(key(number), format!("{number}").into_bytes());
    }
    seal_all(&store, &io);
    let names: Vec<Vec<u8>> = model.keys().cloned().collect();
    let (start, end) = (names[AHEAD].clone(), names[AHEAD + COVERED].clone());
    let before = model.clone();
    let (found, batch_end) = walk_around(&store, |_scan| {
        store.delete_range(COLUMN, &start, &end).expect("delete range");
        seal_all(&store, &io);
    });
    model.retain(|name, _value| *name < start || *name >= end);
    assert_eq!(found, walked(&before, &model, &batch_end));
    store.close().expect("close");
}

// rows past capacity set the index's mark under a walk, and the walk still shows every sealed key ahead of it. The mark stands
// until an open rebuilds the index, and a reopen with room clears it
#[test]
fn a_mark_set_under_a_walk() {
    let io = SimulatedIo::new(Seed(15));
    let store = open(&io, SMALL_CAPACITY);
    let mut model = Model::new();
    for number in 0..SMALL_CAPACITY.0 / 2 {
        store.put(COLUMN, &key(number), b"first").expect("put");
        model.insert(key(number), b"first".to_vec());
    }
    seal_all(&store, &io);
    let index = || store.state.sorted_index(COLUMN).expect("an index");
    assert!(index().mark().is_none());
    let mut late = SMALL_CAPACITY.0;
    let before = model.clone();
    let (found, batch_end) = walk_around(&store, |_scan| {
        while index().mark().is_none() {
            assert!(late < LATE_KEYS, "no row went past capacity");
            for number in late..late + KEYS {
                store.put(COLUMN, &key(number), b"late").expect("put");
            }
            late += KEYS;
            seal_all(&store, &io);
        }
    });
    for number in SMALL_CAPACITY.0..late {
        model.insert(key(number), b"late".to_vec());
    }
    assert_eq!(found, walked(&before, &model, &batch_end));
    let unlisted = model.keys().filter(|name| name.as_slice() > batch_end.as_slice() && entry(&store, name).is_none()).count();
    assert!(unlisted > 0, "every key the walk took had an entry");
    for number in SMALL_CAPACITY.0..late {
        store.delete(COLUMN, &key(number)).expect("delete");
        model.remove(&key(number));
    }
    seal_all(&store, &io);
    assert!(index().mark().is_some(), "the mark went before an open");
    store.close().expect("close");

    let store = open(&io, SMALL_CAPACITY);
    let mut random = SeededRandom::new(Seed(16));
    let written: Vec<Vec<u8>> = model.keys().cloned().collect();
    check(&store, &model, &written);
    check_scans(&store, &model, &mut random);
    store.close().expect("close");
}

// a walk at a cue keeps the cue's rows while every key it has yet to reach takes a newer row or a grave and seals
#[test]
fn a_cue_walk_under_newer_seals() {
    let io = SimulatedIo::new(Seed(17));
    let store = open(&io, CAPACITY);
    let mut model = Model::new();
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), b"first").expect("put");
        model.insert(key(number), b"first".to_vec());
    }
    seal_all(&store, &io);
    let cue = store.cue().expect("cue");
    let mut scan = cue.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
    let (mut name, mut value) = (Vec::new(), Vec::new());
    let mut found = Vec::new();
    assert_eq!(scan.next_row(&mut name, &mut value).expect("step"), ScanStep::Found);
    found.push((name.clone(), value.clone()));
    for number in 0..KEYS {
        if number % 4 == 0 {
            store.delete(COLUMN, &key(number)).expect("delete");
        } else {
            store.put(COLUMN, &key(number), b"newer").expect("put");
        }
    }
    seal_all(&store, &io);
    while scan.next_row(&mut name, &mut value).expect("step") == ScanStep::Found {
        found.push((name.clone(), value.clone()));
    }
    drop(scan);
    let expected: Vec<(Vec<u8>, Vec<u8>)> = model.iter().map(|(name, value)| (name.clone(), value.clone())).collect();
    assert_eq!(found, expected, "the cue's walk showed rows newer than the cue");
    drop(cue);
    store.close().expect("close");
}

// a merge that installs and gives its inputs back between a batch's locate and the reads of its rows leaves every row readable
#[test]
fn a_merge_between_locate_and_read() {
    let io = SimulatedIo::new(Seed(18));
    let store = open(&io, CAPACITY);
    let volume = &store.state.volumes[0];
    volume.holds.compaction.store(true, Ordering::Release);
    let mut model = Model::new();
    for number in 0..MERGE_KEYS {
        store.put(COLUMN, &key(number), b"first").expect("put");
        model.insert(key(number), b"first".to_vec());
    }
    seal(&store, &io);
    for number in (0..MERGE_KEYS).filter(|number| number % 3 != 2) {
        store.put(COLUMN, &key(number), b"second").expect("put");
        model.insert(key(number), b"second".to_vec());
    }
    seal(&store, &io);
    let (found, _batch_end) = walk_around(&store, |scan| {
        assert!(scan.batch.entries.len() > 1, "the first batch located one row");
        let mut job = CompactJob::new(Arc::clone(&store.state), 0);
        while !job.rewrite.start() {
            assert!(job.rewrite.has_looks_left(), "no merge to run");
        }
        while job.rewrite.is_running() {
            job.rewrite.step();
        }
        assert!(volume.segments_rewritten.load(Ordering::Acquire) > 0, "no merge ran");
        cool(&store, &io);
    });
    let expected: Vec<(Vec<u8>, Vec<u8>)> = model.iter().map(|(name, value)| (name.clone(), value.clone())).collect();
    assert_eq!(found, expected);
    store.close().expect("close");
}

// K in a segment of its own, a cue, then a range delete over every key written into another open segment
fn k_then_cue_then_open_cover(store: &Store) -> (reel::engine::cue::Cue<SimulatedIo>, Vec<u8>) {
    let name = key(1);
    store.put_marked(COLUMN, &name, b"live at the cue", MARK).expect("put");
    let cue = store.cue().expect("cue");
    store.delete_range(COLUMN, &[0; WIDTH as usize], &[0xFF; WIDTH as usize]).expect("delete range");
    (cue, name)
}

// seal the marked segment that holds K alone, and leave every other open segment as it is
fn seal_k_alone(store: &Store, io: &SimulatedIo) {
    let volume = &store.state.volumes[0];
    {
        let mut claims = hold(&volume.claims);
        let at = claims.open.iter().position(|open| open.ceiling.is_some() && open.row_count.0 > 0).expect("K's segment");
        store.state.seal_at(&mut claims, volume, at);
    }
    settle(io);
}

// what a cue reads of K: the get, and a scan's first row from K
fn cue_reads(cue: &reel::engine::cue::Cue<SimulatedIo>, name: &[u8]) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    let mut value = Vec::new();
    let got = cue.get(COLUMN, name, &mut value).expect("get at the cue").then_some(value);
    let walked = rows(cue.iter_from(COLUMN, name, Direction::Ascending).expect("scan"));
    let first = walked.first().filter(|(found, _value)| found == name).map(|(_found, value)| value.clone());
    (got, first)
}

// (a) a value sealed under a range delete still open: the cue reads it by get and scan, the current point doesn't. A second arm
// seals the range delete first
#[test]
fn a_cue_reads_a_value_under_an_open_range_delete() {
    for (is_cover_sealed, seed) in [(false, 90), (true, 91)] {
        let io = SimulatedIo::new(Seed(seed));
        let store = open(&io, CAPACITY);
        let (cue, name) = k_then_cue_then_open_cover(&store);
        seal_k_alone(&store, &io);
        // the case that matters: nothing sealed sits above the cue, so the cue's get trusts the index
        let point = cue.point_of(COLUMN).expect("point");
        assert!(!store.state.volumes[0].may_hide_under_sealed(point), "the setup sends the cue to the footers");
        if is_cover_sealed {
            seal_all(&store, &io);
        }
        let live = Some(b"live at the cue".to_vec());
        assert_eq!(cue_reads(&cue, &name), (live.clone(), live), "the cue lost K, cover sealed {is_cover_sealed}");
        assert_eq!(read(&store, &name), None, "the range delete didn't hide K at the current point");
        drop(cue);
        store.close().expect("close");
    }
}

// (b) the cover's own seal stepped one io step at a time: at every step the cue reads K by get and scan, and the current point
// doesn't. The first arm writes the cover before K seals, so K never gets an entry. The second seals K first, so K has an entry
// and only the window between the cover seal's removal and its raise of the sealed place can lose it
#[test]
fn a_cue_reads_the_value_through_every_step_of_the_covers_seal() {
    for (is_k_sealed_first, seed) in [(false, 92), (true, 95)] {
        let io = SimulatedIo::new(Seed(seed));
        let store = open(&io, CAPACITY);
        let name = key(1);
        let cue = if is_k_sealed_first {
            store.put_marked(COLUMN, &name, b"live at the cue", MARK).expect("put");
            seal_k_alone(&store, &io);
            let cue = store.cue().expect("cue");
            store.delete_range(COLUMN, &[0; WIDTH as usize], &[0xFF; WIDTH as usize]).expect("delete range");
            cue
        } else {
            let (cue, _name) = k_then_cue_then_open_cover(&store);
            seal_k_alone(&store, &io);
            cue
        };
        let point = cue.point_of(COLUMN).expect("point");
        assert!(!store.state.volumes[0].may_hide_under_sealed(point), "the setup sends the cue to the footers");
        store.seal_now();
        let live = Some(b"live at the cue".to_vec());
        for step in 0..STEPS {
            assert_eq!(cue_reads(&cue, &name), (live.clone(), live.clone()), "step {step}: the cue lost K, K sealed first {is_k_sealed_first}");
            assert_eq!(read(&store, &name), None, "step {step}: the current point read K");
            if io.step_any().is_none() {
                break;
            }
        }
        drop(cue);
        store.close().expect("close");
    }
}

// (c) K and its range delete in one segment, with the cover starting below K and at K: the current point reads K gone, and a cue
// taken between the two writes reads K
#[test]
fn a_value_and_its_range_delete_in_one_segment() {
    for (is_inside, seed) in [(true, 93), (false, 94)] {
        let io = SimulatedIo::new(Seed(seed));
        let store = open(&io, CAPACITY);
        let name = key(1);
        store.put(COLUMN, &name, b"live at the cue").expect("put");
        let cue = store.cue().expect("cue");
        let start = if is_inside { vec![0; WIDTH as usize] } else { name.clone() };
        store.delete_range(COLUMN, &start, &[0xFF; WIDTH as usize]).expect("delete range");
        let volume = &store.state.volumes[0];
        assert_eq!(hold(&volume.claims).open.iter().filter(|open| open.row_count.0 > 0).count(), 1, "K and its cover landed apart");
        seal_all(&store, &io);
        let live = Some(b"live at the cue".to_vec());
        assert_eq!(cue_reads(&cue, &name), (live.clone(), live), "the cue lost K, inside {is_inside}");
        assert_eq!(read(&store, &name), None, "a deleted key read at the current point, inside {is_inside}");
        drop(cue);
        store.close().expect("close");
    }
}

// The sorted arm of range_apply's a_row_kept_for_a_cue_keeps_its_cover: the index drops its entries under the cover at the
// seal, so the current point reads none, and the cue above the cover reads the index too. The cover stays while the cue below
// lives, and the merge after it drops the rows with the cover
#[test]
fn a_row_kept_for_a_cue_keeps_its_cover_on_a_sorted_column() {
    let io = SimulatedIo::new(Seed(132));
    let store = open(&io, CAPACITY);
    let volume = &store.state.volumes[0];
    let old: Vec<Vec<u8>> = (0..COVERED as u64).map(key).collect();
    for name in &old {
        store.put(COLUMN, name, b"value").expect("put");
    }
    let below = store.cue().expect("cue below the cover");
    let before_cover = store.state.read_point(volume);
    store.delete_range(COLUMN, &[0; WIDTH as usize], &[0xFF; WIDTH as usize]).expect("delete range");
    for number in LATE_KEYS..LATE_KEYS + COVERED as u64 {
        store.put(COLUMN, &key(number), b"later").expect("put");
    }
    let holder = hold(&volume.claims).open[0].number;
    seal_all(&store, &io);
    let above = store.cue().expect("cue above the cover");
    settle(&io);
    let old_in = |scan: StoreScan<SimulatedIo>| rows(scan).iter().filter(|(name, _value)| old.contains(name)).count();
    let mut value = Vec::new();
    assert!(volume.table.holds(holder), "the cover's segment merged under a cue below it");
    assert_eq!(volume.covers.len(), 1, "the cover left while a cue below it lived");
    assert!(below.get(COLUMN, &old[0], &mut value).expect("get below"), "the cue below lost a row live at its point");
    assert!(!above.get(COLUMN, &old[0], &mut value).expect("get above"), "a cue above the cover read a deleted key");
    assert_eq!(old_in(above.iter_from(COLUMN, &[0; WIDTH as usize], Direction::Ascending).expect("scan above")), 0, "a scan above the cover read a deleted key");
    assert_eq!(read(&store, &old[0]), None, "a deleted key read at the current point");
    drop(below);
    drop(above);
    store.state.ask_compaction();
    settle(&io);
    let kept = hold(&volume.summary).rows().filter(|row| row.facts.places.lowest <= before_cover).count();
    assert!(!volume.table.holds(holder), "the cover's segment never merged once the cue went");
    assert_eq!(kept, 0, "the merge kept rows no cue reads");
    assert_eq!(volume.covers.len(), 0, "the cover outlived every row under it");
    assert_eq!(read(&store, &old[0]), None, "a deleted key read after the merge");
    assert_eq!(old_in(store.iter_from(COLUMN, &[0; WIDTH as usize], Direction::Ascending).expect("scan")), 0, "a scan read a deleted key");
    store.close().expect("close");
}

const CUE_SEEDS: u64 = 8;
const CUE_STEPS: u64 = 400;
const CUE_KEYS: u64 = 300;
const MOST_CUES_HELD: usize = 4;
const CUE_CHECK_EVERY: u64 = 25;

// a held cue and the model as it stood when the cue was taken
struct HeldCue {
    cue: reel::engine::cue::Cue<SimulatedIo>,
    model: Model,
}

// every read a cue gives matches the model at its point: a get of each key the run has written, and a whole walk
fn check_cue(held: &HeldCue, written: &[Vec<u8>], step: u64, seed: u64) {
    let mut value = Vec::new();
    for name in written {
        let got = held.cue.get(COLUMN, name, &mut value).expect("get at a cue").then(|| value.clone());
        assert_eq!(got.as_ref(), held.model.get(name), "seed {seed} step {step}: a cue read a key off its point");
    }
    let walked = rows(held.cue.iter_from(COLUMN, b"", Direction::Ascending).expect("scan at a cue"));
    let expected: Rows = held.model.iter().map(|(name, value)| (name.clone(), value.clone())).collect();
    assert!(walked == expected, "seed {seed} step {step}: a cue's walk read off its point");
}

// random puts, deletes and range deletes on a sorted column, with single segments sealed at random points and cues held across
// all of it: every cue reads its own point by get and by walk, to the end of the run, and the current point reads the model
#[test]
fn cues_held_across_covers_and_single_seals() {
    for seed in seeds(0..CUE_SEEDS) {
        cues_held_at(seed);
    }
}

// seed 606453 had a merge charge a kept row's death by count and a walk charge it again, so its block counted no live row
// and a walk crossed it unread
#[test]
fn cues_held_over_a_row_charged_twice() {
    cues_held_at(606_453);
}

/// Run the cue test on one seed
fn cues_held_at(seed: u64) {
    {
        let io = SimulatedIo::new(Seed(300 + seed));
        let mut random = SeededRandom::new(Seed(400 + seed));
        let store = open(&io, CAPACITY);
        let mut model = Model::new();
        let written: Vec<Vec<u8>> = (0..CUE_KEYS).map(key).collect();
        let mut sorted_names = written.clone();
        sorted_names.sort();
        let mut cues: Vec<HeldCue> = Vec::new();
        for step in 0..CUE_STEPS {
            let name = key(random.below(CUE_KEYS));
            match random.below(20) {
                0 => {
                    let at = random.below(CUE_KEYS) as usize;
                    let until = (at + 1 + random.below(30) as usize).min(sorted_names.len() - 1);
                    let (start, end) = (sorted_names[at].clone(), sorted_names[until].clone());
                    store.delete_range(COLUMN, &start, &end).expect("delete range");
                    model.retain(|name, _value| *name < start || *name >= end);
                },
                1..=3 => {
                    store.delete(COLUMN, &name).expect("delete");
                    model.remove(&name);
                },
                4 => {
                    // one open segment seals on its own, so a cover and the rows under it can seal apart
                    let volume = &store.state.volumes[0];
                    let mut claims = hold(&volume.claims);
                    let open: Vec<usize> = (0..claims.open.len()).filter(|at| claims.open[*at].row_count.0 > 0).collect();
                    if let Some(&at) = open.get(random.below(open.len().max(1) as u64) as usize) {
                        store.state.seal_at(&mut claims, volume, at);
                    }
                    drop(claims);
                    settle(&io);
                },
                5 => {
                    if cues.len() == MOST_CUES_HELD {
                        cues.remove(random.below(MOST_CUES_HELD as u64) as usize);
                    }
                    cues.push(HeldCue { cue: store.cue().expect("cue"), model: model.clone() });
                },
                _ => {
                    let value = format!("{seed}-{step}").into_bytes();
                    // a marked put lands in a segment of its own, so rows and covers spread over open segments
                    if random.below(3) == 0 {
                        store.put_marked(COLUMN, &name, &value, MARK).expect("put");
                    } else {
                        store.put(COLUMN, &name, &value).expect("put");
                    }
                    model.insert(name, value);
                },
            }
            if step % CUE_CHECK_EVERY == 0 {
                for held in &cues {
                    check_cue(held, &written, step, seed);
                }
            }
        }
        for held in &cues {
            check_cue(held, &written, CUE_STEPS, seed);
        }
        check(&store, &model, &written);
        drop(cues);
        store.close().expect("close");
    }
}

const COPY_STEPS: u64 = 2;
const TAIL_KEYS: usize = 10;

use reel::io::Io;
use reel_core::units::FileOffset;
use reel_sim::file_image::BitMask;
use reel_sim::simulated_disk::OperationCount;

fn image_path() -> PathBuf {
    PathBuf::from(ROOT).join(format!("sorted-{}.image", COLUMN.0))
}

fn has_file(io: &SimulatedIo, path: &Path) -> bool {
    io.list_directory(Path::new(ROOT)).expect("list").iter().any(|listed| listed == path)
}

// overwrites, deletes and a range delete over a few seals, so the footers hold values, graves and a cover's removals
fn fill_for_reopen(store: &Store, io: &SimulatedIo, keys: u64) -> Model {
    let mut model = Model::new();
    for number in 0..keys {
        store.put(COLUMN, &key(number), format!("first-{number}").as_bytes()).expect("put");
        model.insert(key(number), format!("first-{number}").into_bytes());
    }
    seal_all(store, io);
    for number in (0..keys).step_by(3) {
        store.put(COLUMN, &key(number), b"second").expect("put");
        model.insert(key(number), b"second".to_vec());
    }
    for number in (1..keys).step_by(7) {
        store.delete(COLUMN, &key(number)).expect("delete");
        model.remove(&key(number));
    }
    let names: Vec<Vec<u8>> = model.keys().cloned().collect();
    let (start, end) = (names[names.len() / 3].clone(), names[names.len() / 3 + COVERED].clone());
    store.delete_range(COLUMN, &start, &end).expect("delete range");
    model.retain(|name, _value| *name < start || *name >= end);
    seal_all(store, io);
    model
}

fn check_all(store: &Store, model: &Model, keys: u64, seed: u64) {
    let written: Vec<Vec<u8>> = (0..keys).map(key).collect();
    check(store, model, &written);
    check_entries(store, model);
    check_scans(store, model, &mut SeededRandom::new(Seed(seed)));
}

// open, check the fill read the footers and left no file of its own, and check every answer against the model
fn reopen_and_check(io: &SimulatedIo, capacity: RowCount, model: &Model, seed: u64) {
    let store = open(io, capacity);
    assert!(report(&store).sorted_fill_bytes.0 > 0, "open filled the index from no footer");
    assert!(!has_file(io, &image_path()), "a sorted index left a file");
    check_all(&store, model, KEYS, seed);
    store.close().expect("close");
}

// a clean close saves nothing for the index, the next open fills it from the footers with the entries it held, every get, scan
// and entry matches the model, and the index takes seals after
#[test]
fn a_reopen_fills_the_index_from_the_footers() {
    let io = SimulatedIo::new(Seed(30));
    let store = open(&io, CAPACITY);
    let mut model = fill_for_reopen(&store, &io, KEYS);
    let entries = store.state.sorted_index(COLUMN).expect("an index").entry_count();
    store.close().expect("close");
    assert!(!has_file(&io, &image_path()), "a close saved a sorted index");
    let store = open(&io, CAPACITY);
    assert_eq!(store.state.sorted_index(COLUMN).expect("an index").entry_count(), entries);
    check_all(&store, &model, KEYS, 31);
    for number in (0..KEYS).step_by(5) {
        store.put(COLUMN, &key(number), b"third").expect("put");
        model.insert(key(number), b"third".to_vec());
    }
    seal_all(&store, &io);
    check_all(&store, &model, KEYS, 32);
    store.close().expect("close");
    reopen_and_check(&io, CAPACITY, &model, 33);
}

// keys past what an index of one row holds, whose pool is a page for each part
const SHORT_KEYS: u64 = 40_000;

// a reopen whose index has room for its pool's floor alone, far under the keys it finds, marks the index past what it holds, and
// every get and a whole scan still answer the model
#[test]
fn a_reopen_into_too_little_room_still_answers() {
    let io = SimulatedIo::new(Seed(38));
    let store = open(&io, CAPACITY);
    let model = fill_for_reopen(&store, &io, SHORT_KEYS);
    store.close().expect("close");
    drop(store);
    let store = open(&io, RowCount(1));
    assert!(report(&store).sorted_columns[0].is_marked, "an index under its keys took no mark");
    let written: Vec<Vec<u8>> = (0..SHORT_KEYS).map(key).collect();
    check(&store, &model, &written);
    let whole: Rows = model.iter().map(|(name, value)| (name.clone(), value.clone())).collect();
    assert_eq!(rows(store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan")), whole);
    check_scans(&store, &model, &mut SeededRandom::new(Seed(39)));
    store.close().expect("close");
}

// a kill after a reopen and later seals fills the index again at the next open
#[test]
fn a_kill_after_a_reopen_fills_again() {
    let io = SimulatedIo::new(Seed(34));
    let store = open(&io, CAPACITY);
    let mut model = fill_for_reopen(&store, &io, KEYS);
    store.close().expect("close");
    let store = open(&io, CAPACITY);
    for number in (0..KEYS).step_by(4) {
        store.put(COLUMN, &key(number), b"after").expect("put");
        model.insert(key(number), b"after".to_vec());
    }
    seal_all(&store, &io);
    store.flush().expect("flush");
    io.kill();
    drop(store);
    reopen_and_check(&io, CAPACITY, &model, 35);
}

// a range delete, a put and a delete still open at close cross the reopen: open fills from the footers, replays them, runs the
// cover pass, and every answer matches
#[test]
fn open_rows_at_close_cross_a_reopen() {
    let io = SimulatedIo::new(Seed(57));
    let store = open(&io, CAPACITY);
    let mut model = fill_for_reopen(&store, &io, KEYS);
    let names: Vec<Vec<u8>> = model.keys().cloned().collect();
    let (start, end) = (names[names.len() / 2].clone(), names[names.len() / 2 + COVERED].clone());
    store.delete_range(COLUMN, &start, &end).expect("delete range");
    model.retain(|name, _value| *name < start || *name >= end);
    store.put(COLUMN, &names[1], b"open").expect("put");
    model.insert(names[1].clone(), b"open".to_vec());
    store.delete(COLUMN, &names[2]).expect("delete");
    model.remove(&names[2]);
    store.close().expect("close");
    let store = open(&io, CAPACITY);
    // rows still open at the close sit in the write index, which the fill never holds, so the entries match once they seal
    let written: Vec<Vec<u8>> = (0..KEYS).map(key).collect();
    check(&store, &model, &written);
    check_scans(&store, &model, &mut SeededRandom::new(Seed(58)));
    seal_all(&store, &io);
    check_all(&store, &model, KEYS, 158);
    store.close().expect("close");
    reopen_and_check(&io, CAPACITY, &model, 258);
}

// a cue taken and dropped before the close leaves the reopen whole
#[test]
fn a_cue_before_the_close_reopens_whole() {
    let io = SimulatedIo::new(Seed(66));
    let store = open(&io, CAPACITY);
    let model = fill_for_reopen(&store, &io, KEYS);
    drop(store.cue().expect("cue"));
    store.close().expect("close");
    reopen_and_check(&io, CAPACITY, &model, 67);
}

// a close with a merge mid-copy on the store's own worker stops it, and the reopen fills an index that matches the store
#[test]
fn a_close_mid_merge_reopens_whole() {
    let io = SimulatedIo::new(Seed(68));
    let store = open(&io, CAPACITY);
    let volume = &store.state.volumes[0];
    volume.holds.compaction.store(true, Ordering::Release);
    let mut model = Model::new();
    for number in 0..MERGE_KEYS {
        store.put(COLUMN, &key(number), b"first").expect("put");
        model.insert(key(number), b"first".to_vec());
    }
    seal(&store, &io);
    for number in (0..MERGE_KEYS).filter(|number| number % 3 != 2) {
        store.put(COLUMN, &key(number), b"second").expect("put");
        model.insert(key(number), b"second".to_vec());
    }
    seal(&store, &io);
    // a merge of the two segments, run by hand until it has its first output and has copied rows into it, then left there
    let first = hold(&volume.summary).rows().map(|row| row.facts.segment).min().expect("a segment");
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    job.rewrite.forced = Some(first);
    job.rewrite.start();
    let is_copying = || volume.output_floor() != reel::engine::volume::NO_SEAL;
    for _step in 0..STEPS {
        if is_copying() || !job.rewrite.is_running() {
            break;
        }
        job.rewrite.step();
    }
    assert!(is_copying(), "no merge took an output");
    for _step in 0..COPY_STEPS {
        if job.rewrite.phase == RewritePhase::Installing {
            break;
        }
        job.rewrite.step();
    }
    assert!(job.rewrite.phase != RewritePhase::Installing && is_copying(), "the merge reached its install before the close");
    store.close().expect("close");
    let store = open(&io, CAPACITY);
    let written: Vec<Vec<u8>> = (0..MERGE_KEYS).map(key).collect();
    check(&store, &model, &written);
    check_entries(&store, &model);
    check_scans(&store, &model, &mut SeededRandom::new(Seed(69)));
    store.close().expect("close");
}

// a put that fails after its place was given leaves the store whole, and the reopen after a clean close answers the model
#[test]
fn a_failed_put_before_the_close_reopens_whole() {
    let io = SimulatedIo::new(Seed(72));
    let store = open(&io, CAPACITY);
    let model = fill_for_reopen(&store, &io, KEYS);
    io.fail_writes(OperationCount(1));
    let _failed = store.put(COLUMN, &key(KEYS + 3), b"lost");
    store.flush().ok();
    store.close().ok();
    let store = open(&io, CAPACITY);
    store.close().expect("close");
    // the write either landed or it didn't, and the model takes what the store says
    let store = open(&io, CAPACITY);
    let mut model = model;
    if let Some(value) = read(&store, &key(KEYS + 3)) {
        model.insert(key(KEYS + 3), value);
    }
    check_all(&store, &model, KEYS + 4, 73);
    store.close().expect("close");
}

// two sorted columns and a complete one on one volume: each sorted column fills from its own footers, and every get and scan of
// each column answers its own model with no footer asked
#[test]
fn two_sorted_columns_and_a_complete_one_reopen_whole() {
    // column 1 is COLUMN; column 2 sorted the same way; column 3 complete
    const SECOND: ColumnId = ColumnId(2);
    const THIRD: ColumnId = ColumnId(3);
    let three = || {
        let mut config = config(CAPACITY);
        let sorted = config.columns[0].clone();
        config.columns.push(ColumnConfig { id: SECOND, name: "second".to_string(), ..sorted });
        config.columns.push(ColumnConfig {
            id: THIRD,
            name: "third".to_string(),
            key_shape: KeyShape::Variable { most: 64 },
            index: ColumnIndex::Complete,
            least_rows: Some(CAPACITY),
            ..config.columns[0].clone()
        });
        config
    };
    let scan = |store: &Store, column: ColumnId| rows(store.iter_from(column, b"", Direction::Ascending).expect("scan"));
    let io = SimulatedIo::new(Seed(70));
    let store = ReelStore::open(io.clone(), &three()).expect("open");
    // each sorted column under its own model, with values that say which column wrote them
    let (mut first, mut second) = (Model::new(), Model::new());
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), format!("one-{number}").as_bytes()).expect("put");
        first.insert(key(number), format!("one-{number}").into_bytes());
        if number % 2 == 0 {
            store.put(SECOND, &key(number), format!("two-{number}").as_bytes()).expect("put");
            second.insert(key(number), format!("two-{number}").into_bytes());
        }
        store.put(THIRD, format!("third-{number}").as_bytes(), b"complete").expect("put");
    }
    seal_all(&store, &io);
    store.close().expect("close");
    drop(store);

    let store = ReelStore::open(io.clone(), &three()).expect("open");
    let asks = report(&store).sorted_footer_asks;
    let as_rows = |model: &Model| model.iter().map(|(name, value)| (name.clone(), value.clone())).collect::<Vec<_>>();
    assert_eq!(scan(&store, COLUMN), as_rows(&first), "column 1 read another column's rows");
    assert_eq!(scan(&store, SECOND), as_rows(&second), "column 2 read another column's rows");
    for number in 0..KEYS {
        let mut value = Vec::new();
        let got = store.get(COLUMN, &key(number), &mut value).expect("get").then(|| value.clone());
        assert_eq!(got.as_ref(), first.get(&key(number)));
        let got = store.get(SECOND, &key(number), &mut value).expect("get").then(|| value.clone());
        assert_eq!(got.as_ref(), second.get(&key(number)));
        assert!(store.get(THIRD, format!("third-{number}").as_bytes(), &mut value).expect("get"));
    }
    assert_eq!(report(&store).sorted_footer_asks, asks, "a sorted get asked the footers");
    store.close().expect("close");
}

// the gets of a store against its model: (found, refused), with no key read absent and every value found its own
fn read_counts(store: &Store, model: &Model) -> (u64, u64) {
    let (mut found, mut refused) = (0, 0);
    for (name, value) in model {
        let mut got = Vec::new();
        match store.get(COLUMN, name, &mut got) {
            Ok(true) => {
                assert_eq!(&got, value);
                found += 1;
            },
            Ok(false) => panic!("a live key read absent: the index has a hole and no mark"),
            Err(_error) => refused += 1,
        }
    }
    (found, refused)
}

// A sealed segment whose second row block rots while the store is closed: open's fill stops at that block, so the keys of the
// later, readable blocks get no entry. Once the damage is cleared they read their values by get and by scan, the rotted block's
// keys refuse, the index stays marked, and the next open finds the same
#[test]
fn a_segment_with_an_unread_block_still_reads_its_rows() {
    let io = SimulatedIo::new(Seed(170));
    let store = open(&io, CAPACITY);
    let mut model = Model::new();
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), format!("value-{number}").as_bytes()).expect("put");
        model.insert(key(number), format!("value-{number}").into_bytes());
    }
    let holder = hold(&store.state.volumes[0].claims).open[0].number;
    seal_all(&store, &io);
    let table = &store.state.volumes[0].table;
    let rotted = table.walk_block(holder, 1).expect("a second block");
    let at = rotted.entry.block_at();
    store.close().expect("close");
    drop(store);
    let path = reel::engine::open_segment::data_path(&PathBuf::from(ROOT), holder).to_path_buf();
    io.rot(&path, FileOffset(at.0 + 40), BitMask(1)).expect("rot");

    let mut counts = None;
    for round in 0..2 {
        let store = open(&io, CAPACITY);
        hold_rebuilds_here(&store);
        store.clear_damage(COLUMN, b"", None).expect("clear");
        let reading = report(&store).sorted_columns[0];
        assert!(reading.is_marked, "round {round}: a fill that missed rows left the index unmarked");
        let (found, refused) = read_counts(&store, &model);
        eprintln!("unread block, round {round}: found {found} refused {refused} of {}", model.len());
        assert_eq!(found + refused, model.len() as u64);
        assert!(refused > 0 && found > refused, "round {round}: found {found} refused {refused}");
        assert_eq!(*counts.get_or_insert((found, refused)), (found, refused), "round {round}: the reads changed across a reopen");
        // a scan over the last block's keys reads them all, and one across the rotted block refuses
        // the model's last few keys sit in the segment's last block, well past the rotted second one
        let first_of_last = model.keys().rev().nth(TAIL_KEYS).cloned().expect("keys");
        let tail: Rows = model.range(first_of_last.clone()..).map(|(name, value)| (name.clone(), value.clone())).collect();
        assert_eq!(rows(store.iter_from(COLUMN, &first_of_last, Direction::Ascending).expect("scan")), tail, "round {round}");
        let mut scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
        let (mut name, mut value) = (Vec::new(), Vec::new());
        let failed = loop {
            match scan.next_row(&mut name, &mut value) {
                Ok(ScanStep::Found) => {},
                Ok(_end) => break false,
                Err(_error) => break true,
            }
        };
        assert!(failed, "round {round}: a scan across the rotted block went on");
        store.close().expect("close");
    }
}

fn hold_rebuilds_here(store: &Store) {
    for volume in &store.state.volumes {
        volume.holds.rebuild.store(true, Ordering::Release);
    }
}

// open a store whose index the store sizes from the keys it held, with no declared count
fn open_sized(io: &SimulatedIo) -> Store {
    let mut config = config(RowCount(0));
    reel::engine::engine_sizing::EXACT_INDEX_ROWS.with(|exact| exact.set(false));
    config.columns[0].least_rows = None;
    ReelStore::open(io.clone(), &config).expect("open")
}

fn sorted_reading(store: &Store) -> reel_core::maintenance_report::SortedReading {
    report(store).sorted_columns.iter().find(|reading| reading.column == COLUMN).copied().expect("a reading")
}

// the bytes a sorted index asks for at open: twice the keys with the store's headroom past them, a quarter of the cap for the one
// index
fn asked_for(keys: u64, cap: ByteCount) -> ByteCount {
    use reel::engine::engine_sizing::{INDEX_GROWTH, store_headroom};
    ByteCount(reel::index::sorted_keys::planned_index_bytes(WIDTH as usize, RowCount(keys * INDEX_GROWTH)).0 + store_headroom(cap.0, 1).0)
}

// an empty index asks for its whole part of the cap, and an open after a clean close or a kill asks by the rows on disk,
// overwrites and all. Every answer matches the model
#[test]
fn a_sorted_index_is_sized_by_the_keys_it_held() {
    let io = SimulatedIo::new(Seed(202));
    let store = open_sized(&io);
    // no merge drops an overwrite, so the rows on disk stay twice the keys
    store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
    let empty = sorted_reading(&store);
    // an empty index asks for the whole cap, the one index's part of it
    let cap = empty.asked_bytes;
    // an empty index takes its whole part, rounded down to the pages its pool holds
    assert!(empty.got_bytes <= empty.asked_bytes && empty.got_bytes.0 >= empty.asked_bytes.0 / 10 * 9, "{empty:?}");
    let mut model = Model::new();
    for round in 0..2 {
        for number in 0..KEYS {
            let value = format!("{round}-{number}").into_bytes();
            store.put(COLUMN, &key(number), &value).expect("put");
            model.insert(key(number), value);
        }
        seal_all(&store, &io);
    }
    let written: Vec<Vec<u8>> = (0..KEYS).map(key).collect();
    check(&store, &model, &written);
    store.close().expect("close");
    drop(store);

    let store = open_sized(&io);
    store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
    let clean = sorted_reading(&store);
    assert_eq!(clean.asked_bytes, asked_for(KEYS * 2, cap), "the rows on disk hold every overwrite: {clean:?}");
    assert!(clean.got_bytes <= clean.asked_bytes && clean.capacity.0 >= KEYS, "{clean:?}");
    check(&store, &model, &written);
    store.flush().expect("flush");
    io.kill();
    drop(store);

    let store = open_sized(&io);
    let killed = sorted_reading(&store);
    assert_eq!(killed.asked_bytes, asked_for(KEYS * 2, cap), "the rows on disk hold every overwrite: {killed:?}");
    check(&store, &model, &written);
    store.close().expect("close");
}

const WRITER_SEEDS: u64 = 4;
const WRITER_KEYS: u64 = 1_500;
const OPEN_KEYS: u64 = 2_000;
const NEW_KEYS_A_BATCH: u64 = 1;
const STEP_WRITE_ONE_IN: u64 = 16;
const DENSE_WRITE_ONE_IN: u64 = 2;
const SEAL_EVERY: u64 = 40;

/// What one walk beside a writer did
#[derive(Debug, Default)]
struct WalkTally {
    fills: u64,
    heads_together: u64,
    asked: u64,
    batched: u64,
    lookups: u64,
}

// the model's next key past the walk's last one, in the walk's direction
fn next_in(model: &Model, last: Option<&Vec<u8>>, direction: Direction) -> Option<(Vec<u8>, Vec<u8>)> {
    let found = match (direction, last) {
        (Direction::Ascending, None) => model.iter().next(),
        (Direction::Ascending, Some(last)) => model.range((Bound::Excluded(last.clone()), Bound::Unbounded)).next(),
        (Direction::Descending, None) => model.iter().next_back(),
        (Direction::Descending, Some(last)) => model.range(..last.clone()).next_back(),
    };
    found.map(|(name, value)| (name.clone(), value.clone()))
}

// a writer's change between steps: a key never seen before, an overwrite or a delete
fn write_one(store: &Store, model: &mut Model, random: &mut SeededRandom, fresh: &mut u64) {
    let names: Vec<Vec<u8>> = model.keys().cloned().collect();
    match random.below(4) {
        0 | 1 => {
            let name = key(*fresh);
            *fresh += 1;
            store.put(COLUMN, &name, format!("new-{fresh}").as_bytes()).expect("put");
            model.insert(name, format!("new-{fresh}").into_bytes());
        },
        2 if !names.is_empty() => {
            let name = names[random.below(names.len() as u64) as usize].clone();
            store.put(COLUMN, &name, b"overwritten").expect("put");
            model.insert(name, b"overwritten".to_vec());
        },
        _ if !names.is_empty() => {
            let name = names[random.below(names.len() as u64) as usize].clone();
            store.delete(COLUMN, &name).expect("delete");
            model.remove(&name);
        },
        _ => {},
    }
}

// walk for values or keys alone with a writer between steps, and check each answer is the model's next key as of its batch
fn walk_beside_writer(store: &Store, io: &SimulatedIo, model: &mut Model, random: &mut SeededRandom, fresh: &mut u64, (direction, write_odds, is_key_walk, told): (Direction, Option<u64>, bool, Option<u64>)) -> WalkTally {
    let is_writing = write_odds.is_some();
    let start = match direction {
        Direction::Ascending => Vec::new(),
        Direction::Descending => vec![0xFF; WIDTH as usize + 1],
    };
    let mut range = reel::engine::scan_state::from_key(COLUMN, &start, direction);
    if let Some(rows) = told {
        range = range.wanting(RowCount(rows));
    }
    let mut scan = store.state.take_scan(&range, None).expect("scan");
    let (mut name, mut value) = (Vec::new(), Vec::new());
    let mut tally = WalkTally::default();
    let heads_before = store.state.heads_together.load(Ordering::Relaxed);
    let mut seen = model.clone();
    let mut last: Option<Vec<u8>> = None;
    loop {
        if !scan.batch.has_next() {
            tally.fills += 1;
            if write_odds == Some(STEP_WRITE_ONE_IN) && tally.fills.is_multiple_of(SEAL_EVERY) {
                store.seal_now();
                settle(io);
            }
            // a seal started and left unsettled keeps the old open trees in the view beside the new ones
            if is_writing && tally.fills == 1 {
                store.seal_now();
            }
            for _write in 0..NEW_KEYS_A_BATCH * u64::from(is_writing) {
                write_one(store, model, random, fresh);
            }
            seen = model.clone();
        }
        let want = match is_key_walk {
            true => reel::engine::scan_path::ScanWant::Key,
            false => reel::engine::scan_path::ScanWant::Value(&mut value),
        };
        let step = store.state.step_scan(&mut scan, &mut name, want).expect("step");
        let expected = next_in(&seen, last.as_ref(), direction);
        match step {
            ScanStep::Found if is_key_walk => {
                assert_eq!(Some(&name), expected.as_ref().map(|(expected, _value)| expected), "after {last:?} going {direction:?}");
                last = Some(name.clone());
            },
            ScanStep::Found => {
                assert_eq!(Some((name.clone(), value.clone())), expected, "after {last:?} going {direction:?}");
                last = Some(name.clone());
            },
            ScanStep::End => {
                assert_eq!(expected, None, "the walk ended early going {direction:?}");
                break;
            },
            ScanStep::Corrupt => panic!("a corrupt record"),
        }
        // a write inside a batch shows from the next one
        if write_odds.is_some_and(|odds| random.one_in(odds)) {
            write_one(store, model, random, fresh);
        }
    }
    tally.heads_together = store.state.heads_together.load(Ordering::Relaxed) - heads_before;
    tally.asked = scan.asked;
    tally.batched = scan.batched;
    tally.lookups = scan.lookups;
    store.state.give_back_scan(scan);
    tally
}

// every batch beside a writer answers each row at or below its point, and heads of shards no row reached stay placed
#[test]
fn writes_beside_batches_match_the_model() {
    for seed in seeds(0..WRITER_SEEDS) {
        writes_beside_batches(seed);
    }
}

// seed 1127449: a batch answered up to a key, passed the graves past it to the column's end, and a put landed between the key and
// the graves while the batch stepped out. The next batch, at a point past the put, started past the graves and ended early
#[test]
fn a_put_behind_passed_graves_shows_in_the_next_batch() {
    writes_beside_batches(1_127_449);
}

// one seed of the walks beside a writer, each batch answering every row at or below its point
fn writes_beside_batches(seed: u64) {
    {
        let io = SimulatedIo::new(Seed(seed));
        let store = open(&io, CAPACITY);
        let mut model = Model::new();
        let mut random = SeededRandom::new(Seed(seed));
        for number in 0..WRITER_KEYS {
            store.put(COLUMN, &key(number), format!("{number}").as_bytes()).expect("put");
            model.insert(key(number), format!("{number}").into_bytes());
        }
        seal_all(&store, &io);
        let mut fresh = WRITER_KEYS;
        // open rows in every shard, so a head placed again descends a tree
        for _key in 0..OPEN_KEYS {
            write_one(&store, &mut model, &mut random, &mut fresh);
        }
        let shards = reel::index::write_index::SHARD_COUNT as u64;
        // a write between most steps reaches shards whose heads moved inside the batch, which the next batch places again
        for direction in [Direction::Ascending, Direction::Descending] {
            let tally = walk_beside_writer(&store, &io, &mut model, &mut random, &mut fresh, (direction, Some(DENSE_WRITE_ONE_IN), false, None));
            println!("seed {seed} {direction:?} beside a busy writer: {tally:?}");
        }
        for direction in [Direction::Ascending, Direction::Descending] {
            let tally = walk_beside_writer(&store, &io, &mut model, &mut random, &mut fresh, (direction, Some(STEP_WRITE_ONE_IN), false, None));
            println!("seed {seed} {direction:?} beside a writer: {tally:?}");
            // a batch places again only the heads whose shard took a row, far fewer than every shard for each BATCH_KEYS keys it
            // asked, whatever size the batches grew to
            let stretches = tally.asked.div_ceil(reel::engine::scan_state::BATCH_KEYS as u64);
            assert!(tally.heads_together < stretches * shards / 2, "{tally:?}");
        }
        // with no writer every head stays placed across batches, and is placed again only when a batch runs past the keys it
        // holds ahead, far fewer times than once for each BATCH_KEYS keys asked
        let tally = walk_beside_writer(&store, &io, &mut model, &mut random, &mut fresh, (Direction::Ascending, None, false, None));
        println!("seed {seed} quiet: {tally:?}");
        let stretches = tally.asked.div_ceil(reel::engine::scan_state::BATCH_KEYS as u64);
        assert!(tally.heads_together < stretches * shards / 8, "{tally:?}");
        check_scans(&store, &model, &mut random);
        store.close().expect("close");
    }
}

const DRAIN_PIECE: u64 = 40;

/// The models a drained row may answer: the model after each write since the drain began. A row comes from a batch filled at
/// one of them, and batches fill in order, so each row answers the model its batch saw or a later one, never an earlier one
struct DrainCheck {
    models: Vec<Model>,
    at: usize,
    last: Option<Vec<u8>>,
    rows: u64,
}

impl DrainCheck {
    // keep the model after a write, which a batch filled from here on sees
    fn wrote(&mut self, model: &Model) {
        self.models.push(model.clone());
    }

    // check a row is the next key of the model its batch saw: the earliest model from the last row's on that it matches
    fn take(&mut self, (name, value): (&[u8], &[u8]), direction: Direction) {
        let row = Some((name.to_vec(), value.to_vec()));
        let Some(found) = (self.at..self.models.len()).find(|at| row == next_in(&self.models[*at], self.last.as_ref(), direction)) else {
            let newest = self.models.last().expect("a model");
            assert_eq!(row, next_in(newest, self.last.as_ref(), direction), "after {:?} going {direction:?}", self.last);
            unreachable!("the newest model matched no row it was asked for");
        };
        self.at = found;
        self.last = Some(name.to_vec());
        self.rows += 1;
    }
}

// drain a walk in pieces of a few rows, a write between pieces, with every other piece a one-row step when mixed, and check each
// row against the model as of its batch
fn drain_beside_writer(store: &Store, model: &mut Model, random: &mut SeededRandom, fresh: &mut u64, (direction, write_odds, is_mixed): (Direction, Option<u64>, bool)) -> u64 {
    let start = match direction {
        Direction::Ascending => Vec::new(),
        Direction::Descending => vec![0xFF; WIDTH as usize + 1],
    };
    let mut scan = store.iter_from(COLUMN, &start, direction).expect("scan");
    let (mut name, mut value) = (Vec::new(), Vec::new());
    let mut check = DrainCheck { models: vec![model.clone()], at: 0, last: None, rows: 0 };
    for piece in 0_u64.. {
        let step = if is_mixed && piece % 2 == 1 {
            let step = scan.next_row(&mut name, &mut value).expect("step");
            if step == ScanStep::Found {
                check.take((&name, &value), direction);
            }
            step
        } else {
            let mut left = 1 + random.below(DRAIN_PIECE);
            scan.drain((&mut name, &mut value), |row_name, row_value| {
                check.take((row_name, row_value), direction);
                left -= 1;
                left > 0
            })
            .expect("drain")
        };
        match step {
            ScanStep::Found => {},
            ScanStep::End => break,
            ScanStep::Corrupt => panic!("a corrupt record"),
        }
        if write_odds.is_some_and(|odds| random.one_in(odds)) {
            write_one(store, model, random, fresh);
            check.wrote(model);
        }
    }
    // the end comes from the last row's batch or a later one, so some model from the last row's on has no key past it
    let is_end = (check.at..check.models.len()).any(|at| next_in(&check.models[at], check.last.as_ref(), direction).is_none());
    assert!(is_end, "the drain ended early going {direction:?}: {:?}", next_in(model, check.last.as_ref(), direction));
    check.rows
}

// a drain lends the model's rows quiet and beside a writer, alone and mixed with one-row steps, over sealed rows and open ones
#[test]
fn drains_match_the_model() {
    for seed in seeds(0..WRITER_SEEDS) {
        let io = SimulatedIo::new(Seed(seed));
        let store = open(&io, CAPACITY);
        let mut model = Model::new();
        let mut random = SeededRandom::new(Seed(seed));
        for number in 0..WRITER_KEYS {
            store.put(COLUMN, &key(number), format!("{number}").as_bytes()).expect("put");
            model.insert(key(number), format!("{number}").into_bytes());
        }
        seal_all(&store, &io);
        let mut fresh = WRITER_KEYS;
        for _key in 0..OPEN_KEYS {
            write_one(&store, &mut model, &mut random, &mut fresh);
        }
        for direction in [Direction::Ascending, Direction::Descending] {
            for write_odds in [None, Some(DENSE_WRITE_ONE_IN), Some(STEP_WRITE_ONE_IN)] {
                for is_mixed in [false, true] {
                    let rows = drain_beside_writer(&store, &mut model, &mut random, &mut fresh, (direction, write_odds, is_mixed));
                    if write_odds.is_none() {
                        assert_eq!(rows, model.len() as u64, "a quiet drain missed rows");
                    }
                }
            }
        }
        check_scans(&store, &model, &mut random);
        store.close().expect("close");
    }
}

const SEALED_UNDER: u64 = 600;
const OPEN_UNDER: u64 = 400;
const WIDE_ONE_IN: u64 = 5;
const WIDE_VALUE: usize = 6 * 1024;
const HUGE_VALUE: usize = 12 * 1024;

// a value whose size says how its record reads: in the window, by a joined read of the slot file, or alone
fn open_value(number: u64) -> Vec<u8> {
    let mut value = format!("open-{number}").into_bytes();
    match number % WIDE_ONE_IN {
        0 => value.resize(WIDE_VALUE, b'w'),
        1 => value.resize(HUGE_VALUE, b'h'),
        _ => {},
    }
    value
}

// sealed keys with open overwrites and new keys among them, small, wide and too wide for a batch
fn open_rows_store(io: &SimulatedIo) -> (Store, Model) {
    let store = open(io, CAPACITY);
    let mut model = Model::new();
    for number in 0..SEALED_UNDER {
        store.put(COLUMN, &key(number), format!("{number}").as_bytes()).expect("put");
        model.insert(key(number), format!("{number}").into_bytes());
    }
    seal_all(&store, io);
    for number in (0..SEALED_UNDER).step_by(3).chain(SEALED_UNDER..SEALED_UNDER + OPEN_UNDER) {
        store.put(COLUMN, &key(number), &open_value(number)).expect("put");
        model.insert(key(number), open_value(number));
    }
    (store, model)
}

// a seal after a batch reads its open rows, or inside the read, leaves every answer the model's
#[test]
fn a_seal_under_a_batch_of_open_rows() {
    for is_in_read in [false, true] {
        let io = SimulatedIo::new(Seed(31));
        let (store, model) = open_rows_store(&io);
        let range = reel::engine::scan_state::from_key(COLUMN, b"", Direction::Ascending);
        let mut scan = store.state.take_scan(&range.wanting(RowCount(SEALED_UNDER + OPEN_UNDER)), None).expect("scan");
        let (mut name, mut value) = (Vec::new(), Vec::new());
        let mut found = Vec::new();
        let windowed = store.state.scan_window_records.load(Ordering::Relaxed);
        let (state, disk, is_done) = (Arc::clone(&store.state), io.clone(), Arc::new(AtomicBool::new(!is_in_read)));
        let hook_done = Arc::clone(&is_done);
        io.on_every_read(Some(Arc::new(move || {
            if !hook_done.swap(true, Ordering::AcqRel) {
                state.seal_fullest(&state.volumes[0]);
                for _step in 0..STEPS {
                    if disk.step_any().is_none() {
                        break;
                    }
                }
            }
        })));
        let step = |scan: &mut reel::engine::scan_state::ScanState, name: &mut Vec<u8>, value: &mut Vec<u8>| {
            store.state.step_scan(scan, name, reel::engine::scan_path::ScanWant::Value(value)).expect("step")
        };
        assert_eq!(step(&mut scan, &mut name, &mut value), ScanStep::Found);
        found.push((name.clone(), value.clone()));
        let open_read = scan.batch.entries.iter().filter(|entry| matches!(entry.located, reel::engine::read_path::Located::Open(_)) && entry.record.is_some()).count();
        assert!(open_read > 0, "the batch read none of its open rows");
        if !is_in_read {
            seal_all(&store, &io);
        }
        while step(&mut scan, &mut name, &mut value) == ScanStep::Found {
            found.push((name.clone(), value.clone()));
        }
        io.on_every_read(None);
        assert!(is_done.load(Ordering::Acquire), "the seal never ran");
        let expected: Rows = model.iter().map(|(name, value)| (name.clone(), value.clone())).collect();
        assert_eq!(found.len(), expected.len());
        assert!(found == expected, "the walk's rows differ from the model, in read {is_in_read}");
        println!("in read {is_in_read}: {open_read} open rows read in the first batch, {} from windows, {} of {} batched", store.state.scan_window_records.load(Ordering::Relaxed) - windowed, scan.batched, scan.asked);
        store.state.give_back_scan(scan);
        check_scans(&store, &model, &mut SeededRandom::new(Seed(31)));
        store.close().expect("close");
    }
}

// a seal between a run's index entry and its write index lookup for a head key sends the key to a step, which reads its newest value
#[test]
fn a_seal_between_a_runs_two_looks() {
    const AT: usize = 10;
    for is_new in [true, false] {
        let io = SimulatedIo::new(Seed(32));
        let store = open(&io, CAPACITY);
        let mut model = Model::new();
        for number in 0..SEALED_UNDER {
            store.put(COLUMN, &key(number), format!("old-{number}").as_bytes()).expect("put");
            model.insert(key(number), format!("old-{number}").into_bytes());
        }
        seal_all(&store, &io);
        // one open row inside the first batch, the only key a head shows: a key never seen, or an overwrite of a sealed one
        let name = if is_new { key(SEALED_UNDER) } else { model.keys().nth(AT).expect("a key").clone() };
        store.put(COLUMN, &name, b"newest").expect("put");
        model.insert(name, b"newest".to_vec());
        let (state, disk, is_fired) = (Arc::clone(&store.state), io.clone(), Arc::new(AtomicBool::new(false)));
        let fired = Arc::clone(&is_fired);
        store.state.volumes[0].holds.set_between_run_looks(Some(reel::engine::volume::RunHook(Arc::new(move || {
            if !fired.swap(true, Ordering::AcqRel) {
                state.seal_fullest(&state.volumes[0]);
                for _step in 0..STEPS {
                    if disk.step_any().is_none() {
                        break;
                    }
                }
            }
        }))));
        let found = rows(store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan"));
        store.state.volumes[0].holds.set_between_run_looks(None);
        assert!(is_fired.load(Ordering::Acquire), "no run took the head key");
        let expected: Rows = model.iter().map(|(name, value)| (name.clone(), value.clone())).collect();
        assert_eq!(found.len(), expected.len(), "new key {is_new}");
        assert!(found == expected, "the walk's rows differ from the model, new key {is_new}");
        store.close().expect("close");
    }
}

const TOLD_ROWS: u64 = 256;

// a walk told 256 rows beside a writer, over dense open keys in two open trees, answers each row as of its batch's point while
// its heads read their next keys ahead
#[test]
fn told_walks_beside_a_writer_over_two_open_trees() {
    for seed in seeds(0..WRITER_SEEDS) {
        let io = SimulatedIo::new(Seed(seed));
        let store = open(&io, CAPACITY);
        let mut model = Model::new();
        let mut random = SeededRandom::new(Seed(seed));
        for number in 0..WRITER_KEYS {
            store.put(COLUMN, &key(number), format!("{number}").as_bytes()).expect("put");
            model.insert(key(number), format!("{number}").into_bytes());
        }
        seal_all(&store, &io);
        let mut fresh = WRITER_KEYS;
        for _key in 0..OPEN_KEYS / 2 {
            write_one(&store, &mut model, &mut random, &mut fresh);
        }
        // a seal left unsettled keeps the first open tree in the view beside the one the rest of the keys go to
        store.seal_now();
        for _key in 0..OPEN_KEYS / 2 {
            write_one(&store, &mut model, &mut random, &mut fresh);
        }
        for direction in [Direction::Ascending, Direction::Descending] {
            for write_odds in [Some(DENSE_WRITE_ONE_IN), None] {
                let tally = walk_beside_writer(&store, &io, &mut model, &mut random, &mut fresh, (direction, write_odds, false, Some(TOLD_ROWS)));
                println!("seed {seed} {direction:?} told {TOLD_ROWS} beside a writer one in {write_odds:?}: {tally:?}");
            }
        }
        check_scans(&store, &model, &mut random);
        store.close().expect("close");
    }
}

const REWRITTEN: u64 = 64;
const HOOK_AT: u64 = 2;

// walk a whole column for values at a fixed point
fn walk_at(store: &Store, point: WritePlace) -> Rows {
    let range = reel::engine::scan_state::from_key(COLUMN, b"", Direction::Ascending);
    let mut scan = store.state.take_scan(&range, Some(point)).expect("scan");
    let (mut name, mut value, mut found) = (Vec::new(), Vec::new(), Rows::new());
    loop {
        match store.state.step_scan(&mut scan, &mut name, reel::engine::scan_path::ScanWant::Value(&mut value)).expect("step") {
            ScanStep::Found => found.push((name.clone(), value.clone())),
            ScanStep::Corrupt => panic!("a corrupt record"),
            ScanStep::End => break,
        }
    }
    store.state.give_back_scan(scan);
    found
}

// a write into the shards between a head's read ahead and its use moves their counts, so the run drops the keys read ahead and
// descends fresh. A quiet walk drops none, and both give the model's rows
#[test]
fn a_write_between_a_read_ahead_and_its_use_drops_it() {
    let io = SimulatedIo::new(Seed(33));
    let store = open(&io, CAPACITY);
    let mut model = Model::new();
    for number in 0..SEALED_UNDER {
        store.put(COLUMN, &key(number), format!("old-{number}").as_bytes()).expect("put");
        model.insert(key(number), format!("old-{number}").into_bytes());
    }
    seal_all(&store, &io);
    // dense open keys, so each head reads several keys ahead
    for number in SEALED_UNDER..SEALED_UNDER + OPEN_KEYS {
        store.put(COLUMN, &key(number), format!("open-{number}").as_bytes()).expect("put");
        model.insert(key(number), format!("open-{number}").into_bytes());
    }
    let expected: Rows = model.iter().map(|(name, value)| (name.clone(), value.clone())).collect();
    let point = store.state.read_point(&store.state.volumes[0]);
    let drops = || store.state.ahead_drops.load(Ordering::Relaxed);
    let before = drops();
    assert!(walk_at(&store, point) == expected, "the quiet walk's rows differ from the model");
    assert_eq!(drops(), before, "a quiet walk at a fixed point dropped keys read ahead");
    // the hook rewrites open keys in every shard with their own values, after the heads have read ahead
    let (state, calls) = (Arc::clone(&store.state), Arc::new(AtomicU64::new(0)));
    let called = Arc::clone(&calls);
    store.state.volumes[0].holds.set_between_run_looks(Some(reel::engine::volume::RunHook(Arc::new(move || {
        if called.fetch_add(1, Ordering::AcqRel) == HOOK_AT {
            for number in SEALED_UNDER..SEALED_UNDER + REWRITTEN {
                state.write_value(COLUMN, &key(number), format!("open-{number}").as_bytes(), None).expect("put");
            }
        }
    }))));
    let found = walk_at(&store, point);
    store.state.volumes[0].holds.set_between_run_looks(None);
    assert!(calls.load(Ordering::Acquire) > HOOK_AT, "the run took too few head keys");
    assert!(drops() > before, "a write between the read ahead and its use left the keys standing");
    assert!(found == expected, "the walk's rows differ from the model");
    store.close().expect("close");
}

const RANGE_WALKS: u64 = 12;
const DELETE_ONE_IN: u64 = 3;

// a run keeps its index keys by their place in the index batch: walks over graves, open keys and a limit, up and down, untold and
// told, give the model's rows
#[test]
fn runs_over_graves_heads_and_a_limit_match_the_model() {
    for seed in seeds(0..WRITER_SEEDS) {
        let io = SimulatedIo::new(Seed(seed));
        let store = open(&io, CAPACITY);
        let mut model = Model::new();
        let mut random = SeededRandom::new(Seed(seed));
        for number in 0..WRITER_KEYS {
            store.put(COLUMN, &key(number), format!("{number}").as_bytes()).expect("put");
            model.insert(key(number), format!("{number}").into_bytes());
        }
        seal_all(&store, &io);
        // sealed graves stand in the index among the values
        for number in (0..WRITER_KEYS).filter(|number| number % DELETE_ONE_IN == 0) {
            store.delete(COLUMN, &key(number)).expect("delete");
            model.remove(&key(number));
        }
        seal_all(&store, &io);
        let mut fresh = WRITER_KEYS;
        for _key in 0..OPEN_KEYS {
            write_one(&store, &mut model, &mut random, &mut fresh);
        }
        let names: Vec<Vec<u8>> = model.keys().cloned().collect();
        for _walk in 0..RANGE_WALKS {
            let (first, second) = (&names[random.below(names.len() as u64) as usize], &names[random.below(names.len() as u64) as usize]);
            let (low, high) = if first <= second { (first.clone(), second.clone()) } else { (second.clone(), first.clone()) };
            let up: Rows = model.range(low.clone()..high.clone()).map(|(name, value)| (name.clone(), value.clone())).collect();
            assert_eq!(rows(store.iter_range(COLUMN, &low, &high, Direction::Ascending).expect("scan")), up, "up over [{low:?}, {high:?})");
            let down: Rows = up.iter().rev().cloned().collect();
            assert_eq!(rows(store.iter_range(COLUMN, &low, &high, Direction::Descending).expect("scan")), down, "down over [{low:?}, {high:?})");
            for direction in [Direction::Ascending, Direction::Descending] {
                let range = ScanRange { wanted: Some(RowCount(TOLD_ROWS)), ..reel::engine::scan_state::in_range(COLUMN, &low, &high, direction) };
                let found = rows(StoreScan::new(Arc::clone(&store.state), &range).expect("scan"));
                let expected = if direction == Direction::Ascending { &up } else { &down };
                assert!(&found == expected, "told {TOLD_ROWS} {direction:?} over [{low:?}, {high:?})");
            }
        }
        check_scans(&store, &model, &mut random);
        store.close().expect("close");
    }
}

// walk a whole column for values, and return its rows and the batches it filled
fn walk_counting_fills(store: &Store, direction: Direction) -> (Rows, u64) {
    let start = match direction {
        Direction::Ascending => Vec::new(),
        Direction::Descending => vec![0xFF; WIDTH as usize + 1],
    };
    let range = reel::engine::scan_state::from_key(COLUMN, &start, direction);
    let mut scan = store.state.take_scan(&range, None).expect("scan");
    let (mut name, mut value, mut found, mut fills) = (Vec::new(), Vec::new(), Vec::new(), 0);
    loop {
        fills += u64::from(!scan.batch.has_next());
        match store.state.step_scan(&mut scan, &mut name, reel::engine::scan_path::ScanWant::Value(&mut value)).expect("step") {
            ScanStep::Found => found.push((name.clone(), value.clone())),
            ScanStep::Corrupt => panic!("a corrupt record"),
            ScanStep::End => break,
        }
    }
    store.state.give_back_scan(scan);
    (found, fills)
}

// a batch reads its records in file order however the walk meets them: keys written in key order skip the sort going up, and
// the walk down over them or over keys written out of order takes the sort. Every walk gives the model's rows
#[test]
fn records_out_of_file_order_read_as_the_model() {
    for is_in_order in [true, false] {
        for direction in [Direction::Ascending, Direction::Descending] {
            let io = SimulatedIo::new(Seed(33));
            let store = open(&io, CAPACITY);
            let mut model = Model::new();
            // the key of a number is a hash of it, so writes by number land out of key order
            let mut names: Vec<Vec<u8>> = (0..KEYS).map(key).collect();
            if is_in_order {
                names.sort();
            }
            for (number, name) in names.iter().enumerate() {
                store.put(COLUMN, name, format!("value-{number}").as_bytes()).expect("put");
                model.insert(name.clone(), format!("value-{number}").into_bytes());
            }
            seal_all(&store, &io);
            let skipped = report(&store).scan_batches_presorted;
            let (found, fills) = walk_counting_fills(&store, direction);
            let skipped = report(&store).scan_batches_presorted - skipped;
            let mut expected: Rows = model.iter().map(|(name, value)| (name.clone(), value.clone())).collect();
            if direction == Direction::Descending {
                expected.reverse();
            }
            assert!(found == expected, "the walk's rows differ from the model, in order {is_in_order} going {direction:?}");
            println!("in order {is_in_order} going {direction:?}: {skipped} of {fills} batches skipped the sort");
            match (is_in_order, direction) {
                (true, Direction::Ascending) => assert!(skipped * 2 > fills, "{skipped} of {fills} batches skipped the sort"),
                (true, Direction::Descending) | (false, Direction::Ascending | Direction::Descending) => {
                    assert!(skipped * 10 < fills, "{skipped} of {fills} batches out of order skipped the sort");
                },
            }
            store.close().expect("close");
        }
    }
}

// a key walk beside a writer answers each key as of its batch's point, and locates its keys a batch at a time
#[test]
fn key_walks_beside_batches_match_the_model() {
    for seed in seeds(0..WRITER_SEEDS) {
        let io = SimulatedIo::new(Seed(seed));
        let store = open(&io, CAPACITY);
        let mut model = Model::new();
        let mut random = SeededRandom::new(Seed(seed));
        for number in 0..WRITER_KEYS {
            store.put(COLUMN, &key(number), format!("{number}").as_bytes()).expect("put");
            model.insert(key(number), format!("{number}").into_bytes());
        }
        seal_all(&store, &io);
        let mut fresh = WRITER_KEYS;
        for _key in 0..OPEN_KEYS {
            write_one(&store, &mut model, &mut random, &mut fresh);
        }
        for write_odds in [DENSE_WRITE_ONE_IN, STEP_WRITE_ONE_IN] {
            for direction in [Direction::Ascending, Direction::Descending] {
                let tally = walk_beside_writer(&store, &io, &mut model, &mut random, &mut fresh, (direction, Some(write_odds), true, None));
                println!("seed {seed} {direction:?} keys beside a writer one in {write_odds}: {tally:?}");
            }
        }
        let tally = walk_beside_writer(&store, &io, &mut model, &mut random, &mut fresh, (Direction::Ascending, None, true, None));
        println!("seed {seed} quiet keys: {tally:?}");
        assert!(tally.fills * 4 < tally.asked, "the key walk located no batches: {tally:?}");
        store.close().expect("close");
    }
}

// a scan keeps its count of keys whose sealed row came from the index, and the store's total takes every one once the walk
// ends, for a walk of keys and a walk of values alike
#[test]
fn sorted_scan_keys_reach_the_total() {
    let io = SimulatedIo::new(Seed(34));
    let store = open(&io, CAPACITY);
    let mut model = Model::new();
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), format!("{number}").as_bytes()).expect("put");
        model.insert(key(number), format!("{number}").into_bytes());
    }
    seal_all(&store, &io);
    let before = store.state.sorted_scan_keys.load(Ordering::Relaxed);
    let mut scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
    let (mut name, mut names) = (Vec::new(), Vec::new());
    while scan.next_key(&mut name).expect("step") == ScanStep::Found {
        names.push(name.clone());
    }
    drop(scan);
    assert_eq!(names, model.keys().cloned().collect::<Vec<_>>());
    let after_keys = store.state.sorted_scan_keys.load(Ordering::Relaxed);
    assert_eq!(after_keys - before, KEYS, "the key walk's count");
    let found = rows(store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan"));
    assert_eq!(found.len() as u64, KEYS);
    assert_eq!(store.state.sorted_scan_keys.load(Ordering::Relaxed) - after_keys, KEYS, "the value walk's count");
    store.close().expect("close");
}

// with a few open keys spread over many sealed ones, most shards show no open key near a walk and their heads stand on bounds
// with no descent; told walks up and down from every kind of start still read as the model does, and writes between walks land
#[test]
fn heads_skip_shards_with_no_open_key_near() {
    let io = SimulatedIo::new(Seed(4_300));
    let store = open(&io, CAPACITY);
    let mut model = Model::new();
    for number in 0..KEYS {
        let value = format!("sealed-{number}").into_bytes();
        store.put(COLUMN, &key(number), &value).expect("put");
        model.insert(key(number), value);
    }
    seal_all(&store, &io);
    let mut random = SeededRandom::new(Seed(4_301));
    for round in 0..20_u64 {
        for _write in 0..4 {
            let number = random.below(KEYS);
            let value = format!("open-{round}-{number}").into_bytes();
            store.put(COLUMN, &key(number), &value).expect("put");
            model.insert(key(number), value);
        }
        for _walk in 0..20 {
            let start = key(random.below(KEYS));
            for direction in [Direction::Ascending, Direction::Descending] {
                let range = from_key(COLUMN, &start, direction).wanting(RowCount(10));
                let found = first_rows(store.iter(&range).expect("scan"), 10);
                let expected: Rows = match direction {
                    Direction::Ascending => model.range(start.clone()..).take(10).map(|(name, value)| (name.clone(), value.clone())).collect(),
                    Direction::Descending => model.range(..=start.clone()).rev().take(10).map(|(name, value)| (name.clone(), value.clone())).collect(),
                };
                assert_eq!(found, expected, "round {round} from {start:?} {direction:?}");
            }
        }
    }
    let skipped = report(&store).heads_skipped;
    println!("{skipped} heads stood on bounds");
    assert!(skipped > 0, "no head stood on a bound");
    store.close().expect("close");
}

// let the jobs run through a write-out period, as the clock would, so a row job waiting on its period enters what landed
fn quiet(io: &SimulatedIo) {
    settle(io);
    io.advance_clock(reel::engine::row_job::WRITE_OUT_PERIOD);
    settle(io);
}

// every row of a walk, keys and values, from a start
fn walk_rows(scan: &mut StoreScan<SimulatedIo>) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let (mut rows, mut name, mut value) = (BTreeMap::new(), Vec::new(), Vec::new());
    while scan.next_row(&mut name, &mut value).expect("step") == ScanStep::Found {
        assert!(rows.insert(name.clone(), value.clone()).is_none(), "a key twice in one walk");
    }
    rows
}

// every key of a walk
fn walk_keys(scan: &mut StoreScan<SimulatedIo>) -> Vec<Vec<u8>> {
    let (mut keys, mut name) = (Vec::new(), Vec::new());
    while scan.next_key(&mut name).expect("step") == ScanStep::Found {
        keys.push(name.clone());
    }
    keys
}

// check a store's walks, keys, counts and gets against a model
fn check_against(store: &Store, model: &BTreeMap<Vec<u8>, Vec<u8>>, when: &str) {
    let rows = walk_rows(&mut store.iter_from(COLUMN, &[], Direction::Ascending).expect("scan"));
    assert!(rows == *model, "{when}: the value walk has {} rows, the model {}", rows.len(), model.len());
    let keys = walk_keys(&mut store.iter_from(COLUMN, &[], Direction::Ascending).expect("scan"));
    assert!(keys.iter().eq(model.keys()), "{when}: the key walk differs");
    let mut descending = walk_keys(&mut store.iter_from(COLUMN, &[0xFF; WIDTH as usize], Direction::Descending).expect("scan"));
    descending.reverse();
    assert!(descending.iter().eq(model.keys()), "{when}: the descending walk differs");
    assert_eq!(store.count_prefix(COLUMN, &[]).expect("count"), RowCount(model.len() as u64), "{when}: the count");
    for first in [0x00_u8, 0x3c, 0x9e, 0xff] {
        let wanted = model.keys().filter(|name| name[0] == first).count() as u64;
        assert_eq!(store.count_prefix(COLUMN, &[first]).expect("count"), RowCount(wanted), "{when}: the count under {first:#x}");
    }
    for number in 0..KEYS + KEYS / 4 {
        assert_eq!(read(store, &key(number)), model.get(&key(number)).cloned(), "{when}: key {number}");
    }
}

// open rows entered in the sorted index ahead of their seal answer walks, keys, counts and gets as the model holds them, with
// heads skipped while quiet; rows written after the entering, and a seal after, change nothing the model doesn't
#[test]
fn entered_rows_answer_as_the_model() {
    let io = SimulatedIo::new(Seed(5_100));
    let store = open(&io, CAPACITY);
    let mut model = BTreeMap::new();
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), b"sealed").expect("put");
        model.insert(key(number), b"sealed".to_vec());
    }
    seal_all(&store, &io);
    for number in (0..KEYS).step_by(3) {
        store.put(COLUMN, &key(number), b"open").expect("put");
        model.insert(key(number), b"open".to_vec());
    }
    for number in (0..KEYS).step_by(7) {
        store.delete(COLUMN, &key(number)).expect("delete");
        model.remove(&key(number));
    }
    for number in KEYS..KEYS + KEYS / 4 {
        store.put(COLUMN, &key(number), b"new").expect("put");
        model.insert(key(number), b"new".to_vec());
    }
    // a key put, deleted and put again in one segment goes in as its newest row
    store.put(COLUMN, &key(5), b"once").expect("put");
    store.delete(COLUMN, &key(5)).expect("delete");
    store.put(COLUMN, &key(5), b"twice").expect("put");
    model.insert(key(5), b"twice".to_vec());
    quiet(&io);
    let entered = report(&store).rows_entered;
    assert!(entered > 0, "no row was entered");
    let (skipped, index_counts) = (report(&store).heads_skipped, store.state.index_counts.load(Ordering::Relaxed));
    check_against(&store, &model, "entered");
    assert!(report(&store).heads_skipped > skipped, "no head was skipped over entered rows");
    assert!(store.state.index_counts.load(Ordering::Relaxed) > index_counts, "no count came from the index");

    // rows past the entering stay counted, so their shards place real heads
    for number in (1..KEYS).step_by(5) {
        store.put(COLUMN, &key(number), b"later").expect("put");
        model.insert(key(number), b"later".to_vec());
    }
    for number in (2..KEYS).step_by(11) {
        store.delete(COLUMN, &key(number)).expect("delete");
        model.remove(&key(number));
    }
    check_against(&store, &model, "written after");
    quiet(&io);
    check_against(&store, &model, "entered again");
    seal_all(&store, &io);
    check_against(&store, &model, "sealed");
    store.close().expect("close");
    let store = open(&io, CAPACITY);
    check_against(&store, &model, "reopened");
    store.close().expect("close");
}

// a cue taken before open rows were overwritten and entered reads the rows of its point: an entry newer than the cue stands over
// an older row still open, and the write index answers it
#[test]
fn a_cue_reads_under_newer_entered_rows() {
    let io = SimulatedIo::new(Seed(5_101));
    let store = open(&io, CAPACITY);
    let mut before = BTreeMap::new();
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), b"first").expect("put");
        before.insert(key(number), b"first".to_vec());
    }
    settle(&io);
    let cue = store.cue().expect("cue");
    for number in (0..KEYS).step_by(2) {
        store.put(COLUMN, &key(number), b"second").expect("put");
    }
    for number in (1..KEYS).step_by(9) {
        store.delete(COLUMN, &key(number)).expect("delete");
    }
    let entered = report(&store).rows_entered;
    quiet(&io);
    assert!(report(&store).rows_entered > entered, "the overwrites were not entered");
    let rows = walk_rows(&mut cue.iter_from(COLUMN, &[], Direction::Ascending).expect("scan"));
    assert!(rows == before, "the cue's walk has {} rows, its point {}", rows.len(), before.len());
    drop(cue);
    store.close().expect("close");
}

// a cue taken before a delete reads the deleted key's sealed value after the open grave is entered: the grave goes in as an
// entry newer than the cue's point, and the cue's read goes past it to the sealed row
#[test]
fn a_cue_reads_under_an_entered_grave() {
    let io = SimulatedIo::new(Seed(5_102));
    let store = open(&io, CAPACITY);
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), b"sealed").expect("put");
    }
    seal_all(&store, &io);
    let cue = store.cue().expect("cue");
    for number in (0..KEYS).step_by(3) {
        store.delete(COLUMN, &key(number)).expect("delete");
    }
    let entered = report(&store).rows_entered;
    quiet(&io);
    assert!(report(&store).rows_entered > entered, "the graves were not entered");
    let rows = walk_rows(&mut cue.iter_from(COLUMN, &[], Direction::Ascending).expect("scan"));
    assert_eq!(rows.len() as u64, KEYS, "the cue's walk lost deleted keys");
    assert_eq!(cue.count_prefix(COLUMN, &[]).expect("count"), RowCount(KEYS), "the cue's count lost deleted keys");
    let mut value = Vec::new();
    for number in 0..KEYS {
        assert!(cue.get(COLUMN, &key(number), &mut value).expect("get"), "the cue's get lost key {number}");
        assert_eq!(value, b"sealed");
    }
    for number in 0..KEYS {
        let wanted = (number % 3 != 0).then(|| b"sealed".to_vec());
        assert_eq!(read(&store, &key(number)), wanted, "key {number} now");
    }
    drop(cue);
    store.close().expect("close");
}

// open rows entered in the index hold their values in their leaves too: a second walk over rows that never sealed takes them
// from the leaves, and walks and gets give the values written, plain and compressed
#[test]
fn entered_open_rows_hold_their_values() {
    for codec in [Codec::None, Codec::Lz4] {
        SIM_BUDGET.with(|budget| budget.set(HELD_BUDGET));
        SIM_CODEC.with(|cell| cell.set(codec));
        let io = SimulatedIo::new(Seed(5_104));
        let store = open(&io, CAPACITY);
        let value = |number: u64| format!("{number:08}-").repeat(20).into_bytes();
        let mut model = BTreeMap::new();
        for number in 0..KEYS {
            store.put(COLUMN, &key(number), &value(number)).expect("put");
            model.insert(key(number), value(number));
        }
        quiet(&io);
        let open: u64 = hold(&store.state.volumes[0].claims).open.iter().map(|segment| segment.row_count.0).sum();
        let first = walk_rows(&mut store.iter_from(COLUMN, &[], Direction::Ascending).expect("scan"));
        let held = report(&store).scan_held_values;
        let second = walk_rows(&mut store.iter_from(COLUMN, &[], Direction::Ascending).expect("scan"));
        let held_after = report(&store).scan_held_values;
        SIM_CODEC.with(|cell| cell.set(Codec::None));
        SIM_BUDGET.with(|budget| budget.set(BUDGET));
        assert!(open >= KEYS, "{codec:?}: the rows sealed, {open} open");
        assert!(first == model && second == model, "{codec:?}: a walk gave other values");
        for number in 0..KEYS {
            assert_eq!(read(&store, &key(number)), Some(value(number)), "{codec:?}: key {number}");
        }
        assert!(held_after > held, "{codec:?}: the second walk took nothing from the leaves");
        // the held values stay right once their rows seal
        seal_all(&store, &io);
        let sealed = walk_rows(&mut store.iter_from(COLUMN, &[], Direction::Ascending).expect("scan"));
        assert!(sealed == model, "{codec:?}: a walk after the seal gave other values");
        store.close().expect("close");
    }
}

const LARGE_KEYS: u64 = 40;
// values a batch never reads, one record each and chunked, so a scan reads each on its own
const TRUSTED_VALUE_BYTES: [usize; 2] = [16 * 1024, 200 * 1024];
// a value a batch reads with its neighbours and checks there
const BATCHED_VALUE_BYTES: usize = 4 * 1024;
const ROTTED_KEY: u64 = 17;

use reel::io::IoFile;

// a value too long for a leaf to hold, whose first bytes say which key wrote it
fn large_value(number: u64, size: usize) -> Vec<u8> {
    let mut value = format!("large-value-{number:06}").into_bytes();
    value.resize(size, (number % 251) as u8);
    value
}

// sealed large values with one byte flipped on the disk inside one of them
fn rotted_large_values(io: &SimulatedIo, size: usize) -> Store {
    SIM_BUDGET.with(|budget| budget.set(HELD_BUDGET));
    let store = open(io, CAPACITY);
    SIM_BUDGET.with(|budget| budget.set(BUDGET));
    for number in 0..LARGE_KEYS {
        store.put(COLUMN, &key(number), &large_value(number, size)).expect("put");
    }
    seal_all(&store, io);
    let needle = format!("large-value-{ROTTED_KEY:06}").into_bytes();
    let listing = io.list_directory(Path::new(ROOT)).expect("list");
    let path = listing.into_iter().find(|path| path.extension().is_some_and(|name| name == "data")).expect("a data file");
    let file = io.open_read_only(&path).expect("open");
    let mut bytes = vec![0; file.length().expect("length").0 as usize];
    file.read_exact_at(FileOffset(0), &mut bytes).expect("read");
    io.close_file(file);
    let at = bytes.windows(needle.len()).position(|window| window == needle).expect("the value");
    io.rot(&path, FileOffset((at + size / 2) as u64), BitMask(1)).expect("rot");
    store
}

// the value a scan from the start answers for a key, or a corrupt step
fn scanned_value(store: &Store, wanted: &[u8]) -> Result<Vec<u8>, ScanStep> {
    let mut scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
    let (mut name, mut value) = (Vec::new(), Vec::new());
    loop {
        match scan.next_row(&mut name, &mut value).expect("step") {
            ScanStep::Found if name == wanted => return Ok(value),
            ScanStep::Found => {},
            other => return Err(other),
        }
    }
}

// a scan reads a sealed value its batch doesn't read under the batch's counts, as a get does, so a byte flipped on the disk under
// it reads back flipped and the trusted count rises, one record or chunked
#[test]
fn a_scanned_large_sealed_value_reads_back_unchecked() {
    for size in TRUSTED_VALUE_BYTES {
        let io = SimulatedIo::new(Seed(47));
        let store = rotted_large_values(&io, size);
        let trusted = report(&store).scan_trusted_records;
        let value = scanned_value(&store, &key(ROTTED_KEY)).expect("the row");
        assert!(report(&store).scan_trusted_records > trusted, "the scan checked every {size} B record");
        assert_eq!(value.len(), size);
        assert_ne!(value, large_value(ROTTED_KEY, size), "the flipped byte was caught at {size} B");
        store.close().expect("close");
    }
}

// a record the batch read under its counts skips its identity hashes, and its payload, no longer than a trusted read sums, is
// still summed against its head, so a byte flipped in it steps the scan corrupt
#[test]
fn a_batched_sealed_value_is_summed_under_trust() {
    let io = SimulatedIo::new(Seed(51));
    let store = rotted_large_values(&io, BATCHED_VALUE_BYTES);
    assert_eq!(scanned_value(&store, &key(ROTTED_KEY)), Err(ScanStep::Corrupt), "a rotten batched value came back");
    store.close().expect("close");
}

// a read-only store has no counts to trust a batch under, so its scan steps corrupt at a rotten batched value
#[test]
fn a_read_only_batched_rotten_value_steps_corrupt() {
    let io = SimulatedIo::new(Seed(51));
    let store = rotted_large_values(&io, BATCHED_VALUE_BYTES);
    store.close().expect("close");
    drop(store);
    let view = ReelStore::open_read_only(io.clone(), &config(CAPACITY)).expect("open read-only");
    assert_eq!(scanned_value(&view, &key(ROTTED_KEY)), Err(ScanStep::Corrupt), "a rotten batched value came back");
    view.close().expect("close");
}

// a read-only store's scan checks every record, so the flipped value never comes back
#[test]
fn a_read_only_scan_checks_a_large_sealed_value() {
    let io = SimulatedIo::new(Seed(48));
    let size = TRUSTED_VALUE_BYTES[0];
    let store = rotted_large_values(&io, size);
    store.close().expect("close");
    drop(store);
    let view = ReelStore::open_read_only(io.clone(), &config(CAPACITY)).expect("open read-only");
    if let Ok(value) = scanned_value(&view, &key(ROTTED_KEY)) {
        assert_eq!(value, large_value(ROTTED_KEY, size), "a view answered the flipped bytes");
    }
    assert_eq!(report(&view).scan_trusted_records, 0, "a view trusted a record");
}

use reel::index::entry_slab::MOST_INLINE_VALUE;
use reel_core::value_list::{ValueAnswer, ValueList};

const KEPT_KEYS: u64 = 300;

// an open row keeps a short value, so gets and batched gets answer from the row: an overwrite answers the new value, a delete
// answers nothing, a value too long to keep reads its record, and every answer matches what was written
#[test]
fn open_rows_answer_their_kept_values() {
    let io = SimulatedIo::new(Seed(49));
    let store = open(&io, CAPACITY);
    let long = vec![7_u8; MOST_INLINE_VALUE + 1];
    for number in 0..KEPT_KEYS {
        store.put(COLUMN, &key(number), format!("first-{number}").as_bytes()).expect("put");
    }
    for number in (0..KEPT_KEYS).step_by(3) {
        store.put(COLUMN, &key(number), format!("second-{number}").as_bytes()).expect("put");
    }
    for number in (1..KEPT_KEYS).step_by(5) {
        store.delete(COLUMN, &key(number)).expect("delete");
    }
    store.put(COLUMN, &key(KEPT_KEYS), &long).expect("put");
    let expected = |number: u64| -> Option<Vec<u8>> {
        if number == KEPT_KEYS {
            return Some(long.clone());
        }
        if number % 5 == 1 {
            return None;
        }
        Some(if number.is_multiple_of(3) { format!("second-{number}") } else { format!("first-{number}") }.into_bytes())
    };

    let kept = report(&store).kept_values;
    for number in 0..=KEPT_KEYS {
        assert_eq!(read(&store, &key(number)), expected(number), "key {number}");
    }
    let single = report(&store).kept_values - kept;
    let found = (0..KEPT_KEYS).filter(|number| number % 5 != 1).count() as u64;
    assert_eq!(single, found, "a short open value took a record read, or a long one was kept");

    let names: Vec<Vec<u8>> = (0..=KEPT_KEYS).map(key).collect();
    let asked: Vec<&[u8]> = names.iter().map(Vec::as_slice).collect();
    let mut values = ValueList::new();
    store.get_many(COLUMN, &asked, &mut values).expect("get many");
    for number in 0..=KEPT_KEYS {
        let answer = match values.answer(number as usize).expect("an answer") {
            ValueAnswer::Found(value) => Some(value.to_vec()),
            ValueAnswer::Missing => None,
            ValueAnswer::Corrupt => panic!("key {number} read corrupt"),
        };
        assert_eq!(answer, expected(number), "batched key {number}");
    }
    assert_eq!(report(&store).kept_values - kept - single, found, "a batched get read a kept value's record");
    store.close().expect("close");
}

const LARGE_RECORD_BYTES: usize = 200 * 1024;

// a value no codec shrinks, of a given seed
fn noise(seed: u64) -> Vec<u8> {
    let mut state = seed | 1;
    (0..LARGE_RECORD_BYTES)
        .map(|_byte| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

// a value lz4 shrinks well
fn text(seed: u64) -> Vec<u8> {
    format!("text value {seed} ").into_bytes().into_iter().cycle().take(LARGE_RECORD_BYTES).collect()
}

// an lz4 column's large records turn from noise to text and back: a run of noise records turns the guess on, one text record
// turns it off, writes and reads both count, a read the guess got wrong falls back, and every value reads back as written
#[test]
fn large_records_steer_their_reads_as_they_change() {
    SIM_CODEC.with(|codec| codec.set(Codec::Lz4));
    let io = SimulatedIo::new(Seed(50));
    let store = open(&io, CAPACITY);
    SIM_CODEC.with(|codec| codec.set(Codec::None));
    let guess = || store.state.guesses_plain(COLUMN);

    store.put(COLUMN, &key(1), &noise(1)).expect("put");
    store.put(COLUMN, &key(3), &noise(3)).expect("put");
    assert!(!guess(), "two noise records turned the guess on");
    store.put(COLUMN, &key(5), &noise(5)).expect("put");
    assert!(guess(), "a run of noise records left the guess off");
    store.put(COLUMN, &key(2), &text(2)).expect("put");
    assert!(!guess(), "a text record left the guess on");
    seal_all(&store, &io);
    for number in [1_u64, 3, 5] {
        assert_eq!(read(&store, &key(number)), Some(noise(number)), "a noise record read under a text guess");
    }
    assert!(guess(), "a run of noise reads left the guess off");
    assert_eq!(read(&store, &key(2)), Some(text(2)), "a text record read under a noise guess");
    assert!(!guess(), "a read of a text record left the guess on");
    for number in [2_u64, 1, 3, 2, 5] {
        let expected = if number == 2 { text(2) } else { noise(number) };
        assert_eq!(read(&store, &key(number)), Some(expected), "key {number}");
    }
    store.close().expect("close");
}

// a value lz4 halves, so its record stays chunked and longer than one chunk
fn half_noise(seed: u64) -> Vec<u8> {
    noise(seed).into_iter().enumerate().map(|(at, byte)| if at % 2 == 0 { byte } else { 0 }).collect()
}

const SHORT_TEXT_BYTES: usize = 2 * 1024;

// an lz4 column's records read right through every shape: a short chunked record takes two reads, not three, a record that
// shrank but stays past one chunk falls back from a plain guess, and a short compressed record whose counts move while it reads
// is read again with every check
#[test]
fn lz4_records_read_right_in_every_shape() {
    SIM_CODEC.with(|codec| codec.set(Codec::Lz4));
    let io = SimulatedIo::new(Seed(52));
    let store = open(&io, CAPACITY);
    SIM_CODEC.with(|codec| codec.set(Codec::None));
    let short_text: Vec<u8> = text(7).into_iter().take(SHORT_TEXT_BYTES).collect();
    store.put(COLUMN, &key(1), &text(1)).expect("put");
    store.put(COLUMN, &key(2), &half_noise(2)).expect("put");
    store.put(COLUMN, &key(3), &short_text).expect("put");
    store.put(COLUMN, &key(4), &noise(4)).expect("put");
    seal_all(&store, &io);

    let reads = io.read_count().0;
    assert_eq!(read(&store, &key(1)), Some(text(1)));
    assert_eq!(io.read_count().0 - reads, 1, "a short chunked record took more than one read");

    // a run of noise reads turns the guess on, so the half record is first read as if plain
    for _read in 0..3 {
        assert_eq!(read(&store, &key(4)), Some(noise(4)));
    }
    assert!(store.state.guesses_plain(COLUMN));
    assert_eq!(read(&store, &key(2)), Some(half_noise(2)), "a shrunk record past one chunk misread under a plain guess");
    assert!(!store.state.guesses_plain(COLUMN));

    let state = Arc::clone(&store.state);
    io.on_every_read(Some(Arc::new(move || state.cues.note_free())));
    assert_eq!(read(&store, &key(3)), Some(short_text.clone()), "a compressed record read under moving counts");
    io.on_every_read(None);
    store.close().expect("close");
    drop(store);

    // a read-only view checks every record, and still reads a short chunked one once
    SIM_CODEC.with(|codec| codec.set(Codec::Lz4));
    let view = ReelStore::open_read_only(io.clone(), &config(CAPACITY)).expect("open read-only");
    SIM_CODEC.with(|codec| codec.set(Codec::None));
    let reads = io.read_count().0;
    assert_eq!(read(&view, &key(1)), Some(text(1)));
    assert_eq!(io.read_count().0 - reads, 1, "a checked short chunked record took more than one read");
    for number in [2_u64, 3, 4] {
        assert!(read(&view, &key(number)).is_some(), "a view lost key {number}");
    }
}

// a kept value answers at the edges of what a row keeps, an empty value and one exactly as long as the most, and a range cover
// over a key with a kept value hides it
#[test]
fn kept_values_hold_at_their_edges_and_under_covers() {
    let io = SimulatedIo::new(Seed(53));
    let store = open(&io, CAPACITY);
    let longest = vec![9_u8; MOST_INLINE_VALUE];
    store.put(COLUMN, &key(1), b"").expect("put");
    store.put(COLUMN, &key(2), &longest).expect("put");
    let kept = report(&store).kept_values;
    assert_eq!(read(&store, &key(1)), Some(Vec::new()));
    assert_eq!(read(&store, &key(2)), Some(longest));
    assert_eq!(report(&store).kept_values - kept, 2, "an edge value took a record read");

    let names: Vec<Vec<u8>> = (10..20).map(key).collect();
    for name in &names {
        store.put(COLUMN, name, b"covered").expect("put");
    }
    let (start, end) = (names.iter().min().expect("a key").clone(), names.iter().max().expect("a key").clone());
    store.delete_range(COLUMN, &start, &end).expect("delete range");
    for name in &names {
        let expected = (*name >= end).then(|| b"covered".to_vec());
        assert_eq!(read(&store, name), expected, "a cover left a kept value showing");
    }
    store.close().expect("close");
}

const MANY_KEYS: u64 = 60;
const MANY_SEEDS: u64 = 4;

// a value of one of three widths by key: short, longer than a batch reads, and many chunks long
fn many_value(number: u64, round: u64) -> Vec<u8> {
    let mut value = format!("many-{number}-{round}").into_bytes();
    match number % 3 {
        0 => {},
        1 => value.resize(16 * 1024, (number % 251) as u8),
        _ => value.resize(200 * 1024, (round % 251) as u8),
    }
    value
}

// the answers a batched get gives for every key, as the single gets would read them
fn many_answers(store: &Store, names: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
    let asked: Vec<&[u8]> = names.iter().map(Vec::as_slice).collect();
    let mut values = ValueList::new();
    store.get_many(COLUMN, &asked, &mut values).expect("get many");
    (0..names.len())
        .map(|at| match values.answer(at).expect("an answer") {
            ValueAnswer::Found(value) => Some(value.to_vec()),
            ValueAnswer::Missing => None,
            ValueAnswer::Corrupt => panic!("key {at} read corrupt"),
        })
        .collect()
}

// batched gets of short, long and many-chunk values, sealed and open, overwritten and deleted, beside seals, answer as single gets
// and the model do
#[test]
fn batched_long_values_match_single_gets() {
    for seed in seeds(0..MANY_SEEDS) {
        SIM_BUDGET.with(|budget| budget.set(HELD_BUDGET));
        let io = SimulatedIo::new(Seed(seed));
        let store = open(&io, CAPACITY);
        SIM_BUDGET.with(|budget| budget.set(BUDGET));
        let mut random = SeededRandom::new(Seed(seed));
        let mut model = Model::new();
        let names: Vec<Vec<u8>> = (0..MANY_KEYS + 5).map(key).collect();
        for round in 0..4 {
            for number in 0..MANY_KEYS {
                match random.below(5) {
                    0 => {
                        store.delete(COLUMN, &key(number)).expect("delete");
                        model.remove(&key(number));
                    },
                    1 => {},
                    _ => {
                        store.put(COLUMN, &key(number), &many_value(number, round)).expect("put");
                        model.insert(key(number), many_value(number, round));
                    },
                }
            }
            if round % 2 == 0 {
                seal_all(&store, &io);
            }
            let batched = many_answers(&store, &names);
            for (name, answer) in names.iter().zip(&batched) {
                assert_eq!(*answer, read(&store, name), "seed {seed} round {round}: a batched answer differs from a single get");
                assert_eq!(answer.as_ref(), model.get(name), "seed {seed} round {round}: a batched answer differs from the model");
            }
        }
        store.close().expect("close");
    }
}

// a rotted long value answers in a batch as a single get does: under counts that held, both unchecked, and once a job moved them
// while it read, the batch reads it again with every check and answers corrupt
#[test]
fn a_batched_rotten_long_value_answers_as_a_get_does() {
    let io = SimulatedIo::new(Seed(54));
    let size = TRUSTED_VALUE_BYTES[0];
    let store = rotted_large_values(&io, size);
    let names: Vec<Vec<u8>> = (0..LARGE_KEYS).map(key).collect();
    let rotted = ROTTED_KEY as usize;
    let batched = many_answers(&store, &names);
    assert_eq!(batched[rotted], read(&store, &key(ROTTED_KEY)), "a batch read a rotten value other than a get does");
    assert_ne!(batched[rotted], Some(large_value(ROTTED_KEY, size)), "the flipped byte was caught under counts that held");

    let state = Arc::clone(&store.state);
    io.on_every_read(Some(Arc::new(move || state.cues.note_free())));
    let asked: Vec<&[u8]> = names.iter().map(Vec::as_slice).collect();
    let mut values = ValueList::new();
    store.get_many(COLUMN, &asked, &mut values).expect("get many");
    io.on_every_read(None);
    assert_eq!(values.answer(rotted), Some(ValueAnswer::Corrupt), "a rotten value came back when the counts moved");
    for (at, name) in names.iter().enumerate().filter(|(at, _name)| *at != rotted) {
        let number = at as u64;
        assert_eq!(values.answer(at), Some(ValueAnswer::Found(&large_value(number, size))), "key {name:?} misread under moving counts");
    }
    store.close().expect("close");
}

// with damage marked on the volume, a batch reads a rotted long value with every check and answers it corrupt, though the counts
// held, and answers every other key right
#[test]
fn a_batch_beside_marked_damage_checks_long_values() {
    let io = SimulatedIo::new(Seed(55));
    let size = TRUSTED_VALUE_BYTES[0];
    let store = rotted_large_values(&io, size);
    store.state.volumes[0].damage.add(&reel::segment::damage_list::DamagedRange {
        column: COLUMN,
        segment: SegmentNumber(u64::MAX),
        highest_place: WritePlace(0),
        start: Vec::new(),
        end: None,
    });
    let names: Vec<Vec<u8>> = (0..LARGE_KEYS).map(key).collect();
    let asked: Vec<&[u8]> = names.iter().map(Vec::as_slice).collect();
    let mut values = ValueList::new();
    store.get_many(COLUMN, &asked, &mut values).expect("get many");
    for at in 0..LARGE_KEYS as usize {
        let expected = if at == ROTTED_KEY as usize { ValueAnswer::Corrupt } else { ValueAnswer::Found(&large_value(at as u64, size)) };
        assert_eq!(values.answer(at), Some(expected), "key {at} beside marked damage");
    }
    store.close().expect("close");
}

// the node's shape: an lz4 column of noise slices long enough to chunk, so a run of them turns the one-read guess on and the
// batch reads them trusted in one read, with a text value in the middle that breaks the run, all answering as single gets do
#[test]
fn batched_noise_in_an_lz4_column_matches_single_gets() {
    SIM_CODEC.with(|codec| codec.set(Codec::Lz4));
    SIM_BUDGET.with(|budget| budget.set(HELD_BUDGET));
    let io = SimulatedIo::new(Seed(56));
    let store = open(&io, CAPACITY);
    SIM_BUDGET.with(|budget| budget.set(BUDGET));
    SIM_CODEC.with(|codec| codec.set(Codec::None));
    let value_of = |number: u64| if number == 6 { text(number) } else { noise(number) };
    let names: Vec<Vec<u8>> = (0..12).map(key).collect();
    for (number, name) in names.iter().enumerate() {
        store.put(COLUMN, name, &value_of(number as u64)).expect("put");
    }
    seal_all(&store, &io);
    // a run of noise reads turns the guess on, so the batch below reads noise in one read and falls back at the text value
    for number in [0_u64, 1, 2] {
        assert_eq!(read(&store, &names[number as usize]), Some(value_of(number)));
    }
    assert!(store.state.guesses_plain(COLUMN), "a run of noise reads left the guess off");
    for round in 0..3 {
        let batched = many_answers(&store, &names);
        for (number, answer) in batched.iter().enumerate() {
            assert_eq!(answer.as_ref(), Some(&value_of(number as u64)), "round {round}: key {number} misread in a batch");
        }
    }
    store.close().expect("close");
}

// a batch that passes a key whose put is still in flight, at a point below its row, and runs to the end, leaves the walk able to
// take the key once the put returns: the next step answers it, and the walk doesn't end past it. A walk does go back for such a key,
// and never goes back over keys it swept and found dead
#[test]
fn a_key_in_flight_past_a_batch_still_comes() {
    walk_meets_a_key_in_flight(Direction::Ascending, Seed(31));
}

// the same at the bottom of a walk downward
#[test]
fn a_key_in_flight_below_a_batch_still_comes() {
    walk_meets_a_key_in_flight(Direction::Descending, Seed(32));
}

// seal some keys, put the far end's key in the write index with its put held in flight, walk to the end, land the put, and walk on
fn walk_meets_a_key_in_flight(direction: Direction, seed: Seed) {
    const SEALED: u64 = 100;
    let io = SimulatedIo::new(seed);
    let store = open(&io, CAPACITY);
    // key 0 is all zeros, the far key of a walk downward
    for number in 1..=SEALED {
        store.put(COLUMN, &key(number), b"sealed").expect("put");
    }
    seal_all(&store, &io);
    settle(&io);

    // the row goes into the write index as a put's does, ahead of its ticket finishing
    let volume = &store.state.volumes[0];
    let request = reel::engine::write_path::ClaimRequest {
        bytes: ByteCount(0),
        rows: RowCount(1),
        is_alone: false,
        has_records: false,
        covers: 0,
        mark: None,
        has_deletes: false,
    };
    let claim = store.state.claim(volume, &request).expect("claim");
    let (start, far) = match direction {
        Direction::Ascending => (Vec::new(), vec![0xFF; WIDTH as usize]),
        Direction::Descending => (vec![0xFF; WIDTH as usize], vec![0; WIDTH as usize]),
    };
    let row = reel::index::write_row::WriteRow {
        place: claim.first_place(),
        offset: claim.offset,
        length: ByteCount(0),
        kind: reel::row_kind::RowKind::Value,
        slot: claim.slot,
        segment: claim.segment,
        has_newer_sealed: false,
    };
    assert!(store.state.index.insert(COLUMN, &far, &row, None));

    let range = from_key(COLUMN, &start, direction).wanting(RowCount(SEALED + 10));
    let mut scan = store.state.take_scan(&range, None).expect("scan");
    let mut name = Vec::new();
    let step = |scan: &mut reel::engine::scan_state::ScanState, name: &mut Vec<u8>| {
        store.state.step_scan(scan, name, reel::engine::scan_path::ScanWant::Key).expect("step")
    };
    assert_eq!(step(&mut scan, &mut name), ScanStep::Found);
    // the put returns, and every later step sees its row
    claim.land();
    let mut found = 1;
    while step(&mut scan, &mut name) == ScanStep::Found {
        found += 1;
        if name == far {
            break;
        }
    }
    assert_eq!(name, far, "the walk ended past a key that landed before its last step, after {found} keys");
    assert_eq!(found, SEALED + 1, "the walk took every sealed key and the landed one, once each");
    store.state.give_back_scan(scan);
}

// every answer of a batch of gets, the value found or none
fn batch_answers(found: &ValueList, count: usize) -> Vec<Option<Vec<u8>>> {
    (0..count)
        .map(|at| match found.answer(at).expect("an answer") {
            ValueAnswer::Found(value) => Some(value.to_vec()),
            ValueAnswer::Missing => None,
            ValueAnswer::Corrupt => panic!("key {at} read corrupt"),
        })
        .collect()
}

// a batch answers sealed keys from the values their leaves hold, as a single get does, with no record read. After an overwrite
// and a delete sealed in, every key reads its newest value and the deleted one reads absent, and a cue taken before them still
// reads the values it saw
#[test]
fn a_batch_answers_sealed_keys_from_their_leaves() {
    const KEYS: u64 = 40;
    // a full budget gives the leaves value pages
    SIM_BUDGET.with(|budget| budget.set(HELD_BUDGET));
    let io = SimulatedIo::new(Seed(91));
    let store = open(&io, RowCount(4_096));
    let first = |number: u64| Some(format!("first-{number}").into_bytes());
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), &first(number).expect("value")).expect("put");
    }
    seal_all(&store, &io);
    let names: Vec<Vec<u8>> = (0..KEYS).map(key).collect();
    let asked: Vec<&[u8]> = names.iter().map(Vec::as_slice).collect();
    let read_batch = |store: &Store| {
        let mut values = ValueList::new();
        store.get_many(COLUMN, &asked, &mut values).expect("get many");
        batch_answers(&values, asked.len())
    };
    let expected: Vec<Option<Vec<u8>>> = (0..KEYS).map(first).collect();
    assert_eq!(read_batch(&store), expected, "the first batch");
    let reads = io.read_count();
    assert_eq!(read_batch(&store), expected, "the batch from the leaves");
    assert_eq!(io.read_count(), reads, "a batch of held values read a record");

    let cue = store.cue().expect("cue");
    for number in (0..KEYS).filter(|number| number % 2 == 0) {
        store.put(COLUMN, &key(number), format!("second-{number}").as_bytes()).expect("put");
    }
    store.delete(COLUMN, &key(1)).expect("delete");
    seal_all(&store, &io);
    let newest: Vec<Option<Vec<u8>>> = (0..KEYS)
        .map(|number| match number {
            1 => None,
            even if even % 2 == 0 => Some(format!("second-{even}").into_bytes()),
            odd => first(odd),
        })
        .collect();
    assert_eq!(read_batch(&store), newest, "the batch after the overwrite");
    let reads = io.read_count();
    assert_eq!(read_batch(&store), newest, "the newest values from the leaves");
    assert_eq!(io.read_count(), reads, "a batch of held values read a record");

    let mut values = ValueList::new();
    cue.get_many(COLUMN, &asked, &mut values).expect("cue get many");
    assert_eq!(batch_answers(&values, asked.len()), expected, "a cue from before the overwrite");
    drop(cue);
    store.close().expect("close");
    SIM_BUDGET.with(|budget| budget.set(BUDGET));
}

// a reopen on a host with no room past its margin opens, leaves the parts its data asks for out and reports them short, and every
// get and a whole scan still answer the model
#[test]
fn a_reopen_on_a_host_with_no_room_runs_short() {
    let io = SimulatedIo::new(Seed(40));
    let store = open(&io, CAPACITY);
    let model = fill_for_reopen(&store, &io, SHORT_KEYS);
    store.close().expect("close");
    drop(store);
    io.set_host_memory(reel::io::host_memory::HostMemory { total: ByteCount(1 << 30), available: ByteCount(0), group_room: None, group_limit: None, group_anon_room: None });
    let store = open(&io, CAPACITY);
    assert!(store.memory_report().pools_short > 0, "a host with no room left no pool short");
    let written: Vec<Vec<u8>> = (0..SHORT_KEYS).map(key).collect();
    check(&store, &model, &written);
    let whole: Rows = model.iter().map(|(name, value)| (name.clone(), value.clone())).collect();
    assert_eq!(rows(store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan")), whole);
    store.close().expect("close");
}

// a target under what the store holds walks the ladder down, every key still reads, and the caps lift once the target is back
// and the store has held well under it for the decay
#[test]
fn a_target_down_and_back_up() {
    let io = SimulatedIo::new(Seed(41));
    let store = open(&io, CAPACITY);
    let model = fill_for_reopen(&store, &io, SHORT_KEYS);
    let written: Vec<Vec<u8>> = (0..SHORT_KEYS).map(key).collect();
    check(&store, &model, &written);
    check(&store, &model, &written);
    let job = reel::engine::reserve_job::ReserveJob::new(Arc::clone(&store.state));
    let before = job.held_bytes();
    // the target goes under what the least budget allows, so the ladder walks every step it has
    store.state.memory_target.store(before / 2, Ordering::Release);
    job.hold_to_target(io.now().0);
    let after = job.held_bytes();
    assert!(after < before, "the ladder gave nothing back: {before} held before, {after} after");
    check(&store, &model, &written);
    store.set_memory_target(BUDGET);
    let now = io.now().0 + 1;
    job.hold_to_target(now);
    job.hold_to_target(now + reel::pool_room::DECAY_NANOS);
    assert!(store.state.pool_rooms.iter().all(|room| room.cap() == room.reserved()), "a cap stayed down under a raised target");
    check(&store, &model, &written);
    store.close().expect("close");
}

// a small cut comes out of the record cache, and no index pool is held under its share for it
#[test]
fn a_small_cut_leaves_the_index_pools_alone() {
    let io = SimulatedIo::new(Seed(42));
    let store = open(&io, CAPACITY);
    let model = fill_for_reopen(&store, &io, SHORT_KEYS);
    let written: Vec<Vec<u8>> = (0..SHORT_KEYS).map(key).collect();
    check(&store, &model, &written);
    check(&store, &model, &written);
    let job = reel::engine::reserve_job::ReserveJob::new(Arc::clone(&store.state));
    let before = job.held_bytes();
    store.state.memory_target.store(before - 1, Ordering::Release);
    job.hold_to_target(io.now().0);
    assert!(job.held_bytes() < before, "the cut gave nothing back");
    assert!(store.state.pool_rooms.iter().filter(|room| room.is_paged()).all(|room| room.cap() == room.reserved()), "an index pool was held for a small cut");
    assert!(!store.memory_report().is_target_short);
    check(&store, &model, &written);
    store.close().expect("close");
}

// a store opened with max_memory past its budget reserves its growing pools for it, starts at the budget, and takes a target up to
// it with no restart, while a target past it comes back clamped
#[test]
fn max_memory_lets_the_target_rise_past_the_budget() {
    let io = SimulatedIo::new(Seed(43));
    let budget = SIM_BUDGET.with(std::cell::Cell::get);
    let most = ByteCount(budget.0 * 2);
    let store = ReelStore::open(io.clone(), &StoreConfig { max_memory: Some(most), ..config(CAPACITY) }).expect("open");
    let model = fill_for_reopen(&store, &io, KEYS);
    assert_eq!(store.memory_report().memory_target, budget);
    assert_eq!(store.set_memory_target(most), most);
    assert_eq!(store.set_memory_target(ByteCount(most.0 * 2)), most, "a target past max_memory went through");
    let shares = store.memory_report().pool_shares;
    let plain = ReelStore::open(SimulatedIo::new(Seed(44)), &config(CAPACITY)).expect("open");
    assert!(shares > plain.memory_report().pool_shares, "the pools reserved no more for max_memory");
    let written: Vec<Vec<u8>> = (0..KEYS).map(key).collect();
    check(&store, &model, &written);
    plain.close().expect("close");
    store.close().expect("close");
}

// a cut the caches can't meet reaches the values the leaves hold: they go, their pages go back, and every key still reads
#[test]
fn a_deep_cut_drops_held_values() {
    SIM_BUDGET.with(|budget| budget.set(HELD_BUDGET));
    let io = SimulatedIo::new(Seed(45));
    let store = open(&io, CAPACITY);
    let model = fill_for_reopen(&store, &io, KEYS);
    let written: Vec<Vec<u8>> = (0..KEYS).map(key).collect();
    check(&store, &model, &written);
    check(&store, &model, &written);
    let values = &store.state.sorted_index(COLUMN).expect("an index").values;
    let held = values.held.load(Ordering::Relaxed);
    assert!(held > 0, "no value was held to drop");
    let job = reel::engine::reserve_job::ReserveJob::new(Arc::clone(&store.state));
    store.state.memory_target.store(0, Ordering::Release);
    job.hold_to_target(io.now().0);
    assert!(values.held.load(Ordering::Relaxed) < held, "a deep cut left every held value");
    check(&store, &model, &written);
    SIM_BUDGET.with(|budget| budget.set(BUDGET));
    store.close().expect("close");
}

// a store the host presses past what its caches and held values give drops its sorted index whole: every get, scan and count
// answers the model through the footers while writes, deletes and a range delete go on, and once the host has room past the decay a
// refill brings the index back with its mark cleared and every entry right
#[test]
fn a_pressed_store_drops_its_index_and_refills_it() {
    let io = SimulatedIo::new(Seed(46));
    let store = open(&io, CAPACITY);
    // the test runs its own merges, at the moments it checks
    store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
    store.state.volumes[0].holds.reclaim.store(true, Ordering::Release);
    let mut model = fill_for_reopen(&store, &io, KEYS);
    check_all(&store, &model, KEYS, 46);
    let job = reel::engine::reserve_job::ReserveJob::new(Arc::clone(&store.state));
    let tight = reel::io::host_memory::HostMemory {
        total: ByteCount(1 << 30),
        available: ByteCount(0),
        group_room: Some(ByteCount(0)),
        group_limit: Some(ByteCount(1 << 30)),
        group_anon_room: Some(ByteCount(0)),
    };
    io.set_host_memory(tight);
    let mut now = io.now().0;
    for _step in 0..256 {
        if store.memory_report().sorted_drops > 0 {
            break;
        }
        now += 100_000_000;
        job.hold_to_target(now);
    }
    let index = store.state.sorted_index(COLUMN).expect("an index");
    assert_eq!(store.memory_report().sorted_drops, 1, "the pressed store kept its index");
    assert_eq!(index.mark(), Some(SegmentNumber(0)));
    assert_eq!(index.entry_count(), RowCount(0));
    let written: Vec<Vec<u8>> = (0..KEYS + KEYS / 2).map(key).collect();
    check(&store, &model, &written);
    check_scans(&store, &model, &mut SeededRandom::new(Seed(146)));

    // writes go on while the index is dropped, and some of them seal and merge
    for number in (KEYS..KEYS + KEYS / 2).chain((0..KEYS).step_by(5)) {
        store.put(COLUMN, &key(number), format!("dropped-{number}").as_bytes()).expect("put");
        model.insert(key(number), format!("dropped-{number}").into_bytes());
    }
    for number in (2..KEYS).step_by(11) {
        store.delete(COLUMN, &key(number)).expect("delete");
        model.remove(&key(number));
    }
    let names: Vec<Vec<u8>> = model.keys().cloned().collect();
    let (start, end) = (names[names.len() / 2].clone(), names[names.len() / 2 + 40].clone());
    store.delete_range(COLUMN, &start, &end).expect("delete range");
    model.retain(|name, _value| *name < start || *name >= end);
    seal_all(&store, &io);
    check(&store, &model, &written);
    check_scans(&store, &model, &mut SeededRandom::new(Seed(147)));
    assert!(merge_once(&store), "no merge ran while the index was dropped");
    check(&store, &model, &written);

    // the host has room again, and past the decay the job lets a refill run
    io.set_host_memory(reel_sim::simulated_disk::SIMULATED_HOST);
    now += 1;
    job.hold_to_target(now);
    job.hold_to_target(now + reel::pool_room::DECAY_NANOS);
    assert!(index.may_refill(), "the store had room and no refill may run");
    let volume = store.state.columns[COLUMN.0 as usize].expect("a column").volume.0 as usize;
    let mut refill = store.state.start_sorted_refill(volume).expect("a refill");
    // the room grows to a quarter past the plan at most, however many rows the refill has put in. It grows in whole extents,
    // so the last one may end past that
    let mut ceiling = index.room().committed().max(reel::engine::complete_path::refill_ceiling(refill.plan) + index.room().extent());
    // a write lands in the middle of the refill
    let mut steps = 0;
    while store.state.sorted_refill_step(volume, &mut refill).expect("refill step") {
        assert!(index.room().committed() <= ceiling, "the refill grew its room to {} past {ceiling}", index.room().committed());
        steps += 1;
        if steps == 2 {
            store.put(COLUMN, &key(1), b"mid-refill").expect("put");
            model.insert(key(1), b"mid-refill".to_vec());
            seal_all(&store, &io);
            let _merged = merge_once(&store);
            // the pool job grows the room for the write, apart from the refill
            ceiling = ceiling.max(index.room().committed());
        }
    }
    assert_eq!(index.mark(), None, "the refill left the mark: {} past capacity, {} missed at start, {} free pages", index.past_capacity().0, refill.missed.0, index.free_pages());
    assert!(!index.is_dropped());
    assert_eq!(store.memory_report().sorted_refills, 1);
    assert!(sorted_reading(&store).is_counted, "the refilled column stayed out of the counts");
    assert!(!sorted_reading(&store).is_dropped);
    check_all(&store, &model, KEYS + KEYS / 2, 48);
    store.close().expect("close");
}

/// Run one merge on volume 0 to its end, and say whether one ran
fn merge_once(store: &Store) -> bool {
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    // a pick goes on only while its look moves, as the compact job's does: a pick refused for room keeps looks left and moves
    // nothing, as with reclaim held and the park full. The tries are bounded as well
    let mut tries = 0;
    loop {
        let looked = job.rewrite.looked_up_to();
        if job.rewrite.start() {
            break;
        }
        tries += 1;
        if !job.rewrite.has_looks_left() || job.rewrite.looked_up_to() == looked || tries == 1_000 {
            return false;
        }
    }
    while job.rewrite.is_running() {
        job.rewrite.step();
    }
    true
}

// a refill that meets a block it can't read keeps the index dropped and its mark standing, and once the back-off passes a refill
// starts again on its own and brings the index back whole, with no restart
#[test]
fn a_refill_past_an_unreadable_block_tries_again() {
    let io = SimulatedIo::new(Seed(47));
    let store = open(&io, CAPACITY);
    store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
    store.state.volumes[0].holds.reclaim.store(true, Ordering::Release);
    let model = fill_for_reopen(&store, &io, KEYS);
    let index = store.state.sorted_index(COLUMN).expect("an index");
    let _freed = store.state.drop_sorted(index);
    index.set_may_refill(true);
    let mut refill = store.state.start_sorted_refill(0).expect("a refill");
    // the next read fails, so the refill's first block can't be read
    io.fail_reads_after(reel_sim::simulated_disk::OperationCount(0));
    while store.state.sorted_refill_step(0, &mut refill).unwrap_or(false) {}
    assert_eq!(index.mark(), Some(SegmentNumber(0)), "a refill past an unreadable block cleared the mark");
    assert!(index.is_dropped(), "a failed refill ended the drop");
    assert!(store.state.start_sorted_refill(0).is_none(), "a refill started inside the back-off");
    let written: Vec<Vec<u8>> = (0..KEYS).map(key).collect();
    check(&store, &model, &written);
    io.advance_clock(Nanoseconds(reel::index::sorted_keys::REFILL_RETRY_NANOS));
    let mut refill = store.state.start_sorted_refill(0).expect("a refill after the back-off");
    while store.state.sorted_refill_step(0, &mut refill).expect("refill step") {}
    assert_eq!(index.mark(), None);
    assert!(!index.is_dropped());
    assert!(sorted_reading(&store).is_counted);
    check_all(&store, &model, KEYS, 47);
    store.close().expect("close");
}

/// Steps a drop seed runs
const DROP_STEPS: u64 = 700;
/// Keys a drop seed writes among
const DROP_KEYS: u64 = 600;

// random puts, deletes, range deletes, seals and merges, with a drop, refill steps and a refill pressure ends at steps the seed
// picks: every get, count and scan answers the model at the step it runs, and a last refill brings the index back whole
#[test]
fn drops_and_refills_match_the_model() {
    for seed in seeds([1_125_500, 1_125_501, 1_125_502, 1_125_503]) {
        run_drop_seed(seed);
    }
}

fn run_drop_seed(seed: u64) {
    let io = SimulatedIo::new(Seed(seed));
    let store = open(&io, CAPACITY);
    store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
    store.state.volumes[0].holds.reclaim.store(true, Ordering::Release);
    let mut random = SeededRandom::new(Seed(seed));
    let mut model = Model::new();
    let index = store.state.sorted_index(COLUMN).expect("an index");
    let mut refill = None;
    let mut drops = 0;
    for step in 0..DROP_STEPS {
        let pick = random.below(100);
        let name = key(random.below(DROP_KEYS));
        match pick {
            0..40 => {
                let value = format!("{seed}-{step}").into_bytes();
                store.put(COLUMN, &name, &value).expect("put");
                model.insert(name, value);
            },
            40..50 => {
                store.delete(COLUMN, &name).expect("delete");
                model.remove(&name);
            },
            50..52 => {
                let other = key(random.below(DROP_KEYS));
                let (start, end) = if name < other { (name, other) } else { (other, name) };
                store.delete_range(COLUMN, &start, &end).expect("delete range");
                model.retain(|kept, _value| *kept < start || *kept >= end);
            },
            52..64 => assert_eq!(read(&store, &name), model.get(&name).cloned(), "seed {seed} step {step}: a get"),
            64..67 => {
                let whole: Rows = model.iter().map(|(name, value)| (name.clone(), value.clone())).collect();
                assert_eq!(rows(store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan")), whole, "seed {seed} step {step}: a scan");
            },
            67..70 => assert_eq!(store.count_prefix(COLUMN, &[]).expect("count").0, model.len() as u64, "seed {seed} step {step}: a count"),
            70..76 => seal_all(&store, &io),
            76..79 => {
                let _merged = merge_once(&store);
            },
            79..81 if !index.is_dropped() => {
                let _freed = store.state.drop_sorted(index);
                refill = None;
                drops += 1;
            },
            81..90 if index.is_dropped() => {
                index.set_may_refill(true);
                if refill.is_none() {
                    refill = store.state.start_sorted_refill(0);
                }
                // pressure ends one refill in eight partway
                if random.one_in(8) {
                    index.set_may_refill(false);
                }
                if let Some(mut going) = refill {
                    refill = store.state.sorted_refill_step(0, &mut going).expect("refill step").then_some(going);
                }
            },
            _ => {},
        }
    }
    seal_all(&store, &io);
    for _try in 0..10_000 {
        if !index.is_dropped() {
            break;
        }
        index.set_may_refill(true);
        if refill.is_none() {
            refill = store.state.start_sorted_refill(0);
        }
        let Some(mut going) = refill else {
            break;
        };
        refill = store.state.sorted_refill_step(0, &mut going).expect("refill step").then_some(going);
    }
    assert_eq!(index.mark(), None, "seed {seed}: the last refill left the mark");
    println!("seed {seed}: {drops} drops, {} refills", store.state.sorted_refills.load(Ordering::Relaxed));
    let written: Vec<Vec<u8>> = (0..DROP_KEYS).map(key).collect();
    check(&store, &model, &written);
    check_entries(&store, &model);
    check_scans(&store, &model, &mut SeededRandom::new(Seed(seed + 1)));
    store.close().expect("close");
}

// a whole drop that lands between a walk step's read of the drop count and its read of the mark sends the walk off the index, and
// the walk still returns every key
#[test]
fn a_drop_between_a_steps_reads_loses_no_key() {
    let io = SimulatedIo::new(Seed(48));
    let store = open(&io, CAPACITY);
    store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
    store.state.volumes[0].holds.reclaim.store(true, Ordering::Release);
    let model = fill_for_reopen(&store, &io, KEYS);
    let whole: Rows = model.iter().map(|(name, value)| (name.clone(), value.clone())).collect();
    let mut scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
    let (mut name, mut value) = (Vec::new(), Vec::new());
    let mut walked = Vec::new();
    for _first in 0..10 {
        assert_eq!(scan.next_row(&mut name, &mut value).expect("step"), ScanStep::Found);
        walked.push((name.clone(), value.clone()));
    }
    let state = Arc::clone(&store.state);
    store.state.volumes[0].holds.set_between_path_reads(Some(reel::engine::volume::RunHook(Arc::new(move || {
        let index = state.sorted_index(COLUMN).expect("an index");
        let _freed = state.drop_sorted(index);
    }))));
    while scan.next_row(&mut name, &mut value).expect("step") == ScanStep::Found {
        walked.push((name.clone(), value.clone()));
    }
    drop(scan);
    assert_eq!(store.memory_report().sorted_drops + u64::from(store.state.sorted_index(COLUMN).expect("an index").is_dropped()), 1, "the hook never ran");
    assert_eq!(walked.len(), whole.len(), "the walk lost keys to a drop between its reads");
    assert_eq!(walked, whole);
    store.close().expect("close");
}

// a walk that runs while its column's counting stops and starts again returns every key
#[test]
fn a_walk_across_a_counting_switch_loses_no_key() {
    for seed in seeds([49, 50, 51, 52]) {
        let io = SimulatedIo::new(Seed(seed));
        let store = open(&io, CAPACITY);
        store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
        store.state.volumes[0].holds.reclaim.store(true, Ordering::Release);
        let mut model = fill_for_reopen(&store, &io, KEYS);
        // open rows, half of them entered in the index
        for number in KEYS..KEYS + 300 {
            store.put(COLUMN, &key(number), b"open").expect("put");
            model.insert(key(number), b"open".to_vec());
            if number == KEYS + 150 {
                let _entered = store.state.enter_open_rows(&mut reel::index::write_index::EnterRun::new());
            }
        }
        let whole: Rows = model.iter().map(|(name, value)| (name.clone(), value.clone())).collect();
        let mut random = SeededRandom::new(Seed(seed));
        let mut scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
        let (mut name, mut value) = (Vec::new(), Vec::new());
        let mut walked = Vec::new();
        let (stop_at, start_at) = (random.below(whole.len() as u64 / 2), whole.len() as u64 / 2 + random.below(whole.len() as u64 / 2));
        let mut step = 0;
        while scan.next_row(&mut name, &mut value).expect("step") == ScanStep::Found {
            walked.push((name.clone(), value.clone()));
            step += 1;
            // a writer puts keys past every landed one, seals now and then, and the entering runs, beside switches of the counting
            let late = key(KEYS * 10 + step);
            store.put(COLUMN, &late, b"late").expect("put");
            match random.below(12) {
                0 => store.state.index.stop_counting(COLUMN),
                1 => store.state.index.start_counting(COLUMN),
                2 => store.seal_now(),
                3 | 4 => {
                    let _entered = store.state.enter_open_rows(&mut reel::index::write_index::EnterRun::new());
                },
                _ => {},
            }
            let _ = (stop_at, start_at);
        }
        drop(scan);
        let lost: Vec<&Vec<u8>> = whole.iter().map(|(name, _value)| name).filter(|name| !walked.iter().any(|(seen, _value)| seen == *name)).collect();
        assert!(lost.is_empty(), "seed {seed}: {} of {} keys lost", lost.len(), whole.len());
        store.close().expect("close");
    }
}

/// A plain column beside the sorted one, whose gets ask the spot index
const PLAIN: ColumnId = ColumnId(2);

/// Steps a cache seed runs
const CACHE_STEPS: u64 = 700;

// random puts, deletes, gets, scans, seals and merges on a sorted and a plain column, with the spot index and the block caches
// given back and brought back in part or whole at steps the seed picks: every get and scan answers the model at the step it runs
#[test]
fn caches_given_back_match_the_model() {
    for seed in seeds([1_125_700, 1_125_701, 1_125_702, 1_125_703]) {
        run_cache_seed(seed);
    }
}

fn run_cache_seed(seed: u64) {
    let io = SimulatedIo::new(Seed(seed));
    let mut both = config(CAPACITY);
    both.columns.push(ColumnConfig { id: PLAIN, name: "plain".to_string(), index: ColumnIndex::Plain, least_rows: None, ..both.columns[0].clone() });
    let store = ReelStore::open(io.clone(), &both).expect("open");
    assert!(store.state.spot.shard_bytes() >= 4_096, "the store has no spot index to give back");
    store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
    store.state.volumes[0].holds.reclaim.store(true, Ordering::Release);
    let mut random = SeededRandom::new(Seed(seed));
    let mut models = [Model::new(), Model::new()];
    let mut value = Vec::new();
    let (mut given_steps, mut brought_steps) = (0, 0);
    for step in 0..CACHE_STEPS {
        let side = random.below(2) as usize;
        let column = [COLUMN, PLAIN][side];
        let name = key(random.below(DROP_KEYS));
        match random.below(100) {
            0..35 => {
                let put = format!("{seed}-{step}").into_bytes();
                store.put(column, &name, &put).expect("put");
                models[side].insert(name, put);
            },
            35..42 => {
                store.delete(column, &name).expect("delete");
                models[side].remove(&name);
            },
            42..65 => {
                let found = store.get(column, &name, &mut value).expect("get").then(|| value.clone());
                assert_eq!(found, models[side].get(&name).cloned(), "seed {seed} step {step}: a get of column {}", column.0);
            },
            65..69 => {
                let whole: Rows = models[side].iter().map(|(name, value)| (name.clone(), value.clone())).collect();
                assert_eq!(rows(store.iter_from(column, b"", Direction::Ascending).expect("scan")), whole, "seed {seed} step {step}: a scan");
            },
            69..76 => seal_all(&store, &io),
            76..79 => {
                let _merged = merge_once(&store);
            },
            79..85 => {
                // a part of the caches or all of them
                let wanted = if random.one_in(2) { usize::MAX } else { 1 + random.below(1 << 20) as usize };
                if reel::engine::reserve_job::give_back_caches(&store.state, wanted) > 0 {
                    given_steps += 1;
                }
            },
            85..90 => {
                let room = if random.one_in(2) { u64::MAX } else { random.below(1 << 22) };
                if reel::engine::reserve_job::bring_back_caches(&store.state, room) > 0 {
                    brought_steps += 1;
                }
            },
            _ => {},
        }
    }
    assert!(given_steps > 0 && brought_steps > 0, "seed {seed}: {given_steps} give-backs and {brought_steps} bring-backs ran");
    while reel::engine::reserve_job::bring_back_caches(&store.state, u64::MAX) > 0 {}
    let report = store.memory_report();
    assert_eq!((report.spot_absent_shards, report.block_absent_shards), (0, 0));
    for (side, column) in [COLUMN, PLAIN].into_iter().enumerate() {
        for number in 0..DROP_KEYS {
            let found = store.get(column, &key(number), &mut value).expect("get").then(|| value.clone());
            assert_eq!(found, models[side].get(&key(number)).cloned(), "seed {seed}: a last get");
        }
        let whole: Rows = models[side].iter().map(|(name, value)| (name.clone(), value.clone())).collect();
        assert_eq!(rows(store.iter_from(column, b"", Direction::Ascending).expect("scan")), whole, "seed {seed}: a last scan");
    }
    store.close().expect("close");
}

// a pressed store gives the spot index and the block caches back before it drops a sorted index, every get still answers, and once
// the host has room past the decay they come back whole
#[test]
fn the_ladder_gives_caches_back_before_an_index() {
    let io = SimulatedIo::new(Seed(53));
    let mut both = config(CAPACITY);
    both.columns.push(ColumnConfig { id: PLAIN, name: "plain".to_string(), index: ColumnIndex::Plain, least_rows: None, ..both.columns[0].clone() });
    let store = ReelStore::open(io.clone(), &both).expect("open");
    let model = fill_for_reopen(&store, &io, KEYS);
    for number in 0..KEYS {
        store.put(PLAIN, &key(number), b"plain").expect("put");
    }
    seal_all(&store, &io);
    let job = reel::engine::reserve_job::ReserveJob::new(Arc::clone(&store.state));
    io.set_host_memory(reel::io::host_memory::HostMemory {
        total: ByteCount(1 << 30),
        available: ByteCount(0),
        group_room: Some(ByteCount(0)),
        group_limit: Some(ByteCount(1 << 30)),
        group_anon_room: Some(ByteCount(0)),
    });
    let mut now = io.now().0;
    let mut gave_before_drop = false;
    for _step in 0..256 {
        now += 100_000_000;
        job.hold_to_target(now);
        let report = store.memory_report();
        if report.sorted_drops > 0 {
            break;
        }
        gave_before_drop |= report.cache_shards_given > 0;
    }
    assert!(gave_before_drop, "the ladder dropped an index before it gave a cache back");
    let report = store.memory_report();
    assert!(report.spot_absent_shards > 0 && report.block_absent_shards > 0, "a cache stayed whole under pressure: {report:?}");
    let written: Vec<Vec<u8>> = (0..KEYS).map(key).collect();
    check(&store, &model, &written);
    let mut value = Vec::new();
    for number in (0..KEYS).step_by(7) {
        assert!(store.get(PLAIN, &key(number), &mut value).expect("get") && value == b"plain", "a plain get missed with the caches given back");
    }
    io.set_host_memory(reel_sim::simulated_disk::SIMULATED_HOST);
    now += 1;
    job.hold_to_target(now);
    // one shard comes back a step, and the steps come soon while one is away
    let mut steps = 0;
    let mut at = now + reel::pool_room::DECAY_NANOS;
    while store.memory_report().spot_absent_shards + store.memory_report().block_absent_shards > 0 && steps < 10_000 {
        job.hold_to_target(at);
        at += 100_000_000;
        steps += 1;
    }
    let report = store.memory_report();
    assert_eq!((report.spot_absent_shards, report.block_absent_shards), (0, 0), "a cache stayed given back with room");
    assert!(steps > 1, "every shard came back in one step");
    for number in (0..KEYS).step_by(7) {
        assert!(store.get(PLAIN, &key(number), &mut value).expect("get") && value == b"plain");
    }
    store.close().expect("close");
}

// with room for every shard, a bring-back still brings one shard a call
#[test]
fn caches_come_back_one_shard_a_call() {
    let io = SimulatedIo::new(Seed(54));
    let mut both = config(CAPACITY);
    both.columns.push(ColumnConfig { id: PLAIN, name: "plain".to_string(), index: ColumnIndex::Plain, least_rows: None, ..both.columns[0].clone() });
    let store = ReelStore::open(io.clone(), &both).expect("open");
    assert!(reel::engine::reserve_job::give_back_caches(&store.state, usize::MAX) > 0);
    let absent = |report: reel_core::memory_report::MemoryReport| report.spot_absent_shards + report.block_absent_shards;
    let mut left = absent(store.memory_report());
    assert!(left > 1);
    while left > 0 {
        assert!(reel::engine::reserve_job::bring_back_caches(&store.state, u64::MAX) > 0);
        let now = absent(store.memory_report());
        assert_eq!(now, left - 1, "a call brought back more than one shard");
        left = now;
    }
    store.close().expect("close");
}

// a merge that retires a segment after a refill entered part of its blocks ends nothing: the refill passes the segment with no
// back-off, clears its mark in the same pass, and every key answers by value, a key that moved to the merge's output and a key a
// grave deleted among them
#[test]
fn a_refill_passes_a_segment_a_merge_retires_under_it() {
    let io = SimulatedIo::new(Seed(55));
    // room for the whole index past the rest, as the reserve job asks of a refill before it lets one run
    SIM_BUDGET.with(|budget| budget.set(ByteCount(BUDGET.0 * 4)));
    let store = open(&io, CAPACITY);
    store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
    // the merges retire their inputs as they go, so reclaim runs
    let mut model = fill_for_reopen(&store, &io, SHORT_KEYS);
    // every live key takes a newer row, so the oldest segment holds only dead rows and a merge picks it
    for name in model.keys().cloned().collect::<Vec<_>>() {
        store.put(COLUMN, &name, b"third").expect("put");
        model.insert(name, b"third".to_vec());
    }
    seal_all(&store, &io);
    let oldest = hold(&store.state.volumes[0].summary).orders().present().first().copied().expect("a segment");
    let index = store.state.sorted_index(COLUMN).expect("an index");
    let _freed = store.state.drop_sorted(index);
    index.set_may_refill(true);
    let mut refill = store.state.start_sorted_refill(0).expect("a refill");
    let mut retired = None;
    let mut steps = 0;
    loop {
        // partway through a segment, a merge takes it
        if retired.is_none() && refill.block > 0 && refill.segment == Some(oldest) && let Some(segment) = refill.segment {
            // the store's own jobs run with compaction let go and no refill of theirs, until the dead segment leaves the table
            index.set_may_refill(false);
            store.state.volumes[0].holds.compaction.store(false, Ordering::Release);
            store.state.ask_compaction();
            for _round in 0..200 {
                if !store.state.volumes[0].table.is_readable(segment) {
                    break;
                }
                io.advance_clock(TICK);
                settle(&io);
            }
            store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
            index.set_may_refill(true);
            if !store.state.volumes[0].table.is_readable(segment) {
                retired = Some(segment);
            }
        }
        steps += 1;
        if !store.state.sorted_refill_step(0, &mut refill).expect("refill step") {
            break;
        }
    }
    let segment = retired.expect("no merge retired a segment the refill was partway through");
    assert!(steps > 1);
    assert!(index.is_refill_due(io.now().0), "a retired segment set the back-off");
    // a segment that leaves between steps is passed before any fill, and only one that leaves during a fill counts as a skip
    assert!(index.refill_skips() <= 1, "segment {segment:?} was passed more than once");
    // rows that found no page partway keep the mark, and the next refill starts from the top as the compact job's does, with no
    // back-off
    for _again in 0..8 {
        if index.mark().is_none() {
            break;
        }
        let mut again = store.state.start_sorted_refill(0).expect("a refill starts again at once");
        while store.state.sorted_refill_step(0, &mut again).expect("refill step") {}
    }
    assert_eq!(index.mark(), None, "no refill cleared the mark: past capacity {} drops {:?} dropped {}", index.past_capacity().0, index.drop_count(), index.is_dropped());
    assert!(sorted_reading(&store).is_counted);
    let written: Vec<Vec<u8>> = (0..SHORT_KEYS).map(key).collect();
    check(&store, &model, &written);
    check_entries(&store, &model);
    store.close().expect("close");
    SIM_BUDGET.with(|budget| budget.set(BUDGET));
}

// a sorted index refuses a fixed key past 64 bytes with that reason, and the same directory then opens with the default index,
// which keeps no sorted index on such keys and answers from the footers
#[test]
fn a_fixed_key_past_the_index_width_opens_by_keys() {
    let io = SimulatedIo::new(Seed(48));
    let wide = |width: u16, index: ColumnIndex| {
        let mut config = config(CAPACITY);
        config.columns[0].key_shape = KeyShape::Fixed { width };
        config.columns[0].index = index;
        config.columns[0].least_rows = None;
        config
    };
    for width in [65, 96] {
        match ReelStore::open(io.clone(), &wide(width, ColumnIndex::Sorted)) {
            Err(reel_core::store_error::StoreError::BadIndex { reason, .. }) => assert_eq!(reason, "a sorted index takes keys of 1 to 64 bytes"),
            other => panic!("width {width} opened as {:?}", other.map(|_store| ())),
        }
    }
    let store = ReelStore::open(io.clone(), &wide(96, ColumnIndex::ByKeys)).expect("open by keys");
    assert!(store.state.sorted_index(COLUMN).is_none(), "a 96 byte key took a sorted index");
    let name = |number: u64| [key(number), vec![7; 96 - WIDTH as usize]].concat();
    for number in 0..64 {
        store.put(COLUMN, &name(number), format!("wide-{number}").as_bytes()).expect("put");
    }
    seal_all(&store, &io);
    let mut value = Vec::new();
    for number in 0..64 {
        assert!(store.get(COLUMN, &name(number), &mut value).expect("get"), "key {number} is missing");
        assert_eq!(value, format!("wide-{number}").into_bytes());
    }
    store.close().expect("close");
}

// fill a sized store's index past the rows it planned at open, to this many halves of them, faster than its pool grows, so it
// marks, and return the keys
fn outgrow_the_plan(store: &Store, io: &SimulatedIo, model: &mut Model, halves: u64) -> u64 {
    let capacity = sorted_reading(store).capacity.0;
    let keys = (capacity * halves / 2).max(KEYS * 4);
    // no seal between the puts, so no job step grows the pool under them
    for number in 0..keys {
        let value = format!("past-{number}").into_bytes();
        store.put(COLUMN, &key(number), &value).expect("put");
        model.insert(key(number), value);
    }
    seal_all(store, io);
    keys
}

// an index that outgrows the plan it sized at open, with max_memory left out, gets its room back once the store has it: past the
// decay a refill grows its pool past the plan and clears the mark with no reopen, the column counts again, and every key answers
// from the index
#[test]
fn an_index_past_its_plan_comes_back_with_no_reopen() {
    // the reopen plans the index from the few rows on disk
    let (io, store, mut model) = small_plan_store(49, BUDGET);
    let index = store.state.sorted_index(COLUMN).expect("an index");
    assert!(index.room().reserved() > index.room().share(), "the index reserved no room past its plan");
    assert_eq!(store.memory_report().pools_held_to_share, 0);
    // half again past the plan, which the open-time rule's cap still covers
    let keys = outgrow_the_plan(&store, &io, &mut model, 3);
    let reading = sorted_reading(&store);
    assert!(reading.is_marked && reading.rows_past_capacity.0 > 0, "the fill never outran the pool: {reading:?}");
    let written: Vec<Vec<u8>> = (0..keys).map(key).collect();
    check(&store, &model, &written);

    // the target has room, and past the decay the job lets a refill run on the pool past the plan
    let job = reel::engine::reserve_job::ReserveJob::new(Arc::clone(&store.state));
    let mut now = io.now().0 + 1;
    job.hold_to_target(now);
    now += reel::pool_room::DECAY_NANOS;
    job.hold_to_target(now);
    assert!(index.may_refill(), "the store had room and no refill may run");
    // the rebalance covered the index, so it only waits for its refill and isn't at the cap
    assert_eq!(store.memory_report().indexes_at_cap, 0, "an index the rebalance covered reads as at the cap");
    let volume = store.state.columns[COLUMN.0 as usize].expect("a column").volume.0 as usize;
    let mut refill = store.state.start_sorted_refill(volume).expect("a refill");
    while store.state.sorted_refill_step(volume, &mut refill).expect("refill step") {}
    let reading = sorted_reading(&store);
    assert!(!reading.is_marked && reading.rows_past_capacity.0 == 0 && reading.is_counted, "the refill left the mark: {reading:?}");
    assert!(index.room().committed() > index.room().share(), "the pool never grew past its plan");
    assert_eq!(index.entry_count().0, model.len() as u64);
    let asks = report(&store).sorted_footer_asks;
    check(&store, &model, &written);
    assert_eq!(report(&store).sorted_footer_asks, asks, "a key past the old plan asked the footers");
    SIM_BUDGET.with(|budget| budget.set(BUDGET));
    store.close().expect("close");
}

// with the target too tight for the rows past the plan, the index stays marked and says so, and every key still reads
#[test]
fn an_index_past_its_plan_stays_marked_under_a_tight_target() {
    let (io, store, mut model) = small_plan_store(50, BUDGET);
    let keys = outgrow_the_plan(&store, &io, &mut model, 6);
    let index = store.state.sorted_index(COLUMN).expect("an index");
    assert!(index.mark().is_some(), "the fill never outran the pool");
    let job = reel::engine::reserve_job::ReserveJob::new(Arc::clone(&store.state));
    let held = job.held_bytes();
    store.state.memory_target.store(held, Ordering::Release);
    let mut now = io.now().0 + 1;
    for _step in 0..4 {
        job.hold_to_target(now);
        now += reel::pool_room::DECAY_NANOS;
    }
    assert!(!index.may_refill(), "a refill may run with no room for it");
    assert!(store.state.start_sorted_refill(store.state.columns[COLUMN.0 as usize].expect("a column").volume.0 as usize).is_none());
    let reading = sorted_reading(&store);
    assert!(reading.is_marked && reading.rows_past_capacity.0 > 0, "{reading:?}");
    let written: Vec<Vec<u8>> = (0..keys).map(key).collect();
    check(&store, &model, &written);
    store.close().expect("close");
}

// a store whose host refuses the reservations past the shares opens on the shares, reports them, and holds at open what a store
// past its shares holds
#[test]
fn a_store_with_its_past_refused_opens_on_its_shares() {
    let past = open_sized(&SimulatedIo::new(Seed(51)));
    reel::pool_room::REFUSE_PAST.with(|refuse| refuse.set(true));
    let io = SimulatedIo::new(Seed(51));
    let held = open_sized(&io);
    reel::pool_room::REFUSE_PAST.with(|refuse| refuse.set(false));
    assert_eq!(past.memory_report().pools_held_to_share, 0);
    assert!(held.memory_report().pools_held_to_share > 0, "a refused past went unreported");
    assert_eq!(held.memory_report().pool_committed, past.memory_report().pool_committed, "a room past its share held more at open");
    let index = held.state.sorted_index(COLUMN).expect("an index");
    assert_eq!(index.room().reserved(), index.room().share());
    let mut model = Model::new();
    for number in 0..KEYS {
        held.put(COLUMN, &key(number), b"held").expect("put");
        model.insert(key(number), b"held".to_vec());
    }
    seal_all(&held, &io);
    check(&held, &model, &(0..KEYS).map(key).collect::<Vec<_>>());
    held.close().expect("close");
    past.close().expect("close");
}


// an index filled to four times its plan gets its room from the caches and leaves the write index its own: a burst of ingest after
// it meets no more waits for index room and no more short pools than a store whose index stayed held to its share, the write
// index's rooms grow as they do there, and the index's mark is gone with no reopen
#[test]
fn an_index_past_its_plan_leaves_ingest_its_room() {
    let held = ingest_after_an_index_fills(true);
    let ruled = ingest_after_an_index_fills(false);
    eprintln!("ingest held to share: {held:?}, {} puts a second", held.puts_a_second);
    eprintln!("ingest under the rule: {ruled:?}, {} puts a second", ruled.puts_a_second);
    assert_eq!(held.capacity, ruled.capacity, "the two arms planned the index apart");
    assert!(held.is_marked_before && held.is_marked && held.rebalances == 0, "the index held to its share: {held:?}");
    assert!(!ruled.is_marked_before && !ruled.is_marked && ruled.rebalances > 0, "the rule left the index marked: {ruled:?}");
    // the burst outruns the write index in both arms alike, so the waits match to within a twentieth
    assert!(ruled.index_waits <= held.index_waits + held.index_waits / 20 + 4, "the burst waited more for index room: {ruled:?} against {held:?}");
    assert!(ruled.most_short <= held.most_short + 1, "the burst ran more pools short: {ruled:?} against {held:?}");
    assert!(ruled.write_rooms.iter().zip(&held.write_rooms).all(|(ruled, held)| ruled + (2 << 20) >= *held), "the write index grew less: {ruled:?} against {held:?}");
}

/// What a burst of ingest met after the sorted keys filled to four times their plan
#[derive(Debug)]
struct Ingest {
    capacity: u64,
    is_marked_before: bool,
    puts_a_second: u64,
    index_waits: u64,
    most_short: usize,
    write_rooms: Vec<usize>,
    is_marked: bool,
    rebalances: u64,
}

// fill a sorted column to four times its plan beside a plain column, give the job its decays and a refill, then a burst across
// both columns with the job's steps 100 ms apart past a decay, and say what the burst met
fn ingest_after_an_index_fills(is_held: bool) -> Ingest {
    const PLAIN: ColumnId = ColumnId(2);
    const BURST: u64 = 600_000;
    let io = SimulatedIo::new(Seed(53));
    // a budget where the caches' shards cover what the rule moves
    SIM_BUDGET.with(|budget| budget.set(HELD_BUDGET));
    let mut config = config(RowCount(0));
    SIM_BUDGET.with(|budget| budget.set(BUDGET));
    reel::engine::engine_sizing::EXACT_INDEX_ROWS.with(|exact| exact.set(false));
    config.columns[0].least_rows = None;
    let mut plain = ColumnConfig::new(PLAIN, "plain", KeyShape::Variable { most: 64 });
    plain.index = ColumnIndex::Plain;
    config.columns.push(plain);
    // a small headroom and a reopen on a few rows plan the index small, so four times its plan still fits the open-time cap
    reel::engine::engine_sizing::TEST_INDEX_HEADROOM.with(|headroom| headroom.set(Some(4 << 20)));
    let first = ReelStore::open(io.clone(), &config).expect("open");
    for number in 0..KEYS / 4 {
        first.put(COLUMN, &key(number), b"first").expect("put");
    }
    seal_all(&first, &io);
    first.close().expect("close");
    drop(first);
    reel::pool_room::REFUSE_PAST.with(|refuse| refuse.set(is_held));
    let store = ReelStore::open(io.clone(), &config).expect("open");
    reel::pool_room::REFUSE_PAST.with(|refuse| refuse.set(false));
    reel::engine::engine_sizing::TEST_INDEX_HEADROOM.with(|headroom| headroom.set(None));
    let job = reel::engine::reserve_job::ReserveJob::new(Arc::clone(&store.state));
    let index = store.state.sorted_index(COLUMN).expect("an index");
    let capacity = sorted_reading(&store).capacity.0;
    let mut now = io.now().0;
    for number in 0..capacity * 4 {
        store.put(COLUMN, &key(number), b"sorted-value").expect("put");
        if number % 4_096 == 4_095 {
            seal_all(&store, &io);
            now += 100_000_000;
            job.grow_pools();
        }
    }
    seal_all(&store, &io);
    // past the decays a marked index the rule has room for refills
    let volume = store.state.columns[COLUMN.0 as usize].expect("a column").volume.0 as usize;
    for _decay in 0..3 {
        now += reel::pool_room::DECAY_NANOS;
        let _target = job.hold_to_target(now);
        if let Some(mut refill) = store.state.start_sorted_refill(volume) {
            while store.state.sorted_refill_step(volume, &mut refill).expect("refill step") {}
        }
    }
    let is_marked_before = index.mark().is_some();
    let write_rooms = |store: &Store| store.state.index.rooms().iter().map(|room| room.committed()).collect::<Vec<_>>();
    let waits_before = store.state.index_waits.load(Ordering::Relaxed);
    let mut most_short = 0;
    let started = std::time::Instant::now();
    for number in 0..BURST {
        store.put(PLAIN, format!("burst-{number:012}").as_bytes(), &[7; 48]).expect("put");
        // the burst's sorted rows overwrite keys the index holds, so they fill the write index and leave the index's keys as they are
        store.put(COLUMN, &key(number % (capacity * 4)), b"burst-sorted").expect("put");
        // the seals settle rarely, so open rows pile up and the write index wants its whole share
        if number % 65_536 == 65_535 {
            settle(&io);
            now += 100_000_000;
            job.grow_pools();
            most_short = most_short.max(store.memory_report().pools_short);
        }
    }
    let seconds = started.elapsed().as_secs_f64();
    let rooms = write_rooms(&store);
    let waits = store.state.index_waits.load(Ordering::Relaxed) - waits_before;
    // the burst's own sorted keys can outrun the pool again, and the next decays bring the index back once more
    seal_all(&store, &io);
    for _decay in 0..3 {
        now += reel::pool_room::DECAY_NANOS;
        let _target = job.hold_to_target(now);
        if let Some(mut refill) = store.state.start_sorted_refill(volume) {
            while store.state.sorted_refill_step(volume, &mut refill).expect("refill step") {}
        }
    }
    let ingest = Ingest {
        is_marked_before,
        capacity,
        puts_a_second: ((2 * BURST) as f64 / seconds) as u64,
        index_waits: waits,
        most_short,
        write_rooms: rooms,
        is_marked: index.mark().is_some(),
        rebalances: store.memory_report().rebalances,
    };
    store.close().expect("close");
    ingest
}


// open a sized store at the full budget on a few rows, so its index plans small, fill the index to this many halves of the plan,
// and step the job past a decay so a rebalance runs. Returns the store, its model and the keys written
fn rebalanced_store(seed: u64, halves: u64, budget: ByteCount) -> (SimulatedIo, Store, Model, u64) {
    let (io, store, mut model) = small_plan_store(seed, budget);
    SIM_BUDGET.with(|set| set.set(budget));
    let keys = outgrow_the_plan(&store, &io, &mut model, halves);
    let job = reel::engine::reserve_job::ReserveJob::new(Arc::clone(&store.state));
    let mut now = io.now().0 + 1;
    for _step in 0..3 {
        job.hold_to_target(now);
        now += reel::pool_room::DECAY_NANOS;
    }
    SIM_BUDGET.with(|set| set.set(BUDGET));
    (io, store, model, keys)
}

// a sized store at this budget opened again on a few rows, so its index plans small
fn small_plan_store(seed: u64, budget: ByteCount) -> (SimulatedIo, Store, Model) {
    SIM_BUDGET.with(|set| set.set(budget));
    reel::engine::engine_sizing::TEST_INDEX_HEADROOM.with(|headroom| headroom.set(Some(SMALL_HEADROOM)));
    let io = SimulatedIo::new(Seed(seed));
    let store = open_sized(&io);
    let mut model = Model::new();
    for number in 0..KEYS / 4 {
        store.put(COLUMN, &key(number), b"first").expect("put");
        model.insert(key(number), b"first".to_vec());
    }
    seal_all(&store, &io);
    store.close().expect("close");
    drop(store);
    let store = open_sized(&io);
    store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
    SIM_BUDGET.with(|set| set.set(BUDGET));
    reel::engine::engine_sizing::TEST_INDEX_HEADROOM.with(|headroom| headroom.set(None));
    (io, store, model)
}

/// The headroom a small plan's index asks past its keys, so a few rows on disk plan it at tens of thousands of rows
const SMALL_HEADROOM: u64 = 4 << 20;

// the bytes one shard of each cache gives a rebalance: the spot and block caches' shards, and a record cache shard's share past
// its extent
fn cache_shards(store: &Store) -> [u64; 3] {
    use reel::engine::reserve_job::{WholeCache, whole_caches};
    let caches: Vec<&dyn WholeCache> = whole_caches(&store.state).collect();
    [caches[0].shard_bytes() as u64, caches[1].shard_bytes() as u64, store.state.record_cache.shard_give()]
}

// an index whose keys pass even what the open-time rule's cap gives stays marked, the caches sit at their floors to within a
// shard, and no other pool's ceiling moved
#[test]
fn an_index_past_the_cap_stays_marked_with_the_caches_at_their_floor() {
    // twelve halves of the plan pass what the cap gives at the small budget
    let (_io, store, model, keys) = rebalanced_store(54, 12, BUDGET);
    let index = store.state.sorted_index(COLUMN).expect("an index");
    let memory = store.memory_report();
    assert!(index.mark().is_some() && memory.indexes_at_cap > 0, "{memory:?}");
    assert!(memory.rebalances > 0 && index.room().ceiling() > index.room().share(), "no rebalance ran");
    let floors = store.state.rebalance_plan.as_ref().expect("a plan").floors();
    let shards = cache_shards(&store);
    for cache in 0..3 {
        let ceiling = memory.cache_ceilings[cache].0;
        assert!(ceiling >= floors[cache], "cache {cache} went under its floor: {ceiling} < {}", floors[cache]);
        // a cache stops at its floor, short of it by under a shard or a page, or where its shards can free no more: the filter
        // arena and the dead bits hold the rest of its share
        let slack = shards[cache].max(reel::index::sorted_keys::PAGE_BYTES as u64);
        let left = reel::engine::rebalance::freeable(&store.state, cache);
        assert!(ceiling - floors[cache] < slack || left < shards[cache].max(1), "cache {cache} kept a shard it could free past its floor: {ceiling} {} with {left} free", floors[cache]);
    }
    // the write index, the summaries and the value pages keep their ceilings
    let sorted_room = index.room();
    let record_rooms = store.state.record_cache.rooms();
    for room in store.state.pool_rooms.iter().filter(|room| !Arc::ptr_eq(room, sorted_room) && !record_rooms.iter().any(|record| Arc::ptr_eq(room, record))) {
        assert!(room.ceiling() >= room.reserved(), "a pool past the indexes and the caches lost room");
    }
    let written: Vec<Vec<u8>> = (0..keys).map(key).collect();
    check(&store, &model, &written);
    store.close().expect("close");
}

// a reopen after a rebalance shares the caches and the index as the rebalance did: the index's share and each cache's bytes
// match the run-time ceilings to within an extent and a shard
#[test]
fn a_reopen_after_a_rebalance_shares_as_the_rebalance_did() {
    // fill a batch at a time and stop on the batch a rebalance ran in, so the reopen counts the keys that rebalance did, to a batch
    // a budget where the caches' shards cover what the rule moves, so the run-time shares can match a reopen's
    let (io, store, mut model) = small_plan_store(55, HELD_BUDGET);
    let index = store.state.sorted_index(COLUMN).expect("an index");
    // two rebalances come within a few doublings of the plan, so a fill past sixteen times it says why it never saw them
    let bound = sorted_reading(&store).capacity.0 * 16;
    let (mut keys, mut rebalances) = (0, 0);
    while keys < bound {
        for number in keys..keys + 4_096 {
            store.put(COLUMN, &key(number), b"batch").expect("put");
            model.insert(key(number), b"batch".to_vec());
        }
        keys += 4_096;
        seal_all(&store, &io);
        let now = store.memory_report().rebalances;
        if now > rebalances && rebalances > 0 {
            break;
        }
        rebalances = now;
    }
    let memory = store.memory_report();
    assert!(
        memory.rebalances > 1,
        "the fill never rebalanced twice: rebalances {} held_to_share {} at_cap {} ceiling {} reserved {} plan {:?}",
        memory.rebalances,
        memory.pools_held_to_share,
        memory.indexes_at_cap,
        index.room().ceiling(),
        index.room().reserved(),
        store.state.rebalance_plan.as_ref().map(|plan| plan.cap),
    );
    let ceiling = index.room().ceiling() as u64;
    let extent = index.room().extent() as u64;
    let ceilings = store.memory_report().cache_ceilings;
    let shards = cache_shards(&store);
    store.close().expect("close");
    drop(store);
    reel::engine::engine_sizing::TEST_INDEX_HEADROOM.with(|headroom| headroom.set(Some(SMALL_HEADROOM)));
    SIM_BUDGET.with(|budget| budget.set(HELD_BUDGET));
    let store = open_sized(&io);
    SIM_BUDGET.with(|budget| budget.set(BUDGET));
    reel::engine::engine_sizing::TEST_INDEX_HEADROOM.with(|headroom| headroom.set(None));
    let reopened = store.state.sorted_index(COLUMN).expect("an index").room().share() as u64;
    let within = |run: u64, open: u64, slack: u64| run.abs_diff(open) <= slack;
    assert!(within(ceiling, reopened, extent + shards.iter().max().copied().unwrap_or(0)), "the index: {ceiling} at run time, {reopened} at the reopen");
    let again = store.memory_report().cache_ceilings;
    for cache in 0..3 {
        assert!(within(ceilings[cache].0, again[cache].0, extent + shards[cache]), "cache {cache}: {} at run time, {} at the reopen", ceilings[cache].0, again[cache].0);
    }
    let written: Vec<Vec<u8>> = (0..keys).map(key).collect();
    check(&store, &model, &written);
    store.close().expect("close");
}

// pressure after a rebalance: the ladder still gives the caches' shards back and brings them back past it, and no shard comes back
// over a ceiling the rebalance lowered
#[test]
fn the_ladder_after_a_rebalance_brings_nothing_back_past_a_ceiling() {
    let (io, store, _model, _keys) = rebalanced_store(56, 3, BUDGET);
    let memory = store.memory_report();
    assert!(memory.rebalances > 0, "no rebalance ran");
    let ceilings = memory.cache_ceilings;
    let given = memory.cache_shards_given;
    // a cache of shards under a page can give none of them back, so it holds them whatever its share
    let (shards, page) = (cache_shards(&store), 4_096);
    let job = reel::engine::reserve_job::ReserveJob::new(Arc::clone(&store.state));
    io.set_host_memory(reel::io::host_memory::HostMemory {
        total: ByteCount(1 << 30),
        available: ByteCount(0),
        group_room: Some(ByteCount(0)),
        group_limit: Some(ByteCount(1 << 30)),
        group_anon_room: Some(ByteCount(0)),
    });
    let mut now = io.now().0 + 1;
    for _step in 0..64 {
        now += 100_000_000;
        job.hold_to_target(now);
        let memory = store.memory_report();
        for cache in (0..3).filter(|cache| shards[*cache] >= page) {
            assert!(memory.cache_present[cache] <= memory.cache_ceilings[cache], "cache {cache} held past its ceiling under pressure");
        }
    }
    let could_give = reel::engine::reserve_job::whole_caches(&store.state)
        .enumerate()
        .any(|(place, cache)| cache.shard_bytes() >= page as usize && cache.shard_count().min(store.state.cache_ceilings[place].load(Ordering::Acquire)) > 0);
    assert!(!could_give || store.memory_report().cache_shards_given > given, "the ladder gave no cache shard back under pressure");
    io.set_host_memory(reel_sim::simulated_disk::SIMULATED_HOST);
    for _step in 0..256 {
        now += reel::pool_room::DECAY_NANOS / 4;
        job.hold_to_target(now);
        let memory = store.memory_report();
        assert_eq!(memory.cache_ceilings, ceilings, "a ceiling moved back up before a reopen");
        for cache in (0..3).filter(|cache| shards[*cache] >= page) {
            assert!(memory.cache_present[cache] <= memory.cache_ceilings[cache], "cache {cache} came back past its ceiling");
        }
    }
    let memory = store.memory_report();
    // every block cache shard under its ceiling came back, and only those over it stay away
    let over: u64 = reel::engine::reserve_job::whole_caches(&store.state)
        .enumerate()
        .skip(1)
        .map(|(place, cache)| cache.shard_count().saturating_sub(store.state.cache_ceilings[place].load(Ordering::Acquire)) as u64)
        .sum();
    assert_eq!(memory.block_absent_shards, over, "the block caches never came back to their ceilings: {memory:?}");
    store.close().expect("close");
}

// the caches filled first, through reads of a plain column, then the index driven past the cap: held stays at or under the target
// at every step, and the caches' bytes in memory, the filter arena and the dead bits with them, stay at or under their ceilings
#[test]
fn a_rebalance_over_full_caches_keeps_held_under_the_target() {
    const PLAIN: ColumnId = ColumnId(2);
    const PLAIN_ROWS: u64 = 150_000;
    let io = SimulatedIo::new(Seed(57));
    let mut config = config(RowCount(0));
    reel::engine::engine_sizing::EXACT_INDEX_ROWS.with(|exact| exact.set(false));
    config.columns[0].least_rows = None;
    let mut plain = ColumnConfig::new(PLAIN, "plain", KeyShape::Variable { most: 64 });
    plain.index = ColumnIndex::Plain;
    config.columns.push(plain);
    reel::engine::engine_sizing::TEST_INDEX_HEADROOM.with(|headroom| headroom.set(Some(SMALL_HEADROOM)));
    let first = ReelStore::open(io.clone(), &config).expect("open");
    for number in 0..KEYS / 4 {
        first.put(COLUMN, &key(number), b"first").expect("put");
    }
    seal_all(&first, &io);
    first.close().expect("close");
    drop(first);
    let store = ReelStore::open(io.clone(), &config).expect("open");
    reel::engine::engine_sizing::TEST_INDEX_HEADROOM.with(|headroom| headroom.set(None));
    let job = reel::engine::reserve_job::ReserveJob::new(Arc::clone(&store.state));
    let plain_key = |number: u64| format!("plain-{number:010}").into_bytes();
    for number in 0..PLAIN_ROWS {
        store.put(PLAIN, &plain_key(number), &[(number % 251) as u8; 200]).expect("put");
        if number % 4_096 == 4_095 {
            seal_all(&store, &io);
            job.grow_pools();
        }
    }
    seal_all(&store, &io);
    // reads over the plain rows until the caches stop growing
    let mut value = Vec::new();
    let mut last = [ByteCount(0); 3];
    for _round in 0..8 {
        for number in (0..PLAIN_ROWS).step_by(3) {
            let _found = store.get(PLAIN, &plain_key(number), &mut value).expect("get");
        }
        job.grow_pools();
        let present = store.memory_report().cache_present;
        if present == last {
            break;
        }
        last = present;
    }
    let caches_held = |memory: &reel_core::memory_report::MemoryReport| {
        memory.cache_present.iter().map(|bytes| bytes.0).sum::<u64>() + memory.arena_room.0 + memory.dead_bits.0
    };
    let check_bounds = |label: &str| {
        let memory = store.memory_report();
        assert!(memory.memory_held <= memory.memory_target, "{label}: held passed the target: {memory:?}");
        let ceilings: u64 = memory.cache_ceilings.iter().map(|bytes| bytes.0).sum();
        assert!(caches_held(&memory) <= ceilings, "{label}: the caches hold {} past their ceilings {ceilings}: {memory:?}", caches_held(&memory));
    };
    check_bounds("caches full");
    let capacity = sorted_reading(&store).capacity.0;
    for number in 0..capacity * 6 {
        store.put(COLUMN, &key(number), b"sorted-value").expect("put");
        if number % 4_096 == 4_095 {
            seal_all(&store, &io);
            job.grow_pools();
            check_bounds("index filling");
        }
    }
    let memory = store.memory_report();
    assert!(memory.rebalances > 0, "no rebalance ran: {memory:?}");
    store.close().expect("close");
}

/// Sorted columns in the split tests, as many as a tape node keeps
const SPLIT_COLUMNS: u8 = 17;

// a sized store of seventeen sorted columns of the sims' keys, each asking by the keys it holds
fn split_config() -> StoreConfig {
    let mut config = config(RowCount(0));
    reel::engine::engine_sizing::EXACT_INDEX_ROWS.with(|exact| exact.set(false));
    let first = config.columns[0].clone();
    config.columns = (1..=SPLIT_COLUMNS)
        .map(|id| ColumnConfig { id: ColumnId(id), name: format!("sorted-{id}"), least_rows: None, ..first.clone() })
        .collect();
    config
}

// fill some of the split store's columns far past their first shares, the others with a few keys, stepping the job between
// batches, and check after every step that the indexes' ceilings stay under the cap, no cache moves, and no index's ceiling
// falls under what it holds and a step. Returns each busy column's keys
fn fill_split(store: &Store, io: &SimulatedIo, busy: &[ColumnId], times: u64) -> Vec<u64> {
    fill_split_beside(store, io, busy, times, (64, true))
}

// fill_split with this many keys in each idle column, none for truly empty ones. A fill that takes past what the idle columns
// leave reaches the caches past their floors' rounding, so it says whether the caches must stay as they are
fn fill_split_beside(store: &Store, io: &SimulatedIo, busy: &[ColumnId], times: u64, (idle_keys, is_caches_fixed): (u64, bool)) -> Vec<u64> {
    let plan = store.state.rebalance_plan.as_ref().expect("a plan");
    let job = reel::engine::reserve_job::ReserveJob::new(Arc::clone(&store.state));
    let caches = store.memory_report().cache_ceilings;
    let given = store.memory_report().cache_shards_given;
    for id in (1..=SPLIT_COLUMNS).map(ColumnId).filter(|id| !busy.contains(id)) {
        for number in 0..idle_keys {
            store.put(id, &key(number), b"idle").expect("put");
        }
    }
    let first = store.state.sorted_index(busy[0]).expect("an index").room().share();
    let rows = reel::index::sorted_keys::rows_for_bytes(WIDTH as usize, ByteCount(first as u64)).0;
    // past the times asked, the fill stops on the batch a rebalance ran in, so a reopen counts the keys that rebalance did
    let (least, bound) = (rows * times, rows * times * 4);
    let mut keys = 0;
    let mut rebalances = store.memory_report().rebalances;
    while keys < bound {
        for number in keys..keys + 4_096 {
            for id in busy {
                store.put(*id, &key(number), b"busy").expect("put");
            }
        }
        keys += 4_096;
        {
            seal_all(store, io);
            job.grow_pools();
            let indexes: Vec<_> = store.state.sorted.iter().flatten().collect();
            let ceilings: u64 = indexes.iter().map(|index| index.room().ceiling() as u64).sum();
            assert!(ceilings <= plan.cap, "the indexes' ceilings {ceilings} passed the cap {}", plan.cap);
            for index in &indexes {
                let room = index.room();
                assert!(room.ceiling() >= room.committed() + room.step() || room.ceiling() >= room.reserved(), "a ceiling fell under what its index holds and a step");
            }
            let memory = store.memory_report();
            if is_caches_fixed {
                assert_eq!(memory.cache_ceilings, caches, "a cache's ceiling moved when the room came from the indexes");
                assert_eq!(memory.cache_shards_given, given, "a cache shard went back when the room came from the indexes");
            }
            let now = memory.rebalances;
            if keys >= least && now > rebalances {
                break;
            }
            rebalances = now;
        }
    }
    assert!(keys < bound, "the fill never rebalanced past {least} keys: {:?}", store.memory_report());
    // past the decays a refill brings back what the fill marked
    for _decay in 0..3 {
        io.advance_clock(reel_core::units::Nanoseconds(reel::pool_room::DECAY_NANOS));
        let _target = job.hold_to_target(io.now().0);
        for volume in 0..store.state.volumes.len() {
            while let Some(mut refill) = store.state.start_sorted_refill(volume) {
                while store.state.sorted_refill_step(volume, &mut refill).expect("refill step") {}
            }
        }
    }
    busy.iter().map(|_id| keys).collect()
}

// read a split store's index ceilings, then close it and open it again, and read the shares the reopen gives each busy column
fn reopen_shares(store: Store, io: &SimulatedIo, busy: &[ColumnId]) -> (Vec<u64>, Vec<u64>, u64) {
    let ceilings: Vec<u64> = busy.iter().map(|id| store.state.sorted_index(*id).expect("an index").room().ceiling() as u64).collect();
    // an extent, and the grant for the one batch of keys the reopen counts past the rebalance's look
    let batch = reel::index::sorted_keys::planned_index_bytes(WIDTH as usize, RowCount(2 * 4_096)).0;
    let extent = store.state.sorted_index(busy[0]).expect("an index").room().extent() as u64 + batch;
    store.close().expect("close");
    drop(store);
    let store = ReelStore::open(io.clone(), &split_config()).expect("open");
    let shares = busy.iter().map(|id| store.state.sorted_index(*id).expect("an index").room().share() as u64).collect();
    store.close().expect("close");
    (ceilings, shares, extent)
}

// one column of seventeen filled far past its first share, the rest near empty: it takes the room the idle columns never used,
// with no cache moved, comes back unmarked with no reopen, and its ceiling matches what a reopen gives it to within an extent and
// a batch
#[test]
fn a_busy_index_takes_the_room_idle_indexes_never_used() {
    let io = SimulatedIo::new(Seed(58));
    let store = ReelStore::open(io.clone(), &split_config()).expect("open");
    store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
    let busy = [ColumnId(5)];
    let _keys = fill_split(&store, &io, &busy, 6);
    let reading = report(&store).sorted_columns.iter().find(|reading| reading.column == busy[0]).copied().expect("a reading");
    assert!(!reading.is_marked && reading.rows_past_capacity.0 == 0, "the busy column stayed marked: {reading:?}");
    assert!(reading.ceiling.0 > store.state.sorted_index(busy[0]).expect("an index").room().share() as u64, "the busy column took no room");
    assert!(store.memory_report().rebalances > 0);
    let (ceilings, shares, extent) = reopen_shares(store, &io, &busy);
    assert!(ceilings[0].abs_diff(shares[0]) <= extent, "the busy column: {} at run time, {} at the reopen", ceilings[0], shares[0]);
}

// two busy columns at once share the idle columns' room by their keys, as share_indexes would, and neither starves the other
#[test]
fn two_busy_indexes_share_the_idle_room_by_their_keys() {
    let io = SimulatedIo::new(Seed(59));
    let store = ReelStore::open(io.clone(), &split_config()).expect("open");
    store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
    let busy = [ColumnId(3), ColumnId(11)];
    let _keys = fill_split(&store, &io, &busy, 4);
    for id in busy {
        let reading = report(&store).sorted_columns.iter().find(|reading| reading.column == id).copied().expect("a reading");
        assert!(!reading.is_marked, "column {id:?} stayed marked: {reading:?}");
    }
    let (ceilings, shares, extent) = reopen_shares(store, &io, &busy);
    for (place, id) in busy.iter().enumerate() {
        assert!(ceilings[place].abs_diff(shares[place]) <= extent, "column {id:?}: {} at run time, {} at the reopen", ceilings[place], shares[place]);
    }
}

/// How a walk's tail of rows is hidden in the tail-walk tests
#[derive(Clone, Copy, Debug)]
enum Hidden {
    /// Each tail key deleted on its own, a grave a row
    Graves,
    /// One range delete over the whole tail
    Range,
}

// a walk told to want every row takes them all in one batch, which then passes the hidden tail to the column's end. A put lands
// just past the last live key, between it and the hidden tail, while the walk steps the batch out: the next batch, at a point past
// the put, answers it, and the walk ends after it. With a seal between the put and that batch, the put shows from a segment
fn walk_past_a_hidden_tail(seed: u64, direction: Direction, hidden: Hidden, is_seal_between: bool, tail: u64) {
    let io = SimulatedIo::new(Seed(seed));
    let store = open(&io, CAPACITY);
    let total = KEYS;
    let mut names: Vec<Vec<u8>> = (0..total).map(key).collect();
    for name in &names {
        store.put(COLUMN, name, b"live").expect("put");
    }
    seal_all(&store, &io);
    names.sort();
    if direction == Direction::Descending {
        names.reverse();
    }
    // the tail is the last keys in the walk's direction, hidden past the live ones
    let (live, hidden_keys) = names.split_at((total - tail) as usize);
    match hidden {
        Hidden::Graves => {
            for name in hidden_keys {
                store.delete(COLUMN, name).expect("delete");
            }
        },
        Hidden::Range => match direction {
            // the range takes keys of the column's width, and no key the sims write is all 0xFF or all zero
            Direction::Ascending => store.delete_range(COLUMN, &hidden_keys[0], &[0xFF; WIDTH as usize]).expect("range delete"),
            Direction::Descending => store.delete_range(COLUMN, &[0; WIDTH as usize], &live[live.len() - 1]).expect("range delete"),
        },
    }
    let last_live = live.last().expect("a live key").clone();
    // a key between the last live one and the hidden tail, one step further in the walk's direction
    let mut behind = last_live.clone();
    match direction {
        Direction::Ascending => *behind.last_mut().expect("a byte") += 1,
        Direction::Descending => *behind.last_mut().expect("a byte") -= 1,
    }
    assert!(!names.contains(&behind));
    let start = match direction {
        Direction::Ascending => Vec::new(),
        Direction::Descending => vec![0xFF; WIDTH as usize + 1],
    };
    let range = reel::engine::scan_state::from_key(COLUMN, &start, direction).wanting(RowCount(total));
    let mut scan = store.state.take_scan(&range, None).expect("scan");
    let (mut name, mut value) = (Vec::new(), Vec::new());
    let mut answered = Vec::new();
    loop {
        let step = store.state.step_scan(&mut scan, &mut name, reel::engine::scan_path::ScanWant::Value(&mut value)).expect("step");
        match step {
            ScanStep::Found => answered.push(name.clone()),
            ScanStep::End => break,
            ScanStep::Corrupt => panic!("a corrupt record"),
        }
        // the put lands once the walk has the last live key, from the batch that went on to pass the tail
        if answered.len() == live.len() {
            store.put(COLUMN, &behind, b"behind").expect("put");
            if is_seal_between {
                store.seal_now();
                settle(&io);
            }
        }
    }
    store.state.give_back_scan(scan);
    let mut expected: Vec<Vec<u8>> = live.to_vec();
    expected.push(behind);
    assert_eq!(answered.len(), expected.len(), "{direction:?} {hidden:?} sealed {is_seal_between} tail {tail}: the walk answered {} of {} rows", answered.len(), expected.len());
    assert_eq!(answered, expected, "{direction:?} {hidden:?} sealed {is_seal_between} tail {tail}");
    store.close().expect("close");
}

// a put that lands behind a batch's passed tail of graves or a range delete shows in the next batch, either way the walk goes,
// with or without a seal between, and a tail longer than one batch still ends
#[test]
fn a_put_behind_a_hidden_tail_shows_either_way() {
    for direction in [Direction::Ascending, Direction::Descending] {
        for hidden in [Hidden::Graves, Hidden::Range] {
            for is_seal_between in [false, true] {
                walk_past_a_hidden_tail(62, direction, hidden, is_seal_between, 8);
            }
        }
        // a tail of graves far past a batch's keys
        walk_past_a_hidden_tail(63, direction, Hidden::Graves, false, KEYS / 2);
    }
}

// a fresh store whose every index is empty gives each its whole part of the cap, as before the rule, and a reopen with one busy
// column and the rest empty asks the empty ones their headroom alone and gives the busy one the rest
#[test]
fn empty_indexes_ask_a_part_only_while_every_index_is_empty() {
    let io = SimulatedIo::new(Seed(66));
    let store = ReelStore::open(io.clone(), &split_config()).expect("open");
    let plan = store.state.rebalance_plan.as_ref().expect("a plan");
    let (cap, headroom) = (plan.cap, plan.headroom);
    let part = cap / u64::from(SPLIT_COLUMNS);
    for reading in report(&store).sorted_columns.iter() {
        assert_eq!(reading.asked_bytes.0, part, "a fresh store's empty index asked other than its part: {reading:?}");
    }
    let busy = ColumnId(5);
    for number in 0..20_000 {
        store.put(busy, &key(number), b"busy").expect("put");
    }
    seal_all(&store, &io);
    store.close().expect("close");
    drop(store);
    let store = ReelStore::open(io.clone(), &split_config()).expect("open");
    let empty_ask = reel::index::sorted_keys::planned_index_bytes(WIDTH as usize, RowCount(0)).0 + headroom.0;
    let busy_ask = reel::index::sorted_keys::planned_index_bytes(WIDTH as usize, RowCount(20_000 * reel::engine::engine_sizing::INDEX_GROWTH)).0 + headroom.0;
    for reading in report(&store).sorted_columns.iter() {
        let asked = if reading.column == busy { busy_ask } else { empty_ask };
        assert_eq!(reading.asked_bytes.0, asked, "{reading:?}");
    }
    let got = report(&store).sorted_columns.iter().find(|reading| reading.column == busy).expect("a reading").got_bytes.0;
    // the busy column gets its whole ask while the asks fit the cap, up to the cap less the empty columns' headroom
    let others = empty_ask * (u64::from(SPLIT_COLUMNS) - 1);
    assert!(got <= busy_ask && got + others <= cap && got + reel::index::sorted_keys::PAGE_BYTES as u64 * 4 >= busy_ask.min(cap - others), "the busy column got {got} of {busy_ask}");
    store.close().expect("close");
}

// one busy column among seventeen truly empty ones takes far more than half the cap with no reopen, since the empty ones ask
// their headroom once it holds keys
#[test]
fn a_busy_index_beside_empty_ones_takes_most_of_the_cap() {
    let io = SimulatedIo::new(Seed(67));
    let store = ReelStore::open(io.clone(), &split_config()).expect("open");
    store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
    let cap = store.state.rebalance_plan.as_ref().expect("a plan").cap;
    let busy = [ColumnId(5)];
    let _keys = fill_split_beside(&store, &io, &busy, 12, (0, false));
    let reading = report(&store).sorted_columns.iter().find(|reading| reading.column == busy[0]).copied().expect("a reading");
    assert!(!reading.is_marked, "the busy column stayed marked: {reading:?}");
    assert!(reading.ceiling.0 > cap / 2, "the busy column's ceiling {} stopped under half the cap {cap}", reading.ceiling.0);
    store.close().expect("close");
}

// an empty column that wakes up after a busy one took the room gets its own back by the rule, with at most one mark and refill
#[test]
fn an_empty_index_that_wakes_gets_room_back() {
    let io = SimulatedIo::new(Seed(68));
    let store = ReelStore::open(io.clone(), &split_config()).expect("open");
    store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
    // the busy column takes a few parts' worth, leaving room it doesn't use for a column that wakes later
    let _keys = fill_split_beside(&store, &io, &[ColumnId(5)], 3, (0, true));
    let late = ColumnId(9);
    let job = reel::engine::reserve_job::ReserveJob::new(Arc::clone(&store.state));
    let share = store.state.sorted_index(late).expect("an index").room().share() as u64;
    let rows = reel::index::sorted_keys::rows_for_bytes(WIDTH as usize, ByteCount(share)).0;
    // a mark that stands across steps is one mark: count the times one began
    let (mut marks, mut was_marked) = (0, false);
    let refills = store.memory_report().sorted_refills;
    for number in 0..rows * 2 {
        store.put(late, &key(number), b"late").expect("put");
        if number % 4_096 == 4_095 {
            seal_all(&store, &io);
            job.grow_pools();
            let is_marked = store.state.sorted_index(late).expect("an index").mark().is_some();
            marks += u64::from(is_marked && !was_marked);
            was_marked = is_marked;
        }
    }
    seal_all(&store, &io);
    for _decay in 0..3 {
        io.advance_clock(reel_core::units::Nanoseconds(reel::pool_room::DECAY_NANOS));
        let _target = job.hold_to_target(io.now().0);
        for volume in 0..store.state.volumes.len() {
            while let Some(mut refill) = store.state.start_sorted_refill(volume) {
                while store.state.sorted_refill_step(volume, &mut refill).expect("refill step") {}
            }
        }
    }
    let reading = report(&store).sorted_columns.iter().find(|reading| reading.column == late).copied().expect("a reading");
    let refilled = store.memory_report().sorted_refills - refills;
    eprintln!("the late column marked {marks} times and was refilled {refilled} times, and ended {reading:?}");
    assert!(marks <= 1, "the late column marked {marks} times on its way to its room");
    assert!(!reading.is_marked && reading.rows_past_capacity.0 == 0, "the late column stayed marked: {reading:?}");
    assert!(reading.ceiling.0 >= share, "the late column holds less room than its first share");
    store.close().expect("close");
}

// gets, overwrites and deletes on a sorted and a plain column answer as the model does across cache shrinks: the spot, block and
// record caches cut by half, then to one shard each, with the keys spread over the shards left
#[test]
fn reads_match_the_model_across_a_cache_shrink() {
    const PLAIN: ColumnId = ColumnId(2);
    const STEPS: u64 = 6_000;
    for seed in seeds(0..4) {
        let io = SimulatedIo::new(Seed(seed));
        let mut config = config(RowCount(0));
        reel::engine::engine_sizing::EXACT_INDEX_ROWS.with(|exact| exact.set(false));
        config.columns[0].least_rows = None;
        let mut plain = ColumnConfig::new(PLAIN, "plain", KeyShape::Variable { most: 64 });
        plain.index = ColumnIndex::Plain;
        config.columns.push(plain);
        let store = ReelStore::open(io.clone(), &config).expect("open");
        let mut model: BTreeMap<(ColumnId, Vec<u8>), Vec<u8>> = BTreeMap::new();
        let mut random = SeededRandom::new(Seed(seed));
        let mut value = Vec::new();
        for step in 0..STEPS {
            let column = if random.one_in(2) { COLUMN } else { PLAIN };
            let number = random.below(800);
            let name = if column == COLUMN { key(number) } else { format!("plain-{number:05}").into_bytes() };
            match random.below(10) {
                0..=3 => {
                    let mut written = format!("{seed}-{step}-").into_bytes();
                    written.resize(written.len() + random.below(3_000) as usize, b'v');
                    store.put(column, &name, &written).expect("put");
                    model.insert((column, name), written);
                },
                4 => {
                    store.delete(column, &name).expect("delete");
                    model.remove(&(column, name));
                },
                _ => {
                    let found = store.get(column, &name, &mut value).expect("get");
                    assert_eq!(found.then_some(value.as_slice()), model.get(&(column, name.clone())).map(Vec::as_slice), "seed {seed} step {step}");
                },
            }
            if step % 500 == 499 {
                seal_all(&store, &io);
            }
            // the caches lose half their room, then all but a shard each, while keys stay cached in them
            if step == STEPS / 3 || step == 2 * STEPS / 3 {
                let present = store.memory_report().cache_present;
                for (cache, held) in present.iter().enumerate() {
                    let bytes = if step == STEPS / 3 { held.0 / 2 } else { u64::MAX / 4 };
                    reel::engine::rebalance::shrink_cache(&store.state, cache, bytes);
                }
            }
        }
        for ((column, name), written) in &model {
            assert!(store.get(*column, name, &mut value).expect("get"), "seed {seed}: {name:?} is missing");
            assert_eq!(&value, written, "seed {seed}");
        }
        let given = store.memory_report().cache_shards_given;
        assert!(given > 0, "seed {seed}: no shard went back");
        store.close().expect("close");
    }
}

// device reads a get on a plain column read with skewed gets, after one column's index grew enough that rebalances took room from the
// caches, against a reopen at the same shares: the caches spread their keys over the shards left, so a get misses no more often
// than after the reopen, which shrinks every shard instead. Gets a second print beside it, a wall clock that loaded hosts move
#[test]
fn gets_after_a_rebalance_read_as_after_a_reopen() {
    const PLAIN: ColumnId = ColumnId(2);
    const PLAIN_ROWS: u64 = 60_000;
    const SORTED_KEYS: u64 = 20_000;
    const GROWN_KEYS: u64 = 900_000;
    const GETS: u64 = 40_000;
    let io = SimulatedIo::new(Seed(69));
    let mut config = config(RowCount(0));
    reel::engine::engine_sizing::EXACT_INDEX_ROWS.with(|exact| exact.set(false));
    config.columns[0].least_rows = None;
    let mut plain = ColumnConfig::new(PLAIN, "plain", KeyShape::Variable { most: 64 });
    plain.index = ColumnIndex::Plain;
    config.columns.push(plain);
    let open_store = || {
        reel::engine::engine_sizing::TEST_INDEX_HEADROOM.with(|headroom| headroom.set(Some(SMALL_HEADROOM)));
        let store = ReelStore::open(io.clone(), &config).expect("open");
        reel::engine::engine_sizing::TEST_INDEX_HEADROOM.with(|headroom| headroom.set(None));
        store
    };
    let plain_key = |number: u64| format!("plain-{number:010}").into_bytes();
    let store = open_store();
    for number in 0..PLAIN_ROWS {
        store.put(PLAIN, &plain_key(number), &[(number % 251) as u8; 1024]).expect("put");
        if number % 8_192 == 8_191 {
            seal_all(&store, &io);
        }
    }
    for number in 0..SORTED_KEYS {
        store.put(COLUMN, &key(number), b"sorted").expect("put");
    }
    seal_all(&store, &io);
    store.close().expect("close");
    drop(store);
    // a cube of a uniform draw puts most gets on a few keys, spread over the shards by their hashes
    let skewed = |random: &mut SeededRandom| {
        let draw = random.below(1 << 20) as f64 / f64::from(1 << 20);
        (draw * draw * draw * PLAIN_ROWS as f64) as u64
    };
    let reads_a_get = |label: &str, store: &Store, seed: u64| {
        let mut random = SeededRandom::new(Seed(seed));
        let mut value = Vec::new();
        for _get in 0..GETS * 2 {
            let _found = store.get(PLAIN, &plain_key(skewed(&mut random)), &mut value).expect("get");
        }
        let reads = io.read_count().0;
        let started = std::time::Instant::now();
        for _get in 0..GETS {
            assert!(store.get(PLAIN, &plain_key(skewed(&mut random)), &mut value).expect("get"));
        }
        let rate = GETS as f64 / started.elapsed().as_secs_f64();
        let per_get = (io.read_count().0 - reads) as f64 / GETS as f64;
        let memory = store.memory_report();
        eprintln!("{label}: {per_get:.3} device reads a get, {rate:.0} gets a second, caches present {:?} MiB of ceilings {:?}, rebalances {}", memory.cache_present.map(|bytes| bytes.0 >> 20), memory.cache_ceilings.map(|bytes| bytes.0 >> 20), memory.rebalances);
        (per_get, memory.rebalances)
    };
    let store = open_store();
    let job = reel::engine::reserve_job::ReserveJob::new(Arc::clone(&store.state));
    for number in SORTED_KEYS..GROWN_KEYS {
        store.put(COLUMN, &key(number), b"grown").expect("put");
        if number % 8_192 == 8_191 {
            seal_all(&store, &io);
            job.grow_pools();
        }
    }
    seal_all(&store, &io);
    job.grow_pools();
    let given = store.memory_report().cache_shards_given;
    let (after_rebalance, rebalances) = reads_a_get("after the rebalance", &store, 2);
    assert!(rebalances > 0 && given > 0, "no rebalance took a cache shard: rebalances {rebalances}, shards given {given}");
    store.close().expect("close");
    drop(store);
    let store = open_store();
    let (after_reopen, _rebalances) = reads_a_get("after a reopen", &store, 3);
    store.close().expect("close");
    assert!(after_rebalance <= after_reopen * 1.15 + 0.02, "a get read the device {after_rebalance:.3} times after the rebalance, {after_reopen:.3} after a reopen");
}

// a plain column's gets answer with each key's newest value across three spot cache shrinks in a row, 16 shards to 8 to 3 to 2 as
// asked, with writes at each but the last: a key whose shard moves twice must never find a location an older version of it left in
// a shard the spot index kept
#[test]
fn plain_gets_stay_newest_across_successive_spot_shrinks() {
    const PLAIN: ColumnId = ColumnId(2);
    const PLAIN_KEYS: u64 = 3_000;
    for seed in seeds(0..4) {
        let io = SimulatedIo::new(Seed(seed));
        let mut config = config(RowCount(0));
        let mut plain = ColumnConfig::new(PLAIN, "plain", KeyShape::Variable { most: 64 });
        plain.index = ColumnIndex::Plain;
        config.columns.push(plain);
        let store = ReelStore::open(io.clone(), &config).expect("open");
        // no merge rewrites or frees the segments the older versions sit in, so a stale slot still points at a record a get can read
        store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
        store.state.volumes[0].holds.reclaim.store(true, Ordering::Release);
        let mut random = SeededRandom::new(Seed(seed));
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut value = Vec::new();
        let shard_bytes = store.memory_report().cache_present[0].0 / 16;
        let mut live = 16;
        for (round, next) in [16, 8, 3, 2].into_iter().enumerate() {
            if next < live {
                reel::engine::rebalance::shrink_cache(&store.state, 0, (live - next) * shard_bytes);
                live = store.memory_report().cache_present[0].0 / shard_bytes;
            }
            // the shrink to 3 overwrites half the keys, so the segment before it keeps live rows and stays, and the last shrink is read
            // only, so no seal at it refreshes the slot a key's shard moves back to
            for number in (0..PLAIN_KEYS).filter(|number| next != 2 && (next != 3 || number % 2 == 0)) {
                let name = format!("plain-{number:06}").into_bytes();
                if random.one_in(10) {
                    store.delete(PLAIN, &name).expect("delete");
                    model.remove(&name);
                } else {
                    let written = format!("{seed}-{round}-{number}").into_bytes();
                    store.put(PLAIN, &name, &written).expect("put");
                    model.insert(name, written);
                }
            }
            seal_all(&store, &io);
            for number in 0..PLAIN_KEYS {
                let name = format!("plain-{number:06}").into_bytes();
                let found = store.get(PLAIN, &name, &mut value).expect("get");
                assert_eq!(found.then_some(value.as_slice()), model.get(&name).map(Vec::as_slice), "seed {seed} round {round}, {live} spot shards: {}", String::from_utf8_lossy(&name));
            }
        }
        assert!(live < 16, "seed {seed}: the spot cache never shrank");
        store.close().expect("close");
    }
}

// a store of fourteen sorted columns, reopened, holds every value its gets read in their leaves within a few of the job's steps
// though its value pool starts at an eighth, and a get then reads the device about as often as on a read-only open, whose pool
// starts whole
#[test]
fn a_reopened_store_holds_values_in_a_few_job_steps() {
    const SORTED: u8 = 14;
    const KEYS: u64 = 200_000;
    let io = SimulatedIo::new(Seed(71));
    SIM_BUDGET.with(|budget| budget.set(HELD_BUDGET));
    let mut config = config(RowCount(0));
    SIM_BUDGET.with(|budget| budget.set(BUDGET));
    reel::engine::engine_sizing::EXACT_INDEX_ROWS.with(|exact| exact.set(false));
    config.columns[0].least_rows = None;
    for id in 2..=SORTED {
        let mut column = config.columns[0].clone();
        column.id = ColumnId(id);
        column.name = format!("sorted-{id}");
        config.columns.push(column);
    }
    let value = |number: u64| {
        let mut bytes = number.to_le_bytes().to_vec();
        bytes.resize(128, (number % 251) as u8);
        bytes
    };
    let store = ReelStore::open(io.clone(), &config).expect("open");
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), &value(number)).expect("put");
        if number % 16_384 == 16_383 {
            seal_all(&store, &io);
        }
    }
    seal_all(&store, &io);
    store.close().expect("close");
    drop(store);
    let mut buf = Vec::new();
    let reads_a_get = |store: &Store, buf: &mut Vec<u8>| {
        let reads = io.read_count().0;
        for number in 0..KEYS {
            assert!(store.get(COLUMN, &key(number), buf).expect("get"));
            assert_eq!(*buf, value(number));
        }
        (io.read_count().0 - reads) as f64 / KEYS as f64
    };
    let held_values = |store: &Store| {
        let mut report = reel_core::maintenance_report::MaintenanceReport::new();
        store.maintenance_report(&mut report);
        report.held_values
    };
    let store = ReelStore::open(io.clone(), &config).expect("reopen");
    let job = reel::engine::reserve_job::ReserveJob::new(Arc::clone(&store.state));
    // each pass of gets holds what the pool has room for and asks for more, and each job step brings it in
    let mut steps = 0;
    while held_values(&store) < KEYS && steps < 4 {
        let _per_get = reads_a_get(&store, &mut buf);
        job.grow_pools();
        steps += 1;
    }
    let held = held_values(&store);
    assert_eq!(held, KEYS, "the leaves held {held} of {KEYS} values after {steps} job steps");
    let after_steps = reads_a_get(&store, &mut buf);
    store.close().expect("close");
    drop(store);
    let view = ReelStore::open_read_only(io.clone(), &config).expect("open read-only");
    let _warm = reads_a_get(&view, &mut buf);
    let whole = reads_a_get(&view, &mut buf);
    eprintln!("held {held} of {KEYS} after {steps} job steps: {after_steps:.3} device reads a get, {whole:.3} on a read-only open");
    assert!(after_steps <= whole * 1.15 + 0.02, "a get read the device {after_steps:.3} times, {whole:.3} with the pool whole");
}

// a fresh store of fourteen sorted columns at 2 GiB that a fast first sync fills half again past six indexes' first rooms, with no job step
// during the fill, ends with every index unmarked after a few idle decays of the job and the compact job's refills
#[test]
fn a_fresh_store_after_a_fast_sync_ends_unmarked() {
    const SORTED: u8 = 14;
    let io = SimulatedIo::new(Seed(72));
    SIM_BUDGET.with(|budget| budget.set(ByteCount(2 << 30)));
    let mut config = config(RowCount(0));
    SIM_BUDGET.with(|budget| budget.set(BUDGET));
    reel::engine::engine_sizing::EXACT_INDEX_ROWS.with(|exact| exact.set(false));
    let first = config.columns[0].clone();
    config.columns = (1..=SORTED).map(|id| ColumnConfig { id: ColumnId(id), name: format!("sorted-{id}"), least_rows: None, ..first.clone() }).collect();
    let store = ReelStore::open(io.clone(), &config).expect("open");
    let busy: Vec<ColumnId> = (1..=6).map(ColumnId).collect();
    let share = store.state.sorted_index(busy[0]).expect("an index").room().share();
    let rows = reel::index::sorted_keys::rows_for_bytes(WIDTH as usize, ByteCount(share as u64)).0 * 3 / 2;
    eprintln!("each busy column takes {rows} keys, past a first room of {} MiB", share >> 20);
    for number in 0..rows {
        for id in &busy {
            store.put(*id, &key(number), b"sync").expect("put");
        }
        if number % 16_384 == 16_383 {
            seal_all(&store, &io);
        }
    }
    seal_all(&store, &io);
    let show = |label: &str| {
        for reading in report(&store).sorted_columns.iter().filter(|reading| busy.contains(&reading.column)) {
            let index = store.state.sorted_index(reading.column).expect("an index");
            let plan = reel::index::sorted_keys::planned_index_bytes(WIDTH as usize, RowCount(reading.used.0 + reading.rows_past_capacity.0)).0;
            eprintln!(
                "{label} {:?}: marked {} counted {} used {} past {} plan {} MiB ceiling {} MiB reserved {} MiB committed {} MiB may_refill {} refill_waits {}",
                reading.column, reading.is_marked, reading.is_counted, reading.used.0, reading.rows_past_capacity.0, plan >> 20,
                index.room().ceiling() >> 20, index.room().reserved() >> 20, index.room().committed() >> 20, index.may_refill(), reading.refill_waits,
            );
        }
        let memory = store.memory_report();
        eprintln!("{label}: held {} MiB target {} MiB rebalances {} refills {} caches {:?}", memory.memory_held.0 >> 20, memory.memory_target.0 >> 20, memory.rebalances, memory.sorted_refills, memory.cache_ceilings.map(|bytes| bytes.0 >> 20));
    };
    show("after the sync");
    let job = reel::engine::reserve_job::ReserveJob::new(Arc::clone(&store.state));
    let mut value = Vec::new();
    for decay in 0..6 {
        for number in (0..rows).step_by(97) {
            let _found = store.get(busy[decay % busy.len()], &key(number), &mut value).expect("get");
        }
        io.advance_clock(reel_core::units::Nanoseconds(reel::pool_room::DECAY_NANOS));
        let _target = job.hold_to_target(io.now().0);
        for volume in 0..store.state.volumes.len() {
            while let Some(mut refill) = store.state.start_sorted_refill(volume) {
                while store.state.sorted_refill_step(volume, &mut refill).expect("refill step") {}
            }
        }
        show(&format!("decay {decay}"));
    }
    let marked: Vec<_> = report(&store).sorted_columns.iter().filter(|reading| reading.is_marked || !reading.is_counted).map(|reading| reading.column).collect();
    assert!(marked.is_empty(), "indexes stayed marked or uncounted: {marked:?}");
    store.close().expect("close");
}

// pressure gives the compress shares' pages back after the caches: large lz4 values still land, stored as written, and read back,
// and the shares come back in memory once the store has held under its target for the decay, when large values compress again
#[test]
fn the_ladder_gives_the_compress_shares_back_and_brings_them_back() {
    let io = SimulatedIo::new(Seed(73));
    SIM_CODEC.with(|codec| codec.set(Codec::Lz4));
    let config = config(CAPACITY);
    SIM_CODEC.with(|codec| codec.set(Codec::None));
    let store = ReelStore::open(io.clone(), &config).expect("open");
    let large = |number: u64| {
        let mut value = format!("large-{number:06}-").into_bytes();
        value.resize(3 * reel::record::codec::CHUNK_BYTES + 100, b'x');
        value
    };
    let job = reel::engine::reserve_job::ReserveJob::new(Arc::clone(&store.state));
    let pressed = reel::io::host_memory::HostMemory {
        total: ByteCount(1 << 30),
        available: ByteCount(0),
        group_room: Some(ByteCount(0)),
        group_limit: Some(ByteCount(1 << 30)),
        group_anon_room: Some(ByteCount(0)),
    };
    io.set_host_memory(pressed);
    let mut now = io.now().0;
    for _step in 0..256 {
        now += 100_000_000;
        job.hold_to_target(now);
        if store.memory_report().compress_given_back.0 > 0 {
            break;
        }
    }
    let report = store.memory_report();
    assert!(report.compress_given_back.0 > 0 && store.state.are_shares_given.load(Ordering::Acquire), "pressure kept the shares: {report:?}");
    let mut value = Vec::new();
    for number in 0..8 {
        store.put(COLUMN, &key(number), &large(number)).expect("put with the shares given back");
        assert!(store.get(COLUMN, &key(number), &mut value).expect("get") && value == large(number), "a large value read back wrong");
    }
    seal_all(&store, &io);
    io.set_host_memory(reel_sim::simulated_disk::SIMULATED_HOST);
    for _step in 0..4_096 {
        now += 100_000_000;
        job.hold_to_target(now);
        if !store.state.are_shares_given.load(Ordering::Acquire) {
            break;
        }
    }
    let report = store.memory_report();
    assert!(report.compress_given_back.0 == 0 && !store.state.are_shares_given.load(Ordering::Acquire), "the shares never came back: {report:?}");
    for number in 8..16 {
        store.put(COLUMN, &key(number), &large(number)).expect("put with the shares back");
    }
    seal_all(&store, &io);
    for number in 0..16 {
        assert!(store.get(COLUMN, &key(number), &mut value).expect("get") && value == large(number), "value {number} read back wrong");
    }
    store.close().expect("close");
}

// a put that read the shares' flag as down just before the job raised it takes shares the job already handed back: it finds them
// empty, never writes into them, and stores its value as written, which reads back right
#[test]
fn a_put_that_missed_the_shares_flag_stores_its_value_as_written() {
    let io = SimulatedIo::new(Seed(74));
    SIM_CODEC.with(|codec| codec.set(Codec::Lz4));
    let config = config(CAPACITY);
    SIM_CODEC.with(|codec| codec.set(Codec::None));
    let store = ReelStore::open(io.clone(), &config).expect("open");
    let large = |number: u64| {
        let mut value = format!("stale-{number:06}-").into_bytes();
        value.resize(3 * reel::record::codec::CHUNK_BYTES + 100, b'y');
        value
    };
    assert!(reel::engine::reserve_job::give_back_shares(&store.state) > 0, "no share went back");
    reel::engine::write_path::STALE_SHARES_FLAG.with(|stale| stale.set(true));
    let landed: Vec<_> = (0..4).map(|number| store.put(COLUMN, &key(number), &large(number))).collect();
    reel::engine::write_path::STALE_SHARES_FLAG.with(|stale| stale.set(false));
    for (number, result) in landed.iter().enumerate() {
        assert!(result.is_ok(), "a put that took given-back shares failed: {result:?}");
        let mut value = Vec::new();
        assert!(store.get(COLUMN, &key(number as u64), &mut value).expect("get") && value == large(number as u64), "value {number} read back wrong");
    }
    seal_all(&store, &io);
    store.close().expect("close");
}

// an lz4 column refuses a value past the lz4 limit whether or not the ladder has the shares, since a value stored as written while
// they are away would be one the column refuses at every other time
#[test]
fn an_lz4_column_refuses_a_value_past_its_limit_with_the_shares_given_back() {
    let io = SimulatedIo::new(Seed(75));
    SIM_CODEC.with(|codec| codec.set(Codec::Lz4));
    let mut config = config(CAPACITY);
    // a segment past the lz4 limit, so only the codec's limit can refuse the value
    for volume in &mut config.volumes {
        volume.segment_bytes = Some(ByteCount(2 * reel::engine::engine_sizing::LZ4_STORED_LIMIT.0));
    }
    SIM_CODEC.with(|codec| codec.set(Codec::None));
    let store = ReelStore::open(io.clone(), &config).expect("open");
    let limit = store.state.sizing.lz4_stored_limit.0 as usize;
    let value = vec![b'z'; limit + 1];
    let refused = |result: Result<(), reel_core::store_error::StoreError>| matches!(result, Err(reel_core::store_error::StoreError::ValueTooLarge { limit: refused_at, .. }) if refused_at.0 as usize == limit);
    assert!(refused(store.put(COLUMN, &key(1), &value).map(|_| ())), "the limit doesn't hold with the shares in");
    assert!(reel::engine::reserve_job::give_back_shares(&store.state) > 0, "no share went back");
    assert!(refused(store.put(COLUMN, &key(2), &value).map(|_| ())), "the limit doesn't hold with the shares given back");
    let mut read = Vec::new();
    assert!(!store.get(COLUMN, &key(2), &mut read).expect("get"), "a refused value landed");
    seal_all(&store, &io);
    store.close().expect("close");
}

// a get whose find runs before a refill's last fill step and whose read of the mark runs after the refill ended reads the footers
#[test]
fn a_get_across_a_refill_end_finds_its_key() {
    let io = SimulatedIo::new(Seed(48));
    let store = open(&io, CAPACITY);
    store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
    store.state.volumes[0].holds.reclaim.store(true, Ordering::Release);
    let model = fill_for_reopen(&store, &io, KEYS);
    let index = store.state.sorted_index(COLUMN).expect("an index");
    // a whole drop and refill, to count the refill's steps
    let _freed = store.state.drop_sorted(index);
    index.set_may_refill(true);
    let mut refill = store.state.start_sorted_refill(0).expect("a refill");
    let mut steps = 1;
    while store.state.sorted_refill_step(0, &mut refill).expect("refill step") {
        steps += 1;
    }
    assert_eq!(index.mark(), None);
    // a second drop, and a refill stopped two steps short: its last fill step and its end are left
    let _freed = store.state.drop_sorted(index);
    index.set_may_refill(true);
    let mut refill = store.state.start_sorted_refill(0).expect("a refill");
    for _step in 0..steps - 2 {
        assert!(store.state.sorted_refill_step(0, &mut refill).expect("refill step"));
    }
    assert!(refill.segment.is_some() && index.mark().is_some(), "the refill ended early");
    let (name, value) = model.iter().find(|(name, _value)| index.find(name).is_none()).expect("a key the last step fills");
    println!("{} of {} live keys wait for the refill's last fill step, {steps} steps in all", model.keys().filter(|name| index.find(name).is_none()).count(), model.len());
    assert_eq!(read(&store, name).as_ref(), Some(value), "a get beside the refill");
    // the get's find runs, the refill's last two steps run, and the get reads the mark
    let state = Arc::clone(&store.state);
    let left = Arc::new(std::sync::Mutex::new(refill));
    store.state.volumes[0].holds.set_between_path_reads(Some(reel::engine::volume::RunHook(Arc::new(move || {
        let mut refill = left.lock().expect("refill");
        let mut ran = 0;
        while state.sorted_refill_step(0, &mut refill).expect("refill step") {
            ran += 1;
        }
        println!("the refill ran {} steps between the get's find and its read of the mark", ran + 1);
    }))));
    let across = read(&store, name);
    assert_eq!(index.mark(), None, "the hook never ended the refill");
    let after = read(&store, name);
    println!("get across the refill's end: {:?}, get after it: {:?}, model: {:?}", across.as_deref().map(String::from_utf8_lossy), after.as_deref().map(String::from_utf8_lossy), String::from_utf8_lossy(value));
    assert_eq!(after.as_ref(), Some(value));
    assert_eq!(across.as_ref(), Some(value), "a get lost a key to a refill that ended between its find and its read of the mark");
    store.close().expect("close");
}
