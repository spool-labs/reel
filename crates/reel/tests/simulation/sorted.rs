//! Sorted columns: an index holds each sealed key's newest place, so a get reads no footers, through seals, merges, covers and opens

use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

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
use reel_core::units::{ByteCount, DeathMark, Nanoseconds, RowCount, VolumeId};
use reel_core::write_batch::WriteBatch;
use reel_sim::seeded_random::{Seed, SeededRandom};
use reel_sim::simulated_io::SimulatedIo;

const ROOT: &str = "/disk/sorted";
const COLUMN: ColumnId = ColumnId(1);
const WIDTH: u16 = 16;
const BUDGET: ByteCount = ByteCount(128 * 1024 * 1024);
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

// every store these sims open gives its index exactly the rows it declared, so a small one fills and marks
fn config(capacity: RowCount) -> StoreConfig {
    reel::engine::engine_sizing::EXACT_INDEX_ROWS.with(|exact| exact.set(true));
    StoreConfig {
        memory_budget: BUDGET,
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
            codec: Codec::None,
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
    let batch_end = scan.batch.keys[last.key_at..last.key_at + last.key_length].to_vec();
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

// walk with a writer between steps, and check each answer is the model's next key as of its batch
fn walk_beside_writer(store: &Store, io: &SimulatedIo, model: &mut Model, random: &mut SeededRandom, fresh: &mut u64, (direction, write_odds): (Direction, Option<u64>)) -> WalkTally {
    let is_writing = write_odds.is_some();
    let start = match direction {
        Direction::Ascending => Vec::new(),
        Direction::Descending => vec![0xFF; WIDTH as usize + 1],
    };
    let range = reel::engine::scan_state::from_key(COLUMN, &start, direction);
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
        let step = store.state.step_scan(&mut scan, &mut name, reel::engine::scan_path::ScanWant::Value(&mut value)).expect("step");
        let expected = next_in(&seen, last.as_ref(), direction);
        match step {
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
            let tally = walk_beside_writer(&store, &io, &mut model, &mut random, &mut fresh, (direction, Some(DENSE_WRITE_ONE_IN)));
            println!("seed {seed} {direction:?} beside a busy writer: {tally:?}");
        }
        for direction in [Direction::Ascending, Direction::Descending] {
            let tally = walk_beside_writer(&store, &io, &mut model, &mut random, &mut fresh, (direction, Some(STEP_WRITE_ONE_IN)));
            println!("seed {seed} {direction:?} beside a writer: {tally:?}");
            // a batch places again only the heads whose shard took a row, far fewer than every shard
            assert!(tally.heads_together < tally.fills * shards / 2, "{tally:?}");
        }
        // with no writer every head stays placed across batches, and moves only past a key it showed
        let tally = walk_beside_writer(&store, &io, &mut model, &mut random, &mut fresh, (Direction::Ascending, None));
        println!("seed {seed} quiet: {tally:?}");
        assert!(tally.heads_together <= 2 * shards, "{tally:?}");
        check_scans(&store, &model, &mut random);
        store.close().expect("close");
    }
}
