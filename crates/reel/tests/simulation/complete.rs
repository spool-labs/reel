//! Complete columns: a table finds every sealed row, so a get needs no footer search

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::compact_job::CompactJob;
use reel::engine::planner::MOST_WALKLESS_STEPS;
use reel::engine::open_segment::data_path;
use reel::io::job::JobStep;
use reel::engine::complete_image::IMAGE_HEAD_BYTES;
use reel::engine::read_path::RecordWant;
use reel::engine::reel_store::ReelStore;
use reel::engine::store_scan::StoreScan;
use reel::{ScanRange, from_key};
use crate::seed_sets::seeds;
use reel::engine::rewrite_job::RewritePhase;
use reel::engine::walk_job::WalkJob;
use reel::guard::hold;
use reel::index::complete_table::{COMPLETE_SHARDS, CompleteSlot, MOST_TAG_SLOTS, TagMatches, shard_of, tag_of};
use reel::io::{Io, IoFile};
use reel::engine::volume::NO_SEAL;
use reel::index::key_hash::hash_key;
use reel::record::codec::Codec;
use reel::segment::segment_tail::SegmentNumber;
use reel_api::column_id::ColumnId;
use reel_api::direction::Direction;
use reel_api::maintenance_report::{MaintenanceReport, UNIT_KINDS, UnitKind};
use reel_api::scan::ScanStep;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, DeathMark, FileOffset, Nanoseconds, RowCount, VolumeId};
use reel_sim::file_image::BitMask;
use reel_sim::seeded_random::{Seed, SeededRandom};
use reel_sim::simulated_disk::OperationCount;
use reel_sim::simulated_io::SimulatedIo;

const ROOT: &str = "/disk/complete";
const IMAGE_NAME: &str = "complete-1.image";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(128 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 400_000;
const INTERLEAVE_SEEDS: u64 = 4;
const INTERLEAVE_KEYS: u64 = 600;
const INTERLEAVE_ROUNDS: u64 = 6;
const INTERLEAVE_STEPS: u64 = 40;
const INTERLEAVE_WORKERS: usize = 4;
const CHURN_ROUNDS: u64 = 12;
const MODEL_ROUNDS: u64 = 8;
const CLEAR_ROUNDS: u64 = 200;
const CHURN_SEEDS: u64 = 6;
const CHURN_WRITES: u64 = 600;
const CHURN_KEYS: u64 = 1_500;
const CHURN_CAPACITY: RowCount = RowCount(1_000);
const COVER_ONE_IN: u64 = 40;
const SCAN_TAKE: usize = 40;
const MARK: DeathMark = DeathMark(1_000_000);
const CHURN_DELETE_ONE_IN: u64 = 8;
const KEYS: u64 = 2_000;
const CAPACITY: RowCount = RowCount(100_000);
const SMALL_CAPACITY: RowCount = RowCount(200);
const TINY_CAPACITY: RowCount = RowCount(64);
const OVERWRITES: u64 = 300;
const SHORT_LIVED: u64 = 40;
const KEPT_PER_ROUND: u64 = 10;
const OUTPUT_ROOM: u64 = 100_000;
const OUTPUT_KEYS: u64 = 15_000;
const OUTPUT_STEPS: usize = 2;
const IMAGE_ROT_STEP: usize = 8;
const IMAGE_LIST_ROOM: u64 = 1_024;
const CHARGE_CRASH_POINTS: u64 = 40;

type Store = ReelStore<SimulatedIo>;
type Model = BTreeMap<Vec<u8>, Vec<u8>>;
type Rows = Vec<(Vec<u8>, Vec<u8>)>;
// keys seen for each shard and tag, with the first home of each
type HomesSeen = BTreeMap<(usize, u16), Vec<(Vec<u8>, usize)>>;
type Filled = (BTreeMap<Vec<u8>, Vec<u8>>, Vec<Vec<u8>>);

fn config(capacity: RowCount) -> StoreConfig {
    StoreConfig {
        memory_budget: BUDGET,
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "complete".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(),
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Variable { most: 64 },
            codec: Codec::None,
            volume: VolumeId(0),
            complete_rows: Some(capacity),
            sorted_rows: None,
            least_value: None,
        }],
    }
}

fn open(io: &SimulatedIo, capacity: RowCount) -> Store {
    let store = ReelStore::open(io.clone(), &config(capacity)).expect("open");
    // these segments keep the few dead rows they were built with
    store.state.volumes[0].holds.reclaim.store(true, Ordering::Release);
    store
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

fn seal(store: &Store, io: &SimulatedIo) {
    store.seal_now();
    settle(io);
    cool(store, io);
}

fn key(number: u64) -> Vec<u8> {
    format!("key-{:016x}", number.wrapping_mul(0x9E37_79B9_7F4A_7C15)).into_bytes()
}

fn read(store: &Store, name: &[u8]) -> Option<Vec<u8>> {
    let mut value = Vec::new();
    store.get(COLUMN, name, &mut value).expect("get").then_some(value)
}

fn check(store: &Store, model: &BTreeMap<Vec<u8>, Vec<u8>>, deleted: &[Vec<u8>]) {
    for (name, value) in model {
        assert_eq!(read(store, name).as_ref(), Some(value), "{name:?}");
    }
    for name in deleted {
        assert_eq!(read(store, name), None, "{name:?}");
    }
}

// sealed keys read back through the table, with overwrites and deletes across seals, through a clean reopen and a kill
#[test]
fn reads_through_the_table() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io, CAPACITY);
    let mut model = BTreeMap::new();
    for number in 0..KEYS {
        let value = format!("first-{number}").into_bytes();
        store.put(COLUMN, &key(number), &value).expect("put");
        model.insert(key(number), value);
    }
    seal(&store, &io);
    let mut deleted = Vec::new();
    for number in (0..KEYS).step_by(3) {
        let value = format!("second-{number}").into_bytes();
        store.put(COLUMN, &key(number), &value).expect("put");
        model.insert(key(number), value);
    }
    for number in (1..KEYS).step_by(7) {
        store.delete(COLUMN, &key(number)).expect("delete");
        model.remove(&key(number));
        deleted.push(key(number));
    }
    seal(&store, &io);
    check(&store, &model, &deleted);
    assert_eq!(read(&store, b"never-written"), None);
    store.close().expect("close");

    let store = open(&io, CAPACITY);
    check(&store, &model, &deleted);
    store.flush().expect("flush");
    io.kill();
    drop(store);

    let store = open(&io, CAPACITY);
    check(&store, &model, &deleted);
    store.close().expect("close");
}

// past capacity every key still reads, and a newer version beats an older slot through the mark
#[test]
fn past_capacity_stays_right() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io, SMALL_CAPACITY);
    let mut model = BTreeMap::new();
    for round in 0..3 {
        for number in 0..KEYS {
            let value = format!("round-{round}-{number}").into_bytes();
            store.put(COLUMN, &key(number), &value).expect("put");
            model.insert(key(number), value);
        }
        seal(&store, &io);
        check(&store, &model, &[]);
    }
    store.close().expect("close");
}

fn slots_of(store: &Store, name: &[u8]) -> Vec<CompleteSlot> {
    let table = store.state.complete_table(COLUMN).expect("a table");
    let mut matches = TagMatches::new();
    table.find(hash_key(COLUMN, name), &mut matches);
    matches.slots().to_vec()
}

fn dead_rows(store: &Store, segment: SegmentNumber) -> u64 {
    let rows = hold(&store.state.volumes[0].summary);
    let Some(row) = rows.get(segment) else {
        return 0;
    };
    row.cursor().column().map_or(0, |facts| facts.dead_rows.0)
}

fn sealed_numbers(store: &Store) -> Vec<SegmentNumber> {
    let rows = hold(&store.state.volumes[0].summary);
    let mut numbers: Vec<SegmentNumber> = rows.rows().map(|row| row.facts.segment).collect();
    numbers.sort_unstable();
    numbers
}

// the walk frees an overwritten row's slot and charges its segment once, so the key holds one slot and a get reads one block
#[test]
fn walk_frees_and_charges() {
    let io = SimulatedIo::new(Seed(3));
    let store = open(&io, CAPACITY);
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), b"value").expect("put");
    }
    store.put(COLUMN, b"key", b"first").expect("put");
    seal(&store, &io);
    let first = *sealed_numbers(&store).first().expect("a segment");
    store.put(COLUMN, b"key", b"second").expect("put");
    seal(&store, &io);

    let slots = slots_of(&store, b"key");
    assert_eq!(slots.len(), 1);
    assert!(slots.iter().all(|slot| slot.segment_number() != first), "the superseded row kept its slot");
    assert_eq!(dead_rows(&store, first), 1);

    store.state.complete_cache.as_ref().expect("a cache").clear();
    let before = io.read_count();
    assert_eq!(read(&store, b"key"), Some(b"second".to_vec()));
    let reads = io.read_count().0 - before.0;
    assert_eq!(reads, 2, "one row block and one record");
    store.close().expect("close");
}

// a fresh key's slot stays alone, and the walk reads nothing for it
#[test]
fn fresh_keys_stay_unmarked() {
    let io = SimulatedIo::new(Seed(4));
    let store = open(&io, CAPACITY);
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), b"value").expect("put");
    }
    seal(&store, &io);
    for number in 0..KEYS {
        assert!(slots_of(&store, &key(number)).iter().all(|slot| !slot.is_stale()));
    }
    store.close().expect("close");
}

// a key overwritten through many segments folds its stale slots, and keeps reading its newest value
#[test]
fn hot_key_folds() {
    let io = SimulatedIo::new(Seed(5));
    let store = open(&io, CAPACITY);
    store.put(COLUMN, b"cold", b"value").expect("put");
    for round in 0..MOST_TAG_SLOTS * 2 {
        let value = format!("round-{round}").into_bytes();
        store.put(COLUMN, b"hot", &value).expect("put");
        // the rest of the segment stays live, so the segment and the hot key's stale slot stand
        for number in 0..KEPT_PER_ROUND {
            store.put(COLUMN, format!("keep-{round}-{number}").as_bytes(), b"value").expect("put");
        }
        seal(&store, &io);
        assert_eq!(read(&store, b"hot"), Some(value));
    }
    let table = store.state.complete_table(COLUMN).expect("a table");
    assert_eq!(table.mark(), None);
    assert!(slots_of(&store, b"hot").len() <= MOST_TAG_SLOTS);
    store.close().expect("close");
}

// a key's rows overwritten faster than the walk reads its newest value, with the walk held
#[test]
fn overwritten_between_walks() {
    let io = SimulatedIo::new(Seed(6));
    let store = open(&io, CAPACITY);
    io.hold_jobs("reel-planner", true);
    for round in 0..MOST_TAG_SLOTS as u64 * 2 {
        let value = format!("round-{round}").into_bytes();
        store.put(COLUMN, b"hot", &value).expect("put");
        seal(&store, &io);
        assert_eq!(read(&store, b"hot"), Some(value));
    }
    io.hold_jobs("reel-planner", false);
    store.close().expect("close");
}

// a read-only view reads through the footers, with no table to fill
#[test]
fn view_reads_the_footers() {
    let io = SimulatedIo::new(Seed(7));
    let store = open(&io, CAPACITY);
    store.put(COLUMN, b"key", b"value").expect("put");
    seal(&store, &io);
    let view = ReelStore::open_read_only(io.clone(), &config(CAPACITY)).expect("open read-only");
    assert!(view.state.complete_table(COLUMN).is_none());
    assert_eq!(read(&view, b"key"), Some(b"value".to_vec()));
    view.close().expect("close");
    store.close().expect("close");
}

fn row_slots(store: &Store) -> Vec<(u32, u16, u16)> {
    let table = store.state.complete_table(COLUMN).expect("a table");
    let mut slots = Vec::new();
    for shard in 0..COMPLETE_SHARDS {
        table.visit_slots(shard, |slot| {
            if slot.is_used() {
                slots.push((slot.segment, slot.block, slot.key_tag()));
            }
        });
    }
    slots.sort_unstable();
    slots
}

fn fill_store(store: &Store, io: &SimulatedIo) -> Filled {
    let mut model = BTreeMap::new();
    for number in 0..KEYS {
        let value = format!("first-{number}").into_bytes();
        store.put(COLUMN, &key(number), &value).expect("put");
        model.insert(key(number), value);
    }
    seal(store, io);
    let mut deleted = Vec::new();
    for number in (0..KEYS).step_by(5) {
        store.delete(COLUMN, &key(number)).expect("delete");
        model.remove(&key(number));
        deleted.push(key(number));
    }
    seal(store, io);
    (model, deleted)
}

// an open from the saved image holds the same rows slot for slot, and an open after a crash holds them all, with slots of
// superseded rows its walks already counted until their segments go
#[test]
fn image_equals_rebuild() {
    let io = SimulatedIo::new(Seed(8));
    let store = open(&io, CAPACITY);
    let (model, deleted) = fill_store(&store, &io);
    let before = row_slots(&store);
    store.close().expect("close");
    assert!(io.list_directory(Path::new(ROOT)).expect("list").iter().any(|file| file.to_string_lossy().ends_with(IMAGE_NAME)));

    let store = open(&io, CAPACITY);
    assert!(!io.list_directory(Path::new(ROOT)).expect("list").iter().any(|file| file.to_string_lossy().ends_with(IMAGE_NAME)));
    assert_eq!(row_slots(&store), before);
    check(&store, &model, &deleted);
    store.flush().expect("flush");
    io.kill();
    drop(store);

    let store = open(&io, CAPACITY);
    let rebuilt = row_slots(&store);
    assert!(before.iter().all(|slot| rebuilt.contains(slot)), "a rebuild lost a slot the image held");
    check(&store, &model, &deleted);
    store.close().expect("close");
}

// an image from before later seals and rewrites speaks only for what it covers
#[test]
fn stale_image_stops_at_what_it_covers() {
    let io = SimulatedIo::new(Seed(9));
    let store = open(&io, CAPACITY);
    let (mut model, deleted) = fill_store(&store, &io);
    store.close().expect("close");
    let image = Path::new(ROOT).join(IMAGE_NAME);
    let kept = Path::new(ROOT).join("kept-image");
    io.rename_file(&image, &kept).expect("rename");

    let store = open(&io, CAPACITY);
    for number in (1..KEYS).step_by(2) {
        if !deleted.contains(&key(number)) {
            let value = format!("later-{number}").into_bytes();
            store.put(COLUMN, &key(number), &value).expect("put");
            model.insert(key(number), value);
        }
    }
    seal(&store, &io);
    store.close().expect("close");
    io.rename_file(&kept, &image).expect("rename");

    let store = open(&io, CAPACITY);
    check(&store, &model, &deleted);
    store.close().expect("close");
}

// an image that fails its sum costs a read of every row block and nothing else
#[test]
fn rotted_image_reads_the_blocks() {
    let io = SimulatedIo::new(Seed(10));
    let store = open(&io, CAPACITY);
    let (model, deleted) = fill_store(&store, &io);
    store.close().expect("close");
    let image = Path::new(ROOT).join(IMAGE_NAME);
    let file = io.open_read_only(&image).expect("open");
    let length = file.length().expect("length");
    io.close_file(file);
    // past the list of segments, or the bad list alone would send open to the blocks
    for offset in (IMAGE_HEAD_BYTES as u64 + IMAGE_LIST_ROOM..length.0).step_by(IMAGE_ROT_STEP) {
        io.rot(&image, FileOffset(offset), BitMask(0x10)).expect("rot");
    }

    let store = open(&io, CAPACITY);
    check(&store, &model, &deleted);
    store.close().expect("close");
}

// the report and the heap count the table, on top of the budget
#[test]
fn reports_the_table() {
    let io = SimulatedIo::new(Seed(11));
    let store = open(&io, CAPACITY);
    store.put(COLUMN, b"key", b"value").expect("put");
    seal(&store, &io);
    let memory = store.memory_report();
    assert!(memory.complete_tables.0 >= CAPACITY.0 * 8);
    // the table comes out of the budget, so the report's budget is the one the store was given
    assert_eq!(memory.budget, BUDGET);
    assert!(memory.heap_total().0 <= memory.budget.0);
    let mut report = MaintenanceReport::default();
    store.maintenance_report(&mut report);
    let reading = report.complete_columns.first().copied().expect("a reading");
    assert!(reading.has_table);
    assert_eq!(reading.used, RowCount(1));
    assert_eq!(reading.rows_past_capacity, RowCount(0));

    let view = ReelStore::open_read_only(io.clone(), &config(CAPACITY)).expect("open read-only");
    view.maintenance_report(&mut report);
    assert!(!report.complete_columns[0].has_table);
    view.close().expect("close");
    store.close().expect("close");
}

// two keys a table can't tell apart, and a third it can't tell from them: a tiny table puts every key of a shard in one bucket
fn colliding_keys() -> [Vec<u8>; 3] {
    let mut seen: BTreeMap<(usize, u16), Vec<Vec<u8>>> = BTreeMap::new();
    for number in 0.. {
        let name = key(number);
        let hash = hash_key(COLUMN, &name);
        let found = seen.entry((shard_of(hash), tag_of(hash))).or_default();
        found.push(name);
        if found.len() == 3 {
            return [found[0].clone(), found[1].clone(), found[2].clone()];
        }
    }
    unreachable!("keys never run out");
}

// a key whose tag matches another's reads its own value, and one never written reads as absent, for the price of a block read. Two
// keys of a tag in one block share one slot
#[test]
fn tag_collision() {
    let [left, right, never] = colliding_keys();
    let io = SimulatedIo::new(Seed(12));
    let store = open(&io, TINY_CAPACITY);
    store.put(COLUMN, &left, b"left").expect("put");
    store.put(COLUMN, &right, b"right").expect("put");
    seal(&store, &io);
    let shared = slots_of(&store, &left);
    assert_eq!(shared.len(), 1, "two keys of a tag in one block share one slot");
    assert!(shared[0].is_shared(), "{shared:?}");
    assert_eq!(read(&store, &left), Some(b"left".to_vec()));
    assert_eq!(read(&store, &right), Some(b"right".to_vec()));
    assert_eq!(read(&store, &never), None);

    store.put(COLUMN, &right, b"newer").expect("put");
    seal(&store, &io);
    assert_eq!(read(&store, &left), Some(b"left".to_vec()));
    assert_eq!(read(&store, &right), Some(b"newer".to_vec()));
    store.close().expect("close");
}

// a cue still reads the value whose slot a walk freed, through the footers
#[test]
fn cue_reads_a_freed_slot() {
    let io = SimulatedIo::new(Seed(13));
    let store = open(&io, CAPACITY);
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), b"value").expect("put");
    }
    store.put(COLUMN, b"key", b"first").expect("put");
    seal(&store, &io);
    let cue = store.cue().expect("cue");
    store.put(COLUMN, b"key", b"second").expect("put");
    seal(&store, &io);
    assert_eq!(slots_of(&store, b"key").len(), 1);

    let mut value = Vec::new();
    assert!(cue.get(COLUMN, b"key", &mut value).expect("get"));
    assert_eq!(value, b"first");
    assert_eq!(read(&store, b"key"), Some(b"second".to_vec()));
    drop(cue);
    store.close().expect("close");
}

fn charged_run(crash_at: u64) -> bool {
    let io = SimulatedIo::new(Seed(14));
    let store = open(&io, CAPACITY);
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), b"first").expect("put");
    }
    seal(&store, &io);
    store.flush().expect("flush");
    let first = *sealed_numbers(&store).first().expect("a segment");
    for number in 0..OVERWRITES {
        store.put(COLUMN, &key(number), b"second").expect("put");
    }
    store.flush().expect("flush");
    io.arm_crash(OperationCount(crash_at));
    store.seal_now();
    for _step in 0..STEPS {
        if io.is_crashed() || io.step_any().is_none() {
            break;
        }
        io.advance_clock(TICK);
    }
    let is_crashed = io.is_crashed();
    io.disarm_crash();
    io.kill();
    drop(store);

    let store = open(&io, CAPACITY);
    seal(&store, &io);
    assert_eq!(dead_rows(&store, first), OVERWRITES, "crash at {crash_at}");
    for number in 0..OVERWRITES {
        assert_eq!(read(&store, &key(number)), Some(b"second".to_vec()));
    }
    store.close().expect("close");
    is_crashed
}

// an overwrite charges the old segment once, wherever a crash lands around the seal and the walk's charge
#[test]
fn charges_once_through_a_crash() {
    let mut crashes = 0;
    for crash_at in 0..CHARGE_CRASH_POINTS {
        crashes += u64::from(charged_run(crash_at));
    }
    assert!(crashes > 0);
}

fn graves_dropped(store: &Store) -> u64 {
    let mut report = MaintenanceReport::default();
    store.maintenance_report(&mut report);
    report.graves_dropped.0
}

// a grave stays through a rewrite of its own segment while a segment still holds the older value, a crash rebuilds the table from
// every block, and the key stays deleted. The older segment's going lets the next rewrite drop the grave
#[test]
fn grave_outlives_its_older_row() {
    let io = SimulatedIo::new(Seed(15));
    let store = open(&io, CAPACITY);
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), b"cold").expect("put");
    }
    store.put(COLUMN, b"deleted", b"older").expect("put");
    seal(&store, &io);

    store.delete(COLUMN, b"deleted").expect("delete");
    for number in 0..SHORT_LIVED {
        store.put(COLUMN, format!("short-{number}").as_bytes(), b"value").expect("put");
    }
    seal(&store, &io);
    for number in 0..SHORT_LIVED {
        store.put(COLUMN, format!("short-{number}").as_bytes(), b"newer").expect("put");
    }
    seal(&store, &io);
    assert_eq!(read(&store, b"deleted"), None);
    assert_eq!(graves_dropped(&store), 0);
    store.flush().expect("flush");
    io.kill();
    drop(store);

    let store = open(&io, CAPACITY);
    settle(&io);
    assert_eq!(read(&store, b"deleted"), None);
    assert_eq!(graves_dropped(&store), 0);

    for number in 0..KEYS {
        store.put(COLUMN, &key(number), b"newer").expect("put");
    }
    seal(&store, &io);
    for number in 0..SHORT_LIVED {
        store.put(COLUMN, format!("short-{number}").as_bytes(), b"newest").expect("put");
    }
    seal(&store, &io);
    assert_eq!(read(&store, b"deleted"), None);
    assert!(graves_dropped(&store) > 0);
    store.close().expect("close");
}

fn walk_all(store: &Store, now: Nanoseconds) {
    let mut walk = WalkJob::new(Arc::clone(&store.state), 0);
    for _step in 0..STEPS {
        if walk.step_unit(now) == JobStep::Idle {
            return;
        }
    }
    panic!("the walk never finished");
}

// a rewrite's output has the higher number and the older row, so the highest place decides, and segment order would read the old value
#[test]
fn rewrite_beside_a_newer_original() {
    let io = SimulatedIo::new(Seed(16));
    let store = open(&io, CAPACITY);
    let volume = &store.state.volumes[0];
    io.hold_jobs("reel-planner", true);
    volume.holds.compaction.store(true, Ordering::Release);
    store.put(COLUMN, b"key", b"older").expect("put");
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), b"value").expect("put");
    }
    seal(&store, &io);
    walk_all(&store, Nanoseconds(0));
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), b"newer").expect("put");
    }
    seal(&store, &io);
    walk_all(&store, Nanoseconds(0));
    // the newest row's segment owes its walk, so the rewrite can't see it replaces the older row
    store.put(COLUMN, b"key", b"newest").expect("put");
    seal(&store, &io);
    let newest = *sealed_numbers(&store).last().expect("a segment");

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    while !job.rewrite.start() {
        assert!(job.rewrite.has_looks_left(), "no merge to run");
    }
    while job.rewrite.phase != RewritePhase::Installing {
        job.rewrite.step();
    }
    job.rewrite.install().expect("install");
    drop(job);

    let slots = slots_of(&store, b"key");
    assert!(slots.iter().any(|slot| slot.segment_number() > newest), "{slots:?}");
    assert_eq!(read(&store, b"key"), Some(b"newest".to_vec()));
    volume.holds.compaction.store(false, Ordering::Release);
    io.hold_jobs("reel-planner", false);
    store.close().expect("close");
}

// a row block a rebuild can't read leaves its keys with no slot, so they read as damage, never as absent, and the walk goes on
#[test]
fn unread_block_reads_as_damage() {
    let io = SimulatedIo::new(Seed(17));
    let store = open(&io, CAPACITY);
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), b"value").expect("put");
    }
    seal(&store, &io);
    let segment = *sealed_numbers(&store).first().expect("a segment");
    let volume = &store.state.volumes[0];
    let (line, _replacement) = volume.table.block_line(segment, 0).expect("a block");
    let path = data_path(Path::new(ROOT), segment);
    store.flush().expect("flush");
    io.kill();
    drop(store);
    io.rot(&path, FileOffset(line.block_at().0 + 2), BitMask(1)).expect("rot");

    let store = open(&io, CAPACITY);
    let mut value = Vec::new();
    assert!(matches!(store.get(COLUMN, &key(0), &mut value), Err(StoreError::Corruption { .. })));
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), b"newer").expect("put");
    }
    seal(&store, &io);
    assert_eq!(read(&store, &key(0)), Some(b"newer".to_vec()));
    store.close().expect("close");
}

// a cue reads a version whose slot many overwrites since have freed, through the footers
#[test]
fn cue_reads_past_overwrites() {
    let io = SimulatedIo::new(Seed(18));
    let store = open(&io, CAPACITY);
    for number in 0..KEPT_PER_ROUND {
        store.put(COLUMN, format!("keep-{number}").as_bytes(), b"value").expect("put");
    }
    store.put(COLUMN, b"hot", b"first").expect("put");
    seal(&store, &io);
    let cue = store.cue().expect("cue");
    for round in 0..MOST_TAG_SLOTS * 2 {
        store.put(COLUMN, b"hot", format!("round-{round}").as_bytes()).expect("put");
        for number in 0..KEPT_PER_ROUND {
            store.put(COLUMN, format!("keep-{round}-{number}").as_bytes(), b"value").expect("put");
        }
        seal(&store, &io);
    }
    assert_eq!(slots_of(&store, b"hot").len(), 1);

    let mut value = Vec::new();
    assert!(cue.get(COLUMN, b"hot", &mut value).expect("get"));
    assert_eq!(value, b"first");
    drop(cue);
    store.close().expect("close");
}

// a get that took its point before an overwrite still reads what stood then, though the walk freed that row's slot since
#[test]
fn read_behind_a_freed_slot() {
    let io = SimulatedIo::new(Seed(19));
    let store = open(&io, CAPACITY);
    for number in 0..KEPT_PER_ROUND {
        store.put(COLUMN, format!("keep-{number}").as_bytes(), b"value").expect("put");
    }
    store.put(COLUMN, b"key", b"first").expect("put");
    seal(&store, &io);
    let latest = store.state.latest_read(COLUMN, b"key").expect("read");
    store.put(COLUMN, b"key", b"second").expect("put");
    seal(&store, &io);
    assert_eq!(slots_of(&store, b"key").len(), 1);

    let mut value = Vec::new();
    let answer = crate::latest::answer(&store, &latest, RecordWant::Whole, &mut value).expect("no job crossed it");
    assert!(answer.expect("get"));
    assert_eq!(value, b"first");
    store.close().expect("close");
}

// a hot key's full tag costs that tag alone: a crash rebuilds the table with no stale marks, and the column takes no mark
#[test]
fn hot_key_leaves_no_mark_through_a_crash() {
    let io = SimulatedIo::new(Seed(20));
    let store = open(&io, CAPACITY);
    let mut newest = Vec::new();
    for round in 0..MOST_TAG_SLOTS * 2 {
        newest = format!("round-{round}").into_bytes();
        store.put(COLUMN, b"hot", &newest).expect("put");
        for number in 0..KEPT_PER_ROUND {
            store.put(COLUMN, format!("keep-{round}-{number}").as_bytes(), b"value").expect("put");
        }
        seal(&store, &io);
    }
    let table = store.state.complete_table(COLUMN).expect("a table");
    assert_eq!(table.mark(), None, "running");
    assert_eq!(read(&store, b"hot").as_ref(), Some(&newest));
    store.flush().expect("flush");
    io.kill();
    drop(store);

    let store = open(&io, CAPACITY);
    let table = store.state.complete_table(COLUMN).expect("a table");
    assert_eq!(table.mark(), None, "after a crash");
    assert_eq!(read(&store, b"hot").as_ref(), Some(&newest));
    store.close().expect("close");
}

// the same with the walk held behind every seal, and a saved image keeps the sticky slot
#[test]
fn hot_key_leaves_no_mark_with_a_held_walk() {
    let io = SimulatedIo::new(Seed(21));
    let store = open(&io, CAPACITY);
    io.hold_jobs("reel-planner", true);
    let mut newest = Vec::new();
    for round in 0..MOST_TAG_SLOTS as u64 * 2 {
        newest = format!("round-{round}").into_bytes();
        store.put(COLUMN, b"hot", &newest).expect("put");
        seal(&store, &io);
    }
    io.hold_jobs("reel-planner", false);
    settle(&io);
    let table = store.state.complete_table(COLUMN).expect("a table");
    assert_eq!(table.mark(), None, "after the walk caught up");
    assert_eq!(read(&store, b"hot").as_ref(), Some(&newest));
    store.close().expect("close");

    let store = open(&io, CAPACITY);
    assert!(slots_of(&store, b"hot").iter().any(CompleteSlot::is_sticky));
    assert_eq!(read(&store, b"hot").as_ref(), Some(&newest));
    store.close().expect("close");
}

// a hot key holds one slot however often it is written, and every row it replaced counts dead
#[test]
fn hot_key_charges_every_version() {
    let io = SimulatedIo::new(Seed(22));
    let store = open(&io, CAPACITY);
    for round in 0..MOST_TAG_SLOTS as u64 * 2 {
        store.put(COLUMN, b"hot", format!("round-{round}").as_bytes()).expect("put");
        for number in 0..KEPT_PER_ROUND {
            store.put(COLUMN, format!("keep-{round}-{number}").as_bytes(), b"value").expect("put");
        }
        seal(&store, &io);
    }
    assert_eq!(slots_of(&store, b"hot").len(), 1);
    let dead: u64 = sealed_numbers(&store).into_iter().map(|segment| dead_rows(&store, segment)).sum();
    assert_eq!(dead, MOST_TAG_SLOTS as u64 * 2 - 1);
    store.close().expect("close");
}

// rows sealed while a rewrite writes land in the buckets of its output's slots, which point at a segment the table has yet to take, so
// they stay. Tiny buckets make every insert visit them
#[test]
fn output_slots_outlast_a_rewrite_in_flight() {
    let io = SimulatedIo::new(Seed(23));
    let store = open(&io, RowCount(OUTPUT_ROOM));
    let volume = &store.state.volumes[0];
    volume.holds.compaction.store(true, Ordering::Release);
    for number in 0..OUTPUT_KEYS {
        store.put(COLUMN, &key(number), b"first").expect("put");
    }
    seal(&store, &io);
    for number in (0..OUTPUT_KEYS).step_by(3) {
        store.put(COLUMN, &key(number), b"second").expect("put");
    }
    for number in (1..OUTPUT_KEYS).step_by(3) {
        store.put(COLUMN, &key(number), b"second").expect("put");
    }
    seal(&store, &io);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    while !job.rewrite.start() {
        assert!(job.rewrite.has_looks_left(), "no merge to run");
    }
    while job.rewrite.phase != RewritePhase::Copying {
        job.rewrite.step();
    }
    // part of the rows are in the footer, and their slots stand for a segment that isn't in the table
    for _step in 0..OUTPUT_STEPS {
        job.rewrite.step();
    }
    assert!(job.rewrite.is_running());
    for number in 0..OUTPUT_KEYS {
        store.put(COLUMN, format!("later-{number}").as_bytes(), b"value").expect("put");
    }
    store.seal_now();
    settle(&io);
    while job.rewrite.is_running() {
        job.rewrite.step();
    }
    assert!(volume.segments_rewritten.load(Ordering::Acquire) > 0);
    let table = store.state.complete_table(COLUMN).expect("a table");
    assert_eq!(table.mark(), None);
    for number in 0..OUTPUT_KEYS {
        let expected: &[u8] = if number % 3 == 2 { b"first" } else { b"second" };
        assert_eq!(read(&store, &key(number)).as_deref(), Some(expected), "key {number}");
    }
    volume.holds.compaction.store(false, Ordering::Release);
    store.close().expect("close");
}

// the writer pauses once a seal lands, so the hot window closes and the rewrites hot ingest put off can start
fn cool(store: &Store, io: &SimulatedIo) {
    let until = store.state.quiet_at(io.clock_reading());
    io.advance_clock(Nanoseconds(until.0.saturating_sub(io.clock_reading().0)));
    settle(io);
}

// a table that ran out of room clears its mark once the walk frees the slots of overwritten rows, with no open between, and every
// key reads its newest value through the table again
#[test]
fn mark_clears_once_room_returns() {
    let io = SimulatedIo::new(Seed(31));
    let store = open(&io, RowCount(2 * KEYS));
    let volume = &store.state.volumes[0];
    let table = store.state.complete_table(COLUMN).expect("a table");
    volume.holds.walk.store(true, Ordering::Release);
    for round in 0..3 {
        for number in 0..KEYS {
            store.put(COLUMN, &key(number), format!("round-{round}").as_bytes()).expect("put");
        }
        seal(&store, &io);
    }
    assert!(table.mark().is_some(), "three versions of every key fit a table sized for two");

    volume.holds.walk.store(false, Ordering::Release);
    store.state.ask_compaction();
    for _step in 0..KEYS {
        if table.mark().is_none() {
            break;
        }
        io.advance_clock(Nanoseconds(1_000_000));
        settle(&io);
    }
    assert_eq!(table.mark(), None, "the mark stood with room back");
    assert_eq!(table.past_capacity(), RowCount(0));
    assert_eq!(table.used_slots(), RowCount(KEYS), "a refill gave superseded rows their slots back");
    for number in 0..KEYS {
        assert_eq!(read(&store, &key(number)), Some(b"round-2".to_vec()));
    }
    store.close().expect("close");
}

// scans stepped between job steps see every kept key once and nothing a range delete or a grave hides, while seals, walks, merges
// and cover settles run under them
#[test]
fn scans_between_job_steps() {
    for seed in 0..INTERLEAVE_SEEDS {
        let io = SimulatedIo::new(Seed(seed));
        let mut config = config(CAPACITY);
        config.threads.maintenance_workers = Some(INTERLEAVE_WORKERS);
        config.memory_budget = ByteCount(256 * 1024 * 1024);
        let store = ReelStore::open(io.clone(), &config).expect("open");
        let mut random = SeededRandom::new(Seed(seed));
        let kept = |number: u64| format!("kept-{number:04}").into_bytes();
        let spanned = |number: u64| format!("span-{number:04}").into_bytes();
        let gone = |number: u64| format!("gone-{number:04}").into_bytes();
        for number in 0..INTERLEAVE_KEYS {
            store.put(COLUMN, &kept(number), b"first").expect("put");
            store.put(COLUMN, &spanned(number), b"gone").expect("put");
            store.put(COLUMN, &gone(number), b"gone").expect("put");
        }
        seal(&store, &io);
        for number in 0..INTERLEAVE_KEYS {
            store.delete(COLUMN, &gone(number)).expect("delete");
        }
        store.delete_range(COLUMN, &spanned(0), &spanned(INTERLEAVE_KEYS)).expect("delete");
        store.flush().expect("flush");
        let cue = store.cue().expect("cue");
        for round in 0..INTERLEAVE_ROUNDS {
            for number in 0..INTERLEAVE_KEYS {
                store.put(COLUMN, &kept(number), format!("round-{round}").as_bytes()).expect("put");
            }
            store.seal_now();
            let mut scan = if round % 2 == 0 {
                store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan")
            } else {
                cue.iter_from(COLUMN, b"", Direction::Ascending).expect("scan")
            };
            let (mut name, mut value) = (Vec::new(), Vec::new());
            let mut next = 0;
            loop {
                for _step in 0..random.below(INTERLEAVE_STEPS) {
                    if io.step_any().is_none() {
                        io.advance_clock(TICK);
                    }
                }
                match scan.next_row(&mut name, &mut value).expect("step") {
                    ScanStep::End => break,
                    ScanStep::Corrupt => panic!("seed {seed}: {name:?} came corrupt"),
                    ScanStep::Found => {},
                }
                assert_eq!(name, kept(next), "seed {seed} round {round}");
                next += 1;
            }
            assert_eq!(next, INTERLEAVE_KEYS, "seed {seed} round {round}");
        }
        drop(cue);
        let volume = &store.state.volumes[0];
        assert!(volume.segments_rewritten.load(Ordering::Acquire) + volume.segments_unlinked.load(Ordering::Acquire) > 0, "seed {seed}: no job ran under the scans");
        settle(&io);
        store.close().expect("close");
    }
}

// random overwrites and deletes over many seals leave the table one slot a key once the walk and the rewrites settle, and a
// reopen finds the same count
#[test]
fn churn_settles_to_a_slot_a_key() {
    let io = SimulatedIo::new(Seed(41));
    let mut config = config(CAPACITY);
    config.threads.maintenance_workers = Some(INTERLEAVE_WORKERS);
    config.memory_budget = ByteCount(256 * 1024 * 1024);
    let store = ReelStore::open(io.clone(), &config).expect("open");
    let mut random = SeededRandom::new(Seed(41));
    for round in 0..CHURN_ROUNDS {
        for _write in 0..KEYS {
            let number = random.below(KEYS);
            if random.below(CHURN_DELETE_ONE_IN) == 0 {
                store.delete(COLUMN, &key(number)).expect("delete");
            } else {
                store.put(COLUMN, &key(number), format!("round-{round}").as_bytes()).expect("put");
            }
        }
        seal(&store, &io);
    }
    cool(&store, &io);
    let table = store.state.complete_table(COLUMN).expect("a table");
    let live = (0..KEYS).filter(|number| read(&store, &key(*number)).is_some()).count() as u64;
    let used = table.used_slots().0;
    assert!(used <= KEYS, "{used} slots for {live} live keys of {KEYS}");
    store.close().expect("close");

    let store = ReelStore::open(io.clone(), &config).expect("open");
    let reopened = store.state.complete_table(COLUMN).expect("a table").used_slots().0;
    assert!(reopened <= KEYS, "{reopened} slots after a reopen for {live} live keys of {KEYS}");
    store.close().expect("close");
}

// a refill waits while a seal at or past the mark or a merge output is in flight, since their rows that found no slot aren't
// listed yet and a refill would clear the mark over them
#[test]
fn refill_waits_for_segments_in_flight() {
    let io = SimulatedIo::new(Seed(32));
    let store = open(&io, RowCount(2 * KEYS));
    let volume = &store.state.volumes[0];
    let table = store.state.complete_table(COLUMN).expect("a table");
    volume.holds.walk.store(true, Ordering::Release);
    volume.holds.compaction.store(true, Ordering::Release);
    for round in 0..3 {
        for number in 0..KEYS {
            store.put(COLUMN, &key(number), format!("round-{round}").as_bytes()).expect("put");
        }
        seal(&store, &io);
    }
    let mark = table.mark().expect("three versions of every key fit a table sized for two");
    // an empty table has room, so only what's in flight holds the refill back
    table.clear();

    volume.sealing.store(mark.0, Ordering::Release);
    assert!(store.state.start_refill(0, u64::MAX).is_none(), "a refill started beside a seal at the mark");
    volume.sealing.store(NO_SEAL, Ordering::Release);
    volume.lanes[0].output_floor.store(mark.0 + 1, Ordering::Release);
    assert!(store.state.start_refill(0, u64::MAX).is_none(), "a refill started beside a merge output");
    volume.lanes[0].output_floor.store(NO_SEAL, Ordering::Release);
    assert!(store.state.start_refill(0, u64::MAX).is_some(), "a refill waited with nothing in flight");
    volume.holds.compaction.store(false, Ordering::Release);
    volume.holds.walk.store(false, Ordering::Release);
    store.close().expect("close");
}

const REFILL_ROUNDS: u64 = 12;
const REFILL_KEYS: u64 = 8_000;
const MOST_REFILLS_ALONE: u64 = 3;

// a refill shares the planner with the walk, so walk blocks land between refill steps while a long refill runs, and the mark
// clears once the walk has freed the slots
#[test]
fn walk_runs_beside_a_refill() {
    let io = SimulatedIo::new(Seed(33));
    let store = open(&io, RowCount(2 * REFILL_KEYS));
    let volume = &store.state.volumes[0];
    let table = store.state.complete_table(COLUMN).expect("a table");
    volume.holds.walk.store(true, Ordering::Release);
    volume.holds.compaction.store(true, Ordering::Release);
    for round in 0..REFILL_ROUNDS {
        for number in 0..REFILL_KEYS {
            store.put(COLUMN, &key(number), format!("round-{round}").as_bytes()).expect("put");
        }
        seal(&store, &io);
    }
    assert!(table.mark().is_some(), "{REFILL_ROUNDS} versions of every key fit a table sized for two");
    volume.holds.compaction.store(false, Ordering::Release);
    volume.holds.walk.store(false, Ordering::Release);
    let refill = UNIT_KINDS.iter().position(|kind| *kind == UnitKind::SlotRefill).expect("a refill unit");
    store.state.ask_compaction();
    let mut report = MaintenanceReport::new();
    let (mut refills, mut walked, mut alone, mut most_alone, mut refill_steps) = (0, 0, 0, 0, 0);
    for _step in 0..STEPS {
        if io.step_any().is_none() {
            io.advance_clock(TICK);
            if io.step_any().is_none() {
                break;
            }
        }
        store.maintenance_report(&mut report);
        let (now_refills, now_walked) = (report.longest_units[refill].work_sum, report.volumes[0].walk_blocks);
        if now_refills != refills {
            refill_steps += 1;
            alone = if now_walked == walked { alone + 1 } else { 0 };
            most_alone = most_alone.max(alone);
            walked = now_walked;
        }
        refills = now_refills;
    }
    assert!(refill_steps > MOST_REFILLS_ALONE, "only {refill_steps} refill steps ran, mark {:?}, used {:?}, past {:?}, walked {walked}, unwalked {}", table.mark(), table.used_slots(), table.past_capacity(), store.state.pending_walks(0));
    assert!(most_alone <= MOST_REFILLS_ALONE, "{most_alone} refill steps ran in a row with no walk block between");
    assert!(report.longest_walkless_steps <= MOST_WALKLESS_STEPS, "the planner held the walk back {} steps", report.longest_walkless_steps);
    assert_eq!(table.mark(), None, "the mark stood once the walk had freed the slots");
    for number in 0..REFILL_KEYS {
        assert_eq!(read(&store, &key(number)), Some(format!("round-{}", REFILL_ROUNDS - 1).into_bytes()));
    }
    store.close().expect("close");
}

// (d) the cue sim on a complete column, which slots every sealed row and applies covers when it reads: K in a segment of its
// own, a cue, a range delete written into another open segment and left there, K's segment sealed alone. The cue reads K by get
// and scan, and the current point doesn't. A guard against the table taking a rule that withholds rows under an open cover
#[test]
fn a_cue_reads_a_value_under_an_open_range_delete_on_a_complete_column() {
    let io = SimulatedIo::new(Seed(96));
    let store = open(&io, CAPACITY);
    let name = b"key-k".to_vec();
    store.put_marked(COLUMN, &name, b"live at the cue", reel_api::units::DeathMark(1_000_000)).expect("put");
    let cue = store.cue().expect("cue");
    store.delete_range(COLUMN, b"", &[0xFF; 8]).expect("delete range");
    let volume = &store.state.volumes[0];
    {
        let mut claims = hold(&volume.claims);
        let at = claims.open.iter().position(|open| open.ceiling.is_some() && open.row_count.0 > 0).expect("K's segment");
        store.state.seal_at(&mut claims, volume, at);
    }
    settle(&io);
    let point = cue.point_of(COLUMN).expect("point");
    assert!(!volume.may_hide_under_sealed(point), "the setup sends the cue to the footers");
    let mut value = Vec::new();
    assert!(cue.get(COLUMN, &name, &mut value).expect("get at the cue"), "the cue lost K on a complete column");
    assert_eq!(value, b"live at the cue");
    let mut scan = cue.iter_from(COLUMN, &name, Direction::Ascending).expect("scan");
    let (mut key, mut row) = (Vec::new(), Vec::new());
    assert_eq!(scan.next_row(&mut key, &mut row).expect("step"), ScanStep::Found);
    assert_eq!((key.as_slice(), row.as_slice()), (name.as_slice(), &b"live at the cue"[..]));
    drop(scan);
    assert!(!store.get(COLUMN, &name, &mut value).expect("get"), "the range delete didn't hide K at the current point");
    drop(cue);
    store.close().expect("close");
}

// random puts, deletes and range deletes over seals, reopens and kills read as a model does, with the table's image taken at
// each close and read or refused at each open
#[test]
fn churn_matches_the_model() {
    let names: Vec<Vec<u8>> = (0..CHURN_KEYS).map(key).collect();
    for seed in seeds(0..CHURN_SEEDS) {
        assert_eq!(churn(seed, CAPACITY, &names), (false, true), "seed {seed}");
    }
}

// the same churn with every key in one shard of the table, so its buckets fill and its keys share tags. The shard holds a 64th
// of the slots, so a seed can mark the table and keep the mark, and reads still match the model
#[test]
fn churn_in_one_shard_matches_the_model() {
    let names = one_shard_keys();
    let (mut marked, mut cleared) = (0, 0);
    for seed in seeds(0..CHURN_SEEDS) {
        let (was_marked, is_cleared) = churn(seed, CAPACITY, &names);
        marked += u64::from(was_marked);
        cleared += u64::from(was_marked && is_cleared);
    }
    println!("complete churn in one shard: {marked} seeds marked, {cleared} cleared");
}

// the seed whose one-shard churn read an older value once a free took a slot two keys of a tag needed
#[test]
fn churn_in_one_shard_keeps_seed_601271() {
    let (_was_marked, _is_cleared) = churn(601_271, CAPACITY, &one_shard_keys());
}

// the same churn on a table with room for fewer rows than a seed writes, so seeds go past capacity, and a range delete of half
// the keys halfway through lets the walk and the refill free room
#[test]
fn churn_past_capacity_matches_the_model() {
    let names: Vec<Vec<u8>> = (0..CHURN_KEYS).map(key).collect();
    let (mut marked, mut cleared) = (0, 0);
    for seed in seeds(0..CHURN_SEEDS) {
        let (was_marked, is_cleared) = churn(seed, CHURN_CAPACITY, &names);
        marked += u64::from(was_marked);
        cleared += u64::from(was_marked && is_cleared);
    }
    println!("complete churn past capacity: {marked} seeds marked, {cleared} cleared");
    assert!(marked > 0, "no seed went past capacity");
}

// keys whose hashes all fall in the table's first shard
fn one_shard_keys() -> Vec<Vec<u8>> {
    (0..).map(key).filter(|name| shard_of(hash_key(COLUMN, name)) == 0).take(CHURN_KEYS as usize).collect()
}

// run one seed's churn over these keys, and say whether the table was ever marked and whether it ends with no mark. A marked
// put goes to a segment of its own, so a key's older row can seal after its grave
fn churn(seed: u64, capacity: RowCount, names: &[Vec<u8>]) -> (bool, bool) {
    let is_marked = |store: &Store| store.state.complete_table(COLUMN).expect("a table").mark().is_some();
    let io = SimulatedIo::new(Seed(300 + seed));
    let mut random = SeededRandom::new(Seed(400 + seed));
    let mut store = open(&io, capacity);
    let mut model = Model::new();
    let mut sorted_names = names.to_vec();
    sorted_names.sort();
    let mut was_marked = false;
    for round in 0..MODEL_ROUNDS {
        if round == MODEL_ROUNDS / 2 {
            let (start, end) = (sorted_names[0].clone(), sorted_names[sorted_names.len() / 2].clone());
            store.delete_range(COLUMN, &start, &end).expect("delete range");
            model.retain(|name, _value| *name < start || *name >= end);
        }
        for write in 0..CHURN_WRITES {
            let name = names[random.below(CHURN_KEYS) as usize].clone();
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
        match random.below(3) {
            0 => seal(&store, &io),
            1 => {
                store.close().expect("close");
                drop(store);
                store = open(&io, capacity);
            },
            _ => {
                store.flush().expect("flush");
                io.kill();
                drop(store);
                store = open(&io, capacity);
            },
        }
        was_marked |= is_marked(&store);
        check_model(&store, &model, names);
        check_scans(&store, &model, &sorted_names, &mut random);
    }
    seal(&store, &io);
    // the walk frees superseded slots and the refill clears the mark, over as many quiet spells as they take
    for _round in 0..CLEAR_ROUNDS {
        if !is_marked(&store) {
            break;
        }
        io.advance_clock(Nanoseconds(1_000_000_000));
        cool(&store, &io);
    }
    was_marked |= is_marked(&store);
    check_model(&store, &model, names);
    check_scans(&store, &model, &sorted_names, &mut random);
    let is_cleared = !is_marked(&store);
    store.close().expect("close");
    (was_marked, is_cleared)
}

// every key reads what the model holds, and nothing for a key the model lacks
fn check_model(store: &Store, model: &Model, names: &[Vec<u8>]) {
    for name in names {
        assert_eq!(read(store, name).as_ref(), model.get(name), "{name:?}");
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

// whole walks both ways, and walks from stored keys with and without a told count, against the model
fn check_scans(store: &Store, model: &Model, names: &[Vec<u8>], random: &mut SeededRandom) {
    let forward: Rows = model.iter().map(|(key, value)| (key.clone(), value.clone())).collect();
    let stored: Vec<&Vec<u8>> = forward.iter().map(|(name, _value)| name).collect();
    assert_eq!(first_rows(store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan"), usize::MAX), forward);
    let mut backward = forward.clone();
    backward.reverse();
    let top = vec![0xFF; 65];
    assert_eq!(first_rows(store.iter_from(COLUMN, &top, Direction::Descending).expect("scan"), usize::MAX), backward);
    for told in [None, Some(100), Some(256), Some(300)] {
        let most = told.unwrap_or(SCAN_TAKE) + SCAN_TAKE;
        for direction in [Direction::Ascending, Direction::Descending] {
            // a start among the stored keys with the rows ahead of it, or any key while the model is too small
            let start = match direction {
                _ if stored.len() <= most => names[random.below(names.len() as u64) as usize].clone(),
                Direction::Ascending => stored[random.below((stored.len() - most) as u64) as usize].clone(),
                Direction::Descending => stored[most + random.below((stored.len() - most) as u64) as usize].clone(),
            };
            let expected: Rows = match direction {
                Direction::Ascending => forward.iter().filter(|(name, _value)| *name >= start).take(most).cloned().collect(),
                Direction::Descending => backward.iter().filter(|(name, _value)| *name <= start).take(most).cloned().collect(),
            };
            assert!(stored.len() <= most || expected.len() == most, "a start with too few rows ahead");
            let range = ScanRange { wanted: told.map(|told| RowCount(told as u64)), ..from_key(COLUMN, &start, direction) };
            let found = first_rows(store.iter(&range).expect("scan"), most);
            assert_eq!(found, expected, "told {told:?} {direction:?} from {start:?}");
        }
    }
}

// keys of one shard whose tags differ from a tag, to fill that shard's slots
fn fillers(shard: usize, tag: u16) -> impl Iterator<Item = Vec<u8>> {
    (1_000_000..).map(key).filter(move |name| {
        let hash = hash_key(COLUMN, name);
        shard_of(hash) == shard && tag_of(hash) != tag
    })
}

// Two keys of one tag share one block's slot when the second finds no room, and a walk that supersedes the first keeps the slot
// for the second. Before shared slots the second key's give failed, a refill took the first key's identical slot for the second's
// and cleared the mark, and the walk then freed the only slot. With the walk held, the refill runs between the first key's newer
// seal and its walk
fn shared_slot_outlives_the_other_keys_free(is_walk_held: bool) {
    let [first, second, _third] = colliding_keys();
    let hash = hash_key(COLUMN, &first);
    let (shard, tag) = (shard_of(hash), tag_of(hash));
    let io = SimulatedIo::new(Seed(196));
    let store = open(&io, TINY_CAPACITY);
    let volume = &store.state.volumes[0];
    let table = store.state.complete_table(COLUMN).expect("a table");
    volume.holds.compaction.store(true, Ordering::Release);
    // fill the shard until one slot is left
    for filler in fillers(shard, tag) {
        if table.free_slots_in(shard) == 1 {
            break;
        }
        store.put(COLUMN, &filler, b"filler").expect("put");
        seal(&store, &io);
    }
    assert_eq!(table.free_slots_in(shard), 1);
    assert_eq!(table.mark(), None, "a filler found no room");
    store.put(COLUMN, &first, b"first").expect("put");
    store.put(COLUMN, &second, b"second").expect("put");
    seal(&store, &io);
    let slots = slots_of(&store, &second);
    assert_eq!(slots.len(), 1, "the second key takes the first key's slot in their block: {slots:?}");
    assert!(slots[0].is_shared(), "{slots:?}");
    assert_eq!(table.mark(), None, "the second key found its slot");
    volume.holds.walk.store(is_walk_held, Ordering::Release);
    store.put(COLUMN, &first, b"newer").expect("put");
    seal(&store, &io);
    store.state.ask_compaction();
    for _round in 0..20 {
        io.advance_clock(Nanoseconds(1_000_000_000));
        cool(&store, &io);
    }
    volume.holds.walk.store(false, Ordering::Release);
    for _round in 0..20 {
        io.advance_clock(Nanoseconds(1_000_000_000));
        cool(&store, &io);
    }
    assert_eq!(read(&store, &first), Some(b"newer".to_vec()));
    assert_eq!(read(&store, &second), Some(b"second".to_vec()), "the second key lost its slot");
    assert!(slots_of(&store, &second).contains(&slots[0]), "the walk freed a slot the second key needs");
    store.close().expect("close");
}

#[test]
fn a_shared_slot_outlives_the_other_keys_free() {
    shared_slot_outlives_the_other_keys_free(false);
}

#[test]
fn a_shared_slot_outlives_a_free_after_a_refill() {
    shared_slot_outlives_the_other_keys_free(true);
}

// three keys of one tag in one block: some slot points at the block while any of them has its newest row there, shared or a copy
// in homes that never meet, and none does once the last of them is superseded
#[test]
fn a_shared_slot_goes_with_the_last_of_its_keys() {
    let keys = colliding_keys();
    let io = SimulatedIo::new(Seed(197));
    let store = open(&io, CAPACITY);
    store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
    for name in &keys {
        store.put(COLUMN, name, b"first").expect("put");
    }
    seal(&store, &io);
    let first = slots_of(&store, &keys[0]);
    let (segment, block) = (first[0].segment, first[0].block);
    let points_at_block = |name: &[u8]| slots_of(&store, name).iter().any(|slot| slot.segment == segment && slot.block == block);
    for (at, name) in keys.iter().enumerate() {
        store.put(COLUMN, name, b"newer").expect("put");
        seal(&store, &io);
        cool(&store, &io);
        for (other, name) in keys.iter().enumerate() {
            let value: &[u8] = if other <= at { b"newer" } else { b"first" };
            assert_eq!(read(&store, name), Some(value.to_vec()), "key {other} after {at} were superseded");
        }
        let is_pointed = keys.iter().any(|name| points_at_block(name));
        assert_eq!(is_pointed, at + 1 < keys.len(), "after {} of {} keys superseded", at + 1, keys.len());
    }
    store.close().expect("close");
}

// two keys of one tag in one block superseded in one seal, in both orders: the walk frees their shared slot once and both read
// their newest rows
#[test]
fn a_shared_slot_goes_once_when_both_keys_go_together() {
    for is_reversed in [false, true] {
        let [mut first, mut second, _third] = colliding_keys();
        if is_reversed {
            std::mem::swap(&mut first, &mut second);
        }
        let io = SimulatedIo::new(Seed(198));
        let store = open(&io, CAPACITY);
        store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
        store.put(COLUMN, &first, b"first").expect("put");
        store.put(COLUMN, &second, b"first").expect("put");
        seal(&store, &io);
        let shared = slots_of(&store, &first);
        assert_eq!(shared.len(), 1, "{shared:?}");
        store.put(COLUMN, &first, b"newer").expect("put");
        store.put(COLUMN, &second, b"newer").expect("put");
        seal(&store, &io);
        cool(&store, &io);
        assert_eq!(read(&store, &first), Some(b"newer".to_vec()));
        assert_eq!(read(&store, &second), Some(b"newer".to_vec()));
        assert!(!slots_of(&store, &first).contains(&shared[0]), "reversed {is_reversed}: the superseded rows kept their slot");
        store.close().expect("close");
    }
}

// A row freed twice never takes another key's slot. The first key's slot sits in its own homes and the second key's identical
// slot in the run, which the first key reads too, and two frees of the first key's row leave the second key's slot standing
#[test]
fn a_second_free_of_a_row_keeps_another_keys_run_slot() {
    let io = SimulatedIo::new(Seed(199));
    let store = open(&io, CAPACITY);
    let volume = &store.state.volumes[0];
    let table = store.state.complete_table(COLUMN).expect("a table");
    volume.holds.compaction.store(true, Ordering::Release);
    // two keys of one shard and tag whose homes differ
    let mut seen = HomesSeen::new();
    let (first, second) = (0..)
        .map(key)
        .find_map(|name| {
            let hash = hash_key(COLUMN, &name);
            let home = table.shards.read(shard_of(hash)).homes(hash).first;
            let found = seen.entry((shard_of(hash), tag_of(hash))).or_default();
            let other = found.iter().find(|(_other, other_home)| *other_home != home).map(|(other, _home)| other.clone());
            found.push((name.clone(), home));
            other.map(|other| (other, name))
        })
        .expect("keys never run out");
    let (first_hash, second_hash) = (hash_key(COLUMN, &first), hash_key(COLUMN, &second));
    store.put(COLUMN, &first, b"first").expect("put");
    store.put(COLUMN, &second, b"second").expect("put");
    seal(&store, &io);
    let slot = slots_of(&store, &second)[0];
    let (segment, block) = (slot.segment_number(), slot.block);
    // the second key's slot moves to the run, where the first key's frees can reach it
    table.free_superseded(second_hash, segment, block, (table.shares(), false));
    assert!(table.shards.write(shard_of(second_hash)).fill_overflow(CompleteSlot { segment: slot.segment, block, tag: tag_of(second_hash) }));
    let (entry, _replacement) = volume.table.block_line(segment, u32::from(block)).expect("a block");
    store.state.free_superseded_slot(volume, COLUMN, first_hash, segment, entry.block_offset);
    store.state.free_superseded_slot(volume, COLUMN, first_hash, segment, entry.block_offset);
    assert_eq!(read(&store, &second), Some(b"second".to_vec()), "a second free of the first key's row took the second key's slot");
    assert!(table.frees_kept() > 0);
    store.close().expect("close");
}
