//! Sorted columns: an index holds each sealed key's newest place, so a get reads no footers, through seals, merges, covers and opens

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::seed_sets::seeds;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::compact_job::CompactJob;
use reel::engine::open_segment::SegmentPhase;
use reel::engine::reel_store::ReelStore;
use reel::engine::rewrite_job::RewritePhase;
use reel::guard::hold;
use reel::index::sorted_keys::KeyEntry;
use reel::record::codec::Codec;
use reel::engine::store_scan::StoreScan;
use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::direction::Direction;
use reel_api::scan::ScanStep;
use reel_api::maintenance_report::MaintenanceReport;
use reel::segment::segment_tail::SegmentNumber;
use reel_api::units::{ByteCount, DeathMark, Nanoseconds, RowCount, VolumeId, WritePlace};
use reel_api::write_batch::WriteBatch;
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
const COVER_ONE_IN: u64 = 40;
const MARK: DeathMark = DeathMark(1_000_000);
const SCAN_STARTS: u64 = 12;
const WANTED_ROWS: u64 = 10;
const AHEAD: usize = 500;
const SCAN_TAKE: usize = 40;
const MERGE_KEYS: u64 = 15_000;
const CUE_ROUNDS: u64 = 3;
const COVERED: usize = 40;
const LATE_KEYS: u64 = 100_000;
const SPOT_CEILING: ByteCount = ByteCount(1024 * 1024);

type Store = ReelStore<SimulatedIo>;
type Model = BTreeMap<Vec<u8>, Vec<u8>>;
type Rows = Vec<(Vec<u8>, Vec<u8>)>;

fn config(capacity: RowCount) -> StoreConfig {
    StoreConfig {
        memory_budget: BUDGET,
        threads: crate::shape::threads(),
        volumes: vec![VolumeConfig {
            name: "sorted".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(),
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Fixed { width: WIDTH },
            codec: Codec::None,
            volume: VolumeId(0),
            complete_rows: None,
            sorted_rows: Some(capacity),
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

// whole walks both ways and walks from keys of every width, against the model
fn check_scans(store: &Store, model: &Model, random: &mut SeededRandom) {
    let forward: Vec<(Vec<u8>, Vec<u8>)> = model.iter().map(|(key, value)| (key.clone(), value.clone())).collect();
    assert_eq!(rows(store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan")), forward);
    let top = vec![0xFF; WIDTH as usize + 1];
    let mut backward = forward.clone();
    backward.reverse();
    assert_eq!(rows(store.iter_from(COLUMN, &top, Direction::Descending).expect("scan")), backward);
    for _start in 0..SCAN_STARTS {
        let mut start = key(random.below(CHURN_KEYS));
        // a start a byte short or a byte long sorts beside the stored keys of its leading bytes
        match random.below(3) {
            0 => {
                start.pop();
            },
            1 => start.push(random.below(256) as u8),
            _ => {},
        }
        let up: Vec<_> = forward.iter().filter(|(name, _value)| *name >= start).take(SCAN_TAKE).cloned().collect();
        let found: Vec<_> = rows(store.iter_from(COLUMN, &start, Direction::Ascending).expect("scan")).into_iter().take(SCAN_TAKE).collect();
        assert_eq!(found, up, "up from {start:?}");
        let down: Vec<_> = backward.iter().filter(|(name, _value)| *name <= start).take(SCAN_TAKE).cloned().collect();
        let found: Vec<_> = rows(store.iter_from(COLUMN, &start, Direction::Descending).expect("scan")).into_iter().take(SCAN_TAKE).collect();
        assert_eq!(found, down, "down from {start:?}");
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
        let io = SimulatedIo::new(Seed(100 + seed));
        let mut random = SeededRandom::new(Seed(200 + seed));
        let mut store = open(&io, CAPACITY);
        let mut model = Model::new();
        let written: Vec<Vec<u8>> = (0..CHURN_KEYS).map(key).collect();
        let mut sorted_names = written.clone();
        sorted_names.sort();
        for round in 0..CHURN_ROUNDS {
            for write in 0..CHURN_WRITES {
                let name = key(random.below(CHURN_KEYS));
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
                    store.close().expect("close");
                    drop(store);
                    store = open(&io, CAPACITY);
                },
                _ => {
                    store.flush().expect("flush");
                    io.kill();
                    drop(store);
                    store = open(&io, CAPACITY);
                },
            }
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
        check_entries(&store, &model);
        // a seed whose rounds all end in a reopen seals nothing until here, so the walks run once more over sealed keys
        check_scans(&store, &model, &mut random);
        assert!(store.state.sorted_answers.load(Ordering::Relaxed) > 0);
        assert!(store.state.sorted_scan_keys.load(Ordering::Relaxed) > 0, "no scan key came from the index");
        store.close().expect("close");
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

// a store whose declared index is past its budget is refused at open, and the error names the least budget it needs
#[test]
fn an_index_past_the_budget_is_refused() {
    let io = SimulatedIo::new(Seed(12));
    let rows = RowCount(50_000_000);
    match ReelStore::open(io.clone(), &config(rows)) {
        Err(StoreError::BudgetTooSmall { budget, least }) => {
            assert_eq!(budget, BUDGET);
            assert!(least.0 > budget.0 + reel::index::sorted_keys::planned_index_bytes(WIDTH as usize, rows).0 / 2, "least {least:?}");
        },
        Err(error) => panic!("refused for {error:?}"),
        Ok(_store) => panic!("opened with an index past its budget"),
    }
}

// a store whose only column is sorted gives the spot cache no share, and a cue behind the newest sealed rows reads its own
// values through the footers while gets at the tip go by the index
#[test]
fn an_all_sorted_store_reads_at_a_cue() {
    let io = SimulatedIo::new(Seed(13));
    let store = open(&io, CAPACITY);
    assert!(store.memory_report().spot_cache.0 < SPOT_CEILING.0, "the spot cache took a share");
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
const FAILURE_TRIES: u64 = 400;
const TAIL_KEYS: usize = 10;
const REFILL_KEPT: usize = 50;
// Draft: sims for the sorted index image, to go at the end of tests/simulation/sorted.rs. Every sim that expects the image taken
// asserts taken 1 and refused 0, and every refusal asserts refused 1 and taken 0, since a refusal rebuilds and matches the model too

use std::io::IoSlice;
use reel::io::{Io, IoFile};
use reel_api::units::FileOffset;
use reel_sim::file_image::BitMask;
use reel_sim::simulated_disk::OperationCount;

const PAGE: u64 = 4_096;
const TENTH_KEYS: u64 = CAPACITY.0 / 10;

fn image_path() -> PathBuf {
    PathBuf::from(ROOT).join(format!("sorted-{}.image", COLUMN.0))
}

fn new_image_path() -> PathBuf {
    PathBuf::from(ROOT).join(format!("sorted-{}.image.new", COLUMN.0))
}

fn has_file(io: &SimulatedIo, path: &Path) -> bool {
    io.list_directory(Path::new(ROOT)).expect("list").iter().any(|listed| listed == path)
}

fn read_file(io: &SimulatedIo, path: &Path) -> Vec<u8> {
    let file = io.open_read_only(path).expect("open");
    let mut bytes = vec![0; file.length().expect("length").0 as usize];
    file.read_exact_at(FileOffset(0), &mut bytes).expect("read");
    io.close_file(file);
    bytes
}

fn write_file(io: &SimulatedIo, path: &Path, bytes: &[u8]) {
    let _gone = io.remove_file(path);
    let file = io.create_file(path, ByteCount(bytes.len() as u64)).expect("create");
    file.write_at(FileOffset(0), &[IoSlice::new(bytes)]).expect("write");
    file.sync().expect("sync");
    io.close_file(file);
}

// (taken, refused) since open
fn image_counts(store: &Store) -> (u64, u64) {
    let report = report(store);
    (report.sorted_images_taken, report.sorted_images_refused)
}

// overwrites, deletes and a range delete over a few seals, so the image holds values, graves and a cover's removals
fn fill_for_image(store: &Store, io: &SimulatedIo, keys: u64) -> Model {
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

// a store closed clean with an image beside it, and the model it holds
fn closed_with_image(io: &SimulatedIo) -> Model {
    let store = open(io, CAPACITY);
    let model = fill_for_image(&store, io, KEYS);
    store.close().expect("close");
    assert!(has_file(io, &image_path()), "a clean close saved no image");
    model
}

// open, check the counters say the image went the expected way, and check every answer against the model
fn reopen_and_check(io: &SimulatedIo, capacity: RowCount, model: &Model, expected: (u64, u64), seed: u64) {
    let store = open(io, capacity);
    assert_eq!(image_counts(&store), expected);
    assert!(!has_file(io, &image_path()), "open left the image behind");
    check_all(&store, model, KEYS, seed);
    store.close().expect("close");
}

// a clean close saves the index, the next open takes it and removes it, and every get, scan and entry matches the model. The index
// the image gave takes seals after
#[test]
fn an_image_carries_the_index_through_a_close() {
    let io = SimulatedIo::new(Seed(30));
    let store = open(&io, CAPACITY);
    let mut model = fill_for_image(&store, &io, KEYS);
    let entries = store.state.sorted_index(COLUMN).expect("an index").entry_count();
    store.close().expect("close");
    let store = open(&io, CAPACITY);
    assert_eq!(image_counts(&store), (1, 0));
    assert!(!has_file(&io, &image_path()));
    assert_eq!(store.state.sorted_index(COLUMN).expect("an index").entry_count(), entries);
    check_all(&store, &model, KEYS, 31);
    for number in (0..KEYS).step_by(5) {
        store.put(COLUMN, &key(number), b"third").expect("put");
        model.insert(key(number), b"third".to_vec());
    }
    seal_all(&store, &io);
    check_all(&store, &model, KEYS, 32);
    store.close().expect("close");
    reopen_and_check(&io, CAPACITY, &model, (1, 0), 33);
}

// open removes the image before any write, so a kill after later seals leaves none, and the next open rebuilds
#[test]
fn a_kill_after_an_image_open_rebuilds() {
    let io = SimulatedIo::new(Seed(34));
    let mut model = closed_with_image(&io);
    let store = open(&io, CAPACITY);
    assert_eq!(image_counts(&store), (1, 0));
    for number in (0..KEYS).step_by(4) {
        store.put(COLUMN, &key(number), b"after").expect("put");
        model.insert(key(number), b"after".to_vec());
    }
    seal_all(&store, &io);
    store.flush().expect("flush");
    io.kill();
    drop(store);
    reopen_and_check(&io, CAPACITY, &model, (0, 0), 35);
}

// a kill after the image is read and removed but before the directory sync lands leaves the file, and the next open takes it
#[test]
fn a_kill_before_the_unlink_syncs_takes_the_image_again() {
    // an open fails at the directory sync after the image's removal, and a kill follows. Whether the removal survived the kill
    // or the image came back, the next open answers the model: an image that came back is taken, and none rebuilds
    let landed = (0..FAILURE_TRIES).find_map(|skip| {
        let io = SimulatedIo::new(Seed(36));
        let model = closed_with_image(&io);
        io.fail_directory_syncs_after(OperationCount(skip), OperationCount(1));
        let is_failed = ReelStore::open(io.clone(), &config(CAPACITY)).is_err();
        (is_failed && !has_file(&io, &image_path())).then_some((io, model))
    });
    let (io, model) = landed.expect("no failed sync landed after the image's removal");
    io.kill();
    let is_back = has_file(&io, &image_path());
    reopen_and_check(&io, CAPACITY, &model, if is_back { (1, 0) } else { (0, 0) }, 37);
}

// an image from an index of another capacity is refused, and open rebuilds
#[test]
fn an_image_of_another_capacity_is_refused() {
    let io = SimulatedIo::new(Seed(38));
    let model = closed_with_image(&io);
    reopen_and_check(&io, RowCount(CAPACITY.0 * 4), &model, (0, 1), 39);
}

// an image that lists a segment the volume no longer has is refused
#[test]
fn an_image_missing_a_segment_is_refused() {
    let io = SimulatedIo::new(Seed(40));
    let store = open(&io, CAPACITY);
    let mut model = fill_for_image(&store, &io, KEYS);
    // a segment of keys the model can drop whole, so its files can go with no answer changing
    let lone = key(KEYS + 1);
    store.put(COLUMN, &lone, b"lone").expect("put");
    seal_all(&store, &io);
    let number = store.state.sorted_index(COLUMN).expect("an index").find(&lone).expect("an entry").segment;
    store.close().expect("close");
    for path in io.list_directory(Path::new(ROOT)).expect("list") {
        if path.file_stem().is_some_and(|stem| stem.to_string_lossy().ends_with(&format!("{:016x}", number.0))) {
            io.remove_file(&path).expect("remove");
        }
    }
    model.remove(&lone);
    reopen_and_check(&io, CAPACITY, &model, (0, 1), 41);
}

// an image whose listed segment now holds other rows under the same number is refused
#[test]
fn an_image_with_a_swapped_segment_is_refused() {
    let io = SimulatedIo::new(Seed(42));
    let model = closed_with_image(&io);
    // flip the seal stamp of the first listed segment, and fix the sum so only the identity check can refuse it
    let mut bytes = read_file(&io, &image_path());
    let listed = reel::engine::sorted_image::SORTED_HEAD_BYTES;
    bytes[listed + 16] ^= 1;
    reseal(&mut bytes);
    write_file(&io, &image_path(), &bytes);
    reopen_and_check(&io, CAPACITY, &model, (0, 1), 43);
}

// write the image's sum again over everything before it, as a save would
fn reseal(bytes: &mut [u8]) {
    let body = bytes.len() - 8;
    let mut sum = reel::record::record_check::RunningSum::new();
    sum.add(&bytes[..body]);
    bytes[body..].copy_from_slice(&sum.sum().0.to_le_bytes());
}

// a flipped bit anywhere fails the sum: in the head, the list, the pages at three depths and the stored sum itself
#[test]
fn a_flipped_bit_refuses_the_image() {
    for (at, seed) in [(0.0, 44), (0.15, 45), (0.1, 46), (0.5, 47), (0.99, 48), (1.0, 49)] {
        let io = SimulatedIo::new(Seed(seed));
        let model = closed_with_image(&io);
        let length = read_file(&io, &image_path()).len() as u64;
        let offset = match at {
            0.0 => 56,
            0.15 => reel::engine::sorted_image::SORTED_HEAD_BYTES as u64,
            1.0 => length - 4,
            share => ((length as f64 * share) as u64).min(length - 9),
        };
        io.rot(&image_path(), FileOffset(offset), BitMask(1)).expect("rot");
        reopen_and_check(&io, CAPACITY, &model, (0, 1), seed + 100);
    }
}

// a file a page short or a page long is refused before any page is read
#[test]
fn an_image_a_page_short_or_long_is_refused() {
    for (is_long, seed) in [(false, 50), (true, 51)] {
        let io = SimulatedIo::new(Seed(seed));
        let model = closed_with_image(&io);
        let mut bytes = read_file(&io, &image_path());
        if is_long {
            bytes.extend(std::iter::repeat_n(0, PAGE as usize));
        } else {
            bytes.drain(bytes.len() - 8 - PAGE as usize..bytes.len() - 8);
        }
        write_file(&io, &image_path(), &bytes);
        reopen_and_check(&io, CAPACITY, &model, (0, 1), seed + 100);
    }
}

// an empty file is refused
#[test]
fn an_empty_image_is_refused() {
    let io = SimulatedIo::new(Seed(52));
    let model = closed_with_image(&io);
    write_file(&io, &image_path(), &[]);
    reopen_and_check(&io, CAPACITY, &model, (0, 1), 53);
}

// a .new a close left beside no image goes at open, nothing is taken, and the next close saves again
#[test]
fn a_leftover_new_file_is_removed() {
    let io = SimulatedIo::new(Seed(54));
    let model = closed_with_image(&io);
    let bytes = read_file(&io, &image_path());
    io.remove_file(&image_path()).expect("remove");
    write_file(&io, &new_image_path(), &bytes);
    let store = open(&io, CAPACITY);
    assert_eq!(image_counts(&store), (0, 0));
    assert!(!has_file(&io, &new_image_path()), "open left the .new");
    check_all(&store, &model, KEYS, 55);
    store.close().expect("close");
    assert!(has_file(&io, &image_path()));
}

// an index with a mark rebuilds at open, so its close saves nothing
#[test]
fn a_marked_index_saves_no_image() {
    let io = SimulatedIo::new(Seed(56));
    let store = open(&io, SMALL_CAPACITY);
    let mut late = 0;
    while store.state.sorted_index(COLUMN).expect("an index").mark().is_none() {
        assert!(late < LATE_KEYS, "no row went past capacity");
        for number in late..late + KEYS {
            store.put(COLUMN, &key(number), b"late").expect("put");
        }
        late += KEYS;
        seal_all(&store, &io);
    }
    store.close().expect("close");
    assert!(!has_file(&io, &image_path()), "a marked index saved an image");
    let store = open(&io, SMALL_CAPACITY);
    assert_eq!(image_counts(&store), (0, 0));
    for number in (0..late).step_by(97) {
        assert_eq!(read(&store, &key(number)).as_deref(), Some(&b"late"[..]));
    }
    store.close().expect("close");
}

// a range delete, a put and a delete still open at close cross the image: open takes it, replays them, runs the cover pass,
// and every answer matches
#[test]
fn open_rows_at_close_cross_an_image() {
    let io = SimulatedIo::new(Seed(57));
    let store = open(&io, CAPACITY);
    let mut model = fill_for_image(&store, &io, KEYS);
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
    assert_eq!(image_counts(&store), (1, 0));
    // rows still open at the close sit in the write index, which the image never holds, so the entries match once they seal
    let written: Vec<Vec<u8>> = (0..KEYS).map(key).collect();
    check(&store, &model, &written);
    check_scans(&store, &model, &mut SeededRandom::new(Seed(58)));
    seal_all(&store, &io);
    check_all(&store, &model, KEYS, 158);
    store.close().expect("close");
    reopen_and_check(&io, CAPACITY, &model, (1, 0), 258);
}

// a failed image write costs the next open its image and nothing else
#[test]
fn a_full_disk_mid_image_reopens_by_rebuild() {
    // the image's write fails half way, as a full disk or a failed sync leaves it: the close still succeeds, the half file is
    // gone by the next open, the open rebuilds, and a round trip after takes the next image
    let io = SimulatedIo::new(Seed(59));
    let store = open(&io, CAPACITY);
    let model = fill_for_image(&store, &io, KEYS);
    store.state.volumes[0].holds.sorted_image_fails.store(true, Ordering::Release);
    store.close().expect("a close failed on its image");
    assert!(!has_file(&io, &image_path()), "a failed write left an image");
    reopen_and_check(&io, CAPACITY, &model, (0, 0), 60);
    assert!(!has_file(&io, &new_image_path()), "the half written file stayed");
    let store = open(&io, CAPACITY);
    store.close().expect("close");
    reopen_and_check(&io, CAPACITY, &model, (1, 0), 61);
}

// a column at a tenth of its rows saves about a tenth of its pool, and round trips
#[test]
fn a_column_at_a_tenth_saves_a_tenth() {
    let io = SimulatedIo::new(Seed(62));
    let store = open(&io, CAPACITY);
    let model = fill_for_image(&store, &io, TENTH_KEYS);
    let pool = store.state.sorted_index(COLUMN).expect("an index").heap_bytes().0;
    store.close().expect("close");
    let written = read_file(&io, &image_path()).len() as u64;
    assert!(written < pool / 5, "{written} of a {pool} pool for a tenth of the rows");
    let store = open(&io, CAPACITY);
    assert_eq!(image_counts(&store), (1, 0));
    check_all(&store, &model, TENTH_KEYS, 63);
    store.close().expect("close");
}

// an old image put back after later seals is refused. With only open rows added after it, it is taken or refused and every answer
// matches either way
#[test]
fn a_stale_image_is_refused() {
    for (is_sealed, seed) in [(true, 64), (false, 65)] {
        let io = SimulatedIo::new(Seed(seed));
        let mut model = closed_with_image(&io);
        let old = read_file(&io, &image_path());
        let store = open(&io, CAPACITY);
        for number in (0..KEYS).step_by(6) {
            store.put(COLUMN, &key(number), b"later").expect("put");
            model.insert(key(number), b"later".to_vec());
        }
        if is_sealed {
            seal_all(&store, &io);
        }
        store.close().expect("close");
        write_file(&io, &image_path(), &old);
        let store = open(&io, CAPACITY);
        let counts = image_counts(&store); eprintln!("STALE sealed {is_sealed} counts {counts:?}");
        if is_sealed {
            assert_eq!(counts, (0, 1));
        } else {
            assert_eq!(counts.0 + counts.1, 1);
        }
        // rows put back after their delete sit in the open segments, which the index never holds, so its entries match the
        // model once those seal
        let written: Vec<Vec<u8>> = (0..KEYS).map(key).collect();
        check(&store, &model, &written);
        check_scans(&store, &model, &mut SeededRandom::new(Seed(seed + 100)));
        seal_all(&store, &io);
        check_entries(&store, &model);
        store.close().expect("close");
    }
}

// a cue taken and dropped before the close leaves the image as good as any
#[test]
fn an_image_after_a_cue_round_trips() {
    let io = SimulatedIo::new(Seed(66));
    let store = open(&io, CAPACITY);
    let model = fill_for_image(&store, &io, KEYS);
    drop(store.cue().expect("cue"));
    store.close().expect("close");
    reopen_and_check(&io, CAPACITY, &model, (1, 0), 67);
}

// a close with a merge mid-copy on the store's own worker stops it before the save, and the image matches the store it reopens
#[test]
fn a_close_mid_merge_saves_a_still_index() {
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
    assert_eq!(image_counts(&store), (1, 0));
    let written: Vec<Vec<u8>> = (0..MERGE_KEYS).map(key).collect();
    check(&store, &model, &written);
    check_entries(&store, &model);
    check_scans(&store, &model, &mut SeededRandom::new(Seed(69)));
    store.close().expect("close");
}

// an image read into an index that already took a page or an entry is refused, since the saved free chain runs into the tail of a
// pool this index has written, and the index still answers the key it held
#[test]
fn an_image_into_a_used_index_is_refused() {
    let io = SimulatedIo::new(Seed(69));
    let _model = closed_with_image(&io);
    let index = reel::index::sorted_keys::SortedKeys::new(WIDTH as usize, CAPACITY);
    let held = key(KEYS + 7);
    let entry = KeyEntry { place: WritePlace(1), segment: SegmentNumber(1), offset: FileOffset(0), length: ByteCount(1), is_grave: false };
    index.upsert(&held, entry, WritePlace(0));
    let file = io.open_read_only(&image_path()).expect("open");
    let mut chunk = vec![0; reel::engine::complete_image::IMAGE_CHUNK_BYTES];
    let read = reel::engine::sorted_image::read_sorted_image(&file, COLUMN, &index, &mut chunk, (&|_read_point, _count| true, &|_listed| true));
    io.close_file(file);
    assert_eq!(read.map_err(|error| error.kind()), Err(std::io::ErrorKind::InvalidData));
    assert_eq!(index.find(&held), Some(entry));
}

// a pool whose leaves emptied and went back on the free chain round trips through an image, and the reopened index takes every
// freed page and pages past the high-water mark with nothing lost
#[test]
fn freed_pages_round_trip_through_an_image() {
    let io = SimulatedIo::new(Seed(70));
    let store = open(&io, CAPACITY);
    let mut model = Model::new();
    for number in 0..KEYS * 4 {
        store.put(COLUMN, &key(number), b"first").expect("put");
        model.insert(key(number), b"first".to_vec());
    }
    seal_all(&store, &io);
    // half the keys in one contiguous run empty whole leaves
    let names: Vec<Vec<u8>> = model.keys().cloned().collect();
    let (start, end) = (names[names.len() / 4].clone(), names[names.len() * 3 / 4].clone());
    for name in names.iter().filter(|name| **name >= start && **name < end) {
        store.delete(COLUMN, name).expect("delete");
        model.remove(name);
    }
    seal_all(&store, &io);
    let index = store.state.sorted_index(COLUMN).expect("an index");
    assert!(index.free_pages() > index.page_count() - index.high_water(), "no page went back on the chain below the high-water mark");
    store.close().expect("close");
    let store = open(&io, CAPACITY);
    assert_eq!(image_counts(&store), (1, 0));
    for number in KEYS * 4..KEYS * 12 {
        store.put(COLUMN, &key(number), b"after").expect("put");
        model.insert(key(number), b"after".to_vec());
    }
    seal_all(&store, &io);
    let written: Vec<Vec<u8>> = (0..KEYS * 12).map(key).collect();
    check(&store, &model, &written);
    check_entries(&store, &model);
    check_scans(&store, &model, &mut SeededRandom::new(Seed(71)));
    store.close().expect("close");
}

// a put that fails after its place was given leaves the read point where the disk is, so the next close's image is taken
#[test]
fn an_image_after_a_failed_put_round_trips() {
    let io = SimulatedIo::new(Seed(72));
    let store = open(&io, CAPACITY);
    let model = fill_for_image(&store, &io, KEYS);
    io.fail_writes(OperationCount(1));
    let _failed = store.put(COLUMN, &key(KEYS + 3), b"lost");
    store.flush().ok();
    store.close().ok();
    let store = open(&io, CAPACITY);
    store.close().expect("close");
    // the write either landed or it didn't, and the model takes what the store says
    let store = open(&io, CAPACITY);
    // a reopen after a clean close takes the image; (0, 1) here means the read point refused a good image
    assert_eq!(image_counts(&store), (1, 0), "the read point refused an image after a failed put");
    let mut model = model;
    if let Some(value) = read(&store, &key(KEYS + 3)) {
        model.insert(key(KEYS + 3), value);
    }
    check_all(&store, &model, KEYS + 4, 73);
    store.close().expect("close");
}

// the disk fills during the image: the close still succeeds, and the next open rebuilds
#[test]
fn a_disk_full_during_the_image_reopens_by_rebuild() {
    // the same failure after writes that left open rows: the open replays them, rebuilds the index and answers the model
    let io = SimulatedIo::new(Seed(74));
    let store = open(&io, CAPACITY);
    let mut model = fill_for_image(&store, &io, KEYS);
    for number in (0..KEYS).step_by(9) {
        store.put(COLUMN, &key(number), b"open").expect("put");
        model.insert(key(number), b"open".to_vec());
    }
    store.state.volumes[0].holds.sorted_image_fails.store(true, Ordering::Release);
    store.close().expect("a close failed on its image");
    let store = open(&io, CAPACITY);
    assert_eq!(image_counts(&store), (0, 0));
    let written: Vec<Vec<u8>> = (0..KEYS).map(key).collect();
    check(&store, &model, &written);
    check_scans(&store, &model, &mut SeededRandom::new(Seed(75)));
    store.close().expect("close");
}

// two sorted columns and a complete one on one volume: each sorted column takes its own image, and the complete table its own
#[test]
fn two_sorted_columns_and_a_complete_one_take_their_own_images() {
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
            sorted_rows: None,
            complete_rows: Some(CAPACITY),
            ..config.columns[0].clone()
        });
        config
    };
    let sorted_path = |column: ColumnId| PathBuf::from(ROOT).join(format!("sorted-{}.image", column.0));
    let scan = |store: &Store, column: ColumnId| rows(store.iter_from(column, b"", Direction::Ascending).expect("scan"));
    for is_swapped in [false, true] {
        let io = SimulatedIo::new(Seed(70 + u64::from(is_swapped)));
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
        assert!(has_file(&io, &sorted_path(COLUMN)) && has_file(&io, &sorted_path(SECOND)), "a sorted column saved no image");
        if is_swapped {
            // column 1's file carries column 2's image: its column word refuses it
            let bytes = read_file(&io, &sorted_path(SECOND));
            write_file(&io, &sorted_path(COLUMN), &bytes);
        }

        let store = ReelStore::open(io.clone(), &three()).expect("open");
        let opened = report(&store);
        let expected = if is_swapped { (1, 1) } else { (2, 0) };
        assert_eq!((opened.sorted_images_taken, opened.sorted_images_refused), expected, "swapped {is_swapped}");
        let asks = opened.sorted_footer_asks;
        let as_rows = |model: &Model| model.iter().map(|(name, value)| (name.clone(), value.clone())).collect::<Vec<_>>();
        assert_eq!(scan(&store, COLUMN), as_rows(&first), "swapped {is_swapped}: column 1 read another column's pages");
        assert_eq!(scan(&store, SECOND), as_rows(&second), "swapped {is_swapped}: column 2 read another column's pages");
        for number in 0..KEYS {
            let mut value = Vec::new();
            let got = store.get(COLUMN, &key(number), &mut value).expect("get").then(|| value.clone());
            assert_eq!(got.as_ref(), first.get(&key(number)));
            let got = store.get(SECOND, &key(number), &mut value).expect("get").then(|| value.clone());
            assert_eq!(got.as_ref(), second.get(&key(number)));
            assert!(store.get(THIRD, format!("third-{number}").as_bytes(), &mut value).expect("get"));
        }
        assert_eq!(report(&store).sorted_footer_asks, asks, "swapped {is_swapped}: a sorted get asked the footers");
        store.close().expect("close");
    }
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
// keys refuse, the index stays marked, the close saves no image, and the next open finds the same
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
    let _gone = io.remove_file(&image_path());
    let path = reel::engine::open_segment::data_path(&PathBuf::from(ROOT), holder).to_path_buf();
    io.rot(&path, FileOffset(at.0 + 40), BitMask(1)).expect("rot");

    let mut counts = None;
    for round in 0..2 {
        let store = open(&io, CAPACITY);
        hold_rebuilds_here(&store);
        assert_eq!(image_counts(&store), (0, 0), "round {round}: an image stood");
        store.clear_damage(COLUMN, b"", None).expect("clear");
        let reading = report(&store).sorted_columns[0];
        assert!(reading.is_marked, "round {round}: a fill that missed rows left the index unmarked");
        // a refill that meets the unread block ends with the mark standing over its rows, and starts no more
        store.state.ask_compaction();
        settle(&io);
        assert!(report(&store).sorted_columns[0].is_marked, "round {round}: a refill cleared the mark over an unread block");
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
        assert!(!has_file(&io, &image_path()), "round {round}: a marked index saved an image");
    }
}

fn hold_rebuilds_here(store: &Store) {
    for volume in &store.state.volumes {
        volume.holds.rebuild.store(true, Ordering::Release);
    }
}

// An index past its declared rows takes its mark, and once a range delete takes most of its entries back out the refill gives
// the rows past the mark their entries and clears it: gets and scans then answer from the index alone, and every answer
// matches the model
#[test]
fn a_marked_index_refills_once_room_returns() {
    let io = SimulatedIo::new(Seed(190));
    let store = open(&io, SMALL_CAPACITY);
    let index = store.state.sorted_index(COLUMN).expect("an index");
    let mut model = Model::new();
    let written: Vec<Vec<u8>> = (0..KEYS * 20).map(key).collect();
    for number in 0..KEYS * 20 {
        store.put(COLUMN, &key(number), format!("value-{number}").as_bytes()).expect("put");
        model.insert(key(number), format!("value-{number}").into_bytes());
    }
    seal_all(&store, &io);
    assert!(index.mark().is_some(), "the index never went past its rows");
    // a range delete over all but the highest keys frees most of the pool once it seals
    let names: Vec<Vec<u8>> = model.keys().cloned().collect();
    let kept = names[names.len() - REFILL_KEPT..].to_vec();
    store.delete_range(COLUMN, &names[0], &kept[0]).expect("delete range");
    model.retain(|name, _value| kept.contains(name));
    seal_all(&store, &io);
    store.state.ask_compaction();
    settle(&io);
    let state = &store.state;
    eprintln!(
        "refill: mark {:?} entries {} passes {} clears {} refused {} stuck {}",
        index.mark(),
        index.entry_count().0,
        state.sorted_refill_passes.load(Ordering::Acquire),
        state.sorted_refill_clears.load(Ordering::Acquire),
        state.sorted_refill_refused.load(Ordering::Acquire),
        state.sorted_refill_stuck.load(Ordering::Acquire)
    );
    assert!(index.mark().is_none(), "the refill never cleared the mark");
    let before = report(&store).sorted_footer_asks;
    check(&store, &model, &written);
    check_entries(&store, &model);
    check_scans(&store, &model, &mut SeededRandom::new(Seed(191)));
    assert_eq!(report(&store).sorted_footer_asks, before, "a get asked the footers after the refill");
    store.close().expect("close");
}

// a scan told no count starts at the small batch and doubles with the rows it takes: one that stops after 33 rows locates no
// more than 96 keys from the index, one that takes 1,000 no more than 1,256, and both return the model's rows
#[test]
fn an_untold_scan_grows_its_batch_with_its_rows() {
    let io = SimulatedIo::new(Seed(195));
    let store = open(&io, CAPACITY);
    let mut model = Model::new();
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), format!("value-{number}").as_bytes()).expect("put");
        model.insert(key(number), format!("value-{number}").into_bytes());
    }
    seal_all(&store, &io);
    for (rows, most) in [(33_usize, 96_u64), (1_000, 1_256)] {
        let before = report(&store).sorted_scan_keys;
        let mut scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
        let (mut name, mut value) = (Vec::new(), Vec::new());
        let mut taken = Vec::new();
        while taken.len() < rows && scan.next_row(&mut name, &mut value).expect("step") == ScanStep::Found {
            taken.push((name.clone(), value.clone()));
        }
        drop(scan);
        let located = report(&store).sorted_scan_keys - before;
        let expected: Rows = model.iter().take(rows).map(|(name, value)| (name.clone(), value.clone())).collect();
        assert_eq!(taken, expected, "{rows} rows");
        assert!(located <= most, "{rows} rows located {located} keys, past {most}");
    }
    store.close().expect("close");
}
