//! Sorted columns: an index holds each sealed key's newest place, so a get reads no footers, through seals, merges, covers and opens

use std::collections::BTreeMap;
use std::path::PathBuf;
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
use reel_api::direction::Direction;
use reel_api::scan::ScanStep;
use reel_api::maintenance_report::MaintenanceReport;
use reel::segment::segment_tail::SegmentNumber;
use reel_api::units::{ByteCount, DeathMark, Nanoseconds, RowCount, VolumeId};
use reel_api::write_batch::WriteBatch;
use reel_sim::seeded_random::{Seed, SeededRandom};
use reel_sim::simulated_io::SimulatedIo;

const ROOT: &str = "/disk/sorted";
const COLUMN: ColumnId = ColumnId(1);
const WIDTH: u16 = 16;
const BUDGET: ByteCount = ByteCount(64 * 1024 * 1024);
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
const SCAN_TAKE: usize = 40;
const MERGE_KEYS: u64 = 15_000;

type Store = ReelStore<SimulatedIo>;
type Model = BTreeMap<Vec<u8>, Vec<u8>>;

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
    assert!(report(&store).sorted_past_capacity > 0);
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
        let mut scan = store.state.take_scan(&range, None).expect("scan");
        scan.wanted = wanted;
        let (mut name, mut value) = (Vec::new(), Vec::new());
        let mut found = Vec::new();
        for _row in 0..wanted.min(KEYS) {
            let step = store.state.step_scan(&mut scan, &mut name, reel::engine::scan_path::ScanWant::Value(&mut value)).expect("step");
            assert_eq!(step, ScanStep::Found);
            assert!(scan.batch.entries.len() as u64 <= wanted, "{} located for {wanted} wanted", scan.batch.entries.len());
            found.push((name.clone(), value.clone()));
        }
        let expected: Vec<_> = model.iter().take(wanted.min(KEYS) as usize).map(|(name, value)| (name.clone(), value.clone())).collect();
        assert_eq!(found, expected);
        store.state.give_back_scan(scan);
    }
    store.close().expect("close");
}
