//! The seal's ration: writers draw free index room in step with the walk

use std::path::PathBuf;
use std::sync::atomic::Ordering;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::io::Io;
use reel::record::codec::Codec;
use reel_core::column_id::ColumnId;
use reel_core::maintenance_report::{MaintenanceReport, PauseCause};
use reel_core::store_error::StoreError;
use reel_core::units::{ByteCount, Nanoseconds, RowCount, VolumeId};
use reel_core::write_batch::WriteBatch;
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_io::SimulatedIo;

const BULK_ROOT: &str = "/disk/bulk";
const META_ROOT: &str = "/disk/meta";
const BULK: ColumnId = ColumnId(1);
const META: ColumnId = ColumnId(2);
const BUDGET: ByteCount = ByteCount(128 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 200_000;
const SEAL_JOB: &str = "reel-seal";
const FIRST_ROWS: u64 = 4_000;
const NEXT_ROWS: u64 = 3_000;
const BATCH_ROWS: u64 = 1_000;

type Store = ReelStore<SimulatedIo>;

fn volume(name: &str, root: &str) -> VolumeConfig {
    VolumeConfig {
        name: name.to_string(),
        roots: vec![PathBuf::from(root)],
        sync: SyncPolicy::Never,
        device: Default::default(), open_segments: None,
    }
}

fn column(id: ColumnId, name: &str, volume: u8) -> ColumnConfig {
    ColumnConfig {
        id,
        name: name.to_string(),
        key_shape: KeyShape::Variable { most: 64 },
        codec: Codec::None,
        volume: VolumeId(volume),
        complete_rows: None,
        sorted_rows: None,
        least_value: None,
    }
}

fn open(io: &SimulatedIo) -> Store {
    let config = StoreConfig {
        memory_budget: BUDGET,
        threads: reel::config::NO_WORKERS,
        volumes: vec![volume("bulk", BULK_ROOT), volume("meta", META_ROOT)],
        columns: vec![column(BULK, "bulk", 0), column(META, "meta", 1)],
    };
    ReelStore::open(io.clone(), &config).expect("open")
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
    panic!("jobs never went quiet");
}

fn key(number: u64) -> Vec<u8> {
    format!("key-{number:06}").into_bytes()
}

fn put_keys(store: &Store, first: u64, count: u64) {
    for number in first..first + count {
        store.put(BULK, &key(number), b"value").expect("put");
    }
}

fn line(store: &Store) -> Option<RowCount> {
    store.state.volumes[0].ration.line(|| store.state.io.now())
}

// a seal's first step starts the ration, so the line stands where the walk has it
fn start_seal(store: &Store, io: &SimulatedIo) {
    store.seal_now();
    while line(store).is_none() {
        assert!(io.step_any().is_some(), "the seal never started");
    }
}

// every write past the line waits for the walk, so the index never holds more than the line allows
#[test]
fn writes_keep_to_the_line() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io);
    put_keys(&store, 0, FIRST_ROWS);
    start_seal(&store, &io);
    let start = store.state.rows_held();
    let past_the_first_line = (store.state.index_capacity().0 - start.0) / 2;
    for number in 0..past_the_first_line {
        store.put(BULK, &key(FIRST_ROWS + number), b"value").expect("put");
        if let Some(line) = line(&store) {
            assert!(store.state.rows_held() <= line, "{:?} past {line:?}", store.state.rows_held());
        }
    }
    assert!(store.state.rows_held() > start);
    settle(&io);
    let (waits, _longest) = store.index_waits();
    assert!(waits > 0);
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    assert!(report.pauses[PauseCause::Ration.index()] > 0, "the waits never paused on the ration");
    store.close().expect("close");
}

// a write that waits on the ration lands and reads back, and the count comes back to nothing once every segment seals
#[test]
fn rows_come_back() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io);
    put_keys(&store, 0, FIRST_ROWS);
    start_seal(&store, &io);
    put_keys(&store, FIRST_ROWS, NEXT_ROWS);
    settle(&io);
    store.seal_now();
    settle(&io);
    assert_eq!(store.state.rows_held(), RowCount(0));
    assert_eq!(line(&store), None);
    let mut value = Vec::new();
    for number in 0..FIRST_ROWS + NEXT_ROWS {
        assert!(store.get(BULK, &key(number), &mut value).expect("get"));
    }
    store.close().expect("close");
}

// a batch waits for room for all its rows in one piece
#[test]
fn batch_draws_whole() {
    let io = SimulatedIo::new(Seed(3));
    let store = open(&io);
    put_keys(&store, 0, FIRST_ROWS);
    start_seal(&store, &io);
    // the rows go right up to the line, so the batch needs the walk to move before it fits
    let mut next = FIRST_ROWS + BATCH_ROWS;
    while line(&store).is_some_and(|line| store.state.rows_held().0 + 1 < line.0) {
        put_keys(&store, next, 1);
        next += 1;
    }
    let mut batch = WriteBatch::new();
    for number in 0..BATCH_ROWS {
        batch.put(BULK, &key(FIRST_ROWS + number), b"value");
    }
    let before = store.state.rows_held();
    store.write_batch(&batch).expect("batch");
    if let Some(line) = line(&store) {
        assert!(before.0 + BATCH_ROWS <= line.0);
    }
    store.close().expect("close");
}

// a bulk seal's line holds back only its own writers, so a meta put goes on while the bulk seal stands still
#[test]
fn meta_goes_past_a_bulk_seal() {
    let io = SimulatedIo::new(Seed(4));
    let store = open(&io);
    put_keys(&store, 0, FIRST_ROWS);
    start_seal(&store, &io);
    io.hold_jobs(SEAL_JOB, true);
    let bulk_line = line(&store).expect("line");
    for number in 0..bulk_line.0 + 100 {
        store.put(META, &key(number), b"value").expect("put");
    }
    let (waits, _longest) = store.index_waits();
    assert_eq!(waits, 0);
    io.hold_jobs(SEAL_JOB, false);
    store.close().expect("close");
}

// a failed volume frees a writer from the line with its own error
#[test]
fn failed_volume_ends_the_wait() {
    let io = SimulatedIo::new(Seed(5));
    let store = open(&io);
    put_keys(&store, 0, FIRST_ROWS);
    start_seal(&store, &io);
    io.hold_jobs(SEAL_JOB, true);
    let volume = &store.state.volumes[0];
    let past_line = RowCount(line(&store).expect("line").0);
    let drew = store.state.draw_ration(volume, past_line);
    assert!(drew.is_ok() || !volume.has_failed());
    volume.fail();
    assert!(matches!(store.state.draw_ration(volume, past_line), Err(StoreError::VolumeFailed { .. })));
    assert!(!volume.ration.has_line_of_writers());
    io.hold_jobs(SEAL_JOB, false);
    drop(store);
}

// an open counts the rows it brings back, so a seal started right after one has a true line
#[test]
fn open_counts_its_rows() {
    let io = SimulatedIo::new(Seed(6));
    let store = open(&io);
    put_keys(&store, 0, FIRST_ROWS);
    store.flush().expect("flush");
    settle(&io);
    io.power_cut();
    drop(store);
    let store = open(&io);
    assert_eq!(store.state.rows_held(), RowCount(FIRST_ROWS));
    assert_eq!(store.state.volumes[0].rows_in_index.load(Ordering::Acquire), FIRST_ROWS);
    store.close().expect("close");
}
