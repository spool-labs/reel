//! The worst pick of each kind runs to its end with no push past a list's capacity, and writes no more than its need

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel::config::{ColumnConfig, KeyShape, MAX_KEY_BYTES, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::compact_job::CompactJob;
use reel::engine::reel_store::ReelStore;
use reel::engine::rewrite_job::{MERGE_BUFFER_BYTES, MOST_OVERLAP};
use reel::fixed_list::lists_full;
use reel::guard::hold;
use reel::record::codec::Codec;
use reel::segment::segment_tail::MOST_MERGE_SEGMENTS;
use reel_api::column_id::ColumnId;
use reel_api::maintenance_report::{MaintenanceMemory, MaintenanceReport};
use reel_api::units::{ByteCount, Nanoseconds, VolumeId};
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_io::SimulatedIo;

type Store = ReelStore<SimulatedIo>;

const ROOT: &str = "/disk/bounds";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(128 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 200_000;
const PILE: u64 = MOST_OVERLAP as u64 + 8;
const WIDE_KEYS: u64 = 60;
const WIDE_VALUE_BYTES: usize = 2 * MERGE_BUFFER_BYTES + 1;

fn config() -> StoreConfig {
    StoreConfig {
        memory_budget: BUDGET,
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "bounds".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(),
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Variable { most: MAX_KEY_BYTES },
            codec: Codec::None,
            volume: VolumeId(0),
            complete_rows: None,
            least_value: None,
        }],
    }
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

fn seal(store: &Store, io: &SimulatedIo) {
    store.seal_now();
    settle(io);
    cool(store, io);
}

// a key of the most bytes a key can take, ending in its number
fn wide_key(prefix: u8, number: u64) -> Vec<u8> {
    let mut key = vec![prefix; MAX_KEY_BYTES as usize];
    let tail = key.len() - 8;
    key[tail..].copy_from_slice(&number.to_be_bytes());
    key
}

fn memory(store: &Store) -> MaintenanceMemory {
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    report.memory
}

// every run gave its region back, and none wrote past the need it was admitted with
fn check_runs(store: &Store, full_before: u64) {
    let memory = memory(store);
    assert_eq!(lists_full(), full_before, "a list's bound came out short");
    assert!(memory.admits > 0, "no run took a region");
    assert_eq!(memory.now, ByteCount(0), "a region outlived its run");
    assert!(memory.rewrite_touched <= memory.rewrite_need, "{memory:?}");
    assert!(memory.walk_need.0 > 0 && memory.walk_touched <= memory.walk_need, "{memory:?}");
}

// a merge of the most inputs, over keys of the most bytes, so the writers close a block every few rows
#[test]
fn most_inputs_of_widest_keys() {
    let full_before = lists_full();
    let io = SimulatedIo::new(Seed(91));
    let store = ReelStore::open(io.clone(), &config()).expect("open");
    let volume = &store.state.volumes[0];
    volume.is_compaction_held.store(true, Ordering::Release);
    volume.is_walk_held.store(true, Ordering::Release);
    for number in 0..PILE {
        for key in 0..WIDE_KEYS {
            store.put(COLUMN, &wide_key(b'k', key), &number.to_le_bytes()).expect("put");
        }
        seal(&store, &io);
    }
    volume.is_compaction_held.store(false, Ordering::Release);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(job.rewrite.start(), "no merge started");
    assert_eq!(job.rewrite.input_numbers().len(), MOST_MERGE_SEGMENTS);
    while job.rewrite.is_running() {
        job.rewrite.step();
    }
    assert!(store.state.volumes[0].segments_rewritten.load(Ordering::Acquire) > 0);
    drop(job);
    volume.is_walk_held.store(false, Ordering::Release);
    store.state.ask_compaction();
    settle(&io);
    let mut buf = Vec::new();
    assert!(store.get(COLUMN, &wide_key(b'k', 0), &mut buf).expect("get"));
    assert_eq!(buf, (PILE - 1).to_le_bytes());
    check_runs(&store, full_before);
    store.close().expect("close");
}

// one input with as many rows as a segment takes, copied in offset order with a mark and a place for each row
#[test]
fn most_rows_in_one_input() {
    let full_before = lists_full();
    let io = SimulatedIo::new(Seed(92));
    let store = ReelStore::open(io.clone(), &config()).expect("open");
    let rows = store.state.volumes[0].segment_rows.0 - 1;
    for key in 0..rows {
        store.put(COLUMN, &key.to_be_bytes(), b"first").expect("put");
    }
    seal(&store, &io);
    for key in 0..rows * 3 / 4 {
        store.put(COLUMN, &key.to_be_bytes(), b"second").expect("put");
    }
    seal(&store, &io);
    settle(&io);
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    assert!(report.segments_rewritten.0 > 0, "nothing was rewritten");
    assert_eq!(report.volumes[0].key_order_copies, 0, "the one-input copy went in key order");
    let mut buf = Vec::new();
    assert!(store.get(COLUMN, &(rows - 1).to_be_bytes(), &mut buf).expect("get"));
    assert_eq!(buf, b"first");
    check_runs(&store, full_before);
    store.close().expect("close");
}

// a merge whose inputs each hold a record past the copy buffer, which a run copies in parts
#[test]
fn widest_records() {
    let full_before = lists_full();
    let io = SimulatedIo::new(Seed(93));
    let store = ReelStore::open(io.clone(), &config()).expect("open");
    let volume = &store.state.volumes[0];
    volume.is_compaction_held.store(true, Ordering::Release);
    volume.is_walk_held.store(true, Ordering::Release);
    let value = vec![7; WIDE_VALUE_BYTES];
    for number in 0..PILE {
        store.put(COLUMN, &wide_key(b'a', number), &value).expect("put");
        store.put(COLUMN, &wide_key(b'm', 0), &number.to_le_bytes()).expect("put");
        store.put(COLUMN, &wide_key(b'z', number), &value).expect("put");
        seal(&store, &io);
    }
    volume.is_compaction_held.store(false, Ordering::Release);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(job.rewrite.start(), "no merge started");
    while job.rewrite.is_running() {
        job.rewrite.step();
    }
    assert!(volume.segments_rewritten.load(Ordering::Acquire) > 0);
    drop(job);
    volume.is_walk_held.store(false, Ordering::Release);
    store.state.ask_compaction();
    settle(&io);
    let mut buf = Vec::new();
    for number in 0..PILE {
        assert!(store.get(COLUMN, &wide_key(b'a', number), &mut buf).expect("get"));
        assert_eq!(buf, value);
    }
    assert!(hold(&volume.summary).rows().count() > 0);
    check_runs(&store, full_before);
    store.close().expect("close");
}

// an offset-order copy of scattered live records reads the near ones together, and each lands where its row says
#[test]
fn near_records_copy_in_runs() {
    let full_before = lists_full();
    let io = SimulatedIo::new(Seed(94));
    let store = ReelStore::open(io.clone(), &config()).expect("open");
    let rows = store.state.volumes[0].segment_rows.0 - 1;
    for key in 0..rows {
        store.put(COLUMN, &key.to_be_bytes(), &[1; 100]).expect("put");
    }
    seal(&store, &io);
    for key in (0..rows).filter(|key| key % 4 != 0) {
        store.put(COLUMN, &key.to_be_bytes(), b"second").expect("put");
    }
    seal(&store, &io);
    settle(&io);
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    let volume = &report.volumes[0];
    assert!(volume.span_reads > 0, "no run was read: {} lone copies", volume.lone_copies);
    assert!(volume.span_records >= 2 * volume.span_reads, "runs of {} records over {} reads", volume.span_records, volume.span_reads);
    let mut buf = Vec::new();
    for key in (0..rows).step_by(4) {
        assert!(store.get(COLUMN, &key.to_be_bytes(), &mut buf).expect("get"));
        assert_eq!(buf, [1; 100]);
    }
    check_runs(&store, full_before);
    store.close().expect("close");
}

// the writer pauses once a seal lands, so the hot window closes and the rewrites hot ingest put off can start
fn cool(store: &Store, io: &SimulatedIo) {
    let until = store.state.volumes.iter().map(|volume| volume.hot_until().0).max().unwrap_or(0);
    io.advance_clock(Nanoseconds(until.saturating_sub(io.clock_reading().0)));
    settle(io);
}
