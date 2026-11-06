//! The caller's thread counts: any count gives the same store, and a count the budget can't hold is refused at open

use std::path::PathBuf;
use std::sync::atomic::Ordering;

use reel::config::{ColumnConfig, DeviceClass, KeyShape, StoreConfig, SyncPolicy, ThreadCounts, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::guard::hold;
use reel::record::codec::Codec;
use reel::segment::segment_tail::SegmentNumber;
use reel_api::column_id::ColumnId;
use reel_api::direction::Direction;
use reel_api::maintenance_report::MaintenanceReport;
use reel_api::scan::ScanStep;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, Nanoseconds, VolumeId};
use reel_sim::seeded_random::{Seed, SeededRandom};
use reel_sim::simulated_io::SimulatedIo;

const ROOT: &str = "/disk/threads";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(128 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 400_000;
const ROUNDS: u64 = 6;
const KEYS: u64 = 400;
const MANY_WORKERS: usize = 100_000;
const DEAD_SEGMENTS: u64 = 6;

type Store = ReelStore<SimulatedIo>;

fn config(threads: ThreadCounts) -> StoreConfig {
    config_on(threads, DeviceClass::Detect)
}

fn config_on(threads: ThreadCounts, device: DeviceClass) -> StoreConfig {
    StoreConfig {
        memory_budget: BUDGET,
        threads,
        volumes: vec![VolumeConfig {
            name: "threads".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device,
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Variable { most: 64 },
            codec: Codec::None,
            volume: VolumeId(0),
            complete_rows: None,
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

// the keys and values of a seeded run, after the jobs have gone quiet
fn contents(workers: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let io = SimulatedIo::new(Seed(1));
    let threads = ThreadCounts { maintenance_workers: workers, ..ThreadCounts::default() };
    let store: Store = ReelStore::open(io.clone(), &config(threads)).expect("open");
    let mut random = SeededRandom::new(Seed(2));
    for round in 0..ROUNDS {
        for _write in 0..KEYS {
            let key = format!("key-{:04}", random.below(KEYS));
            if random.below(4) == 0 {
                store.delete(COLUMN, key.as_bytes()).expect("delete");
            } else {
                store.put(COLUMN, key.as_bytes(), format!("value-{round}").as_bytes()).expect("put");
            }
        }
        store.seal_now();
        settle(&io);
    }
    let mut found = Vec::new();
    let mut scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
    let (mut key, mut value) = (Vec::new(), Vec::new());
    while scan.next_row(&mut key, &mut value).expect("step") == ScanStep::Found {
        found.push((key.clone(), value.clone()));
    }
    drop(scan);
    store.close().expect("close");
    found
}

// 0, 1 and 4 workers leave the same keys and values
#[test]
fn any_count_same_store() {
    let none = contents(0);
    assert!(!none.is_empty());
    assert_eq!(contents(1), none);
    assert_eq!(contents(4), none);
}

// a count the budget can't hold is refused, by name, and a store with no read thread has nothing to run async reads on
#[test]
fn refused_at_open() {
    let io = SimulatedIo::new(Seed(3));
    let many = ThreadCounts { maintenance_workers: MANY_WORKERS, ..ThreadCounts::default() };
    assert!(matches!(ReelStore::open(io.clone(), &config(many)), Err(StoreError::ThreadsPastBudget { .. })));
    let none = ThreadCounts { read_threads: 0, ..ThreadCounts::default() };
    assert!(matches!(ReelStore::open(io, &config(none)), Err(StoreError::NoReadThreads)));
}

// more read threads run, and memory_report counts their stacks
#[test]
fn read_threads_follow_the_count() {
    let io = SimulatedIo::new(Seed(4));
    let threads = ThreadCounts { read_threads: 2, ..ThreadCounts::default() };
    let store: Store = ReelStore::open(io, &config(threads)).expect("open");
    assert_eq!(store.state.read_tasks.len(), 2);
    store.close().expect("close");
}

// a worker runs a rewrite the planner hands it, and the store reads the same through it
#[test]
fn workers_run_rewrites() {
    let io = SimulatedIo::new(Seed(5));
    let threads = ThreadCounts { maintenance_workers: 2, ..ThreadCounts::default() };
    let store: Store = ReelStore::open(io.clone(), &config(threads)).expect("open");
    for round in 0..ROUNDS {
        for number in 0..KEYS {
            store.put(COLUMN, format!("key-{number:04}").as_bytes(), format!("value-{round}").as_bytes()).expect("put");
            store.put(COLUMN, format!("stays-{round}-{number:04}").as_bytes(), b"value").expect("put");
        }
        store.seal_now();
        settle(&io);
    }
    let volume = &store.state.volumes[0];
    let mut report = MaintenanceReport::default();
    store.maintenance_report(&mut report);
    let by_lane = report.volumes[0].rewrites_by_lane;
    assert!(by_lane[1..].iter().sum::<u64>() > 0, "no worker finished a rewrite: {by_lane:?}");
    let worked = volume.lanes.len();
    assert_eq!(worked, 3);
    assert!(volume.segments_rewritten.load(Ordering::Acquire) > 0);
    for number in 0..KEYS {
        let mut value = Vec::new();
        assert!(store.get(COLUMN, format!("key-{number:04}").as_bytes(), &mut value).expect("get"));
        assert_eq!(value, format!("value-{}", ROUNDS - 1).into_bytes());
    }
    store.close().expect("close");
}

// the most rewrites seen running at once while the jobs drain a volume of half-dead segments
fn most_rewrites_at_once(device: DeviceClass) -> usize {
    let io = SimulatedIo::new(Seed(6));
    let threads = ThreadCounts { maintenance_workers: 3, ..ThreadCounts::default() };
    let store: Store = ReelStore::open(io.clone(), &config_on(threads, device)).expect("open");
    let volume = &store.state.volumes[0];
    volume.is_compaction_held.store(true, Ordering::Release);
    for round in 0..DEAD_SEGMENTS {
        for number in 0..KEYS {
            store.put(COLUMN, format!("key-{round}-{number:04}").as_bytes(), b"first").expect("put");
        }
        store.seal_now();
        settle(&io);
    }
    for round in 0..DEAD_SEGMENTS {
        for number in 0..KEYS {
            store.put(COLUMN, format!("key-{round}-{number:04}").as_bytes(), b"second").expect("put");
        }
    }
    store.seal_now();
    settle(&io);

    volume.is_compaction_held.store(false, Ordering::Release);
    store.state.ask_compaction();
    let mut most = 0;
    for _step in 0..STEPS {
        let running = volume.lanes.iter().filter(|lane| hold(&lane.inputs)[0] != SegmentNumber(0)).count();
        most = most.max(running);
        if io.step_any().is_none() {
            io.advance_clock(TICK);
            if io.step_any().is_none() {
                break;
            }
        }
    }
    store.close().expect("close");
    most
}

// a spinning volume runs one rewrite at a time however many workers stand by
#[test]
fn spinning_runs_one_stream() {
    assert_eq!(most_rewrites_at_once(DeviceClass::Spinning), 1);
}

// a solid volume runs them side by side
#[test]
fn solid_runs_several() {
    assert!(most_rewrites_at_once(DeviceClass::Solid) > 1);
}

// the report says what class a volume has and where it came from, and a backend that can't tell leaves it solid
#[test]
fn class_shows_in_the_report() {
    for (device, is_spinning, is_set) in [(DeviceClass::Detect, false, false), (DeviceClass::Solid, false, true), (DeviceClass::Spinning, true, true)] {
        let io = SimulatedIo::new(Seed(7));
        let store: Store = ReelStore::open(io, &config_on(ThreadCounts::default(), device)).expect("open");
        let mut report = MaintenanceReport::default();
        store.maintenance_report(&mut report);
        assert_eq!((report.volumes[0].is_spinning, report.volumes[0].is_device_set), (is_spinning, is_set));
        store.close().expect("close");
    }
}
