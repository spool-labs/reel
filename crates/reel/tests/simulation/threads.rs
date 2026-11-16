//! The caller's thread counts: any count gives the same store, and a count the budget can't hold is refused at open

use std::path::PathBuf;
use std::sync::atomic::Ordering;

use reel::config::{default_workers, ColumnConfig, DeviceClass, KeyShape, StoreConfig, SyncPolicy, ThreadCounts, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::engine::walk_job::{SLOT_ASSIGNED, SLOT_FREE};
use reel::guard::hold;
use reel::record::codec::Codec;
use reel::segment::segment_tail::SegmentNumber;
use reel_core::column_id::ColumnId;
use reel_core::direction::Direction;
use reel_core::maintenance_report::MaintenanceReport;
use reel_core::scan::ScanStep;
use reel_core::store_error::StoreError;
use reel_core::units::{ByteCount, Nanoseconds, VolumeId};
use reel_sim::seeded_random::{Seed, SeededRandom};
use reel_sim::simulated_io::SimulatedIo;

const ROOT: &str = "/disk/threads";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(148 * 1024 * 1024);
// workers a 64 MiB budget can't hold, whatever the host's CPU count
const PAST_A_SMALL_BUDGET: usize = 32;
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 400_000;
const ROUNDS: u64 = 6;
const KEYS: u64 = 400;
const MANY_WORKERS: usize = 100_000;
const DEAD_SEGMENTS: u64 = 6;
const TWO_VOLUME_BUDGET: ByteCount = ByteCount(256 * 1024 * 1024);

type Store = ReelStore<SimulatedIo>;
// keys and values in scan order
type Rows = Vec<(Vec<u8>, Vec<u8>)>;

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
            open_segments: None, segment_bytes: None,
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Variable { most: 64 },
            codec: Codec::None,
            volume: VolumeId(0),
            complete_rows: None,
            sorted_rows: None,
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

// the keys and values of a seeded run, after the jobs have gone quiet
fn contents(workers: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let io = SimulatedIo::new(Seed(1));
    let threads = ThreadCounts { maintenance_workers: Some(workers), ..ThreadCounts::default() };
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
    cool(&store, &io);
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

// a count the budget can't hold is refused, by name
#[test]
fn refused_at_open() {
    let io = SimulatedIo::new(Seed(3));
    let many = ThreadCounts { maintenance_workers: Some(MANY_WORKERS), ..ThreadCounts::default() };
    assert!(matches!(ReelStore::open(io.clone(), &config(many)), Err(StoreError::ThreadsPastBudget { .. })));
}

// more read threads run, and memory_report counts their stacks
#[test]
fn read_threads_follow_the_count() {
    let io = SimulatedIo::new(Seed(4));
    let threads = ThreadCounts { read_threads: 2, ..reel::config::NO_WORKERS };
    let store: Store = ReelStore::open(io, &config(threads)).expect("open");
    assert_eq!(store.state.read_tasks.len(), 2);
    store.close().expect("close");
}

// a worker runs a rewrite the planner hands it, and the store reads the same through it
#[test]
fn workers_run_rewrites() {
    let io = SimulatedIo::new(Seed(5));
    let threads = ThreadCounts { maintenance_workers: Some(2), ..ThreadCounts::default() };
    let store: Store = ReelStore::open(io.clone(), &config(threads)).expect("open");
    for round in 0..ROUNDS {
        for number in 0..KEYS {
            store.put(COLUMN, format!("key-{number:04}").as_bytes(), format!("value-{round}").as_bytes()).expect("put");
            store.put(COLUMN, format!("stays-{round}-{number:04}").as_bytes(), b"value").expect("put");
        }
        store.seal_now();
        settle(&io);
    }
    cool(&store, &io);
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
    most_rewrites(device, false)
}

// the most rewrites that ran at once, with a get on every step when asked, so callers read as busy
fn most_rewrites(device: DeviceClass, is_reading: bool) -> usize {
    let io = SimulatedIo::new(Seed(6));
    let threads = ThreadCounts { maintenance_workers: Some(3), ..ThreadCounts::default() };
    let store: Store = ReelStore::open(io.clone(), &config_on(threads, device)).expect("open");
    let volume = &store.state.volumes[0];
    volume.holds.compaction.store(true, Ordering::Release);
    for round in 0..DEAD_SEGMENTS {
        for number in 0..KEYS {
            store.put(COLUMN, format!("key-{round}-{number:04}").as_bytes(), b"first").expect("put");
        }
        store.seal_now();
        settle(&io);
    }
    cool(&store, &io);
    for round in 0..DEAD_SEGMENTS {
        for number in 0..KEYS / 2 {
            store.put(COLUMN, format!("key-{round}-{number:04}").as_bytes(), b"second").expect("put");
        }
    }
    store.seal_now();
    settle(&io);
    cool(&store, &io);

    volume.holds.compaction.store(false, Ordering::Release);
    store.state.ask_compaction();
    let mut most = 0;
    for _step in 0..STEPS {
        let running = volume.lanes.iter().filter(|lane| hold(&lane.inputs)[0] != SegmentNumber(0)).count();
        most = most.max(running);
        if is_reading {
            let mut buf = Vec::new();
            let _found = store.get(COLUMN, b"key-0-0000", &mut buf).expect("get");
        }
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

fn config_at(threads: ThreadCounts, budget: ByteCount) -> StoreConfig {
    StoreConfig { memory_budget: budget, ..config(threads) }
}

// a store that leaves the count alone runs one worker an online CPU where the budget holds them
#[test]
fn default_runs_at_full_budget() {
    let store: Store = ReelStore::open(SimulatedIo::new(Seed(8)), &config_at(ThreadCounts::default(), ByteCount(512 * 1024 * 1024))).expect("open");
    assert_eq!(store.memory_report().maintenance_workers, default_workers());
    store.close().expect("close");
}

// a default never refuses an open: it comes down to what the budget holds, and the report shows the count that runs
#[test]
fn default_fits_a_small_budget() {
    let small = ByteCount(64 * 1024 * 1024);
    let store: Store = ReelStore::open(SimulatedIo::new(Seed(9)), &config_at(ThreadCounts::default(), small)).expect("open");
    assert!(store.memory_report().maintenance_workers <= default_workers());
    store.close().expect("close");
}

// the same count set by the caller is refused
#[test]
fn set_count_is_refused() {
    let small = ByteCount(64 * 1024 * 1024);
    let threads = ThreadCounts { maintenance_workers: Some(PAST_A_SMALL_BUDGET), ..ThreadCounts::default() };
    let opened: Result<Store, StoreError> = ReelStore::open(SimulatedIo::new(Seed(10)), &config_at(threads, small));
    assert!(matches!(opened, Err(StoreError::ThreadsPastBudget { .. })));
}

// two volumes, each with a column
fn two_volumes(threads: ThreadCounts) -> StoreConfig {
    let volume = |name: &str| VolumeConfig {
        name: name.to_string(),
        roots: vec![PathBuf::from(format!("{ROOT}-{name}"))],
        sync: SyncPolicy::Never,
        device: DeviceClass::Detect, open_segments: None, segment_bytes: None,
    };
    let column = |id: u8, volume: u8| ColumnConfig {
        id: ColumnId(id),
        name: format!("column-{id}"),
        key_shape: KeyShape::Variable { most: 64 },
        codec: Codec::None,
        volume: VolumeId(volume),
        complete_rows: None,
        sorted_rows: None,
        least_value: None,
    };
    StoreConfig {
        memory_budget: TWO_VOLUME_BUDGET,
        threads,
        volumes: vec![volume("a"), volume("b")],
        columns: vec![column(1, 0), column(2, 1)],
    }
}

// step the jobs until they go quiet, and after each step check that a block handed out on any volume has a worker that can run
fn settle_checked(store: &Store, io: &SimulatedIo) {
    let is_assigned = || store.state.volumes.iter().flat_map(|volume| &volume.walk_slots).any(|slot| slot.phase.load(Ordering::SeqCst) == SLOT_ASSIGNED);
    for _step in 0..STEPS {
        let stepped = io.step_any().or_else(|| {
            io.advance_clock(TICK);
            io.step_any()
        });
        assert!(!is_assigned() || io.runnable_jobs("reel-walk") > 0, "a handed-out block waits while every worker sleeps");
        if stepped.is_none() {
            return;
        }
    }
    panic!("jobs never went quiet");
}

// both volumes' keys and values after a seeded run, and the walk blocks each volume counted
fn two_volume_contents(workers: usize) -> (Rows, [u64; 2]) {
    let io = SimulatedIo::new(Seed(11));
    let threads = ThreadCounts { maintenance_workers: Some(workers), ..ThreadCounts::default() };
    let store: Store = ReelStore::open(io.clone(), &two_volumes(threads)).expect("open");
    let mut random = SeededRandom::new(Seed(12));
    for round in 0..ROUNDS {
        for _write in 0..KEYS {
            let column = ColumnId(1 + random.below(2) as u8);
            let key = format!("key-{:04}", random.below(KEYS));
            if random.below(4) == 0 {
                store.delete(column, key.as_bytes()).expect("delete");
            } else {
                store.put(column, key.as_bytes(), format!("value-{round}").as_bytes()).expect("put");
            }
        }
        store.seal_now();
        settle_checked(&store, &io);
    }
    let mut found = Vec::new();
    for column in [ColumnId(1), ColumnId(2)] {
        let mut scan = store.iter_from(column, b"", Direction::Ascending).expect("scan");
        let (mut key, mut value) = (Vec::new(), Vec::new());
        while scan.next_row(&mut key, &mut value).expect("step") == ScanStep::Found {
            found.push((key.clone(), value.clone()));
        }
    }
    let blocks = [0, 1].map(|volume| store.state.volumes[volume].walk_blocks.load(Ordering::Acquire));
    assert_eq!(store.memory_report().maintenance_workers, workers);
    store.close().expect("close");
    (found, blocks)
}

// one set of workers serves both volumes, walks each, and leaves the same keys and values as the planners alone
#[test]
fn workers_serve_every_volume() {
    let (alone, _blocks) = two_volume_contents(0);
    assert!(!alone.is_empty());
    let (shared, blocks) = two_volume_contents(3);
    assert_eq!(shared, alone);
    assert!(blocks.iter().all(|blocks| *blocks > 0), "a volume walked nothing: {blocks:?}");
}

// the writer pauses once a seal lands, so the hot window closes and the rewrites hot ingest put off can start
fn cool(store: &Store, io: &SimulatedIo) {
    let until = store.state.quiet_at(io.clock_reading());
    io.advance_clock(Nanoseconds(until.0.saturating_sub(io.clock_reading().0)));
    settle(io);
}

// callers reading keep merges in flight at the cap, so gets keep their cores, where a quiet volume runs several
#[test]
fn reads_hold_merges_at_the_cap() {
    assert_eq!(most_rewrites(DeviceClass::Solid, true), 1);
}

// the most walk slots holding a block at once over a backlog of unwalked segments, with a get on every step when asked
fn most_walk_slots(is_reading: bool) -> usize {
    let io = SimulatedIo::new(Seed(7));
    let threads = ThreadCounts { maintenance_workers: Some(4), ..ThreadCounts::default() };
    let store: Store = ReelStore::open(io.clone(), &config_on(threads, DeviceClass::Solid)).expect("open");
    let volume = &store.state.volumes[0];
    volume.holds.compaction.store(true, Ordering::Release);
    volume.holds.walk.store(true, Ordering::Release);
    for round in 0..DEAD_SEGMENTS {
        for number in 0..WALK_KEYS {
            store.put(COLUMN, format!("key-{round}-{number:05}").as_bytes(), b"first").expect("put");
        }
        store.seal_now();
        settle(&io);
    }
    cool(&store, &io);
    volume.holds.walk.store(false, Ordering::Release);
    store.state.ask_compaction();
    let mut most = 0;
    for _step in 0..STEPS {
        let busy = volume.walk_slots.iter().filter(|slot| slot.phase.load(Ordering::Acquire) != SLOT_FREE).count();
        most = most.max(busy);
        if is_reading {
            let mut buf = Vec::new();
            let _found = store.get(COLUMN, b"key-0-0000", &mut buf).expect("get");
        }
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

// callers reading keep the walk to its cap, and a quiet volume hands blocks to every worker
#[test]
fn reads_pace_the_walk() {
    assert_eq!(most_walk_slots(true), 1);
    assert!(most_walk_slots(false) > 1);
}
const WALK_KEYS: u64 = 20_000;
