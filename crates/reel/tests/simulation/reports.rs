//! What a store says about itself: sizes from the summaries, and jobs that read as running, idle or stalled

use std::path::PathBuf;
use std::sync::atomic::Ordering;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::job_tally::STALL_AFTER;
use reel::engine::reel_store::ReelStore;
use reel::record::codec::Codec;
use reel_api::column_id::ColumnId;
use reel_api::maintenance_report::{JobKind, JobState, MaintenanceReport};
use reel_api::units::{ByteCount, Nanoseconds, RootId, RowCount, SegmentCount, VolumeId};
use reel_api::usage::{ColumnUsage, UsageReport};
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_io::SimulatedIo;

const ROOT: &str = "/disk/reports";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(64 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 400_000;
const KEYS: u64 = 100;
const VALUE: &[u8] = &[7; 200];

type Store = ReelStore<SimulatedIo>;

fn open(io: &SimulatedIo) -> Store {
    let config = StoreConfig {
        memory_budget: BUDGET,
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "reports".to_string(),
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
            complete_rows: None,
        }],
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

fn seal(store: &Store, io: &SimulatedIo) {
    store.seal_now();
    settle(io);
}

fn put_keys(store: &Store) {
    for number in 0..KEYS {
        store.put(COLUMN, format!("key-{number:03}").as_bytes(), VALUE).expect("put");
    }
}

fn job(report: &MaintenanceReport, kind: JobKind) -> JobState {
    report.jobs[kind.index()].state
}

// sizes follow what's written, replaced and sealed, from the summaries alone
#[test]
fn sizes() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io);
    put_keys(&store);
    // a claim a full window sends back still counted its row, and an estimate can carry that
    assert!(store.key_count_estimate().0.abs_diff(KEYS) <= KEYS / 10);
    seal(&store, &io);
    assert_eq!(store.key_count_estimate(), RowCount(KEYS));
    let live = store.live_data_size_bytes();
    assert!(live.0 >= KEYS * VALUE.len() as u64);

    put_keys(&store);
    seal(&store, &io);
    assert_eq!(store.key_count_estimate(), RowCount(KEYS));
    assert_eq!(store.live_data_size_bytes(), live);

    let mut column = ColumnUsage::default();
    store.column_disk_usage(COLUMN, &mut column).expect("column");
    assert_eq!(column.live, live);
    assert!(column.on_disk >= live);
    assert_eq!(column.roots.len(), 1);
    assert_eq!((column.roots[0].root, column.roots[0].live), (RootId(0), live));

    let mut usage = UsageReport::default();
    store.volume_usage(&mut usage).expect("usage");
    assert_eq!((usage.volumes.len(), usage.roots.len()), (1, 1));
    assert!(usage.volumes[0].used >= live);
    assert_eq!(usage.volumes[0].used, usage.roots[0].used);
    store.close().expect("close");
}

// a job with work standing reads as running, and as stalled once it has moved nothing for an hour
#[test]
fn stall_shows() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io);
    let volume = &store.state.volumes[0];
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    assert_eq!(job(&report, JobKind::DeadByteWalk), JobState::Idle);

    volume.is_walk_held.store(true, Ordering::Release);
    put_keys(&store);
    seal(&store, &io);
    store.maintenance_report(&mut report);
    assert_eq!(job(&report, JobKind::DeadByteWalk), JobState::Running);
    io.advance_clock(STALL_AFTER);
    store.maintenance_report(&mut report);
    assert_eq!(job(&report, JobKind::DeadByteWalk), JobState::Stalled);

    volume.is_walk_held.store(false, Ordering::Release);
    store.state.ask_compaction();
    settle(&io);
    store.maintenance_report(&mut report);
    assert_eq!(job(&report, JobKind::DeadByteWalk), JobState::Idle);
    assert!(report.jobs[JobKind::DeadByteWalk.index()].bytes_moved.0 > 0);
    assert!(report.jobs[JobKind::Seal.index()].bytes_moved.0 > 0);
    assert!(report.jobs[JobKind::RowWriteOut.index()].last_run.is_some());
    store.close().expect("close");
    store.maintenance_report(&mut report);
    assert_eq!(job(&report, JobKind::Seal), JobState::Stopped);
}

// the counts a roll stops on follow what the jobs did
#[test]
fn counts() {
    let io = SimulatedIo::new(Seed(3));
    let store = open(&io);
    put_keys(&store);
    seal(&store, &io);
    for number in 0..KEYS {
        store.delete(COLUMN, format!("key-{number:03}").as_bytes()).expect("delete");
    }
    store.put(COLUMN, b"zz", VALUE).expect("put");
    seal(&store, &io);
    settle(&io);

    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    assert_eq!(report.deletes_since_open, RowCount(KEYS));
    assert_eq!(report.graves_at_open, RowCount(0));
    assert!(report.segments_rewritten.0 + report.segments_unlinked.0 > 0);
    assert_eq!(report.graves_dropped.0 + report.graves_carried.0 > 0, report.segments_rewritten.0 > 0);
    assert_eq!(report.volumes.len(), 1);
    assert!(report.volumes[0].segment_count > SegmentCount(0));
    assert!(report.volumes[0].used_bytes.0 > 0);
    assert_eq!(report.corruption_errors, RowCount(0));
    store.close().expect("close");
    drop(store);

    let store = open(&io);
    settle(&io);
    let mut reopened = MaintenanceReport::new();
    store.maintenance_report(&mut reopened);
    assert_eq!(reopened.deletes_since_open, RowCount(0));
    store.close().expect("close");
}
