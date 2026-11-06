//! The caller's thread counts: any count gives the same store, and a count the budget can't hold is refused at open

use std::path::PathBuf;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, ThreadCounts, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::record::codec::Codec;
use reel_api::column_id::ColumnId;
use reel_api::direction::Direction;
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

type Store = ReelStore<SimulatedIo>;

fn config(threads: ThreadCounts) -> StoreConfig {
    StoreConfig {
        memory_budget: BUDGET,
        threads,
        volumes: vec![VolumeConfig {
            name: "threads".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
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
