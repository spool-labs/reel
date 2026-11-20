use std::fs;
use std::process;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use reel::config::{ColumnConfig, ColumnIndex, KeyShape, StoreConfig, SyncPolicy, ThreadCounts, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::io::plain::PlainIo;
use reel_core::column_id::ColumnId;
use reel_core::units::ByteCount;

const COLUMN: ColumnId = ColumnId(1);
const KEYS: u64 = 60_000;

// spread keys over every shard, so sealed and open ones mix along the walk
fn key(number: u64) -> [u8; 8] {
    number.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes()
}

// a writer puts new keys and seals while counts run: each count lies between the keys written before it began and after it
// ended, so no key is counted twice or lost to a seal between a count's looks
#[test]
fn counts_beside_seals_stay_in_bounds() {
    let root = std::env::temp_dir().join(format!("reel-count-race-{}", process::id()));
    let _gone = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("root");
    let config = StoreConfig {
        memory_budget: ByteCount(512 * 1024 * 1024),
        threads: ThreadCounts::default(),
        volumes: vec![VolumeConfig {
            open_segments: Some(2),
            segment_bytes: Some(ByteCount(2 * 1024 * 1024)),
            ..VolumeConfig::new("count", vec![root.clone()], SyncPolicy::Never)
        }],
        columns: vec![ColumnConfig { index: ColumnIndex::Plain, ..ColumnConfig::new(COLUMN, "count", KeyShape::Fixed { width: 8 }) }],
    };
    let store = ReelStore::open(PlainIo::new(), &config).expect("open");
    let written = AtomicU64::new(0);
    let done = AtomicBool::new(false);
    let counts = std::thread::scope(|scope| {
        let (store, written, done) = (&store, &written, &done);
        scope.spawn(move || {
            for number in 0..KEYS {
                store.put(COLUMN, &key(number), &[5_u8; 100]).expect("put");
                written.store(number + 1, Ordering::Release);
                if number % 5_000 == 4_999 {
                    store.seal_now();
                }
            }
            done.store(true, Ordering::Release);
        });
        let counter = scope.spawn(move || {
            let mut counts = 0;
            while !done.load(Ordering::Acquire) {
                let low = written.load(Ordering::Acquire);
                let count = store.count_prefix(COLUMN, &[]).expect("count").0;
                let high = written.load(Ordering::Acquire);
                assert!(low <= count && count <= high, "count {count} outside {low}..={high}");
                counts += 1;
            }
            counts
        });
        counter.join().expect("counter")
    });
    assert!(counts > 10, "only {counts} counts ran");
    assert_eq!(store.count_prefix(COLUMN, &[]).expect("count").0, KEYS);
    store.close().expect("close");
    let _gone = fs::remove_dir_all(&root);
}

// open a store for a race in its own directory
fn race_store(name: &str) -> (ReelStore<PlainIo>, std::path::PathBuf) {
    let root = std::env::temp_dir().join(format!("reel-{name}-{}", process::id()));
    let _gone = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("root");
    let config = StoreConfig {
        memory_budget: ByteCount(512 * 1024 * 1024),
        threads: ThreadCounts::default(),
        volumes: vec![VolumeConfig {
            open_segments: Some(2),
            segment_bytes: Some(ByteCount(2 * 1024 * 1024)),
            ..VolumeConfig::new("walk", vec![root.clone()], SyncPolicy::Never)
        }],
        columns: vec![ColumnConfig { index: ColumnIndex::Plain, ..ColumnConfig::new(COLUMN, "walk", KeyShape::Fixed { width: 8 }) }],
    };
    (ReelStore::open(PlainIo::new(), &config).expect("open"), root)
}

// a writer puts new keys and seals while walks run: every key that landed before a walk began appears once, and no key twice
fn walks_beside_seals(with_values: bool) {
    let (store, root) = race_store(if with_values { "walk-values" } else { "walk-keys" });
    let written = AtomicU64::new(0);
    let done = AtomicBool::new(false);
    let walks = std::thread::scope(|scope| {
        let (store, written, done) = (&store, &written, &done);
        scope.spawn(move || {
            for number in 0..KEYS {
                store.put(COLUMN, &key(number), &[5_u8; 100]).expect("put");
                written.store(number + 1, Ordering::Release);
                if number % 5_000 == 4_999 {
                    store.seal_now();
                }
            }
            done.store(true, Ordering::Release);
        });
        let walker = scope.spawn(move || {
            let mut walks = 0;
            let mut seen = std::collections::HashSet::new();
            while !done.load(Ordering::Acquire) {
                let low = written.load(Ordering::Acquire);
                seen.clear();
                let mut scan = store.iter_from(COLUMN, &[], reel_core::direction::Direction::Ascending).expect("scan");
                let (mut name, mut value) = (Vec::new(), Vec::new());
                loop {
                    let step = if with_values { scan.next_row(&mut name, &mut value) } else { scan.next_key(&mut name) };
                    match step.expect("step") {
                        reel_core::scan::ScanStep::Found => assert!(seen.insert(name.clone()), "key {name:?} walked twice"),
                        _end => break,
                    }
                }
                let missing: Vec<u64> = (0..low).filter(|number| !seen.contains(key(*number).as_slice())).collect();
                assert!(missing.is_empty(), "values {with_values}: {} of {low} landed keys missing, first {:?}", missing.len(), missing.first());
                walks += 1;
            }
            walks
        });
        walker.join().expect("walker")
    });
    assert!(walks > 3, "only {walks} walks ran");
    store.close().expect("close");
    let _gone = fs::remove_dir_all(&root);
}

#[test]
fn key_walks_beside_seals_see_every_key_once() {
    walks_beside_seals(false);
}

#[test]
fn value_walks_beside_seals_see_every_key_once() {
    walks_beside_seals(true);
}
