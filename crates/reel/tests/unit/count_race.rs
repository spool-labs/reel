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
// Rounds a reader runs at least, so a fast writer can't finish before the race is run
const LEAST_RUNS: u64 = 12;
// Keys a writer puts at most while it waits for the reader's rounds
const MOST_KEYS: u64 = 20 * KEYS;
// Keys a writer past KEYS stays ahead of the reader's finished rounds, so every round sees writes and seals land and none outlasts
// the writer
const PACE_KEYS: u64 = 20_000;

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
        max_memory: None,
        threads: ThreadCounts::default(),
        volumes: vec![VolumeConfig {
            open_segments: Some(2),
            segment_bytes: Some(ByteCount(2 * 1024 * 1024)),
            ..VolumeConfig::new("count", vec![root.clone()], SyncPolicy::Never)
        }],
        columns: vec![ColumnConfig { index: ColumnIndex::Plain, ..ColumnConfig::new(COLUMN, "count", KeyShape::Fixed { width: 8 }) }],
    };
    let store = ReelStore::open(PlainIo::for_tests(), &config).expect("open");
    // rows the writer has acknowledged, and rows it has started: a put is seen once it returns, a moment before `written` moves
    let written = AtomicU64::new(0);
    let started = AtomicU64::new(0);
    let done = AtomicBool::new(false);
    let runs = AtomicU64::new(0);
    let counts = std::thread::scope(|scope| {
        let (store, written, started, done, runs) = (&store, &written, &started, &done, &runs);
        scope.spawn(move || {
            // a writer that panics still stops the counter, so the test fails and doesn't hang
            let _stops = StopOnDrop(done);
            let mut number = 0;
            // the writer goes on past KEYS until the reader has run enough rounds to have raced it
            while number < KEYS || (runs.load(Ordering::Acquire) < LEAST_RUNS && number < MOST_KEYS) {
                started.store(number + 1, Ordering::Release);
                store.put(COLUMN, &key(number), &[5_u8; 100]).expect("put");
                written.store(number + 1, Ordering::Release);
                if number % 5_000 == 4_999 {
                    store.seal_now();
                }
                number += 1;
                pace(number, runs, done);
            }
        });
        let counter = scope.spawn(move || {
            // a reader that panics stops the writer waiting on it
            let _stops = StopOnDrop(done);
            let mut counts = 0;
            while !done.load(Ordering::Acquire) {
                let low = written.load(Ordering::Acquire);
                let count = store.count_prefix(COLUMN, &[]).expect("count").0;
                let high = started.load(Ordering::Acquire);
                assert!(low <= count && count <= high, "count {count} outside {low}..={high}");
                counts += 1;
                runs.fetch_add(1, Ordering::Release);
            }
            counts
        });
        counter.join().expect("counter")
    });
    assert!(counts > 3, "only {counts} counts ran");
    assert_eq!(store.count_prefix(COLUMN, &[]).expect("count").0, written.load(Ordering::Acquire));
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
        max_memory: None,
        threads: ThreadCounts::default(),
        volumes: vec![VolumeConfig {
            open_segments: Some(2),
            segment_bytes: Some(ByteCount(2 * 1024 * 1024)),
            ..VolumeConfig::new("walk", vec![root.clone()], SyncPolicy::Never)
        }],
        columns: vec![ColumnConfig { index: ColumnIndex::Plain, ..ColumnConfig::new(COLUMN, "walk", KeyShape::Fixed { width: 8 }) }],
    };
    (ReelStore::open(PlainIo::for_tests(), &config).expect("open"), root)
}

// a writer puts new keys and seals while walks run: every key that landed before a walk began appears once, and no key twice
fn walks_beside_seals(with_values: bool) {
    walks_beside_seals_and(with_values, false);
}

// walks beside seals, and with `gives_back` a thread that gives the emptied node pages back to the kernel all the while
fn walks_beside_seals_and(with_values: bool, gives_back: bool) {
    let name = match (with_values, gives_back) {
        (_, true) => "walk-give-back",
        (true, false) => "walk-values",
        (false, false) => "walk-keys",
    };
    let (store, root) = race_store(name);
    let written = AtomicU64::new(0);
    let done = AtomicBool::new(false);
    let runs = AtomicU64::new(0);
    // with `gives_back` the writer goes on until a give-back has happened beside the walks, however slow a loaded host makes it
    let has_given = AtomicBool::new(!gives_back);
    let walks = std::thread::scope(|scope| {
        let (store, written, done, runs, has_given) = (&store, &written, &done, &runs, &has_given);
        scope.spawn(move || {
            // a writer that panics still stops the counter, so the test fails and doesn't hang
            let _stops = StopOnDrop(done);
            let mut number = 0;
            // the writer goes on past KEYS until the reader has run enough rounds to have raced it, and the give-back has come
            let is_racing = || runs.load(Ordering::Acquire) < LEAST_RUNS || !has_given.load(Ordering::Acquire);
            while number < KEYS || (is_racing() && number < MOST_KEYS) {
                store.put(COLUMN, &key(number), &[5_u8; 100]).expect("put");
                written.store(number + 1, Ordering::Release);
                if number % 5_000 == 4_999 {
                    store.seal_now();
                }
                number += 1;
                pace(number, runs, done);
            }
        });
        if gives_back {
            scope.spawn(move || {
                let mut given = 0;
                // the reserve job gives the pages back on its own thread, as a store does, and this asks it each round
                while !done.load(Ordering::Acquire) {
                    store.state.release_node_pages.store(true, Ordering::Release);
                    if let Some(task) = store.state.reserve_task.get() {
                        reel::io::IoTask::wake(task);
                    }
                    let is_given = store.memory_report().pool_given_back.0 > 0;
                    given += u64::from(is_given);
                    if is_given {
                        has_given.store(true, Ordering::Release);
                    }
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                assert!(given > 0, "no node page went back beside the walks");
            });
        }
        let walker = scope.spawn(move || {
            // a reader that panics stops the writer waiting on it
            let _stops = StopOnDrop(done);
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
                runs.fetch_add(1, Ordering::Release);
            }
            walks
        });
        walker.join().expect("walker")
    });
    assert!(walks > 1, "only {walks} walks ran");
    store.close().expect("close");
    let _gone = fs::remove_dir_all(&root);
}

// node pages a seal emptied go back to the kernel while walks run beside the seals, and every walk sees every landed key once
#[test]
fn walks_beside_node_pages_given_back() {
    walks_beside_seals_and(true, true);
}

#[test]
fn key_walks_beside_seals_see_every_key_once() {
    walks_beside_seals(false);
}

#[test]
fn value_walks_beside_seals_see_every_key_once() {
    walks_beside_seals(true);
}

// a writer puts, deletes and seals a sorted column while cues count it: each cue's count equals its own walk's keys, so a seal
// dropping an entry during a count from the index never shows
#[test]
fn cue_counts_beside_deleting_seals_match_their_walks() {
    let root = std::env::temp_dir().join(format!("reel-count-sorted-{}", process::id()));
    let _gone = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("root");
    let config = StoreConfig {
        memory_budget: ByteCount(512 * 1024 * 1024),
        max_memory: None,
        threads: ThreadCounts::default(),
        volumes: vec![VolumeConfig {
            open_segments: Some(2),
            segment_bytes: Some(ByteCount(2 * 1024 * 1024)),
            ..VolumeConfig::new("sorted", vec![root.clone()], SyncPolicy::Never)
        }],
        columns: vec![ColumnConfig { index: ColumnIndex::Sorted, ..ColumnConfig::new(COLUMN, "sorted", KeyShape::Fixed { width: 8 }) }],
    };
    let store = ReelStore::open(PlainIo::for_tests(), &config).expect("open");
    let done = AtomicBool::new(false);
    let runs = AtomicU64::new(0);
    let counts = std::thread::scope(|scope| {
        let (store, done, runs) = (&store, &done, &runs);
        scope.spawn(move || {
            // a writer that panics still stops the counter, so the test fails and doesn't hang
            let _stops = StopOnDrop(done);
            let mut number = 0;
            // the writer goes on past KEYS until the reader has run enough rounds to have raced it
            while number < KEYS || (runs.load(Ordering::Acquire) < LEAST_RUNS && number < MOST_KEYS) {
                store.put(COLUMN, &key(number), &[5_u8; 100]).expect("put");
                if number >= 500 && number % 3 == 0 {
                    store.delete(COLUMN, &key(number - 500)).expect("delete");
                }
                if number % 5_000 == 4_999 {
                    store.seal_now();
                    // a pause lets the row job enter what landed, so counts take the index
                    std::thread::sleep(std::time::Duration::from_millis(80));
                }
                number += 1;
                pace(number, runs, done);
            }
        });
        let counter = scope.spawn(move || {
            // a reader that panics stops the writer waiting on it
            let _stops = StopOnDrop(done);
            let mut counts = 0;
            while !done.load(Ordering::Acquire) {
                let cue = store.cue().expect("cue");
                let counted = cue.count_prefix(COLUMN, &[]).expect("count").0;
                let mut scan = cue.iter_from(COLUMN, &[], reel_core::direction::Direction::Ascending).expect("scan");
                let (mut walked, mut name, mut names) = (0, Vec::new(), Vec::new());
                while scan.next_key(&mut name).expect("step") == reel_core::scan::ScanStep::Found {
                    walked += 1;
                    names.push(name.clone());
                }
                if counted != walked {
                    let mut value = Vec::new();
                    let live: Vec<u64> = (0..KEYS).filter(|number| cue.get(COLUMN, &key(*number), &mut value).expect("get")).collect();
                    let walked_dead: Vec<&Vec<u8>> = names.iter().filter(|name| !cue.get(COLUMN, name, &mut value).expect("get")).collect();
                    let twice: Vec<(usize, &Vec<u8>)> = names.iter().enumerate().skip(1).filter(|(at, name)| names[at - 1] >= **name).collect();
                    panic!("count {counted} walk {walked} gets {} walked keys a get misses {} out of order {:?}", live.len(), walked_dead.len(), twice.iter().map(|(at, name)| (*at, names[at - 1].clone(), (*name).clone())).collect::<Vec<_>>());
                }
                counts += 1;
                runs.fetch_add(1, Ordering::Release);
            }
            counts
        });
        counter.join().expect("counter")
    });
    assert!(counts > 3, "only {counts} counts ran");
    assert!(store.state.index_counts.load(Ordering::Relaxed) > 0, "no count came from the index");
    store.close().expect("close");
    let _gone = fs::remove_dir_all(&root);
}

// one thread drops a sorted index and refills it, again and again, while a writer puts and seals and two readers walk and count:
// every key that landed before a walk began appears once, and a count lies between the keys landed before it and those started
#[test]
fn walks_and_counts_beside_drops_and_refills() {
    let root = std::env::temp_dir().join(format!("reel-drop-race-{}", process::id()));
    let _gone = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("root");
    let config = StoreConfig {
        memory_budget: ByteCount(512 * 1024 * 1024),
        max_memory: None,
        threads: ThreadCounts::default(),
        volumes: vec![VolumeConfig {
            open_segments: Some(2),
            segment_bytes: Some(ByteCount(2 * 1024 * 1024)),
            ..VolumeConfig::new("drop", vec![root.clone()], SyncPolicy::Never)
        }],
        columns: vec![ColumnConfig { index: ColumnIndex::Sorted, ..ColumnConfig::new(COLUMN, "drop", KeyShape::Fixed { width: 8 }) }],
    };
    let store = ReelStore::open(PlainIo::for_tests(), &config).expect("open");
    let written = AtomicU64::new(0);
    let started = AtomicU64::new(0);
    let done = AtomicBool::new(false);
    let runs = AtomicU64::new(0);
    let drops = AtomicU64::new(0);
    std::thread::scope(|scope| {
        let (store, written, started, done, runs, drops) = (&store, &written, &started, &done, &runs, &drops);
        scope.spawn(move || {
            let _stops = StopOnDrop(done);
            let mut number = 0;
            while number < KEYS || ((runs.load(Ordering::Acquire) < 4 * LEAST_RUNS || drops.load(Ordering::Acquire) < 40) && number < MOST_KEYS) {
                started.store(number + 1, Ordering::Release);
                store.put(COLUMN, &key(number), &[5_u8; 100]).expect("put");
                written.store(number + 1, Ordering::Release);
                if number % 5_000 == 4_999 {
                    store.seal_now();
                }
                number += 1;
                pace(number, runs, done);
            }
        });
        scope.spawn(move || {
            let _stops = StopOnDrop(done);
            let index = store.state.sorted_index(COLUMN).expect("a sorted index");
            while !done.load(Ordering::Acquire) {
                let _freed = store.state.drop_sorted(index);
                drops.fetch_add(1, Ordering::Relaxed);
                std::thread::yield_now();
                index.set_may_refill(true);
                if let Some(mut refill) = store.state.start_sorted_refill(0) {
                    while store.state.sorted_refill_step(0, &mut refill).expect("refill step") {}
                }
                std::thread::yield_now();
            }
        });
        let walker = scope.spawn(move || {
            let _stops = StopOnDrop(done);
            let mut seen = std::collections::HashSet::new();
            while !done.load(Ordering::Acquire) {
                let low = written.load(Ordering::Acquire);
                seen.clear();
                let mut scan = store.iter_from(COLUMN, &[], reel_core::direction::Direction::Ascending).expect("scan");
                let (mut name, mut value) = (Vec::new(), Vec::new());
                while scan.next_row(&mut name, &mut value).expect("step") == reel_core::scan::ScanStep::Found {
                    assert!(seen.insert(name.clone()), "key {name:?} walked twice");
                }
                let missing = (0..low).filter(|number| !seen.contains(key(*number).as_slice())).count();
                assert_eq!(missing, 0, "{missing} of {low} landed keys missing from a walk");
                runs.fetch_add(1, Ordering::Release);
            }
        });
        let counter = scope.spawn(move || {
            let _stops = StopOnDrop(done);
            while !done.load(Ordering::Acquire) {
                let low = written.load(Ordering::Acquire);
                let count = store.count_prefix(COLUMN, &[]).expect("count").0;
                let high = started.load(Ordering::Acquire);
                assert!(low <= count && count <= high, "count {count} outside {low}..={high}");
                runs.fetch_add(1, Ordering::Release);
            }
        });
        walker.join().expect("walker");
        counter.join().expect("counter");
    });
    assert!(drops.load(Ordering::Relaxed) > 3, "only {} drops ran", drops.load(Ordering::Relaxed));
    // with the writer gone a refill finishes and clears the mark, and the index answers every key
    let index = store.state.sorted_index(COLUMN).expect("a sorted index");
    store.seal_now();
    index.set_may_refill(true);
    for _try in 0..500 {
        if index.mark().is_none() {
            break;
        }
        index.set_may_refill(true);
        if let Some(mut refill) = store.state.start_sorted_refill(0) {
            while store.state.sorted_refill_step(0, &mut refill).expect("refill step") {}
        } else {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
    let report = store.memory_report();
    let volume = &store.state.volumes[0];
    assert_eq!(index.mark(), None, "no refill cleared the mark: pressed {} drops {} held {} target {} waits {} sealing {} output floor {} dropped {} may {} due {} skips {}", report.is_under_pressure, report.sorted_drops, report.memory_held.0 >> 20, report.memory_target.0 >> 20, index.refill_waits(), volume.sealing.load(Ordering::Acquire), volume.output_floor(), index.is_dropped(), index.may_refill(), index.is_refill_due(reel::io::Io::now(&store.state.io).0), index.refill_skips());
    let mut value = Vec::new();
    let lost = (0..written.load(Ordering::Acquire)).filter(|number| !store.get(COLUMN, &key(*number), &mut value).expect("get")).count();
    assert_eq!(lost, 0, "{lost} keys lost after the refill");
    assert_eq!(store.count_prefix(COLUMN, &[]).expect("count").0, written.load(Ordering::Acquire));
    store.close().expect("close");
    let _gone = fs::remove_dir_all(&root);
}

// one thread gives the spot index and the block caches back and brings them back, again and again, while a writer puts and seals a
// plain column and readers get landed keys and walk: every landed key reads, and a walk sees each landed key once
#[test]
fn gets_and_walks_beside_caches_given_back() {
    let (store, root) = race_store("caches");
    let written = AtomicU64::new(0);
    let done = AtomicBool::new(false);
    let runs = AtomicU64::new(0);
    let rounds = AtomicU64::new(0);
    std::thread::scope(|scope| {
        let (store, written, done, runs, rounds) = (&store, &written, &done, &runs, &rounds);
        scope.spawn(move || {
            let _stops = StopOnDrop(done);
            let mut number = 0;
            while number < KEYS || ((runs.load(Ordering::Acquire) < 4 * LEAST_RUNS || rounds.load(Ordering::Acquire) < 40) && number < MOST_KEYS) {
                store.put(COLUMN, &key(number), &number.to_le_bytes()).expect("put");
                written.store(number + 1, Ordering::Release);
                if number % 5_000 == 4_999 {
                    store.seal_now();
                }
                number += 1;
                pace(number, runs, done);
            }
        });
        scope.spawn(move || {
            let _stops = StopOnDrop(done);
            while !done.load(Ordering::Acquire) {
                let _given = reel::engine::reserve_job::give_back_caches(&store.state, usize::MAX);
                std::thread::yield_now();
                while reel::engine::reserve_job::bring_back_caches(&store.state, u64::MAX) > 0 {}
                rounds.fetch_add(1, Ordering::Relaxed);
                std::thread::yield_now();
            }
        });
        let getter = scope.spawn(move || {
            let _stops = StopOnDrop(done);
            let mut value = Vec::new();
            let mut number = 0_u64;
            while !done.load(Ordering::Acquire) {
                let low = written.load(Ordering::Acquire);
                if low == 0 {
                    continue;
                }
                number = number.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
                let asked = number % low;
                assert!(store.get(COLUMN, &key(asked), &mut value).expect("get"), "landed key {asked} of {low} missing");
                assert_eq!(value, asked.to_le_bytes(), "key {asked} read a wrong value");
            }
        });
        let walker = scope.spawn(move || {
            let _stops = StopOnDrop(done);
            let mut seen = std::collections::HashSet::new();
            while !done.load(Ordering::Acquire) {
                let low = written.load(Ordering::Acquire);
                seen.clear();
                let mut scan = store.iter_from(COLUMN, &[], reel_core::direction::Direction::Ascending).expect("scan");
                let (mut name, mut value) = (Vec::new(), Vec::new());
                while scan.next_row(&mut name, &mut value).expect("step") == reel_core::scan::ScanStep::Found {
                    assert!(seen.insert(name.clone()), "key {name:?} walked twice");
                }
                let missing = (0..low).filter(|number| !seen.contains(key(*number).as_slice())).count();
                assert_eq!(missing, 0, "{missing} of {low} landed keys missing from a walk");
                runs.fetch_add(1, Ordering::Release);
            }
        });
        getter.join().expect("getter");
        walker.join().expect("walker");
    });
    assert!(rounds.load(Ordering::Relaxed) > 3, "only {} rounds ran", rounds.load(Ordering::Relaxed));
    store.close().expect("close");
    let _gone = fs::remove_dir_all(&root);
}

/// Sets a flag when dropped, so the threads waiting on a writer stop when it ends, panic or not
struct StopOnDrop<'flag>(&'flag AtomicBool);

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

/// Wait while the writer is a pace ahead of the reader, until the reader has run its rounds or stopped
fn pace(number: u64, runs: &AtomicU64, done: &AtomicBool) {
    while number >= KEYS
        && number >= KEYS + (runs.load(Ordering::Acquire) + 1) * PACE_KEYS
        && runs.load(Ordering::Acquire) < LEAST_RUNS
        && !done.load(Ordering::Acquire)
    {
        std::thread::yield_now();
    }
}
