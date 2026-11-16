//! Scans beside writers and every job on a real disk. The simulator runs one thing at a time, and this runs them at once
#![allow(clippy::expect_used, clippy::panic, clippy::print_stdout)]

use std::fs;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use reel::engine::planner::MOST_WALKLESS_STEPS;
use reel::engine::reel_store::ReelStore;
use reel::engine::store_scan::StoreScan;
use reel::io::plain::PlainIo;
use reel_core::direction::Direction;
use reel_core::maintenance_report::MaintenanceReport;
use reel_core::scan::ScanStep;
use reel_core::units::ByteCount;

#[path = "../steady/store_config.rs"]
mod store_config;

use store_config::{META, config, scratch_root};

const BUDGET: ByteCount = ByteCount(192 * 1024 * 1024);
const KEPT_KEYS: u64 = 3_000;
const GONE_KEYS: u64 = 1_000;
const WRITE_ROUNDS: u64 = 80;
const VALUE_BYTES: usize = 120;
const SEAL_WAIT: Duration = Duration::from_millis(1);
const MOST_SEAL_WAITS: u32 = 20_000;
const MOST_REWRITE_WAITS: u32 = 20_000;
const HIGH_KEY: &[u8] = b"zzz";
const FIRST_ROUND: u64 = 0;
const MOST_VIEWS: u32 = 20;

type Store = ReelStore<PlainIo>;

fn kept(number: u64) -> Vec<u8> {
    format!("kept-{number:06}").into_bytes()
}

fn gone(number: u64) -> Vec<u8> {
    format!("gone-{number:06}").into_bytes()
}

fn spanned(number: u64) -> Vec<u8> {
    format!("span-{number:06}").into_bytes()
}

fn value_of(key: &[u8], round: u64) -> Vec<u8> {
    let mut value = key.to_vec();
    value.extend_from_slice(format!("-{round:06}-").as_bytes());
    value.resize(VALUE_BYTES, b'.');
    value
}

fn wait_for_seal(store: &Store, before: u64) {
    let mut waits = 0;
    while store.state.volumes[0].seals_done.load(Ordering::Acquire) == before && waits < MOST_SEAL_WAITS {
        waits += 1;
        thread::sleep(SEAL_WAIT);
    }
}

// every kept key comes once, in order, with a value of its own, and nothing deleted comes at all
fn check_walk(who: &str, mut scan: StoreScan<PlainIo>, direction: Direction, round: Option<u64>) {
    let (mut key, mut value, mut last) = (Vec::new(), Vec::new(), Vec::new());
    let mut next = if direction == Direction::Ascending { 0 } else { KEPT_KEYS };
    loop {
        match scan.next_row(&mut key, &mut value).expect("step") {
            ScanStep::End => break,
            ScanStep::Corrupt => panic!("{key:?} came corrupt"),
            ScanStep::Found => {},
        }
        assert!(last.is_empty() || (direction == Direction::Ascending) == (key > last), "{key:?} came after {last:?}");
        last.clone_from(&key);
        let expected = match direction {
            Direction::Ascending => {
                next += 1;
                kept(next - 1)
            },
            Direction::Descending => {
                next -= 1;
                kept(next)
            },
        };
        assert_eq!(key, expected, "{who}: {direction:?} walk at a cue {}", round.is_some());
        assert!(value.starts_with(&key), "{key:?} read {value:?}");
        if let Some(round) = round {
            assert_eq!(value, value_of(&key, round));
        }
    }
    let end = if direction == Direction::Ascending { KEPT_KEYS } else { 0 };
    assert_eq!(next, end);
}

// walks in both directions see what they must, and one at a cue sees its point, while seals, walks and merges run under them
#[test]
fn scans_beside_every_job() {
    let with_cue = churn(true);
    let alone = churn(false);
    assert!(with_cue.rewrites > 0);
    assert!(alone.unlinks + alone.rewrites > 0);
}

/// What the jobs did under the walks
struct JobsRun {
    unlinks: u64,
    rewrites: u64,
}

fn churn(has_cue: bool) -> JobsRun {
    let root = scratch_root();
    let store = ReelStore::open(PlainIo::new(), &config(&root, BUDGET)).expect("open");
    for number in 0..KEPT_KEYS {
        let key = kept(number);
        store.put(META, &key, &value_of(&key, FIRST_ROUND)).expect("put");
    }
    for number in 0..GONE_KEYS {
        store.put(META, &gone(number), b"gone").expect("put");
        store.put(META, &spanned(number), b"gone").expect("put");
    }
    let before = store.state.volumes[0].seals_done.load(Ordering::Acquire);
    store.seal_now();
    wait_for_seal(&store, before);
    for number in 0..GONE_KEYS {
        store.delete(META, &gone(number)).expect("delete");
    }
    store.delete_range(META, &spanned(0), &spanned(GONE_KEYS)).expect("delete");
    // a view reads the rows files, and a write reaches them within 100 ms of returning
    store.flush().expect("flush");

    let cue = has_cue.then(|| store.cue().expect("cue"));
    let is_done = AtomicBool::new(false);
    let walks = AtomicU64::new(0);
    let mut views = 0;
    thread::scope(|scope| {
        let walker = |direction: Direction, start: &'static [u8]| {
            let (store, is_done, walks) = (&store, &is_done, &walks);
            move || {
                while !is_done.load(Ordering::Acquire) {
                    check_walk("live", store.iter_from(META, start, direction).expect("scan"), direction, None);
                    walks.fetch_add(1, Ordering::Relaxed);
                }
            }
        };
        let viewer = scope.spawn(|| {
            let mut views = 0;
            let config = config(&root, BUDGET);
            let mut view = ReelStore::open_read_only(PlainIo::new(), &config).expect("open read-only");
            while !is_done.load(Ordering::Acquire) && views < MOST_VIEWS {
                check_walk("view", view.iter_from(META, b"", Direction::Ascending).expect("scan"), Direction::Ascending, None);
                view.refresh().expect("refresh");
                views += 1;
            }
            view.close().expect("close");
            views
        });
        let up = scope.spawn(walker(Direction::Ascending, b""));
        let down = scope.spawn(walker(Direction::Descending, HIGH_KEY));
        let at_cue = scope.spawn(|| {
            while let Some(cue) = cue.as_ref().filter(|_cue| !is_done.load(Ordering::Acquire)) {
                check_walk("cue", cue.iter_from(META, b"", Direction::Ascending).expect("scan"), Direction::Ascending, Some(FIRST_ROUND));
                check_walk("cue", cue.iter_from(META, HIGH_KEY, Direction::Descending).expect("scan"), Direction::Descending, Some(FIRST_ROUND));
            }
        });
        for round in 1..=WRITE_ROUNDS {
            for number in 0..KEPT_KEYS {
                let key = kept(number);
                store.put(META, &key, &value_of(&key, round)).expect("put");
            }
            store.seal_now();
        }
        // the merges hot ingest put off run once the writes stop, still under the walks
        let volume = &store.state.volumes[0];
        for _wait in 0..MOST_REWRITE_WAITS {
            if volume.segments_rewritten.load(Ordering::Acquire) > 0 {
                break;
            }
            thread::sleep(SEAL_WAIT);
        }
        is_done.store(true, Ordering::Release);
        views = viewer.join().expect("join");
        for walker in [up, down, at_cue] {
            walker.join().expect("join");
        }
    });
    drop(cue);

    let volume = &store.state.volumes[0];
    let run = JobsRun {
        unlinks: volume.segments_unlinked.load(Ordering::Acquire),
        rewrites: volume.segments_rewritten.load(Ordering::Acquire),
    };
    println!("{} walks and {views} views beside {} unlinks and {} rewrites", walks.load(Ordering::Relaxed), run.unlinks, run.rewrites);
    assert!(walks.load(Ordering::Relaxed) > 0);
    assert_eq!(store.state.cues.refused_reads(), 0);
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    assert!(report.longest_walkless_steps <= MOST_WALKLESS_STEPS, "the planner held the walk back {} steps", report.longest_walkless_steps);
    store.close().expect("close");
    drop(store);
    let _removed = fs::remove_dir_all(&root);
    run
}
