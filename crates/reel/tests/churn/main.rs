//! Scans beside writers and every job on a real disk. The simulator runs one thing at a time, and this runs them at once
#![allow(clippy::expect_used, clippy::panic, clippy::print_stdout)]

use std::collections::BTreeMap;
use std::fs;
use std::ops::Bound;
use std::future::Future;
use std::task::{Context, Poll, Waker};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use reel::engine::planner::MOST_WALKLESS_STEPS;
use reel::engine::reel_store::ReelStore;
use reel::engine::store_scan::StoreScan;
use reel::io::plain::PlainIo;
use reel::engine::scan_path::ScanWant;
use reel::{ColumnConfig, from_key, ColumnIndex, KeyShape, StoreConfig, SyncPolicy, ThreadCounts, VolumeConfig};
use reel_core::column_id::ColumnId;
use reel_core::reads::Reads;
use reel_core::direction::Direction;
use reel_core::maintenance_report::{MaintenanceReport, PauseCause};
use reel_core::scan::ScanStep;
use reel_core::units::{ByteCount, RowCount};

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
    // a folder of its own, since its removal at the end would take the folders of the tests running beside it
    let root = scratch_root().join(format!("walks-{has_cue}"));
    fs::create_dir_all(&root).expect("create");
    let store = ReelStore::open(PlainIo::for_tests(), &config(&root, BUDGET)).expect("open");
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
            let mut view = ReelStore::open_read_only(PlainIo::for_tests(), &config).expect("open read-only");
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

const NEW_KEY_TIME: Duration = Duration::from_millis(1_500);
const MODEL_TIME: Duration = Duration::from_secs(3);
const SEAL_EVERY: Duration = Duration::from_millis(3);
const READERS: u64 = 3;
const SCANNERS: u64 = 2;
const KEY_SPACE: u64 = 40_000;
const PRELOADED: u64 = 4_000;
const FIXED_WIDTH: u16 = 16;
const MODEL_VALUE_BYTES: usize = 48;
const WIDE_MODEL_VALUE_BYTES: usize = 6 * 1024;
const MODEL_COLUMN: ColumnId = ColumnId(1);
const SMALL_SEGMENT: ByteCount = ByteCount(4 << 20);
const MODEL_BUDGET: ByteCount = ByteCount(256 << 20);
const NEAREST_KEYS: u64 = 64;
const POLL_WAIT: Duration = Duration::from_micros(20);
const LOG_ENTRIES: usize = 1 << 20;
const VERSION_BITS: u32 = 40;

// a fixed key whose first bytes spread the numbers over the column, and whose last bytes say the number
fn spread_key(number: u64) -> Vec<u8> {
    number.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes().iter().chain(&number.to_be_bytes()).copied().collect()
}

// a value that says its key's number and the version the writer gave it
fn versioned(number: u64, version: u64, value_bytes: usize) -> Vec<u8> {
    let mut value = number.to_be_bytes().to_vec();
    value.extend_from_slice(&version.to_be_bytes());
    value.resize(value_bytes, b'.');
    value
}

fn number_and_version(value: &[u8]) -> (u64, u64) {
    let word = |at: usize| u64::from_be_bytes(value[at..at + 8].try_into().expect("a value starts with two words"));
    (word(0), word(8))
}

// one fixed column on a volume of small segments, so seals come often
fn model_store(root: &std::path::Path, index: ColumnIndex) -> Store {
    let volume = VolumeConfig { segment_bytes: Some(SMALL_SEGMENT), ..VolumeConfig::new("model", vec![root.to_path_buf()], SyncPolicy::Never) };
    let column = ColumnConfig { index, least_rows: Some(RowCount(2 * KEY_SPACE)), ..ColumnConfig::new(MODEL_COLUMN, "model", KeyShape::Fixed { width: FIXED_WIDTH }) };
    let config = StoreConfig { memory_budget: MODEL_BUDGET, max_memory: None, threads: ThreadCounts::default(), volumes: vec![volume], columns: vec![column] };
    ReelStore::open(PlainIo::for_tests(), &config).expect("open")
}

fn block_on<Output>(future: impl Future<Output = Output>) -> Output {
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        thread::sleep(POLL_WAIT);
    }
}

// every new key a writer was told is in comes back by get, scan, async get and contains while seals run
#[test]
fn new_keys_show_to_every_read() {
    for index in [ColumnIndex::Sorted, ColumnIndex::Plain] {
        let root = scratch_root().join(format!("new-keys-{index:?}"));
        fs::create_dir_all(&root).expect("create");
        let store = model_store(&root, index);
        let (mark, is_done, reads, misses) = (AtomicU64::new(0), AtomicBool::new(false), AtomicU64::new(0), AtomicU64::new(0));
        thread::scope(|scope| {
            scope.spawn(|| {
                let mut number = 1;
                while !is_done.load(Ordering::Acquire) && number < KEY_SPACE {
                    store.put(MODEL_COLUMN, &spread_key(number), &versioned(number, 1, MODEL_VALUE_BYTES)).expect("put");
                    mark.store(number, Ordering::Release);
                    number += 1;
                }
            });
            scope.spawn(|| {
                while !is_done.load(Ordering::Acquire) {
                    store.seal_now();
                    thread::sleep(SEAL_EVERY);
                }
            });
            for reader in 0..READERS {
                let (store, mark, is_done, reads, misses) = (&store, &mark, &is_done, &reads, &misses);
                scope.spawn(move || {
                    let mut seed = 0x9E37_79B9_7F4A_7C15_u64 ^ reader;
                    let (mut buf, mut name, mut value) = (Vec::new(), Vec::new(), Vec::new());
                    while !is_done.load(Ordering::Acquire) {
                        let top = mark.load(Ordering::Acquire);
                        if top == 0 {
                            continue;
                        }
                        seed ^= seed << 13;
                        seed ^= seed >> 7;
                        seed ^= seed << 17;
                        // most reads go to the newest keys, where a seal hands rows over
                        let back = if seed.is_multiple_of(4) { seed % top } else { seed % NEAREST_KEYS.min(top) };
                        let key = spread_key(top - back);
                        let found = match (seed >> 32) % 4 {
                            0 => store.get(MODEL_COLUMN, &key, &mut buf).expect("get"),
                            1 => {
                                let mut scan = store.iter_from(MODEL_COLUMN, &key, Direction::Ascending).expect("scan");
                                scan.next_row(&mut name, &mut value).expect("step") == ScanStep::Found && name == key
                            },
                            2 => block_on(store.get_async(MODEL_COLUMN, &key, &mut buf)).expect("get"),
                            _ => store.contains(MODEL_COLUMN, &key).expect("contains"),
                        };
                        reads.fetch_add(1, Ordering::Relaxed);
                        misses.fetch_add(u64::from(!found), Ordering::Relaxed);
                    }
                });
            }
            thread::sleep(NEW_KEY_TIME);
            is_done.store(true, Ordering::Release);
        });
        println!("{index:?}: {} keys, {} reads, {} misses", mark.load(Ordering::Relaxed), reads.load(Ordering::Relaxed), misses.load(Ordering::Relaxed));
        assert!(reads.load(Ordering::Relaxed) > 0);
        assert_eq!(misses.load(Ordering::Relaxed), 0, "{index:?}: an acknowledged key went missing");
        store.close().expect("close");
        drop(store);
        let _removed = fs::remove_dir_all(&root);
    }
}

/// What a writer has been told is in, in the order it was told: each entry a key's number above its version
struct WriteLog {
    entries: Vec<AtomicU64>,
    length: AtomicUsize,
}

impl WriteLog {
    fn push(&self, number: u64, version: u64) -> bool {
        let at = self.length.load(Ordering::Relaxed);
        let Some(entry) = self.entries.get(at) else {
            return false;
        };
        entry.store((number << VERSION_BITS) | version, Ordering::Relaxed);
        self.length.store(at + 1, Ordering::Release);
        true
    }
}

// one scan's look at the log: every key told in so far at its newest version
fn catch_up(log: &WriteLog, read: &mut usize, known: &mut BTreeMap<Vec<u8>, u64>) {
    let length = log.length.load(Ordering::Acquire);
    for entry in &log.entries[*read..length] {
        let word = entry.load(Ordering::Relaxed);
        known.insert(spread_key(word >> VERSION_BITS), word & ((1 << VERSION_BITS) - 1));
    }
    *read = length;
}

// every batch of a scan beside a writer and seals gives each key told in before it, once, in order, at that version or newer
#[test]
fn scans_beside_a_writer_see_every_acknowledged_key() {
    scan_model(MODEL_VALUE_BYTES);
}

// the same, with open records too wide for a slot's window, which a batch reads from the slot file in joined runs
#[test]
fn scans_beside_a_writer_read_wide_open_rows() {
    scan_model(WIDE_MODEL_VALUE_BYTES);
}

fn scan_model(value_bytes: usize) {
    let root = scratch_root().join(format!("scan-model-{value_bytes}"));
    fs::create_dir_all(&root).expect("create");
    let store = model_store(&root, ColumnIndex::Sorted);
    let log = WriteLog { entries: (0..LOG_ENTRIES).map(|_entry| AtomicU64::new(0)).collect(), length: AtomicUsize::new(0) };
    let mut versions = vec![0_u64; KEY_SPACE as usize];
    for number in 0..PRELOADED {
        store.put(MODEL_COLUMN, &spread_key(number), &versioned(number, 1, value_bytes)).expect("put");
        versions[number as usize] = 1;
        assert!(log.push(number, 1));
    }
    let before = store.state.volumes[0].seals_done.load(Ordering::Acquire);
    store.seal_now();
    wait_for_seal(&store, before);
    let (is_done, scans, fills, rows) = (AtomicBool::new(false), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0));
    let (asked, batched, lookups) = (AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0));
    let heads_before = store.state.heads_together.load(Ordering::Relaxed);
    thread::scope(|scope| {
        scope.spawn(|| {
            let (mut fresh, mut seed) = (PRELOADED, 0x2545_F491_4F6C_DD1D_u64);
            while !is_done.load(Ordering::Acquire) {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                // half the writes are keys never seen, the rest overwrite a key already in
                let number = if seed.is_multiple_of(2) && fresh < KEY_SPACE {
                    fresh += 1;
                    fresh - 1
                } else {
                    seed % fresh
                };
                let version = versions[number as usize] + 1;
                store.put(MODEL_COLUMN, &spread_key(number), &versioned(number, version, value_bytes)).expect("put");
                versions[number as usize] = version;
                if !log.push(number, version) {
                    break;
                }
            }
        });
        scope.spawn(|| {
            while !is_done.load(Ordering::Acquire) {
                store.seal_now();
                thread::sleep(SEAL_EVERY);
            }
        });
        for scanner in 0..SCANNERS {
            let (store, log, is_done, scans, fills, rows) = (&store, &log, &is_done, &scans, &fills, &rows);
            let (asked, batched, lookups) = (&asked, &batched, &lookups);
            scope.spawn(move || {
                let (mut name, mut value) = (Vec::new(), Vec::new());
                let mut known = BTreeMap::new();
                let mut read = 0;
                let mut walks = scanner;
                while !is_done.load(Ordering::Acquire) {
                    let direction = if walks.is_multiple_of(2) { Direction::Ascending } else { Direction::Descending };
                    let start = if direction == Direction::Ascending { Vec::new() } else { vec![0xFF; FIXED_WIDTH as usize + 1] };
                    let mut scan = store.state.take_scan(&from_key(MODEL_COLUMN, &start, direction), None).expect("scan");
                    let mut last: Option<Vec<u8>> = None;
                    loop {
                        // a batch locates at a point read after this look, so it sees every key the look found
                        if !scan.batch.has_next() {
                            catch_up(log, &mut read, &mut known);
                            fills.fetch_add(1, Ordering::Relaxed);
                        }
                        let step = store.state.step_scan(&mut scan, &mut name, ScanWant::Value(&mut value)).expect("step");
                        let passed = match (direction, &last) {
                            (Direction::Ascending, None) => known.range::<Vec<u8>, _>(..).next(),
                            (Direction::Ascending, Some(last)) => known.range::<Vec<u8>, _>((Bound::Excluded(last), Bound::Unbounded)).next(),
                            (Direction::Descending, None) => known.range::<Vec<u8>, _>(..).next_back(),
                            (Direction::Descending, Some(last)) => known.range::<Vec<u8>, _>(..last).next_back(),
                        };
                        match step {
                            ScanStep::End => {
                                assert_eq!(passed, None, "an acknowledged key didn't come before the end");
                                break;
                            },
                            ScanStep::Corrupt => panic!("{name:?} came corrupt"),
                            ScanStep::Found => {},
                        }
                        // no acknowledged key lies between the last answer and this one
                        if let Some((skipped, _version)) = passed {
                            let is_skipped = match direction {
                                Direction::Ascending => *skipped < name,
                                Direction::Descending => *skipped > name,
                            };
                            assert!(!is_skipped, "{skipped:?} was in before its batch and the walk passed it for {name:?}");
                        }
                        let (number, version) = number_and_version(&value);
                        assert_eq!(name, spread_key(number), "a value under another key");
                        let told = known.get(&name).copied().unwrap_or(0);
                        assert!(version >= told, "key {number} read version {version} after {told} was in");
                        last = Some(name.clone());
                        rows.fetch_add(1, Ordering::Relaxed);
                    }
                    asked.fetch_add(scan.asked, Ordering::Relaxed);
                    batched.fetch_add(scan.batched, Ordering::Relaxed);
                    lookups.fetch_add(scan.lookups, Ordering::Relaxed);
                    store.state.give_back_scan(scan);
                    scans.fetch_add(1, Ordering::Relaxed);
                    walks += 1;
                }
            });
        }
        thread::sleep(MODEL_TIME);
        is_done.store(true, Ordering::Release);
    });
    let heads = store.state.heads_together.load(Ordering::Relaxed) - heads_before;
    let (scans, fills, rows) = (scans.load(Ordering::Relaxed), fills.load(Ordering::Relaxed), rows.load(Ordering::Relaxed));
    let (asked, batched, lookups) = (asked.load(Ordering::Relaxed), batched.load(Ordering::Relaxed), lookups.load(Ordering::Relaxed));
    println!("{value_bytes} byte values: {scans} scans, {fills} batches, {rows} rows, {heads} heads placed together, {batched} of {asked} rows batched, {lookups} lookups");
    assert!(scans > 0);
    store.close().expect("close");
    drop(store);
    let _removed = fs::remove_dir_all(&root);
}

const BURST_ROWS: u64 = 300;
const BURST_PAUSE: Duration = Duration::from_millis(4);
const BURST_KEYS: u64 = 600_000;
const TOLD_ROWS: u64 = 256;

// a value that says its key's number at the first version
fn burst_value(number: u64) -> Vec<u8> {
    let mut value = number.to_be_bytes().to_vec();
    value.extend_from_slice(&1_u64.to_be_bytes());
    value.resize(MODEL_VALUE_BYTES, b'.');
    value
}

// every key a bursty writer appended inside the walk's range before a batch began is in that batch, or behind the walk
#[test]
fn bursts_between_batches_show() {
    let root = scratch_root().join("bursts");
    fs::create_dir_all(&root).expect("create");
    let store = model_store(&root, ColumnIndex::Sorted);
    let log = WriteLog { entries: (0..LOG_ENTRIES).map(|_entry| AtomicU64::new(0)).collect(), length: AtomicUsize::new(0) };
    for number in 0..PRELOADED {
        store.put(MODEL_COLUMN, &spread_key(number), &burst_value(number)).expect("put");
        assert!(log.push(number, 1));
    }
    let before = store.state.volumes[0].seals_done.load(Ordering::Acquire);
    store.seal_now();
    wait_for_seal(&store, before);
    let (is_done, batches, rows) = (AtomicBool::new(false), AtomicU64::new(0), AtomicU64::new(0));
    thread::scope(|scope| {
        scope.spawn(|| {
            let mut fresh = PRELOADED;
            // a burst then a pause with no write, so the next batch's point is the one the heads were placed at
            while !is_done.load(Ordering::Acquire) && fresh < BURST_KEYS {
                for _row in 0..BURST_ROWS {
                    store.put(MODEL_COLUMN, &spread_key(fresh), &burst_value(fresh)).expect("put");
                    if !log.push(fresh, 1) {
                        return;
                    }
                    fresh += 1;
                }
                thread::sleep(BURST_PAUSE);
            }
        });
        for _scanner in 0..SCANNERS {
            let (store, log, is_done, batches, rows) = (&store, &log, &is_done, &batches, &rows);
            scope.spawn(move || {
                let (mut name, mut value) = (Vec::new(), Vec::new());
                let mut known = BTreeMap::new();
                let mut read = 0;
                let mut start = Vec::new();
                while !is_done.load(Ordering::Acquire) {
                    let range = from_key(MODEL_COLUMN, &start, Direction::Ascending).wanting(RowCount(TOLD_ROWS));
                    let mut scan = store.state.take_scan(&range, None).expect("scan");
                    let mut last: Option<Vec<u8>> = None;
                    let mut taken = 0;
                    while taken < TOLD_ROWS {
                        if !scan.batch.has_next() {
                            catch_up(log, &mut read, &mut known);
                            batches.fetch_add(1, Ordering::Relaxed);
                        }
                        let step = store.state.step_scan(&mut scan, &mut name, ScanWant::Value(&mut value)).expect("step");
                        let passed = match &last {
                            None => known.range::<Vec<u8>, _>(start.clone()..).next(),
                            Some(last) => known.range::<Vec<u8>, _>((Bound::Excluded(last), Bound::Unbounded)).next(),
                        };
                        match step {
                            ScanStep::End => {
                                assert_eq!(passed, None, "an acknowledged key didn't come before the end");
                                break;
                            },
                            ScanStep::Corrupt => panic!("{name:?} came corrupt"),
                            ScanStep::Found => {},
                        }
                        if let Some((skipped, _version)) = passed {
                            assert!(*skipped >= name, "{skipped:?} was in before its batch and the walk passed it for {name:?}");
                        }
                        let (number, _version) = number_and_version(&value);
                        assert_eq!(name, spread_key(number), "a value under another key");
                        last = Some(name.clone());
                        taken += 1;
                    }
                    rows.fetch_add(taken, Ordering::Relaxed);
                    store.state.give_back_scan(scan);
                    start = match last {
                        Some(mut last) if taken == TOLD_ROWS => {
                            last.push(0);
                            last
                        },
                        _other => Vec::new(),
                    };
                }
            });
        }
        thread::sleep(MODEL_TIME);
        is_done.store(true, Ordering::Release);
    });
    let (batches, rows) = (batches.load(Ordering::Relaxed), rows.load(Ordering::Relaxed));
    println!("{batches} batches, {rows} rows beside bursts of {BURST_ROWS}");
    assert!(batches > 0);
    store.close().expect("close");
    drop(store);
    let _removed = fs::remove_dir_all(&root);
}

// tries a timed slot wake gets before the test gives up on a host too loaded to time it
const SLOT_WAKE_TRIES: usize = 20;

// a writer that waits for an open slot sleeps until a slot frees or another writer makes a segment, up to SLOT_WAIT, and not the
// hundred microseconds a wait on the store's progress takes; a slot that freed after the writer read the line ends the wait at once
#[test]
fn a_slot_wait_sleeps_until_a_slot_frees() {
    let root = scratch_root().join("slot-waits");
    fs::create_dir_all(&root).expect("create");
    let store = ReelStore::open(PlainIo::for_tests(), &config(&root, BUDGET)).expect("open");
    let state = &store.state;
    // nothing frees a slot: the wait runs its whole time
    let seen = state.slot_line.seen();
    let started = std::time::Instant::now();
    state.pause_for_slot(PauseCause::NoSegment, seen);
    assert!(started.elapsed() >= reel::engine::reports::SLOT_WAIT / 2, "a slot wait woke after {:?} with no slot", started.elapsed());
    // a slot frees from another thread partway through: the waiter wakes with it, timed from the wake, since a loaded host can
    // oversleep the waker. A wake that came too late in the wait to tell the two apart goes again
    let wait = reel::engine::reports::SLOT_WAIT;
    let mut is_told = false;
    for _try in 0..SLOT_WAKE_TRIES {
        let seen = state.slot_line.seen();
        let started = std::time::Instant::now();
        let woke_at = thread::scope(|scope| {
            let waker = scope.spawn(|| {
                thread::sleep(wait / 4);
                let woke_at = std::time::Instant::now();
                state.slot_line.wake();
                woke_at
            });
            state.pause_for_slot(PauseCause::NoSegment, seen);
            let ended = std::time::Instant::now();
            (waker.join().expect("waker"), ended)
        });
        let (woke_at, ended) = woke_at;
        if woke_at.duration_since(started) > wait / 2 {
            continue;
        }
        assert!(ended.saturating_duration_since(woke_at) < wait / 2, "a slot wait slept past the slot's wake: {:?} after it", ended.saturating_duration_since(woke_at));
        is_told = true;
        break;
    }
    assert!(is_told, "the waker never woke inside the first half of a wait");
    // a slot that freed after the read ends the wait at once
    let seen = state.slot_line.seen();
    let slot = state.take_slot(0).expect("a free slot");
    state.free_slot(slot);
    let started = std::time::Instant::now();
    state.pause_for_slot(PauseCause::Creating, seen);
    assert!(started.elapsed() < reel::engine::reports::SLOT_WAIT / 4, "a wait missed a slot freed before it slept: {:?}", started.elapsed());
    store.close().expect("close");
    drop(store);
    let _ = fs::remove_dir_all(&root);
}
