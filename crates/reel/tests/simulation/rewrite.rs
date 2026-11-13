//! Rewrite: a half-dead segment becomes a new one with only what a read can reach, through any crash

use std::future::{Future, ready};
use std::io::{IoSlice, IoSliceMut};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::shape;
use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::compact_job::{CompactJob, can_unlink, park};
use reel::engine::cue::Cue;
use reel::engine::load_state::DISK_CHECK_PERIOD;
use reel::engine::open_segment::{OUTPUT_SUFFIX, data_path};
use reel::engine::read_path::{RecordWant, WalkAsk};
use reel::engine::planner::{MOST_WALKLESS_STEPS, Planner};
use reel::engine::unit_bounds::unit_bound;
use reel::engine::reel_store::ReelStore;
use reel::engine::reserve_job::RESERVE_PERIOD;
use reel::engine::row_job::IDLE_SEAL_AFTER;
use reel::engine::dead_bits::DECIDED_SET;
use reel::engine::rewrite_job::{MERGE_BUFFER_BYTES, MergeTarget, MOST_OVERLAP, MOST_PARK_WAIT, PLANNER_LANE, RewriteJob, RewritePhase, is_overlap_input, overlaps, rows_of};
use reel::lifetime::MarkRange;
use reel::engine::volume::{NO_SEAL, Volume};
use reel::engine::walk_job::{BlockCounter, WalkJob, WalkWorker};
use reel::guard::hold;
use reel::io::{DeviceReads, InPlace, Io, IoFile, in_place};
use reel::io::job::{Job, JobStep};
use reel::record::codec::Codec;
use reel::segment::column_directory::ColumnDirectory;
use reel::segment::damage_list::DamagedRange;
use reel::segment::footer_reader::FooterReader;
use reel::segment::segment_table::{NO_SHARD, RetiredSegment, TABLE_SHARDS, shard_of};
use reel::segment::segment_tail::{BlockCount, MOST_MERGE_SEGMENTS, SegmentNumber, TailHead};
use reel_api::column_id::ColumnId;
use reel_api::direction::Direction;
use reel_api::maintenance_report::{MaintenanceReport, PickOutcome, UnitKind, WriteSource};
use reel_api::scan::ScanStep;
use reel_api::units::{ByteCount, FileOffset, Nanoseconds, RootId, VolumeId, WritePlace};
use reel_api::write_batch::WriteBatch;
use reel_sim::seeded_random::{Seed, SeededRandom};
use reel_sim::simulated_disk::OperationCount;
use reel_sim::simulated_file::SimulatedFile;
use reel::io::host_memory::HostMemory;
use reel_sim::simulated_io::SimulatedIo;

const ROOT: &str = "/disk/rewrite";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(64 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 20_000;
const SMALL_RECORD_BYTES: usize = 1_024;
const IDLE_KEYS: u64 = 600;
const STRIPE_KEYS: u64 = 300_000;
const IDLE_VALUE_BYTES: usize = 1_000;
const IDLE_LOOK: Nanoseconds = Nanoseconds(1_000_000_000);
const CRASH_POINTS: u64 = 300;
const KEYS: [&[u8]; 4] = [b"a", b"b", b"c", b"d"];
const MOST_LOOPS: usize = 2_000;
const OVER_THE_BOUND: u64 = MOST_OVERLAP as u64 + 8;
const KEPT_LIVE_KEYS: u64 = 16;
/// Graves past two grave blocks that still fit one segment at the sims' budget
const MANY_GRAVES: u64 = 12_000;
const SET_CRASH_STRIDE: u64 = 7;
const IDLE_TICKS: u64 = 8;
const TWO_BLOCK_ROWS: u64 = 12_000;
const UNWALKED_CRASH_POINTS: u64 = 40;
const UNWALKED_CRASH_STRIDE: u64 = 3;
const SEAT_KEYS: u64 = 400;
const WORKER_BUDGET: ByteCount = ByteCount(96 * 1024 * 1024);
const LEAST_VALUE: u64 = 100;
const STEPPED_COPY_KEYS: u64 = 512;
const HAND_OUT_LINE: Duration = Duration::from_millis(1);
const PARK_WAIT: Duration = Duration::from_secs(10);
const HOT_ROUNDS: u64 = 600;
const HOT_ROUND_KEYS: u64 = 8;
const HOT_FILL: Nanoseconds = Nanoseconds(100 * TICK.0);
const PLANNER_STEPS: usize = 200_000;
const WIDE_VALUE_BYTES: usize = 64 * 1024;

type Store = ReelStore<SimulatedIo>;

fn config() -> StoreConfig {
    StoreConfig {
        memory_budget: BUDGET,
        threads: shape::threads(),
        volumes: vec![VolumeConfig {
            name: "rewrite".to_string(),
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
            least_value: None,
        }],
    }
}

fn open(io: &SimulatedIo) -> Store {
    let config = config();
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

fn seal(store: &Store, io: &SimulatedIo) -> SegmentNumber {
    let number = hold(&store.state.volumes[0].claims).open[0].number;
    store.seal_now();
    settle(io);
    cool(store, io);
    number
}

fn read(store: &Store, key: &[u8]) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    store.get(COLUMN, key, &mut buf).expect("get").then_some(buf)
}

// the segment keeps the few dead rows it was built with
fn hold_reclaim(store: &Store) {
    store.state.volumes[0].holds.reclaim.store(true, Ordering::Release);
}

fn has_file(io: &SimulatedIo, number: SegmentNumber) -> bool {
    let data = data_path(Path::new(ROOT), number).to_path_buf();
    io.list_directory(Path::new(ROOT)).expect("list").contains(&data)
}

fn has_partial(io: &SimulatedIo) -> bool {
    let files = io.list_directory(Path::new(ROOT)).expect("list");
    files.iter().any(|file| file.to_string_lossy().ends_with(OUTPUT_SUFFIX))
}

fn dead_rows(store: &Store, number: SegmentNumber) -> u64 {
    let rows = hold(&store.state.volumes[0].summary);
    rows.get(number).expect("a row").cursor().column().expect("a column").dead_rows.0
}

fn newest_segment(store: &Store) -> SegmentNumber {
    hold(&store.state.volumes[0].summary).rows().map(|row| row.facts.segment).max().expect("a row")
}

// a pick looks at a few segments at a time, so a test that wants a merge goes on until the looks run out
fn start_merge(job: &mut CompactJob<SimulatedIo>) -> bool {
    for _look in 0..MOST_LOOPS {
        if job.rewrite.start() {
            return true;
        }
        if !job.rewrite.has_looks_left() {
            return false;
        }
    }
    false
}

fn hold_compaction(store: &Store, is_held: bool) {
    store.state.volumes[0].holds.compaction.store(is_held, Ordering::Release);
}

// write every key, seal, then write all but the last again: the first segment is mostly dead with one live row
fn half_dead(store: &Store, io: &SimulatedIo) -> (SegmentNumber, SegmentNumber) {
    for key in KEYS {
        store.put(COLUMN, key, b"first").expect("put");
    }
    let older = seal(store, io);
    for key in &KEYS[..3] {
        store.put(COLUMN, key, b"second").expect("put");
    }
    (older, seal(store, io))
}

fn check_reads(store: &Store) {
    for key in &KEYS[..3] {
        assert_eq!(read(store, key), Some(b"second".to_vec()));
    }
    assert_eq!(read(store, KEYS[3]), Some(b"first".to_vec()));
}

// a half-dead segment goes, and a smaller one with its live rows takes its place, counted and walked
#[test]
fn rewrites() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io);
    let (older, newer) = half_dead(&store, &io);

    let volume = &store.state.volumes[0];
    assert!(!volume.table.holds(older));
    assert!(!has_file(&io, older));
    let output = newest_segment(&store);
    assert!(output > newer);
    assert!(volume.table.holds(output));
    assert_eq!(volume.segments_rewritten.load(Ordering::Acquire), 1);

    let rows = hold(&volume.summary);
    let row = rows.get(output).expect("a row");
    assert!(row.facts.is_walked());
    assert_eq!(row.cursor().column().expect("a column").row_count.0, 1);
    drop(rows);
    assert_eq!(dead_rows(&store, output), 0);
    check_reads(&store);

    store.close().expect("close");
    assert_eq!(io.open_handle_count(), 0);
    let store = open(&io);
    settle(&io);
    check_reads(&store);
    assert!(hold(&store.state.volumes[0].damaged).is_empty());
    store.close().expect("close");
}

// a row a cue can see stays in the output, counted dead, and it answers the cue alone: a newer row in a lower-numbered
// segment still wins a plain read, and the output goes once the cue does
#[test]
fn cue_keeps_row() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io);
    hold_compaction(&store, true);
    store.put(COLUMN, b"a", b"first").expect("put");
    let cue = Cue::new(Arc::clone(&store.state)).expect("cue");
    for key in &KEYS[1..] {
        store.put(COLUMN, key, b"first").expect("put");
    }
    let older = seal(&store, &io);
    for key in KEYS {
        store.put(COLUMN, key, b"second").expect("put");
    }
    let newer = seal(&store, &io);

    hold_compaction(&store, false);
    store.state.ask_compaction();
    settle(&io);
    let volume = &store.state.volumes[0];
    assert!(!volume.table.holds(older));
    let output = newest_segment(&store);
    assert!(output > newer);
    assert_eq!(dead_rows(&store, output), 1);

    assert_eq!(read(&store, b"a"), Some(b"second".to_vec()));
    let mut buf = Vec::new();
    assert!(cue.read(COLUMN, b"a", RecordWant::Whole, &mut buf).expect("read"));
    assert_eq!(buf, b"first");

    drop(cue);
    settle(&io);
    assert!(!volume.table.holds(output));
    assert_eq!(read(&store, b"a"), Some(b"second".to_vec()));
    store.close().expect("close");
}

// a cue that shows up after the rows were decided stops the install, the output goes and the input stays
#[test]
fn install_looks_again() {
    let io = SimulatedIo::new(Seed(3));
    let store = open(&io);
    hold_compaction(&store, true);
    let (older, _newer) = half_dead(&store, &io);

    let state = &store.state;
    let volume = &state.volumes[0];
    let mut job = CompactJob::new(Arc::clone(state), 0);
    job.rewrite.is_key_order_held = true;
    assert!(start_merge(&mut job));
    let number = job.rewrite.output.as_ref().expect("an output").number;
    while job.rewrite.phase != RewritePhase::Installing {
        job.rewrite.step();
    }
    assert!(has_file(&io, number));

    let slot = state.cues.take(0, WritePlace(state.read_point(&state.volumes[0]).0 - 3)).expect("a slot");
    job.rewrite.step();
    assert!(!job.rewrite.is_running());
    assert!(!has_file(&io, number));
    assert!(volume.table.holds(older));
    assert!(!volume.is_rewriting());
    state.cues.release(slot);
    check_reads(&store);
    store.close().expect("close");
}

// a walk runs beside a rewrite, its charges on the rewrite's inputs held for the install, and counts the turn it once stood
// aside for
#[test]
fn walk_runs_beside_a_rewrite() {
    let io = SimulatedIo::new(Seed(4));
    let store = open(&io);
    hold_compaction(&store, true);
    half_dead(&store, &io);

    let state = &store.state;
    let volume = &state.volumes[0];
    let mut job = CompactJob::new(Arc::clone(state), 0);
    assert!(start_merge(&mut job));
    store.put(COLUMN, b"e", b"value").expect("put");
    seal(&store, &io);
    assert!(job.rewrite.is_running());
    assert_eq!(state.pending_walks(0), 0, "the walk waited for the rewrite");
    assert!(volume.walks_held.load(Ordering::Acquire) > 0);
    assert_eq!(volume.walk_claim_waits.load(Ordering::Acquire), 0);

    while job.rewrite.is_running() {
        job.rewrite.step();
    }
    state.ask_compaction();
    settle(&io);
    assert_eq!(state.pending_walks(0), 0);
    store.close().expect("close");
}

// a close stops a rewrite, and its partial file goes with it
#[test]
fn stopped() {
    let io = SimulatedIo::new(Seed(5));
    let store = open(&io);
    hold_compaction(&store, true);
    let (older, _newer) = half_dead(&store, &io);

    let volume = &store.state.volumes[0];
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    job.rewrite.is_key_order_held = true;
    assert!(start_merge(&mut job));
    job.rewrite.step();
    assert!(has_partial(&io));
    let output = job.rewrite.output.clone().expect("an output");
    assert!(volume.is_rewriting());
    drop(job);
    store.close().expect("close");

    assert!(!has_partial(&io));
    assert!(!has_file(&io, output.number));
    assert!(has_file(&io, older));
    assert_eq!(io.open_handle_count(), 0);
}

// open drops a partial file a crash left
#[test]
fn partial_goes_at_open() {
    let io = SimulatedIo::new(Seed(6));
    let store = open(&io);
    hold_compaction(&store, true);
    half_dead(&store, &io);
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    job.rewrite.is_key_order_held = true;
    assert!(start_merge(&mut job));
    job.rewrite.step();
    io.kill();
    drop(job);
    drop(store);
    assert!(has_partial(&io));

    let store = open(&io);
    assert!(!has_partial(&io));
    settle(&io);
    check_reads(&store);
    store.close().expect("close");
}

// a crash at any step keeps every read, leaves no partial file and no damage, and the row below the input stays counted
// once: an output rebuilt at open holds rows its input's walk already counted
#[test]
fn crashes() {
    let mut rebuilt = 0;
    for crash_at in 0..CRASH_POINTS {
        for is_power_cut in [false, true] {
            let io = SimulatedIo::new(Seed(7));
            let store = open(&io);
            hold_reclaim(&store);
            store.put(COLUMN, b"a", b"zero").expect("put");
            for key in [b"keep-0", b"keep-1", b"keep-2"] {
                store.put(COLUMN, key, b"value").expect("put");
            }
            let lowest = seal(&store, &io);
            for key in KEYS {
                store.put(COLUMN, key, b"first").expect("put");
            }
            seal(&store, &io);
            for key in &KEYS[..3] {
                store.put(COLUMN, key, b"second").expect("put");
            }
            store.flush().expect("flush");
            store.seal_now();
            io.arm_crash(OperationCount(crash_at));
            for _step in 0..STEPS {
                if io.is_crashed() {
                    break;
                }
                if io.step_any().is_none() {
                    io.advance_clock(TICK);
                    if io.step_any().is_none() {
                        break;
                    }
                }
            }
            if is_power_cut {
                io.power_cut();
            } else {
                io.kill();
            }
            drop(store);

            let store = open(&io);
            hold_reclaim(&store);
            let state = &store.state;
            assert!(!has_partial(&io), "crash at {crash_at}");
            let is_rebuilt = hold(&state.volumes[0].summary).rows().any(|row| row.facts.is_backward);
            rebuilt += u64::from(is_rebuilt);
            store.seal_now();
            settle(&io);
            check_reads(&store);
            assert_eq!(dead_rows(&store, lowest), 1, "crash at {crash_at}");
            assert!(hold(&state.volumes[0].damaged).is_empty(), "crash at {crash_at}");
            assert_eq!(state.dropped_summary_rows.load(Ordering::Acquire), 0, "crash at {crash_at}");
            store.close().expect("close");
        }
    }
    assert!(rebuilt > 0);
}

// a record larger than the merge buffer copies whole
#[test]
fn big_record() {
    let io = SimulatedIo::new(Seed(8));
    let store = open(&io);
    let value = vec![7; MERGE_BUFFER_BYTES + 1000];
    store.put(COLUMN, b"big", &value).expect("put");
    // small dead records are what a rewrite comes for, since a big dead one frees in place
    let small = vec![7; SMALL_RECORD_BYTES];
    for number in 0..MERGE_BUFFER_BYTES * 2 / SMALL_RECORD_BYTES {
        store.put(COLUMN, format!("key-{number:05}").as_bytes(), &small).expect("put");
    }
    let older = seal(&store, &io);
    for number in 0..MERGE_BUFFER_BYTES * 2 / SMALL_RECORD_BYTES {
        store.put(COLUMN, format!("key-{number:05}").as_bytes(), b"gone").expect("put");
    }
    seal(&store, &io);

    assert!(!store.state.volumes[0].table.holds(older));
    assert_eq!(read(&store, b"big"), Some(value));
    store.close().expect("close");
}

// a list published under a search sends it round again, or a row that moved to an output would go unseen
#[test]
fn search_sees_new_list() {
    let io = SimulatedIo::new(Seed(9));
    let store = open(&io);
    store.put(COLUMN, b"a", b"value").expect("put");
    store.put(COLUMN, b"c", b"value").expect("put");
    seal(&store, &io);
    // a fresh open has no filter cached, so the first ask reads the device
    store.close().expect("close");
    drop(store);
    let store = open(&io);

    let state = &store.state;
    let directory = state.directories[COLUMN.0 as usize].as_ref().expect("a directory");
    let mut reader = FooterReader::new();
    let device = ChangesList { directory, has_changed: AtomicBool::new(false) };
    // a key the segment lacks finds no row to stop the second look
    let read = state.key_read_now(COLUMN, b"b").expect("read");
    let walked = in_place(state.walk_candidates(&device, &state.volumes[0], directory, &read, &mut reader, WalkAsk::EVERY));
    walked.expect("walk");
    assert_eq!(reader.asked, 2);
    store.close().expect("close");
}

// a get that took its point before a rewrite dropped its version starts over
#[test]
fn get_crossed() {
    let io = SimulatedIo::new(Seed(10));
    let store = open(&io);
    hold_compaction(&store, true);
    for key in KEYS {
        store.put(COLUMN, key, b"first").expect("put");
    }
    let older = seal(&store, &io);
    let latest = store.state.latest_read(COLUMN, KEYS[0]).expect("read");
    for key in &KEYS[..3] {
        store.put(COLUMN, key, b"second").expect("put");
    }
    seal(&store, &io);

    hold_compaction(&store, false);
    store.state.ask_compaction();
    settle(&io);
    assert!(!store.state.volumes[0].table.holds(older));
    let mut buf = Vec::new();
    assert!(crate::latest::answer(&store, &latest, RecordWant::Whole, &mut buf).is_none());
    check_reads(&store);
    store.close().expect("close");
}

// a crash with a whole output and its input both on disk leaves the input to open, which removes it before counting
#[test]
fn open_removes_input() {
    let io = SimulatedIo::new(Seed(11));
    let store = open(&io);
    hold_compaction(&store, true);
    let (older, _newer) = half_dead(&store, &io);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    job.rewrite.is_key_order_held = true;
    assert!(start_merge(&mut job));
    let number = job.rewrite.output.as_ref().expect("an output").number;
    while job.rewrite.phase != RewritePhase::Installing {
        job.rewrite.step();
    }
    assert!(has_file(&io, older));
    io.kill();
    drop(job);
    drop(store);

    let store = open(&io);
    assert!(!has_file(&io, older));
    assert!(store.state.volumes[0].table.holds(number));
    settle(&io);
    check_reads(&store);
    assert!(hold(&store.state.volumes[0].damaged).is_empty());
    store.close().expect("close");
}

// run compaction by hand until it has nothing to do, with every other job quiet
fn compact(store: &Store, io: &SimulatedIo) {
    hold_compaction(store, false);
    store.state.ask_compaction();
    settle(io);
}

fn delete_rows(store: &Store, io: &SimulatedIo) -> SegmentNumber {
    for key in KEYS {
        store.delete(COLUMN, key).expect("delete");
    }
    seal(store, io)
}

// a grave with nothing older anywhere drops at its first rewrite, and with every row gone so does its segment
#[test]
fn grave_hides_nothing() {
    let io = SimulatedIo::new(Seed(12));
    let store = open(&io);
    let graves = delete_rows(&store, &io);
    compact(&store, &io);
    let volume = &store.state.volumes[0];
    assert!(!volume.table.holds(graves));
    assert!(!has_file(&io, graves));
    assert_eq!(volume.segments_rewritten.load(Ordering::Acquire), 1);
    for key in KEYS {
        assert_eq!(read(&store, key), None);
    }
    store.close().expect("close");
    let store = open(&io);
    settle(&io);
    for key in KEYS {
        assert_eq!(read(&store, key), None);
    }
    store.close().expect("close");
}

// a grave stays while an older version is on disk, and a read never finds that version
#[test]
fn grave_stays_over_older() {
    let io = SimulatedIo::new(Seed(13));
    let store = open(&io);
    hold_reclaim(&store);
    hold_compaction(&store, true);
    store.put(COLUMN, b"keep", b"value").expect("put");
    for key in KEYS {
        store.put(COLUMN, key, b"first").expect("put");
    }
    store.put(COLUMN, b"keep-1", b"value").expect("put");
    store.put(COLUMN, b"keep-2", b"value").expect("put");
    store.put(COLUMN, b"keep-3", b"value").expect("put");
    store.put(COLUMN, b"keep-4", b"value").expect("put");
    let older = seal(&store, &io);
    let graves = delete_rows(&store, &io);
    compact(&store, &io);

    let volume = &store.state.volumes[0];
    assert!(volume.table.holds(older));
    assert!(volume.table.holds(graves));
    assert_eq!(volume.segments_rewritten.load(Ordering::Acquire), 0);
    assert_eq!(read(&store, KEYS[0]), None);
    store.close().expect("close");
}

// an older row in an open segment counts as much as a sealed one
#[test]
fn open_row_is_older_data() {
    let io = SimulatedIo::new(Seed(14));
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");
    let place = WritePlace(store.state.read_point(&store.state.volumes[0]).0 + 1);
    let volume = &store.state.volumes[0];
    let found = store.state.has_older_row(volume, COLUMN, b"key", place, &[]);
    assert!(found.expect("search"));
    store.close().expect("close");
}

// a grave over an older grave waits for it, or the older one's count would stand on nothing and a later write would count it
// twice. The older one drops first, and then this one
#[test]
fn older_grave_goes_first() {
    let io = SimulatedIo::new(Seed(15));
    let store = open(&io);
    hold_compaction(&store, true);
    let first = delete_rows(&store, &io);
    for key in KEYS {
        store.put(COLUMN, key, b"value").expect("put");
    }
    let middle = seal(&store, &io);
    let second = delete_rows(&store, &io);
    compact(&store, &io);

    let volume = &store.state.volumes[0];
    assert!(!volume.table.holds(first));
    assert!(!volume.table.holds(middle));
    assert!(!volume.table.holds(second));
    for key in KEYS {
        assert_eq!(read(&store, key), None);
    }
    store.close().expect("close");
}

// a cue can read a row a grave in the same segment covers, so the grave waits for the cue, and a plain read never finds the row
#[test]
fn cue_keeps_grave() {
    let io = SimulatedIo::new(Seed(16));
    let store = open(&io);
    hold_compaction(&store, true);
    store.put(COLUMN, b"key", b"value").expect("put");
    let cue = store.cue().expect("cue");
    store.delete(COLUMN, b"key").expect("delete");
    let graves = seal(&store, &io);
    compact(&store, &io);

    assert_eq!(read(&store, b"key"), None);
    let mut buf = Vec::new();
    assert!(cue.get(COLUMN, b"key", &mut buf).expect("get"));
    assert_eq!(buf, b"value");

    drop(cue);
    settle(&io);
    assert_eq!(read(&store, b"key"), None);
    assert!(!store.state.volumes[0].table.holds(graves));
    store.close().expect("close");
}

// a file on its way out comes back at a crash, so a grave over its rows stays until the file is gone, and a deleted key
// stays deleted
#[test]
fn parked_file_keeps_grave() {
    for is_power_cut in [false, true] {
        let io = SimulatedIo::new(Seed(17));
        let store = open(&io);
        hold_compaction(&store, true);
        let state = Arc::clone(&store.state);
        let volume = &state.volumes[0];
        for key in KEYS {
            store.put(COLUMN, key, b"first").expect("put");
        }
        let older = seal(&store, &io);
        let graves = delete_rows(&store, &io);

        let pin = volume.table.pin(older).expect("a pin");
        let job = CompactJob::new(Arc::clone(&state), 0);
        job.unlink(volume, older, state.read_point(&state.volumes[0])).expect("unlink");
        assert_eq!(volume.segments_parked.load(Ordering::Acquire), 1);

        let mut job = CompactJob::new(Arc::clone(&state), 0);
        hold_compaction(&store, false);
        for _loop in 0..MOST_LOOPS {
            job.step_unit(Nanoseconds(0));
        }
        assert!(volume.table.holds(graves));

        if is_power_cut {
            io.power_cut();
        } else {
            io.kill();
        }
        drop(pin);
        drop(job);
        drop(store);
        drop(state);

        let store = open(&io);
        settle(&io);
        for key in KEYS {
            assert_eq!(read(&store, key), None);
        }
        store.close().expect("close");
    }
}

// deciding first copies nothing when no grave would drop
#[test]
fn deciding_copies_nothing() {
    let io = SimulatedIo::new(Seed(18));
    let store = open(&io);
    hold_compaction(&store, true);
    store.put(COLUMN, b"a", b"first").expect("put");
    for key in [b"b", b"c", b"d"] {
        store.put(COLUMN, key, b"first").expect("put");
    }
    seal(&store, &io);
    store.delete(COLUMN, b"a").expect("delete");
    let graves = seal(&store, &io);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job));
    assert_eq!(job.rewrite.phase, RewritePhase::Deciding);
    assert!(job.rewrite.output.is_none());
    while job.rewrite.is_running() {
        job.rewrite.step();
    }
    assert!(!has_partial(&io));
    assert!(store.state.volumes[0].table.holds(graves));
    store.close().expect("close");
}

// a ready rewrite doesn't wait for walks still waiting, which at the ingest ceiling never run out
#[test]
fn ahead_of_waiting_walks() {
    let io = SimulatedIo::new(Seed(19));
    let store = open(&io);
    hold_compaction(&store, true);
    let (older, _newer) = half_dead(&store, &io);

    io.hold_jobs("reel-planner", true);
    store.put(COLUMN, b"later", b"value").expect("put");
    seal(&store, &io);
    assert_eq!(store.state.pending_walks(0), 1);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job));
    while job.rewrite.is_running() {
        job.rewrite.step();
    }
    assert!(!store.state.volumes[0].table.holds(older));
    assert_eq!(store.state.pending_walks(0), 1);
    io.hold_jobs("reel-planner", false);
    store.state.ask_compaction();
    settle(&io);
    assert_eq!(store.state.pending_walks(0), 0);
    check_reads(&store);
    store.close().expect("close");
}

// a target in progress lets a rewrite start, and keeps the target itself out of its inputs
#[test]
fn walk_target_lets_a_rewrite_start() {
    let io = SimulatedIo::new(Seed(20));
    let store = open(&io);
    hold_compaction(&store, true);
    let (older, newer) = half_dead(&store, &io);
    let volume = &store.state.volumes[0];
    volume.has_walk_target.store(true, Ordering::Release);
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    // the walk on the one segment ready for a rewrite leaves nothing to pick
    volume.walk_segment.store(older.0, Ordering::Release);
    assert!(!start_merge(&mut job), "the walk's target went into a rewrite");
    // the walk on another segment refuses nothing
    volume.walk_segment.store(newer.0, Ordering::Release);
    assert!(start_merge(&mut job));
    assert!(!job.rewrite.input_numbers().contains(&newer));
    volume.has_walk_target.store(false, Ordering::Release);
    volume.walk_segment.store(NO_SEAL, Ordering::Release);
    while job.rewrite.is_running() {
        job.rewrite.step();
    }
    store.close().expect("close");
}

// an output a give-up couldn't remove for good holds every grave back and stops new rewrites, until a retry has it gone
#[test]
fn stray_output_holds_graves() {
    let io = SimulatedIo::new(Seed(21));
    let store = open(&io);
    hold_compaction(&store, true);
    half_dead(&store, &io);

    let state = &store.state;
    let volume = &state.volumes[0];
    let mut job = CompactJob::new(Arc::clone(state), 0);
    assert!(start_merge(&mut job));
    while job.rewrite.phase != RewritePhase::Installing {
        job.rewrite.step();
    }
    let slot = state.cues.take(0, WritePlace(state.read_point(&state.volumes[0]).0 - 3)).expect("a slot");
    io.fail_directory_syncs(OperationCount(1));
    job.rewrite.step();
    state.cues.release(slot);

    assert_eq!(volume.stray_outputs.load(Ordering::Acquire), 1);
    assert_eq!(volume.reclaim_retries.load(Ordering::Acquire), 1);
    assert!(!job.rewrite.start());
    job.rewrite.clear_stray();
    assert_eq!(volume.stray_outputs.load(Ordering::Acquire), 0);
    assert!(start_merge(&mut job));
    store.close().expect("close");
}

// a name that landed under a failed directory sync is still a segment on disk, so a give-up removes it by that name
#[test]
fn name_landed_before_the_failure() {
    let io = SimulatedIo::new(Seed(22));
    let store = open(&io);
    hold_compaction(&store, true);
    half_dead(&store, &io);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    job.rewrite.is_key_order_held = true;
    assert!(start_merge(&mut job));
    let number = job.rewrite.output.as_ref().expect("an output").number;
    while job.rewrite.phase == RewritePhase::Copying {
        job.rewrite.step();
    }
    io.fail_directory_syncs(OperationCount(1));
    while job.rewrite.is_running() {
        job.rewrite.step();
    }
    assert!(!has_file(&io, number));
    store.close().expect("close");
}

fn run_to_install(job: &mut CompactJob<SimulatedIo>) {
    assert!(start_merge(job));
    while job.rewrite.phase != RewritePhase::Installing {
        job.rewrite.step();
    }
}

// a named output a give-up can't remove stays as a stray, and a later try removes it
#[test]
fn failed_remove_stays_a_stray() {
    let io = SimulatedIo::new(Seed(40));
    let store = open(&io);
    hold_compaction(&store, true);
    half_dead(&store, &io);

    let state = &store.state;
    let volume = &state.volumes[0];
    let mut job = CompactJob::new(Arc::clone(state), 0);
    run_to_install(&mut job);
    let slot = state.cues.take(0, WritePlace(state.read_point(&state.volumes[0]).0 - 3)).expect("a slot");
    io.fail_removes(OperationCount(1));
    job.rewrite.step();
    state.cues.release(slot);

    assert!(!job.rewrite.is_running());
    assert_eq!(volume.stray_outputs.load(Ordering::Acquire), 1);
    job.rewrite.clear_stray();
    assert_eq!(volume.stray_outputs.load(Ordering::Acquire), 0);
    store.close().expect("close");
}

// a grave and the value under it share a half-dead segment, so with nothing else in the way the grave drops
fn grave_with_nothing_older(store: &Store, io: &SimulatedIo) {
    store.put(COLUMN, b"key", b"value").expect("put");
    store.delete(COLUMN, b"key").expect("delete");
    half_dead(store, io);
}

fn kept_graves(job: &CompactJob<SimulatedIo>) -> u64 {
    let columns = job.rewrite.writer.columns().iter().chain(job.rewrite.grave_writer.columns());
    columns.map(|column| column.grave_count.0).sum()
}

// the same grave drops when nothing holds it back, which the two tests below turn on
#[test]
fn grave_drops_alone() {
    let io = SimulatedIo::new(Seed(41));
    let store = open(&io);
    hold_compaction(&store, true);
    grave_with_nothing_older(&store, &io);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    run_to_install(&mut job);
    assert_eq!(kept_graves(&job), 0);
    job.rewrite.abandon();
    store.close().expect("close");
}

// a stray that turns up mid-rewrite keeps a grave, since the stray could come back at a crash
#[test]
fn stray_keeps_a_grave() {
    let io = SimulatedIo::new(Seed(42));
    let store = open(&io);
    hold_compaction(&store, true);
    grave_with_nothing_older(&store, &io);

    let volume = &store.state.volumes[0];
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job));
    volume.stray_outputs.store(1, Ordering::Release);
    while job.rewrite.phase != RewritePhase::Installing {
        job.rewrite.step();
    }
    volume.stray_outputs.store(0, Ordering::Release);
    assert_eq!(kept_graves(&job), 1);
    job.rewrite.abandon();
    store.close().expect("close");
}

// damage over a grave's key could hide an older version, so the grave stays
#[test]
fn damage_keeps_a_grave() {
    let io = SimulatedIo::new(Seed(43));
    let store = open(&io);
    hold_compaction(&store, true);
    grave_with_nothing_older(&store, &io);

    let volume = &store.state.volumes[0];
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job));
    volume.damage.add(&DamagedRange {
        column: COLUMN,
        segment: SegmentNumber(0),
        highest_place: WritePlace(1),
        start: b"key".to_vec(),
        end: Some(b"kez".to_vec()),
    });
    while job.rewrite.phase != RewritePhase::Installing {
        job.rewrite.step();
    }
    assert_eq!(kept_graves(&job), 1);
    job.rewrite.abandon();
    store.close().expect("close");
}

// a replacement the mark hasn't reached stops the install: a read below it would find neither version
#[test]
fn mark_holds_the_install() {
    let io = SimulatedIo::new(Seed(44));
    let store = open(&io);
    hold_compaction(&store, true);
    let (older, _newer) = half_dead(&store, &io);

    let state = &store.state;
    let mut job = CompactJob::new(Arc::clone(state), 0);
    run_to_install(&mut job);
    job.rewrite.highest_replacement = WritePlace(state.read_point(&state.volumes[0]).0 + 1);
    job.rewrite.step();

    assert!(!job.rewrite.is_running());
    assert!(state.volumes[0].table.holds(older));
    check_reads(&store);
    store.close().expect("close");
}

fn segments(store: &Store) -> u64 {
    store.state.volumes[0].table.segment_count()
}

// segments piling up over one key range merge down under the bound, with every key still reading its value
#[test]
fn overlap_merges() {
    let io = SimulatedIo::new(Seed(23));
    let store = open(&io);
    for number in 0..OVER_THE_BOUND {
        store.put(COLUMN, format!("a-{number:03}").as_bytes(), b"low").expect("put");
        store.put(COLUMN, format!("z-{number:03}").as_bytes(), b"high").expect("put");
        seal(&store, &io);
    }

    assert!(segments(&store) <= MOST_OVERLAP as u64);
    assert!(store.state.volumes[0].segments_rewritten.load(Ordering::Acquire) > 0);
    for number in 0..OVER_THE_BOUND {
        assert_eq!(read(&store, format!("a-{number:03}").as_bytes()), Some(b"low".to_vec()));
        assert_eq!(read(&store, format!("z-{number:03}").as_bytes()), Some(b"high".to_vec()));
    }
    store.close().expect("close");

    let store = open(&io);
    settle(&io);
    assert_eq!(read(&store, b"a-000"), Some(b"low".to_vec()));
    assert!(hold(&store.state.volumes[0].damaged).is_empty());
    store.close().expect("close");
}

// a merge takes a key's versions from every input in one pass, and only the newest stays
#[test]
fn versions_across_inputs() {
    let io = SimulatedIo::new(Seed(24));
    let store = open(&io);
    hold_compaction(&store, true);
    for version in 0..4_u8 {
        for key in KEYS {
            store.put(COLUMN, key, &[version]).expect("put");
        }
        store.put(COLUMN, format!("only-{version}").as_bytes(), b"value").expect("put");
        seal(&store, &io);
    }
    compact(&store, &io);

    assert!(store.state.volumes[0].segments_rewritten.load(Ordering::Acquire) > 0);
    for key in KEYS {
        assert_eq!(read(&store, key), Some(vec![3]));
    }
    for version in 0..4_u8 {
        assert_eq!(read(&store, format!("only-{version}").as_bytes()), Some(b"value".to_vec()));
    }
    store.close().expect("close");
}

// a grave that stays leaves its data behind: it goes to an output of its own, footer rows and no bytes
#[test]
fn graves_get_their_own_output() {
    let io = SimulatedIo::new(Seed(25));
    let store = open(&io);
    hold_compaction(&store, true);
    store.put(COLUMN, b"a", b"value").expect("put");
    for key in [&b"keep-1"[..], b"keep-2", b"keep-3", b"keep-4", b"keep-5", b"keep-6"] {
        store.put(COLUMN, key, b"value").expect("put");
    }
    seal(&store, &io);
    store.delete(COLUMN, b"a").expect("delete");
    store.put(COLUMN, b"b", b"first").expect("put");
    store.put(COLUMN, b"c", b"live").expect("put");
    let middle = seal(&store, &io);
    store.put(COLUMN, b"b", b"second").expect("put");
    seal(&store, &io);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job));
    assert_eq!(job.rewrite.input_numbers()[0], middle);
    while job.rewrite.is_running() {
        job.rewrite.step();
    }
    let rows = hold(&store.state.volumes[0].summary);
    let graves = rows.rows().filter(|row| row.facts.data_end.0 == 0).count();
    assert_eq!(graves, 1);
    drop(rows);
    drop(job);
    assert_eq!(read(&store, b"a"), None);
    store.close().expect("close");
}

// a crash at any step of a merge with two outputs changes no key and leaves no grave output behind that a finished merge
// wouldn't have
#[test]
fn two_output_crashes() {
    let mut left_grave_output = 0;
    for crash_at in 0..CRASH_POINTS {
        for is_power_cut in [false, true] {
            let io = SimulatedIo::new(Seed(26));
            let store = open(&io);
            hold_compaction(&store, true);
            store.put(COLUMN, b"a", b"value").expect("put");
            for key in [&b"keep-1"[..], b"keep-2", b"keep-3", b"keep-4", b"keep-5", b"keep-6"] {
                store.put(COLUMN, key, b"value").expect("put");
            }
            seal(&store, &io);
            store.delete(COLUMN, b"a").expect("delete");
            store.put(COLUMN, b"b", b"first").expect("put");
            store.put(COLUMN, b"c", b"live").expect("put");
            let middle = seal(&store, &io);
            store.put(COLUMN, b"b", b"second").expect("put");
            seal(&store, &io);

            hold_compaction(&store, false);
            store.state.ask_compaction();
            io.arm_crash(OperationCount(crash_at));
            for _step in 0..STEPS {
                if io.is_crashed() {
                    break;
                }
                if io.step_any().is_none() {
                    io.advance_clock(TICK);
                    if io.step_any().is_none() {
                        break;
                    }
                }
            }
            if is_power_cut {
                io.power_cut();
            } else {
                io.kill();
            }
            drop(store);

            let store = open(&io);
            let is_grave_row = |row: &reel::segment::summary::SummaryRow<'_>| row.facts.data_end.0 == 0;
            let graves = hold(&store.state.volumes[0].summary).rows().filter(is_grave_row).count();
            let has_input = store.state.volumes[0].table.holds(middle);
            assert!(graves == 0 || !has_input, "crash at {crash_at}");
            left_grave_output += graves;
            settle(&io);
            assert_eq!(read(&store, b"a"), None, "crash at {crash_at}");
            assert_eq!(read(&store, b"b"), Some(b"second".to_vec()), "crash at {crash_at}");
            assert_eq!(read(&store, b"keep-1"), Some(b"value".to_vec()), "crash at {crash_at}");
            assert_eq!(read(&store, b"c"), Some(b"live".to_vec()), "crash at {crash_at}");
            assert!(hold(&store.state.volumes[0].damaged).is_empty(), "crash at {crash_at}");
            store.close().expect("close");
        }
    }
    assert!(left_grave_output > 0);
}

// an older row sits in a segment no merge wants, and its grave goes into a merge with a live row, so a merge has both outputs
fn grave_over_older(store: &Store, io: &SimulatedIo) {
    store.put(COLUMN, b"key", b"older").expect("put");
    for number in 0..KEPT_LIVE_KEYS {
        store.put(COLUMN, format!("live-{number}").as_bytes(), b"value").expect("put");
    }
    seal(store, io);
    store.delete(COLUMN, b"key").expect("delete");
    half_dead(store, io);
}

fn run_to_both_outputs(job: &mut CompactJob<SimulatedIo>) {
    assert!(start_merge(job));
    while job.rewrite.phase != RewritePhase::Installing {
        job.rewrite.step();
    }
    assert!(job.rewrite.totals.is_some() && job.rewrite.grave_totals.is_some());
}

// a kept grave is in a list before the inputs leave theirs, so a read between the two still finds it over the older row
#[test]
fn grave_listed_first() {
    let io = SimulatedIo::new(Seed(30));
    let store = open(&io);
    hold_compaction(&store, true);
    grave_over_older(&store, &io);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    run_to_both_outputs(&mut job);
    job.rewrite.install_with(|| assert_eq!(read(&store, b"key"), None), || {}).expect("install");
    assert_eq!(read(&store, b"key"), None);
    assert_eq!(read(&store, b"d"), Some(b"first".to_vec()));
    store.close().expect("close");
}

// a data output that can't be removed lists the inputs, so its grave output stays too, and open finds both at a power cut
#[test]
fn stuck_data_output_keeps_graves() {
    let io = SimulatedIo::new(Seed(31));
    let store = open(&io);
    hold_compaction(&store, true);
    grave_over_older(&store, &io);

    let state = &store.state;
    let mut job = CompactJob::new(Arc::clone(state), 0);
    run_to_both_outputs(&mut job);
    let slot = state.cues.take(0, WritePlace(state.read_point(&state.volumes[0]).0 - 3)).expect("a slot");
    io.fail_removes(OperationCount(1));
    job.rewrite.step();
    state.cues.release(slot);
    assert!(!job.rewrite.is_running());
    assert_eq!(state.volumes[0].stray_outputs.load(Ordering::Acquire), 2);

    io.power_cut();
    drop(job);
    drop(store);
    let store = open(&io);
    assert_eq!(read(&store, b"key"), None);
    store.close().expect("close");
}

// a retry clears the data output first, then the grave output, and nothing holds back after that
#[test]
fn strays_clear_in_order() {
    let io = SimulatedIo::new(Seed(32));
    let store = open(&io);
    hold_compaction(&store, true);
    grave_over_older(&store, &io);

    let state = &store.state;
    let volume = &state.volumes[0];
    let mut job = CompactJob::new(Arc::clone(state), 0);
    run_to_both_outputs(&mut job);
    let slot = state.cues.take(0, WritePlace(state.read_point(&state.volumes[0]).0 - 3)).expect("a slot");
    io.fail_removes(OperationCount(1));
    job.rewrite.step();
    state.cues.release(slot);

    io.fail_removes(OperationCount(1));
    job.rewrite.clear_stray();
    assert_eq!(volume.stray_outputs.load(Ordering::Acquire), 2);
    job.rewrite.clear_stray();
    assert_eq!(volume.stray_outputs.load(Ordering::Acquire), 0);
    assert_eq!(read(&store, b"key"), None);
    store.close().expect("close");
}

// a table with room for one output takes neither, and the merge gives way with the inputs where they were
#[test]
fn no_room_for_the_second_output() {
    let io = SimulatedIo::new(Seed(33));
    let store = open(&io);
    hold_compaction(&store, true);
    grave_over_older(&store, &io);

    let state = &store.state;
    let volume = &state.volumes[0];
    let mut job = CompactJob::new(Arc::clone(state), 0);
    run_to_both_outputs(&mut job);
    let inputs = job.rewrite.input_numbers().to_vec();
    let data = job.rewrite.output.clone().expect("an output");
    let graves = job.rewrite.grave_output.clone().expect("an output");
    let head = job.rewrite.grave_totals.expect("totals").head;

    let shard = shard_of(graves.number);
    let mut number = u64::MAX / 2;
    loop {
        number += 1;
        let filler = SegmentNumber(number);
        if shard_of(filler) == shard
            && filler != data.number
            && !volume.table.install(graves.file.clone(), RootId(0), &TailHead { segment: filler, ..head }, &[], &[])
        {
            break;
        }
    }

    job.rewrite.step();
    assert!(!job.rewrite.is_running());
    assert!(!volume.table.holds(data.number));
    assert!(!volume.table.holds(graves.number));
    assert!(inputs.iter().all(|input| volume.table.holds(*input)));
    assert_eq!(read(&store, b"key"), None);
    io.kill();
}

// a segment that overlaps too many but finds no second input takes no merge, and the next look starts past it
#[test]
fn overlap_needs_two() {
    let io = SimulatedIo::new(Seed(34));
    let store = open(&io);
    hold_compaction(&store, true);
    for number in 0..OVER_THE_BOUND {
        store.put(COLUMN, format!("a-{number:03}").as_bytes(), b"low").expect("put");
        store.put(COLUMN, format!("z-{number:03}").as_bytes(), b"high").expect("put");
        seal(&store, &io);
    }

    let state = &store.state;
    let volume = &state.volumes[0];
    let stand_in = stand_in_parked(&store);
    fill_park(&store, &stand_in, park_size(&store) - 1);
    let mut job = CompactJob::new(Arc::clone(state), 0);
    assert!(!job.rewrite.start());
    assert!(!job.rewrite.is_running());
    assert!(!volume.is_rewriting());
    let first_tried = job.rewrite.looked_up_to();
    assert!(!job.rewrite.start());
    assert!(job.rewrite.looked_up_to() > first_tried);

    hold(&volume.parked).iter_mut().for_each(|slot| *slot = None);
    assert!(start_merge(&mut job));
    assert!(job.rewrite.input_numbers().len() > 1);
    io.kill();
}

// a merge with its outputs written waits on a full park as long as it takes, counts a long wait, and gives way only to a stop,
// with the inputs where they were
#[test]
fn park_wait_lasts_until_a_stop() {
    let io = SimulatedIo::new(Seed(35));
    let store = open(&io);
    hold_compaction(&store, true);
    half_dead(&store, &io);

    let volume = &store.state.volumes[0];
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job));
    while job.rewrite.phase != RewritePhase::Installing {
        job.rewrite.step();
    }
    let inputs = job.rewrite.input_numbers().to_vec();
    let stand_in = stand_in_parked(&store);
    fill_park(&store, &stand_in, park_size(&store));

    for _round in 0..10_000 {
        job.rewrite.step();
    }
    assert!(job.rewrite.is_running(), "steps alone ended the wait");
    io.advance_clock(Nanoseconds(MOST_PARK_WAIT.0 + 1));
    job.rewrite.step();
    assert!(job.rewrite.is_running(), "a long wait gave the written merge up");
    assert_eq!(volume.long_park_waits.load(Ordering::Acquire), 1);
    store.state.is_stopping.store(true, Ordering::Release);
    job.rewrite.step();
    assert!(!job.rewrite.is_running());
    assert!(inputs.iter().all(|input| volume.table.holds(*input)));
    io.kill();
}

// a full grave output leaves the rest of the graves in the data output, and the merge still lands with every key deleted
#[test]
fn graves_past_the_output() {
    let io = SimulatedIo::new(Seed(36));
    let store = open(&io);
    hold_compaction(&store, true);
    for number in 0..MANY_GRAVES {
        store.put(COLUMN, format!("key-{number:05}").as_bytes(), b"older").expect("put");
    }
    // a cover keeps this segment out of every merge, so the older rows stay under the graves
    store.delete_range(COLUMN, b"y", b"z").expect("delete");
    seal(&store, &io);
    for number in 0..MANY_GRAVES {
        store.delete(COLUMN, format!("key-{number:05}").as_bytes()).expect("delete");
    }
    store.put(COLUMN, b"zz", b"first").expect("put");
    store.put(COLUMN, b"zz", b"second").expect("put");
    seal(&store, &io);

    let volume = &store.state.volumes[0];
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    job.rewrite.most_grave_blocks = BlockCount(2);
    assert!(start_merge(&mut job));
    let inputs = job.rewrite.input_numbers().to_vec();
    let (mut has_full_writer, mut has_graves_in_data) = (false, false);
    while job.rewrite.is_running() {
        has_full_writer |= !job.rewrite.grave_writer.has_block_room();
        has_graves_in_data |= job.rewrite.writer.columns().iter().any(|column| column.grave_count.0 > 0);
        job.rewrite.step();
    }
    assert!(has_full_writer && has_graves_in_data);
    assert!(inputs.iter().all(|input| !volume.table.holds(*input)));
    assert_eq!(read(&store, b"key-00000"), None);
    assert_eq!(read(&store, format!("key-{:05}", MANY_GRAVES - 1).as_bytes()), None);
    assert_eq!(read(&store, b"zz"), Some(b"second".to_vec()));
    store.close().expect("close");
}

fn stand_in_parked(store: &Store) -> SegmentNumber {
    hold(&store.state.volumes[0].summary).rows().map(|row| row.facts.segment).min().expect("a row")
}

// the park holds a few slots for each lane, so a store with workers parks more
fn park_size(store: &Store) -> usize {
    hold(&store.state.volumes[0].parked).len()
}

// fill the park with retired stand-ins, which only a kill may leave behind
fn fill_park(store: &Store, stand_in: &SegmentNumber, count: usize) {
    let volume = &store.state.volumes[0];
    let pin = volume.table.pin(*stand_in).expect("a pin");
    for _slot in 0..count {
        let retired = RetiredSegment { file: pin.file.clone(), shard: 0, slot: 0 };
        park(volume, Some(retired), *stand_in, true);
    }
}

fn hold_walk(store: &Store, is_held: bool) {
    store.state.volumes[0].holds.walk.store(is_held, Ordering::Release);
    store.state.ask_compaction();
}

// segments over one key range, each with a version of one shared key and two keys of its own
fn pile_up(store: &Store, io: &SimulatedIo, count: u64) {
    for number in 0..count {
        store.put(COLUMN, format!("a-{number:03}").as_bytes(), b"low").expect("put");
        store.put(COLUMN, b"m-shared", format!("{number}").as_bytes()).expect("put");
        store.put(COLUMN, format!("z-{number:03}").as_bytes(), b"high").expect("put");
        seal(store, io);
    }
}

fn check_pile(store: &Store, count: u64) {
    for number in 0..count {
        assert_eq!(read(store, format!("a-{number:03}").as_bytes()), Some(b"low".to_vec()));
        assert_eq!(read(store, format!("z-{number:03}").as_bytes()), Some(b"high".to_vec()));
    }
    assert_eq!(read(store, b"m-shared"), Some(format!("{}", count - 1).into_bytes()));
}

fn row_totals(store: &Store) -> (u64, u64) {
    let (mut rows, mut dead) = (0, 0);
    for row in hold(&store.state.volumes[0].summary).rows() {
        let column = row.cursor().column().expect("a column");
        rows += column.row_count.0;
        dead += column.dead_rows.0;
    }
    (rows, dead)
}

// an overlap past the bound merges with no walk at all, and every key reads through it
#[test]
fn overlap_needs_no_walk() {
    let io = SimulatedIo::new(Seed(50));
    let store = open(&io);
    hold_walk(&store, true);
    pile_up(&store, &io, OVER_THE_BOUND);

    let volume = &store.state.volumes[0];
    assert!(segments(&store) <= MOST_OVERLAP as u64);
    assert!(volume.segments_rewritten.load(Ordering::Acquire) > 0);
    assert_eq!(volume.walks_waiting.load(Ordering::Acquire), segments(&store));
    assert!(hold(&volume.summary).rows().all(|row| row.facts.walked_blocks == 0 && !row.facts.is_backward));
    check_pile(&store, OVER_THE_BOUND);
    store.close().expect("close");
}

// the walk that comes after counts every row once, whichever segments merged ahead of it
#[test]
fn walk_after_merges_counts_once() {
    let io = SimulatedIo::new(Seed(51));
    let store = open(&io);
    hold_compaction(&store, true);
    hold_walk(&store, true);
    pile_up(&store, &io, MOST_OVERLAP as u64 / 2);
    hold_walk(&store, false);
    settle(&io);
    hold_walk(&store, true);
    let walked = segments(&store);
    pile_up(&store, &io, OVER_THE_BOUND);
    hold_compaction(&store, false);
    store.state.ask_compaction();
    settle(&io);
    assert!(segments(&store) < walked + OVER_THE_BOUND);

    hold_walk(&store, false);
    settle(&io);
    let keys = 2 * OVER_THE_BOUND + 1;
    let (rows, dead) = row_totals(&store);
    assert_eq!(rows - dead, keys);
    check_pile(&store, OVER_THE_BOUND);
    store.close().expect("close");
}

// a merge takes walked inputs or unwalked ones and never both, and the walked ones go first
#[test]
fn no_mix_of_walked() {
    let io = SimulatedIo::new(Seed(52));
    let store = open(&io);
    hold_compaction(&store, true);
    pile_up(&store, &io, MOST_OVERLAP as u64 / 2);
    hold_walk(&store, true);
    pile_up(&store, &io, OVER_THE_BOUND);

    let volume = &store.state.volumes[0];
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job));
    assert!(!job.rewrite.is_unwalked_merge());
    let rows = hold(&volume.summary);
    for input in job.rewrite.input_numbers() {
        assert!(rows.get(*input).expect("a row").facts.is_walked());
    }
    drop(rows);
    job.rewrite.abandon();
    store.close().expect("close");
}

// a walk target in progress holds a walked merge back, and leaves an unwalked one to go ahead without that segment
#[test]
fn walk_target_stays_out() {
    let io = SimulatedIo::new(Seed(53));
    let store = open(&io);
    hold_compaction(&store, true);
    hold_walk(&store, true);
    pile_up(&store, &io, OVER_THE_BOUND);

    let volume = &store.state.volumes[0];
    let target = hold(&volume.summary).rows().map(|row| row.facts.segment).min().expect("a row");
    volume.has_walk_target.store(true, Ordering::Release);
    volume.walk_segment.store(target.0, Ordering::Release);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job));
    assert!(!job.rewrite.input_numbers().contains(&target));
    job.rewrite.abandon();
    volume.has_walk_target.store(false, Ordering::Release);
    volume.walk_segment.store(NO_SEAL, Ordering::Release);
    store.close().expect("close");
}

// the walk leaves a merge's inputs alone, and takes them up again if the merge gives way
#[test]
fn walk_skips_merge_inputs() {
    let io = SimulatedIo::new(Seed(54));
    let store = open(&io);
    hold_compaction(&store, true);
    hold_walk(&store, true);
    pile_up(&store, &io, OVER_THE_BOUND);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job));
    let inputs = job.rewrite.input_numbers().to_vec();
    let mut walk = WalkJob::new(Arc::clone(&store.state), 0);
    assert!(walk.pick_target());
    assert!(!inputs.contains(&walk.target.expect("a target").segment));

    job.rewrite.abandon();
    assert!(walk.pick_target());
    assert!(inputs.contains(&walk.target.expect("a target").segment));
    store.state.volumes[0].has_walk_target.store(false, Ordering::Release);
    store.state.volumes[0].walk_segment.store(NO_SEAL, Ordering::Release);
    store.close().expect("close");
}

// a grave in an unwalked merge stays whatever it hides, since looking would cost a lookup over every segment
#[test]
fn unwalked_merge_keeps_graves() {
    let io = SimulatedIo::new(Seed(55));
    let store = open(&io);
    hold_compaction(&store, true);
    hold_walk(&store, true);
    store.put(COLUMN, b"a-gone", b"value").expect("put");
    store.delete(COLUMN, b"a-gone").expect("delete");
    pile_up(&store, &io, OVER_THE_BOUND);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job));
    while job.rewrite.is_running() {
        job.rewrite.step();
    }
    let graves = hold(&store.state.volumes[0].summary).rows().map(|row| row.cursor().column().expect("a column").grave_count.0).sum::<u64>();
    assert_eq!(graves, 1);
    assert_eq!(read(&store, b"a-gone"), None);
    store.close().expect("close");
}

// an output of unwalked inputs that open has to rebuild owes the forward walk its inputs did, and it comes out counted once
#[test]
fn rebuilt_output_walks_forward() {
    let io = SimulatedIo::new(Seed(56));
    let store = open(&io);
    hold_compaction(&store, true);
    hold_walk(&store, true);
    pile_up(&store, &io, OVER_THE_BOUND);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job));
    while job.rewrite.phase != RewritePhase::Installing {
        job.rewrite.step();
    }
    let output = job.rewrite.output.clone().expect("an output").number;
    drop(job);
    io.kill();
    drop(store);

    let store = open(&io);
    hold_compaction(&store, true);
    let rows = hold(&store.state.volumes[0].summary);
    let row = rows.get(output).expect("a row");
    assert!(!row.facts.is_backward);
    assert_eq!(row.facts.walked_blocks, 0);
    drop(rows);
    settle(&io);
    let (rows, dead) = row_totals(&store);
    assert_eq!(rows - dead, 2 * OVER_THE_BOUND + 1);
    check_pile(&store, OVER_THE_BOUND);
    store.close().expect("close");
}

// a crash at any step of an unwalked merge keeps every read, and the walk after it counts every row once
#[test]
fn unwalked_crashes() {
    for crash_at in (0..UNWALKED_CRASH_POINTS).map(|point| point * UNWALKED_CRASH_STRIDE) {
        for is_power_cut in [false, true] {
            let io = SimulatedIo::new(Seed(57));
            let store = open(&io);
            hold_compaction(&store, true);
            hold_walk(&store, true);
            pile_up(&store, &io, OVER_THE_BOUND);
            hold_compaction(&store, false);
            store.state.ask_compaction();
            io.arm_crash(OperationCount(crash_at));
            for _step in 0..STEPS {
                if io.is_crashed() {
                    break;
                }
                if io.step_any().is_none() {
                    io.advance_clock(TICK);
                    if io.step_any().is_none() {
                        break;
                    }
                }
            }
            io.disarm_crash();
            if is_power_cut {
                io.power_cut();
            } else {
                io.kill();
            }
            drop(store);

            let store = open(&io);
            assert!(!has_partial(&io), "crash at {crash_at}");
            settle(&io);
            check_pile(&store, OVER_THE_BOUND);
            let (rows, dead) = row_totals(&store);
            assert_eq!(rows - dead, 2 * OVER_THE_BOUND + 1, "crash at {crash_at}");
            assert!(hold(&store.state.volumes[0].damaged).is_empty(), "crash at {crash_at}");
            store.close().expect("close");
        }
    }
}

// a half-walked segment's rows are half counted, so it waits for its walk to end
#[test]
fn half_walked_stays_out() {
    let io = SimulatedIo::new(Seed(58));
    let store = open(&io);
    hold_compaction(&store, true);
    hold_walk(&store, true);
    for number in 0..TWO_BLOCK_ROWS {
        store.put(COLUMN, format!("key-{number:06}").as_bytes(), b"value").expect("put");
    }
    let segment = seal(&store, &io);

    let mut rows = hold(&store.state.volumes[0].summary);
    assert!(rows.get(segment).expect("a row").facts.block_count > 1);
    assert!(is_overlap_input(&rows.get(segment).expect("a row")));
    rows.apply_walk(segment, 1, &[]);
    assert!(!is_overlap_input(&rows.get(segment).expect("a row")));
    drop(rows);
    store.close().expect("close");
}

// the planner steps an unwalked merge with a walk target in progress, so an overlap past the bound never waits for one
#[test]
fn merge_ahead_of_a_walk_target() {
    let io = SimulatedIo::new(Seed(59));
    let store = open(&io);
    hold_compaction(&store, true);
    hold_walk(&store, true);
    pile_up(&store, &io, OVER_THE_BOUND);
    hold_compaction(&store, false);

    let state = &store.state;
    let mut planner = Planner {
        state: Arc::clone(state),
        walks: vec![WalkJob::new(Arc::clone(state), 0)],
        compactions: vec![CompactJob::new(Arc::clone(state), 0)],
        filters: Vec::new(),
        last: 0,
    };
    assert!(planner.walks[0].pick_target());
    state.volumes[0].wants_compaction.store(true, Ordering::Release);
    let is_unwalked = |job: &CompactJob<SimulatedIo>| shape::any_rewrite(job, RewriteJob::is_unwalked_merge);
    for _step in 0..OVER_THE_BOUND {
        planner.step();
        if is_unwalked(&planner.compactions[0]) {
            break;
        }
    }
    assert!(is_unwalked(&planner.compactions[0]));
    shape::abandon_rewrites(&mut planner.compactions[0]);
    state.volumes[0].has_walk_target.store(false, Ordering::Release);
    state.volumes[0].walk_segment.store(NO_SEAL, Ordering::Release);
    drop(planner);
    store.close().expect("close");
}

// a key found in the newest segment stops the look there, so a read costs the distance to its previous version
#[test]
fn newest_first_stops() {
    let io = SimulatedIo::new(Seed(60));
    let store = open(&io);
    hold_compaction(&store, true);
    for number in 0..MOST_OVERLAP as u64 {
        store.put(COLUMN, b"a", b"low").expect("put");
        store.put(COLUMN, b"m-shared", format!("{number}").as_bytes()).expect("put");
        store.put(COLUMN, b"z", b"high").expect("put");
        seal(&store, &io);
    }

    let state = &store.state;
    let volume = &state.volumes[0];
    let directory = state.directories[COLUMN.0 as usize].as_ref().expect("a directory");
    let read = state.key_read_now(COLUMN, b"m-shared").expect("read");
    let mut reader = state.footer_readers.lease();
    let found = in_place(state.walk_candidates(&InPlace, volume, directory, &read, &mut reader, WalkAsk::EVERY));
    assert_eq!(reader.asked, 1);
    assert!(found.expect("a read").is_some());
    store.close().expect("close");
}

// a full unwalked output below small unwalked segments stays out of a merge, so the small ones never wait on its walk
#[test]
fn full_segment_stays_out() {
    let io = SimulatedIo::new(Seed(61));
    let store = open(&io);
    // the merge mechanics under test, not the size of the records, decide what merges
    store.state.volumes[0].holds.small_bound.store(true, Ordering::Release);
    hold_compaction(&store, true);
    hold_walk(&store, true);
    let half = store.state.volumes[0].segment_bytes.0 as usize / 2;
    store.put(COLUMN, b"a", &vec![1; half + 1]).expect("put");
    store.put(COLUMN, b"z", b"high").expect("put");
    let full = seal(&store, &io);
    pile_up(&store, &io, OVER_THE_BOUND);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job));
    assert!(job.rewrite.is_unwalked_merge());
    assert_eq!(job.rewrite.input_numbers().len(), MOST_MERGE_SEGMENTS);
    assert!(!job.rewrite.input_numbers().contains(&full));
    job.rewrite.abandon();
    store.close().expect("close");
}

// with only large segments near, they merge whole, and the output's cut keeps each one under a segment's size
#[test]
fn large_merge_when_alone() {
    let io = SimulatedIo::new(Seed(62));
    let store = open(&io);
    // the merge mechanics under test, not the size of the records, decide what merges
    store.state.volumes[0].holds.small_bound.store(true, Ordering::Release);
    hold_compaction(&store, true);
    hold_walk(&store, true);
    let large = store.state.volumes[0].segment_bytes.0 as usize * 3 / 5;
    for _segment in 0..OVER_THE_BOUND {
        store.put(COLUMN, b"a", &vec![1; large]).expect("put");
        store.put(COLUMN, b"z", b"high").expect("put");
        seal(&store, &io);
    }
    store.put(COLUMN, b"a", b"small").expect("put");
    store.put(COLUMN, b"z", b"high").expect("put");
    let small = seal(&store, &io);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job));
    assert_eq!(job.rewrite.input_numbers().len(), MOST_MERGE_SEGMENTS);
    assert!(job.rewrite.input_numbers().contains(&small));
    job.rewrite.abandon();
    store.close().expect("close");
}

// small segments go first and merge among themselves, so the large ones wait for a merge of their own size and aren't copied
// again with the small
#[test]
fn small_segments_merge_by_size() {
    let io = SimulatedIo::new(Seed(63));
    let store = open(&io);
    // the merge mechanics under test, not the size of the records, decide what merges
    store.state.volumes[0].holds.small_bound.store(true, Ordering::Release);
    hold_compaction(&store, true);
    hold_walk(&store, true);
    let large = store.state.volumes[0].segment_bytes.0 as usize * 3 / 5;
    for _segment in 0..OVER_THE_BOUND {
        store.put(COLUMN, b"a", &vec![1; large]).expect("put");
        store.put(COLUMN, b"z", b"high").expect("put");
        seal(&store, &io);
    }
    let mut small = Vec::new();
    for _segment in 0..3 {
        store.put(COLUMN, b"a", b"small").expect("put");
        store.put(COLUMN, b"z", b"high").expect("put");
        small.push(seal(&store, &io));
    }

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job));
    let mut inputs = job.rewrite.input_numbers().to_vec();
    inputs.sort_unstable();
    small.sort_unstable();
    assert_eq!(inputs, small, "a large segment joined the small ones' merge");
    job.rewrite.abandon();
    store.close().expect("close");
}

// a merge whose inputs are still on disk at a crash, from a job stopped between its save and its removes
fn inputs_left_on_disk(store: Store, io: &SimulatedIo) -> Vec<SegmentNumber> {
    hold_compaction(&store, true);
    grave_over_older(&store, io);
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    run_to_install(&mut job);
    let inputs = job.rewrite.input_numbers().to_vec();
    job.rewrite.step();
    assert!(!job.rewrite.is_running());
    io.kill();
    drop(job);
    drop(store);
    inputs
}

// an input open can't remove stays out of the lists, the store serves, the graves wait, and a retry takes it
#[test]
fn input_held_at_open() {
    let io = SimulatedIo::new(Seed(37));
    let inputs = inputs_left_on_disk(open(&io), &io);
    assert!(inputs.iter().all(|input| has_file(&io, *input)));

    io.fail_removes(OperationCount(1));
    let store = open(&io);
    let volume = &store.state.volumes[0];
    assert_eq!(volume.segments_parked.load(Ordering::Acquire), 1);
    assert!(inputs.iter().all(|input| !volume.table.holds(*input)));
    assert_eq!(read(&store, b"key"), None);
    assert_eq!(read(&store, b"d"), Some(b"first".to_vec()));
    assert!(store.state.unlisted_segments(volume).expect("list").is_empty());

    settle(&io);
    assert_eq!(volume.segments_parked.load(Ordering::Acquire), 0);
    assert!(inputs.iter().all(|input| !has_file(&io, *input)));
    store.close().expect("close");
}

// a remove that keeps failing costs no open, however many in a row, and the first one that works ends it
#[test]
fn input_held_across_opens() {
    let io = SimulatedIo::new(Seed(38));
    let inputs = inputs_left_on_disk(open(&io), &io);

    for _open in 0..3 {
        io.fail_removes(OperationCount(u64::MAX));
        let store = open(&io);
        assert_eq!(read(&store, b"key"), None);
        io.kill();
        drop(store);
    }

    io.fail_removes(OperationCount(0));
    let store = open(&io);
    settle(&io);
    assert_eq!(store.state.volumes[0].segments_parked.load(Ordering::Acquire), 0);
    assert!(inputs.iter().all(|input| !has_file(&io, *input)));
    assert_eq!(read(&store, b"key"), None);
    store.close().expect("close");
}

// a segment with newer rows can sort between older ones by lowest key, and a read walks on past the older one to find it
#[test]
fn newest_past_an_older_segment() {
    let io = SimulatedIo::new(Seed(64));
    let store = open(&io);
    hold_compaction(&store, true);
    let put_range = |lowest: &[u8], highest: &[u8], value: &[u8]| {
        store.put(COLUMN, lowest, b"edge").expect("put");
        store.put(COLUMN, b"m", value).expect("put");
        store.put(COLUMN, highest, b"edge").expect("put");
        seal(&store, &io)
    };
    put_range(b"b", b"y", b"oldest");
    put_range(b"a", b"z", b"middle");
    let newest = put_range(b"c", b"x", b"newest");

    // the spot cache would answer a plain get, so this asks the footers
    let state = &store.state;
    let read = state.key_read_now(COLUMN, b"m").expect("read");
    let found = in_place(state.search_footers(&InPlace, &state.volumes[0], &read)).expect("search").expect("a row");
    assert_eq!(found.segment, newest);
    store.close().expect("close");
}

fn plant(io: &SimulatedIo, number: SegmentNumber, bytes: &[u8]) {
    let file = io.create_file(&data_path(Path::new(ROOT), number), ByteCount(1)).expect("create");
    if !bytes.is_empty() {
        file.write_at(FileOffset(0), &[IoSlice::new(bytes)]).expect("write");
    }
    io.close_file(file);
}

const PLANTED: SegmentNumber = SegmentNumber(u64::MAX / 4);

// a full park excuses an empty file, and the count shows it
#[test]
fn full_park_excuses_an_empty_file() {
    let io = SimulatedIo::new(Seed(65));
    let store = open(&io);
    hold_compaction(&store, true);
    half_dead(&store, &io);
    let volume = &store.state.volumes[0];
    fill_park(&store, &stand_in_parked(&store), park_size(&store));
    plant(&io, PLANTED, &[]);

    assert!(store.state.unlisted_segments(volume).expect("list").is_empty());
    io.fail_removes(OperationCount(1));
    assert!(!store.state.remove_or_park(volume, Path::new(ROOT), PLANTED));
    assert_eq!(volume.empties_left.load(Ordering::Acquire), 1);
    io.kill();
}

// a full park excuses nothing with a record in it
#[test]
fn full_park_excuses_no_record() {
    let io = SimulatedIo::new(Seed(66));
    let store = open(&io);
    hold_compaction(&store, true);
    half_dead(&store, &io);
    let volume = &store.state.volumes[0];
    fill_park(&store, &stand_in_parked(&store), park_size(&store));
    plant(&io, PLANTED, b"a record");

    assert_eq!(store.state.unlisted_segments(volume).expect("list"), vec![PLANTED]);
    io.kill();
}

// an empty file with room in the park belongs in it
#[test]
fn park_room_excuses_nothing() {
    let io = SimulatedIo::new(Seed(67));
    let store = open(&io);
    hold_compaction(&store, true);
    plant(&io, PLANTED, &[]);

    assert_eq!(store.state.unlisted_segments(&store.state.volumes[0]).expect("list"), vec![PLANTED]);
    io.kill();
}

// a data file's removal has to reach the directory before its rows go: a power cut that kept only the rows' would leave a file
// that reads as damage, so a sync that fails holds the file and its rows
#[test]
fn data_goes_before_its_rows() {
    let io = SimulatedIo::new(Seed(69));
    let store = open(&io);
    hold_compaction(&store, true);
    let volume = &store.state.volumes[0];
    plant(&io, PLANTED, &[]);

    io.fail_directory_syncs(OperationCount(1));
    assert!(!store.state.remove_or_park(volume, Path::new(ROOT), PLANTED));
    assert_eq!(volume.segments_parked.load(Ordering::Acquire), 1);
    io.kill();
}

// a close that can't remove a data output leaves its grave output too, so the reopen that takes the merge keeps the graves
#[test]
fn close_keeps_graves_with_a_stuck_output() {
    let io = SimulatedIo::new(Seed(68));
    let store = open(&io);
    hold_compaction(&store, true);
    grave_over_older(&store, &io);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    run_to_both_outputs(&mut job);
    io.fail_removes(OperationCount(1));
    store.close().expect("close");
    drop(job);
    drop(store);

    let store = open(&io);
    assert_eq!(read(&store, b"key"), None);
    store.close().expect("close");
}

const SET_KEYS: u64 = 400;
const SET_VALUE_BYTES: usize = 100;

fn set_value(number: u64, version: u8) -> Vec<u8> {
    vec![version; SET_VALUE_BYTES + number as usize % 7]
}

fn set_key(number: u64) -> Vec<u8> {
    format!("key-{number:05}").into_bytes()
}

// two segments over one range, the older mostly replaced, with enough bytes for a merge to cut
fn set_of_inputs(store: &Store, io: &SimulatedIo) {
    for number in 0..SET_KEYS {
        store.put(COLUMN, &set_key(number), &set_value(number, 1)).expect("put");
    }
    seal(store, io);
    for number in (0..SET_KEYS).filter(|number| number % 4 != 0) {
        store.put(COLUMN, &set_key(number), &set_value(number, 2)).expect("put");
    }
    seal(store, io);
}

fn check_set(store: &Store) {
    for number in 0..SET_KEYS {
        let version = if number % 4 == 0 { 1 } else { 2 };
        assert_eq!(read(store, &set_key(number)), Some(set_value(number, version)), "key {number}");
    }
}

// run a merge up to its install, cutting at small sizes, with the outputs written so far
fn run_to_set(job: &mut CompactJob<SimulatedIo>) -> usize {
    job.rewrite.is_key_order_held = true;
    job.rewrite.cut_floor = 0;
    assert!(start_merge(job));
    while job.rewrite.phase != RewritePhase::Installing {
        job.rewrite.step();
    }
    let outputs = job.rewrite.staged_count() + 1;
    assert!(outputs > 1);
    outputs
}

// the outputs of one merge cover ranges that never meet
#[test]
fn outputs_never_overlap() {
    let io = SimulatedIo::new(Seed(70));
    let store = open(&io);
    hold_compaction(&store, true);
    set_of_inputs(&store, &io);
    let old: Vec<SegmentNumber> = hold(&store.state.volumes[0].summary).rows().map(|row| row.facts.segment).collect();

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    job.rewrite.is_key_order_held = true;
    let count = run_to_set(&mut job);
    job.rewrite.step();
    assert!(!job.rewrite.is_running());

    let volume = &store.state.volumes[0];
    let rows = hold(&volume.summary);
    let outputs: Vec<_> = rows.rows().filter(|row| !old.contains(&row.facts.segment)).collect();
    assert_eq!(outputs.len(), count);
    for (index, left) in outputs.iter().enumerate() {
        assert!(outputs[index + 1..].iter().all(|right| !overlaps(left, right)));
    }
    drop(rows);
    check_set(&store);
    store.close().expect("close");
}

// a read with every output listed and the inputs still there finds every key, whichever output it landed in
#[test]
fn read_between_a_set_and_its_inputs() {
    let io = SimulatedIo::new(Seed(71));
    let store = open(&io);
    hold_compaction(&store, true);
    set_of_inputs(&store, &io);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    job.rewrite.is_key_order_held = true;
    run_to_set(&mut job);
    // the spot cache would answer a plain get, so this asks the footers, which go by the lists
    let state = &store.state;
    job.rewrite
        .install_with(|| {
            for number in 0..SET_KEYS {
                let key = set_key(number);
                let read = state.key_read_now(COLUMN, &key).expect("read");
                assert!(in_place(state.search_footers(&InPlace, &state.volumes[0], &read)).expect("search").is_some(), "key {number}");
            }
        }, || {})
        .expect("install");
    check_set(&store);
    store.close().expect("close");
}

// a give-up that can't remove the last output keeps the whole set, and a power cut takes the set as the merge
#[test]
fn stuck_last_output_keeps_the_set() {
    let io = SimulatedIo::new(Seed(72));
    let store = open(&io);
    hold_compaction(&store, true);
    set_of_inputs(&store, &io);

    let state = &store.state;
    let mut job = CompactJob::new(Arc::clone(state), 0);
    let count = run_to_set(&mut job);
    let slot = state.cues.take(0, WritePlace(state.read_point(&state.volumes[0]).0 - 3)).expect("a slot");
    io.fail_removes(OperationCount(1));
    job.rewrite.step();
    state.cues.release(slot);
    assert!(!job.rewrite.is_running());
    assert_eq!(state.volumes[0].stray_outputs.load(Ordering::Acquire) as usize, count);

    io.power_cut();
    drop(job);
    drop(store);
    let store = open(&io);
    check_set(&store);
    store.close().expect("close");
}

// a key's versions stay in one output, kept for a cue with a cut due at every key
#[test]
fn cut_keeps_a_key_whole() {
    let io = SimulatedIo::new(Seed(73));
    let store = open(&io);
    hold_compaction(&store, true);
    for number in 0..SET_KEYS {
        store.put(COLUMN, &set_key(number), &set_value(number, 1)).expect("put");
    }
    let cue = store.cue().expect("cue");
    for number in 0..SET_KEYS {
        store.put(COLUMN, &set_key(number), &set_value(number, 2)).expect("put");
    }
    let only = seal(&store, &io);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    job.rewrite.is_key_order_held = true;
    job.rewrite.forced = Some(only);
    job.rewrite.cut_floor = 0;
    assert!(start_merge(&mut job));
    while job.rewrite.is_running() {
        job.rewrite.step();
    }

    let volume = &store.state.volumes[0];
    let rows = hold(&volume.summary);
    let outputs: Vec<_> = rows.rows().collect();
    assert!(outputs.len() > 1);
    for (index, left) in outputs.iter().enumerate() {
        assert!(outputs[index + 1..].iter().all(|right| !overlaps(left, right)));
    }
    drop(rows);
    let mut buf = Vec::new();
    for number in 0..SET_KEYS {
        assert!(cue.get(COLUMN, &set_key(number), &mut buf).expect("get"));
        assert_eq!(buf, set_value(number, 1));
        assert_eq!(read(&store, &set_key(number)), Some(set_value(number, 2)));
    }
    drop(cue);
    store.close().expect("close");
}

// a crash at any step leaves the inputs or the whole set, and every key reads and counts once either way
#[test]
fn set_crashes() {
    let mut sets = 0;
    let mut crash_at = 0;
    loop {
        let mut is_finished = false;
        for is_power_cut in [false, true] {
            let io = SimulatedIo::new(Seed(74));
            let store = open(&io);
            hold_compaction(&store, true);
            set_of_inputs(&store, &io);

            let mut job = CompactJob::new(Arc::clone(&store.state), 0);
            job.rewrite.cut_floor = 0;
            assert!(job.rewrite.start());
            let inputs = job.rewrite.input_numbers().to_vec();
            io.arm_crash(OperationCount(crash_at));
            while job.rewrite.is_running() && !io.is_crashed() {
                job.rewrite.step();
            }
            is_finished |= !io.is_crashed();
            io.disarm_crash();
            if is_power_cut {
                io.power_cut();
            } else {
                io.kill();
            }
            drop(job);
            drop(store);

            let store = open(&io);
            assert!(!has_partial(&io), "crash at {crash_at}");
            check_set(&store);
            let volume = &store.state.volumes[0];
            let held = inputs.iter().filter(|input| volume.table.holds(**input)).count();
            assert!(held == 0 || held == inputs.len(), "crash at {crash_at}: {held} inputs of {}", inputs.len());
            sets += u64::from(held == 0);
            settle(&io);
            check_set(&store);
            assert!(hold(&volume.damaged).is_empty(), "crash at {crash_at}");
            store.close().expect("close");
        }
        if is_finished {
            break;
        }
        crash_at += SET_CRASH_STRIDE;
    }
    assert!(sets > 0);
}

// a quiet volume under the overlap bound, with nothing past the trigger, starts no merge
#[test]
fn idle_starts_no_merge() {
    let io = SimulatedIo::new(Seed(75));
    let store = open(&io);
    for number in 0..MOST_OVERLAP as u64 / 2 {
        store.put(COLUMN, format!("a-{number:03}").as_bytes(), b"low").expect("put");
        store.put(COLUMN, format!("z-{number:03}").as_bytes(), b"high").expect("put");
        seal(&store, &io);
    }
    for _tick in 0..IDLE_TICKS {
        store.state.ask_compaction();
        settle(&io);
    }
    assert_eq!(store.state.volumes[0].segments_rewritten.load(Ordering::Acquire), 0);
    store.close().expect("close");
}

// a quiet volume a third dead spends its idle time reclaiming every dead byte
#[test]
fn idle_volume_reclaims_every_dead_byte() {
    let io = SimulatedIo::new(Seed(77));
    let store = open(&io);
    let value = vec![7; IDLE_VALUE_BYTES];
    for number in 0..IDLE_KEYS {
        store.put(COLUMN, format!("key-{number:04}").as_bytes(), &value).expect("put");
    }
    seal(&store, &io);
    for number in 0..IDLE_KEYS / 3 {
        store.put(COLUMN, format!("key-{number:04}").as_bytes(), &value).expect("put");
    }
    seal(&store, &io);
    for _tick in 0..IDLE_TICKS {
        io.advance_clock(IDLE_LOOK);
        store.state.ask_compaction();
        settle(&io);
    }
    let volume = &store.state.volumes[0];
    assert!(volume.segments_rewritten.load(Ordering::Acquire) > 0, "an idle volume with dead bytes rewrote nothing");
    assert_eq!(hold(&volume.summary).orders().dead_bytes(), 0, "the volume left dead bytes");
    store.close().expect("close");
}

// disjoint narrow segments under one wide one are no crowd, whatever one segment's count of neighbours says
#[test]
fn narrow_neighbours_are_no_crowd() {
    let io = SimulatedIo::new(Seed(76));
    let store = open(&io);
    hold_compaction(&store, true);
    for number in 0..OVER_THE_BOUND {
        store.put(COLUMN, format!("k-{number:03}-a").as_bytes(), b"value").expect("put");
        store.put(COLUMN, format!("k-{number:03}-b").as_bytes(), b"value").expect("put");
        seal(&store, &io);
    }
    store.put(COLUMN, b"a", b"value").expect("put");
    store.put(COLUMN, b"z", b"value").expect("put");
    seal(&store, &io);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(!start_merge(&mut job));
    store.close().expect("close");
}

// the segment that wins back the most for what it costs to read goes first, whatever its number
#[test]
fn most_won_back_goes_first() {
    let io = SimulatedIo::new(Seed(77));
    let store = open(&io);
    hold_compaction(&store, true);
    for number in 0..20 {
        store.put(COLUMN, format!("a-{number:02}").as_bytes(), b"value").expect("put");
    }
    let half = seal(&store, &io);
    for number in 0..20 {
        store.put(COLUMN, format!("b-{number:02}").as_bytes(), b"value").expect("put");
    }
    let most = seal(&store, &io);
    for number in 0..10 {
        store.put(COLUMN, format!("a-{number:02}").as_bytes(), b"newer").expect("put");
    }
    for number in 0..19 {
        store.put(COLUMN, format!("b-{number:02}").as_bytes(), b"newer").expect("put");
    }
    seal(&store, &io);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job));
    assert_eq!(job.rewrite.input_numbers()[0], most);
    assert!(!job.rewrite.input_numbers().contains(&half));
    job.rewrite.abandon();
    store.close().expect("close");
}

// a big live record beside small dead ones stays put: the rewrite would copy far more than it frees
#[test]
fn rows_alone_start_no_copy() {
    let io = SimulatedIo::new(Seed(78));
    let store = open(&io);
    hold_reclaim(&store);
    store.put(COLUMN, b"big", &vec![7; MERGE_BUFFER_BYTES]).expect("put");
    store.put(COLUMN, b"a", b"first").expect("put");
    store.put(COLUMN, b"b", b"first").expect("put");
    store.put(COLUMN, b"c", b"first").expect("put");
    let older = seal(&store, &io);
    store.put(COLUMN, b"a", b"second").expect("put");
    store.put(COLUMN, b"b", b"second").expect("put");
    store.put(COLUMN, b"c", b"second").expect("put");
    seal(&store, &io);

    assert!(store.state.volumes[0].table.holds(older));
    assert_eq!(store.state.volumes[0].segments_rewritten.load(Ordering::Acquire), 0);
    store.close().expect("close");
}

fn run_to_end(job: &mut CompactJob<SimulatedIo>) {
    while job.rewrite.is_running() {
        job.rewrite.step();
    }
}

// a segment of graves that all hide a row gets one look, and no more until a segment leaves the volume
#[test]
fn hiding_graves_get_one_look() {
    let io = SimulatedIo::new(Seed(79));
    let store = open(&io);
    hold_reclaim(&store);
    hold_compaction(&store, true);
    let value = vec![7; IDLE_VALUE_BYTES];
    for number in 0..IDLE_KEYS {
        store.put(COLUMN, format!("key-{number:04}").as_bytes(), &value).expect("put");
    }
    seal(&store, &io);
    for number in 0..IDLE_KEYS / 5 {
        store.delete(COLUMN, format!("key-{number:04}").as_bytes()).expect("delete");
    }
    let graves = seal(&store, &io);

    let volume = &store.state.volumes[0];
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job));
    assert_eq!(job.rewrite.input_numbers(), &[graves]);
    run_to_end(&mut job);
    assert_eq!(volume.segments_rewritten.load(Ordering::Acquire), 0);
    assert!(!job.rewrite.start(), "the graves would be read again");

    volume.segments_unlinked.fetch_add(1, Ordering::AcqRel);
    assert!(start_merge(&mut job));
    job.rewrite.abandon();
    store.close().expect("close");
}

// a segment a cue keeps whole writes no second output: the copy that found nothing to drop counts for as long as the cue lives,
// and the unlink takes the segment once the cue goes
#[test]
fn cue_held_rows_copy_once() {
    let io = SimulatedIo::new(Seed(80));
    let store = open(&io);
    hold_compaction(&store, true);
    let value = vec![7; IDLE_VALUE_BYTES];
    for number in 0..IDLE_KEYS {
        store.put(COLUMN, format!("key-{number:04}").as_bytes(), &value).expect("put");
    }
    let older = seal(&store, &io);
    let cue = store.cue().expect("cue");
    for number in 0..IDLE_KEYS {
        store.put(COLUMN, format!("key-{number:04}").as_bytes(), b"newer").expect("put");
    }
    seal(&store, &io);

    let volume = &store.state.volumes[0];
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job));
    run_to_end(&mut job);
    assert_eq!(volume.segments_rewritten.load(Ordering::Acquire), 0);
    assert!(!job.rewrite.start(), "the copy would run again");

    // with the cue gone the unlink takes the wholly dead segment, so no copy runs again
    drop(cue);
    assert!(!start_merge(&mut job), "a copy took a segment the unlink frees");
    hold_compaction(&store, false);
    store.state.ask_compaction();
    settle(&io);
    assert!(!volume.table.holds(older), "the unlink never took the segment");
    assert_eq!(volume.segments_rewritten.load(Ordering::Acquire), 0);
    store.close().expect("close");
}

// a rewrite lays records down in the order they lay in its input, whatever order their keys sort in
#[test]
fn rewrite_keeps_offset_order() {
    let io = SimulatedIo::new(Seed(81));
    let store = open(&io);
    hold_compaction(&store, true);
    let value = vec![7; IDLE_VALUE_BYTES];
    // written from the highest key down, so key order runs against offset order
    for number in (0..IDLE_KEYS).rev() {
        store.put(COLUMN, format!("key-{number:04}").as_bytes(), &value).expect("put");
    }
    let older = seal(&store, &io);
    for number in (0..IDLE_KEYS).step_by(2) {
        store.put(COLUMN, format!("key-{number:04}").as_bytes(), b"newer").expect("put");
    }
    for number in (1..IDLE_KEYS).step_by(4) {
        store.put(COLUMN, format!("key-{number:04}").as_bytes(), b"newer").expect("put");
    }
    seal(&store, &io);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job));
    assert_eq!(job.rewrite.input_numbers(), &[older]);
    run_to_end(&mut job);
    assert!(!store.state.volumes[0].table.holds(older));

    let volume = &store.state.volumes[0];
    let offset_of = |number: u64| {
        let key = format!("key-{number:04}");
        let read = store.state.key_read(COLUMN, key.as_bytes(), WritePlace(u64::MAX), false).expect("read");
        in_place(store.state.search_footers(&InPlace, volume, &read)).expect("search").expect("a row").row.offset
    };
    let kept: Vec<u64> = (3..IDLE_KEYS).step_by(4).collect();
    for pair in kept.windows(2) {
        assert!(offset_of(pair[0]) > offset_of(pair[1]), "keys {pair:?}");
    }
    store.close().expect("close");
}

// a stripe that lists more rows than it holds narrows, and every record still lands where its row says
#[test]
fn stripe_narrows_when_full() {
    let io = SimulatedIo::new(Seed(82));
    let store = open(&io);
    hold_compaction(&store, true);
    for number in 0..STRIPE_KEYS {
        store.put(COLUMN, format!("key-{number:06}").as_bytes(), b"first").expect("put");
    }
    seal(&store, &io);
    for number in (0..STRIPE_KEYS).step_by(2) {
        store.put(COLUMN, format!("key-{number:06}").as_bytes(), b"second").expect("put");
    }
    seal(&store, &io);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job));
    run_to_end(&mut job);
    assert!(store.state.volumes[0].segments_rewritten.load(Ordering::Acquire) > 0);
    for number in 0..STRIPE_KEYS {
        let expected: &[u8] = if number % 2 == 0 { b"second" } else { b"first" };
        assert_eq!(read(&store, format!("key-{number:06}").as_bytes()).as_deref(), Some(expected), "key {number}");
    }
    store.close().expect("close");
}

/// A device that publishes a new list at a walk's first read, as a seal landing under it would
struct ChangesList<'list> {
    directory: &'list ColumnDirectory,
    has_changed: AtomicBool,
}

impl ChangesList<'_> {
    fn change(&self) {
        if !self.has_changed.swap(true, Ordering::AcqRel) {
            self.directory.replace(None, &[SegmentNumber(u64::MAX)]);
        }
    }
}

impl DeviceReads<SimulatedFile> for ChangesList<'_> {
    fn read_exact(&self, file: &SimulatedFile, offset: FileOffset, buf: &mut [u8]) -> impl Future<Output = std::io::Result<()>> + Send {
        self.change();
        ready(file.read_exact_at(offset, buf))
    }

    fn read_parts(&self, file: &SimulatedFile, offset: FileOffset, parts: &mut [IoSliceMut<'_>]) -> impl Future<Output = std::io::Result<()>> + Send {
        self.change();
        ready(file.read_parts_at(offset, parts))
    }
}

// an overlap merge copies records, so while writes arrive the first worker runs it and the planner leaves it to that worker
#[test]
fn first_worker_takes_overlap() {
    let io = SimulatedIo::new(Seed(74));
    let mut config = config();
    config.memory_budget = ByteCount(256 * 1024 * 1024);
    config.threads.maintenance_workers = Some(1);
    let store = ReelStore::open(io.clone(), &config).expect("open");
    hold_compaction(&store, true);
    hold_walk(&store, true);
    pile_up(&store, &io, OVER_THE_BOUND);
    hold_compaction(&store, false);
    // a seal just started and the last fill took an hour, so writes still arrive
    let volume = &store.state.volumes[0];
    volume.seal_interval.store(3_600_000_000_000, Ordering::Relaxed);
    volume.seal_started_at.store(io.clock_reading().0, Ordering::Relaxed);
    volume.last_write_at.store(io.clock_reading().0, Ordering::Relaxed);

    let state = &store.state;
    let mut planner = RewriteJob::new(Arc::clone(state), 0, PLANNER_LANE);
    assert!((0..OVER_THE_BOUND).all(|_look| planner.pick(&state.volumes[0]).is_none()));
    let mut worker = RewriteJob::new(Arc::clone(state), 0, PLANNER_LANE + 1);
    assert!((0..OVER_THE_BOUND).any(|_look| worker.pick(&state.volumes[0]).is_some()));
    worker.abandon();
    drop(planner);
    drop(worker);
    store.close().expect("close");
}

// a worker takes its running rewrite out of the seat and steps it with no lock held, so the planner's hand-out finishes
// inside its line while a step parks on a read, and the rewrite goes back once it ends
#[test]
fn seat_lock_is_held_for_a_swap() {
    let io = SimulatedIo::new(Seed(80));
    let mut config = config();
    config.threads.maintenance_workers = Some(1);
    config.memory_budget = WORKER_BUDGET;
    let store = ReelStore::open(io.clone(), &config).expect("open");
    hold_compaction(&store, true);
    let state = &store.state;
    let key = |number: u64| format!("k-{number:05}").into_bytes();
    for number in 0..SEAT_KEYS {
        store.put(COLUMN, &key(number), b"first").expect("put");
    }
    seal(&store, &io);
    for number in 0..SEAT_KEYS * 3 / 4 {
        store.put(COLUMN, &key(number), b"second").expect("put");
    }
    seal(&store, &io);

    // no overlap stands, so the record rewrite needs no look ahead of it
    state.volumes[0].is_over_bound.store(false, Ordering::Release);
    let mut job = CompactJob::new(Arc::clone(state), 0);
    assert_eq!(job.seats.len(), 1);
    assert!(job.hand_out(), "no rewrite started");
    let seats = Arc::clone(&job.seats);
    let seat = &seats[0];
    let mut worker = WalkWorker {
        state: Arc::clone(state),
        index: 0,
        next_volume: 0,
        counter: BlockCounter::new(Arc::clone(state)),
        seats: vec![Arc::clone(&job.seats)],
        rewrite: None,
        seat: (0, 0),
    };
    // the planner claims and leaves the populate to the worker: the first step maps the region and opens the output, and the steps
    // after it read
    assert!(hold(&job.seats[0].job).as_ref().is_some_and(|held| held.is_claimed() && held.region_bytes().0 == 0));
    assert!(worker.take_seat(), "the worker never took the handed-out rewrite");
    assert_eq!(worker.step_rewrite(), JobStep::Worked);
    assert!(worker.rewrite.as_ref().is_some_and(|held| !held.is_claimed() && held.region_bytes().0 > 0));
    let (parked, is_parked) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let released = Mutex::new(released);
    let is_armed = AtomicBool::new(true);
    io.on_every_read(Some(Arc::new(move || {
        if is_armed.swap(false, Ordering::AcqRel) {
            let _sent = parked.send(());
            let _woken = hold(&released).recv();
        }
    })));
    let handed = thread::scope(|scope| {
        let stepping = scope.spawn(|| worker.step_rewrite());
        is_parked.recv_timeout(PARK_WAIT).expect("the step never read");
        let (done, is_done) = mpsc::channel();
        let job = &mut job;
        scope.spawn(move || {
            let started = Instant::now();
            let has_started = job.hand_out();
            let _sent = done.send((has_started, started.elapsed()));
        });
        let handed = is_done.recv_timeout(PARK_WAIT);
        release.send(()).expect("release");
        assert_eq!(stepping.join().expect("step"), JobStep::Worked);
        handed.unwrap_or_else(|_late| is_done.recv().expect("hand-out"))
    });
    io.on_every_read(None);
    assert!(!handed.0, "a second rewrite started on a busy seat");
    assert!(handed.1 < HAND_OUT_LINE, "the hand-out took {:?} beside a parked step", handed.1);
    assert!(worker.rewrite.as_ref().is_some_and(RewriteJob::is_running), "the rewrite ended in one step");
    assert!(seat.job.try_lock().is_ok_and(|held| held.is_none()), "the seat kept its rewrite through a step");
    assert!(seat.is_busy() && seat.heap_bytes() > 0);
    assert!(!job.hand_out(), "a second rewrite started on a busy seat");
    while worker.rewrite.is_some() {
        assert_eq!(worker.step_rewrite(), JobStep::Worked);
    }
    assert!(!seat.is_busy(), "the rewrite never went back to its seat");
    assert_eq!(worker.step_rewrite(), JobStep::Idle);
    for number in 0..SEAT_KEYS {
        let value: &[u8] = if number < SEAT_KEYS * 3 / 4 { b"second" } else { b"first" };
        assert_eq!(read(&store, &key(number)), Some(value.to_vec()));
    }
    store.close().expect("close");
}

// a value under the least its column declares lands as any other and counts in the column's reading, from a put, an owned
// put and a batch row alike
#[test]
fn value_under_the_least_lands_and_counts() {
    let io = SimulatedIo::new(Seed(84));
    let mut config = config();
    config.columns[0].least_value = Some(ByteCount(LEAST_VALUE));
    let store = ReelStore::open(io.clone(), &config).expect("open");
    let short = vec![1; LEAST_VALUE as usize - 1];
    let whole = vec![2; LEAST_VALUE as usize];
    store.put(COLUMN, b"put", &short).expect("put");
    store.put_owned(COLUMN, b"owned", short.clone()).expect("put");
    let mut batch = WriteBatch::new();
    batch.put(COLUMN, b"batch", &short);
    batch.put(COLUMN, b"whole", &whole);
    store.write_batch(&batch).expect("batch");

    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    let reading = report.least_value_columns.iter().find(|reading| reading.column == COLUMN).expect("a reading");
    assert_eq!(reading.least_value, ByteCount(LEAST_VALUE));
    assert_eq!(reading.values_under_least, 3, "a value under the least went uncounted, or one at it counted");
    for key in [&b"put"[..], b"owned", b"batch"] {
        assert_eq!(read(&store, key), Some(short.clone()));
    }
    assert_eq!(read(&store, b"whole"), Some(whole));
    store.close().expect("close");
}

// a record rewrite copies its stripe a step at a time, so no unit copies more records than its bound, however many the
// stripe holds
#[test]
fn stripe_copies_in_steps() {
    let io = SimulatedIo::new(Seed(88));
    let store = open(&io);
    hold_compaction(&store, true);
    let key = |number: u64| format!("k-{number:05}").into_bytes();
    for number in 0..STEPPED_COPY_KEYS {
        store.put(COLUMN, &key(number), b"first").expect("put");
    }
    let older = seal(&store, &io);
    for number in 0..STEPPED_COPY_KEYS * 3 / 4 {
        store.put(COLUMN, &key(number), b"second").expect("put");
    }
    seal(&store, &io);
    hold_compaction(&store, false);
    store.state.ask_compaction();
    settle(&io);
    assert!(!store.state.volumes[0].table.holds(older), "the mostly dead segment never rewrote");
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    let copy = report.longest_units[UnitKind::StripeCopy.index()];
    assert!(copy.work > 0, "no stripe copied");
    assert!(copy.work <= unit_bound(UnitKind::StripeCopy).expect("a bound"), "a stripe copy took {} bytes", copy.work);
    assert_eq!(read(&store, &key(STEPPED_COPY_KEYS - 1)), Some(b"first".to_vec()));
    store.close().expect("close");
}

// a merge cut into several outputs places each output's records in offset order in its own file, and every key reads back
#[test]
fn cut_merge_copies_in_offset_order() {
    let io = SimulatedIo::new(Seed(74));
    let store = open(&io);
    hold_compaction(&store, true);
    set_of_inputs(&store, &io);
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    job.rewrite.cut_floor = 0;
    assert!(job.rewrite.start());
    while job.rewrite.is_running() {
        job.rewrite.step();
    }
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    let volume = &report.volumes[0];
    assert_eq!((volume.offset_order_copies, volume.key_order_copies), (1, 0));
    assert!(volume.span_reads + volume.lone_copies > 0, "no record was placed");
    check_set(&store);
    store.close().expect("close");
}

// a host with no room past its reserve refuses the offset-order lists, and the floor takes the same pick in key order
#[test]
fn short_host_copies_in_key_order() {
    let io = SimulatedIo::new(Seed(75));
    io.set_host_memory(HostMemory { total: ByteCount(1 << 30), available: ByteCount(0), group_room: None, group_limit: None });
    let store = open(&io);
    hold_compaction(&store, true);
    set_of_inputs(&store, &io);
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(job.rewrite.start());
    while job.rewrite.is_running() {
        job.rewrite.step();
    }
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    let volume = &report.volumes[0];
    assert_eq!((volume.offset_order_copies, volume.key_order_copies), (0, 1));
    assert!(report.memory.floor_admits >= 1, "the floor took no start");
    check_set(&store);
    store.close().expect("close");
}

// a row the walk finds dead in an input a rewrite holds lands once, on the output's copy of it, at the install
#[test]
fn held_row_lands_on_its_copy() {
    let io = SimulatedIo::new(Seed(70));
    let store = open(&io);
    hold_compaction(&store, true);
    let (older, newer) = half_dead(&store, &io);
    let state = &store.state;
    let volume = &state.volumes[0];
    let mut job = CompactJob::new(Arc::clone(state), 0);
    assert!(start_merge(&mut job));
    assert_eq!(job.rewrite.input_numbers(), [older]);
    while job.rewrite.phase != RewritePhase::Draining {
        job.rewrite.step();
    }

    // the merge kept d, and a newer d makes the walk charge the held one
    store.put(COLUMN, KEYS[3], b"third").expect("put");
    let charged = seal(&store, &io);
    assert_eq!(state.pending_walks(0), 0);
    assert_eq!(hold(&volume.lanes[PLANNER_LANE].held).marked, 1);
    while job.rewrite.is_running() {
        job.rewrite.step();
    }
    let present: Vec<SegmentNumber> = hold(&volume.summary).orders().present().to_vec();
    let outputs: Vec<SegmentNumber> = present.into_iter().filter(|segment| *segment != newer && *segment != charged).collect();
    assert_eq!(outputs.len(), 1, "one output holds d");
    assert_eq!(dead_rows(&store, outputs[0]), 1);
    assert_eq!(volume.walk_claim_waits.load(Ordering::Acquire), 0);
    assert!(!volume.lanes[PLANNER_LANE].is_holding.load(Ordering::Acquire));
    assert_eq!(read(&store, KEYS[3]), Some(b"third".to_vec()));
    store.close().expect("close");
}

// a held row the merge dropped lands nothing, since no output holds it
#[test]
fn dropped_held_row_lands_nothing() {
    let io = SimulatedIo::new(Seed(71));
    let store = open(&io);
    hold_compaction(&store, true);
    let (older, newer) = half_dead(&store, &io);
    let state = &store.state;
    let volume = &state.volumes[0];
    let mut job = CompactJob::new(Arc::clone(state), 0);
    assert!(start_merge(&mut job));
    assert_eq!(job.rewrite.input_numbers(), [older]);

    // before the merge decides d, a newer d lands, so the walk holds the older d and the merge drops it
    store.put(COLUMN, KEYS[3], b"third").expect("put");
    let charged = seal(&store, &io);
    assert_eq!(hold(&volume.lanes[PLANNER_LANE].held).marked, 1);
    while job.rewrite.is_running() {
        job.rewrite.step();
    }
    let present: Vec<SegmentNumber> = hold(&volume.summary).orders().present().to_vec();
    assert_eq!(present, [newer, charged], "the merge kept a row");
    assert_eq!(dead_rows(&store, newer) + dead_rows(&store, charged), 0);
    assert_eq!(read(&store, KEYS[3]), Some(b"third".to_vec()));
    store.close().expect("close");
}

// the default store opens with the workers the run asks for, so the workers pass runs with a worker beside each planner
#[test]
fn default_store_has_the_asked_workers() {
    let io = SimulatedIo::new(Seed(138));
    let store = open(&io);
    assert_eq!(store.state.volumes[0].lanes.len(), shape::workers() + 1, "the store opened without the asked workers");
    store.close().expect("close");
}

// one key written twice, sealed after each write: the first segment is wholly dead, counted and replaced by the second
fn one_dead_segment(store: &Store, io: &SimulatedIo) -> (SegmentNumber, SegmentNumber) {
    store.put(COLUMN, b"w", b"first").expect("put");
    let dead = seal(store, io);
    store.put(COLUMN, b"w", b"second").expect("put");
    (dead, seal(store, io))
}

// many keys written twice, sealed after each pass: the first segment is wholly dead over several row blocks
fn many_block_dead_segment(store: &Store, io: &SimulatedIo) -> SegmentNumber {
    for number in 0..TWO_BLOCK_ROWS {
        store.put(COLUMN, format!("dead-{number:06}").as_bytes(), b"first").expect("put");
    }
    let dead = seal(store, io);
    for number in 0..TWO_BLOCK_ROWS {
        store.put(COLUMN, format!("dead-{number:06}").as_bytes(), b"second").expect("put");
    }
    seal(store, io);
    assert!(hold(&store.state.volumes[0].summary).get(dead).expect("a row").facts.block_count > 1);
    dead
}

// the unlink takes a wholly dead segment only when nothing stands in its way, and each thing that does leaves it to a rewrite
#[test]
fn unlink_gate_leaves_held_segments_to_a_rewrite() {
    let io = SimulatedIo::new(Seed(134));
    let store = open(&io);
    hold_compaction(&store, true);
    let state = &store.state;
    let volume = &state.volumes[0];
    store.put(COLUMN, b"w", b"first").expect("put");
    let cue = store.cue().expect("cue");
    let dead = seal(&store, &io);
    store.put(COLUMN, b"w", b"second").expect("put");
    let live = seal(&store, &io);
    let can = |segment| can_unlink(state, 0, volume, segment);
    assert!(!can(dead), "a cue below the replacement left the segment to the unlink");
    drop(cue);
    assert!(can(dead));
    assert!(!can(live), "a segment with a live row went to the unlink");
    volume.is_applying.store(true, Ordering::Release);
    assert!(!can(dead), "a segment under a cover being applied went to the unlink");
    volume.is_applying.store(false, Ordering::Release);
    hold(&volume.lanes[PLANNER_LANE].inputs)[0] = dead;
    assert!(!can(dead), "a segment a rewrite holds went to the unlink");
    hold(&volume.lanes[PLANNER_LANE].inputs)[0] = SegmentNumber(0);
    assert!(can(dead));
    volume.table.flag_unproven(dead);
    assert!(!can(dead), "a segment a proof found live went to the unlink");
    store.close().expect("close");
}

// a segment a rewrite holds starts no proof, which the rewrite would throw away, and the unlink takes it once the rewrite lets go
#[test]
fn held_segment_starts_no_proof() {
    let io = SimulatedIo::new(Seed(135));
    let store = open(&io);
    hold_compaction(&store, true);
    let volume = &store.state.volumes[0];
    let (dead, _live) = one_dead_segment(&store, &io);
    io.hold_jobs("reel-planner", true);
    hold_compaction(&store, false);
    hold(&volume.lanes[PLANNER_LANE].inputs)[0] = dead;
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    volume.wants_compaction.store(true, Ordering::Release);
    for _step in 0..MOST_FORCED_STEPS {
        job.step_unit(io.clock_reading());
        assert!(!job.is_confirming(), "a proof began on a segment a rewrite holds");
    }
    assert!(volume.table.holds(dead));
    hold(&volume.lanes[PLANNER_LANE].inputs)[0] = SegmentNumber(0);
    volume.wants_compaction.store(true, Ordering::Release);
    for _step in 0..MOST_LOOPS {
        if !volume.table.holds(dead) {
            break;
        }
        job.step_unit(io.clock_reading());
    }
    assert!(!volume.table.holds(dead), "the unlink never took the segment once the rewrite let go");
    shape::finish_rewrites(&mut job);
    io.hold_jobs("reel-planner", false);
    store.close().expect("close");
}

// a proof stops at its next step once a rewrite takes its segment, so it proves no block the rewrite will remove anyway
#[test]
fn proof_stops_once_a_rewrite_takes_its_segment() {
    let io = SimulatedIo::new(Seed(136));
    let store = open(&io);
    hold_compaction(&store, true);
    let volume = &store.state.volumes[0];
    many_block_dead_segment(&store, &io);
    io.hold_jobs("reel-planner", true);
    hold_compaction(&store, false);
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    volume.wants_compaction.store(true, Ordering::Release);
    for _step in 0..MOST_LOOPS {
        if job.is_confirming() {
            break;
        }
        job.step_unit(io.clock_reading());
    }
    let dead = job.confirming().expect("no proof began");
    hold(&volume.lanes[PLANNER_LANE].inputs)[0] = dead;
    job.step_unit(io.clock_reading());
    assert_ne!(job.confirming(), Some(dead), "the proof went on under a rewrite");
    assert!(volume.table.holds(dead));
    hold(&volume.lanes[PLANNER_LANE].inputs)[0] = SegmentNumber(0);
    shape::finish_rewrites(&mut job);
    io.hold_jobs("reel-planner", false);
    store.close().expect("close");
}

// with a worker, a seal that lands between two proof steps hands the seat a pick, and the pick leaves the segment under proof to
// the unlink: the segment goes once, by the unlink, with each block proven once and no record rewrite
#[test]
fn seal_during_a_proof_leaves_the_segment_to_the_unlink() {
    let io = SimulatedIo::new(Seed(137));
    let mut config = config();
    config.threads.maintenance_workers = Some(1);
    config.memory_budget = WORKER_BUDGET;
    let store = ReelStore::open(io.clone(), &config).expect("open");
    hold_compaction(&store, true);
    let volume = &store.state.volumes[0];
    let dead = many_block_dead_segment(&store, &io);
    let rows = rows_of(&hold(&volume.summary).get(dead).expect("a row"));
    let proven = || store.state.units[UnitKind::UnlinkCheck.index()].reading().work_sum;
    io.hold_jobs("reel-planner", true);
    hold_compaction(&store, false);
    let records = || volume.picks[PickOutcome::Record.index()].load(Ordering::Acquire);
    let (records_before, unlinked_before, proven_before) = (records(), volume.segments_unlinked.load(Ordering::Acquire), proven());
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    volume.wants_compaction.store(true, Ordering::Release);
    let mut is_sealed = false;
    for _step in 0..MOST_LOOPS {
        if !volume.table.holds(dead) {
            break;
        }
        let is_proving = job.is_confirming();
        job.step_unit(io.clock_reading());
        // a seal lands after the proof's first block, and the next step hands out what it changed
        if is_proving && !is_sealed {
            store.put(COLUMN, b"mid-proof", b"value").expect("put");
            store.seal_now();
            settle(&io);
            is_sealed = true;
        }
    }
    assert!(is_sealed, "no proof began");
    assert!(!volume.table.holds(dead), "the segment never went");
    assert_eq!(volume.segments_unlinked.load(Ordering::Acquire), unlinked_before + 1);
    assert_eq!(records(), records_before, "a record rewrite took the segment under proof");
    assert_eq!(proven() - proven_before, rows, "the proof asked other than each row once");
    shape::finish_rewrites(&mut job);
    io.hold_jobs("reel-planner", false);
    store.close().expect("close");
}

/// Wholly dead segments ahead of a nearly dead one in the top share bucket, one more than a bucket's looks
const DEAD_AHEAD: u64 = reel::engine::rewrite_job::BUCKET_LOOKS as u64 + 1;
/// Rows of the nearly dead segment, all but one written again, so its share falls in the top bucket
const NEARLY_DEAD_ROWS: u64 = 200;

// segments the unlink can take count for none of a pick's looks, so a queue of them at the head of the top bucket hides no
// nearly dead segment behind it
#[test]
fn record_pick_passes_unlink_candidates() {
    let io = SimulatedIo::new(Seed(139));
    let store = open(&io);
    hold_compaction(&store, true);
    for number in 0..DEAD_AHEAD {
        store.put(COLUMN, format!("dead-{number:03}").as_bytes(), b"first").expect("put");
        seal(&store, &io);
    }
    for number in 0..DEAD_AHEAD {
        store.put(COLUMN, format!("dead-{number:03}").as_bytes(), b"second").expect("put");
    }
    seal(&store, &io);
    for number in 0..NEARLY_DEAD_ROWS {
        store.put(COLUMN, format!("near-{number:03}").as_bytes(), b"first").expect("put");
    }
    let nearly_dead = seal(&store, &io);
    for number in 1..NEARLY_DEAD_ROWS {
        store.put(COLUMN, format!("near-{number:03}").as_bytes(), b"second").expect("put");
    }
    seal(&store, &io);
    let volume = &store.state.volumes[0];
    let top = hold(&volume.summary).orders().share_list(reel::segment::summary_orders::SHARE_BUCKETS - 1).collect::<Vec<_>>();
    assert!(top.len() as u64 > DEAD_AHEAD && top.last() == Some(&nearly_dead), "the shape is off: {top:?}");

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job), "no rewrite started");
    assert_eq!(job.rewrite.input_numbers(), [nearly_dead], "the pick missed the nearly dead segment");
    shape::abandon_rewrites(&mut job);
    store.close().expect("close");
}

// a rewrite starts once the steps an unlink takes ahead of it are done, and the wholly dead segment goes first
#[test]
fn ask_outlives_an_unlink() {
    let io = SimulatedIo::new(Seed(79));
    let store = open(&io);
    hold_compaction(&store, true);
    let state = &store.state;
    let volume = &state.volumes[0];
    let value = vec![7; IDLE_VALUE_BYTES];
    for number in 0..IDLE_KEYS {
        store.put(COLUMN, format!("live-{number:04}").as_bytes(), &value).expect("put");
    }
    seal(&store, &io);
    half_dead(&store, &io);
    store.put(COLUMN, b"w", b"first").expect("put");
    let gone = seal(&store, &io);
    store.put(COLUMN, b"w", b"second").expect("put");
    seal(&store, &io);
    io.hold_jobs("reel-planner", true);
    hold_compaction(&store, false);
    // no overlap stands, so nothing past the ask brings a rewrite
    volume.is_over_bound.store(false, Ordering::Release);
    assert!(!volume.is_hot(io.clock_reading()), "the volume read hot");

    let mut job = CompactJob::new(Arc::clone(state), 0);
    volume.wants_compaction.store(true, Ordering::Release);
    let mut is_started = false;
    for _step in 0..MOST_LOOPS {
        job.step_unit(io.clock_reading());
        // with workers a seat may start its rewrite before the unlink, but never on the wholly dead segment
        assert!(!volume.is_taken(gone), "a rewrite took the wholly dead segment");
        is_started |= shape::is_rewriting(&job);
        if is_started && !volume.table.holds(gone) {
            break;
        }
    }
    assert!(!volume.table.holds(gone), "the wholly dead segment never went");
    assert!(is_started, "the ask went with the unlink");
    shape::finish_rewrites(&mut job);
    io.hold_jobs("reel-planner", false);
    store.close().expect("close");
}

// a fill time runs from a segment's first row to its seal: a volume idle before a quick fill reads quiet a minute after its
// seal, and one sealing back to back faster than a merge takes a segment reads hot
#[test]
fn fill_time_runs_from_the_first_row() {
    let io = SimulatedIo::new(Seed(80));
    let store = open(&io);
    let volume = &store.state.volumes[0];
    let second = Nanoseconds(1_000_000_000);
    let minute = Nanoseconds(60 * second.0);
    assert!(!volume.is_hot(io.clock_reading()), "a volume with no seal read hot");
    // the first gap is the idle open, the second the hour between seals
    for _gap in 0..2 {
        io.advance_clock(Nanoseconds(60 * minute.0));
        store.put(COLUMN, b"key", b"value").expect("put");
        seal(&store, &io);
        let fill = volume.seal_interval.load(Ordering::Relaxed);
        assert!(fill > 0 && fill < minute.0, "the idle gap counted in the fill time: {fill}");
        io.advance_clock(minute);
        assert!(!volume.is_hot(io.clock_reading()), "a volume idle between seals read hot");
    }
    // the last merge took a minute a segment, so seals a second apart outrun it
    volume.merge_interval.store(minute.0, Ordering::Relaxed);
    for _seal in 0..2 {
        store.put(COLUMN, b"key", b"value").expect("put");
        io.advance_clock(second);
        store.seal_now();
        settle(&io);
    }
    assert!(volume.is_hot(io.clock_reading()), "a volume sealing back to back read quiet");
    store.close().expect("close");
}

// a trickle that fills each segment slower than a merge takes one reads quiet however steady it is, so maintenance keeps up
// with it, and no merge yet reads quiet too
#[test]
fn trickle_a_merge_outpaces_reads_quiet() {
    let io = SimulatedIo::new(Seed(85));
    let store = open(&io);
    let volume = &store.state.volumes[0];
    let second = Nanoseconds(1_000_000_000);
    let minute = Nanoseconds(60 * second.0);
    for _seal in 0..3 {
        store.put(COLUMN, b"key", b"value").expect("put");
        io.advance_clock(minute);
        store.seal_now();
        settle(&io);
        assert!(!volume.is_hot(io.clock_reading()), "a volume no merge has run on read hot");
    }
    volume.merge_interval.store(second.0, Ordering::Relaxed);
    for _seal in 0..3 {
        store.put(COLUMN, b"key", b"value").expect("put");
        io.advance_clock(minute);
        store.seal_now();
        settle(&io);
        assert!(!volume.is_hot(io.clock_reading()), "a trickle a merge outpaces read hot");
    }
    store.close().expect("close");
}

// the writer pauses once a seal lands, so the hot window closes and the rewrites hot ingest put off can start
fn cool(store: &Store, io: &SimulatedIo) {
    let until = store.state.quiet_at(io.clock_reading());
    io.advance_clock(Nanoseconds(until.0.saturating_sub(io.clock_reading().0)));
    settle(io);
}


// an overlap merge takes the deadest segments over its point first, so segments of overwritten rows merge ahead of wide ones
// that hold live rows
#[test]
fn overlap_takes_the_deadest_first() {
    let io = SimulatedIo::new(Seed(82));
    let store = open(&io);
    // the merge mechanics under test, not the size of the records, decide what merges
    store.state.volumes[0].holds.small_bound.store(true, Ordering::Release);
    hold_compaction(&store, true);
    let value = vec![7; WIDE_VALUE_BYTES];
    let mut wide = Vec::new();
    for number in 0..OVER_THE_BOUND {
        store.put(COLUMN, b"m-shared", format!("{number}").as_bytes()).expect("put");
        if number % 2 == 0 {
            store.put(COLUMN, format!("a-{number:03}").as_bytes(), &value).expect("put");
            store.put(COLUMN, format!("z-{number:03}").as_bytes(), &value).expect("put");
            wide.push(seal(&store, &io));
        } else {
            seal(&store, &io);
        }
    }

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job));
    let inputs = job.rewrite.input_numbers().to_vec();
    assert!(inputs.len() > 1 && inputs.iter().all(|input| !wide.contains(input)), "a live segment went ahead of a dead one: {inputs:?}");
    run_to_end(&mut job);
    store.close().expect("close");
}

// a quiet volume merges back to the bound before it reclaims: a record rewrite waits on every lane while an overlap merge runs or
// can start, and runs once the planner's look finds no point a merge can take
#[test]
fn records_wait_behind_overlap() {
    let io = SimulatedIo::new(Seed(83));
    let mut config = config();
    config.threads.maintenance_workers = Some(1);
    config.memory_budget = WORKER_BUDGET;
    let store = ReelStore::open(io.clone(), &config).expect("open");
    hold_compaction(&store, true);
    let state = &store.state;
    let volume = &state.volumes[0];
    pile_up(&store, &io, OVER_THE_BOUND);
    half_dead(&store, &io);
    let records = || volume.picks[PickOutcome::Record.index()].load(Ordering::Acquire);

    let mut job = CompactJob::new(Arc::clone(state), 0);
    // a worker may take an overlap merge of its own, and no record rewrite starts ahead of the merges
    let _handed = job.hand_out();
    assert_eq!(records(), 0, "a worker's record rewrite started ahead of the overlap merge");
    // the planner's own unit drives the merges, the hand-outs and the park
    io.hold_jobs("reel-planner", true);
    hold_compaction(&store, false);
    for _step in 0..PLANNER_STEPS {
        job.step_unit(io.clock_reading());
        step_seats(&job);
        if records() > 0 {
            break;
        }
    }
    assert!(records() > 0, "the record rewrite never ran once overlap was back at the bound");
    assert!(!volume.is_over_bound.load(Ordering::Acquire), "a record rewrite ran while overlap stood past the bound");
    assert!(volume.picks[PickOutcome::Overlap.index()].load(Ordering::Acquire) > 0, "no overlap merge ran ahead of it");
    while job.rewrite.is_running() {
        job.rewrite.step();
    }
    for seat in job.seats.iter() {
        if let Some(rewrite) = hold(&seat.job).as_mut() {
            while rewrite.is_running() {
                rewrite.step();
            }
        }
    }
    io.hold_jobs("reel-planner", false);
    store.close().expect("close");
}

// step each seat's rewrite once, as a worker would
fn step_seats(job: &CompactJob<SimulatedIo>) {
    for seat in job.seats.iter() {
        if let Some(rewrite) = hold(&seat.job).as_mut().filter(|rewrite| rewrite.is_running()) {
            rewrite.step();
        }
    }
}

// hot ingest puts merges off while the table has room, and once its fullest shard passes half they run under the ingest, so no
// write meets a full table
#[test]
fn hot_ingest_merges_once_room_runs_short() {
    let io = SimulatedIo::new(Seed(84));
    let store = open(&io);
    let volume = &store.state.volumes[0];
    let mut random = SeededRandom::new(Seed(84));
    let mut short_at = None;
    // the last merge took an hour a segment, so every fill outruns it
    volume.merge_interval.store(3_600_000_000_000, Ordering::Relaxed);
    for round in 0..HOT_ROUNDS {
        // random keys put every segment's range over the whole key space, and none goes dead
        for _key in 0..HOT_ROUND_KEYS {
            store.put(COLUMN, &random.next_word().to_be_bytes(), b"hot").expect("a write met a full table");
        }
        let could_pass_half = is_near_half(volume);
        io.advance_clock(HOT_FILL);
        store.seal_now();
        settle(&io);
        // the merge that room forces sets the merge time from the sim's clock, which stands still, so only the rounds before it
        // read hot
        if volume.segments_rewritten.load(Ordering::Acquire) > 0 {
            assert!(could_pass_half || short_at.is_some(), "a merge ran under hot ingest with room to spare");
            assert!(volume.picks[PickOutcome::Deferred.index()].load(Ordering::Acquire) > 0, "nothing was put off");
            store.close().expect("close");
            return;
        }
        assert!(volume.is_hot(io.clock_reading()), "round {round} read quiet");
        if let Some(at) = short_at {
            assert!(round <= at + 1, "no merge ran once the table passed half");
        } else if volume.is_short_of_room() {
            short_at = Some(round);
        }
    }
    panic!("the table never passed half");
}

// check one more segment would take the table's fullest shard past half
fn is_near_half(volume: &Volume<SimulatedIo>) -> bool {
    (0..TABLE_SHARDS).any(|shard| {
        let shard = volume.table.shards.read(shard);
        2 * (shard.segments.len() - shard.free_slots.len() + 1) > shard.segments.len()
    })
}

// random keys with deletes put every segment over the whole key space, and once more than the bound stand quiet, overlap
// merges bring them back under it with the walk running beside them
#[test]
fn random_churn_merges_back_to_the_bound() {
    let io = SimulatedIo::new(Seed(86));
    let mut config = config();
    config.threads.maintenance_workers = Some(2);
    config.memory_budget = WORKER_BUDGET;
    let store = ReelStore::open(io.clone(), &config).expect("open");
    hold_reclaim(&store);
    let volume = &store.state.volumes[0];
    let mut random = SeededRandom::new(Seed(86));
    let mut written = Vec::new();
    for _round in 0..OVER_THE_BOUND + 8 {
        for count in 0..HOT_ROUND_KEYS * 4 {
            let mut key = [0u8; 32];
            for chunk in key.chunks_mut(8) {
                chunk.copy_from_slice(&random.next_word().to_be_bytes());
            }
            store.put(COLUMN, &key, b"churn").expect("put");
            written.push(key);
            if count % 8 == 7 {
                let old = written[(random.next_word() % written.len() as u64) as usize];
                store.delete(COLUMN, &old).expect("delete");
            }
        }
        seal(&store, &io);
    }
    for _step in 0..PLANNER_STEPS {
        io.advance_clock(TICK);
        settle(&io);
    }
    let overlap = volume.picks[PickOutcome::Overlap.index()].load(Ordering::Acquire);
    let misses = volume.picks[PickOutcome::OverlapMiss.index()].load(Ordering::Acquire);
    let lone = volume.picks[PickOutcome::OverlapLone.index()].load(Ordering::Acquire);
    assert!(overlap > 0, "no overlap merge ran: misses {misses} lone {lone}");
    store.close().expect("close");
}

// segments a running rewrite holds still count toward the crowd, so overlap past the bound merges what's free and never waits
// on the held ones to fall under it
#[test]
fn held_segments_count_toward_the_crowd() {
    let io = SimulatedIo::new(Seed(87));
    let mut config = config();
    config.threads.maintenance_workers = Some(1);
    config.memory_budget = WORKER_BUDGET;
    let store = ReelStore::open(io.clone(), &config).expect("open");
    hold_compaction(&store, true);
    let volume = &store.state.volumes[0];
    pile_up(&store, &io, OVER_THE_BOUND);
    // a worker's rewrite holds enough of the pile to leave the rest at the bound
    let held: Vec<SegmentNumber> = hold(&volume.summary).rows().map(|row| row.facts.segment).take(MOST_MERGE_SEGMENTS).collect();
    hold(&volume.lanes[1].inputs)[..held.len()].copy_from_slice(&held);
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job), "no merge started with the held segments counted out of the crowd");
    assert!(job.rewrite.input_numbers().iter().all(|input| !held.contains(input)), "a held segment went into the merge");
    run_to_end(&mut job);
    *hold(&volume.lanes[1].inputs) = [SegmentNumber(0); MOST_MERGE_SEGMENTS];
    check_pile(&store, OVER_THE_BOUND);
    store.close().expect("close");
}

// a mostly dead segment is rewritten while writes still arrive, as long as merges keep up with them, so steady writes reclaim
#[test]
fn records_reclaim_under_a_trickle() {
    let io = SimulatedIo::new(Seed(88));
    let store = open(&io);
    let volume = &store.state.volumes[0];
    let records = || volume.picks[PickOutcome::Record.index()].load(Ordering::Acquire);
    for key in KEYS {
        store.put(COLUMN, key, b"first").expect("put");
    }
    io.advance_clock(TICK);
    store.seal_now();
    settle(&io);
    for key in &KEYS[..3] {
        store.put(COLUMN, key, b"second").expect("put");
    }
    io.advance_clock(TICK);
    store.seal_now();
    settle(&io);
    assert!(volume.is_ingesting(io.clock_reading()), "a seal just landed and writes read stopped");
    assert!(records() > 0, "no record rewrite ran under a trickle");
    cool(&store, &io);
    check_reads(&store);
    store.close().expect("close");
}

// once writes stop, an idle worker takes an overlap merge of its own beside the planner's, over segments no other merge holds
#[test]
fn workers_merge_back_once_writes_stop() {
    let io = SimulatedIo::new(Seed(89));
    let mut config = config();
    config.threads.maintenance_workers = Some(1);
    config.memory_budget = WORKER_BUDGET;
    let store = ReelStore::open(io.clone(), &config).expect("open");
    hold_compaction(&store, true);
    let volume = &store.state.volumes[0];
    pile_up(&store, &io, 2 * OVER_THE_BOUND);
    assert!(!volume.is_ingesting(io.clock_reading()), "writes read as still arriving");
    hold_compaction(&store, false);
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    // a look takes a few segments at a time, and this pile is first crowded past the bound's count of them
    let handed = (0..2 * OVER_THE_BOUND).any(|_look| job.hand_out());
    assert!(handed, "no worker took an overlap merge once writes stopped");
    let worker_inputs = *hold(&volume.lanes[1].inputs);
    assert!(worker_inputs.iter().filter(|input| input.0 != 0).count() > 1, "the worker's merge took fewer than two inputs");
    assert_eq!(volume.picks[PickOutcome::Overlap.index()].load(Ordering::Acquire), 1);
    store.close().expect("close");
}

// a segment sealed after its writes went quiet reads as writes stopped, so the work they held back starts at once
#[test]
fn idle_seal_reads_quiet() {
    let io = SimulatedIo::new(Seed(90));
    let store = open(&io);
    let volume = &store.state.volumes[0];
    store.put(COLUMN, b"key", b"value").expect("put");
    io.advance_clock(TICK);
    settle(&io);
    io.advance_clock(Nanoseconds(600 * 1_000_000_000));
    store.seal_now();
    settle(&io);
    assert!(volume.seal_interval.load(Ordering::Relaxed) > 0, "the seal measured no fill time");
    assert!(!volume.is_ingesting(io.clock_reading()), "a seal after ten quiet minutes read as writes arriving");
    store.close().expect("close");
}

// an overlap merge over records several segments long cuts its outputs a segment long, short of where its row blocks start, so no
// record runs into a block
#[test]
fn overlap_cut_stays_under_the_row_blocks() {
    let io = SimulatedIo::new(Seed(88));
    let store = open(&io);
    let state = &store.state;
    let segment = state.volumes[0].segment_bytes.0;
    let mut job = RewriteJob::new(Arc::clone(state), 0, PLANNER_LANE);
    job.picked_as = Some(PickOutcome::Overlap);
    for inputs in [1, 2, 3, MOST_MERGE_SEGMENTS as u64] {
        let data = inputs * segment;
        let target = MergeTarget { blocks_at: FileOffset(data.min(2 * segment)), data: ByteCount(data), length: ByteCount(data + segment), marks: MarkRange::NONE };
        let cut = job.merge_cut(target);
        assert!(cut <= segment && cut <= target.blocks_at.0, "{inputs} inputs cut at {cut} B, a segment is {segment} B");
    }
    store.close().expect("close");
}

// a walk with a floor asks only the segments with a row above it, and the newest of those still answers
#[test]
fn floor_skips_older_segments() {
    let io = SimulatedIo::new(Seed(91));
    let store = open(&io);
    hold_compaction(&store, true);
    for round in 0..4 {
        store.put(COLUMN, b"key", format!("value-{round}").as_bytes()).expect("put");
        seal(&store, &io);
    }
    let state = &store.state;
    let volume = &state.volumes[0];
    let directory = state.directories[COLUMN.0 as usize].as_ref().expect("a directory");
    let newest = state.newest_walked(volume, COLUMN, b"key", &[], None).expect("walk").expect("a row");
    let second = state.newest_walked(volume, COLUMN, b"key", &[newest.segment], None).expect("walk").expect("a row");

    let mut reader = FooterReader::new();
    let read = state.key_read_now(COLUMN, b"key").expect("read");
    let ask = WalkAsk { skipped: &[newest.segment], is_finished_only: true, above: Some(second.row.place), ..WalkAsk::EVERY };
    let found = in_place(state.walk_candidates(&InPlace, volume, directory, &read, &mut reader, ask)).expect("walk");
    assert_eq!(found, None, "nothing outside lies above the second newest row");
    assert_eq!(reader.asked, 0, "every segment left has nothing above the floor");

    let ask = WalkAsk { skipped: &[], ..ask };
    let found = in_place(state.walk_candidates(&InPlace, volume, directory, &read, &mut reader, ask)).expect("walk").expect("a row");
    assert_eq!((found.segment, reader.asked), (newest.segment, 1));
    store.close().expect("close");
}

// an offset-order merge makes the outputs its deciding pass cut one a step, so no step waits on more than one file's room
#[test]
fn cut_outputs_come_one_a_step() {
    let io = SimulatedIo::new(Seed(92));
    let store = open(&io);
    hold_compaction(&store, true);
    set_of_inputs(&store, &io);
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    job.rewrite.cut_floor = 0;
    assert!(start_merge(&mut job));
    assert_eq!(store.state.volumes[0].offset_order_copies.load(Ordering::Acquire), 1, "the inputs' lists fit, so the copy goes in offset order");

    let lane = &store.state.volumes[0].lanes[PLANNER_LANE];
    let made = || hold(&lane.partial_outputs).iter().filter(|output| output.is_some()).count();
    let mut counts = vec![made()];
    while job.rewrite.phase == RewritePhase::Deciding {
        job.rewrite.step();
        counts.push(made());
    }
    assert!(counts.windows(2).all(|pair| pair[1] <= pair[0] + 1), "a step made more than one output: {counts:?}");
    assert!(counts.last().is_some_and(|last| *last >= 2), "the pass cut no output: {counts:?}");

    run_to_end(&mut job);
    check_set(&store);
    store.close().expect("close");
}

// writes that stop close the hot window after a quiet spell, however long the last fill took
#[test]
fn silence_ends_ingest() {
    let io = SimulatedIo::new(Seed(93));
    let store = open(&io);
    let volume = &store.state.volumes[0];
    let now = io.clock_reading().0;
    volume.seal_interval.store(3_600_000_000_000, Ordering::Relaxed);
    volume.seal_started_at.store(now, Ordering::Relaxed);
    volume.last_write_at.store(now, Ordering::Relaxed);
    assert!(volume.is_ingesting(Nanoseconds(now)));
    assert!(!volume.is_ingesting(Nanoseconds(now + IDLE_SEAL_AFTER.0)), "an hour's fill held the window past the silence");
    store.close().expect("close");
}


// an overlap merge waits for disk room for its output beside the writers' reserve, and starts once the room is there
#[test]
fn merge_waits_for_disk_room() {
    let io = SimulatedIo::new(Seed(94));
    let root = PathBuf::from(ROOT);
    io.add_disk(&root, ByteCount(1 << 40));
    let store = open(&io);
    hold_compaction(&store, true);
    pile_up(&store, &io, OVER_THE_BOUND);
    let volume = &store.state.volumes[0];
    let used = (1 << 40) - io.free_space(&root).expect("free").0;
    io.add_disk(&root, ByteCount(used + volume.reserve.0 + volume.segment_bytes.0 - 1));

    let overlaps = || volume.picks[PickOutcome::Overlap.index()].load(Ordering::Acquire);
    let before = overlaps();
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    // a merge that frees space may still start, and it runs to its end first
    if start_merge(&mut job) {
        run_to_end(&mut job);
    }
    assert_eq!(overlaps(), before, "an overlap merge started with no room for its output");

    // a disk reading lasts a period, so the room shows once that period is over
    io.add_disk(&root, ByteCount(1 << 40));
    io.advance_clock(DISK_CHECK_PERIOD);
    assert!(start_merge(&mut job));
    assert!(overlaps() > before);
    run_to_end(&mut job);
    check_pile(&store, OVER_THE_BOUND);
    store.close().expect("close");
}

// a column of large records holds more segments over a key before a merge is due, so its bytes seldom move
#[test]
fn large_records_raise_the_bound() {
    let io = SimulatedIo::new(Seed(95));
    let store = open(&io);
    hold_compaction(&store, true);
    let large = vec![6; 64 * 1024];
    for number in 0..OVER_THE_BOUND {
        store.put(COLUMN, format!("a-{number:03}").as_bytes(), &large).expect("put");
        store.put(COLUMN, b"m-shared", &large).expect("put");
        store.put(COLUMN, format!("z-{number:03}").as_bytes(), &large).expect("put");
        seal(&store, &io);
    }
    let volume = &store.state.volumes[0];
    let overlaps = || volume.picks[PickOutcome::Overlap.index()].load(Ordering::Acquire);
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    let started = start_merge(&mut job);
    assert_eq!(overlaps(), 0, "a merge started on {OVER_THE_BOUND} segments of 64 KiB records");
    if started {
        run_to_end(&mut job);
    }
    store.close().expect("close");
}

// a claim sets its inputs' park slots aside, so a merge beside it starts only when the park holds both
#[test]
fn claims_share_the_park() {
    let io = SimulatedIo::new(Seed(96));
    let mut config = config();
    config.threads.maintenance_workers = Some(2);
    config.memory_budget = WORKER_BUDGET;
    let store = ReelStore::open(io.clone(), &config).expect("open");
    hold_compaction(&store, true);
    pile_up(&store, &io, 2 * OVER_THE_BOUND);
    let stand_in = stand_in_parked(&store);
    let park = hold(&store.state.volumes[0].parked).len();
    fill_park(&store, &stand_in, park - MOST_MERGE_SEGMENTS);

    // a refused pick moves the look no further, so the looks are bounded
    let start = |job: &mut RewriteJob<SimulatedIo>| {
        for _look in 0..MOST_LOOPS {
            if job.start() {
                return true;
            }
            if !job.has_looks_left() {
                return false;
            }
        }
        false
    };
    let mut first = RewriteJob::new(Arc::clone(&store.state), 0, PLANNER_LANE + 1);
    let mut second = RewriteJob::new(Arc::clone(&store.state), 0, PLANNER_LANE + 2);
    assert!(start(&mut first));
    assert!(!start(&mut second), "a second merge claimed the park slots the first set aside");
    io.kill();
}

// overwrite a few large values and add a live one each segment, with the disk unbounded and the bound at its least
fn pile_on_disk(seed: u64, has_live: bool) -> (SimulatedIo, Store) {
    let io = SimulatedIo::new(Seed(seed));
    io.add_disk(Path::new(ROOT), ByteCount(1 << 40));
    let store = open(&io);
    hold_compaction(&store, true);
    store.state.volumes[0].holds.small_bound.store(true, Ordering::Release);
    for round in 0..OVER_THE_BOUND {
        for key in 0..DEAD_PILE_KEYS {
            store.put(COLUMN, format!("d-{key}").as_bytes(), &dead_pile_value(round)).expect("put");
        }
        if has_live {
            store.put(COLUMN, format!("l-{round:03}").as_bytes(), &dead_pile_value(round)).expect("put");
        }
        seal(&store, &io);
    }
    (io, store)
}

fn cap_disk(io: &SimulatedIo, room: u64) {
    let used = (1 << 40) - io.free_space(Path::new(ROOT)).expect("free").0;
    io.add_disk(Path::new(ROOT), ByteCount(used + room));
}

// a held cue makes the merge promise all it reads, and on a disk with exactly that much room it never outgrows the promise
#[test]
fn held_cue_fits_an_exact_disk() {
    let promise_of = |is_cued: bool| {
        let (_io, store) = pile_on_disk(97, false);
        let cue = is_cued.then(|| half_cue(&store));
        let mut job = CompactJob::new(Arc::clone(&store.state), 0);
        assert!(start_merge(&mut job));
        let promised = store.state.volumes[0].outputs_promised.load(Ordering::Acquire);
        run_to_end(&mut job);
        assert_eq!(store.state.volumes[0].outputs_promised.load(Ordering::Acquire), 0, "a finished merge kept its promise");
        assert_eq!(store.state.volumes[0].promises_outgrown.load(Ordering::Acquire), 0);
        cue.into_iter().for_each(|slot| store.state.cues.release(slot));
        store.close().expect("close");
        promised
    };
    let (plain, cued) = (promise_of(false), promise_of(true));
    assert!(cued > plain, "the cue left the promise at {cued}, the plain one is {plain}");

    let (io, store) = pile_on_disk(97, false);
    let volume = &store.state.volumes[0];
    let cue = Some(half_cue(&store));
    cap_disk(&io, volume.reserve.0 + volume.segment_bytes.0 + cued);
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job));
    assert_eq!(volume.outputs_promised.load(Ordering::Acquire), cued);
    run_to_end(&mut job);
    assert_eq!(volume.promises_outgrown.load(Ordering::Acquire), 0);
    assert_eq!(volume.given_up_by_kind.iter().map(|kind| kind.load(Ordering::Acquire)).sum::<u64>(), 0);
    cue.into_iter().for_each(|slot| store.state.cues.release(slot));
    check_dead_pile(&store, false);
    store.close().expect("close");
}

// counts that charged dead rows twice promise too little: with room the merge takes more and lands, without it the merge gives way
#[test]
fn low_estimate_takes_more_room() {
    let (_io, store) = pile_on_disk(98, true);
    let volume = &store.state.volumes[0];
    volume.holds.low_estimate.store(true, Ordering::Release);
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job));
    run_to_end(&mut job);
    assert!(volume.promises_outgrown.load(Ordering::Acquire) > 0);
    assert_eq!(volume.outputs_promised.load(Ordering::Acquire), 0);
    check_dead_pile(&store, true);
    store.close().expect("close");

    let (io, store) = pile_on_disk(98, true);
    let volume = &store.state.volumes[0];
    volume.holds.low_estimate.store(true, Ordering::Release);
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job));
    let promised = volume.outputs_promised.load(Ordering::Acquire);
    cap_disk(&io, volume.reserve.0 + volume.segment_bytes.0 + promised);
    run_to_end(&mut job);
    assert_eq!(volume.promises_outgrown.load(Ordering::Acquire), 0);
    assert!(volume.given_up_by_kind[1].load(Ordering::Acquire) > 0, "the merge wrote on past its room");
    assert_eq!(volume.outputs_promised.load(Ordering::Acquire), 0);
    check_dead_pile(&store, true);
    store.close().expect("close");
}

// a cue at half the read point sees rows the second half of the pile overwrote
fn half_cue(store: &Store) -> usize {
    let state = &store.state;
    state.cues.take(0, WritePlace(state.read_point(&state.volumes[0]).0 / 2)).expect("a slot")
}

const DEAD_PILE_KEYS: u64 = 8;

fn dead_pile_value(round: u64) -> Vec<u8> {
    vec![round as u8; 4096]
}

fn check_dead_pile(store: &Store, has_live: bool) {
    for round in (0..OVER_THE_BOUND).filter(|_round| has_live) {
        assert_eq!(read(store, format!("l-{round:03}").as_bytes()), Some(dead_pile_value(round)));
    }
    for key in 0..DEAD_PILE_KEYS {
        assert_eq!(read(store, format!("d-{key}").as_bytes()), Some(dead_pile_value(OVER_THE_BOUND - 1)));
    }
}

// every byte a merge writes past its records lands in the footer count of its kind
#[test]
fn footers_by_kind_match_the_total() {
    let (_io, store) = pile_on_disk(99, true);
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job));
    run_to_end(&mut job);
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    let footers = report.volumes[0].rewrite_bytes.footers_by_share;
    assert!(footers.iter().any(|bytes| bytes.0 > 0));
    assert_eq!(footers.iter().map(|bytes| bytes.0).sum::<u64>(), report.written[WriteSource::MergeFooters.index()].0);
    store.close().expect("close");
}

// a count from before a reopen may rest on a segment that walks again, so while any segment is unwalked a pick promises its
// inputs' whole length, and the merge never writes past it
#[test]
fn reopened_counts_promise_whole_inputs() {
    let io = SimulatedIo::new(Seed(99));
    let store = open(&io);
    hold_compaction(&store, true);
    let (_older, _newer) = half_dead(&store, &io);
    settle(&io);
    store.close().expect("close");

    let store = open(&io);
    let volume = &store.state.volumes[0];
    hold_compaction(&store, true);
    volume.holds.walk.store(true, Ordering::Release);
    store.put(COLUMN, b"z", b"after the reopen").expect("put");
    seal(&store, &io);
    hold_compaction(&store, true);
    // writes stop, so record rewrites may run
    io.advance_clock(Nanoseconds(IDLE_SEAL_AFTER.0 * 4));
    assert!(hold(&volume.summary).rows().any(|row| !row.facts.is_walked()), "no segment is unwalked");

    hold_compaction(&store, false);
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job), "no merge started");
    let whole: u64 = job.rewrite.input_numbers().iter().filter_map(|number| hold(&volume.summary).get(*number).map(|row| row.facts.file_length.0)).sum();
    assert_eq!(volume.outputs_promised.load(Ordering::Acquire), whole, "the pick trusted a count from before the reopen");
    run_to_end(&mut job);
    assert_eq!(volume.promises_outgrown.load(Ordering::Acquire), 0);
    volume.holds.walk.store(false, Ordering::Release);
    store.close().expect("close");
}

const BIT_CRASH_POINTS: u64 = 60;
const BIT_CRASH_STRIDE: u64 = 3;

// one merge rewrites the segment holding newer rows and parks it, and a second drops the older rows by their bits while it sits
// in the park: a crash at any step of either install leaves every key at its newest version
#[test]
fn bit_drops_beside_a_parked_replacement() {
    let mut decided = 0;
    let mut finished = 0;
    for crash_at in (0..BIT_CRASH_POINTS).map(|point| point * BIT_CRASH_STRIDE) {
        for is_power_cut in [false, true] {
            let io = SimulatedIo::new(Seed(101));
            let mut config = config();
            config.threads.maintenance_workers = Some(2);
            config.memory_budget = WORKER_BUDGET;
            let store = ReelStore::open(io.clone(), &config).expect("open");
            hold_compaction(&store, true);
            for key in KEYS {
                store.put(COLUMN, key, b"first").expect("put");
            }
            let older = seal(&store, &io);
            for key in KEYS {
                store.put(COLUMN, key, b"second").expect("put");
            }
            let newer = seal(&store, &io);
            for key in &KEYS[..2] {
                store.put(COLUMN, key, b"third").expect("put");
            }
            store.put(COLUMN, b"z", b"tail").expect("put");
            seal(&store, &io);
            store.flush().expect("flush");
            settle(&io);
            let state = &store.state;
            let volume = &state.volumes[0];
            assert!(volume.dead_bits.exact(older).is_some(), "the older segment has no exact bits");

            let mut rewrite_newer = RewriteJob::new(Arc::clone(state), 0, PLANNER_LANE + 1);
            rewrite_newer.forced = Some(newer);
            let mut drop_older = RewriteJob::new(Arc::clone(state), 0, PLANNER_LANE + 2);
            drop_older.forced = Some(older);
            io.arm_crash(OperationCount(crash_at));
            run_alone(&mut rewrite_newer, &io);
            let was_parked = volume.segments_parked.load(Ordering::Acquire) > 0;
            let before = volume.dead_bits.decided[DECIDED_SET].load(Ordering::Acquire);
            run_alone(&mut drop_older, &io);
            decided += u64::from(was_parked) * (volume.dead_bits.decided[DECIDED_SET].load(Ordering::Acquire) - before);
            finished += u64::from(!io.is_crashed());
            if is_power_cut {
                io.power_cut();
            } else {
                io.kill();
            }
            drop(store);

            let store = ReelStore::open(io.clone(), &config).expect("reopen");
            for (key, value) in KEYS.iter().zip([&b"third"[..], b"third", b"second", b"second"]) {
                assert_eq!(read(&store, key), Some(value.to_vec()), "crash at {crash_at}, power cut {is_power_cut}");
            }
            assert_eq!(read(&store, b"z"), Some(b"tail".to_vec()));
            store.close().expect("close");
        }
    }
    assert!(decided > 0, "no bit decided a drop while a segment was parked");
    assert!(finished > 0, "every crash point came before both installs ended");
}

// start a forced merge and step it to its end, or until the crash comes
fn run_alone(job: &mut RewriteJob<SimulatedIo>, io: &SimulatedIo) {
    while !io.is_crashed() && !job.is_running() {
        if job.start() {
            break;
        }
        if !job.has_looks_left() {
            return;
        }
    }
    while !io.is_crashed() && (job.is_claimed() || job.is_running()) {
        job.step();
    }
}

// a crowd whose filters aren't all in the arena keeps the least bound however large its records, since each get probes every
// segment over a key and a probe past the arena costs a cache lock or a read
#[test]
fn filters_out_keep_the_least_bound() {
    let io = SimulatedIo::new(Seed(95));
    let store = open(&io);
    hold_compaction(&store, true);
    let volume = &store.state.volumes[0];
    volume.holds.filters_out.store(true, Ordering::Release);
    let large = vec![6; 64 * 1024];
    for number in 0..OVER_THE_BOUND {
        store.put(COLUMN, format!("a-{number:03}").as_bytes(), &large).expect("put");
        store.put(COLUMN, b"m-shared", &large).expect("put");
        store.put(COLUMN, format!("z-{number:03}").as_bytes(), &large).expect("put");
        seal(&store, &io);
    }
    let overlaps = || volume.picks[PickOutcome::Overlap.index()].load(Ordering::Acquire);
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(start_merge(&mut job), "no merge started");
    assert!(overlaps() > 0, "no overlap merge started on {OVER_THE_BOUND} segments with their filters out");
    run_to_end(&mut job);
    store.close().expect("close");
}

const LIGHT_KEYS: u64 = 200;
const BUSY_SPELLS: u64 = 4;
/// Two keys in five written again leave each segment about 40% dead: past the quiet bar, under the plain one
const PAST_QUIET: (u64, u64) = (2, 5);
/// Three keys in twenty leave each segment about 15% dead, under the quiet bar
const UNDER_QUIET: (u64, u64) = (3, 20);

fn light_key(number: u64) -> Vec<u8> {
    format!("light-{number:04}").into_bytes()
}

fn is_written_again(number: u64, (dead, of): (u64, u64)) -> bool {
    number % of < dead
}

// write every key and seal, then write a share of them again and seal, with the walk counting the dead rows
fn partly_dead(io: &SimulatedIo, share: (u64, u64)) {
    let store = open(io);
    hold_compaction(&store, true);
    for number in 0..LIGHT_KEYS {
        store.put(COLUMN, &light_key(number), b"first").expect("put");
    }
    seal(&store, io);
    for number in (0..LIGHT_KEYS).filter(|number| is_written_again(*number, share)) {
        store.put(COLUMN, &light_key(number), b"second").expect("put");
    }
    seal(&store, io);
    settle(io);
    assert_eq!(store.state.pending_walks(0), 0, "the walk left a segment uncounted");
    store.close().expect("close");
}

fn record_picks(store: &Store) -> u64 {
    store.state.volumes[0].picks[PickOutcome::Record.index()].load(Ordering::Acquire)
}

// step the jobs a tick at a time until the clock passes the line, calling between ticks
fn run_until(store: &Store, io: &SimulatedIo, until: Nanoseconds, mut call: impl FnMut()) {
    while io.clock_reading() < until {
        call();
        store.state.ask_compaction();
        for _step in 0..STEPS {
            if io.step_any().is_none() {
                break;
            }
        }
        io.advance_clock(TICK);
    }
}

fn spells_from_now(io: &SimulatedIo, spells: u64) -> Nanoseconds {
    Nanoseconds(io.clock_reading().0 + spells * IDLE_SEAL_AFTER.0)
}

fn check_partly(store: &Store, share: (u64, u64)) {
    for number in 0..LIGHT_KEYS {
        let value: &[u8] = if is_written_again(number, share) { b"second" } else { b"first" };
        assert_eq!(read(store, &light_key(number)).as_deref(), Some(value), "key {number}");
    }
}

// a store just opened reads busy, so a segment past the quiet bar waits for a quiet spell with no call before the bar takes it
#[test]
fn open_reads_busy_until_a_quiet_spell() {
    let io = SimulatedIo::new(Seed(120));
    partly_dead(&io, PAST_QUIET);
    let store = open(&io);
    run_until(&store, &io, Nanoseconds(io.clock_reading().0 + IDLE_SEAL_AFTER.0 / 2), || {});
    assert_eq!(record_picks(&store), 0, "a record rewrite ran at the quiet bar right after the open");
    run_until(&store, &io, spells_from_now(&io, 2), || {});
    assert!(record_picks(&store) > 0, "the quiet bar took nothing after a quiet spell");
    check_partly(&store, PAST_QUIET);
    store.close().expect("close");
}

// a scan's rows count as calls, so a long scan holds the plain bar, and the quiet bar takes the segment once it ends
#[test]
fn scan_holds_the_plain_bar() {
    let io = SimulatedIo::new(Seed(121));
    partly_dead(&io, PAST_QUIET);
    let store = open(&io);
    let mut scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
    let (mut key, mut value) = (Vec::new(), Vec::new());
    run_until(&store, &io, spells_from_now(&io, BUSY_SPELLS), || {
        if scan.next_row(&mut key, &mut value).expect("step") == ScanStep::End {
            scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
        }
    });
    assert_eq!(record_picks(&store), 0, "a record rewrite ran at the quiet bar under a scan");
    drop(scan);
    run_until(&store, &io, spells_from_now(&io, 2), || {});
    assert!(record_picks(&store) > 0, "the quiet bar took nothing once the scan ended");
    check_partly(&store, PAST_QUIET);
    store.close().expect("close");
}

// puts count as calls before their first seal, so writes after an open hold the plain bar
#[test]
fn puts_hold_the_plain_bar() {
    let io = SimulatedIo::new(Seed(122));
    partly_dead(&io, PAST_QUIET);
    let store = open(&io);
    let mut number = 0_u64;
    run_until(&store, &io, spells_from_now(&io, BUSY_SPELLS), || {
        store.put(COLUMN, format!("fresh-{number:06}").as_bytes(), b"value").expect("put");
        number += 1;
    });
    assert_eq!(record_picks(&store), 0, "a record rewrite ran at the quiet bar under puts");
    run_until(&store, &io, spells_from_now(&io, BUSY_SPELLS), || {});
    assert!(record_picks(&store) > 0, "the quiet bar took nothing once the puts stopped");
    check_partly(&store, PAST_QUIET);
    store.close().expect("close");
}

// busy readers still leave a mostly dead segment to the plain bar, so a store that is never quiet holds no more than half dead
#[test]
fn reads_leave_the_plain_bar_working() {
    let io = SimulatedIo::new(Seed(123));
    let store = open(&io);
    hold_compaction(&store, true);
    half_dead(&store, &io);
    settle(&io);
    hold_compaction(&store, false);
    run_until(&store, &io, spells_from_now(&io, BUSY_SPELLS), || {
        let _found = read(&store, KEYS[0]);
    });
    assert!(record_picks(&store) > 0, "a mostly dead segment waited out busy readers");
    check_reads(&store);
    store.close().expect("close");
}

// a quiet store leaves segments under the quiet bar as they are, and its planner goes idle with them
#[test]
fn quiet_bar_leaves_light_dead() {
    let io = SimulatedIo::new(Seed(124));
    partly_dead(&io, UNDER_QUIET);
    let store = open(&io);
    run_until(&store, &io, spells_from_now(&io, BUSY_SPELLS), || {});
    assert_eq!(record_picks(&store), 0, "a quiet store rewrote a segment under the quiet bar");
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    assert!(report.longest_walkless_steps <= MOST_WALKLESS_STEPS, "the planner looked again and again at nothing to take");
    check_partly(&store, UNDER_QUIET);
    store.close().expect("close");
}

// a quiet store short of disk room takes any segment that wins a byte back
#[test]
fn room_short_takes_light_dead() {
    let io = SimulatedIo::new(Seed(125));
    let root = PathBuf::from(ROOT);
    io.add_disk(&root, ByteCount(1 << 40));
    partly_dead(&io, UNDER_QUIET);
    let store = open(&io);
    let volume = &store.state.volumes[0];
    let used = (1 << 40) - io.free_space(&root).expect("free").0;
    io.add_disk(&root, ByteCount(used + volume.reserve.0 + volume.segment_bytes.0 - 1));
    run_until(&store, &io, spells_from_now(&io, BUSY_SPELLS), || {});
    assert!(record_picks(&store) > 0, "a quiet store short of room left dead bytes it could take");
    check_partly(&store, UNDER_QUIET);
    store.close().expect("close");
}

const BUSY_LANE_WORKERS: usize = 8;

// callers reading keep overlap merges to the busy lane, one at a time under the cap, and every idle worker may take one once a
// quiet spell passes
#[test]
fn busy_callers_merge_on_one_lane() {
    let io = SimulatedIo::new(Seed(126));
    let mut config = config();
    config.threads.maintenance_workers = Some(BUSY_LANE_WORKERS);
    config.memory_budget = WORKER_BUDGET;
    let store = ReelStore::open(io.clone(), &config).expect("open");
    hold_compaction(&store, true);
    let volume = &store.state.volumes[0];
    pile_up(&store, &io, 3 * OVER_THE_BOUND);
    hold_compaction(&store, false);
    let overlaps = || volume.picks[PickOutcome::Overlap.index()].load(Ordering::Acquire);
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    let _found = read(&store, b"m-shared");
    let _quiet = store.state.quiet_at(io.clock_reading());
    for _look in 0..3 * OVER_THE_BOUND {
        let _handed = job.hand_out();
    }
    assert_eq!(overlaps(), 1, "busy callers saw more than one overlap merge");
    assert_ne!(hold(&volume.lanes[1].inputs)[0], SegmentNumber(0), "the overlap merge ran off the busy lane");

    let until = store.state.quiet_at(io.clock_reading());
    io.advance_clock(Nanoseconds(until.0.saturating_sub(io.clock_reading().0)));
    for _look in 0..3 * OVER_THE_BOUND {
        let _handed = job.hand_out();
    }
    assert!(overlaps() > 1, "no other lane took an overlap merge once callers went quiet");
    store.close().expect("close");
}

// a disk short of room frees what it can while readers keep the store busy
#[test]
fn room_short_frees_under_busy_readers() {
    let io = SimulatedIo::new(Seed(127));
    let root = PathBuf::from(ROOT);
    io.add_disk(&root, ByteCount(1 << 40));
    partly_dead(&io, UNDER_QUIET);
    let store = open(&io);
    let volume = &store.state.volumes[0];
    let used = (1 << 40) - io.free_space(&root).expect("free").0;
    io.add_disk(&root, ByteCount(used + volume.reserve.0 + volume.segment_bytes.0 - 1));
    run_until(&store, &io, spells_from_now(&io, BUSY_SPELLS), || {
        let _found = read(&store, &light_key(0));
    });
    assert!(record_picks(&store) > 0, "a disk short of room kept its dead bytes while readers called");
    check_partly(&store, UNDER_QUIET);
    store.close().expect("close");
}

/// Refused picks a back-off of half a reserve period may see, a few looks and no spin
const MOST_REFUSED_IN_BACK_OFF: u64 = 16;

fn refused_picks(store: &Store) -> u64 {
    store.state.volumes[0].picks[PickOutcome::Refused.index()].load(Ordering::Acquire)
}

// step the jobs a tick at a time until the clock passes the line, with nothing asking for compaction
fn step_until(io: &SimulatedIo, until: Nanoseconds) {
    while io.clock_reading() < until {
        for _step in 0..STEPS {
            if io.step_any().is_none() {
                break;
            }
        }
        io.advance_clock(TICK);
    }
}

// a quiet pass a refusal ended tries again once the refusal's wait is over, with no seal, rewrite or unlink between
#[test]
fn refused_pass_looks_again() {
    let io = SimulatedIo::new(Seed(128));
    partly_dead(&io, PAST_QUIET);
    // with a worker the seats pick, and the planner's own job keeps no looks, as on every store a node runs
    let mut config = config();
    config.threads.maintenance_workers = Some(1);
    config.memory_budget = WORKER_BUDGET;
    let store = ReelStore::open(io.clone(), &config).expect("open");
    let volume = &store.state.volumes[0];
    let until = store.state.quiet_at(io.clock_reading());
    io.advance_clock(Nanoseconds(until.0.saturating_sub(io.clock_reading().0)));
    // a merge just gave up for room, so picks back off for a reserve period
    volume.room_refused_at.store(io.clock_reading().0, Ordering::Release);
    volume.is_room_refused.store(true, Ordering::Release);
    store.state.ask_compaction();
    step_until(&io, Nanoseconds(io.clock_reading().0 + RESERVE_PERIOD.0 / 2));
    assert_eq!(record_picks(&store), 0, "a pick ran while backing off for room");
    assert!(refused_picks(&store) > 0, "no pick was refused while backing off");
    assert!(refused_picks(&store) < MOST_REFUSED_IN_BACK_OFF, "the planner spun on refused picks: {}", refused_picks(&store));
    step_until(&io, Nanoseconds(io.clock_reading().0 + 2 * RESERVE_PERIOD.0));
    assert!(record_picks(&store) > 0, "the pass never looked again once the wait was over");
    check_partly(&store, PAST_QUIET);
    store.close().expect("close");
}

// an asked pass on a quiet store under the bound, with nothing dead, waits out a refusal for room and then ends once room comes
// back, with no seal to wake it and no spin while it waits
#[test]
fn refused_ask_ends_once_room_returns() {
    let io = SimulatedIo::new(Seed(131));
    let mut config = config();
    config.threads.maintenance_workers = Some(1);
    config.memory_budget = WORKER_BUDGET;
    let store = ReelStore::open(io.clone(), &config).expect("open");
    let volume = &store.state.volumes[0];
    hold_compaction(&store, true);
    // segments over keys no other holds, so nothing is dead and nothing overlaps, and the pass needs more than one look
    for number in 0..LONE_SEGMENTS {
        store.put(COLUMN, format!("zz-lone-{number:03}").as_bytes(), b"alone").expect("put");
        seal(&store, &io);
    }
    hold_compaction(&store, false);
    settle(&io);
    let until = store.state.quiet_at(io.clock_reading());
    io.advance_clock(Nanoseconds(until.0.saturating_sub(io.clock_reading().0)));
    let newest = newest_segment(&store);
    let refused_before = refused_picks(&store);
    volume.room_refused_at.store(io.clock_reading().0, Ordering::Release);
    volume.is_room_refused.store(true, Ordering::Release);
    store.state.ask_compaction();
    step_until(&io, Nanoseconds(io.clock_reading().0 + RESERVE_PERIOD.0 / 2));
    let refused = refused_picks(&store) - refused_before;
    assert!(refused > 0, "no pick was refused while room was short");
    assert!(refused < MOST_REFUSED_IN_BACK_OFF, "the planner spun on refused picks: {refused}");
    step_until(&io, Nanoseconds(io.clock_reading().0 + 2 * RESERVE_PERIOD.0));
    assert!(!volume.looks.has_left(), "the pass never ended once room came back");
    let refused = refused_picks(&store) - refused_before;
    assert!(refused < MOST_REFUSED_IN_BACK_OFF, "refused picks kept coming after room came back: {refused}");
    assert_eq!(newest_segment(&store), newest, "a seal ran during the wait");
    assert_eq!(record_picks(&store), 0, "a record rewrite ran with nothing dead");
    store.close().expect("close");
}

/// Three keys in five written again leave each segment past half dead, over the plain bar
const PAST_HALF: (u64, u64) = (3, 5);
/// Planner steps a forced segment may wait before its pick
const MOST_FORCED_STEPS: u64 = 4;

// seals a second apart outrun a merge a minute long, and the last write and seal were just now, so the volume reads hot for
// the next two fill times
fn make_hot(volume: &Volume<SimulatedIo>, io: &SimulatedIo) {
    let now = io.clock_reading().0;
    volume.seal_interval.store(1_000_000_000, Ordering::Relaxed);
    volume.merge_interval.store(60_000_000_000, Ordering::Relaxed);
    volume.seal_started_at.store(now, Ordering::Relaxed);
    volume.last_write_at.store(now, Ordering::Relaxed);
    assert!(volume.is_hot(io.clock_reading()), "the volume read quiet");
}

// a disk short of room reclaims under hot ingest, since waiting out the ingest would fill it with dead rows still on it
#[test]
fn short_disk_reclaims_under_hot_ingest() {
    let io = SimulatedIo::new(Seed(132));
    let root = PathBuf::from(ROOT);
    io.add_disk(&root, ByteCount(1 << 40));
    partly_dead(&io, PAST_HALF);
    let store = open(&io);
    let volume = &store.state.volumes[0];
    let used = (1 << 40) - io.free_space(&root).expect("free").0;
    io.add_disk(&root, ByteCount(used + volume.reserve.0 + volume.segment_bytes.0 - 1));
    make_hot(volume, &io);
    store.state.ask_compaction();
    step_until(&io, Nanoseconds(io.clock_reading().0 + 20 * TICK.0));
    assert!(volume.is_hot(io.clock_reading()), "the hot window closed during the test");
    assert!(record_picks(&store) > 0, "hot ingest held a short disk's dead rows back");
    check_partly(&store, PAST_HALF);
    store.close().expect("close");
}

// a pick put off by hot ingest waits for the window to close, and a seal stuck in that wait forces a segment that moves at once
#[test]
fn forced_segment_ends_a_hot_wait() {
    let io = SimulatedIo::new(Seed(133));
    let store = open(&io);
    let volume = &store.state.volumes[0];
    hold_compaction(&store, true);
    for number in 0..LONE_SEGMENTS {
        store.put(COLUMN, format!("zz-lone-{number:03}").as_bytes(), b"alone").expect("put");
        seal(&store, &io);
    }
    hold_compaction(&store, false);
    io.hold_jobs("reel-planner", true);
    let forced = || volume.picks[PickOutcome::Forced.index()].load(Ordering::Acquire);
    let deferred = || volume.picks[PickOutcome::Deferred.index()].load(Ordering::Acquire);
    make_hot(volume, &io);
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    let deferred_before = deferred();
    store.state.ask_compaction();
    for _step in 0..MOST_FORCED_STEPS {
        let _step = job.step_unit(io.clock_reading());
    }
    assert!(deferred() > deferred_before, "hot ingest put no pick off");
    assert_eq!(forced(), 0);
    volume.stuck_shard.store(shard_of(newest_segment(&store)), Ordering::Release);
    for _step in 0..MOST_FORCED_STEPS {
        let _step = job.step_unit(io.clock_reading());
        if forced() > 0 {
            break;
        }
    }
    assert!(forced() > 0, "the forced segment waited out the hot window");
    volume.stuck_shard.store(NO_SHARD, Ordering::Release);
    run_to_end(&mut job);
    io.hold_jobs("reel-planner", false);
    store.close().expect("close");
}

const LONE_SEGMENTS: u64 = 2 * reel::engine::rewrite_job::OVERLAP_LOOKS as u64;

// with workers the seats look and the planner reads the volume's pass, so one ask keeps looking past the first look's segments
// until it reaches a crowd beyond them
#[test]
fn seats_carry_one_look_pass() {
    let io = SimulatedIo::new(Seed(129));
    let mut config = config();
    config.threads.maintenance_workers = Some(1);
    config.memory_budget = WORKER_BUDGET;
    let store = ReelStore::open(io.clone(), &config).expect("open");
    hold_compaction(&store, true);
    let volume = &store.state.volumes[0];
    // segments over keys no other holds come first, so the crowd sits past what one look reaches
    for number in 0..LONE_SEGMENTS {
        store.put(COLUMN, format!("zz-lone-{number:03}").as_bytes(), b"alone").expect("put");
        seal(&store, &io);
    }
    pile_up(&store, &io, OVER_THE_BOUND);
    let overlaps = || volume.picks[PickOutcome::Overlap.index()].load(Ordering::Acquire);
    let before = overlaps();
    hold_compaction(&store, false);
    let until = store.state.quiet_at(io.clock_reading());
    io.advance_clock(Nanoseconds(until.0.saturating_sub(io.clock_reading().0)));
    // dead rows would keep the planner asking for picks on their own, so reclaim is held and only the look's own pass goes on
    hold_reclaim(&store);
    // only the look finds the crowd, with no seal's word that the volume is over the bound
    volume.is_over_bound.store(false, Ordering::Release);
    store.state.ask_compaction();
    settle(&io);
    assert!(overlaps() > before, "the pass stopped after its first look with a crowd past it");
    check_pile(&store, OVER_THE_BOUND);
    store.close().expect("close");
}

const MANY_LONE_SEGMENTS: u64 = 1_100;

// a look pass over many segments steps beside the walk, so a pass of more looks than the bound never holds a waiting walk past it
#[test]
fn long_look_pass_leaves_the_walk_its_turn() {
    let io = SimulatedIo::new(Seed(130));
    let mut config = config();
    config.threads.maintenance_workers = Some(1);
    config.memory_budget = WORKER_BUDGET;
    let store = ReelStore::open(io.clone(), &config).expect("open");
    hold_compaction(&store, true);
    let volume = &store.state.volumes[0];
    for number in 0..MANY_LONE_SEGMENTS {
        store.put(COLUMN, format!("zz-lone-{number:05}").as_bytes(), b"alone").expect("put");
        store.seal_now();
        settle(&io);
    }
    // a segment sealed with the walk held leaves it a target while the pass looks
    volume.holds.walk.store(true, Ordering::Release);
    store.put(COLUMN, b"waiting", b"value").expect("put");
    seal(&store, &io);
    assert!(store.state.pending_walks(0) > 0, "no walk waited");
    hold_reclaim(&store);
    volume.is_over_bound.store(false, Ordering::Release);
    volume.holds.walk.store(false, Ordering::Release);
    hold_compaction(&store, false);
    let until = store.state.quiet_at(io.clock_reading());
    io.advance_clock(Nanoseconds(until.0.saturating_sub(io.clock_reading().0)));
    store.state.ask_compaction();
    settle(&io);
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    assert_eq!(store.state.pending_walks(0), 0, "the walk never ended");
    assert!(!volume.looks.has_left(), "the pass never ended");
    assert!(
        report.longest_walkless_steps <= MOST_WALKLESS_STEPS,
        "the planner held the walk back {} steps, last {:?}",
        report.longest_walkless_steps,
        report.longest_walkless_kind
    );
    store.close().expect("close");
}
