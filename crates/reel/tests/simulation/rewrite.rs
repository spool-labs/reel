//! Rewrite: a half-dead segment becomes a new one with only what a read can reach, through any crash

use std::io::IoSlice;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::compact_job::{CompactJob, park};
use reel::engine::cue::{Cue, CueTable};
use reel::engine::open_segment::{OUTPUT_SUFFIX, data_path};
use reel::engine::read_path::RecordWant;
use reel::engine::planner::Planner;
use reel::engine::reel_store::ReelStore;
use reel::engine::rewrite_job::{MERGE_BUFFER_BYTES, MOST_OVERLAP, MOST_PARK_WAITS, RewritePhase, is_overlap_input};
use reel::engine::volume::{MOST_PARKED, NO_SEAL};
use reel::engine::walk_job::WalkJob;
use reel::guard::hold;
use reel::io::{Io, IoFile};
use reel::io::job::Job;
use reel::record::codec::Codec;
use reel::segment::damage_list::DamagedRange;
use reel::segment::footer_reader::FooterReader;
use reel::segment::footer_writer::FooterWriter;
use reel::segment::segment_table::{RetiredSegment, shard_of};
use reel::segment::segment_tail::{BlockCount, MOST_MERGE_SEGMENTS, SegmentNumber, TailHead};
use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, FileOffset, Nanoseconds, VolumeId, WritePlace};
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_disk::OperationCount;
use reel_sim::simulated_io::SimulatedIo;

const ROOT: &str = "/disk/rewrite";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(64 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 20_000;
const CRASH_POINTS: u64 = 300;
const KEYS: [&[u8]; 4] = [b"a", b"b", b"c", b"d"];
const MOST_LOOPS: usize = 2_000;
const OVER_THE_BOUND: u64 = MOST_OVERLAP as u64 + 8;
const KEPT_LIVE_KEYS: u64 = 16;
const MANY_GRAVES: u64 = 30_000;
const TWO_BLOCK_ROWS: u64 = 12_000;
const UNWALKED_CRASH_POINTS: u64 = 40;
const UNWALKED_CRASH_STRIDE: u64 = 3;

type Store = ReelStore<SimulatedIo>;

fn open(io: &SimulatedIo) -> Store {
    let config = StoreConfig {
        memory_budget: BUDGET,
        volumes: vec![VolumeConfig {
            name: "rewrite".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Variable { most: 64 },
            codec: Codec::None,
            volume: VolumeId(0),
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

fn seal(store: &Store, io: &SimulatedIo) -> SegmentNumber {
    let number = hold(&store.state.volumes[0].claims).open[0].number;
    store.seal_now();
    settle(io);
    number
}

fn read(store: &Store, key: &[u8]) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    store.get(COLUMN, key, &mut buf).expect("get").then_some(buf)
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

fn hold_compaction(store: &Store, is_held: bool) {
    store.state.volumes[0].is_compaction_held.store(is_held, Ordering::Release);
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
    assert!(job.rewrite.start());
    let number = job.rewrite.output.as_ref().expect("an output").number;
    while job.rewrite.phase != RewritePhase::Installing {
        job.rewrite.step();
    }
    assert!(has_file(&io, number));

    let slot = state.cues.take(WritePlace(state.read_point().0 - 3)).expect("a slot");
    job.rewrite.step();
    assert!(!job.rewrite.is_running());
    assert!(!has_file(&io, number));
    assert!(volume.table.holds(older));
    assert!(!volume.is_rewriting.load(Ordering::Acquire));
    state.cues.release(slot);
    check_reads(&store);
    store.close().expect("close");
}

// a walk starts no target while a rewrite runs, says so, and goes ahead once it's done
#[test]
fn walk_stands_aside() {
    let io = SimulatedIo::new(Seed(4));
    let store = open(&io);
    hold_compaction(&store, true);
    half_dead(&store, &io);

    let state = &store.state;
    let volume = &state.volumes[0];
    let mut job = CompactJob::new(Arc::clone(state), 0);
    assert!(job.rewrite.start());
    store.put(COLUMN, b"e", b"value").expect("put");
    seal(&store, &io);
    assert_eq!(state.pending_walks(0), 1);
    assert!(volume.walks_held.load(Ordering::Acquire) > 0);

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
    assert!(job.rewrite.start());
    job.rewrite.step();
    assert!(has_partial(&io));
    let output = job.rewrite.output.clone().expect("an output");
    assert!(volume.is_rewriting.load(Ordering::Acquire));
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
    assert!(job.rewrite.start());
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
    store.put(COLUMN, b"a", b"first").expect("put");
    store.put(COLUMN, b"b", b"first").expect("put");
    store.put(COLUMN, b"c", b"live").expect("put");
    let older = seal(&store, &io);
    store.put(COLUMN, b"a", b"second").expect("put");
    store.put(COLUMN, b"b", b"second").expect("put");
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
    seal(&store, &io);

    let state = &store.state;
    let directory = state.directories[COLUMN.0 as usize].as_ref().expect("a directory");
    let mut reader = FooterReader::new();
    let mut asks = 0;
    let walked = state.walk_candidates(directory, b"a", &mut reader, |_segment, _reader, _best| {
        asks += 1;
        if asks == 1 {
            directory.replace(None, &[SegmentNumber(u64::MAX)]);
        }
        Ok(())
    });
    walked.expect("walk");
    assert_eq!(asks, 2);
    store.close().expect("close");
}

// a cue counts inside a row's window from the row's place up to, and not including, its replacement's
#[test]
fn cue_window() {
    let table = CueTable::new();
    let slot = table.take(WritePlace(10)).expect("a slot");
    assert!(table.has_cue_between(WritePlace(10), WritePlace(11)));
    assert!(!table.has_cue_between(WritePlace(11), WritePlace(20)));
    assert!(!table.has_cue_between(WritePlace(5), WritePlace(10)));
    table.release(slot);
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
    assert!(store.state.answer_latest(&latest, RecordWant::Whole, &mut buf).is_none());
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
    assert!(job.rewrite.start());
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
    let place = WritePlace(store.state.read_point().0 + 1);
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
        job.unlink(volume, older, state.read_point()).expect("unlink");
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
    assert!(job.rewrite.start());
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
    assert!(job.rewrite.start());
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

// a target in progress keeps a rewrite from starting, or its counts would land on rows the rewrite decided on
#[test]
fn walk_target_holds_rewrite() {
    let io = SimulatedIo::new(Seed(20));
    let store = open(&io);
    hold_compaction(&store, true);
    half_dead(&store, &io);
    store.state.volumes[0].has_walk_target.store(true, Ordering::Release);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(!job.rewrite.start());
    store.state.volumes[0].has_walk_target.store(false, Ordering::Release);
    assert!(job.rewrite.start());
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
    assert!(job.rewrite.start());
    while job.rewrite.phase != RewritePhase::Installing {
        job.rewrite.step();
    }
    let slot = state.cues.take(WritePlace(state.read_point().0 - 3)).expect("a slot");
    io.fail_directory_syncs(OperationCount(1));
    job.rewrite.step();
    state.cues.release(slot);

    assert_eq!(volume.stray_outputs.load(Ordering::Acquire), 1);
    assert_eq!(volume.reclaim_retries.load(Ordering::Acquire), 1);
    assert!(!job.rewrite.start());
    job.rewrite.clear_stray();
    assert_eq!(volume.stray_outputs.load(Ordering::Acquire), 0);
    assert!(job.rewrite.start());
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
    assert!(job.rewrite.start());
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
    assert!(job.rewrite.start());
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
    let slot = state.cues.take(WritePlace(state.read_point().0 - 3)).expect("a slot");
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
    assert!(job.rewrite.start());
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
    assert!(job.rewrite.start());
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
    job.rewrite.highest_replacement = WritePlace(state.read_point().0 + 1);
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
    assert!(job.rewrite.start());
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
    assert!(job.rewrite.start());
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
    job.rewrite.install_with(|| assert_eq!(read(&store, b"key"), None)).expect("install");
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
    let slot = state.cues.take(WritePlace(state.read_point().0 - 3)).expect("a slot");
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
    let slot = state.cues.take(WritePlace(state.read_point().0 - 3)).expect("a slot");
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
            && !volume.table.install(graves.file.clone(), &TailHead { segment: filler, ..head }, &[], &[])
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
    fill_park(&store, &stand_in, MOST_PARKED - 1);
    let mut job = CompactJob::new(Arc::clone(state), 0);
    assert!(!job.rewrite.start());
    assert!(!job.rewrite.is_running());
    assert!(!volume.is_rewriting.load(Ordering::Acquire));
    let first_tried = job.rewrite.looked_up_to();
    assert!(!job.rewrite.start());
    assert!(job.rewrite.looked_up_to() > first_tried);

    *hold(&volume.parked) = [const { None }; MOST_PARKED];
    assert!(job.rewrite.start());
    assert!(job.rewrite.input_numbers().len() > 1);
    io.kill();
}

// a park that stays full ends the wait, and the merge gives way with the inputs where they were
#[test]
fn park_wait_ends() {
    let io = SimulatedIo::new(Seed(35));
    let store = open(&io);
    hold_compaction(&store, true);
    half_dead(&store, &io);

    let volume = &store.state.volumes[0];
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(job.rewrite.start());
    while job.rewrite.phase != RewritePhase::Installing {
        job.rewrite.step();
    }
    let inputs = job.rewrite.input_numbers().to_vec();
    let stand_in = stand_in_parked(&store);
    fill_park(&store, &stand_in, MOST_PARKED);

    for _round in 0..MOST_PARK_WAITS {
        job.rewrite.step();
    }
    assert!(job.rewrite.is_running());
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
    job.rewrite.grave_writer = FooterWriter::new(BlockCount(2));
    assert!(job.rewrite.start());
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
    store.state.volumes[0].is_walk_held.store(is_held, Ordering::Release);
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

// a merge takes walked inputs or unwalked ones and never both, and an unwalked one leaves the walk free
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
    assert!(job.rewrite.start());
    assert!(job.rewrite.is_unwalked_merge());
    assert!(!volume.is_rewriting.load(Ordering::Acquire));
    let rows = hold(&volume.summary);
    for input in job.rewrite.input_numbers() {
        assert!(!rows.get(*input).expect("a row").facts.is_walked());
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
    assert!(job.rewrite.start());
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
    assert!(job.rewrite.start());
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
    assert!(job.rewrite.start());
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
    assert!(job.rewrite.start());
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
        last: 0,
    };
    assert!(planner.walks[0].pick_target());
    state.volumes[0].wants_compaction.store(true, Ordering::Release);
    planner.step();
    assert!(planner.compactions[0].rewrite.is_unwalked_merge());
    planner.compactions[0].rewrite.abandon();
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
    let mut asked = 0;
    let found = state.footer_readers.with(|reader| {
        state.walk_candidates(directory, b"m-shared", reader, |segment, reader, best| {
            asked += 1;
            state.ask_segment(volume, &read, segment, reader, best)
        })
    });
    assert_eq!(asked, 1);
    assert!(found.expect("a read").is_some());
    store.close().expect("close");
}

// a full unwalked output below small unwalked segments stays out of a merge, so the small ones never wait on its walk
#[test]
fn full_segment_stays_out() {
    let io = SimulatedIo::new(Seed(61));
    let store = open(&io);
    hold_compaction(&store, true);
    hold_walk(&store, true);
    let half = store.state.volumes[0].segment_bytes.0 as usize / 2;
    store.put(COLUMN, b"a", &vec![1; half + 1]).expect("put");
    store.put(COLUMN, b"z", b"high").expect("put");
    let full = seal(&store, &io);
    pile_up(&store, &io, OVER_THE_BOUND);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    assert!(job.rewrite.start());
    assert!(job.rewrite.is_unwalked_merge());
    assert_eq!(job.rewrite.input_numbers().len(), MOST_MERGE_SEGMENTS);
    assert!(!job.rewrite.input_numbers().contains(&full));
    job.rewrite.abandon();
    store.close().expect("close");
}

// with only large segments near, a small one still pairs with one, and a large one that doesn't fit is passed over
#[test]
fn large_merge_when_alone() {
    let io = SimulatedIo::new(Seed(62));
    let store = open(&io);
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
    assert!(job.rewrite.start());
    assert_eq!(job.rewrite.input_numbers().len(), 2);
    assert!(job.rewrite.input_numbers().contains(&small));
    job.rewrite.abandon();
    store.close().expect("close");
}

// a few small segments merge alone, with room left over that a large one could have taken
#[test]
fn small_ones_merge_alone() {
    let io = SimulatedIo::new(Seed(63));
    let store = open(&io);
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
    assert!(job.rewrite.start());
    let mut inputs = job.rewrite.input_numbers().to_vec();
    inputs.sort_unstable();
    assert_eq!(inputs, small);
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
    let found = state.search_footers(&state.volumes[0], &read).expect("search").expect("a row");
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
    fill_park(&store, &stand_in_parked(&store), MOST_PARKED);
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
    fill_park(&store, &stand_in_parked(&store), MOST_PARKED);
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
