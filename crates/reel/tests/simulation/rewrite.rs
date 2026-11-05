//! Rewrite: a half-dead segment becomes a new one with only what a read can reach, through any crash

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::compact_job::CompactJob;
use reel::engine::cue::{Cue, CueTable};
use reel::engine::open_segment::{OUTPUT_SUFFIX, data_path};
use reel::engine::read_path::RecordWant;
use reel::engine::reel_store::ReelStore;
use reel::engine::rewrite_job::{MERGE_BUFFER_BYTES, RewritePhase};
use reel::guard::hold;
use reel::io::Io;
use reel::record::codec::Codec;
use reel::segment::footer_reader::FooterReader;
use reel::segment::segment_tail::SegmentNumber;
use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, Nanoseconds, VolumeId, WritePlace};
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
