//! What a store says about itself: sizes from the summaries, and jobs that read as running, idle or stalled

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::engine_sizing::{ENTRY_LINE_DIVISOR, ENTRY_MARGIN, floor_entries};
use reel::engine::job_tally::STALL_AFTER;
use reel::engine::reel_store::ReelStore;
use reel::io::Io;
use reel::record::codec::Codec;
use reel_api::column_id::ColumnId;
use reel_api::maintenance_report::{JobKind, JobState, MaintenanceReport, PickOutcome, WriteSource};
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, Nanoseconds, RootId, RowCount, SegmentCount, VolumeId};
use reel_api::usage::{ColumnUsage, UsageReport};
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_io::{PAUSE_TICK, SimulatedIo};

use crate::seed_sets::seeds;

const ROOT: &str = "/disk/reports";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(64 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 400_000;
const KEYS: u64 = 100;
const VALUE: &[u8] = &[7; 200];
const SMALL_DISK: ByteCount = ByteCount(1024 * 1024 * 1024);
const RUN_STEPS: u64 = 20_000;
const LARGE_DISK: ByteCount = ByteCount(1 << 40);
const COVER_BUDGET: ByteCount = ByteCount(128 * 1024 * 1024);
const MOST_PAST_PUTS: u64 = 10_000_000;
const BIG_DISK: ByteCount = ByteCount(8 << 30);
const REAL_RECORD: &[u8] = &[5; 256 * 1024];
const REAL_SEGMENTS: u64 = 4;

type Store = ReelStore<SimulatedIo>;

fn open(io: &SimulatedIo) -> Store {
    open_at(io, BUDGET)
}

fn open_at(io: &SimulatedIo, budget: ByteCount) -> Store {
    let config = StoreConfig {
        memory_budget: budget,
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
            least_value: None,
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

    volume.holds.walk.store(true, Ordering::Release);
    put_keys(&store);
    seal(&store, &io);
    store.maintenance_report(&mut report);
    assert_eq!(job(&report, JobKind::DeadByteWalk), JobState::Running);
    io.advance_clock(STALL_AFTER);
    store.maintenance_report(&mut report);
    assert_eq!(job(&report, JobKind::DeadByteWalk), JobState::Stalled);

    volume.holds.walk.store(false, Ordering::Release);
    volume.holds.compaction.store(false, Ordering::Release);
    store.state.ask_compaction();
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

// a disk of a known size sets the table's entries, the reports give them beside the disk, and a full table stops a seal and
// counts the stop once, in every seed of a sweep
#[test]
fn full_table_stops_a_seal() {
    for seed in seeds([5]) {
        let io = SimulatedIo::new(Seed(seed));
        io.add_disk(Path::new(ROOT), SMALL_DISK);
        let store = open(&io);
        store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
        let memory = store.memory_report();
        let mut report = MaintenanceReport::new();
        store.maintenance_report(&mut report);
        let room = report.volumes[0].entry_room;

        assert_eq!(memory.disk_bytes, SMALL_DISK);
        let segment = report.volumes[0].cover_segment_bytes;
        assert!(segment < store.state.sizing.segment_bytes, "seed {seed}: the cover assumed the byte target for rows of no declared size");
        assert_eq!(memory.disk_covered, ByteCount((room.0 / ENTRY_MARGIN * segment.0).min(SMALL_DISK.0)));
        assert!(room.0 >= floor_entries() as u64);
        let mut number = 0;
        while report.volumes[0].seal_stops == 0 && number < room.0 * 2 {
            store.put(COLUMN, format!("key-{number:05}").as_bytes(), VALUE).expect("put");
            store.seal_now();
            run(&io);
            store.maintenance_report(&mut report);
            number += 1;
        }
        assert_eq!(report.volumes[0].seal_stops, 1, "seed {seed}: the table never filled, or a stop counted twice");
        assert!(report.volumes[0].entries_used < room, "seed {seed}: a shard filled before the table");
        run(&io);
        store.maintenance_report(&mut report);
        assert_eq!(report.volumes[0].seal_stops, 1, "seed {seed}: a stuck seal counted its retries");
        let mut value = Vec::new();
        assert!(store.get(COLUMN, b"key-00000", &mut value).expect("get"), "seed {seed}: a sealed key went missing past the stop");
        store.put(COLUMN, b"past-the-stop", VALUE).expect("put");
        store.state.volumes[0].holds.compaction.store(false, Ordering::Release);
        store.close().expect("close");
    }
}

// step the jobs for a while, past a seal that keeps trying
fn run(io: &SimulatedIo) {
    for _step in 0..RUN_STEPS {
        if io.step_any().is_none() {
            io.advance_clock(TICK);
        }
    }
}

// a disk past what the capped line covers still opens, with the entries line at its cap and the cover it reaches
#[test]
fn disk_past_the_cover() {
    let io = SimulatedIo::new(Seed(6));
    io.add_disk(Path::new(ROOT), LARGE_DISK);
    let store = open_at(&io, COVER_BUDGET);
    let memory = store.memory_report();
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    let room = report.volumes[0].entry_room;

    assert_eq!(memory.disk_bytes, LARGE_DISK);
    let floor = floor_entries() as u64 * report.volumes[0].entry_bytes.0;
    assert!(memory.entry_line.0 <= floor + (COVER_BUDGET.0 - memory.least_budget.0) / ENTRY_LINE_DIVISOR, "the line passed its cap");
    assert!(memory.entry_line.0 >= room.0 * report.volumes[0].entry_bytes.0, "the entries passed their line");
    assert_eq!(memory.disk_covered, ByteCount(room.0 / ENTRY_MARGIN * report.volumes[0].cover_segment_bytes.0));
    assert!(memory.disk_covered < LARGE_DISK, "the cover reached the whole disk");
    store.put(COLUMN, b"key", VALUE).expect("put");
    store.close().expect("close");
}

// a put past the open segments' room under a seal stop is refused at once, and close still works
#[test]
fn puts_under_a_seal_stop() {
    let io = SimulatedIo::new(Seed(7));
    io.add_disk(Path::new(ROOT), SMALL_DISK);
    let store = open(&io);
    store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    let room = report.volumes[0].entry_room.0;
    let mut number = 0;
    while report.volumes[0].seal_stops == 0 && number < room * 2 {
        store.put(COLUMN, format!("key-{number:05}").as_bytes(), VALUE).expect("put");
        store.seal_now();
        run(&io);
        store.maintenance_report(&mut report);
        number += 1;
    }
    assert_eq!(report.volumes[0].seal_stops, 1, "the table never filled");

    let mut puts = 0;
    let (outcome, waited) = loop {
        let before = io.now();
        let outcome = store.put(COLUMN, format!("past-{puts:08}").as_bytes(), VALUE);
        puts += 1;
        if outcome.is_err() || puts == MOST_PAST_PUTS {
            break (outcome, io.now().0 - before.0);
        }
    };
    assert!(matches!(outcome, Err(StoreError::TableFull { volume: VolumeId(0) })), "{outcome:?} after {puts} puts");
    assert!(waited < PAUSE_TICK.0, "the refused put paused for {waited} ns");
    store.state.volumes[0].holds.compaction.store(false, Ordering::Release);
    store.close().expect("close");
}

// a record rewrite of a mostly dead segment takes more bytes off the disk than it writes and drops dead bytes, and the readings
// say what the looks for work came to and how many walk blocks landed
#[test]
fn rewrite_bytes_and_picks() {
    let io = SimulatedIo::new(Seed(8));
    let store = open(&io);
    put_keys(&store);
    seal(&store, &io);
    for number in 1..KEYS {
        store.put(COLUMN, format!("key-{number:03}").as_bytes(), VALUE).expect("put");
    }
    seal(&store, &io);
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    let volume = &report.volumes[0];
    let bytes = volume.rewrite_bytes;

    assert!(volume.walk_blocks > 0, "no walk block landed");
    assert!(volume.picks[PickOutcome::Record.index()] > 0, "no record rewrite picked: {:?}", volume.picks);
    assert!(bytes.inputs_gone > bytes.written && bytes.dead_gone > bytes.dead_kept, "{bytes:?}");
    assert_eq!(bytes.unreclaimed, 0, "{bytes:?}");
    store.close().expect("close");
}

// an undeclared column on a big disk plans its cover at the row floor, far short of the disk, while its records seal full
// segments, so the table's room holds the whole disk at their real size and no write meets TableFull
#[test]
fn undeclared_column_on_a_big_disk() {
    let io = SimulatedIo::new(Seed(9));
    io.add_disk(Path::new(ROOT), BIG_DISK);
    let store = open(&io);
    let per_segment = store.state.sizing.segment_bytes.0 / REAL_RECORD.len() as u64;
    for segment in 0..REAL_SEGMENTS {
        for number in 0..per_segment {
            let outcome = store.put(COLUMN, format!("real-{segment:03}-{number:06}").as_bytes(), REAL_RECORD);
            assert!(!matches!(outcome, Err(StoreError::TableFull { .. })), "segment {segment} met TableFull");
            outcome.expect("put");
        }
        seal(&store, &io);
    }
    let memory = store.memory_report();
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    let volume = &report.volumes[0];

    assert!(memory.disk_covered < BIG_DISK, "the cover at the row floor reached the disk");
    assert!(volume.cover_at_mean >= BIG_DISK, "the room at the real segment size falls short of the disk");
    assert_eq!(volume.seal_stops, 0);
    store.close().expect("close");
}

// the report splits what reel wrote by source: the records, their rows and the seal's footers
#[test]
fn written_by_source() {
    let io = SimulatedIo::new(Seed(31));
    let store = open(&io);
    let value = vec![4; 1_000];
    for number in 0..100 {
        store.put(COLUMN, format!("key-{number:03}").as_bytes(), &value).expect("put");
    }
    seal(&store, &io);
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    let written = |source: WriteSource| report.written[source.index()].0;
    assert!(written(WriteSource::Records) >= 100 * 1_000);
    assert!(written(WriteSource::Rows) > 0);
    assert!(written(WriteSource::SealFooters) > 0);
    assert_eq!(written(WriteSource::GivenUp), 0);
    store.close().expect("close");
}

// filters come before the record cache: a reopen with rows on disk sizes the arena for their filters, out of the record cache's
// share, and the budget holds the same
#[test]
fn arena_takes_room_for_rows_on_disk() {
    let io = SimulatedIo::new(Seed(41));
    let store = open(&io);
    let fresh = store.state.memory_report();
    for number in 0..ARENA_ROWS {
        store.put(COLUMN, format!("key-{number:06}").as_bytes(), b"value").expect("put");
    }
    seal(&store, &io);
    store.close().expect("close");

    let store = open(&io);
    let grown = store.state.memory_report();
    assert!(grown.arena_room.0 > fresh.arena_room.0, "the arena stayed at {:?}", grown.arena_room);
    assert!(grown.record_cache.0 < fresh.record_cache.0, "the record cache kept {:?}", grown.record_cache);
    let moved = grown.arena_room.0 - fresh.arena_room.0;
    assert!(fresh.record_cache.0 - grown.record_cache.0 + PAGE_SLACK >= moved, "the arena grew past what the record cache gave");
    store.close().expect("close");
}

const ARENA_ROWS: u64 = 1_000_000;
const PAGE_SLACK: u64 = 64 * 1024;

// a seal stuck on one full shard comes free once compaction runs, with every segment live and none overlapping another
#[test]
fn stuck_shard_comes_free() {
    let io = SimulatedIo::new(Seed(5));
    io.add_disk(Path::new(ROOT), SMALL_DISK);
    let store = open(&io);
    store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    let room = report.volumes[0].entry_room;
    let mut number = 0;
    while report.volumes[0].seal_stops == 0 && number < room.0 * 2 {
        store.put(COLUMN, format!("key-{number:05}").as_bytes(), VALUE).expect("put");
        store.seal_now();
        run(&io);
        store.maintenance_report(&mut report);
        number += 1;
    }
    assert!(store.state.volumes[0].is_seal_stuck.load(Ordering::Acquire));
    store.state.volumes[0].holds.compaction.store(false, Ordering::Release);
    store.state.ask_compaction();
    run(&io);
    run(&io);
    assert!(!store.state.volumes[0].is_seal_stuck.load(Ordering::Acquire), "the seal stayed stuck on a full shard");
    store.put(COLUMN, b"past-the-stop", VALUE).expect("put");
    let mut value = Vec::new();
    for key in 0..number {
        assert!(store.get(COLUMN, format!("key-{key:05}").as_bytes(), &mut value).expect("get"), "key {key}");
    }
    store.close().expect("close");
}

const SPLIT_KEYS: u64 = 500;

fn split_of(store: &Store) -> (u64, u64, u64, u64) {
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    let volume = &report.volumes[0];
    (volume.dead_counted.0, volume.unwalked_bytes.0, volume.inexact_dead.0, volume.cue_held_dead.0)
}

// the report splits dead space: segments the walk hasn't reached count as unknown, the walk then counts their dead bytes, and
// a live cue that can read the replaced rows holds them
#[test]
fn dead_space_splits_by_cause() {
    let io = SimulatedIo::new(Seed(70));
    let store = open(&io);
    let volume = &store.state.volumes[0];
    volume.holds.compaction.store(true, Ordering::Release);
    volume.holds.walk.store(true, Ordering::Release);
    let write_all = |value: &[u8]| {
        for number in 0..SPLIT_KEYS {
            store.put(COLUMN, format!("split-{number:04}").as_bytes(), value).expect("put");
        }
        store.seal_now();
        settle(&io);
    };
    write_all(b"first");
    let cue = store.cue().expect("cue");
    write_all(b"second");
    let (_counted, unwalked, _inexact, _held) = split_of(&store);
    assert!(unwalked > 0, "segments the walk hasn't reached show as unknown");

    volume.holds.walk.store(false, Ordering::Release);
    volume.holds.compaction.store(false, Ordering::Release);
    store.state.ask_compaction();
    settle(&io);
    let (counted, unwalked, _inexact, held) = split_of(&store);
    assert_eq!(unwalked, 0);
    assert!(counted > 0, "the walk counted the replaced rows");
    assert_eq!(held, counted, "the cue can read every replaced row");
    drop(cue);
    let (_counted, _unwalked, _inexact, held) = split_of(&store);
    assert_eq!(held, 0);
    store.close().expect("close");
}
