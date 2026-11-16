//! What a store says about itself: sizes from the summaries, and jobs that read as running, idle or stalled

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use crate::shape;
use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, ThreadCounts, VolumeConfig};
use reel::engine::engine_sizing::{ENTRY_LINE_DIVISOR, ENTRY_MARGIN, floor_entries};
use reel::engine::job_tally::STALL_AFTER;
use reel::engine::reel_store::ReelStore;
use reel::io::Io;
use reel::record::codec::Codec;
use reel_core::column_id::ColumnId;
use reel_core::maintenance_report::{JobKind, JobState, MaintenanceReport, PickOutcome, WriteSource};
use reel_core::store_error::StoreError;
use reel_core::units::{ByteCount, Nanoseconds, RootId, RowCount, SegmentCount, VolumeId};
use reel_core::usage::{ColumnUsage, UsageReport};
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_io::{PAUSE_TICK, SimulatedIo};

use crate::seed_sets::seeds;

const ROOT: &str = "/disk/reports";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(66 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 400_000;
const KEYS: u64 = 100;
const VALUE: &[u8] = &[7; 200];
const SMALL_DISK: ByteCount = ByteCount(1024 * 1024 * 1024);
const RUN_STEPS: u64 = 20_000;
const LARGE_DISK: ByteCount = ByteCount(1 << 40);
const COVER_BUDGET: ByteCount = ByteCount(128 * 1024 * 1024);
// a budget a little past the least, so the segment table's share fills before a small disk does
const TIGHT_BUDGET: ByteCount = ByteCount(48 * 1024 * 1024);
// the same with room for one worker
const TIGHT_WORKER_BUDGET: ByteCount = ByteCount(52 * 1024 * 1024);

// the tight budget for the threads a sim opens with: the workers pass adds a worker
fn tight_budget() -> ByteCount {
    if shape::workers() > 0 { TIGHT_WORKER_BUDGET } else { TIGHT_BUDGET }
}
const MOST_PAST_PUTS: u64 = 10_000_000;
const BIG_DISK: ByteCount = ByteCount(8 << 30);
const REAL_RECORD: &[u8] = &[5; 256 * 1024];
const REAL_SEGMENTS: u64 = 4;

type Store = ReelStore<SimulatedIo>;

fn open(io: &SimulatedIo) -> Store {
    open_at(io, BUDGET)
}

fn open_at(io: &SimulatedIo, budget: ByteCount) -> Store {
    open_with(io, budget, shape::threads())
}

fn open_with(io: &SimulatedIo, budget: ByteCount, threads: ThreadCounts) -> Store {
    let config = StoreConfig {
        memory_budget: budget,
        threads,
        volumes: vec![VolumeConfig {
            name: "reports".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(), open_segments: None, segment_bytes: None,
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Variable { most: 64 },
            codec: Codec::None,
            volume: VolumeId(0),
            complete_rows: None,
            sorted_rows: None,
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
        let store = open_at(&io, tight_budget());
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
    let store = open_at(&io, tight_budget());
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
    // a worker takes a share of the budget the table would cover the disk with, and this size of budget holds the table alone
    let store = open_with(&io, BUDGET, reel::config::NO_WORKERS);
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

// filters come before the record cache: a reopen with rows on disk past the floor grows the arena into the filter share, whose
// idle room the record cache gives back, and the record cache gives no more than the dead bits and the arena
#[test]
fn arena_takes_room_for_rows_on_disk() {
    reel::engine::engine_sizing::TEST_FLOOR_ROWS.with(|floor| floor.set(FLOOR_TEST_ROWS));
    let io = SimulatedIo::new(Seed(41));
    let store = open(&io);
    let fresh = store.state.memory_report();
    let filter_cache = store.state.filter_cache.heap_bytes();
    for number in 0..ARENA_ROWS {
        store.put(COLUMN, format!("key-{number:06}").as_bytes(), b"value").expect("put");
    }
    seal(&store, &io);
    store.close().expect("close");

    let store = open(&io);
    reel::engine::engine_sizing::TEST_FLOOR_ROWS.with(|floor| floor.set(0));
    let grown = store.state.memory_report();
    assert!(grown.arena_room.0 > fresh.arena_room.0, "the arena stayed at {:?}", grown.arena_room);
    assert_eq!(grown.budget, fresh.budget);
    let bits_grew = grown.dead_bits.0 - fresh.dead_bits.0;
    let arena_grew = grown.arena_room.0 - fresh.arena_room.0;
    let cache_gave = fresh.record_cache.0 - grown.record_cache.0;
    // the block share holds the arena, and the idle filter room it took from the record cache once rows sat in blocks
    let blocks_grew = grown.block_cache.0 - fresh.block_cache.0;
    assert!(blocks_grew >= arena_grew, "the block share grew {blocks_grew} for an arena of {arena_grew}");
    assert!(cache_gave <= bits_grew + blocks_grew + PAGE_SLACK, "the record cache gave {cache_gave} past the dead bits {bits_grew} and the block share {blocks_grew}");
    // the filter cache's idle room went to the other caches on both opens, so the arena grew into room they gave back
    assert_eq!(store.state.filter_cache.heap_bytes(), filter_cache, "the filter cache kept more than its stripe");
    store.close().expect("close");
}

const ARENA_ROWS: u64 = 1_000_000;
const PAGE_SLACK: u64 = 64 * 1024;

// fill one shard until the seal stops, then let compaction run: the seal comes free, with every segment live and none overlapping
fn stuck_shard_frees(io: &SimulatedIo, store: Store) {
    store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    let room = report.volumes[0].entry_room;
    let mut number = 0;
    while report.volumes[0].seal_stops == 0 && number < room.0 * 2 {
        store.put(COLUMN, format!("key-{number:05}").as_bytes(), VALUE).expect("put");
        store.seal_now();
        run(io);
        store.maintenance_report(&mut report);
        number += 1;
    }
    assert!(store.state.volumes[0].is_seal_stuck.load(Ordering::Acquire));
    store.state.volumes[0].holds.compaction.store(false, Ordering::Release);
    store.state.ask_compaction();
    run(io);
    run(io);
    assert!(!store.state.volumes[0].is_seal_stuck.load(Ordering::Acquire), "the seal stayed stuck on a full shard");
    store.put(COLUMN, b"past-the-stop", VALUE).expect("put");
    let mut value = Vec::new();
    for key in 0..number {
        assert!(store.get(COLUMN, format!("key-{key:05}").as_bytes(), &mut value).expect("get"), "key {key}");
    }
    store.close().expect("close");
}

// a seal stuck on one full shard comes free once compaction runs
#[test]
fn stuck_shard_comes_free() {
    let io = SimulatedIo::new(Seed(5));
    io.add_disk(Path::new(ROOT), SMALL_DISK);
    let store = open_at(&io, tight_budget());
    stuck_shard_frees(&io, store);
}

// with workers the forced pick goes to a seat, so a store a node runs frees a stuck shard too
#[test]
fn stuck_shard_comes_free_with_workers() {
    let io = SimulatedIo::new(Seed(5));
    io.add_disk(Path::new(ROOT), SMALL_DISK);
    let store = open_with(&io, TIGHT_WORKER_BUDGET, ThreadCounts { maintenance_workers: Some(1), ..reel::config::NO_WORKERS });
    stuck_shard_frees(&io, store);
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

const FLOOR_TEST_ROWS: u64 = 200;
const FLOOR_FILL_KEYS: u64 = 4_000;
const FLOOR_ROUNDS: u64 = 4;

// a store opened empty gets bits and filters for the floor's rows, and one filled past it answers every key right while the
// report shows the rows that went without bits
#[test]
fn past_the_floor_answers_right() {
    reel::engine::engine_sizing::TEST_FLOOR_ROWS.with(|floor| floor.set(FLOOR_TEST_ROWS));
    let io = SimulatedIo::new(Seed(71));
    let store = open(&io);
    reel::engine::engine_sizing::TEST_FLOOR_ROWS.with(|floor| floor.set(0));
    for round in 0..FLOOR_ROUNDS {
        for number in 0..FLOOR_FILL_KEYS {
            store.put(COLUMN, format!("floor-{number:05}").as_bytes(), format!("{round}").as_bytes()).expect("put");
        }
        store.seal_now();
        settle(&io);
    }
    let last = format!("{}", FLOOR_ROUNDS - 1).into_bytes();
    let mut value = Vec::new();
    for number in 0..FLOOR_FILL_KEYS {
        assert!(store.get(COLUMN, format!("floor-{number:05}").as_bytes(), &mut value).expect("get"), "key {number}");
        assert_eq!(value, last, "key {number}");
    }
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    assert!(report.volumes[0].rows_without_bits > 0, "no row went without a bit past the floor");
    assert!(store.memory_report().arena_segments_left_out.0 > 0, "no segment went without filters past the floor");
    store.close().expect("close");
}

const WIDE_OPEN_KEYS: u64 = 300;
const WIDE_OPEN_VALUE_BYTES: usize = 32 * 1024;

// a reopen over wide records sizes the write index for them and gives the rest to the record cache's share
#[test]
fn wide_records_grow_the_record_cache() {
    let io = SimulatedIo::new(Seed(42));
    let store = open(&io);
    let fresh = store.state.memory_report();
    let value = vec![7; WIDE_OPEN_VALUE_BYTES];
    for number in 0..WIDE_OPEN_KEYS {
        store.put(COLUMN, format!("wide-{number:05}").as_bytes(), &value).expect("put");
    }
    seal(&store, &io);
    store.close().expect("close");

    let store = open(&io);
    let wide = store.state.memory_report();
    assert!(wide.write_index < fresh.write_index, "the write index stayed at {:?}", wide.write_index);
    // filters come first, so the arena takes what it lacks out of the record cache's share, and the share is what grew
    let share = |report: &reel_core::memory_report::MemoryReport| report.record_cache.0 + report.arena_room.0;
    assert!(share(&wide) > share(&fresh), "the record cache's share stayed at {}", share(&wide));
    let mut found = Vec::new();
    assert!(store.get(COLUMN, b"wide-00007", &mut found).expect("get"));
    assert_eq!(found, value);
    store.close().expect("close");
}

// the report counts the record cache's misses, the admit on a record's second miss, the hit after it and the values it holds
#[test]
fn record_cache_counts_show() {
    let io = SimulatedIo::new(Seed(43));
    let store = open(&io);
    store.put(COLUMN, b"counted", b"value").expect("put");
    seal(&store, &io);
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    let (hits, misses, admits) = (report.record_cache_hits, report.record_cache_misses, report.record_cache_admits);
    let mut found = Vec::new();
    for _read in 0..3 {
        assert!(store.get(COLUMN, b"counted", &mut found).expect("get"));
        assert_eq!(found, b"value");
    }
    store.maintenance_report(&mut report);
    assert_eq!(report.record_cache_misses - misses, 2);
    assert_eq!(report.record_cache_admits - admits, 1);
    assert_eq!(report.record_cache_hits - hits, 1);
    assert!(report.record_cache_values >= 1);
    store.close().expect("close");
}

const DEAD_BLOCK_KEYS: u64 = 40_000;
const DEAD_BLOCK_BUDGET: ByteCount = ByteCount(256 * 1024 * 1024);

fn dead_key(number: u64) -> Vec<u8> {
    format!("dead-{number:06}-{:0>52}", number).into_bytes()
}

// an unlink proves a wholly dead segment of a couple of hundred blocks a block a step beside the walk, so a walk waiting meanwhile
// is never held past the bound, the segment goes, and the walk ends
#[test]
fn unlink_proof_steps_beside_the_walk() {
    let io = SimulatedIo::new(Seed(44));
    let store = open_at(&io, DEAD_BLOCK_BUDGET);
    let volume = &store.state.volumes[0];
    volume.holds.compaction.store(true, Ordering::Release);
    for number in 0..DEAD_BLOCK_KEYS {
        store.put(COLUMN, &dead_key(number), b"old").expect("put");
    }
    seal(&store, &io);
    let dead = reel::guard::hold(&volume.summary).rows().map(|row| row.facts.segment).next().expect("a segment");
    for number in 0..DEAD_BLOCK_KEYS {
        store.put(COLUMN, &dead_key(number), b"new").expect("put");
    }
    seal(&store, &io);
    settle(&io);
    // a segment sealed with the walk held leaves it a target to take while the unlink proves the dead one
    volume.holds.walk.store(true, Ordering::Release);
    store.put(COLUMN, b"waiting", b"value").expect("put");
    seal(&store, &io);
    assert!(store.state.pending_walks(0) > 0, "no walk waited");
    volume.holds.walk.store(false, Ordering::Release);
    volume.holds.compaction.store(false, Ordering::Release);
    store.state.ask_compaction();
    settle(&io);
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    assert!(!volume.table.holds(dead), "the dead segment stayed");
    assert_eq!(store.state.pending_walks(0), 0, "the walk never ended");
    assert!(
        report.longest_walkless_steps <= reel::engine::planner::MOST_WALKLESS_STEPS,
        "the planner held the walk back {} steps, last {:?}",
        report.longest_walkless_steps,
        report.longest_walkless_kind
    );
    let mut value = Vec::new();
    assert!(store.get(COLUMN, &dead_key(7), &mut value).expect("get"));
    assert_eq!(value, b"new");
    store.close().expect("close");
}

const WIDE_COLUMN: ColumnId = ColumnId(2);
const MIXED_SMALL_KEYS: u64 = 900;
const MIXED_WIDE_KEYS: u64 = 100;
const MIXED_SMALL_VALUE_BYTES: usize = 150;
const MIXED_WIDE_VALUE_BYTES: usize = 64 * 1024;

// a store with a column of small rows, and when asked a column of values too wide to cache
fn open_mixed(io: &SimulatedIo) -> Store {
    let column = |id: ColumnId| ColumnConfig {
        id,
        name: format!("column-{}", id.0),
        key_shape: KeyShape::Variable { most: 64 },
        codec: Codec::None,
        volume: VolumeId(0),
        complete_rows: None,
        sorted_rows: None,
        least_value: None,
    };
    let config = StoreConfig {
        memory_budget: BUDGET,
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig { name: "mixed".to_string(), roots: vec![PathBuf::from(ROOT)], sync: SyncPolicy::Never, device: Default::default(), open_segments: None, segment_bytes: None }],
        columns: vec![column(COLUMN), column(WIDE_COLUMN)],
    };
    ReelStore::open(io.clone(), &config).expect("open")
}

fn reopened_slots(has_wide: bool) -> usize {
    let io = SimulatedIo::new(Seed(46));
    let store = open_mixed(&io);
    for number in 0..MIXED_SMALL_KEYS {
        store.put(COLUMN, format!("small-{number:05}").as_bytes(), &[3; MIXED_SMALL_VALUE_BYTES]).expect("put");
    }
    if has_wide {
        for number in 0..MIXED_WIDE_KEYS {
            store.put(WIDE_COLUMN, format!("wide-{number:05}").as_bytes(), &[4; MIXED_WIDE_VALUE_BYTES]).expect("put");
        }
    }
    seal(&store, &io);
    store.close().expect("close");
    let store = open_mixed(&io);
    let slots = store.state.record_cache.index_slots();
    store.close().expect("close");
    slots
}

// a column of values too wide to cache leaves the record cache planned for the small rows beside it
#[test]
fn wide_column_leaves_the_cache_plan() {
    assert_eq!(reopened_slots(true), reopened_slots(false));
}

fn volume_reading(store: &Store) -> reel_core::maintenance_report::VolumeReading {
    let mut report = MaintenanceReport::default();
    store.maintenance_report(&mut report);
    report.volumes[0]
}

// the rows a scan meets in the write index, and the open segments holding them, as a volume reading: the rows put and one
// segment while they sit open, and none once they seal
#[test]
fn a_volume_reports_its_open_rows() {
    let io = SimulatedIo::new(Seed(160));
    let store = open(&io);
    for number in 0..KEYS {
        store.put(COLUMN, format!("key-{number:03}").as_bytes(), VALUE).expect("put");
    }
    let reading = volume_reading(&store);
    assert_eq!(reading.rows_in_index, RowCount(KEYS), "{reading:?}");
    assert_eq!(reading.open_segments_with_rows, SegmentCount(1), "{reading:?}");
    store.seal_now();
    settle(&io);
    let reading = volume_reading(&store);
    assert_eq!(reading.rows_in_index, RowCount(0), "{reading:?}");
    assert_eq!(reading.open_segments_with_rows, SegmentCount(0), "{reading:?}");
    store.close().expect("close");
}

// a range delete a cue below it holds in the pool shows as a pending cover and as looks the cue held, and both settle once the
// cue goes
#[test]
fn a_volume_reports_covers_a_cue_holds() {
    let io = SimulatedIo::new(Seed(161));
    let store = open(&io);
    for number in 0..KEYS {
        store.put(COLUMN, format!("key-{number:03}").as_bytes(), VALUE).expect("put");
    }
    let cue = store.cue().expect("cue below the cover");
    store.delete_range(COLUMN, b"key-", b"key-~").expect("delete range");
    store.seal_now();
    settle(&io);
    store.state.ask_compaction();
    settle(&io);
    let held = volume_reading(&store);
    assert_eq!(held.pending_covers, 1, "{held:?}");
    assert!(held.covers_held_by_cues > 0, "{held:?}");
    drop(cue);
    store.state.ask_compaction();
    settle(&io);
    let gone = volume_reading(&store);
    assert_eq!(gone.pending_covers, 0, "{gone:?}");
    store.close().expect("close");
}
