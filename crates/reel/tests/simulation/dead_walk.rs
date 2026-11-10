//! The dead-byte walk: every superseded row counts once, whatever order segments seal and walk in

use std::iter;
use std::thread;
use std::time::{Duration, Instant};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::compact_job::CompactJob;
use reel::engine::engine_state::IndexWait;
use reel::engine::open_segment::SegmentPhase;
use reel::engine::planner::{Planner, WALK_LAG_SEGMENTS};
use reel::engine::reel_store::ReelStore;
use reel::engine::engine_sizing::CURSOR_BYTES;
use reel::segment::key_cursor::KeyCursor;
use reel_sim::simulated_file::SimulatedFile;
use reel::engine::held_bits::HeldBits;
use reel::fixed_list::FixedList;
use reel::engine::walk_job::{SLOT_COUNTED, WalkJob, WalkTarget, FREE_IN_PLACE_BYTES};
use reel::guard::hold;
use reel::index::key_hash::hash_key;
use reel::io::IoTask;
use reel::io::job::{Job, JobStep};
use reel::record::codec::Codec;
use reel::segment::segment_tail::SegmentNumber;
use reel::segment::summary::{LostRows, SummaryPaths, SummaryRows, copy_ends, read_summary, write_copy};
use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, Nanoseconds, RowCount, VolumeId, WritePlace};
use reel_sim::seeded_random::{Seed, SeededRandom};
use reel_sim::simulated_disk::OperationCount;
use reel_sim::simulated_io::SimulatedIo;

const WALK_WORKERS: usize = 2;
const ROOT: &str = "/disk/walk";
const COLUMN: ColumnId = ColumnId(1);
const SECOND_COLUMN: ColumnId = ColumnId(2);
const SECOND_ROOT: &str = "/disk/walk-second";
const BUDGET: ByteCount = ByteCount(128 * 1024 * 1024);
const LARGE_BUDGET: ByteCount = ByteCount(512 * 1024 * 1024);
const WALK_JOB: &str = "reel-planner";
const SEAL_JOB: &str = "reel-seal";
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 20_000;
const YIELD_STEPS: u64 = 200;
const TURN_SEEDS: u64 = 8;
const BUSY: u32 = 1_000;
const VERSIONS: u64 = 600;
const LONG_KEYS: u64 = 62;
const LONG_KEY_BYTES: usize = 1_000;
const RANGE_ROWS: u64 = 4_000;
const ORDERS: [[usize; 3]; 6] = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]];
// how long a test waits for a landing to take the claim lock, and for a begin to show it waited
const LOCK_WAIT: Duration = Duration::from_secs(5);
const BEGIN_WAIT: Duration = Duration::from_millis(50);

type Store = ReelStore<SimulatedIo>;

fn config(budget: ByteCount) -> StoreConfig {
    StoreConfig {
        memory_budget: budget,
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "walk".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(),
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Variable { most: LONG_KEY_BYTES as u16 + 8 },
            codec: Codec::None,
            volume: VolumeId(0),
            complete_rows: None,
            least_value: None,
        }],
    }
}

fn open(io: &SimulatedIo) -> Store {
    held(ReelStore::open(io.clone(), &config(BUDGET)).expect("open"))
}

// a key's rows all sit in one shard, so a long run of them needs a large index
fn open_workers(io: &SimulatedIo) -> Store {
    let mut config = config(BUDGET);
    config.threads.maintenance_workers = Some(WALK_WORKERS);
    held(ReelStore::open(io.clone(), &config).expect("open"))
}

fn open_large(io: &SimulatedIo) -> Store {
    held(ReelStore::open(io.clone(), &config(LARGE_BUDGET)).expect("open"))
}

// these tests read the counts of segments compaction would take away
fn held(store: Store) -> Store {
    for volume in &store.state.volumes {
        volume.holds.compaction.store(true, Ordering::Release);
    }
    store
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

// seal one segment by number, with whatever else runs held to the seal's own steps
fn seal_segment(store: &Store, io: &SimulatedIo, number: SegmentNumber) {
    let volume = &store.state.volumes[0];
    {
        let mut claims = hold(&volume.claims);
        let segment = claims.open.iter_mut().find(|segment| segment.number == number).expect("open");
        segment.phase = SegmentPhase::Sealing;
    }
    if let Some(task) = store.state.seal_tasks[0].get() {
        task.wake();
    }
    settle(io);
    assert!(volume.table.holds(number));
}

// open another segment beside the ones there and return its number
fn add_open(store: &Store, io: &SimulatedIo) -> SegmentNumber {
    let volume = &store.state.volumes[0];
    hold(&volume.claims).wants_prepared = true;
    store.state.prepare_segments();
    settle(io);
    let mut claims = hold(&volume.claims);
    let spare = claims.prepared.take().expect("a spare");
    let number = spare.number;
    volume.open_slot(spare.slot);
    claims.open.push(spare);
    number
}

// write a key to one open segment, by making every other look busy
fn put_in(store: &Store, number: SegmentNumber, value: &[u8]) {
    let volume = &store.state.volumes[0];
    let busy = |is_busy: bool| {
        for segment in hold(&volume.claims).open.iter_mut().filter(|segment| segment.number != number) {
            segment.in_flight = if is_busy { BUSY } else { 0 };
        }
    };
    busy(true);
    store.put(COLUMN, b"key", value).expect("put");
    busy(false);
}

// read a segment's rows and dead rows from its summary row
fn counts(store: &Store, number: SegmentNumber) -> (RowCount, RowCount) {
    let rows = hold(&store.state.volumes[0].summary);
    let row = rows.get(number).expect("a row");
    let column = row.cursor().column().expect("a column");
    (column.row_count, column.dead_rows)
}

// the block lines add up to the same live rows as the row's column
fn assert_blocks(store: &Store, segments: &[SegmentNumber]) {
    for number in segments {
        let (rows, dead) = counts(store, *number);
        assert_eq!(store.state.volumes[0].table.live_rows(*number), Some(rows.0 - dead.0));
    }
}

fn live_rows(store: &Store, segments: &[SegmentNumber]) -> u64 {
    segments.iter().map(|number| counts(store, *number)).map(|(rows, dead)| rows.0 - dead.0).sum()
}

// long keys that nearly fill a row block, then versions of one key, so the block ends among them
fn fill_block(store: &Store) {
    for number in 0..LONG_KEYS {
        let mut key = format!("a{number:03}").into_bytes();
        key.resize(LONG_KEY_BYTES, number as u8);
        store.put(COLUMN, &key, b"value").expect("put");
    }
    for number in 0..VERSIONS {
        store.put(COLUMN, b"key", &number.to_le_bytes()).expect("put");
    }
}

// two segments of one key, both sealed before the walk starts: the older row counts once
#[test]
fn lagging_walk() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io);
    io.hold_jobs(WALK_JOB, true);
    store.put(COLUMN, b"key", b"older").expect("put");
    let first = hold(&store.state.volumes[0].claims).open[0].number;
    seal_segment(&store, &io, first);
    store.put(COLUMN, b"key", b"newer").expect("put");
    let second = hold(&store.state.volumes[0].claims).open[0].number;
    seal_segment(&store, &io, second);
    assert_eq!(store.state.pending_walks(0), 2);

    io.hold_jobs(WALK_JOB, false);
    settle(&io);

    assert_eq!(store.state.pending_walks(0), 0);
    assert_eq!(counts(&store, first).1, RowCount(1));
    assert_eq!(counts(&store, second).1, RowCount(0));
    assert_blocks(&store, &[first, second]);
    store.close().expect("close");
}

// three segments of one key, with places in one order and seals in every other: one row stays live
#[test]
fn every_seal_order() {
    for (seed, order) in ORDERS.iter().enumerate() {
        for is_held in [false, true] {
            let io = SimulatedIo::new(Seed(seed as u64 + 2));
            let store = open(&io);
            io.hold_jobs(WALK_JOB, is_held);
            store.put(COLUMN, b"first", b"value").expect("put");
            let first = hold(&store.state.volumes[0].claims).open[0].number;
            let segments = [first, add_open(&store, &io), add_open(&store, &io)];
            for (place, number) in segments.iter().enumerate() {
                put_in(&store, *number, format!("value-{place}").as_bytes());
            }

            for index in order {
                seal_segment(&store, &io, segments[*index]);
            }
            io.hold_jobs(WALK_JOB, false);
            settle(&io);

            let newest = counts(&store, segments[2]);
            assert_eq!(live_rows(&store, &segments), 2, "order {order:?}");
            assert_blocks(&store, &segments);
            assert_eq!(newest.0.0 - newest.1.0, newest.0.0, "order {order:?}");
            store.close().expect("close");
        }
    }
}

// a key whose versions run across row blocks counts each once, and flips the older segment's row once
#[test]
fn versions_across_blocks() {
    let io = SimulatedIo::new(Seed(10));
    let store = open(&io);
    store.put(COLUMN, b"key", b"older").expect("put");
    let older = hold(&store.state.volumes[0].claims).open[0].number;
    seal_segment(&store, &io, older);
    fill_block(&store);
    let newer = hold(&store.state.volumes[0].claims).open[0].number;
    seal_segment(&store, &io, newer);
    settle(&io);

    assert_eq!(counts(&store, older).1, RowCount(1));
    assert_eq!(counts(&store, newer), (RowCount(LONG_KEYS + VERSIONS), RowCount(VERSIONS - 1)));
    assert_blocks(&store, &[older, newer]);
    store.close().expect("close");
}

// a walk stopped part way resumes at its saved position and counts nothing twice
#[test]
fn walk_resumes() {
    let io = SimulatedIo::new(Seed(11));
    let store = open(&io);
    store.put(COLUMN, b"key", b"older").expect("put");
    let older = hold(&store.state.volumes[0].claims).open[0].number;
    seal_segment(&store, &io, older);
    settle(&io);
    io.hold_jobs(WALK_JOB, true);
    fill_block(&store);
    let newer = hold(&store.state.volumes[0].claims).open[0].number;
    seal_segment(&store, &io, newer);

    let mut job = WalkJob::new(store.state.clone(), 0);
    assert!(job.pick_target());
    let target = job.target.expect("a target");
    assert!(target.block_count > 1);
    job.walk_block(target).expect("a block");
    store.state.save_summary(&store.state.volumes[0]).expect("save");
    drop(job);
    io.kill();
    drop(store);

    let store = open(&io);
    settle(&io);

    assert_eq!(counts(&store, older).1, RowCount(1));
    assert_eq!(counts(&store, newer), (RowCount(LONG_KEYS + VERSIONS), RowCount(VERSIONS - 1)));
    assert_blocks(&store, &[older, newer]);
    store.close().expect("close");
}

// a row from the older copy rebuilds its own counts and flips nothing, so no row counts twice
#[test]
fn older_copy_row() {
    for is_newer_lost in [false, true] {
        let io = SimulatedIo::new(Seed(12));
        let store = open(&io);
        store.put(COLUMN, b"key", b"older").expect("put");
        let older = hold(&store.state.volumes[0].claims).open[0].number;
        seal_segment(&store, &io, older);
        store.put(COLUMN, b"key", b"newer").expect("put");
        let newer = hold(&store.state.volumes[0].claims).open[0].number;
        seal_segment(&store, &io, newer);
        settle(&io);
        store.close().expect("close");
        drop(store);

        let paths = SummaryPaths::new(Path::new(ROOT));
        let mut rows = SummaryRows::new(ByteCount(0));
        let mark = read_summary(&io, &paths, &mut rows, &mut LostRows::default()).expect("read").expect("a mark");
        rows.remove(if is_newer_lost { newer } else { older });
        let ends = copy_ends(WritePlace(mark.0 + 1), rows.highest_forward, rows.bytes());
        write_copy(&io, &paths, &paths.current, &ends, rows.bytes()).expect("write");

        let store = open(&io);
        settle(&io);

        assert_eq!(counts(&store, older).1, RowCount(1));
        assert_eq!(counts(&store, newer).1, RowCount(0));
        assert_blocks(&store, &[older, newer]);
        store.close().expect("close");
    }
}

// a reopen brings each block's dead rows back from the summary
#[test]
fn blocks_reopen() {
    let io = SimulatedIo::new(Seed(13));
    let store = open(&io);
    store.put(COLUMN, b"key", b"older").expect("put");
    let older = hold(&store.state.volumes[0].claims).open[0].number;
    seal_segment(&store, &io, older);
    store.put(COLUMN, b"key", b"newer").expect("put");
    let newer = hold(&store.state.volumes[0].claims).open[0].number;
    seal_segment(&store, &io, newer);
    settle(&io);
    store.close().expect("close");
    drop(store);

    let store = open(&io);

    assert_eq!(counts(&store, older).1, RowCount(1));
    assert_blocks(&store, &[older, newer]);
    store.close().expect("close");
}

// a read that fails on a block's first key leaves nothing behind: the retry finds that key's top again
#[test]
fn failed_read_retry() {
    let io = SimulatedIo::new(Seed(14));
    let store = open(&io);
    io.hold_jobs(WALK_JOB, true);
    store.put(COLUMN, b"key", b"older").expect("put");
    let older = hold(&store.state.volumes[0].claims).open[0].number;
    seal_segment(&store, &io, older);
    store.put(COLUMN, b"key", b"newer").expect("put");
    let newer = hold(&store.state.volumes[0].claims).open[0].number;
    seal_segment(&store, &io, newer);

    let mut job = WalkJob::new(store.state.clone(), 0);
    assert!(job.pick_target());
    job.walk_block(job.target.expect("a target")).expect("the older segment");
    assert!(job.pick_target());
    let target = job.target.expect("a target");
    assert_eq!(target.segment, newer);
    io.fail_process_reads_after(OperationCount(1));
    assert!(job.walk_block(target).is_err());
    assert_eq!(store.state.walk_retries(0), 1);
    job.walk_block(target).expect("the retry");

    assert_eq!(counts(&store, older).1, RowCount(1));
    assert_eq!(counts(&store, newer).1, RowCount(0));
    assert_blocks(&store, &[older, newer]);
    drop(job);
    store.close().expect("close");
}

// a block of range deletes alone inside a key's run hides no key from a walk that resumes after it
#[test]
fn resume_past_ranges() {
    let long_key = vec![b'k'; LONG_KEY_BYTES];
    let mut end = long_key.clone();
    end.push(b'z');
    let mut position = 1;
    loop {
        let io = SimulatedIo::new(Seed(15));
        let store = open_large(&io);
        store.put(COLUMN, &long_key, b"oldest").expect("put");
        let older = hold(&store.state.volumes[0].claims).open[0].number;
        seal_segment(&store, &io, older);
        settle(&io);
        io.hold_jobs(WALK_JOB, true);
        store.put(COLUMN, &long_key, b"older").expect("put");
        for _range in 0..RANGE_ROWS {
            store.delete_range(COLUMN, &long_key, &end).expect("delete");
        }
        store.put(COLUMN, &long_key, b"newer").expect("put");
        let newer = hold(&store.state.volumes[0].claims).open[0].number;
        seal_segment(&store, &io, newer);

        let mut job = WalkJob::new(store.state.clone(), 0);
        assert!(job.pick_target());
        let target = job.target.expect("a target");
        assert!(target.block_count > 2, "{} blocks", target.block_count);
        let blocks = target.block_count;
        for _block in 0..position {
            job.walk_block(job.target.expect("a target")).expect("a block");
        }
        store.state.save_summary(&store.state.volumes[0]).expect("save");
        drop(job);
        io.kill();
        drop(store);

        let store = open_large(&io);
        settle(&io);
        assert_eq!(counts(&store, older).1, RowCount(1), "resume at {position}");
        assert_eq!(counts(&store, newer).1, RowCount(1), "resume at {position}");
        store.close().expect("close");

        position += 1;
        if position >= blocks {
            return;
        }
    }
}

// a seal and a backward walk agree on live bytes, range delete rows left out of both
#[test]
fn live_bytes_agree() {
    let io = SimulatedIo::new(Seed(16));
    let store = open(&io);
    store.put(COLUMN, b"aaa", b"value").expect("put");
    store.delete_range(COLUMN, b"key", b"kez").expect("delete");
    let number = hold(&store.state.volumes[0].claims).open[0].number;
    seal_segment(&store, &io, number);
    settle(&io);
    let sealed = bytes_of(&store, number);
    assert!(sealed.0 > 0);
    store.close().expect("close");
    drop(store);

    let paths = SummaryPaths::new(Path::new(ROOT));
    let mut rows = SummaryRows::new(ByteCount(0));
    let mark = read_summary(&io, &paths, &mut rows, &mut LostRows::default()).expect("read").expect("a mark");
    rows.remove(number);
    let ends = copy_ends(WritePlace(mark.0 + 1), rows.highest_forward, rows.bytes());
    write_copy(&io, &paths, &paths.current, &ends, rows.bytes()).expect("write");

    let store = open(&io);
    settle(&io);

    assert_eq!(bytes_of(&store, number), sealed);
    store.close().expect("close");
}

fn bytes_of(store: &Store, number: SegmentNumber) -> (u64, u64) {
    let rows = hold(&store.state.volumes[0].summary);
    let column = rows.get(number).expect("a row").cursor().column().expect("a column");
    (column.live_bytes.0, column.dead_bytes.0)
}

// the walk stands aside while a writer waits for index room, and goes on once none does
#[test]
fn yields_to_writers() {
    let io = SimulatedIo::new(Seed(17));
    let store = open(&io);
    io.hold_jobs(WALK_JOB, true);
    store.put(COLUMN, b"key", b"value").expect("put");
    let number = hold(&store.state.volumes[0].claims).open[0].number;
    seal_segment(&store, &io, number);
    assert_eq!(store.state.pending_walks(0), 1);

    let waiting = IndexWait::new(&store.state.index_waiters);
    io.hold_jobs(WALK_JOB, false);
    for _step in 0..YIELD_STEPS {
        io.advance_clock(TICK);
        io.step_any();
    }
    assert_eq!(store.state.pending_walks(0), 1);

    drop(waiting);
    io.advance_clock(TICK);
    settle(&io);
    assert_eq!(store.state.pending_walks(0), 0);
    store.close().expect("close");
}

// a walk waiting out a failed read holds up no other volume's walk
#[test]
fn volumes_in_turn() {
    // the simulator picks the unit that goes first, so some seeds try the waiting walk before the other
    for seed in 0..TURN_SEEDS {
        turn_with(Seed(seed));
    }
}

fn turn_with(seed: Seed) {
    let io = SimulatedIo::new(seed);
    let mut config = config(BUDGET);
    config.volumes.push(VolumeConfig {
        name: "second".to_string(),
        roots: vec![PathBuf::from(SECOND_ROOT)],
        sync: SyncPolicy::Never,
        device: Default::default(),
    });
    config.columns.push(ColumnConfig {
        id: SECOND_COLUMN,
        name: "second".to_string(),
        key_shape: KeyShape::Variable { most: 64 },
        codec: Codec::None,
        volume: VolumeId(1),
        complete_rows: None,
        least_value: None,
    });
    let store = held(ReelStore::open(io.clone(), &config).expect("open"));
    io.hold_jobs(WALK_JOB, true);
    store.put(COLUMN, b"key", b"value").expect("put");
    store.put(SECOND_COLUMN, b"key", b"value").expect("put");
    store.seal_now();
    settle(&io);
    assert_eq!((store.state.pending_walks(0), store.state.pending_walks(1)), (1, 1));

    let mut walks = vec![WalkJob::new(store.state.clone(), 0), WalkJob::new(store.state.clone(), 1)];
    walks[0].retry_at = Some(Nanoseconds(u64::MAX));
    let mut planner = Planner {
        state: store.state.clone(),
        walks,
        compactions: vec![CompactJob::new(store.state.clone(), 0), CompactJob::new(store.state.clone(), 1)],
        filters: Vec::new(),
        last: 0,
    };

    assert_eq!(land_block(&mut planner, &io), JobStep::Worked);

    assert_eq!((store.state.pending_walks(0), store.state.pending_walks(1)), (1, 0));
    assert_eq!(planner.step(), JobStep::IdleUntil { deadline: Nanoseconds(u64::MAX) });
    drop(planner);
    store.close().expect("close");
}

// a stop ends a walk at its next probe, keeps nothing of the block, and a later open counts it whole
#[test]
fn stop_mid_block() {
    let io = SimulatedIo::new(Seed(19));
    let store = open(&io);
    store.put(COLUMN, b"key", b"older").expect("put");
    let older = hold(&store.state.volumes[0].claims).open[0].number;
    seal_segment(&store, &io, older);
    settle(&io);
    io.hold_jobs(WALK_JOB, true);
    store.put(COLUMN, b"key", b"newer").expect("put");
    let newer = hold(&store.state.volumes[0].claims).open[0].number;
    seal_segment(&store, &io, newer);
    assert!(store.state.is_summary_lookup_whole(0));

    let mut job = WalkJob::new(store.state.clone(), 0);
    assert!(job.pick_target());
    let target = job.target.expect("a target");
    store.state.is_stopping.store(true, Ordering::Release);
    assert!(job.walk_block(target).is_err());
    assert_eq!(store.state.walk_retries(0), 0);
    assert_eq!(store.state.pending_walks(0), 1);
    assert_eq!(counts(&store, older).1, RowCount(0));
    drop(job);
    store.close().expect("close");
    drop(store);

    let store = open(&io);
    settle(&io);

    assert_eq!(counts(&store, older).1, RowCount(1));
    assert_eq!(store.state.pending_walks(0), 0);
    store.close().expect("close");
}

// a row from the older copy sits behind counts the newer copy holds, so it rebuilds its own and flips nothing again
#[test]
fn older_row_behind() {
    let io = SimulatedIo::new(Seed(20));
    let store = open(&io);
    io.hold_jobs(WALK_JOB, true);
    store.put(COLUMN, b"key", b"older").expect("put");
    let older = hold(&store.state.volumes[0].claims).open[0].number;
    seal_segment(&store, &io, older);
    fill_block(&store);
    let newer = hold(&store.state.volumes[0].claims).open[0].number;
    seal_segment(&store, &io, newer);

    let mut job = WalkJob::new(store.state.clone(), 0);
    assert!(job.pick_target());
    job.walk_block(job.target.expect("the older segment")).expect("a block");
    assert!(job.pick_target());
    let target = job.target.expect("the newer segment");
    assert!(target.block_count > 1);
    // both copies hold the older segment live and the newer one unwalked
    store.state.save_summary(&store.state.volumes[0]).expect("save");
    job.walk_block(target).expect("a block");
    assert_eq!(counts(&store, older).1, RowCount(1));

    // the newer copy holds the flip and lost the row that says how far the walk came
    let paths = SummaryPaths::new(Path::new(ROOT));
    let mut on_disk = SummaryRows::new(ByteCount(0));
    let mark = read_summary(&io, &paths, &mut on_disk, &mut LostRows::default()).expect("read").expect("a mark");
    let mut kept = SummaryRows::new(BUDGET);
    {
        let rows = hold(&store.state.volumes[0].summary);
        for (number, at) in rows.offsets() {
            if number != newer {
                assert!(kept.copy_row(&rows, at));
            }
        }
    }
    let ends = copy_ends(WritePlace(mark.0 + 1), on_disk.highest_forward, kept.bytes());
    write_copy(&io, &paths, &paths.current, &ends, kept.bytes()).expect("write");
    drop(job);
    io.kill();
    drop(store);

    let store = open(&io);
    settle(&io);

    assert_eq!(counts(&store, older).1, RowCount(1));
    assert_eq!(counts(&store, newer).1, RowCount(VERSIONS - 1));
    store.close().expect("close");
}

// the walk stands aside while a seal has a segment waiting, and stops standing aside once it has fallen too far behind
#[test]
fn stands_aside_for_seals() {
    let io = SimulatedIo::new(Seed(21));
    let store = open(&io);
    io.hold_jobs(WALK_JOB, true);
    store.put(COLUMN, b"key", b"value").expect("put");
    let number = hold(&store.state.volumes[0].claims).open[0].number;
    seal_segment(&store, &io, number);
    io.hold_jobs(SEAL_JOB, true);
    store.put(COLUMN, b"other", b"value").expect("put");
    let volume = &store.state.volumes[0];
    assert_eq!(volume.walks_waiting.load(Ordering::Acquire), 1);
    let mut planner = Planner {
        state: store.state.clone(),
        walks: vec![WalkJob::new(store.state.clone(), 0)],
        compactions: vec![CompactJob::new(store.state.clone(), 0)],
        filters: Vec::new(),
        last: 0,
    };
    assert!(!planner.is_seal_first());

    hold(&volume.claims).open[0].phase = SegmentPhase::Sealing;
    assert!(planner.is_seal_first());
    assert_eq!(planner.step(), JobStep::Idle);
    assert_eq!(store.state.pending_walks(0), 1);

    volume.walks_waiting.store(WALK_LAG_SEGMENTS, Ordering::Release);
    assert!(!planner.is_seal_first());
    assert_eq!(land_block(&mut planner, &io), JobStep::Worked);
    assert_eq!(store.state.pending_walks(0), 0);

    drop(planner);
    io.hold_jobs(SEAL_JOB, false);
    store.close().expect("close");
}

// run the planner by hand, with the workers it hands blocks to
fn land_block(planner: &mut Planner<SimulatedIo>, io: &SimulatedIo) -> JobStep {
    io.hold_jobs("reel-planner", true);
    for _step in 0..STEPS {
        match planner.step() {
            JobStep::Idle | JobStep::IdleUntil { .. } => {
                io.step_any();
            },
            step => return step,
        }
    }
    panic!("no block landed");
}

// a reopen counts the segments still waiting for a walk
#[test]
fn waiting_reopen() {
    let io = SimulatedIo::new(Seed(22));
    let store = open(&io);
    io.hold_jobs(WALK_JOB, true);
    store.put(COLUMN, b"key", b"value").expect("put");
    let number = hold(&store.state.volumes[0].claims).open[0].number;
    seal_segment(&store, &io, number);
    store.close().expect("close");
    drop(store);

    let store = open(&io);

    assert_eq!(store.state.volumes[0].walks_waiting.load(Ordering::Acquire), 1);
    settle(&io);
    assert_eq!(store.state.volumes[0].walks_waiting.load(Ordering::Acquire), 0);
    store.close().expect("close");
}

fn two_blocks(store: &Store, io: &SimulatedIo) -> WalkJob<SimulatedIo> {
    io.hold_jobs("reel-planner", true);
    io.hold_jobs("reel-walk", true);
    fill_block(store);
    fill_block(store);
    let number = hold(&store.state.volumes[0].claims).open[0].number;
    seal_segment(store, io, number);
    let mut job = WalkJob::new(store.state.clone(), 0);
    assert!(job.pick_target());
    assert!(job.target.expect("a target").block_count >= 2);
    job
}

// a worker can finish ahead of the block before it, and its counts wait their turn, so the position stays contiguous
#[test]
fn blocks_land_in_order() {
    let io = SimulatedIo::new(Seed(40));
    let store = open_workers(&io);
    let mut job = two_blocks(&store, &io);
    let target = job.target.expect("a target");

    let slot = &store.state.volumes[0].walk_slots[1];
    let ahead = WalkTarget {
        next_block: target.next_block + 1,
        ..target
    };
    let mut work = hold(&slot.work);
    work.target = ahead;
    work.next_block = ahead.next_block + 1;
    drop(work);
    slot.phase.store(SLOT_COUNTED, Ordering::Release);

    assert_eq!(job.step_unit(Nanoseconds(0)), JobStep::Idle);
    assert_eq!(job.target, Some(target));
    assert_eq!(slot.phase.load(Ordering::Acquire), SLOT_COUNTED);
    drop(job);
    io.hold_jobs("reel-walk", false);
    store.close().expect("close");
}

// counts left from a target that ended go nowhere, and their slot comes free
#[test]
fn stale_slot_comes_free() {
    let io = SimulatedIo::new(Seed(41));
    let store = open_workers(&io);
    let mut job = two_blocks(&store, &io);
    let target = job.target.expect("a target");

    let slot = &store.state.volumes[0].walk_slots[1];
    hold(&slot.work).target = WalkTarget {
        segment: SegmentNumber(target.segment.0 + 1_000),
        ..target
    };
    slot.phase.store(SLOT_COUNTED, Ordering::Release);

    assert_eq!(job.step_unit(Nanoseconds(0)), JobStep::Idle);
    assert_eq!(job.target, Some(target));
    assert_ne!(slot.phase.load(Ordering::Acquire), SLOT_COUNTED);
    drop(job);
    io.hold_jobs("reel-walk", false);
    store.close().expect("close");
}

// delete a key in one open segment, by making every other look busy
fn delete_in(store: &Store, number: SegmentNumber) {
    let volume = &store.state.volumes[0];
    let busy = |is_busy: bool| {
        for segment in hold(&volume.claims).open.iter_mut().filter(|segment| segment.number != number) {
            segment.in_flight = if is_busy { BUSY } else { 0 };
        }
    };
    busy(true);
    store.delete(COLUMN, b"key").expect("delete");
    busy(false);
}

fn has_key(store: &Store) -> bool {
    let mut value = Vec::new();
    store.get(COLUMN, b"key", &mut value).expect("get")
}

// a grave over an older value in one segment leaves the key absent, and the older row never reaches the spot cache
#[test]
fn grave_over_a_value_in_one_segment() {
    let io = SimulatedIo::new(Seed(60));
    let store = open(&io);
    store.put(COLUMN, b"key", b"older").expect("put");
    store.delete(COLUMN, b"key").expect("delete");
    let number = hold(&store.state.volumes[0].claims).open[0].number;
    seal_segment(&store, &io, number);
    assert!(!has_key(&store));
    assert!(store.state.spot.lookup(hash_key(COLUMN, b"key")).is_none());
    store.close().expect("close");
}

// a newer grave sealed first from another open segment keeps the older value out when that value's segment seals
#[test]
fn newer_grave_sealed_first() {
    let io = SimulatedIo::new(Seed(61));
    let store = open(&io);
    store.put(COLUMN, b"warm", b"value").expect("put");
    let older = hold(&store.state.volumes[0].claims).open[0].number;
    let newer = add_open(&store, &io);
    put_in(&store, older, b"older");
    delete_in(&store, newer);
    seal_segment(&store, &io, newer);
    assert!(!has_key(&store));
    seal_segment(&store, &io, older);
    assert!(!has_key(&store));
    assert!(store.state.spot.lookup(hash_key(COLUMN, b"key")).is_none());
    store.close().expect("close");
}

// a newer value sealed first stays in the spot cache when the older value's segment seals behind it
#[test]
fn newer_value_sealed_first() {
    let io = SimulatedIo::new(Seed(62));
    let store = open(&io);
    store.put(COLUMN, b"warm", b"value").expect("put");
    let older = hold(&store.state.volumes[0].claims).open[0].number;
    let newer = add_open(&store, &io);
    put_in(&store, older, b"older");
    put_in(&store, newer, b"newer");
    seal_segment(&store, &io, newer);
    seal_segment(&store, &io, older);
    let mut value = Vec::new();
    assert!(store.get(COLUMN, b"key", &mut value).expect("get"));
    assert_eq!(value, b"newer");
    store.close().expect("close");
}

// an older segment of a..d and a newer one of a..c, both walked, so the older is ready for a rewrite and d stays live in it,
// then a third segment of some keys again, sealed with the walk held. Returns the three
fn three_segments(store: &Store, io: &SimulatedIo, third: &[&[u8]]) -> (SegmentNumber, SegmentNumber, SegmentNumber) {
    let volume = &store.state.volumes[0];
    let newest = || *hold(&volume.summary).orders().present().last().expect("a segment");
    for key in [b"a", b"b", b"c", b"d"] {
        store.put(COLUMN, key, b"first").expect("put");
    }
    store.seal_now();
    settle(io);
    let older = newest();
    for key in [b"a", b"b", b"c"] {
        store.put(COLUMN, key, b"second").expect("put");
    }
    store.seal_now();
    settle(io);
    let newer = newest();
    io.hold_jobs(WALK_JOB, true);
    io.hold_jobs("reel-walk", true);
    for key in third {
        store.put(COLUMN, key, b"third").expect("put");
    }
    store.seal_now();
    settle(io);
    (older, newer, newest())
}

// let the workers count what they were handed, and hold them again
fn count_slots(io: &SimulatedIo) {
    io.hold_jobs("reel-walk", false);
    while io.step_any().is_some() {}
    io.hold_jobs("reel-walk", true);
}

// the dead rows a segment's summary row counts, over its columns
fn dead_rows_of(store: &Store, segment: SegmentNumber) -> u64 {
    let rows = hold(&store.state.volumes[0].summary);
    let Some(row) = rows.get(segment) else {
        return 0;
    };
    let mut cursor = row.cursor();
    (0..row.column_count).filter_map(|_column| cursor.column()).map(|column| column.dead_rows.0).sum()
}

// a lane holding one segment's rows by hand, its bits on the heap, maybe already closed to the walk
fn hold_lane(store: &Store, lane: usize, segment: SegmentNumber, is_closed: bool) {
    let volume = &store.state.volumes[0];
    let facts = hold(&volume.summary).get(segment).expect("a row").facts;
    let inputs = || iter::once((facts.block_count, facts.most_block_rows));
    let words = FixedList::filled_on_heap(HeldBits::words_for(inputs()), 0);
    let mut bits = HeldBits::open(iter::once((segment, facts.block_count, facts.most_block_rows)), words);
    bits.is_closed = is_closed;
    let _claim = hold(&volume.claim_lock);
    *hold(&volume.lanes[lane].held) = bits;
    hold(&volume.lanes[lane].inputs)[0] = segment;
    volume.lanes[lane].is_holding.store(true, Ordering::Release);
    volume.claims_changed.fetch_add(1, Ordering::AcqRel);
}

// let a lane go, as a rewrite's end does
fn release_lane(store: &Store, lane: usize) {
    let volume = &store.state.volumes[0];
    let _claim = hold(&volume.claim_lock);
    *hold(&volume.lanes[lane].held) = HeldBits::default();
    hold(&volume.lanes[lane].inputs)[0] = SegmentNumber(0);
    volume.lanes[lane].is_holding.store(false, Ordering::Release);
    volume.claims_changed.fetch_add(1, Ordering::AcqRel);
}

// a block counted before a rewrite began holding counts again, and lands once: its row in the held input goes to a bit, which
// lands nowhere, since the newer row the walk counted also replaces it in the merge
#[test]
fn recounted_block_lands_once() {
    let io = SimulatedIo::new(Seed(62));
    let store = open_workers(&io);
    let state = &store.state;
    let volume = &state.volumes[0];
    let (older, newer, third) = three_segments(&store, &io, &[b"d"]);

    let mut job = WalkJob::new(state.clone(), 0);
    job.step_unit(io.clock_reading());
    count_slots(&io);
    let mut merge = CompactJob::new(state.clone(), 0);
    while !merge.rewrite.start() {
        assert!(merge.rewrite.has_looks_left, "no rewrite started");
    }
    assert_eq!(merge.rewrite.input_numbers(), [older]);
    let blocks = volume.walk_blocks.load(Ordering::Relaxed);
    assert_eq!(job.step_unit(io.clock_reading()), JobStep::Worked);
    assert_eq!(volume.walk_blocks.load(Ordering::Relaxed), blocks, "a block counted before the hold landed");
    count_slots(&io);
    assert_eq!(job.step_unit(io.clock_reading()), JobStep::Worked);
    assert_eq!(volume.walk_blocks.load(Ordering::Relaxed), blocks + 1);
    assert_eq!(hold(&volume.lanes[0].held).marked, 1);

    while merge.rewrite.is_running() {
        merge.rewrite.step();
    }
    assert_eq!(hold(&volume.summary).orders().present(), [newer, third], "the merge kept the older d");
    assert_eq!(dead_rows_of(&store, newer) + dead_rows_of(&store, third), 0, "the held row landed somewhere");
    drop(job);
    io.hold_jobs(WALK_JOB, false);
    io.hold_jobs("reel-walk", false);
    store.close().expect("close");
}

// a block with held rows in two lanes, one closed, waits and marks neither, and lands none of its counts; after the release
// it counts again and lands each row once
#[test]
fn waiting_block_marks_no_lane() {
    let io = SimulatedIo::new(Seed(63));
    let store = open_workers(&io);
    let state = &store.state;
    let volume = &state.volumes[0];
    let (older, newer, _third) = three_segments(&store, &io, &[b"c", b"d"]);
    // the open lane holds c, which comes first in the block, and the closed one holds d
    hold_lane(&store, 1, newer, false);
    hold_lane(&store, 2, older, true);
    let blocks = volume.walk_blocks.load(Ordering::Relaxed);
    let older_dead = dead_rows_of(&store, older);

    let mut job = WalkJob::new(state.clone(), 0);
    job.step_unit(io.clock_reading());
    count_slots(&io);
    assert_eq!(job.step_unit(io.clock_reading()), JobStep::Idle);
    assert_eq!(hold(&volume.lanes[1].held).marked, 0, "a waiting block marked a lane");
    assert_eq!(volume.walk_blocks.load(Ordering::Relaxed), blocks, "a waiting block landed its counts");
    assert_eq!(volume.walk_claim_waits.load(Ordering::Relaxed), 1);
    assert_eq!(job.step_unit(io.clock_reading()), JobStep::Idle, "a wait counted again before the release");

    release_lane(&store, 2);
    assert_eq!(job.step_unit(io.clock_reading()), JobStep::Worked);
    count_slots(&io);
    assert_eq!(job.step_unit(io.clock_reading()), JobStep::Worked);
    assert_eq!(volume.walk_blocks.load(Ordering::Relaxed), blocks + 1);
    assert_eq!(hold(&volume.lanes[1].held).marked, 1);
    assert_eq!(dead_rows_of(&store, older), older_dead + 1);
    release_lane(&store, 1);
    drop(job);
    io.hold_jobs(WALK_JOB, false);
    io.hold_jobs("reel-walk", false);
    store.close().expect("close");
}

// a rewrite that begins holding while a block lands waits for the landing, so the block's counts land before the hold and none
// lands on an input after its rewrite has begun to decide
#[test]
fn claim_waits_for_a_landing_block() {
    let io = SimulatedIo::new(Seed(64));
    let store = open_workers(&io);
    let state = &store.state;
    let volume = &state.volumes[0];
    let (older, _newer, _third) = three_segments(&store, &io, &[b"d"]);
    let mut merge = CompactJob::new(state.clone(), 0);
    while !merge.rewrite.claim() {
        assert!(merge.rewrite.has_looks_left, "no rewrite claimed");
    }
    assert_eq!(merge.rewrite.input_numbers(), [older]);
    let blocks = volume.walk_blocks.load(Ordering::Relaxed);
    let older_dead = dead_rows_of(&store, older);

    let mut job = WalkJob::new(state.clone(), 0);
    job.step_unit(io.clock_reading());
    count_slots(&io);
    // the landing stops at the summary, inside the claim lock
    let summary = hold(&volume.summary);
    thread::scope(|scope| {
        let landing = scope.spawn(|| job.step_unit(io.clock_reading()));
        let started = Instant::now();
        while volume.claim_lock.try_lock().is_ok() {
            assert!(started.elapsed() < LOCK_WAIT, "the landing never took the claim lock");
            thread::yield_now();
        }
        let begin = scope.spawn(|| merge.rewrite.begin());
        thread::sleep(BEGIN_WAIT);
        assert!(!volume.lanes[0].is_holding.load(Ordering::Acquire), "a rewrite began holding inside a landing");
        drop(summary);
        assert_eq!(landing.join().expect("landing"), JobStep::Worked);
        assert!(begin.join().expect("begin"));
    });
    assert_eq!(volume.walk_blocks.load(Ordering::Relaxed), blocks + 1);
    assert_eq!(dead_rows_of(&store, older), older_dead + 1, "the block's charge went other than to the input before the hold");
    assert_eq!(hold(&volume.lanes[0].held).marked, 0);
    assert!(volume.lanes[0].is_holding.load(Ordering::Acquire));
    while merge.rewrite.is_running() {
        merge.rewrite.step();
    }
    drop(job);
    io.hold_jobs(WALK_JOB, false);
    io.hold_jobs("reel-walk", false);
    store.close().expect("close");
}

// a free the walk finds on a segment a rewrite has claimed is dropped, so the record stays whole for the rewrite's copy
#[test]
fn free_on_a_claimed_segment_is_dropped() {
    let io = SimulatedIo::new(Seed(65));
    let store = open(&io);
    let state = &store.state;
    let volume = &state.volumes[0];
    let large = vec![7; FREE_IN_PLACE_BYTES.0 as usize * 2];
    store.put(COLUMN, b"large", &large).expect("put");
    store.seal_now();
    settle(&io);
    let older = *hold(&volume.summary).orders().present().last().expect("a segment");
    io.hold_jobs(WALK_JOB, true);
    store.put(COLUMN, b"large", b"small").expect("put");
    store.seal_now();
    settle(&io);
    // a rewrite claims the older segment and hasn't begun, so the walk charges it directly
    hold(&volume.lanes[0].inputs)[0] = older;
    let freed = volume.bytes_freed.load(Ordering::Acquire);

    let mut job = WalkJob::new(state.clone(), 0);
    assert_eq!(job.step_unit(io.clock_reading()), JobStep::Worked);
    assert_eq!(dead_rows_of(&store, older), 1);
    assert_eq!(volume.bytes_freed.load(Ordering::Acquire), freed, "a free cut a record a rewrite claimed");
    hold(&volume.lanes[0].inputs)[0] = SegmentNumber(0);
    drop(job);
    io.hold_jobs(WALK_JOB, false);
    store.close().expect("close");
}

const CURSOR_SEEDS: u64 = 6;
const CURSOR_SEGMENTS_WRITTEN: u64 = 40;
const CURSOR_PUTS: u64 = 3_000;
const CURSOR_KEYS: u64 = 40_000;
const CURSOR_KEY_BYTES: usize = 200;

// overwrites spread over many segments, half of them under keys whose first 16 bytes past the shared ones match, walked with
// the cursor and through the table alone: every segment ends with the same counts. In the second pair the walk starts once
// every segment is sealed, and merges claim and retire segments between its blocks
#[test]
fn cursor_counts_as_the_table_does() {
    for seed in 0..CURSOR_SEEDS {
        let (table, _none) = cursor_arm(seed, true, false);
        let (cursor, lookups) = cursor_arm(seed, false, false);
        assert_eq!(cursor, table, "seed {seed}: the cursor counted differently");
        assert!(table.iter().any(|(_segment, _rows, dead)| *dead > 0), "seed {seed}: nothing counted dead");
        assert!(lookups[0] > 0 && lookups[2] > 0, "seed {seed}: the cursor answered nothing: {lookups:?}");
        let (merged_table, _none) = cursor_arm(seed, true, true);
        let (merged_cursor, _lookups) = cursor_arm(seed, false, true);
        assert_eq!(merged_cursor, merged_table, "seed {seed}: the cursor counted differently under merges");
        assert!(merged_table.len() < table.len(), "seed {seed}: nothing merged");
    }
}

fn cursor_arm(seed: u64, is_cursor_off: bool, is_merging: bool) -> (Vec<(u64, u64, u64)>, [u64; 4]) {
    let io = SimulatedIo::new(Seed(seed));
    let store = open(&io);
    let volume = &store.state.volumes[0];
    volume.holds.no_cursor.store(is_cursor_off, Ordering::Release);
    volume.holds.small_bound.store(true, Ordering::Release);
    volume.holds.walk.store(is_merging, Ordering::Release);
    let mut random = SeededRandom::new(Seed(seed));
    for _segment in 0..CURSOR_SEGMENTS_WRITTEN {
        for _put in 0..CURSOR_PUTS {
            let number = random.below(CURSOR_KEYS);
            let key = match number % 2 {
                0 => format!("k{number:05}"),
                _odd => format!("shared-stem-0123456789-{number:05}"),
            };
            let mut key = key.into_bytes();
            key.resize(CURSOR_KEY_BYTES, (number % 251) as u8);
            store.put(COLUMN, &key, &random.below(u64::MAX).to_le_bytes()).expect("put");
        }
        store.seal_now();
        settle(&io);
    }
    if is_merging {
        volume.holds.walk.store(false, Ordering::Release);
        let mut job = CompactJob::new(Arc::clone(&store.state), 0);
        for step in 0..CURSOR_MERGE_STEPS {
            if io.step_any().is_none() {
                io.advance_clock(TICK);
            }
            if step % CURSOR_MERGE_EVERY == 0 {
                if job.rewrite.is_running() {
                    job.rewrite.step();
                } else {
                    job.rewrite.start();
                }
            }
        }
        while job.rewrite.is_running() {
            job.rewrite.step();
        }
    }
    settle(&io);
    let counts = hold(&volume.summary).rows().map(|row| {
        let column = row.cursor().column().expect("a column");
        (row.facts.segment.0, column.row_count.0, column.dead_rows.0)
    }).collect();
    let lookups = store.state.cursor_lookups.each_ref().map(|count| count.load(Ordering::Acquire));
    store.close().expect("close");
    (counts, lookups)
}

const CURSOR_MERGE_STEPS: u64 = 4_000;
const CURSOR_MERGE_EVERY: u64 = 3;

// a cursor holds no more than the line the budget plans for it
#[test]
fn cursor_fits_its_plan() {
    let cursor: KeyCursor<SimulatedFile> = KeyCursor::new();
    assert!(cursor.heap_bytes().0 <= CURSOR_BYTES, "{:?} past {CURSOR_BYTES}", cursor.heap_bytes());
}
