//! The dead-byte walk: every superseded row counts once, whatever order segments seal and walk in

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::engine_state::IndexWait;
use reel::engine::open_segment::SegmentPhase;
use reel::engine::planner::Planner;
use reel::engine::reel_store::ReelStore;
use reel::engine::walk_job::WalkJob;
use reel::guard::hold;
use reel::io::IoTask;
use reel::io::job::{Job, JobStep};
use reel::record::codec::Codec;
use reel::segment::segment_tail::SegmentNumber;
use reel::segment::summary::{LostRows, SummaryPaths, SummaryRows, copy_ends, read_summary, write_copy};
use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, Nanoseconds, RowCount, VolumeId, WritePlace};
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_disk::OperationCount;
use reel_sim::simulated_io::SimulatedIo;

const ROOT: &str = "/disk/walk";
const COLUMN: ColumnId = ColumnId(1);
const SECOND_COLUMN: ColumnId = ColumnId(2);
const SECOND_ROOT: &str = "/disk/walk-second";
const BUDGET: ByteCount = ByteCount(128 * 1024 * 1024);
const LARGE_BUDGET: ByteCount = ByteCount(512 * 1024 * 1024);
const WALK_JOB: &str = "reel-planner";
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

type Store = ReelStore<SimulatedIo>;

fn config(budget: ByteCount) -> StoreConfig {
    StoreConfig {
        memory_budget: budget,
        volumes: vec![VolumeConfig {
            name: "walk".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Variable { most: LONG_KEY_BYTES as u16 + 8 },
            codec: Codec::None,
            volume: VolumeId(0),
        }],
    }
}

fn open(io: &SimulatedIo) -> Store {
    ReelStore::open(io.clone(), &config(BUDGET)).expect("open")
}

// a key's rows all sit in one shard, so a long run of them needs a large index
fn open_large(io: &SimulatedIo) -> Store {
    ReelStore::open(io.clone(), &config(LARGE_BUDGET)).expect("open")
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
        let ends = copy_ends(WritePlace(mark.0 + 1), rows.bytes());
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
    let ends = copy_ends(WritePlace(mark.0 + 1), rows.bytes());
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
    });
    config.columns.push(ColumnConfig {
        id: SECOND_COLUMN,
        name: "second".to_string(),
        key_shape: KeyShape::Variable { most: 64 },
        codec: Codec::None,
        volume: VolumeId(1),
    });
    let store = ReelStore::open(io.clone(), &config).expect("open");
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
        last: 0,
    };

    assert_eq!(planner.step(), JobStep::Worked);

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
    let ends = copy_ends(WritePlace(mark.0 + 1), kept.bytes());
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
