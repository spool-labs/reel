//! Range deletes reach the disk: what a cover hides is counted once, leaves at a merge or an unlink, and the cover goes after it

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::compact_job::CompactJob;
use reel::engine::open_segment::data_path;
use reel::engine::reel_store::ReelStore;
use reel::engine::rewrite_job::{RewritePhase, has_settled_covers};
use reel::engine::walk_job::{WalkJob, WalkTarget};
use reel::guard::hold;
use reel::record::codec::Codec;
use reel::segment::range_covers::MOST_RANGE_COVERS;
use reel::segment::segment_tail::SegmentNumber;
use reel::io::Io;
use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, Nanoseconds, VolumeId};
use reel_api::write_batch::WriteBatch;
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_io::SimulatedIo;

const ROOT: &str = "/disk/range";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(64 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 400_000;
const KEYS: u64 = 40;
const WHOLE_KEYS: u64 = 14_000;
const LIVE_KEYS: u64 = 50_000;
const MOST_WHOLE_READS: u64 = 40;
const MOST_CRASH_POINTS: u64 = 200;

type Store = ReelStore<SimulatedIo>;

fn open(io: &SimulatedIo) -> Store {
    let config = StoreConfig {
        memory_budget: BUDGET,
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "range".to_string(),
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
            sorted_rows: None,
            least_value: None,
        }],
    };
    let store = ReelStore::open(io.clone(), &config).expect("open");
    // these segments keep the few dead rows they were built with
    for volume in &store.state.volumes {
        volume.holds.reclaim.store(true, Ordering::Release);
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

fn hold_compaction(store: &Store, is_held: bool) {
    store.state.volumes[0].holds.compaction.store(is_held, Ordering::Release);
    store.state.ask_compaction();
}

fn put_keys(store: &Store, prefix: &str, value: &[u8]) {
    for number in 0..KEYS {
        store.put(COLUMN, format!("{prefix}-{number:03}").as_bytes(), value).expect("put");
    }
}

fn dead_rows(store: &Store, number: SegmentNumber) -> u64 {
    let rows = hold(&store.state.volumes[0].summary);
    rows.get(number).expect("a row").cursor().column().expect("a column").dead_rows.0
}

fn pending(store: &Store) -> usize {
    store.state.volumes[0].covers.len()
}

// a segment wholly inside a range goes with no row read, and the cover goes after it
#[test]
fn whole_segment_unlinks() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io);
    put_keys(&store, "m", b"value");
    let older = seal(&store, &io);
    store.delete_range(COLUMN, b"m", b"n").expect("delete");
    put_keys(&store, "z", b"value");
    seal(&store, &io);

    settle(&io);
    assert!(!store.state.volumes[0].table.holds(older));
    assert_eq!(pending(&store), 0);
    assert_eq!(read(&store, b"m-000"), None);
    assert_eq!(read(&store, b"z-000"), Some(b"value".to_vec()));
    store.close().expect("close");

    let store = open(&io);
    assert_eq!(pending(&store), 0);
    assert_eq!(read(&store, b"m-000"), None);
    store.close().expect("close");
}

// a segment partly under a range has just those rows counted, whichever came first
#[test]
fn rows_inside_count() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io);
    hold_compaction(&store, true);
    put_keys(&store, "a", b"value");
    put_keys(&store, "m", b"value");
    put_keys(&store, "z", b"value");
    let older = seal(&store, &io);
    store.delete_range(COLUMN, b"m", b"n").expect("delete");
    store.put(COLUMN, b"zz", b"value").expect("put");
    seal(&store, &io);

    assert_eq!(dead_rows(&store, older), KEYS);
    assert_eq!(read(&store, b"a-000"), Some(b"value".to_vec()));
    assert_eq!(read(&store, b"m-000"), None);
    store.close().expect("close");
}

// a walk that comes after the cover counts a row under it, once
#[test]
fn walk_counts_a_later_segment() {
    let io = SimulatedIo::new(Seed(3));
    let store = open(&io);
    hold_compaction(&store, true);
    let volume = &store.state.volumes[0];
    volume.holds.walk.store(true, Ordering::Release);
    put_keys(&store, "a", b"value");
    put_keys(&store, "m", b"value");
    let older = seal(&store, &io);
    store.delete_range(COLUMN, b"m", b"n").expect("delete");
    store.put(COLUMN, b"zz", b"value").expect("put");
    let cover_segment = seal(&store, &io);

    walk_one(&store, &io, cover_segment);
    assert_eq!(dead_rows(&store, older), 0);
    walk_one(&store, &io, older);
    assert_eq!(dead_rows(&store, older), KEYS);
    assert_eq!(dead_rows(&store, cover_segment), 0);
    store.close().expect("close");
}

// walk one segment by hand, so a test can pick the order
fn walk_one(store: &Store, io: &SimulatedIo, segment: SegmentNumber) {
    let mut walk = WalkJob::new(Arc::clone(&store.state), 0);
    let rows = hold(&store.state.volumes[0].summary);
    let block_count = rows.get(segment).expect("a row").facts.block_count;
    drop(rows);
    walk.target = Some(WalkTarget {
        segment,
        next_block: 0,
        block_count,
        is_backward: false,
        is_damaged: false,
    });
    while walk.target.is_some() {
        walk.step_unit(io.now());
        let _stepped = io.step_any();
        io.advance_clock(TICK);
    }
}

// two covers over one row and a newer write of its key count it once
#[test]
fn counted_once() {
    let io = SimulatedIo::new(Seed(4));
    let store = open(&io);
    hold_compaction(&store, true);
    put_keys(&store, "a", b"value");
    put_keys(&store, "m", b"value");
    let older = seal(&store, &io);
    store.delete_range(COLUMN, b"l", b"mz").expect("delete");
    store.put(COLUMN, b"zz", b"value").expect("put");
    seal(&store, &io);
    store.delete_range(COLUMN, b"m", b"n").expect("delete");
    store.put(COLUMN, b"m-000", b"newer").expect("put");
    seal(&store, &io);

    assert_eq!(dead_rows(&store, older), KEYS);
    assert_eq!(read(&store, b"m-000"), Some(b"newer".to_vec()));
    assert_eq!(read(&store, b"m-001"), None);
    store.close().expect("close");
}

// a merge drops what a cover hides, and a row written after the cover stays
#[test]
fn merge_drops_under_a_cover() {
    let io = SimulatedIo::new(Seed(5));
    let store = open(&io);
    hold_compaction(&store, true);
    put_keys(&store, "a", b"value");
    put_keys(&store, "m", b"value");
    let older = seal(&store, &io);
    store.delete_range(COLUMN, b"a", b"n").expect("delete");
    store.put(COLUMN, b"a-000", b"newer").expect("put");
    seal(&store, &io);

    let volume = &store.state.volumes[0];
    assert_eq!(dead_rows(&store, older), 2 * KEYS);
    hold_compaction(&store, false);
    settle(&io);
    assert!(!volume.table.holds(older));
    assert_eq!(read(&store, b"a-000"), Some(b"newer".to_vec()));
    assert_eq!(read(&store, b"a-001"), None);
    store.close().expect("close");
}

// a cue below a cover keeps the cover and the rows it can still read, and the cover goes with the cue
#[test]
fn cue_keeps_a_cover() {
    let io = SimulatedIo::new(Seed(6));
    let store = open(&io);
    put_keys(&store, "m", b"value");
    let older = seal(&store, &io);
    let cue = store.cue().expect("cue");
    store.delete_range(COLUMN, b"m", b"n").expect("delete");
    put_keys(&store, "z", b"value");
    seal(&store, &io);
    settle(&io);

    assert!(store.state.volumes[0].table.holds(older));
    assert_eq!(pending(&store), 1);
    let mut buf = Vec::new();
    assert!(cue.get(COLUMN, b"m-000", &mut buf).expect("get"));

    drop(cue);
    settle(&io);
    assert!(!store.state.volumes[0].table.holds(older));
    assert_eq!(pending(&store), 0);
    store.close().expect("close");
}

// a cover stays while a segment holds a row under it, and a segment that only spans its range doesn't hold it back
#[test]
fn cover_waits_for_its_rows() {
    let io = SimulatedIo::new(Seed(7));
    let store = open(&io);
    hold_compaction(&store, true);
    put_keys(&store, "a", b"value");
    put_keys(&store, "z", b"value");
    let spanning = seal(&store, &io);
    store.delete_range(COLUMN, b"m", b"n").expect("delete");
    store.put(COLUMN, b"zz", b"value").expect("put");
    seal(&store, &io);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    let _has_settled = job.settle_covers(&store.state.volumes[0]);
    assert_eq!(pending(&store), 0);
    assert!(store.state.volumes[0].table.holds(spanning));
    store.close().expect("close");
}

// a row under a cover in a segment no merge wants holds the cover back
#[test]
fn row_under_keeps_a_cover() {
    let io = SimulatedIo::new(Seed(8));
    let store = open(&io);
    hold_compaction(&store, true);
    put_keys(&store, "a", b"value");
    store.put(COLUMN, b"m", b"value").expect("put");
    put_keys(&store, "z", b"value");
    seal(&store, &io);
    store.delete_range(COLUMN, b"m", b"n").expect("delete");
    store.put(COLUMN, b"zz", b"value").expect("put");
    seal(&store, &io);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    let _has_settled = job.settle_covers(&store.state.volumes[0]);
    assert_eq!(pending(&store), 1);
    store.close().expect("close");
}

// a merge past a dropped cover waits for the newer one to seal: a crash could still lose that one, and the older one's rows with it
#[test]
fn dropped_cover_waits_for_the_newer() {
    let io = SimulatedIo::new(Seed(9));
    let store = open(&io);
    hold_compaction(&store, true);
    let volume = &store.state.volumes[0];
    volume.holds.walk.store(true, Ordering::Release);
    store.delete_range(COLUMN, b"m", b"n").expect("delete");
    store.put(COLUMN, b"a", b"value").expect("put");
    let older = seal(&store, &io);
    store.delete_range(COLUMN, b"l", b"o").expect("delete");
    settle(&io);
    assert_eq!(pending(&store), 1);
    volume.holds.walk.store(false, Ordering::Release);
    store.state.ask_compaction();
    settle(&io);

    let rows = hold(&volume.summary);
    let row = rows.get(older).expect("a row");
    assert!(row.facts.is_walked() && row.facts.has_covers);
    assert!(!has_settled_covers(volume, &row));
    drop(rows);
    store.close().expect("close");
}

// a segment wholly under a range reads no row block, whatever it holds
#[test]
fn whole_segment_reads_no_rows() {
    let io = SimulatedIo::new(Seed(10));
    let store = open(&io);
    hold_compaction(&store, true);
    for number in 0..WHOLE_KEYS {
        store.put(COLUMN, format!("m-{number:06}").as_bytes(), b"value").expect("put");
    }
    let older = seal(&store, &io);
    let reads_before = io.read_count();
    store.delete_range(COLUMN, b"m", b"n").expect("delete");
    store.put(COLUMN, b"zz", b"value").expect("put");
    seal(&store, &io);

    assert_eq!(dead_rows(&store, older), WHOLE_KEYS);
    assert!(io.read_count().0 - reads_before.0 < MOST_WHOLE_READS);
    store.close().expect("close");
}

// a batch's row under its own range delete leaves with the cover, and a read never sees it in between
#[test]
fn row_under_its_own_cover() {
    let io = SimulatedIo::new(Seed(11));
    let store = open(&io);
    let mut batch = WriteBatch::new();
    batch.put(COLUMN, b"m-1", b"overwritten");
    batch.delete_range(COLUMN, b"m", b"n");
    store.write_batch(&batch).expect("batch");
    let segment = seal(&store, &io);
    settle(&io);

    assert_eq!(read(&store, b"m-1"), None);
    assert_eq!(pending(&store), 0);
    assert!(!store.state.volumes[0].table.holds(segment));
    store.close().expect("close");

    let store = open(&io);
    assert_eq!(read(&store, b"m-1"), None);
    store.close().expect("close");
}

// an applied cover stays until a newer one over it is applied too, or the newer one's apply would count its rows again
#[test]
fn wider_cover_counts_once() {
    let io = SimulatedIo::new(Seed(12));
    let store = open(&io);
    hold_compaction(&store, true);
    put_keys(&store, "a", b"value");
    put_keys(&store, "m", b"value");
    let older = seal(&store, &io);
    store.delete_range(COLUMN, b"m", b"n").expect("delete");
    store.put(COLUMN, b"zz", b"value").expect("put");
    seal(&store, &io);
    assert_eq!(dead_rows(&store, older), KEYS);

    store.delete_range(COLUMN, b"l", b"o").expect("delete");
    settle(&io);
    assert_eq!(pending(&store), 2);
    store.put(COLUMN, b"zz", b"newer").expect("put");
    seal(&store, &io);
    settle(&io);
    assert_eq!(dead_rows(&store, older), KEYS);
    hold_compaction(&store, false);
    settle(&io);
    assert!(pending(&store) < 2);
    store.close().expect("close");
}

// a pool past half sends the segments under its oldest covers to a rewrite below the trigger, so a full pool drains
#[test]
fn full_pool_drains() {
    let io = SimulatedIo::new(Seed(13));
    let store = open(&io);
    hold_compaction(&store, true);
    for number in 0..MOST_RANGE_COVERS {
        store.put(COLUMN, format!("k-{number:05}").as_bytes(), b"value").expect("put");
    }
    for number in 0..LIVE_KEYS {
        store.put(COLUMN, format!("l-{number:06}").as_bytes(), b"value").expect("put");
    }
    let older = seal(&store, &io);
    for number in 0..MOST_RANGE_COVERS {
        let (start, end) = (format!("k-{number:05}"), format!("k-{number:05}z"));
        store.delete_range(COLUMN, start.as_bytes(), end.as_bytes()).expect("delete");
    }
    let refused = store.delete_range(COLUMN, b"x", b"y");
    assert!(matches!(refused, Err(StoreError::RangeDeletesFull { .. })));
    seal(&store, &io);
    assert!(dead_rows(&store, older) > 0);

    hold_compaction(&store, false);
    settle(&io);
    assert!(pending(&store) * 2 <= MOST_RANGE_COVERS);
    store.delete_range(COLUMN, b"x", b"y").expect("delete");
    assert_eq!(read(&store, b"l-000000"), Some(b"value".to_vec()));
    assert_eq!(read(&store, b"k-00000"), None);
    store.close().expect("close");
}

// a newer row beside a cover in one segment has already counted the row under both, so the cover's apply leaves it
#[test]
fn newer_row_beside_the_cover_counts_once() {
    let io = SimulatedIo::new(Seed(14));
    let store = open(&io);
    hold_compaction(&store, true);
    put_keys(&store, "a", b"value");
    put_keys(&store, "m", b"value");
    let older = seal(&store, &io);
    store.delete_range(COLUMN, b"m", b"n").expect("delete");
    store.put(COLUMN, b"m-000", b"newer").expect("put");
    seal(&store, &io);

    assert_eq!(dead_rows(&store, older), KEYS);
    assert_eq!(read(&store, b"m-000"), Some(b"newer".to_vec()));
    store.close().expect("close");
}

// a segment wholly under a cover stays until the cover is applied: it holds the row that says an older one is already counted
#[test]
fn unlink_waits_for_the_apply() {
    let io = SimulatedIo::new(Seed(15));
    let store = open(&io);
    hold_compaction(&store, true);
    let volume = &store.state.volumes[0];
    put_keys(&store, "a", b"value");
    put_keys(&store, "m", b"value");
    let older = seal(&store, &io);
    put_keys(&store, "m", b"newer");
    let newer = seal(&store, &io);
    assert_eq!(dead_rows(&store, older), KEYS);
    store.delete_range(COLUMN, b"m", b"n").expect("delete");
    store.put(COLUMN, b"zz", b"value").expect("put");
    let cover_segment = hold(&volume.claims).open[0].number;
    volume.holds.walk.store(true, Ordering::Release);
    store.seal_now();
    settle(&io);

    let rows = hold(&volume.summary);
    let block_count = rows.get(cover_segment).expect("a row").facts.block_count;
    drop(rows);
    let mut walk = WalkJob::new(Arc::clone(&store.state), 0);
    walk.target = Some(WalkTarget {
        segment: cover_segment,
        next_block: 0,
        block_count,
        is_backward: false,
        is_damaged: false,
    });
    while walk.apply.segment.is_none() {
        walk.step_unit(io.now());
        let _stepped = io.step_any();
        io.advance_clock(TICK);
    }
    let compaction = CompactJob::new(Arc::clone(&store.state), 0);
    for _step in 0..KEYS {
        walk.step_unit(io.now());
        if walk.apply.segment.is_none() {
            break;
        }
        assert_eq!(compaction.pick_dead(volume), None);
    }
    while walk.target.is_some() {
        walk.step_unit(io.now());
    }
    assert_eq!(dead_rows(&store, newer), KEYS);
    assert_eq!(dead_rows(&store, older), KEYS);
    assert_eq!(compaction.pick_dead(volume), Some(newer));
    store.close().expect("close");
}

// a cover part way through its apply has counted rows, so a newer one that isn't applied leaves it in the pool
#[test]
fn counting_cover_stays() {
    let io = SimulatedIo::new(Seed(21));
    let store = open(&io);
    hold_compaction(&store, true);
    let volume = &store.state.volumes[0];
    put_keys(&store, "a", b"value");
    put_keys(&store, "m", b"value");
    let older = seal(&store, &io);
    store.delete_range(COLUMN, b"m", b"n").expect("delete");
    store.put(COLUMN, b"zz", b"value").expect("put");
    let cover_segment = hold(&volume.claims).open[0].number;
    volume.holds.walk.store(true, Ordering::Release);
    store.seal_now();
    settle(&io);

    let rows = hold(&volume.summary);
    let block_count = rows.get(cover_segment).expect("a row").facts.block_count;
    drop(rows);
    let mut walk = WalkJob::new(Arc::clone(&store.state), 0);
    walk.target = Some(WalkTarget {
        segment: cover_segment,
        next_block: 0,
        block_count,
        is_backward: false,
        is_damaged: false,
    });
    while dead_rows(&store, older) < KEYS {
        walk.step_unit(io.now());
        let _stepped = io.step_any();
        io.advance_clock(TICK);
    }
    assert!(walk.apply.segment.is_some());
    store.delete_range(COLUMN, b"l", b"o").expect("delete");
    assert_eq!(pending(&store), 2);
    store.close().expect("close");
}

// a rebuilt row's backward walk leaves what its cover's apply already counted
#[test]
fn rebuilt_cover_counts_once() {
    let io = SimulatedIo::new(Seed(22));
    let store = open(&io);
    hold_compaction(&store, true);
    put_keys(&store, "a", b"value");
    put_keys(&store, "m", b"value");
    let older = seal(&store, &io);
    store.delete_range(COLUMN, b"m", b"n").expect("delete");
    store.put(COLUMN, b"zz", b"value").expect("put");
    let cover_segment = seal(&store, &io);
    assert_eq!(dead_rows(&store, older), KEYS);

    let volume = &store.state.volumes[0];
    hold(&volume.summary).remove(cover_segment);
    store.state.save_summary(volume).expect("save");
    io.kill();
    drop(store);

    let store = open(&io);
    hold_compaction(&store, true);
    settle(&io);
    assert_eq!(dead_rows(&store, older), KEYS);
    assert_eq!(read(&store, b"m-000"), None);
    store.close().expect("close");
}

// a segment whose summary row went for room still holds a cover back, or the rows the cover hides there would come back
#[test]
fn row_gone_keeps_a_cover() {
    let io = SimulatedIo::new(Seed(23));
    let store = open(&io);
    hold_compaction(&store, true);
    let volume = &store.state.volumes[0];
    put_keys(&store, "m", b"value");
    let older = seal(&store, &io);
    store.delete_range(COLUMN, b"m", b"n").expect("delete");
    store.put(COLUMN, b"zz", b"value").expect("put");
    seal(&store, &io);

    hold(&volume.summary).remove(older);
    hold_compaction(&store, false);
    settle(&io);
    assert_eq!(pending(&store), 1);
    assert_eq!(read(&store, b"m-000"), None);
    store.close().expect("close");
}

// a settled cover costs its segment no rewrite, and open leaves it out of the pool
#[test]
fn settled_cover_stays_put() {
    let io = SimulatedIo::new(Seed(16));
    let store = open(&io);
    put_keys(&store, "a", b"value");
    let older = seal(&store, &io);
    store.delete_range(COLUMN, b"a-000", b"a-001").expect("delete");
    put_keys(&store, "z", b"value");
    let holder = seal(&store, &io);
    settle(&io);
    let volume = &store.state.volumes[0];
    assert_eq!(pending(&store), 1);

    // the row under it goes at its own segment's rewrite, and the holder stays as it was
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    job.rewrite.forced = Some(older);
    job.rewrite.start();
    while job.rewrite.is_running() {
        job.rewrite.step();
    }
    store.state.ask_compaction();
    settle(&io);
    assert_eq!(pending(&store), 0);
    assert!(volume.table.holds(holder));
    assert!(!hold(&volume.summary).get(holder).expect("a row").facts.has_covers);
    store.close().expect("close");

    let store = open(&io);
    assert_eq!(pending(&store), 0);
    // the row stays this tail's, with its counts and its walk, or open would rebuild it and lose them
    let rows = hold(&store.state.volumes[0].summary);
    assert!(rows.get(holder).expect("a row").facts.is_walked());
    drop(rows);
    assert_eq!(read(&store, b"a-000"), None);
    assert_eq!(read(&store, b"z-000"), Some(b"value".to_vec()));
    store.close().expect("close");
}

// an install saves the summary without its inputs before it parks them, and a cover can't settle over an input file still on
// disk, or a crash or a view would find the file with no row and no cover
#[test]
fn cover_waits_for_a_leaving_input() {
    let io = SimulatedIo::new(Seed(24));
    let store = open(&io);
    put_keys(&store, "a", b"value");
    let older = seal(&store, &io);
    store.delete_range(COLUMN, b"a-000", b"a-001").expect("delete");
    put_keys(&store, "z", b"value");
    let holder = seal(&store, &io);
    settle(&io);
    let volume = &store.state.volumes[0];
    assert_eq!(pending(&store), 1);

    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    job.rewrite.forced = Some(older);
    job.rewrite.start();
    while job.rewrite.phase != RewritePhase::Installing {
        job.rewrite.step();
    }
    let mut planner = CompactJob::new(Arc::clone(&store.state), 0);
    job.rewrite
        .install_with(|| {}, || {
            assert!(hold(&volume.summary).get(older).is_none());
            let data = data_path(&PathBuf::from(ROOT), older).to_path_buf();
            assert!(io.list_directory(&PathBuf::from(ROOT)).expect("list").contains(&data));
            assert!(!planner.settle_covers(volume));
            assert_eq!(pending(&store), 1);
        })
        .expect("install");
    store.state.ask_compaction();
    settle(&io);
    assert_eq!(pending(&store), 0);
    assert!(!hold(&volume.summary).get(holder).expect("a row").facts.has_covers);
    assert_eq!(read(&store, b"a-000"), None);
    store.close().expect("close");
}

// a crash anywhere from the input's unlink to the settle's summary save leaves the range deleted after a reopen
#[test]
fn leaving_input_crash_points() {
    for crash_at in 0..MOST_CRASH_POINTS {
        let io = SimulatedIo::new(Seed(25));
        let store = open(&io);
        put_keys(&store, "a", b"value");
        let older = seal(&store, &io);
        store.delete_range(COLUMN, b"a-000", b"a-001").expect("delete");
        put_keys(&store, "z", b"value");
        seal(&store, &io);
        settle(&io);
        let mut job = CompactJob::new(Arc::clone(&store.state), 0);
        job.rewrite.forced = Some(older);
        job.rewrite.start();
        while job.rewrite.is_running() {
            job.rewrite.step();
        }
        drop(job);

        io.arm_crash(reel_sim::simulated_disk::OperationCount(crash_at));
        store.state.ask_compaction();
        settle(&io);
        let is_done = !io.is_crashed();
        io.power_cut();
        drop(store);

        let store = open(&io);
        settle(&io);
        assert_eq!(read(&store, b"a-000"), None, "crash at {crash_at}");
        assert_eq!(read(&store, b"z-000"), Some(b"value".to_vec()), "crash at {crash_at}");
        store.close().expect("close");
        if is_done {
            return;
        }
    }
    panic!("the settle never ran to the end");
}

// a segment that held nothing but a settled cover goes like any other dead segment
#[test]
fn cover_alone_goes() {
    let io = SimulatedIo::new(Seed(17));
    let store = open(&io);
    store.delete_range(COLUMN, b"a", b"b").expect("delete");
    let holder = seal(&store, &io);
    settle(&io);

    assert_eq!(pending(&store), 0);
    assert!(!store.state.volumes[0].table.holds(holder));
    store.close().expect("close");
}

// a range delete whose walk has settled its cover still hides every row under it after a reopen, whether or not a merge has
// taken those rows off the disk yet
#[test]
fn settled_cover_hides_after_reopen() {
    let io = SimulatedIo::new(Seed(91));
    let store = open(&io);
    for number in 0..SPAN_KEYS {
        store.put(COLUMN, format!("span-{number:06}").as_bytes(), b"value").expect("put");
    }
    seal(&store, &io);
    store.delete_range(COLUMN, b"span-000000", format!("span-{SPAN_KEYS:06}").as_bytes()).expect("delete");
    seal(&store, &io);
    settle(&io);
    let mut buf = Vec::new();
    assert!(!store.get(COLUMN, b"span-000001", &mut buf).expect("get"), "the live store showed a deleted key");
    store.close().expect("close");

    let store = open(&io);
    settle(&io);
    for number in 0..SPAN_KEYS {
        assert!(!store.get(COLUMN, format!("span-{number:06}").as_bytes(), &mut buf).expect("get"), "span-{number:06} came back after a reopen");
    }
    store.close().expect("close");
}

const SPAN_KEYS: u64 = 1_000;

// Every a- row and the cover over all of them sit in one segment. While a cue below the cover lives, the cover stays and its
// segment doesn't merge, so the cue reads the rows and every other read finds them deleted. Once the cue goes, the merge
// drops the rows with the cover. A reopen in between finds the cover in its segment's footer
#[test]
fn a_row_kept_for_a_cue_keeps_its_cover() {
    for (is_reopened, seed) in [(false, 130), (true, 131)] {
        let io = SimulatedIo::new(Seed(seed));
        let mut store = open(&io);
        put_keys(&store, "a", b"value");
        let below = store.cue().expect("cue below the cover");
        let before_cover = store.state.read_point(&store.state.volumes[0]);
        store.delete_range(COLUMN, b"a", b"b").expect("delete");
        put_keys(&store, "z", b"value");
        let holder = seal(&store, &io);
        let above = store.cue().expect("cue above the cover");
        settle(&io);
        let volume = &store.state.volumes[0];
        assert_eq!(pending(&store), 1, "reopened {is_reopened}: the cover left while a cue below it lived");
        assert!(volume.table.holds(holder), "reopened {is_reopened}: the cover's segment merged under a cue below it");
        assert!(volume.covers_held_by_cues.load(Ordering::Acquire) > 0, "reopened {is_reopened}: no look was held back");
        let get = |cue: &reel::engine::cue::Cue<SimulatedIo>| {
            let mut value = Vec::new();
            cue.get(COLUMN, b"a-000", &mut value).expect("get at a cue").then_some(value)
        };
        assert_eq!(get(&below), Some(b"value".to_vec()), "reopened {is_reopened}: the cue below lost a row live at its point");
        assert_eq!(get(&above), None, "reopened {is_reopened}: a cue above the cover read a deleted key");
        assert_eq!(read(&store, b"a-000"), None, "reopened {is_reopened}: a deleted key read at the current point");
        drop(below);
        drop(above);
        if is_reopened {
            store.close().expect("close");
            store = open(&io);
            assert_eq!(pending(&store), 1, "the cover didn't come back from its footer");
            assert_eq!(read(&store, b"a-000"), None, "a deleted key read after the reopen");
        }
        store.state.ask_compaction();
        settle(&io);
        let volume = &store.state.volumes[0];
        let kept = hold(&volume.summary).rows().filter(|row| row.facts.places.lowest <= before_cover).count();
        assert!(!volume.table.holds(holder), "reopened {is_reopened}: the cover's segment never merged once the cue went");
        assert_eq!(kept, 0, "reopened {is_reopened}: the merge kept rows no cue reads");
        assert_eq!(pending(&store), 0, "reopened {is_reopened}: the cover outlived every row under it");
        assert_eq!(read(&store, b"a-000"), None, "reopened {is_reopened}: a deleted key read after the merge");
        store.close().expect("close");
    }
}

// a scan open from before the cover across its segment's merge holds no point, so the merge drops the a- rows with the cover
#[test]
fn a_scan_open_across_the_merge_keeps_no_row() {
    let io = SimulatedIo::new(Seed(134));
    let store = open(&io);
    let volume = &store.state.volumes[0];
    put_keys(&store, "a", b"value");
    let mut scan = store.iter_from(COLUMN, b"a", reel_api::direction::Direction::Ascending).expect("scan");
    let (mut name, mut value) = (Vec::new(), Vec::new());
    assert_eq!(scan.next_row(&mut name, &mut value).expect("step"), reel_api::scan::ScanStep::Found);
    let before_cover = store.state.read_point(volume);
    store.delete_range(COLUMN, b"a", b"b").expect("delete");
    put_keys(&store, "z", b"value");
    hold_compaction(&store, true);
    let rewritten = volume.segments_rewritten.load(Ordering::Acquire);
    let holder = seal(&store, &io);
    hold_compaction(&store, false);
    settle(&io);
    let kept = hold(&volume.summary).rows().filter(|row| row.facts.places.lowest <= before_cover).count();
    eprintln!(
        "scan open: listed {} rewritten {} kept segments {kept} covers {} current {:?}",
        volume.table.holds(holder),
        volume.segments_rewritten.load(Ordering::Acquire) - rewritten,
        pending(&store),
        read(&store, b"a-000")
    );
    assert!(!volume.table.holds(holder), "the cover's segment never merged");
    assert_eq!(kept, 0, "the merge kept rows for an open scan");
    assert_eq!(read(&store, b"a-000"), None, "a deleted key read at the current point");
    drop(scan);
    store.close().expect("close");
}

// A cue above the cover and a re-put of a key it hides in a later segment: the cover clears, and its segment's merge drops
// the old row against the cover, the first place that hides it, though the newest place over it is the re-put
#[test]
fn a_row_under_a_cover_goes_against_the_cover() {
    let io = SimulatedIo::new(Seed(135));
    let store = open(&io);
    // no merge before every cue and segment is in place
    hold_compaction(&store, true);
    put_keys(&store, "a", b"value");
    store.delete_range(COLUMN, b"a", b"b").expect("delete");
    let holder = seal(&store, &io);
    let between = store.cue().expect("cue between the cover and the re-put");
    store.put(COLUMN, b"a-000", b"later").expect("re-put");
    seal(&store, &io);
    hold_compaction(&store, false);
    settle(&io);
    let volume = &store.state.volumes[0];
    let mut value = Vec::new();
    let at_between = between.get(COLUMN, b"a-000", &mut value).expect("get between").then_some(value);
    assert!(!volume.table.holds(holder), "the cover's segment never merged");
    assert_eq!(pending(&store), 0, "the cover stayed with nothing under it");
    assert_eq!(at_between, None, "a cue between the cover and the re-put read a deleted key");
    assert_eq!(read(&store, b"a-000"), Some(b"later".to_vec()));
    assert_eq!(read(&store, b"a-001"), None);
    drop(between);
    store.close().expect("close");
}

// Two covers over one key in two segments with a cue between them: the older cover is the one the merge of its segment drops
// the row against, so the cue reads it deleted after the merge. The newest cover would keep the row for the cue and leave it bare
#[test]
fn a_row_under_two_covers_goes_against_the_older() {
    for (is_cue_held, seed) in [(true, 136), (false, 137)] {
        let io = SimulatedIo::new(Seed(seed));
        let store = open(&io);
        // no merge before every cue and segment is in place
        hold_compaction(&store, true);
        put_keys(&store, "a", b"value");
        store.delete_range(COLUMN, b"a", b"b").expect("older delete");
        let holder = seal(&store, &io);
        let mut mid = Some(store.cue().expect("cue between the covers"));
        store.delete_range(COLUMN, b"a", b"b").expect("newer delete");
        put_keys(&store, "z", b"value");
        seal(&store, &io);
        if !is_cue_held {
            mid = None;
        }
        hold_compaction(&store, false);
        settle(&io);
        let volume = &store.state.volumes[0];
        let at_mid = mid.as_ref().map(|cue| {
            let mut value = Vec::new();
            cue.get(COLUMN, b"a-000", &mut value).expect("get between").then_some(value)
        });
        assert!(!volume.table.holds(holder), "held {is_cue_held}: the covers' segment never merged");
        assert_eq!(pending(&store), 0, "held {is_cue_held}: a cover stayed with nothing under it");
        assert_eq!(at_mid.flatten(), None, "held {is_cue_held}: a cue between the covers read a deleted key");
        assert_eq!(read(&store, b"a-000"), None, "held {is_cue_held}: a deleted key read at the current point");
        drop(mid);
        store.close().expect("close");
    }
}
