//! How long each kind of pick holds a volume's summary lock at 100,000 segments, against the 1 ms line

use std::iter;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, ThreadCounts, VolumeConfig};
use reel::engine::compact_job::{CompactJob, can_unlink};
use reel::engine::filter_job::FilterLoader;
use reel::engine::reel_store::ReelStore;
use reel::engine::rewrite_job::{MOST_PASSED_LOOKS, RewriteJob};
use reel::engine::walk_job::WalkJob;
use reel::guard::hold;
use reel::io::Io;
use reel::lifetime::MarkRange;
use reel::record::codec::Codec;
use reel::record::record_check::{Checksum, checksum};
use reel::segment::block_entry::{BlockEntry, KeyBound, KeyPrefix};
use reel::segment::column_directory::{DirectoryEntry, Listing, RangeEntry};
use reel::segment::segment_tail::{MOST_MERGE_SEGMENTS, PlaceRange, SegmentNumber, TailHead};
use reel::segment::summary::{ColumnFacts, DeadCount, SegmentFacts};
use reel_api::column_id::ColumnId;
use reel_api::maintenance_report::PickOutcome;
use reel_api::units::{ByteCount, DeathMark, FileOffset, RootId, RowCount, VolumeId, WritePlace};
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_io::SimulatedIo;

type Store = ReelStore<SimulatedIo>;

const ROOT: &str = "/disk/picks";
const COLUMN: ColumnId = ColumnId(1);
const SEGMENTS: u64 = 100_000;
// a disk past what 100,000 segments take, which sizes the table and the directories for them
const DISK: ByteCount = ByteCount(1 << 42);
const BLOCKS: u32 = 64;
// room for a row's facts, its blocks' dead counts and one column with two keys
const ROW_ROOM: u64 = 1_024;
const LOOKS: usize = 11;
// the gate runs with one worker and with as many as a large node runs, since each lane adds a lock to the checks a pick makes
const GATE_WORKERS: [usize; 2] = [1, 32];
// no pick holds the summary longer than this, at 100,000 segments a volume, in a release build
const HOLD_LINE: Duration = Duration::from_millis(1);
// a record pick past a queue of wholly dead segments holds this, four times the 60 µs it takes with one count a segment and
// under half the 584 µs a walk over every block line took
const DEAD_QUEUE_LINE: Duration = Duration::from_micros(250);
const WORKER_LANE: usize = 1;
const PLANNER_LANE: usize = 0;

fn config(workers: usize) -> StoreConfig {
    StoreConfig {
        memory_budget: ByteCount(1 << 30),
        threads: ThreadCounts { maintenance_workers: Some(workers), ..ThreadCounts::default() },
        volumes: vec![VolumeConfig {
            name: "picks".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(),
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Variable { most: 16 },
            codec: Codec::None,
            volume: VolumeId(0),
            complete_rows: None,
            least_value: None,
        }],
    }
}

// a walked segment with nine tenths of its rows and bytes dead, past the early bar, so every one is ready for a rewrite
fn facts(number: u64) -> SegmentFacts {
    SegmentFacts {
        has_covers: false,
        segment: SegmentNumber(number),
        file_length: ByteCount(4 << 20),
        data_end: FileOffset(3 << 20),
        row_block_end: FileOffset(4 << 20),
        seal_mark: Checksum(7),
        places: PlaceRange { lowest: WritePlace(number * 1_000), highest: WritePlace(number * 1_000 + 999) },
        is_damaged: false,
        damage_place: WritePlace(0),
        block_count: BLOCKS,
        walked_blocks: BLOCKS,
        is_backward: false,
        has_lost_rows: false,
        marks: MarkRange { lowest: DeathMark(1), highest: DeathMark(1) },
        most_block_rows: 160,
    }
}

// one column's counts: nine tenths of its rows and bytes dead
fn column_facts() -> ColumnFacts<'static> {
    ColumnFacts {
        column: COLUMN,
        lowest: b"",
        bound: b"",
        row_count: RowCount(10_000),
        grave_count: RowCount(0),
        dead_rows: RowCount(9_000),
        dead_graves: RowCount(0),
        live_bytes: ByteCount(300_000),
        dead_bytes: ByteCount(2_700_000),
    }
}

// fill the volume's summary with a row a segment, each over its own keys or all over the same ones
fn fill(store: &Store, is_crowded: bool) {
    let mut listings = Vec::with_capacity(SEGMENTS as usize);
    let mut rows = hold(&store.state.volumes[0].summary);
    rows.make_room(ByteCount(SEGMENTS * ROW_ROOM));
    rows.make_index(SEGMENTS as usize);
    for number in 1..=SEGMENTS {
        let (lowest, bound) = if is_crowded { (0u64, u64::MAX) } else { (number << 20, (number + 1) << 20) };
        let (lowest, bound) = (lowest.to_be_bytes(), bound.to_be_bytes());
        let column = ColumnFacts { column: COLUMN, lowest: &lowest, bound: &bound, ..column_facts() };
        assert!(rows.push(&facts(number), iter::once(column), iter::empty()), "the summary filled at {number}");
        listings.push(listing(number, &lowest, &bound));
    }
    drop(rows);
    let directory = store.state.directories[COLUMN.0 as usize].as_ref().expect("a directory");
    assert!(directory.publish_all(&listings), "the directory has no room for every segment");
}

// a segment's listing in the column's directory, from its lowest key to the last key under its bound
fn listing(number: u64, lowest: &[u8; 8], bound: &[u8; 8]) -> Listing {
    let highest = u64::from_be_bytes(*bound).saturating_sub(1).to_be_bytes();
    let entry = DirectoryEntry {
        shared_length: 0,
        shared_sum: checksum(&[]),
        lowest: KeyPrefix::of_key(lowest, 0),
        highest: KeyPrefix::of_key(&highest, 0),
        reach: 0,
        segment: SegmentNumber(number),
        newest_place: WritePlace(number * 1_000 + 999),
    };
    let range = RangeEntry { lowest: KeyBound::of_key(lowest), highest: KeyBound::of_key(&highest), reach: 0, segment: SegmentNumber(number) };
    Listing { entry, range }
}

// time a pick a number of times against a line, print the least, the median and the most, and return how many looks found one
fn time(label: &str, line: Duration, mut pick: impl FnMut() -> bool) -> usize {
    let mut times: Vec<Duration> = Vec::with_capacity(LOOKS);
    let mut found = 0;
    for _look in 0..LOOKS {
        let started = Instant::now();
        found += usize::from(pick());
        times.push(started.elapsed());
    }
    times.sort();
    // a debug build runs some ten times slower, and only the release run holds the line. The median holds it, since one look
    // a parallel test preempts says nothing about the pick
    assert!(cfg!(debug_assertions) || times[LOOKS / 2] < line, "{label} held the summary {:?} at the median", times[LOOKS / 2]);
    println!(
        "pick {label}: least {:?} median {:?} most {:?}, found {found} of {LOOKS}",
        times[0],
        times[LOOKS / 2],
        times[LOOKS - 1]
    );
    found
}

// each kind of pick over 100,000 segments, which CI also runs in a release build to hold the line
#[test]
fn picks_at_most_segments() {
    for (workers, is_crowded) in GATE_WORKERS.into_iter().flat_map(|workers| [(workers, false), (workers, true)]) {
        let io = SimulatedIo::new(Seed(71));
        // the table plans an entry for each segment the disk can hold, and the directory takes the same room
        io.add_disk(Path::new(ROOT), DISK);
        let store: Store = ReelStore::open(io, &config(workers)).expect("open");
        println!("room for {} rows", hold(&store.state.volumes[0].summary).index_room());
        fill(&store, is_crowded);
        let state = &store.state;
        let volume = &state.volumes[0];
        let shape = format!("{} with {workers} workers", if is_crowded { "crowded" } else { "apart" });
        println!("{shape}: {} rows, {} summary bytes", SEGMENTS, hold(&volume.summary).bytes().len());

        // a record pick waits behind an owed overlap merge, so the record timings clear the flag to walk the buckets
        let was_over_bound = volume.is_over_bound.swap(false, Ordering::AcqRel);
        // with a worker and callers busy only the worker's lane takes an overlap merge, so the planner's lane walks the record
        // buckets in both shapes
        let mut planner = RewriteJob::new(Arc::clone(state), 0, PLANNER_LANE);
        let records = || volume.picks[PickOutcome::Record as usize].load(Ordering::Relaxed);
        let records_before = records();
        let found = time(&format!("{shape} record"), HOLD_LINE, || planner.pick_within(volume, MOST_MERGE_SEGMENTS).is_some());
        assert_eq!((found, records() - records_before), (LOOKS, LOOKS as u64), "{shape} record took other than a record pick each look");
        // the worker's lane looks for an overlap merge first, which only crowded keys give it
        let mut lane = RewriteJob::new(Arc::clone(state), 0, WORKER_LANE);
        let overlaps_before = volume.picks[PickOutcome::Overlap as usize].load(Ordering::Relaxed);
        let found = time(&format!("{shape} worker overlap then record"), HOLD_LINE, || lane.pick_within(volume, MOST_MERGE_SEGMENTS).is_some());
        assert_eq!(found, LOOKS, "{shape} worker lane found nothing");
        let overlaps = volume.picks[PickOutcome::Overlap as usize].load(Ordering::Relaxed) - overlaps_before;
        println!("{shape} worker overlap picks: {overlaps}, misses: {}", volume.picks[PickOutcome::OverlapMiss as usize].load(Ordering::Relaxed));
        assert_eq!(overlaps > 0, is_crowded, "the worker's overlap picks went against the keys");
        volume.is_over_bound.store(was_over_bound, Ordering::Release);
        // every row is walked and none is wholly dead, so these two find nothing, and record_pick_past_a_dead_queue times a queue
        let mut walk = WalkJob::new(Arc::clone(state), 0);
        assert_eq!(time(&format!("{shape} walk target"), HOLD_LINE, || walk.pick_target()), 0);
        let compaction = CompactJob::new(Arc::clone(state), 0);
        assert_eq!(time(&format!("{shape} wholly dead"), HOLD_LINE, || compaction.pick_dead(volume).is_some()), 0);
        let mut loaders: Vec<FilterLoader<SimulatedIo>> = (0..LOOKS).map(|_loader| FilterLoader::new(Arc::clone(state), 0)).collect();
        assert_eq!(time(&format!("{shape} filter"), HOLD_LINE, || loaders.pop().is_some_and(|mut loader| loader.pick(volume))), LOOKS);
        // the holds a seal and an install take: a row added at the end, a counted block landed, and a row taken out
        let mut next = SEGMENTS;
        let pushed = time(&format!("{shape} push"), HOLD_LINE, || {
            next += 1;
            let lowest = (next << 20).to_be_bytes();
            let bound = ((next + 1) << 20).to_be_bytes();
            let column = ColumnFacts { column: COLUMN, lowest: &lowest, bound: &bound, ..column_facts() };
            hold(&volume.summary).push(&facts(next), iter::once(column), iter::empty())
        });
        let mut walked = 0;
        let applied = time(&format!("{shape} apply walk"), HOLD_LINE, || {
            walked += 1;
            let count = DeadCount { segment: SegmentNumber(walked), column: COLUMN, block: 0, rows: 1, ..DeadCount::default() };
            hold(&volume.summary).apply_walk(SegmentNumber(walked), BLOCKS, &[count]);
            true
        });
        let mut removed = SEGMENTS / 2;
        let taken_out = time(&format!("{shape} remove"), HOLD_LINE, || {
            removed += 1;
            hold(&volume.summary).remove(SegmentNumber(removed));
            true
        });
        assert_eq!((pushed, applied, taken_out), (LOOKS, LOOKS, LOOKS));
        drop((planner, lane, walk, compaction));
        store.close().expect("close");
    }
}

/// Blocks each wholly dead segment's table entry holds, as many as a segment can have, halved until the table has room
const DEAD_BLOCKS: u32 = 64 * 1024;

// a row whose counts say nothing in its segment is live
fn dead_column_facts() -> ColumnFacts<'static> {
    ColumnFacts { dead_rows: RowCount(10_000), live_bytes: ByteCount(0), dead_bytes: ByteCount(3_000_000), ..column_facts() }
}

// a queue of wholly dead segments the unlink can take, at the head of the top share bucket with real table entries of many
// blocks, ahead of 100,000 segments a record pick can take: the pick passes the queue and holds the line
#[test]
fn record_pick_past_a_dead_queue() {
    for workers in GATE_WORKERS {
        dead_queue(workers);
    }
}

// time a record pick past the dead queue with this many workers, each a lane whose inputs the unlink check reads
fn dead_queue(workers: usize) {
    let io = SimulatedIo::new(Seed(72));
    io.add_disk(Path::new(ROOT), DISK);
    let store: Store = ReelStore::open(io.clone(), &config(workers)).expect("open");
    let state = &store.state;
    let volume = &state.volumes[0];
    let file = io.create_file(&Path::new(ROOT).join("stand-in"), ByteCount(0)).expect("a file");
    let ahead = MOST_PASSED_LOOKS as u64;
    let mut total = 0;
    let lines = vec![BlockEntry::default(); DEAD_BLOCKS as usize];
    for number in SEGMENTS + 1..=SEGMENTS + ahead {
        let head = TailHead { segment: SegmentNumber(number), ..TailHead::default() };
        let mut blocks = DEAD_BLOCKS as usize;
        while !volume.table.install(file.clone(), RootId(0), &head, &[], &lines[..blocks]) {
            assert!(blocks > 0, "the table has no slot for segment {number}");
            blocks /= 2;
        }
        total += blocks;
    }
    println!("{ahead} dead segments over {total} blocks");
    let mut rows = hold(&volume.summary);
    rows.make_room(ByteCount((SEGMENTS + ahead) * ROW_ROOM));
    rows.make_index((SEGMENTS + ahead) as usize);
    for number in SEGMENTS + 1..=SEGMENTS + ahead {
        let (lowest, bound) = ((number << 20).to_be_bytes(), ((number + 1) << 20).to_be_bytes());
        let column = ColumnFacts { column: COLUMN, lowest: &lowest, bound: &bound, ..dead_column_facts() };
        assert!(rows.push(&facts(number), iter::once(column), iter::empty()), "the summary filled at {number}");
    }
    drop(rows);
    fill(&store, false);
    let mut lane = RewriteJob::new(Arc::clone(state), 0, WORKER_LANE);
    let candidate = SegmentNumber(SEGMENTS + 1);
    assert!(can_unlink(state, 0, volume, candidate), "the queue isn't one the unlink can take");
    // segments over keys of their own owe no overlap merge, so the record pick walks the buckets
    volume.is_over_bound.store(false, Ordering::Release);
    let found = time(&format!("record past a dead queue with {workers} workers"), DEAD_QUEUE_LINE, || lane.pick_within(volume, MOST_MERGE_SEGMENTS).is_some_and(|(target, _)| target.length.0 > 0));
    assert_eq!(found, LOOKS, "the pick found nothing past the dead queue");
    drop(lane);
    store.close().expect("close");
}
