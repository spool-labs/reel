//! The scrub: a paced pass over the sealed segments that checks every record and marks a rotted one as damage

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel::config::{ColumnConfig, ColumnIndex, KeyShape, NO_WORKERS, StoreConfig, SyncPolicy, ThreadCounts, VolumeConfig};
use reel::engine::compact_job::CompactJob;
use reel::engine::open_segment::data_path;
use reel::engine::reel_store::ReelStore;
use reel::engine::scrub_job::ScrubJob;
use reel::engine::walk_job::FREE_IN_PLACE_BYTES;
use reel::guard::hold;
use reel::io::job::JobStep;
use reel::io::{InPlace, Io, in_place};
use reel::record::codec::Codec;
use reel::record::record_head::SMALL_HEAD_BYTES;
use reel::segment::segment_tail::SegmentNumber;
use reel_core::column_id::ColumnId;
use reel_core::units::{ByteCount, FileOffset, Nanoseconds, VolumeId};
use reel_sim::file_image::BitMask;
use reel_sim::seeded_random::{Seed, SeededRandom};
use reel_sim::simulated_io::SimulatedIo;

const ROOT: &str = "/disk/scrub";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(64 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 40_000;
const KEYS: u64 = 300;
const RATE: u64 = 1024 * 1024;
const MOST_SCRUB_STEPS: u64 = 100_000;
const BUSY_ROUNDS: u64 = 12;
const BUSY_KEYS: u64 = 40;

type Store = ReelStore<SimulatedIo>;

fn open(io: &SimulatedIo, scrub_bytes_per_second: u64) -> Store {
    let config = StoreConfig {
        memory_budget: BUDGET,
        threads: ThreadCounts { scrub_bytes_per_second, ..NO_WORKERS },
        volumes: vec![VolumeConfig {
            name: "scrub".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(), open_segments: None, segment_bytes: None, lifetime_groups: None,
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Variable { most: 64 },
            codec: Codec::None,
            volume: VolumeId(0),
            index: ColumnIndex::Plain,
            least_rows: None,
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

fn seal(store: &Store, io: &SimulatedIo) -> SegmentNumber {
    let number = hold(&store.state.volumes[0].claims).open[0].number;
    store.seal_now();
    settle(io);
    number
}

fn key(number: u64) -> Vec<u8> {
    format!("key-{number:05}").into_bytes()
}

fn value(number: u64, round: u64) -> Vec<u8> {
    format!("value-{number:05}-{round}").into_bytes()
}

fn read(store: &Store, name: &[u8]) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    store.get(COLUMN, name, &mut buf).expect("get").then_some(buf)
}

// step a scrub of its own until it ends a pass, with the clock moving while the rate holds it back, and say how many steps
fn scrub_one_pass(store: &Store, io: &SimulatedIo, job: &mut ScrubJob<SimulatedIo>) -> u64 {
    let passes = store.state.scrub_passes.load(Ordering::Relaxed);
    for step in 0..MOST_SCRUB_STEPS {
        if store.state.scrub_passes.load(Ordering::Relaxed) > passes {
            return step;
        }
        match job.step_unit(io.now()) {
            JobStep::Worked => {},
            JobStep::IdleUntil { deadline } => io.advance_clock(Nanoseconds(deadline.0.saturating_sub(io.now().0).max(1))),
            JobStep::Idle | JobStep::Finished => return step,
        }
    }
    panic!("the scrub never ended a pass");
}

// a flipped byte in a sealed record is found within one pass: its segment is damaged, and the counters show the failure and the
// bytes checked
#[test]
fn a_flipped_byte_is_found_within_one_pass() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io, 0);
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), &value(number, 0)).expect("put");
    }
    let segment = seal(&store, &io);
    let volume = &store.state.volumes[0];
    let name = key(KEYS / 2);
    let read_at = store.state.key_read(COLUMN, &name, store.state.read_point(volume), false).expect("read");
    let row = in_place(store.state.search_sealed(&InPlace, volume, &read_at)).expect("search").expect("a sealed row");
    io.rot(&data_path(&volume.roots[0], row.segment), FileOffset(row.row.offset.0 + SMALL_HEAD_BYTES as u64), BitMask(1)).expect("rot");

    let mut job = ScrubJob::new(Arc::clone(&store.state), 0);
    job.rate = RATE;
    scrub_one_pass(&store, &io, &mut job);
    assert_eq!(store.state.scrub_failures.load(Ordering::Relaxed), 1);
    assert!(hold(&volume.damaged).contains(&segment), "the scrub left the rotted segment undamaged");
    assert!(store.state.scrub_bytes.load(Ordering::Relaxed) > 0);
    store.close().expect("close");
}

// a rate of 0 checks nothing
#[test]
fn rate_zero_runs_nothing() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io, 0);
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), &value(number, 0)).expect("put");
    }
    seal(&store, &io);
    let mut job = ScrubJob::new(Arc::clone(&store.state), 0);
    assert_eq!(job.step_unit(io.now()), JobStep::Idle);
    settle(&io);
    assert_eq!(store.state.scrub_bytes.load(Ordering::Relaxed), 0);
    assert_eq!(store.state.scrub_passes.load(Ordering::Relaxed), 0);
    store.close().expect("close");
}

// a segment a merge takes away while a pass stands in it is passed over with no error and no damage
#[test]
fn a_segment_leaving_mid_pass_is_passed_over() {
    let io = SimulatedIo::new(Seed(3));
    let store = open(&io, 0);
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), &value(number, 0)).expect("put");
    }
    // half the keys again, so a merge of the segment drops rows and makes a new one
    for number in (0..KEYS).filter(|number| number % 2 == 0) {
        store.put(COLUMN, &key(number), &value(number, 1)).expect("put");
    }
    let first = seal(&store, &io);
    let mut job = ScrubJob::new(Arc::clone(&store.state), 0);
    job.rate = RATE;
    assert_eq!(job.step_unit(io.now()), JobStep::Worked);
    let mut merge = CompactJob::new(Arc::clone(&store.state), 0);
    merge.rewrite.forced = Some(first);
    if merge.rewrite.start() {
        while merge.rewrite.is_running() {
            merge.rewrite.step();
        }
    }
    settle(&io);
    assert!(!store.state.volumes[0].table.holds(first), "the merge left its input in place");
    scrub_one_pass(&store, &io, &mut job);
    assert_eq!(store.state.scrub_failures.load(Ordering::Relaxed), 0);
    assert!(hold(&store.state.volumes[0].damaged).is_empty());
    for number in 0..KEYS {
        assert_eq!(read(&store, &key(number)), Some(value(number, u64::from(number % 2 == 0))));
    }
    store.close().expect("close");
}

// the store's own scrub runs beside writers, seals, merges and frees in place, and finds no false damage
#[test]
fn a_scrub_beside_writers_and_merges_finds_no_false_damage() {
    let io = SimulatedIo::new(Seed(4));
    let store = open(&io, RATE);
    let mut random = SeededRandom::new(Seed(4));
    let large = vec![7_u8; FREE_IN_PLACE_BYTES.0 as usize];
    for round in 0..BUSY_ROUNDS {
        for number in 0..BUSY_KEYS {
            match random.below(4) {
                0 => store.put(COLUMN, &key(number), &large).expect("put"),
                1 => store.delete(COLUMN, &key(number)).expect("delete"),
                _ => store.put(COLUMN, &key(number), &value(number, round)).expect("put"),
            }
        }
        seal(&store, &io);
        io.advance_clock(Nanoseconds(TICK.0 * 1_000));
        settle(&io);
    }
    assert!(store.state.scrub_bytes.load(Ordering::Relaxed) > 0, "the store's scrub checked nothing");
    assert_eq!(store.state.scrub_failures.load(Ordering::Relaxed), 0);
    assert!(hold(&store.state.volumes[0].damaged).is_empty(), "the scrub damaged a sound segment");
    store.close().expect("close");
}

// a value read unchecked and held in its index leaf stops answering once the scrub marks its record bad: the next get refuses
#[test]
fn a_held_rotted_value_stops_answering_after_the_scrub() {
    let io = SimulatedIo::new(Seed(5));
    let mut config = sorted_config();
    config.threads = ThreadCounts { scrub_bytes_per_second: 0, ..NO_WORKERS };
    let store = ReelStore::open(io.clone(), &config).expect("open");
    for number in 0..KEYS {
        store.put(SORTED, &sorted_key(number), &value(number, 0)).expect("put");
    }
    seal(&store, &io);
    let volume = &store.state.volumes[0];
    let name = sorted_key(KEYS / 2);
    let read_at = store.state.key_read(SORTED, &name, store.state.read_point(volume), false).expect("read");
    let row = in_place(store.state.search_sealed(&InPlace, volume, &read_at)).expect("search").expect("a sealed row");
    io.rot(&data_path(&volume.roots[0], row.segment), FileOffset(row.row.offset.0 + SMALL_HEAD_BYTES as u64), BitMask(1)).expect("rot");
    let mut buf = Vec::new();
    // the get trusts the sealed record its counts vouch for, reads the rotted bytes and holds them
    assert!(store.get(SORTED, &name, &mut buf).expect("get"));
    assert_ne!(buf, value(KEYS / 2, 0));
    assert!(store.state.sorted_index(SORTED).expect("an index").values.held.load(Ordering::Relaxed) > 0, "nothing was held");

    let mut job = ScrubJob::new(Arc::clone(&store.state), 0);
    job.rate = RATE;
    scrub_one_pass(&store, &io, &mut job);
    assert_eq!(store.state.scrub_failures.load(Ordering::Relaxed), 1);
    buf.clear();
    let again = store.get(SORTED, &name, &mut buf);
    assert!(!matches!(again, Ok(true)) || buf == value(KEYS / 2, 0), "the held rotted value answered after the scrub: {again:?} {buf:?}");
    store.close().expect("close");
}

const SORTED: ColumnId = ColumnId(2);
const FULL_BUDGET_BYTES: ByteCount = ByteCount(1024 * 1024 * 1024);

fn sorted_key(number: u64) -> Vec<u8> {
    let mut key = [0_u8; 16];
    key[8..].copy_from_slice(&number.to_be_bytes());
    key.to_vec()
}

// a store on a full budget with a sorted column, so its index leaves hold values
fn sorted_config() -> StoreConfig {
    StoreConfig {
        memory_budget: FULL_BUDGET_BYTES,
        threads: NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "scrub".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(), open_segments: None, segment_bytes: None, lifetime_groups: None,
        }],
        columns: vec![ColumnConfig {
            id: SORTED,
            name: "sorted".to_string(),
            key_shape: KeyShape::Fixed { width: 16 },
            codec: Codec::None,
            volume: VolumeId(0),
            index: ColumnIndex::Sorted,
            least_rows: None,
            least_value: None,
        }],
    }
}
