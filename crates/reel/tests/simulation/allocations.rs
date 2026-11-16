//! The zero-allocation gate on the simulated disk: every call of a warm store allocates and frees nothing

use std::hint::black_box;
use std::time::Duration;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::open_segment::SegmentPhase;
use reel::engine::reel_store::ReelStore;
use reel::guard::hold;
use reel::io::job::{Job, JobStep};
use reel::io::wake_line::WakeLine;
use reel::io::{Io, IoFile};
use reel::record::codec::Codec;
use reel::segment::range_covers::MOST_RANGE_COVERS;
use reel::segment::segment_tail::SECOND_TAIL_DISTANCE;
use reel_core::column_id::ColumnId;
use reel_core::direction::Direction;
use reel_core::scan::ScanStep;
use reel_core::units::{ByteCount, FileOffset, Nanoseconds, RowCount, VolumeId};
use reel_core::write_batch::WriteBatch;
use reel_sim::count_pause::CountPause;
use reel_sim::file_image::BitMask;
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_io::SimulatedIo;

use crate::counting::counted;

const ROOT: &str = "/disk/allocations";
const META: ColumnId = ColumnId(1);
const BULK: ColumnId = ColumnId(2);
const PLAIN: ColumnId = ColumnId(3);
const BUDGET: ByteCount = ByteCount(177 * 1024 * 1024);
const TABLE_ROWS: RowCount = RowCount(1_000_000);
const LONG_ROWS: usize = 4_000;
const LEAST_LONG_ROWS: usize = 500;
const LONG_VALUE: &[u8] = b"a value on each column of a long batch, a value on each column of a long batch";
const KEYS: usize = 64;
const KEY_BUFFER_BYTES: usize = 128;
const SCAN_ROWS: usize = 8;
const CHUNKED_BYTES: usize = 200_000;
const WARM_ROUNDS: usize = 8;
const ROUNDS: usize = 64;
const FLUSH_ROUNDS: usize = 4;
const SEAL_ROUNDS: usize = 8;
const SETTLE_STEPS: u64 = 2_000_000;
const SETTLE_TICK: Nanoseconds = Nanoseconds(1_000_000);
const DAMAGED_KEY: &[u8] = b"damaged-100";
const DAMAGED_END: &[u8] = b"damaged-101";
const DAMAGED_KEYS: u64 = 200;

type Store = ReelStore<SimulatedIo>;

/// Everything a round uses, built before the count starts
struct Inputs {
    keys: Vec<Vec<u8>>,
    small: Vec<u8>,
    chunked: Vec<u8>,
    compressible: Vec<u8>,
    batch: WriteBatch,
    large_batch: WriteBatch,
    long_batch: WriteBatch,
    buf: Vec<u8>,
    scan_key: Vec<u8>,
}

fn config() -> StoreConfig {
    config_at(BUDGET)
}

fn config_at(budget: ByteCount) -> StoreConfig {
    let column = |id: ColumnId, name: &str, codec: Codec, complete_rows: Option<RowCount>| ColumnConfig {
        id,
        name: name.to_string(),
        key_shape: KeyShape::Variable { most: 64 },
        codec,
        volume: VolumeId(0),
        complete_rows,
        sorted_rows: None,
        least_value: None,
    };
    StoreConfig {
        memory_budget: budget,
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "allocations".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(), open_segments: None,
        }],
        columns: vec![column(META, "meta", Codec::None, Some(TABLE_ROWS)), column(BULK, "bulk", Codec::Lz4, None), column(PLAIN, "plain", Codec::None, None)],
    }
}

fn settle(io: &SimulatedIo) {
    for _step in 0..SETTLE_STEPS {
        if io.step_any().is_none() {
            io.advance_clock(SETTLE_TICK);
            if io.step_any().is_none() {
                return;
            }
        }
    }
    panic!("jobs never went quiet");
}

fn both<Answer>(count: (Answer, u64, u64)) -> u64 {
    count.1 + count.2
}

// run every path once, and return what the calls allocated and freed
fn round(store: &Store, io: &SimulatedIo, inputs: &mut Inputs, number: usize) -> u64 {
    let key = &inputs.keys[number % KEYS];
    let other = &inputs.keys[(number + KEYS / 2) % KEYS];
    let buf = &mut inputs.buf;
    let mut total = 0;

    total += both(counted(|| store.put(META, key, &inputs.small).expect("put")));
    total += both(counted(|| store.put(META, other, &inputs.chunked).expect("put chunked")));
    total += both(counted(|| store.put(BULK, key, &inputs.compressible).expect("put lz4")));
    total += both(counted(|| store.get(META, key, buf).expect("get")));
    total += both(counted(|| store.get(META, other, buf).expect("get chunked")));
    total += both(counted(|| store.get(BULK, key, buf).expect("get lz4")));
    total += both(counted(|| store.get(META, b"missing", buf).expect("miss")));
    total += both(counted(|| store.contains(META, key).expect("contains")));
    // the plain small shape runs beside the complete one, on the same keys
    total += both(counted(|| store.put(PLAIN, key, &inputs.small).expect("put plain")));
    total += both(counted(|| store.get(PLAIN, key, buf).expect("get plain")));
    total += both(counted(|| store.get(PLAIN, b"missing", buf).expect("plain miss")));
    total += both(counted(|| store.delete(PLAIN, other).expect("delete plain")));
    total += both(counted(|| store.delete(BULK, other).expect("delete")));
    // the one free here is the caller's own buffer, which the call takes
    let owned = inputs.small.clone();
    let (_put, allocations, frees) = counted(|| store.put_owned(META, key, owned).expect("put owned"));
    assert_eq!((allocations, frees), (0, 1));
    total += both(counted(|| store.delete_range(META, b"range-a", b"range-b").expect("delete range")));
    total += both(counted(|| store.write_batch(&inputs.batch).expect("batch")));
    total += both(counted(|| store.write_batch(&inputs.large_batch).expect("large batch")));
    let scan_key = &mut inputs.scan_key;
    total += both(counted(|| {
        let mut scan = store.iter_prefix(BULK, b"key-").expect("scan");
        while scan.next_key(scan_key).expect("step") == ScanStep::Found {}
    }));
    total += both(counted(|| {
        let mut scan = store.iter_from(BULK, b"key-z", Direction::Descending).expect("scan");
        for _row in 0..SCAN_ROWS {
            if scan.next_row(scan_key, buf).expect("step") == ScanStep::End {
                break;
            }
        }
    }));
    let refused = counted(|| store.get(META, DAMAGED_KEY, buf).is_err());
    assert!(refused.0);
    total += both(refused);
    total += both(counted(|| store.clear_damage(META, DAMAGED_END, Some(DAMAGED_END)).expect("clear")));
    if number.is_multiple_of(FLUSH_ROUNDS) {
        total += both(counted(|| store.write_batch(&inputs.long_batch).expect("long batch")));
        total += both(counted(|| store.flush().expect("flush")));
    }
    if number.is_multiple_of(SEAL_ROUNDS) {
        total += both(counted(|| store.seal_now()));
    }
    total += both(counted(|| settle(io)));
    total
}

// a sealed segment that opens damaged, so a round has a refused read and a clear to do
fn damage(io: &SimulatedIo) {
    let store = ReelStore::open(io.clone(), &config()).expect("open");
    for number in 0..DAMAGED_KEYS {
        store.put(META, format!("damaged-{number:03}").as_bytes(), b"value").expect("put");
    }
    store.seal_now();
    settle(io);
    store.close().expect("close");
    drop(store);

    let root = PathBuf::from(ROOT);
    for path in io.list_directory(&root).expect("list") {
        if path.extension().is_some_and(|extension| extension == "data") {
            let file = io.open_read_only(&path).expect("open");
            let length = file.length().expect("length").0;
            io.close_file(file);
            io.rot(&path, FileOffset(length - 3), BitMask(1)).expect("rot");
            io.rot(&path, FileOffset(length - SECOND_TAIL_DISTANCE - 3), BitMask(1)).expect("rot");
        }
    }
}

// once every path has run, no call on the simulated disk allocates or frees
#[test]
fn no_allocations() {
    no_allocations_at(BUDGET, LONG_ROWS);
}

// the least budget the store opens with leaves the steady rounds as quiet
#[test]
fn no_allocations_at_the_least_budget() {
    let store = ReelStore::open(SimulatedIo::new(Seed(1)), &config()).expect("open");
    let least = store.memory_report().least_budget;
    store.close().expect("close");
    assert!(least < BUDGET, "the least budget is above the usual one");
    no_allocations_at(least, LEAST_LONG_ROWS);
}

fn no_allocations_at(budget: ByteCount, long_rows: usize) {
    let io = SimulatedIo::new(Seed(1));
    damage(&io);
    let store = ReelStore::open(io.clone(), &config_at(budget)).expect("open");
    // the loop reads a span as damaged on every round, so no rebuild may clear it
    for volume in &store.state.volumes {
        volume.holds.rebuild.store(true, Ordering::Release);
    }
    let keys: Vec<Vec<u8>> = (0..KEYS).map(|number| format!("key-{number:06}").into_bytes()).collect();
    let small = vec![7; 100];
    let compressible = vec![b'x'; CHUNKED_BYTES];
    let mut batch = WriteBatch::new();
    batch.put(META, &keys[0], &small);
    batch.put(META, &keys[1], &small);
    batch.delete(BULK, &keys[1]);
    batch.delete_range(META, b"range-c", b"range-d");
    let mut large_batch = WriteBatch::new();
    large_batch.put(BULK, &keys[2], &compressible);
    large_batch.put(META, &keys[2], &small);
    let mut long_batch = WriteBatch::new();
    for number in 0..long_rows {
        let key = format!("long-{number:06}");
        long_batch.put(META, key.as_bytes(), LONG_VALUE);
        long_batch.put(BULK, key.as_bytes(), LONG_VALUE);
    }
    let mut inputs = Inputs {
        keys,
        small,
        chunked: (0..CHUNKED_BYTES).map(|index| ((index * 31) >> 3) as u8).collect(),
        compressible,
        batch,
        large_batch,
        long_batch,
        buf: vec![1; CHUNKED_BYTES * 2],
        scan_key: vec![1; KEY_BUFFER_BYTES],
    };

    for number in 0..WARM_ROUNDS {
        round(&store, &io, &mut inputs, number);
    }
    let mut total = 0;
    for number in 0..ROUNDS {
        total += round(&store, &io, &mut inputs, number);
    }
    assert_eq!(total, 0);

    // close is the one place that frees
    let (closed, allocations, _frees) = counted(|| store.close());
    closed.expect("close");
    assert_eq!(allocations, 0, "close allocated");
}

/// A job that allocates at every step, for the count to catch
struct AllocatingJob;

impl Job for AllocatingJob {
    fn step(&mut self) -> JobStep {
        black_box(vec![1_u8; 64]);
        JobStep::Finished
    }
}

// the count sees engine code in a call and in a job, and a pause hides only what runs inside it
#[test]
fn count_sees() {
    let (_list, allocations, frees) = counted(|| black_box(vec![1_u8; 64]).len());
    assert_eq!((allocations, frees), (1, 1));

    let io = SimulatedIo::new(Seed(2));
    let _task = io.spawn_job("allocating", AllocatingJob).expect("spawn");
    let (_step, allocations, _frees) = counted(|| io.step_any());
    assert_eq!(allocations, 1);

    let (_hidden, allocations, _frees) = counted(|| {
        let _pause = CountPause::new();
        black_box(vec![1_u8; 64]).len()
    });
    assert_eq!(allocations, 0);
}

// one range repeated until its segment holds all the covers it can: a newer cover in the next segment drops the last of
// them from the pool, so the seal keeps every row, and the buffer open gave it still holds them
#[test]
fn full_cover_block() {
    let io = SimulatedIo::new(Seed(4));
    let store = ReelStore::open(io.clone(), &config()).expect("open");
    // a first seal sets up what any seal touches, as the warm rounds of the gate do
    store.delete_range(META, b"warm-a", b"warm-b").expect("delete");
    store.seal_now();
    settle(&io);

    for _repeat in 0..MOST_RANGE_COVERS {
        store.delete_range(META, b"range-a", b"range-b").expect("delete");
    }
    store.delete_range(META, b"range-a", b"range-b").expect("delete");
    let volume = &store.state.volumes[0];
    store.state.seal_most_covers(&mut hold(&volume.claims), volume);
    assert!(hold(&volume.claims).open.iter().any(|segment| segment.phase == SegmentPhase::Sealing));

    let sealed = counted(|| settle(&io));

    assert_eq!((sealed.1, sealed.2), (0, 0));
    store.close().expect("close");
}

// the first reservation after an open writes the superblock from a buffer open sized, and later ones reuse it
#[test]
fn first_reservation() {
    let io = SimulatedIo::new(Seed(6));
    let store = ReelStore::open(io.clone(), &config()).expect("open");
    let (is_held, allocations, frees) = counted(|| store.state.reserve_ahead());
    assert!(is_held);
    assert_eq!((allocations, frees), (0, 0));
    store.close().expect("close");
}

// a writer's first wait and wake on the progress line allocate nothing: on a target whose standard library builds a lock and a
// condition at their first use, the line builds them at open
#[test]
fn first_wait_on_the_progress_line() {
    let line = WakeLine::new();
    let ((), allocations, frees) = counted(|| {
        line.wait(line.seen(), Duration::ZERO);
        line.wake();
    });
    assert_eq!((allocations, frees), (0, 0));
}
