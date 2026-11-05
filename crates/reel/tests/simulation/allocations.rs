//! The zero-allocation gate on the simulated disk: every call of a warm store allocates and frees nothing

use std::hint::black_box;
use std::path::PathBuf;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::io::job::{Job, JobStep};
use reel::io::{Io, IoFile};
use reel::record::codec::Codec;
use reel::segment::segment_tail::SECOND_TAIL_DISTANCE;
use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, FileOffset, Nanoseconds, VolumeId};
use reel_sim::count_pause::CountPause;
use reel_sim::file_image::BitMask;
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_io::SimulatedIo;

use crate::counting::counted;

const ROOT: &str = "/disk/allocations";
const META: ColumnId = ColumnId(1);
const BULK: ColumnId = ColumnId(2);
const BUDGET: ByteCount = ByteCount(160 * 1024 * 1024);
const KEYS: usize = 64;
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
    buf: Vec<u8>,
}

fn config() -> StoreConfig {
    let column = |id: ColumnId, name: &str, codec: Codec| ColumnConfig {
        id,
        name: name.to_string(),
        key_shape: KeyShape::Variable { most: 64 },
        codec,
        volume: VolumeId(0),
    };
    StoreConfig {
        memory_budget: BUDGET,
        volumes: vec![VolumeConfig {
            name: "allocations".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
        }],
        columns: vec![column(META, "meta", Codec::None), column(BULK, "bulk", Codec::Lz4)],
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
    total += both(counted(|| store.delete(BULK, other).expect("delete")));
    let refused = counted(|| store.get(META, DAMAGED_KEY, buf).is_err());
    assert!(refused.0);
    total += both(refused);
    total += both(counted(|| store.clear_damage(META, DAMAGED_END, Some(DAMAGED_END)).expect("clear")));
    if number.is_multiple_of(FLUSH_ROUNDS) {
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
    let io = SimulatedIo::new(Seed(1));
    damage(&io);
    let store = ReelStore::open(io.clone(), &config()).expect("open");
    let mut inputs = Inputs {
        keys: (0..KEYS).map(|number| format!("key-{number:06}").into_bytes()).collect(),
        small: vec![7; 100],
        chunked: (0..CHUNKED_BYTES).map(|index| ((index * 31) >> 3) as u8).collect(),
        compressible: vec![b'x'; CHUNKED_BYTES],
        buf: vec![1; CHUNKED_BYTES * 2],
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
