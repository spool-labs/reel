//! Compaction: what no read can reach goes, and nothing else does

use std::path::{Path, PathBuf};

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::open_segment::data_path;
use reel::engine::reel_store::ReelStore;
use reel::guard::hold;
use reel::io::Io;
use reel::record::codec::Codec;
use reel::segment::segment_tail::SegmentNumber;
use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, Nanoseconds, VolumeId};
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_disk::OperationCount;
use reel_sim::simulated_io::SimulatedIo;

const ROOT: &str = "/disk/compact";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(64 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 20_000;
const CRASH_POINTS: u64 = 40;

type Store = ReelStore<SimulatedIo>;

fn open(io: &SimulatedIo) -> Store {
    let config = StoreConfig {
        memory_budget: BUDGET,
        volumes: vec![VolumeConfig {
            name: "compact".to_string(),
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

// put a key and seal it, and return its segment
fn sealed_put(store: &Store, io: &SimulatedIo, value: &[u8]) -> SegmentNumber {
    store.put(COLUMN, b"key", value).expect("put");
    let number = hold(&store.state.volumes[0].claims).open[0].number;
    store.seal_now();
    settle(io);
    number
}

fn read(store: &Store) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    store.get(COLUMN, b"key", &mut buf).expect("get").then_some(buf)
}

fn has_file(io: &SimulatedIo, number: SegmentNumber) -> bool {
    let data = data_path(Path::new(ROOT), number).to_path_buf();
    io.list_directory(Path::new(ROOT)).expect("list").contains(&data)
}

// a segment with nothing live leaves the table, the summary and the disk, and the newest value still reads
#[test]
fn dead_unlinks() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io);
    let older = sealed_put(&store, &io, b"older");
    let newer = sealed_put(&store, &io, b"newer");

    let volume = &store.state.volumes[0];
    assert!(!volume.table.holds(older));
    assert!(hold(&volume.summary).get(older).is_none());
    assert!(!has_file(&io, older));
    assert!(volume.table.holds(newer));
    assert_eq!(read(&store), Some(b"newer".to_vec()));
    store.close().expect("close");
    assert_eq!(io.open_handle_count(), 0);
}

// a segment with a live row stays
#[test]
fn live_stays() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io);
    store.put(COLUMN, b"other", b"value").expect("put");
    let older = sealed_put(&store, &io, b"older");
    sealed_put(&store, &io, b"newer");

    assert!(store.state.volumes[0].table.holds(older));
    assert!(has_file(&io, older));
    store.close().expect("close");
}

// a cue newer than a segment can still need its rows, when their replacements came after the cue, and the segment goes
// as soon as the cue does
#[test]
fn cue_keeps_segment() {
    let io = SimulatedIo::new(Seed(3));
    let store = open(&io);
    let older = sealed_put(&store, &io, b"older");
    let cue = store.cue().expect("cue");
    sealed_put(&store, &io, b"newer");

    let mut buf = Vec::new();
    assert!(store.state.volumes[0].table.holds(older));
    assert!(cue.get(COLUMN, b"key", &mut buf).expect("get"));
    assert_eq!(buf, b"older");

    drop(cue);
    settle(&io);
    assert!(!store.state.volumes[0].table.holds(older));
    store.close().expect("close");
}

// a crash at any step of an unlink loses nothing, and the table and the disk agree. A crash between a seal's tail and its
// row leaves that segment to a backward walk, which flips nothing, so the older segment can stay, which only understates
#[test]
fn unlink_crashes() {
    for crash_at in 0..CRASH_POINTS {
        for is_power_cut in [false, true] {
            let io = SimulatedIo::new(Seed(4));
            let store = open(&io);
            store.put(COLUMN, b"key", b"older").expect("put");
            let older = hold(&store.state.volumes[0].claims).open[0].number;
            store.seal_now();
            settle(&io);
            store.put(COLUMN, b"key", b"newer").expect("put");
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
            settle(&io);
            assert_eq!(read(&store), Some(b"newer".to_vec()), "crash at {crash_at}");
            assert_eq!(has_file(&io, older), store.state.volumes[0].table.holds(older), "crash at {crash_at}");
            store.close().expect("close");
        }
    }
}
