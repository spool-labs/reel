//! count_prefix and bytes_prefix: exact whatever the walks and merges have reached, and one line a block on a settled column

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::engine::store_scan::StoreScan;
use reel::guard::hold;
use reel::record::codec::Codec;
use reel_api::column_id::ColumnId;
use reel_api::direction::Direction;
use reel_api::scan::ScanStep;
use reel_api::units::{ByteCount, Nanoseconds, RowCount, VolumeId};
use reel_api::usage::ColumnUsage;
use reel_sim::seeded_random::{Seed, SeededRandom};
use reel_sim::simulated_io::SimulatedIo;

use crate::seed_sets::seeds;

const ROOT: &str = "/disk/prefix";
const COLUMN: ColumnId = ColumnId(1);
const PACKED: ColumnId = ColumnId(2);
const BUDGET: ByteCount = ByteCount(192 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 400_000;
const HOOK_STEPS: u64 = 20_000;
const KEYS: u64 = 20_000;
const SMALL_KEYS: u64 = 300;
const ABOVE_PREFIX: u64 = 100_000;
const GHOST_EVERY: u64 = 5;
const MOST_EDGE_READS: u64 = 3;
const PACKED_VALUE: &[u8] = &[b'x'; 2_000];
const RANDOM_ROUNDS: u64 = 300;
const FIXED_SEEDS: [u64; 4] = [0, 1, 2, 3];

type Store = ReelStore<SimulatedIo>;

fn open(io: &SimulatedIo) -> Store {
    let column = |id, name: &str, codec| ColumnConfig {
        id,
        name: name.to_string(),
        key_shape: KeyShape::Variable { most: 64 },
        codec,
        volume: VolumeId(0),
        complete_rows: None,
        least_value: None,
    };
    let config = StoreConfig {
        memory_budget: BUDGET,
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "prefix".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(),
        }],
        columns: vec![column(COLUMN, "column", Codec::None), column(PACKED, "packed", Codec::Lz4)],
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

fn hold_compaction(store: &Store, is_held: bool) {
    store.state.volumes[0].holds.compaction.store(is_held, Ordering::Release);
    store.state.ask_compaction();
}

fn key(number: u64) -> Vec<u8> {
    format!("key-{number:06}").into_bytes()
}

fn put_all(store: &Store) {
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), b"v").expect("put");
    }
}

fn walked_count(mut scan: StoreScan<SimulatedIo>) -> u64 {
    let (mut name, mut count) = (Vec::new(), 0);
    while scan.next_key(&mut name).expect("step") == ScanStep::Found {
        count += 1;
    }
    count
}

fn scan_count(store: &Store, prefix: &[u8]) -> RowCount {
    RowCount(walked_count(store.iter_prefix(COLUMN, prefix).expect("scan")))
}

// a settled ordered column counts by its lines, and reads only the blocks at the prefix's two ends
#[test]
fn settled_by_lines() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io);
    put_all(&store);
    seal(&store, &io);
    store.close().expect("close");
    drop(store);

    let store = open(&io);
    hold_compaction(&store, true);
    settle(&io);
    let before = io.read_count();
    assert_eq!(store.count_prefix(COLUMN, b"key-").expect("count"), RowCount(KEYS));
    assert!(io.read_count().0 - before.0 <= MOST_EDGE_READS, "{} reads", io.read_count().0 - before.0);
    assert_eq!(store.count_prefix(COLUMN, b"key-01").expect("count"), RowCount(10_000));
    assert_eq!(store.count_prefix(COLUMN, b"nothing").expect("count"), RowCount(0));
    store.close().expect("close");
}

// a count with walks pending matches a walk of the keys, though the replaced rows still read as live
#[test]
fn walks_pending() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io);
    hold_compaction(&store, true);
    store.state.volumes[0].holds.walk.store(true, Ordering::Release);
    put_all(&store);
    seal(&store, &io);
    put_all(&store);
    seal(&store, &io);
    assert!(store.state.pending_walks(0) > 0);
    assert_eq!(store.count_prefix(COLUMN, b"key-").expect("count"), scan_count(&store, b"key-"));
    assert_eq!(store.count_prefix(COLUMN, b"key-").expect("count"), RowCount(KEYS));
    store.close().expect("close");
}

// a column with live graves counts them out, in whatever block they sit
#[test]
fn graves_counted_out() {
    let io = SimulatedIo::new(Seed(3));
    let store = open(&io);
    hold_compaction(&store, true);
    for number in 0..KEYS {
        if number.is_multiple_of(GHOST_EVERY) {
            store.delete(COLUMN, &key(number)).expect("delete");
        } else {
            store.put(COLUMN, &key(number), b"v").expect("put");
        }
    }
    seal(&store, &io);
    let expected = RowCount(KEYS - KEYS / GHOST_EVERY);
    assert_eq!(store.count_prefix(COLUMN, b"key-").expect("count"), expected);
    assert_eq!(scan_count(&store, b"key-"), expected);
    store.close().expect("close");
}

// a key with rows in two overlapping segments counts once, walked or not
#[test]
fn two_segments_count_once() {
    let io = SimulatedIo::new(Seed(4));
    let store = open(&io);
    hold_compaction(&store, true);
    put_all(&store);
    seal(&store, &io);
    for number in (0..KEYS).step_by(2) {
        store.put(COLUMN, &key(number), b"w").expect("put");
    }
    seal(&store, &io);
    assert_eq!(store.count_prefix(COLUMN, b"key-").expect("count"), RowCount(KEYS));
    store.close().expect("close");
}

// a segment no walk has reached still counts each version of a key in its lines, so its blocks step their rows
#[test]
fn own_versions_unwalked() {
    let io = SimulatedIo::new(Seed(11));
    let store = open(&io);
    hold_compaction(&store, true);
    store.state.volumes[0].holds.walk.store(true, Ordering::Release);
    put_all(&store);
    put_all(&store);
    seal(&store, &io);
    assert_eq!(store.count_prefix(COLUMN, b"key-").expect("count"), RowCount(KEYS));
    store.close().expect("close");
}

// a walked block with an unwalked segment's row inside its span steps its rows, since its line still counts the replaced row
#[test]
fn overlap_inside_a_block_steps() {
    let io = SimulatedIo::new(Seed(5));
    let store = open(&io);
    hold_compaction(&store, true);
    put_all(&store);
    seal(&store, &io);
    store.state.volumes[0].holds.walk.store(true, Ordering::Release);
    store.put(COLUMN, &key(KEYS / 2), b"w").expect("put");
    seal(&store, &io);
    assert_eq!(store.count_prefix(COLUMN, b"key-").expect("count"), RowCount(KEYS));
    store.close().expect("close");
}

// an index key inside a block's span keeps it from going by its line
#[test]
fn index_key_inside_a_block_steps() {
    let io = SimulatedIo::new(Seed(6));
    let store = open(&io);
    hold_compaction(&store, true);
    put_all(&store);
    seal(&store, &io);
    store.put(COLUMN, &key(KEYS / 2), b"w").expect("put");
    store.delete(COLUMN, &key(KEYS / 3)).expect("delete");
    assert_eq!(store.count_prefix(COLUMN, b"key-").expect("count"), RowCount(KEYS - 1));
    store.close().expect("close");
}

// a pending cover keeps every block off its line, since a row it hides can still read as live
#[test]
fn cover_steps() {
    let io = SimulatedIo::new(Seed(7));
    let store = open(&io);
    hold_compaction(&store, true);
    put_all(&store);
    seal(&store, &io);
    store.delete_range(COLUMN, &key(100), &key(KEYS - 100)).expect("delete");
    assert_eq!(store.count_prefix(COLUMN, b"key-").expect("count"), RowCount(200));
    store.close().expect("close");
}

// a count at a cue takes what the cue can see, whatever sealed after it
#[test]
fn count_at_a_cue() {
    let io = SimulatedIo::new(Seed(8));
    let store = open(&io);
    hold_compaction(&store, true);
    put_all(&store);
    seal(&store, &io);
    let cue = store.cue().expect("cue");
    for number in 0..KEYS {
        store.put(COLUMN, format!("new-{number:06}").as_bytes(), b"v").expect("put");
    }
    seal(&store, &io);
    for number in 0..KEYS {
        store.delete(COLUMN, &key(number)).expect("delete");
    }
    store.put(COLUMN, b"key-new", b"v").expect("put");
    seal(&store, &io);
    assert_eq!(cue.count_prefix(COLUMN, b"key-").expect("count"), RowCount(KEYS));
    assert_eq!(cue.count_prefix(COLUMN, b"new-").expect("count"), RowCount(0));
    assert_eq!(store.count_prefix(COLUMN, b"key-").expect("count"), RowCount(1));
    drop(cue);
    store.close().expect("close");
}

// a list that moves after a block went by its line starts the count over, so no block counts twice
#[test]
fn list_moves_under_a_count() {
    let io = SimulatedIo::new(Seed(9));
    let store = open(&io);
    put_all(&store);
    for number in 0..KEYS {
        store.put(COLUMN, &key(ABOVE_PREFIX + number), b"v").expect("put");
    }
    seal(&store, &io);
    store.close().expect("close");
    drop(store);

    let store = open(&io);
    settle(&io);
    store.put(COLUMN, b"zz", b"v").expect("put");
    let (state, disk, reads) = (Arc::clone(&store.state), io.clone(), Arc::new(AtomicU64::new(0)));
    let seen = Arc::clone(&reads);
    io.on_every_read(Some(Arc::new(move || {
        if seen.fetch_add(1, Ordering::AcqRel) == 1 {
            state.seal_fullest(&state.volumes[0]);
            for _step in 0..HOOK_STEPS {
                let _stepped = disk.step_any();
                disk.advance_clock(TICK);
            }
        }
    })));
    let count = store.count_prefix(COLUMN, b"key-0").expect("count");
    io.on_every_read(None);
    assert!(reads.load(Ordering::Acquire) > 1);
    assert_eq!(count, RowCount(KEYS));
    store.close().expect("close");
}

// bytes are what the values take on disk, compressed on a compressing column, and no record is read to find them
#[test]
fn stored_bytes() {
    let io = SimulatedIo::new(Seed(10));
    let store = open(&io);
    for number in 0..SMALL_KEYS {
        store.put(PACKED, &key(number), PACKED_VALUE).expect("put");
    }
    seal(&store, &io);
    store.close().expect("close");
    drop(store);

    let store = open(&io);
    hold_compaction(&store, true);
    settle(&io);
    let mut usage = ColumnUsage::default();
    store.column_disk_usage(PACKED, &mut usage).expect("usage");
    let blocks = hold(&store.state.volumes[0].summary).rows().map(|row| row.facts.block_count).sum::<u32>();
    let before = io.read_count();
    let bytes = store.bytes_prefix(PACKED, b"key-").expect("bytes");
    assert_eq!(bytes, usage.live);
    assert!(bytes.0 < SMALL_KEYS * PACKED_VALUE.len() as u64);
    assert!(io.read_count().0 - before.0 <= u64::from(blocks));
    assert_eq!(store.bytes_prefix(PACKED, b"nothing").expect("bytes"), ByteCount(0));
    store.close().expect("close");
}

// random writes, seals and merges keep a count equal to a walk of the same keys
#[test]
fn random_against_a_walk() {
    for seed in seeds(FIXED_SEEDS) {
        let io = SimulatedIo::new(Seed(seed));
        let store = open(&io);
        let mut random = SeededRandom::new(Seed(seed));
        for round in 0..RANDOM_ROUNDS {
            let number = random.below(SMALL_KEYS);
            match random.below(10) {
                0..=5 => store.put(COLUMN, &key(number), b"v").expect("put"),
                6 | 7 => store.delete(COLUMN, &key(number)).expect("delete"),
                8 => store.delete_range(COLUMN, &key(number), &key(number + random.below(30))).expect("delete"),
                _not_written => seal(&store, &io),
            }
            if round % 25 == 0 {
                for prefix in [&b"key-"[..], b"key-0001", b"key-00015"] {
                    assert_eq!(store.count_prefix(COLUMN, prefix).expect("count"), scan_count(&store, prefix), "seed {seed}");
                }
            }
        }
        settle(&io);
        assert_eq!(store.count_prefix(COLUMN, b"key-").expect("count"), scan_count(&store, b"key-"), "seed {seed}");
        assert_eq!(walked_count(store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan")), store.count_prefix(COLUMN, b"").expect("count").0);
        store.close().expect("close");
    }
}
