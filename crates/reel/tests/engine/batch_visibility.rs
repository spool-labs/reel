//! A batch is visible whole or not at all, never halfway through its publish
//!
//! A batch lands on the device as one write and one sync, but the index moves key by
//! key behind a publish barrier, and a reader that caught that loop halfway would
//! answer from a state the volume was never in. The writer alternates a batch that
//! writes every key with one that deletes every key, so all present and all gone are
//! the only two states, and anything between them is the defect. A follower applying
//! a catch-up pass is held to the same rule.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use tempfile::TempDir;

use reel_core::{Store, WriteBatch};

use reel::io::fault::FaultPlan;
use reel::io::sim_backend::SimIo;
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape, Preallocate, ReelConfig,
    ReelStore, SyncPolicy, ThreadBudget,
};

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: "rows",
    key_width: KeyWidth::Fixed(8),
    shard_bytes: 0,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

/// Keys one batch carries, enough that publishing them takes a visible while
const KEYS: u64 = 48;

/// Rounds the writer alternates over
const ROUNDS: u64 = 400;

/// Readers looking at the column while the writer works
const READERS: usize = 3;

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(4 * 1024 * 1024),
        alloc_chunk: ByteCount::from_bytes(256 * 1024),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        ..ReelConfig::default()
    }
}

fn open() -> ReelStore {
    ReelStore::open_with_io(
        PathBuf::from("/visibility"),
        config(),
        COLUMNS,
        Arc::new(SimIo::new(FaultPlan::new(11))),
    )
    .expect("open")
}

fn all_keys() -> Vec<[u8; 8]> {
    (0..KEYS).map(u64::to_be_bytes).collect()
}

/// A thread that reads every key at once until told to stop, counting torn reads
///
/// A read spanning every key must find them all present or all gone, since those are
/// the only two states the writer leaves the volume in.
fn tearing_reader(
    store: Arc<ReelStore>,
    is_writing: Arc<AtomicBool>,
    start: Arc<Barrier>,
) -> thread::JoinHandle<(u64, u64)> {
    thread::spawn(move || {
        let keys = all_keys();
        let asked: Vec<&[u8]> = keys.iter().map(|key| key.as_slice()).collect();
        let mut torn = 0u64;
        let mut reads = 0u64;
        start.wait();
        while is_writing.load(Ordering::Acquire) {
            let answers = Store::get_many(&*store, "rows", &asked).expect("get many");
            let present = answers.iter().filter(|answer| answer.is_some()).count();
            if present != 0 && present != asked.len() {
                torn += 1;
            }
            reads += 1;
        }
        (reads, torn)
    })
}

/// Write every key, then delete every key, and again, in batches of the whole set
fn alternating_rounds(store: &ReelStore, keys: &[[u8; 8]], payload: &[u8]) {
    for round in 0..ROUNDS {
        let is_put_round = round % 2 == 0;
        let mut batch = WriteBatch::new();
        for key in keys {
            if is_put_round {
                batch.put("rows", key, payload);
            } else {
                batch.delete("rows", key);
            }
        }
        Store::write_batch(store, batch).expect("write batch");
    }
}

// a follower applying a pass shows the same all-or-nothing to its own readers
//
// The simulator serves one store at a time, so this leg runs on a real directory,
// which is what a follower is anyway: a second process reading the writer's files.
#[test]
fn follower_or_nothing() {
    let root = TempDir::new().expect("tempdir");
    let writer = ReelStore::open(root.path().to_path_buf(), config(), COLUMNS).expect("open");
    let keys = all_keys();
    let payload = vec![0x27u8; 96];

    let mut batch = WriteBatch::new();
    for key in &keys {
        batch.put("rows", key, &payload);
    }
    Store::write_batch(&writer, batch).expect("first batch");

    let follower = Arc::new(
        ReelStore::open_read_only(root.path().to_path_buf(), config(), COLUMNS)
            .expect("read only open"),
    );
    let is_writing = Arc::new(AtomicBool::new(true));
    let start = Arc::new(Barrier::new(3));

    let following = {
        let follower = Arc::clone(&follower);
        let is_writing = Arc::clone(&is_writing);
        let start = Arc::clone(&start);
        thread::spawn(move || {
            start.wait();
            while is_writing.load(Ordering::Acquire) {
                follower.refresh().expect("refresh");
            }
        })
    };

    let reader = tearing_reader(
        Arc::clone(&follower),
        Arc::clone(&is_writing),
        Arc::clone(&start),
    );

    start.wait();
    alternating_rounds(&writer, &keys, &payload);
    is_writing.store(false, Ordering::Release);

    let (reads, torn) = reader.join().expect("reader");
    following.join().expect("follower");

    assert_eq!(
        torn, 0,
        "a follower read caught {torn} of {reads} passes halfway"
    );
    assert!(reads > 0, "the follower never got a read in");
}

// a read spanning every key sees all of a batch or none of it, never a prefix
#[test]
fn batch_or_nothing() {
    let store = Arc::new(open());
    let is_writing = Arc::new(AtomicBool::new(true));
    let start = Arc::new(Barrier::new(READERS + 1));
    let payload = vec![0x27u8; 96];

    let mut readers = Vec::with_capacity(READERS);
    for _ in 0..READERS {
        let store = Arc::clone(&store);
        let is_writing = Arc::clone(&is_writing);
        let start = Arc::clone(&start);
        readers.push(tearing_reader(store, is_writing, start));
    }

    let keys = all_keys();
    start.wait();
    alternating_rounds(&store, &keys, &payload);
    is_writing.store(false, Ordering::Release);

    let mut reads = 0u64;
    for reader in readers {
        let (seen, torn) = reader.join().expect("reader");
        assert_eq!(torn, 0, "a read caught {torn} of {seen} batches halfway");
        reads += seen;
    }

    assert!(reads > 0, "the readers never got a read in");
}
