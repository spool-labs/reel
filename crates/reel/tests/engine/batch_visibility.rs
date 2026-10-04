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

/// The same column sharded on its first byte, so one batch's keys land in many shards
const SPREAD: ColumnSet = &[ColumnSpec {
    shard_bytes: 1,
    ..COLUMNS[0]
}];

/// Key groups the many-writer test spreads its batches over
const GROUPS: u64 = 8;

/// Keys in one group, every one of them written by each batch on the group
const GROUP_KEYS: u64 = 64;

/// Batches each writer lands in the many-writer test
const WRITER_BATCHES: u64 = 1500;

/// A group's key, led by its place in the group so a batch crosses every shard
fn group_key(group: u64, at: u64) -> [u8; 8] {
    (at << 56 | group).to_be_bytes()
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// Write or delete a whole group with one tag, so a group only ever holds one tag
fn group_batch(group: u64, tag: Option<u64>) -> WriteBatch {
    let mut batch = WriteBatch::new();
    for at in 0..GROUP_KEYS {
        let key = group_key(group, at);
        match tag {
            Some(tag) => batch.put("rows", &key, &[tag.to_le_bytes(), [0x5a; 8]].concat()),
            None => batch.delete("rows", &key),
        }
    }
    batch
}

// many writers at once, and every read of one group sees one batch's tag or none of it
#[test]
fn many_writers_or_nothing() {
    let store = Arc::new(
        ReelStore::open_with_io(
            PathBuf::from("/visibility"),
            ReelConfig {
                active_tails: ThreadBudget::threads(4),
                ..config()
            },
            SPREAD,
            Arc::new(SimIo::new(FaultPlan::new(12))),
        )
        .expect("open"),
    );
    for group in 0..GROUPS {
        Store::write_batch(&*store, group_batch(group, Some(group))).expect("seed");
    }

    let is_writing = Arc::new(AtomicBool::new(true));
    let writers: Vec<_> = (0..4u64)
        .map(|writer| {
            let store = Arc::clone(&store);
            thread::spawn(move || {
                let mut state = 0x9e37_79b9_7f4a_7c15 ^ (writer + 1);
                for round in 0..WRITER_BATCHES {
                    let group = next_random(&mut state) % GROUPS;
                    let tag = (writer + 1) << 32 | round;
                    let tag = (!next_random(&mut state).is_multiple_of(5)).then_some(tag);
                    Store::write_batch(&*store, group_batch(group, tag)).expect("batch");
                }
            })
        })
        .collect();

    let readers: Vec<_> = (0..3u64)
        .map(|reader| {
            let store = Arc::clone(&store);
            let is_writing = Arc::clone(&is_writing);
            thread::spawn(move || {
                let mut state = 0x2545_f491_4f6c_dd1d ^ (reader + 1);
                let mut page = reel::KeyPage::with_lens();
                let (mut reads, mut torn) = (0u64, 0u64);
                while is_writing.load(Ordering::Acquire) {
                    let group = next_random(&mut state) % GROUPS;
                    let picked: Vec<[u8; 8]> = (0..2 + next_random(&mut state) % 15)
                        .map(|_| group_key(group, next_random(&mut state) % GROUP_KEYS))
                        .collect();
                    let asked: Vec<&[u8]> = picked.iter().map(|key| key.as_slice()).collect();
                    let answers = Store::get_many(&*store, "rows", &asked).expect("get many");
                    let tags: Vec<Option<[u8; 8]>> = answers
                        .iter()
                        .map(|answer| {
                            answer
                                .as_ref()
                                .map(|value| value[..8].try_into().expect("tag"))
                        })
                        .collect();
                    if tags.iter().any(|tag| *tag != tags[0]) {
                        torn += 1;
                    }

                    // One page of the whole column, where every group is all there or gone.
                    let whole = (GROUPS * GROUP_KEYS) as usize;
                    store
                        .page(ColumnId(1), std::ops::Bound::Unbounded, whole, &mut page)
                        .expect("page");
                    let mut held = [0u64; GROUPS as usize];
                    for at in 0..page.len() {
                        if let Some(key) = page.key_ref(at) {
                            held[key[7] as usize] += 1;
                        }
                    }
                    if held.iter().any(|count| *count != 0 && *count != GROUP_KEYS) {
                        torn += 1;
                    }
                    reads += 1;
                }
                (reads, torn)
            })
        })
        .collect();

    for writer in writers {
        writer.join().expect("writer");
    }
    is_writing.store(false, Ordering::Release);
    let mut reads = 0u64;
    for reader in readers {
        let (seen, torn) = reader.join().expect("reader");
        assert_eq!(torn, 0, "{torn} of {seen} reads saw part of a batch");
        reads += seen;
    }
    assert!(reads > 0, "the readers never got a read in");
}
