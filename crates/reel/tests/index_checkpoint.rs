//! The index written down at a cue, and what an open may believe of it
//!
//! An open that sweeps every footer pays per key rather than per segment, which at a
//! huge key count is the whole of the start-up time. These hold the other half: the
//! file is read back instead of the footers, an unreadable or outdated file costs the
//! open the sweep it would have done anyway, and a file believed too far never hands
//! back a deleted row or drops a live one.

#[allow(dead_code)]
mod harness;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use reel::index::persisted::{PersistedIndex, PERSISTED_INDEX};
use reel::io::fault::FaultPlan;
use reel::io::sim_backend::{DurableImage, SimIo};
use reel::sync::rendezvous;
use reel::{
    segment_file_name, ByteCount, ColumnSet, CompactRate, IndexResidency, Preallocate, ReelConfig,
    ReelStore, SyncPolicy, ThreadBudget,
};
use reel_mock::MemoryStore;

use harness::observe::observe;
use harness::op_stream::{self, StreamOp};
use harness::wire::{apply_mutation, TEST_COLUMNS};

/// Virtual bulk root the simulator files live under
const REEL_ROOT: &str = "/bulk";

/// Segment size that rolls several times over the streams here
const SEGMENT_BYTES: u64 = 16 * 1024;

/// Space reserved ahead of the write head per allocation step
const ALLOC_CHUNK: u64 = 4 * 1024;

/// Length of the streams the targeted tests drive
const STREAM_LEN: usize = 160;

/// Compaction passes a test drives when it wants the volume drained
const DRAIN_PASSES: usize = 64;

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(SEGMENT_BYTES),
        alloc_chunk: ByteCount::from_bytes(ALLOC_CHUNK),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::EveryPut,
        active_tails: ThreadBudget::threads(1),
        ..ReelConfig::default()
    }
}

/// The same volume with the compaction gate lifted, for a test that wants it drained
///
/// The rate gate prices a device, so a stream this small would spend its passes held
/// back, and a stale file with nothing retired under it is the fresh case renamed.
fn draining_config() -> ReelConfig {
    ReelConfig {
        compact_mbps: CompactRate::Mbps(100_000),
        ..config()
    }
}

fn root() -> PathBuf {
    PathBuf::from(REEL_ROOT)
}

fn open(sim: &SimIo, config: ReelConfig, columns: ColumnSet) -> ReelStore {
    ReelStore::open_with_io(root(), config, columns, Arc::new(sim.clone())).expect("open reel")
}

/// Path the persisted index takes in the simulated volume
fn index_path() -> PathBuf {
    root().join(PERSISTED_INDEX)
}

/// Replace one file's bytes in a durable image, or add it where it is absent
fn replace_file(image: &mut DurableImage, path: &Path, bytes: Vec<u8>) {
    for entry in image.iter_mut() {
        if entry.0 == path {
            entry.1 = bytes;
            return;
        }
    }
    image.push((path.to_path_buf(), bytes));
}

/// The same image with one file taken out of it, as a volume that never wrote it
fn without_file(image: &DurableImage, path: &Path) -> DurableImage {
    image
        .iter()
        .filter(|(name, _)| name != path)
        .cloned()
        .collect()
}

/// The bytes one file holds in a durable image
fn held(image: &DurableImage, path: &Path) -> Option<Vec<u8>> {
    for (name, bytes) in image {
        if name == path {
            return Some(bytes.clone());
        }
    }
    None
}

/// Drive a stream against the reel and a memory store, so one oracle stands for both
fn run(store: &ReelStore, memory: &MemoryStore, ops: &[StreamOp]) {
    for op in ops {
        apply_mutation(memory, op).expect("memory mutation");
        apply_mutation(store, op).expect("reel mutation");
    }
    store.flush().expect("flush");
    // The sweep the maintenance tick would run. Nothing retires while a cover is
    // still owed one, so a stream that dropped a group leaves the compactor idle.
    while store.sweep_covers().expect("sweep covers") {}
}

/// Assert a reopened volume serves exactly what the oracle does
fn assert_agrees(reopened: &ReelStore, memory: &MemoryStore, context: &str) {
    let want = observe(memory);
    let got = observe(reopened);
    assert_eq!(
        want.records, got.records,
        "the reopened volume diverged from the oracle {context}",
    );
}

// a volume writes its index down and reads it back at the next open
//
// Measured rather than asserted: the open that finds a file reads far less than the
// same image with the file taken out of it, and both serve the same records.
#[test]
fn the_open_reads_the_index() {
    let sim = SimIo::new(FaultPlan::new(1));
    let memory = MemoryStore::new();
    let ops = op_stream::generate_durable(1, STREAM_LEN);
    let image = {
        let store = open(&sim, config(), TEST_COLUMNS);
        run(&store, &memory, &ops);
        let taken = store.checkpoint_index().expect("index checkpoint");
        assert!(taken.keys > 0, "the file stood for no key");
        assert!(taken.segments > 0, "the file stood for no segment");
        store.close().expect("close");
        sim.durable_image()
    };
    assert!(
        held(&image, &index_path()).is_some(),
        "no index reached the medium"
    );

    let holding = SimIo::from_image(image.clone());
    let reading = open(&holding, config(), TEST_COLUMNS);
    let with_index = holding.read_count();

    let bare = SimIo::from_image(without_file(&image, &index_path()));
    let sweeping = open(&bare, config(), TEST_COLUMNS);
    let sweeping_reads = bare.read_count();

    assert_eq!(
        observe(&reading).records,
        observe(&sweeping).records,
        "the two opens disagree about what the volume holds",
    );
    assert!(
        with_index < sweeping_reads,
        "the open with a file read {with_index} times against the sweep's {sweeping_reads}, so it swept too",
    );
    assert_agrees(&reading, &memory, "after an open from its own index");
}

// a file describing a volume that has moved on never resurrects a row, or loses one
//
// Believed one segment too far the open hands back a row the stream deleted,
// believed one too little it drops a live one.
#[test]
fn a_stale_index_over_a_moved_volume() {
    let sim = SimIo::new(FaultPlan::new(42));
    let memory = MemoryStore::new();
    let store = open(&sim, draining_config(), TEST_COLUMNS);

    run(
        &store,
        &memory,
        &op_stream::generate_durable(42, STREAM_LEN),
    );
    store.checkpoint_index().expect("first index checkpoint");
    let stale = sim
        .durable_bytes(&index_path())
        .expect("the index reached the medium");

    // The volume moves on: another stream over the same key space shadows most of what
    // the file names, then the compactor retires what holds nothing live any more.
    run(&store, &memory, &op_stream::generate_durable(7, STREAM_LEN));
    for _ in 0..DRAIN_PASSES {
        store.compact_once().expect("compact");
    }
    store.checkpoint_index().expect("second index checkpoint");
    let current = sim.durable_bytes(&index_path()).expect("an index");
    assert_ne!(
        current, stale,
        "the volume stands where the first file left it"
    );
    store.close().expect("close");

    let mut image = sim.durable_image();
    let (standing, named) = vouched_for(&image, &stale);
    assert!(
        named > 0,
        "the first file named no segment, so it cannot go stale"
    );
    assert!(
        standing < named,
        "the volume retired none of the {named} segments the file names, so nothing went stale",
    );
    replace_file(&mut image, &index_path(), stale);

    let restored = SimIo::from_image(image);
    let reopened = open(&restored, config(), TEST_COLUMNS);

    assert_agrees(&reopened, &memory, "with a stale index on the volume");
}

/// Segments a file names that still stand at the length it recorded, and how many it names
fn vouched_for(image: &DurableImage, persisted: &[u8]) -> (usize, usize) {
    let persisted = PersistedIndex::unpack(persisted).expect("unpack");
    let mut standing = 0usize;
    for stamp in &persisted.segments {
        let path = root().join(segment_file_name(stamp.segment));
        if held(image, &path).map(|bytes| bytes.len() as u64) == Some(stamp.len) {
            standing += 1;
        }
    }
    (standing, persisted.segments.len())
}

// a file that fails its own checks costs the open a sweep and nothing else
#[test]
fn a_rotted_index_is_ignored() {
    let sim = SimIo::new(FaultPlan::new(3));
    let memory = MemoryStore::new();
    let store = open(&sim, config(), TEST_COLUMNS);
    run(&store, &memory, &op_stream::generate_durable(3, STREAM_LEN));
    store.checkpoint_index().expect("index checkpoint");
    store.close().expect("close");

    let mut image = sim.durable_image();
    let mut rotted = held(&image, &index_path()).expect("an index");
    let middle = rotted.len() / 2;
    rotted[middle] ^= 0xff;
    replace_file(&mut image, &index_path(), rotted);

    let restored = SimIo::from_image(image);
    let reopened = open(&restored, config(), TEST_COLUMNS);

    assert_agrees(&reopened, &memory, "with a rotted index on the volume");
}

// a file cut short is refused whole rather than read as far as it goes
#[test]
fn a_truncated_index_is_ignored() {
    let sim = SimIo::new(FaultPlan::new(4));
    let memory = MemoryStore::new();
    let store = open(&sim, config(), TEST_COLUMNS);
    run(&store, &memory, &op_stream::generate_durable(4, STREAM_LEN));
    store.checkpoint_index().expect("index checkpoint");
    store.close().expect("close");

    let mut image = sim.durable_image();
    let whole = held(&image, &index_path()).expect("an index");
    replace_file(&mut image, &index_path(), whole[..whole.len() / 2].to_vec());

    let restored = SimIo::from_image(image);
    let reopened = open(&restored, config(), TEST_COLUMNS);

    assert_agrees(&reopened, &memory, "with a half index on the volume");
}

// a paging volume leaves its sealed keys in the footers and has none to write down
#[test]
fn a_paging_volume_refuses() {
    let sim = SimIo::new(FaultPlan::new(5));
    let memory = MemoryStore::new();
    let paging = ReelConfig {
        index: IndexResidency::Paged,
        ..config()
    };
    let store = open(&sim, paging, TEST_COLUMNS);
    run(&store, &memory, &op_stream::generate_durable(5, STREAM_LEN));

    assert!(
        store.checkpoint_index().is_err(),
        "a paging volume wrote one anyway"
    );

    store.close().expect("close");
    let image = sim.durable_image();
    assert!(
        held(&image, &index_path()).is_none(),
        "a paging volume left a file"
    );
}

// a follower has no tails to seal, so it does not answer to this name
#[test]
fn a_read_only_volume_refuses() {
    let sim = SimIo::new(FaultPlan::new(6));
    let memory = MemoryStore::new();
    {
        let store = open(&sim, config(), TEST_COLUMNS);
        run(&store, &memory, &op_stream::generate_durable(6, STREAM_LEN));
        store.close().expect("close");
    }

    let follower =
        ReelStore::open_read_only_with_io(root(), config(), TEST_COLUMNS, Arc::new(sim.clone()))
            .expect("open read only");

    assert!(follower.checkpoint_index().is_err(), "a follower wrote one");
}

// a crash before the rename leaves the file that was already there
//
// Parked here the bytes are all down and synced under the staging name, so the
// rename is the only point where what an open reads changes.
#[test]
fn a_crash_before_the_rename_keeps_the_old_file() {
    let sim = SimIo::new(FaultPlan::new(8));
    let memory = MemoryStore::new();
    let store = Arc::new(open(&sim, config(), TEST_COLUMNS));
    run(&store, &memory, &op_stream::generate_durable(8, STREAM_LEN));
    store.checkpoint_index().expect("first index checkpoint");
    let first = sim.durable_bytes(&index_path()).expect("an index");

    run(&store, &memory, &op_stream::generate_durable(9, STREAM_LEN));

    let script = rendezvous::script();
    script.hold("index/persisted-staged");
    let taking = {
        let store = Arc::clone(&store);
        script.cast(move || store.checkpoint_index().expect("second index checkpoint"))
    };
    script.await_reached("index/persisted-staged", 1);

    assert_eq!(
        sim.durable_bytes(&index_path()),
        Some(first.clone()),
        "the published name changed before the rename that publishes it",
    );

    script.release("index/persisted-staged");
    taking.join().expect("checkpoint thread");
    let published = sim.durable_bytes(&index_path()).expect("an index");
    assert_ne!(
        published, first,
        "the released checkpoint published nothing"
    );
    store.close().expect("close");

    let restored = SimIo::from_image(sim.durable_image());
    let reopened = open(&restored, config(), TEST_COLUMNS);

    assert_agrees(
        &reopened,
        &memory,
        "after the released checkpoint published",
    );
}

// what the file never saw is read, and the rows it holds do not outlive their keys
//
// The stream after the cue deletes and overwrites keys whose records still sit in
// the segments the reopen believes and does not read.
#[test]
fn segments_the_file_never_saw() {
    let sim = SimIo::new(FaultPlan::new(11));
    let memory = MemoryStore::new();
    let store = open(&sim, config(), TEST_COLUMNS);

    run(
        &store,
        &memory,
        &op_stream::generate_durable(11, STREAM_LEN),
    );
    let taken = store.checkpoint_index().expect("index checkpoint");
    let vouched = observe(&store).records;
    run(
        &store,
        &memory,
        &op_stream::generate_durable(12, STREAM_LEN),
    );
    let left = observe(&store).records;
    store.close().expect("close");

    let restored = SimIo::from_image(sim.durable_image());
    let reopened = open(&restored, config(), TEST_COLUMNS);

    assert!(
        taken.segments > 0,
        "the file stood for no segment, so nothing was skipped"
    );
    assert!(
        reopened.sequence() > taken.at,
        "the second stream wrote nothing past the cue, so no segment is unseen",
    );
    assert!(
        vouched
            .iter()
            .any(|(key, _)| !left.iter().any(|(theirs, _)| theirs == key)),
        "the second stream deleted none of the keys the file vouches for",
    );
    assert_agrees(&reopened, &memory, "over segments written after the file");
}
