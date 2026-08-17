//! A checkpoint is the volume at a cue, and opens as one
//!
//! The copy is exactly the volume at the sequence number it reports, nothing written
//! afterwards reaches it, a restore is an open rather than a procedure, and the copy
//! survives the original compacting away every segment it was made from.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use tempfile::TempDir;

use reel::config::{IndexResidency, ReelConfig, SyncPolicy, ThreadBudget};
use reel::format::column::{Codec, ColumnId, ColumnSpec, MapShape, RecordKey};
use reel::index::persisted::PERSISTED_INDEX;
use reel::reel::checkpoint::staging_of;
use reel::sync::rendezvous;
use reel::units::ByteCount;
use reel::{CompactPass, KeyWidth, ReelStore};
use reel_core::Value;

const RECORDS: ColumnId = ColumnId(1);

const COLUMNS: &[ColumnSpec] = &[ColumnSpec {
    id: RECORDS,
    name: "records",
    key_width: KeyWidth::Fixed(34),
    shard_bytes: 2,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

fn key(at: u32) -> RecordKey {
    let mut bytes = [0u8; 34];
    bytes[..2].copy_from_slice(&7u16.to_be_bytes());
    bytes[2..6].copy_from_slice(&at.to_be_bytes());
    RecordKey::from_bytes(RECORDS, &bytes).expect("key")
}

/// Small segments, so a modest write count rolls several and the link set is real
fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::mb(1),
        alloc_chunk: ByteCount::from_bytes(64 * 1024),
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(2),
        index: IndexResidency::Resident,
        ..ReelConfig::default()
    }
}

/// The same volume, armed to keep an index of its own beside the segments
fn checkpointing_config() -> ReelConfig {
    ReelConfig {
        index_checkpoint: true,
        ..config()
    }
}

fn open(dir: &Path) -> ReelStore {
    open_with(dir, config())
}

fn open_with(dir: &Path, config: ReelConfig) -> ReelStore {
    ReelStore::open(dir.to_path_buf(), config, COLUMNS).expect("open")
}

fn fill(store: &ReelStore, range: std::ops::Range<u32>, byte: u8) {
    for at in range {
        store.put(&key(at), &vec![byte; 4_096]).expect("put");
    }
    store.flush().expect("flush");
}

fn segment_files(dir: &Path) -> BTreeSet<String> {
    std::fs::read_dir(dir)
        .expect("read dir")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".reel"))
        .collect()
}

// the copy holds what the volume held at the cue, and opens without a procedure
#[test]
fn a_checkpoint_opens_as_the_volume_it_copied() {
    let home = TempDir::new().expect("tempdir");
    let live = home.path().join("live");
    let copy = home.path().join("copy");

    let store = open(&live);
    fill(&store, 0..400, 0xa1);

    let taken = store.checkpoint(&copy).expect("checkpoint");
    assert!(taken.segments > 0, "a filled volume linked no segments");
    assert!(copy.is_dir(), "the checkpoint did not land on its target");
    assert!(
        !staging_of(&copy).exists(),
        "the staging directory outlived the rename",
    );

    let restored = open(&copy);
    for at in 0..400u32 {
        assert_eq!(
            restored.get(&key(at)).expect("read").map(Value::into_vec),
            Some(vec![0xa1; 4_096]),
            "key {at} did not survive the checkpoint",
        );
    }
    assert_eq!(
        restored.totals().count,
        store.totals().count,
        "the copy holds a different number of keys",
    );
}

// nothing written after the cue reaches the copy, and the original keeps it
#[test]
fn writes_after_the_cue_stay_out_of_the_copy() {
    let home = TempDir::new().expect("tempdir");
    let live = home.path().join("live");
    let copy = home.path().join("copy");

    let store = open(&live);
    fill(&store, 0..200, 0xb2);
    let taken = store.checkpoint(&copy).expect("checkpoint");

    // Written afterwards, and an overwrite of a key the copy already holds.
    fill(&store, 200..400, 0xc3);
    store.put(&key(0), &vec![0xff; 4_096]).expect("overwrite");
    store.flush().expect("flush");

    let restored = open(&copy);
    for at in 200..400u32 {
        assert_eq!(
            restored.get(&key(at)).expect("read"),
            None,
            "key {at} was written after the cue and is in the copy",
        );
    }
    assert_eq!(
        restored.get(&key(0)).expect("read").map(Value::into_vec),
        Some(vec![0xb2; 4_096]),
        "the copy took a version written after the cue",
    );
    assert_eq!(
        store.get(&key(0)).expect("read").map(Value::into_vec),
        Some(vec![0xff; 4_096]),
        "the live volume lost its own newer version",
    );
    assert!(
        taken.at < store.sequence(),
        "the cue did not sit below what came after"
    );
}

// the copy is whole once the original has compacted every segment it came from
//
// The volume unlinks its own name for a segment and the checkpoint's link keeps the
// inode alive, so a copy that read through to the original would end up empty.
#[test]
fn the_copy_survives_the_original_compacting_it_away() {
    let home = TempDir::new().expect("tempdir");
    let live = home.path().join("live");
    let copy = home.path().join("copy");

    let store = open(&live);
    fill(&store, 0..300, 0xd4);
    store.checkpoint(&copy).expect("checkpoint");
    let linked = segment_files(&copy);
    assert!(!linked.is_empty(), "nothing was linked");

    // Overwrite everything, so every segment the checkpoint linked goes wholly dead.
    fill(&store, 0..300, 0xe5);
    for _ in 0..64 {
        if matches!(store.compact_once().expect("compact"), CompactPass::Idle) {
            break;
        }
    }
    store.flush().expect("flush");

    let left = segment_files(&live);
    assert!(
        linked.iter().any(|name| !left.contains(name)),
        "the volume retired none of the linked segments, so this proves nothing",
    );

    let restored = open(&copy);
    for at in 0..300u32 {
        assert_eq!(
            restored.get(&key(at)).expect("read").map(Value::into_vec),
            Some(vec![0xd4; 4_096]),
            "key {at} was lost when the original retired its segment",
        );
    }
}

// a copy taken from a volume that keeps an index carries one of its own
//
// The index over exactly the linked segments comes from the same cue, so the restore
// reads one file rather than every footer. It lands on home, since its rows name
// segments across every piece.
#[test]
fn the_copy_carries_its_own_index() {
    let home = TempDir::new().expect("tempdir");
    let live = home.path().join("live");
    let copy = home.path().join("copy");

    let store = open_with(&live, checkpointing_config());
    fill(&store, 0..400, 0x28);
    store.checkpoint(&copy).expect("checkpoint");

    assert!(
        copy.join(PERSISTED_INDEX).is_file(),
        "the copy took the segments and left the index behind",
    );

    let restored = open_with(&copy, checkpointing_config());
    for at in 0..400u32 {
        assert_eq!(
            restored.get(&key(at)).expect("read").map(Value::into_vec),
            Some(vec![0x28; 4_096]),
            "key {at} did not survive a restore through the copy's own index",
        );
    }
    assert_eq!(
        restored.totals().count,
        store.totals().count,
        "the copy holds a different number of keys",
    );
}

// a target that exists is refused rather than published over
#[test]
fn a_checkpoint_refuses_a_target_that_exists() {
    let home = TempDir::new().expect("tempdir");
    let live = home.path().join("live");
    let copy = home.path().join("copy");

    let store = open(&live);
    fill(&store, 0..40, 0xf6);
    store.checkpoint(&copy).expect("first checkpoint");

    let again = store.checkpoint(&copy);
    assert!(
        again.is_err(),
        "a second checkpoint published over the first"
    );

    // And the refusal leaves the first one exactly as it was.
    let restored = open(&copy);
    assert_eq!(
        restored.get(&key(0)).expect("read").map(Value::into_vec),
        Some(vec![0xf6; 4_096]),
    );
}

// a read-only volume has no tails to seal, so it does not answer to this name
#[test]
fn a_read_only_volume_refuses_to_checkpoint() {
    let home = TempDir::new().expect("tempdir");
    let live = home.path().join("live");
    let copy = home.path().join("copy");

    {
        let store = open(&live);
        fill(&store, 0..40, 0x17);
    }

    let follower =
        ReelStore::open_read_only(live.clone(), config(), COLUMNS).expect("open read only");
    assert!(
        follower.checkpoint(&copy).is_err(),
        "a follower answered a promise it cannot make",
    );
}

// a crash before the rename leaves a staging directory and no target
//
// Parked at the rename the links are all down and synced, so this is what a crash
// leaves. The staging name is also the refusal the next attempt meets, which makes
// the leftover a thing to sweep rather than a thing to build on.
#[test]
fn a_crash_before_the_rename_leaves_only_staging() {
    let home = TempDir::new().expect("tempdir");
    let live = home.path().join("live");
    let copy = home.path().join("copy");
    let staging = staging_of(&copy);

    let store = Arc::new(open(&live));
    fill(&store, 0..400, 0xd4);

    let script = rendezvous::script();
    script.hold("checkpoint/staged");

    let taking = {
        let store = Arc::clone(&store);
        let copy = copy.clone();
        script.cast(move || store.checkpoint(&copy).expect("checkpoint"))
    };
    script.await_reached("checkpoint/staged", 1);

    assert!(
        staging.is_dir(),
        "the links went somewhere other than staging"
    );
    assert!(
        !copy.exists(),
        "the target exists before the rename that publishes it"
    );
    assert!(
        !segment_files(&staging).is_empty(),
        "the staging directory holds no segments, so the link pass did nothing",
    );
    // The volume is untouched by a checkpoint that never finished.
    assert_eq!(
        store.get(&key(0)).expect("read").map(Value::into_vec),
        Some(vec![0xd4; 4_096]),
        "the live volume lost a key to a checkpoint standing at its rename",
    );

    script.release("checkpoint/staged");
    taking.join().expect("checkpoint thread");
    assert!(copy.is_dir(), "the released checkpoint did not publish");
    assert!(
        !staging.exists(),
        "the staging directory outlived the rename"
    );
}

// a crash after the rename leaves a whole copy, one that opens
//
// The parent directory is not synced yet, so only the weaker promise is asserted:
// the copy is complete and readable. Whether the name survives a power cut is the
// parent sync's job, not something a test in this process can ask.
#[test]
fn a_crash_after_the_rename_leaves_a_whole_copy() {
    let home = TempDir::new().expect("tempdir");
    let live = home.path().join("live");
    let copy = home.path().join("copy");

    let store = Arc::new(open(&live));
    fill(&store, 0..400, 0xe5);

    let script = rendezvous::script();
    script.hold("checkpoint/published");

    let taking = {
        let store = Arc::clone(&store);
        let copy = copy.clone();
        script.cast(move || store.checkpoint(&copy).expect("checkpoint"))
    };
    script.await_reached("checkpoint/published", 1);

    assert!(
        copy.is_dir(),
        "the target is not there after its own rename"
    );
    assert!(
        !staging_of(&copy).exists(),
        "the staging name survived the rename that consumed it",
    );
    let staged = segment_files(&copy);

    script.release("checkpoint/published");
    taking.join().expect("checkpoint thread");

    // What stood on disk at the rename, not what the call did afterwards.
    assert_eq!(
        segment_files(&copy),
        staged,
        "the copy changed after its rename"
    );
    let restored = open(&copy);
    for at in 0..400u32 {
        assert_eq!(
            restored.get(&key(at)).expect("read").map(Value::into_vec),
            Some(vec![0xe5; 4_096]),
            "key {at} is missing from a copy that was whole at its rename",
        );
    }
}
