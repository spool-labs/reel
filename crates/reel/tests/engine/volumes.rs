//! A reel spanning two roots: the scan finds what either holds, a moved
//! segment keeps serving, the manifest refuses a missing mount, pinned tails
//! put a stream on every volume, and a full volume's draw hops to the next
//!
//! Segment files copied between roots, with the list in config, open as the same
//! store. The ENOSPC coverage is a positional sweep rather than a named op, so it
//! keeps holding when open's op sequence moves.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;

use reel::config::{ReelConfig, SyncPolicy, ThreadBudget, VolumeSpec};
use reel::format::column::{Codec, ColumnId, ColumnSet, ColumnSpec, MapShape, RecordKey};
use reel::io::fault::{FaultKind, FaultPlan};
use reel::io::sim_backend::SimIo;
use reel::units::ByteCount;
use reel::{KeyWidth, Preallocate, ReelStore};

const ROWS: ColumnId = ColumnId(1);

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ROWS,
    name: "rows",
    key_width: KeyWidth::Fixed(16),
    shard_bytes: 0,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

fn key(at: u64) -> RecordKey {
    let mut bytes = [0u8; 16];
    bytes[8..].copy_from_slice(&at.to_be_bytes());
    RecordKey::from_bytes(ROWS, &bytes).expect("key")
}

fn config(extra: &[PathBuf]) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(256 * 1024),
        alloc_chunk: ByteCount::from_bytes(64 * 1024),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        volumes: extra.iter().cloned().map(VolumeSpec::fast).collect(),
        ..ReelConfig::default()
    }
}

/// The segment files a root holds, smallest id first
fn segments_in(root: &Path) -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = std::fs::read_dir(root)
        .expect("read root")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "reel"))
        .collect();
    found.sort();
    found
}

// a sealed segment moved to another configured root keeps serving its records
#[test]
fn a_moved_segment_keeps_serving() {
    let home = TempDir::new().expect("home");
    let extra = TempDir::new().expect("extra");
    let extras = [extra.path().to_path_buf()];

    let store = ReelStore::open(home.path().to_path_buf(), config(&extras), COLUMNS).expect("open");
    let payload = vec![0x5Au8; 8 * 1024];
    for at in 0..120u64 {
        store.put(&key(at), &payload).expect("put");
    }
    store.close().expect("close");
    drop(store);

    // Single-threaded puts all ride the first tail, which pins home, so the
    // sealed run sits there.
    let sealed = segments_in(home.path());
    assert!(
        sealed.len() > 1,
        "the volume sealed only {} segments",
        sealed.len()
    );
    let moved = &sealed[0];
    let landed = extra.path().join(moved.file_name().expect("name"));
    std::fs::rename(moved, &landed).expect("move");

    let reopened =
        ReelStore::open(home.path().to_path_buf(), config(&extras), COLUMNS).expect("reopen");
    for at in 0..120u64 {
        assert!(
            reopened.get(&key(at)).expect("get").is_some(),
            "key {at} went missing after the move"
        );
    }
    assert!(
        !segments_in(extra.path()).is_empty(),
        "the moved segment left the extra root"
    );
}

// an open whose config dropped a manifest-named root refuses, loudly
#[test]
fn a_missing_volume_refuses_the_open() {
    let home = TempDir::new().expect("home");
    let extra = TempDir::new().expect("extra");
    let extras = [extra.path().to_path_buf()];

    let store = ReelStore::open(home.path().to_path_buf(), config(&extras), COLUMNS).expect("open");
    store.close().expect("close");
    drop(store);

    let refused = ReelStore::open(home.path().to_path_buf(), config(&[]), COLUMNS);
    assert!(
        refused.is_err(),
        "an open missing a manifest-named volume was let through"
    );
}

// the same id standing on two roots is refused rather than resolved by luck
#[test]
fn a_duplicated_segment_refuses_the_open() {
    let home = TempDir::new().expect("home");
    let extra = TempDir::new().expect("extra");
    let extras = [extra.path().to_path_buf()];

    let store = ReelStore::open(home.path().to_path_buf(), config(&extras), COLUMNS).expect("open");
    let payload = vec![0x5Au8; 8 * 1024];
    for at in 0..120u64 {
        store.put(&key(at), &payload).expect("put");
    }
    store.close().expect("close");
    drop(store);

    let sealed = segments_in(home.path());
    assert!(!sealed.is_empty());
    let copied = extra.path().join(sealed[0].file_name().expect("name"));
    std::fs::copy(&sealed[0], &copied).expect("copy");

    let refused = ReelStore::open(home.path().to_path_buf(), config(&extras), COLUMNS);
    assert!(refused.is_err(), "a segment on two volumes was let through");
}

// a manifest-named root whose directory is gone refuses the open
#[test]
fn an_unmounted_volume_refuses_the_open() {
    let home = TempDir::new().expect("home");
    let holder = TempDir::new().expect("holder");
    let vol = holder.path().join("vol");
    std::fs::create_dir(&vol).expect("mkdir");
    let extras = [vol.clone()];

    let store = ReelStore::open(home.path().to_path_buf(), config(&extras), COLUMNS).expect("open");
    store.close().expect("close");
    drop(store);

    std::fs::rename(&vol, holder.path().join("gone")).expect("unmount");
    let refused = ReelStore::open(home.path().to_path_buf(), config(&extras), COLUMNS);
    assert!(refused.is_err(), "an absent volume was read as empty");

    // The unmounted-mountpoint lookalike: an empty directory standing exactly
    // where the volume should be, with no marker to prove it is one.
    std::fs::create_dir(&vol).expect("mountpoint");
    let refused = ReelStore::open(home.path().to_path_buf(), config(&extras), COLUMNS);
    assert!(
        refused.is_err(),
        "an empty mountpoint was read as a fresh volume"
    );
}

// a marker naming another path is a crossed mount, refused by name
#[test]
fn a_crossed_mount_refuses_the_open() {
    let home = TempDir::new().expect("home");
    let extra = TempDir::new().expect("extra");
    let extras = [extra.path().to_path_buf()];

    let store = ReelStore::open(home.path().to_path_buf(), config(&extras), COLUMNS).expect("open");
    store.close().expect("close");
    drop(store);

    std::fs::write(extra.path().join("reel.volume"), "/somebody/elses/volume\n")
        .expect("cross the marker");
    let refused = ReelStore::open(home.path().to_path_buf(), config(&extras), COLUMNS);
    assert!(
        refused.is_err(),
        "another store's volume was accepted in place"
    );
}

// the tail floor gives every volume a pinned tail, visible as one fresh
// segment per root the moment the store opens
#[test]
fn every_volume_opens_with_a_tail_of_its_own() {
    let home = TempDir::new().expect("home");
    let extra = TempDir::new().expect("extra");
    let extras = [extra.path().to_path_buf()];

    let store = ReelStore::open(home.path().to_path_buf(), config(&extras), COLUMNS).expect("open");
    assert!(
        !segments_in(home.path()).is_empty(),
        "home opened without a tail segment"
    );
    assert!(
        !segments_in(extra.path()).is_empty(),
        "the extra volume opened without a tail segment"
    );
    drop(store);
}

// a capacity volume takes no tail and no fresh segment, only its marker
#[test]
fn a_capacity_volume_attracts_nothing_fresh() {
    let home = TempDir::new().expect("home");
    let cold = TempDir::new().expect("cold");
    let mut config = config(&[]);
    config.volumes = vec![VolumeSpec::capacity(cold.path().to_path_buf())];

    let store = ReelStore::open(home.path().to_path_buf(), config, COLUMNS).expect("open");
    let payload = vec![0x5Au8; 8 * 1024];
    for at in 0..120u64 {
        store.put(&key(at), &payload).expect("put");
    }
    store.close().expect("close");
    drop(store);

    assert!(
        segments_in(cold.path()).is_empty(),
        "a fresh segment landed on the capacity tier"
    );
    assert!(!segments_in(home.path()).is_empty());
}

// a volume declared dead opens degraded: its records miss cleanly, the
// survivors serve, and the store keeps taking writes
#[test]
fn a_dead_volume_opens_degraded() {
    let home = TempDir::new().expect("home");
    let holder = TempDir::new().expect("holder");
    let vol = holder.path().join("vol");
    std::fs::create_dir(&vol).expect("mkdir");
    let extras = [vol.clone()];
    let payload = vec![0x5Au8; 8 * 1024];

    let store = ReelStore::open(home.path().to_path_buf(), config(&extras), COLUMNS).expect("open");
    for at in 0..120u64 {
        store.put(&key(at), &payload).expect("put");
    }
    store.close().expect("close");
    drop(store);

    let sealed = segments_in(home.path());
    assert!(sealed.len() > 1);
    let landed = vol.join(sealed[0].file_name().expect("name"));
    std::fs::rename(&sealed[0], &landed).expect("move");
    {
        let whole = ReelStore::open(home.path().to_path_buf(), config(&extras), COLUMNS)
            .expect("whole open");
        for at in 0..120u64 {
            assert!(whole.get(&key(at)).expect("get").is_some());
        }
        whole.close().expect("close");
    }
    std::fs::remove_dir_all(&vol).expect("lose the drive");

    // Without the operator's word the open refuses; with it, the lost records are
    // misses rather than errors.
    let refused = ReelStore::open(home.path().to_path_buf(), config(&extras), COLUMNS);
    assert!(
        refused.is_err(),
        "a lost volume opened without being declared dead"
    );

    let mut degraded_config = config(&[]);
    degraded_config.volumes = vec![VolumeSpec::fast(vol.clone()).declared_dead()];
    let degraded = ReelStore::open(home.path().to_path_buf(), degraded_config, COLUMNS)
        .expect("degraded open");
    assert_eq!(degraded.dead_volumes(), vec![vol.clone()]);
    let mut missing = 0u32;
    for at in 0..120u64 {
        if degraded
            .get(&key(at))
            .expect("a miss, never an error")
            .is_none()
        {
            missing += 1;
        }
    }
    assert!(
        missing > 0,
        "the dead volume's records did not read as missing"
    );
    assert!(
        (missing as usize) < 120,
        "records the survivors hold went missing too"
    );

    degraded
        .put(&key(500), &payload)
        .expect("a write while degraded");
    assert!(degraded.get(&key(500)).expect("get").is_some());
}

// a checkpoint of a multi-volume store is itself a multi-volume store: each
// live volume stages its own piece, and the copy opens and serves everything
#[test]
fn a_checkpoint_spans_the_volumes() {
    let home = TempDir::new().expect("home");
    let extra = TempDir::new().expect("extra");
    let cues = TempDir::new().expect("cues");
    let extras = [extra.path().to_path_buf()];
    let payload = vec![0x5Au8; 8 * 1024];

    let store = ReelStore::open(home.path().to_path_buf(), config(&extras), COLUMNS).expect("open");
    for at in 0..120u64 {
        store.put(&key(at), &payload).expect("put");
    }
    store.close().expect("close");
    drop(store);

    // Put real data on the extra volume, so the copy has to link across both.
    let sealed = segments_in(home.path());
    let landed = extra.path().join(sealed[0].file_name().expect("name"));
    std::fs::rename(&sealed[0], &landed).expect("move");

    let store =
        ReelStore::open(home.path().to_path_buf(), config(&extras), COLUMNS).expect("reopen");
    let target = cues.path().join("cue");
    let taken = store.checkpoint(&target).expect("checkpoint");
    assert!(taken.segments > 0);

    // A name the reel itself uses is refused before anything seals.
    assert!(store.checkpoint(&cues.path().join("000007.reel")).is_err());
    assert!(store.checkpoint(&cues.path().join("reel.volumes")).is_err());
    // Publishing over the copy just taken is refused too.
    assert!(store.checkpoint(&target).is_err());
    drop(store);

    let piece = extra.path().join("cue");
    assert!(target.is_dir(), "the home piece did not publish");
    assert!(piece.is_dir(), "the extra volume's piece did not publish");

    let mut copy_config = config(&[]);
    copy_config.volumes = vec![VolumeSpec::fast(piece)];
    let copy = ReelStore::open(target, copy_config, COLUMNS).expect("the copy opens");
    for at in 0..120u64 {
        assert!(
            copy.get(&key(at)).expect("get").is_some(),
            "key {at} went missing from the copy"
        );
    }
}

// a piece left by an unfinished checkpoint refuses the next one by name
#[test]
fn a_leftover_piece_refuses_the_next_checkpoint() {
    let home = TempDir::new().expect("home");
    let extra = TempDir::new().expect("extra");
    let cues = TempDir::new().expect("cues");
    let extras = [extra.path().to_path_buf()];

    let store = ReelStore::open(home.path().to_path_buf(), config(&extras), COLUMNS).expect("open");
    std::fs::create_dir(extra.path().join("cue")).expect("debris");
    assert!(
        store.checkpoint(&cues.path().join("cue")).is_err(),
        "a checkpoint built on another's debris"
    );
}

// destroying a multi-volume store takes every manifest-named root with it
#[test]
fn destroy_walks_the_manifest() {
    let home = TempDir::new().expect("home");
    let extra = TempDir::new().expect("extra");
    let extras = [extra.path().to_path_buf()];

    let store = ReelStore::open(home.path().to_path_buf(), config(&extras), COLUMNS).expect("open");
    store.close().expect("close");
    drop(store);

    ReelStore::destroy(home.path()).expect("destroy");
    assert!(!home.path().exists(), "home survived its own destroy");
    assert!(
        !extra.path().exists(),
        "the extra volume survived the destroy"
    );
}

/// Simulated two-volume config, small segments so a short run rolls
///
/// Full preallocation puts exactly one allocate op on each segment creation, which
/// is the op the ENOSPC sweeps aim at.
fn sim_config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(32 * 1024),
        alloc_chunk: ByteCount::from_bytes(32 * 1024),
        preallocate: Preallocate::Full,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        volumes: vec![VolumeSpec::fast("/extra")],
        ..ReelConfig::default()
    }
}

// one ENOSPC anywhere in the stream never fails a write and never leaves a
// segment standing on two volumes
//
// A single allocate failure is scheduled at every op position of the run in turn.
// At the positions holding an allocate that is the draw hopping volumes, open
// included; everywhere else the fault is a no-op.
#[test]
fn a_full_volume_hops_the_draw() {
    let payload = vec![0x5Au8; 4 * 1024];
    let mut at = 0u64;
    loop {
        let plan = FaultPlan::new(1).with_fault(at, FaultKind::EnospcAllocate);
        let sim = SimIo::new(plan);
        let store = ReelStore::open_with_io(
            PathBuf::from("/reel"),
            sim_config(),
            COLUMNS,
            Arc::new(sim.clone()),
        )
        .unwrap_or_else(|error| panic!("open under a fault at op {at}: {error}"));
        for key_at in 0..40u64 {
            store
                .put(&key(key_at), &payload)
                .unwrap_or_else(|error| panic!("put {key_at} under a fault at op {at}: {error}"));
        }
        store.close().expect("close");
        // Read before the reopen runs more ops, so an unreached position ends
        // the sweep at the stream's own length rather than at a guess.
        let (fired, _) = sim.fault_reach();
        drop(store);

        let reopened = ReelStore::open_with_io(
            PathBuf::from("/reel"),
            sim_config(),
            COLUMNS,
            Arc::new(sim.clone()),
        )
        .unwrap_or_else(|error| panic!("reopen after a fault at op {at}: {error}"));
        for key_at in 0..40u64 {
            assert!(
                reopened.get(&key(key_at)).expect("get").is_some(),
                "key {key_at} went missing after a fault at op {at}"
            );
        }

        if fired == 0 {
            assert!(
                at > 60,
                "the run finished in only {at} ops, the sweep covered almost nothing"
            );
            break;
        }
        at += 1;
    }
}

// a draw fails only when every volume is full, and recovers when one is not
#[test]
fn a_full_class_refuses_and_a_drained_one_recovers() {
    let payload = vec![0x5Au8; 4 * 1024];
    let sim = SimIo::new(FaultPlan::new(1));
    let store = ReelStore::open_with_io(
        PathBuf::from("/reel"),
        sim_config(),
        COLUMNS,
        Arc::new(sim.clone()),
    )
    .expect("open");

    // Every allocate in the window is ENOSPC, so the spare drawn ahead runs out and
    // the first roll that needs a fresh segment has nowhere to go on either volume.
    // The window outlasts the loop by an order of magnitude.
    sim.arm_next_ops(2_000, FaultKind::EnospcAllocate);
    let mut refused = false;
    let mut acked = Vec::new();
    for key_at in 0..80u64 {
        match store.put(&key(key_at), &payload) {
            Ok(()) => acked.push(key_at),
            Err(_) => {
                refused = true;
                break;
            }
        }
    }
    assert!(
        refused,
        "a store with every volume full kept accepting writes"
    );

    // Space comes back, and the tail that failed its roll draws again.
    sim.disarm();
    store
        .put(&key(900), &payload)
        .expect("a put after space came back");
    store.close().expect("close");
    drop(store);

    let reopened = ReelStore::open_with_io(
        PathBuf::from("/reel"),
        sim_config(),
        COLUMNS,
        Arc::new(sim.clone()),
    )
    .expect("reopen");
    for key_at in acked {
        assert!(
            reopened.get(&key(key_at)).expect("get").is_some(),
            "acked key {key_at} went missing"
        );
    }
    assert!(reopened.get(&key(900)).expect("get").is_some());
}
