//! What a restart costs on disk: an empty tail leaves no file behind, and a
//! sealed segment keeps its records rather than its reservation
//!
//! Before these held, every clean stop sealed a header-only tail at its full
//! preallocation, and a node restarted daily banked a segment of slack per tail
//! per day. A store holding megabytes could sit on tens of gigabytes of shells.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;

use reel::config::{ReelConfig, SyncPolicy, ThreadBudget};
use reel::format::column::{Codec, ColumnId, ColumnSet, ColumnSpec, MapShape, RecordKey};
use reel::io::fault::FaultPlan;
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

const SEGMENT: u64 = 1024 * 1024;

fn key(at: u64) -> RecordKey {
    let mut bytes = [0u8; 16];
    bytes[8..].copy_from_slice(&at.to_be_bytes());
    RecordKey::from_bytes(ROWS, &bytes).expect("key")
}

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(SEGMENT),
        alloc_chunk: ByteCount::from_bytes(SEGMENT / 4),
        preallocate: Preallocate::Full,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
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

fn bytes_in(root: &Path) -> u64 {
    segments_in(root)
        .iter()
        .map(|path| std::fs::metadata(path).expect("metadata").len())
        .sum()
}

// a store opened and closed without a write leaves nothing on disk
#[test]
fn an_idle_restart_leaves_no_segment() {
    let home = TempDir::new().expect("home");

    for round in 0..5 {
        let store =
            ReelStore::open(home.path().to_path_buf(), config(), COLUMNS).expect("open");
        store.close().expect("close");
        drop(store);
        let left = segments_in(home.path());
        assert!(
            left.is_empty(),
            "restart {round} left {} segment files",
            left.len()
        );
    }
}

// a seal cuts the file to its records, not its preallocation
#[test]
fn a_sealed_segment_sheds_its_reservation() {
    let home = TempDir::new().expect("home");

    let store = ReelStore::open(home.path().to_path_buf(), config(), COLUMNS).expect("open");
    let payload = vec![0x5Au8; 8 * 1024];
    for at in 0..4u64 {
        store.put(&key(at), &payload).expect("put");
    }
    store.close().expect("close");
    drop(store);

    let sealed = segments_in(home.path());
    assert_eq!(sealed.len(), 1, "expected the one sealed tail");
    let len = std::fs::metadata(&sealed[0]).expect("metadata").len();
    assert!(
        len < SEGMENT / 4,
        "sealed file kept {len} of a {SEGMENT} byte reservation"
    );

    let reopened = ReelStore::open(home.path().to_path_buf(), config(), COLUMNS).expect("reopen");
    for at in 0..4u64 {
        assert!(
            reopened.get(&key(at)).expect("get").is_some(),
            "key {at} went missing after the cut"
        );
    }
}

// a crash's leftover reservation is cut at the next writable open
#[test]
fn reopen_cuts_a_crashed_tail() {
    let config = ReelConfig {
        sync: SyncPolicy::EveryPut,
        ..config()
    };
    let sim = SimIo::new(FaultPlan::new(1));
    let store = ReelStore::open_with_io(
        PathBuf::from("/reel"),
        config.clone(),
        COLUMNS,
        Arc::new(sim.clone()),
    )
    .expect("open");
    store.put(&key(0), &vec![0x5Au8; 8 * 1024]).expect("put");
    // What a power loss leaves: the tail unsealed at its full reservation.
    let image = sim.durable_image();
    drop(store);
    assert!(
        image
            .iter()
            .any(|(path, bytes)| is_segment(path) && bytes.len() as u64 >= SEGMENT),
        "the crash image never held the reservation this test is about"
    );

    let survivor = SimIo::from_image(image);
    let reopened = ReelStore::open_with_io(
        PathBuf::from("/reel"),
        config,
        COLUMNS,
        Arc::new(survivor.clone()),
    )
    .expect("reopen");
    assert!(reopened.get(&key(0)).expect("get").is_some());

    let widest = survivor
        .durable_image()
        .iter()
        .filter(|(path, _)| is_segment(path))
        .map(|(_, bytes)| bytes.len() as u64)
        .max()
        .expect("the walked tail survived the reopen");
    assert!(
        widest < SEGMENT / 4,
        "a walked tail kept {widest} of its reservation"
    );
}

fn is_segment(path: &Path) -> bool {
    path.extension().is_some_and(|extension| extension == "reel")
}

// restarts after the data landed cost nothing further
#[test]
fn idle_restarts_do_not_grow_the_store() {
    let home = TempDir::new().expect("home");

    let store = ReelStore::open(home.path().to_path_buf(), config(), COLUMNS).expect("open");
    store.put(&key(0), &vec![0x5Au8; 8 * 1024]).expect("put");
    store.close().expect("close");
    drop(store);
    let settled = bytes_in(home.path());

    for _ in 0..5 {
        let store =
            ReelStore::open(home.path().to_path_buf(), config(), COLUMNS).expect("reopen");
        store.close().expect("close");
        drop(store);
    }

    assert_eq!(
        bytes_in(home.path()),
        settled,
        "idle restarts changed what the store weighs"
    );
}
