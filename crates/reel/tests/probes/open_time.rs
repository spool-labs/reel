//! What opening a volume costs as its segment count grows
//!
//! A resident open resolves newest-wins across every record on the volume by inserting
//! every key into one map, so the map is the join and the peak is the algorithm rather
//! than an accident of it. A paged open does not do that join, a sealed segment's
//! footer already being the sorted index its reads search, so it installs two keys per
//! segment rather than all of them and its index column should be flat.
//!
//! The third arm is the same resident map read back rather than rebuilt: the volume wrote
//! its index down before closing, so the open takes the rows instead of sweeping footers.
//! All three reopen one image and only the third keeps its file.
//!
//! The simulator serves every read out of memory, so what it times is the join rather than
//! the medium. Point `REEL_OPEN_TIME_DIR` at a directory to run the same three arms on the
//! real backend, which is where a per-segment read costs a seek. Each arm opens its own
//! copy of a volume built once, so these are warm opens rather than cold ones.
//!
//! Opt-in. Run with:
//!   cargo test -p tape-reel --release --test probes -- open_time

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::TempDir;

use reel::index::persisted::PERSISTED_INDEX;
use reel::io::fault::FaultPlan;
use reel::io::sim_backend::{DurableImage, SimIo};
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, IndexResidency, IoBackend, KeyWidth,
    MapShape, Preallocate, RecordKey, ReelConfig, ReelStore, SyncPolicy, ThreadBudget,
};

/// Virtual root the simulator's files live under
const ROOT: &str = "/bulk";

const RECORDS: ColumnId = ColumnId(1);
const BLOB: ColumnId = ColumnId(2);

/// Bytes a record key occupies: two group bytes then a thirty-two byte id
const RECORD_KEY_LEN: usize = 34;

/// The columns the volume is opened with
const COLUMNS: ColumnSet = &[
    ColumnSpec {
        id: RECORDS,
        name: "records",
        key_width: KeyWidth::Fixed(RECORD_KEY_LEN as u16),
        shard_bytes: 2,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
    ColumnSpec {
        id: BLOB,
        name: "blob_data",
        key_width: KeyWidth::Fixed(32),
        shard_bytes: 0,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
];

/// The group every record here is written under
const GROUP: u16 = 7;

/// Payload every record carries
///
/// Small, since the question is how many keys and segments an open has to resolve
/// rather than how many bytes it moves.
const PAYLOAD: usize = 64;

/// Segment size, which sets how many keys land in each one
const SEGMENT: u64 = 64 * 1024;

/// Segment counts the sweep reports, and the keys it takes to reach them
const CASES: &[usize] = &[64, 256, 1024];

fn config(index: IndexResidency) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(SEGMENT),
        alloc_chunk: ByteCount::from_bytes(8 * 1024),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        index,
        ..ReelConfig::default()
    }
}

/// The record column key for a group and an id, big endian group at the front
fn record_key(group: u16, id: [u8; 32]) -> RecordKey {
    let mut bytes = [0u8; RECORD_KEY_LEN];
    bytes[..2].copy_from_slice(&group.to_be_bytes());
    bytes[2..].copy_from_slice(&id);
    RecordKey::from_bytes(RECORDS, &bytes).expect("key")
}

fn id(at: u64) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&at.to_be_bytes());
    bytes
}

/// Fill a volume until it holds this many sealed segments, and say what it took
fn fill_to_segments(store: &ReelStore, wanted: usize) -> u64 {
    let payload = vec![0xa5u8; PAYLOAD];
    let mut written = 0u64;
    while store.index().segments_snapshot().len() < wanted + 1 {
        store
            .put(&record_key(GROUP, id(written)), &payload)
            .expect("put");
        written += 1;
    }
    written
}

/// The three configs the arms open under, in the order the table reports them
///
/// The first two sweep because their image has no index file in it; the third is the
/// same resident config over the image that kept one.
fn arms() -> [ReelConfig; 3] {
    [
        config(IndexResidency::Resident),
        config(IndexResidency::Paged),
        config(IndexResidency::Resident),
    ]
}

/// What one case measured: segments and keys on the volume, then an arm each
struct Case {
    segments: usize,
    keys: u64,
    arms: Vec<(Duration, u64)>,
}

/// Directory the operator asked for a real volume under, if they asked for one
fn named_root() -> Option<PathBuf> {
    std::env::var_os("REEL_OPEN_TIME_DIR").map(PathBuf::from)
}

// how long an open takes, and what it holds afterwards, as segments multiply
pub fn open_time_by_segment_count() {
    // libtest leaves the test name line open, so a header needs a newline ahead of it.
    println!();
    let root = named_root();
    match &root {
        Some(root) => println!(
            "backend {:?} under {}",
            IoBackend::default(),
            root.display()
        ),
        None => println!("backend simulator"),
    }
    println!(
        "{:>9}  {:>9}  {:>12}  {:>12}  {:>12}  {:>12}  {:>12}  {:>12}",
        "segments",
        "keys",
        "resident",
        "res index",
        "paged",
        "paged index",
        "checkpointed",
        "cp index",
    );

    for &wanted in CASES {
        let case = match &root {
            Some(root) => on_disk(root, wanted),
            None => simulated(wanted),
        };
        let arms = &case.arms;
        println!(
            "{:>9}  {:>9}  {:>12.2?}  {:>9} KiB  {:>12.2?}  {:>9} KiB  {:>12.2?}  {:>9} KiB",
            case.segments,
            case.keys,
            arms[0].0,
            arms[0].1,
            arms[1].0,
            arms[1].1,
            arms[2].0,
            arms[2].1,
        );
    }
}

/// Whether an arm keeps the index file, which is what the third one measures
fn keeps_index(at: usize) -> bool {
    at == 2
}

/// One case against the simulator, every arm reopening the same durable image
fn simulated(wanted: usize) -> Case {
    let sim = SimIo::new(FaultPlan::new(1));
    let store = ReelStore::open_with_io(
        PathBuf::from(ROOT),
        config(IndexResidency::Resident),
        COLUMNS,
        Arc::new(sim.clone()),
    )
    .expect("open");
    let keys = fill_to_segments(&store, wanted);
    store.flush().expect("flush");
    store.checkpoint_index().expect("checkpoint");
    let segments = store.index().segments_snapshot().len();
    let image = sim.durable_image();
    drop(store);

    let mut arms_taken = Vec::new();
    for (at, config) in arms().into_iter().enumerate() {
        // The swept arms reopen the same volume with the file taken out from under
        // them, so all three measure one image rather than three.
        let restored = SimIo::from_image(match keeps_index(at) {
            true => image.clone(),
            false => without_index(&image),
        });
        let start = Instant::now();
        let store =
            ReelStore::open_with_io(PathBuf::from(ROOT), config, COLUMNS, Arc::new(restored))
                .expect("reopen");
        let elapsed = start.elapsed();
        // What the open left behind, before any maintenance tick has run.
        let held = store.resident_bytes().to_bytes() / 1024;
        arms_taken.push((elapsed, held));
        drop(store);
    }
    Case {
        segments,
        keys,
        arms: arms_taken,
    }
}

/// The same image with no index file in it, which is a volume that wrote none
fn without_index(image: &DurableImage) -> DurableImage {
    let named = PathBuf::from(ROOT).join(PERSISTED_INDEX);
    image
        .iter()
        .filter(|(path, _)| *path != named)
        .cloned()
        .collect()
}

/// The same case on the real backend, under a temporary volume the run removes
fn on_disk(root: &Path, wanted: usize) -> Case {
    let home = TempDir::new_in(root).expect("tempdir");
    let built = home.path().join("built");
    let store =
        ReelStore::open(built.clone(), config(IndexResidency::Resident), COLUMNS).expect("open");
    let keys = fill_to_segments(&store, wanted);
    store.flush().expect("flush");
    store.checkpoint_index().expect("checkpoint");
    let segments = store.index().segments_snapshot().len();
    drop(store);

    let mut arms_taken = Vec::new();
    for (at, config) in arms().into_iter().enumerate() {
        let arm = home.path().join(format!("arm{at}"));
        copy_tree(&built, &arm);
        if !keeps_index(at) {
            std::fs::remove_file(arm.join(PERSISTED_INDEX)).expect("drop the index");
        }
        let start = Instant::now();
        let store = ReelStore::open(arm, config, COLUMNS).expect("reopen");
        let elapsed = start.elapsed();
        let held = store.resident_bytes().to_bytes() / 1024;
        arms_taken.push((elapsed, held));
        drop(store);
    }
    Case {
        segments,
        keys,
        arms: arms_taken,
    }
}

/// Copy a built volume so an arm opens files nothing else has opened
fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("arm directory");
    for entry in std::fs::read_dir(from).expect("built volume") {
        let entry = entry.expect("directory entry");
        let target = to.join(entry.file_name());
        match entry.file_type().expect("file type").is_dir() {
            true => copy_tree(&entry.path(), &target),
            false => {
                std::fs::copy(entry.path(), target).expect("copy");
            }
        }
    }
}
