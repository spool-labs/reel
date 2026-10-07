//! What opening a volume costs as its segment count grows
//!
//! An open joins nothing across the sealed segments: each footer is already the sorted
//! index its reads search, so the open takes the footers into the spot index and
//! installs only what the tails hold. Its index column should be flat.
//!
//! The simulator serves every read out of memory, so it times the open's own work. Point
//! `REEL_OPEN_TIME_DIR` at a directory to run the same open on the real backend, where a
//! per-segment read costs a seek. The volume was just written, so it is a warm open.
//!
//! Opt-in. Run with:
//!   cargo test -p tape-reel --release --test probes -- open_time

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::TempDir;

use reel::io::fault::FaultPlan;
use reel::io::sim_backend::SimIo;
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, IoBackend, KeyWidth, RecordKey, ReelConfig,
    ReelStore, SyncPolicy, ThreadBudget,
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
        purge_mark: None,
        codec: Codec::None,
    },
    ColumnSpec {
        id: BLOB,
        name: "blob_data",
        key_width: KeyWidth::Fixed(32),
        shard_bytes: 0,
        purge_mark: None,
        codec: Codec::None,
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

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(SEGMENT),
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
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

/// What one case measured: segments and keys on the volume, then the open
struct Case {
    segments: usize,
    keys: u64,
    open: Duration,
    held: u64,
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
        "{:>9}  {:>9}  {:>12}  {:>12}",
        "segments", "keys", "open", "index"
    );

    for &wanted in CASES {
        let case = match &root {
            Some(root) => on_disk(root, wanted),
            None => simulated(wanted),
        };
        println!(
            "{:>9}  {:>9}  {:>12.2?}  {:>9} KiB",
            case.segments, case.keys, case.open, case.held
        );
    }
}

/// One case against the simulator, the open reading back the durable image
fn simulated(wanted: usize) -> Case {
    let sim = SimIo::new(FaultPlan::new(1));
    let io = Arc::new(sim.clone());
    let store = ReelStore::open_with_io(PathBuf::from(ROOT), config(), COLUMNS, io).expect("open");
    let keys = fill_to_segments(&store, wanted);
    store.flush().expect("flush");
    let segments = store.index().segments_snapshot().len();
    let image = sim.durable_image();
    drop(store);

    let restored = Arc::new(SimIo::from_image(image));
    let start = Instant::now();
    let store =
        ReelStore::open_with_io(PathBuf::from(ROOT), config(), COLUMNS, restored).expect("reopen");
    let open = start.elapsed();
    // What the open left behind, before any maintenance tick has run.
    let held = store.resident_bytes().to_bytes() / 1024;
    Case {
        segments,
        keys,
        open,
        held,
    }
}

/// The same case on the real backend, under a temporary volume the run removes
fn on_disk(root: &Path, wanted: usize) -> Case {
    let home = TempDir::new_in(root).expect("tempdir");
    let built = home.path().join("built");
    let store = ReelStore::open(built.clone(), config(), COLUMNS).expect("open");
    let keys = fill_to_segments(&store, wanted);
    store.flush().expect("flush");
    let segments = store.index().segments_snapshot().len();
    drop(store);

    let start = Instant::now();
    let store = ReelStore::open(built, config(), COLUMNS).expect("reopen");
    let open = start.elapsed();
    let held = store.resident_bytes().to_bytes() / 1024;
    Case {
        segments,
        keys,
        open,
        held,
    }
}
