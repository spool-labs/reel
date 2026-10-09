//! Measures open time as segments grow, on the simulator or under `REEL_OPEN_TIME_DIR`
//! Run `cargo test -p tape-reel --release --test probes -- open_time`

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

/// The simulator's files live under this virtual root
const ROOT: &str = "/bulk";

const RECORDS: ColumnId = ColumnId(1);
const BLOB: ColumnId = ColumnId(2);

/// Record key length: two group bytes then a thirty-two byte id
const RECORD_KEY_LEN: usize = 34;

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

/// Every record here is written under this group
const GROUP: u16 = 7;

/// Each record's payload size, small since the probe counts keys and segments
const PAYLOAD: usize = 64;

/// Segment size, which sets how many keys land in each one
const SEGMENT: u64 = 64 * 1024;

/// The sweep reports each of these segment counts and the keys it took to reach them
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

/// Fill a volume until it holds this many sealed segments, and return the keys written
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
    spot: u64,
}

/// The real volume directory from `REEL_OPEN_TIME_DIR`, if set
fn named_root() -> Option<PathBuf> {
    std::env::var_os("REEL_OPEN_TIME_DIR").map(PathBuf::from)
}

// how long an open takes, and what it holds afterwards, as segments multiply
pub fn open_time_by_segment_count() {
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
        "{:>9}  {:>9}  {:>12}  {:>12}  {:>12}",
        "segments", "keys", "open", "index", "spot"
    );

    for &wanted in CASES {
        let case = match &root {
            Some(root) => on_disk(root, wanted),
            None => simulated(wanted),
        };
        println!(
            "{:>9}  {:>9}  {:>12.2?}  {:>8} KiB  {:>8} KiB",
            case.segments, case.keys, case.open, case.held, case.spot
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
    // What the open left behind, before any maintenance tick runs
    let held = store.resident_bytes().to_bytes() / 1024;
    let spot = store.index().spot_heap_bytes() / 1024;
    Case {
        segments,
        keys,
        open,
        held,
        spot,
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
    let spot = store.index().spot_heap_bytes() / 1024;
    Case {
        segments,
        keys,
        open,
        held,
        spot,
    }
}
