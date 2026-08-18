//! What opening a volume costs as its segment count grows
//!
//! A resident open resolves newest-wins across every record on the volume by inserting
//! every key into one map, so the map is the join and the peak is the algorithm rather
//! than an accident of it. A paged open does not do that join, a sealed segment's
//! footer already being the sorted index its reads search, so it installs two keys per
//! segment rather than all of them and its index column should be flat.
//!
//! The third arm is the same resident map read back rather than rebuilt: the volume wrote
//! its index down before closing, so the open takes the rows for every segment the file
//! speaks for instead of sweeping their footers. Same volume and same segment counts as
//! the swept arm, since all three reopen one image and only an armed open reads the file.
//!
//! Opt-in. Run with:
//!   cargo test -p reel --release --test probes -- open_time

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use reel::io::fault::FaultPlan;
use reel::io::sim_backend::SimIo;
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, IndexResidency, KeyWidth, MapShape,
    Preallocate, RecordKey, ReelConfig, ReelStore, SyncPolicy, ThreadBudget,
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

/// The resident volume armed to write its index down, and to read one back
fn checkpointing() -> ReelConfig {
    ReelConfig {
        index_checkpoint: true,
        ..config(IndexResidency::Resident)
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

// how long an open takes, and what it holds afterwards, as segments multiply
pub fn open_time_by_segment_count() {
    // libtest leaves the test name line open, so a header needs a newline ahead of it.
    println!();
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
        let sim = SimIo::new(FaultPlan::new(1));
        let store = ReelStore::open_with_io(
            PathBuf::from(ROOT),
            checkpointing(),
            COLUMNS,
            Arc::new(sim.clone()),
        )
        .expect("open");
        let keys = fill_to_segments(&store, wanted);
        store.flush().expect("flush");
        // The file goes into the image every arm reopens: an unarmed open never reads it,
        // so the swept arms measure the same volume rather than a smaller one.
        store.checkpoint_index().expect("checkpoint");
        let segments = store.index().segments_snapshot().len();
        let image = sim.durable_image();
        drop(store);

        let mut row = Vec::new();
        for config in [
            config(IndexResidency::Resident),
            config(IndexResidency::Paged),
            checkpointing(),
        ] {
            let restored = SimIo::from_image(image.clone());
            let start = Instant::now();
            let store =
                ReelStore::open_with_io(PathBuf::from(ROOT), config, COLUMNS, Arc::new(restored))
                    .expect("reopen");
            let elapsed = start.elapsed();
            // What the open left behind, before any maintenance tick has run.
            let held = store.resident_bytes().to_bytes() / 1024;
            row.push((elapsed, held));
            drop(store);
        }

        println!(
            "{:>9}  {:>9}  {:>12.2?}  {:>9} KiB  {:>12.2?}  {:>9} KiB  {:>12.2?}  {:>9} KiB",
            segments, keys, row[0].0, row[0].1, row[1].0, row[1].1, row[2].0, row[2].1,
        );
    }
}
