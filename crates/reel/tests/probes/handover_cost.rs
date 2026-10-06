//! Measures what it costs to hand a sealed segment's keys to its footer
//! Opt-in, run with `cargo test -p tape-reel --release --test probes -- handover_cost`

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use reel::io::fault::FaultPlan;
use reel::io::sim_backend::SimIo;
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, IndexResidency, KeyWidth, Preallocate,
    RecordKey, ReelConfig, ReelStore, SyncPolicy, ThreadBudget,
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
/// Small, since the question is how many rows the sweep visits rather than how many
/// bytes the volume holds.
const PAYLOAD: usize = 64;

/// Segment size, which sets how many keys land in each one
const SEGMENT: u64 = 64 * 1024;

/// Segment counts the sweep reports
const CASES: &[usize] = &[64, 256, 1024];

/// A paged index, so one `page_out_sealed` drains the whole queue and the timer covers the sweep
fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(SEGMENT),
        alloc_chunk: ByteCount::from_bytes(8 * 1024),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        index: IndexResidency::Paged,
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

// what the sweep costs per row, as the rows it has to visit multiply
pub fn handover_cost_by_segment_count() {
    // libtest leaves the test name line open, so a header needs a newline ahead of it.
    println!();
    println!(
        "{:>9}  {:>9}  {:>9}  {:>12}  {:>12}",
        "segments", "keys", "paged", "handover", "per row"
    );

    for &wanted in CASES {
        let sim = SimIo::new(FaultPlan::new(1));
        let store = ReelStore::open_with_io(
            PathBuf::from(ROOT),
            config(),
            COLUMNS,
            Arc::new(sim.clone()),
        )
        .expect("open");

        let keys = fill_to_segments(&store, wanted);
        store.flush().expect("flush");
        let segments = store.index().segments_snapshot().len();

        let start = Instant::now();
        let paged = store.page_out_sealed().expect("page out");
        let elapsed = start.elapsed();

        // A sweep that handed nothing over measured the queue rather than the rows.
        assert!(
            paged > 0,
            "the sweep handed nothing over, so it timed nothing"
        );
        let per_row = elapsed / paged as u32;

        println!(
            "{:>9}  {:>9}  {:>9}  {:>12.2?}  {:>12.2?}",
            segments, keys, paged, elapsed, per_row,
        );
        drop(store);
    }
}
