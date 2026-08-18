//! What filling a segment costs the writer that happens to fill it
//!
//! The seal runs once per segment roll, so it is one or two samples in many thousands
//! and a mean cannot see it, while the writer that lands on it pays a footer sort, a
//! multi megabyte write and an fsync inside its own put. So this reports the tail,
//! times every put rather than the batch, and asserts on the roll count, since a run
//! that never rolled is a bench of nothing.
//!
//! Ignored by default. Run with:
//!   cargo test -p reel --test seal_stall --release -- --ignored --nocapture

use std::time::Instant;

use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape, Preallocate, RecordKey,
    ReelConfig, ReelStore, SyncPolicy,
};

const RECORDS: ColumnId = ColumnId(1);
const BLOB: ColumnId = ColumnId(2);

/// A record-shaped column keyed by group then id, and a blob column keyed by id
const COLUMNS: ColumnSet = &[
    ColumnSpec {
        id: RECORDS,
        name: "records",
        key_width: KeyWidth::Fixed(34),
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

/// Segment size, small enough that the run rolls many times over
const SEGMENT_BYTES: u64 = 8 * 1024 * 1024;

/// One record, sized so a segment takes a few hundred of them
const RECORD_BYTES: usize = 16 * 1024;

/// Records written, which at the sizes above is a few dozen rolls
const RECORD_COUNT: usize = 24_000;

/// Group every record is addressed under
const GROUP: u16 = 7;

/// The record column key for a group and an id, big endian group at the front
fn record_key(group: u16, id: [u8; 32]) -> RecordKey {
    let mut bytes = [0u8; 34];
    bytes[..2].copy_from_slice(&group.to_be_bytes());
    bytes[2..].copy_from_slice(&id);
    RecordKey::from_bytes(RECORDS, &bytes).expect("key")
}

fn payload(seed: u64) -> Vec<u8> {
    let mut state = seed | 1;
    let mut out = Vec::with_capacity(RECORD_BYTES);
    while out.len() < RECORD_BYTES {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(RECORD_BYTES);
    out
}

fn record_id(index: usize) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&(index as u64).to_be_bytes());
    bytes
}

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(SEGMENT_BYTES),
        alloc_chunk: ByteCount::from_bytes(1024 * 1024),
        preallocate: Preallocate::Chunk,
        // The seal is what is measured, so nothing else may sync or move the device.
        sync: SyncPolicy::Never,
        scrub_mbps: 0,
        ..ReelConfig::default()
    }
}

/// The value at a percentile of a sorted slice
fn at(sorted: &[u128], percentile: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let at = ((sorted.len() - 1) as f64 * percentile).round() as usize;
    sorted[at]
}

// what the segment-filling writer pays, at the tail rather than on average
#[test]
#[ignore]
fn seal_stall() {
    // libtest leaves "test name ... " open, so a header printed into it lands a
    // screen-width right of the rows underneath it.
    println!();
    let dir = TempDir::new().expect("tempdir");
    let store = ReelStore::open(dir.path().to_path_buf(), config(), COLUMNS).expect("open");

    let payload = payload(1);
    let mut taken: Vec<u128> = Vec::with_capacity(RECORD_COUNT);
    for index in 0..RECORD_COUNT {
        let key = record_key(GROUP, record_id(index));
        let at = Instant::now();
        store.put(&key, &payload).expect("put");
        taken.push(at.elapsed().as_nanos());
    }
    store.flush().expect("flush");

    let written = RECORD_COUNT as u64 * RECORD_BYTES as u64;
    let rolls = written / SEGMENT_BYTES;
    assert!(rolls > 4, "only {rolls} rolls, so the seal barely ran");

    taken.sort_unstable();
    let total: u128 = taken.iter().sum();
    println!("seal stall, {RECORD_COUNT} puts of {RECORD_BYTES} B, segment {SEGMENT_BYTES} B");
    println!("  rolls          {rolls}");
    println!(
        "  mean       {:>10.2} us",
        total as f64 / taken.len() as f64 / 1000.0
    );
    println!("  p50        {:>10.2} us", at(&taken, 0.50) as f64 / 1000.0);
    println!("  p90        {:>10.2} us", at(&taken, 0.90) as f64 / 1000.0);
    println!("  p99        {:>10.2} us", at(&taken, 0.99) as f64 / 1000.0);
    println!(
        "  p99.9      {:>10.2} us",
        at(&taken, 0.999) as f64 / 1000.0
    );
    println!(
        "  max        {:>10.2} us",
        taken[taken.len() - 1] as f64 / 1000.0
    );
    // The tail against the middle, which is what a mean cannot show.
    println!(
        "  max/p50    {:>10.1}x",
        taken[taken.len() - 1] as f64 / at(&taken, 0.50).max(1) as f64
    );
}
