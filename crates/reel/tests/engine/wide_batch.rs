//! A batch wider than the kernel's iovec cap lands whole, on either backend
//!
//! A vectored call carries at most 1024 spans on Linux and macOS alike, and past that
//! the kernel refuses the whole call rather than writing a prefix of it. Posix walks
//! a wide drain in capped calls; the ring carries a write's span list in one
//! submission and cannot split it, so an oversized write takes the posix path. A
//! batch reaches the cap in records rather than bytes, so small payloads are what
//! gets there. Off Linux a ring request downgrades to posix, so this is the posix leg
//! run twice.

use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, IoBackend, KeyWidth, MapShape, RecordKey,
    RecordWrite, ReelConfig, ReelStore, SyncPolicy,
};

/// Spans one vectored call carries, which is the bound this file is about
const IOVEC_CAP: usize = 1024;

/// Records in the batch, comfortably past the cap so no rounding lands under it
const RECORDS: usize = 4 * IOVEC_CAP;

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

fn config(io_backend: IoBackend) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::mb(64),
        alloc_chunk: ByteCount::mb(16),
        sync: SyncPolicy::Never,
        io_backend,
        ..ReelConfig::default()
    }
}

fn key(at: usize) -> RecordKey {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&(at as u64).to_be_bytes());
    RecordKey::from_bytes(ROWS, &bytes).expect("key")
}

/// Write one batch past the cap and read every record back
fn a_batch_past_the_cap(io_backend: IoBackend) {
    let dir = TempDir::new().expect("tempdir");
    let store =
        ReelStore::open(dir.path().to_path_buf(), config(io_backend), COLUMNS).expect("open");

    // Small payloads on purpose: large ones would reach the cap in bytes long before
    // the batch reached it in records.
    let writes: Vec<RecordWrite> = (0..RECORDS)
        .map(|at| RecordWrite::Put {
            key: key(at),
            payload: vec![0x5Cu8; 64],
        })
        .collect();

    store.apply_batch(writes).unwrap_or_else(|error| {
        panic!("a {RECORDS} record batch on {io_backend:?} failed: {error}")
    });
    store.flush().expect("flush");

    for at in 0..RECORDS {
        assert!(
            store.get(&key(at)).expect("get").is_some(),
            "record {at} of {RECORDS} did not land on {io_backend:?}",
        );
    }
}

// the posix backend splits a wide batch, as it always has
#[test]
fn a_wide_batch_lands_on_posix() {
    a_batch_past_the_cap(IoBackend::Posix);
}

// the ring hands a wide batch to the path that can split it
//
// Without that hand-off the kernel refuses the whole call: nothing written, and the
// batch reported to the caller as an io error.
#[test]
fn a_wide_batch_lands_on_the_ring() {
    a_batch_past_the_cap(IoBackend::Uring);
}
