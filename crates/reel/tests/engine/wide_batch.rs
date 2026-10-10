//! A batch wider than the kernel's iovec cap lands whole, on either backend

use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, IoBackend, KeyWidth, RecordKey, RecordWrite,
    ReelConfig, ReelStore, SyncPolicy,
};

/// One vectored call takes at most this many spans
const IOVEC_CAP: usize = 1024;

/// Records in the batch, comfortably past the cap so no rounding lands under it
const RECORDS: usize = 4 * IOVEC_CAP;

const ROWS: ColumnId = ColumnId(1);

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ROWS,
    name: "rows",
    key_width: KeyWidth::Fixed(16),
    shard_bytes: 0,
    purge_mark: None,
    codec: Codec::None,
}];

fn config(io_backend: IoBackend) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::mb(64),
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

    // Small payloads, so the batch reaches the cap in records before any byte bound
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

// the posix backend splits a wide batch into capped calls
#[test]
fn a_wide_batch_lands_on_posix() {
    a_batch_past_the_cap(IoBackend::Posix);
}

// the ring hands a wide batch to the path that can split it
#[test]
fn a_wide_batch_lands_on_the_ring() {
    a_batch_past_the_cap(IoBackend::Uring);
}
