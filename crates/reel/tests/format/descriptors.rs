//! Descriptor and disk-space reclamation against a real filesystem
//!
//! Posix backend only: the simulator models an unlink as removing the bytes, while
//! a real filesystem frees a file's blocks only once its last link and its last
//! descriptor are both gone. A segment retired by compaction was necessarily opened
//! to copy its live records out, so a descriptor never released keeps the extents
//! after the file leaves the directory. The same seam bounds the handle cache: its
//! capacity means nothing unless evicting a handle closes the file behind it.

use std::sync::Arc;

use tempfile::TempDir;

use reel::io::posix_backend::PosixBackend;
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, RecordKey, ReelConfig,
    ReelStore, SyncPolicy, ThreadBudget,
};

/// Bytes the group takes at the front of a record key
const GROUP_PREFIX_LEN: usize = 2;

/// Bytes a record key occupies
const RECORD_KEY_LEN: usize = GROUP_PREFIX_LEN + 32;

const RECORDS: ColumnId = ColumnId(1);
const RECORDS_CF: &str = "records";

const BLOB: ColumnId = ColumnId(2);
const BLOB_CF: &str = "blob_data";

const COLUMNS: ColumnSet = &[
    ColumnSpec {
        id: RECORDS,
        name: RECORDS_CF,
        key_width: KeyWidth::Fixed(RECORD_KEY_LEN as u16),
        shard_bytes: GROUP_PREFIX_LEN as u8,
        purge_mark: None,
        codec: Codec::None,
    },
    ColumnSpec {
        id: BLOB,
        name: BLOB_CF,
        key_width: KeyWidth::Fixed(32),
        shard_bytes: 0,
        purge_mark: None,
        codec: Codec::None,
    },
];

/// Groups the fixtures write into
const GROUPS: &[u16] = &[7, 8, 9];

/// Sealed descriptors the reader cache holds, which is what these ceilings are read
/// against
///
/// The fixtures below roll tens of segments rather than hundreds, so the ceilings catch
/// a table that grows per segment touched rather than the eviction itself. Eviction at a
/// full cache is pinned in `FdCache`'s own tests.
const FD_CACHE: u64 = reel::DEFAULT_FD_CACHE;

/// Segment size small enough that a fixture rolls many segments
const SEGMENT_BYTES: u64 = 32 * 1024;

/// Records written per group
const KEYS: u64 = 40;

/// Payload length each record carries
const PAYLOAD: usize = 3_000;

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(SEGMENT_BYTES),
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        compact_dead_ratio: 0.1,
        ..ReelConfig::default()
    }
}

/// A record key: the group big endian, then the identifier
fn record_key(group: u16, id: [u8; 32]) -> RecordKey {
    let mut bytes = [0u8; RECORD_KEY_LEN];
    bytes[..GROUP_PREFIX_LEN].copy_from_slice(&group.to_be_bytes());
    bytes[GROUP_PREFIX_LEN..].copy_from_slice(&id);
    RecordKey::from_bytes(RECORDS, &bytes).expect("record key")
}

fn id(index: u64) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&index.to_be_bytes());
    bytes
}

/// Build the tree a volume expects, since an injected backend skips that step
fn prepare(dir: &std::path::Path) {
    std::fs::create_dir_all(dir).expect("root");
    for group in GROUPS {
        std::fs::create_dir_all(dir.join(format!("reel-{group:04}"))).expect("reel dir");
    }
}

fn fill(store: &ReelStore) {
    let body = vec![0xa5u8; PAYLOAD];
    for group in GROUPS {
        for index in 0..KEYS {
            store
                .put_owned(&record_key(*group, id(index)), body.clone())
                .expect("put");
        }
    }
}

// the descriptors a volume holds stay under the cache it was configured with
#[test]
fn descriptors_stay_under_the_cache_bound() {
    let dir = TempDir::new().expect("tempdir");
    prepare(dir.path());
    let backend = Arc::new(PosixBackend::new());
    let store =
        ReelStore::open_with_io(dir.path().to_path_buf(), config(), COLUMNS, backend.clone())
            .expect("open");

    fill(&store);
    store.flush().expect("flush");
    // Reading every key touches every sealed segment, which is what grows the
    // descriptor table if nothing ever closes.
    for group in GROUPS {
        for index in 0..KEYS {
            store.get(&record_key(*group, id(index))).expect("get");
        }
    }

    let held = backend.open_file_count();
    let tails = GROUPS.len();
    let ceiling = FD_CACHE as usize + tails + GROUPS.len();

    assert!(
        held <= ceiling,
        "the volume holds {held} descriptors, above the cache bound plus one active tail per group ({ceiling})"
    );
}

// retiring segments does not accumulate the descriptors that hold their blocks
//
// du cannot answer this: it walks directory entries, and an unlinked file has
// none, so it reports the space as returned either way.
#[test]
fn retiring_segments_releases_their_descriptors() {
    let dir = TempDir::new().expect("tempdir");
    prepare(dir.path());
    let backend = Arc::new(PosixBackend::new());
    let store =
        ReelStore::open_with_io(dir.path().to_path_buf(), config(), COLUMNS, backend.clone())
            .expect("open");

    fill(&store);
    let body = vec![0x5au8; PAYLOAD];
    for group in GROUPS {
        for index in 0..KEYS {
            store
                .put_owned(&record_key(*group, id(index)), body.clone())
                .expect("overwrite");
        }
    }
    store.flush().expect("flush");

    for _ in 0..64 {
        store.compact_once().expect("compact");
    }
    store.flush().expect("flush");

    let counters = store.compaction_counters();
    let retired = counters.segments_rewritten + counters.segments_unlinked_whole;
    assert!(
        retired > 0,
        "compaction retired nothing, so the test proves nothing"
    );

    let held = backend.open_file_count();
    let ceiling = FD_CACHE as usize + GROUPS.len() * 2;
    assert!(
        held <= ceiling,
        "{retired} segments retired and the volume still holds {held} descriptors, above {ceiling}"
    );
}
