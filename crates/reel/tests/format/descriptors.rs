//! Descriptor and disk-space reclamation against a real filesystem
//!
//! Posix backend only: the simulator models an unlink as removing the bytes, while
//! a real filesystem frees a file's blocks only once its last link and its last
//! descriptor are both gone. A segment retired by compaction was necessarily opened
//! to copy its live records out, so a descriptor never released keeps the extents
//! after the file leaves the directory. The same seam bounds the handle cache: its
//! capacity means nothing unless evicting a handle closes the file behind it.
//!
//! Set REEL_DIRECT_REQUIRED to refuse a run where no direct plane is live rather
//! than skipping the half that needs one.

use std::sync::Arc;

use tempfile::TempDir;

use reel::io::posix_backend::PosixBackend;
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape, Preallocate,
    RangedReads, RecordKey, ReelConfig, ReelStore, SyncPolicy, ThreadBudget,
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
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
    ColumnSpec {
        id: BLOB,
        name: BLOB_CF,
        key_width: KeyWidth::Fixed(32),
        shard_bytes: 0,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
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
        alloc_chunk: ByteCount::from_bytes(8 * 1024),
        preallocate: Preallocate::Chunk,
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

/// Payload wide enough that a window into it is routed around the page cache
///
/// Below the one megabyte floor a window keeps the page cache whatever the knob asks.
const ROUTED_PAYLOAD: usize = 2 * 1024 * 1024;

/// Records written per group for the routed fixture, enough to roll several segments
const ROUTED_KEYS: u64 = 24;

/// Readers driving the routed windows at once
///
/// The route needs concurrent cold reads, since the direct plane loses to a lone
/// reader at every record size.
const ROUTED_READERS: usize = 3;

fn routed_config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::mb(16),
        alloc_chunk: ByteCount::from_bytes(64 * 1024),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        // Exact counts, which is the only cadence the plane counters mean anything at
        active_tails: ThreadBudget::threads(1),
        ranged_reads: RangedReads::Direct,
        ..ReelConfig::default()
    }
}

// a routed volume holds two descriptors per segment and gives both of them back
#[test]
fn direct_windows_double_the_descriptors() {
    let dir = TempDir::new().expect("tempdir");
    prepare(dir.path());
    let backend = Arc::new(PosixBackend::new());
    let store = ReelStore::open_with_io(
        dir.path().to_path_buf(),
        routed_config(),
        COLUMNS,
        backend.clone(),
    )
    .expect("open");

    let body = vec![0xa5u8; ROUTED_PAYLOAD];
    for group in GROUPS {
        for index in 0..ROUTED_KEYS {
            store
                .put_owned(&record_key(*group, id(index)), body.clone())
                .expect("put");
        }
    }
    store.flush().expect("flush");

    // A window from every key asks every sealed segment for a second descriptor and
    // then evicts the handles that hold them. Driven from several threads, since the
    // route reads the cold depth it arrived into.
    std::thread::scope(|scope| {
        for reader in 0..ROUTED_READERS {
            let store = &store;
            scope.spawn(move || {
                // Readers start on different groups so they overlap without queueing
                // on one segment.
                for turn in 0..GROUPS.len() {
                    let group = GROUPS[(reader + turn) % GROUPS.len()];
                    for index in 0..ROUTED_KEYS {
                        store
                            .get_range(&record_key(group, id(index)), 40_000, 4_000)
                            .expect("range");
                    }
                }
            });
        }
    });

    // The flag only knows one of the two ways there is no plane to count: a refused
    // direct open retires the route, but a platform without the open flag never
    // attempts one, so the flag stays true over a plane that was compiled out.
    if !cfg!(target_os = "linux") || !store.cold_direct_live() {
        assert!(
            std::env::var_os("REEL_DIRECT_REQUIRED").is_none(),
            "there is no cold read plane here and REEL_DIRECT_REQUIRED is set",
        );
        println!(
            "skipped: no direct open on this platform or filesystem, so the cold plane is not live"
        );
        return;
    }
    let routed = backend.cold_reads();
    assert!(
        routed.direct > 0,
        "no window reached the direct plane, so nothing is proven"
    );

    let held = backend.open_file_count();
    let tails = GROUPS.len();
    let ceiling = 2 * FD_CACHE as usize + tails + GROUPS.len();
    assert!(
        held <= ceiling,
        "the volume holds {held} descriptors, above twice the cache bound plus a tail per group ({ceiling})"
    );

    drop(store);
    assert_eq!(
        backend.open_file_count(),
        0,
        "a dropped volume left descriptors open, so a direct one is never closed"
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
