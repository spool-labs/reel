//! Keys on both sides of the inline bound, driven through the store
//!
//! A key past the inline bound spills outside the record's prefix, so the drain has
//! to put the two pieces on the wire adjacent or the record frames wider than it
//! writes. Every write path runs at widths either side of the bound and reads back
//! through a reopen, which is what says the tail landed in the order a walk expects
//! rather than merely that a put returned.

use tempfile::TempDir;

use reel::{
    Codec, ColumnId, ColumnSet, ColumnSpec, IndexResidency, KeyWidth, MapShape, RecordKey,
    RecordWrite, ReelConfig, ReelStore, SyncPolicy, INLINE_KEY_LEN, MAX_KEY_LEN,
};

/// The column these fixtures write into, taking a key of any width
const WIDE_COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: "wide",
    key_width: KeyWidth::Variable,
    shard_bytes: 2,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

/// Widths every case runs, straddling the bound a key spills at
///
/// One below, exactly at and one above is where the representation changes, and the
/// ceiling is the widest key the format admits.
const WIDTHS: [usize; 6] = [
    8,
    32,
    INLINE_KEY_LEN - 1,
    INLINE_KEY_LEN,
    INLINE_KEY_LEN + 1,
    MAX_KEY_LEN,
];

/// Records each reopen case writes at one width
const RUN: u8 = 4;

fn config() -> ReelConfig {
    residency_config(IndexResidency::Resident)
}

fn residency_config(index: IndexResidency) -> ReelConfig {
    ReelConfig {
        sync: SyncPolicy::Never,
        // Not under test, and it would take the disk away from what is.
        scrub_mbps: 0,
        index,
        ..ReelConfig::default()
    }
}

/// A key of one width, distinct per width and per seed
fn key(width: usize, seed: u8) -> RecordKey {
    let mut bytes = vec![0x5A; width];
    bytes[0] = width as u8;
    bytes[1] = (width >> 8) as u8;
    bytes[width - 1] = seed;
    RecordKey::from_bytes(ColumnId(1), &bytes).expect("key fits")
}

/// The payload a key carries, distinct for the same reason
fn payload(width: usize, seed: u8) -> Vec<u8> {
    vec![(width % 251) as u8 ^ seed; 64 + width % 17]
}

fn assert_serves(store: &ReelStore, width: usize, seeds: impl Iterator<Item = u8>, at: &str) {
    for seed in seeds {
        let got = store
            .get(&key(width, seed))
            .unwrap_or_else(|error| panic!("get a {width} byte key {at}: {error}"))
            .unwrap_or_else(|| panic!("a {width} byte key is missing {at}"));
        assert_eq!(
            &*got,
            payload(width, seed).as_slice(),
            "a {width} byte key served the wrong payload {at}",
        );
    }
}

// one record per call, which is the path a caller takes by default
#[test]
fn a_spilled_key_survives_a_put() {
    let dir = TempDir::new().expect("tempdir");
    let store = ReelStore::open(dir.path().to_path_buf(), config(), WIDE_COLUMNS).expect("open");

    for width in WIDTHS {
        store.put(&key(width, 0), &payload(width, 0)).expect("put");
    }

    for width in WIDTHS {
        assert_serves(&store, width, 0..1, "from the tail");
    }
}

// the batched drain, which gathers many records into one vectored write
#[test]
fn a_spilled_key_survives_a_batch() {
    let dir = TempDir::new().expect("tempdir");
    let store = ReelStore::open(dir.path().to_path_buf(), config(), WIDE_COLUMNS).expect("open");

    let writes = WIDTHS
        .iter()
        .map(|&width| RecordWrite::Put {
            key: key(width, 0),
            payload: payload(width, 0),
        })
        .collect();
    store.apply_batch(writes).expect("batch");

    for width in WIDTHS {
        assert_serves(&store, width, 0..1, "from a batch");
    }
}

// the bytes on disk, read back through the footer a seal wrote
//
// One width per store, which is the case a strided footer partition covers.
#[test]
fn a_spilled_key_survives_a_reopen() {
    for width in WIDTHS {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().to_path_buf();

        let store = ReelStore::open(path.clone(), config(), WIDE_COLUMNS).expect("open");
        for seed in 0..RUN {
            store
                .put(&key(width, seed), &payload(width, seed))
                .expect("put");
        }
        store.flush().expect("flush");
        drop(store);

        let store = ReelStore::open(path, config(), WIDE_COLUMNS).expect("reopen");
        assert_eq!(
            store.totals().count,
            u64::from(RUN),
            "a reopen of a {width} byte column recovered a different number of records",
        );
        assert_serves(&store, width, 0..RUN, "after a reopen");
    }
}

// a segment holding records of differing key widths recovers all of them
//
// A strided footer cannot hold this: rows carry their own starts, so the reader asks
// the table where a row begins rather than multiplying by one width.
#[test]
fn mixed_key_widths_survive_a_reopen() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().to_path_buf();

    let store = ReelStore::open(path.clone(), config(), WIDE_COLUMNS).expect("open");
    for width in WIDTHS {
        store.put(&key(width, 0), &payload(width, 0)).expect("put");
    }
    store.flush().expect("flush");
    drop(store);

    let store = ReelStore::open(path, config(), WIDE_COLUMNS).expect("reopen");
    assert_eq!(store.totals().count, WIDTHS.len() as u64);
    for width in WIDTHS {
        assert_serves(&store, width, 0..1, "after a mixed width reopen");
    }
}

// the same mixed widths read a block at a time rather than from a parsed footer
//
// A different reader: the paged path cuts a block of rows up from the start table
// alone, without the footer around them, so the resident case proves nothing for it.
#[test]
fn mixed_key_widths_read_through_the_paged_index() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().to_path_buf();

    let store = ReelStore::open(path.clone(), config(), WIDE_COLUMNS).expect("open");
    for width in WIDTHS {
        for seed in 0..RUN {
            store
                .put(&key(width, seed), &payload(width, seed))
                .expect("put");
        }
    }
    store.flush().expect("flush");
    drop(store);

    let paged = residency_config(IndexResidency::Paged);
    let store = ReelStore::open(path, paged, WIDE_COLUMNS).expect("reopen paged");
    for width in WIDTHS {
        assert_serves(&store, width, 0..RUN, "from the paged index");
    }
}

// a key wider than the format admits is refused rather than truncated
#[test]
fn an_over_wide_key_is_refused() {
    let bytes = vec![0u8; MAX_KEY_LEN + 1];
    assert!(RecordKey::from_bytes(ColumnId(1), &bytes).is_err());
}
