//! Keys on both sides of the inline bound, driven through the store

use tempfile::TempDir;

use reel::{
    Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, RecordKey, RecordWrite, ReelConfig,
    ReelStore, SyncPolicy, INLINE_KEY_LEN, MAX_KEY_LEN,
};

/// A column that takes a key of any width
const WIDE_COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: "wide",
    key_width: KeyWidth::Variable,
    shard_bytes: 2,
    purge_mark: None,
    codec: Codec::None,
}];

/// Widths around the inline bound, up to the widest key the format admits
const WIDTHS: [usize; 6] = [
    8,
    32,
    INLINE_KEY_LEN - 1,
    INLINE_KEY_LEN,
    INLINE_KEY_LEN + 1,
    MAX_KEY_LEN,
];

/// Each reopen case writes this many records at one width
const RUN: u8 = 4;

fn config() -> ReelConfig {
    ReelConfig {
        sync: SyncPolicy::Never,
        // Scrub is not under test, so it stays off
        scrub_mbps: 0,
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

/// A payload distinct per width and per seed
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

// every width survives one record per put, the default write path
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

// every width survives the batched drain, which gathers records into one vectored write
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

// every width reads back through the footer a seal wrote, one width per store
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

// the same mixed widths read back a block at a time through the paged index
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

    let store = ReelStore::open(path, config(), WIDE_COLUMNS).expect("reopen");
    for width in WIDTHS {
        assert_serves(&store, width, 0..RUN, "from the paged index");
    }
}

// a key wider than the format admits is refused
#[test]
fn an_over_wide_key_is_refused() {
    let bytes = vec![0u8; MAX_KEY_LEN + 1];
    assert!(RecordKey::from_bytes(ColumnId(1), &bytes).is_err());
}
