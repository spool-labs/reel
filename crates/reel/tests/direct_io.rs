//! Direct io on a real filesystem, where the kernel checks the alignment for us
//!
//! Nothing here can be simulated: only a real descriptor opened with the flag
//! refuses an op whose offset, length or buffer address is off a block boundary.
//! The flag is a Linux one and not every filesystem takes it, so a volume that
//! cannot open direct reports that rather than failing.

#![cfg(target_os = "linux")]

use tempfile::TempDir;

use reel::config::{IoBackend, Preallocate, ReelConfig, SyncPolicy};
use reel::sync::tension::block_on;
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape, RecordKey, ReelStore,
};
use reel_core::{Direction, Store};

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

/// Group the fixtures write into
const GROUP: u16 = 7;

/// A record key: the group big endian, then the identifier
fn record_key(group: u16, id: [u8; 32]) -> RecordKey {
    let mut bytes = [0u8; RECORD_KEY_LEN];
    bytes[..GROUP_PREFIX_LEN].copy_from_slice(&group.to_be_bytes());
    bytes[GROUP_PREFIX_LEN..].copy_from_slice(&id);
    RecordKey::from_bytes(RECORDS, &bytes).expect("record key")
}

fn direct_config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::mb(16),
        alloc_chunk: ByteCount::mb(4),
        preallocate: Preallocate::Full,
        sync: SyncPolicy::Never,
        scrub_mbps: 0,
        io_backend: IoBackend::UringDirect,
        ..ReelConfig::default()
    }
}

fn key(byte: u8) -> Vec<u8> {
    record_key(GROUP, [byte; 32]).as_slice().to_vec()
}

/// A payload whose bytes depend on the key, so a misplaced read is visible
fn payload(byte: u8, len: usize) -> Vec<u8> {
    (0..len).map(|at| byte.wrapping_add(at as u8)).collect()
}

/// Open a direct volume, or report why this machine cannot host one
fn open_direct(dir: &TempDir) -> Option<ReelStore> {
    match ReelStore::open(dir.path().to_path_buf(), direct_config(), COLUMNS) {
        Ok(store) => Some(store),
        Err(error) => {
            eprintln!("skipping: this filesystem does not take a direct open: {error}");
            None
        }
    }
}

// records of every awkward size round trip through a direct volume
//
// The sizes straddle the block, and a read asks for the record's exact length at the
// offset the format chose, so every one lands off a boundary on at least one end.
#[test]
fn records_round_trip_unaligned() {
    let dir = TempDir::new().expect("tempdir");
    let Some(store) = open_direct(&dir) else {
        return;
    };

    let sizes = [1usize, 100, 4095, 4096, 4097, 9000, 65_536, 131_071];
    for (index, len) in sizes.iter().enumerate() {
        let byte = index as u8 + 1;
        Store::put(&store, RECORDS_CF, &key(byte), &payload(byte, *len)).expect("put");
    }

    for (index, len) in sizes.iter().enumerate() {
        let byte = index as u8 + 1;
        let got = Store::get(&store, RECORDS_CF, &key(byte))
            .expect("get")
            .map(|value| value.into_vec());
        assert_eq!(
            got,
            Some(payload(byte, *len)),
            "a {len} byte record came back wrong",
        );
    }
}

// an awaited read on a direct volume answers rather than refusing the buffer
//
// The async door hands its ops to the engine thread's ring, where a data op's buffer
// is the caller's own and sits wherever the allocator put it. A green run is only as
// strong as the filesystem under the temporary directory: btrfs serves the unaligned
// direct reads that ext4 refuses.
#[test]
fn an_awaited_read_answers_direct() {
    let dir = TempDir::new().expect("tempdir");
    let Some(store) = open_direct(&dir) else {
        return;
    };

    let sizes = [100usize, 4096, 9_000];
    for (index, len) in sizes.iter().enumerate() {
        let byte = index as u8 + 1;
        Store::put(&store, RECORDS_CF, &key(byte), &payload(byte, *len)).expect("put");
    }
    store.flush().expect("flush");

    for (index, len) in sizes.iter().enumerate() {
        let byte = index as u8 + 1;
        let got = block_on(Store::get_wait(&store, RECORDS_CF, &key(byte)))
            .expect("the awaited read answered")
            .map(|value| value.into_vec());
        assert_eq!(
            got,
            Some(payload(byte, *len)),
            "an awaited {len} byte read came back wrong",
        );
    }
}

// a direct volume survives a reopen, so what the kernel wrote is what a rebuild
// reads back out of the segment footers and the tail
#[test]
fn direct_volume_reopens() {
    let dir = TempDir::new().expect("tempdir");
    let Some(store) = open_direct(&dir) else {
        return;
    };

    for byte in 1..=32u8 {
        Store::put(&store, RECORDS_CF, &key(byte), &payload(byte, 3_000)).expect("put");
    }
    store.flush().expect("flush");
    drop(store);

    let reopened =
        ReelStore::open(dir.path().to_path_buf(), direct_config(), COLUMNS).expect("reopen direct");
    for byte in 1..=32u8 {
        assert_eq!(
            Store::get(&reopened, RECORDS_CF, &key(byte))
                .expect("get")
                .map(|value| value.into_vec()),
            Some(payload(byte, 3_000)),
            "record {byte} did not survive the reopen",
        );
    }
    assert_eq!(reopened.totals().count, 32);
}

// a direct volume plays its keys in order like any other
#[test]
fn direct_volume_iterates() {
    let dir = TempDir::new().expect("tempdir");
    let Some(store) = open_direct(&dir) else {
        return;
    };

    for byte in 1..=16u8 {
        Store::put(&store, RECORDS_CF, &key(byte), &payload(byte, 1_500)).expect("put");
    }

    let played: Vec<_> = Store::iter(&store, RECORDS_CF).expect("iter").collect();
    assert_eq!(played.len(), 16, "the playback lost records");

    let mut keys: Vec<_> = played.iter().map(|(key, _)| key.clone()).collect();
    let sorted = {
        let mut copy = keys.clone();
        copy.sort();
        copy
    };
    assert_eq!(keys, sorted, "the playback came back out of order");
    keys.dedup();
    assert_eq!(keys.len(), 16, "the playback repeated a key");
}

// an overwrite and a delete resolve on a direct volume, so the index and the
// records agree about which version is live
#[test]
fn direct_volume_overwrites_and_deletes() {
    let dir = TempDir::new().expect("tempdir");
    let Some(store) = open_direct(&dir) else {
        return;
    };

    Store::put(&store, RECORDS_CF, &key(1), &payload(1, 2_000)).expect("put");
    Store::put(&store, RECORDS_CF, &key(1), &payload(9, 5_000)).expect("overwrite");
    assert_eq!(
        Store::get(&store, RECORDS_CF, &key(1))
            .expect("get")
            .map(|value| value.into_vec()),
        Some(payload(9, 5_000)),
        "the overwrite did not win",
    );

    Store::delete(&store, RECORDS_CF, &key(1)).expect("delete");
    assert_eq!(Store::get(&store, RECORDS_CF, &key(1)).expect("get"), None);
    assert_eq!(store.totals().count, 0);
}

// a descending playback over a direct volume reads the same keys backwards
#[test]
fn direct_volume_walks_backwards() {
    let dir = TempDir::new().expect("tempdir");
    let Some(store) = open_direct(&dir) else {
        return;
    };

    for byte in 1..=8u8 {
        Store::put(&store, RECORDS_CF, &key(byte), &payload(byte, 900)).expect("put");
    }

    let mut ascending: Vec<_> = Store::iter(&store, RECORDS_CF)
        .expect("asc")
        .map(|(key, _)| key)
        .collect();
    // Above every key written, so the playback starts past the end and comes back
    // through all of them.
    let descending: Vec<_> = Store::iter_from(&store, RECORDS_CF, &key(255), Direction::Desc)
        .expect("desc")
        .map(|(key, _)| key)
        .collect();

    ascending.reverse();
    assert_eq!(ascending, descending, "the two directions disagreed");
}

// a ranged read on a direct volume answers through the bounce, both doors
//
// A window is one pread of exactly the window, and its offset and length land off a
// block boundary by construction, which ext4 refuses unless the bounce carries it.
#[test]
fn a_direct_range_reads_a_window() {
    let dir = TempDir::new().expect("tempdir");
    let Some(store) = open_direct(&dir) else {
        return;
    };

    let len = 131_071usize;
    Store::put(&store, RECORDS_CF, &key(9), &payload(9, len)).expect("put");
    store.flush().expect("flush");

    let record = RecordKey::from_bytes(RECORDS, &key(9)).expect("record key");
    let full = payload(9, len);
    for (at, wanted) in [
        (3u64, 100usize),
        (4093, 9),
        (40_000, 4_000),
        (127_071, 8_000),
    ] {
        let end = (at as usize + wanted).min(len);
        let blocked = store
            .get_range(&record, at, wanted)
            .expect("range")
            .expect("found");
        assert_eq!(&*blocked, &full[at as usize..end], "window at {at}");
        let awaited = block_on(store.get_range_wait(&record, at, wanted))
            .expect("awaited range")
            .expect("found");
        assert_eq!(
            blocked, awaited,
            "the doors disagree about the window at {at}"
        );
    }
}
