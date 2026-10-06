//! Keyless records on a paged volume: what a get reads, and what a seal leaves on disk
//!
//! A small record carries no key, so a get confirms it by its keyed check. A key's lone
//! spot slot answers in that one read with no footer search, and a seal leaves the
//! segment and its footer with the journal gone.

use tempfile::TempDir;

use reel::config::{IndexResidency, ReelConfig, SyncPolicy, ThreadBudget};
use reel::format::column::{Codec, ColumnId, ColumnSpec, RecordKey};
use reel::format::journal::JOURNAL_SUFFIX;
use reel::units::ByteCount;
use reel::{KeyWidth, Preallocate, ReelStore, SEGMENT_SUFFIX};

const RECORDS: ColumnId = ColumnId(1);

const COLUMNS: &[ColumnSpec] = &[ColumnSpec {
    id: RECORDS,
    name: "records",
    key_width: KeyWidth::Fixed(16),
    shard_bytes: 1,
    purge_mark: None,
    codec: Codec::None,
}];

/// Keys written, enough to roll the small segment several times
const KEYS: u64 = 2_000;

/// Payload bytes per record, well under the keyless ceiling
const VALUE_BYTES: usize = 200;

fn key(at: u64) -> RecordKey {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&at.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes());
    bytes[8..].copy_from_slice(&at.to_be_bytes());
    RecordKey::from_bytes(RECORDS, &bytes).expect("key")
}

fn value(at: u64) -> Vec<u8> {
    vec![at as u8; VALUE_BYTES]
}

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(64 * 1024),
        alloc_chunk: ByteCount::from_bytes(16 * 1024),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        index: IndexResidency::Paged,
        ..ReelConfig::default()
    }
}

/// A volume with every key written once, its sealed keys handed to the spot index
fn filled(dir: &TempDir) -> ReelStore {
    let store = ReelStore::open(dir.path().to_path_buf(), config(), COLUMNS).expect("open");
    for at in 0..KEYS {
        store.put(&key(at), &value(at)).expect("put");
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");
    store
}

// a key's lone spot slot answers a get in its one record read, with no footer search
#[test]
fn a_lone_spot_slot_reads_the_record_once() {
    let dir = TempDir::new().expect("tempdir");
    let store = filled(&dir);
    assert!(store.index().spot_held() > 0, "nothing reached the spot index");

    let before = store.filter_probes();
    let mut answered = 0u64;
    for at in 0..KEYS {
        let got = store.get(&key(at)).expect("get").expect("present");
        assert_eq!(got.as_ref(), &value(at)[..], "key {at} read another record");
        answered += 1;
    }
    let probes = store.filter_probes().since(before);
    assert_eq!(answered, KEYS);
    assert_eq!(probes.asked, 0, "a get searched {} footers", probes.asked);
}

// a reopen reads the keyless records back, through the footers and the open tail's journal
#[test]
fn keyless_records_read_back_after_a_reopen() {
    let dir = TempDir::new().expect("tempdir");
    let store = filled(&dir);
    store.close().expect("close");
    drop(store);

    let reopened = ReelStore::open(dir.path().to_path_buf(), config(), COLUMNS).expect("reopen");
    for at in 0..KEYS {
        let got = reopened.get(&key(at)).expect("get").expect("present");
        assert_eq!(got.as_ref(), &value(at)[..], "key {at} after a reopen");
    }
}

// a sealed segment keeps its footer and loses its journal, and only the open tail has one
#[test]
fn a_seal_leaves_no_journal() {
    let dir = TempDir::new().expect("tempdir");
    let store = filled(&dir);
    let names: Vec<String> = std::fs::read_dir(dir.path())
        .expect("list")
        .map(|entry| entry.expect("entry").file_name().to_string_lossy().into_owned())
        .collect();
    let segments = names.iter().filter(|name| name.ends_with(SEGMENT_SUFFIX)).count();
    let journals = names.iter().filter(|name| name.ends_with(JOURNAL_SUFFIX)).count();
    assert!(segments > 2, "the stream sealed too little to say anything");
    assert!(journals <= 2, "{journals} journals stand beside {segments} segments");
    drop(store);
}
