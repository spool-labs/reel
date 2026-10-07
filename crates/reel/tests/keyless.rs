//! Keyless records on a paged volume: what a get reads, and what a seal leaves on disk

use tempfile::TempDir;

use reel::config::{ReelConfig, SyncPolicy, ThreadBudget};
use reel::format::column::{Codec, ColumnId, ColumnSpec, RecordKey};
use reel::format::journal::JOURNAL_SUFFIX;
use reel::units::ByteCount;
use reel::{KeyWidth, ReelStore, SEGMENT_SUFFIX};

const RECORDS: ColumnId = ColumnId(1);

const WIDE: ColumnId = ColumnId(2);

const COLUMNS: &[ColumnSpec] = &[
    ColumnSpec {
        id: RECORDS,
        name: "records",
        key_width: KeyWidth::Fixed(16),
        shard_bytes: 1,
        purge_mark: None,
        codec: Codec::None,
    },
    ColumnSpec {
        id: WIDE,
        name: "wide",
        key_width: KeyWidth::Fixed(108),
        shard_bytes: 1,
        purge_mark: None,
        codec: Codec::None,
    },
];

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
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
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
    assert!(
        store.index().spot_held() > 0,
        "nothing reached the spot index"
    );

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
        .map(|entry| {
            entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    let segments = names
        .iter()
        .filter(|name| name.ends_with(SEGMENT_SUFFIX))
        .count();
    let journals = names
        .iter()
        .filter(|name| name.ends_with(JOURNAL_SUFFIX))
        .count();
    assert!(segments > 2, "the stream sealed too little to say anything");
    assert!(
        journals <= 2,
        "{journals} journals stand beside {segments} segments"
    );
    drop(store);
}

// an open segment rolls once its records and journal fill it, however small the records
#[test]
fn a_journal_stays_within_its_segment() {
    let dir = TempDir::new().expect("tempdir");
    let store = ReelStore::open(dir.path().to_path_buf(), config(), COLUMNS).expect("open");
    for at in 0..KEYS {
        let mut bytes = [0u8; 108];
        bytes[..8].copy_from_slice(&at.to_be_bytes());
        let key = RecordKey::from_bytes(WIDE, &bytes).expect("key");
        store.put(&key, &[1u8]).expect("put");
    }
    store.flush().expect("flush");
    let limit = config().segment_bytes.to_bytes();
    for entry in std::fs::read_dir(dir.path()).expect("list") {
        let entry = entry.expect("entry");
        if entry
            .file_name()
            .to_string_lossy()
            .ends_with(JOURNAL_SUFFIX)
        {
            let len = entry.metadata().expect("stat").len();
            assert!(
                len <= limit,
                "a journal of {len} bytes outgrew its {limit} byte segment"
            );
        }
    }
    drop(store);
}

/// Puts the aborting child makes, enough to seal some segments and leave one open
#[cfg(target_os = "linux")]
const ABORT_PUTS: u64 = 1_000;

// every put that returned survives the process dying with no flush, on a mapped volume
#[cfg(target_os = "linux")]
#[test]
fn puts_survive_a_process_crash() {
    if let Ok(dir) = std::env::var("REEL_CRASH_CHILD_DIR") {
        let store = ReelStore::open(dir.into(), config(), COLUMNS).expect("open");
        for at in 0..ABORT_PUTS {
            store.put(&key(at), &value(at)).expect("put");
        }
        // Exits with no destructors run, as a crash would
        std::process::exit(3);
    }
    let dir = TempDir::new().expect("tempdir");
    let status = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args(["--exact", "puts_survive_a_process_crash", "--nocapture"])
        .env("REEL_CRASH_CHILD_DIR", dir.path())
        .status()
        .expect("child");
    assert_eq!(status.code(), Some(3), "the child did not reach its exit");
    let store = ReelStore::open(dir.path().to_path_buf(), config(), COLUMNS).expect("reopen");
    for at in 0..ABORT_PUTS {
        let got = store
            .get(&key(at))
            .expect("get")
            .unwrap_or_else(|| panic!("put {at} was lost"));
        assert_eq!(got.as_ref(), &value(at)[..], "put {at} read back wrong");
    }
}
