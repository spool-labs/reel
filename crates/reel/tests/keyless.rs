//! Keyless records on a paged volume: what a get reads, and what a seal leaves on disk

use tempfile::TempDir;

use reel::config::{ReelConfig, SyncPolicy, ThreadBudget};
use reel::format::column::{Codec, ColumnId, ColumnSpec, RecordKey};
use reel::format::journal::rows_region;
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

/// Every segment file under a volume root, read whole
fn segment_files(dir: &TempDir) -> Vec<Vec<u8>> {
    std::fs::read_dir(dir.path())
        .expect("list")
        .map(|entry| entry.expect("entry").path())
        .filter(|path| path.to_string_lossy().ends_with(SEGMENT_SUFFIX))
        .map(|path| std::fs::read(path).expect("read"))
        .collect()
}

// a seal cuts its segment at the footer, so only the open tails still run out to their rows
#[test]
fn a_seal_leaves_no_rows() {
    let dir = TempDir::new().expect("tempdir");
    let store = filled(&dir);
    let files = segment_files(&dir);
    let open = files
        .iter()
        .filter(|bytes| rows_region(bytes).is_some())
        .count();
    assert!(
        files.len() > 2,
        "the stream sealed too little to say anything"
    );
    assert!(
        open <= 2,
        "{open} of {} segments still hold rows",
        files.len()
    );
    drop(store);
}

// an open segment rolls once its records and rows fill it, however small the records
#[test]
fn rows_stay_within_their_segment() {
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
    for bytes in segment_files(&dir) {
        if let Some((_, rows)) = rows_region(&bytes) {
            let len = rows.len() as u64;
            assert!(
                len <= limit,
                "rows of {len} bytes outgrew their {limit} byte segment"
            );
        }
    }
    drop(store);
}

/// Puts the aborting child makes, enough to seal some segments and leave one open
#[cfg(target_os = "linux")]
const ABORT_PUTS: u64 = 1_000;

// every put that returned survives the process dying with no flush, on a Linux buffered volume
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
