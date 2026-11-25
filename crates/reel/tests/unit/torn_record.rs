use std::fs;
use std::path::{Path, PathBuf};
use std::process;
use std::time::{Duration, Instant};

use reel::config::{ColumnConfig, ColumnIndex, KeyShape, StoreConfig, SyncPolicy, ThreadCounts, VolumeConfig};
use reel::engine::open_segment::{DATA_SUFFIX, ROWS_SUFFIX};
use reel::engine::reel_store::ReelStore;
use reel::io::plain::PlainIo;
use reel::record::codec::Codec;
use reel_core::column_id::ColumnId;
use reel_core::units::ByteCount;

const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(256 * 1024 * 1024);
const SYNCED: u64 = 64;
const LAST: u64 = SYNCED;
const LAST_BYTES: usize = 64 * 1024;
const ROW_WAIT: Duration = Duration::from_secs(20);

fn config(root: &Path) -> StoreConfig {
    StoreConfig {
        memory_budget: BUDGET,
        max_memory: None,
        threads: ThreadCounts::default(),
        volumes: vec![VolumeConfig { open_segments: Some(1), ..VolumeConfig::new("torn", vec![root.to_path_buf()], SyncPolicy::Never) }],
        columns: vec![ColumnConfig { codec: Codec::None, index: ColumnIndex::Plain, ..ColumnConfig::new(COLUMN, "torn", KeyShape::Fixed { width: 8 }) }],
    }
}

// the last value's bytes, a pattern no other value holds, so the copy can find its record on disk
fn last_value() -> Vec<u8> {
    (0..LAST_BYTES).map(|at| (at % 251) as u8 ^ 0xA5).collect()
}

fn files_with(root: &Path, suffix: &str) -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = fs::read_dir(root).expect("read the root").map(|entry| entry.expect("entry").path()).filter(|path| path.to_string_lossy().ends_with(suffix)).collect();
    found.sort();
    found
}

fn rows_bytes(root: &Path) -> u64 {
    files_with(root, ROWS_SUFFIX).iter().map(|path| fs::metadata(path).map_or(0, |meta| meta.len())).sum()
}

// copy every file of a root, as a power cut would leave the pages the kernel held
fn copy_root(from: &Path, to: &Path) {
    fs::create_dir_all(to).expect("copy root");
    for entry in fs::read_dir(from).expect("read the root") {
        let path = entry.expect("entry").path();
        if path.is_file() {
            fs::copy(&path, to.join(path.file_name().expect("a name"))).expect("copy a file");
        }
    }
}

// how the copy loses the last record's bytes: the file ends before them, as the macOS reserve leaves it, or they read as
// zeros inside the file, as a Linux reserve leaves it
#[derive(Clone, Copy, Debug)]
enum Loss {
    PastTheEnd,
    Zeros,
}

// an open segment whose rows file lists one row more than its data file holds, the rows past the last sync and the record
// lost, opens with that key gone and every synced row before it read back, whether the lost bytes end the file or read as
// zeros
fn torn_record_drops(loss: Loss) {
    let base = std::env::temp_dir().join(format!("reel-torn-{loss:?}-{}", process::id()));
    let _gone = fs::remove_dir_all(&base);
    let (live, copy) = (base.join("live"), base.join("copy"));
    fs::create_dir_all(&live).expect("root");
    let store = ReelStore::open(PlainIo::for_tests(), &config(&live)).expect("open");
    for number in 0..SYNCED {
        store.put(COLUMN, &number.to_be_bytes(), &number.to_le_bytes()).expect("put");
    }
    store.flush().expect("sync the first rows");
    let before = rows_bytes(&live);
    store.put(COLUMN, &LAST.to_be_bytes(), &last_value()).expect("put the last");
    let started = Instant::now();
    while rows_bytes(&live) == before {
        assert!(started.elapsed() < ROW_WAIT, "the last row never reached the rows file");
        std::thread::sleep(Duration::from_millis(5));
    }
    copy_root(&live, &copy);
    store.close().expect("close");

    let data = files_with(&copy, DATA_SUFFIX);
    let (path, at) = data
        .iter()
        .find_map(|path| {
            let bytes = fs::read(path).expect("read a data file");
            bytes.windows(LAST_BYTES).position(|window| window == last_value()).map(|at| (path.clone(), at))
        })
        .expect("the last record in the copy");
    match loss {
        Loss::PastTheEnd => fs::OpenOptions::new().write(true).open(&path).expect("open").set_len(at as u64).expect("cut"),
        Loss::Zeros => {
            let mut bytes = fs::read(&path).expect("read");
            bytes[at..at + LAST_BYTES].fill(0);
            fs::write(&path, bytes).expect("zero");
        },
    }

    let store = ReelStore::open(PlainIo::for_tests(), &config(&copy)).unwrap_or_else(|error| panic!("{loss:?}: the open refused the copy: {error}"));
    let mut value = Vec::new();
    assert!(!store.get(COLUMN, &LAST.to_be_bytes(), &mut value).expect("get the last"), "{loss:?}: the lost record read back");
    for number in 0..SYNCED {
        assert!(store.get(COLUMN, &number.to_be_bytes(), &mut value).expect("get"), "{loss:?}: synced row {number} is gone");
        assert_eq!(value, number.to_le_bytes());
    }
    store.close().expect("close");
    let _gone = fs::remove_dir_all(&base);
}

#[test]
fn a_record_past_the_end_drops() {
    torn_record_drops(Loss::PastTheEnd);
}

#[test]
fn a_record_of_zeros_drops() {
    torn_record_drops(Loss::Zeros);
}
