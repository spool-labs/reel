use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process;
use std::thread;
use std::time::{Duration, SystemTime};

use reel::config::{ColumnConfig, ColumnIndex, KeyShape, StoreConfig, SyncPolicy, ThreadCounts, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::guard::hold;
use reel::io::plain::PlainIo;
use reel::record::codec::Codec;
use reel_core::column_id::ColumnId;
use reel_core::direction::Direction;
use reel_core::maintenance_report::{MaintenanceReport, SortedReading};
use reel_core::scan::ScanStep;
use reel_core::units::{ByteCount, RowCount, VolumeId, WritePlace};

const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(1024 * 1024 * 1024);
const KEYS: u64 = 200_000;
const ROUNDS: u64 = 4;
const OPENS: usize = 50;
const FILL_THREADS: usize = 8;
const VIEW_KEYS: u64 = 50_000;
const LATE_KEYS: u64 = 20_000;
const SEAL_WAIT: Duration = Duration::from_millis(5);

type Store = ReelStore<PlainIo>;

fn config(root: &Path, workers: usize) -> StoreConfig {
    StoreConfig {
        memory_budget: BUDGET,
        threads: ThreadCounts { maintenance_workers: Some(workers), ..ThreadCounts::default() },
        volumes: vec![VolumeConfig {
            name: "fill".to_string(),
            roots: vec![root.to_path_buf()],
            sync: SyncPolicy::Never,
            device: Default::default(),
            open_segments: None,
            segment_bytes: None,
            lifetime_groups: None,
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "sorted".to_string(),
            key_shape: KeyShape::Fixed { width: 16 },
            codec: Codec::None,
            volume: VolumeId(0),
            index: ColumnIndex::Sorted,
            least_rows: Some(RowCount(KEYS * 2)),
            least_value: None,
        }],
    }
}

fn fresh_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!("reel-{name}-{}", process::id()));
    let _gone = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("root");
    root
}

fn key(number: u64) -> [u8; 16] {
    let mut key = [0; 16];
    // spread over every part of the index, and in order within each
    key[..8].copy_from_slice(&number.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes());
    key[8..].copy_from_slice(&number.to_be_bytes());
    key
}

// several seals of puts, overwrites and deletes over the first keys
fn write_rounds(store: &Store, keys: u64, seal: impl Fn(&Store)) {
    for round in 0..ROUNDS {
        for number in (0..keys).filter(|number| round == 0 || number % (round + 2) == 0) {
            store.put(COLUMN, &key(number), format!("{round}-{number}").as_bytes()).expect("put");
        }
        for number in (round..keys).step_by(97) {
            store.delete(COLUMN, &key(number)).expect("delete");
        }
        seal(store);
    }
}

// wait until no open segment holds a row, so every row written so far sits in a sealed segment
fn seal_all(store: &Store) {
    loop {
        let is_sealed = store.state.volumes.iter().all(|volume| hold(&volume.claims).open.iter().all(|segment| segment.row_count.0 == 0));
        if is_sealed {
            return;
        }
        store.seal_now();
        thread::sleep(SEAL_WAIT);
    }
}

// every live entry of the index, by key with the place of its row
fn index_entries(store: &Store) -> Vec<([u8; 16], WritePlace)> {
    let mut entries = Vec::new();
    store.state.sorted_index(COLUMN).expect("an index").visit(|name, entry| {
        if !entry.is_grave {
            entries.push((name.try_into().expect("a 16 byte key"), entry.place));
        }
    });
    entries
}

// every entry of the index, graves among them
fn all_entries(store: &Store) -> u64 {
    let mut count = 0;
    store.state.sorted_index(COLUMN).expect("an index").visit(|_name, _entry| count += 1);
    count
}

// every live entry of the index after an open. A merge after the open moves an entry's segment and offset and keeps its place,
// so the places say what the fill gave
fn live_entries(root: &Path, workers: usize) -> Vec<([u8; 16], WritePlace)> {
    let store = ReelStore::open(PlainIo::new(), &config(root, workers)).expect("open");
    let entries = index_entries(&store);
    store.close().expect("close");
    entries
}

fn sorted_reading(store: &Store) -> SortedReading {
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    let mut readings = report.sorted_columns.into_iter().filter(|reading| reading.column == COLUMN);
    readings.next().expect("a reading of the sorted column")
}

fn gets(store: &Store, keys: u64) -> Vec<Option<Vec<u8>>> {
    let mut buf = Vec::new();
    (0..keys).map(|number| store.get(COLUMN, &key(number), &mut buf).expect("get").then(|| buf.clone())).collect()
}

fn scan_rows(store: &Store) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
    let (mut name, mut value, mut rows) = (Vec::new(), Vec::new(), BTreeMap::new());
    while scan.next_row(&mut name, &mut value).expect("row") == ScanStep::Found {
        rows.insert(name.clone(), value.clone());
    }
    rows
}

// every file and directory under the root with its bytes and modified time
fn root_files(root: &Path) -> BTreeMap<PathBuf, (Vec<u8>, SystemTime)> {
    let mut files = BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        let metadata = fs::metadata(&path).expect("metadata");
        let modified = metadata.modified().expect("modified time");
        if metadata.is_dir() {
            for entry in fs::read_dir(&path).expect("listing") {
                pending.push(entry.expect("entry").path());
            }
            files.insert(path, (Vec::new(), modified));
        } else {
            files.insert(path.clone(), (fs::read(&path).expect("file bytes"), modified));
        }
    }
    files
}

// a store of several seals with overwrites and deletes, filled at open on one thread and on eight, gives the same entries every
// time: the only test where two fill threads run at once
#[test]
fn a_fill_on_eight_threads_matches_one_every_time() {
    let root = fresh_root("sorted-fill");
    {
        let store = ReelStore::open(PlainIo::new(), &config(&root, FILL_THREADS)).expect("open");
        write_rounds(&store, KEYS, Store::seal_now);
        store.finish().expect("finish");
    }
    let one = live_entries(&root, 0);
    assert!(one.len() as u64 > KEYS / 2, "the store holds {} live keys", one.len());
    for open in 0..OPENS {
        assert!(live_entries(&root, FILL_THREADS) == one, "open {open}: eight fill threads gave other entries than one");
    }
    let _gone = fs::remove_dir_all(&root);
}

// a read-only open fills its sorted index as a writer's open does, reports it, and answers gets and scans with the same rows
#[test]
fn a_read_only_open_fills_the_sorted_index_a_writer_open_does() {
    let root = fresh_root("sorted-read-only-fill");
    {
        let store = ReelStore::open(PlainIo::new(), &config(&root, FILL_THREADS)).expect("open");
        write_rounds(&store, VIEW_KEYS, Store::seal_now);
        store.finish().expect("finish");
    }
    let writer = ReelStore::open(PlainIo::new(), &config(&root, 0)).expect("open");
    let (entries, writer_reading, writer_gets, writer_rows) = (index_entries(&writer), sorted_reading(&writer), gets(&writer, VIEW_KEYS), scan_rows(&writer));
    writer.close().expect("close");

    let reader = ReelStore::open_read_only(PlainIo::new(), &config(&root, 0)).expect("open read-only");
    assert!(index_entries(&reader) == entries, "the read-only fill gave other entries than the writer's");
    let reading = sorted_reading(&reader);
    let counts = |reading: &SortedReading| (reading.used, reading.rows_past_capacity, reading.is_marked);
    assert_eq!(counts(&reading), counts(&writer_reading));
    assert_eq!(reading.used, RowCount(all_entries(&reader)), "used counts every entry the index holds");
    assert!(!reading.is_marked && reading.rows_past_capacity.0 == 0, "the index ran out of room: {reading:?}");
    let live = writer_gets.iter().filter(|value| value.is_some()).count();
    assert!(entries.len() == live && writer_rows.len() == live, "{} entries, {} scanned rows, {live} live keys", entries.len(), writer_rows.len());
    assert!(gets(&reader, VIEW_KEYS) == writer_gets, "a read-only get gave another row than the writer's");
    assert!(scan_rows(&reader) == writer_rows, "a read-only scan gave other rows than the writer's");
    reader.close().expect("close");
    let _gone = fs::remove_dir_all(&root);
}

// a read-only open beside a live writer reads every row the writer sealed before it opened, while the writer keeps writing
#[test]
fn a_read_only_open_beside_a_writer_reads_every_sealed_row() {
    let root = fresh_root("sorted-read-only-live");
    let writer = ReelStore::open(PlainIo::new(), &config(&root, FILL_THREADS)).expect("open");
    write_rounds(&writer, VIEW_KEYS, seal_all);
    let sealed = gets(&writer, VIEW_KEYS);
    thread::scope(|scope| {
        let late = scope.spawn(|| {
            for number in VIEW_KEYS..VIEW_KEYS + LATE_KEYS {
                writer.put(COLUMN, &key(number), b"late").expect("put");
            }
        });
        let reader = ReelStore::open_read_only(PlainIo::new(), &config(&root, 0)).expect("open read-only");
        let reading = sorted_reading(&reader);
        let live = sealed.iter().filter(|value| value.is_some()).count() as u64;
        assert!(reading.used.0 >= live, "the index holds {} entries under {live} sealed live keys", reading.used.0);
        assert!(gets(&reader, VIEW_KEYS) == sealed, "a read-only get missed a sealed row");
        let rows = scan_rows(&reader);
        for (number, value) in sealed.iter().enumerate() {
            assert_eq!(rows.get(key(number as u64).as_slice()), value.as_ref(), "key {number} in the scan");
        }
        reader.close().expect("close");
        late.join().expect("the late writer");
    });
    writer.close().expect("close");
    let _gone = fs::remove_dir_all(&root);
}

// a read-only open, its fill, gets, scans and close leave every file and directory in the root as it was
#[test]
fn a_read_only_open_leaves_the_roots_as_they_were() {
    let root = fresh_root("sorted-read-only-files");
    {
        let store = ReelStore::open(PlainIo::new(), &config(&root, FILL_THREADS)).expect("open");
        write_rounds(&store, VIEW_KEYS, Store::seal_now);
        store.finish().expect("finish");
    }
    let before = root_files(&root);
    let reader = ReelStore::open_read_only(PlainIo::new(), &config(&root, 0)).expect("open read-only");
    assert!(sorted_reading(&reader).used.0 > 0, "the read-only open filled no index");
    let _gets = gets(&reader, VIEW_KEYS);
    let _rows = scan_rows(&reader);
    reader.close().expect("close");
    let after = root_files(&root);
    assert_eq!(before.keys().collect::<Vec<_>>(), after.keys().collect::<Vec<_>>(), "the root's files changed");
    for (path, (bytes, modified)) in &before {
        let (after_bytes, after_modified) = &after[path];
        assert!(bytes == after_bytes, "{} changed its bytes", path.display());
        assert_eq!(modified, after_modified, "{} changed its modified time", path.display());
    }
    let _gone = fs::remove_dir_all(&root);
}
