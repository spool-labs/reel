use std::fs;
use std::path::{Path, PathBuf};
use std::process;

use reel::config::{ColumnConfig, ColumnIndex, KeyShape, StoreConfig, SyncPolicy, ThreadCounts, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::io::plain::PlainIo;
use reel::record::codec::Codec;
use reel_core::column_id::ColumnId;
use reel_core::units::{ByteCount, RowCount, VolumeId, WritePlace};

const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(1024 * 1024 * 1024);
const KEYS: u64 = 200_000;
const ROUNDS: u64 = 4;
const OPENS: usize = 50;
const FILL_THREADS: usize = 8;

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

fn key(number: u64) -> [u8; 16] {
    let mut key = [0; 16];
    // spread over every part of the index, and in order within each
    key[..8].copy_from_slice(&number.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes());
    key[8..].copy_from_slice(&number.to_be_bytes());
    key
}

// every live entry of the index after an open, by key with the place of its row. A merge after the open moves an entry's
// segment and offset and keeps its place, so the places say what the fill gave
fn live_entries(root: &Path, workers: usize) -> Vec<([u8; 16], WritePlace)> {
    let store = ReelStore::open(PlainIo::new(), &config(root, workers)).expect("open");
    let mut entries = Vec::new();
    store.state.sorted_index(COLUMN).expect("an index").visit(|name, entry| {
        if !entry.is_grave {
            entries.push((name.try_into().expect("a 16 byte key"), entry.place));
        }
    });
    store.close().expect("close");
    entries
}

// a store of several seals with overwrites and deletes, filled at open on one thread and on eight, gives the same entries every
// time: the only test where two fill threads run at once
#[test]
fn a_fill_on_eight_threads_matches_one_every_time() {
    let root = std::env::temp_dir().join(format!("reel-sorted-fill-{}", process::id()));
    let _gone = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("root");
    {
        let store = ReelStore::open(PlainIo::new(), &config(&root, FILL_THREADS)).expect("open");
        for round in 0..ROUNDS {
            for number in (0..KEYS).filter(|number| round == 0 || number % (round + 2) == 0) {
                store.put(COLUMN, &key(number), format!("{round}-{number}").as_bytes()).expect("put");
            }
            for number in (round..KEYS).step_by(97) {
                store.delete(COLUMN, &key(number)).expect("delete");
            }
            store.seal_now();
        }
        store.finish().expect("finish");
        store.close().expect("close");
    }
    let one = live_entries(&root, 0);
    assert!(one.len() as u64 > KEYS / 2, "the store holds {} live keys", one.len());
    for open in 0..OPENS {
        assert!(live_entries(&root, FILL_THREADS) == one, "open {open}: eight fill threads gave other entries than one");
    }
    let _gone = fs::remove_dir_all(PathBuf::from(&root));
}
