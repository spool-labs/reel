//! How long one range delete takes as the covers waiting to apply grow, the case the store bench times.
//! Run with `cargo run --release -p tape-reel --example range_delete -- <budget MiB> [root]`
#![allow(clippy::expect_used, clippy::print_stdout)]

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process;
use std::time::Instant;

use reel::config::{ColumnConfig, ColumnIndex, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::io::plain::PlainIo;
use reel::record::codec::Codec;
use reel_core::column_id::ColumnId;
use reel_core::units::{ByteCount, VolumeId};

const COLUMN: ColumnId = ColumnId(1);
const MIB: u64 = 1024 * 1024;
const KEYS: u64 = 200_000;
const DELETES: u64 = 3_000;
const REPORT_EVERY: u64 = 500;

fn key_of(number: u64) -> [u8; 16] {
    let mut key = [0; 16];
    key[8..].copy_from_slice(&number.to_be_bytes());
    key
}

fn main() {
    let budget = ByteCount(env::args().nth(1).expect("budget in MiB").parse::<u64>().expect("budget") * MIB);
    let base = env::args().nth(2).map_or_else(env::temp_dir, PathBuf::from);
    let root = base.join(format!("reel-range-{}", process::id()));
    fs::create_dir_all(&root).expect("create");
    let config = StoreConfig {
        memory_budget: budget,
        max_memory: None,
        threads: Default::default(),
        volumes: vec![VolumeConfig {
            name: "range".to_string(),
            roots: vec![root.clone()],
            sync: SyncPolicy::Never,
            device: Default::default(), open_segments: None, segment_bytes: None, lifetime_groups: None,
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "range".to_string(),
            key_shape: KeyShape::Fixed { width: 16 },
            codec: Codec::None,
            volume: VolumeId(0),
            index: ColumnIndex::Plain,
            least_rows: None,
            least_value: None,
        }],
    };
    let store = ReelStore::open(PlainIo::new(), &config).expect("open");
    for number in 0..KEYS {
        store.put(COLUMN, &key_of(number), &[7; 100]).expect("put");
    }
    store.seal_now();
    std::thread::sleep(std::time::Duration::from_millis(500));
    let first = Instant::now();
    store.delete_range(COLUMN, &key_of(KEYS - 20), &key_of(KEYS - 10)).expect("delete");
    println!("first delete after the load: {} ns", first.elapsed().as_nanos());
    let second = Instant::now();
    store.delete_range(COLUMN, &key_of(KEYS - 9), &key_of(KEYS - 5)).expect("delete");
    println!("second delete: {} ns", second.elapsed().as_nanos());
    let mut slowest = 0_u128;
    let mut started = Instant::now();
    for delete in 0..DELETES {
        let at = delete * (KEYS / DELETES);
        let begun = Instant::now();
        store.delete_range(COLUMN, &key_of(at), &key_of(at + 10)).expect("delete");
        slowest = slowest.max(begun.elapsed().as_nanos());
        if (delete + 1) % REPORT_EVERY == 0 {
            let mean = started.elapsed().as_nanos() / u128::from(REPORT_EVERY);
            println!("deletes {:>5}: mean {:>7} ns, slowest {:>9} ns, covers waiting {}", delete + 1, mean, slowest, store.state.volumes[0].covers.len());
            slowest = 0;
            started = Instant::now();
        }
    }
    store.close().expect("close");
    fs::remove_dir_all(&root).expect("remove");
}
