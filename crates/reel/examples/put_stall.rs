//! The slowest small put at a thread count, and how often and how long writers waited for the write index.
//! Run with `cargo run --release -p reel --example put_stall -- <budget MiB> <threads> <seconds> [root]`
#![allow(clippy::expect_used, clippy::print_stdout)]

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::io::plain::PlainIo;
use reel::record::codec::Codec;
use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, VolumeId};

const COLUMN: ColumnId = ColumnId(1);
const MIB: u64 = 1024 * 1024;
const VALUE: [u8; 100] = [7; 100];
const KEY_MIX: u64 = 0x9E37_79B9_7F4A_7C15;

fn argument(index: usize, name: &str) -> u64 {
    env::args().nth(index).expect(name).parse().expect(name)
}

fn main() {
    let budget = ByteCount(argument(1, "budget in MiB") * MIB);
    let threads = argument(2, "threads");
    let seconds = argument(3, "seconds");
    let base = env::args().nth(4).map_or_else(env::temp_dir, PathBuf::from);
    let root = base.join(format!("reel-stall-{}", process::id()));
    fs::create_dir_all(&root).expect("create");
    let config = StoreConfig {
        memory_budget: budget,
        threads: Default::default(),
        volumes: vec![VolumeConfig {
            name: "stall".to_string(),
            roots: vec![root.clone()],
            sync: SyncPolicy::Never,
            device: Default::default(),
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "stall".to_string(),
            key_shape: KeyShape::Variable { most: 32 },
            codec: Codec::None,
            volume: VolumeId(0),
            complete_rows: None,
        }],
    };
    let store = ReelStore::open(PlainIo::new(), &config).expect("open");
    let is_done = AtomicBool::new(false);
    let slowest = AtomicU64::new(0);
    let puts = AtomicU64::new(0);

    thread::scope(|scope| {
        for thread_number in 0..threads {
            let (store, is_done, slowest, puts) = (&store, &is_done, &slowest, &puts);
            scope.spawn(move || {
                let mut number = thread_number << 40;
                while !is_done.load(Ordering::Relaxed) {
                    number += 1;
                    let key = number.wrapping_mul(KEY_MIX).to_le_bytes();
                    let start = Instant::now();
                    store.put(COLUMN, &key, &VALUE).expect("put");
                    slowest.fetch_max(start.elapsed().as_micros() as u64, Ordering::Relaxed);
                    puts.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
        thread::sleep(Duration::from_secs(seconds));
        is_done.store(true, Ordering::Relaxed);
    });

    let (waits, waited) = store.index_waits();
    println!(
        "{threads} threads: {} puts a second, slowest {} us, {waits} index waits, {} us waited at most",
        puts.load(Ordering::Relaxed) / seconds,
        slowest.load(Ordering::Relaxed),
        waited.0 / 1_000,
    );
    store.close().expect("close");
    drop(store);
    fs::remove_dir_all(&root).expect("remove");
}
