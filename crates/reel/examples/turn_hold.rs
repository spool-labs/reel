//! How long a long batch holds a writing turn, the time another writer's rows would wait on a shared segment.
//! Run with `cargo run --release -p reel --example turn_hold -- <budget MiB> <key bytes> <row share %> [root]`
#![allow(clippy::expect_used, clippy::print_stdout)]

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process;
use std::time::Instant;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::io::plain::PlainIo;
use reel::record::codec::Codec;
use reel::rows::row_group::ROW_HEADER_BYTES;
use reel_core::column_id::ColumnId;
use reel_core::units::{ByteCount, VolumeId};
use reel_core::write_batch::WriteBatch;

const COLUMN: ColumnId = ColumnId(1);
const MIB: u64 = 1024 * 1024;
const VALUE: &[u8] = b"0123456789";
const NANOS_PER_MILLI: f64 = 1_000_000.0;
const MILLIS_PER_SECOND: f64 = 1_000.0;
const WHOLE_PERCENT: u64 = 100;

fn argument(index: usize, name: &str) -> u64 {
    let text = env::args().nth(index).expect(name);
    text.parse().expect(name)
}

fn main() {
    let budget = ByteCount(argument(1, "budget in MiB") * MIB);
    let key_bytes = argument(2, "key bytes") as usize;
    let share = argument(3, "row share in percent");
    let base = env::args().nth(4).map_or_else(env::temp_dir, PathBuf::from);
    let root = base.join(format!("reel-turn-{}", process::id()));
    fs::create_dir_all(&root).expect("create");

    let config = StoreConfig {
        memory_budget: budget,
        threads: Default::default(),
        volumes: vec![VolumeConfig {
            name: "turn".to_string(),
            roots: vec![root.clone()],
            sync: SyncPolicy::Never,
            device: Default::default(), open_segments: None,
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "turn".to_string(),
            key_shape: KeyShape::Variable { most: key_bytes as u16 },
            codec: Codec::None,
            volume: VolumeId(0),
            complete_rows: None,
            sorted_rows: None,
            least_value: None,
        }],
    };
    let store = ReelStore::open(PlainIo::new(), &config).expect("open");
    let rows = store.state.volumes[0].segment_rows.0 * share / WHOLE_PERCENT;
    let mut batch = WriteBatch::new();
    for number in 0..rows {
        batch.put(COLUMN, format!("{number:0key_bytes$}").as_bytes(), VALUE);
    }

    let start = Instant::now();
    store.write_batch(&batch).expect("batch");
    let whole = start.elapsed().as_secs_f64() * MILLIS_PER_SECOND;
    let turn = store.longest_turn().0 as f64 / NANOS_PER_MILLI;
    let row_bytes = rows * (ROW_HEADER_BYTES + key_bytes) as u64;
    println!("{} MiB, {rows} rows, {row_bytes} B of rows: turn {turn:.1} ms, batch {whole:.1} ms", budget.0 / MIB);

    store.close().expect("close");
    drop(store);
    fs::remove_dir_all(&root).expect("remove");
}
