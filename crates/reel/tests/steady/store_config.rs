//! The store both memory harnesses open, in a scratch root of their own

use std::fs;
use std::path::{Path, PathBuf};
use std::process;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::record::codec::Codec;
use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, RowCount, VolumeId};

pub const META: ColumnId = ColumnId(1);
pub const BULK: ColumnId = ColumnId(2);
pub const PLAIN: ColumnId = ColumnId(3);
const MOST_KEY_BYTES: u16 = 64;
const TABLE_ROWS: RowCount = RowCount(4_000_000);
const TABLE_BUDGET: ByteCount = ByteCount(512 * 1024 * 1024);

pub fn config(root: &Path, budget: ByteCount) -> StoreConfig {
    let column = |id: ColumnId, name: &str, codec: Codec, complete_rows: Option<RowCount>| ColumnConfig {
        id,
        name: name.to_string(),
        key_shape: KeyShape::Variable { most: MOST_KEY_BYTES },
        codec,
        volume: VolumeId(0),
        complete_rows,
        sorted_rows: None,
        least_value: None,
    };
    // a complete column sets aside 29 MiB of block lines at any row count, more than the 128 and 192 MiB budgets have spare
    let table_rows = (budget.0 >= TABLE_BUDGET.0).then_some(TABLE_ROWS);
    StoreConfig {
        memory_budget: budget,
        threads: Default::default(),
        volumes: vec![VolumeConfig {
            name: "steady".to_string(),
            roots: vec![root.to_path_buf()],
            sync: SyncPolicy::Never,
            device: Default::default(),
        }],
        columns: vec![column(META, "meta", Codec::None, table_rows), column(BULK, "bulk", Codec::Lz4, None), column(PLAIN, "plain", Codec::None, None)],
    }
}

pub fn scratch_root() -> PathBuf {
    let root = std::env::temp_dir().join(format!("reel-steady-{}", process::id()));
    fs::create_dir_all(&root).expect("create");
    root
}
