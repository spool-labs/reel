//! A segment of the widest keys fills to its row limit and seals into the list its seal plans for

use std::path::PathBuf;

use reel::config::{ColumnConfig, ColumnIndex, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::engine_sizing::rows_within_blocks;
use reel::engine::reel_store::ReelStore;
use reel::guard::hold;
use reel::record::codec::Codec;
use reel_core::column_id::ColumnId;
use reel_core::units::ByteCount;
use reel_core::write_batch::WriteBatch;
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_io::SimulatedIo;

const COLUMN: ColumnId = ColumnId(1);
const WIDEST_KEY: usize = 1_056;
const BUDGET: ByteCount = ByteCount(1024 * 1024 * 1024);
const BATCH_ROWS: u64 = 8_192;

fn config() -> StoreConfig {
    StoreConfig {
        memory_budget: BUDGET,
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig::new("wide", vec![PathBuf::from("/disk/wide")], SyncPolicy::Never)],
        columns: vec![ColumnConfig {
            codec: Codec::None,
            index: ColumnIndex::Plain,
            ..ColumnConfig::new(COLUMN, "wide", KeyShape::Variable { most: WIDEST_KEY as u16 })
        }],
    }
}

// a key of the widest size, its number at the front so the keys differ early
fn key_of(number: u64) -> Vec<u8> {
    let mut key = vec![b'k'; WIDEST_KEY];
    key[..8].copy_from_slice(&number.to_be_bytes());
    key
}

// a store of the widest keys fills its segment to the row limit its seal's list holds, seals it with every row, and reopens
// with every row read back. The budget's limit once ran past the list and the open refused the store
#[test]
fn widest_keys_fill_and_seal() {
    let io = SimulatedIo::new(Seed(91));
    let store = ReelStore::open(io.clone(), &config()).expect("open");
    let volume = &store.state.volumes[0];
    let limit = volume.segment_rows.0;
    assert!(limit <= rows_within_blocks(volume.seal_blocks, WIDEST_KEY), "the budget's limit stayed under the list");
    let mut written = 0;
    while written < limit {
        let mut batch = WriteBatch::new();
        for number in written..(written + BATCH_ROWS).min(limit) {
            batch.put(COLUMN, &key_of(number), &number.to_le_bytes()[..1]);
        }
        store.write_batch(&batch).expect("batch");
        written = (written + BATCH_ROWS).min(limit);
    }
    let open_rows: Vec<u64> = hold(&volume.claims).open.iter().map(|segment| segment.row_count.0).collect();
    assert!(open_rows.iter().all(|rows| *rows <= limit), "a segment took {open_rows:?} rows past {limit}");
    store.seal_now();
    while io.step_any().is_some() {}
    let mut sealed = 0;
    volume.table.for_each_segment(|_number, _root, _file| sealed += 1);
    assert!(sealed >= 1, "nothing sealed");
    store.close().expect("close");

    let store = ReelStore::open(io.clone(), &config()).expect("reopen");
    let mut value = Vec::new();
    for number in (0..limit).step_by(97).chain([limit - 1]) {
        assert!(store.get(COLUMN, &key_of(number), &mut value).expect("get"), "row {number} of {limit} is gone");
        assert_eq!(value, number.to_le_bytes()[..1]);
    }
    store.close().expect("close");
}
