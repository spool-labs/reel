//! Keys and bounds at the edges of what a column holds

use std::path::PathBuf;
use std::sync::Arc;

use tempfile::TempDir;

use reel::config::{CompactRate, ReelConfig, SyncPolicy, ThreadBudget};
use reel::format::column::{Codec, ColumnId, ColumnSet, ColumnSpec};
use reel::io::fault::{FaultKind, FaultPlan};
use reel::io::sim_backend::SimIo;
use reel::units::ByteCount;
use reel::{KeyWidth, ReelStore, MAX_KEY_LEN};
use reel_core::{Direction, Store};

const fn column(id: u8, name: &'static str, key_width: KeyWidth, shard_bytes: u8) -> ColumnSpec {
    ColumnSpec {
        id: ColumnId(id),
        name,
        key_width,
        shard_bytes,
        purge_mark: None,
        codec: Codec::None,
    }
}

const COLUMNS: ColumnSet = &[
    column(1, "rows", KeyWidth::Variable, 0),
    column(2, "sharded", KeyWidth::Variable, 2),
    column(3, "unit", KeyWidth::Fixed(0), 1),
];

const ROOT: &str = "/edge";

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(96 * 1024),
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        compact_dead_ratio: 1.0,
        compact_mbps: CompactRate::Mbps(100_000),
        ..ReelConfig::default()
    }
}

// a merge whose run fails to write leaves no part file, however many ticks retry it
#[test]
fn a_failed_merge_leaves_no_part_file() {
    let sim = SimIo::new(FaultPlan::new(1));
    let store = ReelStore::open_with_io(
        PathBuf::from(ROOT),
        config(),
        COLUMNS,
        Arc::new(sim.clone()),
    )
    .expect("open");
    for n in 0..6_000u64 {
        let key = n.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes();
        Store::put(&store, "rows", &key, &[7u8; 120]).expect("put");
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("hand over");
    assert!(
        store.index().overlap_depth() > 8,
        "too few runs stacked to merge"
    );

    for _ in 0..3 {
        // Every append fails as though the volume is full
        sim.arm_next_ops(10_000, FaultKind::EnospcAppend);
        let merged = store.merge_when_due();
        sim.disarm();
        assert!(
            merged.is_err(),
            "the merge wrote its run past a full volume"
        );
    }
    let parts: Vec<String> = store
        .driver()
        .list_or_empty(&PathBuf::from(ROOT))
        .expect("list")
        .into_iter()
        .map(|entry| entry.name)
        .filter(|name| name.ends_with(".part"))
        .collect();
    assert!(parts.is_empty(), "failed merges left {parts:?}");

    let merged = store.merge_when_due().expect("merge");
    assert!(
        merged.is_some(),
        "the merge did not go through once the volume had room"
    );
}

// a walk from a start wider than any key still finds the keys on its side of it
#[test]
fn a_walk_from_a_start_wider_than_any_key_finds_keys() {
    let dir = TempDir::new().expect("temp dir");
    let store = ReelStore::open(dir.path().to_path_buf(), config(), COLUMNS).expect("open");
    Store::put(&store, "rows", &[0x01], b"one").expect("put");
    Store::put(&store, "rows", &[0x00; 8], b"low").expect("put");
    Store::put(&store, "rows", &[0x00; MAX_KEY_LEN], b"widest").expect("put");
    let walk = |start: &[u8], direction: Direction| -> Vec<Vec<u8>> {
        Store::iter_from(&store, "rows", start, direction)
            .expect("walk")
            .map(|(key, _)| key)
            .collect()
    };

    // The widest key shares the start's first bytes and sorts below it
    let up = walk(&[0x00; MAX_KEY_LEN + 1], Direction::Asc);
    assert_eq!(up, vec![vec![0x01]]);
    let down = walk(&[0x00; MAX_KEY_LEN + 1], Direction::Desc);
    assert_eq!(down, vec![vec![0x00; MAX_KEY_LEN], vec![0x00; 8]]);
    let all_down = walk(&[0xFF; MAX_KEY_LEN + 1], Direction::Desc);
    assert_eq!(all_down.len(), 3);
}

// a count under a shard-wide prefix leaves out shorter keys that do not carry it
#[test]
fn a_shard_prefix_count_leaves_out_short_keys() {
    let dir = TempDir::new().expect("temp dir");
    let store = ReelStore::open(dir.path().to_path_buf(), config(), COLUMNS).expect("open");
    Store::put(&store, "sharded", &[0x01], b"short").expect("put");
    Store::put(&store, "sharded", &[0x01, 0x00, 0x07], b"long").expect("put");
    Store::put(&store, "unit", &[], b"unit").expect("put");

    let walked = Store::iter_prefix(&store, "sharded", &[0x01, 0x00])
        .expect("walk")
        .count() as u64;
    assert_eq!(walked, 1);
    assert_eq!(
        Store::count_prefix(&store, "sharded", &[0x01, 0x00]).expect("count"),
        walked,
        "the count disagrees with the walk under the same prefix"
    );
    assert_eq!(
        Store::count_prefix(&store, "unit", &[0x00]).expect("count"),
        0
    );
}
