//! Every wholly dead segment drains in one pass, not one per tick
//!
//! A rolling purge can retire segments faster than the maintenance tick fires,
//! and a one-segment-per-pass limit builds a dead backlog behind a bound meant
//! for copying. An unlink copies nothing, so a single pass takes them all.

use std::path::PathBuf;
use std::sync::Arc;

use reel::io::fault::FaultPlan;
use reel::io::sim_backend::SimIo;
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape, Preallocate, ReelConfig,
    ReelStore, SyncPolicy, ThreadBudget,
};
use reel_core::Store;

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: "rows",
    key_width: KeyWidth::Fixed(8),
    shard_bytes: 0,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

// kill five segments, compact once, and all five are gone
#[test]
fn one_pass_drains_every_wholly_dead_segment() {
    let sim = SimIo::new(FaultPlan::new(23));
    let store = ReelStore::open_with_io(
        PathBuf::from("/drain"),
        ReelConfig {
            segment_bytes: ByteCount::from_bytes(128 * 1024),
            alloc_chunk: ByteCount::from_bytes(32 * 1024),
            preallocate: Preallocate::Chunk,
            sync: SyncPolicy::Never,
            active_tails: ThreadBudget::threads(1),
            ..ReelConfig::default()
        },
        COLUMNS,
        Arc::new(sim),
    )
    .expect("open");

    let payload = vec![0x5Au8; 4096];
    for at in 0..200u64 {
        Store::put(&store, "rows", &at.to_be_bytes(), &payload).expect("put");
    }
    for at in 0..200u64 {
        Store::delete(&store, "rows", &at.to_be_bytes()).expect("delete");
    }

    let before = store.compaction_counters();
    assert_eq!(
        before.segments_unlinked_whole, 0,
        "nothing retired before the pass"
    );

    store.compact_once().expect("compact");

    let after = store.compaction_counters();
    assert!(
        after.segments_unlinked_whole >= 4,
        "one pass drained {} segments, the backlog remains",
        after.segments_unlinked_whole
    );
    for at in [0u64, 100, 199] {
        assert!(Store::get(&store, "rows", &at.to_be_bytes())
            .expect("get")
            .is_none());
    }
}
