//! A compaction pass reads the live records of a segment and skips its dead stretches

use std::path::PathBuf;
use std::sync::Arc;

use reel::io::fault::FaultPlan;
use reel::io::sim_backend::SimIo;
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, ReelConfig, ReelStore, SyncPolicy,
    ThreadBudget,
};
use reel_core::Store;

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: "rows",
    key_width: KeyWidth::Fixed(8),
    shard_bytes: 0,
    purge_mark: None,
    codec: Codec::None,
}];

const SEGMENT: u64 = 1024 * 1024;
const VALUE: usize = 4096;

// overwrite the front of a full segment, drain, and the pass reads about the live tail only
#[test]
fn a_pass_skips_a_dead_stretch() {
    let sim = SimIo::new(FaultPlan::new(29));
    let store = ReelStore::open_with_io(
        PathBuf::from("/reads"),
        ReelConfig {
            segment_bytes: ByteCount::from_bytes(SEGMENT),
            sync: SyncPolicy::Never,
            active_tails: ThreadBudget::threads(1),
            ..ReelConfig::default()
        },
        COLUMNS,
        Arc::new(sim),
    )
    .expect("open");

    let first = vec![0x11u8; VALUE];
    let second = vec![0x22u8; VALUE];
    for at in 0..400u64 {
        Store::put(&store, "rows", &at.to_be_bytes(), &first).expect("put");
    }
    // the first segment holds about 250 records, so this kills the front of it
    for at in 0..200u64 {
        Store::put(&store, "rows", &at.to_be_bytes(), &second).expect("overwrite");
    }
    store.flush().expect("flush");

    let before = store.compaction_counters();
    store.drain().expect("drain");
    let after = store.compaction_counters();

    let rewritten = after.segments_rewritten - before.segments_rewritten;
    let read = after.read_bytes - before.read_bytes;
    assert!(rewritten >= 1, "the drain rewrote nothing");
    assert!(
        read < rewritten * SEGMENT / 2,
        "{rewritten} rewritten segments read {read} bytes, the dead front was read too"
    );
    for at in [0u64, 150, 199] {
        let value = Store::get(&store, "rows", &at.to_be_bytes()).expect("get");
        assert_eq!(value.as_deref(), Some(second.as_slice()), "key {at}");
    }
    for at in [200u64, 260, 399] {
        let value = Store::get(&store, "rows", &at.to_be_bytes()).expect("get");
        assert_eq!(value.as_deref(), Some(first.as_slice()), "key {at}");
    }
}
