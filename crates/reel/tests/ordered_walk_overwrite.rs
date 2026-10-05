//! An ordered walk stays on its index when a key in its range was overwritten since its seal

use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, IndexResidency, KeyWidth, MapShape,
    ReelConfig, ReelStore, SyncPolicy, ThreadBudget,
};
use reel_core::Store;

const ORDERED: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: "rows",
    key_width: KeyWidth::Fixed(16),
    shard_bytes: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Ordered,
}];

fn key_of(n: u64) -> Vec<u8> {
    let mut key = n.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes().to_vec();
    key.extend_from_slice(&n.to_be_bytes());
    key
}

#[test]
fn one_overwrite_keeps_an_ordered_walk_on_its_index() {
    let dir = TempDir::new().expect("temp dir");
    let config = ReelConfig {
        segment_bytes: ByteCount::from_bytes(64 * 1024),
        alloc_chunk: ByteCount::from_bytes(16 * 1024),
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        scrub_mbps: 0,
        index: IndexResidency::Paged,
        ..ReelConfig::default()
    };
    let store = ReelStore::open(dir.path().to_path_buf(), config, ORDERED).expect("open");
    for n in 0..2000u64 {
        Store::put(&store, "rows", &key_of(n), &[1u8; 64]).expect("put");
    }
    drop(store.cue().expect("seal"));
    assert!(store.page_out_sealed().expect("hand over") > 0);
    let walk = |store: &ReelStore| Store::iter(store, "rows").expect("iter").count();

    assert_eq!(walk(&store), 2000);
    let (_, before) = store.index().ordered_walks();
    assert_eq!(before, 0, "a walk with no writes went to the footers");

    Store::put(&store, "rows", &key_of(7), &[2u8; 64]).expect("overwrite");
    assert_eq!(walk(&store), 2000);
    let (pages, after) = store.index().ordered_walks();
    assert_eq!(after, 0, "one overwrite sent {after} of {pages} ordered pages to the footers");
}
