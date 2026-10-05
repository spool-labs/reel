//! An ordered column keeps every key across a reopen, even keys that share their leading bytes

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

/// Rows of a few owners, each owner's rows sharing their first eight bytes as an address's signatures do
fn key_of(owner: u64, row: u64) -> Vec<u8> {
    let mut key = owner.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes().to_vec();
    key.extend_from_slice(&row.to_be_bytes());
    key
}

#[test]
fn keys_sharing_a_lead_survive_a_reopen() {
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
    let store = ReelStore::open(dir.path().to_path_buf(), config.clone(), ORDERED).expect("open");
    for owner in 0..4u64 {
        for row in 0..500u64 {
            Store::put(&store, "rows", &key_of(owner, row), &[owner as u8; 8]).expect("put");
        }
    }
    drop(store.cue().expect("seal"));
    store.page_out_sealed().expect("hand over");
    assert_eq!(Store::iter(&store, "rows").expect("iter").count(), 2000, "before the reopen");
    store.close().expect("close");
    drop(store);

    let store = ReelStore::open(dir.path().to_path_buf(), config, ORDERED).expect("reopen");
    let missing = (0..4u64)
        .flat_map(|owner| (0..500u64).map(move |row| key_of(owner, row)))
        .filter(|key| Store::get(&store, "rows", key).expect("get").is_none())
        .count();
    assert_eq!(missing, 0, "{missing} of 2000 keys lost across the reopen");
    assert_eq!(Store::iter(&store, "rows").expect("iter").count(), 2000, "after the reopen");
}
