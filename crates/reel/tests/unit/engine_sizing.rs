use reel::engine::engine_sizing::*;
use reel::index::open_slots::MAX_OPEN_SLOTS;
use reel_api::units::ByteCount;

// the default budget gets near-gigabyte segments, and a small one scales down
#[test]
fn budget_split() {
    let full = EngineSizing::for_budget(FULL_BUDGET, 3);
    let small = EngineSizing::for_budget(ByteCount(8 * 1024 * 1024), 1);

    assert_eq!(full.segment_bytes, SEGMENT_TARGET);
    assert_eq!(full.lz4_stored_limit, LZ4_STORED_LIMIT);
    assert!(small.segment_bytes < full.segment_bytes);
    assert!(small.shard.open_slots <= MAX_OPEN_SLOTS);
    assert!(small.segment_rows.0 > 0);
}
