use reel::segment::block_cache::*;
use reel::segment::segment_tail::SegmentNumber;
use reel_api::units::ByteCount;

const SLOT: usize = 64;
const SHARE: ByteCount = ByteCount(64 * 1024);

fn key(offset: u32) -> BlockKey {
    BlockKey {
        segment: SegmentNumber(7),
        offset,
    }
}

// a block reads back whole, and a block past the slot size isn't kept
#[test]
fn cache_round() {
    let cache = BlockCache::new(SHARE, SLOT);

    cache.insert(key(1), b"block");
    cache.insert(key(2), &[0; SLOT + 1]);

    assert_eq!(cache.with(key(1), <[u8]>::to_vec), Some(b"block".to_vec()));
    assert_eq!(cache.with(key(2), <[u8]>::len), None);
}

// many blocks go through without growing the cache
#[test]
fn cache_budget() {
    let cache = BlockCache::new(SHARE, SLOT);
    let before = cache.heap_bytes();

    for offset in 0..100_000 {
        cache.insert(key(offset), b"block");
    }

    assert_eq!(cache.heap_bytes(), before);
}
