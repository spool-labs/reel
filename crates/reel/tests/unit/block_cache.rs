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

const LARGE_SHARE: ByteCount = ByteCount(512 * 1024 * 1024);
const LARGE_SLOT: usize = 16 * 1024;

// a large cache splits into more shards, so a visit over several blocks holds a small share of it
#[test]
fn large_cache_splits_finer() {
    let cache = BlockCache::new(LARGE_SHARE, LARGE_SLOT);
    assert_eq!(cache.shards.len(), MOST_CACHE_SHARDS);
    assert_eq!(BlockCache::new(SHARE, SLOT).shards.len(), CACHE_SHARDS);
}

// an insert into a shard a visit holds keeps nothing and never waits, and the cache counts it. Reads stay right
#[test]
fn insert_skips_a_held_shard() {
    let cache = BlockCache::new(SHARE, SLOT);
    cache.insert(key(1), b"first");
    let held = cache.with_many(&[key(1)], |blocks| {
        assert_eq!(blocks[0], Some(&b"first"[..]));
        // the visit holds key 1's shard, so a block there finds it held
        let same = (2..10_000).find(|offset| cache.place_of(key(*offset)).0 == cache.place_of(key(1)).0).expect("a key in the shard");
        cache.insert(key(same), b"second");
        same
    });
    assert_eq!(cache.skipped_inserts(), 1);
    assert_eq!(cache.with(key(held), <[u8]>::len), None);
    cache.insert(key(held), b"second");
    assert_eq!(cache.with(key(held), <[u8]>::to_vec), Some(b"second".to_vec()));
    assert_eq!(cache.skipped_inserts(), 1);
}
