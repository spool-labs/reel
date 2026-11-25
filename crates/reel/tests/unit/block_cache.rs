use reel::segment::block_cache::*;
use reel::segment::segment_tail::SegmentNumber;
use reel_core::units::ByteCount;

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

// a shard given back holds no block and keeps none, a many visit skips it, and it comes back empty and takes blocks again
#[test]
fn a_shard_given_back_reads_as_missing_and_comes_back_empty() {
    let cache = BlockCache::new(ByteCount(4 * 1024 * 1024), SLOT);
    for offset in 0..2_000 {
        cache.insert(key(offset), b"block");
    }
    let held: Vec<u32> = (0..2_000).filter(|offset| cache.with(key(*offset), <[u8]>::len).is_some()).collect();
    assert!(!held.is_empty());
    let mut given = 0;
    for shard in 0..cache.shard_count() {
        given += cache.give_back_shard(shard);
    }
    assert!(given > 0, "no page went back");
    assert_eq!(cache.absent_shards(), cache.shard_count());
    assert_eq!(cache.give_back_shard(0), 0, "a shard went back twice");
    for &offset in &held {
        assert_eq!(cache.with(key(offset), <[u8]>::len), None, "an absent shard answered");
        cache.insert(key(offset), b"again");
    }
    let keys: Vec<BlockKey> = held.iter().take(MOST_MANY_BLOCKS).map(|offset| key(*offset)).collect();
    cache.with_many(&keys, |blocks| assert!(blocks.iter().all(Option::is_none)));
    for shard in 0..cache.shard_count() {
        assert!(cache.bring_back_shard(shard));
    }
    assert_eq!(cache.absent_shards(), 0);
    assert!(held.iter().all(|offset| cache.with(key(*offset), <[u8]>::len).is_none()), "a shard came back with old blocks");
    cache.insert(key(held[0]), b"fresh");
    assert_eq!(cache.with(key(held[0]), <[u8]>::to_vec), Some(b"fresh".to_vec()));
}
