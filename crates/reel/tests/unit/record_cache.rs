use std::collections::{BTreeMap, BTreeSet};

use reel::index::record_cache::{CACHE_SHARDS, CacheLookup, MOST_CACHED_BYTES, RecordCache, place_hash};
use reel_api::units::{ByteCount, FileOffset};
use reel::segment::segment_tail::SegmentNumber;

const SEGMENT: SegmentNumber = SegmentNumber(7);
const KEY: &[u8] = b"the-key";
const CACHE_BYTES: ByteCount = ByteCount((CACHE_SHARDS * 64 * 1024) as u64);
const VALUES: u64 = 20_000;
const HOT: u64 = 50;

fn value(offset: u64, length: usize) -> Vec<u8> {
    (0..length).map(|at| (offset as usize * 31 + at) as u8).collect()
}

// a get as the read path makes it: a miss reads the record and puts it in when the cache admitted it
fn read(cache: &RecordCache, offset: u64, bytes: &[u8], out: &mut Vec<u8>) -> bool {
    match cache.get(SEGMENT, FileOffset(offset), KEY, out) {
        CacheLookup::Hit => true,
        CacheLookup::Miss { is_admitted } => {
            if is_admitted {
                cache.admit(SEGMENT, FileOffset(offset), KEY, bytes);
            }
            false
        },
    }
}

fn read_twice(cache: &RecordCache, offset: u64, bytes: &[u8]) {
    let mut out = Vec::new();
    read(cache, offset, bytes, &mut out);
    read(cache, offset, bytes, &mut out);
}

// a value goes in on its second miss, and a get copies it back whole or in a window
#[test]
fn second_miss_admits() {
    let cache = RecordCache::new(CACHE_BYTES);
    let bytes = value(1, 300);
    let mut out = Vec::new();
    assert!(!read(&cache, 1, &bytes, &mut out));
    assert!(!read(&cache, 1, &bytes, &mut out), "a first miss went in");
    assert!(read(&cache, 1, &bytes, &mut out));
    assert_eq!(out, bytes);
    assert_eq!(cache.get_window(SEGMENT, FileOffset(1), KEY, ByteCount(10), ByteCount(20), &mut out), CacheLookup::Hit);
    assert_eq!(out, bytes[10..30]);
    assert_ne!(cache.get(SegmentNumber(8), FileOffset(1), KEY, &mut out), CacheLookup::Hit, "another segment's record read as cached");
}

// values past the cap stay out
#[test]
fn big_values_stay_out() {
    let cache = RecordCache::new(CACHE_BYTES);
    let bytes = value(2, MOST_CACHED_BYTES + 1);
    read_twice(&cache, 2, &bytes);
    assert_ne!(cache.get(SEGMENT, FileOffset(2), KEY, &mut Vec::new()), CacheLookup::Hit);
}

// every value the cache answers with is the one read for that place, through any number of wraps of every shard's log
#[test]
fn wraps_keep_every_answer_right() {
    let cache = RecordCache::new(CACHE_BYTES);
    let mut offered = BTreeMap::new();
    let mut out = Vec::new();
    let mut state = 0x2545_F491_4F6C_DD1D_u64;
    for round in 0..VALUES {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let offset = state % (VALUES / 4);
        let length = 1 + (state >> 20) as usize % 3000;
        let bytes = offered.entry(offset).or_insert_with(|| value(offset, length)).clone();
        read_twice(&cache, offset, &bytes);
        let probe = (state >> 40) % (VALUES / 4);
        if cache.get(SEGMENT, FileOffset(probe), KEY, &mut out) == CacheLookup::Hit {
            assert_eq!(Some(&out), offered.get(&probe), "round {round}: place {probe} answered with other bytes");
        }
    }
    assert!(cache.value_count() > 0);
}

// a hot set read between a scan's reads stays, since the scan's values each miss once and never go in
#[test]
fn scan_leaves_the_hot_set() {
    let cache = RecordCache::new(CACHE_BYTES);
    let mut out = Vec::new();
    for offset in 0..HOT {
        read_twice(&cache, offset, &value(offset, 200));
    }
    for offset in HOT..HOT + VALUES {
        read(&cache, offset, &value(offset, 200), &mut out);
        if offset % 64 == 0 {
            for hot in 0..HOT {
                assert_eq!(cache.get(SEGMENT, FileOffset(hot), KEY, &mut out), CacheLookup::Hit, "hot place {hot} left under the scan at {offset}");
            }
        }
    }
}

// places read once each go in only when two share an admission byte and its fingerprint
#[test]
fn single_misses_stay_out() {
    let cache = RecordCache::new(CACHE_BYTES);
    let mut out = Vec::new();
    for offset in 0..VALUES {
        read(&cache, offset, &value(offset, 100), &mut out);
    }
    assert!(cache.value_count() < VALUES as usize / 100, "{} of {VALUES} single misses went in", cache.value_count());
}

// a place answers only for the key whose record sits there, so a lookup another key's hash led to it misses
#[test]
fn another_key_misses() {
    let cache = RecordCache::new(CACHE_BYTES);
    let bytes = value(3, 100);
    read_twice(&cache, 3, &bytes);
    let mut out = Vec::new();
    assert_eq!(cache.get(SEGMENT, FileOffset(3), KEY, &mut out), CacheLookup::Hit);
    assert_ne!(cache.get(SEGMENT, FileOffset(3), b"other-key", &mut out), CacheLookup::Hit, "another key read this key's value");
}

// records of one size still spread over every slot, since a slot takes the hash's low bits
#[test]
fn even_sizes_spread() {
    for size in [4096, 4120] {
        let slots: BTreeSet<u64> = (0..4096).map(|record| place_hash(SEGMENT.0, record * size) & 1023).collect();
        assert!(slots.len() > 1000, "records of {size} bytes fell on {} of 1024 slots", slots.len());
    }
}
