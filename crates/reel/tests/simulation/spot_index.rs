//! The spot cache keeps its budget, keeps hot keys, and never gets a key wrong for long

use reel::index::key_hash::{KeyHash, hash_key};
use reel::index::spot_index::{SpotIndex, SpotLocation};
use reel::segment::segment_tail::SegmentNumber;
use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, FileOffset, WritePlace};

const COLUMN: ColumnId = ColumnId(1);
const SHARE: ByteCount = ByteCount(1024 * 1024);
const KEYS: u64 = 200_000;
const HOT_KEYS: u64 = 1_000;

fn hash_of(number: u64) -> KeyHash {
    hash_key(COLUMN, &number.to_le_bytes())
}

fn location(number: u64) -> SpotLocation {
    SpotLocation {
        segment: SegmentNumber(number % 1_000),
        offset: FileOffset(number * 8),
        length: ByteCount(number % 4_096),
        place: WritePlace(number),
    }
}

// a key reads back where it was put, a new location replaces it, and a removed key is gone
#[test]
fn spot_round() {
    let index = SpotIndex::new(SHARE);
    let hash = hash_of(1);

    assert_eq!(index.lookup(hash), None);
    index.insert(hash, &location(1));
    assert_eq!(index.lookup(hash), Some(location(1)));
    index.insert(hash, &location(2));
    assert_eq!(index.lookup(hash), Some(location(2)));
    index.remove(hash);
    assert_eq!(index.lookup(hash), None);
}

// the cache holds its share whatever number of keys goes through it
#[test]
fn spot_budget() {
    let index = SpotIndex::new(SHARE);
    let before = index.heap_bytes();

    for number in 0..KEYS {
        index.insert(hash_of(number), &location(number));
    }

    assert_eq!(index.heap_bytes(), before);
    assert!(before <= SHARE);
}

// a key that stays in use outlives a stream of keys read once
#[test]
fn hot_keys() {
    let index = SpotIndex::new(SHARE);
    for number in 0..HOT_KEYS {
        index.insert(hash_of(number), &location(number));
    }

    let mut hot_hits = 0;
    for number in HOT_KEYS..KEYS {
        index.insert(hash_of(number), &location(number));
        let hot = number % HOT_KEYS;
        if index.lookup(hash_of(hot)).is_some() {
            hot_hits += 1;
        }
    }

    let streamed = KEYS - HOT_KEYS;
    assert!(hot_hits * 10 > streamed * 9);
}
