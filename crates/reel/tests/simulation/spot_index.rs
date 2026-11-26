//! The spot cache keeps its budget, keeps hot keys, and never gets a key wrong for long

use reel::index::key_hash::{KeyHash, hash_key};
use reel::index::spot_index::{SpotIndex, SpotLocation};
use reel::segment::segment_tail::SegmentNumber;
use reel_core::column_id::ColumnId;
use reel_core::units::{ByteCount, FileOffset, WritePlace};

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

// a shard given back answers no lookup and keeps no insert, and it comes back empty and takes keys again
#[test]
fn a_shard_given_back_reads_as_missing_and_comes_back_empty() {
    let index = SpotIndex::new(ByteCount(16 * 1024 * 1024));
    for number in 0..10_000 {
        index.insert(hash_of(number), &location(number));
    }
    let held: Vec<u64> = (0..10_000).filter(|number| index.lookup(hash_of(*number)).is_some()).collect();
    assert!(held.len() > 9_000);
    let mut given = 0;
    for shard in 0..index.shard_count() {
        given += index.give_back_shard(shard);
    }
    assert!(given > 0, "no page went back");
    assert_eq!(index.absent_shards(), index.shard_count());
    for &number in &held {
        assert_eq!(index.lookup(hash_of(number)), None, "an absent shard answered");
        index.insert(hash_of(number), &location(number + 1));
        index.insert_newer(hash_of(number), &location(number + 2));
        index.remove_older(hash_of(number), WritePlace(number + 3));
    }
    for shard in 0..index.shard_count() {
        assert!(index.bring_back_shard(shard));
    }
    assert_eq!(index.absent_shards(), 0);
    assert!(held.iter().all(|number| index.lookup(hash_of(*number)).is_none()), "a shard came back with old keys");
    index.insert(hash_of(held[0]), &location(held[0]));
    assert_eq!(index.lookup(hash_of(held[0])), Some(location(held[0])));
}

// a lookup and an insert on a shard that is coming back return at once, with no wait on the fill's lock
#[test]
fn a_shard_coming_back_holds_up_no_reader() {
    let index = std::sync::Arc::new(SpotIndex::new(ByteCount(16 * 1024 * 1024)));
    let number = 5;
    let shard = reel::index::write_index::shard_of(hash_of(number));
    assert!(index.give_back_shard(shard) > 0);
    // the test holds the shard's lock as a fill does
    let held = index.shards.write(shard);
    let reader = std::sync::Arc::clone(&index);
    let answer = std::thread::spawn(move || {
        reader.insert(hash_of(number), &location(number));
        reader.lookup(hash_of(number))
    });
    let started = std::time::Instant::now();
    while !answer.is_finished() {
        assert!(started.elapsed() < std::time::Duration::from_secs(5), "a reader waited on a fill");
        std::thread::yield_now();
    }
    drop(held);
    assert_eq!(answer.join().expect("reader"), None);
}
