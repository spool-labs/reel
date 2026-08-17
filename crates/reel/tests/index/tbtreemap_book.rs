//! The tree as the crate's small books use it, against the map it replaced
//!
//! A book keyed by an offset or a segment number takes its keys in ascending order
//! and gives them back in any order, and ascending inserts are the one path that
//! splits a leaf by its tail rather than down the middle. What every one of these
//! books then asks for is the lowest key still held.

use std::collections::BTreeMap;

use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

use reel::index::tbtreemap::TBTreeMap;

type Book = TBTreeMap<u64, 16, u64>;

fn ends(book: &Book, oracle: &BTreeMap<u64, u64>, seed: u64, at: usize) {
    assert_eq!(
        book.len(),
        oracle.len(),
        "seed {seed} step {at}: length diverged"
    );
    assert_eq!(
        book.first_key_value().map(|(key, val)| (*key, *val)),
        oracle.first_key_value().map(|(key, val)| (*key, *val)),
        "seed {seed} step {at}: the lowest key diverged"
    );
    assert_eq!(
        book.last_key_value().map(|(key, val)| (*key, *val)),
        oracle.last_key_value().map(|(key, val)| (*key, *val)),
        "seed {seed} step {at}: the highest key diverged"
    );
    let walked: Vec<(u64, u64)> = book.iter().map(|(key, val)| (*key, *val)).collect();
    let wanted: Vec<(u64, u64)> = oracle.iter().map(|(key, val)| (*key, *val)).collect();
    assert_eq!(walked, wanted, "seed {seed} step {at}: the walk diverged");
}

// ascending reservations, landing in any order, with the frontier read at every step
#[test]
fn a_book_of_ascending_keys_agrees() {
    for seed in [1u64, 9, 4173] {
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut book = Book::default();
        let mut oracle: BTreeMap<u64, u64> = BTreeMap::new();
        let mut held: Vec<u64> = Vec::new();
        let mut next = 0u64;

        for step in 0..4_000 {
            // A reservation goes in above everything drawn so far, which is what
            // an appending writer does and what splits a leaf by its tail.
            let taking = held.is_empty() || rng.gen_bool(0.6);
            match taking {
                true => {
                    let span = rng.gen_range(1..4096u64);
                    assert_eq!(
                        book.insert(next, next + span),
                        oracle.insert(next, next + span)
                    );
                    held.push(next);
                    next += span;
                }
                false => {
                    let at = rng.gen_range(0..held.len());
                    let base = held.swap_remove(at);
                    assert_eq!(
                        book.remove(&base),
                        oracle.remove(&base),
                        "a landing diverged"
                    );
                }
            }
            ends(&book, &oracle, seed, step);
        }

        // Everything lands, which walks the book down to nothing one key at a time.
        held.shuffle(&mut rng);
        for (at, base) in held.iter().enumerate() {
            assert_eq!(book.remove(base), oracle.remove(base));
            ends(&book, &oracle, seed, 10_000 + at);
        }
        assert!(book.is_empty(), "the book still holds {}", book.len());
    }
}

// the same book keyed the other way, since a segment number only ever climbs
#[test]
fn a_book_emptied_and_refilled_agrees() {
    let mut book = Book::default();
    let mut oracle: BTreeMap<u64, u64> = BTreeMap::new();
    for round in 0..4u64 {
        for at in 0..1_000u64 {
            let key = round * 10_000 + at;
            book.insert(key, at);
            oracle.insert(key, at);
        }
        ends(&book, &oracle, 0, round as usize);
        for at in 0..1_000u64 {
            let key = round * 10_000 + at;
            assert_eq!(book.remove(&key), oracle.remove(&key));
        }
        ends(&book, &oracle, 0, round as usize);
    }
}

// a book's room tracks what it holds, not what it has ever been handed
//
// Removal leaves the emptied leaf in the chain, and these books drain their low end
// while their keys climb, so a bare removal grows the tree forever and the frontier
// read walks all of it. The packing removal is what the books take instead.
#[test]
fn a_drained_book_gives_its_room_back() {
    const DEPTH: u64 = 32;
    let mut book = Book::default();
    let mut oracle: BTreeMap<u64, u64> = BTreeMap::new();
    let mut base = 0u64;
    for _ in 0..DEPTH {
        book.insert(base, base + 4096);
        oracle.insert(base, base + 4096);
        base += 4096;
    }

    let mut worst = 0usize;
    for at in 0..20_000u64 {
        book.insert(base, base + 4096);
        oracle.insert(base, base + 4096);
        base += 4096;
        assert_eq!(
            book.remove_packed(&(at * 4096)),
            oracle.remove(&(at * 4096))
        );
        worst = worst.max(book.leaf_count());
        assert_eq!(
            book.first_key_value().map(|(key, _)| *key),
            oracle.first_key_value().map(|(key, _)| *key),
            "land {at}: the frontier diverged"
        );
    }
    ends(&book, &oracle, 0, 20_000);

    // Four leaves would hold the 33 keys in flight, and the doubling guard packs
    // at twice that. Ten is room for the guard and no room for a leak.
    assert!(
        worst <= 10,
        "the book grew to {worst} leaves holding {}",
        book.len()
    );

    // Everything lands, and the book comes to rest on the one leaf the guard will not
    // pack below, not on the room it held at its busiest.
    let held: Vec<u64> = book.iter().map(|(key, _)| *key).collect();
    for key in &held {
        book.remove_packed(key);
        oracle.remove(key);
    }
    assert!(book.is_empty());
    assert!(
        book.leaf_count() <= 1,
        "an emptied book kept {} leaves",
        book.leaf_count()
    );
}
