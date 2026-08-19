//! The held-entry slab against a ring written the obvious way
//!
//! The oracle is a plain vector with a hand walked over it, which is what the caches
//! this replaces documented and could not run: a hash map has no stable hand, so the
//! fd cache's sweep took whatever iteration order handed it and the block cache did
//! not sweep at all. Held one shard against one ring, the two have to agree entry for
//! entry after every op. Across shards the questions are the ones a shard split does
//! not change: what a hit answers, what a retire takes, and that the bound holds.

use std::collections::{HashMap, HashSet, VecDeque};

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use reel::format::column::ColumnId;
use reel::format::loc::SegmentId;
use reel::hold::{hold_key, Hold};

/// Entries the single-shard streams work over
const KEYS: u64 = 64;

/// Ops the shortest stream applies, the rest running one longer apiece
const OPS: usize = 200;

/// The map and the queue the cache was, which is what a hold with nothing read
/// behaves exactly like
struct Oracle {
    budget: usize,
    held: HashMap<u64, (u64, usize)>,
    taken: VecDeque<u64>,
    bytes: usize,
}

impl Oracle {
    fn new(budget: usize) -> Oracle {
        Oracle {
            budget,
            held: HashMap::new(),
            taken: VecDeque::new(),
            bytes: 0,
        }
    }

    fn get(&self, key: u64) -> Option<u64> {
        self.held.get(&key).map(|(value, _)| *value)
    }

    fn insert(&mut self, key: u64, value: u64, weight: usize) {
        if weight > self.budget || self.held.contains_key(&key) {
            return;
        }
        while self.bytes + weight > self.budget {
            let Some(oldest) = self.taken.pop_front() else {
                return;
            };
            if let Some((_, given)) = self.held.remove(&oldest) {
                self.bytes -= given;
            }
        }
        self.held.insert(key, (value, weight));
        self.taken.push_back(key);
        self.bytes += weight;
    }
}

fn key_of(at: u64) -> u64 {
    hold_key(SegmentId(1), ColumnId(0), at as usize)
}

/// Everything the hold answers about the keys in play, against the queue
fn agree(hold: &Hold<u64>, oracle: &Oracle, step: usize) {
    for at in 0..KEYS {
        let key = key_of(at);
        assert_eq!(
            hold.get(key),
            oracle.get(key),
            "key {at} parted at step {step}"
        );
    }
    assert_eq!(hold.len(), oracle.held.len(), "count parted at step {step}");
    assert_eq!(hold.bytes(), oracle.bytes, "bytes parted at step {step}");
    assert!(hold.bytes() <= oracle.budget, "the bound broke at {step}");
}

// with nothing read and one entry given up per insert, the hand is the queue
//
// Clock falls back to the order of arrival exactly when every insert takes one
// entry and puts one back, which is the regime the deque this replaces was always
// in. Uneven weights break the equality rather than the policy: an insert that has
// to take three back leaves slots the hand meets in its own order, and which of
// three cold entries goes is what owning the eviction was for.
#[test]
fn an_unread_hold_of_even_weights_evicts_like_the_deque() {
    for seed in 0..64u64 {
        // Small enough that the hold stays one shard, so the hand is the one thing
        // being compared rather than which shard a key landed in.
        let budget = 40;
        let hold: Hold<u64> = Hold::new(budget, budget);
        let mut oracle = Oracle::new(budget);
        let mut rng = SmallRng::seed_from_u64(seed);

        // Asking what the hold holds is itself a read, so the two are held up once
        // at the end and the stream length is what varies instead.
        let ops = OPS + seed as usize;
        for _ in 0..ops {
            let at = rng.gen_range(0..KEYS);
            hold.insert(key_of(at), at, 4);
            oracle.insert(key_of(at), at, 4);
        }
        agree(&hold, &oracle, ops);
    }
}

// under uneven weights the two give up the same bytes, whichever entries they were
#[test]
fn uneven_weights_still_hold_the_same_bound() {
    for seed in 0..64u64 {
        let budget = 40;
        let hold: Hold<u64> = Hold::new(budget, budget);
        let mut oracle = Oracle::new(budget);
        let mut rng = SmallRng::seed_from_u64(seed);

        let ops = OPS + seed as usize;
        for _ in 0..ops {
            let at = rng.gen_range(0..KEYS);
            let weight = rng.gen_range(1..6);
            hold.insert(key_of(at), at, weight);
            oracle.insert(key_of(at), at, weight);
            assert!(hold.bytes() <= budget, "the bound broke on seed {seed}");
        }
        // Both are full to within one entry of the bound, which is what the eviction
        // loop promises; which entries fill it is the hand's business.
        assert!(
            hold.bytes() > budget - 6,
            "the hold gave up more than it had to"
        );
        assert_eq!(hold.len(), hold.len().min(oracle.held.len() + 2));
    }
}

// every hit answers with what was put under that key, reads and all
#[test]
fn a_hit_always_answers_with_what_was_held() {
    let budget = 40;
    let hold: Hold<u64> = Hold::new(budget, budget);
    // Every value ever put under a key, since a key evicted and put back holds the
    // newer one and the question here is whether a hit ever answers for another key.
    let mut written: HashMap<u64, HashSet<u64>> = HashMap::new();
    let mut rng = SmallRng::seed_from_u64(21);

    for step in 0..20_000u64 {
        let at = rng.gen_range(0..KEYS);
        let key = key_of(at);
        match rng.gen_range(0..10u32) {
            0..=4 => {
                hold.insert(key, step, rng.gen_range(1..6));
                written.entry(key).or_default().insert(step);
            }
            5..=8 => {
                if let Some(found) = hold.get(key) {
                    assert!(
                        written.get(&key).is_some_and(|put| put.contains(&found)),
                        "key {at} answered with what was never put in it"
                    );
                }
            }
            _ => {
                hold.take(key);
            }
        }
        assert!(hold.bytes() <= budget, "the bound broke at step {step}");
    }
}

// an entry read since the hand passed it outlives the sweep that makes room
//
// The policy the fd cache documented and a hash map's iteration order could not
// run, and the one `insert_block` did not attempt at all.
#[test]
fn a_read_entry_outlives_a_sweep() {
    let hold: Hold<u64> = Hold::new(4, 4);
    for at in 0..4 {
        hold.insert(key_of(at), at, 1);
    }
    // The oldest is read, so strict insertion order would take it and clock does not.
    assert_eq!(hold.get(key_of(0)), Some(0));
    hold.insert(key_of(4), 4, 1);

    assert_eq!(hold.len(), 4);
    assert_eq!(hold.get(key_of(0)), Some(0), "the read entry stayed");
    assert_eq!(hold.get(key_of(4)), Some(4), "the new entry landed");
    assert_eq!(hold.get(key_of(1)), None, "and the cold one went");
}

// a retire takes one segment's entries and nothing else, across every shard
#[test]
fn a_retire_takes_one_segment() {
    let hold: Hold<u64> = Hold::new(1 << 20, 8);
    let mut live: HashSet<(u32, usize)> = HashSet::new();
    for segment in 1..40u32 {
        for block in 0..40usize {
            hold.insert(hold_key(SegmentId(segment), ColumnId(3), block), 1, 8);
            live.insert((segment, block));
        }
    }
    assert_eq!(hold.len(), live.len());

    for segment in [7u32, 8, 39] {
        hold.forget(SegmentId(segment));
        live.retain(|(held, _)| *held != segment);
        assert_eq!(hold.len(), live.len(), "retiring {segment} took too much");
        assert_eq!(hold.bytes(), live.len() * 8);
    }
    for segment in 1..40u32 {
        for block in 0..40usize {
            let held = hold.get(hold_key(SegmentId(segment), ColumnId(3), block));
            assert_eq!(held.is_some(), live.contains(&(segment, block)));
        }
    }
}

// entries of different columns and blocks in one segment never answer for each other
#[test]
fn keys_never_answer_for_each_other() {
    let hold: Hold<u64> = Hold::new(1 << 20, 8);
    let mut written = HashMap::new();
    let mut value = 0u64;
    for segment in 0..8u32 {
        for column in 0..8u8 {
            for block in 0..8usize {
                let key = hold_key(SegmentId(segment), ColumnId(column), block);
                hold.insert(key, value, 8);
                written.insert(key, value);
                value += 1;
            }
        }
    }
    assert_eq!(hold.len(), written.len());
    for (key, value) in written {
        assert_eq!(hold.get(key), Some(value));
    }
}

// the bound holds while the keys spread across every shard
#[test]
fn the_bound_holds_across_shards() {
    let budget = 1 << 16;
    let hold: Hold<u64> = Hold::new(budget, 64);
    let mut rng = SmallRng::seed_from_u64(9);
    for step in 0..20_000u64 {
        let segment = rng.gen_range(0..64u32);
        let block = rng.gen_range(0..64usize);
        let key = hold_key(SegmentId(segment), ColumnId(1), block);
        match rng.gen_range(0..8u32) {
            0 => {
                hold.forget(SegmentId(segment));
            }
            1..=2 => {
                hold.get(key);
            }
            _ => hold.insert(key, step, rng.gen_range(16..512)),
        }
        assert!(hold.bytes() <= budget, "the bound broke at step {step}");
    }
    hold.clear();
    assert_eq!(hold.bytes(), 0);
    assert!(hold.is_empty());
}

// an entry heavier than the whole hold is turned away rather than emptying it
#[test]
fn an_oversized_entry_keeps_the_rest() {
    let hold: Hold<u64> = Hold::new(64, 64);
    hold.insert(key_of(0), 0, 32);
    hold.insert(key_of(1), 1, 65);

    assert_eq!(hold.get(key_of(0)), Some(0), "what fit is still held");
    assert_eq!(hold.get(key_of(1)), None);
    assert_eq!(hold.bytes(), 32);
}
