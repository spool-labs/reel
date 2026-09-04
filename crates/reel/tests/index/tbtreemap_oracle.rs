//! The tbtreemap against the map it wants to replace, op for op
//!
//! A seeded stream of inserts, overwrites, removes and reads runs against the tree and a
//! BTreeMap in lockstep, and every answer has to agree: the length after every mutation,
//! every point read, every batched read through all three doors, every bounded range
//! forwards and backwards, the ends, and the full ordered walk at every audit.

use std::collections::BTreeMap;
use std::ops::Bound;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use reel::index::tbtreemap::{TBTreeMap, TreeKey};

type Key = [u8; 34];
type Tree = TBTreeMap<[u8; 34], 32, u64>;

/// A name key, which is what the object columns hold
///
/// The same stream runs against both shapes, since what differs is the half a fixed key
/// never exercises: keys on the heap, a node that retunes its lead past the bucket its
/// keys share, and a placement answering for a probe carrying no bucket the node knows.
type Name = Box<[u8]>;

const SEEDS: &[u64] = &[3, 41, 4173, 0x5eed];
const OPS: usize = 20_000;
const AUDIT_EVERY: usize = 512;

/// A key from one of the two shapes the store actually holds
///
/// Slot led, a counter in the lead bytes the way records are keyed, or uniform the way
/// signatures are. Both run in every stream, since the lead scan's tie handling only earns
/// its keep where leads collide.
fn draw_key(rng: &mut SmallRng, pool: &[Key]) -> Key {
    // Half the draws revisit a key already seen, so overwrites and removes happen.
    if !pool.is_empty() && rng.gen_bool(0.5) {
        return pool[rng.gen_range(0..pool.len())];
    }
    let mut key = [0u8; 34];
    if rng.gen_bool(0.5) {
        let slot: u64 = rng.gen_range(0..4096);
        key[..8].copy_from_slice(&slot.to_be_bytes());
        rng.fill(&mut key[8..]);
    } else {
        rng.fill(&mut key[..]);
    }
    key
}

/// A range bound over the fixed key space, drawn without regard to what is held
fn fixed_bound(rng: &mut SmallRng) -> Key {
    let mut key = [0u8; 34];
    rng.fill(&mut key[..]);
    key
}

/// A name key from the shapes a bucket holds
///
/// Two buckets rather than one, because a leaf that straddles a boundary is the node
/// whose keys agree on nothing and whose window has to fall back. Three name shapes, so
/// the bytes the window tunes past run from none to sixty.
fn draw_name(rng: &mut SmallRng, pool: &[Name]) -> Name {
    if !pool.is_empty() && rng.gen_bool(0.5) {
        return pool[rng.gen_range(0..pool.len())].clone();
    }
    let bucket = match rng.gen_bool(0.5) {
        true => [0x11u8; 32],
        false => [0x12u8; 32],
    };
    let at: u64 = rng.gen_range(0..4096);
    let name = match rng.gen_range(0..3u32) {
        0 => format!("{at:x}"),
        1 => format!(
            "logs/2026/{:02}/{:02}/{at:08x}.json",
            at % 12 + 1,
            at % 28 + 1
        ),
        _ => format!(
            "tenants/{:032x}/exports/2026/08/02/part-{:05}.parquet",
            at / 64,
            at % 64
        ),
    };
    let mut key = bucket.to_vec();
    key.extend_from_slice(name.as_bytes());
    key.into_boxed_slice()
}

/// A range bound over the name space, including ones no bucket holds
fn bound_name(rng: &mut SmallRng) -> Name {
    let mut key = vec![rng.gen_range(0x10u8..0x14); rng.gen_range(0..33)];
    let tail: u64 = rng.gen();
    key.extend_from_slice(format!("{tail:x}").as_bytes());
    key.truncate(rng.gen_range(0..key.len() + 1));
    key.into_boxed_slice()
}

fn audit<K: TreeKey, const B: usize>(
    tree: &TBTreeMap<K, B, u64>,
    oracle: &BTreeMap<K, u64>,
    seed: u64,
    at: usize,
) {
    assert_eq!(
        tree.len(),
        oracle.len(),
        "seed {seed} op {at}: length diverged"
    );

    let ends = (
        tree.first_key_value().map(|(key, val)| (key.clone(), *val)),
        tree.last_key_value().map(|(key, val)| (key.clone(), *val)),
    );
    let wanted_ends = (
        oracle
            .first_key_value()
            .map(|(key, val)| (key.clone(), *val)),
        oracle
            .last_key_value()
            .map(|(key, val)| (key.clone(), *val)),
    );
    assert!(
        ends == wanted_ends,
        "seed {seed} op {at}: the ends diverged"
    );

    let walked: Vec<(&K, u64)> = tree.iter().map(|(key, val)| (key, *val)).collect();
    let wanted: Vec<(&K, u64)> = oracle.iter().map(|(key, val)| (key, *val)).collect();
    assert!(
        walked == wanted,
        "seed {seed} op {at}: the ordered walk diverged"
    );

    let chunked: usize = tree.chunks().map(|(keys, _)| keys.len()).sum();
    assert_eq!(
        chunked,
        oracle.len(),
        "seed {seed} op {at}: chunks lost pairs"
    );
}

fn audit_ranges<K: TreeKey, const B: usize>(
    rng: &mut SmallRng,
    tree: &TBTreeMap<K, B, u64>,
    oracle: &BTreeMap<K, u64>,
    bound: &mut impl FnMut(&mut SmallRng) -> K,
    seed: u64,
    at: usize,
) {
    for _ in 0..8 {
        let one = bound(rng);
        let two = bound(rng);
        let (lo, hi) = match one <= two {
            true => (one, two),
            false => (two, one),
        };
        let bounds = [
            (Bound::Included(&lo), Bound::Excluded(&hi)),
            (Bound::Included(&lo), Bound::Included(&hi)),
            (Bound::Excluded(&lo), Bound::Unbounded),
            (Bound::Unbounded, Bound::Excluded(&hi)),
        ];
        for (low, high) in bounds {
            let walked: Vec<(&K, u64)> = tree
                .range(low, high)
                .map(|(key, val)| (key, *val))
                .collect();
            let wanted: Vec<(&K, u64)> = oracle
                .range::<K, _>((low, high))
                .map(|(key, val)| (key, *val))
                .collect();
            assert!(walked == wanted, "seed {seed} op {at}: a range diverged");

            // The same span the other way, which is what a page resuming backwards walks.
            let back: Vec<(&K, u64)> = tree
                .range_back(low, high)
                .map(|(key, val)| (key, *val))
                .collect();
            let wanted_back: Vec<(&K, u64)> = oracle
                .range::<K, _>((low, high))
                .rev()
                .map(|(key, val)| (key, *val))
                .collect();
            assert!(
                back == wanted_back,
                "seed {seed} op {at}: a backward range diverged"
            );
        }
    }
}

fn audit_batches<K: TreeKey, const B: usize>(
    rng: &mut SmallRng,
    tree: &TBTreeMap<K, B, u64>,
    oracle: &BTreeMap<K, u64>,
    draw: &mut impl FnMut(&mut SmallRng, &[K]) -> K,
    seed: u64,
    at: usize,
) {
    let pool: Vec<K> = oracle.keys().cloned().collect();
    let mut asked: Vec<K> = (0..37).map(|_| draw(rng, &pool)).collect();
    // A duplicate in the batch must answer twice, not confuse the cursors.
    if let Some(first) = asked.first().cloned() {
        asked.push(first);
    }

    let wanted: Vec<Option<u64>> = asked
        .iter()
        .map(|key| oracle.get::<K>(key).copied())
        .collect();
    let mut out = Vec::new();

    tree.get_many(&asked, &mut out);
    let got: Vec<Option<u64>> = out.iter().map(|found| found.copied()).collect();
    assert_eq!(got, wanted, "seed {seed} op {at}: get_many diverged");

    tree.get_many_cold(&asked, &mut out);
    let got: Vec<Option<u64>> = out.iter().map(|found| found.copied()).collect();
    assert_eq!(got, wanted, "seed {seed} op {at}: get_many_cold diverged");

    let mut sorted = asked.clone();
    sorted.sort();
    let wanted: Vec<Option<u64>> = sorted
        .iter()
        .map(|key| oracle.get::<K>(key).copied())
        .collect();
    tree.get_many_sorted(&sorted, &mut out);
    let got: Vec<Option<u64>> = out.iter().map(|found| found.copied()).collect();
    assert_eq!(got, wanted, "seed {seed} op {at}: get_many_sorted diverged");
}

// every op agrees with the oracle, across every seed and both key shapes
#[test]
fn the_tree_agrees_with_the_oracle() {
    stream_against_the_oracle::<Key, 32>(&mut draw_key, &mut fixed_bound);
}

// and so does the tree the object columns hold, at keys with no width at all
#[test]
fn the_name_tree_agrees_with_the_oracle() {
    stream_against_the_oracle::<Name, 32>(&mut draw_name, &mut bound_name);
}

/// The stream and its audits, run against whichever key shape is handed in
fn stream_against_the_oracle<K: TreeKey, const B: usize>(
    draw: &mut impl FnMut(&mut SmallRng, &[K]) -> K,
    bound: &mut impl FnMut(&mut SmallRng) -> K,
) {
    for &seed in SEEDS {
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut tree: TBTreeMap<K, B, u64> = TBTreeMap::new();
        let mut oracle: BTreeMap<K, u64> = BTreeMap::new();
        let mut pool: Vec<K> = Vec::new();

        for at in 0..OPS {
            let key = draw(&mut rng, &pool);
            match rng.gen_range(0..10u32) {
                // Inserts and overwrites dominate, the write path's shape.
                0..=4 => {
                    let val = rng.gen();
                    let mine = tree.insert(key.clone(), val);
                    let theirs = oracle.insert(key.clone(), val);
                    assert_eq!(
                        mine, theirs,
                        "seed {seed} op {at}: insert's return diverged"
                    );
                    pool.push(key);
                }
                5..=6 => {
                    let mine = tree.remove(key.borrow());
                    let theirs = oracle.remove::<K>(&key);
                    assert_eq!(mine, theirs, "seed {seed} op {at}: remove diverged");
                }
                // A value stepped in place has to land where a put would have left it.
                7 => {
                    let step = rng.gen::<u64>();
                    match (tree.get_mut(key.borrow()), oracle.get_mut::<K>(&key)) {
                        (Some(mine), Some(theirs)) => {
                            *mine = mine.wrapping_add(step);
                            *theirs = theirs.wrapping_add(step);
                        }
                        (None, None) => {
                            *tree.get_or_insert(key.clone(), 0) = step;
                            oracle.insert(key.clone(), step);
                            pool.push(key);
                        }
                        (mine, theirs) => panic!(
                            "seed {seed} op {at}: get_mut disagreed on presence, {:?} against {:?}",
                            mine.is_some(),
                            theirs.is_some()
                        ),
                    }
                }
                _ => {
                    let mine = tree.get(key.borrow()).copied();
                    let theirs = oracle.get::<K>(&key).copied();
                    assert_eq!(mine, theirs, "seed {seed} op {at}: get diverged");
                    assert_eq!(
                        tree.contains_key(key.borrow()),
                        oracle.contains_key::<K>(&key),
                        "seed {seed} op {at}: contains diverged"
                    );
                }
            }
            assert_eq!(
                tree.len(),
                oracle.len(),
                "seed {seed} op {at}: length diverged"
            );

            if at % AUDIT_EVERY == AUDIT_EVERY - 1 {
                audit(&tree, &oracle, seed, at);
                audit_ranges(&mut rng, &tree, &oracle, bound, seed, at);
                audit_batches(&mut rng, &tree, &oracle, draw, seed, at);
            }
        }

        audit(&tree, &oracle, seed, OPS);
        audit_ranges(&mut rng, &tree, &oracle, bound, seed, OPS);
        audit_batches(&mut rng, &tree, &oracle, draw, seed, OPS);

        // The survivors, bulk loaded, are the same map again: the rebuild a decayed tree
        // takes has to answer exactly as the tree it replaces.
        let survivors: Vec<(K, u64)> = oracle
            .iter()
            .map(|(key, val)| (key.clone(), *val))
            .collect();
        let rebuilt: TBTreeMap<K, B, u64> = TBTreeMap::from_sorted(survivors, B);
        audit(&rebuilt, &oracle, seed, OPS + 1);

        // And packed in place, which is the same rebuild without the caller handing the
        // survivors over, so the room a stream of removes left has to be gone.
        tree.repack(B);
        audit(&tree, &oracle, seed, OPS + 2);
        audit_ranges(&mut rng, &tree, &oracle, bound, seed, OPS + 2);
        audit_batches(&mut rng, &tree, &oracle, draw, seed, OPS + 2);
        assert_eq!(
            tree.leaf_count(),
            oracle.len().div_ceil(B).max(1),
            "seed {seed}: a repack left leaves behind"
        );
    }
}

// a tree emptied by removes answers as an empty one and takes keys again
#[test]
fn emptied_and_filled_again() {
    let mut rng = SmallRng::seed_from_u64(11);
    let mut tree = Tree::new();
    let mut oracle: BTreeMap<Key, u64> = BTreeMap::new();
    let mut keys: Vec<Key> = Vec::new();
    for _ in 0..2_000 {
        let key = draw_key(&mut rng, &[]);
        tree.insert(key, 1);
        oracle.insert(key, 1);
        keys.push(key);
    }
    for key in &keys {
        tree.remove(key);
        oracle.remove(key);
    }
    assert!(tree.is_empty(), "the tree still holds {} keys", tree.len());
    audit(&tree, &oracle, 11, 0);
    audit_ranges(&mut rng, &tree, &oracle, &mut fixed_bound, 11, 0);

    tree.clear();
    oracle.clear();
    audit(&tree, &oracle, 11, 1);

    for _ in 0..500 {
        let key = draw_key(&mut rng, &[]);
        tree.insert(key, 2);
        oracle.insert(key, 2);
    }
    audit(&tree, &oracle, 11, 2);
    audit_ranges(&mut rng, &tree, &oracle, &mut fixed_bound, 11, 2);
    audit_batches(&mut rng, &tree, &oracle, &mut draw_key, 11, 2);
}

// an empty tree answers everything the empty map answers
#[test]
fn the_empty_tree_agrees() {
    let mut tree = Tree::new();
    let oracle: BTreeMap<Key, u64> = BTreeMap::new();
    let mut rng = SmallRng::seed_from_u64(7);
    assert!(tree.is_empty());
    audit(&tree, &oracle, 7, 0);
    audit_ranges(&mut rng, &tree, &oracle, &mut fixed_bound, 7, 0);
    audit_batches(&mut rng, &tree, &oracle, &mut draw_key, 7, 0);
    // Packing nothing is not a special case anywhere else, so it is one here.
    tree.repack(32);
    audit(&tree, &oracle, 7, 1);
}

// a bulk load and a key at a time load of one run land the same map, repeats included
#[test]
fn a_bulk_load_lands_what_the_key_at_a_time_load_lands() {
    let key = |at: u64| {
        let mut out = [0u8; 34];
        out[..8].copy_from_slice(&at.to_be_bytes());
        out
    };
    // Every repeat shape: a pair, a longer run, one across a leaf boundary, singletons.
    let mut run: Vec<(Key, u64)> = Vec::new();
    for at in 0..200u64 {
        let repeats = match at % 7 {
            0 => 2,
            3 => 4,
            _ => 1,
        };
        for turn in 0..repeats {
            run.push((key(at), at * 100 + turn));
        }
    }

    let oracle: BTreeMap<Key, u64> = run.iter().copied().collect();
    for fill in [1usize, 2, 3, 31, 32] {
        let bulk = Tree::from_sorted(run.clone(), fill);
        let mut keyed = Tree::new();
        for (key, val) in run.iter().copied() {
            keyed.insert(key, val);
        }

        assert_eq!(
            bulk.len(),
            oracle.len(),
            "fill {fill}: the bulk load counted a repeat twice"
        );
        assert_eq!(
            keyed.len(),
            oracle.len(),
            "fill {fill}: the keyed load counted a repeat twice"
        );
        let walked: Vec<(Key, u64)> = bulk.iter().map(|(key, val)| (*key, *val)).collect();
        let wanted: Vec<(Key, u64)> = oracle.iter().map(|(key, val)| (*key, *val)).collect();
        assert_eq!(walked, wanted, "fill {fill}: the bulk walk diverged");

        // Every key taken out, since a second place only shows once the first has gone.
        let mut bulk = bulk;
        for (key, _) in &wanted {
            assert_eq!(
                bulk.remove(key),
                oracle.get(key).copied(),
                "fill {fill}: a removal diverged"
            );
            assert_eq!(
                bulk.get(key),
                None,
                "fill {fill}: a repeat left a phantom behind"
            );
        }
        assert!(
            bulk.is_empty(),
            "fill {fill}: the tree still holds {}",
            bulk.len()
        );
    }
}

// a node that empties and fills again answers for the keys it holds now
//
// A leaf holding one key agrees with itself on the whole width, so its window moves to
// the end of it. What replaces that key agrees on as many bytes and on different ones,
// which is a window unchanged by every measure except the one that matters.
#[test]
fn a_refilled_node_places_probes_against_what_it_holds_now() {
    const LEAVES: u64 = 8;

    let held = |at: u64, tail: u8| {
        let mut key: Key = [0x5a; 34];
        key[0] = at as u8;
        key[33] = tail;
        key
    };

    // A fill of one is the shape that puts a single key in every node, leaf and
    // separator alike, which is where a width-wide window comes from.
    let first: Vec<(Key, u64)> = (0..LEAVES).map(|at| (held(at, 0), at)).collect();
    let mut tree: Tree = TBTreeMap::from_sorted(first.clone(), 1);
    for (key, at) in &first {
        assert_eq!(tree.remove(key), Some(*at), "a key would not leave");
    }
    assert!(tree.is_empty(), "the tree still holds {}", tree.len());

    for at in 0..LEAVES {
        let fresh = held(at, 0x11);
        tree.insert(fresh, at + 100);
    }
    for at in 0..LEAVES {
        assert_eq!(
            tree.get(&held(at, 0x11)),
            Some(&(at + 100)),
            "a refilled node lost the key it took",
        );
        assert_eq!(
            tree.get(&held(at, 0)),
            None,
            "a refilled node answered for the key that left",
        );
    }
    let walked: Vec<Key> = tree.iter().map(|(key, _)| *key).collect();
    let mut wanted: Vec<Key> = (0..LEAVES).map(|at| held(at, 0x11)).collect();
    wanted.sort();
    assert_eq!(walked, wanted, "a refilled tree walked out of order");
}

// keys that share their whole lead have a node read its lead from past them
#[test]
fn a_shared_lead_moves_the_window_and_still_answers() {
    const COUNT: usize = 4_000;

    let mut tied: Tree = TBTreeMap::new();
    let mut spread: Tree = TBTreeMap::new();
    let mut rng = SmallRng::seed_from_u64(0x11ed);

    let mut tied_keys = Vec::with_capacity(COUNT);
    let mut spread_keys = Vec::with_capacity(COUNT);
    for at in 0..COUNT as u64 {
        // An address, an epoch, a pubkey: every key in the scan carries them identically.
        let mut key: Key = [0x5a; 34];
        key[8..].copy_from_slice(&[0u8; 26]);
        key[8..16].copy_from_slice(&at.to_be_bytes());
        tied.insert(key, at);
        tied_keys.push(key);

        let mut other: Key = [0u8; 34];
        rng.fill(&mut other[..]);
        spread.insert(other, at);
        spread_keys.push(other);
    }

    assert_eq!(tied.len(), COUNT, "the tied tree lost keys");
    assert_eq!(spread.len(), COUNT, "the spread tree lost keys");

    assert!(
        tied.lead_skip() >= 8.0,
        "a node holding keys that agree on eight bytes moved its window {:.1} bytes",
        tied.lead_skip(),
    );
    assert!(
        tied.tie_rate() < 0.01,
        "keys sharing eight leading bytes reported a tie rate of {:.4}",
        tied.tie_rate(),
    );
    assert!(
        spread.tie_rate() < 0.01,
        "keys spread across their whole width reported a tie rate of {:.4}",
        spread.tie_rate(),
    );

    // The answers are the point: the window moves the lead and changes nothing else.
    for (at, key) in tied_keys.iter().enumerate() {
        assert_eq!(
            tied.get(key),
            Some(&(at as u64)),
            "a tied key answered wrong"
        );
    }
    for (at, key) in spread_keys.iter().enumerate() {
        assert_eq!(
            spread.get(key),
            Some(&(at as u64)),
            "a spread key answered wrong"
        );
    }

    // A probe carrying none of the bytes the nodes share is placed by that
    // comparison rather than by a lead read from past bytes it does not have.
    for edge in [0x00u8, 0xff] {
        let mut outside: Key = [0x5a; 34];
        outside[0] = edge;
        assert_eq!(
            tied.get(&outside),
            None,
            "a probe outside the shared prefix was found",
        );
    }

    // The order comes from the whole keys, whatever the window reads.
    let walked: Vec<Key> = tied.iter().map(|(key, _)| *key).collect();
    let mut wanted = tied_keys.clone();
    wanted.sort();
    assert_eq!(walked, wanted, "a windowed run walked out of order");
}
