//! The shipped node widths against BTreeMap, on integer and on byte keys
//!
//! Every op is applied to both and compared. The walks and ranges are checked
//! periodically rather than per op, so a run reaches the widths where a node has
//! split and merged many times over.

use std::collections::BTreeMap;
use std::ops::Bound;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use reel::index::tbtreemap::{node_width, TBTreeMap, NODE_WIDTH};

/// Nodes a tree of record keys holds, which the budget makes an odd number
const RECORD_NODES: usize = node_width(34);

// integer keys at the shipped node width answer what BTreeMap answers
#[test]
fn integer_keys_at_shipped_width() {
    for seed in [1u64, 7, 99, 4173] {
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut tree: TBTreeMap<u64, NODE_WIDTH, u64> = TBTreeMap::new();
        let mut model: BTreeMap<u64, u64> = BTreeMap::new();

        for op in 0..60_000u64 {
            // Ascending with occasional scatter, the segment id and lsn shape.
            let key = match rng.gen_range(0..10) {
                0..=6 => op / 3,
                7 => rng.gen_range(0..op + 1),
                _ => rng.gen::<u64>() >> 40,
            };
            match rng.gen_range(0..10) {
                0..=6 => {
                    assert_eq!(
                        tree.insert(key, op),
                        model.insert(key, op),
                        "seed {seed} op {op}: insert"
                    );
                }
                7 => {
                    assert_eq!(
                        tree.remove(&key),
                        model.remove(&key),
                        "seed {seed} op {op}: remove"
                    );
                }
                _ => {
                    assert_eq!(tree.get(&key), model.get(&key), "seed {seed} op {op}: get");
                }
            }
            if op % 4096 == 0 {
                let walked: Vec<u64> = tree.iter().map(|(k, _)| *k).collect();
                let wanted: Vec<u64> = model.keys().copied().collect();
                assert_eq!(walked, wanted, "seed {seed} op {op}: walk");
                let lo = rng.gen_range(0..op / 2 + 2);
                let hi = lo + rng.gen_range(0..op / 2 + 2);
                let ranged: Vec<u64> = tree
                    .range(Bound::Included(&lo), Bound::Excluded(&hi))
                    .map(|(k, _)| *k)
                    .collect();
                let modeled: Vec<u64> = model.range(lo..hi).map(|(k, _)| *k).collect();
                assert_eq!(ranged, modeled, "seed {seed} op {op}: range {lo}..{hi}");
                let back: Vec<u64> = tree
                    .range_back(Bound::Included(&lo), Bound::Excluded(&hi))
                    .map(|(k, _)| *k)
                    .collect();
                let mut wanted_back = modeled.clone();
                wanted_back.reverse();
                assert_eq!(
                    back, wanted_back,
                    "seed {seed} op {op}: range_back {lo}..{hi}"
                );
            }
        }
        assert_eq!(tree.len(), model.len(), "seed {seed}: final length");
    }
}

// byte keys at the record width answer the same, bulk build included
#[test]
fn byte_keys_at_shipped_width() {
    for seed in [3u64, 41, 4173, 0x5eed] {
        let mut rng = SmallRng::seed_from_u64(seed);
        let mut tree: TBTreeMap<[u8; 34], RECORD_NODES, u64> = TBTreeMap::new();
        let mut model: BTreeMap<[u8; 34], u64> = BTreeMap::new();
        let mut pool: Vec<[u8; 34]> = Vec::new();

        for op in 0..40_000u64 {
            let key = if !pool.is_empty() && rng.gen_bool(0.4) {
                pool[rng.gen_range(0..pool.len())]
            } else {
                let mut key = [0u8; 34];
                if rng.gen_bool(0.5) {
                    let slot: u64 = rng.gen_range(0..4096);
                    key[..8].copy_from_slice(&slot.to_be_bytes());
                    rng.fill(&mut key[8..]);
                } else {
                    rng.fill(&mut key[..]);
                }
                pool.push(key);
                key
            };
            match rng.gen_range(0..10) {
                0..=6 => {
                    assert_eq!(
                        tree.insert(key, op),
                        model.insert(key, op),
                        "seed {seed} op {op}: insert"
                    );
                }
                7 => {
                    assert_eq!(
                        tree.remove(&key),
                        model.remove(&key),
                        "seed {seed} op {op}: remove"
                    );
                }
                _ => {
                    assert_eq!(tree.get(&key), model.get(&key), "seed {seed} op {op}: get");
                }
            }
            if op % 4096 == 0 {
                let walked: Vec<[u8; 34]> = tree.iter().map(|(k, _)| *k).collect();
                let wanted: Vec<[u8; 34]> = model.keys().copied().collect();
                assert_eq!(walked, wanted, "seed {seed} op {op}: walk");
            }
        }
        let built: TBTreeMap<[u8; 34], RECORD_NODES, u64> =
            TBTreeMap::from_sorted(model.iter().map(|(k, v)| (*k, *v)), RECORD_NODES);
        for (key, val) in model.iter() {
            assert_eq!(
                built.get(key),
                Some(val),
                "seed {seed}: bulk build lost a key"
            );
        }
    }
}
