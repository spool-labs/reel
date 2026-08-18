//! What deletion costs a tree that never merges, and what a rebuild buys back
//!
//! Removal leaves fill behind rather than carrying merge logic down every delete, so
//! the fill factor is the signal a rebuild watches. Each deletion step reports the
//! fill, the leaves held, a sampled point read, the walk per element, and what the
//! survivor rebuild costs. Two deletion shapes, since they decay differently: uniform
//! thins every leaf evenly, clustered empties whole runs the walk must still step
//! over.
//!
//! Ignored by default. Run with:
//!   cargo test --release --test tbtreemap_fill -- --ignored --nocapture

use std::time::Instant;

use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

use reel::index::tbtreemap::TBTreeMap;

type Key = [u8; 34];
type Tree = TBTreeMap<[u8; 34], 32, u64>;

/// Keys the sweep builds with
const KEYS: usize = 1 << 20;

/// Point reads sampled per step
const PROBES: usize = 20_000;

/// The fill each rebuild packs to
const FILL: usize = 32;

fn slot_led(at: u64) -> Key {
    let mut key = [0u8; 34];
    key[..8].copy_from_slice(&at.to_be_bytes());
    let spread = at.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    key[8..16].copy_from_slice(&spread.to_be_bytes());
    key
}

fn timed_gets(tree: &Tree, asked: &[Key]) -> f64 {
    let start = Instant::now();
    let mut hits = 0usize;
    for key in asked {
        hits += tree.get(key).is_some() as usize;
    }
    let spent = start.elapsed().as_nanos() as f64 / asked.len() as f64;
    std::hint::black_box(hits);
    spent
}

fn timed_walk(tree: &Tree) -> f64 {
    let start = Instant::now();
    let mut walked = 0usize;
    for (keys, _) in tree.chunks() {
        walked += keys.len();
    }
    let spent = start.elapsed().as_nanos() as f64;
    std::hint::black_box(walked);
    match walked {
        0 => 0.0,
        _ => spent / walked as f64,
    }
}

fn sweep(label: &str, doomed: &[Key], survivors_of: impl Fn(&[Key]) -> Vec<Key>) {
    let pairs: Vec<(Key, u64)> = (0..KEYS as u64).map(|at| (slot_led(at), at)).collect();
    let mut tree = Tree::from_sorted(pairs, FILL);
    let mut rng = SmallRng::seed_from_u64(11);

    println!();
    println!("{label}");
    println!(
        "{:>8}  {:>6}  {:>8}  {:>9}  {:>10}  {:>11}  {:>11}",
        "deleted", "fill", "leaves", "get", "walk/elem", "rebuild", "get after"
    );

    let mut removed = 0usize;
    for step in [10usize, 30, 50, 70, 90] {
        let wanted = KEYS * step / 100;
        while removed < wanted {
            tree.remove(&doomed[removed]);
            removed += 1;
        }

        let live = survivors_of(&doomed[..removed]);
        let asked: Vec<Key> = (0..PROBES)
            .map(|_| match rng.gen_bool(0.8) {
                true if !live.is_empty() => live[rng.gen_range(0..live.len())],
                _ => slot_led(rng.gen_range(KEYS as u64..2 * KEYS as u64)),
            })
            .collect();

        let get = timed_gets(&tree, &asked);
        let walk = timed_walk(&tree);

        let survivors: Vec<(Key, u64)> = tree.iter().map(|(key, val)| (*key, *val)).collect();
        let start = Instant::now();
        let rebuilt = Tree::from_sorted(survivors, FILL);
        let rebuild = start.elapsed().as_nanos() as f64 / rebuilt.len().max(1) as f64;
        let after = timed_gets(&rebuilt, &asked);

        println!(
            "{:>7}%  {:>5.1}%  {:>8}  {:>6.1} ns  {:>7.2} ns  {:>8.1} ns  {:>8.1} ns",
            step,
            tree.fill_factor() * 100.0,
            tree.leaf_count(),
            get,
            walk,
            rebuild,
            after,
        );
    }
}

// fill decay and its price under both deletion shapes, and the rebuild's answer
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn fill_decay_under_deletion() {
    let mut uniform: Vec<Key> = (0..KEYS as u64).map(slot_led).collect();
    let mut rng = SmallRng::seed_from_u64(5);
    uniform.shuffle(&mut rng);
    sweep("uniform deletes", &uniform, |gone| {
        let gone: std::collections::HashSet<&Key> = gone.iter().collect();
        (0..KEYS as u64)
            .map(slot_led)
            .filter(|key| !gone.contains(key))
            .collect()
    });

    // Clustered: the front of the key space goes first, the prune's shape.
    let clustered: Vec<Key> = (0..KEYS as u64).map(slot_led).collect();
    sweep("clustered deletes", &clustered, |gone| {
        (gone.len() as u64..KEYS as u64).map(slot_led).collect()
    });
}
