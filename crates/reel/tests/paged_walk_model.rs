//! Walks of a paged column answer as a model does, through writes, deletes, seals, compaction and reopens
//!
//! One column holds 16 byte keys spread like hashes, on a paged volume. A tree column
//! walks its sealed keys out of the footers, and an ordered one out of FastForward in
//! key order. Every walk shape is checked against a model of what the column holds:
//! whole walks both ways, walks from a bound, ranges, prefixes, keys alone, and walks
//! the caller stops early. Checks follow maintenance, so a walk meets a compaction pass
//! that has just retired a segment. The index counts the pages ordered walks filled, so
//! an ordered run that quietly fell back to the footers fails.
//!
//! Knobs: REEL_PWM_SEEDS (how many seeds, default 6), REEL_PWM_FIRST (the first seed,
//! default 1), REEL_PWM_OPS (ops a seed runs, default 3000) and REEL_PWM_SEED (one seed
//! to replay).

use std::collections::BTreeMap;
use std::ops::Bound;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, IndexResidency, KeyWidth, MapShape,
    ReelConfig, ReelStore, SyncPolicy, ThreadBudget,
};
use reel_core::{Direction, Store};

const fn rows(map_shape: MapShape) -> ColumnSpec {
    ColumnSpec {
        id: ColumnId(1),
        name: "rows",
        key_width: KeyWidth::Fixed(16),
        shard_bytes: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape,
    }
}

const TREE: ColumnSet = &[rows(MapShape::Tree)];
const ORDERED: ColumnSet = &[rows(MapShape::Ordered)];

/// Distinct keys a run writes, deletes and walks
const KEYS: u64 = 2000;

/// What the column should hold, key to value
type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn knob(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

/// Key number `n`, its leading bytes spread like a hash and its tail the number itself
fn key_of(n: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(16);
    key.extend_from_slice(&mix(n).to_be_bytes());
    key.extend_from_slice(&n.to_be_bytes());
    key
}

/// A value that starts with its key's number and version, so a value served for the wrong key fails
fn value_of(n: u64, version: u64, len: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(16 + len);
    value.extend_from_slice(&n.to_be_bytes());
    value.extend_from_slice(&version.to_be_bytes());
    value.extend((0..len).map(|at| (n ^ version ^ at as u64) as u8));
    value
}

fn config(rng: &mut SmallRng) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(64 * 1024),
        alloc_chunk: ByteCount::from_bytes(16 * 1024),
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(rng.gen_range(1..=3)),
        compact_dead_ratio: 0.1,
        scrub_mbps: 0,
        index: IndexResidency::Paged,
        ..ReelConfig::default()
    }
}

/// A bound for a walk: a key the run uses, or random bytes that match no key
fn bound_of(rng: &mut SmallRng) -> Vec<u8> {
    match rng.gen_bool(0.5) {
        true => key_of(rng.gen_range(0..KEYS)),
        false => (0..16).map(|_| rng.r#gen::<u8>()).collect(),
    }
}

fn pairs(walk: impl Iterator<Item = (Vec<u8>, reel_core::Value)>) -> Vec<(Vec<u8>, Vec<u8>)> {
    walk.map(|(key, value)| (key, value.to_vec())).collect()
}

fn expect(model: &Model, range: (Bound<&[u8]>, Bound<&[u8]>), down: bool) -> Vec<(Vec<u8>, Vec<u8>)> {
    let run = model
        .range::<[u8], _>(range)
        .map(|(key, value)| (key.clone(), value.clone()));
    match down {
        true => run.rev().collect(),
        false => run.collect(),
    }
}

/// Every walk shape against the model
fn check(store: &ReelStore, model: &Model, rng: &mut SmallRng, seed: u64, stage: &str) {
    let whole = pairs(Store::iter(store, "rows").expect("iter"));
    assert_eq!(whole, expect(model, (Bound::Unbounded, Bound::Unbounded), false), "seed {seed} {stage}: whole walk up");
    let top = [0xFFu8; 16];
    let down = pairs(Store::iter_from(store, "rows", &top, Direction::Desc).expect("iter from"));
    assert_eq!(down, expect(model, (Bound::Unbounded, Bound::Unbounded), true), "seed {seed} {stage}: whole walk down");

    for _ in 0..12 {
        let bound = bound_of(rng);
        let up = pairs(Store::iter_from(store, "rows", &bound, Direction::Asc).expect("iter from"));
        let want = expect(model, (Bound::Included(&bound), Bound::Unbounded), false);
        assert_eq!(up, want, "seed {seed} {stage}: up from {bound:02x?}");

        let down = pairs(Store::iter_from(store, "rows", &bound, Direction::Desc).expect("iter from"));
        let want = expect(model, (Bound::Unbounded, Bound::Included(&bound)), true);
        assert_eq!(down, want, "seed {seed} {stage}: down from {bound:02x?}");

        let (low, high) = {
            let other = bound_of(rng);
            match bound <= other {
                true => (bound.clone(), other),
                false => (other, bound.clone()),
            }
        };
        let range = pairs(Store::iter_range(store, "rows", &low, &high).expect("iter range"));
        let want = expect(model, (Bound::Included(&low), Bound::Excluded(&high)), false);
        assert_eq!(range, want, "seed {seed} {stage}: range {low:02x?}..{high:02x?}");

        let prefix = &bound[..rng.gen_range(1..=2)];
        let prefixed = pairs(Store::iter_prefix(store, "rows", prefix).expect("iter prefix"));
        let want: Vec<(Vec<u8>, Vec<u8>)> = model
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        assert_eq!(prefixed, want, "seed {seed} {stage}: prefix {prefix:02x?}");

        let keys: Vec<Vec<u8>> = store
            .iter_keys_from("rows", Some(&bound), Direction::Asc)
            .expect("keys")
            .collect();
        let want: Vec<Vec<u8>> = model
            .range::<[u8], _>((Bound::Included(bound.as_slice()), Bound::Unbounded))
            .map(|(key, _)| key.clone())
            .collect();
        assert_eq!(keys, want, "seed {seed} {stage}: keys up from {bound:02x?}");

        // A walk the caller stops early, sized by a hint, still starts in the right place.
        let hint = [1usize, 3, 17][rng.gen_range(0..3)];
        let mut lent = store
            .iter_lent("rows", Some(&bound), Direction::Asc, hint)
            .expect("lent");
        let mut short = Vec::new();
        while short.len() < hint {
            let Some((key, value)) = lent.next() else {
                break;
            };
            short.push((key.to_vec(), value.to_vec()));
        }
        let want: Vec<(Vec<u8>, Vec<u8>)> = expect(model, (Bound::Included(&bound), Bound::Unbounded), false)
            .into_iter()
            .take(hint)
            .collect();
        assert_eq!(short, want, "seed {seed} {stage}: {hint} rows up from {bound:02x?}");
    }
}

fn run(seed: u64, columns: ColumnSet) {
    let mut rng = SmallRng::seed_from_u64(seed);
    let dir = TempDir::new().expect("temp dir");
    let config = config(&mut rng);
    let mut store = ReelStore::open(dir.path().to_path_buf(), config.clone(), columns).expect("open");
    let mut model = Model::new();
    let ops = knob("REEL_PWM_OPS", 3000);

    for op in 1..=ops {
        let roll = rng.gen_range(0..100u32);
        let n = rng.gen_range(0..KEYS);
        match roll {
            0..72 => {
                let value = value_of(n, op, rng.gen_range(0..400));
                Store::put(&store, "rows", &key_of(n), &value).expect("put");
                model.insert(key_of(n), value);
            }
            72..98 => {
                Store::delete(&store, "rows", &key_of(n)).expect("delete");
                model.remove(&key_of(n));
            }
            // A narrow range, about one 256th of the key space, so the column keeps keys to walk.
            _ => {
                let low = key_of(n);
                let mut high = low.clone();
                high[1] = high[1].saturating_add(1);
                Store::delete_range(&store, "rows", &low, &high).expect("delete range");
                let gone: Vec<Vec<u8>> = model
                    .range::<[u8], _>((Bound::Included(low.as_slice()), Bound::Excluded(high.as_slice())))
                    .map(|(key, _)| key.clone())
                    .collect();
                for key in gone {
                    model.remove(&key);
                }
            }
        }
        if op % 150 == 0 {
            Store::maintain(&store).expect("maintain");
        }
        if op % 1000 == 0 {
            check(&store, &model, &mut rng, seed, &format!("op {op}"));
        }
        if op % 1400 == 0 {
            store.close().expect("close");
            drop(store);
            store = ReelStore::open(dir.path().to_path_buf(), config.clone(), columns).expect("reopen");
            check(&store, &model, &mut rng, seed, &format!("reopen at op {op}"));
        }
    }

    for _ in 0..4 {
        Store::maintain(&store).expect("maintain");
    }
    check(&store, &model, &mut rng, seed, "settled");
    let (pages, fallbacks) = store.index().ordered_walks();
    println!("seed {seed} {:?}: {pages} ordered pages, {fallbacks} sent to the footers, {} keys held", columns[0].map_shape, model.len());
    if columns[0].map_shape == MapShape::Ordered {
        assert!(pages > 0, "seed {seed}: no walk read the ordered index");
    }


    store.close().expect("close");
    drop(store);
    let reopened = ReelStore::open(dir.path().to_path_buf(), config, columns).expect("reopen");
    check(&reopened, &model, &mut rng, seed, "final reopen");
}

fn seeds(columns: ColumnSet) {
    if let Ok(seed) = std::env::var("REEL_PWM_SEED") {
        run(seed.parse().expect("REEL_PWM_SEED is a number"), columns);
        return;
    }
    let first = knob("REEL_PWM_FIRST", 1);
    for seed in first..first + knob("REEL_PWM_SEEDS", 6) {
        run(seed, columns);
    }
}

#[test]
fn tree_walks_answer_as_the_model() {
    seeds(TREE);
}

#[test]
fn ordered_walks_answer_as_the_model() {
    seeds(ORDERED);
}
