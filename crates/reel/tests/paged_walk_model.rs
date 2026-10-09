//! Paged column walks match a model through writes, deletes, seals, compaction and reopens
//! Knobs: REEL_PWM_SEEDS, REEL_PWM_FIRST, REEL_PWM_OPS, and REEL_PWM_SEED to replay one seed

use std::collections::BTreeMap;
use std::ops::Bound;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, ReelConfig, ReelStore, SyncPolicy,
    ThreadBudget,
};
use reel_core::{Direction, Store};

const fn rows(key_width: KeyWidth) -> ColumnSpec {
    ColumnSpec {
        id: ColumnId(1),
        name: "rows",
        key_width,
        shard_bytes: 0,
        purge_mark: None,
        codec: Codec::None,
    }
}

const FIXED: ColumnSet = &[rows(KeyWidth::Fixed(16))];
const VARIABLE: ColumnSet = &[rows(KeyWidth::Variable)];

/// The variable run cuts one key of every width up to this from each stem
const STEM: usize = 24;

/// What the run's keys look like
#[derive(Clone, Copy, Debug)]
enum Shape {
    /// 16 bytes each, the leading eight spread like a hash
    Fixed,

    /// One to 24 bytes, so a key is a prefix of every longer key from its stem
    Variable,
}

impl Shape {
    fn columns(self) -> ColumnSet {
        match self {
            Shape::Fixed => FIXED,
            Shape::Variable => VARIABLE,
        }
    }

    /// Key number `n`
    fn key(self, n: u64) -> Vec<u8> {
        match self {
            Shape::Fixed => key_of(n),
            Shape::Variable => {
                let stem = n / STEM as u64;
                let mut key: Vec<u8> = (0..3)
                    .flat_map(|word| mix(stem * 3 + word).to_be_bytes())
                    .collect();
                // Zero bytes inside keys catch an order that pads the shorter key with zeros
                key[3] = 0;
                key[9] = 0;
                key.truncate(n as usize % STEM + 1);
                key
            }
        }
    }

    /// A key that sorts past every key in the run
    fn top(self) -> Vec<u8> {
        match self {
            Shape::Fixed => vec![0xFF; 16],
            Shape::Variable => vec![0xFF; STEM + 1],
        }
    }

    /// A bound for a walk: a key the run uses, or random bytes that match no key
    fn bound(self, rng: &mut SmallRng) -> Vec<u8> {
        if rng.gen_bool(0.5) {
            return self.key(rng.gen_range(0..KEYS));
        }
        let len = match self {
            Shape::Fixed => 16,
            Shape::Variable => rng.gen_range(1..=STEM + 1),
        };
        (0..len).map(|_| rng.r#gen::<u8>()).collect()
    }

    /// The far end of a narrow range from a key, about one 256th of the key space
    fn range_end(self, low: &[u8]) -> Vec<u8> {
        match self {
            Shape::Fixed => {
                let mut high = low.to_vec();
                high[1] = high[1].saturating_add(1);
                high
            }
            Shape::Variable => {
                let second = low.get(1).copied().unwrap_or(0);
                vec![low[0], second.saturating_add(1)]
            }
        }
    }
}

/// A run writes, deletes and walks this many distinct keys
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

/// A value that opens with its key's number and version, so a misplaced value fails
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
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(rng.gen_range(1..=3)),
        compact_dead_ratio: 0.1,
        scrub_mbps: 0,
        ..ReelConfig::default()
    }
}

/// Where two walks first part, with the keys on each side, or `None` when they agree
fn parting(got: &[(Vec<u8>, Vec<u8>)], want: &[(Vec<u8>, Vec<u8>)]) -> Option<String> {
    let at = (0..got.len().max(want.len())).find(|&at| got.get(at) != want.get(at))?;
    let side = |rows: &[(Vec<u8>, Vec<u8>)]| {
        rows.get(at)
            .map(|(key, value)| (key.clone(), value.get(..16).map(<[u8]>::to_vec)))
    };
    Some(format!(
        "row {at} of {} got and {} wanted: got {:02x?}, want {:02x?}, the row before {:02x?}",
        got.len(),
        want.len(),
        side(got),
        side(want),
        at.checked_sub(1)
            .and_then(|before| want.get(before))
            .map(|(key, _)| key),
    ))
}

macro_rules! assert_walk {
    ($got:expr, $want:expr, $($what:tt)+) => {
        if let Some(parted) = parting(&$got, &$want) {
            panic!("{}: {}", format!($($what)+), parted);
        }
    };
}

fn pairs(walk: impl Iterator<Item = (Vec<u8>, reel_core::Value)>) -> Vec<(Vec<u8>, Vec<u8>)> {
    walk.map(|(key, value)| (key, value.to_vec())).collect()
}

fn expect(
    model: &Model,
    range: (Bound<&[u8]>, Bound<&[u8]>),
    down: bool,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let run = model
        .range::<[u8], _>(range)
        .map(|(key, value)| (key.clone(), value.clone()));
    match down {
        true => run.rev().collect(),
        false => run.collect(),
    }
}

/// Checks every walk shape against the model
fn check(
    store: &ReelStore,
    model: &Model,
    shape: Shape,
    rng: &mut SmallRng,
    seed: u64,
    stage: &str,
) {
    let whole = pairs(Store::iter(store, "rows").expect("iter"));
    let wanted = expect(model, (Bound::Unbounded, Bound::Unbounded), false);
    assert_walk!(whole, wanted, "seed {seed} {stage}: whole walk up");
    let top = shape.top();
    let down = pairs(Store::iter_from(store, "rows", &top, Direction::Desc).expect("iter from"));
    assert_walk!(
        down,
        expect(model, (Bound::Unbounded, Bound::Unbounded), true),
        "seed {seed} {stage}: whole walk down"
    );

    for _ in 0..12 {
        let bound = shape.bound(rng);
        let up = pairs(Store::iter_from(store, "rows", &bound, Direction::Asc).expect("iter from"));
        let want = expect(model, (Bound::Included(&bound), Bound::Unbounded), false);
        assert_walk!(up, want, "seed {seed} {stage}: up from {bound:02x?}");

        let down =
            pairs(Store::iter_from(store, "rows", &bound, Direction::Desc).expect("iter from"));
        let want = expect(model, (Bound::Unbounded, Bound::Included(&bound)), true);
        assert_walk!(down, want, "seed {seed} {stage}: down from {bound:02x?}");

        let (low, high) = {
            let other = shape.bound(rng);
            match bound <= other {
                true => (bound.clone(), other),
                false => (other, bound.clone()),
            }
        };
        let range = pairs(Store::iter_range(store, "rows", &low, &high).expect("iter range"));
        let want = expect(
            model,
            (Bound::Included(&low), Bound::Excluded(&high)),
            false,
        );
        assert_walk!(
            range,
            want,
            "seed {seed} {stage}: range {low:02x?}..{high:02x?}"
        );

        let prefix = &bound[..rng.gen_range(1..=bound.len().min(2))];
        let prefixed = pairs(Store::iter_prefix(store, "rows", prefix).expect("iter prefix"));
        let want: Vec<(Vec<u8>, Vec<u8>)> = model
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        assert_walk!(prefixed, want, "seed {seed} {stage}: prefix {prefix:02x?}");

        let keys: Vec<Vec<u8>> = store
            .iter_keys_from("rows", Some(&bound), Direction::Asc)
            .expect("keys")
            .collect();
        let want: Vec<Vec<u8>> = model
            .range::<[u8], _>((Bound::Included(bound.as_slice()), Bound::Unbounded))
            .map(|(key, _)| key.clone())
            .collect();
        assert_eq!(keys, want, "seed {seed} {stage}: keys up from {bound:02x?}");

        // A walk the caller stops early, sized by a hint, still starts in the right place
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
        let want: Vec<(Vec<u8>, Vec<u8>)> =
            expect(model, (Bound::Included(&bound), Bound::Unbounded), false)
                .into_iter()
                .take(hint)
                .collect();
        assert_walk!(
            short,
            want,
            "seed {seed} {stage}: {hint} rows up from {bound:02x?}"
        );
    }
}

fn run(seed: u64, shape: Shape) {
    let columns = shape.columns();
    let mut rng = SmallRng::seed_from_u64(seed);
    let dir = TempDir::new().expect("temp dir");
    let config = config(&mut rng);
    let mut store =
        ReelStore::open(dir.path().to_path_buf(), config.clone(), columns).expect("open");
    let mut model = Model::new();
    let ops = knob("REEL_PWM_OPS", 3000);

    for op in 1..=ops {
        let roll = rng.gen_range(0..100u32);
        let n = rng.gen_range(0..KEYS);
        match roll {
            0..72 => {
                let value = value_of(n, op, rng.gen_range(0..400));
                Store::put(&store, "rows", &shape.key(n), &value).expect("put");
                model.insert(shape.key(n), value);
            }
            72..98 => {
                Store::delete(&store, "rows", &shape.key(n)).expect("delete");
                model.remove(&shape.key(n));
            }
            // Narrow ranges leave the column keys to walk
            _ => {
                let low = shape.key(n);
                let high = shape.range_end(&low);
                // A saturated byte can leave the end at or before the key
                if high > low {
                    Store::delete_range(&store, "rows", &low, &high).expect("delete range");
                    let gone: Vec<Vec<u8>> = model
                        .range::<[u8], _>((
                            Bound::Included(low.as_slice()),
                            Bound::Excluded(high.as_slice()),
                        ))
                        .map(|(key, _)| key.clone())
                        .collect();
                    for key in gone {
                        model.remove(&key);
                    }
                }
            }
        }
        if op % 150 == 0 {
            Store::maintain(&store).expect("maintain");
        }
        if op % 1000 == 0 {
            check(&store, &model, shape, &mut rng, seed, &format!("op {op}"));
        }
        if op % 1400 == 0 {
            store.close().expect("close");
            drop(store);
            store =
                ReelStore::open(dir.path().to_path_buf(), config.clone(), columns).expect("reopen");
            check(
                &store,
                &model,
                shape,
                &mut rng,
                seed,
                &format!("reopen at op {op}"),
            );
        }
    }

    for _ in 0..4 {
        Store::maintain(&store).expect("maintain");
    }
    check(&store, &model, shape, &mut rng, seed, "settled");
    println!("seed {seed} {shape:?}: {} keys held", model.len());

    store.close().expect("close");
    drop(store);
    let reopened = ReelStore::open(dir.path().to_path_buf(), config, columns).expect("reopen");
    check(&reopened, &model, shape, &mut rng, seed, "final reopen");
}

fn seeds(shape: Shape) {
    if let Ok(seed) = std::env::var("REEL_PWM_SEED") {
        run(seed.parse().expect("REEL_PWM_SEED is a number"), shape);
        return;
    }
    let first = knob("REEL_PWM_FIRST", 1);
    for seed in first..first + knob("REEL_PWM_SEEDS", 6) {
        run(seed, shape);
    }
}

// walks over sixteen byte keys spread like hashes answer as the model does
#[test]
fn walks_answer_as_the_model() {
    seeds(Shape::Fixed);
}

// walks over keys of every width, each a prefix of the longer ones, answer as the model does
#[test]
fn walks_of_every_key_width_answer_as_the_model() {
    seeds(Shape::Variable);
}
