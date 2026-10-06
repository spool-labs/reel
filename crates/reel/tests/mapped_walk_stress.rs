//! Walks served from segment mappings, under writers, deletes and compaction, drawn from one seed
//!
//! Every read is mapped and segments are tiny, so seals, compaction, handle eviction and
//! remaps all happen while walkers run. A walk must hand back keys in order, inside its
//! bound, each value whole and a version somebody wrote for that key. Once writers stop,
//! a walk up, a walk down and a point read of every key must agree.
//!
//! Knobs: REEL_MW_SEEDS (how many seeds, default 6), REEL_MW_FIRST (the first seed, default 1,
//! so parallel processes split a campaign), REEL_MW_OPS (ops per writer, default 2500),
//! REEL_MW_SEED (one seed to replay), REEL_MW_UNMAPPED (reads through the driver) and
//! REEL_MW_URING (the buffered ring backend, Linux only).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, ReelConfig, ReelStore, SyncPolicy,
    ThreadBudget, MAP_EVERYTHING,
};
use reel_core::{Direction, Store};

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: "rows",
    key_width: KeyWidth::Fixed(8),
    shard_bytes: 0,
    purge_mark: None,
    codec: Codec::None,
}];

const KEYS: u64 = 1500;

fn knob(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// A value that names its key and version and fills the rest from both, so a torn or
/// misplaced read cannot pass
fn value_of(key: u64, version: u64, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + len);
    out.extend_from_slice(&key.to_be_bytes());
    out.extend_from_slice(&version.to_be_bytes());
    let mut x = key.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ version;
    for _ in 0..len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.push(x as u8);
    }
    out
}

/// The key and version a value claims, once its filler checks out
fn check_value(seed: u64, key: u64, value: &[u8]) -> u64 {
    assert!(
        value.len() >= 16,
        "seed {seed}: key {key} value of {} bytes",
        value.len()
    );
    let named = u64::from_be_bytes(value[..8].try_into().unwrap());
    let version = u64::from_be_bytes(value[8..16].try_into().unwrap());
    assert_eq!(
        named, key,
        "seed {seed}: key {key} served the value of key {named}"
    );
    assert_eq!(
        value,
        value_of(key, version, value.len() - 16).as_slice(),
        "seed {seed}: key {key} version {version} came back torn"
    );
    version
}

fn config(rng: &mut SmallRng) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(64 * 1024),
        alloc_chunk: ByteCount::from_bytes(16 * 1024),
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(rng.gen_range(1..=4)),
        compact_dead_ratio: 0.1,
        scrub_mbps: 0,
        io_backend: match std::env::var("REEL_MW_URING").is_ok() {
            true => reel::IoBackend::Uring,
            false => reel::IoBackend::Posix,
        },
        map_above: match std::env::var("REEL_MW_UNMAPPED").is_ok() {
            true => None,
            false => MAP_EVERYTHING,
        },
        ..ReelConfig::default()
    }
}

type Attempted = Arc<Vec<Mutex<BTreeSet<u64>>>>;

fn writer(store: &ReelStore, attempted: &Attempted, seed: u64, id: u64, ops: u64) {
    let mut rng = SmallRng::seed_from_u64(seed ^ (id << 40) ^ 0xA5A5);
    for step in 0..ops {
        let key = rng.gen_range(0..KEYS);
        let version = (id << 32) | step;
        match rng.gen_range(0..100u32) {
            0..=69 => {
                let len = rng.gen_range(0..600);
                attempted[key as usize].lock().unwrap().insert(version);
                Store::put(
                    store,
                    "rows",
                    &key.to_be_bytes(),
                    &value_of(key, version, len),
                )
                .expect("put");
            }
            70..=84 => {
                Store::delete(store, "rows", &key.to_be_bytes()).expect("delete");
            }
            _ => {
                // A run of neighbours, so a walk meets fresh records side by side.
                let width = rng.gen_range(2..20u64);
                for k in key..(key + width).min(KEYS) {
                    let len = rng.gen_range(0..300);
                    attempted[k as usize].lock().unwrap().insert(version);
                    Store::put(store, "rows", &k.to_be_bytes(), &value_of(k, version, len))
                        .expect("put run");
                }
            }
        }
    }
}

fn walker(store: &ReelStore, attempted: &Attempted, seed: u64, id: u64, done: &AtomicBool) -> u64 {
    let mut rng = SmallRng::seed_from_u64(seed ^ (id << 48) ^ 0x5A5A);
    let mut walked = 0u64;
    while !done.load(Ordering::Relaxed) {
        let start = rng.gen_range(0..KEYS);
        let way = match rng.gen_bool(0.5) {
            true => Direction::Asc,
            false => Direction::Desc,
        };
        let want = rng.gen_range(1..=400usize);
        let hint = rng.gen_range(1..=400usize);
        let mut last: Option<u64> = None;
        let mut taken = 0usize;
        Store::walk_from(
            store,
            "rows",
            &start.to_be_bytes(),
            way,
            hint,
            &mut |k, v| {
                let key = u64::from_be_bytes(k.try_into().expect("8 byte key"));
                match way {
                    Direction::Asc => assert!(
                        key >= start,
                        "seed {seed}: asc walk from {start} gave {key}"
                    ),
                    Direction::Desc => assert!(
                        key <= start,
                        "seed {seed}: desc walk from {start} gave {key}"
                    ),
                }
                if let Some(prev) = last {
                    match way {
                        Direction::Asc => {
                            assert!(key > prev, "seed {seed}: asc walk went {prev} then {key}")
                        }
                        Direction::Desc => {
                            assert!(key < prev, "seed {seed}: desc walk went {prev} then {key}")
                        }
                    }
                }
                last = Some(key);
                let version = check_value(seed, key, v);
                assert!(
                    attempted[key as usize].lock().unwrap().contains(&version),
                    "seed {seed}: key {key} served version {version} nobody wrote"
                );
                taken += 1;
                taken < want
            },
        )
        .expect("walk");
        walked += taken as u64;
    }
    walked
}

fn run(seed: u64) {
    let mut rng = SmallRng::seed_from_u64(seed);
    let dir = TempDir::new().expect("tempdir");
    let store = Arc::new(
        ReelStore::open(dir.path().to_path_buf(), config(&mut rng), COLUMNS).expect("open"),
    );
    let attempted: Attempted = Arc::new((0..KEYS).map(|_| Mutex::new(BTreeSet::new())).collect());
    let ops = knob("REEL_MW_OPS", 2500);
    let writers = rng.gen_range(1..=3u64);
    let walkers = rng.gen_range(2..=4u64);
    let done = Arc::new(AtomicBool::new(false));

    let walked = thread::scope(|s| {
        let maintainer = {
            let store = store.clone();
            let done = done.clone();
            s.spawn(move || {
                while !done.load(Ordering::Relaxed) {
                    Store::maintain(&*store).expect("maintain");
                    thread::yield_now();
                }
            })
        };
        let walking: Vec<_> = (0..walkers)
            .map(|id| {
                let (store, attempted, done) = (store.clone(), attempted.clone(), done.clone());
                s.spawn(move || walker(&store, &attempted, seed, id, &done))
            })
            .collect();
        let writing: Vec<_> = (0..writers)
            .map(|id| {
                let (store, attempted) = (store.clone(), attempted.clone());
                s.spawn(move || writer(&store, &attempted, seed, id, ops))
            })
            .collect();
        for w in writing {
            w.join().expect("writer");
        }
        done.store(true, Ordering::Relaxed);
        maintainer.join().expect("maintainer");
        walking
            .into_iter()
            .map(|w| w.join().expect("walker"))
            .sum::<u64>()
    });
    assert!(walked > 0, "seed {seed}: walkers took nothing");

    // Quiet now: a full walk up, a full walk down and a point read of every key agree.
    let mut up = BTreeMap::new();
    Store::walk_from(
        &*store,
        "rows",
        &0u64.to_be_bytes(),
        Direction::Asc,
        128,
        &mut |k, v| {
            let key = u64::from_be_bytes(k.try_into().unwrap());
            up.insert(key, check_value(seed, key, v));
            true
        },
    )
    .expect("walk up");
    let mut down = BTreeMap::new();
    Store::walk_from(
        &*store,
        "rows",
        &(KEYS - 1).to_be_bytes(),
        Direction::Desc,
        128,
        &mut |k, v| {
            let key = u64::from_be_bytes(k.try_into().unwrap());
            down.insert(key, check_value(seed, key, v));
            true
        },
    )
    .expect("walk down");
    if up != down {
        let keys: BTreeSet<u64> = up.keys().chain(down.keys()).copied().collect();
        let diff: Vec<_> = keys
            .into_iter()
            .filter(|k| up.get(k) != down.get(k))
            .map(|k| (k, up.get(&k).copied(), down.get(&k).copied()))
            .take(12)
            .collect();
        panic!("seed {seed}: the walks up ({}) and down ({}) disagree, first (key, up, down): {diff:?}", up.len(), down.len());
    }
    for key in 0..KEYS {
        let got = Store::get(&*store, "rows", &key.to_be_bytes()).expect("get");
        let point = got.map(|v| check_value(seed, key, &v));
        assert_eq!(
            point,
            up.get(&key).copied(),
            "seed {seed}: key {key} walk and point read disagree"
        );
    }
}

#[test]
fn mapped_walks_hold_under_writes_and_compaction() {
    if let Some(seed) = std::env::var("REEL_MW_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
    {
        run(seed);
        return;
    }
    let first = knob("REEL_MW_FIRST", 1);
    for seed in first..first + knob("REEL_MW_SEEDS", 6) {
        run(seed);
    }
}
