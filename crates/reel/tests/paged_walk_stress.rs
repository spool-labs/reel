//! Walks of a paged column under writers, deletes, seals, handovers and compaction, drawn from one seed
//!
//! Each seed runs a tree column, whose walks merge the map with the footers, and an
//! ordered column, whose walks merge the map with FastForward. The column holds three
//! kinds of keys. Stable keys are written once before any
//! thread starts and never touched again. Live keys are written before the threads start
//! too, and the writers overwrite them but never delete them. Churned keys belong to one
//! writer each, which overwrites and deletes them while walks run. A key that is live for
//! the whole of a walk has to come back from it, whatever writes, seals, handovers and
//! compaction do under the walk. So a walk must come back in order with no key twice,
//! every stable and live key in its reach present, stable keys with their one value, and
//! every value starting with the number of the key it came back under. Once the writers stop, and again after a
//! reopen, a whole walk matches what the writers left.
//!
//! Knobs: REEL_PWS_SEEDS (how many seeds, default 3), REEL_PWS_FIRST (the first seed,
//! default 1), REEL_PWS_OPS (ops per writer, default 3000) and REEL_PWS_SEED (one seed to
//! replay).

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

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

/// Keys written once and never touched again
const STABLE: u64 = 400;

/// Keys the writers overwrite and never delete, numbered after the stable ones
const LIVE: u64 = 400;

/// Keys the writers overwrite and delete, numbered after the live ones
const CHURNED: u64 = 1200;

/// Every key a run writes
const KEYS: u64 = STABLE + LIVE + CHURNED;

const WRITERS: u64 = 3;
const WALKERS: u64 = 3;

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

/// The key number a key holds in its tail
fn number_of(key: &[u8]) -> u64 {
    u64::from_be_bytes(key[8..16].try_into().expect("a sixteen byte key"))
}

/// A value that starts with its key's number and op, so a value served under the wrong key fails
fn value_of(n: u64, op: u64, len: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(16 + len);
    value.extend_from_slice(&n.to_be_bytes());
    value.extend_from_slice(&op.to_be_bytes());
    value.extend((0..len).map(|at| (n ^ op ^ at as u64) as u8));
    value
}

fn config(rng: &mut SmallRng) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(64 * 1024),
        alloc_chunk: ByteCount::from_bytes(16 * 1024),
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(rng.gen_range(1..=4)),
        compact_dead_ratio: 0.1,
        scrub_mbps: 0,
        index: IndexResidency::Paged,
        ..ReelConfig::default()
    }
}

/// The key range a walk crossed, as bounds on the stable and live keys
type Reach<'a> = (Bound<&'a [u8]>, Bound<&'a [u8]>);

/// One walk: where it started, which way, and how it was asked for
struct Walk<'a> {
    from: &'a [u8],
    down: bool,
    reached_end: bool,
    shape: &'static str,
}

/// Check one walk's rows against the keys it crossed that were live throughout, and the values it returned
fn check_walk(store: &ReelStore, case: &str, rows: &[(Vec<u8>, Vec<u8>)], stable: &BTreeMap<Vec<u8>, Vec<u8>>, live: &BTreeSet<Vec<u8>>, walk: &Walk<'_>) {
    let Walk { from, down, reached_end, shape } = *walk;
    for pair in rows.windows(2) {
        let in_order = match down {
            true => pair[0].0 > pair[1].0,
            false => pair[0].0 < pair[1].0,
        };
        assert!(in_order, "{case}: a walk came back out of order or with a key twice");
    }
    for (key, value) in rows {
        let n = number_of(key);
        assert!(n < KEYS, "{case}: a walk returned a key nobody wrote");
        assert_eq!(&value[..8], &n.to_be_bytes(), "{case}: key {n} came back with another key's value");
        if let Some(stable_value) = stable.get(key) {
            assert_eq!(value, stable_value, "{case}: stable key {n} came back with the wrong value");
        }
    }
    // Every stable and live key between the walk's start and the last key it returned is in it.
    use std::ops::Bound::{Included, Unbounded};
    let reach: Option<Reach<'_>> = match (down, rows.last()) {
        (false, Some((last, _))) => Some((Included(from), Included(last.as_slice()))),
        (true, Some((last, _))) => Some((Included(last.as_slice()), Included(from))),
        (false, None) if reached_end => Some((Included(from), Unbounded)),
        (true, None) if reached_end => Some((Unbounded, Included(from))),
        (_, None) => None,
    };
    let crossed: Vec<&Vec<u8>> = match reach {
        Some(reach) => stable
            .range::<[u8], _>(reach)
            .map(|(key, _)| key)
            .chain(live.range::<[u8], _>(reach))
            .collect(),
        None => Vec::new(),
    };
    let returned: BTreeSet<&Vec<u8>> = rows.iter().map(|(key, _)| key).collect();
    for key in crossed {
        if !returned.contains(key) {
            let held = Store::get(store, "rows", key).expect("get").is_some();
            panic!(
                "{case}: {shape} {} from {} skipped key {} after {} rows, and a get finds it now: {held}",
                if down { "down" } else { "up" },
                number_of(from),
                number_of(key),
                rows.len()
            );
        }
    }
}

fn walker(store: &ReelStore, stable: &BTreeMap<Vec<u8>, Vec<u8>>, live: &BTreeSet<Vec<u8>>, case: &str, seed: u64, id: u64, done: &AtomicBool) -> u64 {
    let mut rng = SmallRng::seed_from_u64(seed ^ (id << 40) ^ 0x57A1);
    let mut walks = 0;
    while !done.load(Ordering::Relaxed) {
        let from = key_of(rng.gen_range(0..KEYS));
        let down = rng.gen_bool(0.5);
        let direction = match down {
            true => Direction::Desc,
            false => Direction::Asc,
        };
        let wanted = [1usize, 5, 40, 400][rng.gen_range(0..4)];
        let mut rows = Vec::new();
        let mut reached_end = false;
        let shape = match rng.gen_range(0..3) {
            0 => {
                let mut walk = Store::iter_from(store, "rows", &from, direction).expect("iter from");
                while rows.len() < wanted {
                    match walk.next() {
                        Some((key, value)) => rows.push((key, value.to_vec())),
                        None => {
                            reached_end = true;
                            break;
                        }
                    }
                }
                "iter"
            }
            1 => {
                let mut walk = store.iter_lent("rows", Some(&from), direction, wanted).expect("lent");
                while rows.len() < wanted {
                    match walk.next() {
                        Some((key, value)) => rows.push((key.to_vec(), value.to_vec())),
                        None => {
                            reached_end = true;
                            break;
                        }
                    }
                }
                "lent walk"
            }
            _ => {
                // A key walk returns no values, so each key stands in with its own number.
                let mut walk = store.iter_keys_from("rows", Some(&from), direction).expect("keys");
                while rows.len() < wanted {
                    match walk.next() {
                        Some(key) => {
                            let n = number_of(&key);
                            let value = stable.get(&key).cloned().unwrap_or_else(|| n.to_be_bytes().to_vec());
                            rows.push((key, value));
                        }
                        None => {
                            reached_end = true;
                            break;
                        }
                    }
                }
                "key walk"
            }
        };
        let walk = Walk {
            from: &from,
            down,
            reached_end,
            shape,
        };
        check_walk(store, case, &rows, stable, live, &walk);
        walks += 1;
    }
    walks
}

fn writer(store: &ReelStore, seed: u64, id: u64, ops: u64, last: &Mutex<BTreeMap<Vec<u8>, Option<Vec<u8>>>>) {
    let mut rng = SmallRng::seed_from_u64(seed ^ (id << 32) ^ 0xB0B);
    let owned: Vec<u64> = (STABLE..KEYS).filter(|n| n % WRITERS == id).collect();
    for op in 1..=ops {
        let n = owned[rng.gen_range(0..owned.len())];
        let key = key_of(n);
        let is_live = n < STABLE + LIVE;
        match rng.gen_range(0..100u32) {
            roll if roll < 75 || is_live => {
                let value = value_of(n, op, rng.gen_range(0..300));
                Store::put(store, "rows", &key, &value).expect("put");
                last.lock().expect("last").insert(key, Some(value));
            }
            _ => {
                Store::delete(store, "rows", &key).expect("delete");
                last.lock().expect("last").insert(key, None);
            }
        }
    }
}

/// A whole walk against the stable keys and what the writers left
fn settled(store: &ReelStore, stable: &BTreeMap<Vec<u8>, Vec<u8>>, last: &BTreeMap<Vec<u8>, Option<Vec<u8>>>, case: &str, stage: &str) {
    let mut want: BTreeMap<Vec<u8>, Vec<u8>> = stable.clone();
    for (key, value) in last {
        if let Some(value) = value {
            want.insert(key.clone(), value.clone());
        }
    }
    let got: Vec<(Vec<u8>, Vec<u8>)> = Store::iter(store, "rows")
        .expect("iter")
        .map(|(key, value)| (key, value.to_vec()))
        .collect();
    let want: Vec<(Vec<u8>, Vec<u8>)> = want.into_iter().collect();
    assert_eq!(got.len(), want.len(), "{case} {stage}: a whole walk counts {} keys against {}", got.len(), want.len());
    assert_eq!(got, want, "{case} {stage}: a whole walk differs from what the writers left");
}

fn run(seed: u64, columns: ColumnSet) {
    let case = format!("seed {seed} {:?}", columns[0].map_shape);
    let case = case.as_str();
    let mut rng = SmallRng::seed_from_u64(seed);
    let dir = TempDir::new().expect("temp dir");
    let config = config(&mut rng);
    let store = Arc::new(ReelStore::open(dir.path().to_path_buf(), config.clone(), columns).expect("open"));
    let ops = knob("REEL_PWS_OPS", 3000);

    let mut stable = BTreeMap::new();
    let mut live = BTreeSet::new();
    let mut last = BTreeMap::new();
    for n in 0..STABLE + LIVE {
        let value = value_of(n, 0, rng.gen_range(0..300));
        Store::put(&*store, "rows", &key_of(n), &value).expect("put");
        match n < STABLE {
            true => {
                stable.insert(key_of(n), value);
            }
            false => {
                live.insert(key_of(n));
                last.insert(key_of(n), Some(value));
            }
        }
    }
    let (stable, live) = (Arc::new(stable), Arc::new(live));
    let last = Arc::new(Mutex::new(last));
    let done = Arc::new(AtomicBool::new(false));
    let walks = Arc::new(AtomicU64::new(0));

    thread::scope(|scope| {
        let maintainer = {
            let (store, done) = (store.clone(), done.clone());
            scope.spawn(move || {
                while !done.load(Ordering::Relaxed) {
                    Store::maintain(&*store).expect("maintain");
                }
            })
        };
        let walkers: Vec<_> = (0..WALKERS)
            .map(|id| {
                let (store, stable, live, done, walks) = (store.clone(), stable.clone(), live.clone(), done.clone(), walks.clone());
                scope.spawn(move || {
                    walks.fetch_add(walker(&store, &stable, &live, case, seed, id, &done), Ordering::Relaxed);
                })
            })
            .collect();
        let writers: Vec<_> = (0..WRITERS)
            .map(|id| {
                let (store, last) = (store.clone(), last.clone());
                scope.spawn(move || writer(&store, seed, id, ops, &last))
            })
            .collect();
        for writer in writers {
            writer.join().expect("writer");
        }
        done.store(true, Ordering::Relaxed);
        for walker in walkers {
            walker.join().expect("walker");
        }
        maintainer.join().expect("maintainer");
    });

    let (pages, fallbacks) = store.index().ordered_walks();
    let last = last.lock().expect("last").clone();
    println!(
        "{case}: {} walks, {pages} ordered pages, {fallbacks} sent to the footers",
        walks.load(Ordering::Relaxed)
    );
    if columns[0].map_shape == MapShape::Ordered {
        assert!(pages > 0, "{case}: no walk read the ordered index");
    }
    Store::maintain(&*store).expect("maintain");
    settled(&store, &stable, &last, case, "after the writers");

    let store = Arc::try_unwrap(store).ok().expect("one owner");
    store.close().expect("close");
    drop(store);
    let reopened = ReelStore::open(dir.path().to_path_buf(), config, columns).expect("reopen");
    settled(&reopened, &stable, &last, case, "after a reopen");
}

fn seeds(columns: ColumnSet) {
    if let Ok(seed) = std::env::var("REEL_PWS_SEED") {
        run(seed.parse().expect("REEL_PWS_SEED is a number"), columns);
        return;
    }
    let first = knob("REEL_PWS_FIRST", 1);
    for seed in first..first + knob("REEL_PWS_SEEDS", 3) {
        run(seed, columns);
    }
}

#[test]
fn tree_walks_hold_under_writes_and_compaction() {
    seeds(TREE);
}

#[test]
fn ordered_walks_hold_under_writes_and_compaction() {
    seeds(ORDERED);
}
