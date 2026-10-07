//! A paged column's counters answer as a model does, through writes, deletes, range deletes, seals, compaction and reopens
//! Knobs: REEL_PTM_SEEDS (default 6), REEL_PTM_FIRST (default 1), REEL_PTM_OPS (ops per seed, default 3000), REEL_PTM_SEED (replays one seed)

use std::collections::BTreeMap;
use std::ops::Bound;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, ReelConfig, ReelStore, SyncPolicy,
    ThreadBudget,
};
use reel_core::Store;

const ROWS: ColumnId = ColumnId(1);

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ROWS,
    name: "rows",
    key_width: KeyWidth::Fixed(16),
    shard_bytes: 2,
    purge_mark: None,
    codec: Codec::None,
}];

/// A run writes and deletes this many distinct keys
const KEYS: u64 = 2000;

/// Keys lead with one of this many two-byte prefixes, a shard each
const SPOOLS: u64 = 37;

/// What the column should hold, key to value length
type Model = BTreeMap<Vec<u8>, u64>;

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

fn prefix_of(spool: u64) -> [u8; 2] {
    (spool as u16).to_be_bytes()
}

/// Key number `n`: its spool's prefix, then bytes spread like a hash
fn key_of(n: u64) -> Vec<u8> {
    let mut key = prefix_of(n % SPOOLS).to_vec();
    key.extend_from_slice(&mix(n).to_be_bytes()[..6]);
    key.extend_from_slice(&n.to_be_bytes());
    key
}

fn config(rng: &mut SmallRng) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(64 * 1024),
        alloc_chunk: ByteCount::from_bytes(16 * 1024),
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(rng.gen_range(1..=3)),
        compact_dead_ratio: 0.1,
        scrub_mbps: 0,
        ..ReelConfig::default()
    }
}

/// The model's count and payload bytes under a prefix
fn modelled(model: &Model, prefix: &[u8]) -> (u64, u64) {
    model
        .range::<[u8], _>((Bound::Included(prefix), Bound::Unbounded))
        .take_while(|(key, _)| key.starts_with(prefix))
        .fold((0, 0), |(count, bytes), (_, len)| (count + 1, bytes + len))
}

/// Checks the column's totals, while no cover is owed its sweep
fn check_totals(store: &ReelStore, model: &Model, context: &str) {
    if !store.counters_agree(ROWS) {
        return;
    }
    let slack = store.spot_slack();
    let (count, bytes) = modelled(model, &[]);
    let totals = store.column_totals(ROWS).expect("column totals");
    assert_eq!(totals.count, count, "{context}: column count");
    let off = totals.bytes.to_bytes().abs_diff(bytes);
    assert!(
        off <= slack,
        "{context}: column bytes {off} off, slack {slack}"
    );
    assert_eq!(store.totals(), totals, "{context}: store totals");
}

/// Checks every spool's prefix totals and the whole column, through the engine and the store trait
fn check(store: &ReelStore, model: &Model, context: &str) {
    assert!(
        store.counters_agree(ROWS),
        "{context}: a cover outlived the tick"
    );
    check_totals(store, model, context);
    let slack = store.spot_slack();
    for spool in 0..SPOOLS + 2 {
        let prefix = prefix_of(spool);
        let (count, bytes) = modelled(model, &prefix);
        let totals = store
            .prefix_totals(ROWS, &prefix)
            .expect("a shard-aligned prefix answers");
        assert_eq!(totals.count, count, "{context}: spool {spool} count");
        let off = totals.bytes.to_bytes().abs_diff(bytes);
        assert!(
            off <= slack,
            "{context}: spool {spool} bytes {off} off, slack {slack}"
        );
        let counted = Store::count_prefix(store, "rows", &prefix).expect("count prefix");
        assert_eq!(
            counted, count,
            "{context}: spool {spool} counted through the trait"
        );
    }
    let whole = Store::count_prefix(store, "rows", &[]).expect("count prefix");
    assert_eq!(
        whole,
        model.len() as u64,
        "{context}: whole column through the trait"
    );
}

fn run(seed: u64) {
    let mut rng = SmallRng::seed_from_u64(seed);
    let dir = TempDir::new().expect("temp dir");
    let config = config(&mut rng);
    let mut store =
        ReelStore::open(dir.path().to_path_buf(), config.clone(), COLUMNS).expect("open");
    let mut model = Model::new();
    let ops = knob("REEL_PTM_OPS", 3000);

    for op in 1..=ops {
        let n = rng.gen_range(0..KEYS);
        match rng.gen_range(0..100u32) {
            0..70 => {
                let len = rng.gen_range(0..1200u64);
                let value = vec![(n ^ op) as u8; len as usize];
                Store::put(&store, "rows", &key_of(n), &value).expect("put");
                model.insert(key_of(n), len);
            }
            70..96 => {
                Store::delete(&store, "rows", &key_of(n)).expect("delete");
                model.remove(&key_of(n));
            }
            // A narrow range inside one spool, or a whole spool as a node drops one
            roll => {
                let low = key_of(n);
                let high = match roll {
                    96..99 => {
                        let mut high = low.clone();
                        high[2] = high[2].saturating_add(16);
                        high
                    }
                    _ => {
                        let mut high = low[..2].to_vec();
                        high[1] = high[1].saturating_add(1);
                        high.resize(16, 0);
                        high
                    }
                };
                let low = match roll {
                    96..99 => low,
                    _ => [&low[..2], &[0u8; 14][..]].concat(),
                };
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
        check_totals(&store, &model, &format!("seed {seed} op {op}"));
        if op % 150 == 0 {
            Store::maintain(&store).expect("maintain");
            check(&store, &model, &format!("seed {seed} tick at op {op}"));
        }
        if op % 1000 == 0 {
            store.close().expect("close");
            drop(store);
            store =
                ReelStore::open(dir.path().to_path_buf(), config.clone(), COLUMNS).expect("reopen");
            check(&store, &model, &format!("seed {seed} reopen at op {op}"));
        }
    }

    for _ in 0..4 {
        Store::maintain(&store).expect("maintain");
    }
    check(&store, &model, &format!("seed {seed} settled"));
    println!("seed {seed}: {} keys held", model.len());

    store.close().expect("close");
    drop(store);
    let reopened = ReelStore::open(dir.path().to_path_buf(), config, COLUMNS).expect("reopen");
    check(&reopened, &model, &format!("seed {seed} final reopen"));
}

// a paged column's whole and per-prefix totals answer as the model does, a reopen included
#[test]
fn paged_totals_answer_as_the_model() {
    if let Ok(seed) = std::env::var("REEL_PTM_SEED") {
        run(seed.parse().expect("REEL_PTM_SEED is a number"));
        return;
    }
    let first = knob("REEL_PTM_FIRST", 1);
    for seed in first..first + knob("REEL_PTM_SEEDS", 6) {
        run(seed);
    }
}
