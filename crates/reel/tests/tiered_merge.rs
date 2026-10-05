//! A volume taking only fresh keys keeps its sorted runs from piling up over one key
//!
//! Every sealed run of scattered keys reaches over the whole column, so with nothing dead
//! to reclaim the runs would stack one a seal and a walk would seek in every one. The
//! maintenance tick merges the youngest runs among themselves once too many stand over
//! the same keys, and every key still answers, alone and in a walk, across a reopen.

use std::collections::BTreeMap;

use tempfile::TempDir;

use reel::config::{CompactRate, IndexResidency, ReelConfig, SyncPolicy, ThreadBudget};
use reel::format::column::{Codec, ColumnId, ColumnSet, ColumnSpec};
use reel::units::ByteCount;
use reel::{KeyWidth, Preallocate, ReelStore};
use reel_core::Store;

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: "rows",
    key_width: KeyWidth::Fixed(32),
    shard_bytes: 0,
    purge_mark: None,
    codec: Codec::None,
}];

/// Fresh keys each round adds, a few segments' worth
const PER_ROUND: u64 = 600;

/// Rounds, enough runs that the stack would pass the merge depth several times over
const ROUNDS: u64 = 16;

/// Runs one key may fall inside once maintenance has caught up, the volume's merge depth
const SETTLED_DEPTH: usize = 8;

/// Ticks a round drives maintenance for, past what its merges take
const TICKS: usize = 12;

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(128 * 1024),
        alloc_chunk: ByteCount::from_bytes(32 * 1024),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        index: IndexResidency::Paged,
        rewrite_on_seal: true,
        merge_sorted_runs: true,
        compact_dead_ratio: 1.0,
        compact_mbps: CompactRate::Mbps(100_000),
        ..ReelConfig::default()
    }
}

fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

/// Key number `n`, spread so every run's range covers the whole column
fn key_of(n: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(32);
    for word in 0..4 {
        key.extend_from_slice(&mix(n * 4 + word).to_be_bytes());
    }
    key
}

fn value_of(n: u64) -> Vec<u8> {
    let mut value = n.to_be_bytes().to_vec();
    value.resize(150 + (n % 300) as usize, (n % 251) as u8);
    value
}

fn check(store: &ReelStore, model: &BTreeMap<Vec<u8>, Vec<u8>>, stage: &str) {
    for (key, want) in model {
        let got = Store::get(store, "rows", key).expect("get").map(|value| value.to_vec());
        assert_eq!(got.as_ref(), Some(want), "{stage}: a key lost its value");
    }
    let walked: Vec<(Vec<u8>, Vec<u8>)> = Store::iter(store, "rows")
        .expect("iter")
        .map(|(key, value)| (key, value.to_vec()))
        .collect();
    let want: Vec<(Vec<u8>, Vec<u8>)> = model.iter().map(|(key, value)| (key.clone(), value.clone())).collect();
    assert_eq!(walked.len(), want.len(), "{stage}: a walk came back with the wrong count");
    assert!(walked == want, "{stage}: a walk came back out of step with the model");
}

// fresh keys alone keep the runs over one key within the merge depth, and every key answers
#[test]
fn fresh_keys_keep_the_runs_over_a_key_within_the_depth() {
    let dir = TempDir::new().expect("temp dir");
    let store = ReelStore::open(dir.path().to_path_buf(), config(), COLUMNS).expect("open");
    let mut model = BTreeMap::new();
    let mut deepest = 0;
    for round in 0..ROUNDS {
        for n in round * PER_ROUND..(round + 1) * PER_ROUND {
            Store::put(&store, "rows", &key_of(n), &value_of(n)).expect("put");
            model.insert(key_of(n), value_of(n));
        }
        store.flush().expect("flush");
        for _ in 0..TICKS {
            Store::maintain(&store).expect("maintain");
        }
        deepest = deepest.max(store.index().overlap_depth());
        assert!(
            store.index().overlap_depth() <= SETTLED_DEPTH,
            "round {round}: {} runs stand over one key after maintenance",
            store.index().overlap_depth()
        );
    }
    let merged = store.compaction_counters().runs_merged;
    assert!(merged > 0, "no tier merged, so the depth never passed what it was bounded to");
    check(&store, &model, "after the rounds");
    println!("{merged} runs merged, the deepest key under {deepest} runs");

    store.close().expect("close");
    drop(store);
    let reopened = ReelStore::open(dir.path().to_path_buf(), config(), COLUMNS).expect("reopen");
    check(&reopened, &model, "after a reopen");
}
