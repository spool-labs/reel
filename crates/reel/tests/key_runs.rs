//! Key runs answer every walk and get as the footers they merged would
//!
//! Rounds of fresh keys, overwrites and deletes seal segment after segment, and the
//! maintenance tick merges the walk's runs into key runs whenever too many stand over one
//! key, moving no record. After every round each key answers its model value alone and
//! in walks both ways, and so does every key after a reopen reads the runs back.

use std::collections::BTreeMap;

use tempfile::TempDir;

use reel::config::{CompactRate, IndexResidency, ReelConfig, SyncPolicy, ThreadBudget};
use reel::format::column::{Codec, ColumnId, ColumnSet, ColumnSpec};
use reel::units::ByteCount;
use reel::{KeyWidth, Preallocate, ReelStore};
use reel_core::{Direction, Store};

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: "rows",
    key_width: KeyWidth::Fixed(16),
    shard_bytes: 0,
    purge_mark: None,
    codec: Codec::None,
}];

/// Fresh keys each round adds, a few segments' worth
const PER_ROUND: u64 = 500;

/// Rounds, enough runs that the merge goes several times and merges its own runs
const ROUNDS: u64 = 14;

/// Ticks a round drives maintenance for, past what its merges take
const TICKS: usize = 10;

/// Runs one key may fall inside once maintenance has caught up
const SETTLED_DEPTH: usize = 8;

fn config(tails: u32) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(96 * 1024),
        alloc_chunk: ByteCount::from_bytes(32 * 1024),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(tails),
        index: IndexResidency::Paged,
        key_runs: true,
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

fn key_of(n: u64) -> Vec<u8> {
    let mut key = mix(n).to_be_bytes().to_vec();
    key.extend_from_slice(&mix(n ^ 0xABCD).to_be_bytes());
    key
}

fn value_of(n: u64, round: u64) -> Vec<u8> {
    let mut value = (n ^ (round << 40)).to_be_bytes().to_vec();
    value.resize(100 + (n % 200) as usize, (n % 251) as u8);
    value
}

fn check(store: &ReelStore, model: &BTreeMap<Vec<u8>, Vec<u8>>, stage: &str) {
    for (key, want) in model {
        let got = Store::get(store, "rows", key).expect("get").map(|value| value.to_vec());
        assert_eq!(got.as_ref(), Some(want), "{stage}: a key lost its value");
    }
    let up: Vec<(Vec<u8>, Vec<u8>)> = Store::iter(store, "rows").expect("iter").map(|(key, value)| (key, value.to_vec())).collect();
    let want: Vec<(Vec<u8>, Vec<u8>)> = model.iter().map(|(key, value)| (key.clone(), value.clone())).collect();
    assert_eq!(up.len(), want.len(), "{stage}: an ascending walk came back with the wrong count");
    assert!(up == want, "{stage}: an ascending walk came back out of step with the model");
    let down: Vec<(Vec<u8>, Vec<u8>)> = Store::iter_from(store, "rows", &[0xFF; 16], Direction::Desc)
        .expect("iter down")
        .map(|(key, value)| (key, value.to_vec()))
        .collect();
    let mut want_down = want.clone();
    want_down.reverse();
    assert!(down == want_down, "{stage}: a descending walk came back out of step with the model");
    // a walk from the middle lands where the model says
    if let Some((middle, _)) = want.get(want.len() / 2) {
        let from: Vec<Vec<u8>> = Store::iter_from(store, "rows", middle, Direction::Asc)
            .expect("iter from")
            .take(5)
            .map(|(key, _)| key)
            .collect();
        let expect: Vec<Vec<u8>> = model.range(middle.clone()..).take(5).map(|(key, _)| key.clone()).collect();
        assert_eq!(from, expect, "{stage}: a walk from the middle started in the wrong place");
    }
}

fn rounds(tails: u32) {
    let dir = TempDir::new().expect("temp dir");
    let store = ReelStore::open(dir.path().to_path_buf(), config(tails), COLUMNS).expect("open");
    let mut model = BTreeMap::new();
    for round in 0..ROUNDS {
        for n in round * PER_ROUND..(round + 1) * PER_ROUND {
            Store::put(&store, "rows", &key_of(n), &value_of(n, round)).expect("put");
            model.insert(key_of(n), value_of(n, round));
        }
        // every third round overwrites a slice of older keys and deletes another
        if round % 3 == 2 {
            for n in (0..round * PER_ROUND).step_by(7) {
                Store::put(&store, "rows", &key_of(n), &value_of(n, round)).expect("overwrite");
                model.insert(key_of(n), value_of(n, round));
            }
            for n in (3..round * PER_ROUND).step_by(11) {
                Store::delete(&store, "rows", &key_of(n)).expect("delete");
                model.remove(&key_of(n));
            }
        }
        store.flush().expect("flush");
        for _ in 0..TICKS {
            Store::maintain(&store).expect("maintain");
        }
        assert!(
            store.index().overlap_depth() <= SETTLED_DEPTH,
            "round {round}: {} runs stand over one key after maintenance",
            store.index().overlap_depth()
        );
        check(&store, &model, &format!("round {round}"));
    }
    assert!(!store.index().key_runs().runs().is_empty(), "no key run was ever written");
    store.close().expect("close");
    drop(store);
    let reopened = ReelStore::open(dir.path().to_path_buf(), config(tails), COLUMNS).expect("reopen");
    assert!(!reopened.index().key_runs().runs().is_empty(), "the reopen read no key run back");
    check(&reopened, &model, "after a reopen");
}

// one tail's runs merge into key runs and every key answers through them
#[test]
fn key_runs_answer_as_the_model_on_one_tail() {
    rounds(1);
}

// several tails seal side by side, and the merges still leave every key where it was
#[test]
fn key_runs_answer_as_the_model_on_four_tails() {
    rounds(4);
}
