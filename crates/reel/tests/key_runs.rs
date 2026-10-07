//! Key runs answer every walk and get as the footers they merged would

use std::collections::BTreeMap;

use tempfile::TempDir;

use reel::config::{CompactRate, ReelConfig, SyncPolicy, ThreadBudget};
use reel::format::column::{Codec, ColumnId, ColumnSet, ColumnSpec};
use reel::units::ByteCount;
use reel::{CompactPass, KeyWidth, Preallocate, ReelStore};
use reel_core::{Direction, Store};

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: "rows",
    key_width: KeyWidth::Fixed(16),
    shard_bytes: 0,
    purge_mark: None,
    codec: Codec::None,
}];

/// The same column with keys of any width
const VARYING: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: "rows",
    key_width: KeyWidth::Variable,
    shard_bytes: 0,
    purge_mark: None,
    codec: Codec::None,
}];

/// Each round adds this many fresh keys, a few segments' worth
const PER_ROUND: u64 = 500;

/// Rounds, enough runs that the merge goes several times and merges its own runs
const ROUNDS: u64 = 14;

/// Each round drives maintenance for this many ticks, more than its merges take
const TICKS: usize = 10;

/// One key may fall inside this many runs once maintenance has caught up
const SETTLED_DEPTH: usize = 8;

/// Dead share at which a segment is rewritten, never for the runs alone
const NEVER: f64 = 1.0;

/// Dead share at which the overwrites and deletes below have a segment rewritten
const RECLAIM: f64 = 0.1;

fn config(tails: u32, dead_ratio: f64) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(96 * 1024),
        alloc_chunk: ByteCount::from_bytes(32 * 1024),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(tails),
        compact_dead_ratio: dead_ratio,
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

/// A key of eight to sixteen bytes, its width from its number
fn varying_key(n: u64) -> Vec<u8> {
    let mut key = key_of(n);
    key.truncate(8 + (n % 9) as usize);
    key
}

fn value_of(n: u64, round: u64) -> Vec<u8> {
    let mut value = (n ^ (round << 40)).to_be_bytes().to_vec();
    value.resize(100 + (n % 200) as usize, (n % 251) as u8);
    value
}

fn check(store: &ReelStore, model: &BTreeMap<Vec<u8>, Vec<u8>>, stage: &str) {
    for (key, want) in model {
        let got = Store::get(store, "rows", key)
            .expect("get")
            .map(|value| value.to_vec());
        assert_eq!(got.as_ref(), Some(want), "{stage}: a key lost its value");
    }
    let up: Vec<(Vec<u8>, Vec<u8>)> = Store::iter(store, "rows")
        .expect("iter")
        .map(|(key, value)| (key, value.to_vec()))
        .collect();
    let want: Vec<(Vec<u8>, Vec<u8>)> = model
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    assert_eq!(
        up.len(),
        want.len(),
        "{stage}: an ascending walk came back with the wrong count"
    );
    assert!(
        up == want,
        "{stage}: an ascending walk came back out of step with the model"
    );
    // A keys-only walk reads no record, so a run's row alone decides whether a key shows
    let keys = Store::iter_keys_prefix(store, "rows", &[]).expect("keys");
    let want_keys: Vec<Vec<u8>> = model.keys().cloned().collect();
    assert_eq!(
        keys.len(),
        want_keys.len(),
        "{stage}: a keys-only walk came back with the wrong count"
    );
    assert!(
        keys == want_keys,
        "{stage}: a keys-only walk came back out of step with the model"
    );
    let down: Vec<(Vec<u8>, Vec<u8>)> =
        Store::iter_from(store, "rows", &[0xFF; 16], Direction::Desc)
            .expect("iter down")
            .map(|(key, value)| (key, value.to_vec()))
            .collect();
    let mut want_down = want.clone();
    want_down.reverse();
    assert!(
        down == want_down,
        "{stage}: a descending walk came back out of step with the model"
    );
    if let Some((middle, _)) = want.get(want.len() / 2) {
        let from: Vec<Vec<u8>> = Store::iter_from(store, "rows", middle, Direction::Asc)
            .expect("iter from")
            .take(5)
            .map(|(key, _)| key)
            .collect();
        let expect: Vec<Vec<u8>> = model
            .range(middle.clone()..)
            .take(5)
            .map(|(key, _)| key.clone())
            .collect();
        assert_eq!(
            from, expect,
            "{stage}: a walk from the middle started in the wrong place"
        );
    }
}

fn rounds(tails: u32, dead_ratio: f64) {
    rounds_of(tails, dead_ratio, COLUMNS, key_of);
}

fn rounds_of(tails: u32, dead_ratio: f64, columns: ColumnSet, key_of: fn(u64) -> Vec<u8>) {
    let dir = TempDir::new().expect("temp dir");
    let store = ReelStore::open(dir.path().to_path_buf(), config(tails, dead_ratio), columns)
        .expect("open");
    let mut model = BTreeMap::new();
    for round in 0..ROUNDS {
        for n in round * PER_ROUND..(round + 1) * PER_ROUND {
            Store::put(&store, "rows", &key_of(n), &value_of(n, round)).expect("put");
            model.insert(key_of(n), value_of(n, round));
        }
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
    assert!(
        !store.index().key_runs().runs().is_empty(),
        "no key run was ever written"
    );
    if dead_ratio < NEVER {
        let counters = store.compaction_counters();
        assert!(
            counters.segments_rewritten + counters.segments_unlinked_whole > 0,
            "no segment was ever rewritten, so the runs never lost a segment under them"
        );
    }
    store.close().expect("close");
    drop(store);
    let reopened = ReelStore::open(dir.path().to_path_buf(), config(tails, dead_ratio), columns)
        .expect("reopen");
    assert!(
        !reopened.index().key_runs().runs().is_empty(),
        "the reopen read no key run back"
    );
    check(&reopened, &model, "after a reopen");
}

// one tail's runs merge into key runs and every key answers through them
#[test]
fn key_runs_answer_as_the_model_on_one_tail() {
    rounds(1, NEVER);
}

// several tails seal side by side, and the merges still leave every key where it was
#[test]
fn key_runs_answer_as_the_model_on_four_tails() {
    rounds(4, NEVER);
}

// rewrites move records out of covered segments, and the runs over them keep answering
#[test]
fn key_runs_answer_as_the_model_while_rewrites_reclaim_their_segments() {
    rounds(1, RECLAIM);
}

// the same with four tails, where a rewrite's copy can land in a lower-numbered segment
#[test]
fn key_runs_answer_as_the_model_while_four_tails_reclaim() {
    rounds(4, RECLAIM);
}

// keys of eight to sixteen bytes merge into key runs whose rows each say their width
#[test]
fn key_runs_of_varying_keys_answer_as_the_model() {
    rounds_of(1, RECLAIM, VARYING, varying_key);
}

// the same on four tails
#[test]
fn key_runs_of_varying_keys_answer_as_the_model_on_four_tails() {
    rounds_of(4, RECLAIM, VARYING, varying_key);
}

/// The scenarios below rewrite a segment at this dead share, so half-dead ones go
const HALF_DEAD: f64 = 0.3;

/// Whether an older run points a key at a retired segment and a newer run at a standing one
fn is_stale_under_fresh(store: &ReelStore, key: &[u8]) -> bool {
    let index = store.index();
    let mut seen = Vec::new();
    for run in index.key_runs().runs() {
        let Some(column) = run.column(ColumnId(1)) else {
            continue;
        };
        let at = run.seek(column, key, false);
        if at >= column.rows() {
            continue;
        }
        if let Ok((found, row)) = reel::index::keyrun::row_in(run.rows(column), column, at as usize)
        {
            if found == key {
                seen.push(index.holds_sealed(row.loc.segment));
            }
        }
    }
    seen.windows(2).any(|pair| !pair[0] && pair[1])
}

/// Seal what the tails hold and run maintenance until its merges have caught up
fn settle(store: &ReelStore) {
    store.flush().expect("flush");
    for _ in 0..TICKS {
        Store::maintain(store).expect("maintain");
    }
}

/// Rewrite every segment the dead share takes, then seal and hand over the copies
fn compact_all(store: &ReelStore) {
    store.flush().expect("flush");
    for _ in 0..1_000 {
        if matches!(store.compact_once().expect("compact"), CompactPass::Idle) {
            break;
        }
    }
    store.flush().expect("flush");
    store.page_out_sealed().expect("hand over");
}

// a rewritten record answers through the newer run even when a stale run is read first
#[test]
fn a_rewritten_record_answers_through_the_newer_run() {
    let dir = TempDir::new().expect("temp dir");
    let store =
        ReelStore::open(dir.path().to_path_buf(), config(1, HALF_DEAD), COLUMNS).expect("open");
    let mut model = BTreeMap::new();
    let mut put = |store: &ReelStore, n: u64, round: u64| {
        Store::put(store, "rows", &key_of(n), &value_of(n, round)).expect("put");
        model.insert(key_of(n), value_of(n, round));
    };
    for n in 0..10_000 {
        put(&store, n, 0);
    }
    settle(&store);
    assert_eq!(
        store.index().key_runs().runs().len(),
        1,
        "the base keys did not merge into one run"
    );

    for n in (0..900).step_by(2) {
        put(&store, n, 1);
    }
    compact_all(&store);
    assert!(
        store.compaction_counters().segments_rewritten > 0,
        "no covered segment was rewritten"
    );

    // Enough fresh segments to merge, and few enough rows that the base run sits it out
    for n in 20_000..23_000 {
        put(&store, n, 2);
    }
    settle(&store);
    assert!(
        store.index().key_runs().runs().len() >= 2,
        "the copies never reached a run of their own"
    );
    assert!(
        (1..900)
            .step_by(2)
            .any(|n| is_stale_under_fresh(&store, &key_of(n))),
        "no key has a run pointing into its retired segment ahead of a run pointing at its copy"
    );
    check(&store, &model, "after the rewrite");

    store.close().expect("close");
    drop(store);
    let reopened =
        ReelStore::open(dir.path().to_path_buf(), config(1, HALF_DEAD), COLUMNS).expect("reopen");
    check(&reopened, &model, "after a reopen");
}

// rewriting a segment of deletes keeps each delete while a run still holds its key
#[test]
fn a_delete_stands_while_a_run_still_holds_its_key() {
    let dir = TempDir::new().expect("temp dir");
    let store =
        ReelStore::open(dir.path().to_path_buf(), config(1, HALF_DEAD), COLUMNS).expect("open");
    let mut model = BTreeMap::new();
    for n in 0..10_000 {
        Store::put(&store, "rows", &key_of(n), &value_of(n, 0)).expect("put");
    }
    settle(&store);
    assert_eq!(
        store.index().key_runs().runs().len(),
        1,
        "the base keys did not merge into one run"
    );

    // Fillers between the deletes, overwritten after, give the deletes' segments dead bytes
    let filler = |n: u64| key_of(50_000 + n);
    for n in 0..10_000 {
        Store::delete(&store, "rows", &key_of(n)).expect("delete");
        if n % 10 == 0 {
            Store::put(&store, "rows", &filler(n), &value_of(n, 1)).expect("filler");
        }
    }
    for n in (0..10_000).step_by(10) {
        Store::put(&store, "rows", &filler(n), &value_of(n, 2)).expect("filler again");
        model.insert(filler(n), value_of(n, 2));
    }
    compact_all(&store);
    // A pass that copies nothing live counts as an unlink whether or not it copied deletes
    assert!(
        store.compaction_counters().segments_unlinked_whole > 0,
        "no old segment retired"
    );
    check(&store, &model, "after the rewrite");

    store.close().expect("close");
    drop(store);
    let reopened =
        ReelStore::open(dir.path().to_path_buf(), config(1, HALF_DEAD), COLUMNS).expect("reopen");
    check(&reopened, &model, "after a reopen");
}
