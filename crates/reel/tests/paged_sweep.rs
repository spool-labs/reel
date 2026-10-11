//! A sweep hands out every live key, sealed or not

use std::collections::BTreeMap;

use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, ReelConfig, ReelStore, SyncPolicy,
};
use reel_core::Store;

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: "rows",
    key_width: KeyWidth::Fixed(16),
    shard_bytes: 0,
    purge_mark: None,
    codec: Codec::None,
}];

const KEYS: u64 = 2000;

/// Each sweep page asks for this many keys, so a sweep resumes from its mark many times
const PAGE: usize = 37;

/// Key number `n`, its first byte one of four groups so a prefix takes a quarter of them
fn key_of(n: u64) -> Vec<u8> {
    let mut key = vec![(n % 4) as u8];
    key.extend_from_slice(&n.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes()[..7]);
    key.extend_from_slice(&n.to_be_bytes());
    key
}

fn value_of(n: u64) -> Vec<u8> {
    let mut value = n.to_be_bytes().to_vec();
    value.resize(200, n as u8);
    value
}

/// A volume holding every key, most sealed and handed over, the last few still in the map
fn filled(dir: &TempDir) -> (ReelStore, BTreeMap<Vec<u8>, Vec<u8>>) {
    let config = ReelConfig {
        segment_bytes: ByteCount::from_bytes(64 * 1024),
        sync: SyncPolicy::Never,
        ..ReelConfig::default()
    };
    let store = ReelStore::open(dir.path().to_path_buf(), config, COLUMNS).expect("open");
    let mut model = BTreeMap::new();
    for n in 0..KEYS {
        Store::put(&store, "rows", &key_of(n), &value_of(n)).expect("put");
        model.insert(key_of(n), value_of(n));
        if n % 500 == 499 {
            Store::maintain(&store).expect("maintain");
        }
    }
    (store, model)
}

fn sweep_all(store: &ReelStore) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = Vec::new();
    let mut from: Option<Vec<u8>> = None;
    loop {
        let (page, next) = Store::sweep(store, "rows", from.as_deref(), PAGE).expect("sweep");
        rows.extend(page.into_iter().map(|(key, value)| (key, value.to_vec())));
        match next {
            Some(next) => from = Some(next),
            None => return rows,
        }
    }
}

fn sweep_prefix_all(store: &ReelStore, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = Vec::new();
    let mut from: Option<Vec<u8>> = None;
    loop {
        let (page, next) = Store::sweep_prefix(store, "rows", prefix, from.as_deref(), PAGE)
            .expect("sweep prefix");
        rows.extend(page.into_iter().map(|(key, value)| (key, value.to_vec())));
        match next {
            Some(next) => from = Some(next),
            None => return rows,
        }
    }
}

fn sweep_keys_prefix_all(store: &ReelStore, prefix: &[u8]) -> Vec<Vec<u8>> {
    let mut keys = Vec::new();
    let mut from: Option<Vec<u8>> = None;
    loop {
        let (page, next) = Store::sweep_keys_prefix(store, "rows", prefix, from.as_deref(), PAGE)
            .expect("sweep keys");
        keys.extend(page);
        match next {
            Some(next) => from = Some(next),
            None => return keys,
        }
    }
}

fn sorted<T: Ord>(mut rows: Vec<T>) -> Vec<T> {
    rows.sort();
    rows
}

// a sweep hands out every key, from the footers and the map alike
#[test]
fn a_sweep_hands_out_every_key() {
    let dir = TempDir::new().expect("temp dir");
    let (store, model) = filled(&dir);
    let whole: Vec<(Vec<u8>, Vec<u8>)> = model.clone().into_iter().collect();
    assert_eq!(
        sorted(sweep_all(&store)),
        whole,
        "a whole sweep differs from what was written"
    );

    let prefix = [2u8];
    let under: Vec<(Vec<u8>, Vec<u8>)> = model
        .iter()
        .filter(|(key, _)| key.starts_with(&prefix))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    assert_eq!(
        sorted(sweep_prefix_all(&store, &prefix)),
        under,
        "a prefix sweep differs"
    );
    let keys: Vec<Vec<u8>> = under.into_iter().map(|(key, _)| key).collect();
    assert_eq!(
        sorted(sweep_keys_prefix_all(&store, &prefix)),
        keys,
        "a prefix key sweep differs"
    );

    // A foreign mark starts the sweep over, so every key still comes back
    let (page, _) =
        Store::sweep(&store, "rows", Some(b"not a mark"), KEYS as usize).expect("sweep");
    assert_eq!(
        page.len(),
        KEYS as usize,
        "a sweep from a foreign mark lost keys"
    );
}

// deletes over sealed keys stand in the map, and a sweep pages on past a page of them
#[test]
fn a_sweep_pages_past_deletes_over_sealed_keys() {
    let dir = TempDir::new().expect("temp dir");
    let (store, mut model) = filled(&dir);
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");
    // Three pages of keys in a row under a prefix, with live keys on both sides
    let run = |model: &BTreeMap<Vec<u8>, Vec<u8>>, prefix: u8| -> Vec<Vec<u8>> {
        model
            .keys()
            .filter(|key| key[0] == prefix)
            .skip(PAGE)
            .take(3 * PAGE)
            .cloned()
            .collect()
    };

    // Each point delete leaves a grave in the map over a sealed row
    for key in run(&model, 1) {
        Store::delete(&store, "rows", &key).expect("delete");
        model.remove(&key);
    }

    // Keys written again stand in the map over their sealed rows, and one range delete covers them
    let covered = run(&model, 2);
    for key in &covered {
        Store::put(&store, "rows", key, &value_of(0)).expect("rewrite");
    }
    let last = covered.last().expect("a covered key");
    let end = model
        .keys()
        .find(|key| *key > last)
        .expect("a key past the range")
        .clone();
    Store::delete_range(&store, "rows", &covered[0], &end).expect("delete range");
    for key in &covered {
        model.remove(key);
    }

    // A short page reads as the end, so a sweep that stops early comes back short
    let whole: Vec<(Vec<u8>, Vec<u8>)> = model.clone().into_iter().collect();
    let swept = sweep_all(&store);
    assert_eq!(
        swept.len(),
        whole.len(),
        "a whole sweep stopped at the deletes"
    );
    assert!(swept == whole, "a whole sweep differs from what is left");
    for prefix in [[1u8], [2u8]] {
        let under: Vec<(Vec<u8>, Vec<u8>)> = model
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let swept = sweep_prefix_all(&store, &prefix);
        assert_eq!(
            swept.len(),
            under.len(),
            "a prefix sweep under {prefix:?} stopped at the deletes"
        );
        assert!(swept == under, "a prefix sweep under {prefix:?} differs");
        let keys: Vec<Vec<u8>> = under.into_iter().map(|(key, _)| key).collect();
        let swept = sweep_keys_prefix_all(&store, &prefix);
        assert_eq!(
            swept.len(),
            keys.len(),
            "a key sweep under {prefix:?} stopped at the deletes"
        );
        assert!(swept == keys, "a key sweep under {prefix:?} differs");
    }
}

// an empty value in the open tail comes back from a walk, a sweep and a batched get
#[test]
fn empty_values_in_the_tail() {
    let dir = TempDir::new().expect("dir");
    let store =
        ReelStore::open(dir.path().to_path_buf(), ReelConfig::default(), COLUMNS).expect("open");
    let key = key_of(1);
    Store::put(&store, "rows", &key, &[]).expect("put");

    let walked: Vec<(Vec<u8>, usize)> = Store::iter_prefix(&store, "rows", &key[..1])
        .expect("walk")
        .map(|(key, value)| (key, value.len()))
        .collect();
    assert_eq!(
        walked,
        vec![(key.clone(), 0)],
        "a walk lost the empty value"
    );

    let (swept, _) = Store::sweep(&store, "rows", None, PAGE).expect("sweep");
    assert_eq!(swept.len(), 1, "a sweep lost the empty value");

    let many = Store::get_many(&store, "rows", &[key.as_slice()]).expect("get many");
    assert_eq!(
        many[0].as_ref().map(|value| value.len()),
        Some(0),
        "a batched get lost the empty value"
    );
}
