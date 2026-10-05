//! A sweep hands out every live key, sealed or not, on a paged volume as on a resident one
//!
//! A paged volume's map holds only what no footer covers yet, so a sweep of the map alone
//! would miss every key handed over to the footers. Pages are small, so every sweep resumes
//! from its mark many times.

use std::collections::BTreeMap;

use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, IndexResidency, KeyWidth,
    ReelConfig, ReelStore, SyncPolicy,
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

/// Keys a sweep page asks for, small so a sweep resumes from its mark many times
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

/// A volume holding every key, most of them sealed and handed over, the last few still in the map
fn filled(dir: &TempDir, index: IndexResidency) -> (ReelStore, BTreeMap<Vec<u8>, Vec<u8>>) {
    let config = ReelConfig {
        segment_bytes: ByteCount::from_bytes(64 * 1024),
        alloc_chunk: ByteCount::from_bytes(16 * 1024),
        sync: SyncPolicy::Never,
        index,
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
        let (page, next) = Store::sweep_prefix(store, "rows", prefix, from.as_deref(), PAGE).expect("sweep prefix");
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
        let (page, next) = Store::sweep_keys_prefix(store, "rows", prefix, from.as_deref(), PAGE).expect("sweep keys");
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

fn check(index: IndexResidency) {
    let dir = TempDir::new().expect("temp dir");
    let (store, model) = filled(&dir, index);
    let whole: Vec<(Vec<u8>, Vec<u8>)> = model.clone().into_iter().collect();
    assert_eq!(sorted(sweep_all(&store)), whole, "a whole sweep differs from what was written");

    let prefix = [2u8];
    let under: Vec<(Vec<u8>, Vec<u8>)> = model
        .iter()
        .filter(|(key, _)| key.starts_with(&prefix))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    assert_eq!(sorted(sweep_prefix_all(&store, &prefix)), under, "a prefix sweep differs");
    let keys: Vec<Vec<u8>> = under.into_iter().map(|(key, _)| key).collect();
    assert_eq!(sorted(sweep_keys_prefix_all(&store, &prefix)), keys, "a prefix key sweep differs");

    // A mark from nowhere starts the sweep over, and every key still comes back.
    let (page, _) = Store::sweep(&store, "rows", Some(b"not a mark"), KEYS as usize).expect("sweep");
    assert_eq!(page.len(), KEYS as usize, "a sweep from a foreign mark lost keys");
}

#[test]
fn a_paged_sweep_hands_out_every_key() {
    check(IndexResidency::Paged);
}

#[test]
fn a_resident_sweep_hands_out_every_key() {
    check(IndexResidency::Resident);
}
