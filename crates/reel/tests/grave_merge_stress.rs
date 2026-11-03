//! Stress: a volume buried in deletes of rows it never held, merged and closed
//!
//! Run with `cargo test --release --test grave_merge_stress -- --ignored --nocapture`.
//! GRAVE_STRESS_KEYS, GRAVE_STRESS_ROUNDS and GRAVE_STRESS_MISSING scale it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::TempDir;

use reel::config::{ReelConfig, SyncPolicy, ThreadBudget};
use reel::format::column::{Codec, ColumnId, ColumnSet, ColumnSpec};
use reel::units::ByteCount;
use reel::{KeyWidth, ReelStore};
use reel_core::Store;

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: "rows",
    key_width: KeyWidth::Variable,
    shard_bytes: 0,
    purge_mark: None,
    codec: Codec::None,
}];

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(env("GRAVE_STRESS_SEGMENT", 256 << 10)),
        // Smaller than the volume's footers, as on a node whose sealed footers outgrow the cache
        footer_cache: ByteCount::from_bytes(env("GRAVE_STRESS_FOOTER_CACHE", 1 << 20)),
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        ..ReelConfig::default()
    }
}

/// A key spread over the whole key space, so every segment's span overlaps every other
fn key(n: u64) -> [u8; 8] {
    n.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes()
}

fn value(n: u64) -> Vec<u8> {
    vec![(n % 251) as u8; 512]
}

/// Live keys, then rounds of deletes: every round deletes the same missing keys again, as the
/// gc sweep did, and one live key in ten once
fn fill(store: &ReelStore, keys: u64, rounds: u64, missing: u64) {
    for n in 0..keys {
        Store::put(store, "rows", &key(n), &value(n)).expect("put");
    }
    // The live keys merge into one big run first, so the graves' merges leave its segments out
    store.flush().expect("flush");
    store.page_out_sealed().expect("page out");
    while store.merge_when_due().expect("merge").is_some() {
        store.page_out_sealed().expect("page out");
    }
    for round in 0..rounds {
        for n in 0..missing {
            Store::delete(store, "rows", &key(1 << 40 | n)).expect("delete missing");
        }
        for n in (round..keys).step_by(10 * rounds as usize) {
            Store::delete(store, "rows", &key(n)).expect("delete live");
        }
        store.flush().expect("flush");
    }
    store.flush().expect("flush");
}

fn is_deleted(n: u64, keys: u64, rounds: u64) -> bool {
    n < keys && (n % (10 * rounds)) < rounds
}

fn check(store: &ReelStore, keys: u64, rounds: u64, missing: u64) {
    for n in 0..keys {
        let got = Store::get(store, "rows", &key(n)).expect("get");
        match is_deleted(n, keys, rounds) {
            true => assert!(got.is_none(), "deleted key {n} came back"),
            false => assert_eq!(
                got.map(|value| value.to_vec()),
                Some(value(n)),
                "live key {n} lost"
            ),
        }
    }
    for n in (0..missing).step_by(97) {
        assert!(
            Store::get(store, "rows", &key(1 << 40 | n)).expect("get").is_none(),
            "a missing key appeared"
        );
    }
}

// merge to the end, close, reopen and read every key back
#[test]
#[ignore]
fn merge_through_graves_then_close() {
    let (keys, rounds, missing) = (
        env("GRAVE_STRESS_KEYS", 200_000),
        env("GRAVE_STRESS_ROUNDS", 8),
        env("GRAVE_STRESS_MISSING", 200_000),
    );
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().to_path_buf();
    let store = ReelStore::open(root.clone(), config(), COLUMNS).expect("open");
    let began = Instant::now();
    fill(&store, keys, rounds, missing);
    store.page_out_sealed().expect("page out");
    println!(
        "fill {:.1}s, {} keys, {} rounds of {} missing deletes",
        began.elapsed().as_secs_f64(),
        keys,
        rounds,
        missing
    );

    let began = Instant::now();
    let mut merges = 0;
    let mut dropped = 0;
    while let Some(report) = store.merge_when_due().expect("merge") {
        println!("  {report:?}");
        merges += 1;
        dropped += report.graves_dropped;
        store.page_out_sealed().expect("page out");
    }
    println!(
        "merge {:.2}s, {merges} merges, {dropped} graves dropped",
        began.elapsed().as_secs_f64()
    );
    check(&store, keys, rounds, missing);

    let began = Instant::now();
    store.close().expect("close");
    println!("close {:.2}s", began.elapsed().as_secs_f64());
    drop(store);

    let began = Instant::now();
    let store = ReelStore::open(root, config(), COLUMNS).expect("reopen");
    println!("reopen {:.2}s", began.elapsed().as_secs_f64());
    check(&store, keys, rounds, missing);
}

// a close while maintenance runs returns soon, and the reopened volume reads every key back
#[test]
#[ignore]
fn close_while_maintenance_runs() {
    let (keys, rounds, missing) = (
        env("GRAVE_STRESS_KEYS", 200_000),
        env("GRAVE_STRESS_ROUNDS", 8),
        env("GRAVE_STRESS_MISSING", 200_000),
    );
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().to_path_buf();
    let store = Arc::new(ReelStore::open(root.clone(), config(), COLUMNS).expect("open"));
    fill(&store, keys, rounds, missing);

    let ticking = Arc::clone(&store);
    let ticker = std::thread::spawn(move || {
        let began = Instant::now();
        let outcome = ticking.maintain_once();
        (outcome, began.elapsed())
    });
    std::thread::sleep(Duration::from_millis(env("GRAVE_STRESS_CLOSE_AFTER_MS", 300)));
    let began = Instant::now();
    store.close().expect("close");
    let closed = began.elapsed();
    let (outcome, ran) = ticker.join().expect("ticker");
    outcome.expect("maintain");
    println!(
        "close {:.2}s, the pass ran {:.2}s",
        closed.as_secs_f64(),
        ran.as_secs_f64()
    );
    drop(store);

    let store = ReelStore::open(root, config(), COLUMNS).expect("reopen");
    check(&store, keys, rounds, missing);
    while store.merge_when_due().expect("merge").is_some() {
        store.page_out_sealed().expect("page out");
    }
    check(&store, keys, rounds, missing);
}
