//! What arming the carried tier buys, priced against the case it exists for
//!
//! A bulk load beside a hot point-get working set on a carrying column. Unarmed, every
//! write's capture stays resident and the hot set survives by accident; armed, a
//! capture enters at the bottom of the clock, a read admits only on its second touch,
//! and the shed reclaims to the budget. Each leg reports the tier's own accounting and
//! the hot set's latency either side of the load: a hot read served by the tier is an
//! index entry and a memcpy, one that lost its value goes back to the device.
//!
//! Opt-in. Run with:
//!   cargo test -p tape-reel --release --test probes -- carried_price
//! Knobs: CARRIED_HOT_KEYS, CARRIED_LOAD_MB, CARRIED_BUDGET_MB, CARRIED_VALUE.

use std::path::Path;
use std::time::Instant;

use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape, RecordKey, ReelConfig,
    ReelStore, SyncPolicy,
};

const COLUMN: ColumnId = ColumnId(1);

/// The carrying column, shaped like the meta column that shipped the tier
const CARRY_COLUMNS: ColumnSet = &[ColumnSpec {
    id: COLUMN,
    name: "meta",
    key_width: KeyWidth::Fixed(16),
    shard_bytes: 0,
    inline_max: 1024,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

fn env_num(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(default)
}

fn key_of(at: u64, hot: bool) -> RecordKey {
    let mut key = [0u8; 16];
    key[0] = match hot {
        true => 1,
        false => 2,
    };
    key[8..].copy_from_slice(&at.to_be_bytes());
    RecordKey::from_bytes(COLUMN, &key).expect("key")
}

fn quantile(sorted: &[u64], q: f64) -> f64 {
    match sorted.is_empty() {
        true => 0.0,
        false => sorted[((sorted.len() - 1) as f64 * q).round() as usize] as f64,
    }
}

/// Time one pass over the hot set, answering p50 and p99 in nanoseconds
fn timed_hot_pass(store: &ReelStore, hot: u64) -> (f64, f64) {
    let mut took = Vec::with_capacity(hot as usize);
    for at in 0..hot {
        let began = Instant::now();
        let found = store.get(&key_of(at, true)).expect("hot get");
        took.push(began.elapsed().as_nanos() as u64);
        assert!(found.is_some(), "a hot key vanished");
    }
    took.sort_unstable();
    (quantile(&took, 0.50), quantile(&took, 0.99))
}

fn run_leg(dir: &Path, budget_mb: u64) {
    let value_len = env_num("CARRIED_VALUE", 512) as usize;
    let hot = env_num("CARRIED_HOT_KEYS", 20_000);
    let load_bytes = env_num("CARRIED_LOAD_MB", 256) * 1024 * 1024;

    let config = ReelConfig {
        sync: SyncPolicy::Never,
        scrub_mbps: 0,
        carried_budget: ByteCount::from_bytes(budget_mb * 1024 * 1024),
        ..ReelConfig::default()
    };
    let store = ReelStore::open(dir.to_path_buf(), config, CARRY_COLUMNS).expect("open");
    let value = vec![0xC4u8; value_len];

    // The hot set: written, then touched twice, which is what admission asks of a read
    // under the armed policy and free under the unarmed one.
    for at in 0..hot {
        store.put(&key_of(at, true), &value).expect("hot put");
    }
    for _ in 0..2 {
        for at in 0..hot {
            store.get(&key_of(at, true)).expect("warm get");
        }
    }
    let (before_p50, before_p99) = timed_hot_pass(&store, hot);
    let carried_before = store.carried_bytes().to_bytes();

    // The load: unique keys the hot set never asks for again. Armed, these captures sit
    // at the clock's bottom and the shed takes them first; unarmed they stay.
    let load_keys = load_bytes / value_len as u64;
    for at in 0..load_keys {
        store.put(&key_of(at, false), &value).expect("load put");
    }
    for _ in 0..8 {
        store.maintain_once().expect("maintain");
    }
    let carried_after = store.carried_bytes().to_bytes();
    let (after_p50, after_p99) = timed_hot_pass(&store, hot);

    println!(
        "| {} | {:.0} | {:.0} | {:.0} | {:.0} | {:.0} | {:.0} |",
        match budget_mb {
            0 => "unarmed".to_string(),
            mb => format!("{mb} MiB"),
        },
        carried_before as f64 / (1024.0 * 1024.0),
        carried_after as f64 / (1024.0 * 1024.0),
        before_p50,
        before_p99,
        after_p50,
        after_p99,
    );
}

// a bulk load beside a hot working set, the case the whole policy exists for
pub fn a_bulk_load_meets_a_hot_working_set() {
    println!();
    println!(
        "hot {} keys of {} B, load {} MiB",
        env_num("CARRIED_HOT_KEYS", 20_000),
        env_num("CARRIED_VALUE", 512),
        env_num("CARRIED_LOAD_MB", 256),
    );
    println!();
    println!("| budget | carried warm MiB | carried after MiB | hot p50 ns | p99 | after p50 | after p99 |");
    println!("|---|---|---|---|---|---|---|");

    for budget_mb in [0, env_num("CARRIED_BUDGET_MB", 64)] {
        let dir = TempDir::new().expect("tempdir");
        run_leg(dir.path(), budget_mb);
    }
}
