//! What a definite miss costs, which is what the shard filter exists to move
//!
//! The filter in front of a shard answers a certain no at two relaxed loads, and an
//! occupied shard's false positives fall through to the lock a negative lookup always
//! took. The present-key row is the control: it pays the two loads on top of the lock
//! and should move by noise alone. The rows mean nothing single-ended, so run both
//! sides of the A/B. Concurrency is the second axis, since a read lock taken by eight
//! threads at once is a shared line they bounce however short the hold.
//!
//! Opt-in. Run with:
//!   cargo test -p reel --release --test probes -- negative_price

use std::sync::Arc;
use std::time::Instant;

use tempfile::TempDir;

use reel::{
    Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape, RecordKey, ReelConfig, ReelStore,
    SyncPolicy,
};

const RECORDS: ColumnId = ColumnId(1);

/// A record-shaped column: two shard bytes, thirty-four byte keys
const COLUMNS: ColumnSet = &[ColumnSpec {
    id: RECORDS,
    name: "records",
    key_width: KeyWidth::Fixed(34),
    shard_bytes: 2,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

/// Keys the store holds, all under one two-byte prefix so one shard is occupied
const PRESENT: u64 = 100_000;

/// Lookups each timed case makes per thread
const ASKS: u64 = 200_000;

fn key_of(prefix: u16, at: u64) -> RecordKey {
    let mut key = [0u8; 34];
    key[..2].copy_from_slice(&prefix.to_be_bytes());
    key[26..].copy_from_slice(&at.to_be_bytes());
    RecordKey::from_bytes(RECORDS, &key).expect("key")
}

/// Per-op nanoseconds for one lookup shape at one thread count
fn timed<Ask>(store: &Arc<ReelStore>, threads: u64, ask: Ask) -> f64
where
    Ask: Fn(&ReelStore, u64, u64) + Send + Sync + Copy,
{
    let began = Instant::now();
    std::thread::scope(|scope| {
        for thread in 0..threads {
            let store = Arc::clone(store);
            scope.spawn(move || {
                for at in 0..ASKS {
                    ask(&store, thread, at);
                }
            });
        }
    });
    began.elapsed().as_nanos() as f64 / (threads * ASKS) as f64
}

// misses by the thousand, beside the present-key control
pub fn a_miss_is_answered_without_the_lock() {
    let dir = TempDir::new().expect("tempdir");
    let config = ReelConfig {
        sync: SyncPolicy::Never,
        scrub_mbps: 0,
        ..ReelConfig::default()
    };
    let store = Arc::new(ReelStore::open(dir.path().to_path_buf(), config, COLUMNS).expect("open"));
    for at in 0..PRESENT {
        store.put(&key_of(7, at), b"held").expect("put");
    }

    println!();
    println!("| case | threads | ns per ask |");
    println!("|---|---|---|");
    for threads in [1u64, 8] {
        let same_shard = timed(&store, threads, |store, thread, at| {
            let miss = store
                .get(&key_of(7, PRESENT + thread * ASKS + at))
                .expect("get");
            assert!(miss.is_none());
        });
        let foreign_shard = timed(&store, threads, |store, thread, at| {
            let miss = store.get(&key_of(40_000 + thread as u16, at)).expect("get");
            assert!(miss.is_none());
        });
        let present = timed(&store, threads, |store, thread, at| {
            let found = store
                .get(&key_of(7, (thread * ASKS + at) % PRESENT))
                .expect("get");
            assert!(found.is_some());
        });
        println!("| same-shard miss | {threads} | {same_shard:.0} |");
        println!("| foreign-shard miss | {threads} | {foreign_shard:.0} |");
        println!("| present key | {threads} | {present:.0} |");
    }
}
