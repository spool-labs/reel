//! Measures whether compaction rewrites or unlinks segments, by how the garbage was made

use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, CompactRate, KeyWidth, RecordKey,
    ReelConfig, ReelStore, SyncPolicy,
};

const RECORDS: ColumnId = ColumnId(1);
const BLOB: ColumnId = ColumnId(2);

const COLUMNS: ColumnSet = &[
    ColumnSpec {
        id: RECORDS,
        name: "records",
        key_width: KeyWidth::Fixed(34),
        shard_bytes: 2,
        purge_mark: None,
        codec: Codec::None,
    },
    ColumnSpec {
        id: BLOB,
        name: "blob_data",
        key_width: KeyWidth::Fixed(32),
        shard_bytes: 0,
        purge_mark: None,
        codec: Codec::None,
    },
];

/// Bytes written before any garbage is made
const VOLUME_BYTES: u64 = 512 * 1024 * 1024;

/// One record's payload
const RECORD_BYTES: usize = 256 * 1024;

/// Segment size, small enough that the volume spans many segments
const SEGMENT_BYTES: u64 = 32 * 1024 * 1024;

/// The update shape rewrites every key this many times
const ROUNDS: usize = 3;

/// The delete shape deletes this share of keys, about what the rewrites leave dead
const KILL_FRACTION: f64 = 0.66;

fn unique_id() -> [u8; 32] {
    rand::random()
}

fn record_key(group: u16, id: [u8; 32]) -> RecordKey {
    let mut bytes = [0u8; 34];
    bytes[..2].copy_from_slice(&group.to_be_bytes());
    bytes[2..].copy_from_slice(&id);
    RecordKey::from_bytes(RECORDS, &bytes).expect("key")
}

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(SEGMENT_BYTES),
        sync: SyncPolicy::Never,
        compact_mbps: CompactRate::Mbps(0),
        scrub_mbps: 0,
        ..ReelConfig::default()
    }
}

fn open(dir: &TempDir) -> ReelStore {
    ReelStore::open(dir.path().to_path_buf(), config(), COLUMNS).expect("open")
}

/// Compact until two passes in a row reclaim nothing
fn drain(store: &ReelStore) {
    let mut idle = 0u32;
    let mut last = store.dead_bytes().to_bytes();
    while idle < 2 {
        store.compact_once().expect("compact");
        let now = store.dead_bytes().to_bytes();
        if now >= last {
            idle += 1;
        } else {
            idle = 0;
        }
        last = now;
    }
}

fn report(shape: &str, store: &ReelStore, dead_before: u64) {
    let counters = store.compaction_counters();
    let retired = counters.segments_rewritten + counters.segments_unlinked_whole;
    println!(
        "{shape:>8} {:>10} {:>10} {:>12.2} {:>13.1}M {:>13.1}M",
        counters.segments_rewritten,
        counters.segments_unlinked_whole,
        counters.move_ratio(),
        counters.compaction_bytes as f64 / 1e6,
        dead_before.saturating_sub(store.dead_bytes().to_bytes()) as f64 / 1e6,
    );
    assert!(
        retired > 0,
        "{shape} retired nothing, so it measured nothing"
    );
}

// how a segment retires, rewritten or unlinked, across three churn shapes
#[test]
#[ignore = "writes real files, run explicitly with --ignored --nocapture"]
fn what_compaction_rewrites_by_churn_shape() {
    // libtest leaves the test name line open, so start the table on a fresh line
    println!();
    let count = (VOLUME_BYTES / RECORD_BYTES as u64) as usize;
    let group = 1u16;
    let body = vec![0x5au8; RECORD_BYTES];

    println!(
        "volume {} MiB in {count} records of {} KiB, {ROUNDS} rewrite rounds against {:.0}% killed\n",
        VOLUME_BYTES / (1024 * 1024),
        RECORD_BYTES / 1024,
        KILL_FRACTION * 100.0,
    );
    println!(
        "{:>8} {:>10} {:>10} {:>12} {:>14} {:>14}",
        "shape", "rewritten", "unlinked", "move_ratio", "copied", "reclaimed"
    );

    // Updates: every key rewritten, so the segments behind hold only shadows
    {
        let dir = TempDir::new().expect("tempdir");
        let store = open(&dir);
        let ids: Vec<[u8; 32]> = (0..count).map(|_| unique_id()).collect();
        for id in &ids {
            store
                .put_owned(&record_key(group, *id), body.clone())
                .expect("put");
        }
        for round in 0..ROUNDS {
            let body = vec![0xB0u8.wrapping_add(round as u8); RECORD_BYTES];
            for id in &ids {
                store
                    .put_owned(&record_key(group, *id), body.clone())
                    .expect("update");
            }
        }
        store.flush().expect("flush");
        let dead_before = store.dead_bytes().to_bytes();
        drain(&store);
        report("update", &store, dead_before);
    }

    // Mixed: half the keys rewritten, so untouched records keep segments partly alive
    {
        let dir = TempDir::new().expect("tempdir");
        let store = open(&dir);
        let ids: Vec<[u8; 32]> = (0..count).map(|_| unique_id()).collect();
        for id in &ids {
            store
                .put_owned(&record_key(group, *id), body.clone())
                .expect("put");
        }
        for round in 0..ROUNDS {
            let body = vec![0xB0u8.wrapping_add(round as u8); RECORD_BYTES];
            for id in ids.iter().step_by(2) {
                store
                    .put_owned(&record_key(group, *id), body.clone())
                    .expect("update");
            }
        }
        store.flush().expect("flush");
        let dead_before = store.dead_bytes().to_bytes();
        drain(&store);
        report("mixed", &store, dead_before);
    }

    // Deletes: dead bytes stay interleaved with live records where they were written
    {
        let dir = TempDir::new().expect("tempdir");
        let store = open(&dir);
        let ids: Vec<[u8; 32]> = (0..count).map(|_| unique_id()).collect();
        for id in &ids {
            store
                .put_owned(&record_key(group, *id), body.clone())
                .expect("put");
        }
        let kill = (count as f64 * KILL_FRACTION) as usize;
        for id in ids.iter().take(kill) {
            store.delete(&record_key(group, *id)).expect("delete");
        }
        store.flush().expect("flush");
        let dead_before = store.dead_bytes().to_bytes();
        drain(&store);
        report("delete", &store, dead_before);
    }
}
