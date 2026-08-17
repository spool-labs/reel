//! What compaction has to rewrite, depending on how the garbage was made
//!
//! A delete leaves its dead bytes where they lie, among records that are still live,
//! so reclaiming that segment means copying the survivors out first. An update
//! shadows the old copy where it sits and writes the new one to the tail, so a volume
//! whose keys are all rewritten leaves older segments holding nothing live, and a
//! segment with no survivors is unlinked whole. move_ratio, the fraction of retired
//! segments that had to be rewritten, is what separates the two shapes. The question
//! is structural rather than a rate, so the volume is small enough to run anywhere.

use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, CompactRate, KeyWidth, MapShape, RecordKey,
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
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
    ColumnSpec {
        id: BLOB,
        name: "blob_data",
        key_width: KeyWidth::Fixed(32),
        shard_bytes: 0,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
];

/// Bytes written before any garbage is made
///
/// Which way a segment retires does not need a volume larger than memory.
const VOLUME_BYTES: u64 = 512 * 1024 * 1024;

/// One record's payload
const RECORD_BYTES: usize = 256 * 1024;

/// Segments small enough that a volume this size spans a useful number of them
const SEGMENT_BYTES: u64 = 32 * 1024 * 1024;

/// Times every key is rewritten in the update shape
const ROUNDS: usize = 3;

/// Fraction of keys deleted in the delete shape, matched to what the rewrites
/// leave dead so the two shapes are asked to reclaim comparable amounts
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
        alloc_chunk: ByteCount::from_bytes(SEGMENT_BYTES / 8),
        sync: SyncPolicy::Never,
        compact_mbps: CompactRate::Mbps(0),
        scrub_mbps: 0,
        ..ReelConfig::default()
    }
}

fn open(dir: &TempDir) -> ReelStore {
    ReelStore::open(dir.path().to_path_buf(), config(), COLUMNS).expect("open")
}

/// Drive compaction until two passes running give nothing further back
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
    // libtest leaves "test name ... " open, so a header printed into it lands a
    // screen-width right of the rows underneath it.
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

    // Updates: every key rewritten in place, so the live copy walks forward and
    // the segments behind it are left holding only shadows.
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

    // Mixed: only some keys are rewritten, so the untouched records keep their
    // segments partly alive, which is the shape a metadata-heavy caller makes.
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

    // Deletes: the dead bytes stay where they were written, interleaved with the
    // records that are still live.
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
