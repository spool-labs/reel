//! Measures what a hole punch reclaims from dead runs, and checks a punched volume reads back
//! Run `cargo test -p tape-reel --release --test probes -- erase_probe` with TMPDIR off tmpfs

#[cfg(target_os = "linux")]
use std::path::Path;

use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, RecordKey, ReelConfig, ReelStore,
    SyncPolicy,
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

fn unique_id() -> [u8; 32] {
    rand::random()
}

fn record_key(group: u16, id: [u8; 32]) -> RecordKey {
    let mut bytes = [0u8; 34];
    bytes[..2].copy_from_slice(&group.to_be_bytes());
    bytes[2..].copy_from_slice(&id);
    RecordKey::from_bytes(RECORDS, &bytes).expect("key")
}

fn config(segment: ByteCount) -> ReelConfig {
    ReelConfig {
        sync: SyncPolicy::Never,
        scrub_mbps: 0,
        segment_bytes: segment,
        ..ReelConfig::default()
    }
}

fn payload(byte: u8, len: usize) -> Vec<u8> {
    vec![byte; len]
}

/// Bytes the filesystem allocates for the tree, from block counts
#[cfg(target_os = "linux")]
fn held_bytes(dir: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    let mut total = 0u64;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else { continue };
        if meta.is_dir() {
            total += held_bytes(&entry.path());
        } else {
            total += meta.blocks() * 512;
        }
    }
    total
}

// a punched volume answers every live key after a rebuild
#[cfg(target_os = "linux")]
pub fn an_erased_volume_rebuilds_every_live_key() {
    let dir = TempDir::new().expect("tempdir");
    let group = 7u16;
    let record = 64 * 1024;
    let count = 64usize;

    let ids: Vec<[u8; 32]> = (0..count).map(|_| unique_id()).collect();
    {
        let store = ReelStore::open(dir.path().to_path_buf(), config(ByteCount::mb(1)), COLUMNS)
            .expect("open");
        for (i, id) in ids.iter().enumerate() {
            store
                .put(&record_key(group, *id), &payload(i as u8, record))
                .expect("put");
        }
        // Overwrite every other key, so dead records scatter through the sealed segments
        for (i, id) in ids.iter().enumerate() {
            if i % 2 == 0 {
                store
                    .put(&record_key(group, *id), &payload(0xA0 ^ i as u8, record))
                    .expect("overwrite");
            }
        }
        store.flush().expect("flush");

        let before = held_bytes(dir.path());
        let report = store.erase_dead_runs().expect("punch");
        assert!(report.segments > 0, "no sealed segment was examined");
        assert!(report.erased_bytes > 0, "nothing was punched");
        let after = held_bytes(dir.path());
        assert!(
            before.saturating_sub(after) >= report.erased_bytes / 2,
            "the filesystem gave back {} of {} punched bytes",
            before.saturating_sub(after),
            report.erased_bytes,
        );
    }

    let reopened = ReelStore::open(dir.path().to_path_buf(), config(ByteCount::mb(1)), COLUMNS)
        .expect("reopen");
    for (i, id) in ids.iter().enumerate() {
        let want = if i % 2 == 0 { 0xA0 ^ i as u8 } else { i as u8 };
        let found = reopened
            .get(&record_key(group, *id))
            .expect("read")
            .expect("live key lost after punch and rebuild");
        assert_eq!(&*found, &payload(want, record), "key {i} came back wrong");
    }
}

// a second erase finds deaths behind the holes the first one left
#[cfg(target_os = "linux")]
pub fn a_second_erase_reaches_past_the_first_holes() {
    let dir = TempDir::new().expect("tempdir");
    let group = 7u16;
    let record = 32 * 1024;
    let count = 64usize;

    let store =
        ReelStore::open(dir.path().to_path_buf(), config(ByteCount::mb(4)), COLUMNS).expect("open");
    let ids: Vec<[u8; 32]> = (0..count).map(|_| unique_id()).collect();
    for (i, id) in ids.iter().enumerate() {
        store
            .put(&record_key(group, *id), &payload(i as u8, record))
            .expect("put");
    }
    // Roll the tail so the first segment seals with a footer
    for i in 0..count {
        store
            .put(
                &record_key(group, unique_id()),
                &payload(0xF0 ^ i as u8, record),
            )
            .expect("filler");
    }
    store.flush().expect("flush");

    for id in &ids[..count / 4] {
        store.delete(&record_key(group, *id)).expect("front kill");
    }
    store.flush().expect("flush");
    let first = store.erase_dead_runs().expect("first erase");
    assert!(first.erased_bytes > 0, "the first erase found nothing");

    for id in &ids[count / 2..] {
        store.delete(&record_key(group, *id)).expect("back kill");
    }
    store.flush().expect("flush");
    let second = store.erase_dead_runs().expect("second erase");
    assert!(
        second.erased_bytes > 0,
        "the second erase stopped at the first pass's holes",
    );
}

// prints the share of dead bytes a punch reclaims on a churned volume
pub fn erase_reclaim_share() {
    println!();
    let volume: u64 = std::env::var("REEL_ERASE_VOLUME_BYTES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(8 * 1024 * 1024 * 1024);
    let record: usize = std::env::var("REEL_ERASE_RECORD_BYTES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(4096);
    let count = (volume / record as u64) as usize;
    let group = 7u16;

    let dir = TempDir::new().expect("tempdir");
    let store =
        ReelStore::open(dir.path().to_path_buf(), config(ByteCount::gb(1)), COLUMNS).expect("open");

    println!(
        "volume {} GiB in {} records of {} B",
        volume >> 30,
        count,
        record
    );
    let mut ids = Vec::with_capacity(count);
    let body = payload(0x5A, record);
    for _ in 0..count {
        let id = unique_id();
        store.put(&record_key(group, id), &body).expect("put");
        ids.push(id);
    }
    // `stride` kills three of every five in a row, `random` scatters the same share
    let mode = std::env::var("REEL_ERASE_MODE").unwrap_or_else(|_| "stride".to_string());
    let mut victims: Vec<usize> = (0..count).filter(|i| (i % 5) < 3).collect();
    if mode == "random" {
        let mut order: Vec<usize> = (0..count).collect();
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        for i in (1..order.len()).rev() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            order.swap(i, (state as usize) % (i + 1));
        }
        victims = order.into_iter().take(count * 3 / 5).collect();
    }
    println!("kill mode {mode}, {} victims", victims.len());
    for i in victims {
        store.delete(&record_key(group, ids[i])).expect("delete");
    }
    store.flush().expect("flush");

    let dead = store.dead_bytes().to_bytes();
    let report = store.erase_dead_runs().expect("punch");
    println!(
        "segments {}, dead {:.2} GiB, dead runs {:.2} GiB, punched {:.2} GiB, {:.1}% of dead",
        report.segments,
        dead as f64 / (1u64 << 30) as f64,
        report.dead_run_bytes as f64 / (1u64 << 30) as f64,
        report.erased_bytes as f64 / (1u64 << 30) as f64,
        report.erased_bytes as f64 * 100.0 / dead.max(1) as f64,
    );
}
