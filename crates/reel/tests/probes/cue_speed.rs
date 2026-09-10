//! What a cue point costs to take, and what a read through one costs
//!
//! Two questions a caller needs answered before building on this: whether
//! cueing stalls the writer, and how much slower a historical read is than a
//! live one. Opt-in, run with:
//!   cargo test -p tape-reel --release --test probes -- cue_speed

use std::time::Instant;

use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSpec, IndexResidency, KeyWidth, MapShape, RecordKey,
    ReelConfig, ReelStore, SyncPolicy, ThreadBudget, MAP_EVERYTHING,
};

const RECORDS: ColumnId = ColumnId(1);

const COLUMNS: &[ColumnSpec] = &[ColumnSpec {
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

/// The group every record here is written under
const GROUP: u16 = 7;

fn key(index: u32) -> RecordKey {
    let mut bytes = [0u8; 34];
    bytes[..2].copy_from_slice(&GROUP.to_be_bytes());
    bytes[2..6].copy_from_slice(&index.to_be_bytes());
    RecordKey::from_bytes(RECORDS, &bytes).expect("key")
}

fn config(tails: u32) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::mb(64),
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(tails),
        index: IndexResidency::Resident,
        // The read path the engine serves callers with
        map_above: MAP_EVERYTHING,
        ..ReelConfig::default()
    }
}

fn open_at(dir: &TempDir, tails: u32) -> ReelStore {
    ReelStore::open(dir.path().to_path_buf(), config(tails), COLUMNS).expect("open")
}

// what cueing costs, against how much the volume holds and how many tails it runs
pub fn cue_cost() {
    // libtest leaves the test name line open, so a header needs a newline ahead of it.
    println!();
    println!(
        "{:>8} {:>7} {:>12} {:>12}",
        "records", "tails", "first cue", "repeat cue"
    );
    for records in [1_000u32, 10_000, 100_000] {
        for tails in [1u32, 4, 8] {
            let dir = TempDir::new().expect("tempdir");
            let store = open_at(&dir, tails);
            let payload = vec![0xa5u8; 1024];
            for index in 0..records {
                store.put(&key(index), &payload).expect("put");
            }

            let start = Instant::now();
            let first = store.cue().expect("cue");
            let first_took = start.elapsed();

            // A second cue with nothing written between seals nothing, which is
            // the cost of cueing a quiet volume rather than a busy one.
            let start = Instant::now();
            let repeat = store.cue().expect("cue");
            let repeat_took = start.elapsed();

            println!(
                "{:>8} {:>7} {:>12} {:>12}",
                records,
                tails,
                format!("{first_took:.2?}"),
                format!("{repeat_took:.2?}"),
            );
            drop((first, repeat));
        }
    }
}

// what a read through a cue point costs against a live one
pub fn read_cost() {
    // libtest leaves the test name line open, so a header needs a newline ahead of it.
    println!();
    println!(
        "{:>8} {:>14} {:>14} {:>10}",
        "records", "live get", "cued get", "ratio"
    );
    for records in [1_000u32, 10_000, 100_000] {
        let dir = TempDir::new().expect("tempdir");
        let store = open_at(&dir, 4);
        let payload = vec![0xa5u8; 1024];
        for index in 0..records {
            store.put(&key(index), &payload).expect("put");
        }
        let cue = store.cue().expect("cue");

        let sample = 1_000.min(records);
        let start = Instant::now();
        for index in 0..sample {
            store.get(&key(index)).expect("get");
        }
        let live = start.elapsed() / sample;

        let start = Instant::now();
        for index in 0..sample {
            store.get_at(&key(index), &cue).expect("get at");
        }
        let cued = start.elapsed() / sample;

        println!(
            "{:>8} {:>14} {:>14} {:>9.2}x",
            records,
            format!("{live:.2?}"),
            format!("{cued:.2?}"),
            cued.as_secs_f64() / live.as_secs_f64().max(f64::MIN_POSITIVE),
        );
    }
}

// what holding a cue point costs a writer that keeps going
pub fn write_under_cue() {
    // libtest leaves the test name line open, so a header needs a newline ahead of it.
    println!();
    println!(
        "{:>8} {:>16} {:>16} {:>10}",
        "records", "put unheld", "put under cue", "ratio"
    );
    for records in [10_000u32, 100_000] {
        let payload = vec![0xa5u8; 1024];

        let dir = TempDir::new().expect("tempdir");
        let store = open_at(&dir, 4);
        let start = Instant::now();
        for index in 0..records {
            store.put(&key(index), &payload).expect("put");
        }
        let unheld = start.elapsed() / records;
        drop(store);

        let dir = TempDir::new().expect("tempdir");
        let store = open_at(&dir, 4);
        let cue = store.cue().expect("cue");
        let start = Instant::now();
        for index in 0..records {
            store.put(&key(index), &payload).expect("put");
        }
        let held = start.elapsed() / records;
        drop(cue);

        println!(
            "{:>8} {:>16} {:>16} {:>9.2}x",
            records,
            format!("{unheld:.2?}"),
            format!("{held:.2?}"),
            held.as_secs_f64() / unheld.as_secs_f64().max(f64::MIN_POSITIVE),
        );
    }
}
