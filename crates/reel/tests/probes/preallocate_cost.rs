//! What preallocation costs, in time at creation and in blocks held on disk
//!
//! `Preallocate::Full` reserves a whole segment the moment a tail opens one and
//! `Chunk` reserves `alloc_chunk` at a time as the head advances. Both halves are
//! reported because they do not move together: where `fallocate` writes extent
//! metadata and nothing else the whole difference is blocks held before anything has
//! been written, and where it falls back to zeroing the cost moves into the clock.
//! Blocks rather than lengths, since both modes reach the same file length as soon as
//! the head passes. The fill column carries the flush and so pins to the device's
//! sustained write: it is a control saying the drive was not the variable, not a
//! result. Linux is where this means anything, the macOS reservation path being a
//! different call with a shortfall of its own.
//!
//! Opt-in. Run with:
//!   cargo test -p tape-reel --release --test probes -- preallocate_cost

use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::time::Instant;

use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSpec, IndexResidency, KeyWidth, MapShape, Preallocate,
    RecordKey, ReelConfig, ReelStore, SyncPolicy, ThreadBudget,
};

const RECORDS: ColumnId = ColumnId(1);

const COLUMNS: &[ColumnSpec] = &[ColumnSpec {
    id: RECORDS,
    name: "records",
    key_width: KeyWidth::Fixed(8),
    shard_bytes: 0,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

/// Payload per record, large enough that a roll is reached in few enough puts
const RECORD_BYTES: usize = 64 * 1024;

/// Times over the tails' whole reservation the fill writes
///
/// Enough that every tail rolls more than once, a roll being where `Full` pays its
/// reservation again and `Chunk` pays only its next chunk.
const FILL_OVER_RESERVATION: u64 = 3;

fn key(at: u64) -> RecordKey {
    RecordKey::from_bytes(RECORDS, &at.to_be_bytes()).expect("key")
}

/// Blocks the volume's segments actually hold, in MiB
///
/// `blocks()` is 512-byte units whatever the filesystem's own block size is, and it
/// counts what was committed rather than what the length claims.
fn blocks_mib(dir: &Path) -> f64 {
    let mut blocks = 0u64;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0.0;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            blocks += (blocks_mib(&path) * 2048.0) as u64;
            continue;
        }
        if let Ok(meta) = entry.metadata() {
            blocks += meta.blocks();
        }
    }
    blocks as f64 * 512.0 / (1024.0 * 1024.0)
}

fn config(mode: Preallocate, segment_mib: u64, tails: u32) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::mb(segment_mib),
        alloc_chunk: ByteCount::mb(64),
        preallocate: mode,
        // No sync per put, so the fill is not a queue of one-at-a-time durability
        // points. The reservation still does not show in the fill column, which the
        // flush pins to the device either way.
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(tails),
        index: IndexResidency::Resident,
        scrub_mbps: 0,
        ..ReelConfig::default()
    }
}

struct Leg {
    open_ms: f64,
    open_mib: f64,
    fill_mbps: f64,
    end_mib: f64,
    payload_mib: f64,
    segments: usize,
}

fn run(mode: Preallocate, segment_mib: u64, tails: u32) -> Leg {
    let home = TempDir::new().expect("tempdir");
    let dir = home.path().join("vol");

    let started = Instant::now();
    let store =
        ReelStore::open(dir.clone(), config(mode, segment_mib, tails), COLUMNS).expect("open");
    let open_ms = started.elapsed().as_secs_f64() * 1000.0;
    let open_mib = blocks_mib(&dir);

    let payload = vec![0x5Au8; RECORD_BYTES];
    let target = segment_mib * tails as u64 * FILL_OVER_RESERVATION * 1024 * 1024;
    let records = target / RECORD_BYTES as u64;

    let started = Instant::now();
    for at in 0..records {
        store.put(&key(at), &payload).expect("put");
    }
    store.flush().expect("flush");
    let fill = started.elapsed().as_secs_f64();

    let payload_mib = (records * RECORD_BYTES as u64) as f64 / (1024.0 * 1024.0);
    let end_mib = blocks_mib(&dir);
    let segments = std::fs::read_dir(&dir)
        .expect("read dir")
        .flatten()
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.ends_with(".reel"))
        })
        .count();

    Leg {
        open_ms,
        open_mib,
        fill_mbps: payload_mib / fill,
        end_mib,
        payload_mib,
        segments,
    }
}

// what a whole-segment reservation costs against a chunked one, per shape
//
// A small volume at two tails, then the harness default of eight tails at a gibibyte,
// which is the cell where a reservation weighs gibibytes before a record is written.
pub fn preallocation_cost_by_shape() {
    println!(
        "{:>6}  {:>8}  {:>5}  {:>9}  {:>10}  {:>10}  {:>10}  {:>10}  {:>8}",
        "mode",
        "seg MiB",
        "tails",
        "open ms",
        "open MiB",
        "fill MB/s",
        "payload",
        "end MiB",
        "segments",
    );

    for (segment_mib, tails) in [(64u64, 2u32), (256, 2), (1024, 8)] {
        for mode in [Preallocate::Full, Preallocate::Chunk] {
            let leg = run(mode, segment_mib, tails);
            println!(
                "{:>6}  {:>8}  {:>5}  {:>9.2}  {:>10.1}  {:>10.1}  {:>10.1}  {:>10.1}  {:>8}",
                match mode {
                    Preallocate::Full => "full",
                    Preallocate::Chunk => "chunk",
                },
                segment_mib,
                tails,
                leg.open_ms,
                leg.open_mib,
                leg.fill_mbps,
                leg.payload_mib,
                leg.end_mib,
                leg.segments,
            );
        }
    }

    println!();
    println!("open MiB is blocks committed before a record was written, so it is what the");
    println!("reservation itself costs, and open ms is what committing them takes. end MiB");
    println!("against payload is what the volume weighs for what it holds.");
    println!();
    println!("fill MB/s is to the DEVICE, not the page cache: the flush is inside the timed");
    println!("window, so every cell converges on the drive's sustained write and reads the");
    println!("same whatever else changed. It is a control, not a result. Read it to check");
    println!("the drive was not the variable, and price the reservation off open ms, which");
    println!("is the only column that isolates it.");
}
