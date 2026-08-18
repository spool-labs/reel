//! What a mapping is worth on a cold random read, and what the advice changed
//!
//! `map_above` is a byte floor rather than a switch because a mapping wins warm and
//! loses cold: a warm mapped read skips the kernel crossing a pread pays, and a cold
//! fault pulls a window in around the record rather than the record. Advice on the
//! open file does not reach a mapping of the same file, so a mapping has to be advised
//! `MADV_RANDOM` in its own right, and this is the pair that says what that is worth.
//! Cold means a fill past memory, so `REEL_MAP_FILL` has to name one: nothing here
//! asks the kernel to give pages back.
//!
//! `REEL_MAP_FILL` sizes the fill, `REEL_MAP_SIZES` picks the record sizes.
//!
//! Opt-in, run with:
//!   cargo test -p reel --release --test probes -- mapped_reads

use std::time::Instant;

use tempfile::TempDir;

use reel::config::{IndexResidency, ReelConfig, SyncPolicy, ThreadBudget};
use reel::format::column::{Codec, ColumnId, ColumnSet, ColumnSpec, MapShape, RecordKey};
use reel::units::ByteCount;
use reel::{KeyWidth, Preallocate, ReelStore, MAP_EVERYTHING};

const ROWS: ColumnId = ColumnId(1);

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ROWS,
    name: "rows",
    key_width: KeyWidth::Fixed(32),
    shard_bytes: 1,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

/// Record sizes the sweep walks, which straddle the floor
const SIZES: [usize; 5] = [4 << 10, 16 << 10, 64 << 10, 256 << 10, 1 << 20];

/// Bytes each size writes by default, enough segments to read randomly across
///
/// Well under memory, which makes every row here warm. `REEL_MAP_FILL` sizes a row
/// past memory, where neither arm can retain, and a cold mapped read is not quotable
/// without it.
const FILL_DEFAULT: usize = 256 << 20;

fn fill_bytes() -> usize {
    std::env::var("REEL_MAP_FILL")
        .ok()
        .and_then(|raw| raw.trim().parse().ok())
        .unwrap_or(FILL_DEFAULT)
}

/// Sizes this run walks, so a past-memory sweep can ask for the band that matters
fn sizes() -> Vec<usize> {
    match std::env::var("REEL_MAP_SIZES") {
        Ok(raw) => raw
            .split(',')
            .map(|size| size.trim().parse().expect("a size in bytes"))
            .collect(),
        Err(_) => SIZES.to_vec(),
    }
}

/// Reads timed per row, capped so a small record size does not run for minutes
const READS: usize = 4_000;

fn config(mapped: bool) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(64 * 1024 * 1024),
        alloc_chunk: ByteCount::from_bytes(4 * 1024 * 1024),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        index: IndexResidency::Resident,
        scrub_mbps: 0,
        map_above: match mapped {
            true => MAP_EVERYTHING,
            false => None,
        },
        ..ReelConfig::default()
    }
}

fn key(at: u64) -> RecordKey {
    let mixed = at
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .rotate_left(29)
        .wrapping_mul(0xBF58_476D_1CE4_E5B9);
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&mixed.to_be_bytes());
    bytes[8..16].copy_from_slice(&at.to_be_bytes());
    RecordKey::from_bytes(ROWS, &bytes).expect("key")
}

/// The order the reads are asked in, shuffled from a fixed seed
///
/// Write order is ascending offsets inside a segment, which the kernel serves with
/// readahead whatever the record size, so a sequential row measures a stream.
fn shuffled(count: u64) -> Vec<u64> {
    let mut order: Vec<u64> = (0..count).collect();
    let mut state = 0x243F_6A88_85A3_08D3u64;
    for at in (1..order.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        order.swap(at, (state % (at as u64 + 1)) as usize);
    }
    order
}

/// One arm: fill a real volume, then read it back cold in a scattered order
fn cold_random(size: usize, mapped: bool) -> (f64, u64) {
    let dir = TempDir::new().expect("tempdir");
    let count = (fill_bytes() / size) as u64;

    let store = ReelStore::open(dir.path().to_path_buf(), config(mapped), COLUMNS).expect("open");
    let payload = vec![0x3Cu8; size];
    for at in 0..count {
        store.put(&key(at), &payload).expect("put");
    }
    store.flush().expect("flush");

    let order = shuffled(count);
    let asked = READS.min(order.len());
    let began = Instant::now();
    let mut read = 0u64;
    for at in order.iter().take(asked) {
        let found = store.get(&key(*at)).expect("get").expect("present");
        read += found.as_ref().len() as u64;
    }
    let elapsed = began.elapsed().as_secs_f64();
    drop(store);
    (elapsed * 1e6 / asked as f64, read)
}

/// Mapped against unmapped, cold and scattered, at each record size
pub fn mapped_over_unmapped_cold_random() {
    // libtest leaves the test name line open, so a header needs a newline ahead of it.
    println!();
    println!(
        "{:>9} {:>8} {:>13} {:>13} {:>10}",
        "size", "records", "unmapped us", "mapped us", "mapped/un"
    );

    for size in sizes() {
        let (plain, _) = cold_random(size, false);
        let (mapped, _) = cold_random(size, true);
        let label = match size >= 1 << 20 {
            true => format!("{} MiB", size >> 20),
            false => format!("{} KiB", size >> 10),
        };
        println!(
            "{label:>9} {:>8} {plain:>13.2} {mapped:>13.2} {:>10.2}",
            fill_bytes() / size,
            plain / mapped.max(f64::MIN_POSITIVE),
        );
    }
}

/// The same pair warm, which is the plane a mapping was taken for
pub fn mapped_over_unmapped_warm() {
    println!();
    println!(
        "{:>9} {:>13} {:>13} {:>10}",
        "size", "unmapped us", "mapped us", "mapped/un"
    );

    for size in sizes() {
        let mut arms = Vec::new();
        for mapped in [false, true] {
            let dir = TempDir::new().expect("tempdir");
            let count = (fill_bytes() / size).min(4_000) as u64;
            let store =
                ReelStore::open(dir.path().to_path_buf(), config(mapped), COLUMNS).expect("open");
            let payload = vec![0x3Cu8; size];
            for at in 0..count {
                store.put(&key(at), &payload).expect("put");
            }
            store.flush().expect("flush");

            let order = shuffled(count);
            // Warm: one untimed pass so every page a read wants is already resident.
            for at in &order {
                store.get(&key(*at)).expect("get").expect("present");
            }
            let began = Instant::now();
            for at in &order {
                store.get(&key(*at)).expect("get").expect("present");
            }
            arms.push(began.elapsed().as_secs_f64() * 1e6 / order.len() as f64);
            drop(store);
            drop(dir);
        }
        let label = match size >= 1 << 20 {
            true => format!("{} MiB", size >> 20),
            false => format!("{} KiB", size >> 10),
        };
        println!(
            "{label:>9} {:>13.2} {:>13.2} {:>10.2}",
            arms[0],
            arms[1],
            arms[0] / arms[1].max(f64::MIN_POSITIVE)
        );
    }
}
