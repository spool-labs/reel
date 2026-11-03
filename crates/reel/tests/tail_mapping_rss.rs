//! File pages a node's process holds after reads into a tail that has since sealed
//! Run with `cargo test --release -p tape-reel --test tail_mapping_rss -- --ignored --nocapture`

#![cfg(target_os = "linux")]

use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng};
use tempfile::TempDir;

use reel::config::{IoBackend, ReelConfig, SyncPolicy, ThreadBudget};
use reel::format::column::{Codec, ColumnId, ColumnSet, ColumnSpec};
use reel::units::ByteCount;
use reel::{KeyWidth, ReelStore, Store};

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(14),
    name: "slice",
    key_width: KeyWidth::Fixed(34),
    shard_bytes: 2,
    purge_mark: None,
    codec: Codec::Lz4,
}];

/// A slice of a 64 MiB track at k = 7
const SLICE_BYTES: usize = 9 * 1024 * 1024 + 512 * 1024;

/// Small enough that a few dozen slices seal it
const SEGMENT_BYTES: u64 = 256 * 1024 * 1024;

/// Slices per phase, about two thirds of a segment
const SLICES: u32 = 18;

/// The process may hold this many file pages once every tail it read from has sealed
const SEALED_CEILING_MIB: u64 = 32;

fn key(at: u32) -> [u8; 34] {
    let mut key = [0u8; 34];
    key[..4].copy_from_slice(&at.to_be_bytes());
    key
}

/// File pages mapped into this process, from its status
fn rss_file_mib() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").expect("status");
    let kib = status
        .lines()
        .find_map(|line| line.strip_prefix("RssFile:"))
        .and_then(|rest| {
            rest.trim()
                .trim_end_matches("kB")
                .trim()
                .parse::<u64>()
                .ok()
        })
        .expect("RssFile");
    kib / 1024
}

fn config(backend: IoBackend) -> ReelConfig {
    // The node's config: no mapped reads, one tail
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(SEGMENT_BYTES),
        sync: SyncPolicy::Bytes(ByteCount::mb(16)),
        map_above: None,
        io_backend: backend,
        active_tails: ThreadBudget::threads(1),
        ..ReelConfig::default()
    }
}

fn write(store: &ReelStore, from: u32, rng: &mut SmallRng) {
    let mut payload = vec![0u8; SLICE_BYTES];
    for at in from..from + SLICES {
        rng.fill_bytes(&mut payload);
        Store::put(store, "slice", &key(at), &payload).expect("put");
    }
    store.flush().expect("flush");
}

fn read(store: &ReelStore, from: u32) {
    for at in from..from + SLICES {
        let value = Store::get(store, "slice", &key(at))
            .expect("get")
            .expect("present");
        assert_eq!(value.len(), SLICE_BYTES);
    }
}

fn run(backend: IoBackend) {
    let home = TempDir::new().expect("tempdir");
    let store = ReelStore::open(home.path().to_path_buf(), config(backend), COLUMNS).expect("open");
    let mut rng = SmallRng::seed_from_u64(7);
    let base = rss_file_mib();
    let step = |phase: &str| {
        println!(
            "{backend:?} {phase:<38} RssFile +{} MiB",
            rss_file_mib() - base
        )
    };

    // A: written and never read, then sealed by B
    write(&store, 0, &mut rng);
    step("A written to the tail");

    // B: written, read back while still the tail
    write(&store, 100, &mut rng);
    step("B written, A sealed unread");
    read(&store, 100);
    step("B read back from the tail");

    // C seals B
    write(&store, 200, &mut rng);
    step("C written, B sealed");

    // Reads of sealed segments go through the driver
    read(&store, 0);
    step("A read back sealed");
    read(&store, 100);
    step("B read back sealed");

    // D seals C, so every segment read as a tail is sealed
    write(&store, 300, &mut rng);
    let held = rss_file_mib() - base;
    step("D written, C sealed");

    assert!(
        held <= SEALED_CEILING_MIB,
        "{backend:?}: the process holds {held} MiB of file pages from tails that have sealed"
    );
}

#[test]
#[ignore = "writes about 1 GiB to a real disk"]
fn sealed_tail_pages_posix() {
    run(IoBackend::Posix);
}

#[test]
#[ignore = "writes about 1 GiB to a real disk"]
fn sealed_tail_pages_mixed() {
    run(IoBackend::Mixed);
}
