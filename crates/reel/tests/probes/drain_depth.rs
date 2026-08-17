//! How much company a record has when it goes down, the ceiling on batching
//!
//! A batched drain can only collect writers that are already in flight, so this
//! measures whether there is anything to collect before the coordination is built. How
//! often concurrent writers overlap is a property of the software, and a slow device
//! biases the answer upward by holding writers in flight longer, so a depth measured on
//! a laptop is an optimistic bound on a server.
//!
//! Opt-in, since it spawns threads and writes real files. Run with:
//!   cargo test -p reel --test probes -- drain_depth

use std::sync::{Arc, Barrier};
use std::thread;

use tempfile::TempDir;

use reel::append::admission::InflightBudget;
use reel::append::Commit;
use reel::config::{Preallocate, ReelConfig, SyncPolicy};
use reel::format::column::RecordKey;
use reel::io::posix_backend::PosixBackend;
use reel::reel::segment::{FdCache, IoDriver};
use reel::{
    Appender, ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape, ReelShared,
};

/// Bytes the group takes at the front of a record key
const GROUP_PREFIX_LEN: usize = 2;

/// Bytes a record key occupies
const RECORD_KEY_LEN: usize = GROUP_PREFIX_LEN + 32;

const RECORDS: ColumnId = ColumnId(1);
const RECORDS_CF: &str = "records";

const BLOB: ColumnId = ColumnId(2);
const BLOB_CF: &str = "blob_data";

const COLUMNS: ColumnSet = &[
    ColumnSpec {
        id: RECORDS,
        name: RECORDS_CF,
        key_width: KeyWidth::Fixed(RECORD_KEY_LEN as u16),
        shard_bytes: GROUP_PREFIX_LEN as u8,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
    ColumnSpec {
        id: BLOB,
        name: BLOB_CF,
        key_width: KeyWidth::Fixed(32),
        shard_bytes: 0,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
];

/// Group the fixtures write into
const GROUP: u16 = 7;

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::gb(1),
        alloc_chunk: ByteCount::mb(64),
        preallocate: Preallocate::Full,
        sync: SyncPolicy::Never,
        scrub_mbps: 0,
        ..ReelConfig::default()
    }
}

/// A record key: the group big endian, then the identifier
fn record_key(group: u16, id: [u8; 32]) -> RecordKey {
    let mut bytes = [0u8; RECORD_KEY_LEN];
    bytes[..GROUP_PREFIX_LEN].copy_from_slice(&group.to_be_bytes());
    bytes[GROUP_PREFIX_LEN..].copy_from_slice(&id);
    RecordKey::from_bytes(RECORDS, &bytes).expect("record key")
}

fn key(byte: u8, at: u64) -> RecordKey {
    let mut id = [byte; 32];
    id[1..9].copy_from_slice(&at.to_be_bytes());
    record_key(GROUP, id)
}

/// Drive `writers` threads at one tail and report how deep the overlap got
fn measure(writers: usize, record: usize, per_writer: u64) -> (Vec<(u64, u64)>, f64) {
    let dir = TempDir::new().expect("tempdir");
    let driver = Arc::new(IoDriver::new(Arc::new(PosixBackend::new())));
    let settings = config();
    let budget = Arc::new(InflightBudget::default());
    let fd_cache = Arc::new(FdCache::new(reel::DEFAULT_FD_CACHE as usize));
    let shared = Arc::new(ReelShared::new(
        dir.path().to_path_buf(),
        driver,
        budget,
        fd_cache,
        settings,
        COLUMNS,
        1,
    ));
    let appender = Arc::new(Appender::open(Arc::clone(&shared), 0).expect("appender"));

    let gate = Arc::new(Barrier::new(writers));
    let mut threads = Vec::with_capacity(writers);
    for writer in 0..writers {
        let appender = Arc::clone(&appender);
        let gate = Arc::clone(&gate);
        threads.push(thread::spawn(move || {
            let payload = vec![writer as u8; record];
            gate.wait();
            for at in 0..per_writer {
                appender
                    .append_data(
                        key(writer as u8 + 1, at),
                        payload.clone(),
                        0,
                        Commit::Batched,
                    )
                    .expect("append");
            }
        }));
    }
    for thread in threads {
        thread.join().expect("writer joins");
    }

    let depth = appender.drain_depth();
    (depth.snapshot(), depth.batchable_fraction())
}

// how deep the overlap gets across writer counts and record sizes
//
// The number that decides whether a batched drain is worth its coordination: a tail
// whose records mostly go down alone has nothing to collect.
pub fn report_drain_depth() {
    // libtest leaves the test name line open, so a header needs a newline ahead of it.
    println!();
    println!(
        "\n{:>8}{:>10}{:>12}   depth distribution (<=n: count)",
        "writers", "record", "batchable",
    );
    for record in [100usize, 4096, 1 << 20] {
        for writers in [1usize, 4, 16, 50] {
            let per_writer = if record >= (1 << 20) { 40 } else { 400 };
            let (buckets, batchable) = measure(writers, record, per_writer);
            let shown: Vec<String> = buckets
                .iter()
                .filter(|(_, count)| *count > 0)
                .map(|(bound, count)| {
                    let label = if *bound == u64::MAX {
                        ">64".to_string()
                    } else {
                        bound.to_string()
                    };
                    format!("{label}:{count}")
                })
                .collect();
            println!(
                "{writers:>8}{:>10}{:>11.1}%   {}",
                size_label(record),
                batchable * 100.0,
                shown.join("  "),
            );
        }
    }
}

// a single writer never has company, the floor the histogram has to show before any
// number above it can be believed
pub fn one_writer_is_never_batchable() {
    // libtest leaves the test name line open, so a header needs a newline ahead of it.
    println!();
    let (buckets, batchable) = measure(1, 4096, 200);
    let alone = buckets[0].1;
    let total: u64 = buckets.iter().map(|(_, count)| count).sum();

    assert!(total > 0, "nothing was written");
    assert_eq!(alone, total, "a lone writer saw company: {buckets:?}");
    assert_eq!(batchable, 0.0);
}

fn size_label(bytes: usize) -> String {
    if bytes >= 1 << 20 {
        format!("{} MiB", bytes >> 20)
    } else if bytes >= 1024 {
        format!("{} KiB", bytes >> 10)
    } else {
        format!("{bytes} B")
    }
}
