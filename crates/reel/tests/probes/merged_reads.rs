//! What the device sees of one batched read: requests, merges, bytes per request
//!
//! Fill and read are two arms so the read alone can run under perf. Linux only, root
//! for `drop_caches`. `REEL_MERGE_DIR` names the volume, `REEL_MERGE_BACKEND` the plane,
//! `REEL_MERGE_DEVICE` the diskstats row where the volume's own device has none.
//! Opt-in, run with:
//!   cargo test -p reel --release --test probes -- merged_reads::fill
//!   cargo test -p reel --release --test probes -- merged_reads::read

use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::Instant;

use reel::config::{IndexResidency, IoBackend, ReelConfig, SyncPolicy, ThreadBudget};
use reel::format::column::{Codec, ColumnId, ColumnSet, ColumnSpec, MapShape, RecordKey};
use reel::units::ByteCount;
use reel::{KeyWidth, Preallocate, ReelStore};

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

/// The fleet's record shape, enough of them to seal four segments
const RECORD_BYTES: usize = 64 << 10;
const RECORDS: u64 = 4096;

/// Bytes a diskstats sector counts, whatever the device's own block size
const SECTOR_BYTES: u64 = 512;

fn dir() -> PathBuf {
    match std::env::var_os("REEL_MERGE_DIR") {
        Some(path) => PathBuf::from(path),
        None => std::env::temp_dir().join("reel-merged-reads"),
    }
}

fn backend() -> IoBackend {
    match std::env::var("REEL_MERGE_BACKEND").as_deref() {
        Ok("uring_direct") => IoBackend::UringDirect,
        Ok("posix") => IoBackend::Posix,
        Ok("uring") | Err(_) => IoBackend::Uring,
        Ok(other) => panic!("REEL_MERGE_BACKEND `{other}` is not a known backend"),
    }
}

fn config() -> ReelConfig {
    ReelConfig {
        io_backend: backend(),
        segment_bytes: ByteCount::mb(64),
        alloc_chunk: ByteCount::mb(4),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        index: IndexResidency::Resident,
        scrub_mbps: 0,
        ..ReelConfig::default()
    }
}

fn key(at: u64) -> RecordKey {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&at.to_be_bytes());
    RecordKey::from_bytes(ROWS, &bytes).expect("key")
}

/// Reads completed, reads merged and sectors read on the device holding a path
fn device_reads(path: &Path) -> Option<(String, u64, u64, u64)> {
    let named = std::env::var("REEL_MERGE_DEVICE").ok();
    let dev = std::fs::metadata(path).ok()?.dev();
    let (major, minor) = (libc::major(dev) as u64, libc::minor(dev) as u64);
    let stats = std::fs::read_to_string("/proc/diskstats").ok()?;
    for line in stats.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 6 {
            continue;
        }
        let matched = match &named {
            Some(name) => parts[2] == name,
            None => {
                parts[0].parse::<u64>().ok() == Some(major)
                    && parts[1].parse::<u64>().ok() == Some(minor)
            }
        };
        if !matched {
            continue;
        }
        let field = |at: usize| parts[at].parse::<u64>().ok();
        return Some((parts[2].to_string(), field(3)?, field(4)?, field(5)?));
    }
    None
}

/// Write the records in key order, which is disk order, and leave the volume for the read
pub fn fill() {
    let dir = dir();
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("volume dir");
    let store = ReelStore::open(dir.clone(), config(), COLUMNS).expect("open");
    let payload = vec![0x3Cu8; RECORD_BYTES];
    for at in 0..RECORDS {
        store.put(&key(at), &payload).expect("put");
    }
    store.flush().expect("flush");
    drop(store);
    println!(
        "filled {RECORDS} records of {RECORD_BYTES} bytes at {}",
        dir.display()
    );
}

/// One batched read of every key in write order, with the device's counters around it
pub fn read() {
    let dir = dir();
    let store = ReelStore::open(dir.clone(), config(), COLUMNS).expect("open");
    let dropped = std::fs::write("/proc/sys/vm/drop_caches", "3").is_ok();
    let asked: Vec<RecordKey> = (0..RECORDS).map(key).collect();

    let before = device_reads(&dir);
    let began = Instant::now();
    let found = store.get_many(&asked).expect("get many");
    let elapsed = began.elapsed();
    let after = device_reads(&dir);

    let present = found.iter().flatten().count();
    println!(
        "backend={:?} serving={:?} dropped_caches={dropped} present={present} ms={}",
        backend(),
        store.serving_backend(),
        elapsed.as_millis(),
    );
    let (Some((device, reads0, merges0, sectors0)), Some((_, reads1, merges1, sectors1))) =
        (before, after)
    else {
        println!("device counters unavailable for {}", dir.display());
        return;
    };
    let (reads, merges, sectors) = (reads1 - reads0, merges1 - merges0, sectors1 - sectors0);
    let per_request_kib = match reads {
        0 => 0.0,
        reads => (sectors * SECTOR_BYTES) as f64 / reads as f64 / 1024.0,
    };
    let merged_pct = match reads + merges {
        0 => 0.0,
        all => merges as f64 * 100.0 / all as f64,
    };
    println!(
        "device={device} requests={reads} merges={merges} read_mib={:.1} \
         per_request_kib={per_request_kib:.1} merged_pct={merged_pct:.1}",
        (sectors * SECTOR_BYTES) as f64 / (1 << 20) as f64,
    );
}
