//! What a paged open holds while it loads the spot index, on a volume on disk
//!
//! Ignored by default, since it writes REEL_FF_OPEN_KEYS records (two million when
//! unset) and reports the open's time and the process's resident memory around it.

use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::TempDir;

use reel::config::{IndexResidency, ReelConfig, SyncPolicy};
use reel::format::column::{Codec, ColumnId, ColumnSpec, RecordKey};
use reel::units::ByteCount;
use reel::{KeyWidth, ReelStore};

const RECORDS: ColumnId = ColumnId(1);

const COLUMNS: &[ColumnSpec] = &[ColumnSpec {
    id: RECORDS,
    name: "records",
    key_width: KeyWidth::Fixed(16),
    shard_bytes: 1,
    purge_mark: None,
    codec: Codec::None,
}];

/// Payload bytes per record, W4's size
const VALUE_BYTES: usize = 200;

/// Records written when the environment gives no count
const DEFAULT_KEYS: u64 = 2_000_000;

/// One record in this many is written twice, so versions of one key sit in two segments
const OVERWRITE_EVERY: usize = 10;

/// How often the sampler asks for the resident set
const SAMPLE_EVERY: Duration = Duration::from_millis(20);

fn key(at: u64) -> RecordKey {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&at.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes());
    bytes[8..].copy_from_slice(&at.to_be_bytes());
    RecordKey::from_bytes(RECORDS, &bytes).expect("key")
}

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::mb(64),
        sync: SyncPolicy::Never,
        index: IndexResidency::Paged,
        ..ReelConfig::default()
    }
}

/// This process's resident set in KiB, as ps reports it
fn resident_kib() -> u64 {
    let pid = std::process::id().to_string();
    let out = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
        .expect("ps");
    String::from_utf8_lossy(&out.stdout).trim().parse().unwrap_or(0)
}

// a paged open loads the spot index a footer at a time, and every key reads back
#[test]
#[ignore]
fn a_paged_open_loads_the_spot_index_in_bounded_memory() {
    let keys: u64 = std::env::var("REEL_FF_OPEN_KEYS")
        .ok()
        .and_then(|keys| keys.parse().ok())
        .unwrap_or(DEFAULT_KEYS);
    let home = TempDir::new().expect("tempdir");
    let value = vec![0xa5u8; VALUE_BYTES];
    {
        let store = ReelStore::open(home.path().to_path_buf(), config(), COLUMNS).expect("open");
        for at in 0..keys {
            store.put(&key(at), &value).expect("put");
        }
        store.flush().expect("flush");
        for at in (0..keys).step_by(OVERWRITE_EVERY) {
            store.put(&key(at), &value).expect("overwrite");
        }
        store.close().expect("close");
    }

    let before = resident_kib();
    let peak = Arc::new(AtomicU64::new(before));
    let done = Arc::new(AtomicBool::new(false));
    let sampler = {
        let (peak, done) = (Arc::clone(&peak), Arc::clone(&done));
        std::thread::spawn(move || {
            while !done.load(Ordering::Relaxed) {
                peak.fetch_max(resident_kib(), Ordering::Relaxed);
                std::thread::sleep(SAMPLE_EVERY);
            }
        })
    };
    let started = Instant::now();
    let store = ReelStore::open(home.path().to_path_buf(), config(), COLUMNS).expect("reopen");
    let opened = started.elapsed();
    done.store(true, Ordering::Relaxed);
    sampler.join().expect("sampler");
    let after = resident_kib();
    println!(
        "{keys} keys: open {:.2} s, resident {} MiB before, {} MiB peak, {} MiB after, the spot index holds {}",
        opened.as_secs_f64(),
        before / 1024,
        peak.load(Ordering::Relaxed) / 1024,
        after / 1024,
        store.index().spot_held(),
    );
    for at in (0..keys).step_by((keys / 1000).max(1) as usize) {
        assert!(store.get(&key(at)).expect("get").is_some(), "key {at} went missing");
    }
}
