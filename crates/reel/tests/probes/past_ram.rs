//! What a volume does once it no longer fits in memory
//!
//! Sweeps cold reads at three record shapes against a fill sized from MemTotal. Three
//! things make a past-memory number honest: the fill has to beat memory rather than a
//! file's own cache, the sample has to be a stride over distinct keys many times
//! smaller than the fill or the repeats come back warm and print an amplification
//! under one, and the temp directory must not be tmpfs, which this refuses. The
//! headline is device bytes per useful byte rather than throughput.
//!
//! Knobs: REEL_PAST_RAM_SIZES, REEL_PAST_RAM_BACKEND, REEL_PAST_RAM_VOLUME_BYTES.
//!
//! Linux only, and root for `drop_caches`. Opt-in. Run with:
//!   sudo -E cargo test -p reel --release --test probes -- past_ram

#![cfg(target_os = "linux")]

use std::time::Instant;

use rand::{thread_rng, Rng};
use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, CompactRate, IoBackend, KeyWidth, MapShape,
    RecordKey, ReelConfig, ReelStore, SyncPolicy,
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

/// Group the fill writes into
const GROUP: u16 = 7;

/// Record sizes swept, smallest first, overridable for one shape
///
/// The three are the shapes the engine behaves differently at: a metadata row
/// under a filesystem block, a record at a block, and a bulk payload.
const SIZES: &[usize] = &[4096, 65536, 1024 * 1024];

/// Reads taken per size, against a fill many times larger
///
/// Small on purpose: the fill has to dwarf the sample or later reads find the earlier
/// ones' pages resident, which is the collision that fakes an amplification below one.
const READS: usize = 2_000;

/// A record key: the group big endian, then the identifier
fn record_key(group: u16, id: [u8; 32]) -> RecordKey {
    let mut bytes = [0u8; RECORD_KEY_LEN];
    bytes[..GROUP_PREFIX_LEN].copy_from_slice(&group.to_be_bytes());
    bytes[GROUP_PREFIX_LEN..].copy_from_slice(&id);
    RecordKey::from_bytes(RECORDS, &bytes).expect("record key")
}

/// An identifier no other record in the fill holds
fn unique_id() -> [u8; 32] {
    let mut bytes = [0u8; 32];
    thread_rng().fill(&mut bytes[..]);
    bytes
}

fn sizes() -> Vec<usize> {
    match std::env::var("REEL_PAST_RAM_SIZES") {
        Ok(list) => list
            .split(',')
            .filter(|part| !part.is_empty())
            .map(|part| {
                part.parse()
                    .expect("REEL_PAST_RAM_SIZES entry is not a number")
            })
            .collect(),
        Err(_) => SIZES.to_vec(),
    }
}

fn backend() -> IoBackend {
    match std::env::var("REEL_PAST_RAM_BACKEND").as_deref() {
        Ok("uring") => IoBackend::Uring,
        Ok("uring_direct") => IoBackend::UringDirect,
        Ok("posix") | Err(_) => IoBackend::Posix,
        Ok(other) => panic!("REEL_PAST_RAM_BACKEND `{other}` is not a known backend"),
    }
}

/// Total machine memory, which is what the fill has to beat
fn machine_memory_bytes() -> u64 {
    let meminfo = std::fs::read_to_string("/proc/meminfo").expect("read /proc/meminfo");
    for line in meminfo.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kib: u64 = rest
                .split_whitespace()
                .next()
                .expect("MemTotal value")
                .parse()
                .expect("MemTotal is a number");
            return kib * 1024;
        }
    }
    panic!("no MemTotal in /proc/meminfo");
}

/// Bytes the fill holds, a quarter again past memory unless told otherwise
fn volume_bytes() -> u64 {
    if let Ok(value) = std::env::var("REEL_PAST_RAM_VOLUME_BYTES") {
        return value
            .parse()
            .expect("REEL_PAST_RAM_VOLUME_BYTES is not a number");
    }
    let memory = machine_memory_bytes();
    memory + memory / 4
}

/// Refuse a temp directory that is memory, since every row would be a RAM row
fn refuse_tmpfs(dir: &std::path::Path) {
    let mounts = std::fs::read_to_string("/proc/mounts").expect("read /proc/mounts");
    let path = dir.to_string_lossy().to_string();
    for line in mounts.lines() {
        let mut parts = line.split_whitespace();
        let Some(_source) = parts.next() else {
            continue;
        };
        let Some(point) = parts.next() else { continue };
        let Some(kind) = parts.next() else { continue };
        if path.starts_with(point) && (kind == "tmpfs" || kind == "ramfs") {
            panic!(
                "TMPDIR resolves to {point}, which is {kind}: every read would be memory. \
                 Set TMPDIR to a directory on the device under test."
            );
        }
    }
}

/// Empty the page cache so the next read has to reach the device
fn drop_caches() {
    std::fs::write("/proc/sys/vm/drop_caches", "3")
        .expect("write /proc/sys/vm/drop_caches, which needs root");
}

/// Bytes the device has served since boot, for the amplification column
///
/// Sectors are always 512 bytes in `/proc/diskstats` whatever the device's own block
/// size is.
fn device_read_bytes() -> u64 {
    let stats = std::fs::read_to_string("/proc/diskstats").expect("read /proc/diskstats");
    let mut total = 0u64;
    for line in stats.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 6 {
            continue;
        }
        let name = parts[2];
        // Whole devices only: counting a partition and its disk would double every
        // byte. A trailing-digit test gets that wrong on nvme, where whole disks
        // end in digits too, so ask the kernel: whole devices are the entries of
        // /sys/block, partitions are not. Loop devices are listed there and still
        // are not the drive under test.
        if name.starts_with("loop") || !std::path::Path::new(&format!("/sys/block/{name}")).exists()
        {
            continue;
        }
        total += parts[5].parse::<u64>().unwrap_or(0) * 512;
    }
    total
}

fn config(record: usize) -> ReelConfig {
    let mut config = ReelConfig {
        sync: SyncPolicy::Never,
        scrub_mbps: 0,
        io_backend: backend(),
        // Compaction is not what this measures and a pass mid-read would steal the
        // device from the sample.
        compact_mbps: CompactRate::Mbps(1),
        segment_bytes: ByteCount::gb(1),
        ..ReelConfig::default()
    };
    // A segment has to hold many records or the fill is mostly segment rolls, and it
    // has to hold a whole one or the put is refused.
    if record as u64 * 16 > config.segment_bytes.to_bytes() {
        config.segment_bytes = ByteCount::from_bytes(record as u64 * 1024);
    }
    config
}

fn payload(seed: u64, bytes: usize) -> Vec<u8> {
    let mut state = seed | 1;
    let mut out = Vec::with_capacity(bytes);
    while out.len() < bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(bytes);
    out
}

// what a cold read costs once the volume is larger than memory
pub fn cold_reads_past_ram() {
    println!();
    let memory = machine_memory_bytes();
    let volume = volume_bytes();

    assert!(
        volume > memory,
        "a {} GiB fill against {} GiB of memory is a page-cache benchmark",
        volume >> 30,
        memory >> 30,
    );

    println!(
        "memory {} GiB, fill {} GiB, {} reads a size, backend {:?}",
        memory >> 30,
        volume >> 30,
        READS,
        backend(),
    );
    println!(
        "\n{:>10} {:>10} {:>12} {:>12} {:>12} {:>10}",
        "record", "records", "read MB/s", "us/read", "device MB/s", "amp"
    );

    for record in sizes() {
        let count = (volume / record as u64) as usize;
        assert!(
            count > READS * 20,
            "a {count} record fill against {READS} reads lets the sample collide with itself",
        );

        let dir = TempDir::new().expect("tempdir");
        refuse_tmpfs(dir.path());
        let store =
            ReelStore::open(dir.path().to_path_buf(), config(record), COLUMNS).expect("open");

        let body = payload(0x9E37_79B9_7F4A_7C15, record);
        let mut ids = Vec::with_capacity(count);
        for _ in 0..count {
            let id = unique_id();
            store.put(&record_key(GROUP, id), &body).expect("put");
            ids.push(id);
        }
        store.flush().expect("flush");

        // The store's own maps and the kernel's cache both have to forget, so the
        // volume is closed and reopened around the drop.
        drop(store);
        drop_caches();
        let store =
            ReelStore::open(dir.path().to_path_buf(), config(record), COLUMNS).expect("reopen");

        // A stride rather than a prefix or a random walk: a prefix reads one end of the
        // key space and a random walk repeats keys, while a stride visits distinct keys
        // spread across the whole volume.
        let stride = count / READS;
        let device_before = device_read_bytes();
        let start = Instant::now();
        let mut served = 0u64;
        for step in 0..READS {
            let id = ids[step * stride];
            let found = store
                .get(&record_key(GROUP, id))
                .expect("read")
                .expect("record present after reopen");
            served += found.len() as u64;
        }
        let elapsed = start.elapsed().as_secs_f64();
        let device = device_read_bytes().saturating_sub(device_before);

        println!(
            "{:>9}B {:>10} {:>12.1} {:>12.1} {:>12.1} {:>10.2}",
            record,
            count,
            served as f64 / elapsed / 1e6,
            elapsed * 1e6 / READS as f64,
            device as f64 / elapsed / 1e6,
            device as f64 / served.max(1) as f64,
        );

        // Under one means the sample was served from memory somewhere, which makes
        // every column beside it a memory number rather than a device one.
        assert!(
            device as f64 / served.max(1) as f64 >= 1.0,
            "device read {device} bytes to serve {served}, so the sample was warm",
        );
    }
}
