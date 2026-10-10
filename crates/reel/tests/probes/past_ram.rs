//! Cold read cost and device amplification once the volume no longer fits in memory
//! Linux, as root: `sudo -E cargo test -p tape-reel --release --test probes -- past_ram`

#![cfg(target_os = "linux")]

use std::time::Instant;

use rand::{thread_rng, Rng};
use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, CompactRate, IoBackend, KeyWidth, RecordKey,
    ReelConfig, ReelStore, SyncPolicy,
};

/// The group takes this many bytes at the front of a record key
const GROUP_PREFIX_LEN: usize = 2;

/// Length of a record key
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
        purge_mark: None,
        codec: Codec::None,
    },
    ColumnSpec {
        id: BLOB,
        name: BLOB_CF,
        key_width: KeyWidth::Fixed(32),
        shard_bytes: 0,
        purge_mark: None,
        codec: Codec::None,
    },
];

/// The fill writes into this group
const GROUP: u16 = 7;

/// Record sizes swept, smallest first, unless REEL_PAST_RAM_SIZES is set
const SIZES: &[usize] = &[4096, 65536, 1024 * 1024];

/// Reads per size, few enough that the fill dwarfs the sample
const READS: usize = 2_000;

/// A record key: the group big endian, then the identifier
fn record_key(group: u16, id: [u8; 32]) -> RecordKey {
    let mut bytes = [0u8; RECORD_KEY_LEN];
    bytes[..GROUP_PREFIX_LEN].copy_from_slice(&group.to_be_bytes());
    bytes[GROUP_PREFIX_LEN..].copy_from_slice(&id);
    RecordKey::from_bytes(RECORDS, &bytes).expect("record key")
}

/// A random identifier, unique within the fill
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

/// The fill holds this many bytes, a quarter past memory unless REEL_PAST_RAM_VOLUME_BYTES is set
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

/// How many bytes the device has served since boot, for the amplification column
fn device_read_bytes() -> u64 {
    let stats = std::fs::read_to_string("/proc/diskstats").expect("read /proc/diskstats");
    let mut total = 0u64;
    for line in stats.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 6 {
            continue;
        }
        let name = parts[2];
        // Whole devices only, which are the entries of /sys/block, and no loop devices
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
        // A pass mid-read would steal the device from the sample, so throttle compaction
        compact_mbps: CompactRate::Mbps(1),
        segment_bytes: ByteCount::gb(1),
        ..ReelConfig::default()
    };
    // A segment has to hold many records, or the fill is mostly segment rolls
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

        // Close and reopen around the drop so the store's maps and the kernel's cache both forget
        drop(store);
        drop_caches();
        let store =
            ReelStore::open(dir.path().to_path_buf(), config(record), COLUMNS).expect("reopen");

        // A stride visits distinct keys spread across the whole volume
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

        // Under one means the sample was served from memory somewhere
        assert!(
            device as f64 / served.max(1) as f64 >= 1.0,
            "device read {device} bytes to serve {served}, so the sample was warm",
        );
    }
}
