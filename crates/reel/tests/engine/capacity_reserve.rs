//! The reserve refusing a foreground write before the disk is full
//!
//! An append-only volume that fills its disk cannot compact its way out, because
//! compaction has to write the survivors somewhere. The engine holds a segment
//! back from foreground writes for exactly that, and this checks the guard fires
//! while the space it is protecting is still there.
//!
//! It needs a filesystem small enough to reach a ceiling, so the volume is a loopback
//! file mounted for the test and torn down after, which needs root and Linux. Without
//! either, the test says what it skipped rather than passing quietly.
//!
//! Ignored by default. Run with:
//!   cargo test -p reel --test capacity_reserve --release -- --ignored --nocapture

#![cfg(target_os = "linux")]

use std::path::{Path, PathBuf};
use std::process::Command;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, CompactPass, KeyWidth, MapShape, RecordKey,
    ReelConfig, ReelError, ReelStore, SyncPolicy, ThreadBudget,
};

const RECORDS: ColumnId = ColumnId(1);
const BLOB: ColumnId = ColumnId(2);

const COLUMNS: ColumnSet = &[
    ColumnSpec {
        id: RECORDS,
        name: "records",
        key_width: KeyWidth::Fixed(34),
        shard_bytes: 2,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
    ColumnSpec {
        id: BLOB,
        name: "blob_data",
        key_width: KeyWidth::Fixed(32),
        shard_bytes: 0,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
];

/// Backing file for the loopback filesystem
const IMAGE_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// One segment; the reserve is this per tail plus one for compaction
const SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// Tails, pinned so the reserve the test reasons about does not vary by box
const TAILS: u32 = 4;

/// What the engine holds back: a segment per tail, plus one for compaction
const RESERVE_BYTES: u64 = SEGMENT_BYTES * (TAILS as u64 + 1);

const RECORD_BYTES: usize = 1024 * 1024;

fn unique_id() -> [u8; 32] {
    rand::random()
}

fn record_key(group: u16, id: [u8; 32]) -> RecordKey {
    let mut bytes = [0u8; 34];
    bytes[..2].copy_from_slice(&group.to_be_bytes());
    bytes[2..].copy_from_slice(&id);
    RecordKey::from_bytes(RECORDS, &bytes).expect("key")
}

/// A mounted loopback filesystem that unmounts itself when dropped
struct Loopback {
    image: PathBuf,
    mount: PathBuf,
}

impl Loopback {
    /// Build, format and mount a small ext4, or say why not
    fn mount(tag: &str) -> Result<Loopback, String> {
        if !is_root() {
            return Err("not root, so nothing can be mounted".to_string());
        }
        let image = PathBuf::from(format!("/tmp/reel-reserve-{tag}.img"));
        let mount = PathBuf::from(format!("/tmp/reel-reserve-{tag}.mnt"));
        let _ = std::fs::remove_file(&image);
        let _ = std::fs::create_dir_all(&mount);

        let image_path = image.to_str().expect("image path is utf8");
        let mount_path = mount.to_str().expect("mount path is utf8");
        run("truncate", &["-s", &IMAGE_BYTES.to_string(), image_path])?;
        // Zero reserved blocks, so the numbers the test reasons about are the
        // ones the filesystem will actually hand out.
        run("mkfs.ext4", &["-q", "-F", "-m", "0", image_path])?;
        run("mount", &["-o", "loop", image_path, mount_path])?;

        Ok(Loopback { image, mount })
    }

    fn path(&self) -> &Path {
        &self.mount
    }
}

impl Drop for Loopback {
    fn drop(&mut self) {
        let _ = Command::new("umount").arg(&self.mount).status();
        let _ = std::fs::remove_dir(&self.mount);
        let _ = std::fs::remove_file(&self.image);
    }
}

fn is_root() -> bool {
    // Cheaper than pulling a dependency in for one number.
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find_map(|line| line.strip_prefix("Uid:"))
                .and_then(|rest| rest.split_whitespace().next().map(|uid| uid == "0"))
        })
        .unwrap_or(false)
}

fn run(program: &str, args: &[&str]) -> Result<(), String> {
    let out = Command::new(program)
        .args(args)
        .output()
        .map_err(|why| format!("{program} did not start: {why}"))?;
    if out.status.success() {
        return Ok(());
    }
    Err(format!(
        "{program} failed: {}",
        String::from_utf8_lossy(&out.stderr).trim()
    ))
}

/// Bytes the filesystem still has free, which is what the reserve protects
fn free_bytes(path: &Path) -> u64 {
    let out = Command::new("df")
        .args(["-B1", "--output=avail"])
        .arg(path)
        .output()
        .expect("df");
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .nth(1)
        .and_then(|line| line.trim().parse().ok())
        .unwrap_or(0)
}

/// Bytes the volume's files actually occupy, which is what ENOSPC counts
fn disk_used(path: &Path) -> u64 {
    let out = Command::new("du")
        .args(["-sb"])
        .arg(path)
        .output()
        .expect("du");
    String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .next()
        .and_then(|bytes| bytes.parse().ok())
        .unwrap_or(0)
}

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(SEGMENT_BYTES),
        active_tails: ThreadBudget::threads(TAILS),
        sync: SyncPolicy::Never,
        scrub_mbps: 0,
        ..ReelConfig::default()
    }
}

fn payload() -> Vec<u8> {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut out = Vec::with_capacity(RECORD_BYTES);
    while out.len() < RECORD_BYTES {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(RECORD_BYTES);
    out
}

// the reserve refuses a write while a segment's worth of space is still free
#[test]
#[ignore = "mounts a loopback filesystem; needs root on Linux"]
fn reserve_refuses_before_the_disk_is_full() {
    println!();
    let volume = match Loopback::mount("refuse") {
        Ok(volume) => volume,
        Err(why) => {
            println!("skipped: {why}");
            return;
        }
    };

    let store = ReelStore::open(volume.path().to_path_buf(), config(), COLUMNS).expect("open");
    let facts = store.bias().expect("a mounted filesystem reports its size");
    let capacity = facts.volume_capacity_bytes.expect("capacity");
    println!(
        "volume {} MiB, {TAILS} tails, reserve {} MiB, ceiling {} MiB",
        capacity / (1024 * 1024),
        RESERVE_BYTES / (1024 * 1024),
        (capacity - RESERVE_BYTES) / (1024 * 1024),
    );

    let group = 3u16;
    let body = payload();
    let mut written = Vec::new();
    let mut refusal = None;

    // The slow half of the door, watched across the fill. A volume that goes
    // from its full budget straight to a refusal never entered the band.
    let full_budget = store.write_budget_bytes().to_bytes();
    let mut slowest = full_budget;

    // The ceiling is republished by the maintenance tick rather than per put, so the
    // tick has to be driven or the footprint the guard reads never moves off zero.
    for i in 0..(capacity / RECORD_BYTES as u64 + 64) {
        let id = unique_id();
        match store.put(&record_key(group, id), &body) {
            Ok(()) => written.push(id),
            Err(ReelError::Rejected(why)) => {
                refusal = Some(why);
                break;
            }
            Err(other) => {
                // The disk filling before the guard fires is the deadlock this exists
                // to catch, so report what the guard was looking at.
                let accounted = store.totals().bytes.to_bytes() + store.dead_bytes().to_bytes();
                panic!(
                    "put failed for a reason that is not the reserve: {other}\n  \
                     wrote {} records of 1 MiB\n  \
                     accounted live plus dead {} MiB\n  \
                     actually on disk {} MiB, free {} MiB\n  \
                     ceiling was {} MiB",
                    written.len(),
                    accounted / (1024 * 1024),
                    disk_used(volume.path()) / (1024 * 1024),
                    free_bytes(volume.path()) / (1024 * 1024),
                    (capacity - RESERVE_BYTES) / (1024 * 1024),
                );
            }
        }
        if i % 16 == 0 {
            store.maintain_once().expect("maintain");
            slowest = slowest.min(store.write_budget_bytes().to_bytes());
        }
    }

    let why = refusal.expect(
        "the volume took every write a 2 GiB filesystem could hold without the reserve \
         refusing one, so the guard never fired",
    );
    println!("refused after {} records: {why}", written.len());
    println!(
        "write budget fell from {} MiB to {} MiB before the refusal",
        full_budget / (1024 * 1024),
        slowest / (1024 * 1024),
    );
    assert!(
        slowest < full_budget,
        "the volume ran at its full write budget until it was refused, so the \
         slowdown band never opened and writers saw a cliff rather than a slope",
    );
    assert!(
        why.contains("compaction"),
        "the refusal says what the space is being held for: {why}",
    );

    // The point of refusing early is that compaction still has room, so a filesystem
    // out of space here means the guard fired too late.
    let free = free_bytes(volume.path());
    println!(
        "free at refusal {} MiB, one segment is {} MiB",
        free / (1024 * 1024),
        SEGMENT_BYTES / (1024 * 1024),
    );
    assert!(
        free >= SEGMENT_BYTES,
        "refused with only {free} bytes free, which is under the {SEGMENT_BYTES} byte \
         segment compaction needs to write survivors into",
    );

    // And compaction has to be able to work inside that band.
    for (i, id) in written.iter().enumerate() {
        if (i % 5) < 3 {
            store.delete(&record_key(group, *id)).expect("delete");
        }
    }
    store.flush().expect("flush");
    let dead_start = store.dead_bytes().to_bytes();

    let mut passes = 0;
    let mut copied = 0;
    for _ in 0..2000 {
        match store.compact_once().expect("compact inside the reserve") {
            CompactPass::Copied => {
                copied += 1;
                passes += 1;
            }
            CompactPass::Idle => break,
            CompactPass::Held => std::thread::sleep(std::time::Duration::from_millis(5)),
        }
        if passes > 0 && store.dead_bytes().to_bytes() == 0 {
            break;
        }
    }
    let dead_end = store.dead_bytes().to_bytes();
    println!(
        "compaction inside the reserve: {copied} copying passes, dead {:.1}G to {:.1}G",
        dead_start as f64 / 1e9,
        dead_end as f64 / 1e9,
    );
    assert!(
        dead_end < dead_start,
        "compaction reclaimed nothing from inside the reserve, which is the deadlock \
         the reserve exists to prevent",
    );

    // Having reclaimed, the volume takes writes again.
    store.maintain_once().expect("maintain");
    let id = unique_id();
    store
        .put(&record_key(group, id), &body)
        .expect("a volume that has just been compacted takes a write again");
    println!("took a write again after reclaiming");
}
