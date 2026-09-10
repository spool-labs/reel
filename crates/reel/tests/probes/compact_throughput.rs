//! What compaction delivers on a device rather than in a model
//!
//! Fills a real volume, kills most of it in a stride, then drives `compact_once` and
//! measures what comes back per second against what the rate was set to. Two things
//! keep the answer honest: the volume is sized past MemTotal, since one that fits in
//! RAM measures the page cache whatever the table says, and a leg counts as idle only
//! when nothing has been reclaimed and no driver is inside a pass, since a rate cap
//! buys exactly a stretch with no reclaim in it. Paced legs are quoted as a share of
//! the unpaced ceiling, absolute caps not travelling across volume sizes, and the
//! `ending` column says whether a row drained or ran out of deadline.
//!
//! Opt-in, since it writes tens of gigabytes. Run with:
//!   cargo test -p tape-reel --release --test probes -- compact_throughput

use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rand::Rng;
use tempfile::TempDir;

use reel::{
    Codec, ColumnId, ColumnSet, ColumnSpec, CompactPass, CompactRate, IoBackend, KeyWidth,
    MapShape, RecordKey, ReelConfig, ReelStore, SyncPolicy,
};

const RECORDS: ColumnId = ColumnId(1);
const BLOB: ColumnId = ColumnId(2);

/// Bytes a record key occupies: two group bytes then a thirty-two byte id
const RECORD_KEY_LEN: usize = 34;

/// Bytes the group takes at the front of a record key
const GROUP_PREFIX_LEN: usize = 2;

/// The columns a volume is opened with, records addressed by group and id
const COLUMNS: ColumnSet = &[
    ColumnSpec {
        id: RECORDS,
        name: "records",
        key_width: KeyWidth::Fixed(RECORD_KEY_LEN as u16),
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

/// The record column with lz4 on, for the compressed legs
///
/// Same id, name and key shape as the plain records column, so `record_key` addresses
/// it unchanged. Coded payloads do not compress, so this prices the metadata case.
const LZ4_COLUMNS: ColumnSet = &[ColumnSpec {
    id: RECORDS,
    name: "records",
    key_width: KeyWidth::Fixed(RECORD_KEY_LEN as u16),
    shard_bytes: 2,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::Lz4,
    map_shape: MapShape::Tree,
}];

/// The record column key for a group and an id
///
/// Big endian group at the front, so the key space groups by group.
fn record_key(group: u16, id: [u8; 32]) -> RecordKey {
    let mut bytes = [0u8; RECORD_KEY_LEN];
    bytes[..GROUP_PREFIX_LEN].copy_from_slice(&group.to_be_bytes());
    bytes[GROUP_PREFIX_LEN..].copy_from_slice(&id);
    RecordKey::from_bytes(RECORDS, &bytes).expect("key")
}

/// A record id nothing else in the run will draw
fn unique_id() -> [u8; 32] {
    let mut id = [0u8; 32];
    rand::thread_rng().fill(&mut id[..]);
    id
}

/// Bytes written before anything is killed
///
/// Past memory, or the rewrite reads out of the page cache and the number is about
/// RAM. `REEL_COMPACT_VOLUME_BYTES` overrides it for a quick run.
fn volume_bytes() -> u64 {
    if let Ok(value) = std::env::var("REEL_COMPACT_VOLUME_BYTES") {
        return value
            .parse()
            .expect("REEL_COMPACT_VOLUME_BYTES is not a number");
    }
    let memory = machine_memory_bytes().unwrap_or(0);
    // A quarter again past memory, floored at forty gibibytes.
    (memory + memory / 4).max(40 * 1024 * 1024 * 1024)
}

/// Total memory, so the volume can be sized past it
fn machine_memory_bytes() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in meminfo.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            return rest
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()
                .map(|kib| kib * 1024);
        }
    }
    None
}

/// One record, at the size a few mebibytes of coded payload splits into
///
/// `REEL_COMPACT_RECORD_BYTES` overrides it, and 4096 is the override that matters:
/// ids are random, so key order never matches offset order, and small records are the
/// shape where the ordered rewrite's traversal dominates.
fn record_bytes() -> usize {
    if let Ok(value) = std::env::var("REEL_COMPACT_RECORD_BYTES") {
        return value
            .parse()
            .expect("REEL_COMPACT_RECORD_BYTES is not a number");
    }
    1024 * 1024
}

/// Fraction of the volume killed before compaction is asked to reclaim it
const KILL_FRACTION: f64 = 0.60;

/// Samples of a still volume a leg takes as the drain being finished
///
/// Sampled at `IDLE_SAMPLE_EVERY`, so this is five seconds of nothing reclaimed and
/// no driver inside a pass.
const IDLE_SAMPLES: u32 = 25;

/// How often the drain loop looks at the volume
const IDLE_SAMPLE_EVERY: Duration = Duration::from_millis(200);

/// What a driver publishes while it is between passes rather than inside one
const NOT_IN_A_PASS: u64 = u64::MAX;

/// Paced legs as a share of the unpaced ceiling, plus the unpaced leg itself
///
/// Absolute rates cannot travel across volume sizes, and whether written bytes track
/// the cap does not depend on the absolute number. A share keeps every leg the same
/// wall-clock length whatever the box is.
const RATE_SHARES: &[f64] = &[0.01, 0.05, 0.25];

/// The shares a run actually takes, `REEL_COMPACT_RATE_SHARES` to trim them
///
/// A 1% leg against tens of gigabytes of dead is hours of wall clock, so a run chasing
/// the unpaced ceiling passes `0.25` or an empty string.
fn rate_shares() -> Vec<f64> {
    match std::env::var("REEL_COMPACT_RATE_SHARES") {
        Ok(list) => list
            .split(',')
            .filter(|part| !part.is_empty())
            .map(|part| {
                part.parse()
                    .expect("REEL_COMPACT_RATE_SHARES entry is not a number")
            })
            .collect(),
        Err(_) => RATE_SHARES.to_vec(),
    }
}

/// Bytes the volume's files actually hold, which is the stored side of the ratio
fn dir_bytes(dir: &std::path::Path) -> u64 {
    let mut total = 0u64;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    for entry in entries.flatten() {
        if let Ok(meta) = entry.metadata() {
            if meta.is_file() {
                total += meta.len();
            } else if meta.is_dir() {
                total += dir_bytes(&entry.path());
            }
        }
    }
    total
}

fn payload(seed: u64, bytes: usize) -> Vec<u8> {
    let mut state = seed | 1;
    let mut out = Vec::with_capacity(bytes);
    let random = if lz4() {
        bytes / LZ4_RATIO_HINT as usize
    } else {
        bytes
    };
    while out.len() < random {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(random);
    out.resize(bytes, 0);
    out
}

/// Backend the legs open with, so a box can race posix against the ring
///
/// `REEL_COMPACT_BACKEND` takes `posix`, `uring` or `uring_direct`.
/// The ring is where a fetch wave becomes outstanding reads; posix serves it as a loop.
fn backend() -> IoBackend {
    match std::env::var("REEL_COMPACT_BACKEND").as_deref() {
        Ok("uring") => IoBackend::Uring,
        Ok("uring_direct") => IoBackend::UringDirect,
        Ok("posix") | Err(_) => IoBackend::Posix,
        Ok(other) => panic!("REEL_COMPACT_BACKEND `{other}` is not a known backend"),
    }
}

/// Whether the legs run compressed, from `REEL_COMPACT_CODEC=lz4`
fn lz4() -> bool {
    match std::env::var("REEL_COMPACT_CODEC").as_deref() {
        Ok("lz4") => true,
        Ok("none") | Err(_) => false,
        Ok(other) => panic!("REEL_COMPACT_CODEC `{other}` is not lz4 or none"),
    }
}

/// Logical bytes per stored byte the compressible payload is built to give
///
/// A third of every record is random and the rest zeros. The volume is sized by this
/// so a compressed fill's stored bytes still exceed memory.
const LZ4_RATIO_HINT: u64 = 3;

/// Segment rewrites driven at once, from `REEL_COMPACT_PASSES`
///
/// The ring only pays with depth, and one pass on one thread is the one shape it
/// cannot win.
fn driver_count() -> u32 {
    std::env::var("REEL_COMPACT_PASSES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1)
}

fn config(compact_mbps: u64) -> ReelConfig {
    ReelConfig {
        // Compaction is the thing under test, so nothing else may pace the
        // device: the scrub is off and writes are not synced per record.
        sync: SyncPolicy::Never,
        scrub_mbps: 0,
        compact_mbps: CompactRate::Mbps(compact_mbps),
        io_backend: backend(),
        ..ReelConfig::default()
    }
}

pub fn reclaim_throughput_by_rate() {
    // libtest leaves the test name line open, so a header needs a newline ahead of it.
    println!();
    // A compressed fill stores about a third of what it is fed, so the logical volume
    // triples to keep the stored bytes past memory.
    let volume = match lz4() {
        true => volume_bytes() * LZ4_RATIO_HINT,
        false => volume_bytes(),
    };
    let record = record_bytes();
    let count = (volume / record as u64) as usize;
    let kill = (count as f64 * KILL_FRACTION) as usize;

    println!(
        "volume {} GiB in {} records of {} KiB, killing {}, memory {} GiB, codec {}",
        volume / (1024 * 1024 * 1024),
        count,
        record / 1024,
        kill,
        machine_memory_bytes().unwrap_or(0) / (1024 * 1024 * 1024),
        if lz4() { "lz4" } else { "none" },
    );
    assert!(
        volume / if lz4() { LZ4_RATIO_HINT } else { 1 } > machine_memory_bytes().unwrap_or(0),
        "a volume inside memory measures the page cache, which is what this test \
         exists to avoid",
    );
    println!(
        "\n{:>10} {:>12} {:>14} {:>14} {:>12} {:>10} {:>12} {:>8} {:>10}",
        "set MB/s",
        "passes",
        "reclaim MB/s",
        "written MB/s",
        "dead start",
        "dead end",
        "read MB/s",
        "amp",
        "ending"
    );

    // The unpaced leg first, because the paced ones are quoted against it.
    let mut rates = vec![0u64];
    let ceiling = std::cell::Cell::new(0f64);

    let mut leg = 0usize;
    while leg < rates.len() {
        let rate = rates[leg];
        leg += 1;
        let dir = TempDir::new().expect("tempdir");
        let columns = if lz4() { LZ4_COLUMNS } else { COLUMNS };
        let store = ReelStore::open(dir.path().to_path_buf(), config(rate), columns).expect("open");
        let group = 7u16;

        let mut ids = Vec::with_capacity(count);
        let body = payload(0x9E37_79B9_7F4A_7C15, record);
        for _ in 0..count {
            let id = unique_id();
            store.put(&record_key(group, id), &body).expect("put");
            ids.push(id);
        }
        store.flush().expect("flush");
        if rate == 0 {
            let stored = dir_bytes(dir.path());
            let gib = (1u64 << 30) as f64;
            println!(
                "stored {:.1} GiB for {:.1} GiB logical, ratio {:.2}",
                stored as f64 / gib,
                volume as f64 / gib,
                volume as f64 / stored.max(1) as f64,
            );
        }

        // Kill in a stride rather than a prefix: a prefix empties whole early segments,
        // and an empty segment is unlinked rather than rewritten, so the pass copies
        // nothing and the rate never binds.
        for (i, id) in ids.iter().enumerate() {
            if (i % 5) < 3 {
                store.delete(&record_key(group, *id)).expect("delete");
            }
        }
        store.flush().expect("flush");
        let _ = kill;

        let dead_start = store.dead_bytes().to_bytes();
        let before = store.compaction_counters().compaction_bytes;
        let read_before = store.compaction_counters().read_bytes;

        // The fill and the kill are most of the wall clock and not what this measures,
        // so this names the boundary for a profiler to wait on.
        println!("drain start");
        std::io::stdout().flush().ok();

        // Drive until the volume stops giving anything back.
        let start = Instant::now();
        let mut idle = 0u32;
        let mut last_dead = dead_start;
        // Derived rather than fixed, since a fixed cap under what the slowest rate
        // needs reads as a stall. It also bounds the wait on a paced gate, so a leg
        // that hits it says so in its row rather than passing itself off as drained.
        let expected_secs = match rate {
            0 => 300.0,
            set => (dead_start as f64 / 1e6) / set as f64,
        };
        let deadline = (expected_secs * 3.0).max(120.0);

        // One driver thread per admitted pass. They share the store, the gate and the
        // plane; what they must not share is a segment, which selection's in-flight
        // claim is what prevents.
        let drivers = driver_count().max(1) as usize;
        let done = AtomicBool::new(false);
        let turns = AtomicU64::new(0);
        // Nanoseconds from the leg's start at which each driver's pass began,
        // `NOT_IN_A_PASS` between passes. Nothing else says a driver is working, since
        // dead bytes and the compaction counters both move only when a pass ends.
        let running: Vec<AtomicU64> = (0..drivers)
            .map(|_| AtomicU64::new(NOT_IN_A_PASS))
            .collect();
        std::thread::scope(|scope| {
            for at in 0..drivers {
                let store = &store;
                let done = &done;
                let turns = &turns;
                let running = &running[at];
                scope.spawn(move || {
                    while !done.load(Ordering::Relaxed) {
                        // A turn is a pass that reclaimed something, judged by the
                        // gauge rather than the verdict: `Copied` only says a pass
                        // believed it did work. Held and Idle sleep rather than spin,
                        // or the drivers spend the cores the passes need.
                        let before = store.dead_bytes().to_bytes();
                        running.store(start.elapsed().as_nanos() as u64, Ordering::Release);
                        let verdict = store.compact_once().expect("compact");
                        running.store(NOT_IN_A_PASS, Ordering::Release);
                        let freed = store.dead_bytes().to_bytes() < before;
                        match verdict {
                            CompactPass::Copied if freed => {
                                turns.fetch_add(1, Ordering::Relaxed);
                            }
                            _ => std::thread::sleep(Duration::from_millis(20)),
                        }
                    }
                });
            }
            // Idle is seconds without reclaim rather than samples, and reclaim alone
            // will not judge it: a paced pass moves its bytes over the wall clock its
            // rate implies, and the compaction counters post a pass's bytes only when
            // it finishes. What a paced leg has to wait out is a driver inside a pass.
            let sample_nanos = IDLE_SAMPLE_EVERY.as_nanos() as u64;
            while idle < IDLE_SAMPLES && start.elapsed().as_secs_f64() < deadline {
                std::thread::sleep(IDLE_SAMPLE_EVERY);
                let dead_now = store.dead_bytes().to_bytes();
                // A pass younger than one sample is a driver that has just gone back
                // for another look, which every drained leg does forever.
                let elapsed_nanos = start.elapsed().as_nanos() as u64;
                let is_copying = running.iter().any(|began| {
                    let began = began.load(Ordering::Acquire);
                    began != NOT_IN_A_PASS && elapsed_nanos.saturating_sub(began) >= sample_nanos
                });
                if dead_now >= last_dead && !is_copying {
                    idle += 1;
                } else {
                    idle = 0;
                }
                last_dead = dead_now;
            }
            done.store(true, Ordering::Relaxed);
        });
        let passes = turns.load(Ordering::Relaxed);
        let elapsed = start.elapsed().as_secs_f64();

        let dead_end = store.dead_bytes().to_bytes();
        let written = store.compaction_counters().compaction_bytes - before;
        let read = store.compaction_counters().read_bytes - read_before;
        let reclaimed = dead_start.saturating_sub(dead_end);

        // The regions the passes swept are roughly the reclaimed dead plus the live
        // bytes copied out. Reads near that are a traversal reading the volume once,
        // and a multiple of it is the window thrashing.
        let swept = (reclaimed + written).max(1);
        let written_mbps = written as f64 / elapsed / 1e6;
        // A leg that ran out of deadline rather than out of work quotes a rate over a
        // window it chose, not over a drain, and the column says so.
        let ending = match idle < IDLE_SAMPLES {
            true => "deadline",
            false => "drained",
        };
        println!(
            "{:>10} {passes:>12} {:>14.0} {:>14.0} {:>11.1}G {:>9.1}G {:>12.0} {:>8.2} {ending:>10}",
            match rate {
                0 => "unpaced".to_string(),
                set => set.to_string(),
            },
            reclaimed as f64 / elapsed / 1e6,
            written_mbps,
            dead_start as f64 / 1e9,
            dead_end as f64 / 1e9,
            read as f64 / elapsed / 1e6,
            read as f64 / swept as f64,
        );

        // The unpaced leg sets the ceiling the paced ones are shares of, so it has to
        // have run before they can be chosen.
        if rate == 0 {
            ceiling.set(written_mbps);
            for share in rate_shares() {
                rates.push(((written_mbps * share) as u64).max(1));
            }
        }
    }
}
