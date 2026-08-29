//! Raw put and get throughput, and the CPU terms underneath them
//!
//! The engine measured as a store of arbitrary bytes. The CPU probes measure checksum,
//! copy and index-insert rates with no io at all, which is the ceiling this machine
//! could ever reach; the matrix then drives the real store across backends, page cache
//! settings, sync policies, thread counts, record sizes and write modes, so the gap
//! between the two says whether a cell is CPU bound, syscall bound or device bound.
//!
//! Point `REEL_RAW_DIR` at the filesystem under test. A run on tmpfs measures the engine
//! with no block device beneath it, and a run whose total bytes exceed the machine's
//! memory measures the drive; a run that is neither measures the page cache and says
//! nothing about either.
//!
//! On the key sweep, read the microsecond columns rather than the MB/s ones: a wider
//! key grows the record without growing the payload the rate is computed from.
//!
//! Knobs, all optional: `REEL_RAW_BACKEND`, `REEL_RAW_SYNC`,
//! `REEL_RAW_SIZES`, `REEL_RAW_KEYS`, `REEL_RAW_KEY_SHAPES`, `REEL_RAW_THREADS`,
//! `REEL_RAW_WRITE`, `REEL_RAW_BYTES`, `REEL_RAW_SHARDS`, `REEL_RAW_MAX_OPS`,
//! `REEL_RAW_TAILS`, `REEL_RAW_SEGMENT`, `REEL_RAW_VOLUMES`, `REEL_RAW_DROP_CACHES`,
//! `REEL_RAW_RANDOM_READS`, `REEL_RAW_SKIP_READS`, `REEL_RAW_WEIGH`, `REEL_RAW_CSV`.
//!
//! Ignored by default. Run with:
//!   cargo test -p reel --test raw_throughput --release -- --ignored --nocapture --test-threads=1

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::RefCell;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use crc_fast::CrcAlgorithm;
use tempfile::TempDir;

use reel::config::VolumeSpec;
use reel::format::loc::{Loc, SegmentId};
use reel::format::lsn::Lsn;
use reel::format::record::checksum;
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, IndexResidency, IoBackend, KeyWidth,
    MapShape, RecordKey, RecordWrite, ReelConfig, ReelIndex, ReelStore, ShardShapes, SyncPolicy,
    ThreadBudget, INLINE_KEY_LEN, MAP_EVERYTHING, MAX_KEY_LEN,
};

/// The column this bench writes into
///
/// The shard count matters: the resident index splits a column by leading key bytes, so
/// a column with one shard puts every writer behind one lock and measures that lock.
const BENCH_COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: "raw",
    key_width: KeyWidth::Fixed(KEY_WIDTH as u16),
    shard_bytes: 2,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

/// The record column of the fixture set, keyed by a group and an id
const RECORDS: ColumnId = ColumnId(1);

/// The blob column of the fixture set, keyed by an id alone
const BLOB: ColumnId = ColumnId(2);

/// Bytes the group takes at the front of a record key
const GROUP_PREFIX_LEN: usize = 2;

/// Bytes a content address occupies
const ID_LEN: usize = 32;

/// Bytes a record key occupies
const RECORD_KEY_LEN: usize = GROUP_PREFIX_LEN + ID_LEN;

/// A caller-declared column set of the shape a deployment hands the engine
///
/// Records lead with the group, so their shards are the groups a volume holds; blobs
/// lead with a content address, which is uniform, so sharding them buys nothing.
const TEST_COLUMNS: ColumnSet = &[
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
        key_width: KeyWidth::Fixed(ID_LEN as u16),
        shard_bytes: 0,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
];

/// Bytes a bench key occupies unless the matrix is sweeping the width
const KEY_WIDTH: usize = 16;

/// Key widths the matrix sweeps
const DEFAULT_KEYS: &str = "16";

/// Key shapes the matrix sweeps, a fixed-width column against a variable one
const DEFAULT_KEY_SHAPES: &str = "fixed";

/// The fixed key widths the resident index is monomorphised over
///
/// A fixed column of any other width is refused at open, so the matrix checks the list
/// before it runs. Anything wider than `INLINE_KEY_LEN` needs a variable column.
const INDEXED_WIDTHS: [u64; 11] = [8, 12, 16, 24, 32, 34, 40, 44, 48, 72, 108];

/// Bytes each matrix cell writes, before the read phase reads them back
const DEFAULT_CELL_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Record sizes the matrix sweeps, spanning header-dominated to device-dominated
const DEFAULT_SIZES: &str = "100,4096,65536,1048576";

/// Writer and reader counts the matrix sweeps
const DEFAULT_THREADS: &str = "1,4,16";

/// Write modes the matrix sweeps, one record per call against batched runs
const DEFAULT_WRITES: &str = "put,batch16";

/// Shards the keys are spread across, which is one reel and one tail each
const DEFAULT_SHARDS: u64 = 8;

/// Records one cell will write however small they are
///
/// Without a cap a two gigabyte cell of hundred byte records is twenty million keys and
/// about a gigabyte of resident index, which measures the index and not the write path.
const DEFAULT_MAX_OPS: u64 = 2_000_000;

/// Bytes each CPU probe moves in total, whatever buffer it moves them through
const CPU_TOTAL_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Buffer sizes the streaming probes run at
///
/// The small one fits in cache and is the per-record case; the large one fits in no last
/// level cache, so it says whether a core keeps up with a drive streaming at GB/s.
const CPU_BUFFERS: [usize; 2] = [1024 * 1024, 256 * 1024 * 1024];

/// Record sizes the per-call probe charges the checksum at
const CPU_RECORDS: [usize; 4] = [100, 4096, 65_536, 1024 * 1024];

/// The wider checksum the shipped one is charged against
const CRC64: CrcAlgorithm = CrcAlgorithm::Crc64Nvme;

/// The checksum the record header ships
///
/// Named here rather than reached through `record::checksum`, so a column labelled crc64
/// measures crc64 whatever the header currently uses.
const CRC32C: CrcAlgorithm = CrcAlgorithm::Crc32Iscsi;

/// Vector the shipped entry point is held to the algorithm this probe names
const CHECK_VECTOR: &[u8] = b"123456789";

/// Keys the index probe inserts
const INDEX_KEYS: usize = 1_000_000;

/// Group counts the index probe spreads those keys over, which is its shard count
const INDEX_GROUPS: [u64; 3] = [1, 8, 50];

/// Writer counts the index probe drives those inserts from
const INDEX_WRITERS: [usize; 3] = [1, 4, 8];

/// Rounds each index cell runs, of which the fastest is the one reported
const INDEX_ROUNDS: usize = 5;

/// Whether allocations are being weighed
///
/// Off, this is one relaxed load per allocation, so a weighed run stays comparable
/// against a run taken without the scale.
static WEIGHING: AtomicBool = AtomicBool::new(false);

/// One thread's running allocation totals
///
/// Per thread rather than global: a global counter would put every allocation in the
/// engine on one contended cache line, which taxes the cells the scale exists to weigh.
#[derive(Clone, Copy, Default)]
struct Scales {
    /// Bytes handed to this thread, only ever counting upward
    taken: u64,

    /// Allocation calls this thread made
    calls: u64,

    /// Bytes handed to this thread and not yet given back, if it frees its own
    held: i64,
}

thread_local! {
    static SCALE: std::cell::Cell<Scales> = const {
        std::cell::Cell::new(Scales { taken: 0, calls: 0, held: 0 })
    };
}

/// Zero the calling thread's scale
fn weigh_reset() {
    SCALE.set(Scales::default());
}

/// Read the calling thread's scale
fn weigh_read() -> Scales {
    SCALE.get()
}

/// An allocator that can be asked what a structure cost to build
struct Scale;

unsafe impl GlobalAlloc for Scale {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if WEIGHING.load(Ordering::Relaxed) {
            // A thread late enough in teardown to have lost its locals is past anything
            // the scale is weighing, so a failed lookup is dropped rather than counted.
            let _ = SCALE.try_with(|scale| {
                let mut now = scale.get();
                now.taken += layout.size() as u64;
                now.calls += 1;
                now.held += layout.size() as i64;
                scale.set(now);
            });
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if WEIGHING.load(Ordering::Relaxed) {
            let _ = SCALE.try_with(|scale| {
                let mut now = scale.get();
                now.held -= layout.size() as i64;
                scale.set(now);
            });
        }
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if WEIGHING.load(Ordering::Relaxed) {
            let _ = SCALE.try_with(|scale| {
                let mut now = scale.get();
                now.taken += new_size as u64;
                now.calls += 1;
                now.held += new_size as i64 - layout.size() as i64;
                scale.set(now);
            });
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Scale = Scale;

/// Odd stride that walks a record set in an order unrelated to its layout
const READ_STRIDE: u64 = 0x9E37_79B9_7F4A_7C15 | 1;

/// Shortest phase worth quoting a throughput from
const MIN_PHASE_SECS: f64 = 1.0;

fn env_string(name: &str, fallback: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| fallback.to_string())
}

/// A comma separated number list from the environment, or the fallback
///
/// Loud on anything it cannot read: dropping a malformed entry quietly is how a mistyped
/// axis becomes a run measuring a different cell than the one that was asked for.
fn env_list(name: &str, fallback: &str) -> Vec<u64> {
    let raw = env_string(name, fallback);
    let list: Vec<u64> = raw
        .split(',')
        .map(|item| {
            item.trim().parse().unwrap_or_else(|_| {
                panic!("{name} holds {raw:?}, which is not a comma separated number list")
            })
        })
        .collect();
    assert!(!list.is_empty(), "{name} is empty");
    list
}

/// A byte count, in bytes or with a binary suffix
///
/// Cell sizes are quoted in GiB everywhere they are discussed, so `16GiB` is what gets
/// typed.
fn env_bytes(name: &str, fallback: u64) -> u64 {
    let Some(value) = std::env::var(name).ok() else {
        return fallback;
    };
    let text = value.trim();
    let suffixes = [("GiB", 1u64 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)];
    for (suffix, scale) in suffixes {
        if let Some(count) = text
            .strip_suffix(suffix)
            .or_else(|| text.strip_suffix(&suffix.to_lowercase()))
        {
            return count.trim().parse::<u64>().unwrap_or_else(|_| {
                panic!("{name} holds {value:?}, whose {suffix} count is not a number")
            }) * scale;
        }
    }
    text.parse().unwrap_or_else(|_| {
        panic!("{name} holds {value:?}, which is not a byte count or a KiB/MiB/GiB of one")
    })
}

fn backends() -> Vec<(String, IoBackend)> {
    env_string("REEL_RAW_BACKEND", "posix")
        .split(',')
        .map(|name| {
            let backend = match name.trim() {
                "posix" => IoBackend::Posix,
                "uring" => IoBackend::Uring,
                "uring_direct" => IoBackend::UringDirect,
                // Dropping the name would run the sweep a column short and print a table
                // that looks complete.
                other => panic!("REEL_RAW_BACKEND holds `{other}`, which is not a backend"),
            };
            (name.trim().to_string(), backend)
        })
        .collect()
}

/// How a cell's writers hand their records to the store
#[derive(Clone, Copy, Eq, PartialEq)]
enum WriteMode {
    /// One record per call, each its own reservation and its own durability point
    PerRecord,

    /// This many records per call, one reservation and one durability point
    Batched(usize),
}

impl WriteMode {
    /// Records one call carries
    fn span(self) -> u64 {
        match self {
            WriteMode::PerRecord => 1,
            WriteMode::Batched(records) => records as u64,
        }
    }
}

/// Write modes, given as `put` or `batchN` for a run of N records per call
fn writes() -> Vec<(String, WriteMode)> {
    env_string("REEL_RAW_WRITE", DEFAULT_WRITES)
        .split(',')
        .map(|name| {
            let name = name.trim();
            if name == "put" {
                return (name.to_string(), WriteMode::PerRecord);
            }
            // Dropping the name would run the sweep a column short and print a table
            // that looks complete.
            let records: usize = name
                .strip_prefix("batch")
                .and_then(|count| count.parse().ok())
                .filter(|records| *records > 0)
                .unwrap_or_else(|| {
                    panic!("REEL_RAW_WRITE holds `{name}`, which is not put or batchN")
                });
            (name.to_string(), WriteMode::Batched(records))
        })
        .collect()
}

/// Key shapes the matrix sweeps, a fixed-width column against a variable one
///
/// Two different index paths at the same width, which separates what declaring a column
/// variable costs from what a wide key costs.
fn key_shapes() -> Vec<(String, bool)> {
    env_string("REEL_RAW_KEY_SHAPES", DEFAULT_KEY_SHAPES)
        .split(',')
        .map(|name| match name.trim() {
            "fixed" => ("fixed".to_string(), false),
            "var" => ("var".to_string(), true),
            other => panic!("unknown key shape {other}, expected fixed or var"),
        })
        .collect()
}

/// Sync policies, given as `never`, `0` for every put, or a byte cadence
fn syncs() -> Vec<(String, SyncPolicy)> {
    env_string("REEL_RAW_SYNC", "never")
        .split(',')
        .filter_map(|name| {
            let name = name.trim();
            let policy = match name {
                "never" => SyncPolicy::Never,
                "0" => SyncPolicy::EveryPut,
                bytes => SyncPolicy::Bytes(ByteCount::from_bytes(bytes.parse().ok()?)),
            };
            Some((name.to_string(), policy))
        })
        .collect()
}

/// A payload of incompressible bytes, so nothing downstream can cheat on it
fn payload(size: usize) -> Vec<u8> {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut out = Vec::with_capacity(size + 8);
    while out.len() < size {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(size);
    out
}

thread_local! {
    /// The buffer every key of this thread is cut from
    ///
    /// Held rather than built per call, so key construction costs the same at sixteen
    /// bytes as at a kibibyte and every width-dependent cost in a row is the engine's.
    static KEY_BUF: RefCell<[u8; MAX_KEY_LEN]> = const {
        RefCell::new([0x5A; MAX_KEY_LEN])
    };
}

/// The key for one record, derived from its index so a reader can rebuild it
///
/// The scrambled half leads, so consecutive records land in different index shards and
/// the ordered map is not built by a sorted insert, which is a BTreeMap's cheapest case.
fn key_at(index: u64, width: usize) -> RecordKey {
    KEY_BUF.with(|buf| {
        let mut buf = buf.borrow_mut();
        buf[0..8].copy_from_slice(&index.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes());
        buf[8..16].copy_from_slice(&index.to_le_bytes());
        RecordKey::from_bytes(ColumnId(1), &buf[..width]).expect("key fits")
    })
}

/// The bench column at one key width, declared fixed or variable
///
/// Leaked because a `ColumnSet` is a `&'static [ColumnSpec]` and the swept width is only
/// known at run time. One spec per cell, in a test binary that exits.
fn bench_columns(key_width: usize, variable: bool) -> ColumnSet {
    let spec = ColumnSpec {
        id: ColumnId(1),
        name: "raw",
        key_width: if variable {
            KeyWidth::Variable
        } else {
            KeyWidth::Fixed(key_width as u16)
        },
        shard_bytes: 2,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    };
    &Box::leak(Box::new([spec]))[..]
}

/// Extra volume roots from the environment, the sweep's multi-device axis
///
/// Comma separated paths, `:capacity` marking the capacity tier, empty for a
/// single-volume run. The tail floor rises with the list on its own.
fn raw_volumes() -> Vec<VolumeSpec> {
    env_string("REEL_RAW_VOLUMES", "")
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| match entry.strip_suffix(":capacity") {
            Some(path) => VolumeSpec::capacity(path),
            None => VolumeSpec::fast(entry),
        })
        .collect()
}

fn config(backend: IoBackend, sync: SyncPolicy) -> ReelConfig {
    let shipped = ReelConfig::default();
    ReelConfig {
        io_backend: backend,
        sync,
        volumes: raw_volumes(),
        // The sweep can name a segment size; zero keeps what ships.
        segment_bytes: match env_bytes("REEL_RAW_SEGMENT", 0) {
            0 => shipped.segment_bytes,
            bytes => ByteCount::from_bytes(bytes),
        },
        // One reel serves every writer, so the tail count is the parallelism knob. Zero
        // takes the shipped default, which resolves against the machine.
        active_tails: ThreadBudget::threads(env_bytes("REEL_RAW_TAILS", 0) as u32),
        // The maintenance plane is not under test and would take the device away from
        // the phase that is.
        scrub_mbps: 0,
        // A direct volume bypasses the page cache a mapping reads and pairing the two is
        // refused at validation, so the direct leg keeps the driver.
        map_above: match backend.is_direct() {
            true => None,
            false => MAP_EVERYTHING,
        },
        ..ReelConfig::default()
    }
}

/// Nanoseconds one call costs, averaged over a round count
fn charge<Work>(rounds: u64, mut work: Work) -> f64
where
    Work: FnMut(),
{
    let start = Instant::now();
    for _ in 0..rounds {
        work();
    }
    start.elapsed().as_secs_f64() * 1e9 / rounds as f64
}

/// Run a phase across threads and return the elapsed seconds
fn timed<Work>(threads: u64, work: Work) -> f64
where
    Work: Fn(u64) + Sync,
{
    let start = Instant::now();
    std::thread::scope(|scope| {
        for thread in 0..threads {
            let work = &work;
            scope.spawn(move || work(thread));
        }
    });
    start.elapsed().as_secs_f64()
}

// what one core charges for the work the engine does per byte and per record
#[test]
#[ignore = "cpu probe, run explicitly"]
fn cpu_terms() {
    // libtest leaves the test name line open, so a header printed into it starts a
    // screen-width right of the rows underneath.
    println!();
    assert_eq!(
        checksum(CHECK_VECTOR),
        crc_fast::checksum(CRC32C, CHECK_VECTOR) as u32,
        "the shipped checksum is no longer crc32c, so this probe's column headings lie",
    );

    println!("cpu terms, one core, no io");
    println!(
        "\n{:>12} {:>12} {:>12} {:>12} {:>12} {:>16}",
        "buffer", "crc64 MB/s", "c32c MB/s", "xxh3 MB/s", "memcpy MB/s", "c32c+copy"
    );

    for size in CPU_BUFFERS {
        let buffer = payload(size);
        let mut copy = vec![0u8; size];
        let rounds = (CPU_TOTAL_BYTES / size as u64).max(1);
        let per_round = size as f64;

        let crc64_mbps = per_round
            / charge(rounds, || {
                std::hint::black_box(crc_fast::checksum(CRC64, std::hint::black_box(&buffer)));
            })
            * 1000.0;
        let crc32c_mbps = per_round
            / charge(rounds, || {
                std::hint::black_box(crc_fast::checksum(CRC32C, std::hint::black_box(&buffer)));
            })
            * 1000.0;
        let xxh3_mbps = per_round
            / charge(rounds, || {
                std::hint::black_box(xxhash_rust::xxh3::xxh3_64(std::hint::black_box(&buffer)));
            })
            * 1000.0;

        // Both ends go through black_box, or the copy has no observable effect and the
        // optimiser deletes the loop.
        let copy_mbps = per_round
            / charge(rounds, || {
                std::hint::black_box(&mut copy).copy_from_slice(std::hint::black_box(&buffer));
            })
            * 1000.0;

        let label = if size >= 1024 * 1024 {
            format!("{} MiB", size / (1024 * 1024))
        } else {
            format!("{} KiB", size / 1024)
        };
        // The combined column charges the shipped checksum against the one copy a write
        // already pays, which is the per-byte cost the engine cannot avoid.
        println!(
            "{label:>12} {crc64_mbps:>12.0} {crc32c_mbps:>12.0} {xxh3_mbps:>12.0} {copy_mbps:>12.0} {:>16.0}",
            1.0 / (1.0 / crc32c_mbps + 1.0 / copy_mbps)
        );
    }

    println!("\nchecksum charged per record, which is what a small write pays");
    println!(
        "{:>12} {:>10} {:>10} {:>10} {:>10} {:>12}",
        "record", "crc64 ns", "c32c ns", "xxh3 ns", "c32c MB/s", "fastest MB/s"
    );
    for size in CPU_RECORDS {
        let buffer = payload(size);
        let rounds = (CPU_TOTAL_BYTES / 16 / size as u64).max(1_000);

        let crc64_ns = charge(rounds, || {
            std::hint::black_box(crc_fast::checksum(CRC64, std::hint::black_box(&buffer)));
        });
        let crc32c_ns = charge(rounds, || {
            std::hint::black_box(crc_fast::checksum(CRC32C, std::hint::black_box(&buffer)));
        });
        let xxh3_ns = charge(rounds, || {
            std::hint::black_box(xxhash_rust::xxh3::xxh3_64(std::hint::black_box(&buffer)));
        });

        let fastest = crc64_ns.min(crc32c_ns).min(xxh3_ns);
        println!(
            "{size:>12} {crc64_ns:>10.0} {crc32c_ns:>10.0} {xxh3_ns:>10.0} {:>10.0} {:>12.0}",
            size as f64 / crc32c_ns * 1000.0,
            size as f64 / fastest * 1000.0,
        );
    }

    index_terms();
}

/// What the resident index charges per key, on the shape a caller actually holds
///
/// Shards bound the depth of the tree under each of them, so the same key count costs
/// more the fewer groups it is spread across.
fn index_terms() {
    println!("\nresident index, record column, group prefix ahead of a content address");
    println!(
        "\n{:>10} {:>10} {:>12} {:>12} {:>12}",
        "groups", "writers", "keys", "insert ns", "lookup ns"
    );

    for groups in INDEX_GROUPS {
        for writers in INDEX_WRITERS {
            // Best of a few rounds: a contended cell swings by a third between runs,
            // wider than the effects this probe exists to see.
            let mut insert_ns = f64::MAX;
            let mut lookup_ns = f64::MAX;
            for _ in 0..INDEX_ROUNDS {
                let (insert, lookup) = index_pass(groups, writers, INDEX_KEYS);
                insert_ns = insert_ns.min(insert);
                lookup_ns = lookup_ns.min(lookup);
            }
            println!(
                "{groups:>10} {writers:>10} {INDEX_KEYS:>12} {insert_ns:>12.1} {lookup_ns:>12.1}"
            );
        }
    }

    println!("\n{:>28} {:>16}", "column", "resident B/key");
    for groups in INDEX_GROUPS {
        let label = format!("record, {RECORD_KEY_LEN} B key, {groups} groups");
        println!("{label:>28} {:>16.0}", index_bytes_per_key(groups));
    }
    // Both spreads are weighed because many thinly filled maps cost more per key than
    // one full one.
    println!(
        "{:>28} {:>16.0}",
        "16 B key, ascending",
        narrow_bytes_per_key(true)
    );
    println!(
        "{:>28} {:>16.0}",
        "16 B key, scattered",
        narrow_bytes_per_key(false)
    );

    // One flat map keyed by a heap allocated Vec, printed beside the real index so the
    // difference between the two shapes is measured rather than assumed.
    let start = Instant::now();
    let mut flat = std::collections::BTreeMap::new();
    for at in 0..INDEX_KEYS as u64 {
        flat.insert(key_at(at, KEY_WIDTH).as_slice().to_vec(), at);
    }
    let flat_ns = start.elapsed().as_secs_f64() * 1e9 / INDEX_KEYS as f64;
    println!("\none flat map keyed by a fresh Vec, for reference:");
    println!("{flat_ns:.1} ns per insert, against the one writer rows above");
}

/// What one key of a 16 byte column costs, at the two key spreads a column can have
///
/// Ascending keys concentrate in one shard, which is what a counter-led key does, and
/// scattered keys spread over every shard, which is what a content address does.
fn narrow_bytes_per_key(ascending: bool) -> f64 {
    weigh_reset();
    WEIGHING.store(true, Ordering::Relaxed);

    let index = ReelIndex::new(BENCH_COLUMNS, IndexResidency::Resident, ShardShapes::Tree)
        .expect("index over the bench column");
    for at in 0..INDEX_KEYS as u64 {
        let loc = Loc::new(SegmentId((at >> 20) as u32 + 1), at as u32, 65_536);
        let key = if ascending {
            let mut bytes = [0u8; KEY_WIDTH];
            bytes[..8].copy_from_slice(&at.to_be_bytes());
            RecordKey::from_bytes(BENCH_COLUMNS[0].id, &bytes).expect("key fits")
        } else {
            key_at(at, KEY_WIDTH)
        };
        index.insert(&key, loc, Lsn(at + 1), None).expect("insert");
    }
    let held = weigh_read().held;

    WEIGHING.store(false, Ordering::Relaxed);
    drop(index);
    held as f64 / INDEX_KEYS as f64
}

/// What one key of the record column costs in resident memory
///
/// Weighed rather than estimated: the index is built with the allocator counting, and
/// what it holds afterwards divided by its keys is the answer.
fn index_bytes_per_key(groups: u64) -> f64 {
    weigh_reset();
    WEIGHING.store(true, Ordering::Relaxed);

    let index = ReelIndex::new(TEST_COLUMNS, IndexResidency::Resident, ShardShapes::Tree)
        .expect("index over the fixture columns");
    for at in 0..INDEX_KEYS as u64 {
        let loc = Loc::new(SegmentId((at >> 20) as u32 + 1), at as u32, 65_536);
        index
            .insert(&index_key(groups, at), loc, Lsn(at + 1), None)
            .expect("insert");
    }
    let held = weigh_read().held;

    WEIGHING.store(false, Ordering::Relaxed);
    drop(index);
    held as f64 / INDEX_KEYS as f64
}

/// One index pass: every key inserted, then every key resolved, across writers
fn index_pass(groups: u64, writers: usize, keys: usize) -> (f64, f64) {
    let index = ReelIndex::new(TEST_COLUMNS, IndexResidency::Resident, ShardShapes::Tree)
        .expect("index over the fixture columns");
    let per_writer = keys / writers;

    let insert_secs = std::thread::scope(|scope| {
        let start = Instant::now();
        for writer in 0..writers {
            let index = &index;
            scope.spawn(move || {
                let first = (writer * per_writer) as u64;
                for at in first..first + per_writer as u64 {
                    let loc = Loc::new(SegmentId((at >> 20) as u32 + 1), at as u32, 65_536);
                    index
                        .insert(&index_key(groups, at), loc, Lsn(at + 1), None)
                        .expect("insert");
                }
            });
        }
        start
    })
    .elapsed()
    .as_secs_f64();

    let lookup_secs = std::thread::scope(|scope| {
        let start = Instant::now();
        for writer in 0..writers {
            let index = &index;
            scope.spawn(move || {
                let first = (writer * per_writer) as u64;
                for at in first..first + per_writer as u64 {
                    std::hint::black_box(index.get(&index_key(groups, at)).expect("read"));
                }
            });
        }
        start
    })
    .elapsed()
    .as_secs_f64();

    let ops = (per_writer * writers).max(1);
    (
        insert_secs * 1e9 / ops as f64,
        lookup_secs * 1e9 / ops as f64,
    )
}

/// The record column key for a group and a content address
///
/// The group leads big endian, so the key space groups by it.
fn record_key(group: u16, id: [u8; ID_LEN]) -> RecordKey {
    let mut bytes = [0u8; RECORD_KEY_LEN];
    bytes[..GROUP_PREFIX_LEN].copy_from_slice(&group.to_be_bytes());
    bytes[GROUP_PREFIX_LEN..].copy_from_slice(&id);
    RecordKey::from_bytes(RECORDS, &bytes).expect("key fits")
}

/// A record column key, spread over a given number of groups
///
/// The address half is scattered so keys arrive at a shard's map in no order, and the
/// counter is kept in the low bytes so a lookup can rebuild the key it wants.
fn index_key(groups: u64, at: u64) -> RecordKey {
    let mut id = [0u8; ID_LEN];
    let scattered = at.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    id[..8].copy_from_slice(&scattered.to_le_bytes());
    id[8..16].copy_from_slice(&at.to_le_bytes());
    record_key((at % groups.max(1)) as u16, id)
}

// raw put and get across backend, page cache, sync policy, threads and size
#[test]
#[ignore = "writes gigabytes, run explicitly on the box under test"]
fn raw_matrix() {
    // libtest leaves the test name line open, so a header printed into it starts a
    // screen-width right of the rows underneath.
    println!();
    let sizes = env_list("REEL_RAW_SIZES", DEFAULT_SIZES);
    let key_widths = env_list("REEL_RAW_KEYS", DEFAULT_KEYS);
    let thread_counts = env_list("REEL_RAW_THREADS", DEFAULT_THREADS);
    let cell_bytes = env_bytes("REEL_RAW_BYTES", DEFAULT_CELL_BYTES);
    let shards = env_bytes("REEL_RAW_SHARDS", DEFAULT_SHARDS).max(1);
    let max_ops = env_bytes("REEL_RAW_MAX_OPS", DEFAULT_MAX_OPS).max(1);
    let root = std::env::var("REEL_RAW_DIR").ok();
    let drop_caches = std::env::var("REEL_RAW_DROP_CACHES").is_ok();
    let random_reads = std::env::var("REEL_RAW_RANDOM_READS").is_ok();
    let weigh = std::env::var("REEL_RAW_WEIGH").is_ok();
    let mut csv = std::env::var("REEL_RAW_CSV")
        .ok()
        .map(|path| std::fs::File::create(&path).expect("create the csv"));

    for (shape_name, variable) in key_shapes() {
        for &width in &key_widths {
            assert!(
                width as usize <= MAX_KEY_LEN,
                "a key of {width} bytes is wider than the {MAX_KEY_LEN} byte format maximum",
            );
            assert!(
                variable || INDEXED_WIDTHS.contains(&width),
                "a {shape_name} column of {width} bytes is refused at open, because the resident \
                 index is monomorphised over {INDEXED_WIDTHS:?}. Anything else, and anything wider \
                 than {INLINE_KEY_LEN}, needs REEL_RAW_KEY_SHAPES=var",
            );
        }
    }

    println!(
        "cell {} MiB capped at {} ops, {} shards, drop_caches {}, weigh {}, machine memory {} MiB, dir {}",
        cell_bytes / (1024 * 1024),
        max_ops,
        shards,
        drop_caches,
        weigh,
        machine_memory_bytes() / (1024 * 1024),
        root.clone().unwrap_or_else(|| "tempdir".to_string())
    );
    if weigh {
        println!(
            "the scale is on: alloc columns are what the calling thread allocated per record, \
             and the inline key ceiling is {INLINE_KEY_LEN} bytes"
        );
    }

    if let Some(file) = csv.as_mut() {
        writeln!(
            file,
            "backend,sync,size,key,shape,threads,write,cached_mbps,durable_mbps,read_mbps,\
             write_us,read_us,syncs_per_kop,write_alloc_bytes,write_alloc_calls,\
             read_alloc_bytes,read_alloc_calls,read_secs,cache_resident"
        )
        .expect("write the csv header");
    }

    println!(
        "\n{:>13} {:>10} {:>9} {:>6} {:>6} {:>8} {:>9} {:>12} {:>13} {:>12} {:>11} {:>11} {:>11}",
        "backend",
        "sync",
        "size",
        "key",
        "shape",
        "threads",
        "write",
        "cached MB/s",
        "durable MB/s",
        "read MB/s",
        "write us",
        "read us",
        "syncs/kop",
    );
    println!(
        "{:>13} {:>10} {:>9} {:>6} {:>6} {:>8} {:>9} {:>12} {:>13} {:>12} {:>11} {:>11} {:>11} {:>10}",
        "", "", "", "", "", "", "", "", "", "", "", "", "", "warning"
    );

    for (backend_name, backend) in backends() {
        for (sync_name, sync) in syncs() {
            for &size in &sizes {
                for (shape_name, key_variable) in key_shapes() {
                    for &key_width in &key_widths {
                        for &threads in &thread_counts {
                            for (write_name, write) in writes() {
                                let cell = Cell {
                                    backend,
                                    sync,
                                    size: size as usize,
                                    key_width: key_width as usize,
                                    key_variable,
                                    threads,
                                    write,
                                    shards,
                                    cell_bytes,
                                    max_ops,
                                    drop_caches,
                                    random_reads,
                                    weigh,
                                    root: root.clone(),
                                };
                                let result = run_cell(&cell);
                                // A shorter phase is thread spawn and timer noise
                                // wearing a throughput number's clothes.
                                let flag = match (
                                    result.read_secs < MIN_PHASE_SECS,
                                    result.is_cache_resident,
                                ) {
                                    (true, _) => "too short",
                                    (false, true) => "cached",
                                    (false, false) => "",
                                };
                                let alloc = if weigh {
                                    format!(
                                        " {:>11.0} {:>9.2} {:>11.0} {:>9.2}",
                                        result.write_alloc_bytes,
                                        result.write_alloc_calls,
                                        result.read_alloc_bytes,
                                        result.read_alloc_calls,
                                    )
                                } else {
                                    String::new()
                                };
                                println!(
                                    "{backend_name:>13} {sync_name:>10} {size:>9} \
                                     {key_width:>6} {shape_name:>6} {threads:>8} {write_name:>9} \
                                     {:>12.0} {:>13.0} {:>12.0} {:>11.2} {:>11.2} {:>11.1} \
                                     {flag:>10}{alloc}",
                                    result.cached_mbps,
                                    result.durable_mbps,
                                    result.read_mbps,
                                    result.write_micros,
                                    result.read_micros,
                                    result.syncs_per_kop,
                                );
                                if let Some(file) = csv.as_mut() {
                                    writeln!(
                                        file,
                                        "{backend_name},{sync_name},{size},\
                                         {key_width},{shape_name},{threads},{write_name},\
                                         {:.1},{:.1},{:.1},\
                                         {:.3},{:.3},{:.2},{:.1},{:.3},{:.1},{:.3},{:.3},{}",
                                        result.cached_mbps,
                                        result.durable_mbps,
                                        result.read_mbps,
                                        result.write_micros,
                                        result.read_micros,
                                        result.syncs_per_kop,
                                        result.write_alloc_bytes,
                                        result.write_alloc_calls,
                                        result.read_alloc_bytes,
                                        result.read_alloc_calls,
                                        result.read_secs,
                                        result.is_cache_resident,
                                    )
                                    .expect("write a csv row");
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

struct Cell {
    backend: IoBackend,
    sync: SyncPolicy,
    size: usize,
    key_width: usize,
    key_variable: bool,
    threads: u64,
    write: WriteMode,
    shards: u64,
    cell_bytes: u64,
    max_ops: u64,
    drop_caches: bool,
    random_reads: bool,
    weigh: bool,
    root: Option<String>,
}

struct CellResult {
    /// Bytes per second into the page cache, which is not a write rate
    cached_mbps: f64,

    /// Bytes per second to durable, the put loop plus the flush behind it
    durable_mbps: f64,

    /// Bytes per second back off the volume
    read_mbps: f64,

    /// Microseconds one put took
    write_micros: f64,

    /// Microseconds one get took
    read_micros: f64,

    /// Seconds the read phase ran for, so a window too short to trust is visible
    read_secs: f64,

    /// Whether the data written could have fitted in the machine's memory
    is_cache_resident: bool,

    /// Device flushes the write phase asked for, per thousand records
    syncs_per_kop: f64,

    /// Bytes the calling thread allocated per record, read as a difference between rows
    write_alloc_bytes: f64,

    /// Allocation calls the write path made per record
    write_alloc_calls: f64,

    /// Bytes the read path allocated per record
    read_alloc_bytes: f64,

    /// Allocation calls the read path made per record
    read_alloc_calls: f64,
}

fn run_cell(cell: &Cell) -> CellResult {
    // The temp dir has to outlive the store, so it is bound whether or not a root came in.
    let temp = TempDir::new().expect("tempdir");
    let base = match &cell.root {
        Some(root) => std::path::PathBuf::from(root).join(format!(
            "raw-{}-{}-{}",
            cell.size, cell.key_width, cell.threads
        )),
        None => temp.path().to_path_buf(),
    };
    let _ = std::fs::remove_dir_all(&base);

    let store = ReelStore::open(
        base.clone(),
        config(cell.backend, cell.sync),
        bench_columns(cell.key_width, cell.key_variable),
    )
    .expect("open");
    let body = payload(cell.size);
    let span = cell.write.span();
    // Every writer's share is a whole number of calls, so a batched cell writes the same
    // records a per-record cell does and the two rows compare directly.
    let per_thread = (cell.cell_bytes / cell.size.max(1) as u64)
        .min(cell.max_ops)
        .max(cell.threads * span)
        / cell.threads
        / span
        * span;
    let count = per_thread * cell.threads;
    let payload_bytes = (count * cell.size as u64) as f64;

    // Open every shard before the clock starts: creating a reel writes a mount file,
    // syncs the root and preallocates a whole segment, so a small cell charged for that
    // measures preallocation. Rolls inside the phase still count.
    let warm = payload(cell.size.min(64));
    for shard in 0..cell.shards {
        store
            .put(&key_at(u64::MAX - shard, cell.key_width), &warm)
            .expect("warm put");
    }
    store.flush().expect("warm flush");

    // Each thread weighs itself and folds its totals in once at the end, so the phase
    // pays one atomic per thread rather than one per allocation.
    let write_taken = AtomicU64::new(0);
    let write_calls = AtomicU64::new(0);
    let syncs_before = store.sync_count();
    WEIGHING.store(cell.weigh, Ordering::Relaxed);
    let write_secs = timed(cell.threads, |thread| {
        weigh_reset();
        match cell.write {
            WriteMode::PerRecord => {
                for step in 0..per_thread {
                    let at = thread * per_thread + step;
                    store.put(&key_at(at, cell.key_width), &body).expect("put");
                }
            }
            WriteMode::Batched(records) => {
                for run in 0..per_thread / span {
                    let base = thread * per_thread + run * span;
                    let mut writes = Vec::with_capacity(records);
                    for step in 0..span {
                        writes.push(RecordWrite::Put {
                            key: key_at(base + step, cell.key_width),
                            payload: body.clone(),
                        });
                    }
                    store.apply_batch(writes).expect("batch");
                }
            }
        }
        let scale = weigh_read();
        write_taken.fetch_add(scale.taken, Ordering::Relaxed);
        write_calls.fetch_add(scale.calls, Ordering::Relaxed);
    });
    WEIGHING.store(false, Ordering::Relaxed);
    let write_syncs = store.sync_count() - syncs_before;

    // The put loop returns when the bytes are in the page cache, which under the default
    // policy is before any have reached the drive, so timing only that is a memcpy rate.
    // The durable column covers the flush too and is the one to compare against a device.
    let flush_start = Instant::now();
    store.flush().expect("flush");
    let durable_secs = write_secs + flush_start.elapsed().as_secs_f64();

    // The only way to get a cold read on a box whose memory is comparable to the cell:
    // without it most reads come out of memory, at microseconds no device could answer in.
    if cell.drop_caches {
        drop_page_cache();
    }

    // Read in a stride rather than in write order, so a reader is not simply walking the
    // file the writer just laid down.
    let hits = AtomicU64::new(0);
    let read_taken = AtomicU64::new(0);
    let read_calls = AtomicU64::new(0);
    // A write-scaling sweep can decline the read phase, whose columns then print zeros.
    let skip_reads = std::env::var("REEL_RAW_SKIP_READS").is_ok();
    WEIGHING.store(cell.weigh, Ordering::Relaxed);
    let read_secs = if skip_reads {
        0.0
    } else {
        timed(cell.threads, |thread| {
            weigh_reset();
            let mut found = 0u64;
            for step in 0..per_thread {
                // Insertion order makes the read pattern follow the file layout, which
                // measures layout rather than the engine.
                let at = if cell.random_reads {
                    (step.wrapping_mul(READ_STRIDE).wrapping_add(thread)) % count
                } else {
                    (step * cell.threads + thread).min(count - 1)
                };
                if let Some(value) = store.get(&key_at(at, cell.key_width)).expect("get") {
                    found += value.len() as u64;
                }
            }
            let scale = weigh_read();
            read_taken.fetch_add(scale.taken, Ordering::Relaxed);
            read_calls.fetch_add(scale.calls, Ordering::Relaxed);
            hits.fetch_add(found, Ordering::Relaxed);
        })
    };
    WEIGHING.store(false, Ordering::Relaxed);

    let read_bytes = hits.load(Ordering::Relaxed) as f64;
    assert!(
        skip_reads || read_bytes > 0.0,
        "the read phase found nothing to read"
    );

    drop(store);
    let _ = std::fs::remove_dir_all(&base);

    let ops = count as f64;
    CellResult {
        cached_mbps: payload_bytes / write_secs / 1e6,
        durable_mbps: payload_bytes / durable_secs / 1e6,
        read_mbps: match skip_reads {
            true => 0.0,
            false => read_bytes / read_secs / 1e6,
        },
        write_micros: durable_secs * 1e6 / ops,
        read_micros: read_secs * 1e6 / ops,
        read_secs,
        is_cache_resident: !cell.drop_caches && payload_bytes < machine_memory_bytes() as f64,
        syncs_per_kop: write_syncs as f64 * 1000.0 / ops,
        write_alloc_bytes: write_taken.load(Ordering::Relaxed) as f64 / ops,
        write_alloc_calls: write_calls.load(Ordering::Relaxed) as f64 / ops,
        read_alloc_bytes: read_taken.load(Ordering::Relaxed) as f64 / ops,
        read_alloc_calls: read_calls.load(Ordering::Relaxed) as f64 / ops,
    }
}

/// Give the whole page cache back to the kernel, so the next read reaches the drive
///
/// Needs root and Linux. Anywhere else this is a no-op and the numbers stay warm.
fn drop_page_cache() {
    #[cfg(target_os = "linux")]
    {
        use std::io::Write;
        let _ = std::process::Command::new("sync").status();
        match std::fs::OpenOptions::new()
            .write(true)
            .open("/proc/sys/vm/drop_caches")
        {
            Ok(mut file) => {
                if file.write_all(b"3\n").is_err() {
                    eprintln!("could not drop the page cache, reads stay warm");
                }
            }
            Err(_) => eprintln!("could not open drop_caches, reads stay warm"),
        }
    }
}

/// Bytes of memory the machine has, for deciding whether a cell could be cached
///
/// A cell smaller than this was read out of the page cache whatever the drive underneath
/// is, so a row that does not say so invites a memcpy being read as a device.
fn machine_memory_bytes() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(text) = std::fs::read_to_string("/proc/meminfo") {
            for line in text.lines() {
                if let Some(rest) = line.strip_prefix("MemTotal:") {
                    if let Some(kb) = rest.split_whitespace().next() {
                        if let Ok(kb) = kb.parse::<u64>() {
                            return kb * 1024;
                        }
                    }
                }
            }
        }
    }
    #[cfg(target_os = "macos")]
    {
        if let Ok(out) = std::process::Command::new("sysctl")
            .arg("-n")
            .arg("hw.memsize")
            .output()
        {
            if let Ok(text) = String::from_utf8(out.stdout) {
                if let Ok(bytes) = text.trim().parse::<u64>() {
                    return bytes;
                }
            }
        }
    }
    0
}
