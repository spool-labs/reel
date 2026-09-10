//! What a paced background copier costs the reads running underneath it
//!
//! Drives real compaction over a past-memory volume at swept pace targets while
//! reader threads take scattered point reads and record every latency. Three
//! things keep the answer honest: the fill has to beat MemTotal, every loud arm
//! needs a quiet arm beside it because a closed-loop reader warms the live set as
//! the run goes, and the volume has to still owe dead space at the end or a late
//! arm is a quiet arm wearing another arm's label.
//!
//! Opt-in, since it writes over a hundred gigabytes and runs for twenty minutes:
//!   TMPDIR=/some/device cargo test -p tape-reel --release --test probes -- interference

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, CompactRate, IoBackend, KeyWidth, MapShape,
    RecordKey, ReelConfig, ReelStore, SyncPolicy,
};

/// Bytes the group takes at the front of a record key
const GROUP_PREFIX_LEN: usize = 2;

/// Bytes a record key occupies: the group then a thirty-two byte identifier
const RECORD_KEY_LEN: usize = GROUP_PREFIX_LEN + 32;

/// Group every record in the fill lands in
const GROUP: u16 = 7;

const RECORDS: ColumnId = ColumnId(1);

/// The one column the fill uses, records addressed by group and identifier
const COLUMNS: ColumnSet = &[ColumnSpec {
    id: RECORDS,
    name: "records",
    key_width: KeyWidth::Fixed(RECORD_KEY_LEN as u16),
    shard_bytes: GROUP_PREFIX_LEN as u8,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

/// One in five records the kill leaves alive, at this position and the next
///
/// A stride rather than a prefix: a prefix empties whole early segments, and an
/// empty segment is unlinked rather than rewritten, so there is nothing to pace.
const KILL_STRIDE: u64 = 5;

/// Positions in each stride the kill takes, leaving the rest alive
const KILLED_PER_STRIDE: u64 = 3;

/// What one compaction pass spent, for the duty and burst columns
///
/// Taken in the driver, not by sampling: a pass posts its bytes only when it
/// finishes, so a sampler would read every pass as instant.
struct Occupancy {
    /// Nanoseconds spent inside passes that moved something
    busy_nanos: AtomicU64,

    /// Passes that moved something
    passes: AtomicU64,
}

/// What one arm asks of the background copier
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Arm {
    /// No compaction at all, the tail the paced arms are measured against
    Quiet,

    /// Compaction held to this many megabytes a second of read plus write
    Paced(u64),

    /// Compaction held by the engine's own `compact_mbps` rather than the governor
    Engine,

    /// Compaction driven as fast as it will go
    Unpaced,
}

impl Arm {
    /// The arm named by one entry of `REEL_INTERFERENCE_RATES`
    fn parse(text: &str) -> Arm {
        match text {
            "quiet" => Arm::Quiet,
            "unpaced" => Arm::Unpaced,
            "engine" => Arm::Engine,
            number => Arm::Paced(number.parse().expect("REEL_INTERFERENCE_RATES entry")),
        }
    }

    /// The label the table prints for this arm
    fn label(&self) -> String {
        match self {
            Arm::Quiet => "quiet".to_string(),
            Arm::Paced(mbps) => format!("paced {mbps}"),
            Arm::Engine => format!("engine {}", engine_mbps()),
            Arm::Unpaced => "unpaced".to_string(),
        }
    }

    /// What the arm asks for in megabytes a second, or nothing when it names no rate
    fn requested_mbps(&self) -> Option<u64> {
        match self {
            Arm::Quiet => Some(0),
            Arm::Paced(mbps) => Some(*mbps),
            Arm::Engine => Some(engine_mbps()),
            Arm::Unpaced => None,
        }
    }

    /// The governor target this arm sets: zero is off and a negative is unpaced
    fn target(&self) -> i64 {
        match self {
            Arm::Quiet => 0,
            Arm::Paced(mbps) => *mbps as i64,
            Arm::Engine | Arm::Unpaced => -1,
        }
    }
}

/// A log-linear latency histogram, eight buckets an octave
///
/// Counts rather than samples, since a p99.9 off a reservoir is a guess. The
/// maximum is kept exactly, being the one number a bucket would round down.
struct Latencies {
    /// Reads that landed in each bucket
    buckets: Vec<u64>,

    /// Reads recorded, the divisor every quantile is taken against
    count: u64,

    /// The longest single read, kept outside the buckets
    max_ns: u64,

    /// Payload bytes the recorded reads returned
    served: u64,
}

/// Buckets per octave, so a reported value is within 6.25 percent of its sample
const SUB_BUCKETS: u32 = 8;

/// Octave the sub-bucketing starts at, below which a nanosecond is its own bucket
const SUB_SHIFT: u32 = 3;

/// Buckets held, which covers a read of up to about a minute
const BUCKETS: usize = 512;

impl Latencies {
    fn new() -> Latencies {
        Latencies {
            buckets: vec![0; BUCKETS],
            count: 0,
            max_ns: 0,
            served: 0,
        }
    }

    /// The bucket a duration falls in
    fn bucket_of(ns: u64) -> usize {
        if ns < (1 << SUB_SHIFT) {
            return ns as usize;
        }
        let octave = 63 - ns.leading_zeros();
        let sub = (ns >> (octave - SUB_SHIFT)) & (SUB_BUCKETS as u64 - 1);
        let index = ((octave - SUB_SHIFT + 1) * SUB_BUCKETS) as usize + sub as usize;
        index.min(BUCKETS - 1)
    }

    /// The middle of a bucket's range, which is what a quantile reports
    fn value_of(bucket: usize) -> u64 {
        if bucket < (1 << SUB_SHIFT) {
            return bucket as u64;
        }
        let octave = (bucket as u32 / SUB_BUCKETS) + SUB_SHIFT - 1;
        let sub = bucket as u64 % SUB_BUCKETS as u64;
        let width = 1u64 << (octave - SUB_SHIFT);
        ((SUB_BUCKETS as u64 + sub) << (octave - SUB_SHIFT)) + width / 2
    }

    fn record(&mut self, ns: u64, served: usize) {
        self.buckets[Latencies::bucket_of(ns)] += 1;
        self.count += 1;
        self.max_ns = self.max_ns.max(ns);
        self.served += served as u64;
    }

    fn absorb(&mut self, other: &Latencies) {
        for (mine, theirs) in self.buckets.iter_mut().zip(&other.buckets) {
            *mine += theirs;
        }
        self.count += other.count;
        self.max_ns = self.max_ns.max(other.max_ns);
        self.served += other.served;
    }

    fn clear(&mut self) {
        self.buckets.iter_mut().for_each(|bucket| *bucket = 0);
        self.count = 0;
        self.max_ns = 0;
        self.served = 0;
    }

    /// The quantile in microseconds, taken over the bucket counts
    fn quantile_us(&self, quantile: f64) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let target = (self.count as f64 * quantile).ceil() as u64;
        let mut seen = 0u64;
        for (bucket, count) in self.buckets.iter().enumerate() {
            seen += count;
            if seen >= target {
                return Latencies::value_of(bucket) as f64 / 1e3;
            }
        }
        self.max_ns as f64 / 1e3
    }

    fn max_us(&self) -> f64 {
        self.max_ns as f64 / 1e3
    }
}

/// A rate gate over the background copier, the engine's control law in the harness
///
/// `compact_mbps` is fixed at open, so sweeping it would mean a fill an arm. This
/// holds the same average rate but not the same pass shape: the engine charges
/// step by step as it copies, and this waits between whole passes.
struct Governor {
    /// Megabytes a second of read plus write, zero for off and negative for unpaced
    target: AtomicI64,

    /// When the next pass may start, and the bytes already accounted for
    debt: Mutex<(Instant, u64)>,
}

impl Governor {
    fn new() -> Governor {
        Governor {
            target: AtomicI64::new(0),
            debt: Mutex::new((Instant::now(), 0)),
        }
    }

    /// Set the arm's target and forget the standing debt, so an arm starts open
    fn aim(&self, target: i64, moved: u64) {
        let mut debt = self.debt.lock().expect("debt");
        *debt = (Instant::now(), moved);
        self.target.store(target, Ordering::Relaxed);
    }

    /// How long the copier must wait before its next pass, given what it has moved
    ///
    /// `since` is when the driver's last pass began, and the debt is added on top
    /// of it rather than on top of now, which is what stops the pass's own runtime
    /// going free.
    fn owed(&self, moved: u64, since: Instant) -> Duration {
        let target = self.target.load(Ordering::Relaxed);
        if target <= 0 {
            return Duration::ZERO;
        }
        let mut debt = self.debt.lock().expect("debt");
        let (ready_at, accounted) = *debt;
        let fresh = moved.saturating_sub(accounted);
        let owed = Duration::from_nanos(fresh.saturating_mul(1000) / target as u64);
        let standing = ready_at.max(since) + owed;
        *debt = (standing, moved);
        standing.saturating_duration_since(Instant::now())
    }
}

/// Total machine memory, which the fill has to beat
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

/// Bytes the fill holds, twice memory unless told otherwise
fn volume_bytes() -> u64 {
    if let Ok(value) = std::env::var("REEL_INTERFERENCE_VOLUME_BYTES") {
        return value
            .parse()
            .expect("REEL_INTERFERENCE_VOLUME_BYTES is not a number");
    }
    let memory = machine_memory_bytes().unwrap_or(0);
    (memory * 2).max(48 * 1024 * 1024 * 1024)
}

/// Bytes one record holds, at the size a state row runs
fn record_bytes() -> usize {
    match std::env::var("REEL_INTERFERENCE_RECORD_BYTES") {
        Ok(value) => value
            .parse()
            .expect("REEL_INTERFERENCE_RECORD_BYTES is not a number"),
        Err(_) => 4096,
    }
}

/// Bytes a segment holds, which is the granularity one compaction pass moves
///
/// Well under the gibibyte default: a pass is charged whole, so a gibibyte at
/// 40 MB/s is one burst and half a minute of silence, and an arm gets one sample.
fn segment_bytes() -> u64 {
    match std::env::var("REEL_INTERFERENCE_SEGMENT_BYTES") {
        Ok(value) => value
            .parse()
            .expect("REEL_INTERFERENCE_SEGMENT_BYTES is not a number"),
        Err(_) => 64 * 1024 * 1024,
    }
}

/// Reader threads taking point reads through the blocking door
fn reader_count() -> usize {
    match std::env::var("REEL_INTERFERENCE_READERS") {
        Ok(value) => value
            .parse()
            .expect("REEL_INTERFERENCE_READERS is not a number"),
        Err(_) => 8,
    }
}

/// Threads driving compaction passes
fn driver_count() -> usize {
    match std::env::var("REEL_INTERFERENCE_DRIVERS") {
        Ok(value) => value
            .parse()
            .expect("REEL_INTERFERENCE_DRIVERS is not a number"),
        Err(_) => 1,
    }
}

/// Seconds an arm is measured over, after its settle
fn arm_seconds() -> f64 {
    match std::env::var("REEL_INTERFERENCE_SECONDS") {
        Ok(value) => value
            .parse()
            .expect("REEL_INTERFERENCE_SECONDS is not a number"),
        Err(_) => 45.0,
    }
}

/// Seconds an arm runs before its window opens, so the copier is at rate
fn settle_seconds() -> f64 {
    match std::env::var("REEL_INTERFERENCE_SETTLE") {
        Ok(value) => value
            .parse()
            .expect("REEL_INTERFERENCE_SETTLE is not a number"),
        Err(_) => 8.0,
    }
}

/// The arms the run takes, a quiet arm between every loud one
///
/// Readers in a closed loop warm the live set as the run goes, so a one-directional
/// sweep cannot tell warmth from interference. A quiet arm beside each loud one
/// gives every loud row a baseline at its own warmth.
fn arms() -> Vec<Arm> {
    let list = std::env::var("REEL_INTERFERENCE_RATES").unwrap_or_else(|_| {
        "quiet,40,quiet,100,quiet,200,quiet,400,quiet,unpaced,quiet".to_string()
    });
    list.split(',')
        .filter(|part| !part.is_empty())
        .map(Arm::parse)
        .collect()
}

/// The engine's own `compact_mbps` for a run holding an engine arm, zero for none
fn engine_mbps() -> u64 {
    match std::env::var("REEL_INTERFERENCE_ENGINE_MBPS") {
        Ok(value) => value
            .parse()
            .expect("REEL_INTERFERENCE_ENGINE_MBPS is not a number"),
        Err(_) => 0,
    }
}

fn backend() -> IoBackend {
    match std::env::var("REEL_INTERFERENCE_BACKEND").as_deref() {
        Ok("uring") => IoBackend::Uring,
        Ok("uring_direct") => IoBackend::UringDirect,
        Ok("posix") | Err(_) => IoBackend::Posix,
        Ok(other) => panic!("REEL_INTERFERENCE_BACKEND `{other}` is not a known backend"),
    }
}

fn config() -> ReelConfig {
    ReelConfig {
        // The scrub stays off: a second background copier is a second variable.
        compact_mbps: match engine_mbps() {
            0 => CompactRate::Auto,
            mbps => CompactRate::Mbps(mbps),
        },
        scrub_mbps: 0,
        sync: SyncPolicy::Never,
        segment_bytes: ByteCount::from_bytes(segment_bytes()),
        // A segment smaller than its own allocation chunk is refused at open.
        alloc_chunk: ByteCount::from_bytes(segment_bytes().min(64 * 1024 * 1024)),
        io_backend: backend(),
        ..ReelConfig::default()
    }
}

/// A record identifier for a position in the fill
///
/// splitmix64 is a bijection, so no two records share a key and the fill needs no
/// table of what it wrote. Key order and offset order disagree by construction.
fn id_of(position: u64) -> [u8; 32] {
    let mut id = [0u8; 32];
    for lane in 0..4u64 {
        let mut state = position
            .wrapping_mul(4)
            .wrapping_add(lane)
            .wrapping_add(0x9E37_79B9_7F4A_7C15);
        state = (state ^ (state >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        state = (state ^ (state >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        state ^= state >> 31;
        id[lane as usize * 8..(lane as usize + 1) * 8].copy_from_slice(&state.to_le_bytes());
    }
    id
}

/// The record key for a position in the fill
fn record_key(position: u64) -> RecordKey {
    let mut bytes = [0u8; RECORD_KEY_LEN];
    bytes[..GROUP_PREFIX_LEN].copy_from_slice(&GROUP.to_be_bytes());
    bytes[GROUP_PREFIX_LEN..].copy_from_slice(&id_of(position));
    RecordKey::from_bytes(RECORDS, &bytes).expect("record key")
}

/// Whether the kill takes this position
fn is_killed(position: u64) -> bool {
    position % KILL_STRIDE < KILLED_PER_STRIDE
}

/// The position of the nth surviving record
///
/// The readers draw over the live records only, so the sample never asks for a key
/// the kill took and never counts a miss as a read.
fn live_position(nth: u64) -> u64 {
    let alive = KILL_STRIDE - KILLED_PER_STRIDE;
    (nth / alive) * KILL_STRIDE + KILLED_PER_STRIDE + (nth % alive)
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

/// Bytes the devices have served since boot, for the page-cache share
///
/// Sectors are always 512 bytes in `/proc/diskstats` whatever the device's own
/// block size is. Zero on a machine that cannot answer the question.
fn device_read_bytes() -> u64 {
    let Ok(stats) = std::fs::read_to_string("/proc/diskstats") else {
        return 0;
    };
    let mut total = 0u64;
    for line in stats.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 6 {
            continue;
        }
        let name = parts[2];
        // Whole devices only. Counting a partition and its disk would double every
        // byte, and loop devices are not the drive under test.
        if name.starts_with("loop") || name.chars().last().is_some_and(char::is_numeric) {
            continue;
        }
        total += parts[5].parse::<u64>().unwrap_or(0) * 512;
    }
    total
}

/// Refuse a temp directory that is memory, since every read would be a RAM read
fn refuse_tmpfs(dir: &std::path::Path) {
    let Ok(mounts) = std::fs::read_to_string("/proc/mounts") else {
        return;
    };
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

/// Empty the page cache, so the opening quiet arm is not reading the fill back
///
/// Needs root. Without it the opening arm reads warmer than the ones after it.
fn drop_caches() -> bool {
    std::fs::write("/proc/sys/vm/drop_caches", "3").is_ok()
}

/// One arm's answer, held so the table can be printed once at the end
struct Row {
    arm: Arm,
    background_mbps: f64,
    written_mbps: f64,
    duty: f64,
    burst_mbps: f64,
    pass_ms: f64,
    reads_per_second: f64,
    p50_us: f64,
    p99_us: f64,
    p999_us: f64,
    max_us: f64,
    device_mbps: f64,
    amplification: f64,
    dead_start: u64,
    dead_end: u64,
}

// what a paced background copier costs the foreground's read tails
pub fn tails_under_a_paced_copier() {
    println!();
    let memory = machine_memory_bytes().unwrap_or(0);
    let volume = volume_bytes();
    let record = record_bytes();
    // Floored to a whole number of strides, so the live count is exact.
    let count = volume / record as u64 / KILL_STRIDE * KILL_STRIDE;
    let live = count / KILL_STRIDE * (KILL_STRIDE - KILLED_PER_STRIDE);
    let arms = arms();
    let readers = reader_count();
    let drivers = driver_count();

    // A run that sized itself has to land past memory or every latency is a
    // page-cache latency. A named size is a calibration and skips the guards.
    let past_memory = memory > 0 && volume > memory;
    assert!(
        past_memory || std::env::var("REEL_INTERFERENCE_VOLUME_BYTES").is_ok(),
        "a {} GiB fill against {} GiB of memory is a page-cache benchmark",
        volume >> 30,
        memory >> 30,
    );

    println!(
        "volume {} GiB in {count} records of {} B, {live} alive after the kill, memory {} GiB",
        volume >> 30,
        record,
        memory >> 30,
    );
    if !past_memory {
        println!("this fill is not past memory, so the guards are off and the tails are memory");
    }
    println!(
        "segments {} MiB, {readers} readers, {drivers} drivers, backend {:?}, {:.0}s an arm after {:.0}s settle",
        segment_bytes() >> 20,
        backend(),
        arm_seconds(),
        settle_seconds(),
    );

    // The engine's rate is fixed at open, so mixing it with governor arms would
    // cap those too and every swept row would read as the engine's cap.
    let has_engine_arm = arms.contains(&Arm::Engine);
    assert_eq!(
        has_engine_arm,
        engine_mbps() > 0,
        "an engine arm needs REEL_INTERFERENCE_ENGINE_MBPS and a rate named there needs \
         an engine arm to spend it",
    );
    if has_engine_arm {
        println!("compact_mbps {} in the store, governor open", engine_mbps());
        for arm in &arms {
            assert!(
                matches!(arm, Arm::Quiet | Arm::Engine),
                "{} would run under the engine's cap, so it is not the rate it names",
                arm.label(),
            );
        }
    }

    let dir = TempDir::new().expect("tempdir");
    refuse_tmpfs(dir.path());
    let store = ReelStore::open(dir.path().to_path_buf(), config(), COLUMNS).expect("open");

    let body = payload(0x9E37_79B9_7F4A_7C15, record);
    let fill_start = Instant::now();
    let mut announced = 0u64;
    for position in 0..count {
        store.put(&record_key(position), &body).expect("put");
        if position * 10 / count > announced {
            announced = position * 10 / count;
            println!(
                "fill {}0 percent, {:.0} MB/s",
                announced,
                (position * record as u64) as f64 / fill_start.elapsed().as_secs_f64() / 1e6,
            );
        }
    }
    store.flush().expect("flush");
    println!("fill done in {:.0}s", fill_start.elapsed().as_secs_f64());

    let kill_start = Instant::now();
    for position in 0..count {
        if is_killed(position) {
            store.delete(&record_key(position)).expect("delete");
        }
    }
    store.flush().expect("flush");
    println!(
        "kill done in {:.0}s, {:.1} GB dead",
        kill_start.elapsed().as_secs_f64(),
        store.dead_bytes().to_bytes() as f64 / 1e9,
    );

    println!("caches dropped: {}", drop_caches());

    // One slot a phase, settle and measure alternating, so a reader labels its
    // reads by an atomic load and the settle's reads never enter a window.
    let slots = arms.len() * 2 + 1;
    let slot = AtomicUsize::new(0);
    let done = AtomicBool::new(false);
    let governor = Governor::new();
    let occupancy = Occupancy {
        busy_nanos: AtomicU64::new(0),
        passes: AtomicU64::new(0),
    };
    let histograms: Vec<Mutex<Latencies>> =
        (0..slots).map(|_| Mutex::new(Latencies::new())).collect();
    let mut rows = Vec::with_capacity(arms.len());

    println!(
        "\n{:>10} {:>9} {:>9} {:>9} {:>7} {:>9} {:>9} {:>10} {:>9} {:>9} {:>9} {:>10} {:>9} {:>6}",
        "arm",
        "req MB/s",
        "bg MB/s",
        "wrote",
        "duty",
        "burst",
        "pass ms",
        "reads/s",
        "p50 us",
        "p99 us",
        "p99.9 us",
        "max us",
        "dev MB/s",
        "amp",
    );

    std::thread::scope(|scope| {
        for reader in 0..readers {
            let store = &store;
            let slot = &slot;
            let done = &done;
            let histograms = &histograms;
            scope.spawn(move || {
                let mut local = Latencies::new();
                let mut local_slot = slot.load(Ordering::Relaxed);
                // Per-thread: a shared generator would time its own lock.
                let mut state =
                    0x243F_6A88_85A3_08D3u64 ^ (reader as u64).wrapping_mul(0x9E37_79B9);
                while !done.load(Ordering::Relaxed) {
                    let now_slot = slot.load(Ordering::Relaxed);
                    if now_slot != local_slot {
                        histograms[local_slot]
                            .lock()
                            .expect("histogram")
                            .absorb(&local);
                        local.clear();
                        local_slot = now_slot;
                    }
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    let key = record_key(live_position(state % live));
                    let start = Instant::now();
                    let found = store.get(&key).expect("read").expect("live record");
                    local.record(start.elapsed().as_nanos() as u64, found.len());
                }
                histograms[local_slot]
                    .lock()
                    .expect("histogram")
                    .absorb(&local);
            });
        }

        for _ in 0..drivers {
            let store = &store;
            let done = &done;
            let governor = &governor;
            let occupancy = &occupancy;
            scope.spawn(move || {
                // The debt is added on top of when the pass began, not on top of
                // now, or the pass's own runtime is spent twice.
                let mut began_at = Instant::now();
                while !done.load(Ordering::Relaxed) {
                    if governor.target.load(Ordering::Relaxed) == 0 {
                        std::thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    let counters = store.compaction_counters();
                    let moved = counters.read_bytes + counters.compaction_bytes;
                    let owed = governor.owed(moved, began_at);
                    if !owed.is_zero() {
                        // Slept in slices, so a long debt still sees the next arm.
                        std::thread::sleep(owed.min(Duration::from_millis(5)));
                        continue;
                    }
                    let start = Instant::now();
                    began_at = start;
                    store.compact_once().expect("compact");
                    let elapsed = start.elapsed();
                    let after = store.compaction_counters();
                    // A pass that moved nothing is not occupancy, and spinning on
                    // it would spend the core the rewrite needs.
                    if after.read_bytes + after.compaction_bytes > moved {
                        occupancy
                            .busy_nanos
                            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
                        occupancy.passes.fetch_add(1, Ordering::Relaxed);
                    } else {
                        std::thread::sleep(Duration::from_millis(20));
                    }
                }
            });
        }

        for (index, arm) in arms.iter().enumerate() {
            let counters = store.compaction_counters();
            governor.aim(
                arm.target(),
                counters.read_bytes + counters.compaction_bytes,
            );
            slot.store(index * 2, Ordering::Relaxed);
            std::thread::sleep(Duration::from_secs_f64(settle_seconds()));

            slot.store(index * 2 + 1, Ordering::Relaxed);
            let opening = store.compaction_counters();
            let busy_before = occupancy.busy_nanos.load(Ordering::Relaxed);
            let passes_before = occupancy.passes.load(Ordering::Relaxed);
            let dead_start = store.dead_bytes().to_bytes();
            let device_before = device_read_bytes();
            let start = Instant::now();
            std::thread::sleep(Duration::from_secs_f64(arm_seconds()));
            let elapsed = start.elapsed().as_secs_f64();
            let closing = store.compaction_counters();
            let busy = occupancy.busy_nanos.load(Ordering::Relaxed) - busy_before;
            let passes = occupancy.passes.load(Ordering::Relaxed) - passes_before;
            let dead_end = store.dead_bytes().to_bytes();
            let device = device_read_bytes().saturating_sub(device_before);

            // The readers flush a closed slot during the next arm's settle.
            slot.store(index * 2 + 2, Ordering::Relaxed);
            std::thread::sleep(Duration::from_millis(400));
            let mut merged = Latencies::new();
            merged.absorb(&histograms[index * 2 + 1].lock().expect("histogram"));

            let background = (closing.read_bytes + closing.compaction_bytes)
                .saturating_sub(opening.read_bytes + opening.compaction_bytes);
            let written = closing
                .compaction_bytes
                .saturating_sub(opening.compaction_bytes);
            // Share of the window a pass held the device, over the drivers that
            // could be holding it at once.
            let duty = busy as f64 / 1e9 / elapsed / drivers as f64;
            let burst = match busy {
                0 => 0.0,
                busy => background as f64 / (busy as f64 / 1e9) / 1e6,
            };
            // Exact on a quiet arm and an estimate elsewhere, since a compaction
            // read may be served from the page cache it just filled.
            let foreground_device =
                device.saturating_sub(closing.read_bytes.saturating_sub(opening.read_bytes));
            let row = Row {
                arm: *arm,
                background_mbps: background as f64 / elapsed / 1e6,
                written_mbps: written as f64 / elapsed / 1e6,
                duty,
                burst_mbps: burst,
                pass_ms: busy as f64 / 1e6 / passes.max(1) as f64,
                reads_per_second: merged.count as f64 / elapsed,
                p50_us: merged.quantile_us(0.50),
                p99_us: merged.quantile_us(0.99),
                p999_us: merged.quantile_us(0.999),
                max_us: merged.max_us(),
                device_mbps: device as f64 / elapsed / 1e6,
                amplification: foreground_device as f64 / merged.served.max(1) as f64,
                dead_start,
                dead_end,
            };
            print_row(&row);
            rows.push(row);
        }
        done.store(true, Ordering::Relaxed);
    });

    println!(
        "\n{:>10} {:>12} {:>12} {:>12}",
        "arm", "dead start", "dead end", "reclaimed"
    );
    for row in &rows {
        println!(
            "{:>10} {:>11.1}G {:>11.1}G {:>11.1}G",
            row.arm.label(),
            row.dead_start as f64 / 1e9,
            row.dead_end as f64 / 1e9,
            row.dead_start.saturating_sub(row.dead_end) as f64 / 1e9,
        );
    }

    let live_bytes = store.totals().bytes.to_bytes();
    let last_dead = rows.last().map(|row| row.dead_end).unwrap_or(0);
    let dead_fraction = last_dead as f64 / (last_dead + live_bytes).max(1) as f64;
    println!(
        "\ndead fraction at the end {dead_fraction:.2}, live {:.1}G",
        live_bytes as f64 / 1e9
    );

    // Every compaction arm has to have moved bytes, or its label is a fiction. An
    // arm that opened under a segment of dead space is the drained-volume case.
    for row in &rows {
        if row.arm == Arm::Quiet {
            continue;
        }
        if row.dead_start < segment_bytes() {
            println!(
                "{} opened with under a segment of dead space",
                row.arm.label()
            );
            continue;
        }
        assert!(
            row.background_mbps > 0.0,
            "{} moved nothing, so it is a quiet arm wearing another label",
            row.arm.label(),
        );
    }

    if !past_memory {
        println!("guards skipped: this fill was inside memory");
        return;
    }

    // A run that drained its debt turned its later arms quiet without saying so.
    assert!(
        dead_fraction > 0.20,
        "the volume ran out of dead space at {dead_fraction:.2}, so the later arms had \
         no background load to pace",
    );

    // The opening quiet arm runs straight off the cache drop and is the one arm
    // whose reads are known cold. The arms after it warm as readers revisit keys,
    // which the amp column reports rather than asserts.
    let first_quiet = rows
        .iter()
        .find(|row| row.arm == Arm::Quiet)
        .expect("a quiet arm");
    if first_quiet.device_mbps > 0.0 {
        assert!(
            first_quiet.amplification >= 1.0,
            "the opening quiet arm's reads were served warm at amplification {:.2}, so the \
             fill did not reach past memory",
            first_quiet.amplification,
        );
    }

    // A flat curve here is a broken harness, not a store that copies for free.
    // Measured against the best quiet arm, the hardest baseline to beat.
    let quiet_p999 = rows
        .iter()
        .filter(|row| row.arm == Arm::Quiet)
        .map(|row| row.p999_us)
        .fold(f64::INFINITY, f64::min);
    let loudest = rows
        .iter()
        .filter(|row| row.arm != Arm::Quiet)
        .max_by(|left, right| left.background_mbps.total_cmp(&right.background_mbps));
    if let Some(loudest) = loudest {
        assert!(
            loudest.p999_us > quiet_p999 * 1.2,
            "{} moved {:.0} MB/s and moved the p99.9 from {:.0} to {:.0} us, which is no \
             interference at all: the foreground is not sharing the device",
            loudest.arm.label(),
            loudest.background_mbps,
            quiet_p999,
            loudest.p999_us,
        );
    }
}

fn print_row(row: &Row) {
    let requested = match row.arm.requested_mbps() {
        Some(mbps) => mbps.to_string(),
        None => "none".to_string(),
    };
    println!(
        "{:>10} {requested:>9} {:>9.0} {:>9.0} {:>6.0}% {:>9.0} {:>9.0} {:>10.0} {:>9.0} {:>9.0} {:>9.0} {:>10.0} {:>9.0} {:>6.2}",
        row.arm.label(),
        row.background_mbps,
        row.written_mbps,
        row.duty * 100.0,
        row.burst_mbps,
        row.pass_ms,
        row.reads_per_second,
        row.p50_us,
        row.p99_us,
        row.p999_us,
        row.max_us,
        row.device_mbps,
        row.amplification,
    );
}
