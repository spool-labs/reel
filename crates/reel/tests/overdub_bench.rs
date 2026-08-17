//! The composed posture driven end to end by a state-shaped write and read stream
//!
//! Every knob the posture is made of is priced somewhere on its own. This drives them
//! together against the traffic they were chosen for: a small hot core rewritten every
//! round, a mid population whose re-write gaps are long tailed, and a majority of keys
//! written exactly once. The reads are batched and skewed toward what was just written,
//! which is what makes a paged get's search count the term that matters.
//!
//! Two flavours, each on its own volume, each run with the merge armed and with it left
//! standing. The merge column is the one to read first: an armed volume collapses its
//! sorted runs on its own tick, and the undriven cell is the same traffic with the runs
//! left to pile up, so the pair prices what the collapse is worth. At the shipped
//! triggers the pair comes out identical, because the reclaim rewrite takes a segment's
//! dead bytes long before the stack as a whole reaches the collapse mark; putting
//! `REEL_OVERDUB_MERGE_RATIO` under where the stack settles is what makes the merge fire.
//!
//! Two read columns rather than one: a search that finds its footer parsed and held costs
//! no device read at all, so a volume small enough to fit its own footer cache prints
//! nothing in the device column and everything it did in the search one.
//!
//! `REEL_OVERDUB_READERS` spreads a round's read batches over that many threads while the
//! driver keeps the writes and the tick, so a wide box is asked for more than one core can
//! ask it. One reader is the sequential run: the driver takes the batches itself, in order,
//! between the writes and the tick. Past one the columns are a contended volume's, the
//! writes timed against readers on the same store. Whatever the count, a round joins its
//! readers before it samples, since a run count or a probe total read while the volume is
//! still being asked is neither this round's nor the next's. `REEL_OVERDUB_TAILS` sets the
//! append tails the volume runs, which is what a spread write stream needs to land in.
//!
//! Knobs, all optional: `REEL_OVERDUB_DIR`, `REEL_OVERDUB_ROUNDS`, `REEL_OVERDUB_SCALE`,
//! `REEL_OVERDUB_HOT`, `REEL_OVERDUB_MID`, `REEL_OVERDUB_FRESH`, `REEL_OVERDUB_READS`,
//! `REEL_OVERDUB_BATCH`, `REEL_OVERDUB_VALUE`, `REEL_OVERDUB_SEGMENT`,
//! `REEL_OVERDUB_PASSES`, `REEL_OVERDUB_CARRIED`, `REEL_OVERDUB_FOOTER_CACHE`,
//! `REEL_OVERDUB_COMPACT_MBPS`, `REEL_OVERDUB_DEAD_RATIO`, `REEL_OVERDUB_MERGE_RATIO`,
//! `REEL_OVERDUB_READERS`, `REEL_OVERDUB_TAILS`.
//!
//! Point `REEL_OVERDUB_DIR` at the filesystem under test. Without it the cells land
//! wherever the temporary directory does, which on a machine with a memory backed one
//! measures no device at all.
//!
//! Ignored by default. Run with:
//!   cargo test -p reel --test overdub_bench --release -- --ignored --nocapture --test-threads=1

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use tempfile::TempDir;

use reel::format::column::ROW_CARRY_MAX;
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, CompactPass, CompactRate, FenceResidency,
    IndexResidency, KeyWidth, MapShape, MergeReport, Preallocate, ProbeCounts, RecordKey,
    ReelConfig, ReelStore, ShardShapes, SyncPolicy, ThreadBudget,
};

/// The one column the workload writes
const STATE: ColumnId = ColumnId(1);

/// Bytes a key occupies
const KEY_WIDTH: usize = 32;

/// Leading key bytes the column shards on
///
/// One, so a scattered key spreads over two hundred and fifty six shards. Two would give
/// a population this size about three keys a shard, which measures the shard array.
const SHARD_BYTES: u8 = 1;

/// Bits per key a seal spends on a filter
///
/// Declared for both flavours; a resident volume answers from its map and spends none of
/// them whatever this says.
const FILTER_BITS: u8 = 10;

/// Keys the hot core holds, every one of them rewritten every round
const HOT_KEYS: u64 = 1_400;

/// Keys the mid population holds, each with a re-write gap of its own
///
/// Sized so the gap distribution below draws about a thousand of them a round, which is
/// what the mid share of the write stream comes to.
const MID_KEYS: u64 = 3_000;

/// Keys each round opens and never writes again
const FRESH_KEYS: u64 = 600;

/// Keys a round reads back
const READ_KEYS: u64 = 3_500;

/// Keys one batched read asks for at once
const BATCH_KEYS: u64 = 128;

/// Rounds a bare run drives
const ROUNDS: u64 = 200;

/// Threads a round's read batches are spread over
///
/// One, so a bare run is the sequential stream it always was. A box with cores to spare
/// wants enough of them that the reads stop being what the wall clock is waiting on.
const READER_THREADS: u64 = 1;

/// Append tails the volume runs, zero for the machine's own count
const ACTIVE_TAILS: u64 = 1;

/// Mean value length in bytes, which the spread sits either side of
const VALUE_MEAN: u64 = 180;

/// Segment size, which is how many rounds' writes stand in one sorted run
const SEGMENT_BYTES: u64 = 2 * 1024 * 1024;

/// Bytes reserved ahead of the write head per allocation step
const ALLOC_CHUNK: u64 = 256 * 1024;

/// Rewrite passes one tick drives before it gives the round back
const COMPACT_PASSES: u64 = 8;

/// Compaction pace, in megabytes a second
///
/// Effectively unpaced by default, so both cells of a pair drain their debt at the same
/// speed and the merge column is the only thing that differs between them. A box run
/// should set this to what its device actually gives back.
const COMPACT_MBPS: u64 = 100_000;

/// Byte budget for the values the index carries, zero for the shipped unshed tier
const CARRIED_BYTES: u64 = 0;

/// Bytes of sealed-footer state a paged volume keeps at once
///
/// The shipped size, which on a volume this small holds every footer, so a search reads
/// no blocks and the device column stays at nothing. A box run wanting that column to
/// mean something has to set this below what the volume ends up holding.
const FOOTER_CACHE_BYTES: u64 = 64 * 1024 * 1024;

/// Dead fraction at which a sealed segment is rewritten, the shipped one
const COMPACT_DEAD_RATIO: f64 = 0.50;

/// Dead share of the standing runs at which the tick collapses them, the shipped one
///
/// The rewrite above is what keeps this from ever being reached under steady traffic: it
/// reclaims a segment's dead bytes long before the stack as a whole goes half dead. A run
/// wanting to price the collapse has to put this under where the stack actually settles.
const MERGE_DEAD_RATIO: f64 = 0.50;

/// Rounds back a read still counts as recent
const RECENT_ROUNDS: usize = 3;

/// Rounds back a read counts as mid recency, past which it is cold
const MID_RECENCY_ROUNDS: usize = 48;

/// Share of a round's reads, in hundredths, that go to recently written keys
const RECENT_SHARE: u64 = 65;

/// Share of a round's reads, in hundredths, that go to mid recency
const MID_RECENCY_SHARE: u64 = 30;

/// Quantiles of the measured re-write gap, in rounds, with the ends the run is capped at
///
/// Interpolated on a log scale between the knots, so the drawn gaps reproduce the
/// measured p50, p90 and p99 exactly and stay smooth in between.
const GAP_QUANTILES: [(f64, f64); 5] = [
    (0.0, 1.0),
    (0.5, 3.0),
    (0.9, 258.0),
    (0.99, 4_300.0),
    (1.0, 20_000.0),
];

/// Salt that turns a key number into the bytes of its key
const KEY_SALT: u64 = 0x51ED_270B_2717_7F32;

/// Salt that draws a mid key's re-write gap
const GAP_SALT: u64 = 0x2545_F491_4F6C_DD1D;

/// Salt that places a mid key's rewrites inside its gap
const PHASE_SALT: u64 = 0x1D8E_4E27_C47D_124F;

/// Salt that draws a key's value length
const VALUE_SALT: u64 = 0x9E37_79B9_7F4A_7C15;

/// Salt the read draw runs off
const READ_SALT: u64 = 0xBF58_476D_1CE4_E5B9;

/// What one run of the bench was asked for
struct Knobs {
    /// Rounds each cell drives
    rounds: u64,

    /// Keys the hot core holds
    hot: u64,

    /// Keys the mid population holds
    mid: u64,

    /// Keys each round opens for the first and last time
    fresh: u64,

    /// Keys a round reads back
    reads: u64,

    /// Keys one batched read asks for
    batch: usize,

    /// Threads a round's read batches are spread over
    readers: usize,

    /// Append tails the volume runs
    tails: u32,

    /// Mean value length in bytes
    value_mean: u64,

    /// Segment size in bytes
    segment_bytes: u64,

    /// Rewrite passes one tick drives
    passes: u64,

    /// Byte budget for the carried values
    carried_bytes: u64,

    /// Bytes of sealed-footer state a paged volume keeps at once
    footer_cache_bytes: u64,

    /// Compaction pace in megabytes a second
    compact_mbps: u64,

    /// Dead fraction at which a sealed segment is rewritten
    compact_dead_ratio: f64,

    /// Dead share of the standing runs at which the tick collapses them
    merge_dead_ratio: f64,
}

/// A number from the environment, or the fallback
///
/// Loud on anything it cannot read: a mistyped knob that falls back quietly is a run
/// measuring a cell nobody asked for.
fn env_num(name: &str, fallback: u64) -> u64 {
    let Ok(raw) = std::env::var(name) else {
        return fallback;
    };
    raw.trim()
        .parse()
        .unwrap_or_else(|_| panic!("{name} holds {raw:?}, which is not a number"))
}

/// A byte count, in bytes or with a binary suffix
fn env_bytes(name: &str, fallback: u64) -> u64 {
    let Ok(raw) = std::env::var(name) else {
        return fallback;
    };
    let text = raw.trim();
    for (suffix, scale) in [("GiB", 1u64 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)] {
        let lower = suffix.to_lowercase();
        let Some(count) = text
            .strip_suffix(suffix)
            .or_else(|| text.strip_suffix(&lower))
        else {
            continue;
        };
        let count: u64 = count.trim().parse().unwrap_or_else(|_| {
            panic!("{name} holds {raw:?}, whose {suffix} count is not a number")
        });
        return count * scale;
    }
    text.parse().unwrap_or_else(|_| {
        panic!("{name} holds {raw:?}, which is not a byte count or a KiB/MiB/GiB of one")
    })
}

/// A share between nothing and everything, from the environment, or the fallback
fn env_share(name: &str, fallback: f64) -> f64 {
    let Ok(raw) = std::env::var(name) else {
        return fallback;
    };
    let share: f64 = raw
        .trim()
        .parse()
        .unwrap_or_else(|_| panic!("{name} holds {raw:?}, which is not a number"));
    assert!(
        (0.0..=1.0).contains(&share),
        "{name} holds {raw:?}, which is not a share between zero and one",
    );
    share
}

/// What the run was asked for, with the scale knob already folded into the populations
fn knobs() -> Knobs {
    let scale = env_num("REEL_OVERDUB_SCALE", 1).max(1);
    Knobs {
        rounds: env_num("REEL_OVERDUB_ROUNDS", ROUNDS).max(1),
        hot: env_num("REEL_OVERDUB_HOT", HOT_KEYS) * scale,
        mid: env_num("REEL_OVERDUB_MID", MID_KEYS) * scale,
        fresh: env_num("REEL_OVERDUB_FRESH", FRESH_KEYS) * scale,
        reads: env_num("REEL_OVERDUB_READS", READ_KEYS) * scale,
        batch: env_num("REEL_OVERDUB_BATCH", BATCH_KEYS).max(1) as usize,
        readers: env_num("REEL_OVERDUB_READERS", READER_THREADS).max(1) as usize,
        tails: env_num("REEL_OVERDUB_TAILS", ACTIVE_TAILS) as u32,
        value_mean: env_num("REEL_OVERDUB_VALUE", VALUE_MEAN).max(2),
        segment_bytes: env_bytes("REEL_OVERDUB_SEGMENT", SEGMENT_BYTES),
        passes: env_num("REEL_OVERDUB_PASSES", COMPACT_PASSES),
        carried_bytes: env_bytes("REEL_OVERDUB_CARRIED", CARRIED_BYTES),
        footer_cache_bytes: env_bytes("REEL_OVERDUB_FOOTER_CACHE", FOOTER_CACHE_BYTES),
        compact_mbps: env_num("REEL_OVERDUB_COMPACT_MBPS", COMPACT_MBPS).max(1),
        compact_dead_ratio: env_share("REEL_OVERDUB_DEAD_RATIO", COMPACT_DEAD_RATIO),
        merge_dead_ratio: env_share("REEL_OVERDUB_MERGE_RATIO", MERGE_DEAD_RATIO),
    }
}

/// A scattered number from a key number and a salt
fn mix(number: u64, salt: u64) -> u64 {
    let mut state = number
        .wrapping_add(salt)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15);
    state ^= state >> 30;
    state = state.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    state ^= state >> 27;
    state = state.wrapping_mul(0x94D0_49BB_1331_11EB);
    state ^ (state >> 31)
}

/// The key a key number stands for, scattered so no run's range rules another one out
fn key_at(number: u64) -> RecordKey {
    let mut bytes = [0u8; KEY_WIDTH];
    bytes[..8].copy_from_slice(&mix(number, KEY_SALT).to_be_bytes());
    bytes[8..16].copy_from_slice(&number.to_be_bytes());
    RecordKey::from_bytes(STATE, &bytes).expect("key")
}

/// Bytes this key's value occupies, spread either side of the mean
fn value_len(number: u64, mean: u64) -> usize {
    (mean / 2 + mix(number, VALUE_SALT) % mean) as usize
}

/// Bytes a sealed row reserves for the value itself
///
/// The widest the spread produces, or the widest a row will hold, whichever is smaller:
/// past the ceiling the top of the spread rides as a record and the rest still carries.
fn carry_width(mean: u64) -> u16 {
    (mean / 2 + mean - 1).min(ROW_CARRY_MAX as u64) as u16
}

/// The gap at one point of the measured distribution, in rounds
fn gap_at(quantile: f64) -> u64 {
    for pair in GAP_QUANTILES.windows(2) {
        let (low_quantile, low_gap) = pair[0];
        let (high_quantile, high_gap) = pair[1];
        if quantile > high_quantile {
            continue;
        }
        let span = (quantile - low_quantile) / (high_quantile - low_quantile);
        return (low_gap * (high_gap / low_gap).powf(span)).round().max(1.0) as u64;
    }
    GAP_QUANTILES[GAP_QUANTILES.len() - 1].1 as u64
}

/// Rounds between this mid key's rewrites
fn gap_of(number: u64) -> u64 {
    gap_at(mix(number, GAP_SALT) as f64 / u64::MAX as f64)
}

/// Where inside its gap this mid key's rewrites fall
fn phase_of(number: u64, gap: u64) -> u64 {
    mix(number, PHASE_SALT) % gap
}

/// How the key numbers are laid out: the hot core, then the mid population, then a block
/// of fresh keys for every round
struct Population {
    /// Keys the hot core holds
    hot: u64,

    /// Keys the mid population holds
    mid: u64,

    /// Keys one round's fresh block holds
    fresh: u64,
}

impl Population {
    /// The first key number the mid population holds
    fn mid_base(&self) -> u64 {
        self.hot
    }

    /// The first key number of a round's fresh block
    fn fresh_base(&self, round: u64) -> u64 {
        self.hot + self.mid + round * self.fresh
    }
}

/// The key numbers a round writes: the whole hot core, the mid keys their gaps are due
/// on, and a block of keys nothing will write again
fn writes_of(population: &Population, round: u64, into: &mut Vec<u64>) {
    into.clear();
    for at in 0..population.hot {
        into.push(at);
    }
    for at in 0..population.mid {
        let number = population.mid_base() + at;
        let gap = gap_of(number);
        if round % gap == phase_of(number, gap) {
            into.push(number);
        }
    }
    for at in 0..population.fresh {
        into.push(population.fresh_base(round) + at);
    }
}

/// Whether this round's writes are the first this key ever gets
///
/// A concurrent round holds these reads back until its writes have landed: a read racing
/// the put that opens a key is answered with a miss, and a miss is not a read.
fn is_opened_in(population: &Population, number: u64, round: u64) -> bool {
    if number < population.hot {
        return round == 0;
    }
    if number < population.mid_base() + population.mid {
        let gap = gap_of(number);
        return round == phase_of(number, gap);
    }
    number >= population.fresh_base(round)
}

/// The rounds a read draw can still reach back into, newest last
struct Recency {
    /// What each of those rounds wrote, in the order the rounds ran
    rounds: VecDeque<Vec<u64>>,
}

impl Recency {
    /// A window holding nothing, for a run that has not written a round yet
    fn new() -> Recency {
        Recency {
            rounds: VecDeque::with_capacity(MID_RECENCY_ROUNDS + 1),
        }
    }

    /// Take what a round wrote, dropping whatever has aged past mid recency
    fn note(&mut self, written: &[u64]) {
        self.rounds.push_back(written.to_vec());
        while self.rounds.len() > MID_RECENCY_ROUNDS {
            self.rounds.pop_front();
        }
    }

    /// A key from one of the last few rounds
    fn recent(&self, roll: u64) -> Option<u64> {
        self.at(roll, 0, RECENT_ROUNDS)
    }

    /// A key from the rounds behind those and inside the window
    fn mid(&self, roll: u64) -> Option<u64> {
        self.at(roll, RECENT_ROUNDS, MID_RECENCY_ROUNDS)
    }

    /// A key from a round between these two depths, counting back from the newest
    fn at(&self, roll: u64, from: usize, until: usize) -> Option<u64> {
        let held = self.rounds.len();
        let reach = until.min(held);
        if from >= reach {
            return None;
        }
        let depth = from + (roll % (reach - from) as u64) as usize;
        let round = self.rounds.get(held - 1 - depth)?;
        round
            .get(mix(roll, READ_SALT) as usize % round.len().max(1))
            .copied()
    }
}

/// The key numbers a round reads back, skewed the way the traffic was measured
///
/// Two thirds land on what the last few rounds wrote, a slice on the rounds behind those,
/// and the rest on keys written once and long since aged out of the window. The cold draw
/// is computed from the round rather than remembered, since keeping every fresh block
/// would be the whole key space in memory.
fn reads_of(
    population: &Population,
    knobs: &Knobs,
    recency: &Recency,
    round: u64,
    into: &mut Vec<u64>,
) {
    into.clear();
    let cold_rounds = round.saturating_sub(MID_RECENCY_ROUNDS as u64);
    for at in 0..knobs.reads {
        let roll = mix(round * knobs.reads + at, READ_SALT);
        let share = roll % 100;
        let drawn = match share {
            share if share < RECENT_SHARE => recency.recent(roll >> 7),
            share if share < RECENT_SHARE + MID_RECENCY_SHARE => recency.mid(roll >> 7),
            _ => match (cold_rounds, population.fresh) {
                (0, _) | (_, 0) => None,
                _ => {
                    let cold = (roll >> 7) % cold_rounds;
                    Some(population.fresh_base(cold) + (roll >> 23) % population.fresh)
                }
            },
        };
        // A window that has not filled yet answers with nothing, and the hot core is the
        // one population that exists from the first round on.
        into.push(drawn.unwrap_or_else(|| roll % population.hot.max(1)));
    }
}

/// One flavour of the composed posture
struct Arm {
    /// What the table calls it
    name: &'static str,

    /// Where a sealed segment's keys live
    index: IndexResidency,

    /// Where the fence over a sealed segment's blocks lives
    fence: FenceResidency,

    /// Whether the volume writes its index down for the next open
    is_checkpointing: bool,
}

const ARMS: [Arm; 2] = [
    Arm {
        name: "paged-carrying",
        index: IndexResidency::Paged,
        fence: FenceResidency::Resident,
        is_checkpointing: false,
    },
    Arm {
        name: "resident",
        index: IndexResidency::Resident,
        fence: FenceResidency::Off,
        is_checkpointing: true,
    },
];

/// The column both flavours declare, its sealed rows sized to hold the values themselves
///
/// Leaked once a cell, since a column set is static and the carry width comes from a knob.
fn columns(row_carry: u16) -> ColumnSet {
    let specs: Box<[ColumnSpec]> = Box::new([ColumnSpec {
        id: STATE,
        name: "state",
        key_width: KeyWidth::Fixed(KEY_WIDTH as u16),
        shard_bytes: SHARD_BYTES,
        inline_max: 0,
        row_carry,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Open,
    }]);
    Box::leak(specs)
}

/// What one cell opens its volume with, the flavour and the merge being all that differ
fn config(arm: &Arm, is_merge_driven: bool, knobs: &Knobs) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(knobs.segment_bytes),
        alloc_chunk: ByteCount::from_bytes(ALLOC_CHUNK),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(knobs.tails),
        // Off, so nothing reads the volume behind the reads being timed.
        scrub_mbps: 0,
        index: arm.index,
        fence: arm.fence,
        index_checkpoint: arm.is_checkpointing,
        shard_shapes: ShardShapes::Declared,
        rewrite_on_seal: true,
        merge_sorted_runs: is_merge_driven,
        filter_bits: FILTER_BITS,
        carried_budget: ByteCount::from_bytes(knobs.carried_bytes),
        footer_cache: ByteCount::from_bytes(knobs.footer_cache_bytes),
        compact_mbps: CompactRate::Mbps(knobs.compact_mbps),
        compact_dead_ratio: knobs.compact_dead_ratio,
        merge_dead_ratio: knobs.merge_dead_ratio,
        ..ReelConfig::default()
    }
}

/// One maintenance tick, identical on every cell, with the merge report handed back
///
/// The pieces rather than the whole plane: the tick that ships swallows its merge report
/// and the driven column would have nothing to print.
fn tick(store: &ReelStore, knobs: &Knobs) -> MergeReport {
    store.page_out_sealed().expect("page out");
    for _ in 0..knobs.passes {
        match store.compact_once().expect("compact") {
            CompactPass::Copied => {}
            CompactPass::Idle | CompactPass::Held => break,
        }
    }
    store.shed_carried();
    store.merge_when_due().expect("merge").unwrap_or_default()
}

/// Bytes the volume occupies, whatever the index thinks it holds
fn dir_bytes(dir: &Path) -> u64 {
    let mut total = 0u64;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return total;
    };
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        match meta.is_dir() {
            true => total += dir_bytes(&entry.path()),
            false => total += meta.len(),
        }
    }
    total
}

/// The value at one point of a sorted sample
fn quantile(sorted: &[u64], at: f64) -> u64 {
    match sorted.is_empty() {
        true => 0,
        false => sorted[((sorted.len() - 1) as f64 * at).round() as usize],
    }
}

/// What one cell of the matrix measured
struct Cell {
    /// Records the write stream put
    written: u64,

    /// Seconds those writes took, with nothing else inside the clock
    write_secs: f64,

    /// Batched read latency, in nanoseconds, at the median and the tail
    get_p50: u64,
    get_p99: u64,

    /// Keys the read stream asked for
    gets: u64,

    /// Keys the read stream asked for and did not get
    misses: u64,

    /// What asking the sealed segments cost, over the whole run
    probes: ProbeCounts,

    /// Bytes the index held at the end, before any tick ran under it
    resident: u64,

    /// Bytes the volume occupied at the end
    on_disk: u64,

    /// Bytes the counters still resolved into records at the end
    live: u64,

    /// Bytes the reclaim rewrite copied over the run
    rewritten: u64,

    /// Segments standing after every round
    standing: Vec<usize>,

    /// Passes that collapsed at least one run
    merges: u64,

    /// Bytes those passes read and did not write out again
    collapsed: u64,
}

impl Cell {
    /// Records a second the write stream put, with nothing else inside the clock
    fn writes_per_sec(&self) -> f64 {
        self.written as f64 / self.write_secs.max(f64::MIN_POSITIVE)
    }

    /// Sealed segments one key of a batch was searched in, past what the filter ruled out
    fn searches_per_get(&self) -> f64 {
        self.probes.searched() as f64 / self.gets.max(1) as f64
    }

    /// Device reads one key of a batch cost, over both halves of a search
    ///
    /// Nothing where every footer the searches touched was already parsed and held, which
    /// is what a footer cache the size of the volume gives.
    fn reads_per_get(&self) -> f64 {
        (self.probes.block_reads + self.probes.map_reads) as f64 / self.gets.max(1) as f64
    }

    /// What the volume occupies for every byte it still resolves into a record
    fn disk_over_live(&self) -> f64 {
        self.on_disk as f64 / self.live.max(1) as f64
    }

    /// Segments standing across the run, averaged over the rounds
    fn standing_mean(&self) -> f64 {
        match self.standing.is_empty() {
            true => 0.0,
            false => self.standing.iter().sum::<usize>() as f64 / self.standing.len() as f64,
        }
    }

    /// The most segments that ever stood at once
    fn standing_max(&self) -> usize {
        self.standing.iter().copied().max().unwrap_or(0)
    }
}

/// What a round's reads cost, kept per thread and merged once the round closes
///
/// Per thread rather than shared: a batch that takes a lock to post its own latency is
/// timing the lock as well, and at eight readers that is what the tail would be.
struct Reads {
    /// Keys the batches asked for
    gets: u64,

    /// Keys the batches asked for and did not get
    misses: u64,

    /// Nanoseconds each batch took
    batch_nanos: Vec<u64>,
}

impl Reads {
    fn new() -> Reads {
        Reads {
            gets: 0,
            misses: 0,
            batch_nanos: Vec::new(),
        }
    }

    /// Take another thread's tally into this one
    fn absorb(&mut self, other: Reads) {
        self.gets += other.gets;
        self.misses += other.misses;
        self.batch_nanos.extend(other.batch_nanos);
    }
}

/// Take batches off a round's read list until it is empty, timing each one
///
/// The cursor is what makes a batch one thread's: it hands out the same chunks a
/// sequential pass would walk, in the same order, to whoever asks next.
fn read_batches(
    store: &ReelStore,
    reading: &[u64],
    batch: usize,
    cursor: &AtomicUsize,
    into: &mut Reads,
) {
    let mut keys: Vec<RecordKey> = Vec::with_capacity(batch);
    loop {
        let at = cursor.fetch_add(1, Ordering::Relaxed) * batch;
        if at >= reading.len() {
            return;
        }
        keys.clear();
        for number in &reading[at..(at + batch).min(reading.len())] {
            keys.push(key_at(*number));
        }
        let began = Instant::now();
        let found = store.get_many(&keys).expect("get many");
        into.batch_nanos.push(began.elapsed().as_nanos() as u64);
        into.gets += keys.len() as u64;
        for answer in &found {
            if answer.is_none() {
                into.misses += 1;
            }
        }
    }
}

/// Put what the round writes, with nothing else inside the clock, and say what it took
fn write_round(
    store: &ReelStore,
    writing: &[u64],
    round: u64,
    knobs: &Knobs,
    payload: &mut Vec<u8>,
) -> f64 {
    let began = Instant::now();
    for number in writing {
        payload.clear();
        payload.resize(value_len(*number, knobs.value_mean), round as u8);
        store
            .put(&key_at(*number), payload.as_slice())
            .expect("put");
    }
    began.elapsed().as_secs_f64()
}

/// Drive the whole workload against one volume and say what it cost
fn run_cell(arm: &Arm, is_merge_driven: bool, knobs: &Knobs, root: &Path) -> Cell {
    std::fs::create_dir_all(root).expect("cell root");
    let store = ReelStore::open(
        root.to_path_buf(),
        config(arm, is_merge_driven, knobs),
        columns(carry_width(knobs.value_mean)),
    )
    .expect("open");

    let population = Population {
        hot: knobs.hot,
        mid: knobs.mid,
        fresh: knobs.fresh,
    };
    let mut recency = Recency::new();
    let mut writing: Vec<u64> = Vec::new();
    let mut reading: Vec<u64> = Vec::new();
    let mut racing: Vec<u64> = Vec::new();
    let mut held: Vec<u64> = Vec::new();
    let mut payload: Vec<u8> = Vec::with_capacity(knobs.value_mean as usize * 2);

    let mut cell = Cell {
        written: 0,
        write_secs: 0.0,
        get_p50: 0,
        get_p99: 0,
        gets: 0,
        misses: 0,
        probes: ProbeCounts::default(),
        resident: 0,
        on_disk: 0,
        live: 0,
        rewritten: 0,
        standing: Vec::with_capacity(knobs.rounds as usize),
        merges: 0,
        collapsed: 0,
    };
    let mut batch_nanos: Vec<u64> =
        Vec::with_capacity((knobs.rounds * knobs.reads / BATCH_KEYS) as usize);
    let probes_before = store.filter_probes();

    for round in 0..knobs.rounds {
        writes_of(&population, round, &mut writing);
        recency.note(&writing);
        reads_of(&population, knobs, &recency, round, &mut reading);

        let (mut reads, report) = match knobs.readers {
            1 => {
                let mut reads = Reads::new();
                cell.write_secs += write_round(&store, &writing, round, knobs, &mut payload);
                read_batches(
                    &store,
                    &reading,
                    knobs.batch,
                    &AtomicUsize::new(0),
                    &mut reads,
                );
                (reads, tick(&store, knobs))
            }
            readers => {
                racing.clear();
                held.clear();
                for number in &reading {
                    match is_opened_in(&population, *number, round) {
                        true => held.push(*number),
                        false => racing.push(*number),
                    }
                }
                let merged = Mutex::new(Reads::new());
                let cursor = AtomicUsize::new(0);
                std::thread::scope(|scope| {
                    for _ in 0..readers {
                        scope.spawn(|| {
                            let mut local = Reads::new();
                            read_batches(&store, &racing, knobs.batch, &cursor, &mut local);
                            merged.lock().expect("reads").absorb(local);
                        });
                    }
                    cell.write_secs += write_round(&store, &writing, round, knobs, &mut payload);
                });

                // The reads the writes were holding back, taken against the tick rather
                // than after it: a volume in the field is asked while it is collapsing.
                let cursor = AtomicUsize::new(0);
                let mut report = MergeReport::default();
                std::thread::scope(|scope| {
                    for _ in 0..readers {
                        scope.spawn(|| {
                            let mut local = Reads::new();
                            read_batches(&store, &held, knobs.batch, &cursor, &mut local);
                            merged.lock().expect("reads").absorb(local);
                        });
                    }
                    report = tick(&store, knobs);
                });
                (merged.into_inner().expect("reads"), report)
            }
        };
        cell.written += writing.len() as u64;
        cell.gets += reads.gets;
        cell.misses += reads.misses;
        batch_nanos.append(&mut reads.batch_nanos);

        if report.runs_merged > 0 {
            cell.merges += 1;
            cell.collapsed += report.bytes_read.saturating_sub(report.bytes_written);
        }
        // Every reader is joined by here, so the run count is this round's and not a
        // reading taken while the volume was still being asked.
        cell.standing.push(store.index().segments_snapshot().len());
    }

    store.flush().expect("flush");
    // Taken with the last round joined. A probe count is a handful of counters read one
    // at a time, so a reading taken beside a live reader is torn as well as short.
    cell.probes = store.filter_probes().since(probes_before);
    cell.resident = store.resident_bytes().to_bytes();
    cell.live = store.totals().bytes.to_bytes();
    cell.rewritten = store.compaction_counters().compaction_bytes;
    drop(store);
    cell.on_disk = dir_bytes(root);

    batch_nanos.sort_unstable();
    cell.get_p50 = quantile(&batch_nanos, 0.50);
    cell.get_p99 = quantile(&batch_nanos, 0.99);
    cell
}

// what the composed posture does under a state-shaped stream, by flavour and by merge
//
// Measurement only, apart from the checks that the run was a run: every key the stream
// asked for came back, and the volume ended holding something.
#[test]
#[ignore = "drives a whole workload, run explicitly on the machine under test"]
fn composed_posture() {
    // libtest leaves the test name line open, so a header printed into it starts a
    // screen-width right of the rows underneath.
    println!();
    let knobs = knobs();
    let held = TempDir::new().expect("tempdir");
    let root = match std::env::var("REEL_OVERDUB_DIR") {
        Ok(named) => PathBuf::from(named),
        Err(_) => held.path().to_path_buf(),
    };
    println!(
        "{} rounds, hot {} mid {} fresh {} a round, {} reads a round in batches of {}, \
         values about {} bytes, segments {} KiB, under {}",
        knobs.rounds,
        knobs.hot,
        knobs.mid,
        knobs.fresh,
        knobs.reads,
        knobs.batch,
        knobs.value_mean,
        knobs.segment_bytes / (1 << 10),
        root.display(),
    );
    println!(
        "reads on {} thread(s), {} append tails",
        knobs.readers,
        match knobs.tails {
            0 => "auto".to_string(),
            tails => tails.to_string(),
        },
    );
    println!(
        "trigger: rewrite at {:.2} dead, collapse at {:.2}, footers held {} MiB, pace {} MB/s",
        knobs.compact_dead_ratio,
        knobs.merge_dead_ratio,
        knobs.footer_cache_bytes / (1 << 20),
        knobs.compact_mbps,
    );
    println!(
        "{:>15} {:>9} {:>11} {:>8} {:>8} {:>11} {:>10} {:>13} {:>10} {:>12} {:>9} {:>9} {:>7} {:>10}",
        "arm",
        "merge",
        "writes/s",
        "p50 us",
        "p99 us",
        "search/get",
        "reads/get",
        "resident MiB",
        "disk/live",
        "rewrite MiB",
        "runs avg",
        "runs max",
        "merges",
        "freed MiB",
    );

    for arm in &ARMS {
        for is_merge_driven in [true, false] {
            let name = match is_merge_driven {
                true => "driven",
                false => "standing",
            };
            let cell = run_cell(
                arm,
                is_merge_driven,
                &knobs,
                &root.join(format!("{}-{name}", arm.name)),
            );

            assert!(cell.written > 0, "{} {name} wrote nothing", arm.name);
            assert_eq!(
                cell.misses, 0,
                "{} {name} lost keys the stream had written",
                arm.name
            );
            assert!(
                cell.live > 0,
                "{} {name} ended holding no live bytes",
                arm.name
            );
            assert!(cell.on_disk > 0, "{} {name} left nothing on disk", arm.name);

            println!(
                "{:>15} {:>9} {:>11.0} {:>8.1} {:>8.1} {:>11.3} {:>10.3} {:>13.1} {:>9.2}x {:>12.1} {:>9.1} {:>9} {:>7} {:>10.1}",
                arm.name,
                name,
                cell.writes_per_sec(),
                cell.get_p50 as f64 / 1e3,
                cell.get_p99 as f64 / 1e3,
                cell.searches_per_get(),
                cell.reads_per_get(),
                cell.resident as f64 / (1 << 20) as f64,
                cell.disk_over_live(),
                cell.rewritten as f64 / (1 << 20) as f64,
                cell.standing_mean(),
                cell.standing_max(),
                cell.merges,
                cell.collapsed as f64 / (1 << 20) as f64,
            );
        }
    }
    // Nothing is asserted about the timing columns: a microsecond figure held against
    // another would fail on a busy laptop and prove nothing about the posture.
}
