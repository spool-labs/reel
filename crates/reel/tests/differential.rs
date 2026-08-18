//! Differential property tests of the reel store against an in-memory oracle
//!
//! Each seeded op stream runs against a reel-mock store and a reel store at once, and the
//! observable state of both must agree after every step. Streams run at one active tail
//! and at four, since four tails is what puts the reel through the version guard on every
//! insert.

#[allow(dead_code)]
mod harness;

use reel::io::fault::{FaultKind, FaultPlan};
use reel::{
    ByteCount, CompactRate, FenceResidency, HotIndex, IndexResidency, Preallocate, ReelConfig,
    SyncPolicy, ThreadBudget,
};

use harness::fixture::Differential;
use harness::op_stream;

/// Seeds the default streams are drawn from
const SEEDS: &[u64] = &[1, 2, 3, 7, 42, 99, 123, 2024];

/// Length of each default stream
const STREAM_LEN: usize = 120;

/// Segment size that rolls a few times over a default stream
const SEGMENT_BYTES: u64 = 64 * 1024;

/// Space reserved ahead of the write head per allocation step
const ALLOC_CHUNK: u64 = 4 * 1024;

/// How often the soak stream checks agreement and compacts the reel
const SOAK_CHECK_EVERY: usize = 200;

/// Length of the soak stream when the environment does not override it
const DEFAULT_SOAK_OPS: usize = 1_000_000;

/// Segment size the soak uses so segments roll slowly enough for compaction to keep pace
const SOAK_SEGMENT_BYTES: u64 = 1024 * 1024;

/// Segment size a paged stream uses, small enough that a stream seals many segments
///
/// A key only reaches a footer once its segment seals, so a paged run over the default
/// segment would page nothing out and measure the resident index twice.
const PAGED_SEGMENT_BYTES: u64 = 16 * 1024;

/// Length of a paged stream, long enough to seal several segments at that size
const PAGED_STREAM_LEN: usize = 640;

/// Dead share of the standing runs a maintained stream's ticks merge at
///
/// At nothing, so every tick that finds a stack collapses it: a stream of a few hundred
/// ops leaves a shallower stack than a running volume's, and only some seeds would reach
/// a threshold worth the name. What this cell asks is whether a tick-driven merge still
/// serves what the oracle serves, not where the threshold belongs.
const MAINTAINED_MERGE_RATIO: f64 = 0.0;

fn reel_config(active_tails: u32) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(SEGMENT_BYTES),
        alloc_chunk: ByteCount::from_bytes(ALLOC_CHUNK),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::EveryPut,
        active_tails: ThreadBudget::threads(active_tails),
        ..ReelConfig::default()
    }
}

fn never_config(active_tails: u32) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(SEGMENT_BYTES),
        alloc_chunk: ByteCount::from_bytes(ALLOC_CHUNK),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(active_tails),
        ..ReelConfig::default()
    }
}

fn paged_config(active_tails: u32) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(PAGED_SEGMENT_BYTES),
        alloc_chunk: ByteCount::from_bytes(ALLOC_CHUNK),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(active_tails),
        index: IndexResidency::Paged,
        ..ReelConfig::default()
    }
}

/// A paged volume that rewrites its sealed segments in key order
///
/// The rate gate is lifted because a stream this small would otherwise spend its passes
/// held rather than rewriting.
fn rewriting_config(active_tails: u32) -> ReelConfig {
    ReelConfig {
        rewrite_on_seal: true,
        compact_mbps: CompactRate::Mbps(100_000),
        ..paged_config(active_tails)
    }
}

/// The same rewriting volume with the merge armed, so a caller may collapse its runs
///
/// The dead ratio is at one so compaction reclaims only wholly dead segments, which is
/// what leaves the half-live runs standing for a merge to find.
fn merging_config(active_tails: u32) -> ReelConfig {
    ReelConfig {
        merge_sorted_runs: true,
        compact_dead_ratio: 1.0,
        ..rewriting_config(active_tails)
    }
}

/// The same merging volume with the trigger a maintained stream's ticks decide on
fn maintained_config(active_tails: u32) -> ReelConfig {
    ReelConfig {
        merge_dead_ratio: MAINTAINED_MERGE_RATIO,
        ..merging_config(active_tails)
    }
}

/// A paged volume with barely room for its footers, whose sealed keys answer by fence
///
/// A bound this tight gives a footer up as soon as another wants its place, so most
/// searches descend the blocks rather than read the footer whole, and the residency
/// decides whether the leads are held or read. Not a bound of nothing: a playback has
/// to read the footer whole, and one it cannot keep it rereads for every page.
fn fenced_config(active_tails: u32, fence: FenceResidency) -> ReelConfig {
    ReelConfig {
        footer_cache: ByteCount::from_bytes(64 * 1024),
        fence,
        ..paged_config(active_tails)
    }
}

/// A hot index whose age has already run out, so it pages on the first tick
fn hot_config(active_tails: u32) -> ReelConfig {
    ReelConfig {
        index: IndexResidency::Hot(HotIndex {
            after_secs: 0,
            budget: ByteCount::gb(1),
        }),
        ..paged_config(active_tails)
    }
}

fn soak_config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(SOAK_SEGMENT_BYTES),
        alloc_chunk: ByteCount::from_bytes(ALLOC_CHUNK),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(4),
        ..ReelConfig::default()
    }
}

fn soak_op_count() -> usize {
    std::env::var("REEL_SOAK_OPS")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(DEFAULT_SOAK_OPS)
}

// a seeded stream keeps the oracle and the reel in agreement at one tail
#[test]
#[cfg(not(miri))]
fn single_tail() {
    for seed in SEEDS {
        let mut fixture = Differential::open(*seed, reel_config(1));
        fixture.run_stream(&op_stream::generate(*seed, STREAM_LEN));
    }
}

/// A plan that only moves when a caller is answered, never what it is answered with
///
/// Delays every few ops and drains in reverse of submit order, so a caller never sees the
/// order it filed in. Neither can fail an op, which is what lets the exact oracle stand.
fn completion_plan(seed: u64) -> FaultPlan {
    let mut plan = FaultPlan::new(seed).with_reorder();
    for at in (0..20_000u64).step_by(7) {
        plan = plan.with_fault(
            at,
            FaultKind::DelayCompletion {
                polls: 1 + (at % 3) as u32,
            },
        );
    }
    plan
}

// delayed and reordered completions do not change what any caller is answered
#[test]
#[cfg(not(miri))]
fn delayed_completions_single_tail() {
    for seed in SEEDS {
        let mut fixture =
            Differential::open_with_plan(*seed, reel_config(1), completion_plan(*seed));
        fixture.run_stream(&op_stream::generate(*seed, STREAM_LEN));
        let (fired, drawn) = fixture.fault_reach();
        assert!(
            fired > 0,
            "seed {seed}: the stream never reached one of {drawn} delays"
        );
    }
}

// the same under four tails, where the version guard runs on every insert
#[test]
#[cfg(not(miri))]
fn delayed_completions_multi_tail() {
    for seed in SEEDS {
        let mut fixture =
            Differential::open_with_plan(*seed, reel_config(4), completion_plan(*seed));
        fixture.run_stream(&op_stream::generate(*seed, STREAM_LEN));
        let (fired, drawn) = fixture.fault_reach();
        assert!(
            fired > 0,
            "seed {seed}: the stream never reached one of {drawn} delays"
        );
    }
}

// and on a paged column, where a delayed completion races the handover
#[test]
#[cfg(not(miri))]
fn delayed_completions_paged() {
    for seed in SEEDS {
        let mut fixture =
            Differential::open_with_plan(*seed, paged_config(1), completion_plan(*seed));
        fixture.run_stream(&op_stream::generate(*seed, STREAM_LEN));
        let (fired, drawn) = fixture.fault_reach();
        assert!(
            fired > 0,
            "seed {seed}: the stream never reached one of {drawn} delays"
        );
    }
}

// the same streams agree at four active tails, exercising the version guard
#[test]
#[cfg(not(miri))]
fn multi_tail() {
    for seed in SEEDS {
        let mut fixture = Differential::open(*seed, reel_config(4));
        fixture.run_stream(&op_stream::generate(*seed, STREAM_LEN));
    }
}

// under never sync a reopen still reproduces the oracle at one tail
#[test]
#[cfg(not(miri))]
fn never_reopen_single_tail() {
    for seed in SEEDS {
        let mut fixture = Differential::open(*seed, never_config(1));
        fixture.run_stream(&op_stream::generate(*seed, STREAM_LEN));
    }
}

// the same reopen agreement holds under never sync at four active tails
#[test]
#[cfg(not(miri))]
fn never_reopen_multi_tail() {
    for seed in SEEDS {
        let mut fixture = Differential::open(*seed, never_config(4));
        fixture.run_stream(&op_stream::generate(*seed, STREAM_LEN));
    }
}

// a paged volume serves what a resident one serves, with its keys in the footers
#[test]
#[cfg(not(miri))]
fn paged_single_tail() {
    for seed in SEEDS {
        let mut fixture = Differential::open(*seed, paged_config(1));
        fixture.run_stream(&op_stream::generate(*seed, PAGED_STREAM_LEN));
        fixture.assert_paged_out();
    }
}

// the same holds at four tails, where a key can seal in one and be rewritten in another
#[test]
#[cfg(not(miri))]
fn paged_multi_tail() {
    for seed in SEEDS {
        let mut fixture = Differential::open(*seed, paged_config(4));
        fixture.run_stream(&op_stream::generate(*seed, PAGED_STREAM_LEN));
        fixture.assert_paged_out();
    }
}

// a volume whose searches descend a fence serves what one that walks blocks serves
#[test]
#[cfg(not(miri))]
fn fenced_paged_single_tail() {
    for seed in SEEDS {
        let mut fixture = Differential::open(*seed, fenced_config(1, FenceResidency::Resident));
        fixture.run_stream(&op_stream::generate(*seed, PAGED_STREAM_LEN));
        fixture.assert_paged_out();
    }
}

// and with the leads left on the volume, where a search reads them a page at a time
#[test]
#[cfg(not(miri))]
fn fenced_paged_leads() {
    for seed in SEEDS {
        let mut fixture = Differential::open(*seed, fenced_config(1, FenceResidency::Paged));
        fixture.run_stream(&op_stream::generate(*seed, PAGED_STREAM_LEN));
        fixture.assert_paged_out();
    }
}

// a volume whose sealed rows carry their values serves what a resident one serves
#[test]
#[cfg(not(miri))]
fn carrying_paged_single_tail() {
    for seed in SEEDS {
        let mut fixture = Differential::open_carrying(*seed, paged_config(1));
        fixture.run_stream(&op_stream::generate(*seed, PAGED_STREAM_LEN));
        fixture.assert_paged_out();
    }
}

// the same at four tails, where a key can seal in one and be rewritten in another
#[test]
#[cfg(not(miri))]
fn carrying_paged_multi_tail() {
    for seed in SEEDS {
        let mut fixture = Differential::open_carrying(*seed, paged_config(4));
        fixture.run_stream(&op_stream::generate(*seed, PAGED_STREAM_LEN));
        fixture.assert_paged_out();
    }
}

// a volume that lists its rows and deletes the records serves what a resident one does
#[test]
#[cfg(not(miri))]
fn carrying_rewritten_single_tail() {
    for seed in SEEDS {
        let mut fixture = Differential::open_carrying(*seed, rewriting_config(1));
        fixture.run_stream(&op_stream::generate(*seed, PAGED_STREAM_LEN));
        fixture.assert_paged_out();
        fixture.assert_rows_listed();
    }
}

// the same at four tails, where the rewriter's tail is not the one the puts are on
#[test]
#[cfg(not(miri))]
fn carrying_rewritten_multi_tail() {
    for seed in SEEDS {
        let mut fixture = Differential::open_carrying(*seed, rewriting_config(4));
        fixture.run_stream(&op_stream::generate(*seed, PAGED_STREAM_LEN));
        fixture.assert_paged_out();
        fixture.assert_rows_listed();
    }
}

// a volume whose runs a merge collapses serves what one that never merged serves
#[test]
#[cfg(not(miri))]
fn merged_single_tail() {
    for seed in SEEDS {
        let mut fixture = Differential::open(*seed, merging_config(1));
        fixture.run_stream(&op_stream::generate(*seed, PAGED_STREAM_LEN));
        fixture.assert_paged_out();
        fixture.assert_merged_runs();
    }
}

// the same at four tails, where a key can seal in one tail and be rewritten in another
#[test]
#[cfg(not(miri))]
fn merged_multi_tail() {
    for seed in SEEDS {
        let mut fixture = Differential::open(*seed, merging_config(4));
        fixture.run_stream(&op_stream::generate(*seed, PAGED_STREAM_LEN));
        fixture.assert_paged_out();
        fixture.assert_merged_runs();
    }
}

// a volume whose own ticks collapse its runs serves what one that never merged serves
#[test]
#[cfg(not(miri))]
fn maintained_merge_single_tail() {
    for seed in SEEDS {
        let mut fixture = Differential::open_maintained(*seed, maintained_config(1));
        fixture.run_stream(&op_stream::generate(*seed, PAGED_STREAM_LEN));
        fixture.assert_merged_runs();
    }
}

// the same at four tails, where a key can seal in one tail and be rewritten in another
#[test]
#[cfg(not(miri))]
fn maintained_merge_multi_tail() {
    for seed in SEEDS {
        let mut fixture = Differential::open_maintained(*seed, maintained_config(4));
        fixture.run_stream(&op_stream::generate(*seed, PAGED_STREAM_LEN));
        fixture.assert_merged_runs();
    }
}

// and on a carrying column, where a merged row is the only copy of its value
#[test]
#[cfg(not(miri))]
fn merged_carrying_single_tail() {
    for seed in SEEDS {
        let mut fixture = Differential::open_carrying(*seed, merging_config(1));
        fixture.run_stream(&op_stream::generate(*seed, PAGED_STREAM_LEN));
        fixture.assert_rows_listed();
        fixture.assert_merged_runs();
    }
}

// and on a hot volume, where a carried row is reached only once its keys age out
#[test]
#[cfg(not(miri))]
fn carrying_hot_single_tail() {
    for seed in SEEDS {
        let mut fixture = Differential::open_carrying(*seed, hot_config(1));
        fixture.run_stream(&op_stream::generate(*seed, PAGED_STREAM_LEN));
        fixture.assert_paged_out();
    }
}

// a hot volume whose keys have aged out serves what a resident one serves
#[test]
#[cfg(not(miri))]
fn hot_single_tail() {
    for seed in SEEDS {
        let mut fixture = Differential::open(*seed, hot_config(1));
        fixture.run_stream(&op_stream::generate(*seed, PAGED_STREAM_LEN));
        fixture.assert_paged_out();
    }
}

// an open-addressed index serves what the oracle serves
#[test]
#[cfg(not(miri))]
fn open_single_tail() {
    for seed in SEEDS {
        let mut fixture = Differential::open_shaped(*seed, reel_config(1));
        fixture.run_stream(&op_stream::generate(*seed, STREAM_LEN));
    }
}

// the same at four tails, where the version guard runs on every insert
#[test]
#[cfg(not(miri))]
fn open_multi_tail() {
    for seed in SEEDS {
        let mut fixture = Differential::open_shaped(*seed, reel_config(4));
        fixture.run_stream(&op_stream::generate(*seed, STREAM_LEN));
    }
}

// a reopen rebuilds the open shards without resurrecting anything
#[test]
#[cfg(not(miri))]
fn open_never_reopen() {
    for seed in SEEDS {
        let mut fixture = Differential::open_shaped(*seed, never_config(1));
        fixture.run_stream(&op_stream::generate(*seed, STREAM_LEN));
    }
}

// and on a paged volume, where the open shard holds only what no footer covers
#[test]
#[cfg(not(miri))]
fn open_paged_single_tail() {
    for seed in SEEDS {
        let mut fixture = Differential::open_shaped(*seed, paged_config(1));
        fixture.run_stream(&op_stream::generate(*seed, PAGED_STREAM_LEN));
        fixture.assert_paged_out();
    }
}

/// The index written down at a cue, and read back at the reopens the stream takes
fn checkpointing_config(active_tails: u32) -> ReelConfig {
    ReelConfig {
        index_checkpoint: true,
        ..reel_config(active_tails)
    }
}

// a volume reopening from its written-down index serves what the oracle serves
#[test]
#[cfg(not(miri))]
fn checkpointed_single_tail() {
    for seed in SEEDS {
        let mut fixture = Differential::open(*seed, checkpointing_config(1));
        fixture.run_stream(&op_stream::generate(*seed, STREAM_LEN));
        assert!(
            fixture.checkpointed_keys() > 0,
            "seed {seed} wrote no key into any index checkpoint",
        );
    }
}

// the same at four tails, where a key can seal in one tail and be rewritten in another
#[test]
#[cfg(not(miri))]
fn checkpointed_multi_tail() {
    for seed in SEEDS {
        let mut fixture = Differential::open(*seed, checkpointing_config(4));
        fixture.run_stream(&op_stream::generate(*seed, STREAM_LEN));
        assert!(
            fixture.checkpointed_keys() > 0,
            "seed {seed} wrote no key into any index checkpoint",
        );
    }
}

// and under never sync, where a reopen also has to reproduce a rolled segment
#[test]
#[cfg(not(miri))]
fn checkpointed_never_reopen() {
    for seed in SEEDS {
        let mut fixture = Differential::open(
            *seed,
            ReelConfig {
                index_checkpoint: true,
                ..never_config(1)
            },
        );
        fixture.run_stream(&op_stream::generate(*seed, STREAM_LEN));
        assert!(
            fixture.checkpointed_keys() > 0,
            "seed {seed} wrote no key into any index checkpoint",
        );
    }
}

// an open-addressed column writes its index down and reads it back unchanged
#[test]
#[cfg(not(miri))]
fn checkpointed_open_shards() {
    for seed in SEEDS {
        let mut fixture = Differential::open_shaped(*seed, checkpointing_config(1));
        fixture.run_stream(&op_stream::generate(*seed, STREAM_LEN));
        assert!(
            fixture.checkpointed_keys() > 0,
            "seed {seed} wrote no key into any index checkpoint",
        );
    }
}

// the same seed always produces the same stream
#[test]
fn stream_reproduces() {
    let left = op_stream::generate(42, STREAM_LEN);
    let right = op_stream::generate(42, STREAM_LEN);

    assert_eq!(left, right);
}

// a very long stream stays in agreement, REEL_SOAK_OPS setting its length
#[test]
#[ignore]
#[cfg(not(miri))]
fn soak() {
    let seed = 1;
    let ops = op_stream::generate_durable(seed, soak_op_count());

    let mut fixture = Differential::open_memory_only(seed, soak_config());
    fixture.run_stream_sampled(&ops, SOAK_CHECK_EVERY);
}
