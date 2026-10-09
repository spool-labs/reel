//! Differential fixture over the harness columns
//! Applies each stream op to a memory store and a simulated reel and checks they agree

use std::path::PathBuf;
use std::sync::Arc;

use reel::io::fault::FaultPlan;
use reel::io::sim_backend::SimIo;
use reel::{ReelConfig, ReelStore};
use reel_core::{Direction, Store};
use reel_mock::MemoryStore;

use crate::harness::observe::observe;
use crate::harness::op_stream::StreamOp;
use crate::harness::wire::{apply_mutation, group_prefix, wire_key, RECORDS_CF, TEST_COLUMNS};

/// A guard drives the reel this many rounds to reach the state the stream should reach
const LIVENESS_ROUNDS: u32 = 64;

/// The reel simulator's files live under this virtual root
const REEL_ROOT: &str = "/bulk";

/// Each compaction stop runs this many passes
const COMPACT_PASSES: u32 = 8;

/// Steps between compaction passes inside a default stream
const COMPACT_EVERY: usize = 10;

/// Steps between merge passes inside a default stream, coprime with `COMPACT_EVERY`
const MERGE_EVERY: usize = 3;

pub struct Differential {
    /// The last step and op, reported with a divergence
    at_step: Option<(usize, String)>,

    /// Runs merged by every store before the current one
    merged_before: u64,

    /// Whether the stream runs compaction passes, off for a merging run
    is_compacting: bool,

    /// The seed this run was drawn from, so a failure points at the stream to replay
    seed: u64,

    /// The oracle for every observation
    memory: MemoryStore,

    /// The store under test
    reel: ReelStore,

    /// The reel's simulated device
    reel_sim: SimIo,

    /// Every reopen rebuilds the reel with this configuration
    reel_config: ReelConfig,

    /// Every incarnation of the device replays this plan, reopens included
    reel_plan: FaultPlan,

    /// How many keys the reel has handed to a footer across the whole run
    paged_out: usize,
}

impl Differential {
    /// Open the memory oracle and the reel for a seed and a reel configuration
    pub fn open(seed: u64, reel_config: ReelConfig) -> Differential {
        let sim = SimIo::new(FaultPlan::new(seed));
        let reel = ReelStore::open_with_io(
            PathBuf::from(REEL_ROOT),
            reel_config.clone(),
            TEST_COLUMNS,
            Arc::new(sim.clone()),
        )
        .expect("open reel");

        Differential {
            at_step: None,
            merged_before: 0,
            is_compacting: true,
            seed,
            memory: MemoryStore::new(),
            reel,
            reel_sim: sim,
            reel_config,
            reel_plan: FaultPlan::new(seed),
            paged_out: 0,
        }
    }

    /// The same pair, for a long soak
    pub fn open_memory_only(seed: u64, reel_config: ReelConfig) -> Differential {
        Differential::open(seed, reel_config)
    }

    /// The same pair with compaction off, so sealed segments stay for key merges
    pub fn open_merging(seed: u64, reel_config: ReelConfig) -> Differential {
        Differential {
            is_compacting: false,
            ..Differential::open(seed, reel_config)
        }
    }

    /// Opens both stores with a plan the reel's device replays, which must never fail an op
    pub fn open_with_plan(seed: u64, reel_config: ReelConfig, plan: FaultPlan) -> Differential {
        let mut fixture = Differential::open(seed, reel_config);
        fixture.reopen_device(plan);
        fixture
    }

    /// Replace the device with one replaying a plan, before anything is written
    fn reopen_device(&mut self, plan: FaultPlan) {
        let plan_for_reopen = plan.clone();
        let sim = SimIo::new(plan);
        self.reel = ReelStore::open_with_io(
            PathBuf::from(REEL_ROOT),
            self.reel_config.clone(),
            TEST_COLUMNS,
            Arc::new(sim.clone()),
        )
        .expect("open reel");
        self.reel_sim = sim;
        self.reel_plan = plan_for_reopen;
    }

    /// How many keys the reel has handed to a footer over the whole run
    pub fn paged_out(&self) -> usize {
        self.paged_out
    }

    /// Runs read by key merges over the whole stream, summed across its reopens
    pub fn merged_runs(&self) -> u64 {
        self.merged_before + self.reel.compaction_counters().runs_merged
    }

    /// How many faults the reel's device reached, and how many its plan scheduled
    pub fn fault_reach(&self) -> (u64, usize) {
        self.reel_sim.fault_reach()
    }

    /// Asserts the stream handed keys over to a footer, driving the handover until it does
    pub fn assert_paged_out(&mut self) {
        let seed = self.seed;
        self.drive_until(
            |fixture| {
                fixture.hand_over_reel();
                fixture.paged_out > 0
            },
            &format!("seed {seed} paged nothing out"),
        );
    }

    /// The stream stacked its walk past the merge depth and a key merge folded it
    pub fn assert_merged_runs(&mut self) {
        let seed = self.seed;
        self.drive_until(
            |fixture| {
                fixture.hand_over_reel();
                fixture.compact_reel();
                fixture.merge_reel();
                fixture.merged_runs() > 0
            },
            &format!("seed {seed} never stacked its walk deep enough to merge"),
        );
    }

    /// Flushes, retries broken seals and pages out every segment the stream rolled
    fn hand_over_reel(&mut self) {
        self.reel.flush().expect("flush reel");
        self.reel.retry_broken_seals();
        self.page_out_reel();
    }

    /// Counts the segment files on the device
    fn segments_standing(&self) -> usize {
        self.reel_sim
            .durable_image()
            .iter()
            .filter(|(path, _)| path.to_string_lossy().ends_with(".reel"))
            .count()
    }

    /// Runs the passes behind a condition until it holds, or fails after `LIVENESS_ROUNDS`
    fn drive_until(&mut self, mut reached: impl FnMut(&mut Differential) -> bool, what: &str) {
        for _ in 0..LIVENESS_ROUNDS {
            if reached(self) {
                return;
            }
        }
        panic!(
            "{what}, in {LIVENESS_ROUNDS} rounds of driving it, \
             {} segments standing, {} keys paged, faults {:?}",
            self.segments_standing(),
            self.paged_out,
            self.fault_reach(),
        );
    }

    /// Apply a whole stream, checking agreement before the first op and after each
    pub fn run_stream(&mut self, ops: &[StreamOp]) {
        self.assert_agrees();
        for (step, op) in ops.iter().enumerate() {
            self.apply(op);
            self.hand_over_reel();
            if step % COMPACT_EVERY == COMPACT_EVERY - 1 {
                self.compact_reel();
            }
            if step % MERGE_EVERY == MERGE_EVERY - 1 {
                self.merge_reel();
            }
            // Record the step so a divergence reports which op produced it
            self.at_step = Some((step, format!("{op:?}")));
            self.assert_agrees();
        }
    }

    /// Apply a whole stream, checking agreement only every so many steps and at the end
    pub fn run_stream_sampled(&mut self, ops: &[StreamOp], every: usize) {
        self.assert_agrees();
        for (step, op) in ops.iter().enumerate() {
            self.apply(op);
            self.hand_over_reel();
            if step % every == 0 {
                self.assert_agrees();
                self.compact_reel();
            }
        }
        self.assert_agrees();
    }

    fn apply(&mut self, op: &StreamOp) {
        match op {
            StreamOp::Reopen => self.reopen(),
            StreamOp::IterFrom { .. }
            | StreamOp::IterRange { .. }
            | StreamOp::IterKeysPrefix { .. } => self.query(op),
            StreamOp::Put { .. }
            | StreamOp::Overwrite { .. }
            | StreamOp::Delete { .. }
            | StreamOp::DropGroup { .. }
            | StreamOp::DeleteRange { .. } => self.mutate(op),
        }
    }

    fn query(&self, op: &StreamOp) {
        match op {
            StreamOp::IterFrom {
                group,
                address,
                descending,
            } => {
                let start = wire_key(*group, *address);
                let memory = read_from(&self.memory, &start, *descending);
                assert_eq!(
                    memory,
                    read_from(&self.reel, &start, *descending),
                    "reel iter_from diverged from memory"
                );
            }
            StreamOp::IterRange { group, lo, hi } => {
                let start = wire_key(*group, *lo);
                let end = wire_key(*group, *hi);
                let memory = read_range(&self.memory, &start, &end);
                assert_eq!(
                    memory,
                    read_range(&self.reel, &start, &end),
                    "reel iter_range diverged from memory"
                );
            }
            StreamOp::IterKeysPrefix { group } => {
                let prefix = group_prefix(*group);
                let memory = read_keys(&self.memory, &prefix);
                // Count reads only, since the sealer may write in this window on its own clock
                let before = self.reel_sim.read_count();
                let reel = read_keys(&self.reel, &prefix);
                assert_eq!(
                    self.reel_sim.read_count(),
                    before,
                    "reel keys-only playback read a payload"
                );
                assert_eq!(memory, reel, "reel keys diverged from memory");
            }
            StreamOp::Put { .. }
            | StreamOp::Overwrite { .. }
            | StreamOp::Delete { .. }
            | StreamOp::DropGroup { .. }
            | StreamOp::DeleteRange { .. }
            | StreamOp::Reopen => unreachable!("query handles only the read ops"),
        }
    }

    fn mutate(&mut self, op: &StreamOp) {
        apply_mutation(&self.memory, op).expect("memory mutation");
        apply_mutation(&self.reel, op).expect("reel mutation");
    }

    fn reopen(&mut self) {
        self.merged_before += self.reel.compaction_counters().runs_merged;
        self.reel.flush().expect("flush reel");
        let image = self.reel_sim.durable_image();
        let restored = SimIo::from_image_with_plan(image, self.reel_plan.clone());
        self.reel = ReelStore::open_with_io(
            PathBuf::from(REEL_ROOT),
            self.reel_config.clone(),
            TEST_COLUMNS,
            Arc::new(restored.clone()),
        )
        .expect("reopen reel");
        self.reel_sim = restored;
    }

    /// Runs `COMPACT_PASSES` compaction passes, on a run that compacts
    fn compact_reel(&self) {
        if !self.is_compacting {
            return;
        }
        for _ in 0..COMPACT_PASSES {
            self.reel.compact_once().expect("compact reel");
        }
    }

    /// Fold the walk's runs into a key run, where the stream has stacked them deep enough
    fn merge_reel(&self) {
        self.reel.merge_when_due().expect("merge reel");
    }

    /// Hand the keys of any newly sealed segment over to their footers
    fn page_out_reel(&mut self) {
        self.paged_out += self.reel.page_out_sealed().expect("page out sealed");
        // Sweep as the maintenance tick would, since a drop's counters converge at the sweep
        while self.reel.sweep_covers().expect("sweep covers") {}
    }

    fn assert_agrees(&self) {
        let memory = observe(&self.memory);
        let reel = observe(&self.reel);
        if memory != reel {
            let missing: Vec<String> = memory
                .records
                .iter()
                .filter(|(key, _)| !reel.records.iter().any(|(theirs, _)| theirs == key))
                .map(|(key, value)| describe(key, value))
                .collect();
            let extra: Vec<String> = reel
                .records
                .iter()
                .filter(|(key, _)| !memory.records.iter().any(|(theirs, _)| theirs == key))
                .map(|(key, value)| describe(key, value))
                .collect();
            panic!(
                "reel diverged from memory at {:?}: memory has {} keys, reel {}, \
                 keys memory has and the reel does not: {missing:?}, the other way: {extra:?}",
                self.at_step,
                memory.records.len(),
                reel.records.len(),
            );
        }

        assert_eq!(memory, reel, "reel diverged from memory");
        let live: Vec<String> = reel
            .records
            .iter()
            .map(|(key, value)| describe(key, value))
            .collect();
        assert_eq!(
            self.reel.totals().count,
            reel.global.count,
            "reel count counter disagreed with a scan, seed {} at {:?}, live keys {live:?}",
            self.seed,
            self.at_step,
        );
        // An overwrite booked by length class sits up to half a class off
        let (counted, slack) = (self.reel.totals().bytes.to_bytes(), self.reel.spot_slack());
        assert!(
            counted.abs_diff(reel.global.bytes) <= slack,
            "reel byte counter disagreed with a scan: {counted} against {}, slack {slack}",
            reel.global.bytes
        );
    }
}

/// One key in hex and the size of its value, for failure messages
fn describe(key: &[u8], value: &[u8]) -> String {
    let hex: String = key.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("{hex}={}B", value.len())
}

/// Seek from a key in one direction and collect the ordered record pairs
fn read_from<Backend: Store>(
    store: &Backend,
    start: &[u8],
    descending: bool,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let direction = if descending {
        Direction::Desc
    } else {
        Direction::Asc
    };
    store
        .iter_from(RECORDS_CF, start, direction)
        .expect("iter_from")
        .map(|(key, value)| (key, value.into_vec()))
        .collect()
}

/// Collect the ordered record pairs inside a half open key range
fn read_range<Backend: Store>(
    store: &Backend,
    start: &[u8],
    end: &[u8],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    store
        .iter_range(RECORDS_CF, start, end)
        .expect("iter_range")
        .map(|(key, value)| (key, value.into_vec()))
        .collect()
}

/// Collect the record keys under a prefix without reading any payload
fn read_keys<Backend: Store>(store: &Backend, prefix: &[u8]) -> Vec<Vec<u8>> {
    store
        .iter_keys_prefix(RECORDS_CF, prefix)
        .expect("iter_keys_prefix")
}
