//! Differential fixture over the harness columns
//!
//! Opens a memory store and a reel store over a deterministic simulator, applies one
//! stream operation to each at once, and checks that everything they serve still agrees.
//! A reopen flushes and rebuilds the reel from what it made durable while the memory
//! store keeps its live state, so an equal observation after one means recovery
//! reproduced the live state exactly.

use std::path::PathBuf;
use std::sync::Arc;

use reel::io::fault::FaultPlan;
use reel::io::sim_backend::SimIo;
use reel::{ColumnSet, ColumnSpec, MapShape, ReelConfig, ReelStore, ShardShapes};
use reel_core::{Direction, Store};
use reel_mock::MemoryStore;

use crate::harness::observe::observe;
use crate::harness::op_stream::StreamOp;
use crate::harness::wire::{
    apply_mutation, group_prefix, wire_key, BLOB, RECORDS, RECORDS_CF, TEST_COLUMNS,
};

/// The harness columns with one of them carrying its values in its sealed rows
///
/// Derived from the harness columns field by field, so a change to the declaration
/// cannot leave this holding a different key width or codec. Only the carry differs, and
/// only on the record column, whose payloads straddle the carry so both populations run.
const fn carrying(spec: &ColumnSpec, row_carry: u16) -> ColumnSpec {
    ColumnSpec {
        id: spec.id,
        name: spec.name,
        key_width: spec.key_width,
        shard_bytes: spec.shard_bytes,
        inline_max: spec.inline_max,
        row_carry,
        purge_mark: spec.purge_mark,
        codec: spec.codec,
        map_shape: spec.map_shape,
    }
}

/// Bytes the carrying variant asks its record rows to hold
pub const STREAM_CARRY: u16 = 256;

/// Rounds of driving a guard gives the reel to reach a state the stream should have
/// taken it to
///
/// Counted in passes rather than in seconds, since every round runs the work the state
/// needs rather than waiting for somebody else to run it. The passes are idempotent, so
/// a run that has not reached it by here will not reach it at all.
const LIVENESS_ROUNDS: u32 = 64;

/// The carrying variant of the harness column set
pub const CARRYING_COLUMNS: ColumnSet = &[
    carrying(&TEST_COLUMNS[0], STREAM_CARRY),
    carrying(&TEST_COLUMNS[1], 0),
];

/// The harness columns asking for open-addressed shards
///
/// Both widths are ones the index holds an open arm for, which is why the whole set can
/// flip rather than half of it.
const fn opened(spec: &ColumnSpec) -> ColumnSpec {
    ColumnSpec {
        id: spec.id,
        name: spec.name,
        key_width: spec.key_width,
        shard_bytes: spec.shard_bytes,
        inline_max: spec.inline_max,
        row_carry: spec.row_carry,
        purge_mark: spec.purge_mark,
        codec: spec.codec,
        map_shape: MapShape::Open,
    }
}

/// The open-addressed variant of the harness column set
pub const OPEN_COLUMNS: ColumnSet = &[opened(&TEST_COLUMNS[0]), opened(&TEST_COLUMNS[1])];

/// Virtual bulk root the reel simulator files live under
const REEL_ROOT: &str = "/bulk";

/// Compaction passes a sampled run takes each time it stops to bound the reel
const COMPACT_PASSES: u32 = 8;

/// Steps between compaction passes inside a default stream
///
/// Compaction has to be invisible: it rewrites live records into a new segment, repoints
/// the index at the copies and retires the source, and none of that may reach what the
/// store serves.
const COMPACT_EVERY: usize = 10;

/// Steps between merge passes inside a default stream
///
/// Coprime with the cadences above, so a merge lands before, after and between the
/// compaction passes and the checkpoints rather than always at the same point.
const MERGE_EVERY: usize = 3;

/// Steps between index checkpoints inside a default stream
///
/// Coprime with the compaction cadence: a file written just before a pass retires the
/// segments it names is the case the reopens have to survive.
const CHECKPOINT_EVERY: usize = 7;

pub struct Differential {
    /// Columns the reel under test was opened with
    columns: ColumnSet,

    /// The step and op a divergence is reported against
    at_step: Option<(usize, String)>,

    /// Rows listed by passes before the reopens that reset the reel's own counter
    listed_before: u64,

    /// Sorted runs merged before those reopens, counted the same way
    merged_before: u64,

    /// Whether the stream drives the maintenance tick rather than the passes under it
    is_maintained: bool,

    /// Whether the stream writes the reel's index down as it goes
    is_checkpointing: bool,

    /// The seed this run was drawn from, so a failure names the stream to replay
    seed: u64,

    /// The oracle every observation is compared against
    memory: MemoryStore,

    /// The store under test
    reel: ReelStore,

    /// The device that store runs over
    reel_sim: SimIo,

    /// The configuration every reopen rebuilds it with
    reel_config: ReelConfig,

    /// The plan every incarnation of the device replays, reopens included
    reel_plan: FaultPlan,

    /// Keys the reel has handed over to a footer across the whole run
    paged_out: usize,

    /// Keys the reel wrote into index checkpoints across the whole run
    checkpointed_keys: u64,
}

impl Differential {
    /// Open the memory oracle and the reel for a seed and a reel configuration
    pub fn open(seed: u64, reel_config: ReelConfig) -> Differential {
        Differential::open_with_columns(seed, reel_config, TEST_COLUMNS)
    }

    /// The same pair, under the name a long soak calls for
    pub fn open_memory_only(seed: u64, reel_config: ReelConfig) -> Differential {
        Differential::open(seed, reel_config)
    }

    /// The same pair, with the maintenance tick driving the plane the stream would
    ///
    /// The tick decides the merge on the standing stack's own debt, so an armed volume
    /// opened this way collapses its runs when its traffic has shadowed enough of them
    /// rather than on a cadence of the stream's.
    pub fn open_maintained(seed: u64, reel_config: ReelConfig) -> Differential {
        Differential {
            is_maintained: true,
            ..Differential::open(seed, reel_config)
        }
    }

    /// The same run, writing the reel's index down at a cadence of the stream's
    ///
    /// Nothing on the volume schedules this, so a run that wants the reopens to read
    /// a file back says so here.
    pub fn checkpointing(self) -> Differential {
        Differential {
            is_checkpointing: true,
            ..self
        }
    }

    /// The same, on a column set whose sealed rows carry their values
    pub fn open_carrying(seed: u64, reel_config: ReelConfig) -> Differential {
        Differential::open_with_columns(seed, reel_config, CARRYING_COLUMNS)
    }

    /// The same, on a column set whose resident shards are open addressed
    pub fn open_shaped(seed: u64, reel_config: ReelConfig) -> Differential {
        let reel_config = ReelConfig {
            shard_shapes: ShardShapes::Declared,
            ..reel_config
        };
        let fixture = Differential::open_with_columns(seed, reel_config, OPEN_COLUMNS);
        // A declaration the volume did not honour is a tree run wearing another name.
        for column in [RECORDS, BLOB] {
            let index = fixture
                .reel
                .index()
                .column(column)
                .expect("a declared column");
            assert_eq!(
                index.map_shape(),
                MapShape::Open,
                "column {column:?} opened in the tree"
            );
        }
        fixture
    }

    /// Open both stores with a plan the reel's device replays
    ///
    /// Only a plan that never fails an op belongs here, since the oracle is exact and
    /// every mutation is expected to succeed. Delays and reordering fit, changing when a
    /// caller is answered and never what it is answered with.
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
            self.columns,
            Arc::new(sim.clone()),
        )
        .expect("open reel");
        self.reel_sim = sim;
        self.reel_plan = plan_for_reopen;
    }

    /// Open on a named column set, which is the only thing a carrying run needs
    fn open_with_columns(seed: u64, reel_config: ReelConfig, columns: ColumnSet) -> Differential {
        let sim = SimIo::new(FaultPlan::new(seed));
        let reel = ReelStore::open_with_io(
            PathBuf::from(REEL_ROOT),
            reel_config.clone(),
            columns,
            Arc::new(sim.clone()),
        )
        .expect("open reel");

        Differential {
            columns,
            at_step: None,
            listed_before: 0,
            merged_before: 0,
            is_maintained: false,
            is_checkpointing: false,
            seed,
            memory: MemoryStore::new(),
            reel,
            reel_sim: sim,
            reel_config,
            reel_plan: FaultPlan::new(seed),
            paged_out: 0,
            checkpointed_keys: 0,
        }
    }

    /// Keys the reel has given up to a footer over the whole run
    pub fn paged_out(&self) -> usize {
        self.paged_out
    }

    /// Keys the reel wrote into index checkpoints over the whole run
    pub fn checkpointed_keys(&self) -> u64 {
        self.checkpointed_keys
    }

    /// Sorted runs merge passes read together over the whole run
    ///
    /// Carried across the stream's reopens, since each one is a fresh store with fresh
    /// counters and the question is what the whole run did.
    pub fn merged_runs(&self) -> u64 {
        self.merged_before + self.reel.compaction_counters().runs_merged
    }

    /// Values a rewrite put in a row and wrote no record for
    ///
    /// Carried across the stream's reopens, since each one is a fresh store with fresh
    /// counters and the question is what the whole run did.
    pub fn rows_listed(&self) -> u64 {
        self.listed_before + self.reel.compaction_counters().rows_listed
    }

    /// Faults the reel's device reached, against the count its plan scheduled
    pub fn fault_reach(&self) -> (u64, usize) {
        self.reel_sim.fault_reach()
    }

    /// The stream handed keys over to a footer
    ///
    /// Driven rather than read: the handover is a pass this fixture calls, so a run that
    /// has not paged yet is asked again rather than failed. What runs the bound out is a
    /// handover that never comes, which is the defect the guard is here for.
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

    /// The stream's rewrites listed rows and deleted the records behind them
    pub fn assert_rows_listed(&mut self) {
        let seed = self.seed;
        self.drive_until(
            |fixture| {
                fixture.hand_over_reel();
                fixture.compact_reel();
                fixture.rows_listed() > 0
            },
            &format!("seed {seed} listed no row, so it deleted no record"),
        );
    }

    /// The stream left two sorted runs standing and something collapsed them
    pub fn assert_merged_runs(&mut self) {
        let seed = self.seed;
        self.drive_until(
            |fixture| {
                fixture.hand_over_reel();
                fixture.compact_reel();
                fixture.merge_reel();
                fixture.merged_runs() > 0
            },
            &format!("seed {seed} never had two runs to merge"),
        );
    }

    /// Hand over every segment the stream rolled, with nothing left to a clock
    ///
    /// A rolled segment seals on the sealer's thread and only a sealed one can give its
    /// keys up, so the flush is what makes the handover due rather than likely: it
    /// returns once every footer the stream owed is down. The retry after it takes the
    /// segments a device failure left footerless, which is a fault landing on the
    /// sealer's write and so is a property of the interleaving rather than of the seed.
    fn hand_over_reel(&mut self) {
        self.reel.flush().expect("flush reel");
        self.reel.retry_broken_seals();
        self.page_out_reel();
    }

    /// Segments standing on the device, which says whether anything sealed at all
    fn segments_standing(&self) -> usize {
        self.reel_sim
            .durable_image()
            .iter()
            .filter(|(path, _)| path.to_string_lossy().ends_with(".reel"))
            .count()
    }

    /// Run the passes behind a condition until it holds, or fail on the bound
    ///
    /// The bound is rounds of the passes themselves, not seconds: every round flushes,
    /// which waits the sealer out rather than sleeping past it, so a saturated machine
    /// makes each round slower and never makes one fewer. What runs the bound out is a
    /// condition the passes cannot reach, which is the defect the guard is here for.
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
            if step % CHECKPOINT_EVERY == CHECKPOINT_EVERY - 1 {
                self.checkpoint_reel_index();
            }
            if step % COMPACT_EVERY == COMPACT_EVERY - 1 {
                self.compact_reel();
            }
            if step % MERGE_EVERY == MERGE_EVERY - 1 {
                self.merge_reel();
            }
            // Named, so a divergence reports which op produced it.
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
                // Reads specifically, not the op total: the sealer lands deferred seals
                // on its own clock, so a write of its can fall inside this window, and the
                // guard is owed only that the playback fetched no payload.
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
        self.listed_before += self.reel.compaction_counters().rows_listed;
        self.merged_before += self.reel.compaction_counters().runs_merged;
        self.reel.flush().expect("flush reel");
        let image = self.reel_sim.durable_image();
        let restored = SimIo::from_image_with_plan(image, self.reel_plan.clone());
        self.reel = ReelStore::open_with_io(
            PathBuf::from(REEL_ROOT),
            self.reel_config.clone(),
            self.columns,
            Arc::new(restored.clone()),
        )
        .expect("reopen reel");
        self.reel_sim = restored;
    }

    /// Write the reel's index down, on a run that asked for it
    ///
    /// The keys are counted across the run so a checkpointing test can tell a run that
    /// exercised the path from one whose every file stood for nothing.
    fn checkpoint_reel_index(&mut self) {
        // A paging volume leaves its sealed keys in the footers and has no resident
        // index to write down, which the store refuses on.
        if !self.is_checkpointing || self.reel_config.index.pages() {
            return;
        }
        let taken = self.reel.checkpoint_index().expect("index checkpoint");
        self.checkpointed_keys += taken.keys;
    }

    /// Bound the reel, either pass by pass or by handing the whole plane to the tick
    fn compact_reel(&self) {
        if self.is_maintained {
            for _ in 0..COMPACT_PASSES {
                self.reel.maintain_once().expect("maintain reel");
            }
            return;
        }
        for _ in 0..COMPACT_PASSES {
            self.reel.compact_once().expect("compact reel");
        }
    }

    /// Collapse whatever sorted runs the stream has left standing
    ///
    /// A volume that did not arm the merge refuses it outright, so this costs the other
    /// streams a load. A maintained run decides its own merges and takes none here.
    fn merge_reel(&self) {
        if self.is_maintained || !self.reel_config.merge_sorted_runs {
            return;
        }
        self.reel.merge_once().expect("merge reel");
    }

    /// Hand the keys of any newly sealed segment over to their footers
    ///
    /// A resident volume does nothing here. On a paged volume, running it inside the
    /// stream rather than at the end is what puts every op after it through a half paged
    /// index.
    fn page_out_reel(&mut self) {
        self.paged_out += self.reel.page_out_sealed().expect("page out sealed");
        // The sweep the maintenance tick would run: agreement is asserted after every op,
        // and a drop's counters converge at the sweep rather than in the drop itself.
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
        // A paged rebuild counts nothing sealed, so while a born segment stands the
        // counters promise a floor rather than the total.
        if self.reel.born_segments() == 0 {
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
            assert_eq!(
                self.reel.totals().bytes.to_bytes(),
                reel.global.bytes,
                "reel byte counter disagreed with a scan"
            );
        } else {
            assert!(
                self.reel.totals().count <= reel.global.count,
                "reel count counter overcounted under born segments"
            );
            assert!(
                self.reel.totals().bytes.to_bytes() <= reel.global.bytes,
                "reel byte counter overcounted under born segments"
            );
        }
    }
}

/// One key and the size of what it serves, for a failure that has to name keys
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
