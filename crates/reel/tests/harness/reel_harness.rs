//! Reel only crash driver over the deterministic simulator
//!
//! Replays a durable op stream over a simulated volume until the first crash boundary
//! or error, then hands the simulator back so a test can reopen from its durable
//! image. A clean pass with per op sampling counts the boundaries a stream crosses, so
//! a test can crash before each one in turn.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use reel::io::fault::FaultPlan;
use reel::io::sim_backend::{DurableImage, SimIo};
use reel::{ColumnSet, ReelConfig, ReelStore, SEGMENT_SUFFIX};

use crate::harness::observe::{observe, Totals};
use crate::harness::op_stream::StreamOp;
use crate::harness::wire::{apply_mutation, TEST_COLUMNS};

/// Virtual bulk root the reel simulator files live under
const REEL_ROOT: &str = "/bulk";

pub struct ReelHarness {
    config: ReelConfig,
    columns: ColumnSet,
}

impl ReelHarness {
    /// A crash driver for one reel configuration
    pub fn new(config: ReelConfig) -> ReelHarness {
        ReelHarness::with_columns(config, TEST_COLUMNS)
    }

    /// The same driver over a named column set
    pub fn with_columns(config: ReelConfig, columns: ColumnSet) -> ReelHarness {
        ReelHarness { config, columns }
    }

    /// Count the io boundaries a clean run of the stream crosses
    pub fn boundary_count(&self, ops: &[StreamOp]) -> u64 {
        let sim = SimIo::new(FaultPlan::new(0));
        let store = self.open_or_panic(sim.clone());
        replay(&store, &sim, ops);
        store.flush().ok();
        sim.ops()
    }

    /// Replay a stream under a plan, returning the simulator and how many ops acknowledged
    ///
    /// Under a synced policy every acknowledged op is durable, so the count names the
    /// durable prefix a reopen must reproduce.
    pub fn run(&self, plan: FaultPlan, ops: &[StreamOp]) -> (SimIo, usize) {
        let sim = SimIo::new(plan);
        let mut acknowledged = 0;
        if let Ok(store) = ReelStore::open_with_io(
            root(),
            self.config.clone(),
            self.columns,
            Arc::new(sim.clone()),
        ) {
            acknowledged = replay(&store, &sim, ops);
            if !sim.is_crashed() {
                store.flush().ok();
            }
        }
        (sim, acknowledged)
    }

    /// Count the boundaries a clean run crossed with an index checkpoint partway
    pub fn boundary_count_across_index_checkpoint(&self, ops: &[StreamOp], after: usize) -> u64 {
        let sim = SimIo::new(FaultPlan::new(0));
        let store = self.open_or_panic(sim.clone());
        replay_across(&store, &sim, ops, after);
        store.flush().ok();
        sim.ops()
    }

    /// Replay a stream under a plan, writing the index down partway through it
    ///
    /// A crash either side of the publishing rename has to reopen to the same durable
    /// prefix a volume that never wrote a file reopens to.
    pub fn run_across_index_checkpoint(
        &self,
        plan: FaultPlan,
        ops: &[StreamOp],
        after: usize,
    ) -> (SimIo, usize) {
        let sim = SimIo::new(plan);
        let mut acknowledged = 0;
        if let Ok(store) = ReelStore::open_with_io(
            root(),
            self.config.clone(),
            self.columns,
            Arc::new(sim.clone()),
        ) {
            acknowledged = replay_across(&store, &sim, ops, after);
            if !sim.is_crashed() {
                store.flush().ok();
            }
        }
        (sim, acknowledged)
    }

    /// Reopen read write from a durable image, rebuilding the index
    pub fn reopen(&self, image: DurableImage) -> ReelStore {
        let restored = SimIo::from_image(image);
        self.open_or_panic(restored)
    }

    /// Open a store over a supplied simulator, expecting the open to succeed
    pub fn open_or_panic(&self, sim: SimIo) -> ReelStore {
        ReelStore::open_with_io(root(), self.config.clone(), self.columns, Arc::new(sim))
            .expect("open reel over sim")
    }
}

fn root() -> PathBuf {
    PathBuf::from(REEL_ROOT)
}

/// The same replay with the index written down once the prefix has landed
fn replay_across(store: &ReelStore, sim: &SimIo, ops: &[StreamOp], after: usize) -> usize {
    let mut acknowledged = replay(store, sim, &ops[..after.min(ops.len())]);
    if sim.is_crashed() || acknowledged < after {
        return acknowledged;
    }
    // A crash inside the checkpoint is one of the boundaries under test, so the
    // refusal it comes back as is the replay stopping rather than the test failing.
    let _ = store.checkpoint_index();
    if sim.is_crashed() {
        return acknowledged;
    }
    acknowledged += replay(store, sim, &ops[after.min(ops.len())..]);
    acknowledged
}

fn replay(store: &ReelStore, sim: &SimIo, ops: &[StreamOp]) -> usize {
    let mut acknowledged = 0;
    for op in ops {
        let outcome = apply_mutation(store, op);
        if sim.is_crashed() || outcome.is_err() {
            break;
        }
        acknowledged += 1;
    }
    acknowledged
}

/// The reel counters as a comparable snapshot
pub fn counter_totals(store: &ReelStore) -> Totals {
    Totals {
        count: store.totals().count,
        bytes: store.totals().bytes.to_bytes(),
    }
}

/// A scan of everything the reel serves, the from scratch recount
pub fn scan_totals(store: &ReelStore) -> Totals {
    observe(store).global
}

/// Assert the constant time counters equal a scan of what the reel serves
///
/// The scan runs first and the counters are read after it: a scan is not a passive
/// observer, since a record whose bytes fail their checksum is evicted by the read
/// that found it, which moves the counters.
pub fn assert_recount(store: &ReelStore, context: u64) {
    let scanned = scan_totals(store);
    let counted = counter_totals(store);
    assert_eq!(
        counted, scanned,
        "the reel counters disagree with a scan at {context}"
    );
}

/// Flip a byte in the middle of the largest segment, corrupting a record payload
pub fn flip_largest_segment(image: &mut DurableImage) -> bool {
    let mut chosen: Option<usize> = None;
    let mut largest = 0usize;
    for (index, (path, bytes)) in image.iter().enumerate() {
        if is_segment(path) && bytes.len() > largest {
            largest = bytes.len();
            chosen = Some(index);
        }
    }
    match chosen {
        Some(index) => {
            let bytes = &mut image[index].1;
            let middle = bytes.len() / 2;
            bytes[middle] ^= 0xff;
            true
        }
        None => false,
    }
}

fn is_segment(path: &Path) -> bool {
    file_name(path).ends_with(SEGMENT_SUFFIX)
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}
