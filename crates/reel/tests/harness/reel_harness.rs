//! Reel only crash driver over the deterministic simulator

use std::path::{Path, PathBuf};
use std::sync::Arc;

use reel::format::journal::rows_region;
use reel::io::fault::FaultPlan;
use reel::io::sim_backend::{DurableImage, SimIo};
use reel::{ColumnSet, ReelConfig, ReelStore, SEGMENT_SUFFIX};

use crate::harness::observe::{observe, Totals};
use crate::harness::op_stream::StreamOp;
use crate::harness::wire::{apply_mutation, TEST_COLUMNS};

/// The reel simulator's files live under this virtual bulk root
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

    /// The same driver over a given column set
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

    /// Replay a stream under a plan, returning the simulator and how many ops were acknowledged
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
pub fn assert_recount(store: &ReelStore, context: u64) {
    let scanned = scan_totals(store);
    let counted = counter_totals(store);
    // An overwrite booked by length class can be up to half a class off in bytes
    let slack = store.spot_slack();
    assert!(
        counted.count == scanned.count && counted.bytes.abs_diff(scanned.bytes) <= slack,
        "the reel counters disagree with a scan at {context}: {counted:?} against {scanned:?}, slack {slack}"
    );
}

/// Flip a byte in the middle of the largest segment, corrupting a record payload
pub fn flip_largest_segment(image: &mut DurableImage) -> bool {
    let mut chosen: Option<usize> = None;
    let mut largest = 0usize;
    for (index, (path, bytes)) in image.iter().enumerate() {
        if is_segment(path) && content_len(bytes) > largest {
            largest = content_len(bytes);
            chosen = Some(index);
        }
    }
    match chosen {
        Some(index) => {
            let bytes = &mut image[index].1;
            let middle = content_len(bytes) / 2;
            bytes[middle] ^= 0xff;
            true
        }
        None => false,
    }
}

/// Record bytes: up to the last nonzero byte before an open segment's rows, else the whole file
fn content_len(bytes: &[u8]) -> usize {
    match rows_region(bytes) {
        Some((rows_at, _)) => bytes[..rows_at as usize]
            .iter()
            .rposition(|byte| *byte != 0)
            .map_or(0, |last| last + 1),
        None => bytes.len(),
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
