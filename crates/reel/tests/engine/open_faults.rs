//! Open-path fault injection for the reel store

use std::path::PathBuf;
use std::sync::Arc;

use reel::io::fault::{FaultKind, FaultPlan};
use reel::io::sim_backend::{DurableImage, SimIo};
use reel::{ByteCount, ReelConfig, ReelStore, SyncPolicy, ThreadBudget};

use crate::harness::observe::observe;
use crate::harness::op_stream::StreamOp;
use crate::harness::wire::{
    apply_mutation, framed_value, record_key, wire_key, ID_LEN, TEST_COLUMNS,
};

/// The simulator's files live under this virtual bulk root
const ROOT: &str = "/bulk";

/// The fixtures write into this group
const GROUP: u16 = 7;

/// The fixture writes this many records before the faulted reopen
const FIXTURE_KEYS: u8 = 4;

/// Each fixture record's payload length
const FIXTURE_LEN: usize = 300;

/// The first root listing, which finds the key runs, executes at this op position
const ROOT_LIST_AT: u64 = 0;

/// The first segment read executes at this op position, after the listings
const FIRST_READ_AT: u64 = 4;

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::mb(1),
        sync: SyncPolicy::Bytes(ByteCount::from_bytes(0)),
        active_tails: ThreadBudget::threads(1),
        ..ReelConfig::default()
    }
}

fn record_id(byte: u8) -> [u8; ID_LEN] {
    [byte; ID_LEN]
}

fn root() -> PathBuf {
    PathBuf::from(ROOT)
}

/// A durable image of a volume holding a few records
fn populated() -> DurableImage {
    let sim = SimIo::new(FaultPlan::new(1));
    let store = ReelStore::open_with_io(root(), config(), TEST_COLUMNS, Arc::new(sim.clone()))
        .expect("open");
    for byte in 1..=FIXTURE_KEYS {
        apply_mutation(&store, &put(byte)).expect("fixture put");
    }
    store.flush().expect("flush");
    drop(store);
    sim.durable_image()
}

fn put(address: u8) -> StreamOp {
    StreamOp::Put {
        group: GROUP,
        address,
        len: FIXTURE_LEN,
        fill: address,
    }
}

/// Open over an image under a plan, so the open itself meets the fault
fn open_under(image: DurableImage, plan: FaultPlan) -> reel::Result<ReelStore> {
    let sim = SimIo::from_image_with_plan(image, plan);
    ReelStore::open_with_io(root(), config(), TEST_COLUMNS, Arc::new(sim))
}

fn served_keys(store: &ReelStore) -> Vec<Vec<u8>> {
    observe(store)
        .records
        .into_iter()
        .map(|(key, _)| key)
        .collect()
}

// a volume with nothing in it yet opens empty
#[test]
fn accepts_a_volume_that_does_not_exist_yet() {
    let opened = open_under(DurableImage::new(), FaultPlan::new(1));

    let store = opened.expect("a fresh volume opens");
    assert_eq!(store.totals().count, 0);
    assert_eq!(store.dead_bytes().to_bytes(), 0);
}

// a read-only open beside a live writer serves only whole records
#[test]
fn a_read_only_open_beside_a_live_writer_serves_whole_records() {
    let sim = SimIo::new(FaultPlan::new(1));
    let writer = ReelStore::open_with_io(root(), config(), TEST_COLUMNS, Arc::new(sim.clone()))
        .expect("writer opens");

    for byte in 1..=FIXTURE_KEYS {
        apply_mutation(&writer, &put(byte)).expect("write");

        // Mid stream, with the tail unsealed and the writer still holding it
        let onlooker = ReelStore::open_read_only_with_io(
            root(),
            config(),
            TEST_COLUMNS,
            Arc::new(SimIo::from_image(sim.durable_image())),
        )
        .expect("a read only open beside a writer");

        let served = observe(&onlooker).records;
        for (key, value) in &served {
            let id = key[key.len() - 1];
            assert_eq!(
                value,
                &framed_value(FIXTURE_LEN, id),
                "the onlooker served a partial record for key {id}"
            );
        }
        assert_eq!(
            onlooker.totals().count,
            served.len() as u64,
            "the onlooker counts more than it can read back"
        );
        assert!(
            onlooker
                .put(&record_key(GROUP, record_id(1)), &[0u8; 8])
                .is_err(),
            "it stayed read only"
        );
    }
}

// a refused open leaves the volume exactly as it was for the next one
#[test]
fn a_refused_open_leaves_the_volume_intact() {
    let image = populated();

    let plan = FaultPlan::new(1).with_fault(ROOT_LIST_AT, FaultKind::ListError);
    assert!(
        open_under(image.clone(), plan).is_err(),
        "refused on the listing"
    );
    let plan = FaultPlan::new(1).with_fault(FIRST_READ_AT, FaultKind::ReadError);
    assert!(
        open_under(image.clone(), plan).is_err(),
        "refused on the read"
    );

    let store = open_under(image, FaultPlan::new(1)).expect("a clean open still works");

    let expected: Vec<Vec<u8>> = (1..=FIXTURE_KEYS)
        .map(|byte| wire_key(GROUP, byte))
        .collect();
    assert_eq!(served_keys(&store), expected, "the refused opens cost data");
    assert_eq!(store.totals().count, u64::from(FIXTURE_KEYS));
}
