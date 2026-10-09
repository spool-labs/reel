//! Crash behaviour on a simulated device: a torn append, a refused one, then a reopen
//!
//! cargo run --example simulated_crash

use std::path::PathBuf;
use std::sync::Arc;

use reel::format::column::RecordKey;
use reel::format::record::HEADER_LEN;
use reel::io::fault::{FaultKind, FaultPlan};
use reel::io::sim_backend::SimIo;
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, ReelConfig, ReelStore, SyncPolicy,
    ThreadBudget,
};
use reel_core::Value;

const RECORDS: ColumnId = ColumnId(1);

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: RECORDS,
    name: "records",
    key_width: KeyWidth::Fixed(KEY_LEN as u16),
    shard_bytes: 2,
    purge_mark: None,
    codec: Codec::None,
}];

/// The simulated files live under this virtual root, so no real directory is touched
const ROOT: &str = "/bulk";

const KEY_LEN: usize = 16;
const PAYLOAD_LEN: usize = 512;

/// The run writes this many records, and the plan targets the last two
const RECORD_COUNT: u32 = 8;
const TORN_RECORD: u32 = 6;
const REFUSED_RECORD: u32 = 7;

/// Op positions of the last two records' writes, with one tail syncing every put
const TORN_AT: u64 = 26;
const ENOSPC_AT: u64 = 29;

/// The tear keeps the header and this many payload bytes
const TORN_PAYLOAD_BYTES: u64 = 8;

/// The fault plan's seed
const SEED: u64 = 1;

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::mb(1),
        sync: SyncPolicy::Bytes(ByteCount::from_bytes(0)),
        active_tails: ThreadBudget::threads(1),
        ..ReelConfig::default()
    }
}

fn key(at: u32) -> RecordKey {
    let mut bytes = [0u8; KEY_LEN];
    bytes[..4].copy_from_slice(&at.to_be_bytes());
    RecordKey::from_bytes(RECORDS, &bytes).expect("key")
}

fn payload(at: u32) -> Vec<u8> {
    vec![at as u8 + 1; PAYLOAD_LEN]
}

fn open(io: SimIo) -> reel::Result<ReelStore> {
    ReelStore::open_with_io(PathBuf::from(ROOT), config(), COLUMNS, Arc::new(io))
}

fn main() -> reel::Result<()> {
    let torn = FaultKind::TornWrite {
        durable_bytes: HEADER_LEN as u64 + TORN_PAYLOAD_BYTES,
    };
    let plan = FaultPlan::new(SEED)
        .with_fault(TORN_AT, torn)
        .with_fault(ENOSPC_AT, FaultKind::EnospcAppend);

    let sim = SimIo::new(plan);
    let store = open(sim.clone())?;

    let mut refused: Vec<u32> = Vec::new();
    for at in 0..RECORD_COUNT {
        if let Err(error) = store.put(&key(at), &payload(at)) {
            println!("record {at} refused: {error}");
            refused.push(at);
        }
    }

    // A torn append still reports success, so the volume counts that record as live
    assert_eq!(
        refused,
        vec![REFUSED_RECORD],
        "a different append was refused"
    );
    assert_eq!(store.totals().count, u64::from(RECORD_COUNT - 1));
    println!(
        "{RECORD_COUNT} records written, {} counted live",
        store.totals().count
    );

    // Power cut: nothing is closed or flushed, so the image is what the medium holds now
    let image = sim.durable_image();
    drop(store);

    let reopened = open(SimIo::from_image(image))?;

    for at in 0..TORN_RECORD {
        assert_eq!(
            reopened.get(&key(at))?.map(Value::into_vec),
            Some(payload(at)),
            "record {at} was durable before the crash and did not come back",
        );
    }
    assert!(
        reopened.get(&key(TORN_RECORD))?.is_none(),
        "the torn record came back from a payload the device never took",
    );
    assert!(
        reopened.get(&key(REFUSED_RECORD))?.is_none(),
        "a refused append left a record behind",
    );
    assert_eq!(
        reopened.totals().count,
        u64::from(TORN_RECORD),
        "the counters disagree with what the reopened volume serves",
    );

    println!(
        "reopened from the crashed image: {} whole records",
        reopened.totals().count
    );
    println!("record {TORN_RECORD} was acknowledged and is not among them");

    Ok(())
}
