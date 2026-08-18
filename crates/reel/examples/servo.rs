//! What this machine says about itself, and what the bias pass makes of it
//!
//! Nothing here is measured: every fact is a file the kernel already wrote or a
//! stat an opening volume already takes. The engine reads the same facts on open
//! and keeps them, which the open at the end checks against these.
//!
//! cargo run --example servo

use std::path::Path;

use reel::reel::bias::{access_ranges, available_bytes, MachineFacts, Plane, RingAvailability};
use reel::{
    Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape, RangedReads, ReelConfig, ReelStore,
};

const RECORDS: &str = "records";

/// Eight byte keys, sharded on their leading byte, values stored raw
const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: RECORDS,
    key_width: KeyWidth::Fixed(8),
    shard_bytes: 1,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

/// A byte count as a person reads it, or the absence the platform reported
fn size(value: Option<u64>) -> String {
    match value {
        None => "unknown".to_string(),
        Some(value) if value >= 1 << 30 => format!("{:.1} GiB", value as f64 / (1u64 << 30) as f64),
        Some(value) if value >= 1 << 20 => format!("{:.1} MiB", value as f64 / (1u64 << 20) as f64),
        Some(value) => format!("{value} B"),
    }
}

fn report(root: &Path, facts: &MachineFacts) {
    println!("{:<20}{}", "root", root.display());
    println!("{:<20}{}", "memory", size(facts.memory_bytes));
    println!("{:<20}{}", "filesystem", size(facts.volume_capacity_bytes));
    println!("{:<20}{}", "free", size(available_bytes(root)));
    println!("{:<20}{}", "already held", size(facts.volume_bytes));
    println!("{:<20}{}", "logical block", size(facts.logical_block_bytes));
    println!(
        "{:<20}{}",
        "rotational",
        match facts.is_rotational {
            Some(true) => "yes",
            Some(false) => "no",
            None => "the device does not say",
        }
    );
    println!(
        "{:<20}{}",
        "open file limit",
        match facts.open_file_limit {
            Some(limit) => limit.to_string(),
            None => "unlimited".to_string(),
        }
    );
    println!(
        "{:<20}{}",
        "io_uring",
        match facts.ring {
            RingAvailability::Available => "available",
            RingAvailability::Unsupported => "no io_uring in this kernel",
            RingAvailability::Denied => "refused, by policy or sandbox",
            RingAvailability::NotLinux => "not linux",
        }
    );
    match access_ranges(root).as_slice() {
        [] => println!("actuators           one, or the drive does not say"),
        spans => {
            for (at, span) in spans.iter().enumerate() {
                println!(
                    "actuator {at:<11}sector {} for {} sectors",
                    span.sector, span.sectors
                );
            }
        }
    }
}

fn main() -> reel::Result<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    let config = ReelConfig::default();
    // What full preallocation would claim before a byte is written, which is the term
    // the reservation half of the verdict turns on.
    let reservation = config.segment_bytes.to_bytes() * config.tail_count() as u64;

    let facts = MachineFacts::read(root);
    report(root, &facts);

    let verdict = facts.verdict(reservation);
    println!();
    println!("{:<20}{}", "reservation", size(Some(reservation)));
    println!("{:<20}{:?}", "plane", verdict.plane);
    println!("{:<20}{}", "because", verdict.because);
    println!("{:<20}{}", "mapping", verdict.map_because);
    println!(
        "{:<20}{}",
        "map above",
        match verdict.map_above {
            Some(floor) => size(Some(floor.to_bytes())),
            None => "off".to_string(),
        }
    );
    println!("{:<20}{:?}", "ranged reads", verdict.ranged_reads);
    println!("{:<20}{:?}", "preallocate", verdict.preallocate);
    println!("{:<20}{}", "fd cache", verdict.fd_cache);

    // A window is read where the volume opened: a direct volume is refused mapped
    // reads at validation, and a mapping is only ever advised on the buffered plane.
    match verdict.plane {
        Plane::Direct => {
            assert_eq!(verdict.map_above, None);
            assert_eq!(verdict.ranged_reads, RangedReads::Direct);
        }
        Plane::Buffered => assert_eq!(verdict.ranged_reads, RangedReads::Cached),
    }
    assert!(
        verdict.fd_cache > 0,
        "a reader cache of nothing reopens a segment per read"
    );

    let store = ReelStore::open(root.to_path_buf(), config, COLUMNS)?;
    let kept = store
        .bias()
        .expect("a volume on a real filesystem keeps its facts");
    assert_eq!(kept.memory_bytes, facts.memory_bytes);
    assert_eq!(kept.volume_capacity_bytes, facts.volume_capacity_bytes);
    assert_eq!(kept.ring, facts.ring);
    assert_eq!(kept.verdict(reservation).plane, verdict.plane);

    println!();
    println!("the open read these facts for itself and reached the same plane");
    Ok(())
}
