//! Follow a writer from a second, read-only open of the same directory
//!
//! One process owns a reel for writing; another opens it read-only, serves reads from
//! an index of its own, and catches that index up when it wants to. A follower behind
//! an append answers a miss rather than a stale record, until a refresh pass.
//!
//! cargo run --example follower

use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape, Preallocate, ReelConfig,
    ReelStore, Store, StoreResult, ThreadBudget,
};

/// Family both opens are given, since a reader declares the columns it reads
const ROWS: &str = "rows";

/// Eight byte keys, one index shard, values stored raw
const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: ROWS,
    key_width: KeyWidth::Fixed(8),
    shard_bytes: 0,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

/// Small enough that the run writes a file rather than a gibibyte of zeros
const SEGMENT_BYTES: ByteCount = ByteCount::mb(4);
const ALLOC_CHUNK: ByteCount = ByteCount::mb(1);

/// Records the follower finds at its open, and the records written after it
const SEEDED: u64 = 3;
const APPENDED: u64 = 2;

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: SEGMENT_BYTES,
        alloc_chunk: ALLOC_CHUNK,
        preallocate: Preallocate::Chunk,
        active_tails: ThreadBudget::threads(1),
        ..ReelConfig::default()
    }
}

fn key_of(number: u64) -> [u8; 8] {
    number.to_be_bytes()
}

fn payload_of(number: u64) -> Vec<u8> {
    format!("payload {number}").into_bytes()
}

fn main() -> StoreResult<()> {
    let root = TempDir::new()?;
    let writer = ReelStore::open(root.path().to_path_buf(), config(), COLUMNS)?;
    for number in 0..SEEDED {
        Store::put(&writer, ROWS, &key_of(number), &payload_of(number))?;
    }

    let follower = ReelStore::open_read_only(root.path().to_path_buf(), config(), COLUMNS)?;
    for number in 0..SEEDED {
        let found = Store::get(&follower, ROWS, &key_of(number))?.expect("seeded record");
        assert_eq!(&*found, payload_of(number).as_slice());
    }
    println!("follower opened read-only beside the writer and read {SEEDED} records");

    // One process owns a reel for writing, so the second open takes no lock and
    // takes no writes either.
    let refused = Store::put(&follower, ROWS, &key_of(SEEDED), &payload_of(SEEDED));
    assert!(refused.is_err());

    for number in SEEDED..SEEDED + APPENDED {
        Store::put(&writer, ROWS, &key_of(number), &payload_of(number))?;
        assert!(Store::get(&follower, ROWS, &key_of(number))?.is_none());
    }
    assert!(Store::contains(&follower, ROWS, &key_of(0))?);
    println!("writer appended {APPENDED} records: the follower misses them and serves the rest");

    let caught_up = follower.refresh()?;
    assert!(caught_up.applied >= APPENDED);

    for number in SEEDED..SEEDED + APPENDED {
        let now = Store::get(&follower, ROWS, &key_of(number))?.expect("appended record");
        assert_eq!(&*now, payload_of(number).as_slice());
    }
    println!(
        "refresh applied {} records up to sequence {}, and the appends read",
        caught_up.applied,
        caught_up.highest_lsn.as_u64(),
    );

    // The next pass starts where this one stopped, so following an append costs
    // that append rather than a walk of the volume.
    let idle = follower.refresh()?;
    assert_eq!(idle.applied, 0);
    println!("a second refresh applied {} records", idle.applied);

    writer.close()?;

    Ok(())
}
