use reel::engine::engine_sizing::*;
use reel::index::open_slots::MAX_OPEN_SLOTS;
use reel_api::units::{ByteCount, RowCount};

const ROWS_PER_BLOCK: u64 = 1_400;
const MOST_ROW_BYTES: u64 = 1_024;

// the default budget gets near-gigabyte segments, and a small one scales down
#[test]
fn budget_split() {
    let full = EngineSizing::for_store(&shape(FULL_BUDGET, 3));
    let small = EngineSizing::for_store(&shape(ByteCount(32 * 1024 * 1024), 1));

    assert_eq!(full.segment_bytes, SEGMENT_TARGET);
    assert_eq!(full.lz4_stored_limit, LZ4_STORED_LIMIT);
    assert!(small.segment_bytes < full.segment_bytes);
    assert!(small.shard.open_slots <= MAX_OPEN_SLOTS);
    assert!(small.segment_rows.0 > 0);
}

fn shape(budget: ByteCount, roots: usize) -> StoreShape {
    StoreShape {
        budget,
        roots,
        volumes: 1,
        has_lz4: false,
        widest_keys: 64,
    }
}

// lz4's compress buffer counts against the budget, and the floor rises with it
#[test]
fn least_budget() {
    let plain = shape(FULL_BUDGET, 1);
    let lz4 = StoreShape { has_lz4: true, ..plain };

    assert!(plain.least_budget() < LEAST_LZ4_BUDGET);
    assert_eq!(lz4.least_budget().0, plain.least_budget().0 + LZ4_STORED_LIMIT.0);
}

// the rows a segment takes at each budget, as `SEAL_ROWS_EIGHTHS` documents them
#[test]
fn documented_rows() {
    for (mib, rows) in [(128, 52_801), (192, 145_186), (512, 607_114), (4_096, 5_780_705)] {
        let store = StoreShape {
            budget: ByteCount(mib * 1024 * 1024),
            roots: 1,
            volumes: 1,
            has_lz4: true,
            widest_keys: 64,
        };

        assert_eq!(EngineSizing::for_store(&store).segment_rows, RowCount(rows), "{mib} MiB");
    }
}

// a summary at the default budget holds the block counts of a billion keys, beside a row for each segment
#[test]
fn summary_holds_blocks() {
    let store = StoreShape {
        budget: ByteCount(512 * 1024 * 1024),
        roots: 1,
        volumes: 1,
        has_lz4: true,
        widest_keys: 64,
    };
    let rows_room = EngineSizing::for_store(&store).summary_share.0 / 2;
    let blocks = 1_000_000_000 / ROWS_PER_BLOCK;
    let segments = 1_024;

    assert!(rows_room > blocks * size_of::<u32>() as u64 + segments * MOST_ROW_BYTES);
}
