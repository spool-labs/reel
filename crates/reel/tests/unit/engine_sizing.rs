use reel::engine::engine_sizing::*;
use reel::index::open_slots::MAX_OPEN_SLOTS;
use reel_api::units::{ByteCount, RowCount};

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
    for (mib, rows) in [(128, 62_967), (192, 155_353), (512, 617_281), (4_096, 5_790_871)] {
        let store = StoreShape {
            budget: ByteCount(mib * 1024 * 1024),
            roots: 1,
            volumes: 1,
            has_lz4: true,
        };

        assert_eq!(EngineSizing::for_store(&store).segment_rows, RowCount(rows), "{mib} MiB");
    }
}
