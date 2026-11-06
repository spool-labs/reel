use reel::engine::engine_sizing::*;
use reel::engine::seal_job::MOST_SEGMENT_BLOCKS;
use reel::index::open_slots::MAX_OPEN_SLOTS;
use reel::row_kind::RowKind;
use reel::segment::row_block::{COMPLETE_BLOCK_BYTES, FooterRow, RowBlockBuilder};
use reel_api::units::{ByteCount, FileOffset, RowCount, WritePlace};

const ROWS_PER_BLOCK: u64 = 1_400;
const MOST_ROW_BYTES: u64 = 1_024;

// the default budget gets near-gigabyte segments, and a small one scales down
#[test]
fn budget_split() {
    let full = EngineSizing::for_store(&shape(FULL_BUDGET, 3));
    let small = EngineSizing::for_store(&shape(ByteCount(48 * 1024 * 1024), 1));

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
        threads: reel::config::NO_WORKERS,
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
    for (mib, rows) in [(128, 27_516), (192, 119_901), (512, 581_830), (4_096, 5_755_420)] {
        let store = StoreShape {
            budget: ByteCount(mib * 1024 * 1024),
            roots: 1,
            volumes: 1,
            has_lz4: true,
            widest_keys: 64,
            threads: reel::config::NO_WORKERS,
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
        threads: reel::config::NO_WORKERS,
    };
    let rows_room = EngineSizing::for_store(&store).summary_share.0 / 2;
    let blocks = 1_000_000_000 / ROWS_PER_BLOCK;
    let segments = 1_024;

    assert!(rows_room > blocks * size_of::<u32>() as u64 + segments * MOST_ROW_BYTES);
}

// the reserve an idle volume holds at each budget, as `SEAL_ROWS_EIGHTHS` documents it
#[test]
fn documented_reserve() {
    for (mib, reserve) in [(128, 128), (192, 192), (512, 2_048), (4_096, 2_048)] {
        let store = StoreShape {
            budget: ByteCount(mib * 1024 * 1024),
            roots: 1,
            volumes: 1,
            has_lz4: true,
            widest_keys: 64,
            threads: reel::config::NO_WORKERS,
        };

        let segment = EngineSizing::for_store(&store).segment_bytes;
        assert_eq!(2 * segment.0, reserve * 1024 * 1024, "{mib} MiB");
    }
}

fn blocks_for(rows: u64, row_of: impl Fn(u64) -> (Vec<u8>, FooterRow)) -> u64 {
    let mut builder = RowBlockBuilder::new();
    builder.set_block_bytes(COMPLETE_BLOCK_BYTES);
    let mut blocks = 1;
    for number in 0..rows {
        let (key, row) = row_of(number);
        if !builder.push(&key, &row) {
            let _bytes = builder.finish();
            builder.clear();
            blocks += 1;
            assert!(builder.push(&key, &row));
        }
    }
    blocks
}

// a seal's row limit fits the block lines a slot can address, with the widest rows and the smallest the format allows
#[test]
fn complete_rows_fit_the_lines() {
    let widest = 32;
    let rows = complete_row_limit(widest).0;
    let worst = |number: u64| {
        let mut key = vec![0; widest];
        key[..8].copy_from_slice(&number.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes());
        let row = FooterRow {
            place: WritePlace(u64::MAX - number),
            offset: FileOffset(u64::from(u32::MAX) - number),
            length: ByteCount(u64::from(u32::MAX)),
            kind: RowKind::Value,
        };
        (key, row)
    };
    assert!(blocks_for(rows, worst) <= u64::from(MOST_SEGMENT_BLOCKS.0));

    let smallest = |number: u64| {
        let row = FooterRow {
            place: WritePlace(0),
            offset: FileOffset(0),
            length: ByteCount(0),
            kind: RowKind::Grave,
        };
        (number.to_be_bytes().to_vec(), row)
    };
    assert!(blocks_for(rows, smallest) <= u64::from(MOST_SEGMENT_BLOCKS.0));
}

// a seal point leaves room for what arrives while the seal runs, and stays inside the row limit and its floor
#[test]
fn seal_point_from_rates() {
    let limit = RowCount(5_000_000);
    let index_rows = limit.0 * 8 / SEAL_ROWS_EIGHTHS;
    assert_eq!(seal_point(limit, 0, 1_000_000), limit);
    assert_eq!(seal_point(limit, 1_000_000, 0), limit);

    let (seal_rate, ingest_rate) = (2_000_000, 1_600_000);
    let point = seal_point(limit, seal_rate, ingest_rate).0;
    assert!(point < limit.0);
    let seal_seconds = point as f64 / seal_rate as f64;
    let room_seconds = (index_rows - point) as f64 / ingest_rate as f64;
    assert!(room_seconds >= seal_seconds, "{room_seconds} s of room, {seal_seconds} s of seal");

    assert_eq!(seal_point(limit, 100_000_000, 1), limit);
    assert_eq!(seal_point(limit, 1, 100_000_000).0, index_rows / LEAST_SEAL_DIVISOR);

    // a seal starts with room for a tenth of a second of its own rate, whatever the ingest
    let (fast_seal, slow_ingest) = (6_000_000, 1);
    assert!(index_rows - seal_point(limit, fast_seal, slow_ingest).0 >= fast_seal / 10);
}

// a rate needs time to have passed
#[test]
fn rates() {
    assert_eq!(rows_per_second(RowCount(500), SECOND / 2), 1_000);
    assert_eq!(rows_per_second(RowCount(500), 0), 0);
}
