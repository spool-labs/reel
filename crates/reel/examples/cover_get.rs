//! What the cover check adds to a get, with the pool empty, with one cover and with the pool full.
//! Run with `cargo run --release -p reel --example cover_get`
#![allow(clippy::print_stdout)]

use std::hint::black_box;
use std::time::Instant;

use reel::segment::range_covers::{Cover, MOST_RANGE_COVERS, RangeCovers};
use reel::segment::segment_tail::SegmentNumber;
use reel_core::column_id::ColumnId;
use reel_core::units::WritePlace;

const COLUMN: ColumnId = ColumnId(1);
const KEY_BYTES: usize = 32;
const LOOKUPS: u64 = 4_000_000;
const NESTED_LOOKUPS: u64 = 20_000;
const NANOS_PER_SECOND: f64 = 1_000_000_000.0;

fn key_of(number: u64) -> Vec<u8> {
    format!("{number:0KEY_BYTES$}").into_bytes()
}

fn add(covers: &RangeCovers, number: u64) {
    let (start, end) = (key_of(2 * number), key_of(2 * number + 1));
    covers.add(&Cover {
        column: COLUMN,
        start: &start,
        end: &end,
        place: WritePlace(number + 1),
        segment: SegmentNumber(1),
    }, false);
}

fn time(covers: &RangeCovers, name: &str) {
    let keys: Vec<Vec<u8>> = (0..1_024).map(|number| key_of(number * 7)).collect();
    let start = Instant::now();
    for lookup in 0..LOOKUPS {
        let key = &keys[lookup as usize % keys.len()];
        black_box(covers.newest_over(COLUMN, key, WritePlace(u64::MAX)));
    }
    let nanos = start.elapsed().as_secs_f64() * NANOS_PER_SECOND / LOOKUPS as f64;
    println!("{name}: {nanos:.0} ns a check, {} covers", covers.len());
}

fn main() {
    let covers = RangeCovers::new(KEY_BYTES);
    time(&covers, "empty");
    add(&covers, 0);
    time(&covers, "one cover");
    for number in 1..MOST_RANGE_COVERS as u64 {
        add(&covers, number);
    }
    time(&covers, "full pool");

    // the worst case: every cover in the pool holds the key, so the walk visits all of them
    let nested = RangeCovers::new(KEY_BYTES);
    for depth in 0..MOST_RANGE_COVERS as u64 {
        let (start, end) = (key_of(depth), key_of(2 * MOST_RANGE_COVERS as u64 - depth));
        nested.add(&Cover {
            column: COLUMN,
            start: &start,
            end: &end,
            place: WritePlace(depth + 1),
            segment: SegmentNumber(1),
        }, false);
    }
    let key = key_of(MOST_RANGE_COVERS as u64);
    let start = Instant::now();
    for _lookup in 0..NESTED_LOOKUPS {
        black_box(nested.newest_over(COLUMN, &key, WritePlace(u64::MAX)));
    }
    let nanos = start.elapsed().as_secs_f64() * NANOS_PER_SECOND / NESTED_LOOKUPS as f64;
    println!("every cover over one key: {nanos:.0} ns a check, {} steps", nested.longest_walk());
}
