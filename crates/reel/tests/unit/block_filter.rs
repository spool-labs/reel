use reel::index::key_hash::{KeyHash, hash_key};
use reel::segment::block_filter::*;
use reel_api::column_id::ColumnId;

const ROWS: u64 = 1_400;
const CHECKS: u64 = 100_000;
const COLUMN: ColumnId = ColumnId(1);
// the bound: 0.05 across 32 overlapping segments
const MOST_FALSE_RATE: f64 = 0.05 / 32.0;

fn hash_of(number: u64) -> KeyHash {
    hash_key(COLUMN, &number.to_le_bytes())
}

// a stored filter opens to its bits, and one flipped bit is refused
#[test]
fn filter_rot() {
    let mut stored = vec![0; filter_bytes(100) + FILTER_SUM_BYTES];
    let bits = stored.len() - FILTER_SUM_BYTES;
    add_key(&mut stored[..bits], hash_of(1));
    seal_filter(&mut stored);

    assert_eq!(open_filter(&stored).map(<[u8]>::len), Some(bits));
    stored[3] ^= 1;
    assert_eq!(open_filter(&stored), None);
}

// every added key is found, and the false positive rate holds the bound
#[test]
fn false_rate() {
    let mut filter = vec![0; filter_bytes(ROWS as usize)];
    for number in 0..ROWS {
        add_key(&mut filter, hash_of(number));
    }

    for number in 0..ROWS {
        assert!(may_contain(&filter, hash_of(number)));
    }

    let mut false_hits = 0;
    for number in ROWS..ROWS + CHECKS {
        if may_contain(&filter, hash_of(number)) {
            false_hits += 1;
        }
    }
    assert!((false_hits as f64 / CHECKS as f64) <= MOST_FALSE_RATE);
}
