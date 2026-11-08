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

// the multiply-high reduction with its full stride keeps the false positive rate the modulo and its half stride had, from the least filter up to a full block
#[test]
fn reduction_rate_matches_modulo() {
    let modulo_contains = |filter: &[u8], hash: KeyHash| {
        let bits = filter.len() as u64 * u64::from(u8::BITS);
        let stride = (hash.0 >> 32) | 1;
        let mut probe = hash.0;
        (0..PROBE_COUNT).all(|_round| {
            let bit = probe % bits;
            probe = probe.wrapping_add(stride);
            filter[(bit / 8) as usize] & (1 << (bit % 8)) != 0
        })
    };
    let modulo_add = |filter: &mut [u8], hash: KeyHash| {
        let bits = filter.len() as u64 * u64::from(u8::BITS);
        let stride = (hash.0 >> 32) | 1;
        let mut probe = hash.0;
        for _round in 0..PROBE_COUNT {
            let bit = probe % bits;
            filter[(bit / 8) as usize] |= 1 << (bit % 8);
            probe = probe.wrapping_add(stride);
        }
    };
    for rows in [4_u64, 64, 1_400, 16_384] {
        let (mut reduced, mut modulo) = (vec![0; filter_bytes(rows as usize)], vec![0; filter_bytes(rows as usize)]);
        for number in 0..rows {
            add_key(&mut reduced, hash_of(number));
            modulo_add(&mut modulo, hash_of(number));
        }
        let checks = CHECKS.max(rows * 20);
        let mut hits = (0, 0);
        for number in rows..rows + checks {
            hits.0 += u64::from(may_contain(&reduced, hash_of(number)));
            hits.1 += u64::from(modulo_contains(&modulo, hash_of(number)));
        }
        let (reduced_rate, modulo_rate) = (hits.0 as f64 / checks as f64, hits.1 as f64 / checks as f64);
        // a few standard deviations of the modulo's own count
        let noise = 4.0 * (modulo_rate * (1.0 - modulo_rate) / checks as f64).sqrt() + 1.0 / checks as f64;
        assert!(reduced_rate <= modulo_rate + noise, "{rows} rows: reduced {reduced_rate} against modulo {modulo_rate}");
        assert!((0..rows).all(|number| may_contain(&reduced, hash_of(number))));
    }
}
