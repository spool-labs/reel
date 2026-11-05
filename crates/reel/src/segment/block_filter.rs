//! A bloom filter over one row block's keys

use crate::index::key_hash::KeyHash;

/// Bits per row, so false positives across 32 overlapping segments stay at 0.05 or less
pub const BITS_PER_ROW: usize = 14;
/// Probes per key, the best count at that many bits
pub const PROBE_COUNT: u64 = 9;
/// Smallest filter, so a block of few rows still has room
pub const MIN_FILTER_BYTES: usize = 8;
/// Shift that picks the second probe stride
pub const STRIDE_SHIFT: u32 = 32;

/// Count the bytes a filter over some rows takes
pub fn filter_bytes(rows: usize) -> usize {
    (rows * BITS_PER_ROW).div_ceil(u8::BITS as usize).max(MIN_FILTER_BYTES)
}

/// Add a key to a filter
pub fn add_key(filter: &mut [u8], hash: KeyHash) {
    let bits = filter.len() as u64 * u64::from(u8::BITS);
    let stride = probe_stride(hash);
    let mut probe = hash.0;
    for _round in 0..PROBE_COUNT {
        let bit = probe % bits;
        filter[(bit / u64::from(u8::BITS)) as usize] |= 1 << (bit % u64::from(u8::BITS));
        probe = probe.wrapping_add(stride);
    }
}

/// Check whether a key may be in a filter
pub fn may_contain(filter: &[u8], hash: KeyHash) -> bool {
    if filter.is_empty() {
        return false;
    }

    let bits = filter.len() as u64 * u64::from(u8::BITS);
    let stride = probe_stride(hash);
    let mut probe = hash.0;
    for _round in 0..PROBE_COUNT {
        let bit = probe % bits;
        if filter[(bit / u64::from(u8::BITS)) as usize] & (1 << (bit % u64::from(u8::BITS))) == 0 {
            return false;
        }
        probe = probe.wrapping_add(stride);
    }
    true
}

/// Pick the odd stride between a key's probes
pub fn probe_stride(hash: KeyHash) -> u64 {
    (hash.0 >> STRIDE_SHIFT) | 1
}

#[cfg(test)]
mod tests {
    use reel_api::column_id::ColumnId;

    use super::*;
    use crate::index::key_hash::hash_key;

    const ROWS: u64 = 1_400;
    const CHECKS: u64 = 100_000;
    const COLUMN: ColumnId = ColumnId(1);
    // the bound: 0.05 across 32 overlapping segments
    const MOST_FALSE_RATE: f64 = 0.05 / 32.0;

    fn hash_of(number: u64) -> KeyHash {
        hash_key(COLUMN, &number.to_le_bytes())
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
}
