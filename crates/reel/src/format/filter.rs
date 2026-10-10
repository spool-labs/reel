//! A sealed segment's per-column filters, which let a miss skip it and fail open

use crate::format::record::read_u32_le;

/// No filter for this partition, so every probe searches it
pub const KIND_ABSENT: u8 = 0;

/// A blocked bloom filter: one cache line per key, several bits inside it
pub const KIND_BLOOM: u8 = 1;

/// Header length of an encoded filter: kind, seed, probes, spare, keys and total length
pub const HEADER_LEN: usize = 1 + 1 + 1 + 1 + 4 + 4;

/// One block is one cache line
const BLOCK_BYTES: usize = 64;

const BLOCK_BITS: u32 = (BLOCK_BYTES * 8) as u32;

/// A key makes at most this many probes
const MAX_PROBES: u32 = 8;

/// Every filter is built under this seed
const SEED: u8 = 0;

/// Bits per key are capped here, where more bits buy less than the probes cost
const MAX_BITS_PER_KEY: u8 = 32;

/// One column's filter for one sealed segment
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Filter {
    /// Which structure the body holds, so an unknown one can fail open
    kind: u8,

    /// The build's hash seed
    seed: u8,

    /// Each key sets this many bits, and a query must use the same count
    probes: u8,

    /// Number of keys the filter was built over
    keys: u32,

    /// The structure itself
    body: Vec<u8>,
}

impl Filter {
    /// Build a filter over every key a partition holds, or nothing for no keys or no bits
    pub fn build<'keys, Keys>(keys: Keys, count: usize, bits_per_key: u8) -> Option<Filter>
    where
        Keys: Iterator<Item = &'keys [u8]>,
    {
        if count == 0 || bits_per_key == 0 {
            return None;
        }
        let bits_per_key = bits_per_key.min(MAX_BITS_PER_KEY);
        let blocks = blocks_for(count, bits_per_key);
        let probes = probes_for(bits_per_key);
        let mut body = vec![0u8; blocks * BLOCK_BYTES];
        for key in keys {
            let hash = hash_key(key, SEED);
            let block = block_of(hash, blocks) * BLOCK_BYTES;
            for bit in bits_of(hash, probes) {
                let at = bit as usize / 8;
                body[block + at] |= 1u8 << (bit % 8);
            }
        }
        Some(Filter {
            kind: KIND_BLOOM,
            seed: SEED,
            probes: probes as u8,
            keys: count as u32,
            body,
        })
    }

    /// Whether the segment may hold this key, which is only ever a maybe or a no
    pub fn may_hold(&self, key: &[u8]) -> bool {
        match self.kind {
            KIND_BLOOM => self.bloom_holds(key),
            // A kind this build cannot read may not rule anything out.
            _ => true,
        }
    }

    fn bloom_holds(&self, key: &[u8]) -> bool {
        let blocks = self.body.len() / BLOCK_BYTES;
        if blocks == 0 || self.probes == 0 {
            return true;
        }
        let hash = hash_key(key, self.seed);
        let block = block_of(hash, blocks) * BLOCK_BYTES;
        for bit in bits_of(hash, u32::from(self.probes)) {
            let at = bit as usize / 8;
            if self.body[block + at] & (1u8 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }

    /// Number of keys the filter was built over
    pub fn keys(&self) -> u32 {
        self.keys
    }

    /// The filter's on-disk length, header included
    pub fn encoded_len(&self) -> usize {
        HEADER_LEN + self.body.len()
    }

    /// Append the filter's on-disk bytes
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.push(self.kind);
        out.push(self.seed);
        out.push(self.probes);
        out.push(0);
        out.extend_from_slice(&self.keys.to_le_bytes());
        out.extend_from_slice(&(self.encoded_len() as u32).to_le_bytes());
        out.extend_from_slice(&self.body);
    }

    /// Take one filter per partition off a footer's filter region
    pub fn parse_region(region: &[u8], partitions: usize) -> Vec<Option<Filter>> {
        let mut filters = Vec::with_capacity(partitions);
        let mut at = 0usize;
        while filters.len() < partitions {
            if at >= region.len() {
                filters.push(None);
                continue;
            }
            let (filter, took) = Filter::parse(&region[at..]);
            filters.push(filter);
            at += took;
        }
        filters
    }

    /// Take one filter off the front of a region, and say how far it reached
    fn parse(region: &[u8]) -> (Option<Filter>, usize) {
        if region.len() < HEADER_LEN {
            return (None, region.len());
        }
        let kind = region[0];
        let seed = region[1];
        let probes = region[2];
        let keys = read_u32_le(&region[4..8]);
        let len = read_u32_le(&region[8..HEADER_LEN]) as usize;
        if len < HEADER_LEN || len > region.len() {
            return (None, region.len());
        }
        if kind == KIND_ABSENT {
            return (None, len);
        }
        let filter = Filter {
            kind,
            seed,
            probes,
            keys,
            body: region[HEADER_LEN..len].to_vec(),
        };
        (Some(filter), len)
    }

    /// What a partition with no filter writes, so the region stays in step
    pub fn absent() -> Filter {
        Filter {
            kind: KIND_ABSENT,
            seed: SEED,
            probes: 0,
            keys: 0,
            body: Vec::new(),
        }
    }
}

/// Number of blocks for this many keys at this many bits per key
fn blocks_for(count: usize, bits_per_key: u8) -> usize {
    let bits = (count as u64).saturating_mul(u64::from(bits_per_key));
    bits.div_ceil(u64::from(BLOCK_BITS)) as usize
}

/// Probes per key at this many bits per key, bits times ln 2 capped at `MAX_PROBES`
fn probes_for(bits_per_key: u8) -> u32 {
    let probes = (f64::from(bits_per_key) * std::f64::consts::LN_2).round() as u32;
    probes.min(MAX_PROBES)
}

/// Which block a hash lands in, from its high half and without a division
fn block_of(hash: u64, blocks: usize) -> usize {
    (((hash >> 32) * blocks as u64) >> 32) as usize
}

/// The bits inside a block one key sets, by double hashing
fn bits_of(hash: u64, probes: u32) -> impl Iterator<Item = u32> {
    let first = hash as u32;
    let step = (hash.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as u32 | 1;
    (0..probes).map(move |probe| first.wrapping_add(probe.wrapping_mul(step)) % BLOCK_BITS)
}

/// A 64 bit hash of a key under a seed
fn hash_key(key: &[u8], seed: u8) -> u64 {
    const ODD: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut state = 0xcbf2_9ce4_8422_2325u64 ^ u64::from(seed).wrapping_mul(ODD);
    let mut chunks = key.chunks_exact(8);
    for chunk in &mut chunks {
        let word = u64::from_le_bytes(chunk.try_into().unwrap_or([0u8; 8]));
        state = (state ^ word).wrapping_mul(ODD).rotate_left(31);
    }
    let mut tail = 0u64;
    for (at, byte) in chunks.remainder().iter().enumerate() {
        tail |= u64::from(*byte) << (at * 8);
    }
    state ^= tail ^ key.len() as u64;
    state ^= state >> 30;
    state = state.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    state ^= state >> 27;
    state = state.wrapping_mul(0x94D0_49BB_1331_11EB);
    state ^ (state >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(count: usize) -> Vec<[u8; 8]> {
        (0..count as u64).map(u64::to_be_bytes).collect()
    }

    fn built(count: usize, bits: u8) -> Filter {
        let held = keys(count);
        Filter::build(held.iter().map(|key| key.as_slice()), held.len(), bits).expect("filter")
    }

    // every key that went in comes back a maybe, which is the only promise it makes
    #[test]
    fn holds_every_key() {
        for count in [1usize, 7, 1_000, 10_000] {
            let filter = built(count, 10);
            for key in keys(count) {
                assert!(filter.may_hold(&key), "{count} keys lost one of them");
            }
        }
    }

    // keys that never went in are mostly ruled out, which is what makes it worth keeping
    #[test]
    fn rules_most_out() {
        let filter = built(10_000, 10);
        let mut kept = 0;
        for absent in 10_000u64..20_000 {
            if filter.may_hold(&absent.to_be_bytes()) {
                kept += 1;
            }
        }

        assert!(kept < 200, "{kept} of 10000 absent keys were not ruled out");
    }

    // more bits rule more out
    #[test]
    fn bits_buy_accuracy() {
        let thin = built(4_000, 4);
        let fat = built(4_000, 16);
        let held = |filter: &Filter| {
            (4_000u64..8_000)
                .filter(|key| filter.may_hold(&key.to_be_bytes()))
                .count()
        };

        assert!(held(&fat) < held(&thin));
    }

    // an encoded filter parses back to the same answers
    #[test]
    fn round_trip() {
        let filter = built(500, 10);
        let mut region = Vec::new();
        filter.encode(&mut region);

        let (parsed, took) = Filter::parse(&region);

        assert_eq!(took, region.len());
        assert_eq!(parsed.as_ref(), Some(&filter));
        for key in keys(500) {
            assert!(parsed.as_ref().expect("parsed").may_hold(&key));
        }
    }

    // several filters ride one region and come back in order
    #[test]
    fn region_walks() {
        let first = built(100, 10);
        let second = built(50, 8);
        let mut region = Vec::new();
        first.encode(&mut region);
        Filter::absent().encode(&mut region);
        second.encode(&mut region);

        let (one, took) = Filter::parse(&region);
        let (none, absent) = Filter::parse(&region[took..]);
        let (two, _) = Filter::parse(&region[took + absent..]);

        assert_eq!(one.as_ref(), Some(&first));
        assert_eq!(none, None);
        assert_eq!(two.as_ref(), Some(&second));
    }

    // a truncated region rules nothing out
    #[test]
    fn truncation_fails_open() {
        let filter = built(100, 10);
        let mut region = Vec::new();
        filter.encode(&mut region);
        region.truncate(region.len() - 8);

        let (parsed, took) = Filter::parse(&region);

        assert_eq!(parsed, None);
        assert_eq!(
            took,
            region.len(),
            "a bad region consumes the rest of itself"
        );
    }

    // a kind this build does not know says maybe to everything
    #[test]
    fn unknown_kind_fails_open() {
        let mut filter = built(100, 10);
        filter.kind = 200;

        assert!(filter.may_hold(b"anything at all"));
    }

    // nothing to filter is not a filter
    #[test]
    fn nothing_to_build() {
        assert_eq!(Filter::build(std::iter::empty(), 0, 10), None);
        assert_eq!(Filter::build([b"a".as_slice()].into_iter(), 1, 0), None);
    }
}
