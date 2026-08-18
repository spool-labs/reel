//! The filter a sealed segment carries so a miss can skip it
//!
//! One filter per column per segment, built at seal, saying either "not here" or
//! "search". Everything fails open, because saying no wrongly is a key that has
//! vanished: an unknown kind, a truncated region and a length that disagrees with
//! itself all mean search the segment. For the same reason a filter covers every
//! row the partition holds, tombstones included, since a delete missing from it
//! lets a probe skip past to an older version.

use crate::format::record::read_u32_le;

/// No filter for this partition, so every probe searches it
pub const KIND_ABSENT: u8 = 0;

/// A blocked bloom filter: one cache line per key, several bits inside it
pub const KIND_BLOOM: u8 = 1;

/// Bytes of header every encoded filter carries
///
/// Kind, seed, probe count, a spare byte, the keys covered, and its own total
/// length, which is what lets a reader walk a region of them with no directory.
/// The probe count is stored rather than recomputed, since a bloom queried at a
/// different count than it was built at loses keys it holds.
pub const HEADER_LEN: usize = 1 + 1 + 1 + 1 + 4 + 4;

/// Bytes one block covers, which is the cache line a probe wants to touch once
const BLOCK_BYTES: usize = 64;

/// Bits one block holds
const BLOCK_BITS: u32 = (BLOCK_BYTES * 8) as u32;

/// Most probes one key makes, whatever its bits per key would ask for
const MAX_PROBES: u32 = 8;

/// Seed every filter is built under
///
/// One value rather than a search, since a bloom cannot fail to build. Written
/// into the header all the same, for a kind that would have to retry seeds.
const SEED: u8 = 0;

/// Bits per key past which more bits buy less than the probes cost
const MAX_BITS_PER_KEY: u8 = 32;

/// One column's filter for one sealed segment
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Filter {
    /// Which structure the body holds, so an unknown one can fail open
    kind: u8,

    /// Hash seed the build settled on
    seed: u8,

    /// Bits one key sets, which a query must repeat exactly
    probes: u8,

    /// Keys the filter was built over, kept for sizing and for reporting
    keys: u32,

    /// The structure itself
    body: Vec<u8>,
}

impl Filter {
    /// Build a filter over every key a partition holds
    ///
    /// Nothing comes back for no keys or no bits, and a partition without a
    /// filter is searched.
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

    /// An empty filter sized for a key budget, filled by `insert`
    ///
    /// For a filter taking its keys as segments seal rather than in one pass.
    /// Nothing comes back for a zero budget or zero bits, as `build` answers them.
    pub fn sized(budget: usize, bits_per_key: u8) -> Option<Filter> {
        if budget == 0 || bits_per_key == 0 {
            return None;
        }
        let bits_per_key = bits_per_key.min(MAX_BITS_PER_KEY);
        Some(Filter {
            kind: KIND_BLOOM,
            seed: SEED,
            probes: probes_for(bits_per_key) as u8,
            keys: 0,
            body: vec![0u8; blocks_for(budget, bits_per_key) * BLOCK_BYTES],
        })
    }

    /// Set one key's bits, counting it toward the keys this filter holds
    pub fn insert(&mut self, key: &[u8]) {
        let blocks = self.body.len() / BLOCK_BYTES;
        if self.kind != KIND_BLOOM || blocks == 0 || self.probes == 0 {
            return;
        }
        let hash = hash_key(key, self.seed);
        let block = block_of(hash, blocks) * BLOCK_BYTES;
        for bit in bits_of(hash, u32::from(self.probes)) {
            let at = bit as usize / 8;
            self.body[block + at] |= 1u8 << (bit % 8);
        }
        self.keys += 1;
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

    /// Keys this filter was built over
    pub fn keys(&self) -> u32 {
        self.keys
    }

    /// Bytes this filter takes on disk, header and all
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
    ///
    /// One header per partition either way, so the walk stays in step with the
    /// directory. A region that is absent, short, or unparsable leaves the
    /// partitions behind it without filters, which searches them.
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

/// Blocks a filter of this many keys at this many bits takes
fn blocks_for(count: usize, bits_per_key: u8) -> usize {
    let bits = (count as u64).saturating_mul(u64::from(bits_per_key));
    bits.div_ceil(u64::from(BLOCK_BITS)) as usize
}

/// Probes one key makes at this many bits per key
///
/// The bloom optimum is bits times ln 2, capped so a fat filter does not spend
/// its time probing.
fn probes_for(bits_per_key: u8) -> u32 {
    let probes = (f64::from(bits_per_key) * std::f64::consts::LN_2).round() as u32;
    probes.min(MAX_PROBES)
}

/// Which block a hash lands in, without a division
///
/// From the high half only, leaving the low half free to pick bits inside the
/// block without the two being one number twice.
fn block_of(hash: u64, blocks: usize) -> usize {
    (((hash >> 32) * blocks as u64) >> 32) as usize
}

/// The bits inside a block one key sets, by double hashing
///
/// The step is a fresh mix rather than the other half of the same word, which
/// would make the two terms equal and leave every probe on one sequence.
fn bits_of(hash: u64, probes: u32) -> impl Iterator<Item = u32> {
    let first = hash as u32;
    let step = (hash.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as u32 | 1;
    (0..probes).map(move |probe| first.wrapping_add(probe.wrapping_mul(step)) % BLOCK_BITS)
}

/// A 64 bit hash of a key under a seed
///
/// The key is mixed rather than sliced: keys here are close to uniform already,
/// but the bits a probe wants are not independent just because the key is.
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

    // keys that never went in are mostly ruled out, which is what makes it worth carrying
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

    // a truncated region rules nothing out rather than ruling everything out
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
