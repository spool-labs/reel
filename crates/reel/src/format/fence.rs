//! One lead per block of a sealed partition, so a search lands on its block
//!
//! A blocked search would binary search a partition's blocks by their first keys,
//! paying a block read per halving. A fence is those first keys written down, so
//! the descent happens in memory and the search reads the one block it wants.
//!
//! A lead is only the front of a first key, zero padded, which keeps the order.
//! Truncation is safe because a lead is never believed on its own: it narrows
//! which blocks a search reads and the rows decide against whole keys. The error
//! is one-sided, so the bracket hands back every block whose lead ties.

use std::sync::Arc;

use crate::error::{ReelError, Result};

/// Bytes of a block's first key one lead holds
///
/// Eight leaves no tie run at all for a key whose first eight bytes are its own.
pub const FENCE_LEAD: usize = 8;

/// Leads one page of a fence holds, which is the stride the sampled level takes
///
/// The leads are a fixed stride array, so a fence left on the volume is read a
/// page at a time and the sampled level says which page. Four kibibytes of leads.
pub const FENCE_PAGE_LEADS: usize = 512;

/// The lead a key sorts under: its front bytes, zero padded to the lead width
pub fn lead_of(key: &[u8]) -> [u8; FENCE_LEAD] {
    let mut lead = [0u8; FENCE_LEAD];
    let width = key.len().min(FENCE_LEAD);
    lead[..width].copy_from_slice(&key[..width]);
    lead
}

/// Leads the sampled level over this many blocks holds, one per page of leads
pub fn top_leads(blocks: usize) -> usize {
    blocks.div_ceil(FENCE_PAGE_LEADS)
}

/// Bytes one partition's fence occupies: a lead a block, then the sampled level
pub fn fence_bytes(blocks: usize) -> usize {
    (blocks + top_leads(blocks)) * FENCE_LEAD
}

/// A run of one partition's leads, whether that is all of them or one page
#[derive(Clone, Debug)]
pub struct FenceCut {
    /// The leads themselves, packed at the lead width
    leads: Arc<[u8]>,

    /// Which block the first of them leads, so a hit is reported absolutely
    first: usize,
}

impl FenceCut {
    /// A cut over no leads at all, which brackets nothing and reads nothing
    pub fn empty() -> FenceCut {
        FenceCut {
            leads: Arc::from(Vec::new()),
            first: 0,
        }
    }

    /// A cut over leads that ascend, or corruption when they do not
    ///
    /// A fence that does not ascend describes some other partition, and a search
    /// through it would report keys missing that the segment holds.
    pub fn try_new(leads: Arc<[u8]>, first: usize) -> Result<FenceCut> {
        if !leads.len().is_multiple_of(FENCE_LEAD) {
            return Err(ReelError::Corruption(
                "a fence is not a whole number of leads".to_string(),
            ));
        }
        let cut = FenceCut { leads, first };
        for at in 1..cut.len() {
            if cut.lead_at(at - 1) > cut.lead_at(at) {
                return Err(ReelError::Corruption(
                    "a segment's fence leads do not ascend".to_string(),
                ));
            }
        }
        Ok(cut)
    }

    /// Leads the cut holds
    pub fn len(&self) -> usize {
        self.leads.len() / FENCE_LEAD
    }

    /// Whether the cut leads nothing
    pub fn is_empty(&self) -> bool {
        self.leads.is_empty()
    }

    /// Bytes the cut weighs, for a cache that bounds what it holds
    pub fn weight(&self) -> usize {
        self.leads.len()
    }

    /// The blocks a key could be in, as a half-open range of block numbers
    ///
    /// Two bounds rather than one, because a truncated lead ties with keys it does
    /// not lead. A search over the range answers what a search over every block
    /// would: the last block at or below the key, or nothing at all.
    pub fn bracket(&self, key: &[u8]) -> (usize, usize) {
        let lead = lead_of(key);
        let from = self.bound(&lead, false);
        let past = self.bound(&lead, true);
        (self.first + from.saturating_sub(1), self.first + past)
    }

    /// Binary search for the first lead past a key, counting equal leads or not
    fn bound(&self, lead: &[u8], past_equal: bool) -> usize {
        let (mut low, mut high) = (0usize, self.len());
        while low < high {
            let middle = low + (high - low) / 2;
            let found = self.lead_at(middle);
            let is_below = match past_equal {
                true => found <= lead,
                false => found < lead,
            };
            match is_below {
                true => low = middle + 1,
                false => high = middle,
            }
        }
        low
    }

    fn lead_at(&self, at: usize) -> &[u8] {
        &self.leads[at * FENCE_LEAD..(at + 1) * FENCE_LEAD]
    }
}

/// A sealed partition's fence, and where the leads a search needs are
#[derive(Clone, Debug)]
pub enum Fence {
    /// Every lead resident, so a search descends with no io of its own
    Held(FenceCut),

    /// The sampled level resident and the leads on the volume, a page a search
    Sampled {
        /// One lead per page of leads, which is what says which page to read
        tops: FenceCut,

        /// Where the partition's leads begin in the segment file
        at: u64,

        /// Blocks the fence covers, which closes the last page
        blocks: usize,
    },
}

/// What one search has to do before it can use a fence
#[derive(Clone, Debug)]
pub enum FenceReach {
    /// The leads are already in hand
    Ready(FenceCut),

    /// One contiguous read of the leads, which the caller does and hands back
    Read {
        /// Byte offset of the leads within the segment file
        at: u64,

        /// Bytes to read, which is whole pages of leads
        len: usize,

        /// Block the first lead read leads
        first: usize,
    },
}

impl Fence {
    /// What this search needs before it can bracket its blocks
    ///
    /// A held fence answers outright. A sampled one names the pages of leads whose
    /// blocks could hold the key, and nothing at all when the key sorts under the
    /// whole partition.
    pub fn reach(&self, key: &[u8]) -> FenceReach {
        match self {
            Fence::Held(cut) => FenceReach::Ready(cut.clone()),
            Fence::Sampled { tops, at, blocks } => {
                let (low, high) = tops.bracket(key);
                if low >= high {
                    return FenceReach::Ready(FenceCut::empty());
                }
                let first = low * FENCE_PAGE_LEADS;
                let end = (high * FENCE_PAGE_LEADS).min(*blocks);
                FenceReach::Read {
                    at: at + (first * FENCE_LEAD) as u64,
                    len: end.saturating_sub(first) * FENCE_LEAD,
                    first,
                }
            }
        }
    }

    /// Bytes this fence weighs, for a cache that bounds what it holds
    pub fn weight(&self) -> usize {
        match self {
            Fence::Held(cut) => cut.weight(),
            Fence::Sampled { tops, .. } => tops.weight(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The leads of a run of block first keys, as a fence holds them
    fn leads(keys: &[&[u8]]) -> Arc<[u8]> {
        let mut packed = Vec::with_capacity(keys.len() * FENCE_LEAD);
        for key in keys {
            packed.extend_from_slice(&lead_of(key));
        }
        Arc::from(packed)
    }

    /// The last block at or below the key, which is what a block walk would find
    fn walked(keys: &[&[u8]], key: &[u8]) -> Option<usize> {
        let mut found = None;
        for (at, first) in keys.iter().enumerate() {
            if *first <= key {
                found = Some(at);
            }
        }
        found
    }

    /// What the bracket leaves the caller's search, run the way the block path
    /// runs it: the top of the bracket first, then the halvings
    fn found(keys: &[&[u8]], cut: &FenceCut, key: &[u8]) -> Option<usize> {
        let (mut low, mut high) = cut.bracket(key);
        if low < high {
            match keys.get(high - 1) {
                Some(first) if *first <= key => return Some(high - 1),
                _ => high -= 1,
            }
        }
        while low < high {
            let middle = low + (high - low) / 2;
            match keys.get(middle) {
                Some(first) if *first <= key => low = middle + 1,
                _ => high = middle,
            }
        }
        low.checked_sub(1)
    }

    // a key that sorts below another leads below it too, whatever their lengths
    #[test]
    fn leads_keep_order() {
        let pairs: [(&[u8], &[u8]); 5] = [
            (b"a", b"b"),
            (b"a", b"aa"),
            (b"aaaaaaaa", b"aaaaaaaab"),
            (b"aaaaaaaab", b"aaaaaaaac"),
            (&[0u8; 3], &[0u8, 0, 0, 1]),
        ];

        for (low, high) in pairs {
            assert!(low < high, "the fixture pair is not ordered");
            assert!(
                lead_of(low) <= lead_of(high),
                "{low:?} leads above {high:?}",
            );
        }
    }

    // the bracket names the same block a walk over every block would land on
    #[test]
    fn bracket_matches_a_walk() {
        let keys: Vec<Vec<u8>> = (0..64u64)
            .map(|at| (at * 7).to_be_bytes().to_vec())
            .collect();
        let firsts: Vec<&[u8]> = keys.iter().map(|key| key.as_slice()).collect();
        let cut = FenceCut::try_new(leads(&firsts), 0).expect("a fence that ascends");

        for probe in 0..64u64 * 7 + 8 {
            let key = probe.to_be_bytes();
            assert_eq!(
                found(&firsts, &cut, &key),
                walked(&firsts, &key),
                "probe {probe} landed on a different block",
            );
        }
    }

    // keys sharing a lead are bracketed together and the rows still decide
    #[test]
    fn a_shared_lead_brackets_its_run() {
        // Sixteen byte keys sharing their first eight, the case truncation carries
        let keys: Vec<Vec<u8>> = (0..32u64)
            .map(|at| {
                let mut key = [0u8; 16];
                key[8..].copy_from_slice(&at.to_be_bytes());
                key.to_vec()
            })
            .collect();
        let firsts: Vec<&[u8]> = keys.iter().map(|key| key.as_slice()).collect();
        let cut = FenceCut::try_new(leads(&firsts), 0).expect("a fence that ascends");

        for at in 0..32u64 {
            let mut key = [0u8; 16];
            key[8..].copy_from_slice(&at.to_be_bytes());
            assert_eq!(
                found(&firsts, &cut, &key),
                walked(&firsts, &key),
                "key {at} landed on a different block",
            );
        }

        let (low, high) = cut.bracket(&[0u8; 16]);
        assert_eq!((low, high), (0, 32), "every block ties on the lead");
    }

    // a key under the whole partition brackets nothing, so the search reads nothing
    #[test]
    fn a_key_below_everything() {
        let firsts: [&[u8]; 3] = [b"bbbbbbbb", b"cccccccc", b"dddddddd"];
        let cut = FenceCut::try_new(leads(&firsts), 0).expect("a fence that ascends");

        assert_eq!(cut.bracket(b"aaaaaaaa"), (0, 0));
        assert_eq!(found(&firsts, &cut, b"aaaaaaaa"), None);
    }

    // a key above the whole partition still ends in the last block, where its row is
    #[test]
    fn a_key_above_everything() {
        let firsts: [&[u8]; 3] = [b"bbbbbbbb", b"cccccccc", b"dddddddd"];
        let cut = FenceCut::try_new(leads(&firsts), 0).expect("a fence that ascends");

        assert_eq!(cut.bracket(b"zzzzzzzz"), (2, 3));
        assert_eq!(found(&firsts, &cut, b"zzzzzzzz"), Some(2));
    }

    // a cut over one page reports blocks by their place in the whole partition
    #[test]
    fn a_cut_reports_absolute_blocks() {
        let firsts: [&[u8]; 3] = [b"bbbbbbbb", b"cccccccc", b"dddddddd"];
        let cut = FenceCut::try_new(leads(&firsts), 1000).expect("a fence that ascends");

        // The block below a tie is in the bracket: a lead equal to the key can
        // belong to a first key above it, leaving the row in the block before.
        assert_eq!(cut.bracket(b"cccccccc"), (1000, 1002));
        assert_eq!(
            cut.bracket(b"aaaaaaaa"),
            (1000, 1000),
            "the block below the cut"
        );
    }

    // leads that do not ascend are a fence for some other partition
    #[test]
    fn leads_out_of_order() {
        let firsts: [&[u8]; 3] = [b"bbbbbbbb", b"aaaaaaaa", b"dddddddd"];

        assert!(FenceCut::try_new(leads(&firsts), 0).is_err());
        assert!(FenceCut::try_new(Arc::from(vec![0u8; FENCE_LEAD + 1]), 0).is_err());
    }

    // a sampled fence names one page of leads for a key whose front is its own
    #[test]
    fn sampled_names_one_page() {
        let pages = 4usize;
        let blocks = pages * FENCE_PAGE_LEADS;
        let tops: Vec<Vec<u8>> = (0..pages)
            .map(|page| ((page * FENCE_PAGE_LEADS) as u64).to_be_bytes().to_vec())
            .collect();
        let sampled: Vec<&[u8]> = tops.iter().map(|top| top.as_slice()).collect();
        let fence = Fence::Sampled {
            tops: FenceCut::try_new(leads(&sampled), 0).expect("a fence that ascends"),
            at: 4096,
            blocks,
        };

        // A key inside the third page's range
        let key = ((2 * FENCE_PAGE_LEADS + 9) as u64).to_be_bytes();
        match fence.reach(&key) {
            FenceReach::Read { at, len, first } => {
                assert_eq!(
                    first,
                    2 * FENCE_PAGE_LEADS,
                    "the page whose leads bracket it"
                );
                assert_eq!(at, 4096 + (first * FENCE_LEAD) as u64);
                assert_eq!(len, FENCE_PAGE_LEADS * FENCE_LEAD, "one page of leads");
            }
            FenceReach::Ready(_) => panic!("a sampled fence has to read its leads"),
        }
    }

    // a key under a sampled fence reads no leads at all
    #[test]
    fn sampled_below_everything() {
        let tops: [&[u8]; 2] = [b"bbbbbbbb", b"cccccccc"];
        let fence = Fence::Sampled {
            tops: FenceCut::try_new(leads(&tops), 0).expect("a fence that ascends"),
            at: 0,
            blocks: FENCE_PAGE_LEADS + 1,
        };

        match fence.reach(b"aaaaaaaa") {
            FenceReach::Ready(cut) => assert_eq!(cut.bracket(b"aaaaaaaa"), (0, 0)),
            FenceReach::Read { .. } => panic!("nothing under the first lead is worth a read"),
        }
    }

    // the region is a lead a block and one more per page of them
    #[test]
    fn region_size() {
        assert_eq!(fence_bytes(0), 0);
        assert_eq!(fence_bytes(1), 2 * FENCE_LEAD);
        assert_eq!(
            fence_bytes(FENCE_PAGE_LEADS + 1),
            (FENCE_PAGE_LEADS + 1 + 2) * FENCE_LEAD,
        );
    }
}
