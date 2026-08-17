//! Append sequence numbers and the per reel counter that issues them

use std::sync::atomic::{AtomicU64, Ordering};

/// The first sequence number a fresh counter issues
const FIRST_LSN: u64 = 1;

/// A per reel append sequence number that totally orders a reel's records
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Lsn(pub u64);

impl crate::index::tbtreemap::TreeKey for Lsn {
    type Probe = Lsn;
    type Window = crate::index::tbtreemap::Whole;

    fn filler() -> Lsn {
        Lsn(0)
    }

    /// A sequence number is its own lead, so a node is searched in one compare
    fn head(probe: &Lsn) -> u64 {
        probe.0
    }

    fn separator(_left: &Lsn, right: &Lsn) -> (Lsn, bool) {
        (*right, false)
    }
}

impl Lsn {
    /// The reserved sequence number carried by filler and control records
    pub const NONE: Lsn = Lsn(0);

    /// Read the underlying sequence value
    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// Encode as little endian bytes for a record header or footer entry
    pub fn pack(self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    /// Decode from little endian bytes
    pub fn unpack(bytes: [u8; 8]) -> Lsn {
        Lsn(u64::from_le_bytes(bytes))
    }
}

/// The atomic counter a reel draws its append sequence numbers from
pub struct LsnCounter(AtomicU64);

impl LsnCounter {
    /// A counter whose first issued sequence number is one
    pub fn new() -> LsnCounter {
        LsnCounter(AtomicU64::new(FIRST_LSN))
    }

    /// Issue the next sequence number and advance the counter
    pub fn issue(&self) -> Lsn {
        Lsn(self.0.fetch_add(1, Ordering::Relaxed))
    }

    /// The sequence number the next issue would return, without advancing
    pub fn peek(&self) -> Lsn {
        Lsn(self.0.load(Ordering::Relaxed))
    }

    /// Raise the counter so the next issue exceeds a sequence number found on disk
    pub fn recover_to(&self, highest_seen: Lsn) {
        let floor = highest_seen.0.saturating_add(1).max(FIRST_LSN);
        self.0.fetch_max(floor, Ordering::Relaxed);
    }
}

impl Default for LsnCounter {
    fn default() -> LsnCounter {
        LsnCounter::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // a fresh counter issues sequential numbers from one
    #[test]
    fn issues_sequentially() {
        let counter = LsnCounter::new();

        assert_eq!(counter.issue(), Lsn(1));
        assert_eq!(counter.issue(), Lsn(2));
        assert_eq!(counter.peek(), Lsn(3));
    }

    // recovery raises the counter above the highest number seen
    #[test]
    fn recovers_above_seen() {
        let counter = LsnCounter::new();

        counter.recover_to(Lsn(41));

        assert_eq!(counter.issue(), Lsn(42));
    }

    // recovery never lowers a counter that already ran ahead
    #[test]
    fn recovery_never_lowers() {
        let counter = LsnCounter::new();
        counter.issue();
        counter.issue();

        counter.recover_to(Lsn(0));

        assert_eq!(counter.peek(), Lsn(3));
    }

    // a sequence number round trips through its byte form
    #[test]
    fn byte_roundtrip() {
        let lsn = Lsn(0x0102_0304_0506_0708);

        assert_eq!(Lsn::unpack(lsn.pack()), lsn);
    }
}
