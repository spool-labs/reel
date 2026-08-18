//! One column's sealed keys as a single filter, asked before any segment is
//!
//! Ruling a key out through the per-segment filters costs one check per sealed
//! segment. This answers the same question once, ahead of the fan-out.

use crate::format::filter::Filter;
use crate::format::footer::FooterPartition;
use crate::sync::checked::{read, write, AtomicU64, Ordering, RwLock};

/// Keys the first level is sized for
const FIRST_LEVEL_KEYS: usize = 256 * 1024;

/// How much larger each level is than the one before it
///
/// A miss probes every level, so growth is steep to keep the stack short.
const LEVEL_GROWTH: usize = 4;

/// Bits per key each level spends, the footer filters' own default
const LEVEL_BITS_PER_KEY: u8 = 10;

/// The sealed keys of one column, held as a stack of filters
///
/// A filter cannot grow, so the top level takes keys until its budget is spent and
/// the next is born larger. Nothing is ever removed, and a stale bit costs a search
/// that finds nothing, never a wrong answer.
pub struct SealedKeys {
    /// The stack itself, guarded so a seal can add while searches read
    levels: RwLock<Levels>,

    /// Searches the filter answered with "no segment holds this", the measure
    skips: AtomicU64,
}

struct Levels {
    /// The filters, oldest level first
    stack: Vec<Filter>,

    /// Keys the top level still takes before the next one is born
    room: usize,
}

impl Default for SealedKeys {
    fn default() -> Self {
        Self::new()
    }
}

impl SealedKeys {
    pub fn new() -> SealedKeys {
        SealedKeys {
            levels: RwLock::new(Levels {
                stack: Vec::new(),
                room: 0,
            }),
            skips: AtomicU64::new(0),
        }
    }

    /// Take one key in
    pub fn insert(&self, key: &[u8]) {
        let mut levels = write(&self.levels);
        levels.take(key);
    }

    /// Take every key a sealed partition holds, under one lock
    pub fn insert_partition(&self, partition: &FooterPartition) {
        let mut levels = write(&self.levels);
        for at in 0..partition.len() {
            if let Some(key) = partition.key_at(at) {
                levels.take(key);
            }
        }
    }

    /// Whether any sealed segment may hold this key
    ///
    /// A no is authoritative: keys go in before the sealed span they came from is
    /// visible, so any segment a search could reach is already represented here.
    pub fn may_hold(&self, key: &[u8]) -> bool {
        let held = read(&self.levels);
        if held.stack.iter().rev().any(|filter| filter.may_hold(key)) {
            return true;
        }
        drop(held);
        self.skips.fetch_add(1, Ordering::Relaxed);
        false
    }

    /// Searches answered without asking any segment
    pub fn skips(&self) -> u64 {
        self.skips.load(Ordering::Relaxed)
    }

    /// Take another set's levels wholesale, for an install that starts over
    pub fn adopt(&self, other: &SealedKeys) {
        let mut theirs = write(&other.levels);
        let mut held = write(&self.levels);
        held.stack = std::mem::take(&mut theirs.stack);
        held.room = theirs.room;
    }
}

impl Levels {
    fn take(&mut self, key: &[u8]) {
        if self.room == 0 {
            let budget = FIRST_LEVEL_KEYS * LEVEL_GROWTH.pow(self.stack.len() as u32);
            let Some(filter) = Filter::sized(budget, LEVEL_BITS_PER_KEY) else {
                return;
            };
            self.stack.push(filter);
            self.room = budget;
        }
        // The level was just topped up if it was out of room.
        if let Some(top) = self.stack.last_mut() {
            top.insert(key);
            self.room -= 1;
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    // every inserted key is held, across a level boundary
    #[test]
    fn inserted_keys_are_held_across_levels() {
        let keys = SealedKeys::new();
        for at in 0..(FIRST_LEVEL_KEYS + 10) as u64 {
            keys.insert(&at.to_le_bytes());
        }
        for at in (0..(FIRST_LEVEL_KEYS + 10) as u64).step_by(1000) {
            assert!(keys.may_hold(&at.to_le_bytes()));
        }
        assert_eq!(keys.skips(), 0);
    }

    // a key never inserted is almost always ruled out, and the skip is counted
    #[test]
    fn a_fresh_key_is_ruled_out() {
        let keys = SealedKeys::new();
        for at in 0..1000u64 {
            keys.insert(&at.to_le_bytes());
        }
        let missing = (1000u64..2000)
            .filter(|at| !keys.may_hold(&at.to_le_bytes()))
            .count();
        assert!(
            missing > 950,
            "only {missing} of 1000 fresh keys were ruled out"
        );
        assert_eq!(keys.skips(), missing as u64);
    }
}
