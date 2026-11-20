//! Where a write-index shard's open keys lie, as a count for each bucket of the key space, so a scan can tell a shard has no open
//! key near it and place no head there
//!
//! A bucket is the top bits of a key's first eight bytes, so buckets follow key order and a bucket's first key is a real key to
//! stand a head on. Every column shares the buckets: a count from another column only makes a shard look busier, which costs a
//! descent and never an answer. Counts are read with no lock and written under the shard's write lock. A count that reaches its
//! top stays there, since it can't say how many rows it stands for.

use std::sync::atomic::{AtomicU8, Ordering};

use reel_core::direction::Direction;
use reel_core::units::ByteCount;

use crate::pool::touched_from;

/// A count that stays at its top
const STUCK: u8 = u8::MAX;
/// Buckets a scan looks over from its own for the first that holds an open key, so a head stands on that bucket's first key
pub const BUCKET_LOOK: usize = 512;

/// Where a skipped shard's head stands
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpenBound {
    /// No open key lies on this side of this key in the shard
    Key([u8; 8]),
    /// No open key lies anywhere ahead of the walk in the shard
    End,
}

/// One shard's open counts
#[derive(Debug)]
pub struct OpenCounts {
    counts: Vec<AtomicU8>,
    shift: u32,
}

impl OpenCounts {
    /// Allocate and touch counts for this many buckets, rounded to a power of two
    pub fn new(buckets: usize) -> Self {
        let buckets = buckets.max(1).next_power_of_two();
        let counts = touched_from(buckets, || AtomicU8::new(0));
        Self { counts, shift: u64::BITS - buckets.trailing_zeros() }
    }

    /// Read the heap the counts hold
    pub fn heap_bytes(&self) -> ByteCount {
        ByteCount(self.counts.capacity() as u64)
    }

    /// Count an open key in. The caller holds the shard's write lock
    pub fn add(&self, key: &[u8]) {
        let count = &self.counts[self.bucket(key)];
        let now = count.load(Ordering::Relaxed);
        if now < STUCK {
            count.store(now + 1, Ordering::Release);
        }
    }

    /// Count an open key out. The caller holds the shard's write lock
    pub fn remove(&self, key: &[u8]) {
        let count = &self.counts[self.bucket(key)];
        let now = count.load(Ordering::Relaxed);
        if now > 0 && now < STUCK {
            count.store(now - 1, Ordering::Release);
        }
    }

    /// Say where a walk standing at a key, going one way, can stand this shard's head with no descent: at the first bucket ahead
    /// that holds an open key, or past the end when none does. Nothing when that bucket is the walk's own or the next one down
    pub fn bound(&self, key: &[u8], direction: Direction) -> Option<OpenBound> {
        let last = self.counts.len() - 1;
        let at = self.bucket(key);
        let is_open = |bucket: usize| self.counts[bucket].load(Ordering::Acquire) != 0;
        if is_open(at) {
            return None;
        }
        match direction {
            // no open key lies before the first open bucket past this one, or before the last bucket looked at
            Direction::Ascending => {
                let end = (at + BUCKET_LOOK).min(last);
                match (at + 1..=end).find(|bucket| is_open(*bucket)) {
                    Some(open) => Some(OpenBound::Key(self.first_key(open))),
                    None if end == last => Some(OpenBound::End),
                    None => Some(OpenBound::Key(self.first_key(end + 1))),
                }
            },
            // no open key lies at or past the bucket after the first open one below this, or the lowest bucket looked at
            Direction::Descending => {
                let low = at.saturating_sub(BUCKET_LOOK);
                match (low..at).rev().find(|bucket| is_open(*bucket)) {
                    // an open bucket right below the walk's own leaves no bound below the walk, so the head is placed for real
                    Some(open) if open + 1 == at => None,
                    Some(open) => Some(OpenBound::Key(self.first_key(open + 1))),
                    None if low == 0 => Some(OpenBound::End),
                    None => Some(OpenBound::Key(self.first_key(low))),
                }
            },
        }
    }

    /// Read a key's bucket from its first eight bytes, zeros past a shorter key
    fn bucket(&self, key: &[u8]) -> usize {
        let mut word = [0; 8];
        let length = key.len().min(word.len());
        word[..length].copy_from_slice(&key[..length]);
        u64::from_be_bytes(word).checked_shr(self.shift).unwrap_or(0) as usize
    }

    /// Read the first key a bucket holds: its number in the top bits and zeros under them
    fn first_key(&self, bucket: usize) -> [u8; 8] {
        (bucket as u64).checked_shl(self.shift).unwrap_or(0).to_be_bytes()
    }
}
