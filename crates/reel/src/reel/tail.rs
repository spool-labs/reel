//! What each tail publishes: the segment it appends to and how far its write head has reached
//! These numbers are for observing a tail, and read safety must not rely on them

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::format::loc::SegmentId;

/// The observable state of one append tail
pub struct Tail {
    index: u64,
    active_segment: AtomicU32,
    committed_len: AtomicU64,
}

impl Tail {
    /// A tail with no segment yet, identified by its index within the reel
    pub fn new(index: u64) -> Tail {
        Tail {
            index,
            active_segment: AtomicU32::new(0),
            committed_len: AtomicU64::new(0),
        }
    }

    /// Position of this tail among its reel's active tails
    pub fn index(&self) -> u64 {
        self.index
    }

    /// Segment number the tail is currently appending to
    pub fn active_segment(&self) -> SegmentId {
        SegmentId(self.active_segment.load(Ordering::Acquire))
    }

    /// How far the write head has reached in the active segment
    pub fn committed_len(&self) -> u64 {
        self.committed_len.load(Ordering::Acquire)
    }

    /// Start tracking a fresh segment, resetting the write head to its start
    pub fn begin_segment(&self, id: SegmentId) {
        self.active_segment.store(id.as_u32(), Ordering::Release);
        self.committed_len.store(0, Ordering::Release);
    }

    /// Publish how far the write head has reached, moving only forward
    pub fn publish_committed(&self, length: u64) {
        self.committed_len.fetch_max(length, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // committed length advances as the appender publishes it
    #[test]
    fn committed_advances() {
        let tail = Tail::new(0);
        tail.begin_segment(SegmentId(3));

        tail.publish_committed(4096);

        assert_eq!(tail.active_segment(), SegmentId(3));
        assert_eq!(tail.committed_len(), 4096);
    }

    // beginning a new segment resets the write head
    #[test]
    fn roll_resets() {
        let tail = Tail::new(0);
        tail.begin_segment(SegmentId(1));
        tail.publish_committed(8192);

        tail.begin_segment(SegmentId(2));

        assert_eq!(tail.active_segment(), SegmentId(2));
        assert_eq!(tail.committed_len(), 0);
    }
}
