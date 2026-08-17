//! Consistent read views, held open against a volume that keeps changing
//!
//! A cue point is one sequence number plus a promise that everything needed to
//! answer reads at that number is still on disk.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::format::lsn::Lsn;
use crate::index::tbtreemap::{TBTreeMap, NODE_WIDTH};
use crate::sync::lock;

/// Cue points a volume is currently holding open
#[derive(Debug, Default)]
pub struct CuePoints {
    /// Holders per sequence number, ordered so the floor is its lowest key
    held: Mutex<TBTreeMap<Lsn, NODE_WIDTH, usize>>,

    /// Oldest number held, or u64::MAX for none, read by compaction per pass
    floor: AtomicU64,
}

impl CuePoints {
    pub fn new() -> CuePoints {
        CuePoints {
            held: Mutex::new(TBTreeMap::new()),
            floor: AtomicU64::new(u64::MAX),
        }
    }

    fn take(&self, at: Lsn) {
        let mut held = lock(&self.held);
        *held.get_or_insert(at, 0) += 1;
        self.settle(&held);
    }

    fn release(&self, at: Lsn) {
        let mut held = lock(&self.held);
        if let Some(count) = held.get_mut(&at) {
            *count -= 1;
            if *count == 0 {
                // Packed, since the numbers climb and the releases drain the low
                // end: a bare removal leaves emptied leaves in the floor's walk.
                held.remove_packed(&at);
            }
        }
        self.settle(&held);
    }

    fn settle(&self, held: &TBTreeMap<Lsn, NODE_WIDTH, usize>) {
        let floor = held
            .first_key_value()
            .map_or(u64::MAX, |(lsn, _)| lsn.as_u64());
        self.floor.store(floor, Ordering::Release);
    }

    /// The oldest sequence number a live cue point still needs, if any
    pub fn floor(&self) -> Option<Lsn> {
        match self.floor.load(Ordering::Acquire) {
            u64::MAX => None,
            floor => Some(Lsn(floor)),
        }
    }

    /// Whether anything is held at all
    pub fn is_empty(&self) -> bool {
        self.floor.load(Ordering::Acquire) == u64::MAX
    }

    /// Sequence numbers held and how many hold each, for reporting
    pub fn held(&self) -> Vec<(Lsn, usize)> {
        lock(&self.held)
            .iter()
            .map(|(at, count)| (*at, *count))
            .collect()
    }
}

/// A view of the volume as it stood at one sequence number
///
/// Holding one keeps the versions it can see from being reclaimed; dropping it
/// gives that back, which is why it is a guard rather than a bare number.
pub struct CuePoint {
    at: Lsn,
    points: Arc<CuePoints>,
}

impl CuePoint {
    /// Register a hold, taken by the engine once it has sealed
    pub(crate) fn hold(at: Lsn, points: Arc<CuePoints>) -> CuePoint {
        points.take(at);
        CuePoint { at, points }
    }

    /// The sequence number this view is taken at
    pub fn at(&self) -> Lsn {
        self.at
    }
}

impl Drop for CuePoint {
    fn drop(&mut self) {
        self.points.release(self.at);
    }
}

impl std::fmt::Debug for CuePoint {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(out, "CuePoint({})", self.at.as_u64())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn points() -> Arc<CuePoints> {
        Arc::new(CuePoints::new())
    }

    // a volume holding nothing has no floor, so nothing else changes
    #[test]
    fn none_held() {
        let points = points();
        assert!(points.is_empty());
        assert_eq!(points.floor(), None);
    }

    // the floor is the oldest held, and rises as holders let go
    #[test]
    fn floor_follows_oldest() {
        let points = points();
        let old = CuePoint::hold(Lsn(10), Arc::clone(&points));
        let new = CuePoint::hold(Lsn(50), Arc::clone(&points));
        assert_eq!(points.floor(), Some(Lsn(10)));

        drop(old);
        assert_eq!(points.floor(), Some(Lsn(50)));

        drop(new);
        assert_eq!(points.floor(), None);
    }

    // two holders of one number share it, so the first to go holds nothing back
    #[test]
    fn holders_share() {
        let points = points();
        let first = CuePoint::hold(Lsn(7), Arc::clone(&points));
        let second = CuePoint::hold(Lsn(7), Arc::clone(&points));
        assert_eq!(points.held(), vec![(Lsn(7), 2)]);

        drop(first);
        assert_eq!(points.floor(), Some(Lsn(7)));

        drop(second);
        assert_eq!(points.floor(), None);
    }
}
