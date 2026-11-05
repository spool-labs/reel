//! Key ranges a damaged segment may hold rows in, which reads must not answer past

use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::atomic::{AtomicUsize, Ordering};

use reel_api::column_id::ColumnId;
use reel_api::units::WritePlace;

use crate::segment::segment_tail::SegmentNumber;

/// One range of one column, from a start key up to an end key or the column's end
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DamagedRange {
    pub column: ColumnId,
    pub segment: SegmentNumber,
    pub highest_place: WritePlace,
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
}

/// The damaged ranges of a volume, with a count so a healthy store pays one load per get
#[derive(Debug, Default)]
pub struct DamageList {
    ranges: RwLock<Vec<DamagedRange>>,
    count: AtomicUsize,
}

impl DamagedRange {
    /// Check the range holds a key
    pub fn covers(&self, column: ColumnId, key: &[u8]) -> bool {
        self.column == column && key >= self.start.as_slice() && self.end.as_ref().is_none_or(|end| key < end.as_slice())
    }
}

impl DamageList {
    /// Start an empty list
    pub fn new() -> Self {
        Self::default()
    }

    /// Count the ranges
    pub fn len(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    /// Check whether nothing is damaged
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Add a range
    pub fn add(&self, range: DamagedRange) {
        let mut ranges = write_ranges(&self.ranges);
        ranges.push(range);
        self.count.store(ranges.len(), Ordering::Release);
    }

    /// Find the highest damaged place over a key, if any range holds it
    pub fn covering(&self, column: ColumnId, key: &[u8]) -> Option<WritePlace> {
        if self.is_empty() {
            return None;
        }

        let ranges = read_ranges(&self.ranges);
        let mut highest = None;
        for range in ranges.iter() {
            if range.covers(column, key) {
                highest = Some(highest.map_or(range.highest_place, |place: WritePlace| place.max(range.highest_place)));
            }
        }
        highest
    }

    /// Clear the part of every range inside [start, end), and leave what lies outside it
    pub fn clear(&self, column: ColumnId, start: &[u8], end: Option<&[u8]>) {
        let mut ranges = write_ranges(&self.ranges);
        let mut kept = Vec::with_capacity(ranges.len() + 1);
        for range in ranges.drain(..) {
            if range.column != column {
                kept.push(range);
                continue;
            }
            keep_outside(&range, start, end, &mut kept);
        }
        *ranges = kept;
        self.count.store(ranges.len(), Ordering::Release);
    }

    /// Take out every range of one segment
    pub fn drop_segment(&self, segment: SegmentNumber) {
        let mut ranges = write_ranges(&self.ranges);
        ranges.retain(|range| range.segment != segment);
        self.count.store(ranges.len(), Ordering::Release);
    }

    /// Copy the ranges of one segment
    pub fn of_segment(&self, segment: SegmentNumber) -> Vec<DamagedRange> {
        let ranges = read_ranges(&self.ranges);
        let mut found = Vec::new();
        for range in ranges.iter() {
            if range.segment == segment {
                found.push(range.clone());
            }
        }
        found
    }
}

/// Add the parts of a range below and above a cleared span
pub fn keep_outside(range: &DamagedRange, start: &[u8], end: Option<&[u8]>, kept: &mut Vec<DamagedRange>) {
    if range.start.as_slice() < start {
        let reach = match &range.end {
            Some(range_end) => range_end.as_slice().min(start).to_vec(),
            None => start.to_vec(),
        };
        let mut below = range.clone();
        below.end = Some(reach);
        kept.push(below);
    }

    let Some(end) = end else {
        return;
    };
    let reaches_past = range.end.as_ref().is_none_or(|range_end| range_end.as_slice() > end);
    if reaches_past {
        let mut above = range.clone();
        above.start = above.start.max(end.to_vec());
        kept.push(above);
    }
}

/// Read the ranges
pub fn read_ranges(ranges: &RwLock<Vec<DamagedRange>>) -> RwLockReadGuard<'_, Vec<DamagedRange>> {
    match ranges.read() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Change the ranges
pub fn write_ranges(ranges: &RwLock<Vec<DamagedRange>>) -> RwLockWriteGuard<'_, Vec<DamagedRange>> {
    match ranges.write() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
