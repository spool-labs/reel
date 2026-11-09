//! Key ranges a damaged segment may hold rows in, which reads must not answer past, in a fixed pool

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, WritePlace};

use crate::config::MAX_KEY_BYTES;
use crate::guard::hold;
use crate::segment::segment_tail::{MAX_COLUMNS, SegmentNumber};
use crate::segment::summary::SpanFacts;

/// Spans a volume can hold: more than there are columns, so a full pool always has two spans of one column to
/// merge, at about 2 KiB a span
pub const MOST_DAMAGED_SPANS: usize = 2 * MAX_COLUMNS;
/// The longest key a span can keep: a key and the zero that makes it an exclusive end
pub const SPAN_KEY_BYTES: usize = MAX_KEY_BYTES as usize + 1;
// the merge in a full pool needs more spans than columns, and a summary row keeps its span count in two bytes
const _: () = assert!(MOST_DAMAGED_SPANS > MAX_COLUMNS && MOST_DAMAGED_SPANS < u16::MAX as usize);
/// Marks a span that runs to its column's end
const NO_END: u16 = u16::MAX;

/// One range of one column, from a start key up to an end key or the column's end
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DamagedRange {
    pub column: ColumnId,
    pub segment: SegmentNumber,
    pub highest_place: WritePlace,
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
}

/// One span of the pool, with its keys inside it
#[derive(Clone, Debug)]
pub struct DamageSlot {
    column: ColumnId,
    segment: SegmentNumber,
    highest_place: WritePlace,
    start_length: u16,
    end_length: u16,
    start: [u8; SPAN_KEY_BYTES],
    end: [u8; SPAN_KEY_BYTES],
}

/// A range of keys a walk is about to cross
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KeyRange<'key> {
    pub low: &'key [u8],
    pub high: Option<&'key [u8]>,
    pub is_high_inclusive: bool,
}

/// A clear that needs a span the pool doesn't have
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PoolFull;

/// The damaged spans of a volume, with a count so a healthy store pays one load per get
#[derive(Debug)]
pub struct DamageList {
    slots: RwLock<Vec<DamageSlot>>,
    spare: Mutex<Vec<DamageSlot>>,
    count: AtomicUsize,
}

impl DamageSlot {
    /// Build a slot from a range's parts
    pub fn new(range: &DamagedRange) -> Self {
        let mut slot = Self {
            column: range.column,
            segment: range.segment,
            highest_place: range.highest_place,
            start_length: 0,
            end_length: NO_END,
            start: [0; SPAN_KEY_BYTES],
            end: [0; SPAN_KEY_BYTES],
        };
        slot.set_start(&range.start);
        slot.set_end(range.end.as_deref());
        slot
    }

    /// Read the start key
    pub fn start(&self) -> &[u8] {
        &self.start[..self.start_length as usize]
    }

    /// Read the end key, which is nothing when the span runs to the column's end
    pub fn end(&self) -> Option<&[u8]> {
        (self.end_length != NO_END).then(|| &self.end[..self.end_length as usize])
    }

    /// Set the start key, cut to what a span keeps, which can only start lower
    pub fn set_start(&mut self, key: &[u8]) {
        let length = key.len().min(SPAN_KEY_BYTES);
        self.start[..length].copy_from_slice(&key[..length]);
        self.start_length = length as u16;
    }

    /// Set the end key, and a key too long to keep leaves the span open-ended, which can only widen it
    pub fn set_end(&mut self, key: Option<&[u8]>) {
        match key {
            Some(key) if key.len() <= SPAN_KEY_BYTES => {
                self.end[..key.len()].copy_from_slice(key);
                self.end_length = key.len() as u16;
            },
            _too_long_or_none => self.end_length = NO_END,
        }
    }

    /// Check the span meets a range of keys, whose low end it may start at and whose high end comes as the caller says
    pub fn meets(&self, column: ColumnId, range: &KeyRange) -> bool {
        let is_above_low = self.end().is_none_or(|end| end > range.low);
        let is_below_high = range.high.is_none_or(|high| {
            let start = self.start();
            start < high || (range.is_high_inclusive && start == high)
        });
        self.column == column && is_above_low && is_below_high
    }

    /// Check the span holds a key
    pub fn covers(&self, column: ColumnId, key: &[u8]) -> bool {
        self.column == column && key >= self.start() && self.end().is_none_or(|end| key < end)
    }

    /// Read the span as a summary row keeps it
    pub fn facts(&self) -> SpanFacts<'_> {
        SpanFacts {
            column: self.column,
            start: self.start(),
            end: self.end(),
        }
    }

    /// Check a cleared span leaves a part of this span below it and a part above it
    pub fn splits(&self, column: ColumnId, start: &[u8], end: Option<&[u8]>) -> bool {
        let Some(end) = end else {
            return false;
        };
        self.column == column && self.start() < start && self.end().is_none_or(|own_end| end < own_end)
    }

    /// Widen the span to its whole column, at the higher of two places
    pub fn widen(&mut self, place: WritePlace) {
        self.start_length = 0;
        self.end_length = NO_END;
        self.highest_place = self.highest_place.max(place);
    }
}

impl Default for DamageList {
    fn default() -> Self {
        Self::new()
    }
}

impl DamageList {
    /// Take the pool and its spare, and write every page of both
    pub fn new() -> Self {
        let blank = DamageSlot::new(&DamagedRange {
            column: ColumnId(0),
            segment: SegmentNumber(0),
            highest_place: WritePlace(0),
            start: Vec::new(),
            end: None,
        });
        let mut slots = vec![blank.clone(); MOST_DAMAGED_SPANS];
        let mut spare = vec![blank; MOST_DAMAGED_SPANS];
        slots.clear();
        spare.clear();
        Self {
            slots: RwLock::new(slots),
            spare: Mutex::new(spare),
            count: AtomicUsize::new(0),
        }
    }

    /// Read the heap the pool holds
    pub fn heap_bytes(&self) -> ByteCount {
        ByteCount((2 * MOST_DAMAGED_SPANS * size_of::<DamageSlot>()) as u64)
    }

    /// Count the spans
    pub fn len(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    /// Check whether nothing is damaged
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Add a span. A full pool widens instead: the segment's spans in that column become the whole column, or
    /// with none there another segment's span in the column does, or two spans of some column merge to make room
    pub fn add(&self, range: &DamagedRange) {
        let mut slots = write_slots(&self.slots);
        let is_covered = slots.len() == MOST_DAMAGED_SPANS && widen_for(&mut slots, range);
        debug_assert!(is_covered || slots.len() < MOST_DAMAGED_SPANS);
        if !is_covered && slots.len() < MOST_DAMAGED_SPANS {
            slots.push(DamageSlot::new(range));
        }
        self.count.store(slots.len(), Ordering::Release);
    }

    /// Find the highest damaged place over a key, if any span holds it
    pub fn covering(&self, column: ColumnId, key: &[u8]) -> Option<WritePlace> {
        if self.is_empty() {
            return None;
        }

        let slots = read_slots(&self.slots);
        let mut highest = None;
        for slot in slots.iter() {
            if slot.covers(column, key) {
                highest = Some(highest.map_or(slot.highest_place, |place: WritePlace| place.max(slot.highest_place)));
            }
        }
        highest
    }

    /// Check any span meets a range of keys
    pub fn meets(&self, column: ColumnId, range: &KeyRange) -> bool {
        !self.is_empty() && read_slots(&self.slots).iter().any(|slot| slot.meets(column, range))
    }

    /// Clear the part of every span inside [start, end), and leave what lies outside it. A clear that would
    /// split a span on a full pool changes nothing and says so
    pub fn clear(&self, column: ColumnId, start: &[u8], end: Option<&[u8]>) -> Result<(), PoolFull> {
        let mut slots = write_slots(&self.slots);
        let splits = slots.iter().filter(|slot| slot.splits(column, start, end)).count();
        if slots.len() + splits > MOST_DAMAGED_SPANS {
            return Err(PoolFull);
        }

        let mut index = 0;
        while index < slots.len() {
            if slots[index].column != column {
                index += 1;
                continue;
            }
            if slots[index].splits(column, start, end) {
                let mut above = slots[index].clone();
                above.set_start(end.unwrap_or_default());
                slots[index].set_end(Some(start));
                slots.push(above);
                index += 1;
                continue;
            }
            if clear_slot(&mut slots[index], start, end) {
                index += 1;
            } else {
                slots.swap_remove(index);
            }
        }
        self.count.store(slots.len(), Ordering::Release);
        Ok(())
    }

    /// Take out every span of one segment
    pub fn drop_segment(&self, segment: SegmentNumber) {
        let mut slots = write_slots(&self.slots);
        slots.retain(|slot| slot.segment != segment);
        self.count.store(slots.len(), Ordering::Release);
    }

    /// Check any span belongs to a segment
    pub fn holds_segment(&self, segment: SegmentNumber) -> bool {
        read_slots(&self.slots).iter().any(|slot| slot.segment == segment)
    }

    /// Keep a copy of the spans in the spare, which a failed save goes back to
    pub fn keep_spare(&self) {
        hold(&self.spare).clone_from(&read_slots(&self.slots));
    }

    /// Go back to the copy in the spare
    pub fn restore_spare(&self) {
        let mut slots = write_slots(&self.slots);
        slots.clone_from(&hold(&self.spare));
        self.count.store(slots.len(), Ordering::Release);
    }

    /// Hold the spans for a walk, which a summary row writes out
    pub fn slots(&self) -> RwLockReadGuard<'_, Vec<DamageSlot>> {
        read_slots(&self.slots)
    }
}

/// Walk one segment's spans as a summary row keeps them
pub fn spans_of(slots: &[DamageSlot], segment: SegmentNumber) -> impl Iterator<Item = SpanFacts<'_>> + Clone {
    slots.iter().filter(move |slot| slot.segment == segment).map(DamageSlot::facts)
}

/// Find the highest place among one segment's spans
pub fn highest_of(slots: &[DamageSlot], segment: SegmentNumber) -> Option<WritePlace> {
    slots.iter().filter(|slot| slot.segment == segment).map(|slot| slot.highest_place).max()
}

/// Trim a span by a cleared range, and return false when nothing is left of it
pub fn clear_slot(slot: &mut DamageSlot, start: &[u8], end: Option<&[u8]>) -> bool {
    let is_past_clear = end.is_some_and(|end| slot.start() >= end);
    let is_before_clear = slot.end().is_some_and(|own_end| own_end <= start);
    if is_past_clear || is_before_clear {
        return true;
    }

    if slot.start() < start {
        slot.set_end(Some(start));
        return true;
    }
    let Some(end) = end else {
        return false;
    };
    if slot.end().is_none_or(|own_end| own_end > end) {
        slot.set_start(end);
        return true;
    }
    false
}

/// Make a full pool cover a range with no new span and return true, or free a span for it
pub fn widen_for(slots: &mut Vec<DamageSlot>, range: &DamagedRange) -> bool {
    let is_own = |slot: &DamageSlot| slot.column == range.column && slot.segment == range.segment;
    let first = slots.iter().position(is_own).or_else(|| slots.iter().position(|slot| slot.column == range.column));
    if let Some(first) = first {
        let segment = slots[first].segment;
        let mut highest = range.highest_place;
        for slot in slots.iter().filter(|slot| slot.column == range.column && slot.segment == segment) {
            highest = highest.max(slot.highest_place);
        }
        slots[first].widen(highest);
        let kept = first;
        let mut index = 0;
        slots.retain(|slot| {
            let is_kept = index == kept || slot.column != range.column || slot.segment != segment;
            index += 1;
            is_kept
        });
        return true;
    }

    // no span of this column: with more spans than columns, some column has two to merge
    for index in 0..slots.len() {
        let column = slots[index].column;
        if let Some(other) = (index + 1..slots.len()).find(|other| slots[*other].column == column) {
            let place = slots[other].highest_place;
            slots[index].widen(place);
            slots.swap_remove(other);
            return false;
        }
    }
    false
}

/// Read the spans
pub fn read_slots(slots: &RwLock<Vec<DamageSlot>>) -> RwLockReadGuard<'_, Vec<DamageSlot>> {
    match slots.read() {
        Ok(guard) => guard,
        Err(poisoned) => crate::guard::recover(poisoned),
    }
}

/// Change the spans
pub fn write_slots(slots: &RwLock<Vec<DamageSlot>>) -> RwLockWriteGuard<'_, Vec<DamageSlot>> {
    match slots.write() {
        Ok(guard) => guard,
        Err(poisoned) => crate::guard::recover(poisoned),
    }
}
