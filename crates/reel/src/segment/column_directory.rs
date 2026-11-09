//! Which sealed segments can hold a key of one column, in one list a change swaps whole

use std::sync::atomic::{AtomicU64, Ordering};
use std::mem;
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use reel_api::units::{ByteCount, WritePlace};

use crate::fixed_list::ListOut;
use crate::guard::hold;
use crate::pool::touched_capacity;
use crate::record::record_check::{Checksum, checksum};
use crate::segment::block_entry::{KeyBound, KeyPrefix};
use crate::segment::segment_tail::{ColumnTail, SegmentNumber};

/// One segment's range in one column
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DirectoryEntry {
    pub shared_length: u16,
    pub shared_sum: Checksum,
    pub lowest: KeyPrefix,
    pub highest: KeyPrefix,
    pub reach: i32,
    pub segment: SegmentNumber,
    pub newest_place: WritePlace,
}

/// One segment's keys in one column, in order among every segment's, for a walk over a range of keys
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RangeEntry {
    pub lowest: KeyBound,
    pub highest: KeyBound,
    pub reach: u32,
    pub segment: SegmentNumber,
}

/// What a list holds for one segment of a column
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Listing {
    pub entry: DirectoryEntry,
    pub range: RangeEntry,
}

/// A segment whose range holds a key, and the newest place any of its rows has
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Candidate {
    pub segment: SegmentNumber,
    pub newest_place: WritePlace,
}

/// The list, with the generation of its last change
#[derive(Clone, Debug, Default)]
pub struct DirectoryList {
    pub entries: Vec<DirectoryEntry>,
    pub ranges: Vec<RangeEntry>,
    pub generation: u64,
}

/// What one pass over the list left out
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DirectoryPage {
    pub generation: u64,
    pub skipped_past: usize,
    pub is_cut: bool,
}

/// Counts the segments a search has passed and fills a page
#[derive(Debug)]
pub struct PageFill<'out, Out> {
    out: &'out mut Out,
    skip: usize,
    seen: usize,
    is_cut: bool,
}

/// A column's segments, grouped by the bytes each one's keys share and sorted by lowest key in a group, with room fixed at open.
/// A change builds the new list in a spare the volume's columns share and swaps it in, so readers wait for the swap alone
#[derive(Debug)]
pub struct ColumnDirectory {
    list: RwLock<DirectoryList>,
    spare: Arc<Mutex<DirectoryList>>,
    generation: AtomicU64,
}

impl Listing {
    /// Build a segment's listing from its tail
    pub fn of_column(column: &ColumnTail, segment: SegmentNumber, newest_place: WritePlace) -> Self {
        let (lowest, highest) = column.key_bounds();
        Self {
            entry: DirectoryEntry {
                shared_length: column.shared_length,
                shared_sum: column.shared_sum,
                lowest: column.lowest,
                highest: column.highest,
                reach: 0,
                segment,
                newest_place,
            },
            range: RangeEntry {
                lowest,
                highest,
                reach: 0,
                segment,
            },
        }
    }
}

impl<Out> PageFill<'_, Out> {
    /// Add a matching segment, or note that the page is full
    pub fn add<Item: Copy>(&mut self, item: Item)
    where
        Out: ListOut<Item>,
    {
        self.seen += 1;
        if self.seen <= self.skip {
            return;
        }
        if !self.out.push_if_room(item) {
            self.is_cut = true;
        }
    }
}

impl ColumnDirectory {
    /// Allocate room for a number of segments, with a spare of its own
    pub fn new(capacity: usize) -> Self {
        Self::with_spare(capacity, Arc::new(Mutex::new(empty_list(capacity))))
    }

    /// Allocate room for a number of segments, with a spare other columns of the volume share. The spare has the same room
    pub fn with_spare(capacity: usize, spare: Arc<Mutex<DirectoryList>>) -> Self {
        Self {
            list: RwLock::new(empty_list(capacity)),
            spare,
            generation: AtomicU64::new(0),
        }
    }

    /// Read the heap the spare holds
    pub fn spare_bytes(&self) -> ByteCount {
        list_bytes(&hold(&self.spare))
    }

    /// Check two directories build their changes in one spare
    pub fn shares_spare(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.spare, &other.spare)
    }

    /// Read the heap this directory's own list holds, its share of the spare aside
    pub fn heap_bytes(&self) -> ByteCount {
        list_bytes(&read_list(&self.list))
    }

    /// Fill a buffer with the segments whose range holds a key, past those a caller already has
    pub fn candidates(&self, key: &[u8], skip: usize, out: &mut Vec<Candidate>) -> DirectoryPage {
        out.clear();
        let copy = read_list(&self.list);
        let list = &copy.entries;
        let mut fill = PageFill {
            out,
            skip,
            seen: 0,
            is_cut: false,
        };

        let mut start = 0;
        while let Some(first) = list.get(start) {
            let shared = first.shared_length;
            let end = start + list[start..].partition_point(|entry| entry.shared_length == shared);
            if let Some(bytes) = key.get(..shared as usize) {
                let group = group_of(&list[start..end], checksum(bytes));
                search_group(group, KeyPrefix::of_key(key, shared as usize), &mut fill);
            }
            start = end;
        }

        DirectoryPage {
            generation: copy.generation,
            skipped_past: fill.seen,
            is_cut: fill.is_cut,
        }
    }

    /// Fill a buffer with the segments whose keys can meet a range, past those a caller already has
    pub fn overlapping(&self, low: &KeyBound, high: &KeyBound, skip: usize, out: &mut impl ListOut<SegmentNumber>) -> DirectoryPage {
        out.clear();
        let copy = read_list(&self.list);
        let mut fill = PageFill {
            out,
            skip,
            seen: 0,
            is_cut: false,
        };
        search_ranges(&copy.ranges, 0, copy.ranges.len(), low, high, &mut fill);
        DirectoryPage {
            generation: copy.generation,
            skipped_past: fill.seen,
            is_cut: fill.is_cut,
        }
    }

    /// Fill a buffer with the segments whose range starts inside a range, in key order, up to the buffer's room. Bounds can only
    /// widen a range, so a caller checks each one against its keys
    pub fn starting_inside(&self, low: &KeyBound, high: &KeyBound, out: &mut impl ListOut<SegmentNumber>) {
        out.clear();
        let copy = read_list(&self.list);
        let first = copy.ranges.partition_point(|range| range.lowest < *low);
        for range in copy.ranges[first..].iter().take_while(|range| range.lowest <= *high) {
            if !out.push_if_room(range.segment) {
                return;
            }
        }
    }

    /// Publish a list of every listing at once, sorted here, or return false when they don't fit. A listing at a time copies the
    /// whole list for each one, and one sort fills a directory of any size
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn publish_all(&self, listings: &[Listing]) -> bool {
        let mut target = hold(&self.spare);
        if listings.len() > target.entries.capacity() {
            return false;
        }
        target.entries.clear();
        target.ranges.clear();
        target.entries.extend(listings.iter().map(|listing| listing.entry));
        target.ranges.extend(listings.iter().map(|listing| listing.range));
        target.entries.sort_unstable_by_key(sort_key);
        target.ranges.sort_unstable_by_key(|range| range.lowest);
        refresh_groups(&mut target.entries);
        let count = target.ranges.len();
        refresh_range_reach(&mut target.ranges, 0, count);
        self.swap_in(&mut target);
        true
    }

    /// Put a list built in the spare in place, and leave the old one there as the next spare
    pub fn swap_in(&self, spare: &mut DirectoryList) {
        let mut list = write_list(&self.list);
        spare.generation = list.generation + 1;
        mem::swap(&mut *list, spare);
        drop(list);
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    /// Find where the next segment past a bound starts
    pub fn next_above(&self, bound: &KeyBound) -> Option<KeyBound> {
        let copy = read_list(&self.list);
        let at = copy.ranges.partition_point(|range| range.lowest <= *bound);
        copy.ranges.get(at).map(|range| range.lowest)
    }

    /// Find where the next segment below a bound ends
    pub fn next_below(&self, bound: &KeyBound) -> Option<KeyBound> {
        let copy = read_list(&self.list);
        let mut best = None;
        highest_below(&copy.ranges, 0, copy.ranges.len(), bound, &mut best);
        best
    }

    /// Read the generation of the newest list, so a walk can tell one was published under it
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Publish a list with a segment added and some gone, or return false when it doesn't fit
    pub fn replace(&self, added: Option<&Listing>, removed: &[SegmentNumber]) -> bool {
        let mut target = hold(&self.spare);
        target.entries.clear();
        target.ranges.clear();
        {
            // the spare's one writer is this one, so the list can't change under the copy
            let source = read_list(&self.list);
            let kept = source.entries.iter().filter(|entry| !removed.contains(&entry.segment)).count();
            if kept + usize::from(added.is_some()) > target.entries.capacity() {
                return false;
            }
            target.entries.extend(source.entries.iter().filter(|entry| !removed.contains(&entry.segment)));
            target.ranges.extend(source.ranges.iter().filter(|range| !removed.contains(&range.segment)));
        }
        if let Some(listing) = added {
            let entry = &listing.entry;
            let at = target.entries.partition_point(|existing| sort_key(existing) <= sort_key(entry));
            target.entries.insert(at, *entry);
            let at = target.ranges.partition_point(|existing| existing.lowest <= listing.range.lowest);
            target.ranges.insert(at, listing.range);
        }
        refresh_groups(&mut target.entries);
        let count = target.ranges.len();
        refresh_range_reach(&mut target.ranges, 0, count);
        self.swap_in(&mut target);
        true
    }
}

/// Read the heap a list holds
pub fn list_bytes(list: &DirectoryList) -> ByteCount {
    ByteCount((list.entries.capacity() * size_of::<DirectoryEntry>() + list.ranges.capacity() * size_of::<RangeEntry>()) as u64)
}

/// Build an empty copy with fixed room
pub fn empty_list(capacity: usize) -> DirectoryList {
    let blank_range = RangeEntry {
        lowest: KeyBound::of_key(&[]),
        highest: KeyBound::of_key(&[]),
        reach: 0,
        segment: SegmentNumber(0),
    };
    let blank = DirectoryEntry {
        shared_length: 0,
        shared_sum: Checksum(0),
        lowest: KeyPrefix::default(),
        highest: KeyPrefix::default(),
        reach: 0,
        segment: SegmentNumber(0),
        newest_place: WritePlace(0),
    };
    DirectoryList {
        entries: touched_capacity(capacity, blank),
        ranges: touched_capacity(capacity, blank_range),
        generation: 0,
    }
}

/// Order entries by group, then by lowest key
pub fn sort_key(entry: &DirectoryEntry) -> (u16, Checksum, KeyPrefix) {
    (entry.shared_length, entry.shared_sum, entry.lowest)
}

/// Find the run of a group whose shared bytes match a key's
pub fn group_of(length_run: &[DirectoryEntry], sum: Checksum) -> &[DirectoryEntry] {
    let start = length_run.partition_point(|entry| entry.shared_sum < sum);
    let end = length_run.partition_point(|entry| entry.shared_sum <= sum);
    &length_run[start..end]
}

/// Add every segment of a group whose range holds a key, skipping whole runs that can't
pub fn search_group(group: &[DirectoryEntry], key: KeyPrefix, fill: &mut PageFill<impl ListOut<Candidate>>) {
    // a full page ends the search, and the next page starts past what this one holds
    if group.is_empty() || fill.is_cut {
        return;
    }

    // each entry's reach points at the highest key in its half of the list, as in a balanced tree over the sorted run
    let middle = group.len() / 2;
    let entry = &group[middle];
    if group[middle.saturating_add_signed(entry.reach as isize)].highest < key {
        return;
    }

    search_group(&group[..middle], key, fill);
    if entry.lowest > key {
        return;
    }
    if entry.highest >= key {
        fill.add(Candidate {
            segment: entry.segment,
            newest_place: entry.newest_place,
        });
    }
    search_group(&group[middle + 1..], key, fill);
}

/// Add every segment whose keys can meet a range, skipping whole runs that can't. A segment comes out in order of its lowest key
pub fn search_ranges(ranges: &[RangeEntry], start: usize, end: usize, low: &KeyBound, high: &KeyBound, fill: &mut PageFill<impl ListOut<SegmentNumber>>) {
    // a full page ends the search, and the next page starts past what this one holds
    if start == end || fill.is_cut {
        return;
    }

    let middle = start + (end - start) / 2;
    let entry = &ranges[middle];
    if ranges[entry.reach as usize].highest < *low {
        return;
    }

    search_ranges(ranges, start, middle, low, high, fill);
    if entry.lowest > *high {
        return;
    }
    if entry.highest >= *low {
        fill.add(entry.segment);
    }
    search_ranges(ranges, middle + 1, end, low, high, fill);
}

/// Find the highest end below a bound, skipping every run that can't beat the best so far
pub fn highest_below(ranges: &[RangeEntry], start: usize, end: usize, bound: &KeyBound, best: &mut Option<KeyBound>) {
    if start == end {
        return;
    }

    let middle = start + (end - start) / 2;
    let entry = &ranges[middle];
    if best.is_some_and(|found| ranges[entry.reach as usize].highest <= found) {
        return;
    }

    highest_below(ranges, middle + 1, end, bound, best);
    if entry.highest < *bound && best.is_none_or(|found| entry.highest > found) {
        *best = Some(entry.highest);
    }
    highest_below(ranges, start, middle, bound, best);
}

/// Give each entry of a run the index of the highest end in its half, and return the run's
pub fn refresh_range_reach(ranges: &mut [RangeEntry], start: usize, end: usize) -> Option<u32> {
    if start == end {
        return None;
    }

    let middle = start + (end - start) / 2;
    let sides = [refresh_range_reach(ranges, start, middle), refresh_range_reach(ranges, middle + 1, end)];
    let mut reach = middle as u32;
    for side in sides.into_iter().flatten() {
        if ranges[side as usize].highest > ranges[reach as usize].highest {
            reach = side;
        }
    }
    ranges[middle].reach = reach;
    Some(reach)
}

/// Refresh the reach of every group in a list
pub fn refresh_groups(list: &mut [DirectoryEntry]) {
    let mut start = 0;
    while start < list.len() {
        let group = (list[start].shared_length, list[start].shared_sum);
        let run = list[start..].partition_point(|entry| (entry.shared_length, entry.shared_sum) == group);
        refresh_reach(&mut list[start..start + run]);
        start += run;
    }
}

/// Point each entry at the entry with the highest key in its half of the run, as an offset from itself, and return the
/// run's highest entry
pub fn refresh_reach(run: &mut [DirectoryEntry]) -> Option<usize> {
    if run.is_empty() {
        return None;
    }
    let middle = run.len() / 2;
    let left = refresh_reach(&mut run[..middle]);
    let right = refresh_reach(&mut run[middle + 1..]).map(|index| index + middle + 1);
    let mut best = middle;
    for side in [left, right].into_iter().flatten() {
        if run[side].highest > run[best].highest {
            best = side;
        }
    }
    run[middle].reach = (best as isize - middle as isize) as i32;
    Some(best)
}

/// Read one copy of the list
pub fn read_list(list: &RwLock<DirectoryList>) -> RwLockReadGuard<'_, DirectoryList> {
    match list.read() {
        Ok(guard) => guard,
        Err(poisoned) => crate::guard::recover(poisoned),
    }
}

/// Change one copy of the list
pub fn write_list(list: &RwLock<DirectoryList>) -> RwLockWriteGuard<'_, DirectoryList> {
    match list.write() {
        Ok(guard) => guard,
        Err(poisoned) => crate::guard::recover(poisoned),
    }
}
