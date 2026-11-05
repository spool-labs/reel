//! Which sealed segments can hold a key of one column, published with no lock on readers' writers

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use reel_api::units::ByteCount;

use crate::guard::hold;
use crate::record::record_check::{Checksum, checksum};
use crate::segment::block_entry::KeyPrefix;
use crate::segment::segment_tail::SegmentNumber;

/// Copies of the list: readers keep one while a seal builds the other
pub const DIRECTORY_VERSIONS: usize = 2;

/// One segment's range in one column
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DirectoryEntry {
    pub shared_length: u16,
    pub shared_sum: Checksum,
    pub lowest: KeyPrefix,
    pub highest: KeyPrefix,
    pub reach: KeyPrefix,
    pub segment: SegmentNumber,
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
pub struct PageFill<'out> {
    out: &'out mut Vec<SegmentNumber>,
    skip: usize,
    seen: usize,
    is_cut: bool,
}

/// A column's segments, grouped by the bytes each one's keys share and sorted by lowest key in
/// a group, in two copies with room fixed at open
#[derive(Debug)]
pub struct ColumnDirectory {
    versions: [RwLock<Vec<DirectoryEntry>>; DIRECTORY_VERSIONS],
    current: AtomicUsize,
    generation: AtomicU64,
    writer: Mutex<()>,
}

impl PageFill<'_> {
    /// Add a matching segment, or note that the page is full
    pub fn add(&mut self, segment: SegmentNumber) {
        self.seen += 1;
        if self.seen <= self.skip {
            return;
        }
        if self.out.len() < self.out.capacity() {
            self.out.push(segment);
        } else {
            self.is_cut = true;
        }
    }
}

impl ColumnDirectory {
    /// Allocate room for a number of segments
    pub fn new(capacity: usize) -> Self {
        Self {
            versions: [RwLock::new(Vec::with_capacity(capacity)), RwLock::new(Vec::with_capacity(capacity))],
            current: AtomicUsize::new(0),
            generation: AtomicU64::new(0),
            writer: Mutex::new(()),
        }
    }

    /// Read the heap this directory holds
    pub fn heap_bytes(&self) -> ByteCount {
        let mut total = 0;
        for version in &self.versions {
            total += read_list(version).capacity() * size_of::<DirectoryEntry>();
        }
        ByteCount(total as u64)
    }

    /// Fill a buffer with the segments whose range holds a key, past those a caller already has
    pub fn candidates(&self, key: &[u8], skip: usize, out: &mut Vec<SegmentNumber>) -> DirectoryPage {
        out.clear();
        let current = self.current.load(Ordering::Acquire);
        let list = read_list(&self.versions[current]);
        let generation = self.generation.load(Ordering::Acquire);
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
            generation,
            skipped_past: fill.seen,
            is_cut: fill.is_cut,
        }
    }

    /// Publish a list with a segment added and some gone, or return false when it doesn't fit
    pub fn replace(&self, added: Option<&DirectoryEntry>, removed: &[SegmentNumber]) -> bool {
        let _one_writer = hold(&self.writer);
        let current = self.current.load(Ordering::Acquire);
        let next = (current + 1) % DIRECTORY_VERSIONS;
        {
            let source = read_list(&self.versions[current]);
            let mut target = write_list(&self.versions[next]);
            target.clear();

            let room = target.capacity();
            let mut kept = 0;
            for entry in source.iter() {
                if !removed.contains(&entry.segment) {
                    kept += 1;
                }
            }
            if kept + usize::from(added.is_some()) > room {
                return false;
            }

            for entry in source.iter() {
                if !removed.contains(&entry.segment) {
                    target.push(*entry);
                }
            }
            if let Some(entry) = added {
                let at = target.partition_point(|existing| sort_key(existing) <= sort_key(entry));
                target.insert(at, *entry);
            }
            refresh_groups(&mut target);
        }
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.current.store(next, Ordering::Release);
        true
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
pub fn search_group(group: &[DirectoryEntry], key: KeyPrefix, fill: &mut PageFill) {
    if group.is_empty() {
        return;
    }

    // each entry's reach covers its half of the list, as in a balanced tree over the sorted run
    let middle = group.len() / 2;
    let entry = &group[middle];
    if entry.reach < key {
        return;
    }

    search_group(&group[..middle], key, fill);
    if entry.lowest > key {
        return;
    }
    if entry.highest >= key {
        fill.add(entry.segment);
    }
    search_group(&group[middle + 1..], key, fill);
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

/// Give each entry the highest key in its half of the run, and return the run's highest
pub fn refresh_reach(run: &mut [DirectoryEntry]) -> KeyPrefix {
    if run.is_empty() {
        return KeyPrefix::default();
    }

    let middle = run.len() / 2;
    let (left, rest) = run.split_at_mut(middle);
    let Some((entry, right)) = rest.split_first_mut() else {
        return KeyPrefix::default();
    };
    let reach = entry.highest.max(refresh_reach(left)).max(refresh_reach(right));
    entry.reach = reach;
    reach
}

/// Read one copy of the list
pub fn read_list(list: &RwLock<Vec<DirectoryEntry>>) -> RwLockReadGuard<'_, Vec<DirectoryEntry>> {
    match list.read() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Change one copy of the list
pub fn write_list(list: &RwLock<Vec<DirectoryEntry>>) -> RwLockWriteGuard<'_, Vec<DirectoryEntry>> {
    match list.write() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::footer_writer::shared_prefix;

    const CAPACITY: usize = 64;
    const MANY: u64 = 1_000;

    fn entry(number: u64, lowest: &[u8], highest: &[u8]) -> DirectoryEntry {
        let shared = shared_prefix(lowest, highest);
        DirectoryEntry {
            shared_length: shared as u16,
            shared_sum: checksum(&lowest[..shared]),
            lowest: KeyPrefix::of_key(lowest, shared),
            highest: KeyPrefix::of_key(highest, shared),
            reach: KeyPrefix::default(),
            segment: SegmentNumber(number),
        }
    }

    fn found(directory: &ColumnDirectory, key: &[u8]) -> Vec<u64> {
        let mut out = Vec::with_capacity(CAPACITY);
        let page = directory.candidates(key, 0, &mut out);
        assert!(!page.is_cut);
        let mut numbers = Vec::new();
        for segment in out {
            numbers.push(segment.0);
        }
        numbers.sort_unstable();
        numbers
    }

    // ordered ranges give one segment, overlaps give each of them, and a gap gives none
    #[test]
    fn range_search() {
        let directory = ColumnDirectory::new(CAPACITY);
        assert!(directory.replace(Some(&entry(1, b"a", b"c")), &[]));
        assert!(directory.replace(Some(&entry(2, b"d", b"f")), &[]));
        assert!(directory.replace(Some(&entry(3, b"b", b"e")), &[]));
        assert!(directory.replace(Some(&entry(4, b"m", b"p")), &[]));

        assert_eq!(found(&directory, b"a"), vec![1]);
        assert_eq!(found(&directory, b"c"), vec![1, 3]);
        assert_eq!(found(&directory, b"e"), vec![2, 3]);
        assert_eq!(found(&directory, b"g"), Vec::<u64>::new());
        assert_eq!(found(&directory, b"n"), vec![4]);
    }

    // a wide old segment stays in reach of keys past the narrow ones after it
    #[test]
    fn wide_segment() {
        let directory = ColumnDirectory::new(CAPACITY);
        assert!(directory.replace(Some(&entry(1, b"a", b"z")), &[]));
        assert!(directory.replace(Some(&entry(2, b"b", b"c")), &[]));
        assert!(directory.replace(Some(&entry(3, b"d", b"e")), &[]));

        assert_eq!(found(&directory, b"m"), vec![1]);
    }

    // keys sharing a long prefix still rule out the segments of other ranges
    #[test]
    fn long_prefix() {
        let directory = ColumnDirectory::new(CAPACITY);
        assert!(directory.replace(Some(&entry(1, b"address-of-one-tape-0001", b"address-of-one-tape-0100")), &[]));
        assert!(directory.replace(Some(&entry(2, b"address-of-one-tape-0101", b"address-of-one-tape-0200")), &[]));
        assert!(directory.replace(Some(&entry(3, b"address-of-one-tape-0201", b"address-of-one-tape-0300")), &[]));

        assert_eq!(found(&directory, b"address-of-one-tape-0150"), vec![2]);
        assert_eq!(found(&directory, b"address-of-one-tape-0250"), vec![3]);
        assert_eq!(found(&directory, b"address-of-another-0150"), Vec::<u64>::new());
    }

    // a merge swaps its inputs for its output in one publish
    #[test]
    fn merge_swap() {
        let directory = ColumnDirectory::new(CAPACITY);
        assert!(directory.replace(Some(&entry(1, b"a", b"c")), &[]));
        assert!(directory.replace(Some(&entry(2, b"b", b"d")), &[]));

        assert!(directory.replace(Some(&entry(3, b"a", b"d")), &[SegmentNumber(1), SegmentNumber(2)]));

        assert_eq!(found(&directory, b"b"), vec![3]);
    }

    // a small buffer gets every overlapping segment page by page
    #[test]
    fn cut_pages() {
        let directory = ColumnDirectory::new(CAPACITY);
        for number in 0..10 {
            assert!(directory.replace(Some(&entry(number, b"a", b"z")), &[]));
        }

        let mut out = Vec::with_capacity(4);
        let mut numbers = Vec::new();
        let mut skip = 0;
        loop {
            let page = directory.candidates(b"m", skip, &mut out);
            for segment in &out {
                numbers.push(segment.0);
            }
            skip += out.len();
            if !page.is_cut {
                break;
            }
        }

        numbers.sort_unstable();
        assert_eq!(numbers, (0..10).collect::<Vec<u64>>());
    }

    // a wide segment among many narrow ones finds just the two that hold a key
    #[test]
    fn wide_among_many() {
        let directory = ColumnDirectory::new(MANY as usize + 1);
        assert!(directory.replace(Some(&entry(MANY, b"a0000", b"z9999")), &[]));
        for number in 0..MANY {
            let lowest = format!("k{number:04}0");
            let highest = format!("k{number:04}9");
            assert!(directory.replace(Some(&entry(number, lowest.as_bytes(), highest.as_bytes())), &[]));
        }

        let key = b"k05005";
        let narrow = found(&directory, key);

        assert_eq!(narrow, vec![500, MANY]);
    }

    // a full directory refuses a segment and keeps its list
    #[test]
    fn full_directory() {
        let directory = ColumnDirectory::new(1);

        assert!(directory.replace(Some(&entry(1, b"a", b"c")), &[]));
        assert!(!directory.replace(Some(&entry(2, b"d", b"f")), &[]));

        assert_eq!(found(&directory, b"b"), vec![1]);
    }
}
