//! Which sealed segments can hold a key of one column, published with no lock on readers' writers

use std::sync::atomic::{AtomicUsize, Ordering};
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

/// One copy of the list, with the generation it was published under
#[derive(Clone, Debug, Default)]
pub struct DirectoryList {
    pub entries: Vec<DirectoryEntry>,
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
    versions: [RwLock<DirectoryList>; DIRECTORY_VERSIONS],
    current: AtomicUsize,
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
            versions: [RwLock::new(empty_list(capacity)), RwLock::new(empty_list(capacity))],
            current: AtomicUsize::new(0),
            writer: Mutex::new(()),
        }
    }

    /// Read the heap this directory holds
    pub fn heap_bytes(&self) -> ByteCount {
        let mut total = 0;
        for version in &self.versions {
            total += read_list(version).entries.capacity() * size_of::<DirectoryEntry>();
        }
        ByteCount(total as u64)
    }

    /// Fill a buffer with the segments whose range holds a key, past those a caller already has
    pub fn candidates(&self, key: &[u8], skip: usize, out: &mut Vec<SegmentNumber>) -> DirectoryPage {
        out.clear();
        let current = self.current.load(Ordering::Acquire);
        let copy = read_list(&self.versions[current]);
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

    /// Publish a list with a segment added and some gone, or return false when it doesn't fit
    pub fn replace(&self, added: Option<&DirectoryEntry>, removed: &[SegmentNumber]) -> bool {
        let _one_writer = hold(&self.writer);
        let current = self.current.load(Ordering::Acquire);
        let next = (current + 1) % DIRECTORY_VERSIONS;
        {
            let source = read_list(&self.versions[current]);
            let mut target = write_list(&self.versions[next]);
            target.entries.clear();
            target.generation = source.generation + 1;

            let room = target.entries.capacity();
            let mut kept = 0;
            for entry in &source.entries {
                if !removed.contains(&entry.segment) {
                    kept += 1;
                }
            }
            if kept + usize::from(added.is_some()) > room {
                return false;
            }

            for entry in &source.entries {
                if !removed.contains(&entry.segment) {
                    target.entries.push(*entry);
                }
            }
            if let Some(entry) = added {
                let at = target.entries.partition_point(|existing| sort_key(existing) <= sort_key(entry));
                target.entries.insert(at, *entry);
            }
            refresh_groups(&mut target.entries);
        }
        self.current.store(next, Ordering::Release);
        true
    }
}

/// Build an empty copy with fixed room
pub fn empty_list(capacity: usize) -> DirectoryList {
    DirectoryList {
        entries: Vec::with_capacity(capacity),
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
pub fn read_list(list: &RwLock<DirectoryList>) -> RwLockReadGuard<'_, DirectoryList> {
    match list.read() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Change one copy of the list
pub fn write_list(list: &RwLock<DirectoryList>) -> RwLockWriteGuard<'_, DirectoryList> {
    match list.write() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
