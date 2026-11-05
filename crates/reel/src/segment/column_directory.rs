//! Which sealed segments can hold a key of one column, published with no lock on writers

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use reel_api::units::ByteCount;

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

/// A column's segments, grouped by the bytes each one's keys share and sorted by lowest key in
/// a group, in two copies with room fixed at open
#[derive(Debug)]
pub struct ColumnDirectory {
    versions: [RwLock<Vec<DirectoryEntry>>; DIRECTORY_VERSIONS],
    current: AtomicUsize,
}

impl ColumnDirectory {
    /// Allocate room for a number of segments
    pub fn new(capacity: usize) -> Self {
        Self {
            versions: [RwLock::new(Vec::with_capacity(capacity)), RwLock::new(Vec::with_capacity(capacity))],
            current: AtomicUsize::new(0),
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

    /// Fill a buffer with the segments whose range holds a key, up to its capacity
    pub fn candidates(&self, key: &[u8], out: &mut Vec<SegmentNumber>) {
        out.clear();
        let current = self.current.load(Ordering::Acquire);
        let list = read_list(&self.versions[current]);

        let mut start = 0;
        while let Some(first) = list.get(start) {
            let shared = first.shared_length;
            let end = start + list[start..].partition_point(|entry| entry.shared_length == shared);
            if let Some(bytes) = key.get(..shared as usize) {
                let group = group_of(&list[start..end], checksum(bytes));
                search_group(group, KeyPrefix::of_key(key, shared as usize), out);
            }
            start = end;
        }
    }

    /// Publish a list with some segments added and some gone
    pub fn replace(&self, added: Option<&DirectoryEntry>, removed: &[SegmentNumber]) {
        let current = self.current.load(Ordering::Acquire);
        let next = (current + 1) % DIRECTORY_VERSIONS;
        {
            let source = read_list(&self.versions[current]);
            let mut target = write_list(&self.versions[next]);
            target.clear();
            for entry in source.iter() {
                if !removed.contains(&entry.segment) {
                    target.push(*entry);
                }
            }
            if let Some(entry) = added {
                let at = target.partition_point(|existing| sort_key(existing) <= sort_key(entry));
                target.insert(at, *entry);
            }
            refresh_reach(&mut target);
        }
        self.current.store(next, Ordering::Release);
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

/// Add the segments of one group whose range holds a key, up to the buffer's capacity
pub fn search_group(group: &[DirectoryEntry], key: KeyPrefix, out: &mut Vec<SegmentNumber>) {
    let end = group.partition_point(|entry| entry.lowest <= key);
    for entry in group[..end].iter().rev() {
        if entry.reach < key {
            break;
        }
        if entry.highest >= key && out.len() < out.capacity() {
            out.push(entry.segment);
        }
    }
}

/// Carry the highest key seen so far along each group, so a search knows when to stop
pub fn refresh_reach(list: &mut [DirectoryEntry]) {
    let mut reach = KeyPrefix::default();
    let mut group = None;
    for entry in list.iter_mut() {
        let this_group = Some((entry.shared_length, entry.shared_sum));
        if group != this_group {
            group = this_group;
            reach = KeyPrefix::default();
        }
        reach = reach.max(entry.highest);
        entry.reach = reach;
    }
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

    const CAPACITY: usize = 16;

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
        directory.candidates(key, &mut out);
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
        directory.replace(Some(&entry(1, b"a", b"c")), &[]);
        directory.replace(Some(&entry(2, b"d", b"f")), &[]);
        directory.replace(Some(&entry(3, b"b", b"e")), &[]);
        directory.replace(Some(&entry(4, b"m", b"p")), &[]);

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
        directory.replace(Some(&entry(1, b"a", b"z")), &[]);
        directory.replace(Some(&entry(2, b"b", b"c")), &[]);
        directory.replace(Some(&entry(3, b"d", b"e")), &[]);

        assert_eq!(found(&directory, b"m"), vec![1]);
    }

    // keys sharing a long prefix still rule out the segments of other ranges
    #[test]
    fn long_prefix() {
        let directory = ColumnDirectory::new(CAPACITY);
        directory.replace(Some(&entry(1, b"address-of-one-tape-0001", b"address-of-one-tape-0100")), &[]);
        directory.replace(Some(&entry(2, b"address-of-one-tape-0101", b"address-of-one-tape-0200")), &[]);
        directory.replace(Some(&entry(3, b"address-of-one-tape-0201", b"address-of-one-tape-0300")), &[]);

        assert_eq!(found(&directory, b"address-of-one-tape-0150"), vec![2]);
        assert_eq!(found(&directory, b"address-of-one-tape-0250"), vec![3]);
        assert_eq!(found(&directory, b"address-of-another-0150"), Vec::<u64>::new());
    }

    // a merge swaps its inputs for its output in one publish
    #[test]
    fn merge_swap() {
        let directory = ColumnDirectory::new(CAPACITY);
        directory.replace(Some(&entry(1, b"a", b"c")), &[]);
        directory.replace(Some(&entry(2, b"b", b"d")), &[]);

        directory.replace(Some(&entry(3, b"a", b"d")), &[SegmentNumber(1), SegmentNumber(2)]);

        assert_eq!(found(&directory, b"b"), vec![3]);
    }
}
