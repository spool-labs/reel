//! Which sealed segments can hold a key of one column, published with no lock on writers

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use reel_api::units::ByteCount;

use crate::segment::block_entry::KeyPrefix;
use crate::segment::segment_tail::SegmentNumber;

/// Copies of the list: readers keep one while a seal builds the other
pub const DIRECTORY_VERSIONS: usize = 2;

/// One segment's range in one column
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DirectoryEntry {
    pub lowest: KeyPrefix,
    pub highest: KeyPrefix,
    pub reach: KeyPrefix,
    pub segment: SegmentNumber,
}

/// A column's segments sorted by lowest key, in two copies with room fixed at open
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
    pub fn candidates(&self, key: KeyPrefix, out: &mut Vec<SegmentNumber>) {
        out.clear();
        let current = self.current.load(Ordering::Acquire);
        let list = read_list(&self.versions[current]);
        let end = list.partition_point(|entry| entry.lowest <= key);
        for entry in list[..end].iter().rev() {
            if entry.reach < key {
                break;
            }
            if entry.highest >= key && out.len() < out.capacity() {
                out.push(entry.segment);
            }
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
                let at = target.partition_point(|existing| existing.lowest <= entry.lowest);
                target.insert(at, *entry);
            }
            refresh_reach(&mut target);
        }
        self.current.store(next, Ordering::Release);
    }
}

/// Carry the highest key seen so far along the list, so a search knows when to stop
pub fn refresh_reach(list: &mut [DirectoryEntry]) {
    let mut reach = KeyPrefix::default();
    for entry in list.iter_mut() {
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

    const CAPACITY: usize = 16;

    fn prefix(key: &[u8]) -> KeyPrefix {
        KeyPrefix::of_key(key, 0)
    }

    fn entry(number: u64, lowest: &[u8], highest: &[u8]) -> DirectoryEntry {
        DirectoryEntry {
            lowest: prefix(lowest),
            highest: prefix(highest),
            reach: KeyPrefix::default(),
            segment: SegmentNumber(number),
        }
    }

    fn found(directory: &ColumnDirectory, key: &[u8]) -> Vec<u64> {
        let mut out = Vec::with_capacity(CAPACITY);
        directory.candidates(prefix(key), &mut out);
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
