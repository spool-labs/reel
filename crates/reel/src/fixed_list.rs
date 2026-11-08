//! Lists that never grow: a fixed job holds them on the heap from open, and a maintenance job carves them from the region
//! it maps at its start. A push past the capacity fails at the call

use std::alloc::{self, Layout};
use std::fmt;
use std::hint::black_box;
use std::ops::{Deref, DerefMut};
use std::ptr::{self, NonNull};
use std::slice;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::pool::TOUCH_BYTE;
use crate::region::{RegionHead, release_head};

/// A push, an extend or a resize past a list's capacity, which a list sized from its bound never meets
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ListFull;

impl fmt::Display for ListFull {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a fixed list is full")
    }
}

impl std::error::Error for ListFull {}

impl From<ListFull> for std::io::Error {
    fn from(_full: ListFull) -> Self {
        std::io::Error::from(std::io::ErrorKind::OutOfMemory)
    }
}

/// Pushes past a capacity in this process, each a bound that came out short
static LISTS_FULL: AtomicU64 = AtomicU64::new(0);

/// Count the pushes past a capacity in this process so far
pub fn lists_full() -> u64 {
    LISTS_FULL.load(Ordering::Relaxed)
}

/// Note a push past a capacity and return its error
pub fn full() -> ListFull {
    LISTS_FULL.fetch_add(1, Ordering::Relaxed);
    ListFull
}

/// A list a shared reader fills: a job's fixed list, or a heap list its caller sized
pub trait ListOut<Item: Copy>: DerefMut<Target = [Item]> {
    /// Empty the list
    fn clear(&mut self);

    /// Keep the first items and drop the rest
    fn truncate(&mut self, length: usize);

    /// Add an item at the end
    fn push(&mut self, item: Item) -> Result<(), ListFull>;

    /// Add every item of a slice at the end
    fn extend_from_slice(&mut self, items: &[Item]) -> Result<(), ListFull>;

    /// Add an item when the room it has already holds it, and say whether it went in
    fn push_if_room(&mut self, item: Item) -> bool;
}

impl<Item: Copy> ListOut<Item> for Vec<Item> {
    fn clear(&mut self) {
        Vec::clear(self);
    }

    fn truncate(&mut self, length: usize) {
        Vec::truncate(self, length);
    }

    fn push(&mut self, item: Item) -> Result<(), ListFull> {
        Vec::push(self, item);
        Ok(())
    }

    fn extend_from_slice(&mut self, items: &[Item]) -> Result<(), ListFull> {
        Vec::extend_from_slice(self, items);
        Ok(())
    }

    fn push_if_room(&mut self, item: Item) -> bool {
        let has_room = self.len() < self.capacity();
        if has_room {
            Vec::push(self, item);
        }
        has_room
    }
}

impl<Item: Copy> ListOut<Item> for FixedList<Item> {
    fn clear(&mut self) {
        FixedList::clear(self);
    }

    fn truncate(&mut self, length: usize) {
        FixedList::truncate(self, length);
    }

    fn push(&mut self, item: Item) -> Result<(), ListFull> {
        FixedList::push(self, item)
    }

    fn extend_from_slice(&mut self, items: &[Item]) -> Result<(), ListFull> {
        FixedList::extend_from_slice(self, items)
    }

    fn push_if_room(&mut self, item: Item) -> bool {
        FixedList::push_if_room(self, item)
    }
}

/// Where a list's items live, which decides what its drop gives back
#[derive(Debug)]
enum ListOwner {
    /// No memory: a list of no capacity
    Nothing,
    /// The global heap, allocated at open with this layout
    Heap(Layout),
    /// A job's region, which unmaps once its last list and its handle are gone
    Region(NonNull<RegionHead>),
}

/// A list of plain items with a capacity set when it's made
pub struct FixedList<Item: Copy> {
    /// The first item's place
    start: NonNull<Item>,
    /// The items in use
    length: usize,
    /// The items the memory has room for
    capacity: usize,
    /// The most items the list has held, for the touched high water
    peak: usize,
    /// What the memory belongs to
    owner: ListOwner,
}

// SAFETY: a list owns its items' memory alone, a region's lists included, and the region's count is atomic
unsafe impl<Item: Copy + Send> Send for FixedList<Item> {}
// SAFETY: shared access only reads the items
unsafe impl<Item: Copy + Sync> Sync for FixedList<Item> {}

impl<Item: Copy> FixedList<Item> {
    /// Make a list of no capacity, which holds no memory
    pub const fn empty() -> Self {
        Self { start: NonNull::dangling(), length: 0, capacity: 0, peak: 0, owner: ListOwner::Nothing }
    }

    /// Allocate an empty list on the heap with every page of its room written, for a job that keeps it from open
    pub fn on_heap(capacity: usize) -> Self {
        let Ok(layout) = Layout::array::<Item>(capacity) else {
            alloc::handle_alloc_error(Layout::new::<Item>());
        };
        if layout.size() == 0 {
            return Self { start: NonNull::dangling(), length: 0, capacity, peak: 0, owner: ListOwner::Nothing };
        }
        // SAFETY: the layout has a size
        let memory = unsafe { alloc::alloc(layout) };
        let Some(start) = NonNull::new(memory.cast::<Item>()) else {
            alloc::handle_alloc_error(layout);
        };
        // SAFETY: the memory is ours and this long; the items are plain, so any bytes do before a push writes them
        unsafe { ptr::write_bytes(memory, TOUCH_BYTE, layout.size()) };
        black_box(memory);
        Self { start, length: 0, capacity, peak: 0, owner: ListOwner::Heap(layout) }
    }

    /// Allocate a list on the heap with every place holding one value
    pub fn filled_on_heap(length: usize, value: Item) -> Self {
        let mut list = Self::on_heap(length);
        list.fill_to(length, value);
        list
    }

    /// Point a list at a region's memory and take a hold on the region
    ///
    /// # Safety
    /// `start` must be aligned and have room for `capacity` items inside the region `head` heads, carved for this list alone,
    /// and the caller must have taken a hold on the region for it
    pub unsafe fn in_region(start: NonNull<Item>, capacity: usize, head: NonNull<RegionHead>) -> Self {
        Self { start, length: 0, capacity, peak: 0, owner: ListOwner::Region(head) }
    }

    /// Read the items as a slice
    pub fn as_slice(&self) -> &[Item] {
        self
    }

    /// Read the items as a slice to change
    pub fn as_mut_slice(&mut self) -> &mut [Item] {
        self
    }

    /// Count the items in use
    pub fn len(&self) -> usize {
        self.length
    }

    /// Check the list holds nothing
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Count the items it has room for
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Count the items left before it's full
    pub fn room(&self) -> usize {
        self.capacity - self.length
    }

    /// Read the bytes the most items it has held take, its touched high water
    pub fn peak_bytes(&self) -> usize {
        self.peak.max(self.length) * size_of::<Item>()
    }

    /// Read the bytes its room takes
    pub fn held_bytes(&self) -> usize {
        self.capacity * size_of::<Item>()
    }

    /// Empty the list, keeping its room
    pub fn clear(&mut self) {
        self.note_peak();
        self.length = 0;
    }

    /// Keep the first items and drop the rest
    pub fn truncate(&mut self, length: usize) {
        self.note_peak();
        self.length = self.length.min(length);
    }

    /// Add an item at the end
    pub fn push(&mut self, item: Item) -> Result<(), ListFull> {
        if self.length == self.capacity {
            return Err(full());
        }
        // SAFETY: the place is inside the room
        unsafe { self.start.as_ptr().add(self.length).write(item) };
        self.length += 1;
        Ok(())
    }

    /// Add an item at the end when there's room, for a list whose full state is a handled case, and say whether it went in
    pub fn push_if_room(&mut self, item: Item) -> bool {
        if self.length == self.capacity {
            return false;
        }
        // SAFETY: the place is inside the room
        unsafe { self.start.as_ptr().add(self.length).write(item) };
        self.length += 1;
        true
    }

    /// Add every item of a slice at the end, or nothing when they don't all fit
    pub fn extend_from_slice(&mut self, items: &[Item]) -> Result<(), ListFull> {
        if items.len() > self.room() {
            return Err(full());
        }
        // SAFETY: the places are inside the room, and a slice can't overlap the room past the length
        unsafe { ptr::copy_nonoverlapping(items.as_ptr(), self.start.as_ptr().add(self.length), items.len()) };
        self.length += items.len();
        Ok(())
    }

    /// Set the list to this many items, writing the value to each new place
    pub fn resize(&mut self, length: usize, value: Item) -> Result<(), ListFull> {
        if length > self.capacity {
            return Err(full());
        }
        if length <= self.length {
            self.truncate(length);
        } else {
            self.fill_to(length, value);
        }
        Ok(())
    }

    /// Replace the items with a slice's, or keep them when the slice doesn't fit
    pub fn set_from(&mut self, items: &[Item]) -> Result<(), ListFull> {
        if items.len() > self.capacity {
            return Err(full());
        }
        self.clear();
        self.extend_from_slice(items)
    }

    /// Take the last item off
    pub fn pop(&mut self) -> Option<Item> {
        self.note_peak();
        let length = self.length.checked_sub(1)?;
        self.length = length;
        // SAFETY: the place held an item
        Some(unsafe { self.start.as_ptr().add(length).read() })
    }

    /// Keep the items a test passes, in their order
    pub fn retain(&mut self, mut keep: impl FnMut(&Item) -> bool) {
        self.note_peak();
        let mut kept = 0;
        for index in 0..self.length {
            // SAFETY: both places hold items, and the kept one is never past the one read
            unsafe {
                let item = self.start.as_ptr().add(index).read();
                if keep(&item) {
                    self.start.as_ptr().add(kept).write(item);
                    kept += 1;
                }
            }
        }
        self.length = kept;
    }

    /// Remove the item at an index, moving the last into its place
    pub fn swap_remove(&mut self, index: usize) -> Option<Item> {
        if index >= self.length {
            return None;
        }
        let item = self[index];
        let last = self.length - 1;
        self.swap(index, last);
        self.truncate(last);
        Some(item)
    }

    /// Insert an item at an index, moving the later ones up
    pub fn insert(&mut self, index: usize, item: Item) -> Result<(), ListFull> {
        if self.length == self.capacity {
            return Err(full());
        }
        let index = index.min(self.length);
        // SAFETY: the move stays inside the room, which has a place past the length
        unsafe {
            let place = self.start.as_ptr().add(index);
            ptr::copy(place, place.add(1), self.length - index);
            place.write(item);
        }
        self.length += 1;
        Ok(())
    }

    /// Remove the item at an index, moving the later ones down
    pub fn remove(&mut self, index: usize) -> Option<Item> {
        if index >= self.length {
            return None;
        }
        self.note_peak();
        // SAFETY: the item and the ones past it are in use
        unsafe {
            let place = self.start.as_ptr().add(index);
            let item = place.read();
            ptr::copy(place.add(1), place, self.length - index - 1);
            self.length -= 1;
            Some(item)
        }
    }

    /// Note the length as the most items held when it's past the last mark
    fn note_peak(&mut self) {
        self.peak = self.peak.max(self.length);
    }

    /// Write the value from the length up to a new length inside the room
    fn fill_to(&mut self, length: usize, value: Item) {
        for index in self.length..length.min(self.capacity) {
            // SAFETY: the place is inside the room
            unsafe { self.start.as_ptr().add(index).write(value) };
        }
        self.length = length.min(self.capacity);
    }
}

impl<Item: Copy> Drop for FixedList<Item> {
    fn drop(&mut self) {
        match self.owner {
            ListOwner::Nothing => {},
            // SAFETY: the heap gave this memory with this layout
            ListOwner::Heap(layout) => unsafe { alloc::dealloc(self.start.as_ptr().cast(), layout) },
            // SAFETY: the list took a hold when it was carved and gives it back once
            ListOwner::Region(head) => unsafe { release_head(head) },
        }
    }
}

impl<Item: Copy> Default for FixedList<Item> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<Item: Copy> Deref for FixedList<Item> {
    type Target = [Item];

    fn deref(&self) -> &[Item] {
        // SAFETY: the items up to the length are written, and the memory lives as long as the list
        unsafe { slice::from_raw_parts(self.start.as_ptr(), self.length) }
    }
}

impl<Item: Copy> DerefMut for FixedList<Item> {
    fn deref_mut(&mut self) -> &mut [Item] {
        // SAFETY: as for deref, and the list is borrowed alone
        unsafe { slice::from_raw_parts_mut(self.start.as_ptr(), self.length) }
    }
}

impl<'list, Item: Copy> IntoIterator for &'list FixedList<Item> {
    type Item = &'list Item;
    type IntoIter = slice::Iter<'list, Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'list, Item: Copy> IntoIterator for &'list mut FixedList<Item> {
    type Item = &'list mut Item;
    type IntoIter = slice::IterMut<'list, Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl<Item: Copy + fmt::Debug> fmt::Debug for FixedList<Item> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_list().entries(self.iter()).finish()
    }
}

impl<Item: Copy + PartialEq> PartialEq for FixedList<Item> {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl<Item: Copy + Eq> Eq for FixedList<Item> {}

impl<Item: Copy + PartialOrd> PartialOrd for FixedList<Item> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (**self).partial_cmp(&**other)
    }
}

impl<Item: Copy + Ord> Ord for FixedList<Item> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (**self).cmp(&**other)
    }
}

impl<Item: Copy + PartialEq> PartialEq<[Item]> for FixedList<Item> {
    fn eq(&self, other: &[Item]) -> bool {
        **self == *other
    }
}

impl<Item: Copy + PartialEq> PartialEq<&[Item]> for FixedList<Item> {
    fn eq(&self, other: &&[Item]) -> bool {
        **self == **other
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_fails_past_capacity() {
        let mut list = FixedList::<u32>::on_heap(2);
        assert_eq!(list.push(1), Ok(()));
        assert_eq!(list.push(2), Ok(()));
        assert_eq!(list.push(3), Err(ListFull));
        assert_eq!(&*list, &[1, 2]);
        assert_eq!(list.extend_from_slice(&[4]), Err(ListFull));
        list.clear();
        assert_eq!(list.extend_from_slice(&[5, 6]), Ok(()));
        assert_eq!(list.peak_bytes(), 8);
    }

    #[test]
    fn edits_keep_order() {
        let mut list = FixedList::<u8>::on_heap(8);
        assert_eq!(list.extend_from_slice(&[1, 2, 3, 4, 5]), Ok(()));
        list.retain(|item| item % 2 == 1);
        assert_eq!(&*list, &[1, 3, 5]);
        assert_eq!(list.insert(1, 9), Ok(()));
        assert_eq!(&*list, &[1, 9, 3, 5]);
        assert_eq!(list.remove(0), Some(1));
        assert_eq!(list.swap_remove(0), Some(9));
        assert_eq!(&*list, &[5, 3]);
        assert_eq!(list.resize(4, 7), Ok(()));
        assert_eq!(&*list, &[5, 3, 7, 7]);
        assert_eq!(list.resize(9, 0), Err(ListFull));
        assert_eq!(list.pop(), Some(7));
        assert_eq!(FixedList::filled_on_heap(3, 0u64).len(), 3);
    }
}
