//! A list whose room for its largest length is reserved at open and comes into memory as it grows
//!
//! A list sized for every segment a disk could hold would write all its pages at open, though a store holds few segments for a
//! long while. This one maps its room as address space the kernel doesn't charge, as the pools do, and brings pages in a step
//! ahead of its length on the thread that grows it, a seal or a merge, so a reader never faults on it and nothing allocates once
//! it is open. Pages a shrunk list no longer reaches go back to the kernel on the same thread. A push takes only room that is in
//! memory, so no caller ever writes into a page that isn't

use std::io::{Error, Result};
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;

use crate::pool_room::{give_pages_back, map_room, populate, take_pages_back, unmap_room};
use crate::region::page_bytes;

/// Pages a list brings in past its length at once, so a run of pushes populates once
const READY_AHEAD_PAGES: usize = 16;

#[cfg(feature = "test-hooks")]
thread_local! {
    /// Makes every populate on this thread fail, so a simulation, whose jobs run on its own thread, sees what a store does when
    /// memory runs out under a seal while other tests run beside it
    pub static FAIL_POPULATE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// A list of plain items in its own mapping: room for `capacity` items, `ready` bytes of it in memory, `length` items written
#[derive(Debug)]
pub struct GrowingList<Item: Copy> {
    base: NonNull<Item>,
    capacity: usize,
    length: usize,
    mapped: usize,
    ready: usize,
}

// SAFETY: the list owns its mapping alone, and its items are plain values, so it moves and shares as a slice of them would
unsafe impl<Item: Copy + Send> Send for GrowingList<Item> {}
// SAFETY: a shared list hands out only shared slices of its items
unsafe impl<Item: Copy + Sync> Sync for GrowingList<Item> {}

impl<Item: Copy> GrowingList<Item> {
    /// Reserve room for this many items as address space, none of it in memory yet
    pub fn reserved(capacity: usize) -> Result<Self> {
        let bytes = (capacity * size_of::<Item>()).next_multiple_of(page_bytes());
        let base = if bytes == 0 { NonNull::dangling() } else { map_room(bytes)?.cast::<Item>() };
        Ok(Self { base, capacity, length: 0, mapped: bytes, ready: 0 })
    }

    /// Bring in the pages this many items take and a step past them, up to the room. A list that has its pages already does nothing
    pub fn make_ready(&mut self, count: usize) -> Result<()> {
        let wanted = self.bytes_for(count.min(self.capacity));
        if wanted <= self.ready {
            return Ok(());
        }
        #[cfg(feature = "test-hooks")]
        if FAIL_POPULATE.with(std::cell::Cell::get) {
            return Err(Error::other("populating a growing list failed, as a test asked"));
        }
        let page = page_bytes();
        let end = (wanted + READY_AHEAD_PAGES * page).next_multiple_of(page).min(self.mapped);
        let start = self.ready;
        // SAFETY: start and end lie inside the mapping, past every item the list holds, where no reader looks
        let at = unsafe { self.base.as_ptr().cast::<u8>().add(start) };
        take_pages_back(at, end - start);
        populate(at, end - start, page).map_err(|error| Error::new(error.kind(), format!("populating a growing list: {error}")))?;
        self.ready = end;
        Ok(())
    }

    /// Hand back the pages past what this many items take and a step, once the list's length is at or under it
    pub fn give_back_past(&mut self, count: usize) {
        let page = page_bytes();
        let keep = (self.bytes_for(count.max(self.length)) + READY_AHEAD_PAGES * page).next_multiple_of(page);
        if keep >= self.ready {
            return;
        }
        // SAFETY: keep is a whole page inside the mapping, past every item the list holds
        let at = unsafe { self.base.as_ptr().cast::<u8>().add(keep) };
        give_pages_back(at, self.ready - keep);
        self.ready = keep;
    }

    /// Check this many items fit in the pages in memory now
    pub fn is_ready_for(&self, count: usize) -> bool {
        count <= self.capacity && self.bytes_for(count) <= self.ready
    }

    /// Add an item, or refuse it when its room isn't in memory, which a caller makes ready first
    #[must_use]
    pub fn push(&mut self, item: Item) -> bool {
        if !self.is_ready_for(self.length + 1) {
            debug_assert!(false, "a push past the list's pages in memory");
            return false;
        }
        // SAFETY: the slot lies inside the pages in memory, past the items written
        unsafe { self.base.as_ptr().add(self.length).write(item) };
        self.length += 1;
        true
    }

    /// Add every item of an iterator, or refuse the rest once one doesn't fit
    #[must_use]
    pub fn extend_from(&mut self, items: impl IntoIterator<Item = Item>) -> bool {
        items.into_iter().all(|item| self.push(item))
    }

    /// Add every item of a slice, or none when they don't all fit
    #[must_use]
    pub fn extend_from_slice(&mut self, items: &[Item]) -> bool {
        if !self.is_ready_for(self.length + items.len()) {
            debug_assert!(false, "a push past the list's pages in memory");
            return false;
        }
        // SAFETY: the items land inside the pages in memory, past those written, and the slice can't overlap the list's own
        unsafe { self.base.as_ptr().add(self.length).copy_from_nonoverlapping(items.as_ptr(), items.len()) };
        self.length += items.len();
        true
    }

    /// Put an item at a place, moving the later ones up, or refuse it when its room isn't in memory
    #[must_use]
    pub fn insert(&mut self, at: usize, item: Item) -> bool {
        if at > self.length || !self.push(item) {
            return false;
        }
        self[at..].rotate_right(1);
        true
    }

    /// Cut the list to this many items
    pub fn truncate(&mut self, length: usize) {
        self.length = self.length.min(length);
    }

    /// Drop every item
    pub fn clear(&mut self) {
        self.length = 0;
    }

    /// Read the bytes the list holds in memory
    pub fn ready_bytes(&self) -> usize {
        self.ready
    }

    /// Read the most items the list can hold
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    fn bytes_for(&self, count: usize) -> usize {
        count * size_of::<Item>()
    }
}

impl<Item: Copy> Default for GrowingList<Item> {
    fn default() -> Self {
        Self { base: NonNull::dangling(), capacity: 0, length: 0, mapped: 0, ready: 0 }
    }
}

impl<Item: Copy> Deref for GrowingList<Item> {
    type Target = [Item];

    #[inline]
    fn deref(&self) -> &[Item] {
        // SAFETY: the first `length` items are written, in pages in memory
        unsafe { std::slice::from_raw_parts(self.base.as_ptr(), self.length) }
    }
}

impl<Item: Copy> DerefMut for GrowingList<Item> {
    fn deref_mut(&mut self) -> &mut [Item] {
        // SAFETY: the first `length` items are written, in pages in memory, and the list is borrowed whole
        unsafe { std::slice::from_raw_parts_mut(self.base.as_ptr(), self.length) }
    }
}

impl<Item: Copy> Drop for GrowingList<Item> {
    fn drop(&mut self) {
        if self.mapped > 0 {
            unmap_room(self.base.as_ptr().cast(), self.mapped);
        }
    }
}
