//! Memory a maintenance job maps at its start, carves into fixed lists and gives back at its end, apart from the global heap

use std::io::{Error, Result};
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering, fence};

use reel_api::units::ByteCount;

use crate::fixed_list::{FixedList, ListFull, full};

/// Bytes a region's head takes at the start of its mapping, a cache line
pub const REGION_HEAD_BYTES: usize = 64;

/// Bytes a list carved from a region can lose to its alignment, more than any item here asks
pub const LIST_ALIGN_ROOM: usize = 64;

/// Count the region bytes a list of this many items takes, its alignment room included
pub const fn list_bytes<Item>(capacity: usize) -> usize {
    capacity * size_of::<Item>() + LIST_ALIGN_ROOM
}

/// The start of a region's mapping, which counts what still points into it
#[repr(C, align(64))]
pub struct RegionHead {
    /// The region's handle and the lists carved from it that are still alive
    holders: AtomicUsize,
    /// Bytes the mapping takes, head included
    mapped: usize,
}

/// One job's mapping, which hands out lists until its room runs out
pub struct Region {
    /// The mapping's head
    head: NonNull<RegionHead>,
    /// Bytes carved so far, head included
    carved: usize,
}

// SAFETY: the region's memory belongs to it and its lists alone, and its count is atomic
unsafe impl Send for Region {}
// SAFETY: a shared handle only reads the head's length, which nothing changes, and the atomic count
unsafe impl Sync for Region {}

impl Region {
    /// Map this many bytes with every page in memory, so the job's steps never fault
    pub fn map(bytes: ByteCount) -> Result<Self> {
        let _in_flight = MapInFlight::enter();
        let faults_before = thread_faults();
        let page = page_bytes();
        let mapped = (REGION_HEAD_BYTES + bytes.0 as usize).div_ceil(page) * page;
        let flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | populate_flag();
        // SAFETY: an anonymous private mapping with no address asked for touches no memory of ours
        let memory = unsafe { libc::mmap(ptr::null_mut(), mapped, libc::PROT_READ | libc::PROT_WRITE, flags, -1, 0) };
        if memory == libc::MAP_FAILED {
            return Err(Error::last_os_error());
        }
        let Some(head) = NonNull::new(memory.cast::<RegionHead>()) else {
            return Err(Error::other("mmap gave a null mapping"));
        };
        if !populate_written(memory, mapped) {
            touch_pages(memory.cast::<u8>(), mapped, page);
        }
        // the faults the map and its populate took on this thread, which a fault gate can tell apart from a job's own
        note_populate(thread_faults().saturating_sub(faults_before));
        // SAFETY: the mapping starts page aligned and has room for the head
        unsafe { head.as_ptr().write(RegionHead { holders: AtomicUsize::new(1), mapped }) };
        Ok(Self { head, carved: REGION_HEAD_BYTES })
    }

    /// Read the bytes the mapping takes, head included
    pub fn mapped_bytes(&self) -> ByteCount {
        // SAFETY: the handle holds the region, so the head is mapped
        ByteCount(unsafe { self.head.as_ref() }.mapped as u64)
    }

    /// Carve an empty list with room for this many items
    pub fn carve<Item: Copy>(&mut self, capacity: usize) -> std::result::Result<FixedList<Item>, ListFull> {
        let start = self.carved.next_multiple_of(align_of::<Item>());
        let end = capacity.checked_mul(size_of::<Item>()).and_then(|bytes| start.checked_add(bytes)).ok_or_else(full)?;
        // SAFETY: the handle holds the region, so the head is mapped
        let head = unsafe { self.head.as_ref() };
        if end > head.mapped {
            return Err(full());
        }
        self.carved = end;
        head.holders.fetch_add(1, Ordering::Relaxed);
        // SAFETY: the place is aligned, inside the mapping, past every earlier list, and the hold is taken
        let place = unsafe { NonNull::new_unchecked(self.head.as_ptr().cast::<u8>().add(start).cast::<Item>()) };
        // SAFETY: as above
        Ok(unsafe { FixedList::in_region(place, capacity, self.head) })
    }

    /// Carve a list with every place holding one value
    pub fn carve_filled<Item: Copy>(&mut self, length: usize, value: Item) -> std::result::Result<FixedList<Item>, ListFull> {
        let mut list = self.carve(length)?;
        list.resize(length, value)?;
        Ok(list)
    }
}

impl Drop for Region {
    fn drop(&mut self) {
        // SAFETY: the handle took the first hold and gives it back once
        unsafe { release_head(self.head) };
    }
}

/// Give one hold on a region back, and unmap it when that was the last
///
/// # Safety
/// The caller must hold the region and must not touch its memory afterwards
pub unsafe fn release_head(head: NonNull<RegionHead>) {
    // SAFETY: the caller's hold keeps the head mapped until this returns
    let region = unsafe { head.as_ref() };
    if region.holders.fetch_sub(1, Ordering::Release) != 1 {
        return;
    }
    // every other holder's writes happen before the unmap
    fence(Ordering::Acquire);
    let mapped = region.mapped;
    // SAFETY: no hold is left, so nothing reads the mapping again
    unsafe { libc::munmap(head.as_ptr().cast(), mapped) };
}

/// Read the size of a page
fn page_bytes() -> usize {
    // SAFETY: sysconf reads a number and touches no memory of ours
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    usize::try_from(page).unwrap_or(4_096).max(4_096)
}

/// The madvise that has the kernel write-fault every page of a range in, from Linux 5.14
#[cfg(target_os = "linux")]
const MADV_POPULATE_WRITE: libc::c_int = 23;

/// Have the kernel write every page in, where MAP_POPULATE alone left them to fault at a job's first write on Linux 7.0, and
/// say whether it did
#[cfg(target_os = "linux")]
fn populate_written(memory: *mut libc::c_void, mapped: usize) -> bool {
    // SAFETY: the range is the mapping just made, and the call only faults its pages in
    unsafe { libc::madvise(memory, mapped, MADV_POPULATE_WRITE) == 0 }
}

/// Say no kernel populate ran, so the pages get touched
#[cfg(not(target_os = "linux"))]
fn populate_written(_memory: *mut libc::c_void, _mapped: usize) -> bool {
    false
}

/// The flag that has the kernel fault every page in at the map, where it has one
#[cfg(target_os = "linux")]
fn populate_flag() -> libc::c_int {
    libc::MAP_POPULATE
}

/// The flag that has the kernel fault every page in at the map, where it has one
#[cfg(not(target_os = "linux"))]
fn populate_flag() -> libc::c_int {
    0
}

/// Write one byte a page, so the pages are in memory before a step reads them
fn touch_pages(memory: *mut u8, mapped: usize, page: usize) {
    for offset in (0..mapped).step_by(page) {
        // SAFETY: the offset is inside the mapping, and the write keeps the page's zero
        unsafe { ptr::write_volatile(memory.add(offset), 0) };
    }
}

/// Maps under way, so a fault gate reads its counts only when no populate is half counted
pub static MAPS_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);

/// Threads whose populate faults a fault gate can read, each with its id and its total, more than a store runs
pub const POPULATE_THREADS: usize = 64;
/// Each thread that mapped a region: its id, and the minor faults its maps and populates took
pub static POPULATE_FAULTS: [(AtomicU64, AtomicU64); POPULATE_THREADS] = [const { (AtomicU64::new(0), AtomicU64::new(0)) }; POPULATE_THREADS];

/// Add a populate's faults to the calling thread's total, in the slot its id took first
fn note_populate(faults: u64) {
    let id = thread_id();
    for (slot_id, total) in &POPULATE_FAULTS {
        if slot_id.load(Ordering::Acquire) == id || slot_id.compare_exchange(0, id, Ordering::AcqRel, Ordering::Acquire).is_ok() {
            total.fetch_add(faults, Ordering::Relaxed);
            return;
        }
    }
}

/// Read the populate faults a thread took, by its kernel id
#[cfg(feature = "test-hooks")]
pub fn populate_faults_of(id: u64) -> u64 {
    POPULATE_FAULTS.iter().find(|(slot_id, _total)| slot_id.load(Ordering::Acquire) == id).map_or(0, |(_id, total)| total.load(Ordering::Relaxed))
}

/// Read the calling thread's kernel id
#[cfg(target_os = "linux")]
fn thread_id() -> u64 {
    // SAFETY: gettid takes nothing and can't fail
    u64::try_from(unsafe { libc::gettid() }).unwrap_or(0)
}

/// Read no id where the kernel keeps no fault count for one thread
#[cfg(not(target_os = "linux"))]
fn thread_id() -> u64 {
    0
}

/// Read the minor faults the calling thread has taken
#[cfg(target_os = "linux")]
fn thread_faults() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: the call writes one rusage into the buffer, which is that size
    if unsafe { libc::getrusage(libc::RUSAGE_THREAD, usage.as_mut_ptr()) } != 0 {
        return 0;
    }
    // SAFETY: the call succeeded, so it wrote the whole struct
    u64::try_from(unsafe { usage.assume_init() }.ru_minflt).unwrap_or(0)
}

/// Read no faults where the kernel keeps no count for one thread
#[cfg(not(target_os = "linux"))]
fn thread_faults() -> u64 {
    0
}

/// One map under way, counted from its start to its end whichever way it ends
struct MapInFlight;

impl MapInFlight {
    /// Count a map as under way
    fn enter() -> Self {
        MAPS_IN_FLIGHT.fetch_add(1, Ordering::AcqRel);
        Self
    }
}

impl Drop for MapInFlight {
    fn drop(&mut self) {
        MAPS_IN_FLIGHT.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lists_outlive_the_handle() {
        let mut region = Region::map(ByteCount(10_000)).expect("map");
        let mut bytes = region.carve::<u8>(4_000).expect("carve");
        let mut words = region.carve_filled::<u64>(500, 0).expect("carve");
        assert_eq!(bytes.capacity(), 4_000);
        assert_eq!(words.len(), 500);
        assert!(region.carve::<u8>(region.mapped_bytes().0 as usize).is_err());
        drop(region);
        assert_eq!(bytes.extend_from_slice(&[7; 4_000]), Ok(()));
        words[499] = 9;
        assert_eq!(bytes[3_999] + words[499] as u8, 16);
    }
}
