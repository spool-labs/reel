//! Address space a pool reserves at open and populates in steps, so its memory follows the data
//!
//! The pool takes its whole share as address space and populates a first part. A maintenance job populates the next part on its
//! own thread and then publishes the higher limit with one atomic store, so a caller only touches memory under the limit and
//! never faults

use std::io::{Error, ErrorKind, Result};
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use reel_core::units::ByteCount;

use crate::region::{MapInFlight, note_populate, page_bytes, thread_faults, touch_pages};

/// A pool starts at this share of its room when nothing on disk sizes it
pub const FIRST_PART_DIVISOR: usize = 8;

/// One pool's reserved room and the part of it in memory
pub struct PoolRoom {
    base: NonNull<u8>,
    /// Bytes reserved, a whole number of pages
    reserved: usize,
    /// Bytes in memory from the start, a whole number of pages. Callers use memory under it
    committed: AtomicUsize,
    /// Set when the last populate failed, and cleared by the next that works
    has_failed: AtomicBool,
    /// Bytes the pool uses now, which its owner notes and the growing job reads
    used: AtomicUsize,
}

// SAFETY: the room's memory belongs to the pool that owns it, and the limit is atomic
unsafe impl Send for PoolRoom {}
// SAFETY: a shared handle reads the base and the reserved length, which nothing changes, and the atomics
unsafe impl Sync for PoolRoom {}

impl std::fmt::Debug for PoolRoom {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("PoolRoom").field("reserved", &self.reserved).field("committed", &self.committed()).finish()
    }
}

impl PoolRoom {
    /// Reserve a share as address space and populate its first part
    pub fn reserve(share: ByteCount, first: ByteCount) -> Result<Self> {
        let page = page_bytes();
        let reserved = (share.0 as usize).next_multiple_of(page).max(page);
        // SAFETY: an anonymous private mapping with no address asked for touches no memory of ours
        let memory = unsafe { libc::mmap(ptr::null_mut(), reserved, libc::PROT_READ | libc::PROT_WRITE, reserve_flags(), -1, 0) };
        if memory == libc::MAP_FAILED {
            return Err(Error::last_os_error());
        }
        let Some(base) = NonNull::new(memory.cast::<u8>()) else {
            return Err(Error::other("mmap gave a null mapping"));
        };
        ask_huge_pages(memory, reserved);
        let room = Self { base, reserved, committed: AtomicUsize::new(0), has_failed: AtomicBool::new(false), used: AtomicUsize::new(0) };
        room.grow_to(first.0 as usize)?;
        Ok(room)
    }

    /// Reserve a share with its first part sized from nothing on disk
    pub fn reserve_fresh(share: ByteCount) -> Result<Self> {
        Self::reserve(share, ByteCount(share.0 / FIRST_PART_DIVISOR as u64))
    }

    /// Read where the room starts
    pub fn base(&self) -> *mut u8 {
        self.base.as_ptr()
    }

    /// Read the bytes reserved, which is as far as the pool can grow
    pub fn reserved(&self) -> usize {
        self.reserved
    }

    /// Read the bytes in memory now
    pub fn committed(&self) -> usize {
        self.committed.load(Ordering::Acquire)
    }

    /// Note the bytes the pool uses now
    pub fn note_used(&self, bytes: usize) {
        self.used.store(bytes, Ordering::Relaxed);
    }

    /// Read the bytes the pool last noted it uses
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    /// Check the last populate failed
    pub fn has_failed(&self) -> bool {
        self.has_failed.load(Ordering::Acquire)
    }

    /// Read where the pool grows next with this many bytes used: double once half the part in memory is used, up to the room
    pub fn next_step(&self, used: usize) -> Option<usize> {
        let committed = self.committed();
        if committed >= self.reserved || used.saturating_mul(2) < committed {
            return None;
        }
        Some(committed.saturating_mul(2).max(page_bytes()).min(self.reserved))
    }

    /// Grow one step when half the part in memory is used, and say whether it grew
    pub fn grow_step(&self) -> Result<bool> {
        let Some(target) = self.next_step(self.used()) else {
            return Ok(false);
        };
        self.grow_to(target)?;
        Ok(true)
    }

    /// Populate the room up to this many bytes and then publish the limit. A populate that fails leaves the limit where it was
    pub fn grow_to(&self, bytes: usize) -> Result<()> {
        let page = page_bytes();
        let target = bytes.next_multiple_of(page).min(self.reserved);
        let committed = self.committed();
        if target <= committed {
            return Ok(());
        }
        // SAFETY: the range lies inside the reservation and past the limit, so no caller reads or writes it
        let start = unsafe { self.base.as_ptr().add(committed) };
        if let Err(error) = populate(start, target - committed, page) {
            self.has_failed.store(true, Ordering::Release);
            return Err(error);
        }
        self.has_failed.store(false, Ordering::Release);
        self.committed.fetch_max(target, Ordering::Release);
        Ok(())
    }
}

impl Drop for PoolRoom {
    fn drop(&mut self) {
        // SAFETY: the room owns the mapping, and the pool that read it is gone
        unsafe { libc::munmap(self.base.as_ptr().cast(), self.reserved) };
    }
}

/// The flags that take address space without charging memory for it
fn reserve_flags() -> libc::c_int {
    libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE
}

/// Ask for huge pages over a reservation, where the kernel has them
fn ask_huge_pages(memory: *mut libc::c_void, reserved: usize) {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: the range is the reservation just made, and the advice changes how pages back it, never its bytes
        let _answer = unsafe { libc::madvise(memory, reserved, libc::MADV_HUGEPAGE) };
    }
    #[cfg(not(target_os = "linux"))]
    let _hint = (memory, reserved);
}

/// Bring a range into memory, counting its faults as a populate's. On Linux the kernel does it and says when memory ran out, and
/// a kernel without the advice has the pages touched
fn populate(start: *mut u8, bytes: usize, page: usize) -> Result<()> {
    let _in_flight = MapInFlight::enter();
    let faults_before = thread_faults();
    let populated = populate_written(start, bytes);
    let answer = match populated {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::Unsupported => {
            // SAFETY: the range lies past the limit, where no caller reads or writes
            unsafe { touch_pages(start, bytes, page) };
            Ok(())
        }
        Err(error) => Err(error),
    };
    note_populate(thread_faults().saturating_sub(faults_before));
    answer
}

/// Have the kernel write every page of a range in, and report a kernel that lacks the advice as unsupported
#[cfg(target_os = "linux")]
fn populate_written(start: *mut u8, bytes: usize) -> Result<()> {
    // SAFETY: the range lies inside a reservation, and the call only faults its pages in
    if unsafe { libc::madvise(start.cast(), bytes, crate::region::MADV_POPULATE_WRITE) } == 0 {
        return Ok(());
    }
    let error = Error::last_os_error();
    if error.raw_os_error() == Some(libc::EINVAL) {
        return Err(Error::from(ErrorKind::Unsupported));
    }
    Err(error)
}

/// Say no kernel populate ran, so the pages get touched
#[cfg(not(target_os = "linux"))]
fn populate_written(_start: *mut u8, _bytes: usize) -> Result<()> {
    Err(Error::from(ErrorKind::Unsupported))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grows_by_doubling_at_half() {
        let page = page_bytes();
        let room = PoolRoom::reserve(ByteCount(64 * page as u64), ByteCount(4 * page as u64)).expect("reserve");
        assert_eq!(room.committed(), 4 * page);
        assert_eq!(room.next_step(page), None);
        room.note_used(2 * page);
        assert!(room.grow_step().expect("grow"));
        assert_eq!(room.committed(), 8 * page);
        // SAFETY: the bytes lie under the limit
        unsafe { room.base().add(8 * page - 1).write(7) };
        room.grow_to(1 << 40).expect("grow");
        assert_eq!(room.committed(), 64 * page);
        assert_eq!(room.next_step(64 * page), None);
    }

    #[test]
    fn fresh_room_starts_at_an_eighth() {
        let page = page_bytes() as u64;
        let room = PoolRoom::reserve_fresh(ByteCount(64 * page)).expect("reserve");
        assert_eq!(room.committed() as u64, 8 * page);
        assert_eq!(room.reserved() as u64, 64 * page);
    }
}
