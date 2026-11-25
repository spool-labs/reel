//! Address space a pool reserves at open and populates in steps, so its memory follows the data
//!
//! The pool takes its whole share as address space and populates a first part. When less than one extent of the part in memory
//! is free, the pool wakes the reserve job, which populates the next extent on its own thread and then publishes the higher limit
//! with one atomic store. A caller only touches memory under the limit and never faults
//!
//! A room of a few huge pages or more starts on a huge page and grows in whole huge pages, so no huge page the kernel backs a fault
//! with reaches past the limit. A smaller room asks for no huge pages, so a nearly empty pool holds a page and not two megabytes

use std::io::{Error, ErrorKind, Result};
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use reel_core::units::ByteCount;

use crate::pool::HUGE_PAGE_BYTES;
use crate::region::{MapInFlight, note_populate, page_bytes, thread_faults, touch_pages};

/// A pool starts at this share of its room when nothing on disk sizes it
pub const FIRST_PART_DIVISOR: usize = 8;
/// A pool grows by this share of its room at a time, and asks for the next extent when less than one is free
pub const EXTENT_DIVISOR: usize = 16;
/// The least room that starts on a huge page and grows in whole ones
pub const HUGE_ROOM_BYTES: usize = 4 * HUGE_PAGE_BYTES;

/// How far ahead of its fill a pool on the lead arm keeps memory ready, in nanoseconds of writes at its last measured rate
pub const LEAD_NANOS: u64 = 100_000_000;
/// A pool on the demand arm grows by this share of its room at a time
pub const DEMAND_DIVISOR: usize = 64;
/// The variable that picks the growth arm for the A/B/C runs: extent, demand or lead
pub const POLICY_VARIABLE: &str = "REEL_GROW_POLICY";

/// When a pool grows, one arm each for the A/B/C runs. The winner stays and the others go
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GrowPolicy {
    /// Grow an extent when less than one is free
    Extent,
    /// Grow a small step once a caller is at the limit
    Demand,
    /// Keep the fill of the last stretch of time ready ahead of use
    Lead,
}

/// Read the arm the process runs, once
pub fn grow_policy() -> GrowPolicy {
    static POLICY: OnceLock<GrowPolicy> = OnceLock::new();
    *POLICY.get_or_init(|| match std::env::var(POLICY_VARIABLE).as_deref() {
        Ok("demand") => GrowPolicy::Demand,
        Ok("lead") => GrowPolicy::Lead,
        _ => GrowPolicy::Extent,
    })
}

/// Wakes the job that grows the rooms
pub type GrowWake = Arc<dyn Fn() + Send + Sync>;

/// One pool's reserved room and the part of it in memory
pub struct PoolRoom {
    base: NonNull<u8>,
    /// Bytes reserved, a whole number of steps
    reserved: usize,
    /// The bytes every limit is a multiple of: a page, or a huge page in a large room
    unit: usize,
    /// Bytes the room grows by at a time
    extent: usize,
    /// Bytes in memory from the start. Callers use memory under it
    committed: AtomicUsize,
    /// Set when the last populate failed, and cleared by the next that works
    has_failed: AtomicBool,
    /// Set when the host had no room for the next step, and cleared once a step fits
    is_short: AtomicBool,
    /// The bytes the lead arm keeps ready past use, which the job sets from the fill rate
    lead: AtomicUsize,
    /// The job's last look at the fill: when, and the bytes used then
    sample: Mutex<(u64, usize)>,
    /// Bytes the pool uses now, which its owner notes and the growing job reads
    used: AtomicUsize,
    /// Set once the pool asked for its next extent, until the job brings it in
    has_asked: AtomicBool,
    wake: OnceLock<GrowWake>,
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
        let wanted = (share.0 as usize).next_multiple_of(page).max(page);
        let unit = if wanted >= HUGE_ROOM_BYTES { HUGE_PAGE_BYTES } else { page };
        let reserved = wanted.next_multiple_of(unit);
        let base = map_aligned(reserved, unit)?;
        advise_huge_pages(base.as_ptr().cast(), reserved, unit == HUGE_PAGE_BYTES);
        let extent = (reserved / EXTENT_DIVISOR).next_multiple_of(unit).max(unit);
        let room = Self {
            base,
            reserved,
            unit,
            extent,
            committed: AtomicUsize::new(0),
            has_failed: AtomicBool::new(false),
            is_short: AtomicBool::new(false),
            lead: AtomicUsize::new(extent),
            sample: Mutex::new((0, 0)),
            used: AtomicUsize::new(0),
            has_asked: AtomicBool::new(false),
            wake: OnceLock::new(),
        };
        room.grow_to(first.0 as usize)?;
        Ok(room)
    }

    /// Reserve a share with its first part sized from nothing on disk
    pub fn reserve_fresh(share: ByteCount) -> Result<Self> {
        Self::reserve(share, ByteCount(share.0 / FIRST_PART_DIVISOR as u64))
    }

    /// Set what the pool calls when it wants its next extent
    pub fn set_wake(&self, wake: GrowWake) {
        let _set = self.wake.set(wake);
    }

    /// Read where the room starts
    pub fn base(&self) -> *mut u8 {
        self.base.as_ptr()
    }

    /// Read the bytes reserved, which is as far as the pool can grow
    pub fn reserved(&self) -> usize {
        self.reserved
    }

    /// Read the bytes the room grows by at a time
    pub fn extent(&self) -> usize {
        self.extent
    }

    /// Read the bytes in memory now
    pub fn committed(&self) -> usize {
        self.committed.load(Ordering::Acquire)
    }

    /// Note the bytes the pool uses now, and wake the growing job the first time less than one extent is free
    pub fn note_used(&self, bytes: usize) {
        self.used.store(bytes, Ordering::Relaxed);
        if self.next_step(bytes).is_some()
            && !self.has_asked.swap(true, Ordering::AcqRel)
            && let Some(wake) = self.wake.get()
        {
            wake();
        }
    }

    /// Read the bytes the pool last noted it uses
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    /// Check the last populate failed
    pub fn has_failed(&self) -> bool {
        self.has_failed.load(Ordering::Acquire)
    }

    /// Check the host had no room for the pool's next step
    pub fn is_short(&self) -> bool {
        self.is_short.load(Ordering::Acquire)
    }

    /// Check the pool can still grow, so a caller at its limit waits for the next step instead of treating the pool as full
    pub fn can_grow(&self) -> bool {
        self.committed() < self.reserved && !self.has_failed() && !self.is_short()
    }

    /// Read where the pool grows next with this many bytes used, under the process's arm, up to the room
    pub fn next_step(&self, used: usize) -> Option<usize> {
        let committed = self.committed();
        if committed >= self.reserved {
            return None;
        }
        let free = committed.saturating_sub(used);
        let target = match grow_policy() {
            GrowPolicy::Extent => (free < self.extent).then_some(committed + self.extent),
            GrowPolicy::Demand => {
                let step = (self.reserved / DEMAND_DIVISOR).next_multiple_of(self.unit).max(self.unit);
                (free < self.unit).then_some(committed + step)
            },
            GrowPolicy::Lead => {
                let lead = self.lead.load(Ordering::Relaxed);
                (free < lead).then_some(used + 2 * lead)
            },
        }?;
        Some(target.next_multiple_of(self.unit).min(self.reserved))
    }

    /// Set the lead arm's lead from the fill since the job last looked: the bytes that fill takes in LEAD_NANOS, and never less
    /// than one unit
    pub fn note_rate(&self, now: u64) {
        let used = self.used();
        let mut sample = self.sample.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let (then, used_then) = *sample;
        *sample = (now, used);
        let elapsed = now.saturating_sub(then);
        if then == 0 || elapsed == 0 {
            return;
        }
        let rate_lead = (used.saturating_sub(used_then) as u128 * u128::from(LEAD_NANOS) / u128::from(elapsed)) as usize;
        self.lead.store(rate_lead.max(self.unit), Ordering::Relaxed);
    }

    /// Grow until the arm wants no more or the room is whole, taking no step past the room the host has left, and say whether it
    /// grew. A pool the host has no room for stays as it is and reads as short until a step fits
    pub fn grow_step(&self, host_left: &mut u64) -> Result<bool> {
        let mut grew = false;
        while let Some(target) = self.next_step(self.used()) {
            let step = target.saturating_sub(self.committed()) as u64;
            if step > *host_left {
                self.is_short.store(true, Ordering::Release);
                break;
            }
            self.grow_to(target)?;
            *host_left -= step;
            self.is_short.store(false, Ordering::Release);
            grew = true;
        }
        // a use noted between the last look and this clear asks again on its next note
        self.has_asked.store(false, Ordering::Release);
        Ok(grew)
    }

    /// Populate the room up to this many bytes and then publish the limit. A populate that fails leaves the limit where it was
    pub fn grow_to(&self, bytes: usize) -> Result<()> {
        let target = bytes.next_multiple_of(self.unit).min(self.reserved);
        let committed = self.committed();
        if target <= committed {
            return Ok(());
        }
        // SAFETY: the range lies inside the reservation and past the limit, so no caller reads or writes it
        let start = unsafe { self.base.as_ptr().add(committed) };
        if let Err(error) = populate(start, target - committed, page_bytes()) {
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

/// Reserve address space that starts on a multiple of `align`, giving back the slack around it
fn map_aligned(bytes: usize, align: usize) -> Result<NonNull<u8>> {
    let slack = if align > page_bytes() { align } else { 0 };
    // SAFETY: an anonymous private mapping with no address asked for touches no memory of ours
    let memory = unsafe { libc::mmap(ptr::null_mut(), bytes + slack, libc::PROT_READ | libc::PROT_WRITE, reserve_flags(), -1, 0) };
    if memory == libc::MAP_FAILED {
        return Err(Error::last_os_error());
    }
    let raw = memory as usize;
    let start = raw.next_multiple_of(align.max(1));
    let (head, tail) = (start - raw, raw + bytes + slack - (start + bytes));
    if head > 0 {
        // SAFETY: the range is the slack before the aligned start, which nothing points into
        unsafe { libc::munmap(memory, head) };
    }
    if tail > 0 {
        // SAFETY: the range is the slack past the aligned end, which nothing points into
        unsafe { libc::munmap((start + bytes) as *mut libc::c_void, tail) };
    }
    NonNull::new(start as *mut u8).ok_or_else(|| Error::other("mmap gave a null mapping"))
}

/// Ask for huge pages over a large room, whose limits are whole huge pages, and for none over a small one, where the kernel has them
fn advise_huge_pages(memory: *mut libc::c_void, reserved: usize, is_huge: bool) {
    #[cfg(target_os = "linux")]
    {
        let advice = if is_huge { libc::MADV_HUGEPAGE } else { libc::MADV_NOHUGEPAGE };
        // SAFETY: the range is the reservation just made, and the advice changes how pages back it, never its bytes
        let _answer = unsafe { libc::madvise(memory, reserved, advice) };
    }
    #[cfg(not(target_os = "linux"))]
    let _hint = (memory, reserved, is_huge);
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
    use std::sync::atomic::AtomicUsize;

    use super::*;

    #[test]
    fn grows_an_extent_when_less_than_one_is_free() {
        let page = page_bytes();
        let room = PoolRoom::reserve(ByteCount(64 * page as u64), ByteCount(8 * page as u64)).expect("reserve");
        let wakes = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&wakes);
        room.set_wake(Arc::new(move || {
            counted.fetch_add(1, Ordering::Relaxed);
        }));
        assert_eq!((room.committed(), room.extent()), (8 * page, 4 * page));
        room.note_used(4 * page);
        assert_eq!(wakes.load(Ordering::Relaxed), 0, "a room with an extent free asked to grow");
        room.note_used(5 * page);
        room.note_used(6 * page);
        assert_eq!(wakes.load(Ordering::Relaxed), 1, "a room asked more than once for one extent");
        assert!(room.grow_step(&mut u64::MAX.clone()).expect("grow"));
        assert_eq!(room.committed(), 12 * page);
        // SAFETY: the bytes lie under the limit
        unsafe { room.base().add(12 * page - 1).write(7) };
        room.note_used(11 * page);
        assert_eq!(wakes.load(Ordering::Relaxed), 2);
        room.grow_to(1 << 40).expect("grow");
        assert_eq!(room.committed(), 64 * page);
        assert_eq!(room.next_step(64 * page), None);
    }

    #[test]
    fn a_host_with_no_room_leaves_the_pool_short() {
        let page = page_bytes();
        let room = PoolRoom::reserve(ByteCount(64 * page as u64), ByteCount(8 * page as u64)).expect("reserve");
        room.note_used(8 * page);
        let mut tight = 2 * page as u64;
        assert!(!room.grow_step(&mut tight).expect("grow"));
        assert!(room.is_short() && !room.can_grow());
        assert_eq!((room.committed(), tight), (8 * page, 2 * page as u64));
        let mut roomy = u64::MAX;
        assert!(room.grow_step(&mut roomy).expect("grow"));
        assert!(!room.is_short() && room.can_grow());
        assert_eq!(room.committed(), 12 * page);
    }

    #[test]
    fn fresh_room_starts_at_an_eighth() {
        let page = page_bytes() as u64;
        let room = PoolRoom::reserve_fresh(ByteCount(64 * page)).expect("reserve");
        assert_eq!(room.committed() as u64, 8 * page);
        assert_eq!(room.reserved() as u64, 64 * page);
    }

    #[test]
    fn a_large_room_keeps_its_limits_on_huge_pages() {
        let room = PoolRoom::reserve(ByteCount(10 * HUGE_PAGE_BYTES as u64 + 5), ByteCount(1)).expect("reserve");
        assert_eq!(room.base() as usize % HUGE_PAGE_BYTES, 0);
        assert_eq!(room.reserved(), 11 * HUGE_PAGE_BYTES);
        assert_eq!(room.committed(), HUGE_PAGE_BYTES);
        room.note_used(HUGE_PAGE_BYTES);
        assert!(room.grow_step(&mut u64::MAX.clone()).expect("grow"));
        assert_eq!(room.committed() % HUGE_PAGE_BYTES, 0);
    }
}
