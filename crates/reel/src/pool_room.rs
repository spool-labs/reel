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
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use reel_core::units::ByteCount;

use crate::guard::hold;
use crate::pool::{HUGE_PAGE_BYTES, touched_filled};
use crate::engine::caller_activity::thread_slot;
use crate::region::{MapInFlight, note_populate, page_bytes, thread_faults, touch_pages};

/// A pool grows by this share of its room at a time, and asks for the next extent when less than one is free
pub const EXTENT_DIVISOR: usize = 16;
/// The least room that starts on a huge page and grows in whole ones
pub const HUGE_ROOM_BYTES: usize = 4 * HUGE_PAGE_BYTES;

/// How far ahead of its fill a pool on the lead arm keeps memory ready, in nanoseconds of writes at its last measured rate
pub const LEAD_NANOS: u64 = 100_000_000;
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

/// Slots for readers that take no lock of a pool. Threads past this many share a slot, which the counts allow
pub const READ_SLOTS: usize = 64;

/// One thread's readers: how many came in and how many left, on a cache line of its own
#[repr(align(128))]
#[derive(Debug, Default)]
pub struct ReadSlot {
    entries: AtomicU64,
    exits: AtomicU64,
}

/// The read slots of a room, and for each extent on its way back to the kernel the slots it still waits to see empty
pub struct ReadSlots {
    slots: [ReadSlot; READ_SLOTS],
    /// For each extent on its way back, the slots that had a reader inside when its release began and haven't been seen empty since
    busy: Mutex<[u64; EXTENTS]>,
    /// Extents out of the ready set whose pages wait for their readers to leave
    pending: AtomicU64,
}

impl ReadSlots {
    /// Read which of these slots have a reader inside now. A slot whose exits, read first, equal its entries, read after, had
    /// nobody inside at the moment its exits were read, however many threads share it, and a slot seen empty after a release
    /// began holds no reader from before it
    fn busy_now(&self, among: u64) -> u64 {
        let mut busy = 0;
        for (index, slot) in self.slots.iter().enumerate().filter(|(index, _slot)| among & (1 << index) != 0) {
            let exits = slot.exits.load(Ordering::SeqCst);
            if slot.entries.load(Ordering::SeqCst) != exits {
                busy |= 1 << index;
            }
        }
        busy
    }
}

/// A reader on its thread's slot, which counts its exit when it drops
pub struct RoomRead<'room>(Option<&'room ReadSlot>);

impl RoomRead<'_> {
    /// A guard over a room no reader with no lock touches, which counts nothing
    pub fn none() -> Self {
        Self(None)
    }
}

impl Drop for RoomRead<'_> {
    fn drop(&mut self) {
        if let Some(slot) = self.0 {
            slot.exits.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// Wakes the job that grows the rooms
pub type GrowWake = Arc<dyn Fn() + Send + Sync>;


/// Extents a room splits into, one bit each of its ready set
pub const EXTENTS: usize = 64;
/// How long a room stays more than an extent past what it wants before the job gives memory back
pub const DECAY_NANOS: u64 = 30_000_000_000;
/// A decay a test sets in place of DECAY_NANOS, so a drain and a refill fit one run. 0 leaves DECAY_NANOS
#[cfg(feature = "test-hooks")]
pub static DECAY_OVERRIDE: AtomicU64 = AtomicU64::new(0);

/// Read the decay in force
pub fn decay_nanos() -> u64 {
    #[cfg(feature = "test-hooks")]
    {
        let set = DECAY_OVERRIDE.load(Ordering::Relaxed);
        if set > 0 {
            return set;
        }
    }
    DECAY_NANOS
}
/// Marks an empty free chain
pub const NO_ROOM_PAGE: u32 = u32::MAX;

/// A paged pool's free pages, which the room hands out lowest extent first so use packs low and the high extents drain
struct PageState {
    /// Each free page's next, by page number
    links: Vec<u32>,
    /// Each extent's free chain
    heads: [u32; EXTENTS],
    /// Each extent's pages never handed out since it last came into memory, counted from its start
    fresh: [u32; EXTENTS],
    /// Each extent's pages handed out now
    in_use: [u32; EXTENTS],
    /// The ready extents with a free page
    has_free: u64,
    in_use_total: u32,
    ready_pages: u32,
}

/// The pages a paged pool takes from its room
struct RoomPages {
    page: usize,
    per_extent: u32,
    /// The pages the pool holds at most, which the room may round up past
    limit: u32,
    state: Mutex<PageState>,
    /// The free pages in ready extents and the pages in ready extents, readable with no lock
    free_now: AtomicU32,
    ready_now: AtomicU32,
}

/// One pool's reserved room, split into extents that come into memory and go back to the kernel as the pool's use moves
pub struct PoolRoom {
    base: NonNull<u8>,
    /// Bytes the mapping takes, a whole number of units
    mapped: usize,
    /// Bytes the room can grow to: the share, in whole system pages
    reserved: usize,
    /// The bytes every extent is a multiple of: a page, a huge page in a large room, and a pool page in a paged room
    unit: usize,
    /// Bytes of one extent. The last may be shorter
    extent: usize,
    extent_count: usize,
    /// The extents in memory, a bit each. A caller uses memory only in a ready extent
    ready: AtomicU64,
    /// Set when the last populate failed, and cleared by the next that works
    has_failed: AtomicBool,
    /// Set when the host had no room for the next step, and cleared once a step fits. A summary is never short: its rows must stay,
    /// so its writer grows it inline with no host check, and pools_short can read 0 while the summary is what went over
    is_short: AtomicBool,
    /// The bytes the lead arm keeps ready past use, which the job sets from the fill rate
    lead: AtomicUsize,
    /// The job's last look at the fill: when, and the bytes used then
    sample: Mutex<(u64, usize)>,
    /// Since when the room has held more than an extent past what it wants, or 0
    over_since: AtomicU64,
    /// The part the data on disk asks for, which open brings in when the host has room
    first_part: AtomicUsize,
    /// Bytes the room has given back to the kernel, for the report
    given_back: AtomicU64,
    /// Held across a grow, so two growers never bring in one extent at once
    growing: Mutex<()>,
    /// The most the room may grow to now, which the ladder lowers when the store must hold less
    cap: AtomicUsize,
    /// Bytes the pool uses now, which its owner notes and the job reads
    used: AtomicUsize,
    /// Set once the pool asked for its next extent, until the job brings it in
    has_asked: AtomicBool,
    wake: OnceLock<GrowWake>,
    /// A slot a thread for readers that take no lock of the pool, which a release waits out, when the pool has such readers
    read_slots: Option<Box<ReadSlots>>,
    pages: Option<RoomPages>,
}

// SAFETY: the room's memory belongs to the pool that owns it, and the ready set is atomic
unsafe impl Send for PoolRoom {}
// SAFETY: a shared handle reads the base and the reserved length, which nothing changes, the atomics, and the page state under its lock
unsafe impl Sync for PoolRoom {}

impl std::fmt::Debug for PoolRoom {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("PoolRoom").field("reserved", &self.reserved).field("committed", &self.committed()).finish()
    }
}


impl PoolRoom {
    /// Reserve a share as address space and bring its first part into memory
    pub fn reserve(share: ByteCount, first: ByteCount) -> Result<Self> {
        Self::reserve_with(share, first, None)
    }

    /// Reserve a share for a pool of pages of this size, which takes and gives its pages through the room
    pub fn reserve_paged(share: ByteCount, first: ByteCount, page: usize) -> Result<Self> {
        Self::reserve_with(share, first, Some(page))
    }

    /// Reserve a share, split it into extents, and bring the first ones into memory
    fn reserve_with(share: ByteCount, first: ByteCount, page: Option<usize>) -> Result<Self> {
        let system_page = page_bytes();
        let wanted = (share.0 as usize).next_multiple_of(system_page).max(system_page);
        let huge = if wanted >= HUGE_ROOM_BYTES { HUGE_PAGE_BYTES } else { system_page };
        let unit = huge.max(page.unwrap_or(0));
        // the mapping rounds to whole units, and the room counts and fills only the share, so nothing past the pool's own bytes
        // comes into memory or into the report. The last extent of a large room can end inside a huge page, which the kernel may
        // back whole, so resident can pass the report by under one huge page a room
        let mapped = wanted.next_multiple_of(unit);
        let base = map_aligned(mapped, huge)?;
        advise_huge_pages(base.as_ptr().cast(), mapped, huge == HUGE_PAGE_BYTES);
        let reserved = wanted;
        let extent = (reserved / EXTENTS).next_multiple_of(unit).max(unit);
        let extent_count = reserved.div_ceil(extent);
        let pages = page.map(|page| RoomPages {
            page,
            per_extent: (extent / page) as u32,
            limit: (share.0 as usize / page) as u32,
            state: Mutex::new(PageState {
                links: touched_filled(reserved / page, NO_ROOM_PAGE),
                heads: [NO_ROOM_PAGE; EXTENTS],
                fresh: [0; EXTENTS],
                in_use: [0; EXTENTS],
                has_free: 0,
                in_use_total: 0,
                ready_pages: 0,
            }),
            free_now: AtomicU32::new(0),
            ready_now: AtomicU32::new(0),
        });
        let room = Self {
            base,
            reserved,
            mapped,
            unit,
            extent,
            extent_count,
            ready: AtomicU64::new(0),
            has_failed: AtomicBool::new(false),
            is_short: AtomicBool::new(false),
            lead: AtomicUsize::new(extent),
            sample: Mutex::new((0, 0)),
            over_since: AtomicU64::new(0),
            first_part: AtomicUsize::new(0),
            given_back: AtomicU64::new(0),
            growing: Mutex::new(()),
            cap: AtomicUsize::new(usize::MAX),
            used: AtomicUsize::new(0),
            has_asked: AtomicBool::new(false),
            wake: OnceLock::new(),
            read_slots: None,
            pages,
        };
        // a whole room comes in at once, with no host check: a reader opens its rooms whole and runs no job to grow them, so a
        // read-only open can still pass what the host holds. Any other room starts with what its arm keeps ready, and the part the
        // data on disk asks for comes in at open through the host check, in the rooms' order
        if first.0 >= share.0 {
            room.grow_to(room.reserved)?;
        } else {
            room.grow_to(room.lead_bytes())?;
            room.first_part.store(first.0 as usize, Ordering::Relaxed);
        }
        Ok(room)
    }

    /// Set the part open brings in through the host check, past what the room holds now
    pub fn set_first_part(&self, bytes: usize) {
        self.first_part.store(bytes, Ordering::Relaxed);
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

    /// Read the bytes the room has given back to the kernel so far
    pub fn given_back(&self) -> u64 {
        self.given_back.load(Ordering::Relaxed)
    }


    /// Read the bytes of one extent
    pub fn extent(&self) -> usize {
        self.extent
    }

    /// Read the bytes the extent arm grows by at a time: a sixteenth of the room in whole extents, and one extent at least
    pub fn step(&self) -> usize {
        (self.reserved / EXTENT_DIVISOR).next_multiple_of(self.extent).max(self.extent)
    }

    /// Count the pool's pages in an extent, none past the pool's own count
    fn pages_in(&self, pages: &RoomPages, extent: usize) -> u32 {
        let (start, end) = self.extent_range(extent);
        let limit = pages.limit as usize * pages.page;
        ((end.min(limit) - start.min(limit)) / pages.page) as u32
    }

    /// Read an extent's first byte and its end
    fn extent_range(&self, extent: usize) -> (usize, usize) {
        (extent * self.extent, ((extent + 1) * self.extent).min(self.reserved))
    }

    /// Read the bytes in memory now: every ready extent's
    pub fn committed(&self) -> usize {
        let ready = self.ready.load(Ordering::Acquire);
        let full = ready.count_ones() as usize * self.extent;
        let last = self.extent_count - 1;
        if ready & (1 << last) != 0 { full - (self.extent_range(last).0 + self.extent - self.reserved) } else { full }
    }

    /// Check the byte at this offset lies in a ready extent
    pub fn is_ready_at(&self, offset: usize) -> bool {
        self.ready.load(Ordering::SeqCst) & (1 << (offset / self.extent)) != 0
    }

    /// Bring in the part the data on disk asked for at open, when the host has room for it, and leave the room short when not
    pub fn fill_first(&self, host_left: &mut u64) -> Result<()> {
        let target = self.first_part.swap(0, Ordering::Relaxed).min(self.reserved);
        let step = target.saturating_sub(self.committed()) as u64;
        if step == 0 {
            return Ok(());
        }
        if step > *host_left {
            self.is_short.store(true, Ordering::Release);
            return Ok(());
        }
        self.grow_to(target)?;
        *host_left -= step;
        Ok(())
    }

    /// Give the room a slot a thread for readers that take no lock of the pool, so a release waits them out
    pub fn read_unlocked(mut self) -> Self {
        self.read_slots = Some(Box::new(ReadSlots {
            slots: std::array::from_fn(|_slot| ReadSlot::default()),
            busy: Mutex::new([0; EXTENTS]),
            pending: AtomicU64::new(0),
        }));
        self
    }

    /// Count a reader that takes no lock of the pool on its thread's slot until the guard drops. A reader reads only what lies in a
    /// ready extent, and a release finishes only once every reader that came in before it cleared the extent has left
    pub fn reading(&self) -> RoomRead<'_> {
        let Some(slots) = self.read_slots.as_ref() else {
            return RoomRead::none();
        };
        let slot = &slots.slots[thread_slot() % READ_SLOTS];
        slot.entries.fetch_add(1, Ordering::SeqCst);
        RoomRead(Some(slot))
    }

    /// Note the bytes the pool uses now, and wake the job the first time the arm wants the next step
    pub fn note_used(&self, bytes: usize) {
        self.used.store(bytes, Ordering::Relaxed);
        // a room held short or failed waits for the job's own period, so running short costs no wake a page
        if self.next_step(bytes).is_some()
            && !self.is_short()
            && !self.has_failed()
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
        self.committed() < self.cap() && !self.has_failed() && !self.is_short()
    }

    /// Read the bytes the arm keeps ready past use
    fn lead_bytes(&self) -> usize {
        match grow_policy() {
            GrowPolicy::Extent => self.step(),
            GrowPolicy::Demand => self.unit,
            GrowPolicy::Lead => self.lead.load(Ordering::Relaxed),
        }
    }

    /// Read where the pool grows next with this many bytes used, under the process's arm, up to the room
    pub fn next_step(&self, used: usize) -> Option<usize> {
        let committed = self.committed();
        if committed >= self.cap() {
            return None;
        }
        let free = committed.saturating_sub(used);
        let target = match grow_policy() {
            GrowPolicy::Extent => (free < self.step()).then_some(committed + self.step()),
            GrowPolicy::Demand => (free < self.unit).then_some(committed + self.extent),
            GrowPolicy::Lead => {
                let lead = self.lead.load(Ordering::Relaxed);
                (free < lead).then_some(used + 2 * lead)
            },
        }?;
        Some(target.min(self.cap()))
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
        self.lead.store(rate_lead.max(self.extent), Ordering::Relaxed);
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

    /// Bring the lowest extents not in memory in until this many bytes are, each one populated before it reads as ready. A
    /// populate that fails leaves the room as it was
    pub fn grow_to(&self, bytes: usize) -> Result<()> {
        let target = bytes.min(self.reserved);
        // a writer that grows its own room inline and the job never populate one extent at once, since a touch writes the page
        let _growing = hold(&self.growing);
        while self.committed() < target {
            let ready = self.ready.load(Ordering::Acquire);
            // an extent whose readers haven't all left since its release stays out until its pages have gone back
            let pending = self.read_slots.as_ref().map_or(0, |slots| slots.pending.load(Ordering::Acquire));
            let extent = (!(ready | pending)).trailing_zeros() as usize;
            if extent >= self.extent_count {
                return Ok(());
            }
            let (start, end) = self.extent_range(extent);
            guard_extent(self.base.as_ptr(), start, end - start, true);
            // SAFETY: the extent lies inside the reservation and isn't ready, so no caller reads or writes it
            if let Err(error) = populate(unsafe { self.base.as_ptr().add(start) }, end - start, page_bytes()) {
                self.has_failed.store(true, Ordering::Release);
                return Err(error);
            }
            self.has_failed.store(false, Ordering::Release);
            match &self.pages {
                Some(pages) => {
                    let mut state = hold(&pages.state);
                    let count = self.pages_in(pages, extent);
                    state.heads[extent] = NO_ROOM_PAGE;
                    state.fresh[extent] = 0;
                    state.in_use[extent] = 0;
                    if count > 0 {
                        state.has_free |= 1 << extent;
                    }
                    state.ready_pages += count;
                    self.ready.fetch_or(1 << extent, Ordering::SeqCst);
                    pages.publish(&state);
                },
                None => {
                    self.ready.fetch_or(1 << extent, Ordering::SeqCst);
                },
            }
        }
        Ok(())
    }

    /// Give back the highest empty extents of a paged pool that has held more than an extent past what it wants for DECAY_NANOS,
    /// and return the bytes given back. A pool keeps its lowest extent
    pub fn release_idle(&self, now: u64) -> usize {
        let Some(pages) = &self.pages else {
            return 0;
        };
        let want = self.used() + self.lead_bytes();
        if self.committed() <= want + self.extent {
            self.over_since.store(0, Ordering::Relaxed);
            return 0;
        }
        let since = self.over_since.load(Ordering::Relaxed);
        if since == 0 {
            self.over_since.store(now, Ordering::Relaxed);
            return 0;
        }
        if now.saturating_sub(since) < decay_nanos() {
            return 0;
        }
        let given = self.release_down_to(pages, want + self.extent);
        self.over_since.store(0, Ordering::Relaxed);
        given
    }

    /// Give back the highest empty extents of a paged pool past what it wants now, with no decay and no cap, and return the bytes
    /// given back. The ladder's first step, which costs the pool nothing
    pub fn release_spare(&self) -> usize {
        match &self.pages {
            Some(pages) => self.release_down_to(pages, self.used() + self.lead_bytes()),
            None => 0,
        }
    }

    /// Take empty extents out, highest first, until the room holds no more than this many bytes or none is empty
    fn release_down_to(&self, pages: &RoomPages, limit: usize) -> usize {
        let mut given = 0;
        while self.committed() > limit {
            let Some(extent) = self.take_empty_extent(pages) else {
                break;
            };
            match &self.read_slots {
                // readers that came in before the extent left the ready set may still be in it, so its pages go back on a later step
                Some(slots) => {
                    hold(&slots.busy)[extent] = slots.busy_now(u64::MAX);
                    slots.pending.fetch_or(1 << extent, Ordering::AcqRel);
                },
                None => given += self.give_back_extent(extent),
            }
        }
        given
    }

    /// Give back every extent of a room with no pages that starts at or past this byte, once its owner has dropped all it held
    /// there, and return the bytes given back
    pub fn release_above(&self, bytes: usize) -> usize {
        let _growing = hold(&self.growing);
        let first = bytes.div_ceil(self.extent).max(1);
        let mut given = 0;
        for extent in (first..self.extent_count).rev() {
            if self.ready.load(Ordering::Acquire) & (1 << extent) != 0 {
                self.ready.fetch_and(!(1 << extent), Ordering::SeqCst);
                given += self.give_back_extent(extent);
            }
        }
        given
    }

    /// Check the pool takes and gives its pages through the room
    pub fn is_paged(&self) -> bool {
        self.pages.is_some()
    }


    /// Read the most the room may grow to now: its share, or less while the ladder holds it down
    pub fn cap(&self) -> usize {
        self.cap.load(Ordering::Acquire).min(self.reserved)
    }

    /// Set the most the room may grow to, at least one extent, and wake no one: the job applies it on its next step
    pub fn set_cap(&self, bytes: usize) {
        self.cap.store(bytes.max(self.extent), Ordering::Release);
    }

    /// Give back every extent whose readers have all left since its release took it out of the ready set, and return the bytes
    pub fn finish_releases(&self) -> usize {
        let Some(slots) = &self.read_slots else {
            return 0;
        };
        let mut given = 0;
        let mut pending = slots.pending.load(Ordering::Acquire);
        while pending != 0 {
            let extent = pending.trailing_zeros() as usize;
            pending &= pending - 1;
            let mut busy = hold(&slots.busy);
            busy[extent] = slots.busy_now(busy[extent]);
            if busy[extent] == 0 {
                drop(busy);
                given += self.give_back_extent(extent);
                slots.pending.fetch_and(!(1 << extent), Ordering::AcqRel);
            }
        }
        given
    }

    /// Take the highest empty ready extent past the first out of the ready set, under the page lock
    fn take_empty_extent(&self, pages: &RoomPages) -> Option<usize> {
        let mut state = hold(&pages.state);
        let ready = self.ready.load(Ordering::Acquire);
        let extent = (1..self.extent_count).rev().find(|&extent| ready & (1 << extent) != 0 && state.in_use[extent] == 0)?;
        state.heads[extent] = NO_ROOM_PAGE;
        state.fresh[extent] = 0;
        state.has_free &= !(1 << extent);
        state.ready_pages -= self.pages_in(pages, extent);
        self.ready.fetch_and(!(1 << extent), Ordering::SeqCst);
        pages.publish(&state);
        Some(extent)
    }

    /// Give an extent no longer ready back to the kernel. No page in it is in use, a pool that reads under its own lock never
    /// reaches it, and every reader with no lock that could have seen it ready has left
    fn give_back_extent(&self, extent: usize) -> usize {
        let (start, end) = self.extent_range(extent);
        // SAFETY: the extent lies inside the reservation, isn't ready, holds no page in use, and no reader is left in it
        unsafe { libc::madvise(self.base.as_ptr().add(start).cast(), end - start, libc::MADV_DONTNEED) };
        guard_extent(self.base.as_ptr(), start, end - start, false);
        self.given_back.fetch_add((end - start) as u64, Ordering::Relaxed);
        end - start
    }

    /// Take a free page from the lowest ready extent with one
    pub fn take_page(&self) -> Option<u32> {
        let pages = self.pages.as_ref()?;
        let mut state = hold(&pages.state);
        let extent = state.has_free.trailing_zeros() as usize;
        if extent >= EXTENTS {
            return None;
        }
        let count = self.pages_in(pages, extent);
        let page = if state.heads[extent] != NO_ROOM_PAGE {
            let page = state.heads[extent];
            state.heads[extent] = state.links[page as usize];
            page
        } else {
            let page = extent as u32 * pages.per_extent + state.fresh[extent];
            state.fresh[extent] += 1;
            page
        };
        state.in_use[extent] += 1;
        state.in_use_total += 1;
        if state.heads[extent] == NO_ROOM_PAGE && state.fresh[extent] == count {
            state.has_free &= !(1 << extent);
        }
        pages.publish(&state);
        let used = state.in_use_total as usize * pages.page;
        drop(state);
        self.note_used(used);
        Some(page)
    }

    /// Give a page back to its extent's free chain
    pub fn give_page(&self, page: u32) {
        let Some(pages) = &self.pages else {
            return;
        };
        let mut state = hold(&pages.state);
        let extent = (page / pages.per_extent) as usize;
        state.links[page as usize] = state.heads[extent];
        state.heads[extent] = page;
        state.in_use[extent] -= 1;
        state.in_use_total -= 1;
        state.has_free |= 1 << extent;
        pages.publish(&state);
        let used = state.in_use_total as usize * pages.page;
        drop(state);
        self.note_used(used);
    }

    /// Count the free pages in ready extents, a moment old at most
    pub fn free_pages(&self) -> u32 {
        self.pages.as_ref().map_or(0, |pages| pages.free_now.load(Ordering::Acquire))
    }

    /// Count the pages in ready extents, a moment old at most
    pub fn ready_pages(&self) -> u32 {
        self.pages.as_ref().map_or(0, |pages| pages.ready_now.load(Ordering::Acquire))
    }

    /// Count the pages the room can grow to
    pub fn most_pages(&self) -> u32 {
        self.pages.as_ref().map_or(0, |pages| pages.limit)
    }

    /// Count the ready extents that hold no page in use, for the tests
    pub fn empty_extents(&self) -> usize {
        let Some(pages) = &self.pages else {
            return 0;
        };
        let state = hold(&pages.state);
        let ready = self.ready.load(Ordering::Acquire);
        (0..self.extent_count).filter(|&extent| ready & (1 << extent) != 0 && state.in_use[extent] == 0).count()
    }
}

impl RoomPages {
    /// Publish the counts a caller reads with no lock
    fn publish(&self, state: &PageState) {
        self.ready_now.store(state.ready_pages, Ordering::Release);
        self.free_now.store(state.ready_pages - state.in_use_total, Ordering::Release);
    }
}

/// Close an extent to every access while it is out of memory, in debug builds, so a stray touch stops the test, and open it again
/// before it comes back
fn guard_extent(base: *mut u8, start: usize, bytes: usize, is_open: bool) {
    #[cfg(debug_assertions)]
    {
        let access = if is_open { libc::PROT_READ | libc::PROT_WRITE } else { libc::PROT_NONE };
        // SAFETY: the range lies inside the reservation, and only its access changes
        let _answer = unsafe { libc::mprotect(base.add(start).cast(), bytes, access) };
    }
    #[cfg(not(debug_assertions))]
    let _unused = (base, start, bytes, is_open);
}


impl Drop for PoolRoom {
    fn drop(&mut self) {
        // SAFETY: the room owns the mapping, and the pool that read it is gone
        unsafe { libc::munmap(self.base.as_ptr().cast(), self.mapped) };
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

    fn counted_wakes(room: &PoolRoom) -> Arc<AtomicUsize> {
        let wakes = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&wakes);
        room.set_wake(Arc::new(move || {
            counted.fetch_add(1, Ordering::Relaxed);
        }));
        wakes
    }

    #[test]
    fn grows_a_step_when_less_than_one_is_free() {
        let page = page_bytes();
        let room = PoolRoom::reserve(ByteCount(64 * page as u64), ByteCount(0)).expect("reserve");
        let wakes = counted_wakes(&room);
        assert_eq!((room.extent(), room.step(), room.committed()), (page, 4 * page, 4 * page));
        room.note_used(0);
        assert_eq!(wakes.load(Ordering::Relaxed), 0, "a room with a step free asked to grow");
        room.note_used(page);
        room.note_used(2 * page);
        assert_eq!(wakes.load(Ordering::Relaxed), 1, "a room asked more than once for one step");
        assert!(room.grow_step(&mut u64::MAX.clone()).expect("grow"));
        assert_eq!(room.committed(), 8 * page);
        // SAFETY: the bytes lie in a ready extent
        unsafe { room.base().add(8 * page - 1).write(7) };
        room.grow_to(1 << 40).expect("grow");
        assert_eq!(room.committed(), 64 * page);
        assert_eq!(room.next_step(64 * page), None);
    }

    #[test]
    fn a_short_room_wakes_no_more() {
        let page = page_bytes();
        let room = PoolRoom::reserve(ByteCount(64 * page as u64), ByteCount(0)).expect("reserve");
        let wakes = counted_wakes(&room);
        room.note_used(4 * page);
        assert!(!room.grow_step(&mut 0).expect("grow"));
        assert!(room.is_short() && !room.can_grow());
        for used in 0..1_000 {
            room.note_used(4 * page + used % 2);
        }
        assert_eq!(wakes.load(Ordering::Relaxed), 1, "a short room woke the job past its first ask");
        assert!(room.grow_step(&mut u64::MAX.clone()).expect("grow"));
        assert!(!room.is_short() && room.can_grow());
    }

    #[test]
    fn pages_pack_low_and_empty_extents_go_back() {
        let page = page_bytes();
        let room = PoolRoom::reserve_paged(ByteCount(64 * page as u64), ByteCount(64 * page as u64), page).expect("reserve");
        assert_eq!((room.ready_pages(), room.free_pages()), (64, 64));
        let taken: Vec<u32> = (0..64).map(|_page| room.take_page().expect("take")).collect();
        assert_eq!(room.take_page(), None);
        for &page_number in &taken[32..] {
            room.give_page(page_number);
        }
        assert_eq!(room.empty_extents(), 32);
        assert_eq!(room.take_page(), Some(32), "a take went past the lowest extent with a free page");
        room.give_page(32);
        assert_eq!(room.release_idle(1), 0, "a room gave back before it decayed");
        let given = room.release_idle(1 + DECAY_NANOS);
        let want = 32 * page + room.step();
        assert!(given > 0 && room.committed() <= want + room.extent(), "{given} given, {} left", room.committed());
        for &page_number in &taken[..32] {
            assert!(room.is_ready_at(page_number as usize * page), "a page in use went back");
        }
        assert_eq!(room.free_pages() + 32, room.ready_pages());
        room.grow_to(64 * page).expect("grow");
        let again: Vec<u32> = (0..32).map(|_page| room.take_page().expect("take")).collect();
        assert!(again.iter().all(|&page_number| page_number >= 32));
        // SAFETY: the page lies in an extent that came back
        assert_eq!(unsafe { room.base().add(again[0] as usize * page).read() }, 0);
    }

    #[test]
    fn a_release_waits_for_readers_that_came_in_before_it() {
        let page = page_bytes();
        let room = PoolRoom::reserve_paged(ByteCount(64 * page as u64), ByteCount(64 * page as u64), page).expect("reserve").read_unlocked();
        let taken: Vec<u32> = (0..64).map(|_page| room.take_page().expect("take")).collect();
        for &page_number in &taken[8..] {
            room.give_page(page_number);
        }
        let reading = room.reading();
        assert_eq!(room.release_idle(1), 0);
        assert_eq!(room.release_idle(1 + DECAY_NANOS), 0, "a release went back with a reader inside");
        assert!(!room.is_ready_at(63 * page), "a released extent still reads as ready");
        assert_eq!(room.finish_releases(), 0);
        room.grow_to(64 * page).expect("grow");
        assert!(!room.is_ready_at(63 * page), "an extent its readers hadn't left came back");
        drop(reading);
        assert!(room.finish_releases() > 0, "a release never finished once its readers left");
        // a reader that comes in after the release never holds it back
        let _late = room.reading();
        room.grow_to(64 * page).expect("grow");
        assert!(room.is_ready_at(63 * page));
    }


    // two readers on one slot: the one that came in before the release holds it back after the later one has come and gone
    #[test]
    fn a_later_reader_on_the_slot_never_pays_for_an_earlier_one() {
        let page = page_bytes();
        let room = PoolRoom::reserve_paged(ByteCount(64 * page as u64), ByteCount(64 * page as u64), page).expect("reserve").read_unlocked();
        let _low = room.take_page().expect("take");
        let earlier = room.reading();
        assert_eq!(room.release_idle(1), 0);
        assert_eq!(room.release_idle(1 + DECAY_NANOS), 0);
        drop(room.reading());
        assert_eq!(room.finish_releases(), 0, "a later reader's exit let a release past an earlier reader");
        drop(earlier);
        assert!(room.finish_releases() > 0);
    }


    // the child run of the guard test: release an extent of a guarded room and touch it
    #[test]
    fn touch_a_released_extent() {
        if std::env::var_os("REEL_TOUCH_RELEASED").is_none() {
            return;
        }
        let page = page_bytes();
        let room = PoolRoom::reserve_paged(ByteCount(64 * page as u64), ByteCount(64 * page as u64), page).expect("reserve");
        let _low = room.take_page().expect("take");
        assert!(room.release_idle(1) == 0 && room.release_idle(1 + DECAY_NANOS) > 0);
        assert!(!room.is_ready_at(63 * page));
        // SAFETY: none: the read lands in a released extent on purpose, and the guard stops the process
        let _byte = unsafe { std::ptr::read_volatile(room.base().add(63 * page)) };
    }

    // a debug build closes a released extent, so a stray touch of one stops the process instead of reading zeros
    #[cfg(debug_assertions)]
    #[test]
    fn a_touch_of_a_released_extent_stops_a_debug_build() {
        let status = std::process::Command::new(std::env::current_exe().expect("path"))
            .args(["--exact", "pool_room::tests::touch_a_released_extent", "--test-threads=1"])
            .env("REEL_TOUCH_RELEASED", "1")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .expect("run");
        assert!(std::os::unix::process::ExitStatusExt::signal(&status).is_some(), "a touch of a released extent went through: {status}");
    }


    #[test]
    fn a_large_room_keeps_its_limits_on_huge_pages() {
        let room = PoolRoom::reserve(ByteCount(10 * HUGE_PAGE_BYTES as u64 + 5), ByteCount(1)).expect("reserve");
        assert_eq!(room.base() as usize % HUGE_PAGE_BYTES, 0);
        assert_eq!(room.reserved(), (10 * HUGE_PAGE_BYTES + 5).next_multiple_of(page_bytes()));
        assert_eq!(room.extent(), HUGE_PAGE_BYTES);
        assert_eq!(room.committed() % HUGE_PAGE_BYTES, 0);
        room.note_used(room.committed());
        assert!(room.grow_step(&mut u64::MAX.clone()).expect("grow"));
        assert_eq!(room.committed() % HUGE_PAGE_BYTES, 0);
    }
}
