//! Address space a pool reserves at open and populates in steps, so its memory follows the data
//!
//! The pool takes its whole share as address space and populates a first part. When less than one extent of the part in memory
//! is free, the pool wakes the reserve job, which populates the next extent on its own thread and then publishes the higher limit
//! with one atomic store. A caller only touches memory under the limit and never faults
//!
//! A sorted index's pool also reserves address space past its share, up to what all of max(budget, max_memory) holds and no more
//! than MOST_EXTENTS extents, so an index that outgrows the plan it made at open grows on while the store's target has room. The
//! extents are still cut from the share, so open holds no more than a pool held to its share. Every room in the process reserves
//! at most MOST_PAST_BYTES past its share, and a room past that, or one whose map the host refuses, reserves its share alone and
//! says so in the report
//!
//! A room of a few huge pages or more starts on a huge page and grows in whole huge pages, so no huge page the kernel backs a fault
//! with reaches past the limit. A smaller room asks for no huge pages, so a nearly empty pool holds a page and not two megabytes

use std::io::{Error, ErrorKind, Result};
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use reel_core::units::ByteCount;

use crate::guard::hold;
use crate::pool::HUGE_PAGE_BYTES;
use crate::engine::caller_activity::thread_slot;
use crate::region::{MapInFlight, note_populate, page_bytes, thread_faults, touch_pages};

/// A pool grows by this share of its room at a time, and asks for the next extent when less than one is free
pub const EXTENT_DIVISOR: usize = 16;
/// A room led by its fill keeps ready what its fill takes in this long, measured over the job's last look
pub const LEAD_NANOS: u64 = 100_000_000;
/// The least room that starts on a huge page and grows in whole ones
pub const HUGE_ROOM_BYTES: usize = 4 * HUGE_PAGE_BYTES;

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
    busy: Mutex<Vec<u64>>,
    /// Extents out of the ready set whose pages wait for their readers to leave
    pending: ExtentSet,
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


/// Extents a room's share splits into, one bit each of its ready set
pub const EXTENTS: usize = 64;
/// The most extents a room reserved past its share holds, so its sets and counts stay a few kilobytes
pub const MOST_EXTENTS: usize = 64 * EXTENTS;
/// The most address space every room in the process reserves past its share. Rooms past it reserve their share alone
pub const MOST_PAST_BYTES: usize = 1 << 45;
/// The address space rooms in the process reserve past their shares now
static PAST_RESERVED: AtomicUsize = AtomicUsize::new(0);
/// A ceiling a test sets in place of MOST_PAST_BYTES, in a process of its own. 0 leaves MOST_PAST_BYTES
#[cfg(feature = "test-hooks")]
pub static PAST_CEILING: AtomicUsize = AtomicUsize::new(0);

/// Read the ceiling on address space past the shares in force
fn most_past_bytes() -> usize {
    #[cfg(feature = "test-hooks")]
    {
        let set = PAST_CEILING.load(Ordering::Relaxed);
        if set > 0 {
            return set;
        }
    }
    MOST_PAST_BYTES
}
#[cfg(feature = "test-hooks")]
thread_local! {
    /// Set by a test on its thread to refuse every reservation past a share, as a host that caps address space would
    pub static REFUSE_PAST: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// A set of extents, a bit each over as many words as the room has extents for. One extent's bit lives in one word, so adding or
/// taking out an extent is one atomic write
pub struct ExtentSet {
    words: Box<[AtomicU64]>,
}

impl ExtentSet {
    /// Build an empty set for this many extents
    fn new(count: usize) -> Self {
        Self { words: (0..count.div_ceil(64)).map(|_word| AtomicU64::new(0)).collect() }
    }

    /// Check an extent is in the set
    fn has(&self, extent: usize, order: Ordering) -> bool {
        self.words[extent / 64].load(order) & (1 << (extent % 64)) != 0
    }

    /// Put an extent in the set
    fn add(&self, extent: usize, order: Ordering) {
        self.words[extent / 64].fetch_or(1 << (extent % 64), order);
    }

    /// Take an extent out of the set
    fn take(&self, extent: usize, order: Ordering) {
        self.words[extent / 64].fetch_and(!(1 << (extent % 64)), order);
    }

    /// Find the lowest extent below the count in neither this set nor the other, a word at a time. An extent another thread takes
    /// out between two words' loads reads as present, so the grower passes it this time
    fn first_outside(&self, other: Option<&ExtentSet>, count: usize) -> Option<usize> {
        for (index, word) in self.words.iter().enumerate() {
            let taken = word.load(Ordering::Acquire) | other.map_or(0, |other| other.words[index].load(Ordering::Acquire));
            if taken != u64::MAX {
                let extent = index * 64 + (!taken).trailing_zeros() as usize;
                return (extent < count).then_some(extent);
            }
        }
        None
    }

    /// Read the extents in the set now, a word at a time
    fn members(&self) -> impl Iterator<Item = usize> + '_ {
        self.words.iter().enumerate().flat_map(|(index, word)| {
            let mut bits = word.load(Ordering::Acquire);
            std::iter::from_fn(move || {
                (bits != 0).then(|| {
                    let bit = bits.trailing_zeros() as usize;
                    bits &= bits - 1;
                    index * 64 + bit
                })
            })
        })
    }
}

/// Find the lowest member of a set of plain words
fn lowest(words: &[u64]) -> Option<usize> {
    words.iter().enumerate().find(|(_index, word)| **word != 0).map(|(index, word)| index * 64 + word.trailing_zeros() as usize)
}
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
    /// Each extent's free chain
    heads: Vec<u32>,
    /// Each extent's pages never handed out since it last came into memory, counted from its start
    fresh: Vec<u32>,
    /// Each extent's pages handed out now
    in_use: Vec<u32>,
    /// The ready extents with a free page, a bit each
    has_free: Vec<u64>,
    in_use_total: u32,
    ready_pages: u32,
}

/// The pages a paged pool takes from its room
struct RoomPages {
    page: usize,
    per_extent: u32,
    /// Each free page's next, by page number, in a mapping of its own that comes into memory up to the highest extent that came in,
    /// so a room reserved past its share holds no links for pages it never had
    links: NonNull<u32>,
    links_mapped: usize,
    /// The bytes of links in memory, from the start. It only rises, so no populate touches a page of links twice
    links_ready: AtomicUsize,
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
    /// Bytes the room can grow to: the share, or past it to MOST_EXTENTS extents, in whole system pages
    reserved: usize,
    /// Bytes of the share, in whole system pages, which the extents and the steps are cut from
    share: usize,
    /// The room reserved its share alone where it asked for more: the process passed MOST_PAST_BYTES or the host refused the map
    is_held_to_share: bool,
    /// Bytes of one extent. The last may be shorter
    extent: usize,
    extent_count: usize,
    /// The extents in memory, a bit each. A caller uses memory only in a ready extent
    ready: ExtentSet,
    /// The bytes of the ready extents, which move with each extent that comes in or goes back
    ready_bytes: AtomicUsize,
    /// Set when the last populate failed, and cleared by the next that works
    has_failed: AtomicBool,
    /// Set when the host had no room for the next step, and cleared once a step fits. A summary is never short: its rows must stay,
    /// so its writer grows it inline with no host check, and pools_short can read 0 while the summary is what went over
    is_short: AtomicBool,
    /// Since when the room has held more than an extent past its use with no fill, or 0
    over_since: AtomicU64,
    /// The least use the job saw since over_since, and the use an idle trim left. Use past it is fill
    idle_used: AtomicUsize,
    /// Set by an idle trim, and cleared by the next grow. A trimmed room grows a step once fill comes, and not before
    is_trimmed: AtomicBool,
    /// Set for a room whose fill can outrun a step between the job's looks, a sorted index's pages, which grows by its fill rate
    is_rate_led: AtomicBool,
    /// The bytes its fill took in LEAD_NANOS at the job's last look, and the look before: when, and the bytes used then
    rate_lead: AtomicUsize,
    sample: Mutex<(u64, usize)>,
    /// The part the data on disk asks for, which open brings in when the host has room
    first_part: AtomicUsize,
    /// Bytes the room has given back to the kernel, for the report
    given_back: AtomicU64,
    /// Held across a grow, so two growers never bring in one extent at once
    growing: Mutex<()>,
    /// The most the room may grow to now, which the ladder lowers when the store must hold less
    cap: AtomicUsize,
    /// The most the room may grow to past the ladder's word: the share for a room reserved past it, which only the store's
    /// rebalance raises, and a record cache shard's own size until a rebalance holds it to an extent
    ceiling: AtomicUsize,
    /// Bytes the pool uses now, which its owner notes and the job reads
    used: AtomicUsize,
    /// Set once the pool asked for its next extent, until the job brings it in
    has_asked: AtomicBool,
    wake: OnceLock<GrowWake>,
    /// A slot a thread for readers that take no lock of the pool, which a release waits out, when the pool has such readers
    read_slots: Option<Box<ReadSlots>>,
    pages: Option<RoomPages>,
    /// Set by a test to grow the room whole right after a release takes an extent out, where a grower on another thread can come in
    #[cfg(test)]
    grow_in_release: AtomicBool,
    /// Set when that grow took up the extent the release had just taken out
    #[cfg(test)]
    took_up_in_release: AtomicBool,
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
        Self::reserve_with(share, share, first, None)
    }

    /// Reserve a share, and address space past it up to `most` that the room may grow into whole, so its steps and first part go by
    /// the share while rows that must stay still have room
    pub fn reserve_past(share: ByteCount, most: ByteCount, first: ByteCount) -> Result<Self> {
        let room = Self::reserve_with(share, most, first, None)?;
        room.set_ceiling(room.reserved);
        Ok(room)
    }

    /// Reserve a share for a pool of pages of this size, which takes and gives its pages through the room
    pub fn reserve_paged(share: ByteCount, first: ByteCount, page: usize) -> Result<Self> {
        Self::reserve_with(share, share, first, Some(page))
    }

    /// Reserve a share for a pool of pages of this size, and address space past it up to `most`, so the pool can grow past its
    /// share in extents cut from the share. The room reserves its share alone when the process has reserved MOST_PAST_BYTES past
    /// its shares or the host refuses the map
    pub fn reserve_paged_past(share: ByteCount, most: ByteCount, first: ByteCount, page: usize) -> Result<Self> {
        Self::reserve_with(share, most, first, Some(page))
    }

    /// Reserve a share, and past it up to `most`, split it into extents of a sixty-fourth of the share, and bring the first ones
    /// into memory
    fn reserve_with(share: ByteCount, most: ByteCount, first: ByteCount, page: Option<usize>) -> Result<Self> {
        let system_page = page_bytes();
        let wanted = (share.0 as usize).next_multiple_of(system_page).max(system_page);
        let huge = if wanted >= HUGE_ROOM_BYTES { HUGE_PAGE_BYTES } else { system_page };
        let unit = huge.max(page.unwrap_or(0));
        let extent = (wanted / EXTENTS).next_multiple_of(unit).max(unit);
        let past = (most.0 as usize).next_multiple_of(system_page).min(extent * MOST_EXTENTS).max(wanted);
        // the mapping rounds to whole units, and the room counts and fills only the share, so nothing past the pool's own bytes
        // comes into memory or into the report. The last extent of a large room can end inside a huge page, which the kernel may
        // back whole, so resident can pass the report by under one huge page a room
        let (base, reserved, mapped) = match map_past(wanted, past, unit, huge) {
            Some(mapped) => mapped,
            None => {
                let mapped = wanted.next_multiple_of(unit);
                (map_aligned(mapped, huge)?, wanted, mapped)
            },
        };
        advise_huge_pages(base.as_ptr().cast(), mapped, huge == HUGE_PAGE_BYTES);
        let extent_count = reserved.div_ceil(extent);
        let links_mapped = (reserved / page.unwrap_or(reserved).max(1) * size_of::<u32>()).next_multiple_of(system_page).max(system_page);
        let links = match page {
            Some(_page) => Some(map_aligned(links_mapped, system_page)?.cast::<u32>()),
            None => None,
        };
        let pages = page.zip(links).map(|(page, links)| RoomPages {
            page,
            per_extent: (extent / page) as u32,
            links,
            links_mapped,
            links_ready: AtomicUsize::new(0),
            // a room past its share holds as many pages as its reservation, and one held to its share the share's own
            limit: (if reserved > wanted { reserved } else { share.0 as usize } / page) as u32,
            state: Mutex::new(PageState {
                heads: vec![NO_ROOM_PAGE; extent_count],
                fresh: vec![0; extent_count],
                in_use: vec![0; extent_count],
                has_free: vec![0; extent_count.div_ceil(64)],
                in_use_total: 0,
                ready_pages: 0,
            }),
            free_now: AtomicU32::new(0),
            ready_now: AtomicU32::new(0),
        });
        let room = Self {
            base,
            reserved,
            share: wanted,
            is_held_to_share: past > wanted && reserved == wanted,
            mapped,
            extent,
            extent_count,
            ready: ExtentSet::new(extent_count),
            ready_bytes: AtomicUsize::new(0),
            has_failed: AtomicBool::new(false),
            is_short: AtomicBool::new(false),
            over_since: AtomicU64::new(0),
            idle_used: AtomicUsize::new(0),
            is_trimmed: AtomicBool::new(false),
            is_rate_led: AtomicBool::new(false),
            rate_lead: AtomicUsize::new(0),
            sample: Mutex::new((0, 0)),
            first_part: AtomicUsize::new(0),
            given_back: AtomicU64::new(0),
            growing: Mutex::new(()),
            cap: AtomicUsize::new(usize::MAX),
            // the share as granted, before the page rounding, so the indexes' ceilings together stay under the cap open shared
            ceiling: AtomicUsize::new(if reserved > wanted { share.0 as usize } else { usize::MAX }),
            used: AtomicUsize::new(0),
            has_asked: AtomicBool::new(false),
            wake: OnceLock::new(),
            read_slots: None,
            pages,
            #[cfg(test)]
            grow_in_release: AtomicBool::new(false),
            #[cfg(test)]
            took_up_in_release: AtomicBool::new(false),
        };
        // a whole room comes in at once, with no host check: a reader opens its rooms whole and runs no job to grow them, so a
        // read-only open can still pass what the host holds. Any other room starts with a step, and the part the
        // data on disk asks for comes in at open through the host check, in the rooms' order
        if first.0 >= share.0 {
            room.grow_to(room.share)?;
        } else {
            room.grow_to(room.step())?;
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

    /// Read the bytes of the share, which the extents are cut from
    pub fn share(&self) -> usize {
        self.share
    }

    /// Check the room reserved its share alone where it asked for address space past it
    pub fn is_held_to_share(&self) -> bool {
        self.is_held_to_share
    }

    /// Read the bytes the room has given back to the kernel so far
    pub fn given_back(&self) -> u64 {
        self.given_back.load(Ordering::Relaxed)
    }


    /// Read the bytes of one extent
    pub fn extent(&self) -> usize {
        self.extent
    }

    /// Read the bytes a pool grows by at a time: a sixteenth of the share in whole extents, and one extent at least
    pub fn step(&self) -> usize {
        (self.share / EXTENT_DIVISOR).next_multiple_of(self.extent).max(self.extent)
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

    /// Read the bytes in memory now: every ready extent's, as one count that moves with each extent
    pub fn committed(&self) -> usize {
        self.ready_bytes.load(Ordering::Acquire)
    }

    /// Put an extent in the ready set and count its bytes
    fn add_ready(&self, extent: usize) {
        let (start, end) = self.extent_range(extent);
        self.ready_bytes.fetch_add(end - start, Ordering::AcqRel);
        self.ready.add(extent, Ordering::SeqCst);
    }

    /// Take an extent out of the ready set and its bytes out of the count
    fn take_ready(&self, extent: usize) {
        let (start, end) = self.extent_range(extent);
        self.ready.take(extent, Ordering::SeqCst);
        self.ready_bytes.fetch_sub(end - start, Ordering::AcqRel);
    }

    /// Check the byte at this offset lies in a ready extent
    pub fn is_ready_at(&self, offset: usize) -> bool {
        offset < self.reserved && self.ready.has(offset / self.extent, Ordering::SeqCst)
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
            busy: Mutex::new(vec![0; self.extent_count]),
            pending: ExtentSet::new(self.extent_count),
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

    /// Note a take refused for want of a free page in memory as use of every byte in it, so the job brings in the next step. The
    /// next take or give notes the real use
    pub fn note_wanted(&self) {
        self.note_used(self.committed().max(self.used()));
    }

    /// Note the bytes the pool uses now, and wake the job the first time the pool wants the next step
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

    /// Read where the pool grows next with this many bytes used, up to the room. A pool grows a step when less than one is free,
    /// and a pool an idle trim left grows a step once fill comes. A room led by its fill keeps what its fill takes in LEAD_NANOS
    /// free, and grows by twice that when it passes a step, so a fast fill finds its pages in memory
    pub fn next_step(&self, used: usize) -> Option<usize> {
        let committed = self.committed();
        if committed >= self.cap() {
            return None;
        }
        // the lead's part stops a step and an extent under the ceiling, since a grow brings in whole extents, so a rebalance still
        // sees the room a step from full and raises it first
        let lead = self.rate_lead.load(Ordering::Relaxed);
        let lead_limit = self.ceiling().saturating_sub(self.step() + self.extent);
        let free = committed.saturating_sub(used);
        let wants = if self.is_trimmed.load(Ordering::Acquire) {
            used > self.idle_used.load(Ordering::Relaxed)
        } else {
            free < self.step() || (committed < lead_limit && free < lead)
        };
        let led = (committed + 2 * lead).min(lead_limit);
        wants.then(|| (committed + self.step()).max(led).min(self.cap()))
    }

    /// Lead the room by its fill rate from here on
    pub fn lead_by_rate(&self) {
        self.is_rate_led.store(true, Ordering::Release);
    }

    /// Measure a rate-led room's fill since the job's last look, as the bytes it takes in LEAD_NANOS. A room that emptied or
    /// held still leads by nothing, and grows by steps
    pub fn note_rate(&self, now: u64) {
        if !self.is_rate_led.load(Ordering::Acquire) {
            return;
        }
        let used = self.used();
        let mut sample = hold(&self.sample);
        let (then, used_then) = *sample;
        *sample = (now, used);
        let elapsed = now.saturating_sub(then);
        if then == 0 || elapsed == 0 {
            return;
        }
        let lead = (used.saturating_sub(used_then) as u128 * u128::from(LEAD_NANOS) / u128::from(elapsed)) as usize;
        self.rate_lead.store(lead.next_multiple_of(self.extent), Ordering::Relaxed);
    }

    /// Grow until the pool wants no more or the room is whole, taking no step past the room the host has left, and say whether it
    /// grew. A lead past the room left grows a plain step instead. A pool the host has no room for even a step of stays as it is
    /// and reads as short until a step fits
    pub fn grow_step(&self, host_left: &mut u64) -> Result<bool> {
        let mut grew = false;
        while let Some(led) = self.next_step(self.used()) {
            // a lead the host has no room for falls back to a plain step, which may still fit, so the room grows and isn't short
            let plain = self.committed().saturating_add(self.step()).min(self.cap());
            let is_led_past = led.saturating_sub(self.committed()) as u64 > *host_left;
            let target = if is_led_past && led > plain { plain } else { led };
            let step = target.saturating_sub(self.committed()) as u64;
            if step > *host_left {
                self.is_short.store(true, Ordering::Release);
                break;
            }
            self.grow_to(target)?;
            self.is_trimmed.store(false, Ordering::Release);
            *host_left -= step;
            self.is_short.store(false, Ordering::Release);
            grew = true;
            // the lead is prefetch: once the host's room cut it to a step, the room has what it needs and isn't short of the rest
            if target < led {
                break;
            }
        }
        // a use noted between the last look and this clear asks again on its next note
        self.has_asked.store(false, Ordering::Release);
        Ok(grew)
    }

    /// Bring the lowest extents not in memory in until this many bytes are, each one populated before it reads as ready. A
    /// populate that fails leaves the room as it was
    pub fn grow_to(&self, bytes: usize) -> Result<()> {
        let target = bytes.min(self.ceiling());
        // a writer that grows its own room inline and the job never populate one extent at once, since a touch writes the page
        let _growing = hold(&self.growing);
        while self.committed() < target {
            // an extent whose readers haven't all left since its release stays out until its pages have gone back
            let Some(extent) = self.ready.first_outside(self.read_slots.as_ref().map(|slots| &slots.pending), self.extent_count) else {
                return Ok(());
            };
            let (start, end) = self.extent_range(extent);
            // the links of an extent past any that came in before come into memory first, off the page lock, in whole pages past
            // the last ones, which the growing lock keeps to one grower
            if let Some(pages) = &self.pages {
                let needed = ((end / pages.page) * size_of::<u32>()).next_multiple_of(page_bytes()).min(pages.links_mapped);
                let ready = pages.links_ready.load(Ordering::Acquire);
                if needed > ready {
                    // SAFETY: the range lies inside the links' mapping, past every page of it a caller has used
                    populate(unsafe { pages.links.as_ptr().cast::<u8>().add(ready) }, needed - ready, page_bytes())?;
                    pages.links_ready.store(needed, Ordering::Release);
                }
            }
            guard_extent(self.base.as_ptr(), start, end - start, true);
            // SAFETY: the extent lies inside the reservation and isn't ready, so no caller reads or writes it
            take_pages_back(unsafe { self.base.as_ptr().add(start) }, end - start);
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
                        state.has_free[extent / 64] |= 1 << (extent % 64);
                    }
                    state.ready_pages += count;
                    self.add_ready(extent);
                    pages.publish(&state);
                },
                None => {
                    self.add_ready(extent);
                },
            }
        }
        Ok(())
    }

    /// Give back the highest empty extents of a paged pool that has held more than an extent past its use with no fill for the
    /// decay, down to its use and one extent, and return the bytes given back. A pool keeps its lowest extent
    pub fn release_idle(&self, now: u64) -> usize {
        let Some(pages) = &self.pages else {
            return 0;
        };
        let used = self.used();
        let committed = self.committed();
        if committed <= used + self.extent {
            self.over_since.store(0, Ordering::Relaxed);
            return 0;
        }
        // fill since the job last looked starts the decay over, so a pool in use keeps its step
        let since = self.over_since.load(Ordering::Relaxed);
        if since == 0 || used > self.idle_used.load(Ordering::Relaxed) {
            self.over_since.store(now, Ordering::Relaxed);
            self.idle_used.store(used, Ordering::Relaxed);
            return 0;
        }
        self.idle_used.store(used, Ordering::Relaxed);
        if now.saturating_sub(since) < decay_nanos() {
            return 0;
        }
        let given = self.release_down_to(pages, used + self.extent);
        if self.committed() < committed {
            self.is_trimmed.store(true, Ordering::Release);
        }
        self.over_since.store(0, Ordering::Relaxed);
        given
    }

    /// Give back the highest empty extents of a paged pool past what it wants now, with no decay and no cap, and return the bytes
    /// given back. The ladder's first step, which costs the pool nothing
    pub fn release_spare(&self) -> usize {
        match &self.pages {
            Some(pages) => self.release_down_to(pages, self.used() + self.step()),
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
            #[cfg(test)]
            if self.grow_in_release.load(Ordering::Relaxed) {
                let _grown = self.grow_to(self.reserved);
                if self.ready.has(extent, Ordering::Acquire) {
                    self.took_up_in_release.store(true, Ordering::Relaxed);
                }
            }
            // readers that came in before the extent left the ready set may still be in it, so its pages go back on a later step.
            // take_empty_extent put it in the pending set with its busy slots
            if self.read_slots.is_none() {
                given += self.give_back_extent(extent);
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
            if self.ready.has(extent, Ordering::Acquire) {
                self.take_ready(extent);
                given += self.give_back_extent(extent);
            }
        }
        given
    }

    /// Check the pool has readers that take no lock of it, the value pages, which are a cache of their own
    pub fn has_unlocked_readers(&self) -> bool {
        self.read_slots.is_some()
    }


    /// Check the pool takes and gives its pages through the room
    pub fn is_paged(&self) -> bool {
        self.pages.is_some()
    }


    /// Read the most the room may grow to now: its share, or less while the ladder holds it down, and no more than its ceiling
    pub fn cap(&self) -> usize {
        self.cap.load(Ordering::Acquire).min(self.ceiling()).min(self.reserved)
    }

    /// Read the most the room may grow to past the ladder's word
    pub fn ceiling(&self) -> usize {
        self.ceiling.load(Ordering::Acquire).min(self.reserved)
    }

    /// Set the most the room may grow to past the ladder's word, at least one extent. The ladder's caps and the decay leave it be
    pub fn set_ceiling(&self, bytes: usize) {
        self.ceiling.store(bytes.max(self.extent), Ordering::Release);
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
        // an extent that joins the set past the word already read waits for the next call
        for extent in slots.pending.members() {
            let mut busy = hold(&slots.busy);
            busy[extent] = slots.busy_now(busy[extent]);
            if busy[extent] == 0 {
                drop(busy);
                given += self.give_back_extent(extent);
                slots.pending.take(extent, Ordering::AcqRel);
            }
        }
        given
    }

    /// Take the highest empty ready extent past the first out of the ready set, under the page lock
    fn take_empty_extent(&self, pages: &RoomPages) -> Option<usize> {
        let mut state = hold(&pages.state);
        // the page lock holds every taker and giver of pages off, and an extent leaves the ready set only under it or under the
        // growing lock, which release_above holds for a room with no pages
        let extent = (1..self.extent_count).rev().find(|&extent| self.ready.has(extent, Ordering::Acquire) && state.in_use[extent] == 0)?;
        state.heads[extent] = NO_ROOM_PAGE;
        state.fresh[extent] = 0;
        state.has_free[extent / 64] &= !(1 << (extent % 64));
        state.ready_pages -= self.pages_in(pages, extent);
        // the extent joins the pending set before it leaves the ready set, so no grower takes it up in between, and its busy slots
        // are read after it left, so every reader that saw it ready is among them. A finish waits on the busy lock held across all
        // three, so it never reads the slots of an earlier release
        match &self.read_slots {
            Some(slots) => {
                let mut busy = hold(&slots.busy);
                slots.pending.add(extent, Ordering::AcqRel);
                self.take_ready(extent);
                busy[extent] = slots.busy_now(u64::MAX);
            },
            None => self.take_ready(extent),
        }
        pages.publish(&state);
        Some(extent)
    }

    /// Give an extent no longer ready back to the kernel. No page in it is in use, a pool that reads under its own lock never
    /// reaches it, and every reader with no lock that could have seen it ready has left
    fn give_back_extent(&self, extent: usize) -> usize {
        let (start, end) = self.extent_range(extent);
        // SAFETY: the extent lies inside the reservation, isn't ready, holds no page in use, and no reader is left in it
        give_pages_back(unsafe { self.base.as_ptr().add(start) }, end - start);
        guard_extent(self.base.as_ptr(), start, end - start, false);
        self.given_back.fetch_add((end - start) as u64, Ordering::Relaxed);
        end - start
    }

    /// Take a free page from the lowest ready extent with one
    pub fn take_page(&self) -> Option<u32> {
        let pages = self.pages.as_ref()?;
        let mut state = hold(&pages.state);
        let extent = lowest(&state.has_free)?;
        let count = self.pages_in(pages, extent);
        let page = if state.heads[extent] != NO_ROOM_PAGE {
            let page = state.heads[extent];
            state.heads[extent] = pages.link(page);
            page
        } else {
            let page = extent as u32 * pages.per_extent + state.fresh[extent];
            state.fresh[extent] += 1;
            page
        };
        state.in_use[extent] += 1;
        state.in_use_total += 1;
        if state.heads[extent] == NO_ROOM_PAGE && state.fresh[extent] == count {
            state.has_free[extent / 64] &= !(1 << (extent % 64));
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
        pages.set_link(page, state.heads[extent]);
        state.heads[extent] = page;
        state.in_use[extent] -= 1;
        state.in_use_total -= 1;
        state.has_free[extent / 64] |= 1 << (extent % 64);
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
        (0..self.extent_count).filter(|&extent| self.ready.has(extent, Ordering::Acquire) && state.in_use[extent] == 0).count()
    }
}

impl RoomPages {
    /// Read a free page's next. The caller holds the page lock, and the page lies in an extent that came in, so its link is in
    /// memory
    fn link(&self, page: u32) -> u32 {
        // SAFETY: the page lies in the reservation, so its link lies in the links' mapping, under links_ready
        unsafe { self.links.as_ptr().add(page as usize).read() }
    }

    /// Set a free page's next, under the page lock
    fn set_link(&self, page: u32, next: u32) {
        // SAFETY: the page lies in the reservation, so its link lies in the links' mapping, under links_ready
        unsafe { self.links.as_ptr().add(page as usize).write(next) };
    }

    /// Publish the counts a caller reads with no lock
    fn publish(&self, state: &PageState) {
        self.ready_now.store(state.ready_pages, Ordering::Release);
        self.free_now.store(state.ready_pages - state.in_use_total, Ordering::Release);
    }
}

/// Hand an extent's pages back to the kernel. Linux drops them at once with MADV_DONTNEED. On Apple MADV_DONTNEED is a hint the
/// footprint doesn't show, and MADV_FREE_REUSABLE takes the pages out of it at once
pub(crate) fn give_pages_back(start: *mut u8, bytes: usize) {
    #[cfg(target_vendor = "apple")]
    let advice = libc::MADV_FREE_REUSABLE;
    #[cfg(not(target_vendor = "apple"))]
    let advice = libc::MADV_DONTNEED;
    // SAFETY: the caller passes a range inside a reservation that holds nothing in use, and the advice only drops its pages
    let _answer = unsafe { libc::madvise(start.cast(), bytes, advice) };
}

/// Tell the kernel an extent given back is in use again before it is populated, which Apple asks for after MADV_FREE_REUSABLE so
/// the footprint counts the pages once more. Elsewhere there is nothing to say
pub(crate) fn take_pages_back(start: *mut u8, bytes: usize) {
    #[cfg(target_vendor = "apple")]
    {
        // SAFETY: the caller passes a range inside a reservation that no caller reads or writes yet
        let _answer = unsafe { libc::madvise(start.cast(), bytes, libc::MADV_FREE_REUSE) };
    }
    #[cfg(not(target_vendor = "apple"))]
    let _unused = (start, bytes);
}

/// Hand the whole pages inside a buffer back to the kernel, and return the bytes given. The caller holds every reader and writer of
/// the buffer off until it takes them back with take_back_buffer and fills them again
pub fn give_back_buffer(start: *mut u8, bytes: usize) -> usize {
    let Some((first, length)) = whole_pages(start, bytes) else {
        return 0;
    };
    give_pages_back(first, length);
    length
}

/// Ready the whole pages of a buffer given back for use, before the caller fills them again
pub fn take_back_buffer(start: *mut u8, bytes: usize) {
    if let Some((first, length)) = whole_pages(start, bytes) {
        take_pages_back(first, length);
    }
}

/// Find the whole pages a buffer covers, if it covers any
pub fn whole_pages(start: *mut u8, bytes: usize) -> Option<(*mut u8, usize)> {
    let page = crate::region::page_bytes();
    let begin = (start as usize).next_multiple_of(page);
    let end = (start as usize + bytes) / page * page;
    // the pointer keeps the buffer's provenance, so the kernel gets an address inside it
    (end > begin).then(|| (start.wrapping_add(begin - start as usize), end - begin))
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
        if let Some(pages) = &self.pages {
            // SAFETY: the room owns the links' mapping, and nothing takes or gives a page once the room drops
            unsafe { libc::munmap(pages.links.as_ptr().cast(), pages.links_mapped) };
        }
        PAST_RESERVED.fetch_sub(self.reserved - self.share, Ordering::AcqRel);
    }
}

/// Reserve address space past a share, when the process has room under MOST_PAST_BYTES and the host gives the map, and return the
/// base, the bytes reserved and the bytes mapped. Nothing when the room asks for no more than its share
fn map_past(share: usize, past: usize, unit: usize, huge: usize) -> Option<(NonNull<u8>, usize, usize)> {
    let extra = past.checked_sub(share).filter(|extra| *extra > 0)?;
    #[cfg(feature = "test-hooks")]
    if REFUSE_PAST.with(std::cell::Cell::get) {
        return None;
    }
    let before = PAST_RESERVED.fetch_add(extra, Ordering::AcqRel);
    let mapped = past.next_multiple_of(unit);
    match (before + extra <= most_past_bytes()).then(|| map_aligned(mapped, huge)) {
        Some(Ok(base)) => Some((base, past, mapped)),
        _ => {
            PAST_RESERVED.fetch_sub(extra, Ordering::AcqRel);
            None
        },
    }
}

/// The flags that take address space without charging memory for it
fn reserve_flags() -> libc::c_int {
    libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE
}

/// Reserve address space that starts on a multiple of `align`, giving back the slack around it
/// Map room the kernel doesn't charge until a page is written, for a list that grows into it
pub(crate) fn map_room(bytes: usize) -> Result<NonNull<u8>> {
    map_aligned(bytes, page_bytes())
}

/// Unmap room map_room gave
pub(crate) fn unmap_room(start: *mut u8, bytes: usize) {
    // SAFETY: the caller passes a whole mapping map_room gave, which nothing reads or writes any more
    unsafe { libc::munmap(start.cast(), bytes) };
}

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
pub(crate) fn populate(start: *mut u8, bytes: usize, page: usize) -> Result<()> {
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
    fn an_idle_room_trims_to_its_use_and_an_extent_then_grows_a_step() {
        let page = page_bytes();
        let room = PoolRoom::reserve_paged(ByteCount(256 * page as u64), ByteCount(256 * page as u64), page).expect("reserve");
        let step = room.step();
        assert!(step >= 4 * room.extent(), "a step of {step} leaves no room between it and an extent");
        let taken: Vec<u32> = (0..64).map(|_page| room.take_page().expect("take")).collect();
        for &page_number in &taken[32..] {
            room.give_page(page_number);
        }
        room.note_used(32 * page);
        // fill while the decay runs starts it over
        assert_eq!(room.release_idle(1), 0);
        room.note_used(33 * page);
        assert_eq!(room.release_idle(1 + DECAY_NANOS), 0, "a room in use gave back");
        room.note_used(32 * page);
        assert_eq!(room.release_idle(2 + DECAY_NANOS), 0, "a room gave back before it decayed");
        assert!(room.release_idle(2 + 2 * DECAY_NANOS) > 0, "an idle room gave nothing back");
        assert_eq!(room.committed(), 32 * page + room.extent(), "an idle room kept more than its use and an extent");
        // the job grows nothing back until fill comes
        assert_eq!(room.next_step(32 * page), None, "an idle room grew back with no fill");
        assert!(!room.grow_step(&mut u64::MAX.clone()).expect("grow"));
        room.note_used(32 * page + 1);
        let before = room.committed();
        assert!(room.grow_step(&mut u64::MAX.clone()).expect("grow"));
        assert_eq!(room.committed(), before + step, "a burst after an idle trim grew other than a step");
        // past the burst the room grows by its free room again
        assert_eq!(room.next_step(32 * page + 1), None);
        assert_eq!(room.next_step(room.committed() - step + 1), Some(room.committed() + step));
    }

    // a room led by its rate grows by twice what its fill took between the job's looks. A simulation can't show the marks this
    // saves: a sim's index marks at its exact page count, and the growth lag a fast fill on a real host meets shows in the box's rows
    #[test]
    fn a_rate_led_room_grows_by_twice_its_fill() {
        let page = page_bytes();
        let room = PoolRoom::reserve_paged(ByteCount(1024 * page as u64), ByteCount(0), page).expect("reserve");
        room.lead_by_rate();
        let step = room.step();
        room.note_used(room.committed());
        room.note_rate(1);
        // the fill took four steps between the job's looks LEAD_NANOS apart
        let fill = 4 * step;
        room.note_used(room.committed() + fill);
        room.note_rate(1 + LEAD_NANOS);
        let committed = room.committed();
        let led = (committed + 2 * fill).min(room.ceiling() - step - room.extent()).max(committed + step);
        assert!(led > committed + step, "the room has no space for a lead past a step");
        assert_eq!(room.next_step(room.used()), Some(led.min(room.cap())), "a rate-led room grew by a step");
        // a room that held still leads by nothing again, and grows by its steps
        room.note_rate(1 + 2 * LEAD_NANOS);
        assert_eq!(room.next_step(committed), Some(committed + step));
        // a room not led by its rate grows by its steps whatever its fill
        let plain = PoolRoom::reserve_paged(ByteCount(1024 * page as u64), ByteCount(0), page).expect("reserve");
        plain.note_used(plain.committed());
        plain.note_rate(1);
        plain.note_used(plain.committed() + fill);
        plain.note_rate(1 + LEAD_NANOS);
        assert_eq!(plain.next_step(plain.used()), Some(plain.committed() + plain.step()));
    }

    // a lead the host has no room for grows the room by a plain step, and the room doesn't read as short while a step still fits
    #[test]
    fn a_lead_past_the_room_left_grows_a_step() {
        let page = page_bytes();
        let room = PoolRoom::reserve_paged(ByteCount(1024 * page as u64), ByteCount(0), page).expect("reserve");
        room.lead_by_rate();
        let step = room.step();
        room.note_used(room.committed());
        room.note_rate(1);
        room.note_used(room.committed() + 4 * step);
        room.note_rate(1 + LEAD_NANOS);
        let committed = room.committed();
        let mut host_left = (2 * step) as u64;
        assert!(room.grow_step(&mut host_left).expect("grow"), "a room with a step of room left didn't grow");
        assert!(room.committed() > committed && room.committed() <= committed + 2 * step);
        assert!(!room.is_short(), "a room that grew a step read as short");
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


    // a grow that comes in while a release takes an extent out never takes the extent up under a reader that came in before
    #[test]
    fn a_grow_inside_a_release_waits_for_its_readers() {
        let page = page_bytes();
        let room = PoolRoom::reserve_paged(ByteCount(64 * page as u64), ByteCount(64 * page as u64), page).expect("reserve").read_unlocked();
        let taken: Vec<u32> = (0..64).map(|_page| room.take_page().expect("take")).collect();
        for &page_number in &taken[8..] {
            room.give_page(page_number);
        }
        let reading = room.reading();
        room.grow_in_release.store(true, Ordering::Relaxed);
        assert_eq!(room.release_idle(1), 0);
        let _given = room.release_idle(1 + DECAY_NANOS);
        room.grow_in_release.store(false, Ordering::Relaxed);
        assert!(!room.took_up_in_release.load(Ordering::Relaxed), "a grow took up an extent its reader hadn't left");
        assert!(!room.is_ready_at(63 * page));
        drop(reading);
        assert!(room.finish_releases() > 0);
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


    // a paged room past its share grows, gives back and takes back the extents on both sides of each word's edge
    #[test]
    fn grow_and_give_back_across_words() {
        let page = page_bytes();
        let room = PoolRoom::reserve_paged_past(ByteCount(64 * page as u64), ByteCount(200 * page as u64), ByteCount(0), page)
            .expect("reserve")
            .read_unlocked();
        assert_eq!((room.extent(), room.share(), room.reserved()), (page, 64 * page, 200 * page));
        // a room past its share grows only under the ceiling a rebalance raises, from its share
        assert_eq!(room.ceiling(), 64 * page);
        room.set_ceiling(200 * page);
        assert_eq!(room.most_pages(), 200);
        let mut taken = Vec::new();
        while taken.len() < 130 {
            match room.take_page() {
                Some(page_number) => taken.push(page_number),
                None => assert!(room.grow_step(&mut u64::MAX.clone()).expect("grow"), "a room past its share stopped at {}", taken.len()),
            }
        }
        for extent in [63, 64, 65, 127, 128, 129] {
            assert!(room.is_ready_at(extent * page), "extent {extent} isn't ready");
            // SAFETY: the page lies in a ready extent the room handed out
            unsafe { room.base().add(extent * page).write(extent as u8) };
        }
        assert_eq!(room.committed() % page, 0);
        assert!(room.committed() >= 130 * page);
        // give back everything from extent 8 up, so the release crosses two word edges
        for &page_number in taken.iter().filter(|&&page_number| page_number >= 8) {
            room.give_page(page_number);
        }
        let reading = room.reading();
        assert_eq!(room.release_idle(1), 0);
        assert_eq!(room.release_idle(1 + DECAY_NANOS), 0, "a release went back with a reader inside");
        for extent in [63, 64, 65, 127, 128, 129] {
            assert!(!room.is_ready_at(extent * page), "extent {extent} stayed ready past its release");
        }
        // a grow takes up no extent whose readers haven't left, on either side of an edge
        room.grow_to(200 * page).expect("grow");
        for extent in [63, 64, 65, 127, 128, 129] {
            assert!(!room.is_ready_at(extent * page), "extent {extent} came back under a reader");
        }
        drop(reading);
        assert!(room.finish_releases() > 0, "a release never finished once its reader left");
        room.grow_to(200 * page).expect("grow");
        assert_eq!(room.committed(), 200 * page);
        for extent in [63, 64, 65, 127, 128, 129, 199] {
            assert!(room.is_ready_at(extent * page), "extent {extent} didn't come back");
        }
        assert!(!room.is_ready_at(200 * page));
        // pages pack low again: the first take after the comeback lands at 8, and the free pages count every extent across the edges
        assert_eq!(room.take_page(), Some(8));
        assert_eq!(room.free_pages() + 9, room.ready_pages());
    }

    // a room asks for no more than MOST_EXTENTS extents, and a refused map leaves it its share, which it reports
    #[cfg(feature = "test-hooks")]
    #[test]
    fn a_refused_past_keeps_the_share() {
        let page = page_bytes();
        let wide = PoolRoom::reserve_paged_past(ByteCount(64 * page as u64), ByteCount(u64::MAX / 4), ByteCount(0), page).expect("reserve");
        assert_eq!(wide.reserved(), MOST_EXTENTS * page);
        assert!(!wide.is_held_to_share());
        REFUSE_PAST.with(|refuse| refuse.set(true));
        let held = PoolRoom::reserve_paged_past(ByteCount(64 * page as u64), ByteCount(200 * page as u64), ByteCount(0), page).expect("reserve");
        REFUSE_PAST.with(|refuse| refuse.set(false));
        assert_eq!((held.reserved(), held.most_pages()), (64 * page, 64));
        assert!(held.is_held_to_share());
        assert_eq!(held.committed(), wide.committed(), "a room past its share holds more at open");
    }

    // the child run of the ceiling test: rooms past their shares reserve up to the ceiling, the ones past it open on their shares,
    // and a room dropped makes way for the next
    #[cfg(feature = "test-hooks")]
    #[test]
    fn rooms_past_the_ceiling_keep_their_shares() {
        if std::env::var_os("REEL_PAST_CEILING").is_none() {
            return;
        }
        let page = page_bytes();
        let (share, past) = (ByteCount(64 * page as u64), ByteCount(1024 * page as u64));
        PAST_CEILING.store(10 * 960 * page, Ordering::Relaxed);
        let mut rooms: Vec<PoolRoom> = (0..20).map(|_room| PoolRoom::reserve_paged_past(share, past, ByteCount(0), page).expect("reserve")).collect();
        let held: Vec<bool> = rooms.iter().map(PoolRoom::is_held_to_share).collect();
        assert_eq!(held, [vec![false; 10], vec![true; 10]].concat());
        assert!(rooms.iter().all(|room| room.committed() == rooms[0].committed()), "a room past its share held more at open");
        rooms.truncate(5);
        let again = PoolRoom::reserve_paged_past(share, past, ByteCount(0), page).expect("reserve");
        assert!(!again.is_held_to_share(), "a room dropped gave no way to the next");
    }

    // the ceiling test runs in a process of its own, since the ceiling and the address space past the shares are the process's
    #[cfg(feature = "test-hooks")]
    #[test]
    fn the_past_ceiling_holds_rooms_to_their_shares() {
        let status = std::process::Command::new(std::env::current_exe().expect("path"))
            .args(["--exact", "pool_room::tests::rooms_past_the_ceiling_keep_their_shares", "--test-threads=1"])
            .env("REEL_PAST_CEILING", "1")
            .stdout(std::process::Stdio::null())
            .status()
            .expect("run");
        assert!(status.success(), "the ceiling run failed: {status}");
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
