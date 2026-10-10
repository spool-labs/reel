//! Per-segment reclaimable-byte counters, and what a read reports about itself

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{RwLock, RwLockReadGuard};

use crate::format::loc::{SegmentId, SegmentIncarnation};
use crate::format::lsn::Lsn;
use crate::sync::{read, write};

/// Reclaimable-byte counters for one segment, leaving out pads and segment headers
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SegmentBytes {
    /// Footprint of records the index still resolves into this segment
    pub live: u64,

    /// Footprint of records shadowed by a newer version or a delete
    pub dead: u64,

    /// Tombstone footprint, counted inside live
    pub held: u64,

    /// The newest tombstone version in this segment, if any
    pub held_lsn: Option<Lsn>,
}

impl SegmentBytes {
    /// Move a record footprint from live to dead within this segment
    pub fn shadow(&mut self, span: u64) {
        self.live = self.live.saturating_sub(span);
        self.dead = self.dead.saturating_add(span);
    }

    /// Total footprint accounted to this segment
    pub fn total(&self) -> u64 {
        self.live + self.dead
    }

    /// How many tombstone bytes a rewrite would drop, all of them once no older data survives
    pub fn droppable(&self, floor: Option<Lsn>) -> u64 {
        match (self.held_lsn, floor) {
            (None, _) => 0,
            (Some(_), None) => self.held,
            (Some(newest), Some(floor)) if newest < floor => self.held,
            (Some(_), Some(_)) => 0,
        }
    }

    /// How many bytes a rewrite of this segment would reclaim
    pub fn reclaimable(&self, floor: Option<Lsn>) -> u64 {
        self.dead + self.droppable(floor)
    }
}

/// The two oldest record marks on a reel, enough to answer any one exclusion
#[derive(Clone, Copy, Debug, Default)]
pub struct Floors {
    /// The oldest mark on the reel and the segment that holds it
    oldest: Option<(SegmentId, Lsn)>,

    /// The oldest mark in any other segment
    runner_up: Option<Lsn>,
}

impl Floors {
    /// Fold one segment's mark in
    fn see(&mut self, segment: SegmentId, lsn: Lsn) {
        match self.oldest {
            Some((_, oldest)) if oldest <= lsn => {
                self.runner_up = Some(match self.runner_up {
                    Some(current) if current <= lsn => current,
                    Some(_) | None => lsn,
                });
            }
            Some((_, oldest)) => {
                self.runner_up = Some(match self.runner_up {
                    Some(current) if current <= oldest => current,
                    Some(_) | None => oldest,
                });
                self.oldest = Some((segment, lsn));
            }
            None => self.oldest = Some((segment, lsn)),
        }
    }

    /// The oldest record any segment but this one can still surface
    pub fn excluding(&self, segment: SegmentId) -> Option<Lsn> {
        match self.oldest {
            Some((holder, _)) if holder == segment => self.runner_up,
            Some((_, oldest)) => Some(oldest),
            None => None,
        }
    }
}

/// Each `Chunk` holds this many rows, so retiring frees whole allocations
const CHUNK: usize = 256;

/// The window spans at most this many ids, and a booking past that is refused
const MAX_WINDOW: u64 = 1 << 20;

/// The row is counted: it holds bytes, ranks, and answers for its floor
const PRESENT: u32 = 1;

/// The segment retired, so every booking against it is dropped and counted
const RETIRED: u32 = 2;

/// One segment's counters, on its own cache line so two segments never share one
#[repr(align(64))]
#[derive(Debug)]
struct SegmentRow {
    /// How many bytes the index still points at
    live: AtomicU64,

    /// Bytes shadowed by an overwrite or a delete
    dead: AtomicU64,

    /// Tombstone bytes, counted inside live
    held: AtomicU64,

    /// The oldest data version the segment can still surface, or `u64::MAX` before any
    min_lsn: AtomicU64,

    /// The newest lsn in the segment's sealed footer, or zero for none
    max_lsn: AtomicU64,

    /// The frontier the segment's footer tally is current to, or zero if none was read
    sealed_at: AtomicU64,

    /// The newest tombstone version here, or zero for none
    held_lsn: AtomicU64,

    /// The segment's current incarnation, or zero if none was issued
    incarnation: AtomicU32,

    /// The present and retired bits, so one load reads both
    flags: AtomicU32,
}

impl SegmentRow {
    /// A blank row for a segment nothing has booked against yet
    fn new() -> SegmentRow {
        SegmentRow {
            live: AtomicU64::new(0),
            dead: AtomicU64::new(0),
            held: AtomicU64::new(0),
            min_lsn: AtomicU64::new(u64::MAX),
            max_lsn: AtomicU64::new(Lsn::NONE.as_u64()),
            sealed_at: AtomicU64::new(Lsn::NONE.as_u64()),
            held_lsn: AtomicU64::new(Lsn::NONE.as_u64()),
            incarnation: AtomicU32::new(0),
            flags: AtomicU32::new(0),
        }
    }

    fn flags(&self) -> u32 {
        self.flags.load(Ordering::Acquire)
    }

    fn is_present(&self) -> bool {
        self.flags() & PRESENT != 0
    }

    fn is_retired(&self) -> bool {
        self.flags() & RETIRED != 0
    }

    /// Whether this row ever had a flag set or an incarnation issued
    fn is_known(&self) -> bool {
        self.flags() != 0 || self.incarnation.load(Ordering::Acquire) != 0
    }

    fn raise(&self, bits: u32) {
        self.flags.fetch_or(bits, Ordering::AcqRel);
    }

    fn add_live(&self, span: u64) {
        self.live.fetch_add(span, Ordering::AcqRel);
    }

    fn drop_live(&self, span: u64) {
        let _ = self
            .live
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
                Some(held.saturating_sub(span))
            });
    }

    fn shadow(&self, span: u64) {
        self.drop_live(span);
        self.dead.fetch_add(span, Ordering::AcqRel);
    }

    fn bytes(&self) -> SegmentBytes {
        SegmentBytes {
            live: self.live.load(Ordering::Acquire),
            dead: self.dead.load(Ordering::Acquire),
            held: self.held.load(Ordering::Acquire),
            held_lsn: match self.held_lsn.load(Ordering::Acquire) {
                0 => None,
                newest => Some(Lsn(newest)),
            },
        }
    }

    /// Reset the row to its untouched state
    fn blank(&self) {
        self.live.store(0, Ordering::Release);
        self.dead.store(0, Ordering::Release);
        self.held.store(0, Ordering::Release);
        self.min_lsn.store(u64::MAX, Ordering::Release);
        self.max_lsn.store(Lsn::NONE.as_u64(), Ordering::Release);
        self.sealed_at.store(Lsn::NONE.as_u64(), Ordering::Release);
        self.held_lsn.store(Lsn::NONE.as_u64(), Ordering::Release);
        self.incarnation.store(0, Ordering::Release);
        self.flags.store(0, Ordering::Release);
    }

    fn min_lsn(&self) -> Option<Lsn> {
        match self.min_lsn.load(Ordering::Acquire) {
            u64::MAX => None,
            min => Some(Lsn(min)),
        }
    }

    /// Note the oldest sequence number this segment can still surface
    fn note_min(&self, lsn: Lsn) {
        if lsn.0 < self.min_lsn.load(Ordering::Acquire) {
            self.min_lsn.fetch_min(lsn.0, Ordering::AcqRel);
        }
    }
}

/// A fixed block of rows, freed whole when the window slides past it
#[derive(Debug)]
struct Chunk {
    rows: Box<[SegmentRow]>,
}

impl Chunk {
    fn new() -> Chunk {
        let mut rows = Vec::with_capacity(CHUNK);
        rows.resize_with(CHUNK, SegmentRow::new);
        Chunk {
            rows: rows.into_boxed_slice(),
        }
    }

    fn at(&self, slot: usize) -> &SegmentRow {
        &self.rows[slot]
    }
}

/// The range of segment numbers the table has rows for, sliding up as segments retire
#[derive(Debug, Default)]
struct Window {
    /// The segment number of the first block's row zero, aligned to `CHUNK`
    base: u64,

    /// Numbers below this had their block freed
    gone: u64,

    /// One past the highest counted number, which bounds every walk
    reach: u64,

    /// Rows in number order, oldest block first
    chunks: VecDeque<Chunk>,

    /// How many rows have the present bit, so a count needs no walk
    present: usize,
}

impl Window {
    /// The row for a number, retired and untouched ones included
    fn row(&self, segment: SegmentId) -> Option<&SegmentRow> {
        let id = u64::from(segment.as_u32());
        if id < self.gone || id < self.base {
            return None;
        }
        let at = (id - self.base) as usize;
        self.chunks
            .get(at / CHUNK)
            .map(|chunk| chunk.at(at % CHUNK))
    }

    /// The row of a segment still being counted, or nothing for one that is not
    fn counted(&self, segment: SegmentId) -> Option<&SegmentRow> {
        self.row(segment).filter(|row| row.is_present())
    }

    /// Whether the table has already let go of a number
    fn is_retired(&self, segment: SegmentId) -> bool {
        let id = u64::from(segment.as_u32());
        if id < self.gone {
            return true;
        }
        self.row(segment).is_some_and(|row| row.is_retired())
    }

    /// The row for a number, growing the window to reach it, or nothing once it retired
    fn open(&mut self, segment: SegmentId) -> Option<&SegmentRow> {
        let id = u64::from(segment.as_u32());
        if id < self.gone {
            return None;
        }
        if self.chunks.is_empty() {
            self.base = id - (id % CHUNK as u64);
        }
        if id < self.base {
            // Never retired, so grow the window back to reach it
            let reach = self.base - (id - (id % CHUNK as u64));
            if reach > MAX_WINDOW {
                return None;
            }
            for _ in 0..(reach as usize / CHUNK) {
                self.chunks.push_front(Chunk::new());
            }
            self.base -= reach;
        }
        let at = (id - self.base) as usize;
        if at >= MAX_WINDOW as usize {
            return None;
        }
        while at >= self.chunks.len() * CHUNK {
            self.chunks.push_back(Chunk::new());
        }
        let row = self.chunks[at / CHUNK].at(at % CHUNK);
        match row.is_retired() {
            true => None,
            false => Some(row),
        }
    }

    /// Open a row and start it counting, for a caller vouching the segment stands
    fn open_counted(&mut self, segment: SegmentId) -> Option<&SegmentRow> {
        let fresh = {
            let row = self.open(segment)?;
            row.flags.fetch_or(PRESENT, Ordering::AcqRel) & PRESENT == 0
        };
        if fresh {
            self.present += 1;
        }
        self.reach = self.reach.max(u64::from(segment.as_u32()) + 1);
        self.row(segment)
    }

    /// Clear a retired segment's row and free the blocks behind it
    fn retire(&mut self, segment: SegmentId) {
        let cleared = self.row(segment).filter(|row| row.is_known()).map(|row| {
            let was_present = row.is_present();
            row.blank();
            row.raise(RETIRED);
            was_present
        });
        if let Some(was_present) = cleared {
            self.present -= usize::from(was_present);
        }
        if self.present == 0 {
            self.reach = self.base;
        }
        self.slide();
    }

    /// Free the blocks behind a run of retired rows, stopping at any row not retired
    fn slide(&mut self) {
        let mut at = self.base.max(self.gone);
        while let Some(row) = self.row(SegmentId(at as u32)) {
            if !row.is_retired() {
                break;
            }
            at += 1;
        }
        self.reach = self.reach.max(at);
        while at >= self.base + CHUNK as u64 && !self.chunks.is_empty() {
            self.chunks.pop_front();
            self.base += CHUNK as u64;
            self.gone = self.base;
        }
    }

    /// Every counted row and its number in order, walking only up to `reach`
    fn counted_rows(&self) -> impl Iterator<Item = (SegmentId, &SegmentRow)> {
        let from = (self.gone.max(self.base) - self.base) as usize;
        let to = (self.reach.max(self.base) - self.base) as usize;
        self.chunks.iter().enumerate().flat_map(move |(at, chunk)| {
            let start = at * CHUNK;
            let low = from.saturating_sub(start).min(CHUNK);
            let high = to.saturating_sub(start).min(CHUNK);
            (low..high).filter_map(move |slot| {
                let row = chunk.at(slot);
                match row.is_present() {
                    true => Some((SegmentId((self.base + (start + slot) as u64) as u32), row)),
                    false => None,
                }
            })
        })
    }

    fn clear(&mut self) {
        self.chunks.clear();
        self.base = 0;
        self.gone = 0;
        self.reach = 0;
        self.present = 0;
    }
}

/// The segment table held for reading, for a caller looking up many segments at once
pub struct Stamps<'table>(RwLockReadGuard<'table, Window>);

impl Stamps<'_> {
    /// The incarnation a segment currently wears, or none for one that is gone
    pub fn of(&self, segment: SegmentId) -> SegmentIncarnation {
        match self.0.row(segment) {
            Some(row) => SegmentIncarnation(row.incarnation.load(Ordering::Acquire)),
            None => SegmentIncarnation::NONE,
        }
    }
}

/// Per-segment reclaimable-byte counters for a whole reel
#[derive(Debug, Default)]
pub struct SegmentTable {
    /// The rows themselves, a window over the numbers this volume has issued
    window: RwLock<Window>,

    /// Issues incarnations, starting past the reserved none
    next_incarnation: AtomicU32,

    /// How many bookings hit a segment this table already let go of
    dropped: AtomicU64,
}

impl SegmentTable {
    /// An empty table, which fills as segments take their first record
    pub fn new() -> SegmentTable {
        SegmentTable::default()
    }

    /// Book a record live in its segment, and note the version it holds
    pub fn mark_live(&self, segment: SegmentId, lsn: Lsn, span: u64) {
        self.opened(segment, |row| {
            row.note_min(lsn);
            row.add_live(span);
        });
    }

    /// Book a record dead where it lies, and note its version because a rebuild would still find it
    pub fn mark_dead(&self, segment: SegmentId, lsn: Lsn, span: u64) {
        self.opened(segment, |row| {
            row.note_min(lsn);
            row.dead.fetch_add(span, Ordering::AcqRel);
        });
    }

    /// Book a tombstone's footprint against its segment, keeping the newest tombstone version
    pub fn mark_held(&self, segment: SegmentId, lsn: Lsn, span: u64) {
        self.opened(segment, |row| {
            row.add_live(span);
            row.held.fetch_add(span, Ordering::AcqRel);
            row.held_lsn.fetch_max(lsn.as_u64(), Ordering::AcqRel);
        });
    }

    /// Add footprints a rebuild read off a footer tally
    pub fn adopt(&self, segment: SegmentId, bytes: SegmentBytes) {
        self.opened(segment, |row| {
            row.add_live(bytes.live);
            row.dead.fetch_add(bytes.dead, Ordering::AcqRel);
            row.held.fetch_add(bytes.held, Ordering::AcqRel);
            if let Some(lsn) = bytes.held_lsn {
                row.held_lsn.fetch_max(lsn.as_u64(), Ordering::AcqRel);
            }
        });
    }

    /// Move a record's footprint from live to dead within its segment
    pub fn shadow(&self, segment: SegmentId, span: u64) {
        self.booked(segment, |row| row.shadow(span));
    }

    /// Raise a segment's dead count to what a full sweep counted, never lowering it
    pub fn settle_dead(&self, segment: SegmentId, counted: u64) {
        self.booked(segment, |row| {
            let dead = row.dead.load(Ordering::Acquire);
            if counted > dead {
                row.shadow(counted - dead);
            }
        });
    }

    /// Take a record's footprint off a segment's live count without booking it dead
    pub fn release_live(&self, segment: SegmentId, span: u64) {
        self.booked(segment, |row| row.drop_live(span));
    }

    /// Note the oldest sequence number a segment can still surface
    pub fn note_min(&self, segment: SegmentId, lsn: Lsn) {
        self.opened(segment, |row| row.note_min(lsn));
    }

    /// Raise a segment's ceiling to the newest row its sealed footer holds
    pub fn note_max(&self, segment: SegmentId, lsn: Lsn) {
        // An empty footer bounds nothing, and opening a row would rank a segment with no bytes
        if lsn == Lsn::NONE {
            return;
        }
        self.opened(segment, |row| {
            row.max_lsn.fetch_max(lsn.as_u64(), Ordering::AcqRel);
        });
    }

    /// Note the frontier a sealed segment's footer says its tally is current to
    pub fn note_sealed_at(&self, segment: SegmentId, lsn: Lsn) {
        if lsn == Lsn::NONE {
            return;
        }
        self.opened(segment, |row| {
            row.sealed_at.fetch_max(lsn.as_u64(), Ordering::AcqRel);
        });
    }

    /// The frontier a segment's tally is current to, or nothing if no footer was read
    pub fn sealed_at_of(&self, segment: SegmentId) -> Option<Lsn> {
        let window = read(&self.window);
        let row = window.counted(segment)?;
        match row.sealed_at.load(Ordering::Acquire) {
            0 => None,
            frontier => Some(Lsn(frontier)),
        }
    }

    /// The newest row a segment can answer with, where nothing means unknown and no bound
    pub fn max_lsn_of(&self, segment: SegmentId) -> Option<Lsn> {
        let window = read(&self.window);
        let row = window.counted(segment)?;
        match row.max_lsn.load(Ordering::Acquire) {
            0 => None,
            max => Some(Lsn(max)),
        }
    }

    /// Reclaimable-byte counters for one segment
    pub fn bytes_of(&self, segment: SegmentId) -> SegmentBytes {
        read(&self.window)
            .counted(segment)
            .map(|row| row.bytes())
            .unwrap_or_default()
    }

    /// Per-segment live and dead footprints, for choosing a compaction target
    pub fn snapshot(&self) -> Vec<(SegmentId, SegmentBytes)> {
        let window = read(&self.window);
        let mut out = Vec::with_capacity(window.present);
        for (segment, row) in window.counted_rows() {
            out.push((segment, row.bytes()));
        }
        out
    }

    /// Everything ranking a compaction target needs, from one pass under one lock
    pub fn ranking(&self) -> (Vec<(SegmentId, SegmentBytes)>, Floors) {
        let window = read(&self.window);
        let mut out = Vec::with_capacity(window.present);
        let mut floors = Floors::default();
        for (segment, row) in window.counted_rows() {
            out.push((segment, row.bytes()));
            if let Some(min) = row.min_lsn() {
                floors.see(segment, min);
            }
        }
        (out, floors)
    }

    /// The oldest record this segment can still surface
    pub fn min_lsn_of(&self, segment: SegmentId) -> Option<Lsn> {
        read(&self.window).counted(segment)?.min_lsn()
    }

    /// Reclaimable bytes across every segment, the dead-space gauge
    pub fn dead_bytes(&self) -> u64 {
        let mut total = 0u64;
        for (_, row) in read(&self.window).counted_rows() {
            total += row.dead.load(Ordering::Acquire);
        }
        total
    }

    /// The oldest lsn any other segment can surface, below which a tombstone can be dropped
    pub fn min_lsn_excluding(&self, segment: SegmentId) -> Option<Lsn> {
        self.floors().excluding(segment)
    }

    /// Every segment's floor from one pass, since only the two oldest marks matter
    pub fn floors(&self) -> Floors {
        let window = read(&self.window);
        let mut floors = Floors::default();
        for (segment, row) in window.counted_rows() {
            if let Some(min) = row.min_lsn() {
                floors.see(segment, min);
            }
        }
        floors
    }

    /// How many bookings were dropped for a segment this table already let go of
    pub fn dropped_bookings(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// A live segment's incarnation, issued on first ask, or none for a retired segment
    pub fn live_incarnation(&self, segment: SegmentId) -> SegmentIncarnation {
        if let Some(row) = read(&self.window).row(segment) {
            match row.incarnation.load(Ordering::Acquire) {
                0 => {}
                worn => return SegmentIncarnation(worn),
            }
        }
        let mut window = write(&self.window);
        let Some(row) = window.open(segment) else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return SegmentIncarnation::NONE;
        };
        SegmentIncarnation(match row.incarnation.load(Ordering::Acquire) {
            0 => {
                let issued = self.issue_incarnation();
                row.incarnation.store(issued.0, Ordering::Release);
                issued.0
            }
            worn => worn,
        })
    }

    /// The incarnation a segment currently wears, or none for one that is gone
    pub fn incarnation_of(&self, segment: SegmentId) -> SegmentIncarnation {
        self.stamps().of(segment)
    }

    /// The table held for reading, so a batch of lookups takes its lock once
    pub fn stamps(&self) -> Stamps<'_> {
        Stamps(read(&self.window))
    }

    fn issue_incarnation(&self) -> SegmentIncarnation {
        SegmentIncarnation(self.next_incarnation.fetch_add(1, Ordering::Relaxed) + 1)
    }

    /// Forget a segment's counters once its file has been unlinked
    pub fn forget(&self, segment: SegmentId) {
        // Counters and incarnation clear together, so no stamp outlives its counters
        write(&self.window).retire(segment);
    }

    /// Issue an incarnation to each sealed segment a rebuild left, for stamping its footer entries
    pub fn issue_incarnations(&self, segments: impl IntoIterator<Item = SegmentId>) {
        let mut window = write(&self.window);
        for segment in segments {
            let Some(row) = window.open(segment) else {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            };
            if row.incarnation.load(Ordering::Acquire) == 0 {
                let issued = self.issue_incarnation();
                row.incarnation.store(issued.0, Ordering::Release);
            }
        }
    }

    /// How many segments the table is counting
    pub fn len(&self) -> usize {
        read(&self.window).present
    }

    /// Whether the table counts nothing
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drop every row, for a reel that is going away
    pub fn clear(&self) {
        write(&self.window).clear();
    }

    /// Issue incarnations past every one the live table issued, for a rebuild that swaps into it
    pub(crate) fn issue_past(&self, live: &SegmentTable) {
        let issued = live.next_incarnation.load(Ordering::Relaxed);
        self.next_incarnation.fetch_max(issued, Ordering::Relaxed);
    }

    /// Swap in a rebuilt table's rows, keeping incarnations and drop counts climbing
    pub(crate) fn install(&self, fresh: &SegmentTable) {
        std::mem::swap(&mut *write(&self.window), &mut *write(&fresh.window));
        let issued = fresh.next_incarnation.load(Ordering::Relaxed);
        self.next_incarnation.fetch_max(issued, Ordering::Relaxed);
        let dropped = fresh.dropped.load(Ordering::Relaxed);
        self.dropped.fetch_add(dropped, Ordering::Relaxed);
    }

    /// Run `act` on a segment's row, opening it on first touch, or drop a booking to a retired one
    fn opened<T>(&self, segment: SegmentId, act: impl FnOnce(&SegmentRow) -> T) -> Option<T> {
        {
            let window = read(&self.window);
            if let Some(row) = window.counted(segment) {
                return Some(act(row));
            }
            if window.is_retired(segment) {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        }
        let mut window = write(&self.window);
        match window.open_counted(segment) {
            Some(row) => Some(act(row)),
            None => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Run `act` on an existing counted row, or drop the booking if there is none
    fn booked<T>(&self, segment: SegmentId, act: impl FnOnce(&SegmentRow) -> T) -> Option<T> {
        match read(&self.window).counted(segment) {
            Some(row) => Some(act(row)),
            None => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }
}

/// Where an index write books its segments' bytes, the table itself or one rebuild thread's tally
pub trait Bookings {
    /// The incarnation stamped on an entry pointing into a segment
    fn live_incarnation(&self, segment: SegmentId) -> SegmentIncarnation;

    /// Book a record live in its segment, and note the version it holds
    fn mark_live(&self, segment: SegmentId, lsn: Lsn, span: u64);

    /// Book a record dead where it lies, and note the version it holds
    fn mark_dead(&self, segment: SegmentId, lsn: Lsn, span: u64);

    /// Move a record's footprint from live to dead within its segment
    fn shadow(&self, segment: SegmentId, span: u64);

    /// Book a tombstone's own footprint against the segment holding it
    fn mark_held(&self, segment: SegmentId, lsn: Lsn, span: u64);
}

impl Bookings for SegmentTable {
    fn live_incarnation(&self, segment: SegmentId) -> SegmentIncarnation {
        self.live_incarnation(segment)
    }

    fn mark_live(&self, segment: SegmentId, lsn: Lsn, span: u64) {
        self.mark_live(segment, lsn, span)
    }

    fn mark_dead(&self, segment: SegmentId, lsn: Lsn, span: u64) {
        self.mark_dead(segment, lsn, span)
    }

    fn shadow(&self, segment: SegmentId, span: u64) {
        self.shadow(segment, span)
    }

    fn mark_held(&self, segment: SegmentId, lsn: Lsn, span: u64) {
        self.mark_held(segment, lsn, span)
    }
}

/// One rebuild thread's bookings, summed per segment until it hands them over
pub struct Tally<'table> {
    table: &'table SegmentTable,
    rows: std::cell::RefCell<Vec<TallyRow>>,
    last: std::cell::Cell<usize>,
}

/// What a tally has summed for one segment
struct TallyRow {
    segment: SegmentId,
    incarnation: Option<SegmentIncarnation>,
    bytes: SegmentBytes,
    shadowed: u64,
    oldest: Option<Lsn>,
}

impl<'table> Tally<'table> {
    /// An empty tally that settles into this table
    pub fn new(table: &'table SegmentTable) -> Tally<'table> {
        Tally {
            table,
            rows: std::cell::RefCell::new(Vec::new()),
            last: std::cell::Cell::new(0),
        }
    }

    /// Change one segment's sums, trying the row asked about last before any search
    fn with<T>(&self, segment: SegmentId, change: impl FnOnce(&mut TallyRow) -> T) -> T {
        let mut rows = self.rows.borrow_mut();
        let last = self.last.get();
        let found = match rows.get(last) {
            Some(row) if row.segment == segment => Some(last),
            _ => rows.iter().rposition(|row| row.segment == segment),
        };
        let at = match found {
            Some(at) => at,
            None => {
                rows.push(TallyRow {
                    segment,
                    incarnation: None,
                    bytes: SegmentBytes::default(),
                    shadowed: 0,
                    oldest: None,
                });
                rows.len() - 1
            }
        };
        self.last.set(at);
        change(&mut rows[at])
    }

    /// Hand every sum to the table, live bytes first so no count dips below zero
    pub fn settle(self) {
        let rows = self.rows.into_inner();
        for row in &rows {
            if row.bytes != SegmentBytes::default() {
                self.table.adopt(row.segment, row.bytes);
            }
            if let Some(oldest) = row.oldest {
                self.table.note_min(row.segment, oldest);
            }
        }
        for row in rows.iter().filter(|row| row.shadowed > 0) {
            self.table.shadow(row.segment, row.shadowed);
        }
    }
}

impl TallyRow {
    fn note_min(&mut self, lsn: Lsn) {
        self.oldest = Some(self.oldest.map_or(lsn, |oldest| oldest.min(lsn)));
    }
}

impl Bookings for Tally<'_> {
    fn live_incarnation(&self, segment: SegmentId) -> SegmentIncarnation {
        if let Some(known) = self.with(segment, |row| row.incarnation) {
            return known;
        }
        let issued = self.table.live_incarnation(segment);
        self.with(segment, |row| row.incarnation = Some(issued));
        issued
    }

    fn mark_live(&self, segment: SegmentId, lsn: Lsn, span: u64) {
        self.with(segment, |row| {
            row.note_min(lsn);
            row.bytes.live += span;
        });
    }

    fn mark_dead(&self, segment: SegmentId, lsn: Lsn, span: u64) {
        self.with(segment, |row| {
            row.note_min(lsn);
            row.bytes.dead += span;
        });
    }

    fn shadow(&self, segment: SegmentId, span: u64) {
        self.with(segment, |row| row.shadowed += span);
    }

    fn mark_held(&self, segment: SegmentId, lsn: Lsn, span: u64) {
        self.with(segment, |row| {
            row.bytes.live += span;
            row.bytes.held += span;
            row.bytes.held_lsn = Some(row.bytes.held_lsn.map_or(lsn, |held| held.max(lsn)));
        });
    }
}

/// Store-wide counters that no single column owns
#[derive(Debug, Default)]
pub struct ReadCounters {
    unreadable: AtomicU64,
}

impl ReadCounters {
    /// A zeroed set of store-wide read counters
    pub fn new() -> ReadCounters {
        ReadCounters::default()
    }

    /// How many records a playback could not read and dropped from its results
    pub fn unreadable_records(&self) -> u64 {
        self.unreadable.load(Ordering::Acquire)
    }

    /// Note a record a playback asked for and could not read
    pub fn note_unreadable(&self) {
        self.unreadable.fetch_add(1, Ordering::AcqRel);
    }
}

/// How often a sealed segment was asked about a key, and how often it said no
#[derive(Debug, Default)]
pub struct FilterProbes {
    /// How many sealed segments were asked about a key
    asked: AtomicU64,

    /// Of those, the ones a filter ruled out without a search
    skipped: AtomicU64,

    /// How many row blocks the searches asked for, in hand or not
    blocks: AtomicU64,

    /// Of those, the ones that were not in hand and became a read
    block_reads: AtomicU64,

    /// Reads spent opening a sealed segment's directory before searching it
    map_reads: AtomicU64,
}

impl FilterProbes {
    /// Note a sealed segment being asked about a key
    pub fn note_probe(&self) {
        self.asked.fetch_add(1, Ordering::AcqRel);
    }

    /// Note a filter ruling a segment out
    pub fn note_skip(&self) {
        self.skipped.fetch_add(1, Ordering::AcqRel);
    }

    /// Note a search asking for one row block
    pub fn note_block(&self) {
        self.blocks.fetch_add(1, Ordering::AcqRel);
    }

    /// Note one of those blocks becoming a read
    pub fn note_block_read(&self) {
        self.block_reads.fetch_add(1, Ordering::AcqRel);
    }

    /// Note one read spent opening a sealed segment's directory
    pub fn note_map_read(&self) {
        self.map_reads.fetch_add(1, Ordering::AcqRel);
    }

    /// Every counter, read together so the counts cover the same stretch of work
    pub fn counts(&self) -> ProbeCounts {
        ProbeCounts {
            asked: self.asked.load(Ordering::Acquire),
            skipped: self.skipped.load(Ordering::Acquire),
            blocks: self.blocks.load(Ordering::Acquire),
            block_reads: self.block_reads.load(Ordering::Acquire),
            map_reads: self.map_reads.load(Ordering::Acquire),
        }
    }
}

/// What the sealed segments were asked, and what asking them cost
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProbeCounts {
    /// How many sealed segments were asked about a key
    pub asked: u64,

    /// Of those, the ones a filter ruled out without a search
    pub skipped: u64,

    /// How many row blocks the remaining searches asked for
    pub blocks: u64,

    /// Of those, the ones that became a device read
    pub block_reads: u64,

    /// How many reads went to opening those searches' directories
    pub map_reads: u64,
}

impl ProbeCounts {
    /// What happened between an earlier reading and this one
    pub fn since(&self, before: ProbeCounts) -> ProbeCounts {
        ProbeCounts {
            asked: self.asked - before.asked,
            skipped: self.skipped - before.skipped,
            blocks: self.blocks - before.blocks,
            block_reads: self.block_reads - before.block_reads,
            map_reads: self.map_reads - before.map_reads,
        }
    }

    /// How many searches a filter did not rule out
    pub fn searched(&self) -> u64 {
        self.asked - self.skipped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // shadowing a record moves its footprint from live to dead
    #[test]
    fn segment_shadow() {
        let mut segment = SegmentBytes {
            live: 2000,
            ..SegmentBytes::default()
        };

        segment.shadow(800);

        assert_eq!(segment.live, 1200);
        assert_eq!(segment.dead, 800);
        assert_eq!(segment.total(), 2000);
    }

    // the table opens a row on the first record booked into a segment
    #[test]
    fn table_books_a_segment() {
        let table = SegmentTable::new();

        table.mark_live(SegmentId(1), Lsn(4), 1000);
        table.mark_dead(SegmentId(1), Lsn(6), 200);

        assert_eq!(
            table.bytes_of(SegmentId(1)),
            SegmentBytes {
                live: 1000,
                dead: 200,
                ..SegmentBytes::default()
            }
        );
        assert_eq!(table.bytes_of(SegmentId(2)), SegmentBytes::default());
        assert_eq!(table.len(), 1);
        assert_eq!(table.min_lsn_excluding(SegmentId(2)), Some(Lsn(4)));
    }

    // shadowing through the table never takes live below zero
    #[test]
    fn table_shadow_saturates() {
        let table = SegmentTable::new();
        table.mark_live(SegmentId(1), Lsn(1), 500);

        table.shadow(SegmentId(1), 900);

        assert_eq!(
            table.bytes_of(SegmentId(1)),
            SegmentBytes {
                live: 0,
                dead: 900,
                ..SegmentBytes::default()
            }
        );
        assert_eq!(table.dead_bytes(), 900);
    }

    // the oldest sequence number leaves out the given segment
    #[test]
    fn table_min_lsn_excludes() {
        let table = SegmentTable::new();
        table.note_min(SegmentId(1), Lsn(3));
        table.note_min(SegmentId(1), Lsn(9));
        table.note_min(SegmentId(2), Lsn(7));

        assert_eq!(table.min_lsn_excluding(SegmentId(1)), Some(Lsn(7)));
        assert_eq!(table.min_lsn_excluding(SegmentId(2)), Some(Lsn(3)));
        assert_eq!(table.min_lsn_excluding(SegmentId(3)), Some(Lsn(3)));
    }

    // a segment with no recorded ceiling reports none
    #[test]
    fn table_max_lsn_starts_unknown() {
        let table = SegmentTable::new();
        table.mark_live(SegmentId(1), Lsn(4), 100);

        assert_eq!(table.max_lsn_of(SegmentId(1)), None);
        assert_eq!(table.max_lsn_of(SegmentId(2)), None);
    }

    // a ceiling rises with the newest footer and never falls back
    #[test]
    fn table_max_lsn_only_rises() {
        let table = SegmentTable::new();

        table.note_max(SegmentId(1), Lsn(9));
        table.note_max(SegmentId(1), Lsn(4));

        assert_eq!(table.max_lsn_of(SegmentId(1)), Some(Lsn(9)));
        table.note_max(SegmentId(1), Lsn(20));
        assert_eq!(table.max_lsn_of(SegmentId(1)), Some(Lsn(20)));
    }

    // a retired segment gives its ceiling up with its counters
    #[test]
    fn table_forgets_the_ceiling() {
        let table = SegmentTable::new();
        table.note_max(SegmentId(1), Lsn(9));

        table.forget(SegmentId(1));

        assert_eq!(table.max_lsn_of(SegmentId(1)), None);
    }

    // a row opened without a data record behind it has no sequence bound to give
    #[test]
    fn table_min_lsn_skips_unseen() {
        let table = SegmentTable::new();
        table.shadow(SegmentId(1), 100);

        assert_eq!(table.min_lsn_excluding(SegmentId(2)), None);
    }

    // a segment of nothing but tombstones counts what it holds
    #[test]
    fn table_books_a_tombstone() {
        let table = SegmentTable::new();

        table.mark_held(SegmentId(1), Lsn(4), 300);
        table.mark_held(SegmentId(1), Lsn(9), 200);

        let bytes = table.bytes_of(SegmentId(1));
        assert_eq!(bytes.live, 500);
        assert_eq!(bytes.held, 500);
        assert_eq!(bytes.held_lsn, Some(Lsn(9)));
        // Held sits inside live, so a segment of tombstones has a total to rank on.
        assert_eq!(bytes.total(), 500);
        // And no data record, so nothing for another segment's tombstones to clear.
        assert_eq!(table.min_lsn_excluding(SegmentId(2)), None);
    }

    // tombstones come back only once nothing older than them survives
    #[test]
    fn tombstones_reclaim_below_the_floor() {
        let bytes = SegmentBytes {
            held: 500,
            held_lsn: Some(Lsn(9)),
            ..SegmentBytes::default()
        };

        // A record older than the tombstones would come back if they dropped, so they stay.
        assert_eq!(bytes.droppable(Some(Lsn(4))), 0);
        // A floor above the newest tombstone puts every one of them below it.
        assert_eq!(bytes.droppable(Some(Lsn(20))), 500);
        // The floor sitting exactly on the newest is not below it.
        assert_eq!(bytes.droppable(Some(Lsn(9))), 0);
        // No data record left anywhere, so there is nothing to shadow.
        assert_eq!(bytes.droppable(None), 500);
        // A segment holding no tombstones has none to give back either way.
        assert_eq!(
            SegmentBytes {
                dead: 7,
                ..SegmentBytes::default()
            }
            .droppable(None),
            0
        );
        assert_eq!(bytes.reclaimable(Some(Lsn(20))), 500);
    }

    // the floors answer every exclusion from one pass
    #[test]
    fn floors_match_the_singular_form() {
        let table = SegmentTable::new();
        table.note_min(SegmentId(1), Lsn(3));
        table.note_min(SegmentId(2), Lsn(7));
        table.note_min(SegmentId(3), Lsn(9));

        let floors = table.floors();
        for segment in [1, 2, 3, 4] {
            let segment = SegmentId(segment);
            assert_eq!(floors.excluding(segment), table.min_lsn_excluding(segment));
        }
        // Taking out the oldest promotes the runner-up, whatever its id.
        assert_eq!(floors.excluding(SegmentId(1)), Some(Lsn(7)));
        assert_eq!(floors.excluding(SegmentId(3)), Some(Lsn(3)));
    }

    // two segments sharing the oldest mark keep it when either one leaves
    #[test]
    fn floors_survive_a_tie() {
        let table = SegmentTable::new();
        table.note_min(SegmentId(1), Lsn(5));
        table.note_min(SegmentId(2), Lsn(5));

        let floors = table.floors();
        assert_eq!(floors.excluding(SegmentId(1)), Some(Lsn(5)));
        assert_eq!(floors.excluding(SegmentId(2)), Some(Lsn(5)));
    }

    // an empty reel has no floor to give, and one segment leaves none behind
    #[test]
    fn floors_run_out() {
        let table = SegmentTable::new();
        assert_eq!(table.floors().excluding(SegmentId(1)), None);

        table.note_min(SegmentId(1), Lsn(5));
        assert_eq!(table.floors().excluding(SegmentId(1)), None);
    }

    // forgetting a retired segment clears its counters and its sequence bound
    #[test]
    fn table_forgets() {
        let table = SegmentTable::new();
        table.mark_live(SegmentId(1), Lsn(2), 400);

        table.forget(SegmentId(1));

        assert_eq!(table.dead_bytes(), 0);
        assert!(table.is_empty());
        assert_eq!(table.min_lsn_excluding(SegmentId(9)), None);
    }

    // a live segment wears one incarnation for as long as it stands
    #[test]
    fn incarnation_holds() {
        let table = SegmentTable::new();

        let worn = table.live_incarnation(SegmentId(1));

        assert!(!worn.is_none());
        assert_eq!(table.live_incarnation(SegmentId(1)), worn);
        assert_eq!(table.incarnation_of(SegmentId(1)), worn);
        assert!(table.incarnation_of(SegmentId(2)).is_none());
    }

    // a forgotten segment stamps nothing, even when asked for a live incarnation again
    #[test]
    fn incarnation_never_returns() {
        let table = SegmentTable::new();
        let first = table.live_incarnation(SegmentId(1));

        table.forget(SegmentId(1));

        assert!(table.incarnation_of(SegmentId(1)).is_none());
        assert_ne!(table.live_incarnation(SegmentId(1)), first);
    }
}
