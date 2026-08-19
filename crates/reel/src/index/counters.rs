//! Per-segment reclaimable-byte counters, and what a read reports about itself
//!
//! Live against dead bytes is the fraction the compactor reads to decide whether a
//! segment is worth rewriting. A segment number is a counter this volume allocates in
//! order and never reuses, so the rows sit in a chunked window indexed by that number
//! rather than in a hashed map: a booking is a subtraction and an array index, and the
//! window slides as the oldest segments retire.

use crate::sync::checked::{AtomicBool, AtomicU32, AtomicU64, Ordering, RwLock};
use std::collections::{HashMap, VecDeque};

use crate::format::loc::{SegmentId, SegmentIncarnation};
use crate::format::lsn::Lsn;
use crate::sync::checked::{read, write};

/// Reclaimable-byte bookkeeping for one segment
///
/// Pads and segment headers are counted in none of the three.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SegmentBytes {
    /// Footprint of records the index still resolves into this segment
    pub live: u64,

    /// Footprint of records shadowed by a newer version or a delete
    pub dead: u64,

    /// Tombstone footprint, part of live rather than on top of it
    pub held: u64,

    /// Newest tombstone version this segment holds, if it holds one
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

    /// Tombstone footprint a rewrite would drop rather than carry forward
    ///
    /// A tombstone is dropped when no surviving segment holds a data record older than
    /// it, so the newest tombstone here standing below the floor puts every one of
    /// them below it. A newer one says nothing about the rest, so none are dropped.
    pub fn droppable(&self, floor: Option<Lsn>) -> u64 {
        match (self.held_lsn, floor) {
            (None, _) => 0,
            (Some(_), None) => self.held,
            (Some(newest), Some(floor)) if newest < floor => self.held,
            (Some(_), Some(_)) => 0,
        }
    }

    /// Footprint a rewrite of this segment would actually reclaim
    pub fn reclaimable(&self, floor: Option<Lsn>) -> u64 {
        self.dead + self.droppable(floor)
    }
}

/// Everything one segment's row holds, taken in one pass of the table
///
/// The footprints and the oldest-record mark sit behind the same lock, so a caller
/// wanting both takes them together rather than locking twice a segment.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SegmentStamp {
    /// Live, dead and held footprints
    pub bytes: SegmentBytes,

    /// Oldest data record the segment can still surface
    pub min_lsn: Option<Lsn>,
}

/// The oldest record marks on a reel, enough to answer any one exclusion
///
/// Two marks rather than all of them: taking one segment out of the running can
/// only unseat the oldest, and the runner-up is what steps up when it does.
#[derive(Clone, Copy, Debug, Default)]
pub struct Floors {
    /// The oldest mark on the reel, and which segment carries it
    oldest: Option<(SegmentId, Lsn)>,

    /// The oldest mark carried by any other segment
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

/// Rows one chunk of the window holds, so retiring frees whole allocations
const CHUNK: usize = 256;

/// Ids the window stretches to before a booking is refused rather than allocated
///
/// A volume allocates segment numbers in order, so the distance from the oldest
/// standing segment to the newest is the live count. A gap wider than this is a
/// number nobody drew, and covering it would allocate rows for nothing.
const MAX_WINDOW: u64 = 1 << 20;

/// The row is counted: it holds bytes, ranks, and answers for its floor
const PRESENT: u32 = 1;

/// The segment was sealed before any counter saw its keys
const BORN: u32 = 2;

/// The segment retired, so every booking against it is dropped and counted
const RETIRED: u32 = 4;

/// One segment's counters, moved by whichever writer's key points into it
///
/// A blank row reads as a segment that has seen no record at all rather than one
/// whose oldest record is sequence zero, which is what the reserved minimum is for.
/// One line per row, so two segments booked at once never share one.
#[repr(align(64))]
#[derive(Debug)]
struct SegmentRow {
    /// Bytes the index still points at
    live: AtomicU64,

    /// Bytes shadowed by an overwrite or a delete
    dead: AtomicU64,

    /// Tombstone bytes, counted inside live
    held: AtomicU64,

    /// Oldest data record the segment can still surface, or none seen yet
    min_lsn: AtomicU64,

    /// Newest row the segment's sealed footer holds, or the reserved zero for none
    max_lsn: AtomicU64,

    /// Newest tombstone version here, or the reserved zero for none
    held_lsn: AtomicU64,

    /// The life this segment is on, or the reserved zero for none issued
    incarnation: AtomicU32,

    /// Present, born and retired together, so one load answers all three
    flags: AtomicU32,
}

impl SegmentRow {
    /// A row standing for a segment nothing has booked against yet
    fn new() -> SegmentRow {
        SegmentRow {
            live: AtomicU64::new(0),
            dead: AtomicU64::new(0),
            held: AtomicU64::new(0),
            min_lsn: AtomicU64::new(u64::MAX),
            max_lsn: AtomicU64::new(Lsn::NONE.as_u64()),
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

    /// Whether anything has ever named this row, bytes, birth or a life
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

    fn store_live(&self, span: u64) {
        self.live.store(span, Ordering::Release);
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

    /// Take the row back to never having been touched
    fn blank(&self) {
        self.live.store(0, Ordering::Release);
        self.dead.store(0, Ordering::Release);
        self.held.store(0, Ordering::Release);
        self.min_lsn.store(u64::MAX, Ordering::Release);
        self.max_lsn.store(Lsn::NONE.as_u64(), Ordering::Release);
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
    ///
    /// Read before written: the first record settles the minimum, so a plain load
    /// leaves the line shared where a read-modify-write would take it exclusive.
    fn note_min(&self, lsn: Lsn) {
        if lsn.0 < self.min_lsn.load(Ordering::Acquire) {
            self.min_lsn.fetch_min(lsn.0, Ordering::AcqRel);
        }
    }
}

/// A chunk of rows, freed whole when the window slides past it
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

/// The stretch of segment numbers the table holds rows for
///
/// Chunk aligned at the front so a number resolves by subtraction, and sliding at
/// both ends: a retire clears from the bottom and a fresh segment grows the top.
#[derive(Debug, Default)]
struct Window {
    /// The number row zero of the first chunk stands for, chunk aligned
    base: u64,

    /// The oldest number no retire has passed, below which everything is gone
    floor: u64,

    /// One past the highest number ever counted, which bounds every walk
    reach: u64,

    /// Rows in number order, oldest chunk first
    chunks: VecDeque<Chunk>,

    /// Rows carrying the present bit, so a count is not a walk
    present: usize,

    /// Rows carrying the born bit
    born: usize,
}

impl Window {
    /// The row a number stands on, retired numbers and untouched ones alike
    fn row(&self, segment: SegmentId) -> Option<&SegmentRow> {
        let id = u64::from(segment.as_u32());
        if id < self.floor || id < self.base {
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

    /// Whether a number is one the table has already let go of
    fn is_retired(&self, segment: SegmentId) -> bool {
        let id = u64::from(segment.as_u32());
        if id < self.floor {
            return true;
        }
        self.row(segment).is_some_and(|row| row.is_retired())
    }

    /// The row a number stands on, growing the window to reach it
    ///
    /// Nothing for a number the table has retired, which is what keeps a booking
    /// that lost its race from bringing a segment back.
    fn open(&mut self, segment: SegmentId) -> Option<&SegmentRow> {
        let id = u64::from(segment.as_u32());
        if id < self.floor {
            return None;
        }
        if self.chunks.is_empty() {
            self.base = id - (id % CHUNK as u64);
            self.floor = self.floor.max(self.base);
        }
        if id < self.base {
            // A number below the window and above the floor was never retired, so
            // the window reaches back for it rather than dropping the booking.
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

    /// Mark a row born, opening it without putting it on the count
    ///
    /// A born segment holds no bytes any counter saw, so it ranks nothing and the
    /// present bit stays clear until a booking gives it something to rank.
    fn open_born(&mut self, segment: SegmentId) -> bool {
        let fresh = match self.open(segment) {
            Some(row) => row.flags.fetch_or(BORN, Ordering::AcqRel) & BORN == 0,
            None => return false,
        };
        if fresh {
            self.born += 1;
        }
        true
    }

    /// Clear a retired segment's row and slide the floor past the run below it
    ///
    /// A number the table never knew is left alone rather than marked gone: nothing
    /// stood there to bring back, and marking it would refuse a segment whose file
    /// the caller has not actually seen off.
    fn retire(&mut self, segment: SegmentId) {
        let cleared = self.row(segment).filter(|row| row.is_known()).map(|row| {
            let was = row.flags();
            row.blank();
            row.raise(RETIRED);
            (was & PRESENT != 0, was & BORN != 0)
        });
        if let Some((was_present, was_born)) = cleared {
            self.present -= usize::from(was_present);
            self.born -= usize::from(was_born);
        }
        if self.present == 0 {
            self.reach = self.floor;
        }
        self.slide();
    }

    /// Move the floor past retired rows and give back the chunks behind it
    ///
    /// Only retired rows are passed. An untouched row at the floor is a number a
    /// tail drew and has not written into yet, and passing it would drop that
    /// segment's first booking along with the floor it carries.
    fn slide(&mut self) {
        loop {
            let at = self.floor;
            match self.row(SegmentId(at as u32)) {
                Some(row) if row.is_retired() => self.floor = at + 1,
                Some(_) | None => break,
            }
        }
        self.reach = self.reach.max(self.floor);
        while self.floor - self.base >= CHUNK as u64 && !self.chunks.is_empty() {
            self.chunks.pop_front();
            self.base += CHUNK as u64;
        }
    }

    /// Every counted row with the number it stands for, in number order
    ///
    /// The walk is bounded by the numbers actually counted rather than by the rows
    /// allocated, so a volume of one segment reads one row and not a whole chunk.
    fn counted_rows(&self) -> impl Iterator<Item = (SegmentId, &SegmentRow)> {
        let from = (self.floor - self.base) as usize;
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
        self.floor = 0;
        self.reach = 0;
        self.present = 0;
        self.born = 0;
    }
}

/// Per-segment reclaimable-byte counters for a whole reel
#[derive(Debug, Default)]
pub struct SegmentTable {
    /// The rows themselves, a window over the numbers this volume has issued
    window: RwLock<Window>,

    /// Whether any segment carries the born bit, so an unborn volume pays one load
    has_born: AtomicBool,

    /// Issues incarnations, starting past the reserved none
    next_incarnation: AtomicU32,

    /// Bookings against a segment this table has already let go of
    dropped: AtomicU64,
}

impl SegmentTable {
    /// An empty table, which fills as segments take their first record
    pub fn new() -> SegmentTable {
        SegmentTable::default()
    }

    /// Book a record live in its segment, and note the version it carries
    pub fn mark_live(&self, segment: SegmentId, lsn: Lsn, span: u64) {
        self.opened(segment, |row| {
            row.note_min(lsn);
            row.add_live(span);
        });
    }

    /// Book a record dead where it lies, and note the version it carries
    ///
    /// A record whose index write lost its race is still on disk and a rebuild
    /// would still find it, so its version counts as a live one's does.
    pub fn mark_dead(&self, segment: SegmentId, lsn: Lsn, span: u64) {
        self.opened(segment, |row| {
            row.note_min(lsn);
            row.dead.fetch_add(span, Ordering::AcqRel);
        });
    }

    /// Book a tombstone's own footprint against the segment holding it
    ///
    /// The segment's oldest-record mark is left alone, since that mark answers what a
    /// rebuild could surface and a tombstone surfaces nothing. The version is kept as
    /// a maximum, since dropping tombstones is all or nothing per segment.
    pub fn mark_held(&self, segment: SegmentId, lsn: Lsn, span: u64) {
        self.opened(segment, |row| {
            row.add_live(span);
            row.held.fetch_add(span, Ordering::AcqRel);
            row.held_lsn.fetch_max(lsn.as_u64(), Ordering::AcqRel);
        });
    }

    /// Move a record's footprint from live to dead within its segment
    pub fn shadow(&self, segment: SegmentId, span: u64) {
        self.booked(segment, |row| row.shadow(span));
    }

    /// Raise a segment's dead count to what a full sweep of it counted
    ///
    /// A paged open books every sealed record live, since it cannot tell a shadowed
    /// record from a live one, and a completed sweep settles the split. Raised and
    /// never lowered, so this cannot undo what a write counted.
    pub fn settle_dead(&self, segment: SegmentId, counted: u64) {
        self.booked(segment, |row| {
            let dead = row.dead.load(Ordering::Acquire);
            if counted > dead {
                row.shadow(counted - dead);
            }
        });
    }

    /// Take a record's footprint off a segment's live count without booking it dead
    ///
    /// What a compaction copy does to the segment it left: the record moved rather
    /// than being shadowed, so those bytes are not reclaimable.
    pub fn release_live(&self, segment: SegmentId, span: u64) {
        self.booked(segment, |row| row.drop_live(span));
    }

    /// Note the oldest sequence number a segment can still surface
    pub fn note_min(&self, segment: SegmentId, lsn: Lsn) {
        self.opened(segment, |row| row.note_min(lsn));
    }

    /// Note the newest row a segment's sealed footer holds
    ///
    /// Taken from the footer rather than from the bookings, which are not ordered
    /// against the seal that makes a segment searchable. The footer is written once
    /// and lists every row of the segment, tombstones included. Raised and never
    /// lowered, so a rebuild meeting a footer twice cannot take the ceiling down.
    pub fn note_max(&self, segment: SegmentId, lsn: Lsn) {
        // A footer that lists nothing bounds nothing, and taking the row would put a
        // segment on the table that holds no bytes for anything to rank.
        if lsn == Lsn::NONE {
            return;
        }
        self.opened(segment, |row| {
            row.max_lsn.fetch_max(lsn.as_u64(), Ordering::AcqRel);
        });
    }

    /// The newest row a segment can answer with, or nothing recorded for it
    ///
    /// Nothing means nothing is known, not that the segment is empty. A caller
    /// ordering a search by this has to read it as no bound at all.
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
    ///
    /// The footprints and the floors are both a walk of every row, so one pass is
    /// the same work under one acquisition of the lock every insert also wants.
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

    /// Every segment's footprints and floor together, from one pass under one lock
    pub fn stamps(&self) -> HashMap<SegmentId, SegmentStamp> {
        let window = read(&self.window);
        let mut out = HashMap::with_capacity(window.present);
        for (segment, row) in window.counted_rows() {
            out.insert(
                segment,
                SegmentStamp {
                    bytes: row.bytes(),
                    min_lsn: row.min_lsn(),
                },
            );
        }
        out
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

    /// Oldest sequence number any segment other than this one can still surface
    ///
    /// A tombstone below this bound cannot be undone by a rebuild, so the compactor
    /// may drop it rather than carry it.
    pub fn min_lsn_excluding(&self, segment: SegmentId) -> Option<Lsn> {
        self.floors().excluding(segment)
    }

    /// Every segment's floor at once, in one pass rather than one pass each
    ///
    /// Excluding one segment can only ever remove the single oldest mark, so the
    /// two oldest answer the question for all of them.
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

    /// Bookings dropped for naming a segment this table had already let go of
    pub fn dropped_bookings(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// The incarnation a segment the caller holds live wears, issued on first ask
    ///
    /// Only for a caller that can vouch the segment stands. Asked of a retired
    /// segment it stamps none, which the read-only form does too.
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
        match read(&self.window).row(segment) {
            Some(row) => SegmentIncarnation(row.incarnation.load(Ordering::Acquire)),
            None => SegmentIncarnation::NONE,
        }
    }

    fn issue_incarnation(&self) -> SegmentIncarnation {
        SegmentIncarnation(self.next_incarnation.fetch_add(1, Ordering::Relaxed) + 1)
    }

    /// Forget a segment's counters once its file has been unlinked
    pub fn forget(&self, segment: SegmentId) {
        // The counters and the incarnation go together, so the table is never read as
        // uncounted while the stamp it hands out is still current.
        let mut window = write(&self.window);
        window.retire(segment);
        if window.born == 0 {
            self.has_born.store(false, Ordering::Relaxed);
        }
    }

    /// Mark the segments a rebuild left sealed, whose keys no counter holds
    ///
    /// A born segment is on the volume, so it wears an incarnation from here: its
    /// keys resolve through footers, and those entries are stamped with it.
    pub fn mark_born(&self, segments: impl IntoIterator<Item = SegmentId>) {
        let mut window = write(&self.window);
        for segment in segments {
            if !window.open_born(segment) {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let issued = self.issue_incarnation();
            if let Some(row) = window.row(segment) {
                let _ = row.incarnation.compare_exchange(
                    0,
                    issued.0,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
            }
        }
        if window.born > 0 {
            self.has_born.store(true, Ordering::Relaxed);
        }
    }

    /// Whether this segment's keys were sealed away before any counter saw them
    pub fn is_born(&self, segment: SegmentId) -> bool {
        if !self.has_born.load(Ordering::Relaxed) {
            return false;
        }
        read(&self.window)
            .row(segment)
            .is_some_and(|row| row.flags() & BORN != 0)
    }

    /// Segments still standing whose keys the counters exclude
    pub fn born_count(&self) -> usize {
        if !self.has_born.load(Ordering::Relaxed) {
            return 0;
        }
        read(&self.window).born
    }

    /// Replace the whole table with what a rebuild resolved
    ///
    /// Every incarnation starts over, so a stamp taken before the rebuild can only
    /// mismatch and sends that read back through the index.
    pub fn install(
        &self,
        segments: HashMap<SegmentId, SegmentBytes>,
        min_lsn: HashMap<SegmentId, Lsn>,
        max_lsn: HashMap<SegmentId, Lsn>,
    ) {
        let mut window = write(&self.window);
        window.clear();
        self.has_born.store(false, Ordering::Relaxed);
        // The window is opened from the oldest number first, so a rebuild whose
        // segments arrive in map order does not reach back a chunk at a time.
        let lowest = segments
            .keys()
            .chain(min_lsn.keys())
            .chain(max_lsn.keys())
            .map(|segment| segment.as_u32())
            .min();
        if let Some(lowest) = lowest {
            window.open_counted(SegmentId(lowest));
        }
        for (segment, bytes) in segments {
            let Some(row) = window.open_counted(segment) else {
                continue;
            };
            row.store_live(bytes.live);
            row.dead.store(bytes.dead, Ordering::Release);
            row.held.store(bytes.held, Ordering::Release);
            let newest = bytes.held_lsn.unwrap_or(Lsn::NONE);
            row.held_lsn.store(newest.as_u64(), Ordering::Release);
            let issued = self.issue_incarnation();
            row.incarnation.store(issued.0, Ordering::Release);
        }
        for (segment, lsn) in min_lsn {
            let Some(row) = window.open_counted(segment) else {
                continue;
            };
            row.min_lsn.store(lsn.0, Ordering::Release);
            if row.incarnation.load(Ordering::Acquire) == 0 {
                let issued = self.issue_incarnation();
                row.incarnation.store(issued.0, Ordering::Release);
            }
        }
        for (segment, lsn) in max_lsn {
            let Some(row) = window.open_counted(segment) else {
                continue;
            };
            row.max_lsn.store(lsn.as_u64(), Ordering::Release);
            if row.incarnation.load(Ordering::Acquire) == 0 {
                let issued = self.issue_incarnation();
                row.incarnation.store(issued.0, Ordering::Release);
            }
        }
    }

    /// Segments the table is counting
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
        self.has_born.store(false, Ordering::Relaxed);
    }

    /// Run something against a segment's row, opening one the first time it is touched
    ///
    /// For a caller booking a record into a segment it is holding open, which is
    /// every caller that books bytes or a floor. A number the table has retired is
    /// dropped and counted rather than given a row back.
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

    /// Run something against a row that already exists, dropping the booking if none does
    ///
    /// For a caller moving bytes that were booked by somebody else: the segment it
    /// names is whatever an entry pointed at, which compaction may have retired
    /// since. Opening a row here is what left phantom segments on the table.
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

/// Store-wide counters that no single column owns
///
/// Live counts and byte totals belong to the columns holding the keys, so what is
/// left here is what a read reports about itself.
#[derive(Debug, Default)]
pub struct ReadCounters {
    unreadable: AtomicU64,
}

impl ReadCounters {
    /// A zeroed set of store-wide read counters
    pub fn new() -> ReadCounters {
        ReadCounters::default()
    }

    /// Records a playback could not read, so they dropped out of its results
    ///
    /// A playback hands back pairs and has nowhere to put an error, so a device error
    /// under one otherwise looks like a complete playback that found less.
    pub fn unreadable_records(&self) -> u64 {
        self.unreadable.load(Ordering::Acquire)
    }

    /// Note a record a playback asked for and could not read
    pub fn note_unreadable(&self) {
        self.unreadable.fetch_add(1, Ordering::AcqRel);
    }
}

/// How often a sealed segment was asked about a key, and how often it said no
///
/// Counts rather than times, so they say the same thing on any machine.
#[derive(Debug, Default)]
pub struct FilterProbes {
    /// Sealed segments asked about a key
    asked: AtomicU64,

    /// Of those, the ones a filter ruled out without a search
    skipped: AtomicU64,

    /// Row blocks a search asked for, whether they were in hand or not
    blocks: AtomicU64,

    /// Of those, the ones that were not in hand and became a read
    block_reads: AtomicU64,

    /// Reads spent opening a sealed segment's directory rather than reading rows,
    /// which is what a search pays before it searches anything
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

    /// Note one of those blocks being read rather than found in hand
    pub fn note_block_read(&self) {
        self.block_reads.fetch_add(1, Ordering::AcqRel);
    }

    /// Note one read spent opening a sealed segment's directory
    pub fn note_map_read(&self) {
        self.map_reads.fetch_add(1, Ordering::AcqRel);
    }

    /// Everything the counters hold, taken together
    ///
    /// One read of the set rather than four, so the counts describe the same
    /// stretch of work.
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
    /// Sealed segments asked about a key
    pub asked: u64,

    /// Of those, the ones a filter ruled out without a search
    pub skipped: u64,

    /// Row blocks the surviving searches asked for
    pub blocks: u64,

    /// Of those, the ones that became a device read
    pub block_reads: u64,

    /// Reads spent opening the directories those searches went through
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

    /// Searches a filter did not rule out, which are the ones that read blocks
    pub fn searched(&self) -> u64 {
        self.asked - self.skipped
    }
}

/// What the retire ordering does and does not promise a concurrent reader
///
/// A retire drops the incarnation before the counters, so the table never sits in
/// stamped-but-uncounted. Only the ordering is modelled.
#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;

    use loom::sync::Arc;

    // a reader finding the counters gone never then finds the stamp current
    #[test]
    fn uncounted_implies_unstamped() {
        // Outside loom's tracking on purpose: the assert only fires where the retire
        // got there first, so a run where it never did would pass testing nothing.
        static SAW_UNCOUNTED: std::sync::atomic::AtomicUsize =
            std::sync::atomic::AtomicUsize::new(0);

        loom::model(|| {
            let table = Arc::new(SegmentTable::new());
            table.mark_live(SegmentId(1), Lsn(1), 1000);

            let retiring = {
                let table = Arc::clone(&table);
                loom::thread::spawn(move || table.forget(SegmentId(1)))
            };

            let counted = table.bytes_of(SegmentId(1)) != SegmentBytes::default();
            let stamp = table.incarnation_of(SegmentId(1));
            if !counted {
                SAW_UNCOUNTED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                assert!(
                    stamp.is_none(),
                    "a segment read as uncounted still wore a current stamp"
                );
            }

            retiring.join().expect("the retire finishes");
        });

        assert!(
            SAW_UNCOUNTED.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "no interleaving put the retire ahead of the read, so nothing was checked"
        );
    }

    // the read-only form never brings a forgotten segment back
    #[test]
    fn a_read_never_resurrects_a_forgotten_segment() {
        loom::model(|| {
            let table = Arc::new(SegmentTable::new());
            table.mark_live(SegmentId(1), Lsn(1), 1000);

            let reading = {
                let table = Arc::clone(&table);
                loom::thread::spawn(move || {
                    table.incarnation_of(SegmentId(1));
                })
            };

            table.forget(SegmentId(1));
            reading.join().expect("the read finishes");

            assert!(
                table.incarnation_of(SegmentId(1)).is_none(),
                "a read put a forgotten segment back on the table"
            );
        });
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    // a fresh set of read counters has seen nothing go wrong
    #[test]
    fn starts_clean() {
        let counters = ReadCounters::new();

        assert_eq!(counters.unreadable_records(), 0);

        counters.note_unreadable();
        assert_eq!(counters.unreadable_records(), 1);
    }

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

    // the oldest sequence number excludes the named segment
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

    // a segment nothing recorded a ceiling for offers none rather than zero
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

        // A record older than the tombstones could still be resurrected by dropping
        // them, so they stay.
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
        // Taking out the oldest promotes the runner-up rather than the next id.
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

    // a forgotten segment stamps nothing, and coming back it wears a fresh one
    #[test]
    fn incarnation_never_returns() {
        let table = SegmentTable::new();
        let first = table.live_incarnation(SegmentId(1));

        table.forget(SegmentId(1));

        assert!(table.incarnation_of(SegmentId(1)).is_none());
        assert_ne!(table.live_incarnation(SegmentId(1)), first);
    }

    // an install restamps exactly the segments it resolved
    #[test]
    fn install_restamps() {
        let table = SegmentTable::new();
        let before = table.live_incarnation(SegmentId(1));

        let mut segments = HashMap::new();
        segments.insert(SegmentId(1), SegmentBytes::default());
        table.install(segments, HashMap::new(), HashMap::new());
        table.mark_born([SegmentId(3)]);

        assert_ne!(table.incarnation_of(SegmentId(1)), before);
        assert!(!table.incarnation_of(SegmentId(1)).is_none());
        assert!(!table.incarnation_of(SegmentId(3)).is_none());
        assert!(table.incarnation_of(SegmentId(2)).is_none());
    }

    // installing a rebuilt table replaces whatever it held
    #[test]
    fn table_install_replaces() {
        let table = SegmentTable::new();
        table.mark_live(SegmentId(9), Lsn(1), 1);

        let mut segments = HashMap::new();
        segments.insert(
            SegmentId(1),
            SegmentBytes {
                live: 700,
                dead: 50,
                ..SegmentBytes::default()
            },
        );
        let mut min_lsn = HashMap::new();
        min_lsn.insert(SegmentId(1), Lsn(4));
        let mut max_lsn = HashMap::new();
        max_lsn.insert(SegmentId(1), Lsn(30));
        table.install(segments, min_lsn, max_lsn);

        assert_eq!(table.len(), 1);
        assert_eq!(
            table.bytes_of(SegmentId(1)),
            SegmentBytes {
                live: 700,
                dead: 50,
                ..SegmentBytes::default()
            }
        );
        assert_eq!(table.min_lsn_excluding(SegmentId(2)), Some(Lsn(4)));
        assert_eq!(table.max_lsn_of(SegmentId(1)), Some(Lsn(30)));
        assert_eq!(table.max_lsn_of(SegmentId(9)), None);
    }
}
