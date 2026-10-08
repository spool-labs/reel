//! The reel's index: one map per column over one set of segments
//!
//! Resolving by column first is what lets each column keep its keys at its own
//! width and shard as far as its own write plane needs. The segments are shared,
//! since one holds records from every column and the compactor asks about it whole.

use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::ops::{Bound, Range};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use crate::units::ByteCount;

use crate::append::publish::PublishBarrier;
use crate::engine::Totals;
use crate::error::{ReelError, Result};
use crate::format::column::{Codec, ColumnId, ColumnSet, ColumnSpec, KeyBytes, KeyRef, RecordKey};
use crate::format::footer::{FooterPartition, SegmentFooter};
use crate::format::loc::{Loc, SegmentId, SegmentIncarnation};
use crate::format::lsn::Lsn;
use crate::index::column::{ColumnIndex, KeyMove, Landed, PendingCover};
use crate::index::counters::{Floors, SegmentBytes, SegmentTable};
use crate::index::entry::{span_of, Entry};
use crate::index::keyrun::{key_in, row_in, KeyRun, KeyRunSet, RunColumn};
use crate::index::page::KeyPage;
use crate::index::paged::{Candidates, FooterSource, SealedRanges};
use crate::index::playback::{self, merged_page, Paged, PlaybackCursor, WalkRuns, Way};
use crate::index::recovery::SealedSpan;
use crate::index::spot::{
    Booking, Lookup, Pick, RecordSource, Settled, Since, SpotColumn, Taken, LOOKUP_TRIES,
};
use crate::sync::{lock, read, write};

/// Slots in the lookup from a column identifier to its index
const COLUMN_SLOTS: usize = 256;

/// The lists one thread's batched lookups group through, kept between batches
///
/// Both follow the batch's width and neither leaves, so they stay with the thread:
/// what a lookup buys is the answers it hands back.
#[derive(Default)]
struct BatchLookup {
    /// Positions of the asked keys, sorted by column
    order: Vec<usize>,

    /// What one column's run of keys resolved to, in the order handed over
    answers: Vec<Option<Entry>>,
}

thread_local! {
    /// One set of lookup lists per resolving thread, handed back after every batch
    static BATCH_LOOKUP: std::cell::Cell<BatchLookup> = const {
        std::cell::Cell::new(BatchLookup {
            order: Vec::new(),
            answers: Vec::new(),
        })
    };
}

/// This thread's lookup lists, given back however the batch that took them ends
struct HeldLookup(BatchLookup);

impl HeldLookup {
    fn take() -> HeldLookup {
        HeldLookup(BATCH_LOOKUP.with(std::cell::Cell::take))
    }
}

impl Drop for HeldLookup {
    fn drop(&mut self) {
        let mut held = std::mem::take(&mut self.0);
        held.order.clear();
        held.answers.clear();
        BATCH_LOOKUP.with(|spare| spare.set(held));
    }
}

/// What the sealed side of a column says about one key
enum Sealed {
    /// A live record, wherever it is being answered from
    Live(Entry),

    /// A delete or a cover reached it, so the key is gone on purpose
    Gone,

    /// Nothing says anything about it at all
    Absent,
}

/// Where one key is answered from, for telling two disagreeing answers apart
#[derive(Clone, Debug, Default)]
pub struct KeySites {
    /// What the resident map holds for the key, a grave included
    pub resident: Option<Entry>,

    /// The segments a lookup could read, in the order it reaches them
    pub candidates: Vec<SegmentId>,

    /// What every sealed segment's footer says, searched or not
    pub sealed: Vec<SealedSite>,
}

/// One sealed segment's row for a key, and whether the search would reach it
#[derive(Clone, Copy, Debug)]
pub struct SealedSite {
    /// The segment holding the row
    pub segment: SegmentId,

    /// The sequence number that orders this row against the others
    pub lsn: Lsn,

    /// Where the record starts in the segment
    pub offset: u32,

    /// Whether the row is a delete rather than a record
    pub is_grave: bool,

    /// Whether the segment's key range let the search consider it
    pub is_candidate: bool,

    /// Whether the segment's filter would let the search read it
    pub passes_filter: bool,
}

/// One record a pass moved, waiting for the index to be pointed at the copy
///
/// Owned rather than borrowed, since the keys outlive the records they came from.
#[derive(Clone, Debug)]
pub struct KeyRepoint {
    /// Column and key the record is addressed by
    pub key: RecordKey,

    /// Where the pass read the record it copied, when it knows
    pub from: Option<Loc>,

    /// Where the copy landed
    pub to: Loc,

    /// The sequence number the move is guarded by, which is the source's own
    pub lsn: Lsn,
}

/// Where a batch's keys sit: an entry the map or a footer gave, or a spot index lookup to read
pub struct Located {
    /// An entry per key, or nothing where a key has none or a pick answers it
    pub found: Vec<Option<Entry>>,

    /// Keys for the spot index to answer, each with its lookup started
    pub picks: Vec<SpotPick>,
}

/// One batch key answered by the spot index
pub struct SpotPick {
    /// The key's position in the batch
    pub at: usize,

    /// The position of the column whose spot table holds the key
    pub column: usize,

    /// The key's lookup, started under the caller's barrier
    pub pick: Pick,

    /// Where the key's shard stood before the map was asked
    pub since: Since,
}

/// Where a spot index lookup goes before any read
pub enum SpotRoute {
    /// The map answered, or the column is not the spot index's to answer
    Settled(Lookup),

    /// The column at this position holds the key's sealed versions, and its shard stood here before the map was asked
    Column(usize, Since),
}

/// Paged keys a range delete settles at a time
///
/// Holding every key a range reaches would hold the whole range in memory at once.
const RELEASE_RUN: usize = 1024;

/// The ceiling of a sealed segment nothing recorded one for, which rules nothing out
///
/// Above every sequence number a reel can issue, so a segment wearing it sorts to the
/// front of a fan-out and no hit can stop the walk short of it.
const NO_CEILING: Lsn = Lsn(u64::MAX);

/// One range delete's cover as a batch hands it to the index
///
/// The position is what keeps a batch's order: the key moves given before it go in
/// first, so a put the range was meant to sweep and one written after it land on the
/// right sides of the cover.
pub struct RangeMove<'batch> {
    /// Key moves of the batch that precede this range
    pub after: usize,

    /// Column and inclusive start of the range
    pub start: &'batch RecordKey,

    /// Exclusive end, or nothing for a range with no upper bound
    pub end: Option<&'batch [u8]>,

    /// The sequence number the range's own record was written under
    pub lsn: Lsn,

    /// Where that record landed, which is the space the cover holds
    pub tombstone: Loc,
}

/// The index of one reel: a map per column and the segments they share
///
/// The maps hold the keys of segments no footer covers yet, and for the rest only
/// the range of keys each sealed segment covers, so a lookup that misses the map
/// knows which footers to search.
pub struct ReelIndex {
    /// The columns this index was built over
    columns: ColumnSet,

    /// One map per column, in the order the columns were declared
    indexes: Vec<ColumnIndex>,

    /// What each column's sealed segments cover
    sealed: Vec<SealedRanges>,

    /// Footers opened by each column's walks, kept while its sealed set stands
    walk_runs: Vec<WalkRuns>,

    /// Merges write these key runs, and a walk reads them in place of the footers they cover
    key_runs: KeyRunSet,

    /// Each column's sealed keys as record locations, answering a get in one read
    spot: Vec<SpotColumn>,

    /// Whether every sealed key is in `spot`, after an open that loaded them or found none
    spot_ready: AtomicBool,

    /// Segments retired since the spot index last dropped the entries pointing into them
    retired: AtomicU64,

    /// The newest version an open or a hand-over gave the spot index, so a write above it skips the shadow check
    handed: AtomicU64,

    /// Column identifier to its position, so routing a record is one load
    by_id: Vec<Option<usize>>,

    /// Per-segment counters every column's keys point into
    segments: Arc<SegmentTable>,

    /// Where the footers of sealed segments are read from
    footers: OnceLock<Arc<dyn FooterSource>>,

    /// Sealed versions a rebuild found a tail outversioning, taken out once the spot index load settles
    shadowed: Mutex<Vec<(usize, KeyBytes, Loc)>>,

    /// Held while a batch moves the maps, so no spanning read sees part of one
    publish: PublishBarrier,

    /// Held shared by each hand-over and whole by a prune
    handing: RwLock<()>,
}

/// One segment's hand-over splits across this many threads, each owning a lane of the spot index shards
const HANDOVER_LANES: usize = 4;

/// A partition under this many rows hands over on the calling thread, since lanes cost more to start
const SPLIT_AT: usize = 4096;

/// Rows a hand-over samples to tell whether a partition is worth asking the map about whole
const SAMPLE_ROWS: usize = 64;

/// Whether one sampled row in sixteen or more is one the map no longer holds, each costing a spot insert and its removal
fn mostly_outversioned(index: &ColumnIndex, rows: &[(&[u8], Loc)]) -> bool {
    if rows.is_empty() {
        return false;
    }
    let step = rows.len().div_ceil(SAMPLE_ROWS);
    let (mut sampled, mut missing) = (0usize, 0usize);
    for (key, loc) in rows.iter().step_by(step) {
        sampled += 1;
        missing += usize::from(!index.holds(key, *loc));
    }
    missing * 16 >= sampled
}

/// Run one hand-over step over `len` rows, a lane of shards to a thread past the split
fn in_lanes(len: usize, step: &(dyn Fn(usize, usize) -> Vec<bool> + Sync)) -> Vec<bool> {
    if len == 0 {
        return Vec::new();
    }
    let lanes = match len >= SPLIT_AT {
        true => HANDOVER_LANES,
        false => 1,
    };
    let answers: Vec<Vec<bool>> = match lanes {
        1 => vec![step(0, 1)],
        _ => std::thread::scope(|scope| {
            let running: Vec<_> = (0..lanes)
                .map(|lane| scope.spawn(move || step(lane, lanes)))
                .collect();
            running
                .into_iter()
                .map(|lane| lane.join().expect("a hand-over lane panicked"))
                .collect()
        }),
    };
    let mut all = vec![false; len];
    for lane in answers {
        for (all, took) in all.iter_mut().zip(lane) {
            *all |= took;
        }
    }
    all
}

impl ReelIndex {
    /// An empty index over the columns a reel serves
    pub fn new(columns: ColumnSet) -> Result<ReelIndex> {
        let mut indexes = Vec::with_capacity(columns.len());
        let mut sealed = Vec::with_capacity(columns.len());
        let mut by_id = vec![None; COLUMN_SLOTS];
        for (at, spec) in columns.iter().enumerate() {
            if by_id[spec.id.as_index()].is_some() {
                return Err(ReelError::Config(format!(
                    "column {} reuses an identifier another column already took",
                    spec.name,
                )));
            }
            by_id[spec.id.as_index()] = Some(at);
            indexes.push(ColumnIndex::new(spec)?);
            sealed.push(SealedRanges::new());
        }
        Ok(ReelIndex {
            columns,
            indexes,
            walk_runs: sealed.iter().map(|_| WalkRuns::default()).collect(),
            key_runs: KeyRunSet::default(),
            sealed,
            spot: columns.iter().map(|_| SpotColumn::new()).collect(),
            spot_ready: AtomicBool::new(false),
            retired: AtomicU64::new(0),
            handed: AtomicU64::new(0),
            by_id,
            segments: Arc::new(SegmentTable::new()),
            footers: OnceLock::new(),
            shadowed: Mutex::new(Vec::new()),
            publish: PublishBarrier::new(),
            handing: RwLock::new(()),
        })
    }

    /// Tell the index where to read the footers its sealed keys resolve through
    ///
    /// Set once at open, after the volume the footers live on exists.
    pub fn set_footers(&self, footers: Arc<dyn FooterSource>) {
        let _ = self.footers.set(footers);
    }

    /// Tell the spot index where to read the records its entries point at
    pub fn set_records(&self, records: Arc<dyn RecordSource>) {
        for spot in &self.spot {
            spot.attach(Arc::clone(&records), Arc::clone(&self.segments));
        }
    }

    /// Keep each live spot index slot whose segment went, for a read-only open that follows its writer
    pub fn follow(&self) {
        for spot in &self.spot {
            spot.follow();
        }
    }

    /// How many lookups on a read-only open met a live spot index slot whose segment went
    pub fn spot_behind(&self) -> u64 {
        self.spot.iter().map(SpotColumn::behind).sum()
    }

    /// Whether the spot index answers for a column's sealed keys
    fn spot_serves(&self) -> bool {
        self.spot_ready.load(Ordering::Acquire)
    }

    /// A key's newest payload in one read, when the spot index holds the column's sealed keys
    pub fn spot_read(&self, key: &RecordKey) -> Result<Lookup> {
        match self.spot_route(key) {
            SpotRoute::Settled(lookup) => Ok(lookup),
            SpotRoute::Column(at, since) => {
                Ok(self.spot_finish(at, key, since, self.spot[at].read(key)?))
            }
        }
    }

    /// Where a spot index lookup goes: settled by the map already, or to one column's table
    pub fn spot_route(&self, key: &RecordKey) -> SpotRoute {
        let (Some(at), true) = (self.slot(key.column), self.spot_serves()) else {
            return SpotRoute::Settled(Lookup::Unsettled);
        };
        let since = self.spot[at].since(key);
        let index = &self.indexes[at];
        match index.entry_or_grave(key.as_slice()) {
            Some(entry) if entry.is_grave() || index.is_covered_key(key.as_slice(), entry.lsn) => {
                SpotRoute::Settled(Lookup::Missing)
            }
            Some(_) => SpotRoute::Settled(Lookup::Unsettled),
            None => SpotRoute::Column(at, since),
        }
    }

    /// Where a cue read can ask the spot index, only for a key the map has let go
    pub fn spot_route_at(&self, key: &RecordKey) -> Option<(usize, Since)> {
        let (Some(at), true) = (self.slot(key.column), self.spot_serves()) else {
            return None;
        };
        let since = self.spot[at].since(key);
        // A hand-over in flight puts an older version in the spot index before the map lets go
        self.indexes[at]
            .entry_or_grave(key.as_slice())
            .is_none()
            .then_some((at, since))
    }

    /// Apply a cue to a spot index answer, leaving a version the cue cannot see to the footers
    pub fn spot_finish_at(
        &self,
        at: usize,
        key: &RecordKey,
        since: Since,
        snapshot: Lsn,
        lookup: Lookup,
    ) -> Lookup {
        match lookup {
            Lookup::Found(lsn, _) if lsn > snapshot => Lookup::Unsettled,
            Lookup::Found(lsn, _)
                if self.indexes[at].is_covered_key_at(key.as_slice(), lsn, snapshot) =>
            {
                Lookup::Missing
            }
            // A cue has to see the version, and this answer has none
            Lookup::Newest(_) => Lookup::Unsettled,
            Lookup::Missing if self.spot[at].moved(since) => Lookup::Unsettled,
            // A range delete after the cue drops what it spans from the map and the spot index, and the footers still hold it
            Lookup::Missing if self.indexes[at].is_covered_key(key.as_slice(), snapshot) => {
                Lookup::Unsettled
            }
            found => found,
        }
    }

    /// One column's spot index table, so the caller can drive a lookup itself
    pub fn spot_column(&self, at: usize) -> &SpotColumn {
        &self.spot[at]
    }

    /// Apply what a footer cannot know to a spot index answer: covers, and a slot that left under it
    pub fn spot_finish(&self, at: usize, key: &RecordKey, since: Since, lookup: Lookup) -> Lookup {
        match lookup {
            Lookup::Found(lsn, _) if self.indexes[at].is_covered_key(key.as_slice(), lsn) => {
                Lookup::Missing
            }
            // Read after the record, so a range delete that landed before it is seen here
            Lookup::Newest(_) if self.indexes[at].has_covers() => Lookup::Unsettled,
            Lookup::Missing if self.spot[at].moved(since) => Lookup::Unsettled,
            found => found,
        }
    }

    /// Take out up to `budget` older versions the spot index lookups read past, and book them
    pub fn scrub_spot(&self, budget: usize) -> usize {
        self.forget_retired_slots();
        let mut settled = 0;
        for (at, spot) in self.spot.iter().enumerate() {
            for (key, loc) in spot.scrub(budget.saturating_sub(settled)) {
                self.indexes[at].settle_paged(key.as_slice(), loc, &self.segments);
                settled += 1;
            }
        }
        settled
    }

    /// Drop the spot index's slots into retired segments, and hand back how many still held a counted version
    pub fn forget_retired_slots(&self) -> u64 {
        if self.retired.swap(0, Ordering::AcqRel) == 0 {
            return 0;
        }
        // The spot index only points into sealed segments, and a retire forgets the span
        let mut live = 0;
        for (at, spot) in self.spot.iter().enumerate() {
            let standing: HashSet<SegmentId> = self.sealed[at].segments().into_iter().collect();
            live += spot.forget_retired(|segment| standing.contains(&segment));
        }
        live
    }

    /// Slack in the byte counters from the spot index's class bookings
    pub fn spot_slack(&self) -> u64 {
        self.spot.iter().map(SpotColumn::slack).sum()
    }

    /// Overwritten sealed versions booked from their length class, held until compaction retires them
    pub fn spot_displaced(&self) -> u64 {
        self.spot.iter().map(SpotColumn::displaced).sum()
    }

    /// Older versions held in the spot index for its cleaner
    pub fn spot_beside(&self) -> u64 {
        self.spot.iter().map(SpotColumn::beside).sum()
    }

    /// All entries in the spot index
    pub fn spot_held(&self) -> u64 {
        self.spot.iter().map(SpotColumn::held).sum()
    }

    /// Heap bytes of the spot index's tables, counting every bucket whether filled or not
    pub fn spot_heap_bytes(&self) -> u64 {
        self.spot.iter().map(SpotColumn::heap_bytes).sum()
    }

    /// Say every sealed key is in the spot index, so it may answer for them
    pub fn mark_spot_ready(&self) {
        self.spot_ready.store(true, Ordering::Release);
    }

    /// Take a sealed footer's rows during a paged open, and count each fresh one, unless a key run covers the segment
    pub fn take_sealed_footer(&self, segment: SegmentId, footer: &SegmentFooter) -> Result<()> {
        // What an open loads counts as handed over, since a follower applies older records after it
        self.handed
            .fetch_max(footer.max_lsn.as_u64(), Ordering::AcqRel);
        // The run keeps one row a key, so a version whose newer one died in a retired segment stays out
        if self.key_runs.covers(segment) {
            return Ok(());
        }
        for partition in &footer.partitions {
            if let Some(at) = self.slot(partition.column) {
                let fresh = self.spot[at].take_partition(segment, partition)?;
                self.indexes[at].book_sealed(
                    fresh
                        .iter()
                        .filter_map(|(row, len)| Some((partition.key_at(*row as usize)?, *len))),
                );
            }
        }
        Ok(())
    }

    /// Take one stretch of a key run's rows during a paged open, those in standing segments, and count each fresh one
    pub fn take_run_rows(
        &self,
        run: &KeyRun,
        column: &RunColumn,
        rows: Range<u64>,
        standing: &HashSet<SegmentId>,
    ) -> Result<()> {
        let Some(at) = self.slot(column.column) else {
            return Ok(());
        };
        let bytes = run.rows(column);
        let first = rows.start as usize;
        let count = (rows.end - rows.start) as usize;
        let fresh = self.spot[at].take_rows(column.column, first, count, |row| {
            let (key, found) = row_in(bytes, column, first + row)?;
            let is_taken =
                standing.contains(&found.loc.segment) && !found.flags.is_range_tombstone();
            Ok(is_taken.then_some(Taken {
                key,
                loc: found.loc,
                lsn: found.lsn,
                is_tombstone: found.flags.is_tombstone(),
            }))
        })?;
        self.indexes[at].book_sealed(
            fresh
                .iter()
                .map(|(row, len)| (key_in(bytes, column, first + *row as usize), *len)),
        );
        Ok(())
    }

    /// Hold sealed versions a tail outversions, for the end of the load to take out of the spot index
    pub fn shadow_sealed(&self, column: ColumnId, rows: Vec<(KeyBytes, Loc)>) {
        if let Some(at) = self.slot(column) {
            lock(&self.shadowed).extend(rows.into_iter().map(|(key, loc)| (at, key, loc)));
        }
    }

    /// Size the spot index from the first sealed footer of an open, since segments run to one size
    pub fn reserve_fast(&self, footer: &SegmentFooter, segments: usize) {
        for partition in &footer.partitions {
            if let Some(at) = self.slot(partition.column) {
                self.spot[at].reserve((partition.len() * segments) as u64);
            }
        }
    }

    /// Finish an open once records can be read: settle the spot index load, then sweep every cover
    pub fn finish_open(&self) -> Result<()> {
        self.finish_spot_load()?;
        while self.sweep_covers(usize::MAX)? {}
        Ok(())
    }

    /// Finish the open's spot index load and its counts, then let it answer
    fn finish_spot_load(&self) -> Result<()> {
        if self.spot_ready.load(Ordering::Acquire) {
            return Ok(());
        }
        let Some(footers) = self.footers.get() else {
            return Ok(());
        };
        let segments = &self.segments;
        for (at, spot) in self.spot.iter().enumerate() {
            let index = &self.indexes[at];
            spot.settle_rows(
                self.columns[at].id,
                footers.as_ref(),
                &|key, booking| index.book_paged(key, booking),
                // A seal's tally still counts live a version outversioned at or past that seal
                &|key, lost, next| {
                    if segments
                        .sealed_at_of(lost.segment)
                        .is_some_and(|sealed| next >= sealed)
                    {
                        segments.shadow(lost.segment, span_of(key.len() as u16, lost.len));
                    }
                },
            )?;
            spot.finish_load();
        }
        // A version a tail outversions leaves the spot index, so a pruned grave cannot bring it back
        for (at, key, loc) in std::mem::take(&mut *lock(&self.shadowed)) {
            if self.spot[at].take_live(key.as_slice(), loc) {
                let gone = Booking {
                    gone: Some(loc.len),
                    came: None,
                };
                self.indexes[at].book_paged(key.as_slice(), gone);
            }
        }
        self.mark_spot_ready();
        Ok(())
    }

    /// Where a key's live record is, reading a footer where the map holds nothing
    ///
    /// A key the map lacks is looked for in the sealed segments whose key range
    /// covers it, taking the highest sequence number found, since a segment number is
    /// not a version. A grave, a tombstone row and a range delete are all applied
    /// here, because a footer cannot know about any of them.
    pub fn get(&self, key: &RecordKey) -> Result<Option<Entry>> {
        let Some(at) = self.slot(key.column) else {
            return Ok(None);
        };
        for _ in 0..LOOKUP_TRIES {
            let since = self.spot[at].since(key);
            if let Some(answer) = self.mapped(at, key) {
                return Ok(answer);
            }
            let found = self.sealed_entry(at, key)?;
            if found.is_some() || !self.spot_serves() || !self.spot[at].moved(since) {
                return Ok(found);
            }
        }
        // Slots kept leaving the key's shard under every look, and the footers hold still
        match self.mapped(at, key) {
            Some(answer) => Ok(answer),
            None => Ok(self.live_only(at, key, self.newest_sealed(at, key, None)?)),
        }
    }

    /// Whether a key's live version is the record at `loc`
    pub fn is_live_at(&self, key: &RecordKey, loc: Loc, lsn: Lsn) -> Result<bool> {
        if self.surely_at(key, loc, lsn) {
            return Ok(true);
        }
        Ok(self.get(key)?.is_some_and(|entry| entry.loc == loc))
    }

    /// Whether the index surely points a key at the record at `loc` with no read, where false is only unsure
    pub fn surely_at(&self, key: &RecordKey, loc: Loc, lsn: Lsn) -> bool {
        let Some(at) = self.slot(key.column) else {
            return false;
        };
        match self.mapped(at, key) {
            Some(answer) => answer.is_some_and(|entry| entry.loc == loc),
            None => {
                self.spot_serves()
                    && self.spot[at].only_at(key.as_slice(), loc)
                    && !self.indexes[at].is_covered_key(key.as_slice(), lsn)
            }
        }
    }

    /// What the map says about a key, a grave or a cover over it as nothing, or no word
    fn mapped(&self, at: usize, key: &RecordKey) -> Option<Option<Entry>> {
        match self.indexes[at].entry_or_grave(key.as_slice()) {
            Some(entry) if entry.is_grave() => Some(None),
            // An entry a cover spans is a key the drop took, and the map held the
            // newest version, so there is no footer left to ask.
            Some(entry) if self.indexes[at].is_covered_key(key.as_slice(), entry.lsn) => Some(None),
            Some(entry) => Some(Some(entry)),
            None => None,
        }
    }

    /// The newest thing every sealed footer says about a key
    ///
    /// The fan-out below finds it; what is left here is the two things a footer
    /// cannot know about itself, a tombstone row and a range delete.
    fn sealed_entry(&self, at: usize, key: &RecordKey) -> Result<Option<Entry>> {
        Ok(self.live_only(at, key, self.newest_live(at, key)?))
    }

    /// A sealed find with a tombstone row or a range delete over it read as nothing
    fn live_only(&self, at: usize, key: &RecordKey, found: Option<Entry>) -> Option<Entry> {
        match found {
            Some(entry) if entry.is_grave() => None,
            Some(entry) if self.indexes[at].is_covered_key(key.as_slice(), entry.lsn) => None,
            found => found,
        }
    }

    /// The newest sealed version of a key, from the spot index once it holds every sealed key
    fn newest_live(&self, at: usize, key: &RecordKey) -> Result<Option<Entry>> {
        match self.spot_serves() {
            true => match self.spot[at].entry(key)? {
                Settled::Entry(found) => Ok(found),
                Settled::Footers => self.newest_sealed(at, key, None),
            },
            false => self.newest_sealed(at, key, None),
        }
    }

    /// The ceiling on what one sealed segment can answer with, for ordering a fan-out
    ///
    /// A segment nothing recorded one for can hold anything as far as this knows,
    /// which is the reading that keeps a walk from stopping short of it.
    fn ceiling_of(&self, segment: SegmentId) -> Lsn {
        self.segments.max_lsn_of(segment).unwrap_or(NO_CEILING)
    }

    /// The candidates for a key, ordered by what each of them can hold
    ///
    /// Highest ceiling first, and the newest segment first among equal ceilings,
    /// which is the order the fan-out settles a tie between two rows in.
    fn ordered_candidates(&self, candidates: &Candidates) -> Vec<(Lsn, SegmentId)> {
        let mut ordered: Vec<(Lsn, SegmentId)> = Vec::with_capacity(candidates.len());
        for segment in candidates.iter() {
            ordered.push((self.ceiling_of(segment), segment));
        }
        ordered.sort_unstable_by(|left, right| right.cmp(left));
        ordered
    }

    /// The newest row the sealed footers hold for one key, ordered so it can stop
    ///
    /// A ceiling of none takes the newest row there is, which is what a live read
    /// wants; a snapshot read passes its own and the rows above it are not answers.
    ///
    /// Candidates are walked by descending ceiling, the highest sequence number each
    /// sealed footer holds, and the walk stops once the best row in hand is strictly
    /// above the ceiling of the next candidate. The ceiling has to come off the footer
    /// and not off the byte counters, which leave the oldest-record mark alone and
    /// would miss a segment whose newest row is its tombstone. Two cases fall back to
    /// the full fan-out: a segment nothing recorded a ceiling for can hold anything,
    /// and a tie is walked out, since a copy carries its source's sequence number and
    /// two standing segments can hold one version of a key.
    fn newest_sealed(
        &self,
        at: usize,
        key: &RecordKey,
        snapshot: Option<Lsn>,
    ) -> Result<Option<Entry>> {
        let Some(footers) = self.footers.get() else {
            return Ok(None);
        };
        let candidates = self.sealed[at].candidates(key.as_slice());
        // Ordered only where there is something to stop short of, so a column
        // written in key order pays nothing for a walk of one candidate.
        let ordered = match candidates.len() > 1 {
            true => self.ordered_candidates(&candidates),
            false => Vec::new(),
        };

        let mut newest: Option<(Entry, SegmentId)> = None;
        for (visited, unordered) in candidates.iter().enumerate() {
            let segment = match ordered.is_empty() {
                true => unordered,
                false => ordered[visited].1,
            };
            if let (Some((best, _)), Some(next)) = (newest, ordered.get(visited)) {
                // Above this ceiling is above every ceiling left, since the walk is
                // ordered by them, so nothing left can hold a row that wins.
                if best.lsn > next.0 {
                    break;
                }
            }

            let Some(found) = footers.find(segment, key.column, key.as_slice())? else {
                continue;
            };
            if snapshot.is_some_and(|snapshot| found.lsn > snapshot) {
                continue;
            }
            // The newest row wins, and a tie goes to the newest segment: a copy
            // compaction made carries its source's sequence number, so while both
            // stand the two rows tie.
            if newest.is_some_and(|(best, from)| (best.lsn, from) >= (found.lsn, segment)) {
                continue;
            }
            let entry = match found.is_tombstone() || found.is_range_tombstone() {
                true => Entry::grave(found.lsn),
                false => {
                    let loc = Loc::new(segment, found.offset, found.len);
                    // Stamped read-only: a segment retired since the search
                    // offered it stamps none, which no read ever trusts.
                    let stamp = self.segments.incarnation_of(segment);
                    Entry::new(loc, found.lsn).stamped(stamp)
                }
            };
            newest = Some((entry, segment));
        }

        Ok(newest.map(|(entry, _)| entry))
    }

    /// Every place on the volume that answers for one key
    ///
    /// The map's entry, the segments the search would read, and what every sealed
    /// footer holds whether the search asks it or not. The footers are read directly
    /// rather than through the search, so a filter that wrongly rules a segment out is
    /// visible here. It reads every sealed segment for the column, so it belongs in a
    /// report about one key rather than in a hot path.
    pub fn sites(&self, key: &RecordKey) -> Result<KeySites> {
        let Some(at) = self.slot(key.column) else {
            return Ok(KeySites::default());
        };
        let resident = self.indexes[at].entry_or_grave(key.as_slice());
        // The search's own order rather than the raw candidate list, so a report says
        // which segments the walk reaches first and where it would have stopped.
        let found = self.sealed[at].candidates(key.as_slice());
        let candidates: Vec<SegmentId> = self
            .ordered_candidates(&found)
            .into_iter()
            .map(|(_, segment)| segment)
            .collect();

        let mut sealed = Vec::new();
        if let Some(footers) = self.footers.get() {
            for segment in self.sealed[at].segments() {
                let Some(footer) = footers.footer(segment)? else {
                    continue;
                };
                // Every partition rather than the first, since a column reaching a
                // footer twice would leave the search reading only one of them.
                for partition in footer
                    .partitions
                    .iter()
                    .filter(|part| part.column == key.column)
                {
                    let Some(row) = partition.find_row(key.as_slice()).transpose()? else {
                        continue;
                    };
                    sealed.push(SealedSite {
                        segment,
                        lsn: row.lsn,
                        offset: row.offset,
                        is_grave: row.is_tombstone() || row.is_range_tombstone(),
                        is_candidate: candidates.contains(&segment),
                        passes_filter: partition.may_hold(key.as_slice()),
                    });
                }
            }
        }

        Ok(KeySites {
            resident,
            candidates,
            sealed,
        })
    }

    /// What the sealed footers say, with "gone" told apart from "nothing at all"
    ///
    /// A read wants both as a miss. A caller deciding what to do with a record it is
    /// holding needs the difference: one means the record is dead, the other means
    /// nobody is pointing at it.
    fn sealed_state(&self, at: usize, key: &RecordKey) -> Result<Sealed> {
        if let Some(entry) = self.indexes[at].entry_or_grave(key.as_slice()) {
            return Ok(match entry.is_grave() {
                true => Sealed::Gone,
                false => Sealed::Live(entry),
            });
        }
        // The write path takes the read path's own fan-out, so an overwrite probe
        // stops on the same terms a get does.
        Ok(match self.newest_live(at, key)? {
            None => Sealed::Absent,
            Some(entry) if entry.is_grave() => Sealed::Gone,
            Some(entry) if self.indexes[at].is_covered_key(key.as_slice(), entry.lsn) => {
                Sealed::Gone
            }
            Some(entry) => Sealed::Live(entry),
        })
    }

    /// Where a key's live record was as of an older sequence number
    ///
    /// The index holds one version per key, so an entry newer than the snapshot is
    /// the wrong record entirely and what answers is the newest footer row at or
    /// below the snapshot. The seal taken when the snapshot was made puts everything
    /// at or below that number in a segment with a footer, and the search reaches
    /// that footer once its spans are noted, so the caller settles the sealed queue
    /// first.
    pub fn get_at(&self, key: &RecordKey, snapshot: Lsn) -> Result<Option<Entry>> {
        let Some(at) = self.slot(key.column) else {
            return Ok(None);
        };
        let index = &self.indexes[at];
        if let Some(entry) = index.entry_or_grave(key.as_slice()) {
            if entry.lsn <= snapshot {
                // The map is answering as of the snapshot. A grave here is a
                // delete the snapshot can see, so the key was already gone.
                if entry.is_grave() || index.is_covered_key_at(key.as_slice(), entry.lsn, snapshot)
                {
                    return Ok(None);
                }
                return Ok(Some(entry));
            }
            // Newer than the snapshot, so this reader cannot see it. Whatever it
            // replaced is in a footer, which is where the search goes next.
        }
        self.sealed_entry_at(at, key, snapshot)
    }

    /// The newest thing any sealed footer says about a key at or below a number
    ///
    /// Rows above the snapshot record writes that had not happened yet and are
    /// ignored outright; the same tombstone and cover rules apply to what is left.
    fn sealed_entry_at(&self, at: usize, key: &RecordKey, snapshot: Lsn) -> Result<Option<Entry>> {
        match self.newest_sealed(at, key, Some(snapshot))? {
            Some(entry) if entry.is_grave() => Ok(None),
            Some(entry)
                if self.indexes[at].is_covered_key_at(key.as_slice(), entry.lsn, snapshot) =>
            {
                Ok(None)
            }
            found => Ok(found),
        }
    }

    /// Whether a column answers this key from a footer rather than from the map
    ///
    /// Asked before acting rather than after, since the callers book the copy dead
    /// when they decline and acting first would book it twice.
    fn is_paged_key(&self, at: usize, key: &RecordKey) -> bool {
        self.indexes[at].entry_or_grave(key.as_slice()).is_none()
    }

    /// Where one column's index sits, for the callers that need its sealed ranges
    fn slot(&self, column: ColumnId) -> Option<usize> {
        self.by_id[column.as_index()]
    }

    /// Where to read footers for a column that has sealed something
    fn paged_footers(&self, at: usize) -> Option<&Arc<dyn FooterSource>> {
        self.footers.get().filter(|_| !self.sealed[at].is_empty())
    }

    /// Whether a key resolves to a live record
    pub fn contains(&self, key: &RecordKey) -> Result<bool> {
        Ok(self.get(key)?.is_some())
    }

    /// Recorded payload length of a live key, served without reading the record
    pub fn size_of(&self, key: &RecordKey) -> Result<Option<ByteCount>> {
        Ok(self
            .get(key)?
            .map(|entry| ByteCount::from_bytes(u64::from(entry.loc.len))))
    }

    /// Give one sealed partition's keys up, a lane of shards to a thread
    pub fn page_out_partition(
        &self,
        segment: SegmentId,
        partition: &FooterPartition,
    ) -> Result<usize> {
        let Some(at) = self.slot(partition.column) else {
            return Ok(0);
        };
        let spot = &self.spot[at];
        let index = &self.indexes[at];
        // No guard goes while a hand-over runs
        let _handing = read(&self.handing);
        let lifted = index.lifted();
        let mut rows: Vec<(&[u8], Loc)> = Vec::with_capacity(partition.len());
        let mut asked: Vec<(&[u8], Loc)> = Vec::new();
        let mut newest = Lsn::NONE;
        for row_at in 0..partition.len() {
            // The key is borrowed out of the packed bytes, never decoded into an entry
            let Some(key) = partition.key_at(row_at) else {
                continue;
            };
            // Only a key's last version in the segment can be the map's, so older ones are skipped
            if partition.key_at(row_at + 1) == Some(key) {
                continue;
            }
            let row = partition.row_at(row_at)?;
            if !row.flags.is_data() {
                continue;
            }
            newest = newest.max(row.lsn);
            // A row a pruned guard may have hidden goes to the spot index only if the map still points at it
            match row.lsn <= lifted {
                true => asked.push((key, Loc::new(segment, row.offset, row.len))),
                false => rows.push((key, Loc::new(segment, row.offset, row.len))),
            }
        }
        // Raised before any key leaves the map, so a write finding its place empty sees it
        self.handed.fetch_max(newest.as_u64(), Ordering::AcqRel);
        if mostly_outversioned(index, &rows) {
            asked.append(&mut rows);
        }
        // An asked row goes to the spot index under the map's lock, and only if the map still holds it
        let took = in_lanes(asked.len(), &|lane, lanes| {
            index.page_out_lane(&asked, lane, lanes, &|key, loc| {
                spot.insert(key, loc);
            })
        });
        let inserted = in_lanes(rows.len(), &|lane, lanes| {
            spot.insert_lane(&rows, lane, lanes)
        });
        // The spot index takes every key before the map lets any go, so no read misses both
        crate::sync::rendezvous::at("paged/handover-spot");
        let handed = in_lanes(rows.len(), &|lane, lanes| {
            index.page_out_lane(&rows, lane, lanes, &|_, _| {})
        });
        let back: Vec<(&[u8], Loc)> = rows
            .iter()
            .zip(inserted.iter().zip(&handed))
            .filter(|(_, (inserted, handed))| **inserted && !**handed)
            .map(|(row, _)| *row)
            .collect();
        // Only keys this hand-over put in and the map kept come back out of the spot index
        if !back.is_empty() {
            spot.remove_lane(&back, 0, 1);
        }
        Ok(handed.iter().chain(&took).filter(|handed| **handed).count())
    }

    /// Record the keys one newly sealed segment covers for one column
    pub fn note_sealed(
        &self,
        column: ColumnId,
        segment: SegmentId,
        lowest: KeyBytes,
        highest: KeyBytes,
    ) {
        if let Some(at) = self.slot(column) {
            self.sealed[at].note(segment, lowest, highest);
        }
    }

    /// Note every column's span in one sealed segment, from the footer it sealed with
    ///
    /// Worth doing on every volume, since it names the segments a search must consider
    /// and rules out the rest. Here rather than in the engine because a merge notes
    /// its output too.
    pub fn note_spans(&self, segment: SegmentId, footer: &SegmentFooter) -> Result<()> {
        // The ceiling goes down before any span does: a search the span admits must
        // never meet a segment the fan-out believes can hold nothing.
        self.segments.note_max(segment, footer.max_lsn);
        for partition in &footer.partitions {
            let Some((lowest, highest)) = partition.key_range() else {
                continue;
            };
            let (lowest, highest) = (KeyBytes::new(lowest)?, KeyBytes::new(highest)?);
            self.note_sealed(partition.column, segment, lowest, highest);
        }
        Ok(())
    }

    /// The columns this index was built over
    pub fn columns(&self) -> ColumnSet {
        self.columns
    }

    /// Per-segment reclaimable-byte counters for the reel
    pub fn segments(&self) -> &SegmentTable {
        &self.segments
    }

    /// The counters themselves, for the seal that writes a segment's tally down
    pub fn segments_handle(&self) -> Arc<SegmentTable> {
        Arc::clone(&self.segments)
    }

    /// The declaration of one column, or nothing if the reel does not serve it
    pub fn spec(&self, column: ColumnId) -> Option<&ColumnSpec> {
        self.slot(column).map(|at| &self.columns[at])
    }

    /// The index of one column, or nothing if the reel does not serve it
    pub fn column(&self, column: ColumnId) -> Option<&ColumnIndex> {
        self.slot(column).map(|at| &self.indexes[at])
    }

    /// Apply a committed data record, guarded by its sequence number
    ///
    /// A put that lands on an empty place may be an overwrite the map cannot see,
    /// since a paged column gives its keys up. So the map goes first and only a put
    /// that found nothing asks the footers.
    pub fn insert(&self, key: &RecordKey, loc: Loc, lsn: Lsn) -> Result<bool> {
        let landed = self.insert_mapped(key, loc, lsn)?;
        if landed.may_be_paged() {
            self.settle_displaced(key, lsn)?;
        }
        Ok(landed != Landed::Newer)
    }

    /// Move the map for a committed record, leaving the paged settle to the caller
    ///
    /// The shard lock can wait on a spot shard and read a record header, for a key with no map entry at or below the newest version the spot index was given.
    pub fn insert_mapped(&self, key: &RecordKey, loc: Loc, lsn: Lsn) -> Result<Landed> {
        let Some(at) = self.slot(key.column) else {
            return Ok(Landed::Newer);
        };
        let failed = Cell::new(None);
        let landed = self.indexes[at].insert(
            key.as_slice(),
            Entry::new(loc, lsn),
            &self.segments,
            &|key: &[u8], lsn: Lsn| self.is_shadowed(at, key, lsn, &failed, true),
        );
        failed.into_inner().map_or(Ok(landed), Err)
    }

    /// Whether the spot index holds a version newer than `lsn`, a failed read kept for the caller and answered as `failing`
    fn is_shadowed(
        &self,
        at: usize,
        key: &[u8],
        lsn: Lsn,
        failed: &Cell<Option<ReelError>>,
        failing: bool,
    ) -> bool {
        // Nothing the spot index was given is newer than a write above this
        if !self.spot_serves() || lsn.as_u64() > self.handed.load(Ordering::Acquire) {
            return false;
        }
        match self.spot[at].holds_newer(KeyRef::new(self.columns[at].id, key), lsn) {
            Ok(is_newer) => is_newer,
            Err(error) => {
                failed.set(Some(error));
                failing
            }
        }
    }

    /// Book the footer-held record a mapped mutation displaced, safe to defer since only compaction reads the booking
    pub fn settle_displaced(&self, key: &RecordKey, lsn: Lsn) -> Result<bool> {
        let Some(at) = self.slot(key.column) else {
            return Ok(false);
        };
        if self.spot_serves() {
            let mut settled = false;
            let displaced = self.spot[at].displace(key, lsn)?;
            for loc in displaced.booked {
                settled |= self.indexes[at].settle_paged(key.as_slice(), loc, &self.segments);
            }
            for (loc, least) in displaced.classed {
                settled |=
                    self.indexes[at].settle_paged_least(key.as_slice(), loc, least, &self.segments);
            }
            for (loc, least) in displaced.rebooked {
                self.indexes[at].rebook_paged(key.as_slice(), least, loc.len);
            }
            return Ok(settled);
        }
        let Some(displaced) = self.sealed_entry(at, key)? else {
            return Ok(false);
        };
        Ok(self.indexes[at].settle_paged(key.as_slice(), displaced.loc, &self.segments))
    }

    /// Codec this column asks admission to attempt on its payloads
    pub fn codec_of(&self, column: ColumnId) -> Codec {
        self.spec(column).map_or(Codec::None, |spec| spec.codec)
    }

    /// Drop a key on a tombstone, guarded by its sequence number
    ///
    /// The record is resolved before the grave goes in, since on a paged column the
    /// grave is what stops the search that would find it. What comes back says
    /// whether a live record went, wherever it was being answered from.
    pub fn remove(&self, key: &RecordKey, lsn: Lsn, tombstone: Loc) -> Result<bool> {
        let landed = self.remove_mapped(key, lsn, tombstone)?;
        match landed.may_be_paged() && self.settle_displaced(key, lsn)? {
            true => Ok(true),
            false => Ok(landed.dropped_record()),
        }
    }

    /// Publish a batch's moves under the barrier, so a spanning read sees all or none
    ///
    /// Batches publishing at the same time share one hold and move side by side under
    /// shard locks. The hold moves the maps, and for a key with no map entry at or below
    /// the newest version the spot index was given it can wait on a spot shard and read
    /// one record header. Settling what the moves displaced reads more, so it runs after
    /// the hold.
    ///
    /// The key moves go in the order the batch built them, with each range standing its
    /// cover at the point of the run it was given at. What comes back is one answer per
    /// key move, a cover displacing nothing, and beside them any failed shadow read. A
    /// key whose shadow read failed publishes, so the batch stays whole, and the caller
    /// raises the error once its settles have run.
    pub fn publish_batch(
        &self,
        moves: &[KeyMove<'_>],
        ranges: &[RangeMove<'_>],
    ) -> (Vec<Landed>, Result<()>) {
        let failed = Cell::new(None);
        let landed = self.publish.publish_grouped(|| {
            let mut landed = Vec::with_capacity(moves.len());
            let mut at = 0;
            for range in ranges {
                let upto = range.after.min(moves.len());
                if upto > at {
                    self.apply_moves(&moves[at..upto], &failed, &mut landed);
                    at = upto;
                }
                self.cover_range(range.start, range.end, range.lsn, range.tombstone);
            }
            if at < moves.len() {
                self.apply_moves(&moves[at..], &failed, &mut landed);
            }
            landed
        });
        (landed, failed.into_inner().map_or(Ok(()), Err))
    }

    /// Resolve every key against one state of the maps
    ///
    /// Under the barrier so a batch publishing beside this cannot answer some of
    /// the keys from before it and the rest from after. One key takes nothing,
    /// since one key cannot be half a batch.
    pub fn get_many(&self, keys: &[RecordKey]) -> Result<Located> {
        let mut picks = Vec::new();
        // One key cannot be half a batch, and grouping it would put the barrier, the
        // sort and the shard run in front of a single descent for nothing.
        if keys.len() < 2 {
            let Some(key) = keys.first() else {
                return Ok(Located {
                    found: Vec::new(),
                    picks,
                });
            };
            let found = match self.spot_pick(0, key) {
                Some(pick) => {
                    picks.push(pick);
                    None
                }
                None => self.get(key)?,
            };
            return Ok(Located {
                found: vec![found],
                picks,
            });
        }
        let _reading = self.publish.reading();
        let mut found: Vec<Option<Entry>> = vec![None; keys.len()];

        // Grouped by column and handed over together, so the shards a batch touches
        // are locked once each. The answers are placed back where the keys were asked.
        // Both lists it groups through are this thread's and come back after: the keys
        // themselves are addressed by position rather than copied into a list here.
        let mut held = HeldLookup::take();
        let lookup = &mut held.0;
        let order = &mut lookup.order;
        order.clear();
        order.extend(0..keys.len());
        order.sort_unstable_by_key(|at| keys[*at].column);

        let answers = &mut lookup.answers;
        let mut at = 0;
        while at < order.len() {
            let column = keys[order[at]].column;
            let mut end = at + 1;
            while end < order.len() && keys[order[end]].column == column {
                end += 1;
            }
            let Some(slot) = self.slot(column) else {
                at = end;
                continue;
            };
            self.indexes[slot].entry_many(keys, &order[at..end], answers);
            for (index, entry) in order[at..end].iter().zip(answers.iter()) {
                let key = &keys[*index];
                found[*index] = match entry {
                    Some(entry) if entry.is_grave() => None,
                    // An entry a cover spans is a key the drop took, and the map
                    // held the newest version, so no footer is left to ask.
                    Some(entry) if self.indexes[slot].is_covered_key(key.as_slice(), entry.lsn) => {
                        None
                    }
                    Some(entry) => Some(*entry),
                    // A key the map lacks may sit in a sealed segment, so its pick reads later in one batch
                    None => match self.spot_pick(*index, key) {
                        Some(pick) => {
                            picks.push(pick);
                            None
                        }
                        // The map gained the key since the look above, or the spot index is not serving yet, so a full get answers
                        None => self.get(key)?,
                    },
                };
            }
            at = end;
        }
        Ok(Located { found, picks })
    }

    /// Start a spot index lookup under the caller's barrier, for a key missing from the map
    fn spot_pick(&self, at: usize, key: &RecordKey) -> Option<SpotPick> {
        let SpotRoute::Column(column, since) = self.spot_route(key) else {
            return None;
        };
        let pick = self.spot[column].pick(key)?;
        Some(SpotPick {
            at,
            column,
            pick,
            since,
        })
    }

    /// Run a follower's apply pass under the exclusive barrier
    ///
    /// A pass moves the index the same key at a time a batch does while a follower
    /// serves reads throughout. Every device read the pass needs is done before it enters.
    pub fn publish_pass<Applied>(&self, apply: impl FnOnce() -> Applied) -> Applied {
        let _publishing = self.publish.publishing();
        apply()
    }

    /// Apply a batch's moves in arrival order, sharing locks where keys allow
    ///
    /// Runs sharing a column are found here and runs sharing a shard below it.
    /// Nothing is reordered, so a batch publishes exactly what it published. The
    /// answers are appended, since a batch carrying a range applies in several runs.
    fn apply_moves(
        &self,
        moves: &[KeyMove<'_>],
        failed: &Cell<Option<ReelError>>,
        landed: &mut Vec<Landed>,
    ) {
        let mut at = 0;
        while at < moves.len() {
            let column = moves[at].column;
            let mut end = at + 1;
            while end < moves.len() && moves[end].column == column {
                end += 1;
            }
            match self.slot(column) {
                Some(slot) => self.indexes[slot].apply_moves(
                    &moves[at..end],
                    &*self.segments,
                    &|key: &[u8], lsn: Lsn| self.is_shadowed(slot, key, lsn, failed, false),
                    landed,
                ),
                // A column nothing indexes takes the same answer one key at a
                // time would have given, once for each key it would have gone to.
                None => landed.resize(landed.len() + (end - at), Landed::Newer),
            }
            at = end;
        }
    }

    /// Drop a key from the map alone, leaving the paged settle to the caller
    ///
    /// The other half of the split `insert_mapped` describes, with the same shadow check under the shard lock.
    pub fn remove_mapped(&self, key: &RecordKey, lsn: Lsn, tombstone: Loc) -> Result<Landed> {
        let Some(at) = self.slot(key.column) else {
            return Ok(Landed::Newer);
        };
        let failed = Cell::new(None);
        let landed = self.indexes[at].remove(
            key.as_slice(),
            lsn,
            tombstone,
            &self.segments,
            &|key: &[u8], lsn: Lsn| self.is_shadowed(at, key, lsn, &failed, true),
        );
        failed.into_inner().map_or(Ok(landed), Err)
    }

    /// Take a range with one standing cover and one tombstone record, nothing more
    ///
    /// The cover goes up and every read, walk and insert consults it from here on;
    /// the records it spans, resident and footer-held both, are settled by the lazy
    /// sweep on the maintenance tick.
    pub fn remove_range(
        &self,
        start: &RecordKey,
        end: Option<&[u8]>,
        lsn: Lsn,
        tombstone: Loc,
    ) -> Result<()> {
        self.cover_range(start, end, lsn, tombstone);
        Ok(())
    }

    /// Stand one range's cover, which is what a range delete does to the index
    fn cover_range(&self, start: &RecordKey, end: Option<&[u8]>, lsn: Lsn, tombstone: Loc) {
        let Some(at) = self.slot(start.column) else {
            return;
        };
        self.indexes[at].remove_range(start.as_slice(), end, lsn);
        // The delete's own record holds space in whichever segment took it, and a
        // segment of nothing but these would otherwise carry no row at all.
        self.segments.mark_held(
            tombstone.segment,
            lsn,
            self.indexes[at].span_of(tombstone.len),
        );
    }

    /// Run one bounded pass of the lazy sweep every standing cover is owed
    ///
    /// Oldest cover first, and each cover in two phases whose order carries the
    /// correctness: footer-held records are settled while the covered map entries
    /// still stand, and the map entries drop after. What comes back is whether
    /// anything is still owed.
    pub fn sweep_covers(&self, budget: usize) -> Result<bool> {
        let mut remaining = budget;
        for (at, index) in self.indexes.iter().enumerate() {
            while remaining > 0 {
                let Some(pending) = index.next_pending_cover() else {
                    break;
                };
                match &pending.release_from {
                    Some(from) => {
                        let spent = self.release_run(at, &pending, from, remaining)?;
                        remaining = remaining.saturating_sub(spent.max(1));
                    }
                    None => {
                        let (_, examined, done) =
                            index.sweep_run(pending.lsn, remaining, &self.segments);
                        remaining = remaining.saturating_sub(examined.max(1));
                        if !done {
                            remaining = 0;
                        }
                    }
                }
            }
        }
        Ok(self.has_pending_covers())
    }

    /// Whether any column's covers are still owed their sweep
    ///
    /// Compaction asks before retiring anything: a retired segment takes its footer
    /// and its counters with it, and an unsettled record would be held forever.
    pub fn has_pending_covers(&self) -> bool {
        self.indexes.iter().any(|index| index.has_pending_covers())
    }

    /// Settle one bounded run of the footer-held records one cover spans, each one the spot index holds live
    fn release_run(
        &self,
        at: usize,
        pending: &PendingCover,
        from: &[u8],
        limit: usize,
    ) -> Result<usize> {
        let index = &self.indexes[at];
        let Some(footers) = self.paged_footers(at) else {
            index.advance_release(pending.lsn, None);
            return Ok(0);
        };
        let column = self.columns[at].id;
        let paged = self.paged_at(at, column, footers);
        let mut playback = PlaybackCursor::new(column, Way::Up, Bound::Included(from))?;
        let mut found: Vec<(KeyBytes, Loc)> = Vec::new();
        let run = playback::release_rows(
            &paged,
            &mut playback,
            pending.end.as_deref(),
            pending.lsn,
            limit.min(RELEASE_RUN),
            &mut found,
        )?;
        let spot = &self.spot[at];
        for (key, loc) in &found {
            index.release_covered(key.as_slice(), *loc, &self.segments, || {
                spot.take_live(key.as_slice(), *loc)
            });
        }
        index.advance_release(pending.lsn, run.resume.as_deref());
        Ok(run.examined)
    }

    /// Where a follower's copy came from when its source retired before the pass, read off the key's one live slot
    pub fn retired_source(&self, key: &RecordKey, retired: &[SegmentId], len: u32) -> Option<Loc> {
        let at = self.slot(key.column)?;
        if retired.is_empty() || !self.spot_serves() {
            return None;
        }
        let (segment, offset) = self.spot[at].only_live(key.as_slice())?;
        retired
            .contains(&segment)
            .then(|| Loc::new(segment, offset, len))
    }

    /// Repoint a key from a compacted record to its rewritten copy under a guard
    pub fn repoint(
        &self,
        key: &RecordKey,
        from: Option<Loc>,
        to: Loc,
        expected_lsn: Lsn,
    ) -> Result<bool> {
        let moves = [KeyRepoint {
            key: key.clone(),
            from,
            to,
            lsn: expected_lsn,
        }];
        Ok(self.repoint_run(&moves)? == 1)
    }

    /// Repoint a run of moved records at their copies, booking the run's bytes in bulk to spare the shared table
    pub fn repoint_run(&self, moves: &[KeyRepoint]) -> Result<u64> {
        let mut copies: Vec<(SegmentId, u64, Lsn, SegmentIncarnation)> = Vec::new();
        for repoint in moves {
            let span = span_of(repoint.key.as_slice().len() as u16, repoint.to.len);
            match copies.iter_mut().find(|copy| copy.0 == repoint.to.segment) {
                Some(copy) => {
                    copy.1 += span;
                    copy.2 = copy.2.min(repoint.lsn);
                }
                None => copies.push((
                    repoint.to.segment,
                    span,
                    repoint.lsn,
                    SegmentIncarnation::NONE,
                )),
            }
        }
        // Copies go live before any repoint publishes, so a write that drops a moved key finds its copy counted
        for copy in &mut copies {
            self.segments.mark_live(copy.0, copy.2, copy.1);
            copy.3 = self.segments.live_incarnation(copy.0);
        }
        let mut released: Vec<(SegmentId, u64)> = Vec::new();
        let mut lost: Vec<(SegmentId, u64)> = Vec::new();
        let mut moved = 0u64;
        let mut failed = None;
        for repoint in moves {
            let width = repoint.key.as_slice().len() as u16;
            let stamp = copies
                .iter()
                .find(|copy| copy.0 == repoint.to.segment)
                .map_or(SegmentIncarnation::NONE, |copy| copy.3);
            let outcome = match failed {
                Some(_) => Ok(None),
                None => {
                    self.repoint_moved(&repoint.key, repoint.from, repoint.to, repoint.lsn, stamp)
                }
            };
            let (segment, span, into) = match outcome {
                Ok(Some(from)) => {
                    moved += 1;
                    (from.segment, span_of(width, from.len), &mut released)
                }
                Ok(None) => (
                    repoint.to.segment,
                    span_of(width, repoint.to.len),
                    &mut lost,
                ),
                Err(error) => {
                    failed = Some(error);
                    (
                        repoint.to.segment,
                        span_of(width, repoint.to.len),
                        &mut lost,
                    )
                }
            };
            match into.iter_mut().find(|held| held.0 == segment) {
                Some(held) => held.1 += span,
                None => into.push((segment, span)),
            }
        }
        for (segment, span) in lost {
            self.segments.shadow(segment, span);
        }
        for (segment, span) in released {
            self.segments.release_live(segment, span);
        }
        match failed {
            Some(error) => Err(error),
            None => Ok(moved),
        }
    }

    /// Move one key's entry from a compacted record to its copy, returning the source and booking nothing
    fn repoint_moved(
        &self,
        key: &RecordKey,
        from: Option<Loc>,
        to: Loc,
        expected_lsn: Lsn,
        stamp: SegmentIncarnation,
    ) -> Result<Option<Loc>> {
        let Some(at) = self.slot(key.column) else {
            return Ok(None);
        };
        let index = &self.indexes[at];
        if !self.is_paged_key(at, key) {
            match index.repoint(key.as_slice(), to, expected_lsn, stamp) {
                Some(from) => return Ok(Some(from)),
                // A hand-over may page the key out between the looks, and declining then would retire its only record
                None if !self.is_paged_key(at, key) => return Ok(None),
                None => {}
            }
        }
        let spot = &self.spot[at];
        // The pass read the source, so the key's one spot index slot there is this version and needs no read
        if let Some(from) =
            from.filter(|from| self.spot_serves() && spot.only_at(key.as_slice(), *from))
        {
            let take = || spot.take_live(key.as_slice(), from);
            return Ok(index
                .repoint_paged(key.as_slice(), to, expected_lsn, stamp, take)
                .then_some(from));
        }
        match self.sealed_state(at, key)? {
            Sealed::Live(entry) if entry.lsn == expected_lsn => {
                let take = || spot.take_live(key.as_slice(), entry.loc);
                Ok(index
                    .repoint_paged(key.as_slice(), to, expected_lsn, stamp, take)
                    .then_some(entry.loc))
            }
            // A newer version won the race, so the copy is dead on arrival
            Sealed::Live(_) | Sealed::Gone => Ok(None),
            // Nothing anywhere answers for this key, which does not mean nothing
            // does: a segment that sealed since the pass began is invisible here,
            // and adopting the copy would write the source's sequence number into
            // the map, which a read trusts ahead of any footer row. Refusing is
            // safe, since the source holds the record until the pass retires it.
            Sealed::Absent => Ok(None),
        }
    }

    /// Drop a key while it still resolves one exact location, writing no tombstone
    ///
    /// The guard is the location, so a key that has moved on since the caller looked
    /// is left alone. A paged key needs a grave, since taking it out of a map it is
    /// not in would leave the footer answering for a record that will not read. No
    /// tombstone stands behind that grave, so it stays for the life of the process.
    pub fn evict_at(&self, key: &RecordKey, at: Loc) -> Result<bool> {
        let Some(column_at) = self.slot(key.column) else {
            return Ok(false);
        };
        let index = &self.indexes[column_at];
        if !self.is_paged_key(column_at, key) {
            // A key in the map can be one compaction has just repointed into an open
            // tail, and until that tail seals the map is the only thing answering for
            // the record, so taking the key out would take the record with it.
            if !self.sealed[column_at].holds(at.segment) {
                return Ok(false);
            }
            return Ok(index.evict_at(key.as_slice(), at, &self.segments));
        }
        match self.sealed_entry(column_at, key)? {
            Some(entry) if entry.loc == at => {
                let spot = &self.spot[column_at];
                Ok(
                    index.evict_paged(key.as_slice(), at, entry.lsn, &self.segments, || {
                        spot.take_live(key.as_slice(), at)
                    }),
                )
            }
            Some(_) | None => Ok(false),
        }
    }

    /// Stand a grave for a point tombstone compaction copied, so no older record answers before its segment is noted
    pub fn hold_grave(&self, key: &RecordKey, lsn: Lsn, segment: SegmentId) {
        // A loading spot index cannot rule out a newer version, so no grave stands yet
        if !self.spot_serves() {
            return;
        }
        let Some(at) = self.slot(key.column) else {
            return;
        };
        let spot = &self.spot[at];
        // A hand-over fills the spot index before the map lets go, so one of the two shows a newer version
        self.indexes[at].hold_grave(key.as_slice(), lsn, segment, || {
            spot.may_hold_newer(key.as_slice(), lsn)
        });
    }

    /// Take out the grave a tombstone left once compaction drops that tombstone, since nothing older is left for it to hide
    pub fn drop_grave(&self, key: &RecordKey, lsn: Lsn) -> bool {
        self.column(key.column)
            .is_some_and(|index| index.drop_grave(key.as_slice(), lsn))
    }

    /// Book a carried tombstone's footprint in the segment it was copied into
    ///
    /// The key only says which column's width the record was framed at.
    pub fn hold(&self, key: &RecordKey, lsn: Lsn, at: Loc) {
        if let Some(index) = self.column(key.column) {
            self.segments
                .mark_held(at.segment, lsn, index.span_of(at.len));
        }
    }

    /// Drop what tombstones hold across every column, once nothing older can arrive
    ///
    /// The floor is the caller's to choose, since what a tombstone holds out against
    /// is bounded by what the admission budget lets a writer hold in flight.
    pub fn prune_tombstones(&self, before: Lsn) -> u64 {
        // A hand-over's rows sit in the spot index until the map is asked about them
        let _handing = write(&self.handing);
        self.indexes
            .iter()
            .enumerate()
            .map(|(at, index)| index.prune_tombstones(before, &self.sealed[at]))
            .sum()
    }

    /// Graves held across every column, the memory a prune would give back
    pub fn grave_count(&self) -> u64 {
        self.indexes.iter().map(|index| index.grave_count()).sum()
    }

    /// Ranges held across every column, tested against every insert into them
    pub fn cover_count(&self) -> u64 {
        self.indexes.iter().map(|index| index.cover_count()).sum()
    }

    /// Heap bytes allocated by the maps, the shards, their filters and the spot index
    pub fn resident_bytes(&self) -> ByteCount {
        let maps: u64 = self.indexes.iter().map(ColumnIndex::heap_bytes).sum();
        ByteCount::from_bytes(maps + self.spot_heap_bytes())
    }

    /// Live key count and payload byte total across every column
    ///
    /// Under the barrier for the same reason a many-key read is: the counters move a
    /// key at a time as a batch publishes, and a count taken inside that loop counts
    /// part of a batch.
    pub fn totals(&self) -> Totals {
        self.publish.reading_all(|| {
            let mut count = 0u64;
            let mut bytes = 0u64;
            for index in &self.indexes {
                let totals = index.totals();
                count += totals.count;
                bytes += totals.bytes.to_bytes();
            }
            Totals {
                count,
                bytes: ByteCount::from_bytes(bytes),
            }
        })
    }

    /// What every column's keys do to the tree's lead search, column by column
    ///
    /// A column whose keys all share their first eight bytes falls out of the vector
    /// compare into a walk of full keys. It stays correct and says nothing, which is
    /// the whole reason to report it. Nothing comes back for a column with no leads
    /// or too few keys, and it walks the leaves of every occupied shard.
    pub fn lead_tie_rates(&self) -> Vec<(ColumnId, Option<f64>, u64)> {
        self.columns
            .iter()
            .zip(&self.indexes)
            .map(|(spec, index)| (spec.id, index.lead_tie_rate(), index.resident_keys()))
            .collect()
    }

    /// Live key count and payload byte total for one column, zero for a column the map does not hold
    pub fn column_totals(&self, column: ColumnId) -> Totals {
        self.publish.reading_all(|| match self.column(column) {
            Some(index) => index.totals(),
            None => Totals {
                count: 0,
                bytes: ByteCount::from_bytes(0),
            },
        })
    }

    /// Live totals for a shard-aligned prefix of a column, if it is one
    pub fn prefix_totals(&self, column: ColumnId, prefix: &[u8]) -> Option<Totals> {
        self.publish.reading_all(|| {
            let at = self.slot(column)?;
            self.indexes[at].prefix_totals(prefix)
        })
    }

    /// Sealed segments recorded as covering keys of this column
    pub fn sealed_spans(&self, column: ColumnId) -> usize {
        self.slot(column).map_or(0, |at| self.sealed[at].len())
    }

    /// Fill a buffer with one bounded page of a column's keys, ascending
    ///
    /// For a caller that wants one page and no more. A caller walking a column to its
    /// end holds a cursor instead, which keeps a paged playback from reopening its
    /// footers on every page.
    pub fn page(
        &self,
        column: ColumnId,
        start: Bound<&[u8]>,
        limit: usize,
        out: &mut KeyPage,
    ) -> Result<()> {
        self.publish
            .reading_all(|| self.one_page(column, Way::Up, start, limit, out))
    }

    /// Fill a buffer with one bounded page of a column's keys, descending
    pub fn page_back(
        &self,
        column: ColumnId,
        end: Bound<&[u8]>,
        limit: usize,
        out: &mut KeyPage,
    ) -> Result<()> {
        self.publish
            .reading_all(|| self.one_page(column, Way::Down, end, limit, out))
    }

    /// One page and no more, without setting up a playback that will not be resumed
    ///
    /// A column with nothing sealed goes straight to its map: a cursor the caller
    /// would drop on the next line costs two copies of the bound to build.
    fn one_page(
        &self,
        column: ColumnId,
        way: Way,
        from: Bound<&[u8]>,
        limit: usize,
        out: &mut KeyPage,
    ) -> Result<()> {
        let Some(slot) = self.slot(column) else {
            out.clear();
            return Ok(());
        };
        let generation = self.sealed[slot].generation();
        if self.paged_footers(slot).is_none() {
            playback::resident_page(&self.indexes[slot], way, from, limit, out);
            if self.sealed[slot].generation() == generation {
                return Ok(());
            }
        }
        let mut playback = PlaybackCursor::new(column, way, from)?;
        self.page_from_held(&mut playback, limit, out)
    }

    /// Fill a buffer with the next page a playback has reached, and carry it past it
    ///
    /// A column with nothing sealed is the map and nothing else, a lock and a range,
    /// so the merge is reached only when there is something to merge.
    pub fn page_from(
        &self,
        playback: &mut PlaybackCursor,
        limit: usize,
        out: &mut KeyPage,
    ) -> Result<()> {
        // The one whole-set read that carries state into its fill, so it has to put
        // that state back: a fill thrown away has already moved the playback on.
        let mark = playback.mark();
        let mut refilling = false;
        self.publish.reading_all(|| {
            if refilling {
                playback.rewind(&mark);
            }
            refilling = true;
            self.page_from_held(playback, limit, out)
        })
    }

    /// The same page fill for a caller already holding the barrier
    fn page_from_held(
        &self,
        playback: &mut PlaybackCursor,
        limit: usize,
        out: &mut KeyPage,
    ) -> Result<()> {
        let column = playback.column();
        let Some(slot) = self.slot(column) else {
            out.clear();
            return Ok(());
        };
        loop {
            let generation = self.sealed[slot].generation();
            if let Some(footers) = self.paged_footers(slot) {
                return merged_page(&self.paged_at(slot, column, footers), playback, limit, out);
            }
            let mark = playback.mark();
            playback.page_resident(&self.indexes[slot], limit, out)?;
            // A seal noted during the read may have handed over keys the page missed
            if self.sealed[slot].generation() == generation {
                return Ok(());
            }
            playback.rewind(&mark);
        }
    }

    /// The three places one column's keys can be, for the merge that reads all of them
    fn paged_at<'a>(
        &'a self,
        at: usize,
        column: ColumnId,
        footers: &'a Arc<dyn FooterSource>,
    ) -> Paged<'a> {
        Paged {
            column,
            index: &self.indexes[at],
            sealed: &self.sealed[at],
            footers: footers.as_ref(),
            runs: &self.walk_runs[at],
            key_runs: &self.key_runs,
            segments: &self.segments,
        }
    }

    /// The key runs from merges, and the segments they answer for in a walk
    pub fn key_runs(&self) -> &KeyRunSet {
        &self.key_runs
    }

    /// Whether a segment is a sealed run that has not retired
    pub fn holds_sealed(&self, segment: SegmentId) -> bool {
        self.sealed.iter().any(|sealed| sealed.holds(segment))
    }

    /// A walk from any one key merges at most this many runs, the uncovered segments plus every key run
    pub fn overlap_depth(&self) -> usize {
        let covered = self.key_runs.covered();
        let runs = self.key_runs.runs().len();
        self.sealed
            .iter()
            .map(|sealed| sealed.depth_past(&covered))
            .max()
            .unwrap_or(0)
            + runs
    }

    /// Drop footers walks opened under an older sealed set, so no slot holds a retired segment's footer
    pub fn sweep_walk_runs(&self) {
        for (sealed, runs) in self.sealed.iter().zip(&self.walk_runs) {
            runs.sweep(sealed.generation().wrapping_add(self.key_runs.generation()));
        }
    }

    /// Close a rebuild that filled the maps: size them, then stand the sealed spans
    ///
    /// The spans come off each sealed footer the rebuild swept.
    pub fn finish_rebuild(&self, sealed: Vec<SealedSpan>) {
        for index in &self.indexes {
            index.fit();
        }
        // Entries a footer answers for are stamped with their segment's incarnation
        self.segments
            .issue_incarnations(sealed.iter().map(|span| span.segment));
        // Spans are grouped per column and installed in one pass each, since a
        // rebuild brings one per sealed segment and reindexing per segment would
        // make the open quadratic in them.
        let mut by_column: HashMap<ColumnId, Vec<(SegmentId, KeyBytes, KeyBytes)>> = HashMap::new();
        for span in sealed {
            by_column.entry(span.column).or_default().push((
                span.segment,
                span.lowest,
                span.highest,
            ));
        }
        for (at, column) in self.columns.iter().enumerate() {
            let spans = by_column.remove(&column.id).unwrap_or_default();
            self.sealed[at].replace(spans);
        }
        // A volume with nothing sealed has no keys the spot index could be missing
        if self.sealed.iter().all(SealedRanges::is_empty) {
            self.mark_spot_ready();
        }
    }

    /// Drop every key and every segment counter the index holds
    pub fn clear(&self) {
        for index in &self.indexes {
            index.clear();
        }
        for spot in &self.spot {
            spot.clear();
        }
        self.spot_ready.store(false, Ordering::Release);
        self.segments.clear();
    }

    /// Forget a segment's counters once its file has been unlinked
    pub fn forget_segment(&self, segment: SegmentId) {
        self.rebook_retiring(segment);
        self.segments.forget(segment);
        self.retired.fetch_add(1, Ordering::AcqRel);
        // A retired segment's footer goes with its file, so an index that kept
        // searching it would read a file that is no longer there.
        for sealed in &self.sealed {
            sealed.forget(segment);
        }
    }

    /// Book the true length of each class-booked overwrite in a retiring segment, before its locks are taken
    fn rebook_retiring(&self, segment: SegmentId) {
        if self.spot_displaced() == 0 {
            return;
        }
        let Some(footers) = self.footers.get() else {
            return;
        };
        // The pass already holds the footer, and a footer gone on a follower leaves these bookings in the slack
        let Ok(Some(footer)) = footers.footer(segment) else {
            return;
        };
        for (at, spot) in self.spot.iter().enumerate() {
            let Some(partition) = footer.partition(self.columns[at].id) else {
                continue;
            };
            let Ok(rebooked) = spot.settle_displaced_in(segment, partition) else {
                continue;
            };
            for row in rebooked {
                self.indexes[at].rebook_paged(row.key.as_slice(), row.booked, row.actual);
            }
        }
    }

    /// Reclaimable-byte counters for one segment
    pub fn segment_bytes(&self, segment: SegmentId) -> SegmentBytes {
        self.segments.bytes_of(segment)
    }

    /// Per-segment live and dead footprints, for choosing a compaction target
    pub fn segments_snapshot(&self) -> Vec<(SegmentId, SegmentBytes)> {
        self.segments.snapshot()
    }

    /// Raise a segment's dead count to what a completed scrub of it counted
    pub fn settle_dead(&self, segment: SegmentId, counted: u64) {
        self.segments.settle_dead(segment, counted);
    }

    /// The oldest record one segment can still surface
    pub fn min_lsn_of(&self, segment: SegmentId) -> Option<Lsn> {
        self.segments.min_lsn_of(segment)
    }

    /// Oldest sequence number any segment other than this one can still surface
    pub fn min_lsn_excluding(&self, segment: SegmentId) -> Option<Lsn> {
        self.segments.min_lsn_excluding(segment)
    }

    /// Footprints and floors together, for ranking every segment in one pass
    pub fn ranking(&self) -> (Vec<(SegmentId, SegmentBytes)>, Floors) {
        self.segments.ranking()
    }

    /// Reclaimable bytes across every segment, the dead-space gauge
    pub fn dead_bytes(&self) -> u64 {
        self.segments.dead_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::format::column::KeyWidth;
    use crate::index::entry::span_of;

    const RECORD: ColumnId = ColumnId(1);
    const BLOB: ColumnId = ColumnId(2);

    const COLUMNS: ColumnSet = &[
        ColumnSpec {
            id: RECORD,
            name: "record",
            key_width: KeyWidth::Fixed(34),
            shard_bytes: 2,
            purge_mark: None,
            codec: Codec::None,
        },
        ColumnSpec {
            id: BLOB,
            name: "blob_data",
            key_width: KeyWidth::Fixed(32),
            shard_bytes: 0,
            purge_mark: None,
            codec: Codec::None,
        },
    ];

    fn index() -> ReelIndex {
        ReelIndex::new(COLUMNS).expect("index")
    }

    fn record_key(group: u16, byte: u8) -> RecordKey {
        let mut bytes = group.to_be_bytes().to_vec();
        bytes.extend_from_slice(&[byte; 32]);
        RecordKey::from_bytes(RECORD, &bytes).expect("key")
    }

    fn blob_key(byte: u8) -> RecordKey {
        RecordKey::from_bytes(BLOB, &[byte; 32]).expect("key")
    }

    fn loc(segment: u32, offset: u32, len: u32) -> Loc {
        Loc::new(SegmentId(segment), offset, len)
    }

    // two columns holding the same key bytes resolve apart
    #[test]
    fn columns_resolve_apart() {
        let index = index();
        let record = record_key(1, 0x11);
        let blob = blob_key(0x11);

        index
            .insert(&record, loc(1, 0, 400), Lsn(1))
            .expect("insert");
        index
            .insert(&blob, loc(1, 500, 900), Lsn(2))
            .expect("insert");

        assert_eq!(
            index.get(&record).expect("read").expect("record").loc.len,
            400
        );
        assert_eq!(index.get(&blob).expect("read").expect("blob").loc.len, 900);
        assert_eq!(index.column_totals(RECORD).count, 1);
        assert_eq!(index.column_totals(BLOB).count, 1);
        assert_eq!(index.totals().count, 2);
        assert_eq!(index.totals().bytes, ByteCount::from_bytes(1300));
    }

    // a column the reel does not serve resolves nothing and takes nothing
    #[test]
    fn unknown_column_is_refused() {
        let index = index();
        let stray = RecordKey::from_bytes(ColumnId(9), &[0u8; 32]).expect("key");

        assert!(!index
            .insert(&stray, loc(1, 0, 400), Lsn(1))
            .expect("insert"));
        assert!(index.get(&stray).expect("read").is_none());
        assert!(index.column(ColumnId(9)).is_none());
        assert_eq!(index.totals().count, 0);
    }

    // two columns claiming one identifier is a configuration error
    #[test]
    fn duplicate_column_id_refused() {
        const CLASHING: ColumnSet = &[
            ColumnSpec {
                id: ColumnId(1),
                name: "one",
                key_width: KeyWidth::Fixed(32),
                shard_bytes: 0,
                purge_mark: None,
                codec: Codec::None,
            },
            ColumnSpec {
                id: ColumnId(1),
                name: "two",
                key_width: KeyWidth::Fixed(32),
                shard_bytes: 0,
                purge_mark: None,
                codec: Codec::None,
            },
        ];

        assert!(ReelIndex::new(CLASHING).is_err());
    }

    // both columns book their bytes into the segments they share
    #[test]
    fn columns_share_segments() {
        let index = index();
        index
            .insert(&record_key(1, 0x11), loc(1, 0, 400), Lsn(1))
            .expect("insert");
        index
            .insert(&blob_key(0x22), loc(1, 500, 900), Lsn(2))
            .expect("insert");

        let bytes = index.segment_bytes(SegmentId(1));

        assert_eq!(bytes.live, span_of(34, 400) + span_of(32, 900));
        assert_eq!(bytes.dead, 0);
        assert_eq!(index.segments_snapshot().len(), 1);
    }

    // a range delete reaches only the column it names
    #[test]
    fn range_delete_stays_in_its_column() {
        let index = index();
        index
            .insert(&record_key(42, 0x01), loc(1, 0, 100), Lsn(1))
            .expect("insert");
        index
            .insert(&blob_key(0x00), loc(1, 200, 100), Lsn(2))
            .expect("insert");

        let start = RecordKey::from_bytes(RECORD, &[0u8; 34]).expect("key");
        index
            .remove_range(&start, None, Lsn(9), loc(1, 0, 0))
            .expect("range delete");
        while index.sweep_covers(usize::MAX).expect("sweep") {}

        assert_eq!(index.column_totals(RECORD).count, 0);
        assert_eq!(index.column_totals(BLOB).count, 1);
    }

    // a playback pages one column's keys and never crosses into another
    #[test]
    fn pages_one_column() {
        let index = index();
        index
            .insert(&record_key(1, 0x01), loc(1, 0, 100), Lsn(1))
            .expect("insert");
        index
            .insert(&record_key(2, 0x02), loc(1, 0, 100), Lsn(2))
            .expect("insert");
        index
            .insert(&blob_key(0x03), loc(1, 0, 100), Lsn(3))
            .expect("insert");

        let mut out = KeyPage::default();
        index
            .page(RECORD, Bound::Unbounded, 8, &mut out)
            .expect("page");
        assert_eq!(out.len(), 2);

        index
            .page(BLOB, Bound::Unbounded, 8, &mut out)
            .expect("page");
        assert_eq!(out.len(), 1);
    }

    // forgetting a retired segment clears its counters and its sequence bound
    #[test]
    fn forget_clears_segment() {
        let index = index();
        index
            .insert(&record_key(1, 0x01), loc(1, 0, 400), Lsn(1))
            .expect("insert");
        index
            .remove(&record_key(1, 0x01), Lsn(2), loc(1, 0, 0))
            .expect("remove");

        index.forget_segment(SegmentId(1));

        assert_eq!(index.dead_bytes(), 0);
        assert_eq!(index.min_lsn_excluding(SegmentId(9)), None);
    }
}
