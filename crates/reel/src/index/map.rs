//! The reel's resident index: one map per column over one set of segments
//!
//! Resolving by column first is what lets each column keep its keys at its own
//! width and shard as far as its own write plane needs. The segments are shared,
//! since one holds records from every column and the compactor asks about it whole.

use std::collections::HashMap;
use std::ops::Bound;
use std::sync::{Arc, OnceLock};

use crate::units::ByteCount;

use crate::append::publish::{stripe_of_parts, stripes_of, PublishBarrier, ALL_STRIPES};
use crate::config::{IndexResidency, ShardShapes};
use crate::engine::Totals;
use crate::error::{ReelError, Result};
use crate::format::column::{
    Codec, ColumnId, ColumnSet, ColumnSpec, KeyBytes, KeyRef, MapShape, RecordKey, INLINE_MAX,
    ROW_CARRY_MAX,
};
use crate::format::footer::SegmentFooter;
use crate::format::loc::{Loc, SegmentId};
use crate::format::lsn::Lsn;
use crate::index::column::{ColumnIndex, KeyMove, Landed, PendingCover};
use crate::index::counters::{Floors, SegmentBytes, SegmentStamp, SegmentTable};
use crate::index::entry::{Entry, RangeCover};
use crate::index::page::KeyPage;
use crate::index::paged::{Candidates, FooterSource, SealedRanges};
use crate::index::playback::{self, merged_page, Paged, PlaybackCursor, Way};
use crate::index::recovery::SealedSpan;
use crate::index::sealed_keys::SealedKeys;

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

    /// Where the copy landed
    pub to: Loc,

    /// The sequence number the move is guarded by, which is the source's own
    pub lsn: Lsn,
}

/// Bytes one index entry occupies, which is what the map holds beside every key
const ENTRY_BYTES: u64 = std::mem::size_of::<Entry>() as u64;

/// Paged keys a range delete settles at a time
///
/// Holding every key a range reaches would be the resident footprint a paged column
/// exists to not have.
const RELEASE_RUN: usize = 1024;

/// Carried entries one shed pass may visit, the sweep's effort cap
const SHED_VISITS: usize = 4096;

/// The ceiling of a sealed segment nothing recorded one for, which rules nothing out
///
/// Above every sequence number a reel can issue, so a segment wearing it sorts to the
/// front of a fan-out and no hit can stop the walk short of it.
const NO_CEILING: Lsn = Lsn(u64::MAX);

/// Rotates the shard a shed pass starts at, so passes spread over a column
static SHED_CURSOR: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

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
/// A resident index holds every live key. A paged one holds the keys of segments no
/// footer covers yet, and for the rest only the range of keys each sealed segment
/// covers, so a lookup that misses the map knows which footers to search.
pub struct ReelIndex {
    /// The columns this index was built over
    columns: ColumnSet,

    /// One map per column, in the order the columns were declared
    indexes: Vec<ColumnIndex>,

    /// What each column's sealed segments cover, empty unless the column pages
    sealed: Vec<SealedRanges>,

    /// Every sealed key each column holds, as one filter ahead of the fan-out
    sealed_keys: Vec<SealedKeys>,

    /// Column identifier to its position, so routing a record is one load
    by_id: Vec<Option<usize>>,

    /// Per-segment counters every column's keys point into
    segments: Arc<SegmentTable>,

    /// Where the footers of sealed segments are read from, for a paged column
    footers: OnceLock<Arc<dyn FooterSource>>,

    /// Where this volume's operator asked the sealed index to live
    residency: IndexResidency,

    /// Compaction copies refused because nothing else pointed at them
    unclaimed: std::sync::atomic::AtomicU64,

    /// Held while a batch moves the maps, so no spanning read sees part of one
    publish: PublishBarrier,
}

impl ReelIndex {
    /// An empty index over the columns a reel serves
    ///
    /// The shapes say whether a column's own map declaration is honoured, per volume
    /// rather than per column because it is the way back off a shape: a volume that
    /// stops honouring them rebuilds every column into the tree.
    pub fn new(
        columns: ColumnSet,
        residency: IndexResidency,
        shapes: ShardShapes,
    ) -> Result<ReelIndex> {
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
            if spec.inline_max as usize > INLINE_MAX {
                // A ceiling past the entry's array means the value rides in the
                // carried map, which lives beside the resident entries and nowhere
                // else, and a codec would make it disagree with the stored bytes.
                if residency != IndexResidency::Resident {
                    return Err(ReelError::Config(format!(
                        "column {} carries values resident, which a paged index cannot hold",
                        spec.name,
                    )));
                }
                if spec.codec != Codec::None {
                    return Err(ReelError::Config(format!(
                        "column {} cannot both carry values resident and compress them",
                        spec.name,
                    )));
                }
            }
            if spec.map_shape == MapShape::Open
                && shapes == ShardShapes::Declared
                && residency != IndexResidency::Resident
            {
                return Err(ReelError::Config(format!(
                    "column {} asks for an open shard, which a paged walk cannot merge",
                    spec.name,
                )));
            }
            if spec.row_carry as usize > ROW_CARRY_MAX {
                // A row's carry is paid in the stride every block search walks and in
                // the footer bytes of every sealed segment, so the ceiling is refused
                // at open rather than discovered as a wide block later.
                return Err(ReelError::Config(format!(
                    "column {} asks its rows to carry {} bytes, past the {} a row holds",
                    spec.name, spec.row_carry, ROW_CARRY_MAX,
                )));
            }
            if spec.row_carry > 0 && spec.codec != Codec::None {
                // What lands in a row is the payload the reader wanted, and a codec
                // stores something else.
                return Err(ReelError::Config(format!(
                    "column {} cannot both carry values in its rows and compress them",
                    spec.name,
                )));
            }
            by_id[spec.id.as_index()] = Some(at);
            indexes.push(ColumnIndex::new(spec, shapes)?);
            sealed.push(SealedRanges::new());
        }
        let sealed_keys = (0..columns.len()).map(|_| SealedKeys::new()).collect();
        Ok(ReelIndex {
            columns,
            indexes,
            sealed,
            sealed_keys,
            by_id,
            segments: Arc::new(SegmentTable::new()),
            footers: OnceLock::new(),
            residency,
            unclaimed: std::sync::atomic::AtomicU64::new(0),
            publish: PublishBarrier::new(),
        })
    }

    /// Where this volume keeps the index of its sealed segments
    pub fn residency(&self) -> IndexResidency {
        self.residency
    }

    /// Tell the index where to read the footers a paged column resolves through
    ///
    /// Set once at open, after the volume the footers live on exists.
    pub fn set_footers(&self, footers: Arc<dyn FooterSource>) {
        let _ = self.footers.set(footers);
    }

    /// Where a key's live record is, reading a footer if the column pages
    ///
    /// One answer for both residencies, so no caller has to know which it is on. A
    /// resident column stops at the map; a paged one carries on into the sealed
    /// segments whose key range covers the key, taking the highest sequence number it
    /// finds, since a segment number is not a version. A grave, a tombstone row and a
    /// range delete are all applied here, because a footer cannot know about any of
    /// them.
    pub fn get(&self, key: &RecordKey) -> Result<Option<Entry>> {
        self.get_carried(key, None)
    }

    /// The same answer, with the value a sealed row carries copied into a buffer
    ///
    /// Filled only where a key resolved through a footer whose row carries its value,
    /// which is what turns a paged read from two ios into one.
    pub fn get_carried(
        &self,
        key: &RecordKey,
        carry: Option<&mut Vec<u8>>,
    ) -> Result<Option<Entry>> {
        let Some(at) = self.slot(key.column) else {
            return Ok(None);
        };
        match self.indexes[at].entry_or_grave(key.as_slice()) {
            Some(entry) if entry.is_grave() => return Ok(None),
            // An entry a cover spans is a key the drop took, and the map held the
            // newest version, so there is no footer left to ask.
            Some(entry) if self.indexes[at].is_covered_key(key.as_slice(), entry.lsn) => {
                return Ok(None)
            }
            Some(entry) => return Ok(Some(entry)),
            None => {}
        }
        if !self.residency.pages() {
            return Ok(None);
        }
        self.sealed_entry(at, key, carry)
    }

    /// The newest thing every sealed footer says about a key
    ///
    /// The fan-out below finds it; what is left here is the two things a footer
    /// cannot know about itself, a tombstone row and a range delete.
    fn sealed_entry(
        &self,
        at: usize,
        key: &RecordKey,
        carry: Option<&mut Vec<u8>>,
    ) -> Result<Option<Entry>> {
        match self.newest_sealed(at, key, None, carry)? {
            Some(entry) if entry.is_grave() => Ok(None),
            Some(entry) if self.indexes[at].is_covered_key(key.as_slice(), entry.lsn) => Ok(None),
            found => Ok(found),
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
        carry: Option<&mut Vec<u8>>,
    ) -> Result<Option<Entry>> {
        let Some(footers) = self.footers.get() else {
            return Ok(None);
        };
        // Asked once ahead of the fan-out: a key no sealed segment holds skips
        // the candidate walk and every per-segment filter behind it.
        if !self.sealed_keys[at].may_hold(key.as_slice()) {
            return Ok(None);
        }
        let candidates = self.sealed[at].candidates(key.as_slice());
        // Ordered only where there is something to stop short of, so a column
        // written in key order pays nothing for a walk of one candidate.
        let ordered = match candidates.len() > 1 {
            true => self.ordered_candidates(&candidates),
            false => Vec::new(),
        };

        let mut newest: Option<(Entry, SegmentId)> = None;
        // Every candidate fills the scratch and only the winner keeps what it filled,
        // so the accepted row's bytes are taken out of the scratch where it is
        // accepted rather than read back after the loop.
        let mut scratch = Vec::new();
        let mut held: Option<Vec<u8>> = None;
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

            let asking = carry.is_some().then_some(&mut scratch);
            let Some(found) = footers.find(segment, key.column, key.as_slice(), asking)? else {
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
            held = match carry.is_some() && found.carries() {
                true => Some(std::mem::take(&mut scratch)),
                false => None,
            };
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

        if let (Some(into), Some(bytes)) = (carry, held) {
            into.clear();
            into.extend_from_slice(&bytes);
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
                    let Some(row) = partition.find_row(key.as_slice(), None).transpose()? else {
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
        Ok(match self.newest_sealed(at, key, None, None)? {
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
    /// below the snapshot. The seal taken when the snapshot was made is what makes
    /// this complete: everything at or below that number is in a segment with a
    /// footer, so a version this search cannot see does not exist.
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
        match self.newest_sealed(at, key, Some(snapshot), None)? {
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
        self.residency.pages() && self.indexes[at].entry_or_grave(key.as_slice()).is_none()
    }

    /// The sealed segments of one column, for the paths that have to wait on them
    fn sealed_of(&self, at: usize) -> Option<&SealedRanges> {
        self.residency.pages().then(|| &self.sealed[at])
    }

    /// Where one column's index sits, for the callers that need its sealed ranges
    fn slot(&self, column: ColumnId) -> Option<usize> {
        self.by_id[column.as_index()]
    }

    /// Where to read footers for a column that has sealed something, if it pages
    fn paged_footers(&self, at: usize) -> Option<&Arc<dyn FooterSource>> {
        self.footers
            .get()
            .filter(|_| self.residency.pages() && !self.sealed[at].is_empty())
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

    /// Give a key up to the footer of the segment it landed in
    pub fn page_out(&self, column: ColumnId, key: &[u8], loc: Loc) -> bool {
        match self.column(column) {
            Some(index) => index.page_out(key, loc),
            None => false,
        }
    }

    /// Take a listed row's key into the count, when its source never had it counted
    ///
    /// Nothing points at a listed row, so no repoint runs for it. A source sealed at
    /// runtime already has this key in its paged count and keeps it; a born source
    /// never counted it, so this is where it joins.
    pub fn note_listed(&self, key: &RecordKey, from: SegmentId, len: u32) -> bool {
        let Some(at) = self.slot(key.column) else {
            return false;
        };
        if self.counted(from) {
            return false;
        }
        self.indexes[at].count_listed(key.as_slice(), u64::from(len))
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
    /// and rules out the rest. Here rather than in the engine because compaction needs
    /// it too: a pass that lists rows notes its destination before retiring the source.
    pub fn note_spans(&self, segment: SegmentId, footer: &SegmentFooter) -> Result<()> {
        // The ceiling goes down before any span does: a search the span admits must
        // never meet a segment the fan-out believes can hold nothing.
        self.segments.note_max(segment, footer.max_lsn);
        for partition in &footer.partitions {
            let Some((lowest, highest)) = partition.key_range() else {
                continue;
            };
            // Keys go in before the span is visible, so a search the span admits can
            // never be ruled out by a filter that has not heard of this segment.
            if let Some(at) = self.slot(partition.column) {
                self.sealed_keys[at].insert_partition(partition);
            }
            let (lowest, highest) = (KeyBytes::new(lowest)?, KeyBytes::new(highest)?);
            self.note_sealed(partition.column, segment, lowest, highest);
        }
        Ok(())
    }

    /// Sealed searches the key filters answered without asking any segment
    pub fn sealed_skips(&self) -> u64 {
        self.sealed_keys.iter().map(|keys| keys.skips()).sum()
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
    pub fn insert(
        &self,
        key: &RecordKey,
        loc: Loc,
        lsn: Lsn,
        carried: Option<Arc<[u8]>>,
    ) -> Result<bool> {
        let landed = self.insert_mapped(key, loc, lsn, carried);
        if landed.may_be_paged() {
            self.settle_displaced(key)?;
        }
        Ok(landed != Landed::Newer)
    }

    /// Move the map for a committed record, leaving the paged settle to the caller
    ///
    /// This half is memory work under a shard lock, which is what a batch does while
    /// it holds the publish barrier. The other reads a footer, and a reader waiting on
    /// the barrier should not be waiting on the volume.
    pub fn insert_mapped(
        &self,
        key: &RecordKey,
        loc: Loc,
        lsn: Lsn,
        carried: Option<Arc<[u8]>>,
    ) -> Landed {
        let Some(at) = self.slot(key.column) else {
            return Landed::Newer;
        };
        self.indexes[at].insert(
            key.as_slice(),
            Entry::new(loc, lsn),
            &self.segments,
            carried,
        )
    }

    /// Book the footer-held record a mapped mutation displaced, if there was one
    ///
    /// Asked only of a mutation that landed on an empty place, the only kind a footer
    /// can still be answering for. Deferring it past the map move costs nothing a
    /// reader can see: what it settles is the dead-byte accounting the compactor
    /// reads, which is a tick behind by design anyway.
    pub fn settle_displaced(&self, key: &RecordKey) -> Result<bool> {
        let Some(at) = self.slot(key.column) else {
            return Ok(false);
        };
        let Some(displaced) = self.paged_entry(at, key)? else {
            return Ok(false);
        };
        Ok(self.indexes[at].settle_paged(
            key.as_slice(),
            displaced.loc,
            self.counted(displaced.loc.segment),
            &self.segments,
        ))
    }

    /// Whether a footer-held record at this segment was ever in a shard counter
    ///
    /// A key handed over at runtime was counted on its way out; one a rebuild left
    /// sealed never was, and the segment it sits in is what tells the two apart.
    fn counted(&self, segment: SegmentId) -> bool {
        !self.segments.is_born(segment)
    }

    /// What a footer holds for a key the map does not, if the column pages at all
    fn paged_entry(&self, at: usize, key: &RecordKey) -> Result<Option<Entry>> {
        match self.residency.pages() {
            true => self.sealed_entry(at, key, None),
            false => Ok(None),
        }
    }

    /// Bytes this column asks the index to carry of a value itself
    pub fn inline_max(&self, column: ColumnId) -> u16 {
        self.spec(column).map_or(0, |spec| spec.inline_max)
    }

    /// Codec this column asks admission to attempt on its payloads
    pub fn codec_of(&self, column: ColumnId) -> Codec {
        self.spec(column).map_or(Codec::None, |spec| spec.codec)
    }

    /// Ceiling up to which this column carries values beside its entries
    pub fn carry_max(&self, column: ColumnId) -> u16 {
        self.slot(column)
            .map_or(0, |at| self.indexes[at].carry_max())
    }

    /// The payload a write asks the index to carry, when the column carries at all
    ///
    /// Values that fit the entry's own array are served from there and values past
    /// the ceiling read from the volume, so only the span between the two is taken.
    pub fn carry_capture(&self, column: ColumnId, payload: &[u8]) -> Option<Arc<[u8]>> {
        let ceiling = self.carry_max(column) as usize;
        (payload.len() > INLINE_MAX && payload.len() <= ceiling).then(|| payload.into())
    }

    /// The carried value for a key, exactly as new as the entry the caller holds
    pub fn carried_value(&self, key: &RecordKey, lsn: Lsn) -> Option<Arc<[u8]>> {
        self.slot(key.column)
            .and_then(|at| self.indexes[at].carried_value(key.as_slice(), lsn))
    }

    /// Remember a value a read just paid the device for
    pub fn warm_carried(&self, key: KeyRef<'_>, lsn: Lsn, bytes: &[u8], two_touch: bool) {
        if let Some(at) = self.slot(key.column) {
            self.indexes[at].warm_carried(key.as_slice(), lsn, bytes, two_touch);
        }
    }

    /// Bytes of carried values resident across every column
    pub fn carried_total(&self) -> u64 {
        self.indexes.iter().map(ColumnIndex::carried_bytes).sum()
    }

    /// Shed carried values down to a byte budget, coldest first
    ///
    /// One bounded pass over the columns that carry, rotating its starting shard so
    /// repeated passes spread over a column rather than draining the front of it.
    pub fn shed_carried(&self, budget: u64) -> u64 {
        let total = self.carried_total();
        let Some(mut want) = total.checked_sub(budget).filter(|over| *over > 0) else {
            return 0;
        };
        let start = SHED_CURSOR.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut freed = 0u64;
        for index in &self.indexes {
            if want == 0 {
                break;
            }
            let shed = index.shed_carried(want, start, SHED_VISITS);
            freed += shed;
            want = want.saturating_sub(shed);
        }
        freed
    }

    /// Drop a key on a tombstone, guarded by its sequence number
    ///
    /// The record is resolved before the grave goes in, since on a paged column the
    /// grave is what stops the search that would find it. What comes back says
    /// whether a live record went, wherever it was being answered from.
    pub fn remove(&self, key: &RecordKey, lsn: Lsn, tombstone: Loc) -> Result<bool> {
        let landed = self.remove_mapped(key, lsn, tombstone);
        match landed.may_be_paged() && self.settle_displaced(key)? {
            true => Ok(true),
            false => Ok(landed.dropped_record()),
        }
    }

    /// Publish a batch's moves under the barrier, so a spanning read sees all or none
    ///
    /// Only the stripes the batch's own keys fall in are taken, so a batch writing one
    /// group does not order itself against every other batch. A range is the exception
    /// and takes every stripe: its cover answers for a whole column rather than for the
    /// shard one key falls in. The hold covers the map moves alone; whatever settling
    /// they call for runs with the barrier given up.
    ///
    /// The key moves go in the order the batch built them, with each range standing its
    /// cover at the point of the run it was given at. What comes back is one answer per
    /// key move; a cover displaces nothing and has no answer to give.
    pub fn publish_batch(&self, moves: &[KeyMove<'_>], ranges: &[RangeMove<'_>]) -> Vec<Landed> {
        let stripes = match ranges.is_empty() {
            true => moves.iter().fold(0u64, |mask, planned| {
                mask | 1u64 << stripe_of_parts(planned.column, planned.key)
            }),
            false => ALL_STRIPES,
        };
        let _publishing = self.publish.publishing(stripes);

        let mut landed = Vec::with_capacity(moves.len());
        let mut at = 0;
        for range in ranges {
            let upto = range.after.min(moves.len());
            if upto > at {
                self.apply_moves(&moves[at..upto], &mut landed);
                at = upto;
            }
            self.cover_range(range.start, range.end, range.lsn, range.tombstone);
        }
        if at < moves.len() {
            self.apply_moves(&moves[at..], &mut landed);
        }
        landed
    }

    /// Resolve every key against one state of the maps
    ///
    /// Under the barrier so a batch publishing beside this cannot answer some of
    /// the keys from before it and the rest from after. One key takes nothing,
    /// since one key cannot be half a batch.
    pub fn get_many(&self, keys: &[RecordKey]) -> Result<Vec<Option<Entry>>> {
        // One key cannot be half a batch, and grouping it would put the barrier, the
        // sort and the shard run in front of a single descent for nothing.
        if keys.len() < 2 {
            return match keys.first() {
                Some(key) => Ok(vec![self.get(key)?]),
                None => Ok(Vec::new()),
            };
        }
        let _reading = self.publish.reading(stripes_of(keys.iter()));
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
                    // A key the map has nothing for may still be in a sealed footer,
                    // which is a device read rather than a lookup.
                    None => self.paged_entry(slot, key)?,
                };
            }
            at = end;
        }
        Ok(found)
    }

    /// Run a follower's apply pass under the exclusive barrier
    ///
    /// A pass moves the index the same key at a time a batch does while a follower
    /// serves reads throughout, and it cannot name its keys up front, so it takes
    /// every stripe. Every device read the pass needs is done before it enters.
    pub fn publish_pass<Applied>(&self, apply: impl FnOnce() -> Applied) -> Applied {
        let _publishing = self.publish.publishing(ALL_STRIPES);
        apply()
    }

    /// Apply a batch's moves in arrival order, sharing locks where keys allow
    ///
    /// Runs sharing a column are found here and runs sharing a shard below it.
    /// Nothing is reordered, so a batch publishes exactly what it published. The
    /// answers are appended, since a batch carrying a range applies in several runs.
    fn apply_moves(&self, moves: &[KeyMove<'_>], landed: &mut Vec<Landed>) {
        let mut at = 0;
        while at < moves.len() {
            let column = moves[at].column;
            let mut end = at + 1;
            while end < moves.len() && moves[end].column == column {
                end += 1;
            }
            match self.slot(column) {
                Some(slot) => {
                    self.indexes[slot].apply_moves(&moves[at..end], &self.segments, landed)
                }
                // A column nothing indexes takes the same answer one key at a
                // time would have given, once for each key it would have gone to.
                None => landed.resize(landed.len() + (end - at), Landed::Newer),
            }
            at = end;
        }
    }

    /// Drop a key from the map alone, leaving the paged settle to the caller
    ///
    /// The other half of the split `insert_mapped` describes, for the same reason.
    pub fn remove_mapped(&self, key: &RecordKey, lsn: Lsn, tombstone: Loc) -> Landed {
        let Some(at) = self.slot(key.column) else {
            return Landed::Newer;
        };
        self.indexes[at].remove(key.as_slice(), lsn, tombstone, &self.segments)
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

    /// Settle one bounded run of the footer-held records one cover spans
    ///
    /// The rows come newest-below-the-cover per key, so a row rewritten after the
    /// delete stays. Rows a finished cover spans were settled by that cover's own
    /// pass and are skipped, which is what lets two overlapping drops share a range
    /// without booking it dead twice.
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
        for (key, loc) in &found {
            index.release_covered(
                key.as_slice(),
                *loc,
                pending.lsn,
                self.counted(loc.segment),
                &self.segments,
            );
        }
        index.advance_release(pending.lsn, run.resume.as_deref());
        Ok(run.examined)
    }

    /// Repoint a key from a compacted record to its rewritten copy under a guard
    ///
    /// A paged key has no entry to repoint, so it comes back into the map until the
    /// destination seals and hands it over again. Where it came from is read back out
    /// of the footer rather than taken from the caller, since that is also what says
    /// the row is still the version being moved.
    pub fn repoint(&self, key: &RecordKey, to: Loc, expected_lsn: Lsn) -> Result<bool> {
        let Some(at) = self.slot(key.column) else {
            return Ok(false);
        };
        let index = &self.indexes[at];
        if !self.is_paged_key(at, key) {
            return Ok(index.repoint(key.as_slice(), to, expected_lsn, &self.segments));
        }
        match self.sealed_state(at, key)? {
            Sealed::Live(entry) if entry.lsn == expected_lsn => Ok(index.repoint_paged(
                key.as_slice(),
                entry.loc,
                to,
                expected_lsn,
                self.counted(entry.loc.segment),
                &self.segments,
            )),
            // A newer version won the race, so the copy is dead on arrival and
            // the compactor books it as such.
            Sealed::Live(_) | Sealed::Gone => Ok(false),
            // Nothing anywhere answers for this key, which does not mean nothing
            // does: a segment that sealed since the pass began is invisible here,
            // and adopting the copy would write the source's sequence number into
            // the map, which a read trusts ahead of any footer row. Refusing is
            // safe, since the source holds the record until the pass retires it.
            Sealed::Absent => {
                self.unclaimed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(false)
            }
        }
    }

    /// Repoint a run of moved records at their copies, under one hold of the barrier
    ///
    /// Per key it is the guarded move `repoint` makes, taken together so the hold is
    /// paid once for the run. The run length bounds a hold rather than a rate: a
    /// reader waiting on the barrier pays the whole of it, so a pass moving millions
    /// of entries cuts them into runs. A paged key resolves its source row inside the
    /// hold, from a footer the caller has usually just read.
    pub fn repoint_batch(&self, moves: &[KeyRepoint]) -> Result<u64> {
        self.publish_pass(|| {
            let mut moved = 0u64;
            for repoint in moves {
                if self.repoint(&repoint.key, repoint.to, repoint.lsn)? {
                    moved += 1;
                }
            }
            Ok(moved)
        })
    }

    /// Copies compaction made that nothing else was pointing at
    ///
    /// Zero on a healthy volume. Anything else says a paged key went unresolvable
    /// while its record was being rewritten, which is the window this counts.
    pub fn unclaimed_copies(&self) -> u64 {
        self.unclaimed.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Drop a key while it still resolves one exact location, writing no tombstone
    ///
    /// The guard is the location rather than a sequence number, so a key that has
    /// moved on since the caller looked is left alone. A paged key needs a grave
    /// rather than a removal, since taking it out of a map it is not in would leave
    /// the footer answering for a record that will not read. That grave names no
    /// tombstone, so it stands until the segment behind it is retired.
    pub fn evict_at(&self, key: &RecordKey, at: Loc) -> Result<bool> {
        let Some(column_at) = self.slot(key.column) else {
            return Ok(false);
        };
        let index = &self.indexes[column_at];
        if !self.is_paged_key(column_at, key) {
            // A resident key on a paging column can be one compaction has just
            // repointed into an open tail, and until that tail seals the map is the
            // only thing answering for the record, so taking the key out would take
            // the record with it.
            if self.residency.pages() && !self.sealed[column_at].holds(at.segment) {
                return Ok(false);
            }
            return Ok(index.evict_at(key.as_slice(), at, &self.segments));
        }
        match self.sealed_entry(column_at, key, None)? {
            Some(entry) if entry.loc == at => Ok(index.evict_paged(
                key.as_slice(),
                at,
                entry.lsn,
                self.counted(entry.loc.segment),
                &self.segments,
            )),
            _ => Ok(false),
        }
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
        self.indexes
            .iter()
            .enumerate()
            .map(|(at, index)| index.prune_tombstones(before, self.sealed_of(at)))
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

    /// Memory the maps are holding, near enough for a budget to act on
    ///
    /// Every key costs its own bytes, the entry it resolves to, and its share of
    /// whatever holds them. That third term comes from the column's shape, since the
    /// two shapes differ by an order of magnitude and one number for both would
    /// misprice whichever column it was not taken from.
    pub fn resident_bytes(&self) -> ByteCount {
        let bytes: u64 = self
            .columns
            .iter()
            .zip(&self.indexes)
            .map(|(spec, index)| {
                let per_key = u64::from(spec.key_width.fixed().unwrap_or(0))
                    + ENTRY_BYTES
                    + index.overhead_per_key();
                index.resident_keys() * per_key
            })
            .sum();
        ByteCount::from_bytes(bytes)
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

    /// Live key count and payload byte total for one column
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
            // A column answering keys out of footers holds keys no shard counted, so
            // declining sends the caller to the walk, which sees both halves.
            if self.answers_from_footers(column) {
                return None;
            }
            self.indexes[at].prefix_totals(prefix)
        })
    }

    /// Whether this column resolves any of its keys through a sealed footer
    ///
    /// The counters cover what the shards hold, and a paged column's sealed keys are
    /// in none of them, so anything counting rather than walking has to decline.
    pub fn answers_from_footers(&self, column: ColumnId) -> bool {
        self.slot(column)
            .is_some_and(|at| self.paged_footers(at).is_some())
    }

    /// Sealed segments recorded as covering keys of this column
    ///
    /// Kept on every volume rather than only a paging one, since it says where a
    /// version the map no longer holds can still be found.
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
    /// A resident column goes straight to its map: a cursor the caller would drop on
    /// the next line costs two copies of the bound to build.
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
        if self.paged_footers(slot).is_none() {
            playback::resident_page(&self.indexes[slot], way, from, limit, out);
            return Ok(());
        }
        let mut playback = PlaybackCursor::new(column, way, from)?;
        self.page_from_held(&mut playback, limit, out)
    }

    /// Fill a buffer with the next page a playback has reached, and carry it past it
    ///
    /// A resident column is the map and nothing else, a lock and a range. A paged
    /// column whose sealed segments hold nothing for this playback takes the same
    /// path, so the merge is reached only when there is something to merge.
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
        match self.paged_footers(slot) {
            Some(footers) => {
                merged_page(&self.paged_at(slot, column, footers), playback, limit, out)
            }
            None => playback.page_resident(&self.indexes[slot], limit, out),
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
        }
    }

    /// Install a rebuilt index in one pass, replacing whatever it held
    ///
    /// The caller has already resolved newest-wins across every segment, so the
    /// entries are the live winners. The per-segment minimums are the oldest record
    /// each segment can still surface, which the compactor consults before trimming a
    /// tombstone; the maximums, read off each sealed footer, are where a fan-out
    /// stops. A segment brought back without one is one no fan-out will stop short of.
    #[allow(clippy::too_many_arguments)]
    pub fn install(
        &self,
        entries: HashMap<ColumnId, Vec<(KeyBytes, Entry)>>,
        covers: Vec<RangeCover>,
        segments: HashMap<SegmentId, SegmentBytes>,
        seg_min_lsn: HashMap<SegmentId, Lsn>,
        seg_max_lsn: HashMap<SegmentId, Lsn>,
        sealed: Vec<SealedSpan>,
        sealed_keys: HashMap<ColumnId, SealedKeys>,
    ) {
        for index in &self.indexes {
            index.clear();
        }
        let born: Vec<SegmentId> = sealed.iter().map(|span| span.segment).collect();
        // The table goes first, since every entry installed below takes its
        // incarnation stamp from what this resolves.
        self.segments.install(segments, seg_min_lsn, seg_max_lsn);
        // The keys of these segments are in no shard counter, and the settle and
        // repoint paths ask the table before they count. Marked after the install,
        // which starts the set over.
        self.segments.mark_born(born);
        for (column, rows) in entries {
            if let Some(index) = self.column(column) {
                index.install(rows, &self.segments);
            }
        }
        // After the keys, since installing a column's keys clears what it holds.
        for cover in covers {
            if let Some(index) = self.column(cover.start.column) {
                index.install_cover(cover.start.key.as_slice(), cover.end.as_ref(), cover.lsn);
            }
        }
        // The key filters go in ahead of the spans they stand in front of, as they
        // do at a seal.
        for (column, keys) in sealed_keys {
            if let Some(at) = self.slot(column) {
                self.sealed_keys[at].adopt(&keys);
            }
        }
        // Spans are grouped per column and installed in one pass each, since a
        // paging volume brings one per sealed segment and reindexing per segment
        // would make the open quadratic in them.
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
    }

    /// Drop every key and every segment counter the index holds
    pub fn clear(&self) {
        for index in &self.indexes {
            index.clear();
        }
        self.segments.clear();
    }

    /// Forget a segment's counters once its file has been unlinked
    pub fn forget_segment(&self, segment: SegmentId) {
        self.segments.forget(segment);
        // A retired segment's footer goes with its file, so a paged index that
        // kept searching it would read a file that is no longer there.
        for sealed in &self.sealed {
            sealed.forget(segment);
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

    /// Every segment's footprints and floor together, for stamping a whole file
    pub fn segment_stamps(&self) -> HashMap<SegmentId, SegmentStamp> {
        self.segments.stamps()
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
            inline_max: 0,
            row_carry: 0,
            purge_mark: None,
            codec: Codec::None,
            map_shape: MapShape::Tree,
        },
        ColumnSpec {
            id: BLOB,
            name: "blob_data",
            key_width: KeyWidth::Fixed(32),
            shard_bytes: 0,
            inline_max: 0,
            row_carry: 0,
            purge_mark: None,
            codec: Codec::None,
            map_shape: MapShape::Tree,
        },
    ];

    /// The same declaration asking for an open-addressed shard
    ///
    /// Derived field by field, so a change above cannot leave this one holding a
    /// different key width.
    const fn opened(spec: &ColumnSpec) -> ColumnSpec {
        ColumnSpec {
            id: spec.id,
            name: spec.name,
            key_width: spec.key_width,
            shard_bytes: spec.shard_bytes,
            inline_max: spec.inline_max,
            row_carry: spec.row_carry,
            purge_mark: spec.purge_mark,
            codec: spec.codec,
            map_shape: MapShape::Open,
        }
    }

    const OPEN_COLUMNS: ColumnSet = &[opened(&COLUMNS[0]), opened(&COLUMNS[1])];

    fn index() -> ReelIndex {
        ReelIndex::new(COLUMNS, IndexResidency::Resident, ShardShapes::Tree).expect("index")
    }

    fn open_index() -> ReelIndex {
        let index = ReelIndex::new(
            OPEN_COLUMNS,
            IndexResidency::Resident,
            ShardShapes::Declared,
        )
        .expect("index");
        assert_eq!(
            index.column(RECORD).expect("column").map_shape(),
            MapShape::Open
        );
        index
    }

    // an open shard is refused on a paged index, which has no order to merge it
    #[test]
    fn open_refuses_paged() {
        let refused = ReelIndex::new(OPEN_COLUMNS, IndexResidency::Paged, ShardShapes::Declared);
        assert!(refused.is_err());

        // A volume that does not honour declarations never gets the open shard,
        // so there is nothing to refuse.
        assert!(ReelIndex::new(OPEN_COLUMNS, IndexResidency::Paged, ShardShapes::Tree).is_ok());
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
            .insert(&record, loc(1, 0, 400), Lsn(1), None)
            .expect("insert");
        index
            .insert(&blob, loc(1, 500, 900), Lsn(2), None)
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
            .insert(&stray, loc(1, 0, 400), Lsn(1), None)
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
                inline_max: 0,
                row_carry: 0,
                purge_mark: None,
                codec: Codec::None,
                map_shape: MapShape::Tree,
            },
            ColumnSpec {
                id: ColumnId(1),
                name: "two",
                key_width: KeyWidth::Fixed(32),
                shard_bytes: 0,
                inline_max: 0,
                row_carry: 0,
                purge_mark: None,
                codec: Codec::None,
                map_shape: MapShape::Tree,
            },
        ];

        assert!(ReelIndex::new(CLASHING, IndexResidency::Resident, ShardShapes::Tree).is_err());
    }

    // both columns book their bytes into the segments they share
    #[test]
    fn columns_share_segments() {
        let index = index();
        index
            .insert(&record_key(1, 0x11), loc(1, 0, 400), Lsn(1), None)
            .expect("insert");
        index
            .insert(&blob_key(0x22), loc(1, 500, 900), Lsn(2), None)
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
            .insert(&record_key(42, 0x01), loc(1, 0, 100), Lsn(1), None)
            .expect("insert");
        index
            .insert(&blob_key(0x00), loc(1, 200, 100), Lsn(2), None)
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
            .insert(&record_key(1, 0x01), loc(1, 0, 100), Lsn(1), None)
            .expect("insert");
        index
            .insert(&record_key(2, 0x02), loc(1, 0, 100), Lsn(2), None)
            .expect("insert");
        index
            .insert(&blob_key(0x03), loc(1, 0, 100), Lsn(3), None)
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

    // installing a rebuilt index replaces the keys and the segment counters
    #[test]
    fn install_replaces() {
        let index = index();
        index
            .insert(&record_key(5, 0x05), loc(9, 0, 50), Lsn(1), None)
            .expect("insert");

        let mut entries = HashMap::new();
        entries.insert(
            RECORD,
            vec![(
                KeyBytes::new(record_key(1, 0x11).as_slice()).expect("key"),
                Entry::new(loc(1, 0, 400), Lsn(4)),
            )],
        );
        let mut segments = HashMap::new();
        segments.insert(
            SegmentId(1),
            SegmentBytes {
                live: span_of(34, 400),
                dead: 77,
                ..SegmentBytes::default()
            },
        );
        let mut min_lsn = HashMap::new();
        min_lsn.insert(SegmentId(1), Lsn(4));
        index.install(
            entries,
            Vec::new(),
            segments,
            min_lsn,
            HashMap::new(),
            Vec::new(),
            HashMap::new(),
        );

        assert_eq!(index.totals().count, 1);
        assert!(!index.contains(&record_key(5, 0x05)).expect("read"));
        assert_eq!(index.dead_bytes(), 77);
        assert_eq!(index.min_lsn_excluding(SegmentId(2)), Some(Lsn(4)));
    }

    // an open-addressed column serves what a tree one serves, walk included
    #[test]
    fn open_serves_the_same() {
        let tree = index();
        let open = open_index();

        for byte in 0..40u8 {
            for index in [&tree, &open] {
                index
                    .insert(
                        &record_key(1, byte),
                        loc(1, byte as u32 * 100, 100),
                        Lsn(byte as u64 + 1),
                        None,
                    )
                    .expect("insert");
            }
        }
        for byte in (0..40u8).step_by(3) {
            for index in [&tree, &open] {
                index
                    .remove(&record_key(1, byte), Lsn(100 + byte as u64), loc(1, 0, 0))
                    .expect("remove");
            }
        }

        for byte in 0..40u8 {
            assert_eq!(
                tree.get(&record_key(1, byte)).expect("read"),
                open.get(&record_key(1, byte)).expect("read"),
                "byte {byte}",
            );
        }
        let mut from_tree = KeyPage::default();
        let mut from_open = KeyPage::default();
        tree.page(RECORD, Bound::Unbounded, 64, &mut from_tree)
            .expect("page");
        open.page(RECORD, Bound::Unbounded, 64, &mut from_open)
            .expect("page");
        assert_eq!(from_tree.len(), from_open.len());
        for at in 0..from_tree.len() {
            assert_eq!(
                from_tree.key_at(at),
                from_open.key_at(at),
                "key {at} of the walk"
            );
        }
        assert_eq!(tree.totals().count, open.totals().count);
    }

    // a volume that does not honour declarations gives an open column the tree
    #[test]
    fn shape_gate_falls_back() {
        let index = ReelIndex::new(OPEN_COLUMNS, IndexResidency::Resident, ShardShapes::Tree)
            .expect("index");

        index
            .insert(&record_key(1, 0x11), loc(1, 0, 400), Lsn(1), None)
            .expect("insert");

        assert!(index.get(&record_key(1, 0x11)).expect("read").is_some());
        assert_eq!(
            index.column(RECORD).expect("column").map_shape(),
            MapShape::Tree
        );
    }

    // a width the index holds no open arm for is refused rather than quietly treed
    #[test]
    fn open_refuses_odd_widths() {
        const ODD: ColumnSet = &[ColumnSpec {
            id: ColumnId(1),
            name: "odd",
            key_width: KeyWidth::Fixed(16),
            shard_bytes: 0,
            inline_max: 0,
            row_carry: 0,
            purge_mark: None,
            codec: Codec::None,
            map_shape: MapShape::Open,
        }];

        assert!(ReelIndex::new(ODD, IndexResidency::Resident, ShardShapes::Declared).is_err());
        assert!(ReelIndex::new(ODD, IndexResidency::Resident, ShardShapes::Tree).is_ok());
    }

    // the same keys cost less resident in an open shard than in a tree
    #[test]
    fn open_costs_less() {
        let tree = index();
        let open = open_index();

        for byte in 0..64u8 {
            for index in [&tree, &open] {
                index
                    .insert(
                        &blob_key(byte),
                        loc(1, byte as u32 * 100, 100),
                        Lsn(byte as u64 + 1),
                        None,
                    )
                    .expect("insert");
            }
        }

        assert!(
            open.resident_bytes() < tree.resident_bytes(),
            "open {:?} against tree {:?}",
            open.resident_bytes(),
            tree.resident_bytes(),
        );
    }

    // forgetting a retired segment clears its counters and its sequence bound
    #[test]
    fn forget_clears_segment() {
        let index = index();
        index
            .insert(&record_key(1, 0x01), loc(1, 0, 400), Lsn(1), None)
            .expect("insert");
        index
            .remove(&record_key(1, 0x01), Lsn(2), loc(1, 0, 0))
            .expect("remove");

        index.forget_segment(SegmentId(1));

        assert_eq!(index.dead_bytes(), 0);
        assert_eq!(index.min_lsn_excluding(SegmentId(9)), None);
    }
}
