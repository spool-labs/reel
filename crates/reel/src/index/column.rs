//! One column's resident keys, split into shards at the column's own key width
//!
//! Keys are held at the width the column declares and split into shards by their
//! leading bytes, so writers to unrelated parts of a column do not queue behind one
//! another. Every mutation is guarded by sequence number, so a late stale write is
//! a no-op and runtime visibility matches what a crash rebuild would resolve.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

use crate::units::ByteCount;

use crate::config::ShardShapes;
use crate::engine::Totals;
use crate::error::{ReelError, Result};
use crate::format::column::{
    ColumnId, ColumnSpec, KeyBytes, MapShape, RecordKey, INLINE_MAX, MAX_KEY_LEN,
};
use crate::format::loc::{Loc, SegmentId, SegmentIncarnation};
use crate::format::lsn::Lsn;
use crate::index::counters::SegmentTable;
use crate::index::entry::{span_of, Entry};
use crate::index::opentable::{overhead_per_key, OpenTable};
use crate::index::page::KeyPage;
use crate::index::paged::SealedRanges;
use crate::index::tbtreemap::{node_width, TBTreeMap, NODE_WIDTH};
use crate::sync::{read, write};

/// One column's resident index, held at the key width the column declares
///
/// A width not listed here is refused at open rather than padded up to a wider one.
pub enum ColumnIndex {
    W0(WidthIndex<[u8; 0], Trees<0>>),
    W2(WidthIndex<[u8; 2], Trees<2>>),
    W8(WidthIndex<[u8; 8], Trees<8>>),
    W12(WidthIndex<[u8; 12], Trees<12>>),
    W16(WidthIndex<[u8; 16], Trees<16>>),
    W20(WidthIndex<[u8; 20], Trees<20>>),
    W24(WidthIndex<[u8; 24], Trees<24>>),
    W32(WidthIndex<[u8; 32], Trees<32>>),
    W34(WidthIndex<[u8; 34], Trees<34>>),
    W36(WidthIndex<[u8; 36], Trees<36>>),
    W40(WidthIndex<[u8; 40], Trees<40>>),
    W44(WidthIndex<[u8; 44], Trees<44>>),
    W48(WidthIndex<[u8; 48], Trees<48>>),
    W72(WidthIndex<[u8; 72], Trees<72>>),
    W96(WidthIndex<[u8; 96], Trees<96>>),
    W108(WidthIndex<[u8; 108], Trees<108>>),

    /// The widths a column may ask for an open-addressed shard at
    Open32(WidthIndex<[u8; 32], OpenTables<32>>),
    Open34(WidthIndex<[u8; 34], OpenTables<34>>),
    Open72(WidthIndex<[u8; 72], OpenTables<72>>),
    Open108(WidthIndex<[u8; 108], OpenTables<108>>),

    /// A column whose keys are whatever length they are, held on the heap
    Var(WidthIndex<Box<[u8]>, VarTrees>),
}

/// What a mutation found in the map where the key was
///
/// A paged column needs more than a boolean: a key the map does not hold is not a
/// key that is gone, and telling an empty place from a grave is what says whether a
/// footer is still answering for it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Landed {
    /// Nothing at all was there, so a footer may still be answering for the key
    Nothing,

    /// A live record was, which the mutation has dealt with
    Record,

    /// A place an earlier tombstone was holding, so the key was already gone
    Grave,

    /// A version at least as new, so nothing moved
    Newer,
}

impl Landed {
    /// Whether a live record was dropped, which is what a delete counts
    pub fn dropped_record(self) -> bool {
        self == Landed::Record
    }

    /// Whether the mutation took effect at all
    pub fn took_place(self) -> bool {
        self != Landed::Newer
    }

    /// Whether a footer may still hold the live record, true only for an empty place
    pub fn may_be_paged(self) -> bool {
        self == Landed::Nothing
    }
}

/// Run one expression against whichever width a column turned out to hold
macro_rules! on_index {
    ($self:expr, $bound:ident => $body:expr) => {
        match $self {
            ColumnIndex::W0($bound) => $body,
            ColumnIndex::W2($bound) => $body,
            ColumnIndex::W8($bound) => $body,
            ColumnIndex::W12($bound) => $body,
            ColumnIndex::W16($bound) => $body,
            ColumnIndex::W20($bound) => $body,
            ColumnIndex::W24($bound) => $body,
            ColumnIndex::W32($bound) => $body,
            ColumnIndex::W34($bound) => $body,
            ColumnIndex::W36($bound) => $body,
            ColumnIndex::W40($bound) => $body,
            ColumnIndex::W44($bound) => $body,
            ColumnIndex::W48($bound) => $body,
            ColumnIndex::W72($bound) => $body,
            ColumnIndex::W96($bound) => $body,
            ColumnIndex::W108($bound) => $body,
            ColumnIndex::Open32($bound) => $body,
            ColumnIndex::Open34($bound) => $body,
            ColumnIndex::Open72($bound) => $body,
            ColumnIndex::Open108($bound) => $body,
            ColumnIndex::Var($bound) => $body,
        }
    };
}

/// One key's move as a batch hands it to the index
///
/// Carrying them together is what lets keys sharing a shard share its lock.
pub struct KeyMove<'batch> {
    /// The column the key belongs to
    pub column: ColumnId,

    /// The key being moved
    pub key: &'batch [u8],

    /// Where the record landed, or where its tombstone did
    pub loc: Loc,

    /// The sequence number it was written under
    pub lsn: Lsn,

    /// The value kept resident beside the entry, on a column that carries
    pub carried: Option<Arc<[u8]>>,

    /// Whether this is a tombstone rather than a put
    pub is_delete: bool,
}

impl ColumnIndex {
    /// Apply a batch's moves to this column, sharing a shard's lock across keys
    pub fn apply_moves(
        &self,
        moves: &[KeyMove<'_>],
        segments: &SegmentTable,
        landed: &mut Vec<Landed>,
    ) {
        on_index!(self, index => index.apply_moves(moves, segments, landed))
    }

    /// An empty index for one column, refusing a width nothing indexes
    ///
    /// A volume that does not honour declarations drops the open-shard request
    /// rather than refusing it: nothing on disk turns on which structure the keys
    /// sat in.
    pub fn new(spec: &ColumnSpec, shapes: ShardShapes) -> Result<ColumnIndex> {
        let is_open = spec.map_shape == MapShape::Open && shapes == ShardShapes::Declared;
        let Some(width) = spec.key_width.fixed() else {
            if is_open {
                return Err(ReelError::Config(format!(
                    "column {} asks for an open shard, which needs a declared key width",
                    spec.name,
                )));
            }
            return Ok(ColumnIndex::Var(WidthIndex::new(spec)));
        };
        if is_open {
            return match width {
                32 => Ok(ColumnIndex::Open32(WidthIndex::new(spec))),
                34 => Ok(ColumnIndex::Open34(WidthIndex::new(spec))),
                72 => Ok(ColumnIndex::Open72(WidthIndex::new(spec))),
                108 => Ok(ColumnIndex::Open108(WidthIndex::new(spec))),
                other => Err(ReelError::Config(format!(
                    "column {} asks for an open shard at {other} bytes, which the index \
                     holds no arm for",
                    spec.name,
                ))),
            };
        }
        match width {
            0 => Ok(ColumnIndex::W0(WidthIndex::new(spec))),
            2 => Ok(ColumnIndex::W2(WidthIndex::new(spec))),
            8 => Ok(ColumnIndex::W8(WidthIndex::new(spec))),
            12 => Ok(ColumnIndex::W12(WidthIndex::new(spec))),
            16 => Ok(ColumnIndex::W16(WidthIndex::new(spec))),
            20 => Ok(ColumnIndex::W20(WidthIndex::new(spec))),
            24 => Ok(ColumnIndex::W24(WidthIndex::new(spec))),
            32 => Ok(ColumnIndex::W32(WidthIndex::new(spec))),
            34 => Ok(ColumnIndex::W34(WidthIndex::new(spec))),
            36 => Ok(ColumnIndex::W36(WidthIndex::new(spec))),
            40 => Ok(ColumnIndex::W40(WidthIndex::new(spec))),
            44 => Ok(ColumnIndex::W44(WidthIndex::new(spec))),
            48 => Ok(ColumnIndex::W48(WidthIndex::new(spec))),
            72 => Ok(ColumnIndex::W72(WidthIndex::new(spec))),
            96 => Ok(ColumnIndex::W96(WidthIndex::new(spec))),
            108 => Ok(ColumnIndex::W108(WidthIndex::new(spec))),
            other => Err(ReelError::Config(format!(
                "column {} declares a key width of {other} bytes, which no index holds",
                spec.name,
            ))),
        }
    }

    /// Bytes every key in this column occupies
    pub fn key_width(&self) -> u16 {
        on_index!(self, index => index.key_width())
    }

    /// Bytes a resident key costs this column beyond itself and its entry
    ///
    /// The shape decides it, and the two shapes are four times apart, so one number
    /// for both would report a tree's cost for an open shard's keys.
    pub fn overhead_per_key(&self) -> u64 {
        on_index!(self, index => index.overhead_per_key())
    }

    /// Which structure this column's shards opened in, not always what was declared
    pub fn map_shape(&self) -> MapShape {
        on_index!(self, index => index.map_shape())
    }

    /// Apply a committed data record, guarded by its sequence number
    pub fn insert(
        &self,
        key: &[u8],
        entry: Entry,
        segments: &SegmentTable,
        carried: Option<Arc<[u8]>>,
    ) -> Landed {
        on_index!(self, index => index.insert(key, entry, segments, carried))
    }

    /// Ceiling up to which this column carries values beside its entries
    pub fn carry_max(&self) -> u16 {
        on_index!(self, index => index.carry_max())
    }

    /// The carried value for a key, exactly as new as the entry the caller holds
    pub fn carried_value(&self, key: &[u8], lsn: Lsn) -> Option<Arc<[u8]>> {
        on_index!(self, index => index.carried_value(key, lsn))
    }

    /// Remember a value a read just paid the device for
    pub fn warm_carried(&self, key: &[u8], lsn: Lsn, bytes: &[u8], two_touch: bool) {
        on_index!(self, index => index.warm_carried(key, lsn, bytes, two_touch))
    }

    /// Bytes of carried values the column holds resident
    pub fn carried_bytes(&self) -> u64 {
        on_index!(self, index => index.carried_bytes())
    }

    /// Shed carried bytes toward a budget, coldest first
    pub fn shed_carried(&self, want: u64, start_shard: usize, visit_cap: usize) -> u64 {
        on_index!(self, index => index.shed_carried(want, start_shard, visit_cap))
    }

    /// Drop a key on a tombstone, guarded by its sequence number
    pub fn remove(&self, key: &[u8], lsn: Lsn, tombstone: Loc, segments: &SegmentTable) -> Landed {
        on_index!(self, index => index.remove(key, lsn, tombstone, segments))
    }

    /// What a record of this column's width and a payload of this length occupies
    pub fn span_of(&self, len: u32) -> u64 {
        span_of(self.key_width(), len)
    }

    /// Book a record only a footer was answering for as gone
    pub fn settle_paged(
        &self,
        key: &[u8],
        loc: Loc,
        counted: bool,
        segments: &SegmentTable,
    ) -> bool {
        on_index!(self, index => index.settle_paged(key, loc, counted, segments))
    }

    /// Take a paged key out with a grave of its own, for a record that will not read
    pub fn evict_paged(
        &self,
        key: &[u8],
        loc: Loc,
        lsn: Lsn,
        counted: bool,
        segments: &SegmentTable,
    ) -> bool {
        on_index!(self, index => index.evict_paged(key, loc, lsn, counted, segments))
    }

    /// Bring a paged key back into the map at the copy compaction rewrote it to
    pub fn repoint_paged(
        &self,
        key: &[u8],
        from: Loc,
        to: Loc,
        lsn: Lsn,
        counted: bool,
        segments: &SegmentTable,
    ) -> bool {
        on_index!(self, index => index.repoint_paged(key, from, to, lsn, counted, segments))
    }

    /// Take a half-open range with one standing cover, sweeping nothing
    pub fn remove_range(&self, start: &[u8], end: Option<&[u8]>, lsn: Lsn) {
        on_index!(self, index => index.remove_range(start, end, lsn))
    }

    /// The cover the sweep should settle next, oldest first
    pub fn next_pending_cover(&self) -> Option<PendingCover> {
        on_index!(self, index => index.next_pending_cover())
    }

    /// Move a cover's release pass forward, or hand it to the map sweep
    pub fn advance_release(&self, lsn: Lsn, resume: Option<&[u8]>) {
        on_index!(self, index => index.advance_release(lsn, resume))
    }

    /// Drop one bounded run of the map entries a cover has taken
    pub fn sweep_run(
        &self,
        lsn: Lsn,
        budget: usize,
        segments: &SegmentTable,
    ) -> (u64, usize, bool) {
        on_index!(self, index => index.sweep_run(lsn, budget, segments))
    }

    /// Settle a footer-held record a standing cover has taken
    pub fn release_covered(
        &self,
        key: &[u8],
        loc: Loc,
        below: Lsn,
        counted: bool,
        segments: &SegmentTable,
    ) -> bool {
        on_index!(self, index => index.release_covered(key, loc, below, counted, segments))
    }

    /// Whether a finished cover already settled everything at this key and version
    pub fn covered_by_swept(&self, key: &[u8], lsn: Lsn) -> bool {
        on_index!(self, index => index.covered_by_swept(key, lsn))
    }

    /// Whether an unfinished cover reaches into this inclusive key range
    pub fn pending_overlaps(&self, low: &[u8], high: &[u8]) -> bool {
        on_index!(self, index => index.pending_overlaps(low, high))
    }

    /// Whether any cover is still owed its sweep
    pub fn has_pending_covers(&self) -> bool {
        on_index!(self, index => index.has_pending_covers())
    }

    /// Drop what tombstones hold once nothing older than them can still be published
    pub fn prune_tombstones(&self, before: Lsn, sealed: Option<&SealedRanges>) -> u64 {
        on_index!(self, index => index.prune_tombstones(before, sealed))
    }

    /// Graves the column is holding, the memory a prune would give back
    pub fn grave_count(&self) -> u64 {
        on_index!(self, index => index.grave_count())
    }

    /// Ranges the column is still testing inserts against
    pub fn cover_count(&self) -> u64 {
        on_index!(self, index => index.cover_count())
    }

    /// Resolve a key to its live entry
    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        on_index!(self, index => index.get(key))
    }

    /// Whether a key resolves to a live record
    pub fn contains(&self, key: &[u8]) -> bool {
        on_index!(self, index => index.contains(key))
    }

    /// The entry for a key, a grave included, so absent and deleted are different
    pub fn entry_or_grave(&self, key: &[u8]) -> Option<Entry> {
        on_index!(self, index => index.entry_or_grave(key))
    }

    /// The same for many keys at once, answered in the order asked
    pub fn entry_many(&self, keys: &[RecordKey], run: &[usize], out: &mut Vec<Option<Entry>>) {
        on_index!(self, index => index.entry_many(keys, run, out))
    }

    /// Whether a range delete took a version this new, asked of a footer's answer
    pub fn is_covered_key(&self, key: &[u8], lsn: Lsn) -> bool {
        on_index!(self, index => index.is_covered_key(key, lsn))
    }

    /// The same test as a reader at an older sequence number would make it
    pub fn is_covered_key_at(&self, key: &[u8], lsn: Lsn, snapshot: Lsn) -> bool {
        on_index!(self, index => index.is_covered_key_at(key, lsn, snapshot))
    }

    /// Recorded payload length of a live key, served without a read
    pub fn size_of(&self, key: &[u8]) -> Option<ByteCount> {
        on_index!(self, index => index.size_of(key))
    }

    /// Repoint a key from a compacted record to its rewritten copy under a guard
    pub fn repoint(
        &self,
        key: &[u8],
        new_loc: Loc,
        expected_lsn: Lsn,
        segments: &SegmentTable,
    ) -> bool {
        on_index!(self, index => index.repoint(key, new_loc, expected_lsn, segments))
    }

    /// Drop a key while it still resolves one exact location, writing no tombstone
    pub fn evict_at(&self, key: &[u8], loc: Loc, segments: &SegmentTable) -> bool {
        on_index!(self, index => index.evict_at(key, loc, segments))
    }

    /// Give a key up to the footer of the segment it landed in
    pub fn page_out(&self, key: &[u8], loc: Loc) -> bool {
        on_index!(self, index => index.page_out(key, loc))
    }

    /// Count a key a rewrite listed in a row, for a shard that never counted it
    pub fn count_listed(&self, key: &[u8], bytes: u64) -> bool {
        on_index!(self, index => index.count_listed(key, bytes))
    }

    /// Install rebuilt entries in one pass, replacing whatever the column held
    pub fn install(&self, entries: Vec<(KeyBytes, Entry)>, segments: &SegmentTable) {
        on_index!(self, index => index.install(entries, segments))
    }

    /// Put back a range a rebuild resolved, without sweeping for what it covers
    pub fn install_cover(&self, start: &[u8], end: Option<&KeyBytes>, lsn: Lsn) {
        on_index!(self, index => index.install_cover(start, end, lsn))
    }

    /// Drop every key, for a reader rebuilding the whole volume
    pub fn clear(&self) {
        on_index!(self, index => index.clear())
    }

    /// Live key count and payload byte total for the column
    pub fn totals(&self) -> Totals {
        on_index!(self, index => index.totals())
    }

    /// Keys the maps are holding, graves included, for weighing what they cost
    pub fn resident_keys(&self) -> u64 {
        on_index!(self, index => index.resident_keys())
    }

    /// Share of neighbouring keys the shards cannot tell apart by their leads
    pub fn lead_tie_rate(&self) -> Option<f64> {
        on_index!(self, index => index.lead_tie_rate())
    }

    /// Live totals for one shard-aligned prefix, or nothing if it is not one
    pub fn prefix_totals(&self, prefix: &[u8]) -> Option<Totals> {
        on_index!(self, index => index.prefix_totals(prefix))
    }

    /// Fill a page with one bounded run of live keys, ascending from a bound
    pub fn page(&self, start: Bound<&[u8]>, limit: usize, out: &mut KeyPage) {
        on_index!(self, index => index.page(start, limit, out))
    }

    /// Fill a page with one bounded run of live keys, descending from a bound
    pub fn page_back(&self, end: Bound<&[u8]>, limit: usize, out: &mut KeyPage) {
        on_index!(self, index => index.page_back(end, limit, out))
    }
}

/// One key's carried state: a first touch remembered, or the value resident
///
/// The ghost costs a map entry and no bytes, and is what makes a second touch mean
/// something. `Held` carries the value and a clock the shed pass steps down, bumped
/// by hits, so a key keeps its place by being read.
#[derive(Default)]
pub enum Carried {
    /// Touched once; the next warm admits it
    ///
    /// Also what an unfilled place in a tree's value array holds, since it owns nothing.
    #[default]
    Seen,

    /// Resident, serving reads without io
    Held {
        lsn: Lsn,
        bytes: Arc<[u8]>,
        heat: AtomicU8,
    },
}

/// Keys in one shard's run before a batch is worth its bookkeeping
const BATCH_RUN: usize = 4;

/// The shard grouping one thread's batched lookups work through
///
/// Four bytes a key and nothing borrowed, so it stays with the thread rather than
/// being bought per batch.
#[derive(Default)]
struct KeyGroups {
    /// Each key's shard and the position it was asked at, sorted by shard
    wanted: Vec<(u32, u32)>,
}

thread_local! {
    /// One grouping list per looking-up thread, handed back after every batch
    static KEY_GROUPS: std::cell::Cell<KeyGroups> =
        const { std::cell::Cell::new(KeyGroups { wanted: Vec::new() }) };
}

/// This thread's grouping list, given back however the lookup that took it ends
struct HeldGroups(KeyGroups);

impl HeldGroups {
    fn take() -> HeldGroups {
        HeldGroups(KEY_GROUPS.with(std::cell::Cell::take))
    }
}

impl Drop for HeldGroups {
    fn drop(&mut self) {
        let mut held = std::mem::take(&mut self.0);
        held.wanted.clear();
        KEY_GROUPS.with(|spare| spare.set(held));
    }
}

/// Countdown a read admission starts at
const CARRIED_ADMIT: u8 = 2;

/// Ceiling a hit bumps the countdown toward
const CARRIED_HEAT_MAX: u8 = 3;

/// Drop what a key carried, keeping the byte total exact
fn evict_carried<K: IndexKey, S: Shape<K>>(state: &mut ShardState<K, S>, key: &K) {
    if let Some(Carried::Held { bytes, .. }) = state.carried.take(key.as_slice()) {
        state.carried_bytes -= bytes.len() as u64;
    }
}

/// One shard's keys and the payload bytes they resolve to
struct ShardState<K: IndexKey, S: Shape<K>> {
    /// Every key the shard holds, live records and graves alike
    map: S::Entries,

    /// Payload bytes the live records add up to
    bytes: u64,

    /// How many of the map's places are held by a tombstone rather than a record
    graves: usize,

    /// Oldest sequence number any grave carries, a floor that can only understate
    oldest_grave: Lsn,

    /// Live records of this shard that a sealed footer answers for instead
    paged: usize,

    /// Values the column carries beside its entries, under the entries' own lock
    carried: S::Carried,

    /// Bytes the carried values add up to
    carried_bytes: u64,
}

impl<K: IndexKey, S: Shape<K>> ShardState<K, S> {
    fn empty() -> ShardState<K, S> {
        ShardState {
            map: S::Entries::default(),
            bytes: 0,
            graves: 0,
            oldest_grave: Lsn(u64::MAX),
            carried: S::Carried::default(),
            carried_bytes: 0,
            paged: 0,
        }
    }

    /// Count a grave in, keeping the floor the prune skips untouched shards by
    fn note_grave(&mut self, lsn: Lsn) {
        self.graves += 1;
        if lsn < self.oldest_grave {
            self.oldest_grave = lsn;
        }
    }

    /// Live keys the shard holds: its map without the graves, plus what it paged
    fn live_count(&self) -> u64 {
        (self.map.count() - self.graves + self.paged) as u64
    }
}

/// One cover still owed its sweep, as the driver above the column sees it
///
/// Held as plain bytes rather than at the column's own width, since the driver
/// spans columns of every width.
pub struct PendingCover {
    /// Sequence number the delete was drawn under
    pub lsn: Lsn,

    /// Exclusive end of the range, or nothing to the top of the column
    pub end: Option<Vec<u8>>,

    /// Key the release pass resumes from, while that phase is still running
    pub release_from: Option<Vec<u8>>,
}

/// A range one tombstone covered, held at the column's own key width
///
/// The cover is the delete itself, standing: every read, walk and insert consults
/// it, and the records it covers are settled behind it in bounded passes on the
/// maintenance tick. A grave per key is impossible, since a range also covers keys
/// it has not seen yet, and those are unbounded where the range is not.
struct Covered<K: IndexKey> {
    /// Inclusive start of the range
    low: K,

    /// Exclusive end, or nothing when the range ran to the top of the column
    high: Option<K>,

    /// Sequence number the delete was drawn under
    lsn: Lsn,

    /// How far the lazy sweep has taken this cover toward retirement
    phase: SweepPhase<K>,
}

/// Where the lazy sweep stands on one cover
///
/// The order of the phases is load-bearing. Records only footers answer for are
/// settled first, while the covered map entries still stand, or the release pass
/// would settle the same record twice. The map entries drop second, and outright
/// rather than into graves: the cover itself refuses anything drawn before the
/// delete, and it retires no earlier than a grave would.
enum SweepPhase<K: IndexKey> {
    /// Settling records only footers answer for, resuming at this key
    Release(K),

    /// Dropping covered map entries, resuming at this key
    Sweep(K),

    /// Nothing left to settle; the cover stands only to refuse
    Done,
}

impl<K: IndexKey> Covered<K> {
    /// Whether any sealed segment still holds keys this range would take
    ///
    /// A footer never hears about a range delete, so on a paged column the range is
    /// the only thing standing between a covered key and a search that would find
    /// it. It is finished when the segments it reaches into are gone.
    fn reaches_sealed(&self, sealed: &SealedRanges) -> bool {
        sealed.overlaps(
            self.low.as_slice(),
            self.high.as_ref().map(|high| high.as_slice()),
        )
    }

    /// Whether this covered a key and was drawn after that key was written
    ///
    /// Compares the bytes rather than the keys so a lookup can ask without building
    /// one; both key shapes order borrowed exactly as they order owned.
    fn covers(&self, key: &[u8], lsn: Lsn) -> bool {
        if lsn >= self.lsn || key < self.low.as_slice() {
            return false;
        }
        match &self.high {
            Some(high) => key < high.as_slice(),
            None => true,
        }
    }
}

/// Bits one shard's filter holds, sixteen kibibytes allocated on first use
const FILTER_WORDS: usize = 2048;

/// A filter in front of one shard's lock, answering only definite absence
///
/// Allocated the first time its shard takes a key, so the sixty-five thousand
/// shards a two-byte column declares cost nothing until they hold something. A
/// key's bits are set before the map takes it, so a reader that sees a bit clear is
/// reading a state the lock it skipped would also have allowed. Nothing is ever
/// cleared, so a filter that saturates answers maybe for everything.
struct ShardFilter {
    words: OnceLock<Box<[AtomicU64]>>,
}

impl ShardFilter {
    fn empty() -> ShardFilter {
        ShardFilter {
            words: OnceLock::new(),
        }
    }

    /// Record a key on its way into the shard's map
    fn note(&self, hash: u64) {
        let words = self
            .words
            .get_or_init(|| (0..FILTER_WORDS).map(|_| AtomicU64::new(0)).collect());
        let (first, second) = probe_pair(hash);
        words[(first / 64) as usize].fetch_or(1 << (first % 64), Ordering::Relaxed);
        words[(second / 64) as usize].fetch_or(1 << (second % 64), Ordering::Relaxed);
    }

    /// Whether the shard may hold the key, where a no is certain
    fn may_hold(&self, hash: u64) -> bool {
        let Some(words) = self.words.get() else {
            return false;
        };
        let (first, second) = probe_pair(hash);
        words[(first / 64) as usize].load(Ordering::Relaxed) & (1 << (first % 64)) != 0
            && words[(second / 64) as usize].load(Ordering::Relaxed) & (1 << (second % 64)) != 0
    }
}

/// Two bit positions from one key hash, mixed apart so the pair is not one sequence
fn probe_pair(hash: u64) -> (u64, u64) {
    const BITS: u64 = (FILTER_WORDS * 64) as u64;
    let other = hash.wrapping_mul(0xD6E8_FEB8_6659_FD93) ^ (hash >> 32);
    (hash % BITS, other % BITS)
}

/// One hash of a key's bytes, taken borrowed so a probe builds no key
fn filter_hash(key: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for chunk in key.chunks(8) {
        let mut word = [0u8; 8];
        word[..chunk.len()].copy_from_slice(chunk);
        hash = (hash ^ u64::from_le_bytes(word)).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        hash ^= hash >> 29;
    }
    hash
}

/// One column's index at a fixed key width
pub struct WidthIndex<K: IndexKey, S: Shape<K>> {
    /// The column's keys, split by their leading bytes
    shards: Vec<RwLock<ShardState<K, S>>>,

    /// A filter beside each shard's lock, so a definite miss never takes it
    filters: Vec<ShardFilter>,

    /// Ranges a tombstone swept, tested against records that arrive after it
    covers: RwLock<Vec<Covered<K>>>,

    /// Whether the list above holds anything, read with the shard held
    has_covers: AtomicBool,

    /// Shards holding at least one key, so a walk skips the empty ones
    occupied: RwLock<TBTreeMap<u64, NODE_WIDTH, ()>>,

    /// Width the column declared, for accounting only; a variable column has none
    declared_width: u16,

    /// Leading key bytes that pick a shard
    shard_bytes: u8,

    /// Ceiling past which the column stops carrying a value, zero for none
    carry_max: u16,
}

impl<K: IndexKey, S: Shape<K>> WidthIndex<K, S> {
    /// An empty index for a column, split into the shards the column declares
    pub fn new(spec: &ColumnSpec) -> WidthIndex<K, S> {
        let mut shards = Vec::with_capacity(spec.shard_count());
        let mut filters = Vec::with_capacity(spec.shard_count());
        for _ in 0..spec.shard_count() {
            shards.push(RwLock::new(ShardState::empty()));
            filters.push(ShardFilter::empty());
        }
        WidthIndex {
            shards,
            filters,
            covers: RwLock::new(Vec::new()),
            has_covers: AtomicBool::new(false),
            occupied: RwLock::new(TBTreeMap::new()),
            declared_width: spec.key_width.fixed().unwrap_or(0),
            shard_bytes: spec.shard_bytes,
            carry_max: match spec.inline_max as usize > INLINE_MAX {
                true => spec.inline_max,
                false => 0,
            },
        }
    }

    /// Ceiling up to which this column carries values beside its entries
    pub fn carry_max(&self) -> u16 {
        self.carry_max
    }

    /// Bytes every key occupies, the declared width rather than any one key's
    pub fn key_width(&self) -> u16 {
        self.declared_width
    }

    /// Bytes a resident key costs beyond itself and its entry, from the shape
    pub fn overhead_per_key(&self) -> u64 {
        S::OVERHEAD_PER_KEY
    }

    /// Which structure this column's shards took
    pub fn map_shape(&self) -> MapShape {
        S::SHAPE
    }

    /// Apply a committed data record, guarded by its sequence number
    ///
    /// A newer put shadows the record it replaces and moves its bytes to dead. A put
    /// that lost the ordering race books its own bytes dead to keep the counters
    /// exact. What comes back is what the map held: on a paged column a put that
    /// landed on an empty place may have replaced a record only a footer knows about,
    /// and exactly one put can see the place empty, so exactly one goes looking.
    pub fn insert(
        &self,
        key: &[u8],
        entry: Entry,
        segments: &SegmentTable,
        carried: Option<Arc<[u8]>>,
    ) -> Landed {
        let key = match K::from_slice(key) {
            Some(key) => key,
            None => return Landed::Newer,
        };
        let at = self.shard_of(&key);
        let mut state = write(&self.shards[at]);
        self.insert_held(&mut state, at, key, entry, segments, carried)
    }

    /// Apply a batch's moves, holding a shard once for the run of keys in it
    ///
    /// Applied strictly in the order given, so what a reader can see is what it saw
    /// before. Consecutive keys landing in the same shard hold it together; keys
    /// that scatter across shards cost one lock each.
    pub fn apply_moves(
        &self,
        moves: &[KeyMove<'_>],
        segments: &SegmentTable,
        landed: &mut Vec<Landed>,
    ) {
        let mut at = 0;
        while at < moves.len() {
            let Some(key) = K::from_slice(moves[at].key) else {
                landed.push(Landed::Newer);
                at += 1;
                continue;
            };
            let shard = self.shard_of(&key);
            let mut state = write(&self.shards[shard]);
            landed.push(self.apply_held(&mut state, shard, key, &moves[at], segments));
            at += 1;
            while at < moves.len() {
                let Some(next) = K::from_slice(moves[at].key) else {
                    break;
                };
                if self.shard_of(&next) != shard {
                    break;
                }
                landed.push(self.apply_held(&mut state, shard, next, &moves[at], segments));
                at += 1;
            }
        }
    }

    /// One move of either kind, with its shard already held
    ///
    /// The tombstone's own span is booked from in here rather than before the lock,
    /// which adds no lock order the crate did not already have.
    fn apply_held(
        &self,
        state: &mut ShardState<K, S>,
        shard: usize,
        key: K,
        moving: &KeyMove<'_>,
        segments: &SegmentTable,
    ) -> Landed {
        match moving.is_delete {
            true => {
                segments.mark_held(
                    moving.loc.segment,
                    moving.lsn,
                    span_of(key.width(), moving.loc.len),
                );
                self.remove_held(state, shard, key, moving.lsn, moving.loc, segments)
            }
            false => self.insert_held(
                state,
                shard,
                key,
                Entry::new(moving.loc, moving.lsn),
                segments,
                moving.carried.clone(),
            ),
        }
    }

    /// The insert itself, with the key parsed and its shard already held
    ///
    /// Split out so a batch takes a shard once for the keys landing in it rather
    /// than once per key. The publish barrier serialises this work, so an extra
    /// lock take is time every other writer spends queued behind it.
    fn insert_held(
        &self,
        state: &mut ShardState<K, S>,
        at: usize,
        key: K,
        entry: Entry,
        segments: &SegmentTable,
        carried: Option<Arc<[u8]>>,
    ) -> Landed {
        // The record's own hold keeps its segment from retiring until this publish
        // lands, so the stamp can be issued live here.
        let entry = entry.stamped(segments.live_incarnation(entry.loc.segment));
        let (loc, lsn) = (entry.loc, entry.lsn);
        let was_empty = state.map.vacant();
        let new_len = u64::from(loc.len);

        // A record drawn before a range delete is taken by it, key seen or not. The
        // test is under the shard because that is what orders it against the sweep.
        if self.is_covered(key.as_slice(), lsn) {
            segments.mark_dead(loc.segment, lsn, span_of(key.width(), loc.len));
            return Landed::Newer;
        }

        // The entry goes in on the way past and a refusal puts back what it
        // displaced, so an accepted put is one descent rather than a lookup and an
        // insert.
        self.filters[at].note(filter_hash(key.as_slice()));
        // Taken before the key moves into the map, since a variable column's key
        // owns the bytes the span is measured from.
        let width = key.width();
        let landed = match state.map.put(key.clone(), entry) {
            // A grave is refused like any newer version: the delete that left it
            // was drawn after this record.
            Some(existing) if existing.lsn >= lsn => {
                state.map.put(key, existing);
                segments.mark_dead(loc.segment, lsn, span_of(width, loc.len));
                return Landed::Newer;
            }
            // An older grave holds a place and nothing else, so the key comes back
            // fresh and the segment table hears nothing.
            Some(existing) if existing.is_grave() => {
                state.graves -= 1;
                state.bytes += new_len;
                Landed::Grave
            }
            Some(existing) => {
                let old_len = u64::from(existing.loc.len);
                segments.shadow(existing.loc.segment, existing.span(key.width()));
                state.bytes = resize(state.bytes, old_len, new_len);
                Landed::Record
            }
            None => {
                state.bytes += new_len;
                Landed::Nothing
            }
        };

        if self.carry_max != 0 {
            match carried {
                Some(bytes) => {
                    // A write capture enters at the bottom of the clock, so a
                    // value earns its place by being read rather than written.
                    state.carried_bytes += bytes.len() as u64;
                    let held = Carried::Held {
                        lsn,
                        bytes,
                        heat: AtomicU8::new(0),
                    };
                    if let Some(Carried::Held { bytes: old, .. }) = state.carried.put(key, held) {
                        state.carried_bytes -= old.len() as u64;
                    }
                }
                None => evict_carried(state, &key),
            }
        }

        segments.mark_live(loc.segment, lsn, span_of(width, loc.len));
        self.note_filled(at, was_empty);
        landed
    }

    /// The carried value for a key, exactly as new as the entry the caller holds
    pub fn carried_value(&self, key: &[u8], lsn: Lsn) -> Option<Arc<[u8]>> {
        if self.carry_max == 0 {
            return None;
        }
        if !K::accepts(key) {
            return None;
        }
        let state = read(&self.shards[self.shard_of_bytes(key)]);
        match state.carried.at(key)? {
            Carried::Held {
                lsn: held,
                bytes,
                heat,
            } if *held == lsn => {
                // The bump is relaxed and lossy on purpose: racing bumps can
                // only under-count heat.
                let hot = heat.load(Ordering::Relaxed);
                if hot < CARRIED_HEAT_MAX {
                    heat.store(hot + 1, Ordering::Relaxed);
                }
                Some(Arc::clone(bytes))
            }
            _ => None,
        }
    }

    /// Remember a value a read just paid the device for
    ///
    /// The entry is checked again under the write lock, so a value captured against
    /// one version never serves for another.
    pub fn warm_carried(&self, key: &[u8], lsn: Lsn, bytes: &[u8], two_touch: bool) {
        if self.carry_max == 0 || bytes.len() > self.carry_max as usize {
            return;
        }
        let Some(key) = K::from_slice(key) else {
            return;
        };
        let at = self.shard_of(&key);
        let mut state = write(&self.shards[at]);
        match state.map.at(key.as_slice()) {
            Some(existing) if !existing.is_grave() && existing.lsn == lsn => {}
            Some(_) | None => return,
        }
        // Armed, a first touch leaves only the ghost, so one pass over cold
        // keys buys no residency it never earned.
        if two_touch && !state.carried.holds(key.as_slice()) {
            state.carried.put(key, Carried::Seen);
            return;
        }
        state.carried_bytes += bytes.len() as u64;
        let held = Carried::Held {
            lsn,
            bytes: Arc::from(bytes),
            heat: AtomicU8::new(CARRIED_ADMIT),
        };
        if let Some(Carried::Held { bytes: old, .. }) = state.carried.put(key, held) {
            state.carried_bytes -= old.len() as u64;
        }
    }

    /// Bytes of carried values the column holds resident
    pub fn carried_bytes(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| read(shard).carried_bytes)
            .sum()
    }

    /// Shed carried values, ghosts decaying and countdowns stepping down
    ///
    /// Bounded by the bytes wanted and the entries visited, whichever ends first. A
    /// countdown at zero gives its value back and anything warmer steps one down.
    /// The caller hands in the shard to start at, so repeated passes spread over the
    /// column instead of always draining the first shard.
    pub fn shed_carried(&self, want: u64, start_shard: usize, visit_cap: usize) -> u64 {
        if self.carry_max == 0 || want == 0 {
            return 0;
        }
        let mut freed = 0u64;
        let mut visited = 0usize;
        let count = self.shards.len();
        for step in 0..count {
            if freed >= want || visited >= visit_cap {
                break;
            }
            let shard = &self.shards[(start_shard + step) % count];
            if read(shard).carried.vacant() {
                continue;
            }
            let mut state = write(shard);
            let mut dropped: Vec<K> = Vec::new();
            for (key, held) in state.carried.walk() {
                if freed >= want || visited >= visit_cap {
                    break;
                }
                visited += 1;
                match held {
                    Carried::Seen => dropped.push(key.clone()),
                    Carried::Held { bytes, heat, .. } => {
                        let hot = heat.load(Ordering::Relaxed);
                        match hot {
                            0 => {
                                freed += bytes.len() as u64;
                                dropped.push(key.clone());
                            }
                            _ => heat.store(hot - 1, Ordering::Relaxed),
                        }
                    }
                }
            }
            for key in &dropped {
                evict_carried(&mut state, key);
            }
        }
        freed
    }

    /// Drop a key on a tombstone, guarded by its sequence number
    ///
    /// The shadowed record's bytes move to dead and the tombstone's sequence number
    /// stays on the key as a grave, since forgetting the key would leave nothing to
    /// refuse a put drawn before the delete and published after it. The grave carries
    /// the segment the tombstone landed in, which is what lets a paged column give it
    /// up once that segment has a footer.
    pub fn remove(&self, key: &[u8], lsn: Lsn, tombstone: Loc, segments: &SegmentTable) -> Landed {
        let key = match K::from_slice(key) {
            Some(key) => key,
            None => return Landed::Newer,
        };
        // The tombstone record holds space in the segment that took it, whatever it
        // does to the key it names: a segment with no row is one neither compaction
        // nor the scrub can see.
        segments.mark_held(tombstone.segment, lsn, span_of(key.width(), tombstone.len));
        let at = self.shard_of(&key);
        let mut state = write(&self.shards[at]);
        self.remove_held(&mut state, at, key, lsn, tombstone, segments)
    }

    /// The tombstone itself, with the key parsed and its shard already held
    ///
    /// The caller has already booked the tombstone record's own span.
    fn remove_held(
        &self,
        state: &mut ShardState<K, S>,
        at: usize,
        key: K,
        lsn: Lsn,
        tombstone: Loc,
        segments: &SegmentTable,
    ) -> Landed {
        let was_empty = state.map.vacant();
        let existing = state.map.at(key.as_slice()).copied();
        if let Some(existing) = existing {
            if existing.lsn >= lsn {
                return Landed::Newer;
            }
        }

        let landed = match existing {
            Some(existing) if !existing.is_grave() => {
                self.drop_entry(state, &key, existing, segments);
                Landed::Record
            }
            // An older grave is replaced by this one rather than counted again.
            Some(_) => {
                state.graves -= 1;
                Landed::Grave
            }
            None => Landed::Nothing,
        };

        self.filters[at].note(filter_hash(key.as_slice()));
        state
            .map
            .put(key.clone(), Entry::grave_from(lsn, tombstone.segment));
        state.note_grave(lsn);
        evict_carried(state, &key);
        self.note_filled(at, was_empty);
        landed
    }

    /// Book a record only a footer was answering for as gone
    ///
    /// The shard cannot find such a record itself, so the caller resolves it and
    /// says whether any counter ever held it.
    pub fn settle_paged(
        &self,
        key: &[u8],
        loc: Loc,
        counted: bool,
        segments: &SegmentTable,
    ) -> bool {
        let Some(key) = K::from_slice(key) else {
            return false;
        };
        let at = self.shard_of(&key);
        let mut state = write(&self.shards[at]);
        settle(&mut state, loc, segments, key.width(), counted)
    }

    /// Take a paged key out with a grave of its own, for a record that will not read
    ///
    /// The grave carries no origin: no tombstone was written, so nothing on disk
    /// will ever say this key is gone and the grave cannot be given up while the
    /// segment behind it stands.
    pub fn evict_paged(
        &self,
        key: &[u8],
        loc: Loc,
        lsn: Lsn,
        counted: bool,
        segments: &SegmentTable,
    ) -> bool {
        let Some(key) = K::from_slice(key) else {
            return false;
        };
        let at = self.shard_of(&key);
        let mut state = write(&self.shards[at]);
        let was_empty = state.map.vacant();
        if state.map.holds(key.as_slice())
            || !settle(&mut state, loc, segments, key.width(), counted)
        {
            return false;
        }

        self.filters[at].note(filter_hash(key.as_slice()));
        state.map.put(key, Entry::grave(lsn));
        state.note_grave(lsn);
        self.note_filled(at, was_empty);
        true
    }

    /// Bring a paged key back into the map at the copy compaction rewrote it to
    ///
    /// Guarded by where the caller found it, so a key rewritten or deleted since it
    /// was resolved keeps whatever took its place. A key handed over at runtime moves
    /// back from the paged count; one a rebuild left sealed was never counted, so it
    /// is counted fresh on the way in.
    pub fn repoint_paged(
        &self,
        key: &[u8],
        from: Loc,
        to: Loc,
        lsn: Lsn,
        counted: bool,
        segments: &SegmentTable,
    ) -> bool {
        let Some(key) = K::from_slice(key) else {
            return false;
        };
        let at = self.shard_of(&key);
        let mut state = write(&self.shards[at]);
        let was_empty = state.map.vacant();
        if state.map.holds(key.as_slice()) {
            segments.mark_dead(to.segment, lsn, span_of(key.width(), to.len));
            return false;
        }
        segments.release_live(from.segment, span_of(key.width(), from.len));
        segments.mark_live(to.segment, lsn, span_of(key.width(), to.len));
        self.filters[at].note(filter_hash(key.as_slice()));
        state.map.put(
            key,
            Entry::new(to, lsn).stamped(segments.live_incarnation(to.segment)),
        );
        match counted {
            // Saturating on purpose: a count that reaches zero early is a count
            // to fix, not a reason to drop a live record.
            true => state.paged = state.paged.saturating_sub(1),
            false => state.bytes += u64::from(to.len),
        }
        self.note_filled(at, was_empty);
        true
    }

    /// Whether a range delete took a record this new, asked of a key from outside
    ///
    /// A footer is written once and never told about a range delete that came after
    /// it, so the version it names is offered here before it is believed.
    pub fn is_covered_key(&self, key: &[u8], lsn: Lsn) -> bool {
        match K::accepts(key) {
            true => self.is_covered(key, lsn),
            false => false,
        }
    }

    /// The same test made as of an older sequence number
    ///
    /// A cover drawn after the snapshot records a deletion that had not happened
    /// yet, so it hides nothing from that reader.
    pub fn is_covered_key_at(&self, key: &[u8], lsn: Lsn, snapshot: Lsn) -> bool {
        if !self.has_covers.load(Ordering::Relaxed) {
            return false;
        }
        if !K::accepts(key) {
            return false;
        }
        read(&self.covers)
            .iter()
            .any(|cover| cover.lsn <= snapshot && cover.covers(key, lsn))
    }

    /// The entry for a key, a grave included, so absent and deleted are different
    ///
    /// A key with no entry may still be in a sealed footer; a key holding a grave
    /// was deleted and must not be looked for there.
    pub fn entry_or_grave(&self, key: &[u8]) -> Option<Entry> {
        if !K::accepts(key) {
            return None;
        }
        let at = self.shard_of_bytes(key);
        if !self.filters[at].may_hold(filter_hash(key)) {
            return None;
        }
        read(&self.shards[at]).map.at(key).copied()
    }

    /// The same for many keys at once, answered in the order asked
    ///
    /// The keys are named by their positions in the caller's own list rather than
    /// copied into one of this column's, so a batch borrows nothing here. They are
    /// grouped by shard, so a run is one lock take and one batched descent. What
    /// comes back is what the map holds, graves included, one answer per position.
    pub fn entry_many(&self, keys: &[RecordKey], run: &[usize], out: &mut Vec<Option<Entry>>) {
        out.clear();
        out.resize(run.len(), None);

        // A key this column cannot hold and a key its filter rules out are both
        // absences, and neither reaches a lock. The sort below moves the shard and
        // the position it was asked at, four bytes each, rather than the keys.
        let mut held = HeldGroups::take();
        let groups = &mut held.0;
        groups.wanted.clear();
        for (at, index) in run.iter().enumerate() {
            let key = keys[*index].as_slice();
            let shard = self.shard_of_bytes(key);
            if !self.filters[shard].may_hold(filter_hash(key)) {
                continue;
            }
            if !K::accepts(key) {
                continue;
            }
            groups.wanted.push((shard as u32, at as u32));
        }
        groups.wanted.sort_unstable_by_key(|(shard, _)| *shard);

        let mut at = 0;
        while at < groups.wanted.len() {
            let shard = groups.wanted[at].0 as usize;
            let mut end = at + 1;
            while end < groups.wanted.len() && groups.wanted[end].0 as usize == shard {
                end += 1;
            }

            // A short run is looked up under one lock rather than batched: below a
            // handful of keys the batch's own bookkeeping costs more than the
            // overlap buys. The lock is still taken once for the run.
            if end - at < BATCH_RUN {
                let state = read(&self.shards[shard]);
                for (_, slot) in &groups.wanted[at..end] {
                    let key = keys[run[*slot as usize]].as_slice();
                    out[*slot as usize] = state.map.at(key).copied();
                }
                at = end;
                continue;
            }

            // The run goes in the order it was asked: sorting whole keys to share a
            // descent costs more than it saves, and `at_many` needs no order. The two
            // lists here are bought per batched run: one is keyed by the column's own
            // key type and one borrows from the shard guard, so neither can be kept
            // by the thread the way `wanted` above is.
            // The filter hides the count from the iterator, so the room is asked for
            // outright rather than grown into a doubling at a time.
            let mut staged: Vec<K> = Vec::with_capacity(end - at);
            staged.extend(
                groups.wanted[at..end]
                    .iter()
                    .filter_map(|(_, slot)| K::from_slice(keys[run[*slot as usize]].as_slice())),
            );
            // Nothing can be dropped here, since `accepts` above is the same test
            // `from_slice` makes, and the zip below depends on it.
            debug_assert_eq!(
                staged.len(),
                end - at,
                "a key the column accepts failed to build"
            );
            let state = read(&self.shards[shard]);
            let mut found: Vec<Option<&Entry>> = Vec::with_capacity(staged.len());
            state.map.at_many(&staged, &mut found);
            for ((_, slot), entry) in groups.wanted[at..end].iter().zip(found.iter()) {
                out[*slot as usize] = entry.copied();
            }
            at = end;
        }
    }

    /// Whether a range delete already took a record this new, the guarded test
    ///
    /// The flag keeps a volume that never deletes a range off a lock per insert.
    fn is_covered(&self, key: &[u8], lsn: Lsn) -> bool {
        if !self.has_covers.load(Ordering::Relaxed) {
            return false;
        }
        read(&self.covers)
            .iter()
            .any(|cover| cover.covers(key, lsn))
    }

    /// Ranges the column is still testing inserts against
    pub fn cover_count(&self) -> u64 {
        read(&self.covers).len() as u64
    }

    /// Drop what tombstones are holding once nothing older can still be published
    ///
    /// The floor comes from the caller, since what a writer can have drawn but not
    /// published is bounded by the admission budget rather than by anything the
    /// index holds. A paged column waits on more: a grave is done once the tombstone
    /// that left it has a footer of its own, and a cover once no sealed segment
    /// holds keys inside it.
    pub fn prune_tombstones(&self, before: Lsn, sealed: Option<&SealedRanges>) -> u64 {
        let mut pruned = self.prune_covers(before, sealed);
        pruned += self.prune_graves(before, sealed);
        pruned
    }

    /// Drop the ranges nothing older than can still be published
    ///
    /// A cover the sweep has not finished is kept whatever the floor says: it is
    /// the only thing saying the records it covers are gone.
    fn prune_covers(&self, before: Lsn, sealed: Option<&SealedRanges>) -> u64 {
        if !self.has_covers.load(Ordering::Relaxed) {
            return 0;
        }
        let mut covers = write(&self.covers);
        let before_len = covers.len();
        covers.retain(|cover| {
            !matches!(cover.phase, SweepPhase::Done)
                || cover.lsn > before
                || sealed.is_some_and(|sealed| cover.reaches_sealed(sealed))
        });
        // The flag goes down only with the list held, so an insert reading it false
        // is already ordered after the emptying.
        if covers.is_empty() {
            self.has_covers.store(false, Ordering::Relaxed);
        }
        (before_len - covers.len()) as u64
    }

    fn prune_graves(&self, before: Lsn, sealed: Option<&SealedRanges>) -> u64 {
        // Taken once for the pass rather than per grave: the test below runs with a
        // shard held, and reaching for another lock from under one is an ordering
        // every other path would have to know about.
        let mut snapshot: Option<Vec<SegmentId>> = None;
        let occupied: Vec<usize> = read(&self.occupied)
            .iter()
            .map(|(at, _)| *at as usize)
            .collect();
        let mut pruned = 0u64;
        for at in occupied {
            let mut state = write(&self.shards[at]);
            if state.graves == 0 {
                continue;
            }
            // The floor understates at worst, so a shard whose graves are all newer
            // than the prune line is skipped without walking its map.
            if state.oldest_grave > before {
                continue;
            }
            let sealed: Option<&[SegmentId]> = match sealed {
                Some(sealed) => Some(snapshot.get_or_insert_with(|| sealed.segments())),
                None => None,
            };
            let mut doomed: Vec<K> = Vec::new();
            let mut oldest_left = Lsn(u64::MAX);
            for (key, entry) in state.map.walk() {
                if !entry.is_grave() {
                    continue;
                }
                let prunable = entry.lsn <= before
                    && sealed.is_none_or(|sealed| {
                        entry
                            .grave_origin()
                            .is_some_and(|from| sealed.binary_search(&from).is_ok())
                    });
                if prunable {
                    doomed.push(key.clone());
                } else if entry.lsn < oldest_left {
                    oldest_left = entry.lsn;
                }
            }
            // The walk saw every grave, so the floor comes back exact.
            state.oldest_grave = oldest_left;
            if doomed.is_empty() {
                continue;
            }
            for key in &doomed {
                state.map.take(key.as_slice());
            }
            state.graves -= doomed.len();
            pruned += doomed.len() as u64;
            // Deletion leaves room behind in a map that never merges, and this is
            // the pass that just made the most of it.
            state.map.pack_owed();
            self.note_emptied(at, &mut state);
        }
        pruned
    }

    /// Graves the column is holding, the memory a prune would give back
    pub fn grave_count(&self) -> u64 {
        self.sum_shards(|state| state.graves as u64)
    }

    /// Sum one number over every shard holding anything
    ///
    /// Not a snapshot: shards move while it steps them, so a total can miss a key
    /// that arrived after its shard was read.
    fn sum_shards(&self, of: impl Fn(&ShardState<K, S>) -> u64) -> u64 {
        let occupied: Vec<usize> = read(&self.occupied)
            .iter()
            .map(|(at, _)| *at as usize)
            .collect();
        occupied
            .into_iter()
            .map(|at| of(&read(&self.shards[at])))
            .sum()
    }

    /// Take a half-open range with one standing cover, sweeping nothing
    ///
    /// The end is exclusive, and no end runs to the top of the column. The drop is
    /// one push at any key count, and the records it covers are settled by the lazy
    /// sweep on the maintenance tick. A key written after the tombstone was drawn
    /// keeps its place, which is what makes a range delete and a concurrent put
    /// resolve the same way at runtime as on a rebuild.
    pub fn remove_range(&self, start: &[u8], end: Option<&[u8]>, lsn: Lsn) {
        self.push_cover(start, end.map(K::low_bound), lsn);
    }

    /// The cover the sweep should settle next, oldest first
    ///
    /// Oldest first is load-bearing: a later cover's release pass skips rows a
    /// finished cover already settled.
    pub fn next_pending_cover(&self) -> Option<PendingCover> {
        read(&self.covers)
            .iter()
            .filter(|cover| !matches!(cover.phase, SweepPhase::Done))
            .min_by_key(|cover| cover.lsn)
            .map(|cover| PendingCover {
                lsn: cover.lsn,
                end: cover.high.as_ref().map(|high| high.as_slice().to_vec()),
                release_from: match &cover.phase {
                    SweepPhase::Release(from) => Some(from.as_slice().to_vec()),
                    _ => None,
                },
            })
    }

    /// Move a cover's release pass forward, or hand it to the map sweep
    pub fn advance_release(&self, lsn: Lsn, resume: Option<&[u8]>) {
        let mut covers = write(&self.covers);
        let Some(cover) = covers.iter_mut().find(|cover| cover.lsn == lsn) else {
            return;
        };
        cover.phase = match resume {
            Some(resume) => SweepPhase::Release(K::low_bound(resume)),
            None => SweepPhase::Sweep(cover.low.clone()),
        };
    }

    /// Drop one bounded run of the map entries a cover has taken
    ///
    /// The budget counts keys examined rather than keys dropped, so a run of newer
    /// keys inside the range still moves the cursor. What comes back is what was
    /// dropped, how much budget went, and whether the cover's map half finished.
    pub fn sweep_run(
        &self,
        lsn: Lsn,
        budget: usize,
        segments: &SegmentTable,
    ) -> (u64, usize, bool) {
        let (resume, high) = {
            let covers = read(&self.covers);
            match covers.iter().find(|cover| cover.lsn == lsn) {
                Some(cover) => match &cover.phase {
                    SweepPhase::Sweep(from) => (from.clone(), cover.high.clone()),
                    _ => return (0, 0, true),
                },
                None => return (0, 0, true),
            }
        };

        let last = high
            .as_ref()
            .map(|key| self.shard_of(key))
            .unwrap_or(self.shards.len() - 1);
        let mut dropped = 0u64;
        let mut examined = 0usize;
        let mut stopped: Option<K> = None;
        for at in self.occupied_range(self.shard_of(&resume), last) {
            let mut state = write(&self.shards[at]);
            let state = &mut *state;
            let mut doomed: Vec<(K, Entry)> = Vec::new();
            // The trait takes its bounds borrowed, so the high end is held here for
            // as long as the walk it bounds.
            let ceiling = upper_bound(high.clone());
            let span_high = borrowed(&ceiling);
            for (key, entry) in state.map.span(Bound::Included(&resume), span_high) {
                if examined >= budget {
                    stopped = Some(key.clone());
                    break;
                }
                examined += 1;
                if entry.lsn >= lsn || entry.is_grave() {
                    continue;
                }
                doomed.push((key.clone(), *entry));
            }
            if !doomed.is_empty() {
                let mut per_segment: TBTreeMap<SegmentId, NODE_WIDTH, u64> = TBTreeMap::new();
                let mut freed = 0u64;
                for (key, entry) in &doomed {
                    *per_segment.get_or_insert(entry.loc.segment, 0) += entry.span(key.width());
                    freed += u64::from(entry.loc.len);
                    state.map.take(key.as_slice());
                    evict_carried(state, key);
                }
                for (segment, span) in per_segment.iter() {
                    segments.shadow(*segment, *span);
                }
                state.bytes = state.bytes.saturating_sub(freed);
                dropped += doomed.len() as u64;
                // Once for the run rather than once a key, since the whole run goes
                // under one take of the shard.
                state.map.pack_owed();
                self.note_emptied(at, state);
            }
            if stopped.is_some() {
                break;
            }
        }

        match stopped {
            Some(resume) => {
                let mut covers = write(&self.covers);
                if let Some(cover) = covers.iter_mut().find(|cover| cover.lsn == lsn) {
                    cover.phase = SweepPhase::Sweep(resume);
                }
                (dropped, examined, false)
            }
            None => {
                let mut covers = write(&self.covers);
                if let Some(cover) = covers.iter_mut().find(|cover| cover.lsn == lsn) {
                    cover.phase = SweepPhase::Done;
                }
                (dropped, examined, true)
            }
        }
    }

    /// Settle a footer-held record a standing cover has taken
    ///
    /// The release pass owns this settling: every other path declines the record as
    /// covered, which is what keeps it from being booked dead twice. A map entry
    /// older than the cover is itself the record's settling, left to the map sweep.
    pub fn release_covered(
        &self,
        key: &[u8],
        loc: Loc,
        below: Lsn,
        counted: bool,
        segments: &SegmentTable,
    ) -> bool {
        let Some(key) = K::from_slice(key) else {
            return false;
        };
        let at = self.shard_of(&key);
        let mut state = write(&self.shards[at]);
        if let Some(entry) = state.map.at(key.as_slice()) {
            if entry.lsn < below {
                return false;
            }
        }
        if !settle(&mut state, loc, segments, key.width(), counted) {
            return false;
        }
        self.note_emptied(at, &mut state);
        true
    }

    /// Whether a finished cover already settled everything at this key and version
    pub fn covered_by_swept(&self, key: &[u8], lsn: Lsn) -> bool {
        if !self.has_covers.load(Ordering::Relaxed) {
            return false;
        }
        if !K::accepts(key) {
            return false;
        }
        read(&self.covers)
            .iter()
            .any(|cover| matches!(cover.phase, SweepPhase::Done) && cover.covers(key, lsn))
    }

    /// Whether an unfinished cover reaches into this inclusive key range
    pub fn pending_overlaps(&self, low: &[u8], high: &[u8]) -> bool {
        if !self.has_covers.load(Ordering::Relaxed) {
            return false;
        }
        let low = K::low_bound(low);
        let high = K::high_bound(high);
        read(&self.covers).iter().any(|cover| {
            !matches!(cover.phase, SweepPhase::Done)
                && cover.low <= high
                && cover
                    .high
                    .as_ref()
                    .is_none_or(|end| end.as_slice() > low.as_slice())
        })
    }

    /// Whether any cover is still owed its sweep
    pub fn has_pending_covers(&self) -> bool {
        if !self.has_covers.load(Ordering::Relaxed) {
            return false;
        }
        read(&self.covers)
            .iter()
            .any(|cover| !matches!(cover.phase, SweepPhase::Done))
    }

    /// Resolve a key to its live entry
    ///
    /// A grave answers the same as no key at all, and so does an entry older than a
    /// spanning cover: the delete stands whether or not the sweep has been through.
    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        if !K::accepts(key) {
            return None;
        }
        let at = self.shard_of_bytes(key);
        if !self.filters[at].may_hold(filter_hash(key)) {
            return None;
        }
        let entry = read(&self.shards[at]).map.at(key).copied()?;
        if entry.is_grave() || self.is_covered(key, entry.lsn) {
            return None;
        }
        Some(entry)
    }

    /// Whether a key resolves to a live record
    pub fn contains(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Recorded payload length of a live key, served without a read
    pub fn size_of(&self, key: &[u8]) -> Option<ByteCount> {
        self.get(key)
            .map(|entry| ByteCount::from_bytes(u64::from(entry.loc.len)))
    }

    /// Repoint a key from a compacted record to its rewritten copy under a guard
    ///
    /// The copy carries the source record's sequence number, so the entry moves only
    /// while it still resolves that exact version. A raced repoint is declined and the
    /// copy is booked dead in its destination.
    pub fn repoint(
        &self,
        key: &[u8],
        new_loc: Loc,
        expected_lsn: Lsn,
        segments: &SegmentTable,
    ) -> bool {
        let key = match K::from_slice(key) {
            Some(key) => key,
            None => return false,
        };
        let at = self.shard_of(&key);
        let mut state = write(&self.shards[at]);
        match state.map.at(key.as_slice()).copied() {
            Some(existing) if existing.lsn == expected_lsn && !existing.is_grave() => {
                let span = existing.span(key.width());
                segments.release_live(existing.loc.segment, span);
                segments.mark_live(
                    new_loc.segment,
                    expected_lsn,
                    span_of(key.width(), new_loc.len),
                );
                self.filters[at].note(filter_hash(key.as_slice()));
                // The copy sits in an open tail, held live until the repoint is
                // published, so its stamp is issued here.
                state.map.put(
                    key,
                    existing.moved_to(new_loc, segments.live_incarnation(new_loc.segment)),
                );
                true
            }
            Some(_) | None => {
                segments.mark_dead(
                    new_loc.segment,
                    expected_lsn,
                    span_of(key.width(), new_loc.len),
                );
                false
            }
        }
    }

    /// Drop a key while it still resolves one exact location, writing no tombstone
    ///
    /// The bytes stay on disk as dead space until the segment is retired, and a
    /// version that overtook the named location keeps its place.
    pub fn evict_at(&self, key: &[u8], loc: Loc, segments: &SegmentTable) -> bool {
        let key = match K::from_slice(key) {
            Some(key) => key,
            None => return false,
        };
        let at = self.shard_of(&key);
        let mut state = write(&self.shards[at]);
        let existing = match state.map.at(key.as_slice()).copied() {
            Some(existing) => existing,
            None => return false,
        };
        if existing.loc != loc {
            return false;
        }

        self.drop_entry(&mut state, &key, existing, segments);
        state.map.pack_owed();
        self.note_emptied(at, &mut state);
        true
    }

    /// Count a key a rewrite moved into a footer row rather than into a record
    ///
    /// A key handed over at runtime was counted on its way out and stays counted; one
    /// a rebuild left sealed never was, so listing it in a row is where it joins the
    /// count.
    pub fn count_listed(&self, key: &[u8], bytes: u64) -> bool {
        let Some(key) = K::from_slice(key) else {
            return false;
        };
        let at = self.shard_of(&key);
        let mut state = write(&self.shards[at]);
        let was_empty = state.map.vacant() && state.paged == 0;
        state.paged += 1;
        state.bytes += bytes;
        // The shard has to be on the walk now: only occupied shards are summed, and
        // a paged count outside that set reads as the key having been deleted.
        self.note_filled(at, was_empty);
        true
    }

    /// Give a key up to the footer of the segment it landed in
    ///
    /// The record stays live and its bytes stay booked live: what changes is only
    /// who answers for it. Guarded by the location, so a key overwritten since the
    /// segment sealed is left alone. A covered entry is refused too, since handing
    /// it over would put it where the map sweep cannot reach and the release pass
    /// may already have been, so nothing would ever settle it.
    pub fn page_out(&self, key: &[u8], loc: Loc) -> bool {
        let Some(key) = K::from_slice(key) else {
            return false;
        };
        let at = self.shard_of(&key);
        let mut state = write(&self.shards[at]);
        match state.map.at(key.as_slice()) {
            Some(existing)
                if existing.loc == loc
                    && !existing.is_grave()
                    && !self.is_covered(key.as_slice(), existing.lsn) => {}
            Some(_) | None => return false,
        }
        state.map.take(key.as_slice());
        state.paged += 1;
        // A column that pages hands its whole resident half over a key at a time,
        // so this is the pass most able to leave a shard mostly room.
        state.map.pack_owed();
        self.note_emptied(at, &mut state);
        true
    }

    /// Install rebuilt entries in one pass, replacing whatever the column held
    ///
    /// A paged rebuild brings graves as well as records, and they are counted as
    /// graves here rather than left to look like keys. The segment table must already
    /// hold what the same rebuild resolved, since every entry takes its stamp from it
    /// on the way in.
    pub fn install(&self, entries: Vec<(KeyBytes, Entry)>, segments: &SegmentTable) {
        self.clear();
        let mut stamps: HashMap<SegmentId, SegmentIncarnation> = HashMap::new();
        // The entries arrive in key order and the shards cut the key space by
        // prefix, so each shard's keys are one contiguous run, gathered outside any
        // lock and taken as one sorted bulk load under one lock take.
        let mut run: Vec<(K, Entry)> = Vec::new();
        let mut run_shard = 0usize;
        for (key, entry) in entries {
            let key = match K::from_slice(key.as_slice()) {
                Some(key) => key,
                None => continue,
            };
            let stamp = *stamps
                .entry(entry.loc.segment)
                .or_insert_with(|| segments.incarnation_of(entry.loc.segment));
            let entry = entry.stamped(stamp);
            let at = self.shard_of(&key);
            if at != run_shard && !run.is_empty() {
                self.install_run(run_shard, std::mem::take(&mut run));
            }
            run_shard = at;
            run.push((key, entry));
        }
        if !run.is_empty() {
            self.install_run(run_shard, run);
        }
    }

    /// Land one shard's sorted run: counters, filter, and the map in one take
    fn install_run(&self, at: usize, run: Vec<(K, Entry)>) {
        let mut state = write(&self.shards[at]);
        let was_empty = state.map.vacant();
        for (key, entry) in &run {
            state.bytes += u64::from(entry.loc.len);
            if entry.is_grave() {
                state.note_grave(entry.lsn);
            }
            self.filters[at].note(filter_hash(key.as_slice()));
        }
        state.map.absorb_sorted(run);
        self.note_filled(at, was_empty);
    }

    /// Put back a range a rebuild resolved, without sweeping for what it covers
    ///
    /// The cover comes back unswept: a rebuild resolved every record it read against
    /// the range already. On a paged column the refusal is still the only thing
    /// between a covered key and a footer search that would find it.
    pub fn install_cover(&self, start: &[u8], end: Option<&KeyBytes>, lsn: Lsn) {
        self.push_cover(start, end.map(|end| K::low_bound(end.as_slice())), lsn);
    }

    /// Raise one cover, the shape a range delete and a rebuild share
    fn push_cover(&self, start: &[u8], high: Option<K>, lsn: Lsn) {
        let low = K::low_bound(start);
        let mut covers = write(&self.covers);
        covers.push(Covered {
            low: low.clone(),
            high,
            lsn,
            phase: SweepPhase::Release(low),
        });
        self.has_covers.store(true, Ordering::Relaxed);
    }

    /// Drop every key the column holds
    pub fn clear(&self) {
        let occupied: Vec<usize> = read(&self.occupied)
            .iter()
            .map(|(at, _)| *at as usize)
            .collect();
        for at in occupied {
            let mut state = write(&self.shards[at]);
            state.map.empty();
            state.bytes = 0;
            state.graves = 0;
            state.carried.empty();
            state.carried_bytes = 0;
            // What a rebuild installs is resident by definition, so a shard that had
            // handed keys over starts from nothing like any other.
            state.paged = 0;
        }
        write(&self.occupied).clear();
        // A resident rebuild resolves every record against every range itself, so
        // what it installs needs nothing held against it. A paged one reads no
        // sealed row, so it puts its ranges back after this.
        write(&self.covers).clear();
        self.has_covers.store(false, Ordering::Relaxed);
    }

    /// Live key count and payload byte total for the column
    ///
    /// Summed from the shards rather than kept in atomics, which would put two
    /// read-modify-writes on one shared cache line in every insert. Not a snapshot:
    /// a total can miss a key that arrived after its shard was read.
    pub fn totals(&self) -> Totals {
        let occupied: Vec<usize> = read(&self.occupied)
            .iter()
            .map(|(at, _)| *at as usize)
            .collect();
        let mut count = 0u64;
        let mut bytes = 0u64;
        for at in occupied {
            let state = read(&self.shards[at]);
            count += state.live_count();
            bytes += state.bytes;
        }
        Totals {
            count,
            bytes: ByteCount::from_bytes(bytes),
        }
    }

    /// Keys the shards are holding, graves included
    ///
    /// Counts places in the map rather than live keys: a grave takes a place and a
    /// paged key does not.
    pub fn resident_keys(&self) -> u64 {
        self.sum_shards(|state| state.map.count() as u64)
    }

    /// Share of neighbouring keys the shards cannot tell apart by their leads
    ///
    /// Weighted by keys rather than averaged over shards, since most shards of a
    /// two-byte column hold a handful. Nothing comes back from a shape with no leads
    /// or from an empty column: a rate over no keys is not zero, it is unasked. Walks
    /// every occupied shard's leaves, so it is a diagnostic rather than a counter.
    pub fn lead_tie_rate(&self) -> Option<f64> {
        let occupied: Vec<usize> = read(&self.occupied)
            .iter()
            .map(|(at, _)| *at as usize)
            .collect();
        let mut tied = 0.0f64;
        let mut keys = 0u64;
        for at in occupied {
            let state = read(&self.shards[at]);
            let held = state.map.count() as u64;
            // A shard of one key has no neighbouring pair to tie.
            let Some(rate) = state.map.lead_tie_rate().filter(|_| held > 1) else {
                continue;
            };
            tied += rate * (held - 1) as f64;
            keys += held - 1;
        }
        match keys {
            0 => None,
            _ => Some(tied / keys as f64),
        }
    }

    /// Live totals for one shard-aligned prefix, or nothing if it is not one
    ///
    /// A prefix naming exactly one shard is a total the shard already keeps, so the
    /// answer is a lock and two loads rather than a walk of its keys.
    pub fn prefix_totals(&self, prefix: &[u8]) -> Option<Totals> {
        if prefix.len() != self.shard_bytes as usize || self.shard_bytes == 0 {
            return None;
        }
        // An unswept cover over this shard means the counters still hold entries a
        // reader is already told are gone, so declining sends the caller to the
        // walk, which filters.
        if self.pending_overlaps(prefix, prefix) {
            return None;
        }
        let state = read(&self.shards[self.shard_of_bytes(prefix)]);
        Some(Totals {
            count: state.live_count(),
            bytes: ByteCount::from_bytes(state.bytes),
        })
    }

    /// Fill a page with one bounded run of live keys, ascending from a bound
    ///
    /// Each key's location goes into the page with it, so a playback stages its reads
    /// from what this already resolved. A bound shorter than a key is read as its
    /// zero-filled extension, the low end of the range the prefix names.
    pub fn page(&self, start: Bound<&[u8]>, limit: usize, out: &mut KeyPage) {
        out.clear();
        if limit == 0 {
            return;
        }
        let first = match start {
            Bound::Unbounded => 0,
            Bound::Included(key) | Bound::Excluded(key) => self.shard_of_bytes(key),
        };
        for at in self.occupied_range(first, self.shards.len() - 1) {
            // Between one shard and the next is where a page fill that holds no
            // publish barrier can be caught by a batch.
            crate::sync::rendezvous::at("index/page-shard");
            let bound = match at == first {
                true => low_bound::<K>(start),
                false => Bound::Unbounded,
            };
            // The trait takes its bounds borrowed, so the owned one is held here
            // for as long as the walk it opens.
            let low = borrowed(&bound);
            let state = read(&self.shards[at]);
            for (key, entry) in state
                .map
                .span(low, Bound::Unbounded)
                .filter(|(key, entry)| {
                    !entry.is_grave() && !self.is_covered(key.as_slice(), entry.lsn)
                })
            {
                let carried = match out.keeps_carried() {
                    true => self.page_carry(&state, key, entry),
                    false => None,
                };
                out.push_carried(key.as_slice(), *entry, carried);
                if out.len() >= limit {
                    return;
                }
            }
        }
    }

    /// Fill a page with one bounded run of live keys, descending from a bound
    pub fn page_back(&self, end: Bound<&[u8]>, limit: usize, out: &mut KeyPage) {
        out.clear();
        if limit == 0 {
            return;
        }
        let last = match end {
            Bound::Unbounded => self.shards.len() - 1,
            Bound::Included(key) | Bound::Excluded(key) => self.shard_of_bytes(key),
        };
        let mut shards = self.occupied_range(0, last);
        shards.reverse();
        for at in shards {
            let bound = match at == last {
                true => high_bound::<K>(end),
                false => Bound::Unbounded,
            };
            let high = borrowed(&bound);
            let state = read(&self.shards[at]);
            for (key, entry) in
                state
                    .map
                    .span_back(Bound::Unbounded, high)
                    .filter(|(key, entry)| {
                        !entry.is_grave() && !self.is_covered(key.as_slice(), entry.lsn)
                    })
            {
                let carried = match out.keeps_carried() {
                    true => self.page_carry(&state, key, entry),
                    false => None,
                };
                out.push_carried(key.as_slice(), *entry, carried);
                if out.len() >= limit {
                    return;
                }
            }
        }
    }

    /// Take one entry out of a shard, moving its bytes to dead and its totals down
    fn drop_entry(
        &self,
        state: &mut ShardState<K, S>,
        key: &K,
        existing: Entry,
        segments: &SegmentTable,
    ) {
        segments.shadow(existing.loc.segment, existing.span(key.width()));
        let len = u64::from(existing.loc.len);
        state.bytes = state.bytes.saturating_sub(len);
        state.map.take(key.as_slice());
        evict_carried(state, key);
    }

    /// The value a page serves for a key, cloned under the shard lock it holds
    fn page_carry(&self, state: &ShardState<K, S>, key: &K, entry: &Entry) -> Option<Arc<[u8]>> {
        if self.carry_max == 0 {
            return None;
        }
        match state.carried.at(key.as_slice())? {
            // A page serve takes the value without bumping its heat: a scan must
            // not renew a place it did not earn.
            Carried::Held {
                lsn: held, bytes, ..
            } if *held == entry.lsn => Some(Arc::clone(bytes)),
            _ => None,
        }
    }

    /// Record a shard that has just taken its first key
    ///
    /// The transition is already visible with the shard held, so no insert pays the
    /// walk set's shared lock to ask whether the shard was in it.
    fn note_filled(&self, at: usize, was_empty: bool) {
        if was_empty {
            write(&self.occupied).insert(at as u64, ());
        }
    }

    /// Record a shard whose last key has just left it
    ///
    /// A shard that gave its keys up to a footer stays on the walk with an empty
    /// map: dropping it there would take its paged count out of every total, which
    /// reads as the keys having been deleted rather than handed over.
    fn note_emptied(&self, at: usize, state: &mut ShardState<K, S>) {
        if state.map.vacant() && state.paged == 0 {
            // The room goes with the walk: nothing visits a shard outside the
            // occupied set, so a shard left here is one nothing will pack.
            state.map.release();
            write(&self.occupied).remove(&(at as u64));
        }
    }

    /// Occupied shards within an inclusive shard range, in key order
    fn occupied_range(&self, first: usize, last: usize) -> Vec<usize> {
        read(&self.occupied)
            .range(
                Bound::Included(&(first as u64)),
                Bound::Included(&(last as u64)),
            )
            .map(|(at, _)| *at as usize)
            .collect()
    }

    fn shard_of(&self, key: &K) -> usize {
        self.shard_of_bytes(key.as_slice())
    }

    fn shard_of_bytes(&self, key: &[u8]) -> usize {
        let mut shard = 0usize;
        for at in 0..self.shard_bytes as usize {
            let byte = key.get(at).copied().unwrap_or(0);
            shard = (shard << 8) | byte as usize;
        }
        shard
    }
}

/// Book a record a footer was answering for as gone, if anything answers for it
///
/// What the shard's numbers do turns on whether they ever held this record: a key
/// handed over at runtime was counted on its way out, and one a rebuild left sealed
/// never was, so a born row moves the segment's bytes to dead and nothing else.
fn settle<K: IndexKey, S: Shape<K>>(
    state: &mut ShardState<K, S>,
    loc: Loc,
    segments: &SegmentTable,
    key_width: u16,
    counted: bool,
) -> bool {
    if counted {
        if state.paged == 0 {
            return false;
        }
        state.paged -= 1;
        state.bytes = state.bytes.saturating_sub(u64::from(loc.len));
    }
    segments.shadow(loc.segment, span_of(key_width, loc.len));
    true
}

/// What a shard holds its keys in
///
/// Generic in the value as well as the key, because a shard holds two of these:
/// the entries, and the values a carrying column keeps beside them.
pub trait ShardMap<K: IndexKey, V: 'static>: Default {
    /// Put a value in, handing back the one it displaced
    fn put(&mut self, key: K, val: V) -> Option<V>;

    /// What is held for a key, borrowed rather than built
    fn at(&self, key: &[u8]) -> Option<&V>;

    /// Whether a key is held at all
    fn holds(&self, key: &[u8]) -> bool;

    /// Take a key out, handing back what it held
    fn take(&mut self, key: &[u8]) -> Option<V>;

    /// Keys held, graves included
    fn count(&self) -> usize;

    /// Whether the shard holds nothing
    fn vacant(&self) -> bool;

    /// Drop every key, keeping whatever room was already taken
    fn empty(&mut self);

    /// Every pair the shard holds, in key order
    fn walk(&self) -> impl Iterator<Item = (&K, &V)>;

    /// Every pair inside a span, in key order
    fn span<'a>(
        &'a self,
        low: Bound<&K>,
        high: Bound<&'a K>,
    ) -> impl Iterator<Item = (&'a K, &'a V)>;

    /// The same span walked from its high end down
    fn span_back<'a>(
        &'a self,
        low: Bound<&'a K>,
        high: Bound<&K>,
    ) -> impl Iterator<Item = (&'a K, &'a V)>;

    /// Many keys at once, answered in the order asked
    ///
    /// The default asks one at a time. A map with a batched descent takes the whole
    /// run, since a descent is a chain of dependent cache misses that nothing but
    /// other work in flight can shorten.
    fn at_many<'a>(&'a self, keys: &[K], out: &mut Vec<Option<&'a V>>) {
        out.clear();
        for key in keys {
            out.push(self.at(key.borrow()));
        }
    }

    /// Take a sorted run in one pass, the shape a rebuild hands over
    ///
    /// The default puts the pairs in one at a time; a map with a bulk path takes the
    /// run whole and builds at full fill. The run must be in key order, and repeats
    /// are allowed with the last one winning: the same run cannot land differently
    /// for having found the shard empty.
    fn absorb_sorted(&mut self, run: Vec<(K, V)>) {
        for (key, val) in run {
            self.put(key, val);
        }
    }

    /// Pack the map back up where deletion has left room worth taking back
    ///
    /// Cheap to ask: the shape that reclaims what a delete leaves does nothing here,
    /// and the shape that does not guards the pass on the room having doubled.
    fn pack_owed(&mut self) {}

    /// Give the map's room back, for a shard that is holding nothing at all
    ///
    /// `empty` keeps what was allocated, since a shard cleared by a group drop takes
    /// its next fill straight back. This is the other case: a shard drained to
    /// nothing leaves the ordered walk, so no pass visits it again to pack it.
    fn release(&mut self) {
        *self = Self::default();
    }

    /// Share of neighbouring keys this shard cannot tell apart by their leads
    ///
    /// A shard whose keys all share their first eight bytes falls out of the vector
    /// compare into a walk of full keys. It stays correct and says nothing, which is
    /// why this is asked rather than assumed. A map with no leads has no answer.
    fn lead_tie_rate(&self) -> Option<f64> {
        None
    }
}

/// The same shard for a column whose keys have no width, held in the same tree
///
/// The keys sit on the heap and the node holds pointers to them, so a shift moves
/// sixteen bytes a slot and the lead array stays inline and vectorised. An object
/// key is a bucket address and then a name, so a node tunes its lead window past
/// the bytes its own keys agree on.
impl<const B: usize, V: Default + 'static> ShardMap<Box<[u8]>, V> for TBTreeMap<Box<[u8]>, B, V> {
    fn put(&mut self, key: Box<[u8]>, val: V) -> Option<V> {
        self.insert(key, val)
    }

    fn at(&self, key: &[u8]) -> Option<&V> {
        self.get(key)
    }

    fn holds(&self, key: &[u8]) -> bool {
        self.contains_key(key)
    }

    fn take(&mut self, key: &[u8]) -> Option<V> {
        self.remove(key)
    }

    fn count(&self) -> usize {
        self.len()
    }

    fn vacant(&self) -> bool {
        self.is_empty()
    }

    fn empty(&mut self) {
        self.clear();
    }

    fn walk(&self) -> impl Iterator<Item = (&Box<[u8]>, &V)> {
        self.iter()
    }

    fn span<'a>(
        &'a self,
        low: Bound<&Box<[u8]>>,
        high: Bound<&'a Box<[u8]>>,
    ) -> impl Iterator<Item = (&'a Box<[u8]>, &'a V)> {
        self.range(low, high)
    }

    fn span_back<'a>(
        &'a self,
        low: Bound<&'a Box<[u8]>>,
        high: Bound<&Box<[u8]>>,
    ) -> impl Iterator<Item = (&'a Box<[u8]>, &'a V)> {
        self.range_back(low, high)
    }

    fn at_many<'a>(&'a self, keys: &[Box<[u8]>], out: &mut Vec<Option<&'a V>>) {
        self.get_many_sorted(keys, out);
    }

    fn absorb_sorted(&mut self, run: Vec<(Box<[u8]>, V)>) {
        match self.is_empty() {
            true => *self = TBTreeMap::from_sorted(run, B),
            false => {
                for (key, val) in run {
                    self.insert(key, val);
                }
            }
        }
    }

    fn pack_owed(&mut self) {
        if self.repack_owed() {
            self.repack(B);
        }
    }

    fn lead_tie_rate(&self) -> Option<f64> {
        Some(self.tie_rate())
    }
}

/// The same shard, held in the tree a declared width allows
///
/// The width is the const the key type is an array of, which is why this can only
/// exist for a fixed column: `Box<[u8]>` has nothing to hold inline and no lead to
/// take without chasing a pointer.
impl<const N: usize, const B: usize, V: Default + 'static> ShardMap<[u8; N], V>
    for TBTreeMap<[u8; N], B, V>
{
    fn put(&mut self, key: [u8; N], val: V) -> Option<V> {
        self.insert(key, val)
    }

    fn at(&self, key: &[u8]) -> Option<&V> {
        // A probe of the wrong width is a key this column cannot hold, which is
        // an absence rather than a fault.
        let key: &[u8; N] = key.try_into().ok()?;
        self.get(key)
    }

    fn holds(&self, key: &[u8]) -> bool {
        self.at(key).is_some()
    }

    fn take(&mut self, key: &[u8]) -> Option<V> {
        let key: &[u8; N] = key.try_into().ok()?;
        self.remove(key)
    }

    fn count(&self) -> usize {
        self.len()
    }

    fn vacant(&self) -> bool {
        self.is_empty()
    }

    fn empty(&mut self) {
        self.clear();
    }

    fn walk(&self) -> impl Iterator<Item = (&[u8; N], &V)> {
        self.iter()
    }

    fn span<'a>(
        &'a self,
        low: Bound<&[u8; N]>,
        high: Bound<&'a [u8; N]>,
    ) -> impl Iterator<Item = (&'a [u8; N], &'a V)> {
        self.range(low, high)
    }

    fn span_back<'a>(
        &'a self,
        low: Bound<&'a [u8; N]>,
        high: Bound<&[u8; N]>,
    ) -> impl Iterator<Item = (&'a [u8; N], &'a V)> {
        self.range_back(low, high)
    }

    fn at_many<'a>(&'a self, keys: &[[u8; N]], out: &mut Vec<Option<&'a V>>) {
        self.get_many_sorted(keys, out);
    }

    fn absorb_sorted(&mut self, run: Vec<([u8; N], V)>) {
        match self.is_empty() {
            true => *self = TBTreeMap::from_sorted(run, B),
            false => {
                for (key, val) in run {
                    self.insert(key, val);
                }
            }
        }
    }

    fn pack_owed(&mut self) {
        if self.repack_owed() {
            self.repack(B);
        }
    }

    fn lead_tie_rate(&self) -> Option<f64> {
        Some(self.tie_rate())
    }
}

/// The same shard held open-addressed instead of in a tree
///
/// Everything a point read does is here and everything an ordered read does is a
/// gather and a sort, which is the trade the column made when it declared the shape.
/// The cost is the shard rather than the run asked for: a page of ten keys off a
/// shard of a million gathers and sorts the million. Deletion shifts a chain back
/// over its hole rather than leaving a tombstone, so a search never steps over one.
impl<const N: usize, V: Default + 'static> ShardMap<[u8; N], V> for OpenTable<N, V> {
    fn put(&mut self, key: [u8; N], val: V) -> Option<V> {
        self.insert(key, val)
    }

    fn at(&self, key: &[u8]) -> Option<&V> {
        // A probe of the wrong width is a key this column cannot hold, which is
        // an absence rather than a fault.
        let key: &[u8; N] = key.try_into().ok()?;
        self.get(key)
    }

    fn holds(&self, key: &[u8]) -> bool {
        match key.try_into() {
            Ok(key) => self.contains_key(key),
            Err(_) => false,
        }
    }

    fn take(&mut self, key: &[u8]) -> Option<V> {
        let key: &[u8; N] = key.try_into().ok()?;
        self.remove(key)
    }

    fn count(&self) -> usize {
        self.len()
    }

    fn vacant(&self) -> bool {
        self.is_empty()
    }

    fn empty(&mut self) {
        self.clear();
    }

    fn walk(&self) -> impl Iterator<Item = (&[u8; N], &V)> {
        self.sorted().into_iter()
    }

    fn span<'a>(
        &'a self,
        low: Bound<&[u8; N]>,
        high: Bound<&'a [u8; N]>,
    ) -> impl Iterator<Item = (&'a [u8; N], &'a V)> {
        self.sorted_span(low, high).into_iter()
    }

    fn span_back<'a>(
        &'a self,
        low: Bound<&'a [u8; N]>,
        high: Bound<&[u8; N]>,
    ) -> impl Iterator<Item = (&'a [u8; N], &'a V)> {
        self.sorted_span(low, high).into_iter().rev()
    }

    fn absorb_sorted(&mut self, run: Vec<([u8; N], V)>) {
        self.absorb(run);
    }

    fn pack_owed(&mut self) {
        self.pack();
    }
}

/// Which pair of maps a column's shards are built from
///
/// Per column rather than once for the whole index, because a column's key decides
/// its node: a declared width holds its keys inline, and a name holds a pointer.
pub trait Shape<K: IndexKey> {
    /// Where the shard's entries live
    type Entries: ShardMap<K, Entry>;

    /// Where the values a carrying column keeps beside them live
    type Carried: ShardMap<K, Carried>;

    /// What a column's shards actually took, not always what the column asked for
    const SHAPE: MapShape;

    /// Bytes a resident key costs beyond its own bytes and its entry
    ///
    /// A gauge for a budget to act on rather than a measurement: the tree's number
    /// was weighed and the open shard's is arithmetic on its slot.
    const OVERHEAD_PER_KEY: u64;
}

/// Bytes a resident key costs the tree beyond itself and its entry
///
/// A b-tree holds its keys in nodes with a header and slots it has not filled, so
/// a key costs more than the key. A gauge rather than a measurement.
const NODE_BYTES_PER_KEY: u64 = 37;

/// The shape a declared width allows, and what every fixed column takes
pub struct Trees<const N: usize>;

/// Give each declared key width the node width `node_width` sizes for it
///
/// One impl a width rather than one over every width, since a const parameter
/// cannot be arithmetic on another one without `generic_const_exprs`. A width that
/// is not listed fails to compile rather than falling back to a default.
macro_rules! tree_shapes {
    ($($width:literal),* $(,)?) => {
        $(
            impl Shape<[u8; $width]> for Trees<$width> {
                type Entries = TBTreeMap<[u8; $width], { node_width($width) }, Entry>;
                type Carried = TBTreeMap<[u8; $width], { node_width($width) }, Carried>;
                const SHAPE: MapShape = MapShape::Tree;
                const OVERHEAD_PER_KEY: u64 = NODE_BYTES_PER_KEY;
            }
        )*
    };
}

tree_shapes!(0, 2, 8, 12, 16, 20, 24, 32, 34, 36, 40, 44, 48, 72, 96, 108);

/// Keys a node holds on a column whose keys have no declared width
///
/// The budget the fixed arms take is a count of key bytes, since a node there holds
/// whole keys and an insert shifts them. A node holding names shifts pointers
/// instead, sixteen bytes a slot whatever the name weighs, so the budget has nothing
/// to divide and this width is chosen rather than derived.
pub const VAR_NODE_WIDTH: usize = 32;

/// The shape a column whose keys have no width takes
pub struct VarTrees;

impl Shape<Box<[u8]>> for VarTrees {
    type Entries = TBTreeMap<Box<[u8]>, VAR_NODE_WIDTH, Entry>;
    type Carried = TBTreeMap<Box<[u8]>, VAR_NODE_WIDTH, Carried>;
    const SHAPE: MapShape = MapShape::Tree;
    const OVERHEAD_PER_KEY: u64 = NODE_BYTES_PER_KEY;
}

/// The shape a column takes when it asks for an open-addressed shard
///
/// One impl over every width, since nothing here is arithmetic on the width: a slot
/// is the key and the value laid down next to each other. Which widths a column may
/// declare it at is `ColumnIndex`'s to say.
pub struct OpenTables<const N: usize>;

impl<const N: usize> Shape<[u8; N]> for OpenTables<N> {
    type Entries = OpenTable<N, Entry>;
    type Carried = OpenTable<N, Carried>;
    const SHAPE: MapShape = MapShape::Open;
    const OVERHEAD_PER_KEY: u64 = overhead_per_key(N as u64, std::mem::size_of::<Entry>() as u64);
}

/// A key as the resident map holds it
///
/// A fixed column instantiates it at `K` and keeps its keys inline; a variable
/// column instantiates it at `Box<[u8]>` and pays a pointer and an allocation per
/// key. Both borrow as their bytes and order the same borrowed as owned, so a lookup
/// takes a plain `&[u8]` and touches the heap only for a key worth keeping.
pub trait IndexKey: Ord + Clone + Send + Sync + Borrow<[u8]> + 'static {
    /// The key these bytes make, or nothing when the column cannot hold them
    fn from_slice(bytes: &[u8]) -> Option<Self>
    where
        Self: Sized;

    /// Whether these bytes are a key this column could hold, without building one
    fn accepts(bytes: &[u8]) -> bool;

    /// A bound extended low, which is what a prefix seeks from
    fn low_bound(bytes: &[u8]) -> Self
    where
        Self: Sized;

    /// A bound extended high, which is what a prefix seeks to
    fn high_bound(bytes: &[u8]) -> Self
    where
        Self: Sized;

    /// The bytes themselves
    fn as_slice(&self) -> &[u8];

    /// Bytes this key occupies, which a record's span is measured with
    fn width(&self) -> u16 {
        self.as_slice().len() as u16
    }
}

impl<const N: usize> IndexKey for [u8; N] {
    fn from_slice(bytes: &[u8]) -> Option<[u8; N]> {
        match bytes.len() == N {
            true => {
                let mut out = [0u8; N];
                out.copy_from_slice(bytes);
                Some(out)
            }
            false => None,
        }
    }

    fn accepts(bytes: &[u8]) -> bool {
        bytes.len() == N
    }

    fn low_bound(bytes: &[u8]) -> [u8; N] {
        let mut out = [0u8; N];
        let take = bytes.len().min(N);
        out[..take].copy_from_slice(&bytes[..take]);
        out
    }

    fn high_bound(bytes: &[u8]) -> [u8; N] {
        let mut out = [0xffu8; N];
        let take = bytes.len().min(N);
        out[..take].copy_from_slice(&bytes[..take]);
        out
    }

    fn width(&self) -> u16 {
        N as u16
    }

    fn as_slice(&self) -> &[u8] {
        self
    }
}

impl IndexKey for Box<[u8]> {
    /// Every length is this column's length, which is what variable means
    fn from_slice(bytes: &[u8]) -> Option<Box<[u8]>> {
        Some(Box::from(bytes))
    }

    fn accepts(_bytes: &[u8]) -> bool {
        true
    }

    /// A prefix is already the low end of everything that begins with it.
    fn low_bound(bytes: &[u8]) -> Box<[u8]> {
        Box::from(bytes)
    }

    /// The high end needs a byte no stored key can carry past the prefix, and
    /// `MAX_KEY_LEN` bytes of 0xFF is above every key the format admits.
    fn high_bound(bytes: &[u8]) -> Box<[u8]> {
        let mut out = Vec::with_capacity(MAX_KEY_LEN);
        out.extend_from_slice(bytes);
        out.resize(MAX_KEY_LEN, 0xFF);
        out.into_boxed_slice()
    }

    fn as_slice(&self) -> &[u8] {
        self
    }
}

fn low_bound<K: IndexKey>(bound: Bound<&[u8]>) -> Bound<K> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(key) => Bound::Included(K::low_bound(key)),
        Bound::Excluded(key) => Bound::Excluded(K::low_bound(key)),
    }
}

fn high_bound<K: IndexKey>(bound: Bound<&[u8]>) -> Bound<K> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(key) => Bound::Included(K::high_bound(key)),
        Bound::Excluded(key) => Bound::Excluded(K::low_bound(key)),
    }
}

/// One owned bound, borrowed for the walk it opens
fn borrowed<K>(bound: &Bound<K>) -> Bound<&K> {
    match bound {
        Bound::Included(key) => Bound::Included(key),
        Bound::Excluded(key) => Bound::Excluded(key),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn upper_bound<K: IndexKey>(end: Option<K>) -> Bound<K> {
    match end {
        Some(key) => Bound::Excluded(key),
        None => Bound::Unbounded,
    }
}

fn resize(current: u64, old_len: u64, new_len: u64) -> u64 {
    if new_len >= old_len {
        current + (new_len - old_len)
    } else {
        current.saturating_sub(old_len - new_len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::format::column::{Codec, ColumnId, KeyWidth};
    use crate::format::loc::SegmentId;

    const VARIABLE: ColumnSpec = ColumnSpec {
        id: ColumnId(3),
        name: "object_list",
        key_width: KeyWidth::Variable,
        shard_bytes: 1,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    };

    // a variable column opens, where before it was refused outright
    #[test]
    fn a_variable_column_opens() {
        let index = ColumnIndex::new(&VARIABLE, ShardShapes::Tree)
            .expect("a variable column has an index now");
        assert!(matches!(index, ColumnIndex::Var(_)));
        assert_eq!(index.key_width(), 0, "a variable column declares no width");
    }

    // keys of different lengths live in one column and answer for themselves
    #[test]
    fn a_variable_column_holds_every_length() {
        let index = ColumnIndex::new(&VARIABLE, ShardShapes::Tree).expect("index");
        let segments = SegmentTable::new();

        let names: Vec<Vec<u8>> = [
            b"a".as_slice(),
            b"photos/",
            b"photos/2026/cat.jpg",
            b"tenants/0f/exports/2026/08/02/part-00001.parquet",
        ]
        .iter()
        .map(|name| name.to_vec())
        .chain(std::iter::once(vec![b'L'; 900]))
        .collect();

        for (at, name) in names.iter().enumerate() {
            let landed = index.insert(
                name,
                Entry::new(loc(1, at as u32 * 100, 50), Lsn(at as u64 + 1)),
                &segments,
                None,
            );
            assert!(landed.took_place(), "insert {at}");
        }

        for (at, name) in names.iter().enumerate() {
            let entry = index
                .get(name)
                .unwrap_or_else(|| panic!("key {at} went missing"));
            assert_eq!(
                entry.lsn,
                Lsn(at as u64 + 1),
                "key {at} resolved to the wrong version"
            );
        }
        assert!(index.get(b"nothing-wrote-this").is_none());
        assert_eq!(index.totals().count, names.len() as u64);
    }

    // a shorter key sorts before what extends it, which listing depends on
    #[test]
    fn a_variable_column_orders_by_bytes() {
        let index = ColumnIndex::new(&VARIABLE, ShardShapes::Tree).expect("index");
        let segments = SegmentTable::new();

        let mut names: Vec<Vec<u8>> = vec![
            b"photos/b".to_vec(),
            b"photos".to_vec(),
            b"photos/".to_vec(),
            b"photos/a".to_vec(),
            b"photosx".to_vec(),
        ];
        for (at, name) in names.iter().enumerate() {
            index.insert(
                name,
                Entry::new(loc(1, at as u32, 10), Lsn(1)),
                &segments,
                None,
            );
        }
        names.sort();

        let mut page = KeyPage::default();
        index.page(Bound::Unbounded, 64, &mut page);
        assert_eq!(keys_in(&page), names);
    }

    // an overwrite replaces the key rather than adding a second one
    #[test]
    fn a_variable_key_overwrites_in_place() {
        let index = ColumnIndex::new(&VARIABLE, ShardShapes::Tree).expect("index");
        let segments = SegmentTable::new();
        let name = b"photos/2026/cat.jpg".as_slice();

        index.insert(name, Entry::new(loc(1, 0, 10), Lsn(1)), &segments, None);
        index.insert(name, Entry::new(loc(1, 40, 10), Lsn(2)), &segments, None);

        assert_eq!(index.totals().count, 1);
        assert_eq!(index.get(name).expect("present").lsn, Lsn(2));
    }

    const SHARDED: ColumnSpec = ColumnSpec {
        id: ColumnId(1),
        name: "record",
        key_width: KeyWidth::Fixed(34),
        shard_bytes: 2,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    };

    const FLAT: ColumnSpec = ColumnSpec {
        id: ColumnId(2),
        name: "blob_data",
        key_width: KeyWidth::Fixed(32),
        shard_bytes: 0,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    };

    fn sharded() -> WidthIndex<[u8; 34], Trees<34>> {
        WidthIndex::new(&SHARDED)
    }

    fn key(group: u16, byte: u8) -> Vec<u8> {
        let mut out = group.to_be_bytes().to_vec();
        out.extend_from_slice(&[byte; 32]);
        out
    }

    fn loc(segment: u32, offset: u32, len: u32) -> Loc {
        Loc::new(SegmentId(segment), offset, len)
    }

    fn keys_in(out: &KeyPage) -> Vec<Vec<u8>> {
        (0..out.len()).map(|at| out.key_at(at)).collect()
    }

    /// Run the lazy sweep to completion, the way the maintenance tick would
    ///
    /// Nothing here pages, so the release phase is stepped straight past.
    fn sweep_all<K: IndexKey, S: Shape<K>>(index: &WidthIndex<K, S>, segments: &SegmentTable) {
        while let Some(pending) = index.next_pending_cover() {
            if pending.release_from.is_some() {
                index.advance_release(pending.lsn, None);
                continue;
            }
            let (_, _, done) = index.sweep_run(pending.lsn, usize::MAX, segments);
            assert!(done, "an unbounded run finishes its cover");
        }
    }

    // the batched resolve answers what the same keys answer one at a time
    #[test]
    fn a_batch_answers_where_the_keys_were_asked() {
        let index = sharded();
        let segments = SegmentTable::new();
        for group in [7u16, 1, 40, 1, 7] {
            for byte in [9u8, 2, 200, 71] {
                index.insert(
                    &key(group, byte),
                    Entry::new(loc(1, u32::from(byte), 10), Lsn(u64::from(byte))),
                    &segments,
                    None,
                );
            }
        }
        index.remove(&key(7, 2), Lsn(500), loc(2, 0, 0), &segments);

        let asked: Vec<Vec<u8>> = vec![
            key(40, 200),
            key(1, 9),
            key(7, 2),
            key(3, 9),
            key(7, 200),
            key(1, 2),
            key(40, 200),
            key(1, 255),
        ];
        let borrowed: Vec<RecordKey> = asked
            .iter()
            .map(|key| RecordKey::from_bytes(ColumnId(1), key).expect("a key"))
            .collect();
        let run: Vec<usize> = (0..borrowed.len()).collect();
        let mut batched: Vec<Option<Entry>> = Vec::new();
        index.entry_many(&borrowed, &run, &mut batched);

        let looped: Vec<Option<Entry>> = borrowed
            .iter()
            .map(|key| index.entry_or_grave(key.as_slice()))
            .collect();
        assert_eq!(batched, looped, "the batch and the loop disagree");
        assert!(batched[2].expect("the grave is an entry").is_grave());
        assert!(
            batched[3].is_none(),
            "a key in an empty shard is an absence"
        );
        assert!(
            batched[7].is_none(),
            "a key its shard never held is an absence"
        );
    }

    // a column refuses a key width no index holds
    #[test]
    fn unknown_width_refused() {
        let odd = ColumnSpec {
            id: ColumnId(9),
            name: "odd",
            key_width: KeyWidth::Fixed(33),
            shard_bytes: 0,
            inline_max: 0,
            row_carry: 0,
            purge_mark: None,
            codec: Codec::None,
            map_shape: MapShape::Tree,
        };

        assert!(ColumnIndex::new(&odd, ShardShapes::Tree).is_err());
        assert!(ColumnIndex::new(&SHARDED, ShardShapes::Tree).is_ok());
        assert_eq!(
            ColumnIndex::new(&FLAT, ShardShapes::Tree)
                .expect("flat")
                .key_width(),
            32
        );
    }

    // an open shard is taken only at the widths there is an arm for
    #[test]
    fn open_arms_are_declared() {
        let open = |width: u16| ColumnSpec {
            key_width: KeyWidth::Fixed(width),
            map_shape: MapShape::Open,
            ..FLAT
        };

        // The slack a slot holds open grows with the slot, so each width owes its own.
        for (width, overhead) in [(32u16, 9u64), (34, 9), (72, 14), (108, 20)] {
            let index = ColumnIndex::new(&open(width), ShardShapes::Declared).expect("an open arm");
            assert_eq!(index.key_width(), width);
            assert_eq!(index.map_shape(), MapShape::Open, "{width} byte keys");
            assert_eq!(index.overhead_per_key(), overhead, "{width} byte keys");
        }
        for width in [8u16, 16, 48] {
            assert!(
                ColumnIndex::new(&open(width), ShardShapes::Declared).is_err(),
                "{width}"
            );
        }
        assert!(ColumnIndex::new(&VARIABLE, ShardShapes::Tree).is_ok());
        assert!(
            ColumnIndex::new(
                &ColumnSpec {
                    map_shape: MapShape::Open,
                    ..VARIABLE
                },
                ShardShapes::Declared,
            )
            .is_err(),
            "a column with no declared width has no slot to size",
        );
    }

    // small widths hold
    #[test]
    fn small_widths_hold() {
        for width in [8u8, 12, 16, 40, 44, 48, 72, 108] {
            let spec = ColumnSpec {
                id: ColumnId(1),
                name: "small",
                key_width: KeyWidth::Fixed(width as u16),
                shard_bytes: 0,
                inline_max: 0,
                row_carry: 0,
                purge_mark: None,
                codec: Codec::None,
                map_shape: MapShape::Tree,
            };

            let index = ColumnIndex::new(&spec, ShardShapes::Tree).expect("a small width declared");
            assert_eq!(index.key_width(), u16::from(width));
        }
    }

    // the shard filter says absent only when absent is certain
    #[test]
    fn the_filter_never_invents_an_absence() {
        let index = sharded();
        let segments = SegmentTable::new();

        index.insert(
            &key(1, 1),
            Entry::new(loc(1, 0, 400), Lsn(1)),
            &segments,
            None,
        );
        assert!(
            index.get(&key(1, 1)).is_some(),
            "a present key survives the filter"
        );
        assert!(index.get(&key(1, 2)).is_none(), "same shard, absent key");
        assert!(
            index.get(&key(9, 1)).is_none(),
            "a shard that never held a key"
        );

        // A grave must pass the filter, since deleted and absent answer alike from
        // get but differently from entry_or_grave.
        index.remove(&key(1, 1), Lsn(2), loc(2, 0, 20), &segments);
        assert!(index.get(&key(1, 1)).is_none());
        assert!(
            index
                .entry_or_grave(&key(1, 1))
                .is_some_and(|entry| entry.is_grave()),
            "the grave is still visible past the filter",
        );
    }

    // a fresh key inserts and resolves back to its location and version
    #[test]
    fn insert_and_get() {
        let index = sharded();
        let segments = SegmentTable::new();

        assert!(index
            .insert(
                &key(1, 1),
                Entry::new(loc(1, 0, 400), Lsn(1)),
                &segments,
                None
            )
            .took_place());

        let entry = index.get(&key(1, 1)).expect("present");
        assert_eq!(entry.loc, loc(1, 0, 400));
        assert_eq!(entry.lsn, Lsn(1));
        assert_eq!(index.totals().count, 1);
        assert_eq!(index.totals().bytes, ByteCount::from_bytes(400));
    }

    // a key of the wrong width is refused rather than padded into the column
    #[test]
    fn wrong_width_refused() {
        let index = sharded();
        let segments = SegmentTable::new();

        assert_eq!(
            index.insert(
                &[0u8; 20],
                Entry::new(loc(1, 0, 400), Lsn(1)),
                &segments,
                None
            ),
            Landed::Newer
        );
        assert!(index.get(&[0u8; 20]).is_none());
        assert_eq!(index.totals().count, 0);
    }

    // an overwrite moves the shadowed bytes to dead and keeps the totals exact
    #[test]
    fn overwrite_moves_dead() {
        let index = sharded();
        let segments = SegmentTable::new();
        index
            .insert(
                &key(1, 1),
                Entry::new(loc(1, 0, 400), Lsn(1)),
                &segments,
                None,
            )
            .took_place();

        index
            .insert(
                &key(1, 1),
                Entry::new(loc(2, 0, 900), Lsn(2)),
                &segments,
                None,
            )
            .took_place();

        assert_eq!(segments.bytes_of(SegmentId(1)).live, 0);
        assert_eq!(segments.bytes_of(SegmentId(1)).dead, span_of(34, 400));
        assert_eq!(segments.bytes_of(SegmentId(2)).live, span_of(34, 900));
        assert_eq!(index.totals().count, 1);
        assert_eq!(index.totals().bytes, ByteCount::from_bytes(900));
    }

    // a stale put is ignored and its own bytes are booked dead on arrival
    #[test]
    fn stale_put_ignored() {
        let index = sharded();
        let segments = SegmentTable::new();
        index
            .insert(
                &key(1, 1),
                Entry::new(loc(2, 0, 900), Lsn(5)),
                &segments,
                None,
            )
            .took_place();

        let applied = index
            .insert(
                &key(1, 1),
                Entry::new(loc(1, 0, 400), Lsn(3)),
                &segments,
                None,
            )
            .took_place();

        assert!(!applied);
        assert_eq!(index.get(&key(1, 1)).expect("present").lsn, Lsn(5));
        assert_eq!(segments.bytes_of(SegmentId(1)).dead, span_of(34, 400));
        assert_eq!(index.totals().bytes, ByteCount::from_bytes(900));
    }

    // a delete removes the key, moves its bytes to dead, and drops the totals
    #[test]
    fn delete_moves_dead() {
        let index = sharded();
        let segments = SegmentTable::new();
        index
            .insert(
                &key(1, 1),
                Entry::new(loc(1, 0, 400), Lsn(1)),
                &segments,
                None,
            )
            .took_place();

        assert_eq!(
            index.remove(&key(1, 1), Lsn(2), loc(1, 0, 0), &segments),
            Landed::Record
        );

        assert!(!index.contains(&key(1, 1)));
        assert_eq!(segments.bytes_of(SegmentId(1)).dead, span_of(34, 400));
        assert_eq!(index.totals().count, 0);
    }

    // a stale tombstone leaves the newer live entry in place
    #[test]
    fn stale_tombstone_ignored() {
        let index = sharded();
        let segments = SegmentTable::new();
        index
            .insert(
                &key(1, 1),
                Entry::new(loc(1, 0, 400), Lsn(5)),
                &segments,
                None,
            )
            .took_place();

        assert_eq!(
            index.remove(&key(1, 1), Lsn(3), loc(1, 0, 0), &segments),
            Landed::Newer
        );

        assert!(index.contains(&key(1, 1)));
        assert_eq!(index.totals().count, 1);
    }

    // a shard-aligned prefix has its live totals without walking its keys
    #[test]
    fn prefix_totals_from_the_shard() {
        let index = sharded();
        let segments = SegmentTable::new();
        index
            .insert(
                &key(7, 1),
                Entry::new(loc(1, 0, 400), Lsn(1)),
                &segments,
                None,
            )
            .took_place();
        index
            .insert(
                &key(7, 2),
                Entry::new(loc(1, 0, 600), Lsn(2)),
                &segments,
                None,
            )
            .took_place();
        index
            .insert(
                &key(8, 1),
                Entry::new(loc(1, 0, 100), Lsn(3)),
                &segments,
                None,
            )
            .took_place();

        let seven = index.prefix_totals(&7u16.to_be_bytes()).expect("prefix");

        assert_eq!(seven.count, 2);
        assert_eq!(seven.bytes, ByteCount::from_bytes(1000));
        assert_eq!(index.totals().count, 3);
        assert!(index.prefix_totals(&[0x00]).is_none());
    }

    // a flat column keeps one shard and answers no prefix totals at all
    #[test]
    fn flat_column_has_one_shard() {
        let index: WidthIndex<[u8; 32], Trees<32>> = WidthIndex::new(&FLAT);
        let segments = SegmentTable::new();
        index
            .insert(
                &[0x11; 32],
                Entry::new(loc(1, 0, 400), Lsn(1)),
                &segments,
                None,
            )
            .took_place();

        assert_eq!(index.totals().count, 1);
        assert!(index.prefix_totals(&[]).is_none());
    }

    // a page walks the occupied shards in key order and stops at its limit
    #[test]
    fn page_walks_in_order() {
        let index = sharded();
        let segments = SegmentTable::new();
        for (group, byte) in [(9u16, 2u8), (1, 1), (9, 1), (300, 1)] {
            index
                .insert(
                    &key(group, byte),
                    Entry::new(loc(1, 0, 100), Lsn(1)),
                    &segments,
                    None,
                )
                .took_place();
        }

        let mut out = KeyPage::default();
        index.page(Bound::Unbounded, 3, &mut out);

        assert_eq!(keys_in(&out), vec![key(1, 1), key(9, 1), key(9, 2)],);

        index.page(Bound::Included(&key(9, 2)), 8, &mut out);
        assert_eq!(keys_in(&out), vec![key(9, 2), key(300, 1)]);

        index.page(Bound::Excluded(&key(9, 2)), 8, &mut out);
        assert_eq!(keys_in(&out), vec![key(300, 1)]);
    }

    // a descending page walks back from its bound
    #[test]
    fn page_back_walks_in_reverse() {
        let index = sharded();
        let segments = SegmentTable::new();
        for group in [1u16, 9, 300] {
            index
                .insert(
                    &key(group, 1),
                    Entry::new(loc(1, 0, 100), Lsn(1)),
                    &segments,
                    None,
                )
                .took_place();
        }

        let mut out = KeyPage::default();
        index.page_back(Bound::Unbounded, 2, &mut out);

        assert_eq!(keys_in(&out), vec![key(300, 1), key(9, 1)]);
    }

    // a range delete answers gone everywhere before its sweep has run at all
    #[test]
    fn range_delete_covers_its_range() {
        let index = sharded();
        let segments = SegmentTable::new();
        for group in [41u16, 42, 42, 43] {
            index
                .insert(
                    &key(group, group as u8),
                    Entry::new(loc(1, 0, 100), Lsn(1)),
                    &segments,
                    None,
                )
                .took_place();
        }
        index
            .insert(
                &key(42, 0xaa),
                Entry::new(loc(1, 0, 100), Lsn(2)),
                &segments,
                None,
            )
            .took_place();

        index.remove_range(&42u16.to_be_bytes(), Some(&43u16.to_be_bytes()), Lsn(9));

        assert!(index.contains(&key(41, 41)));
        assert!(index.contains(&key(43, 43)));
        assert!(
            !index.contains(&key(42, 42)),
            "the standing cover answers before the sweep"
        );
        assert!(!index.contains(&key(42, 0xaa)));
        let mut out = KeyPage::default();
        index.page(Bound::Unbounded, 8, &mut out);
        assert_eq!(keys_in(&out), vec![key(41, 41), key(43, 43)]);

        // Three spans are dead: the overwrite of the doubled key booked one on the
        // way in, and the sweep settled the two live covered records.
        sweep_all(&index, &segments);
        assert_eq!(index.totals().count, 2);
        assert_eq!(segments.bytes_of(SegmentId(1)).dead, 3 * span_of(34, 100));
        assert!(!index.contains(&key(42, 42)));
    }

    // a key written after the range tombstone was drawn keeps its place
    #[test]
    fn range_delete_spares_newer_keys() {
        let index = sharded();
        let segments = SegmentTable::new();
        index
            .insert(
                &key(42, 1),
                Entry::new(loc(1, 0, 100), Lsn(1)),
                &segments,
                None,
            )
            .took_place();
        index
            .insert(
                &key(42, 2),
                Entry::new(loc(1, 0, 100), Lsn(20)),
                &segments,
                None,
            )
            .took_place();

        index.remove_range(&42u16.to_be_bytes(), Some(&43u16.to_be_bytes()), Lsn(9));

        assert!(!index.contains(&key(42, 1)));
        assert!(index.contains(&key(42, 2)));
        sweep_all(&index, &segments);
        assert!(index.contains(&key(42, 2)));
        assert_eq!(index.totals().count, 1);
    }

    // a range delete with no upper bound runs to the top of the column
    #[test]
    fn unbounded_range_delete() {
        let index = sharded();
        let segments = SegmentTable::new();
        for group in [1u16, 900, 65535] {
            index
                .insert(
                    &key(group, 1),
                    Entry::new(loc(1, 0, 100), Lsn(1)),
                    &segments,
                    None,
                )
                .took_place();
        }

        index.remove_range(&900u16.to_be_bytes(), None, Lsn(9));

        assert!(index.contains(&key(1, 1)));
        assert!(!index.contains(&key(900, 1)));
        assert!(!index.contains(&key(65535, 1)));
        sweep_all(&index, &segments);
        assert_eq!(index.totals().count, 1);
    }

    // installing rebuilt entries sets keys and totals at once
    #[test]
    fn install_replaces() {
        let index = sharded();
        let segments = SegmentTable::new();
        index
            .insert(
                &key(5, 5),
                Entry::new(loc(1, 0, 50), Lsn(1)),
                &segments,
                None,
            )
            .took_place();

        index.install(
            vec![
                (
                    KeyBytes::new(&key(1, 1)).expect("key"),
                    Entry::new(loc(1, 0, 400), Lsn(1)),
                ),
                (
                    KeyBytes::new(&key(2, 2)).expect("key"),
                    Entry::new(loc(2, 0, 900), Lsn(2)),
                ),
            ],
            &segments,
        );

        assert_eq!(index.totals().count, 2);
        assert_eq!(index.totals().bytes, ByteCount::from_bytes(1300));
        assert!(!index.contains(&key(5, 5)));
        assert_eq!(index.get(&key(2, 2)).expect("present").lsn, Lsn(2));
    }

    // a repoint moves a key to its copy only while the version is unchanged
    #[test]
    fn repoint_moves_unchanged() {
        let index = sharded();
        let segments = SegmentTable::new();
        index
            .insert(
                &key(1, 1),
                Entry::new(loc(1, 0, 400), Lsn(1)),
                &segments,
                None,
            )
            .took_place();

        let moved = index.repoint(&key(1, 1), loc(2, 0, 400), Lsn(1), &segments);

        assert!(moved);
        assert_eq!(index.get(&key(1, 1)).expect("present").loc, loc(2, 0, 400));
        assert_eq!(segments.bytes_of(SegmentId(1)).live, 0);
        assert_eq!(segments.bytes_of(SegmentId(1)).dead, 0);
        assert_eq!(segments.bytes_of(SegmentId(2)).live, span_of(34, 400));
        assert_eq!(index.totals().count, 1);
    }

    // a repoint loses to a concurrent overwrite and the copy is booked dead
    #[test]
    fn repoint_loses_to_overwrite() {
        let index = sharded();
        let segments = SegmentTable::new();
        index
            .insert(
                &key(1, 1),
                Entry::new(loc(1, 0, 400), Lsn(1)),
                &segments,
                None,
            )
            .took_place();
        index
            .insert(
                &key(1, 1),
                Entry::new(loc(3, 0, 900), Lsn(2)),
                &segments,
                None,
            )
            .took_place();

        let moved = index.repoint(&key(1, 1), loc(2, 0, 400), Lsn(1), &segments);

        assert!(!moved);
        assert_eq!(index.get(&key(1, 1)).expect("present").loc, loc(3, 0, 900));
        assert_eq!(segments.bytes_of(SegmentId(2)).dead, span_of(34, 400));
    }

    // evicting a corrupt record removes the key and books its bytes dead
    #[test]
    fn evict_at_drops_key() {
        let index = sharded();
        let segments = SegmentTable::new();
        index
            .insert(
                &key(1, 1),
                Entry::new(loc(1, 0, 400), Lsn(1)),
                &segments,
                None,
            )
            .took_place();

        let evicted = index.evict_at(&key(1, 1), loc(1, 0, 400), &segments);

        assert!(evicted);
        assert!(!index.contains(&key(1, 1)));
        assert_eq!(segments.bytes_of(SegmentId(1)).dead, span_of(34, 400));
        assert_eq!(index.totals().count, 0);
    }

    // eviction is declined once a newer version has overtaken the corrupt one
    #[test]
    fn evict_at_declines_moved() {
        let index = sharded();
        let segments = SegmentTable::new();
        index
            .insert(
                &key(1, 1),
                Entry::new(loc(1, 0, 400), Lsn(1)),
                &segments,
                None,
            )
            .took_place();
        index
            .insert(
                &key(1, 1),
                Entry::new(loc(2, 0, 500), Lsn(2)),
                &segments,
                None,
            )
            .took_place();

        assert!(!index.evict_at(&key(1, 1), loc(1, 0, 400), &segments));
        assert!(index.contains(&key(1, 1)));
    }

    // an emptied shard leaves the walk set so a page does not open it again
    #[test]
    fn emptied_shard_leaves_the_walk() {
        let index = sharded();
        let segments = SegmentTable::new();
        index
            .insert(
                &key(1, 1),
                Entry::new(loc(1, 0, 400), Lsn(1)),
                &segments,
                None,
            )
            .took_place();
        index
            .insert(
                &key(2, 1),
                Entry::new(loc(1, 0, 400), Lsn(2)),
                &segments,
                None,
            )
            .took_place();

        index.remove(&key(1, 1), Lsn(3), loc(1, 0, 0), &segments);

        // The shard is not empty yet: the delete left a grave holding the key's
        // place.
        assert_eq!(read(&index.occupied).len(), 2);
        let mut out = KeyPage::default();
        index.page(Bound::Unbounded, 8, &mut out);
        assert_eq!(keys_in(&out), vec![key(2, 1)]);

        index.prune_tombstones(Lsn(3), None);

        assert_eq!(
            read(&index.occupied).len(),
            1,
            "the shard leaves once the grave does"
        );
        index.page(Bound::Unbounded, 8, &mut out);
        assert_eq!(keys_in(&out), vec![key(2, 1)]);
    }

    // a put drawn before a delete but published after it does not bring the key back
    #[test]
    fn a_late_put_cannot_outlive_a_delete() {
        let index = sharded();
        let segments = SegmentTable::new();

        // The delete is drawn second and published first, against a key the index
        // has never seen.
        index.remove(&key(1, 1), Lsn(6), loc(1, 0, 0), &segments);
        assert!(!index
            .insert(
                &key(1, 1),
                Entry::new(loc(1, 0, 400), Lsn(5)),
                &segments,
                None
            )
            .took_place());

        assert!(index.get(&key(1, 1)).is_none(), "the deleted key came back");
        assert_eq!(index.totals().count, 0);
    }

    // the same race against a key that was already there
    #[test]
    fn a_late_put_cannot_outlive_a_delete_of_a_live_key() {
        let index = sharded();
        let segments = SegmentTable::new();
        index
            .insert(
                &key(1, 1),
                Entry::new(loc(1, 0, 100), Lsn(2)),
                &segments,
                None,
            )
            .took_place();

        index.remove(&key(1, 1), Lsn(6), loc(1, 0, 0), &segments);
        assert!(!index
            .insert(
                &key(1, 1),
                Entry::new(loc(1, 0, 400), Lsn(5)),
                &segments,
                None
            )
            .took_place());

        assert!(index.get(&key(1, 1)).is_none());
        assert_eq!(index.totals().count, 0);
    }

    // a put drawn after the delete is a new version and keeps its place
    #[test]
    fn a_put_after_a_delete_is_kept() {
        let index = sharded();
        let segments = SegmentTable::new();

        index.remove(&key(1, 1), Lsn(6), loc(1, 0, 0), &segments);
        assert!(index
            .insert(
                &key(1, 1),
                Entry::new(loc(1, 0, 400), Lsn(7)),
                &segments,
                None
            )
            .took_place());

        assert!(index.get(&key(1, 1)).is_some());
        assert_eq!(index.totals().count, 1);
        assert_eq!(
            index.grave_count(),
            0,
            "the grave gave its place to the record"
        );
    }

    // a range delete guards with its cover alone and leaves no grave per key
    #[test]
    fn a_range_delete_leaves_no_graves() {
        let index = sharded();
        let segments = SegmentTable::new();
        index
            .insert(
                &key(1, 1),
                Entry::new(loc(1, 0, 100), Lsn(2)),
                &segments,
                None,
            )
            .took_place();
        index
            .insert(
                &key(1, 2),
                Entry::new(loc(1, 0, 100), Lsn(3)),
                &segments,
                None,
            )
            .took_place();

        index.remove_range(&key(1, 0), None, Lsn(6));
        sweep_all(&index, &segments);

        assert_eq!(
            index.grave_count(),
            0,
            "the cover is the guard, not a grave per key"
        );
        assert!(!index
            .insert(
                &key(1, 1),
                Entry::new(loc(1, 0, 400), Lsn(5)),
                &segments,
                None
            )
            .took_place());
        assert!(
            index.get(&key(1, 1)).is_none(),
            "the range delete was undone"
        );
        assert_eq!(index.totals().count, 0);
    }

    // graves are given back once nothing older can still be published
    #[test]
    fn graves_are_pruned_at_the_floor() {
        let index = sharded();
        let segments = SegmentTable::new();
        index.remove(&key(1, 1), Lsn(6), loc(1, 0, 0), &segments);
        index.remove(&key(2, 1), Lsn(9), loc(1, 0, 0), &segments);

        assert_eq!(
            index.prune_tombstones(Lsn(6), None),
            1,
            "only the one below the floor"
        );
        assert_eq!(index.grave_count(), 1);

        assert_eq!(index.prune_tombstones(Lsn(9), None), 1);
        assert_eq!(index.grave_count(), 0);
    }

    // the pack a heavy prune triggers keeps every key the shard still holds
    #[test]
    fn a_pruned_shard_packs_without_losing_a_key() {
        let index = sharded();
        let segments = SegmentTable::new();

        let held: Vec<Vec<u8>> = (0..2000u32)
            .map(|at| {
                let mut out = 3u16.to_be_bytes().to_vec();
                out.extend_from_slice(&at.to_be_bytes());
                out.extend_from_slice(&[7u8; 28]);
                out
            })
            .collect();
        for (at, key) in held.iter().enumerate() {
            index.insert(
                key,
                Entry::new(loc(1, at as u32, 10), Lsn(at as u64 + 1)),
                &segments,
                None,
            );
        }

        // Every key but the last hundred, deleted under numbers a prune can reach.
        for key in held.iter().take(1900) {
            index.remove(key, Lsn(10_000), loc(2, 0, 0), &segments);
        }
        assert_eq!(index.grave_count(), 1900);
        assert_eq!(index.prune_tombstones(Lsn(10_000), None), 1900);
        assert_eq!(index.grave_count(), 0);

        let survivors = &held[1900..];
        assert_eq!(index.totals().count, survivors.len() as u64);
        for (at, key) in survivors.iter().enumerate() {
            let entry = index.get(key).expect("a survivor went missing");
            assert_eq!(
                entry.lsn,
                Lsn(1901 + at as u64),
                "a survivor took the wrong entry"
            );
        }

        let mut page = KeyPage::default();
        index.page(Bound::Unbounded, 4096, &mut page);
        assert_eq!(
            keys_in(&page),
            survivors.to_vec(),
            "the packed walk is out of order"
        );
    }

    // a shard the sweep drained gives its room back, and a thinned one packs
    #[test]
    fn a_swept_shard_gives_its_room_back() {
        let index = sharded();
        let segments = SegmentTable::new();

        // Two groups, so one shard is swept away and the neighbour is the control.
        let keys = |group: u16| -> Vec<Vec<u8>> {
            (0..2000u32)
                .map(|at| {
                    let mut out = group.to_be_bytes().to_vec();
                    out.extend_from_slice(&at.to_be_bytes());
                    out.extend_from_slice(&[7u8; 28]);
                    out
                })
                .collect()
        };
        let swept = keys(3);
        let kept = keys(4);
        for key in swept.iter().chain(kept.iter()) {
            index.insert(key, Entry::new(loc(1, 0, 10), Lsn(1)), &segments, None);
        }
        let filled = read(&index.shards[3]).map.leaf_count();
        assert!(filled > 1, "2000 keys landed in {filled} leaves");

        index.remove_range(&3u16.to_be_bytes(), Some(&4u16.to_be_bytes()), Lsn(9));
        sweep_all(&index, &segments);

        assert_eq!(
            read(&index.shards[3]).map.leaf_count(),
            0,
            "a drained shard kept its room"
        );
        assert_eq!(
            read(&index.occupied).len(),
            1,
            "the drained shard is still in the walk"
        );
        assert_eq!(index.totals().count, kept.len() as u64);
        for key in &kept {
            assert!(
                index.get(key).is_some(),
                "the neighbouring shard lost a key"
            );
        }

        // The shard takes keys again after the release, and the walk finds it.
        index.insert(
            &swept[0],
            Entry::new(loc(1, 0, 10), Lsn(20)),
            &segments,
            None,
        );
        assert_eq!(
            read(&index.occupied).len(),
            2,
            "the shard did not rejoin the walk"
        );
        assert!(index.get(&swept[0]).is_some());
    }

    // the pass that pages a column's keys out packs behind itself
    #[test]
    fn a_paged_out_shard_packs_as_it_hands_over() {
        let index = sharded();
        let segments = SegmentTable::new();

        let held: Vec<Vec<u8>> = (0..2000u32)
            .map(|at| {
                let mut out = 3u16.to_be_bytes().to_vec();
                out.extend_from_slice(&at.to_be_bytes());
                out.extend_from_slice(&[7u8; 28]);
                out
            })
            .collect();
        for key in &held {
            index.insert(key, Entry::new(loc(1, 0, 10), Lsn(1)), &segments, None);
        }
        let filled = read(&index.shards[3]).map.leaf_count();

        // All but the last hundred handed to the footers.
        for key in held.iter().take(1900) {
            assert!(
                index.page_out(key, loc(1, 0, 10)),
                "a key refused to page out"
            );
        }
        let packed = read(&index.shards[3]).map.leaf_count();
        assert!(
            packed * 4 < filled,
            "paging 95% out left {packed} of {filled} leaves"
        );

        // A shard that pages keeps its place in the walk, since its paged count is
        // not zero.
        assert_eq!(read(&index.occupied).len(), 1);
        for key in &held[1900..] {
            assert!(index.get(key).is_some(), "a resident key went missing");
        }
    }

    // a put drawn before a range delete cannot land after it, key seen or not
    #[test]
    fn a_late_put_cannot_outlive_a_range_delete() {
        let index = sharded();
        let segments = SegmentTable::new();

        index.remove_range(&key(1, 0), None, Lsn(6));
        assert_eq!(
            index.grave_count(),
            0,
            "the range found no key to leave one on"
        );

        assert!(!index
            .insert(
                &key(1, 5),
                Entry::new(loc(1, 0, 400), Lsn(5)),
                &segments,
                None
            )
            .took_place());
        assert!(index.get(&key(1, 5)).is_none(), "the purged key came back");
        assert_eq!(index.totals().count, 0);
    }

    // a put drawn after the range delete is a new version and keeps its place
    #[test]
    fn a_put_after_a_range_delete_is_kept() {
        let index = sharded();
        let segments = SegmentTable::new();

        index.remove_range(&key(1, 0), None, Lsn(6));

        assert!(index
            .insert(
                &key(1, 5),
                Entry::new(loc(1, 0, 400), Lsn(7)),
                &segments,
                None
            )
            .took_place());
        assert!(index.get(&key(1, 5)).is_some());
    }

    // a range delete holds nothing against a key outside the range it took
    #[test]
    fn a_cover_holds_only_its_own_range() {
        let index = sharded();
        let segments = SegmentTable::new();

        index.remove_range(&key(1, 0), Some(&key(1, 9)), Lsn(6));

        assert!(
            !index
                .insert(
                    &key(1, 5),
                    Entry::new(loc(1, 0, 400), Lsn(5)),
                    &segments,
                    None
                )
                .took_place(),
            "a key inside the range is refused"
        );
        assert!(
            index
                .insert(
                    &key(2, 5),
                    Entry::new(loc(1, 0, 400), Lsn(5)),
                    &segments,
                    None
                )
                .took_place(),
            "a key past the end of the range is not this delete's business"
        );
    }

    // a column that never deletes a range keeps nothing to test inserts against
    #[test]
    fn no_range_delete_leaves_no_covers() {
        let index = sharded();
        let segments = SegmentTable::new();

        index
            .insert(
                &key(1, 1),
                Entry::new(loc(1, 0, 100), Lsn(1)),
                &segments,
                None,
            )
            .took_place();
        index.remove(&key(1, 1), Lsn(2), loc(1, 0, 0), &segments);

        assert_eq!(index.cover_count(), 0);
    }

    // covers are given back on the same floor as the graves, once swept
    #[test]
    fn covers_are_pruned_at_the_floor() {
        let index = sharded();
        let segments = SegmentTable::new();
        index.remove_range(&key(1, 0), Some(&key(1, 9)), Lsn(6));
        index.remove_range(&key(2, 0), Some(&key(2, 9)), Lsn(9));
        assert_eq!(index.cover_count(), 2);

        // The floor alone retires nothing while the sweep is still owed.
        index.prune_tombstones(Lsn(9), None);
        assert_eq!(
            index.cover_count(),
            2,
            "an unswept cover outlives the floor"
        );

        sweep_all(&index, &segments);
        index.prune_tombstones(Lsn(6), None);
        assert_eq!(index.cover_count(), 1, "only the one below the floor");

        index.prune_tombstones(Lsn(9), None);
        assert_eq!(index.cover_count(), 0);

        // Nothing is held any more, so a record older than the retired delete is
        // taken on its own merits.
        assert!(index
            .insert(
                &key(1, 5),
                Entry::new(loc(1, 0, 400), Lsn(5)),
                &segments,
                None
            )
            .took_place());
    }

    // a rebuild's entries are consistent already, so nothing is held against them
    #[test]
    fn a_rebuild_drops_what_was_held() {
        let index = sharded();
        let segments = SegmentTable::new();
        index.remove_range(&key(1, 0), None, Lsn(6));
        index.remove(&key(2, 1), Lsn(7), loc(1, 0, 0), &segments);

        index.clear();

        assert_eq!(index.cover_count(), 0);
        assert_eq!(index.grave_count(), 0);
        assert!(index
            .insert(
                &key(1, 5),
                Entry::new(loc(1, 0, 400), Lsn(5)),
                &segments,
                None
            )
            .took_place());
    }

    // a bounded run resumes where it stopped, and reads stay right throughout
    #[test]
    fn a_partial_sweep_resumes() {
        let index = sharded();
        let segments = SegmentTable::new();
        for byte in 0..8u8 {
            index
                .insert(
                    &key(7, byte),
                    Entry::new(loc(1, 0, 100), Lsn(1 + byte as u64)),
                    &segments,
                    None,
                )
                .took_place();
        }

        index.remove_range(&7u16.to_be_bytes(), Some(&8u16.to_be_bytes()), Lsn(50));
        let pending = index.next_pending_cover().expect("pending");
        index.advance_release(pending.lsn, None);

        let (dropped, examined, done) = index.sweep_run(Lsn(50), 3, &segments);
        assert_eq!((dropped, examined, done), (3, 3, false));
        assert!(
            !index.contains(&key(7, 7)),
            "an unswept key already reads gone"
        );
        assert!(index.has_pending_covers());

        let (dropped, _, done) = index.sweep_run(Lsn(50), usize::MAX, &segments);
        assert_eq!((dropped, done), (5, true));
        assert_eq!(index.totals().count, 0);
        assert_eq!(index.grave_count(), 0);
        assert!(!index.has_pending_covers());
    }

    // a covered entry refuses to page out and waits for the sweep
    #[test]
    fn a_covered_entry_stays_for_the_sweep() {
        let index = sharded();
        let segments = SegmentTable::new();
        index
            .insert(
                &key(7, 1),
                Entry::new(loc(1, 0, 100), Lsn(1)),
                &segments,
                None,
            )
            .took_place();

        index.remove_range(&7u16.to_be_bytes(), None, Lsn(9));

        assert!(
            !index.page_out(&key(7, 1), loc(1, 0, 100)),
            "handing it over would strand it"
        );
        sweep_all(&index, &segments);
        assert_eq!(index.totals().count, 0);
        assert_eq!(segments.bytes_of(SegmentId(1)).dead, span_of(34, 100));
    }

    // a shard under an unswept cover declines its totals instead of lying
    #[test]
    fn prefix_totals_decline_under_a_pending_cover() {
        let index = sharded();
        let segments = SegmentTable::new();
        index
            .insert(
                &key(7, 1),
                Entry::new(loc(1, 0, 400), Lsn(1)),
                &segments,
                None,
            )
            .took_place();
        index
            .insert(
                &key(8, 1),
                Entry::new(loc(1, 0, 100), Lsn(2)),
                &segments,
                None,
            )
            .took_place();

        index.remove_range(&7u16.to_be_bytes(), Some(&8u16.to_be_bytes()), Lsn(9));

        assert!(
            index.prefix_totals(&7u16.to_be_bytes()).is_none(),
            "the shard cannot answer yet"
        );
        assert!(
            index.prefix_totals(&8u16.to_be_bytes()).is_some(),
            "a shard past the range still answers"
        );

        sweep_all(&index, &segments);
        let seven = index
            .prefix_totals(&7u16.to_be_bytes())
            .expect("after the sweep");
        assert_eq!(seven.count, 0);
        assert_eq!(
            index
                .prefix_totals(&8u16.to_be_bytes())
                .expect("untouched")
                .count,
            1
        );
    }

    // a delete twice over holds one place, not one per delete
    #[test]
    fn a_second_delete_replaces_the_grave() {
        let index = sharded();
        let segments = SegmentTable::new();

        index.remove(&key(1, 1), Lsn(4), loc(1, 0, 0), &segments);
        index.remove(&key(1, 1), Lsn(5), loc(1, 0, 0), &segments);

        assert_eq!(index.grave_count(), 1);
        assert!(!index
            .insert(
                &key(1, 1),
                Entry::new(loc(1, 0, 400), Lsn(4)),
                &segments,
                None
            )
            .took_place());
    }
}
