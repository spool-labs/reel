//! One column's resident keys at its declared width, split into shards by leading bytes

use std::borrow::Borrow;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::RwLock;

use crate::units::ByteCount;

use crate::engine::Totals;
use crate::error::{ReelError, Result};
use crate::format::column::{ColumnId, ColumnSpec, KeyBytes, RecordKey, MAX_KEY_LEN};
use crate::format::loc::{Loc, SegmentId, SegmentIncarnation};
use crate::format::lsn::Lsn;
use crate::index::counters::{Bookings, SegmentTable};
use crate::index::entry::{span_of, Entry};
use crate::index::in_turns;
use crate::index::page::KeyPage;
use crate::index::paged::SealedRanges;
use crate::index::spot::Booking;
use crate::index::tbtreemap::{node_width, TBTreeMap, NODE_WIDTH};
use crate::sync::{read, write};

/// One column's resident index at its declared key width, or on the heap for variable keys
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

    Var(WidthIndex<Box<[u8]>, VarTrees>),
}

/// What a mutation found in the map at its key
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Landed {
    /// Nothing was there, so a footer may still hold the key
    Nothing,

    /// A live record was there and the mutation dealt with it
    Record,

    /// An earlier tombstone's grave was there, so the key was already gone
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

/// The shadow check for a map that handed no key over, which never finds a newer version
pub fn never_shadowed(_key: &[u8], _lsn: Lsn) -> bool {
    false
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
            ColumnIndex::Var($bound) => $body,
        }
    };
}

/// One key's move as a batch hands it to the index
pub struct KeyMove<'batch> {
    /// The key's column
    pub column: ColumnId,

    /// The key being moved
    pub key: &'batch [u8],

    /// Where the record landed, or where its tombstone did
    pub loc: Loc,

    /// The sequence number it was written under
    pub lsn: Lsn,

    /// Whether this is a tombstone
    pub is_delete: bool,
}

impl ColumnIndex {
    /// Apply a batch's moves to this column, sharing a shard's lock across keys
    pub fn apply_moves<Book: Bookings>(
        &self,
        moves: &[KeyMove<'_>],
        segments: &Book,
        is_shadowed: &impl Fn(&[u8], Lsn) -> bool,
        landed: &mut Vec<Landed>,
    ) {
        on_index!(self, index => index.apply_moves(moves, segments, is_shadowed, landed))
    }

    /// An empty index for one column, refusing a width nothing indexes
    pub fn new(spec: &ColumnSpec) -> Result<ColumnIndex> {
        let Some(width) = spec.key_width.fixed() else {
            return Ok(ColumnIndex::Var(WidthIndex::new(spec)));
        };
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

    /// The declared key width in bytes, zero for a variable column
    pub fn key_width(&self) -> u16 {
        on_index!(self, index => index.key_width())
    }

    /// How many leading key bytes pick a shard
    pub fn shard_bytes(&self) -> u8 {
        on_index!(self, index => index.shard_bytes)
    }

    /// Heap bytes of the filters in front of the shards
    pub fn filter_bytes(&self) -> u64 {
        on_index!(self, index => index.filter_bytes())
    }

    /// Heap bytes of the column's index, counted from what its maps allocated
    pub fn heap_bytes(&self) -> u64 {
        on_index!(self, index => index.heap_bytes())
    }

    /// Apply a committed record, guarded by sequence number and by `is_shadowed` at an empty place
    pub fn insert(
        &self,
        key: &[u8],
        entry: Entry,
        segments: &SegmentTable,
        is_shadowed: &impl Fn(&[u8], Lsn) -> bool,
    ) -> Landed {
        on_index!(self, index => index.insert_unless(key, entry, segments, is_shadowed))
    }

    /// Drop a key on a tombstone, guarded by sequence number and by `is_shadowed` at an empty place
    pub fn remove(
        &self,
        key: &[u8],
        lsn: Lsn,
        tombstone: Loc,
        segments: &SegmentTable,
        is_shadowed: &impl Fn(&[u8], Lsn) -> bool,
    ) -> Landed {
        on_index!(self, index => index.remove_unless(key, lsn, tombstone, segments, is_shadowed))
    }

    /// Stand a grave for a tombstone that compaction copied, unless a newer version stands
    pub fn hold_grave(
        &self,
        key: &[u8],
        lsn: Lsn,
        segment: SegmentId,
        is_shadowed: impl FnOnce() -> bool,
    ) {
        on_index!(self, index => index.hold_grave(key, lsn, segment, is_shadowed))
    }

    /// Take out a key's grave while it still holds this tombstone's number
    pub fn drop_grave(&self, key: &[u8], lsn: Lsn) -> bool {
        on_index!(self, index => index.drop_grave(key, lsn))
    }

    /// The span of a record in this column with a `len`-byte payload
    pub fn span_of(&self, len: u32) -> u64 {
        span_of(self.key_width(), len)
    }

    /// Book a record that only a footer held as gone
    pub fn settle_paged(&self, key: &[u8], loc: Loc, segments: &SegmentTable) -> bool {
        on_index!(self, index => index.settle_paged(key, loc, segments))
    }

    /// Book a footer-held record at `loc` gone without a read, taking `least` off the live bytes
    pub fn settle_paged_least(
        &self,
        key: &[u8],
        loc: Loc,
        least: u32,
        segments: &SegmentTable,
    ) -> bool {
        on_index!(self, index => index.settle_paged_least(key, loc, least, segments))
    }

    /// Swap the length a class booked for a record's true length, once it is known
    pub fn rebook_paged(&self, key: &[u8], booked: u32, actual: u32) {
        on_index!(self, index => index.rebook_paged(key, booked, actual))
    }

    /// Count sealed records an open put in the spot index, as keys with lengths in key order
    pub fn book_sealed<'a>(&self, rows: impl Iterator<Item = (&'a [u8], u32)>) {
        on_index!(self, index => index.book_sealed(rows))
    }

    /// Move one key's sealed count from the version that went to the one that came
    pub fn book_paged(&self, key: &[u8], booking: Booking) {
        on_index!(self, index => index.book_paged(key, booking))
    }

    /// Take a paged key out with a grave of its own, for a record that will not read
    pub fn evict_paged(
        &self,
        key: &[u8],
        loc: Loc,
        lsn: Lsn,
        segments: &SegmentTable,
        take: impl FnOnce() -> bool,
    ) -> bool {
        on_index!(self, index => index.evict_paged(key, loc, lsn, segments, take))
    }

    /// Bring a paged key back into the map at its rewritten copy
    pub fn repoint_paged(
        &self,
        key: &[u8],
        to: Loc,
        lsn: Lsn,
        stamp: SegmentIncarnation,
        take: impl FnOnce() -> bool,
    ) -> bool {
        on_index!(self, index => index.repoint_paged(key, to, lsn, stamp, take))
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
        segments: &SegmentTable,
        take: impl FnOnce() -> bool,
    ) -> bool {
        on_index!(self, index => index.release_covered(key, loc, segments, take))
    }

    /// Whether an unfinished cover reaches into this inclusive key range
    pub fn pending_overlaps(&self, low: &[u8], high: &[u8]) -> bool {
        on_index!(self, index => index.pending_overlaps(low, high))
    }

    /// Whether any cover is still owed its sweep
    pub fn has_pending_covers(&self) -> bool {
        on_index!(self, index => index.has_pending_covers())
    }

    /// Whether any range delete stands over the column
    pub fn has_covers(&self) -> bool {
        on_index!(self, index => index.has_covers())
    }

    /// Drop what tombstones hold once nothing older than them can still be published
    pub fn prune_tombstones(&self, before: Lsn, sealed: &SealedRanges) -> u64 {
        on_index!(self, index => index.prune_tombstones(before, sealed))
    }

    /// How many graves the column holds, the memory a prune would give back
    pub fn grave_count(&self) -> u64 {
        on_index!(self, index => index.grave_count())
    }

    /// How many covers the column still tests inserts against
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
        stamp: SegmentIncarnation,
    ) -> Option<Loc> {
        on_index!(self, index => index.repoint(key, new_loc, expected_lsn, stamp))
    }

    /// Drop a key while it still resolves one exact location, writing no tombstone
    pub fn evict_at(&self, key: &[u8], loc: Loc, segments: &SegmentTable) -> bool {
        on_index!(self, index => index.evict_at(key, loc, segments))
    }

    /// Give a key up to the footer of the segment it landed in
    pub fn page_out(&self, key: &[u8], loc: Loc) -> bool {
        on_index!(self, index => index.page_out(key, loc))
    }

    /// Page out one lane's keys, calling `first` on each under the lock, and report which went
    pub fn page_out_lane(
        &self,
        rows: &[(&[u8], Loc)],
        lane: usize,
        lanes: usize,
        first: &(dyn Fn(&[u8], Loc) + Sync),
    ) -> Vec<bool> {
        on_index!(self, index => index.page_out_lane(rows, lane, lanes, first))
    }

    /// Whether the map points a key at exactly this place, with no grave or cover over it
    pub fn holds(&self, key: &[u8], loc: Loc) -> bool {
        on_index!(self, index => index.holds(key, loc))
    }

    /// The newest version a pruned grave or cover guarded
    pub fn lifted(&self) -> Lsn {
        on_index!(self, index => index.lifted())
    }

    /// Every entry the column holds, graves included, in key order
    pub fn held(&self) -> Vec<(KeyBytes, Entry)> {
        on_index!(self, index => index.held())
    }

    /// Take out an entry a rebuild found outversioned, booking a record dead
    pub fn drop_shadowed(&self, key: &[u8], segments: &SegmentTable) {
        on_index!(self, index => index.drop_shadowed(key, segments))
    }

    /// Size every shard's map to what it holds, once a rebuild has filled it
    pub fn fit(&self) {
        on_index!(self, index => index.fit())
    }

    /// Drop every key, for a reader rebuilding the whole volume
    pub fn clear(&self) {
        on_index!(self, index => index.clear())
    }

    /// Live key count and payload byte total for the column
    pub fn totals(&self) -> Totals {
        on_index!(self, index => index.totals())
    }

    /// How many keys the maps hold, graves included, for weighing their cost
    pub fn resident_keys(&self) -> u64 {
        on_index!(self, index => index.resident_keys())
    }

    /// The share of neighbouring keys with equal leads
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

/// A shard's run needs this many keys before a batched descent pays for itself
const BATCH_RUN: usize = 4;

/// A hand-over gives up this many keys per hold of a shard's lock
const LOCK_CHUNK: usize = 64;

/// Per-thread scratch for grouping a batched lookup's keys by shard
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

/// One shard's keys and the payload bytes they resolve to
struct ShardState<K: IndexKey, S: Shape<K>> {
    /// Every key the shard holds, live records and graves alike
    map: S::Entries,

    /// Total payload bytes of the live records
    bytes: u64,

    /// How many of the map's places hold a grave
    graves: usize,

    /// The oldest grave's sequence number, a floor that can only understate
    oldest_grave: Lsn,

    /// How many of this shard's live sealed records the spot index answers for
    paged: usize,
}

impl<K: IndexKey, S: Shape<K>> ShardState<K, S> {
    fn empty() -> ShardState<K, S> {
        ShardState {
            map: S::Entries::default(),
            bytes: 0,
            graves: 0,
            oldest_grave: Lsn(u64::MAX),
            paged: 0,
        }
    }

    /// Count a grave and lower `oldest_grave`, the floor a prune uses to skip shards
    fn note_grave(&mut self, lsn: Lsn) {
        self.graves += 1;
        if lsn < self.oldest_grave {
            self.oldest_grave = lsn;
        }
    }

    /// The shard's live keys: its map less the graves, plus what it paged
    fn live_count(&self) -> u64 {
        (self.map.count() - self.graves + self.paged) as u64
    }
}

/// One cover still owed its sweep, in plain bytes for the driver above the column
pub struct PendingCover {
    /// The delete's sequence number
    pub lsn: Lsn,

    /// The range's exclusive end, or none to run to the top of the column
    pub end: Option<Vec<u8>>,

    /// The release pass resumes from this key while that phase runs
    pub release_from: Option<Vec<u8>>,
}

/// A range one tombstone covered, which every read, walk and insert checks
struct Covered<K: IndexKey> {
    /// Inclusive start of the range
    low: K,

    /// Exclusive end, or none when the range runs to the top of the column
    high: Option<K>,

    /// The delete's sequence number
    lsn: Lsn,

    /// How far the lazy sweep has taken this cover toward retirement
    phase: SweepPhase<K>,
}

/// Where the lazy sweep stands on one cover. Release runs before Sweep so no record settles twice
enum SweepPhase<K: IndexKey> {
    /// Settling records only footers answer for, resuming at this key
    Release(K),

    /// Dropping covered map entries, resuming at this key
    Sweep(K),

    /// Nothing left to settle, and the cover stands only to refuse
    Done,
}

impl<K: IndexKey> Covered<K> {
    /// Whether any sealed segment still holds keys this range would take
    fn reaches_sealed(&self, sealed: &SealedRanges) -> bool {
        sealed.overlaps(
            self.low.as_slice(),
            self.high.as_ref().map(|high| high.as_slice()),
        )
    }

    /// Whether this covered a key and was drawn after that key was written
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

/// One shard's filter takes at most this many words, sixteen kibibytes
const FILTER_WORDS: usize = 2048;

/// A column's filters share this many words, a mebibyte, across all of its shards
const COLUMN_FILTER_WORDS: usize = 1 << 17;

/// Filters in front of every shard's lock, set before a key lands and cleared under the lock
struct ShardFilters {
    /// The column's words, every shard's run of them in shard order
    shared: Box<[AtomicU64]>,

    /// How many words each shard owns, a power of two
    per_shard: usize,
}

impl ShardFilters {
    fn new(shards: usize) -> ShardFilters {
        let shards = shards.max(1);
        let even = (COLUMN_FILTER_WORDS / shards).clamp(1, FILTER_WORDS);
        let per_shard = 1usize << even.ilog2();
        ShardFilters {
            shared: (0..per_shard * shards).map(|_| AtomicU64::new(0)).collect(),
            per_shard,
        }
    }

    /// One shard's words
    fn of(&self, shard: usize) -> &[AtomicU64] {
        &self.shared[shard * self.per_shard..(shard + 1) * self.per_shard]
    }

    /// Record a key on its way into the shard's map
    fn note(&self, shard: usize, hash: u64) {
        let words = self.of(shard);
        let (first, second) = probe_pair(hash, self.per_shard);
        words[(first / 64) as usize].fetch_or(1 << (first % 64), Ordering::Relaxed);
        words[(second / 64) as usize].fetch_or(1 << (second % 64), Ordering::Relaxed);
    }

    /// Whether the shard may hold the key, where a no is certain
    fn may_hold(&self, shard: usize, hash: u64) -> bool {
        let words = self.of(shard);
        let (first, second) = probe_pair(hash, self.per_shard);
        words[(first / 64) as usize].load(Ordering::Relaxed) & (1 << (first % 64)) != 0
            && words[(second / 64) as usize].load(Ordering::Relaxed) & (1 << (second % 64)) != 0
    }

    /// Forget every key of a shard whose map holds nothing, under its write lock
    fn clear(&self, shard: usize) {
        for word in self.of(shard) {
            word.store(0, Ordering::Relaxed);
        }
    }

    /// Heap bytes of the filters
    fn heap_bytes(&self) -> u64 {
        std::mem::size_of_val(&*self.shared) as u64
    }
}

/// Two bit positions from one key hash, mixed apart so the pair is not one sequence
fn probe_pair(hash: u64, words: usize) -> (u64, u64) {
    let mask = (words as u64 * 64) - 1;
    let other = hash.wrapping_mul(0xD6E8_FEB8_6659_FD93) ^ (hash >> 32);
    (hash & mask, other & mask)
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

/// One column's index over one key type, fixed width or variable
pub struct WidthIndex<K: IndexKey, S: Shape<K>> {
    /// The column's keys, split by their leading bytes
    shards: Vec<RwLock<ShardState<K, S>>>,

    /// A filter beside each shard's lock, so a definite miss never takes it
    filters: ShardFilters,

    /// Ranges a range tombstone covers, tested against records that arrive after it
    covers: RwLock<Vec<Covered<K>>>,

    /// Whether `covers` holds anything, so a test against no covers skips the lock
    has_covers: AtomicBool,

    /// The newest version a pruned grave or cover guarded
    lifted: AtomicU64,

    /// Shards holding at least one key, so a walk skips the empty ones
    occupied: RwLock<TBTreeMap<u64, NODE_WIDTH, ()>>,

    /// A bit for each shard whose map holds an entry, so a page skips empty maps without a lock
    filled: Box<[AtomicU64]>,

    /// The column's declared key width, zero for a variable column
    declared_width: u16,

    /// How many leading key bytes pick a shard
    shard_bytes: u8,
}

impl<K: IndexKey, S: Shape<K>> WidthIndex<K, S> {
    /// An empty index for a column, split into the shards the column declares
    pub fn new(spec: &ColumnSpec) -> WidthIndex<K, S> {
        let mut shards = Vec::with_capacity(spec.shard_count());
        for _ in 0..spec.shard_count() {
            shards.push(RwLock::new(ShardState::empty()));
        }
        WidthIndex {
            shards,
            filters: ShardFilters::new(spec.shard_count()),
            covers: RwLock::new(Vec::new()),
            has_covers: AtomicBool::new(false),
            lifted: AtomicU64::new(0),
            occupied: RwLock::new(TBTreeMap::new()),
            filled: (0..spec.shard_count().div_ceil(64))
                .map(|_| AtomicU64::new(0))
                .collect(),
            declared_width: spec.key_width.fixed().unwrap_or(0),
            shard_bytes: spec.shard_bytes,
        }
    }

    /// The declared key width in bytes, zero for a variable column
    pub fn key_width(&self) -> u16 {
        self.declared_width
    }

    /// Heap bytes of the filters in front of the shards
    pub fn filter_bytes(&self) -> u64 {
        self.filters.heap_bytes()
    }

    /// Heap bytes of the shards, their maps' allocations and the filters, counting capacity
    pub fn heap_bytes(&self) -> u64 {
        let fixed = (self.shards.capacity() * std::mem::size_of::<RwLock<ShardState<K, S>>>())
            as u64
            + self.filters.heap_bytes();
        fixed + self.sum_shards(|state| state.map.heap_bytes())
    }

    /// Apply a committed data record, guarded by its sequence number
    pub fn insert(&self, key: &[u8], entry: Entry, segments: &SegmentTable) -> Landed {
        self.insert_unless(key, entry, segments, &never_shadowed)
    }

    /// The same insert, refused over an empty place where `is_shadowed` finds a newer version
    pub fn insert_unless(
        &self,
        key: &[u8],
        entry: Entry,
        segments: &SegmentTable,
        is_shadowed: &impl Fn(&[u8], Lsn) -> bool,
    ) -> Landed {
        let key = match K::from_slice(key) {
            Some(key) => key,
            None => return Landed::Newer,
        };
        let at = self.shard_of(&key);
        let mut state = write(&self.shards[at]);
        self.insert_held(&mut state, at, key, entry, segments, is_shadowed)
    }

    /// Apply a batch's moves in order, holding a shard once for each run of keys in it
    pub fn apply_moves<Book: Bookings>(
        &self,
        moves: &[KeyMove<'_>],
        segments: &Book,
        is_shadowed: &impl Fn(&[u8], Lsn) -> bool,
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
            landed.push(self.apply_held(&mut state, shard, key, &moves[at], segments, is_shadowed));
            at += 1;
            while at < moves.len() {
                let Some(next) = K::from_slice(moves[at].key) else {
                    break;
                };
                if self.shard_of(&next) != shard {
                    break;
                }
                landed.push(self.apply_held(
                    &mut state,
                    shard,
                    next,
                    &moves[at],
                    segments,
                    is_shadowed,
                ));
                at += 1;
            }
        }
    }

    /// One move of either kind, with its shard already held
    fn apply_held<Book: Bookings>(
        &self,
        state: &mut ShardState<K, S>,
        shard: usize,
        key: K,
        moving: &KeyMove<'_>,
        segments: &Book,
        is_shadowed: &impl Fn(&[u8], Lsn) -> bool,
    ) -> Landed {
        match moving.is_delete {
            true => {
                segments.mark_held(
                    moving.loc.segment,
                    moving.lsn,
                    span_of(key.width(), moving.loc.len),
                );
                self.remove_held(
                    state,
                    shard,
                    key,
                    moving.lsn,
                    moving.loc,
                    segments,
                    is_shadowed,
                )
            }
            false => self.insert_held(
                state,
                shard,
                key,
                Entry::new(moving.loc, moving.lsn),
                segments,
                is_shadowed,
            ),
        }
    }

    /// The insert itself, with the key parsed and its shard already held
    fn insert_held<Book: Bookings>(
        &self,
        state: &mut ShardState<K, S>,
        at: usize,
        key: K,
        entry: Entry,
        segments: &Book,
        is_shadowed: &impl Fn(&[u8], Lsn) -> bool,
    ) -> Landed {
        // The record's hold keeps its segment from retiring, so the stamp can be issued live here
        let entry = entry.stamped(segments.live_incarnation(entry.loc.segment));
        let (loc, lsn) = (entry.loc, entry.lsn);
        let was_empty = state.map.vacant();
        let new_len = u64::from(loc.len);

        // The cover test runs under the shard lock, which orders it against the sweep
        if self.is_covered(key.as_slice(), lsn) {
            segments.mark_dead(loc.segment, lsn, span_of(key.width(), loc.len));
            return Landed::Newer;
        }

        // Put first and restore on refusal, so an accepted put is one descent
        self.filters.note(at, filter_hash(key.as_slice()));
        // Taken before the map takes the key, since a variable key owns its bytes
        let width = key.width();
        let landed = match state.map.put(key.clone(), entry) {
            // A grave this new refuses the put like any newer version
            Some(existing) if existing.lsn >= lsn => {
                state.map.put(key, existing);
                segments.mark_dead(loc.segment, lsn, span_of(width, loc.len));
                return Landed::Newer;
            }
            // An older grave holds only a place, so the segment table hears nothing
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
            // A hand-over took the map's version and its sequence number with it
            None if is_shadowed(key.as_slice(), lsn) => {
                state.map.take(key.as_slice());
                segments.mark_dead(loc.segment, lsn, span_of(width, loc.len));
                return Landed::Newer;
            }
            None => {
                state.bytes += new_len;
                Landed::Nothing
            }
        };

        segments.mark_live(loc.segment, lsn, span_of(width, loc.len));
        self.note_filled(at, was_empty);
        self.note_held(at, state.map.vacant());
        landed
    }

    /// Drop a key on a tombstone, guarded by its sequence number, leaving a grave
    pub fn remove(&self, key: &[u8], lsn: Lsn, tombstone: Loc, segments: &SegmentTable) -> Landed {
        self.remove_unless(key, lsn, tombstone, segments, &never_shadowed)
    }

    /// The same delete, refused over an empty place where `is_shadowed` finds a newer version
    pub fn remove_unless(
        &self,
        key: &[u8],
        lsn: Lsn,
        tombstone: Loc,
        segments: &SegmentTable,
        is_shadowed: &impl Fn(&[u8], Lsn) -> bool,
    ) -> Landed {
        let key = match K::from_slice(key) {
            Some(key) => key,
            None => return Landed::Newer,
        };
        // The tombstone holds space in its segment whatever it does to the key
        segments.mark_held(tombstone.segment, lsn, span_of(key.width(), tombstone.len));
        let at = self.shard_of(&key);
        let mut state = write(&self.shards[at]);
        self.remove_held(&mut state, at, key, lsn, tombstone, segments, is_shadowed)
    }

    /// Stand a grave for a tombstone that compaction copied, unless a newer version stands
    pub fn hold_grave(
        &self,
        key: &[u8],
        lsn: Lsn,
        segment: SegmentId,
        is_shadowed: impl FnOnce() -> bool,
    ) {
        let Some(key) = K::from_slice(key) else {
            return;
        };
        let at = self.shard_of(&key);
        let mut state = write(&self.shards[at]);
        let was_empty = state.map.vacant();
        let existing = state.map.at(key.as_slice()).copied();
        // The shard stays held through `is_shadowed`, for a newer version the map cannot see
        if existing.is_some_and(|existing| !existing.is_grave() || existing.lsn > lsn)
            || is_shadowed()
        {
            return;
        }
        // An older grave is replaced by this one and never counted again
        if existing.is_some() {
            state.graves -= 1;
        }
        self.filters.note(at, filter_hash(key.as_slice()));
        // The tombstone's span is already booked, so the segment counters stay put
        state.map.put(key, Entry::grave_from(lsn, segment));
        state.note_grave(lsn);
        self.note_filled(at, was_empty);
        self.note_held(at, state.map.vacant());
    }

    /// Take out a key's grave while it still holds this tombstone's number
    pub fn drop_grave(&self, key: &[u8], lsn: Lsn) -> bool {
        let Some(key) = K::from_slice(key) else {
            return false;
        };
        let at = self.shard_of(&key);
        let mut state = write(&self.shards[at]);
        let held = state.map.at(key.as_slice()).copied();
        if !held.is_some_and(|entry| entry.is_grave() && entry.lsn == lsn) {
            return false;
        }
        state.map.take(key.as_slice());
        state.graves -= 1;
        self.note_emptied(at, &mut state);
        true
    }

    /// The tombstone itself, with the key parsed, its shard held and its span already booked
    #[allow(clippy::too_many_arguments)]
    fn remove_held<Book: Bookings>(
        &self,
        state: &mut ShardState<K, S>,
        at: usize,
        key: K,
        lsn: Lsn,
        tombstone: Loc,
        segments: &Book,
        is_shadowed: &impl Fn(&[u8], Lsn) -> bool,
    ) -> Landed {
        let was_empty = state.map.vacant();
        let existing = state.map.at(key.as_slice()).copied();
        if let Some(existing) = existing {
            if existing.lsn >= lsn {
                // A copy of the same tombstone moves its grave to the segment the copy landed in
                if existing.is_grave() && existing.lsn == lsn {
                    state
                        .map
                        .put(key, Entry::grave_from(lsn, tombstone.segment));
                }
                return Landed::Newer;
            }
        }

        let landed = match existing {
            Some(existing) if !existing.is_grave() => {
                self.drop_entry(state, &key, existing, segments);
                Landed::Record
            }
            // An older grave is replaced by this one and never counted again
            Some(_) => {
                state.graves -= 1;
                Landed::Grave
            }
            // A hand-over took the map's version and its sequence number with it
            None if is_shadowed(key.as_slice(), lsn) => return Landed::Newer,
            None => Landed::Nothing,
        };

        self.filters.note(at, filter_hash(key.as_slice()));
        state
            .map
            .put(key.clone(), Entry::grave_from(lsn, tombstone.segment));
        state.note_grave(lsn);
        self.note_filled(at, was_empty);
        self.note_held(at, state.map.vacant());
        landed
    }

    /// Book a record that only a footer held as gone, once the caller has resolved it
    pub fn settle_paged(&self, key: &[u8], loc: Loc, segments: &SegmentTable) -> bool {
        let Some(key) = K::from_slice(key) else {
            return false;
        };
        let at = self.shard_of(&key);
        let mut state = write(&self.shards[at]);
        settle(&mut state, loc, segments, key.width(), true);
        true
    }

    /// Book a footer-held record at `loc` gone without a read, taking `least` off the live bytes
    pub fn settle_paged_least(
        &self,
        key: &[u8],
        loc: Loc,
        least: u32,
        segments: &SegmentTable,
    ) -> bool {
        let Some(key) = K::from_slice(key) else {
            return false;
        };
        let at = self.shard_of(&key);
        let mut state = write(&self.shards[at]);
        state.paged = state.paged.saturating_sub(1);
        // A class's least length never exceeds the record's, so a later rebook lands exactly
        state.bytes = state.bytes.saturating_sub(u64::from(least));
        segments.shadow(loc.segment, span_of(key.width(), loc.len));
        true
    }

    /// Count sealed records an open put in the spot index, one lock per run of keys in a shard
    pub fn book_sealed<'a>(&self, rows: impl Iterator<Item = (&'a [u8], u32)>) {
        let mut run: Option<(usize, usize, u64)> = None;
        for (key, len) in rows {
            let at = self.shard_of_bytes(key);
            match &mut run {
                Some((shard, keys, bytes)) if *shard == at => {
                    *keys += 1;
                    *bytes += u64::from(len);
                }
                _ => {
                    if let Some((shard, keys, bytes)) = run.replace((at, 1, u64::from(len))) {
                        self.book(shard, keys, bytes, None);
                    }
                }
            }
        }
        if let Some((shard, keys, bytes)) = run {
            self.book(shard, keys, bytes, None);
        }
    }

    /// Move one key's sealed count from the version that went to the one that came
    pub fn book_paged(&self, key: &[u8], booking: Booking) {
        let (keys, bytes) = booking.came.map_or((0, 0), |len| (1, u64::from(len)));
        self.book(self.shard_of_bytes(key), keys, bytes, booking.gone);
    }

    /// Add sealed keys and their bytes to a shard, less the one version that went
    fn book(&self, at: usize, keys: usize, bytes: u64, gone: Option<u32>) {
        let mut state = write(&self.shards[at]);
        let was_empty = state.map.vacant() && state.paged == 0;
        state.paged += keys;
        state.bytes += bytes;
        if let Some(len) = gone {
            state.paged = state.paged.saturating_sub(1);
            state.bytes = state.bytes.saturating_sub(u64::from(len));
        }
        self.note_filled(at, was_empty && keys > 0);
    }

    /// Swap the length a class booked for a record's true length, once it is known
    pub fn rebook_paged(&self, key: &[u8], booked: u32, actual: u32) {
        let Some(key) = K::from_slice(key) else {
            return;
        };
        let at = self.shard_of(&key);
        let mut state = write(&self.shards[at]);
        state.bytes = state
            .bytes
            .saturating_add(u64::from(booked))
            .saturating_sub(u64::from(actual));
    }

    /// Take a paged key out with a grave of its own, for a record that will not read
    pub fn evict_paged(
        &self,
        key: &[u8],
        loc: Loc,
        lsn: Lsn,
        segments: &SegmentTable,
        take: impl FnOnce() -> bool,
    ) -> bool {
        let Some(key) = K::from_slice(key) else {
            return false;
        };
        let at = self.shard_of(&key);
        let mut state = write(&self.shards[at]);
        let was_empty = state.map.vacant();
        if state.map.holds(key.as_slice()) {
            return false;
        }
        settle(&mut state, loc, segments, key.width(), take());

        self.filters.note(at, filter_hash(key.as_slice()));
        state.map.put(key, Entry::grave(lsn));
        state.note_grave(lsn);
        self.note_filled(at, was_empty);
        self.note_held(at, state.map.vacant());
        true
    }

    /// Bring a paged key back at its rewritten copy, leaving segment bookings to the caller
    pub fn repoint_paged(
        &self,
        key: &[u8],
        to: Loc,
        lsn: Lsn,
        stamp: SegmentIncarnation,
        take: impl FnOnce() -> bool,
    ) -> bool {
        let Some(key) = K::from_slice(key) else {
            return false;
        };
        let at = self.shard_of(&key);
        let mut state = write(&self.shards[at]);
        let was_empty = state.map.vacant();
        // A key rewritten or deleted since it was resolved keeps whatever took its place
        if state.map.holds(key.as_slice()) {
            return false;
        }
        self.filters.note(at, filter_hash(key.as_slice()));
        state.map.put(key, Entry::new(to, lsn).stamped(stamp));
        // A version the spot index held live was counted there, and one it did not counts fresh
        match take() {
            true => state.paged = state.paged.saturating_sub(1),
            false => state.bytes += u64::from(to.len),
        }
        self.note_filled(at, was_empty);
        self.note_held(at, state.map.vacant());
        true
    }

    /// Whether a range delete took a version this new, asked of a footer's answer
    pub fn is_covered_key(&self, key: &[u8], lsn: Lsn) -> bool {
        match K::accepts(key) {
            true => self.is_covered(key, lsn),
            false => false,
        }
    }

    /// The same test as a reader at an older sequence number would make it
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
    pub fn entry_or_grave(&self, key: &[u8]) -> Option<Entry> {
        if !K::accepts(key) {
            return None;
        }
        let at = self.shard_of_bytes(key);
        if !self.filters.may_hold(at, filter_hash(key)) {
            return None;
        }
        read(&self.shards[at]).map.at(key).copied()
    }

    /// The same for many keys at once, answered in the order asked
    pub fn entry_many(&self, keys: &[RecordKey], run: &[usize], out: &mut Vec<Option<Entry>>) {
        out.clear();
        out.resize(run.len(), None);

        // Keys the filter rules out or the column cannot hold are absent and take no lock
        let mut held = HeldGroups::take();
        let groups = &mut held.0;
        groups.wanted.clear();
        for (at, index) in run.iter().enumerate() {
            let key = keys[*index].as_slice();
            let shard = self.shard_of_bytes(key);
            if !self.filters.may_hold(shard, filter_hash(key)) {
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

            // A short run is looked up key by key under one lock
            if end - at < BATCH_RUN {
                let state = read(&self.shards[shard]);
                for (_, slot) in &groups.wanted[at..end] {
                    let key = keys[run[*slot as usize]].as_slice();
                    out[*slot as usize] = state.map.at(key).copied();
                }
                at = end;
                continue;
            }

            // The run goes in the order it was asked, since `at_many` needs no order
            let mut staged: Vec<K> = Vec::with_capacity(end - at);
            staged.extend(
                groups.wanted[at..end]
                    .iter()
                    .filter_map(|(_, slot)| K::from_slice(keys[run[*slot as usize]].as_slice())),
            );
            // `accepts` above is the test `from_slice` makes, so the zip below lines up
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

    /// Whether a range delete already took a record this new
    fn is_covered(&self, key: &[u8], lsn: Lsn) -> bool {
        if !self.has_covers.load(Ordering::Relaxed) {
            return false;
        }
        read(&self.covers)
            .iter()
            .any(|cover| cover.covers(key, lsn))
    }

    /// How many covers the column still tests inserts against
    pub fn cover_count(&self) -> u64 {
        read(&self.covers).len() as u64
    }

    /// Drop what tombstones are holding once nothing older can still be published
    pub fn prune_tombstones(&self, before: Lsn, sealed: &SealedRanges) -> u64 {
        let mut pruned = self.prune_covers(before, sealed);
        pruned += self.prune_graves(before, sealed);
        pruned
    }

    /// Drop swept covers once nothing older than them can still be published
    fn prune_covers(&self, before: Lsn, sealed: &SealedRanges) -> u64 {
        if !self.has_covers.load(Ordering::Relaxed) {
            return 0;
        }
        let mut covers = write(&self.covers);
        let before_len = covers.len();
        let mut lifted = Lsn::NONE;
        covers.retain(|cover| {
            let keep = !matches!(cover.phase, SweepPhase::Done)
                || cover.lsn > before
                || cover.reaches_sealed(sealed);
            if !keep {
                lifted = lifted.max(cover.lsn);
            }
            keep
        });
        self.lifted.fetch_max(lifted.as_u64(), Ordering::AcqRel);
        // Clear the flag with the list held, which orders a reader of false after the emptying
        if covers.is_empty() {
            self.has_covers.store(false, Ordering::Relaxed);
        }
        (before_len - covers.len()) as u64
    }

    fn prune_graves(&self, before: Lsn, sealed: &SealedRanges) -> u64 {
        // Taken once per pass so the test below takes no other lock under the shard
        let mut snapshot: Option<Vec<SegmentId>> = None;
        let occupied: Vec<usize> = read(&self.occupied)
            .iter()
            .map(|(at, _)| *at as usize)
            .collect();
        let mut pruned = 0u64;
        let mut lifted = Lsn::NONE;
        for at in occupied {
            let mut state = write(&self.shards[at]);
            if state.graves == 0 {
                continue;
            }
            // The floor can only understate, so a shard with all graves past the line is skipped
            if state.oldest_grave > before {
                continue;
            }
            let standing = snapshot.get_or_insert_with(|| sealed.segments());
            let mut doomed: Vec<K> = Vec::new();
            let mut oldest_left = Lsn(u64::MAX);
            for (key, entry) in state.map.walk() {
                if !entry.is_grave() {
                    continue;
                }
                let prunable = entry.lsn <= before
                    && entry
                        .grave_origin()
                        .is_some_and(|from| standing.binary_search(&from).is_ok());
                if prunable {
                    doomed.push(key.clone());
                    lifted = lifted.max(entry.lsn);
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
            // Pack the room the deletes left, since the map never merges
            state.map.pack_owed();
            self.note_emptied(at, &mut state);
        }
        self.lifted.fetch_max(lifted.as_u64(), Ordering::AcqRel);
        pruned
    }

    /// How many graves the column holds, the memory a prune would give back
    pub fn grave_count(&self) -> u64 {
        self.sum_shards(|state| state.graves as u64)
    }

    /// Sum one number over every occupied shard, without a snapshot
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
    pub fn remove_range(&self, start: &[u8], end: Option<&[u8]>, lsn: Lsn) {
        self.push_cover(start, end.map(K::low_bound), lsn);
    }

    /// The cover the sweep should settle next, oldest first
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

    /// Drop one bounded run of a cover's map entries, returning (dropped, examined, finished)
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
            // The trait borrows its bounds, so the high end lives here for the walk
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
                }
                for (segment, span) in per_segment.iter() {
                    segments.shadow(*segment, *span);
                }
                state.bytes = state.bytes.saturating_sub(freed);
                dropped += doomed.len() as u64;
                // Once per run, since the whole run goes under one take of the shard
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

    /// Settle a covered footer-held record if the map lacks the key and `take` finds its slot
    pub fn release_covered(
        &self,
        key: &[u8],
        loc: Loc,
        segments: &SegmentTable,
        take: impl FnOnce() -> bool,
    ) -> bool {
        let Some(key) = K::from_slice(key) else {
            return false;
        };
        let at = self.shard_of(&key);
        let mut state = write(&self.shards[at]);
        if state.map.holds(key.as_slice()) || !take() {
            return false;
        }
        settle(&mut state, loc, segments, key.width(), true);
        self.note_emptied(at, &mut state);
        true
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

    /// Whether any range delete stands over the column
    pub fn has_covers(&self) -> bool {
        self.has_covers.load(Ordering::Relaxed)
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

    /// Resolve a key to its live entry, with graves and covered entries as absent
    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        if !K::accepts(key) {
            return None;
        }
        let at = self.shard_of_bytes(key);
        if !self.filters.may_hold(at, filter_hash(key)) {
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

    /// Repoint a key to its rewritten copy while it holds the same version, returning the old place
    pub fn repoint(
        &self,
        key: &[u8],
        new_loc: Loc,
        expected_lsn: Lsn,
        stamp: SegmentIncarnation,
    ) -> Option<Loc> {
        let key = K::from_slice(key)?;
        let at = self.shard_of(&key);
        let mut state = write(&self.shards[at]);
        // Changed in place, so the map is walked once
        match state.map.at_mut(key.as_slice()) {
            Some(existing) if existing.lsn == expected_lsn && !existing.is_grave() => {
                let from = existing.loc;
                self.filters.note(at, filter_hash(key.as_slice()));
                *existing = existing.moved_to(new_loc, stamp);
                Some(from)
            }
            Some(_) | None => None,
        }
    }

    /// Drop a key while it still resolves one exact location, writing no tombstone
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

    /// The newest version a pruned grave or cover guarded
    pub fn lifted(&self) -> Lsn {
        Lsn(self.lifted.load(Ordering::Acquire))
    }

    /// Give a key up to its segment's footer if it still points there and no cover holds it
    pub fn page_out(&self, key: &[u8], loc: Loc) -> bool {
        self.page_out_lane(&[(key, loc)], 0, 1, &|_, _| {})[0]
    }

    /// Page out one lane's keys, calling `first` on each under the lock, and report which went
    pub fn page_out_lane(
        &self,
        rows: &[(&[u8], Loc)],
        lane: usize,
        lanes: usize,
        first: &(dyn Fn(&[u8], Loc) + Sync),
    ) -> Vec<bool> {
        let mut handed = vec![false; rows.len()];
        let groups = self.lane_shards(rows, lane, lanes);
        for (shard, chunk) in in_turns(&groups, LOCK_CHUNK) {
            let mut state = write(&self.shards[shard]);
            for &at in chunk {
                let (key, loc) = rows[at];
                let Some(key) = K::from_slice(key) else {
                    continue;
                };
                if !self.holds_at(&state, &key, loc) {
                    continue;
                }
                first(key.as_slice(), loc);
                state.map.take(key.as_slice());
                state.paged += 1;
                handed[at] = true;
            }
            state.map.pack_owed();
            self.note_emptied(shard, &mut state);
        }
        handed
    }

    /// Whether the map points a key at exactly this place, with no grave or cover over it
    pub fn holds(&self, key: &[u8], loc: Loc) -> bool {
        let Some(key) = K::from_slice(key) else {
            return false;
        };
        let state = read(&self.shards[self.shard_of(&key)]);
        self.holds_at(&state, &key, loc)
    }

    /// The rows of one lane, grouped by the shard each key falls in
    fn lane_shards(
        &self,
        rows: &[(&[u8], Loc)],
        lane: usize,
        lanes: usize,
    ) -> Vec<(usize, Vec<usize>)> {
        let mut by_shard: Vec<Vec<usize>> = vec![Vec::new(); self.shards.len()];
        for (at, (key, _)) in rows.iter().enumerate() {
            if let Some(key) = K::from_slice(key) {
                let shard = self.shard_of(&key);
                if shard % lanes == lane {
                    by_shard[shard].push(at);
                }
            }
        }
        by_shard
            .into_iter()
            .enumerate()
            .filter(|(_, ats)| !ats.is_empty())
            .collect()
    }

    /// Whether the map points a key at exactly this place, with no grave or cover over it
    fn holds_at(&self, state: &ShardState<K, S>, key: &K, loc: Loc) -> bool {
        state.map.at(key.as_slice()).is_some_and(|existing| {
            existing.loc == loc
                && !existing.is_grave()
                && !self.is_covered(key.as_slice(), existing.lsn)
        })
    }

    /// Every entry the column holds, graves included, in key order
    pub fn held(&self) -> Vec<(KeyBytes, Entry)> {
        let mut out = Vec::new();
        for at in self.occupied_range(0, self.shards.len() - 1) {
            let state = read(&self.shards[at]);
            for (key, entry) in state.map.walk() {
                if let Ok(key) = KeyBytes::new(key.as_slice()) {
                    out.push((key, *entry));
                }
            }
        }
        out
    }

    /// Take out an entry a rebuild found outversioned, booking a record dead
    pub fn drop_shadowed(&self, key: &[u8], segments: &SegmentTable) {
        let Some(key) = K::from_slice(key) else {
            return;
        };
        let at = self.shard_of(&key);
        let mut state = write(&self.shards[at]);
        match state.map.at(key.as_slice()).copied() {
            Some(existing) if existing.is_grave() => {
                state.map.take(key.as_slice());
                state.graves -= 1;
            }
            Some(existing) => self.drop_entry(&mut state, &key, existing, segments),
            None => return,
        }
        self.note_emptied(at, &mut state);
    }

    /// Size every shard's map to what it holds, once a rebuild has filled it
    pub fn fit(&self) {
        for at in self.occupied_range(0, self.shards.len() - 1) {
            write(&self.shards[at]).map.fit();
        }
    }

    /// Raise one cover, the shape a range delete and a rebuild share
    fn push_cover(&self, start: &[u8], high: Option<K>, lsn: Lsn) {
        let low = K::low_bound(start);
        let mut covers = write(&self.covers);
        // A copied range delete is in two segments, so a cover at this number is this one
        if covers.iter().any(|cover| cover.lsn == lsn) {
            return;
        }
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
            self.note_held(at, true);
            self.filters.clear(at);
            state.bytes = 0;
            state.graves = 0;
            // A rebuild counts its sealed keys again, so the paged count starts from zero
            state.paged = 0;
        }
        write(&self.occupied).clear();
        // A rebuild reads no sealed row, so it puts its ranges back after this
        write(&self.covers).clear();
        self.has_covers.store(false, Ordering::Relaxed);
    }

    /// Live key count and payload byte total for the column, summed without a snapshot
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

    /// How many places the shards' maps hold, graves included and paged keys left out
    pub fn resident_keys(&self) -> u64 {
        self.sum_shards(|state| state.map.count() as u64)
    }

    /// The share of neighbouring keys with equal leads, weighted by keys, none when empty
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

    /// Live totals for one shard-aligned prefix, read from the shard's own counters
    pub fn prefix_totals(&self, prefix: &[u8]) -> Option<Totals> {
        if prefix.len() != self.shard_bytes as usize || self.shard_bytes == 0 {
            return None;
        }
        // An unswept cover leaves dead entries in the counters, so the caller walks
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
    pub fn page(&self, start: Bound<&[u8]>, limit: usize, out: &mut KeyPage) {
        out.clear();
        if limit == 0 {
            return;
        }
        // Sized once, so a walk's first page does not regrow its buffers a key at a time
        out.reserve(limit, usize::from(self.declared_width));
        let first = match start {
            Bound::Unbounded => 0,
            Bound::Included(key) | Bound::Excluded(key) => self.shard_of_bytes(key),
        };
        for at in self.filled_range(first, self.shards.len() - 1) {
            // A page fill holds no publish barrier, so a batch can land between shards
            crate::sync::rendezvous::at("index/page-shard");
            let bound = match at == first {
                true => low_bound::<K>(start),
                false => Bound::Unbounded,
            };
            // The trait borrows its bounds, so the owned one lives here for the walk
            let low = borrowed(&bound);
            let state = read(&self.shards[at]);
            // A merge checks graves and covers itself, so its page takes every entry
            let judged = out.keeps_graves();
            let covers = self.has_covers.load(Ordering::Relaxed);
            for (keys, entries) in state.map.span_runs(low) {
                // A run with nothing dead goes over as one copy of keys and one of entries
                let take = keys.len().min(limit - out.len());
                let clean = judged || (!covers && !entries[..take].iter().any(Entry::is_grave));
                let width = keys.first().map_or(0, |key| key.as_slice().len());
                let packed = clean
                    && K::packed(&keys[..take])
                        .is_some_and(|bytes| out.push_packed(bytes, width, &entries[..take]));
                if !packed {
                    // Dead entries are skipped, so the whole run is walked until the page fills.
                    for (key, entry) in keys.iter().zip(entries) {
                        if judged
                            || (!entry.is_grave() && !self.is_covered(key.as_slice(), entry.lsn))
                        {
                            out.push(key.as_slice(), *entry);
                            if out.len() >= limit {
                                return;
                            }
                        }
                    }
                }
                if out.len() >= limit {
                    return;
                }
            }
        }
    }

    /// Fill a page with one bounded run of live keys, descending from a bound
    pub fn page_back(&self, end: Bound<&[u8]>, limit: usize, out: &mut KeyPage) {
        out.clear();
        out.reserve(limit, usize::from(self.declared_width));
        if limit == 0 {
            return;
        }
        let last = match end {
            Bound::Unbounded => self.shards.len() - 1,
            Bound::Included(key) | Bound::Excluded(key) => self.shard_of_bytes(key),
        };
        for at in self.filled_back(last) {
            let bound = match at == last {
                true => high_bound::<K>(end),
                false => Bound::Unbounded,
            };
            let high = borrowed(&bound);
            let state = read(&self.shards[at]);
            let judged = out.keeps_graves();
            for (key, entry) in
                state
                    .map
                    .span_back(Bound::Unbounded, high)
                    .filter(|(key, entry)| {
                        judged || (!entry.is_grave() && !self.is_covered(key.as_slice(), entry.lsn))
                    })
            {
                out.push(key.as_slice(), *entry);
                if out.len() >= limit {
                    return;
                }
            }
        }
    }

    /// Take one entry out of a shard, moving its bytes to dead and its totals down
    fn drop_entry<Book: Bookings>(
        &self,
        state: &mut ShardState<K, S>,
        key: &K,
        existing: Entry,
        segments: &Book,
    ) {
        segments.shadow(existing.loc.segment, existing.span(key.width()));
        let len = u64::from(existing.loc.len);
        state.bytes = state.bytes.saturating_sub(len);
        state.map.take(key.as_slice());
    }

    /// Add a shard that just took its first key to the walk set
    fn note_filled(&self, at: usize, was_empty: bool) {
        if was_empty {
            write(&self.occupied).insert(at as u64, ());
        }
    }

    /// Clean up after a shard's map empties, keeping it on the walk while it has paged keys
    fn note_emptied(&self, at: usize, state: &mut ShardState<K, S>) {
        if state.map.vacant() {
            self.filters.clear(at);
            self.note_held(at, true);
        }
        if state.map.vacant() && state.paged == 0 {
            // Release the room now, since no pass visits a shard outside the walk set
            state.map.release();
            write(&self.occupied).remove(&(at as u64));
        }
    }

    /// Set or clear a shard's bit in `filled` under its write lock, writing only on a change
    fn note_held(&self, at: usize, is_vacant: bool) {
        let (word, bit) = (&self.filled[at / 64], 1u64 << (at % 64));
        let is_set = word.load(Ordering::Relaxed) & bit != 0;
        match (is_vacant, is_set) {
            (false, false) => {
                word.fetch_or(bit, Ordering::Release);
            }
            (true, true) => {
                word.fetch_and(!bit, Ordering::Release);
            }
            (false, true) | (true, false) => {}
        }
    }

    /// Shards within an inclusive range whose map holds an entry, in key order
    fn filled_range(&self, first: usize, last: usize) -> impl Iterator<Item = usize> + '_ {
        let mut at = first;
        std::iter::from_fn(move || {
            while at <= last {
                let bits = self.filled.get(at / 64)?.load(Ordering::Acquire) >> (at % 64);
                if bits == 0 {
                    at = (at / 64 + 1) * 64;
                    continue;
                }
                let found = at + bits.trailing_zeros() as usize;
                if found > last {
                    return None;
                }
                at = found + 1;
                return Some(found);
            }
            None
        })
    }

    /// Shards at or below a shard whose map holds an entry, in descending key order
    fn filled_back(&self, last: usize) -> impl Iterator<Item = usize> + '_ {
        let mut high = Some(last);
        std::iter::from_fn(move || {
            while let Some(at) = high {
                let below = 63 - at % 64;
                let bits = self.filled.get(at / 64)?.load(Ordering::Acquire) << below;
                if bits == 0 {
                    high = (at / 64).checked_sub(1).map(|word| word * 64 + 63);
                    continue;
                }
                let found = at - bits.leading_zeros() as usize;
                high = found.checked_sub(1);
                return Some(found);
            }
            None
        })
    }

    /// Occupied shards in an inclusive range, in key order, looked up as the walk reaches each
    fn occupied_range(&self, first: usize, last: usize) -> impl Iterator<Item = usize> + '_ {
        let (mut low, high) = (first as u64, last as u64);
        std::iter::from_fn(move || {
            let at = *read(&self.occupied)
                .range(Bound::Included(&low), Bound::Included(&high))
                .next()?
                .0;
            low = at + 1;
            Some(at as usize)
        })
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

/// Book a footer-held record gone, taking it off the shard's count when `counted`
fn settle<K: IndexKey, S: Shape<K>>(
    state: &mut ShardState<K, S>,
    loc: Loc,
    segments: &SegmentTable,
    key_width: u16,
    counted: bool,
) {
    if counted {
        state.paged = state.paged.saturating_sub(1);
        state.bytes = state.bytes.saturating_sub(u64::from(loc.len));
    }
    segments.shadow(loc.segment, span_of(key_width, loc.len));
}

/// A packed mark spends this many bytes on its opening's nonce
const NONCE_LEN: usize = 8;

/// Where a sweep of a whole column left off, at the last key it handed out
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnMark {
    /// The opening that minted this mark
    pub nonce: u64,

    /// The last key the sweep handed out
    pub after: Box<[u8]>,
}

impl ColumnMark {
    /// The mark as opaque bytes, for a caller to send over a wire or keep across a restart
    pub fn pack(&self) -> Vec<u8> {
        let mut packed = Vec::with_capacity(NONCE_LEN + self.after.len());
        packed.extend_from_slice(&self.nonce.to_le_bytes());
        packed.extend_from_slice(&self.after);
        packed
    }

    /// A mark read back from bytes, or none when they do not decode
    pub fn unpack(packed: &[u8]) -> Option<ColumnMark> {
        let (nonce, after) = packed.split_at_checked(NONCE_LEN)?;
        Some(ColumnMark {
            nonce: u64::from_le_bytes(nonce.try_into().ok()?),
            after: Box::from(after),
        })
    }
}

/// The map type that holds one shard's keys
pub trait ShardMap<K: IndexKey, V: 'static>: Default {
    /// Put a value in, handing back the one it displaced
    fn put(&mut self, key: K, val: V) -> Option<V>;

    /// The value held for a key, looked up by borrowed bytes
    fn at(&self, key: &[u8]) -> Option<&V>;

    /// The value held for a key, mutable in place
    fn at_mut(&mut self, key: &[u8]) -> Option<&mut V>;

    /// Whether a key is held at all
    fn holds(&self, key: &[u8]) -> bool;

    /// Take a key out, handing back what it held
    fn take(&mut self, key: &[u8]) -> Option<V>;

    /// How many keys the map holds, graves included
    fn count(&self) -> usize;

    /// Whether the shard holds nothing
    fn vacant(&self) -> bool;

    /// The map's heap allocation in bytes, spare room included
    fn heap_bytes(&self) -> u64;

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

    /// Pairs from a low bound on, in runs of one pair unless the map keeps leaves
    fn span_runs<'a>(
        &'a self,
        low: Bound<&'a K>,
    ) -> Box<dyn Iterator<Item = (&'a [K], &'a [V])> + 'a> {
        Box::new(
            self.span(low, Bound::Unbounded)
                .map(|(key, val)| (std::slice::from_ref(key), std::slice::from_ref(val))),
        )
    }

    /// Many keys at once, answered in the order asked
    fn at_many<'a>(&'a self, keys: &[K], out: &mut Vec<Option<&'a V>>) {
        out.clear();
        for key in keys {
            out.push(self.at(key.borrow()));
        }
    }

    /// Give back the room a map grown a key at a time holds past its fill
    fn fit(&mut self) {}

    /// Pack the map once deletion has doubled its spare room
    fn pack_owed(&mut self) {}

    /// Give the map's room back, for a shard that holds nothing and leaves the walk
    fn release(&mut self) {
        *self = Self::default();
    }

    /// The share of neighbouring keys with equal leads, none for a map without leads
    fn lead_tie_rate(&self) -> Option<f64> {
        None
    }
}

/// A variable column's shard, with keys on the heap and pointers in the node
impl<const B: usize, V: Default + 'static> ShardMap<Box<[u8]>, V> for TBTreeMap<Box<[u8]>, B, V> {
    fn put(&mut self, key: Box<[u8]>, val: V) -> Option<V> {
        self.insert(key, val)
    }

    fn at(&self, key: &[u8]) -> Option<&V> {
        self.get(key)
    }

    fn at_mut(&mut self, key: &[u8]) -> Option<&mut V> {
        self.get_mut(key)
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

    fn heap_bytes(&self) -> u64 {
        TBTreeMap::heap_bytes(self)
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

    fn fit(&mut self) {
        if !self.is_packed() {
            self.repack(B);
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

/// A fixed column's shard, with its keys inline in the tree
impl<const N: usize, const B: usize, V: Default + 'static> ShardMap<[u8; N], V>
    for TBTreeMap<[u8; N], B, V>
{
    fn put(&mut self, key: [u8; N], val: V) -> Option<V> {
        self.insert(key, val)
    }

    fn at(&self, key: &[u8]) -> Option<&V> {
        // A probe of the wrong width is a key this column cannot hold, so it is absent
        let key: &[u8; N] = key.try_into().ok()?;
        self.get(key)
    }

    fn at_mut(&mut self, key: &[u8]) -> Option<&mut V> {
        let key: &[u8; N] = key.try_into().ok()?;
        self.get_mut(key)
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

    fn heap_bytes(&self) -> u64 {
        TBTreeMap::heap_bytes(self)
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

    fn span_runs<'a>(
        &'a self,
        low: Bound<&'a [u8; N]>,
    ) -> Box<dyn Iterator<Item = (&'a [[u8; N]], &'a [V])> + 'a> {
        Box::new(self.range_runs(low))
    }

    fn at_many<'a>(&'a self, keys: &[[u8; N]], out: &mut Vec<Option<&'a V>>) {
        self.get_many_sorted(keys, out);
    }

    fn fit(&mut self) {
        if !self.is_packed() {
            self.repack(B);
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

/// Which map a column's shards are built from, chosen per column by its key type
pub trait Shape<K: IndexKey> {
    /// Where the shard's entries live
    type Entries: ShardMap<K, Entry>;
}

/// The shape a declared width allows, and what every fixed column takes
pub struct Trees<const N: usize>;

/// Give each declared key width the node width `node_width` sizes for it
macro_rules! tree_shapes {
    ($($width:literal),* $(,)?) => {
        $(
            impl Shape<[u8; $width]> for Trees<$width> {
                type Entries = TBTreeMap<[u8; $width], { node_width($width) }, Entry>;
            }
        )*
    };
}

tree_shapes!(0, 2, 8, 12, 16, 20, 24, 32, 34, 36, 40, 44, 48, 72, 96, 108);

/// Keys per node on a column whose keys have no declared width
pub const VAR_NODE_WIDTH: usize = 32;

/// The shape a column whose keys have no width takes
pub struct VarTrees;

impl Shape<Box<[u8]>> for VarTrees {
    type Entries = TBTreeMap<Box<[u8]>, VAR_NODE_WIDTH, Entry>;
}

/// A key as the resident map holds it, inline for a fixed column and boxed for a variable one
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

    /// Keys packed end to end, where the key type is laid out that way already
    fn packed(keys: &[Self]) -> Option<&[u8]> {
        let _ = keys;
        None
    }

    /// The key's width in bytes, used to measure a record's span
    fn width(&self) -> u16 {
        self.as_slice().len() as u16
    }
}

impl<const N: usize> IndexKey for [u8; N] {
    fn packed(keys: &[Self]) -> Option<&[u8]> {
        Some(keys.as_flattened())
    }

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

    /// A prefix is already the low end of everything that begins with it
    fn low_bound(bytes: &[u8]) -> Box<[u8]> {
        Box::from(bytes)
    }

    /// The prefix padded with 0xFF to `MAX_KEY_LEN`, above every key the format admits
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
        purge_mark: None,
        codec: Codec::None,
    };

    // keys of different lengths live in one column and answer for themselves
    #[test]
    fn a_variable_column_holds_every_length() {
        let index = ColumnIndex::new(&VARIABLE).expect("index");
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
                &never_shadowed,
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
        let index = ColumnIndex::new(&VARIABLE).expect("index");
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
                &never_shadowed,
            );
        }
        names.sort();

        let mut page = KeyPage::default();
        index.page(Bound::Unbounded, 64, &mut page);
        assert_eq!(keys_in(&page), names);
    }

    // an overwrite replaces the key in place and adds no second one
    #[test]
    fn a_variable_key_overwrites_in_place() {
        let index = ColumnIndex::new(&VARIABLE).expect("index");
        let segments = SegmentTable::new();
        let name = b"photos/2026/cat.jpg".as_slice();

        index.insert(
            name,
            Entry::new(loc(1, 0, 10), Lsn(1)),
            &segments,
            &never_shadowed,
        );
        index.insert(
            name,
            Entry::new(loc(1, 40, 10), Lsn(2)),
            &segments,
            &never_shadowed,
        );

        assert_eq!(index.totals().count, 1);
        assert_eq!(index.get(name).expect("present").lsn, Lsn(2));
    }

    const SHARDED: ColumnSpec = ColumnSpec {
        id: ColumnId(1),
        name: "record",
        key_width: KeyWidth::Fixed(34),
        shard_bytes: 2,
        purge_mark: None,
        codec: Codec::None,
    };

    const FLAT: ColumnSpec = ColumnSpec {
        id: ColumnId(2),
        name: "blob_data",
        key_width: KeyWidth::Fixed(32),
        shard_bytes: 0,
        purge_mark: None,
        codec: Codec::None,
    };

    /// One shard at the sharded width, so its filter gets every word a shard can own
    const SINGLE: ColumnSpec = ColumnSpec {
        id: ColumnId(4),
        name: "single",
        key_width: KeyWidth::Fixed(34),
        shard_bytes: 0,
        purge_mark: None,
        codec: Codec::None,
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

    /// Sealed ranges holding these segments, so a grave whose tombstone landed in one can go
    fn sealed(segments: &[u32]) -> SealedRanges {
        let sealed = SealedRanges::new();
        for segment in segments {
            let key = KeyBytes::new(&[0u8; 34]).expect("key");
            sealed.note(SegmentId(*segment), key.clone(), key);
        }
        sealed
    }

    /// One of many keys in the same shard, told apart by its number
    fn crowded(at: u32) -> Vec<u8> {
        let mut out = 7u16.to_be_bytes().to_vec();
        out.extend_from_slice(&at.to_be_bytes());
        out.resize(34, 0);
        out
    }

    // a paged shard that empties forgets its keys and still finds one put back
    #[test]
    fn a_paged_shard_clears_and_finds_a_reinsert() {
        let index: WidthIndex<[u8; 34], Trees<34>> = WidthIndex::new(&SHARDED);
        let segments = SegmentTable::new();
        for byte in 0..50u8 {
            let entry = Entry::new(loc(1, u32::from(byte), 10), Lsn(u64::from(byte) + 1));
            index.insert(&key(7, byte), entry, &segments);
        }
        for byte in 0..50u8 {
            assert!(index.page_out(&key(7, byte), loc(1, u32::from(byte), 10)));
        }
        assert!(
            !index.filters.may_hold(7, filter_hash(&key(7, 3))),
            "the emptied shard's filter still holds its keys"
        );

        index.insert(&key(7, 3), Entry::new(loc(2, 0, 10), Lsn(100)), &segments);
        assert_eq!(
            index.get(&key(7, 3)).map(|entry| entry.loc),
            Some(loc(2, 0, 10))
        );
        assert!(
            index.get(&key(7, 4)).is_none(),
            "a paged key reads from its footer"
        );
    }

    // a shard of many keys answers a clear miss without taking its lock
    #[test]
    fn a_miss_skips_a_crowded_shard_lock() {
        let index: WidthIndex<[u8; 34], Trees<34>> = WidthIndex::new(&SINGLE);
        let segments = SegmentTable::new();
        for at in 0..1_000u32 {
            let entry = Entry::new(loc(1, at, 10), Lsn(u64::from(at) + 1));
            index.insert(&crowded(at), entry, &segments);
        }

        let held = write(&index.shards[0]);
        let (answer, answered) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let found = (1_000..1_100u32)
                    .filter(|at| index.get(&crowded(*at)).is_some())
                    .count();
                answer.send(found).expect("send");
            });
            let missed = answered.recv_timeout(std::time::Duration::from_secs(5));
            drop(held);
            assert_eq!(missed, Ok(0), "a miss waited on the shard's lock");
        });
    }

    /// Run the lazy sweep to completion, stepping straight past the release phase
    fn sweep_all<K: IndexKey, S: Shape<K>>(index: &WidthIndex<K, S>, segments: &SegmentTable) {
        let mut steps = 0;
        while let Some(pending) = index.next_pending_cover() {
            steps += 1;
            assert!(
                steps < 1000,
                "the sweep keeps coming back to cover {:?}",
                pending.lsn
            );
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
            purge_mark: None,
            codec: Codec::None,
        };

        assert!(ColumnIndex::new(&odd).is_err());
        assert!(ColumnIndex::new(&SHARDED).is_ok());
        assert_eq!(ColumnIndex::new(&FLAT).expect("flat").key_width(), 32);
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
                purge_mark: None,
                codec: Codec::None,
            };

            let index = ColumnIndex::new(&spec).expect("a small width declared");
            assert_eq!(index.key_width(), u16::from(width));
        }
    }

    // the shard filter says absent only when absent is certain
    #[test]
    fn the_filter_never_invents_an_absence() {
        let index = sharded();
        let segments = SegmentTable::new();

        index.insert(&key(1, 1), Entry::new(loc(1, 0, 400), Lsn(1)), &segments);
        assert!(
            index.get(&key(1, 1)).is_some(),
            "a present key survives the filter"
        );
        assert!(index.get(&key(1, 2)).is_none(), "same shard, absent key");
        assert!(
            index.get(&key(9, 1)).is_none(),
            "a shard that never held a key"
        );

        // A grave must pass the filter, since `entry_or_grave` tells it from an absence
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
            .insert(&key(1, 1), Entry::new(loc(1, 0, 400), Lsn(1)), &segments,)
            .took_place());

        let entry = index.get(&key(1, 1)).expect("present");
        assert_eq!(entry.loc, loc(1, 0, 400));
        assert_eq!(entry.lsn, Lsn(1));
        assert_eq!(index.totals().count, 1);
        assert_eq!(index.totals().bytes, ByteCount::from_bytes(400));
    }

    // a key of the wrong width is refused and never padded into the column
    #[test]
    fn wrong_width_refused() {
        let index = sharded();
        let segments = SegmentTable::new();

        assert_eq!(
            index.insert(&[0u8; 20], Entry::new(loc(1, 0, 400), Lsn(1)), &segments,),
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
            .insert(&key(1, 1), Entry::new(loc(1, 0, 400), Lsn(1)), &segments)
            .took_place();

        index
            .insert(&key(1, 1), Entry::new(loc(2, 0, 900), Lsn(2)), &segments)
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
            .insert(&key(1, 1), Entry::new(loc(2, 0, 900), Lsn(5)), &segments)
            .took_place();

        let applied = index
            .insert(&key(1, 1), Entry::new(loc(1, 0, 400), Lsn(3)), &segments)
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
            .insert(&key(1, 1), Entry::new(loc(1, 0, 400), Lsn(1)), &segments)
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
            .insert(&key(1, 1), Entry::new(loc(1, 0, 400), Lsn(5)), &segments)
            .took_place();

        assert_eq!(
            index.remove(&key(1, 1), Lsn(3), loc(1, 0, 0), &segments),
            Landed::Newer
        );

        assert!(index.contains(&key(1, 1)));
        assert_eq!(index.totals().count, 1);
    }

    // a write over an empty place the shadow check finds outversioned is refused, by every door
    #[test]
    fn a_shadowed_write_over_an_empty_place_is_refused() {
        let index = sharded();
        let segments = SegmentTable::new();
        let shadowed = |_: &[u8], _: Lsn| true;

        assert_eq!(
            index.insert_unless(
                &key(1, 1),
                Entry::new(loc(1, 0, 400), Lsn(3)),
                &segments,
                &shadowed,
            ),
            Landed::Newer
        );
        assert!(index.get(&key(1, 1)).is_none(), "the refused put went in");
        assert_eq!(
            segments.bytes_of(SegmentId(1)).dead,
            span_of(34, 400),
            "the refused put was not booked dead"
        );

        assert_eq!(
            index.remove_unless(&key(1, 2), Lsn(4), loc(2, 0, 0), &segments, &shadowed),
            Landed::Newer
        );
        assert!(
            index.entry_or_grave(&key(1, 2)).is_none(),
            "the refused delete stood a grave"
        );

        let (put, deleted) = (key(1, 3), key(1, 4));
        let moves = [
            KeyMove {
                column: SHARDED.id,
                key: &put,
                loc: loc(3, 0, 100),
                lsn: Lsn(5),
                is_delete: false,
            },
            KeyMove {
                column: SHARDED.id,
                key: &deleted,
                loc: loc(3, 128, 0),
                lsn: Lsn(6),
                is_delete: true,
            },
        ];
        let mut landed = Vec::new();
        index.apply_moves(&moves, &segments, &shadowed, &mut landed);
        assert_eq!(landed, vec![Landed::Newer, Landed::Newer]);
        assert!(
            index.entry_or_grave(&put).is_none(),
            "the batch put went in"
        );
        assert!(
            index.entry_or_grave(&deleted).is_none(),
            "the batch delete stood a grave"
        );
        assert_eq!(index.totals().count, 0);

        // A place the map holds is settled by its own entry, and the check is never asked
        let asked = std::cell::Cell::new(0u32);
        let counted = |_: &[u8], _: Lsn| {
            asked.set(asked.get() + 1);
            true
        };
        index.insert(&key(1, 5), Entry::new(loc(4, 0, 10), Lsn(7)), &segments);
        assert_eq!(
            index.insert_unless(
                &key(1, 5),
                Entry::new(loc(4, 64, 10), Lsn(8)),
                &segments,
                &counted,
            ),
            Landed::Record
        );
        assert_eq!(asked.get(), 0, "a held place asked the shadow check");
    }

    // a tombstone compaction drops takes its own grave out, and leaves any other entry alone
    #[test]
    fn a_dropped_tombstone_takes_its_grave() {
        let index = sharded();
        let segments = SegmentTable::new();
        index.remove(&key(2, 1), Lsn(9), loc(5, 0, 0), &segments);
        assert!(
            !index.drop_grave(&key(2, 1), Lsn(8)),
            "another delete's grave went"
        );
        assert!(index.drop_grave(&key(2, 1), Lsn(9)));
        assert!(index.entry_or_grave(&key(2, 1)).is_none());
        assert_eq!(index.grave_count(), 0);

        index.insert(&key(2, 2), Entry::new(loc(5, 64, 10), Lsn(10)), &segments);
        assert!(!index.drop_grave(&key(2, 2), Lsn(10)), "a live record went");
        assert!(index.get(&key(2, 2)).is_some());
    }

    // a copy of a tombstone moves its grave to the segment the copy landed in
    #[test]
    fn a_copied_tombstone_moves_its_grave() {
        let index = sharded();
        let segments = SegmentTable::new();
        index.remove(&key(3, 1), Lsn(9), loc(5, 0, 0), &segments);
        assert_eq!(
            index.remove(&key(3, 1), Lsn(9), loc(8, 0, 0), &segments),
            Landed::Newer
        );
        let grave = index.entry_or_grave(&key(3, 1)).expect("the grave");
        assert_eq!(grave.grave_origin(), Some(SegmentId(8)));
        assert_eq!(index.grave_count(), 1);
    }

    // a shard-aligned prefix has its live totals without walking its keys
    #[test]
    fn prefix_totals_from_the_shard() {
        let index = sharded();
        let segments = SegmentTable::new();
        index
            .insert(&key(7, 1), Entry::new(loc(1, 0, 400), Lsn(1)), &segments)
            .took_place();
        index
            .insert(&key(7, 2), Entry::new(loc(1, 0, 600), Lsn(2)), &segments)
            .took_place();
        index
            .insert(&key(8, 1), Entry::new(loc(1, 0, 100), Lsn(3)), &segments)
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
            .insert(&[0x11; 32], Entry::new(loc(1, 0, 400), Lsn(1)), &segments)
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
                )
                .took_place();
        }
        index
            .insert(
                &key(42, 0xaa),
                Entry::new(loc(1, 0, 100), Lsn(2)),
                &segments,
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

        // Three spans are dead: one overwritten on the way in, two settled by the sweep
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
            .insert(&key(42, 1), Entry::new(loc(1, 0, 100), Lsn(1)), &segments)
            .took_place();
        index
            .insert(&key(42, 2), Entry::new(loc(1, 0, 100), Lsn(20)), &segments)
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

    // a repoint moves a key to its copy only while the version is unchanged
    #[test]
    fn repoint_moves_unchanged() {
        let index = sharded();
        let segments = SegmentTable::new();
        index
            .insert(&key(1, 1), Entry::new(loc(1, 0, 400), Lsn(1)), &segments)
            .took_place();

        let moved = index.repoint(&key(1, 1), loc(2, 0, 400), Lsn(1), SegmentIncarnation(1));

        assert_eq!(moved, Some(loc(1, 0, 400)));
        assert_eq!(index.get(&key(1, 1)).expect("present").loc, loc(2, 0, 400));
        assert_eq!(index.totals().count, 1);
    }

    // a repoint loses to a concurrent overwrite and leaves the newer entry standing
    #[test]
    fn repoint_loses_to_overwrite() {
        let index = sharded();
        let segments = SegmentTable::new();
        index
            .insert(&key(1, 1), Entry::new(loc(1, 0, 400), Lsn(1)), &segments)
            .took_place();
        index
            .insert(&key(1, 1), Entry::new(loc(3, 0, 900), Lsn(2)), &segments)
            .took_place();

        let moved = index.repoint(&key(1, 1), loc(2, 0, 400), Lsn(1), SegmentIncarnation(1));

        assert_eq!(moved, None);
        assert_eq!(index.get(&key(1, 1)).expect("present").loc, loc(3, 0, 900));
    }

    // evicting a corrupt record removes the key and books its bytes dead
    #[test]
    fn evict_at_drops_key() {
        let index = sharded();
        let segments = SegmentTable::new();
        index
            .insert(&key(1, 1), Entry::new(loc(1, 0, 400), Lsn(1)), &segments)
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
            .insert(&key(1, 1), Entry::new(loc(1, 0, 400), Lsn(1)), &segments)
            .took_place();
        index
            .insert(&key(1, 1), Entry::new(loc(2, 0, 500), Lsn(2)), &segments)
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
            .insert(&key(1, 1), Entry::new(loc(1, 0, 400), Lsn(1)), &segments)
            .took_place();
        index
            .insert(&key(2, 1), Entry::new(loc(1, 0, 400), Lsn(2)), &segments)
            .took_place();

        index.remove(&key(1, 1), Lsn(3), loc(1, 0, 0), &segments);

        // The delete left a grave, so the shard is still occupied
        assert_eq!(read(&index.occupied).len(), 2);
        let mut out = KeyPage::default();
        index.page(Bound::Unbounded, 8, &mut out);
        assert_eq!(keys_in(&out), vec![key(2, 1)]);

        index.prune_tombstones(Lsn(3), &sealed(&[1]));

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

        // The delete is drawn second and published first, on a key the index never saw
        index.remove(&key(1, 1), Lsn(6), loc(1, 0, 0), &segments);
        assert!(!index
            .insert(&key(1, 1), Entry::new(loc(1, 0, 400), Lsn(5)), &segments,)
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
            .insert(&key(1, 1), Entry::new(loc(1, 0, 100), Lsn(2)), &segments)
            .took_place();

        index.remove(&key(1, 1), Lsn(6), loc(1, 0, 0), &segments);
        assert!(!index
            .insert(&key(1, 1), Entry::new(loc(1, 0, 400), Lsn(5)), &segments,)
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
            .insert(&key(1, 1), Entry::new(loc(1, 0, 400), Lsn(7)), &segments,)
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
            .insert(&key(1, 1), Entry::new(loc(1, 0, 100), Lsn(2)), &segments)
            .took_place();
        index
            .insert(&key(1, 2), Entry::new(loc(1, 0, 100), Lsn(3)), &segments)
            .took_place();

        index.remove_range(&key(1, 0), None, Lsn(6));
        sweep_all(&index, &segments);

        assert_eq!(
            index.grave_count(),
            0,
            "the cover is the guard, not a grave per key"
        );
        assert!(!index
            .insert(&key(1, 1), Entry::new(loc(1, 0, 400), Lsn(5)), &segments,)
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

        let sealed = sealed(&[1]);
        assert_eq!(index.lifted(), Lsn::NONE);
        assert_eq!(
            index.prune_tombstones(Lsn(6), &sealed),
            1,
            "only the one below the floor"
        );
        assert_eq!(index.grave_count(), 1);
        assert_eq!(
            index.lifted(),
            Lsn(6),
            "the pruned grave's version is lifted"
        );

        assert_eq!(index.prune_tombstones(Lsn(9), &sealed), 1);
        assert_eq!(index.grave_count(), 0);
        assert_eq!(index.lifted(), Lsn(9));
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
            );
        }

        // Every key but the last hundred, deleted under numbers a prune can reach.
        for key in held.iter().take(1900) {
            index.remove(key, Lsn(10_000), loc(2, 0, 0), &segments);
        }
        assert_eq!(index.grave_count(), 1900);
        assert_eq!(index.prune_tombstones(Lsn(10_000), &sealed(&[2])), 1900);
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
            index.insert(key, Entry::new(loc(1, 0, 10), Lsn(1)), &segments);
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
        index.insert(&swept[0], Entry::new(loc(1, 0, 10), Lsn(20)), &segments);
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
            index.insert(key, Entry::new(loc(1, 0, 10), Lsn(1)), &segments);
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

        // A shard that pages stays in the walk while its paged count is above zero
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
            .insert(&key(1, 5), Entry::new(loc(1, 0, 400), Lsn(5)), &segments,)
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
            .insert(&key(1, 5), Entry::new(loc(1, 0, 400), Lsn(7)), &segments,)
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
                .insert(&key(1, 5), Entry::new(loc(1, 0, 400), Lsn(5)), &segments,)
                .took_place(),
            "a key inside the range is refused"
        );
        assert!(
            index
                .insert(&key(2, 5), Entry::new(loc(1, 0, 400), Lsn(5)), &segments,)
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
            .insert(&key(1, 1), Entry::new(loc(1, 0, 100), Lsn(1)), &segments)
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
        let sealed = SealedRanges::new();
        index.prune_tombstones(Lsn(9), &sealed);
        assert_eq!(
            index.cover_count(),
            2,
            "an unswept cover outlives the floor"
        );

        assert_eq!(index.lifted(), Lsn::NONE, "a cover kept lifts nothing");

        sweep_all(&index, &segments);
        index.prune_tombstones(Lsn(6), &sealed);
        assert_eq!(index.cover_count(), 1, "only the one below the floor");
        assert_eq!(
            index.lifted(),
            Lsn(6),
            "the pruned cover's version is lifted"
        );

        index.prune_tombstones(Lsn(9), &sealed);
        assert_eq!(index.cover_count(), 0);
        assert_eq!(index.lifted(), Lsn(9));

        // The retired delete holds nothing, so an older record goes in on its own merits
        assert!(index
            .insert(&key(1, 5), Entry::new(loc(1, 0, 400), Lsn(5)), &segments,)
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
            .insert(&key(1, 5), Entry::new(loc(1, 0, 400), Lsn(5)), &segments,)
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

    // a range delete met in two segments stands once, and its sweep finishes
    #[test]
    fn a_cover_raised_twice_stands_once() {
        let index = sharded();
        let segments = SegmentTable::new();
        for byte in 0..4u8 {
            index
                .insert(
                    &key(7, byte),
                    Entry::new(loc(1, 0, 100), Lsn(1 + byte as u64)),
                    &segments,
                )
                .took_place();
        }

        index.remove_range(&7u16.to_be_bytes(), Some(&8u16.to_be_bytes()), Lsn(50));
        index.remove_range(&7u16.to_be_bytes(), Some(&8u16.to_be_bytes()), Lsn(50));
        sweep_all(&index, &segments);
        assert!(!index.has_pending_covers());
        assert_eq!(index.totals().count, 0);
    }

    // a covered entry refuses to page out and waits for the sweep
    #[test]
    fn a_covered_entry_stays_for_the_sweep() {
        let index = sharded();
        let segments = SegmentTable::new();
        index
            .insert(&key(7, 1), Entry::new(loc(1, 0, 100), Lsn(1)), &segments)
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

    // a shard under an unswept cover declines its totals until the sweep
    #[test]
    fn prefix_totals_decline_under_a_pending_cover() {
        let index = sharded();
        let segments = SegmentTable::new();
        index
            .insert(&key(7, 1), Entry::new(loc(1, 0, 400), Lsn(1)), &segments)
            .took_place();
        index
            .insert(&key(8, 1), Entry::new(loc(1, 0, 100), Lsn(2)), &segments)
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

    // a second delete of one key replaces its grave and holds one place
    #[test]
    fn a_second_delete_replaces_the_grave() {
        let index = sharded();
        let segments = SegmentTable::new();

        index.remove(&key(1, 1), Lsn(4), loc(1, 0, 0), &segments);
        index.remove(&key(1, 1), Lsn(5), loc(1, 0, 0), &segments);

        assert_eq!(index.grave_count(), 1);
        assert!(!index
            .insert(&key(1, 1), Entry::new(loc(1, 0, 400), Lsn(4)), &segments,)
            .took_place());
    }
}
