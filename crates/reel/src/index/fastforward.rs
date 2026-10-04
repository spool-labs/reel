//! FastForward, the index of sealed keys, which keeps record locations and no keys
//!
//! A lookup confirms each candidate against the key in the record's own header.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

use reel_core::Value;

use crate::error::Result;
use crate::format::column::{ColumnId, KeyRef, RecordKey};
use crate::format::footer::{FooterFind, FooterPartition};
use crate::format::loc::{Loc, SegmentId};
use crate::format::lsn::Lsn;
use crate::index::counters::SegmentTable;
use crate::index::entry::Entry;
use crate::index::paged::FooterSource;
use crate::sync::{lock, read, write};

/// Slots in one bucket, which fills one cache line
const WAYS: usize = 4;

/// Share of a shard's slots it fills before it grows
const LOAD: f64 = 0.85;

/// How much a shard grows by when it fills
const GROWTH: f64 = 1.5;

/// Shards, picked by the top byte of a key's hash
const SHARDS: usize = 256;

/// Buckets a shard starts with
const FIRST_BUCKETS: usize = 8;

/// Slots a full pair of buckets moves along before the shard grows instead
const MAX_KICKS: usize = 256;

/// Threads an open settles its set-aside rows on, each read waiting on the device
const SETTLE_THREADS: usize = 8;

/// Times a lookup starts over when a candidate's segment went while it read
///
/// A compaction can move a record, seal its copy and retire the source inside one
/// slow read, and only a fresh look at the table finds the copy. Each try takes one
/// such candidate out, so this many tries outlast every candidate a key can have.
pub const LOOKUP_TRIES: usize = MAX_CANDIDATES + 1;

/// Older versions waiting for the cleaner before new ones are left to the retired-segment sweep
const MAX_STALE: usize = 1 << 20;

/// Bytes one bucket takes
const BUCKET_BYTES: u64 = 64;

/// Candidates one key can have, both of its buckets full
const MAX_CANDIDATES: usize = 2 * WAYS;

/// Payload bytes one small length class covers
const SMALL_STEP: u64 = 64;

/// Small length classes, each one step wider than the last
const SMALL_CLASSES: usize = 16;

/// Length classes in all, enough for the last one to cover any length
const CLASSES: usize = 192;

/// One eighth of an octave as 16-bit fixed point, so each wide class is about 9% wider
const EIGHTHS: [u64; 8] = [65_536, 71_468, 77_936, 84_990, 92_682, 101_070, 110_218, 120_194];

/// The payload bytes each length class covers
static BOUNDS: [u32; CLASSES] = bounds();

const TAG_SHIFT: u32 = 16;
const CLASS_SHIFT: u32 = 8;
const CLASS_MASK: u32 = 0xFF;
const SECOND: u32 = 1;
const GRAVE: u32 = 2;

/// What a record's header says, once its key matched the one asked about
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Head {
    pub lsn: Lsn,
    pub len: u32,
    pub is_tombstone: bool,
}

/// One header read
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeadRead {
    /// The record holds the key asked about
    Same(Head),

    /// The record holds another key
    Other,

    /// The segment is gone or holds no record there
    Missing,

    /// The header is not in memory, from a read that may only answer from memory
    Cold,
}

/// One read of a record's header and payload
pub enum FastRead {
    Found(Head, Value),
    Tombstone(Head),
    Other,

    /// The segment is gone, so whatever the entry pointed at has moved on or died
    Gone,

    /// A record the checked read has to settle, such as one failing its checksum
    Unsure,
}

/// One record a lookup reads: where it sits, and how much of its payload to ask for
#[derive(Clone, Copy, Debug)]
pub struct Candidate {
    pub segment: SegmentId,
    pub offset: u32,
    pub bound: u32,
    slot: Slot,
}

/// A lookup in progress: the candidates in order, and the newest version read so far
pub struct Pick {
    hash: u64,
    ordered: Vec<(Option<Lsn>, Slot)>,
    next: usize,
    best: Option<(Head, Option<Value>)>,
    stale: Vec<(Slot, u32)>,
}

/// What folding one read into a lookup leaves it to do
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Offered {
    /// Read the next candidate, if one is left
    Next,

    /// A candidate's segment went, so the lookup starts over from a fresh look
    Again,

    /// Something only the checked read can settle
    Unsettled,
}

/// What one lookup settled about a key
pub enum Lookup {
    Found(Lsn, Value),
    Missing,

    /// Something only the checked read can settle
    Unsettled,
}

/// Where the index reads the records it points at
pub trait RecordSource: Send + Sync {
    /// The header at a place, read from the device if it must be
    fn head(&self, key: KeyRef<'_>, segment: SegmentId, offset: u32) -> Result<HeadRead>;

    /// The header at a place, answered only from memory
    fn cached_head(&self, key: KeyRef<'_>, segment: SegmentId, offset: u32) -> Result<HeadRead>;

    /// The record at a place, in one read of its header and up to `bound` payload bytes
    fn record(&self, key: &RecordKey, segment: SegmentId, offset: u32, bound: u32) -> Result<FastRead>;
}

/// The payload bytes of every length class, the small ones even and the wide ones geometric
const fn bounds() -> [u32; CLASSES] {
    let mut bounds = [0u32; CLASSES];
    let mut class = 0;
    while class < CLASSES {
        let bound = match class < SMALL_CLASSES {
            true => (class as u64 + 1) * SMALL_STEP,
            false => {
                let wide = class - SMALL_CLASSES + 1;
                ((SMALL_CLASSES as u64 * SMALL_STEP) << (wide / 8)) * EIGHTHS[wide % 8] / EIGHTHS[0] + 1
            }
        };
        bounds[class] = match bound > u32::MAX as u64 {
            true => u32::MAX,
            false => bound as u32,
        };
        class += 1;
    }
    bounds
}

/// The smallest length class covering a payload length
fn class_of(len: u32) -> u32 {
    BOUNDS.partition_point(|bound| *bound < len) as u32
}

fn bound_of(class: u32) -> u32 {
    BOUNDS[(class as usize).min(CLASSES - 1)]
}

/// A key's 64 bit hash, mixed so structured keys spread like random ones
fn hash_of(key: &[u8]) -> u64 {
    const ODD: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut state = 0xCBF2_9CE4_8422_2325 ^ key.len() as u64;
    let mut chunks = key.chunks_exact(8);
    for chunk in &mut chunks {
        let mut word = [0u8; 8];
        word.copy_from_slice(chunk);
        state = (state ^ u64::from_le_bytes(word)).wrapping_mul(ODD).rotate_left(29);
    }
    let mut tail = [0u8; 8];
    tail[..chunks.remainder().len()].copy_from_slice(chunks.remainder());
    mix(state ^ u64::from_le_bytes(tail))
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 33;
    value = value.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
    value ^= value >> 33;
    value = value.wrapping_mul(0xC4CE_B9FE_1A85_EC53);
    value ^ (value >> 33)
}

fn shard_of(hash: u64) -> usize {
    (hash >> 56) as usize
}

/// The 32 hash bits below the shard's, which place a key in any size of table
fn mid_of(hash: u64) -> u32 {
    (hash >> 24) as u32
}

/// The 16 low hash bits, which tell two keys in one place apart
fn tag_of(hash: u64) -> u32 {
    (hash & 0xFFFF) as u32
}

/// One record's place, the 16 bytes a key costs
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Slot {
    /// The segment, where the reserved zero marks an empty slot
    segment: u32,
    offset: u32,
    mid: u32,

    /// Tag, length class and whether the slot sits in its key's second bucket
    meta: u32,
}

impl Slot {
    fn new(hash: u64, loc: Loc) -> Slot {
        Slot {
            segment: loc.segment.as_u32(),
            offset: loc.offset,
            mid: mid_of(hash),
            meta: (tag_of(hash) << TAG_SHIFT) | (class_of(loc.len) << CLASS_SHIFT),
        }
    }

    fn is_empty(&self) -> bool {
        self.segment == 0
    }

    fn segment(&self) -> SegmentId {
        SegmentId(self.segment)
    }

    fn tag(&self) -> u32 {
        self.meta >> TAG_SHIFT
    }

    fn bound(&self) -> u32 {
        bound_of((self.meta >> CLASS_SHIFT) & CLASS_MASK)
    }

    fn is_second(&self) -> bool {
        self.meta & SECOND != 0
    }

    fn is_grave(&self) -> bool {
        self.meta & GRAVE != 0
    }

    fn as_grave(self) -> Slot {
        Slot {
            meta: self.meta | GRAVE,
            ..self
        }
    }

    fn in_second(self, is_second: bool) -> Slot {
        let meta = match is_second {
            true => self.meta | SECOND,
            false => self.meta & !SECOND,
        };
        Slot { meta, ..self }
    }

    /// Whether two slots point at the same record, wherever displacement left them
    fn same_place(&self, other: &Slot) -> bool {
        self.segment == other.segment && self.offset == other.offset && self.mid == other.mid
    }

    fn holds(&self, hash: u64) -> bool {
        !self.is_empty() && self.mid == mid_of(hash) && self.tag() == tag_of(hash)
    }

    fn at(&self, loc: Loc) -> bool {
        self.segment == loc.segment.as_u32() && self.offset == loc.offset
    }
}

#[repr(C, align(64))]
#[derive(Clone, Copy, Default)]
struct Bucket {
    slots: [Slot; WAYS],
}

/// One slot in a table and what it held when it was looked at
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Place {
    bucket: usize,
    way: usize,
    slot: Slot,
}

/// The slots carrying one key's hash, at most both buckets full
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Places {
    held: [Place; MAX_CANDIDATES],
    count: usize,
}

impl Places {
    fn push(&mut self, place: Place) {
        self.held[self.count] = place;
        self.count += 1;
    }
}

impl std::ops::Deref for Places {
    type Target = [Place];

    fn deref(&self) -> &[Place] {
        &self.held[..self.count]
    }
}

struct Table {
    buckets: Vec<Bucket>,
    held: usize,
    seed: u64,
}

impl Table {
    fn with_buckets(count: usize) -> Table {
        Table {
            buckets: vec![Bucket::default(); count.max(2)],
            held: 0,
            seed: count as u64,
        }
    }

    fn home(&self, mid: u32) -> usize {
        ((u64::from(mid) * self.buckets.len() as u64) >> 32) as usize
    }

    fn step(&self, tag: u32) -> usize {
        1 + (mix(u64::from(tag)) % (self.buckets.len() as u64 - 1)) as usize
    }

    /// The bucket a slot does not sit in, of the two its key may use
    fn other(&self, bucket: usize, slot: &Slot) -> usize {
        let count = self.buckets.len();
        let step = self.step(slot.tag());
        match slot.is_second() {
            false => (bucket + step) % count,
            true => (bucket + count - step) % count,
        }
    }

    fn pair(&self, hash: u64) -> [usize; 2] {
        let home = self.home(mid_of(hash));
        [home, (home + self.step(tag_of(hash))) % self.buckets.len()]
    }

    fn matches(&self, hash: u64) -> Places {
        let mut found = Places {
            held: [Place::default(); MAX_CANDIDATES],
            count: 0,
        };
        let [home, second] = self.pair(hash);
        for (bucket, is_second) in [(home, false), (second, true)] {
            for (way, slot) in self.buckets[bucket].slots.iter().enumerate() {
                if slot.holds(hash) && slot.is_second() == is_second {
                    found.push(Place {
                        bucket,
                        way,
                        slot: *slot,
                    });
                }
            }
        }
        found
    }

    fn free_way(&self, bucket: usize) -> Option<usize> {
        self.buckets[bucket].slots.iter().position(Slot::is_empty)
    }

    /// Put a slot in either of its key's buckets, moving others along when both are full
    ///
    /// A failed placement hands back the slot left homeless, which may be another key's.
    fn place(&mut self, slot: Slot) -> std::result::Result<(), Slot> {
        let home = self.home(slot.mid);
        let second = (home + self.step(slot.tag())) % self.buckets.len();
        for (bucket, is_second) in [(home, false), (second, true)] {
            if let Some(way) = self.free_way(bucket) {
                self.buckets[bucket].slots[way] = slot.in_second(is_second);
                self.held += 1;
                return Ok(());
            }
        }
        let (mut bucket, mut moving) = (home, slot.in_second(false));
        for _ in 0..MAX_KICKS {
            self.seed = mix(self.seed);
            let way = (self.seed % WAYS as u64) as usize;
            let evicted = std::mem::replace(&mut self.buckets[bucket].slots[way], moving);
            bucket = self.other(bucket, &evicted);
            moving = evicted.in_second(!evicted.is_second());
            if let Some(free) = self.free_way(bucket) {
                self.buckets[bucket].slots[free] = moving;
                self.held += 1;
                return Ok(());
            }
        }
        Err(moving)
    }

    fn is_full(&self) -> bool {
        self.held as f64 >= (self.buckets.len() * WAYS) as f64 * LOAD
    }

    /// A table this one's slots fit into at the next size
    fn grown(&self) -> Table {
        let mut count = ((self.buckets.len() as f64) * GROWTH).ceil() as usize;
        loop {
            let mut table = Table::with_buckets(count);
            let slots = self.buckets.iter().flat_map(|bucket| bucket.slots.iter());
            if slots.filter(|slot| !slot.is_empty()).all(|slot| table.place(*slot).is_ok()) {
                return table;
            }
            count = ((count as f64) * GROWTH).ceil() as usize;
        }
    }

    /// Put a slot in, growing the table first when it is full or the pair has no room
    fn insert(&mut self, slot: Slot) {
        if self.is_full() {
            *self = self.grown();
        }
        let mut homeless = slot;
        while let Err(left) = self.place(homeless) {
            *self = self.grown();
            homeless = left;
        }
    }

    /// Take out the slot pointing at one record, wherever displacement has moved it
    fn take(&mut self, hash: u64, slot: &Slot) -> bool {
        let found = self.matches(hash);
        match found.iter().find(|place| place.slot.same_place(slot)) {
            Some(place) => {
                self.buckets[place.bucket].slots[place.way] = Slot::default();
                self.held -= 1;
                true
            }
            None => false,
        }
    }
}

/// An older version a lookup read past, for the cleaner to take out
struct Stale {
    key: RecordKey,
    hash: u64,
    slot: Slot,

    /// The record's length, from the header the lookup read
    len: u32,
}

/// The slot a set-aside row stands as, a grave for a tombstone
fn slot_of(hash: u64, row: &SetAside) -> Slot {
    match row.is_tombstone {
        true => Slot::new(hash, row.loc).as_grave(),
        false => Slot::new(hash, row.loc),
    }
}

/// A sealed row an open set aside, because a slot already shared its key's bits
struct SetAside {
    key: RecordKey,
    loc: Loc,
    lsn: Lsn,
    is_tombstone: bool,
}

/// One shard of a column's table, and how many slots have left it
struct Shard {
    table: RwLock<Table>,

    /// Slots taken out so far, which a lookup that found nothing checks for a race
    taken: AtomicU64,
}

impl Shard {
    fn new() -> Shard {
        Shard {
            table: RwLock::new(Table::with_buckets(FIRST_BUCKETS)),
            taken: AtomicU64::new(0),
        }
    }

    fn read(&self) -> RwLockReadGuard<'_, Table> {
        read(&self.table)
    }

    fn write(&self) -> Writing<'_> {
        Writing {
            table: write(&self.table),
            taken: &self.taken,
        }
    }
}

/// A shard held for writing, which counts every slot it takes out before it lets go
struct Writing<'a> {
    table: RwLockWriteGuard<'a, Table>,
    taken: &'a AtomicU64,
}

impl Writing<'_> {
    /// Take out the slot pointing at one record, wherever displacement has moved it
    fn take(&mut self, hash: u64, slot: &Slot) -> bool {
        let took = self.table.take(hash, slot);
        if took {
            self.taken.fetch_add(1, Ordering::Release);
        }
        took
    }

    /// Count slots a sweep emptied by hand
    fn count_taken(&self, slots: u64) {
        if slots > 0 {
            self.taken.fetch_add(slots, Ordering::Release);
        }
    }
}

impl std::ops::Deref for Writing<'_> {
    type Target = Table;

    fn deref(&self) -> &Table {
        &self.table
    }
}

impl std::ops::DerefMut for Writing<'_> {
    fn deref_mut(&mut self) -> &mut Table {
        &mut self.table
    }
}

/// Where a key's shard stood before a lookup asked the map, to check a miss against
#[derive(Clone, Copy, Debug)]
pub struct Since {
    shard: usize,
    taken: u64,
}

/// A column's sealed keys as record locations, in shards picked by hash
pub struct FastColumn {
    shards: Vec<Shard>,
    records: OnceLock<Arc<dyn RecordSource>>,
    segments: OnceLock<Arc<SegmentTable>>,
    stale: Mutex<VecDeque<Stale>>,
    beside: AtomicU64,

    /// Rows an open took before it could read a header, settled once it can
    set_aside: Mutex<Vec<SetAside>>,
}

impl Default for FastColumn {
    fn default() -> FastColumn {
        FastColumn::new()
    }
}

impl FastColumn {
    pub fn new() -> FastColumn {
        FastColumn {
            shards: (0..SHARDS).map(|_| Shard::new()).collect(),
            records: OnceLock::new(),
            segments: OnceLock::new(),
            stale: Mutex::new(VecDeque::new()),
            beside: AtomicU64::new(0),
            set_aside: Mutex::new(Vec::new()),
        }
    }

    /// Where lookups read records, and the counters that order segments
    pub fn attach(&self, records: Arc<dyn RecordSource>, segments: Arc<SegmentTable>) {
        let _ = self.records.set(records);
        let _ = self.segments.set(segments);
    }

    /// Older versions waiting for the cleaner
    pub fn beside(&self) -> u64 {
        self.beside.load(Ordering::Relaxed)
    }

    /// Entries held
    pub fn held(&self) -> u64 {
        self.shards.iter().map(|shard| shard.read().held as u64).sum()
    }

    pub fn heap_bytes(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| shard.read().buckets.len() as u64 * BUCKET_BYTES)
            .sum()
    }

    /// Hold a sealed record's location
    ///
    /// The caller hands over the newest version, so an older entry of the same key is
    /// one the map displaced and the cleaner will settle.
    pub fn insert(&self, key: &[u8], loc: Loc) {
        let hash = hash_of(key);
        let slot = Slot::new(hash, loc);
        let mut table = self.shards[shard_of(hash)].write();
        if table.matches(hash).iter().any(|place| place.slot.same_place(&slot)) {
            return;
        }
        table.insert(slot);
    }

    /// Size every shard for about this many keys, so a load does not grow them a step at a time
    pub fn reserve(&self, keys: u64) {
        let per_shard = keys.div_ceil(SHARDS as u64) as f64;
        let buckets = (per_shard / (WAYS as f64 * LOAD)).ceil() as usize;
        for shard in &self.shards {
            let mut table = shard.write();
            if table.held == 0 && table.buckets.len() < buckets {
                *table = Table::with_buckets(buckets);
            }
        }
    }

    /// Take one sealed footer partition's rows during an open, before any header can be read
    ///
    /// The rows go in a shard at a time, one lock and one warm table each. A row whose
    /// key's bits no slot shares goes straight in. One that meets a slot is a second
    /// version of a key, or rarely another key, and waits for `settle_rows`.
    pub fn take_partition(&self, segment: SegmentId, partition: &FooterPartition) -> Result<()> {
        let mut by_shard: Vec<Vec<(u64, Slot, u32)>> = vec![Vec::new(); SHARDS];
        for at in 0..partition.len() {
            let entry = partition.entry_at(at)?;
            if entry.is_range_tombstone() {
                continue;
            }
            let hash = hash_of(entry.key.as_slice());
            let slot = Slot::new(hash, Loc::new(segment, entry.offset, entry.len));
            let slot = match entry.is_tombstone() {
                true => slot.as_grave(),
                false => slot,
            };
            by_shard[shard_of(hash)].push((hash, slot, at as u32));
        }
        // Loaders start at different shards, so two of them rarely want the same lock.
        let first = segment.as_u32() as usize % SHARDS;
        let mut aside = Vec::new();
        for shard in (first..SHARDS).chain(0..first) {
            let rows = &by_shard[shard];
            if rows.is_empty() {
                continue;
            }
            let mut table = self.shards[shard].write();
            for (hash, slot, at) in rows {
                match table.matches(*hash).is_empty() {
                    true => table.insert(*slot),
                    false => aside.push(*at),
                }
            }
        }
        if aside.is_empty() {
            return Ok(());
        }
        let mut set_aside = Vec::with_capacity(aside.len());
        for at in aside {
            let entry = partition.entry_at(at as usize)?;
            set_aside.push(SetAside {
                loc: Loc::new(segment, entry.offset, entry.len),
                lsn: entry.lsn,
                is_tombstone: entry.is_tombstone(),
                key: entry.key,
            });
        }
        lock(&self.set_aside).extend(set_aside);
        Ok(())
    }

    /// Settle the rows an open set aside, against the footers of the slots they met
    ///
    /// A set-aside row nearly always meets one slot, another version of its key. That
    /// slot's footer says by key which record it is and how new, so rows are grouped by
    /// the slot's segment and each footer is read once, with no record read. A row that
    /// meets no slot or several, or one its footer cannot settle, reads headers.
    pub fn settle_rows(&self, column: ColumnId, footers: &dyn FooterSource) -> Result<()> {
        let rows = std::mem::take(&mut *lock(&self.set_aside));
        if rows.is_empty() {
            return Ok(());
        }
        let mut by_segment: HashMap<SegmentId, Vec<usize>> = HashMap::new();
        let mut by_header = Vec::new();
        for (at, row) in rows.iter().enumerate() {
            let hash = hash_of(row.key.as_slice());
            let seen = self.shards[shard_of(hash)].read().matches(hash);
            match seen.len() {
                1 => by_segment.entry(seen[0].slot.segment()).or_default().push(at),
                _ => by_header.push(at),
            }
        }
        let groups: Vec<(SegmentId, Vec<usize>)> = by_segment.into_iter().collect();
        let next = AtomicUsize::new(0);
        let unsettled = Mutex::new(by_header);
        std::thread::scope(|scope| -> Result<()> {
            let workers: Vec<_> = (0..SETTLE_THREADS)
                .map(|_| {
                    scope.spawn(|| -> Result<()> {
                        while let Some((segment, group)) = groups.get(next.fetch_add(1, Ordering::Relaxed)) {
                            let left = self.settle_against(column, *segment, group, &rows, footers)?;
                            lock(&unsettled).extend(left);
                        }
                        Ok(())
                    })
                })
                .collect();
            for worker in workers {
                worker
                    .join()
                    .unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
            }
            Ok(())
        })?;
        for at in std::mem::take(&mut *lock(&unsettled)) {
            let row = &rows[at];
            self.load(&row.key, row.loc, row.lsn, row.is_tombstone)?;
        }
        Ok(())
    }

    /// Settle the rows whose slot sits in one segment, handing back those its footer cannot
    fn settle_against(
        &self,
        column: ColumnId,
        segment: SegmentId,
        group: &[usize],
        rows: &[SetAside],
        footers: &dyn FooterSource,
    ) -> Result<Vec<usize>> {
        let Some(footer) = footers.footer_once(segment)? else {
            return Ok(group.to_vec());
        };
        let Some(partition) = footer.partition(column) else {
            return Ok(group.to_vec());
        };
        let mut group = group.to_vec();
        group.sort_unstable_by(|left, right| rows[*left].key.cmp(&rows[*right].key));
        let mut unsettled = Vec::new();
        for same_key in group.chunk_by(|left, right| rows[*left].key == rows[*right].key) {
            // The newest set-aside version of the key, a tie to the newer segment.
            let Some(newest) = same_key
                .iter()
                .copied()
                .max_by_key(|at| (rows[*at].lsn, rows[*at].loc.segment))
            else {
                continue;
            };
            let row = &rows[newest];
            let standing = match partition.lookup(row.key.as_slice())? {
                FooterFind::Found(found) => found,
                // The slot this row met holds another key.
                FooterFind::Missing | FooterFind::RuledOut => {
                    unsettled.extend_from_slice(same_key);
                    continue;
                }
            };
            let hash = hash_of(row.key.as_slice());
            let mut table = self.shards[shard_of(hash)].write();
            let held = table.matches(hash);
            let place = held.iter().find(|place| place.slot.segment() == segment).copied();
            let is_newer = (row.lsn, row.loc.segment) > (standing.lsn, segment);
            let settled = match place {
                // The slot is the version this footer holds, so the newer of the two stands.
                Some(place) if place.slot.offset == standing.offset => {
                    if is_newer {
                        table.take(hash, &place.slot);
                        table.insert(slot_of(hash, row));
                    }
                    true
                }
                // The slot is an older version in a segment that rewrote the key, and the
                // footer's newest is this row.
                Some(place) if segment == row.loc.segment && standing.offset == row.loc.offset => {
                    table.take(hash, &place.slot);
                    table.insert(slot_of(hash, row));
                    true
                }
                Some(_) | None => false,
            };
            if !settled {
                unsettled.extend_from_slice(same_key);
            }
        }
        Ok(unsettled)
    }

    /// Load one sealed row, keeping each key's newest version and a tombstone as a grave
    ///
    /// A slot sharing the key's bits is a version of the key, or rarely another key, so
    /// its header is read to tell which and how new it is. A tie goes to the newer
    /// segment, which is a compaction copy of the other.
    pub fn load(&self, key: &RecordKey, loc: Loc, lsn: Lsn, is_tombstone: bool) -> Result<()> {
        let Some(records) = self.records.get() else {
            return Ok(());
        };
        let hash = hash_of(key.as_slice());
        let slot = match is_tombstone {
            true => Slot::new(hash, loc).as_grave(),
            false => Slot::new(hash, loc),
        };
        let shard = shard_of(hash);
        {
            let mut table = self.shards[shard].write();
            if table.matches(hash).is_empty() {
                table.insert(slot);
                return Ok(());
            }
        }
        loop {
            let seen = self.shards[shard].read().matches(hash);
            let mut standing = None;
            for place in seen.iter() {
                if let HeadRead::Same(head) = records.head(key.as_ref(), place.slot.segment(), place.slot.offset)? {
                    standing = Some((place.slot, head.lsn));
                }
            }
            let mut table = self.shards[shard].write();
            // Another thread loaded a row of this key while the headers were read.
            if *table.matches(hash) != *seen {
                continue;
            }
            match standing {
                Some((held, held_lsn)) if (lsn, slot.segment) > (held_lsn, held.segment) => {
                    table.take(hash, &held);
                    table.insert(slot);
                }
                Some(_) => {}
                None => table.insert(slot),
            }
            return Ok(());
        }
    }

    /// Close a load by dropping the graves that only kept older rows out
    pub fn finish_load(&self) {
        for shard in &self.shards {
            let mut table = shard.write();
            let mut dropped = 0;
            for bucket in table.buckets.iter_mut() {
                for slot in bucket.slots.iter_mut() {
                    if slot.is_grave() {
                        *slot = Slot::default();
                        dropped += 1;
                    }
                }
            }
            table.held -= dropped;
            table.count_taken(dropped as u64);
        }
    }

    /// Whether a key's one slot points at this record, which a caller that read it can trust with no read
    pub fn only_at(&self, key: &[u8], loc: Loc) -> bool {
        let hash = hash_of(key);
        let seen = self.shards[shard_of(hash)].read().matches(hash);
        seen.len() == 1 && seen[0].slot.at(loc)
    }

    /// Take out the entry pointing at one record, for a compaction move, an eviction or a release
    pub fn remove_at(&self, key: &[u8], loc: Loc) -> bool {
        let hash = hash_of(key);
        let mut table = self.shards[shard_of(hash)].write();
        let held = table.matches(hash).iter().find(|place| place.slot.at(loc)).copied();
        match held {
            Some(place) => table.take(hash, &place.slot),
            None => false,
        }
    }

    /// Take a key's entries older than `newer` out, now that the map holds that version
    ///
    /// A header memory does not hold is read from the device, so the counters move with
    /// the write. What comes back is every record taken out, for the caller to book.
    pub fn displace(&self, key: &RecordKey, newer: Lsn) -> Result<Vec<Loc>> {
        let Some(records) = self.records.get() else {
            return Ok(Vec::new());
        };
        let hash = hash_of(key.as_slice());
        let shard = shard_of(hash);
        let seen = self.shards[shard].read().matches(hash);
        if seen.is_empty() {
            return Ok(Vec::new());
        }
        let (mut older, mut gone) = (Vec::new(), Vec::new());
        for place in seen.iter() {
            let (segment, offset) = (place.slot.segment(), place.slot.offset);
            let answer = match records.cached_head(key.as_ref(), segment, offset)? {
                HeadRead::Cold => records.head(key.as_ref(), segment, offset)?,
                answer => answer,
            };
            match answer {
                HeadRead::Same(head) if head.lsn < newer => older.push((place.slot, head.len)),
                HeadRead::Same(_) | HeadRead::Other | HeadRead::Cold => {}
                HeadRead::Missing => gone.push(place.slot),
            }
        }
        let mut taken = Vec::with_capacity(older.len());
        let mut table = self.shards[shard].write();
        for slot in &gone {
            table.take(hash, slot);
        }
        for (slot, len) in &older {
            if table.take(hash, slot) {
                taken.push(Loc::new(slot.segment(), slot.offset, *len));
            }
        }
        Ok(taken)
    }

    fn queue(&self, stale: Stale) {
        let mut queue = lock(&self.stale);
        if queue.len() < MAX_STALE {
            queue.push_back(stale);
            self.beside.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Take out up to `budget` older versions lookups read past, handing each back to book
    pub fn scrub(&self, budget: usize) -> Vec<(RecordKey, Loc)> {
        let mut taken = Vec::new();
        for _ in 0..budget {
            let Some(stale) = lock(&self.stale).pop_front() else {
                break;
            };
            self.beside.fetch_sub(1, Ordering::Relaxed);
            let took = self.shards[shard_of(stale.hash)].write().take(stale.hash, &stale.slot);
            if took {
                let loc = Loc::new(stale.slot.segment(), stale.slot.offset, stale.len);
                taken.push((stale.key, loc));
            }
        }
        taken
    }

    /// The candidates for a key, newest segment ceiling first, ties to the newer segment
    fn ordered(&self, key: &RecordKey, segments: &SegmentTable) -> (u64, Vec<(Option<Lsn>, Slot)>) {
        let hash = hash_of(key.as_slice());
        let seen = self.shards[shard_of(hash)].read().matches(hash);
        let mut ordered: Vec<(Option<Lsn>, Slot)> = seen.iter().map(|place| (None, place.slot)).collect();
        if ordered.len() > 1 {
            for (ceiling, slot) in ordered.iter_mut() {
                *ceiling = segments.max_lsn_of(slot.segment());
            }
            ordered.sort_unstable_by(|left, right| (right.0, right.1.segment).cmp(&(left.0, left.1.segment)));
        }
        (hash, ordered)
    }

    /// A key's newest payload in one read of each candidate a ceiling cannot rule out
    ///
    /// A version an older candidate held goes to the cleaner already confirmed. Equal
    /// versions are a compaction copy beside its source, and both stay.
    pub fn read(&self, key: &RecordKey) -> Result<Lookup> {
        let Some(records) = self.records.get() else {
            return Ok(Lookup::Unsettled);
        };
        'tries: for _ in 0..LOOKUP_TRIES {
            let Some(mut pick) = self.pick(key) else {
                return Ok(Lookup::Unsettled);
            };
            while let Some(candidate) = self.next(&mut pick) {
                let read = records.record(key, candidate.segment, candidate.offset, candidate.bound)?;
                match self.offer(&mut pick, candidate, read) {
                    Offered::Next => {}
                    Offered::Again => continue 'tries,
                    Offered::Unsettled => return Ok(Lookup::Unsettled),
                }
            }
            return Ok(self.settle(key, pick));
        }
        Ok(Lookup::Unsettled)
    }

    /// Carry a started lookup on, one read of each candidate left, and settle it
    pub fn read_on(&self, key: &RecordKey, mut pick: Pick) -> Result<Lookup> {
        let Some(records) = self.records.get() else {
            return Ok(Lookup::Unsettled);
        };
        while let Some(candidate) = self.next(&mut pick) {
            let read = records.record(key, candidate.segment, candidate.offset, candidate.bound)?;
            match self.offer(&mut pick, candidate, read) {
                Offered::Next => {}
                Offered::Again | Offered::Unsettled => return Ok(Lookup::Unsettled),
            }
        }
        Ok(self.settle(key, pick))
    }

    /// The key's one candidate, when it has exactly one, which a single read can answer for
    pub fn sole(&self, key: &RecordKey) -> Option<Candidate> {
        let mut pick = self.pick(key)?;
        match pick.ordered.len() {
            1 => self.next(&mut pick),
            _ => None,
        }
    }

    /// Where the key's shard stands, read before the map is asked
    pub fn since(&self, key: &RecordKey) -> Since {
        let shard = shard_of(hash_of(key.as_slice()));
        Since {
            shard,
            taken: self.shards[shard].taken.load(Ordering::Acquire),
        }
    }

    /// Whether a slot left the key's shard since then, which may have been the key's newest version
    ///
    /// A write or a compaction move puts the key in the map before its slot goes, so
    /// a lookup that missed in both stands only when nothing left in between.
    pub fn moved(&self, since: Since) -> bool {
        self.shards[since.shard].taken.load(Ordering::Acquire) != since.taken
    }

    /// Start a lookup, ordering the key's candidates, or nothing before records can be read
    pub fn pick(&self, key: &RecordKey) -> Option<Pick> {
        let segments = self.segments.get()?;
        let (hash, ordered) = self.ordered(key, segments);
        Some(Pick {
            hash,
            ordered,
            next: 0,
            best: None,
            stale: Vec::new(),
        })
    }

    /// The next candidate a lookup reads, past any whose segment's ceiling rules it out
    pub fn next(&self, pick: &mut Pick) -> Option<Candidate> {
        while let Some((ceiling, slot)) = pick.ordered.get(pick.next).copied() {
            pick.next += 1;
            // A segment holding nothing newer than the version in hand needs no read.
            if let (Some((head, _)), Some(ceiling)) = (&pick.best, ceiling) {
                if ceiling < head.lsn {
                    continue;
                }
            }
            return Some(Candidate {
                segment: slot.segment(),
                offset: slot.offset,
                bound: slot.bound(),
                slot,
            });
        }
        None
    }

    /// Fold one candidate's read into a lookup
    pub fn offer(&self, pick: &mut Pick, candidate: Candidate, read: FastRead) -> Offered {
        let (head, value) = match read {
            FastRead::Found(head, value) => (head, Some(value)),
            FastRead::Tombstone(head) => (head, None),
            FastRead::Other => return Offered::Next,
            // The segment is gone, so the slot points at nothing and goes before the next look.
            FastRead::Gone => {
                self.shards[shard_of(pick.hash)].write().take(pick.hash, &candidate.slot);
                return Offered::Again;
            }
            FastRead::Unsure => return Offered::Unsettled,
        };
        match &pick.best {
            Some((current, _)) if current.lsn >= head.lsn => {
                if current.lsn > head.lsn {
                    pick.stale.push((candidate.slot, head.len));
                }
            }
            Some(_) | None => pick.best = Some((head, value)),
        }
        Offered::Next
    }

    /// Close a lookup with its newest version, handing the older ones it read to the cleaner
    pub fn settle(&self, key: &RecordKey, pick: Pick) -> Lookup {
        for (slot, len) in pick.stale {
            self.queue(Stale {
                key: key.clone(),
                hash: pick.hash,
                slot,
                len,
            });
        }
        match pick.best {
            Some((head, Some(value))) => Lookup::Found(head.lsn, value),
            Some((_, None)) | None => Lookup::Missing,
        }
    }

    /// A key's newest sealed entry, reading every candidate's header, for a caller that reads the record itself
    pub fn entry(&self, key: &RecordKey) -> Result<Option<Entry>> {
        let (Some(records), Some(segments)) = (self.records.get(), self.segments.get()) else {
            return Ok(None);
        };
        for _ in 0..LOOKUP_TRIES {
            if let Some(entry) = self.entry_once(key, records.as_ref(), segments)? {
                return Ok(entry);
            }
        }
        Ok(None)
    }

    /// One look at the table and a header read of each candidate, or nothing when a segment went under it
    fn entry_once(
        &self,
        key: &RecordKey,
        records: &dyn RecordSource,
        segments: &SegmentTable,
    ) -> Result<Option<Option<Entry>>> {
        let (hash, ordered) = self.ordered(key, segments);
        let mut best: Option<(Head, Slot)> = None;
        for (ceiling, slot) in &ordered {
            if let (Some((head, _)), Some(ceiling)) = (&best, ceiling) {
                if *ceiling < head.lsn {
                    continue;
                }
            }
            match records.head(key.as_ref(), slot.segment(), slot.offset)? {
                HeadRead::Same(head) if best.is_none_or(|(current, _)| head.lsn > current.lsn) => {
                    best = Some((head, *slot));
                }
                // The segment is gone, so the slot points at nothing and goes before the next look.
                HeadRead::Missing => {
                    self.shards[shard_of(hash)].write().take(hash, slot);
                    return Ok(None);
                }
                HeadRead::Same(_) | HeadRead::Other | HeadRead::Cold => {}
            }
        }
        Ok(Some(best.map(|(head, slot)| match head.is_tombstone {
            true => Entry::grave(head.lsn),
            false => {
                let stamp = segments.incarnation_of(slot.segment());
                Entry::new(Loc::new(slot.segment(), slot.offset, head.len), head.lsn).stamped(stamp)
            }
        })))
    }

    /// Drop every entry pointing into a segment no longer standing, with no reads
    pub fn forget_retired(&self, is_standing: impl Fn(SegmentId) -> bool) -> u64 {
        let mut forgotten = 0;
        for shard in &self.shards {
            let mut table = shard.write();
            let mut dropped = 0;
            for bucket in table.buckets.iter_mut() {
                for slot in bucket.slots.iter_mut() {
                    if !slot.is_empty() && !is_standing(slot.segment()) {
                        *slot = Slot::default();
                        dropped += 1;
                    }
                }
            }
            table.held -= dropped;
            table.count_taken(dropped as u64);
            forgotten += dropped as u64;
        }
        forgotten
    }

    /// Drop every entry, for a rebuild starting over
    pub fn clear(&self) {
        for shard in &self.shards {
            let mut table = shard.write();
            let held = table.held as u64;
            *table = Table::with_buckets(FIRST_BUCKETS);
            table.count_taken(held);
        }
        lock(&self.stale).clear();
        lock(&self.set_aside).clear();
        self.beside.store(0, Ordering::Relaxed);
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;

    use super::*;
    use crate::format::column::ColumnId;

    const COLUMN: ColumnId = ColumnId(1);

    #[derive(Clone)]
    struct Written {
        key: Vec<u8>,
        lsn: Lsn,
        len: u32,
        is_tombstone: bool,
    }

    /// Records kept in memory, the way a segment would answer
    #[derive(Default)]
    struct Records {
        written: Mutex<HashMap<(u32, u32), Written>>,
        is_cold: AtomicBool,
    }

    impl Records {
        fn write(&self, loc: Loc, key: &[u8], lsn: Lsn) {
            self.put(loc, key, lsn, false);
        }

        fn put(&self, loc: Loc, key: &[u8], lsn: Lsn, is_tombstone: bool) {
            let written = Written {
                key: key.to_vec(),
                lsn,
                len: loc.len,
                is_tombstone,
            };
            lock(&self.written).insert((loc.segment.as_u32(), loc.offset), written);
        }

        fn answer(&self, key: KeyRef<'_>, segment: SegmentId, offset: u32) -> HeadRead {
            match lock(&self.written).get(&(segment.as_u32(), offset)) {
                Some(written) if written.key == key.bytes => HeadRead::Same(Head {
                    lsn: written.lsn,
                    len: written.len,
                    is_tombstone: written.is_tombstone,
                }),
                Some(_) => HeadRead::Other,
                None => HeadRead::Missing,
            }
        }
    }

    impl RecordSource for Records {
        fn head(&self, key: KeyRef<'_>, segment: SegmentId, offset: u32) -> Result<HeadRead> {
            Ok(self.answer(key, segment, offset))
        }

        fn cached_head(&self, key: KeyRef<'_>, segment: SegmentId, offset: u32) -> Result<HeadRead> {
            Ok(match self.is_cold.load(Ordering::Relaxed) {
                true => HeadRead::Cold,
                false => self.answer(key, segment, offset),
            })
        }

        fn record(&self, key: &RecordKey, segment: SegmentId, offset: u32, bound: u32) -> Result<FastRead> {
            Ok(match self.answer(key.as_ref(), segment, offset) {
                HeadRead::Same(head) if head.len <= bound => {
                    FastRead::Found(head, Value::from(head.lsn.as_u64().to_le_bytes().to_vec()))
                }
                HeadRead::Same(_) => FastRead::Unsure,
                HeadRead::Missing => FastRead::Gone,
                HeadRead::Other | HeadRead::Cold => FastRead::Other,
            })
        }
    }

    fn key(at: u64) -> RecordKey {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&at.to_be_bytes());
        RecordKey::from_bytes(COLUMN, &bytes).expect("key")
    }

    fn column(records: &Arc<Records>) -> FastColumn {
        let column = FastColumn::new();
        column.attach(Arc::clone(records) as Arc<dyn RecordSource>, Arc::new(SegmentTable::new()));
        column
    }

    fn version(column: &FastColumn, at: u64) -> Option<u64> {
        match column.read(&key(at)).expect("read") {
            Lookup::Found(lsn, _) => Some(lsn.as_u64()),
            Lookup::Missing => None,
            Lookup::Unsettled => panic!("key {at} needed the checked read"),
        }
    }

    // a column that starts tiny grows to hold every key, and each reads back
    #[test]
    fn a_growing_column_keeps_every_key() {
        let records = Arc::new(Records::default());
        let column = column(&records);
        let keys = 50_000u64;
        for at in 0..keys {
            let loc = Loc::new(SegmentId(1 + (at / 10_000) as u32), (at % 10_000) as u32 * 64, 40);
            records.write(loc, key(at).as_slice(), Lsn(at + 1));
            column.insert(key(at).as_slice(), loc);
        }
        assert_eq!(column.held(), keys);
        for at in 0..keys {
            assert_eq!(version(&column, at), Some(at + 1), "key {at}");
        }
        assert_eq!(version(&column, keys + 1), None);
    }

    // a displaced version is taken out at once, from memory or from the device
    #[test]
    fn displaced_versions_go_at_once() {
        let records = Arc::new(Records::default());
        let column = column(&records);
        let warm = Loc::new(SegmentId(1), 0, 40);
        let cold = Loc::new(SegmentId(1), 64, 40);
        records.write(warm, key(1).as_slice(), Lsn(1));
        records.write(cold, key(2).as_slice(), Lsn(2));
        column.insert(key(1).as_slice(), warm);
        column.insert(key(2).as_slice(), cold);

        assert_eq!(column.displace(&key(1), Lsn(10)).expect("displace"), vec![warm]);
        assert_eq!(version(&column, 1), None);

        records.is_cold.store(true, Ordering::Relaxed);
        assert_eq!(column.displace(&key(2), Lsn(11)).expect("displace"), vec![cold]);
        assert_eq!(column.held(), 0);

        // a version at or past the map's stays
        let newer = Loc::new(SegmentId(2), 0, 40);
        records.write(newer, key(3).as_slice(), Lsn(30));
        column.insert(key(3).as_slice(), newer);
        assert!(column.displace(&key(3), Lsn(12)).expect("displace").is_empty());
        assert_eq!(version(&column, 3), Some(30));
    }

    // the newest of several versions answers, a copy beside its source stays, older ones go to the cleaner
    #[test]
    fn the_newest_version_answers_and_copies_stand() {
        let records = Arc::new(Records::default());
        let column = column(&records);
        let old = Loc::new(SegmentId(3), 0, 40);
        let new = Loc::new(SegmentId(5), 0, 40);
        let copy = Loc::new(SegmentId(6), 128, 40);
        records.write(old, key(7).as_slice(), Lsn(3));
        records.write(new, key(7).as_slice(), Lsn(9));
        records.write(copy, key(7).as_slice(), Lsn(9));
        column.insert(key(7).as_slice(), old);
        column.insert(key(7).as_slice(), new);
        column.insert(key(7).as_slice(), copy);
        assert_eq!(version(&column, 7), Some(9));
        assert_eq!(column.beside(), 1, "only the older version is stale");
        let taken = column.scrub(16);
        assert_eq!(taken, vec![(key(7), old)]);
        assert!(column.remove_at(key(7).as_slice(), new));
        assert_eq!(version(&column, 7), Some(9), "the copy still answers");
    }

    // a load keeps each key's newest row, gives a tie to the newer segment, and drops deleted keys
    #[test]
    fn a_load_keeps_the_newest_row_of_each_key() {
        let records = Arc::new(Records::default());
        let column = column(&records);
        let rows = [
            (1u64, Loc::new(SegmentId(2), 0, 40), 5u64, false),
            (1, Loc::new(SegmentId(1), 0, 40), 3, false),
            (2, Loc::new(SegmentId(1), 64, 40), 4, false),
            (2, Loc::new(SegmentId(3), 64, 40), 4, false),
            (3, Loc::new(SegmentId(1), 128, 40), 2, false),
            (3, Loc::new(SegmentId(2), 128, 0), 6, true),
            (4, Loc::new(SegmentId(2), 192, 0), 1, true),
            (4, Loc::new(SegmentId(3), 192, 40), 7, false),
        ];
        for (at, loc, lsn, is_tombstone) in rows {
            records.put(loc, key(at).as_slice(), Lsn(lsn), is_tombstone);
            column.load(&key(at), loc, Lsn(lsn), is_tombstone).expect("load");
        }
        column.finish_load();
        assert_eq!(version(&column, 1), Some(5), "the newer row stands");
        assert_eq!(version(&column, 3), None, "a newer tombstone drops the key");
        assert_eq!(version(&column, 4), Some(7), "a newer row stands over an older tombstone");
        assert_eq!(column.held(), 3);
        assert!(column.remove_at(key(2).as_slice(), Loc::new(SegmentId(3), 64, 40)), "a tie went to the copy");
        assert_eq!(column.held(), 2);
    }

    // a candidate whose segment went is taken out, and the lookup answers from a fresh look
    #[test]
    fn a_gone_candidate_is_taken_out_before_the_next_look() {
        let records = Arc::new(Records::default());
        let column = column(&records);
        let standing = Loc::new(SegmentId(1), 0, 40);
        let retired = Loc::new(SegmentId(2), 0, 40);
        records.write(standing, key(9).as_slice(), Lsn(1));
        column.insert(key(9).as_slice(), standing);
        column.insert(key(9).as_slice(), retired);
        assert_eq!(version(&column, 9), Some(1));
        assert_eq!(column.held(), 1, "the slot into the gone segment went");
        column.insert(key(9).as_slice(), retired);
        assert_eq!(column.entry(&key(9)).expect("entry").map(|entry| entry.lsn), Some(Lsn(1)));
        assert_eq!(column.held(), 1);
    }

    // entries into a retired segment go without a read
    #[test]
    fn retired_segments_are_forgotten() {
        let records = Arc::new(Records::default());
        let column = column(&records);
        for at in 0..100u64 {
            let loc = Loc::new(SegmentId(1 + (at % 2) as u32), at as u32 * 64, 40);
            records.write(loc, key(at).as_slice(), Lsn(at + 1));
            column.insert(key(at).as_slice(), loc);
        }
        assert_eq!(column.forget_retired(|segment| segment != SegmentId(2)), 50);
        assert_eq!(column.held(), 50);
        assert_eq!(version(&column, 0), Some(1));
        assert_eq!(version(&column, 1), None);
    }

    // every length class covers the lengths it is chosen for, up to the largest record
    #[test]
    fn length_classes_cover_their_lengths() {
        for len in [0u32, 1, 63, 64, 65, 200, 1024, 1025, 4096, 65_536, 1 << 20, 10_192_000, u32::MAX] {
            let bound = bound_of(class_of(len));
            assert!(bound >= len, "length {len}");
            assert!(u64::from(bound) <= u64::from(len) * 110 / 100 + SMALL_STEP, "length {len} over-reads to {bound}");
        }
    }
}
