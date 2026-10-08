//! The spot index keeps record locations for sealed keys and checks each key in its record's header

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

use reel_core::Value;

use crate::error::{ReelError, Result};
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

/// A shard grows once it fills this share of its slots
const LOAD: f64 = 0.85;

/// How much a shard grows by when it fills
const GROWTH: f64 = 1.5;

/// Shards, picked by the top byte of a key's hash
const SHARDS: usize = 256;

/// A batch takes a shard's lock for this many keys at a time, so a write behind it waits a short while
const LOCK_CHUNK: usize = 64;

/// The one slot among a key's candidates that no newer write displaced, when there is exactly one
fn sole_live(ordered: &[(Option<Lsn>, Slot)]) -> Option<Slot> {
    let mut live = ordered.iter().filter(|(_, slot)| !slot.is_displaced());
    match (live.next(), live.next()) {
        (Some((_, slot)), None) => Some(*slot),
        (Some(_), Some(_)) | (None, _) => None,
    }
}

/// Put a slot in a held shard unless one already points at the same record
fn put_in(table: &mut Writing<'_>, hash: u64, loc: Loc) -> bool {
    let slot = Slot::new(hash, loc);
    if table
        .matches(hash)
        .iter()
        .any(|place| place.slot.same_place(&slot))
    {
        return false;
    }
    table.insert(slot);
    true
}

/// The rows of one lane of shards, gathered by shard
fn lane_groups(rows: &[(&[u8], Loc)], lane: usize, lanes: usize) -> Vec<(usize, Vec<usize>)> {
    let mut by_shard: Vec<Vec<usize>> = vec![Vec::new(); SHARDS];
    for (at, (key, _)) in rows.iter().enumerate() {
        let shard = shard_of(hash_of(key));
        if shard % lanes == lane {
            by_shard[shard].push(at);
        }
    }
    by_shard
        .into_iter()
        .enumerate()
        .filter(|(_, ats)| !ats.is_empty())
        .collect()
}

/// The lowest rung of a shard's ladder holds this many buckets
const FIRST_BUCKETS: usize = 8;

/// A growth multiplies a shard by at least this much, so a table sized between rungs never grows by a sliver
const LEAST_GROWTH: f64 = 1.2;

/// A full pair of buckets moves this many slots along before the shard grows instead
const MAX_KICKS: usize = 256;

/// An open settles its set-aside rows on this many threads, since each read waits on the device
const SETTLE_THREADS: usize = 8;

/// A lookup tries this many times, since each try takes out one candidate whose segment went mid-read
pub const LOOKUP_TRIES: usize = MAX_CANDIDATES + 1;

/// Older versions waiting for the cleaner before new ones are left to the retired-segment sweep
const MAX_STALE: usize = 1 << 20;

/// One bucket takes this many bytes
const BUCKET_BYTES: u64 = 64;

/// One key can have this many candidates, with both of its buckets full
const MAX_CANDIDATES: usize = 2 * WAYS;

/// An overwrite reads every version of a key holding this many, so its two buckets never fill
const SETTLE_AT: usize = 3;

/// One small length class covers this many payload bytes
const SMALL_STEP: u64 = 64;

/// Small length classes, each one step wider than the last
const SMALL_CLASSES: usize = 16;

/// Length classes in all, enough for the last one to cover any length
const CLASSES: usize = 192;

/// One eighth of an octave as 16-bit fixed point, so each wide class is about 9% wider
const EIGHTHS: [u64; 8] = [
    65_536, 71_468, 77_936, 84_990, 92_682, 101_070, 110_218, 120_194,
];

/// The upper payload bound of each length class
static BOUNDS: [u32; CLASSES] = bounds();

const TAG_SHIFT: u32 = 16;
const CLASS_SHIFT: u32 = 8;
const CLASS_MASK: u32 = 0xFF;
const SECOND: u32 = 1;
const GRAVE: u32 = 2;

/// An overwritten version already booked dead from its length class, left for compaction
const DISPLACED: u32 = 4;

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
pub enum SpotRead {
    /// The record holds the key asked about, read with its payload
    Found(Head, Value),

    /// A lone candidate's record at this length, confirmed by its own check with no version read
    Newest(u32, Value),

    /// The record is a tombstone for the key asked about
    Tombstone(Head),

    /// The record holds another key
    Other,

    /// The segment is gone, so whatever the entry pointed at has moved on or died
    Gone,

    /// The checked read has to settle the record, as when it fails its checksum
    Unsure,
}

/// One sealed row an open takes into the spot index
pub struct Taken<'a> {
    /// The row's key
    pub key: &'a [u8],

    /// Where its record is
    pub loc: Loc,

    /// The version it holds
    pub lsn: Lsn,

    /// Whether it is a point tombstone
    pub is_tombstone: bool,
}

/// What one load step did to a key's counted version, as payload lengths
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Booking {
    /// The record whose slot went, nothing for a tombstone or no slot
    pub gone: Option<u32>,

    /// The record whose slot came in, nothing for a tombstone or no slot
    pub came: Option<u32>,
}

/// Set-aside rows a footer could not settle, what each of the rest booked, and each version that lost with the number that outversioned it
type Settling = (Vec<usize>, Vec<(usize, Booking)>, Vec<(usize, Loc, Lsn)>);

/// What an overwrite settled: records it booked dead, and class bookings it corrected
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Displaced {
    /// Records taken out, each at its true length
    pub booked: Vec<Loc>,

    /// Records marked displaced, each at the middle of its class beside the least length its class covers
    pub classed: Vec<(Loc, u32)>,

    /// Records booked from their class by an earlier overwrite, each at its true length beside the least length booked
    pub rebooked: Vec<(Loc, u32)>,
}

/// A class booking corrected by a retiring segment's footer
#[derive(Debug, PartialEq, Eq)]
pub struct Rebooked {
    /// The record's key
    pub key: RecordKey,

    /// The length booked, the least its class covers
    pub booked: u32,

    /// The record's true length
    pub actual: u32,
}

/// A record for a lookup to read: where it sits, and how much of its payload to ask for
#[derive(Clone, Copy, Debug)]
pub struct Candidate {
    /// The record's segment
    pub segment: SegmentId,

    /// The record's offset in its segment
    pub offset: u32,

    /// How many payload bytes to ask for, the top of the slot's length class
    pub bound: u32,

    /// Whether the slot is the key's only one, so the read may answer with no version
    pub alone: bool,

    /// The candidate's slot in the table
    slot: Slot,
}

/// A lookup in progress: the candidates in order, and the newest version read so far
pub struct Pick {
    /// The key's hash
    hash: u64,

    /// The candidates in read order, each with its segment's ceiling when there are several
    ordered: Vec<(Option<Lsn>, Slot)>,

    /// Where the next candidate sits in the order
    next: usize,

    /// The newest version read so far, with no payload for a tombstone
    best: Option<(Head, Option<Value>)>,

    /// Whether the best version so far came from a displaced slot
    best_displaced: bool,

    /// Whether a lone candidate may answer with no version, which a cue reader cannot use
    takes_newest: bool,

    /// Whether the best version came with no sequence number
    is_versionless: bool,

    /// Each older version read past and its length, for the cleaner
    stale: Vec<(Slot, u32)>,
}

/// What the header path makes of a key's slots
#[derive(Debug, PartialEq, Eq)]
pub enum Settled {
    /// The newest version in a live slot, a grave for a tombstone, or nothing
    Entry(Option<Entry>),

    /// The newest version sits in a displaced slot, which only the footers can settle
    Footers,
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
    /// The key's newest sealed version and its payload
    Found(Lsn, Value),

    /// The key's one sealed version, confirmed by its record's own check, its sequence number unread
    Newest(Value),

    /// The key has no sealed version, or its newest is a tombstone
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

    /// The length of the key's data record at a place, read whole up to `bound`, when the record's own check proves it is the key's
    fn checked_len(
        &self,
        key: &RecordKey,
        segment: SegmentId,
        offset: u32,
        bound: u32,
    ) -> Result<Option<u32>>;

    /// The record at a place, in one read of its header and up to `bound` payload bytes
    fn record(
        &self,
        key: &RecordKey,
        segment: SegmentId,
        offset: u32,
        bound: u32,
        alone: bool,
    ) -> Result<SpotRead>;
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
                ((SMALL_CLASSES as u64 * SMALL_STEP) << (wide / 8)) * EIGHTHS[wide % 8] / EIGHTHS[0]
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
    #[cfg(test)]
    if let Some(shared) = tests::shared_hash(key) {
        return shared;
    }
    const ODD: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut state = 0xCBF2_9CE4_8422_2325 ^ key.len() as u64;
    let mut chunks = key.chunks_exact(8);
    for chunk in &mut chunks {
        let mut word = [0u8; 8];
        word.copy_from_slice(chunk);
        state = (state ^ u64::from_le_bytes(word))
            .wrapping_mul(ODD)
            .rotate_left(29);
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

    /// The record's offset in its segment
    offset: u32,

    /// The key's mid hash bits, which pick its home bucket
    mid: u32,

    /// Tag, length class, and the bits for second bucket, grave and displaced
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

    fn is_displaced(&self) -> bool {
        self.meta & DISPLACED != 0
    }

    /// The least length in the slot's class, which live bytes book with no read so they never go below the truth
    fn least(&self) -> u32 {
        match (self.meta >> CLASS_SHIFT) & CLASS_MASK {
            0 => 0,
            class => bound_of(class - 1) + 1,
        }
    }

    /// How far the slot's true length can sit above the least of its class
    fn width(&self) -> u32 {
        bound_of((self.meta >> CLASS_SHIFT) & CLASS_MASK) - self.least()
    }

    /// The middle of the slot's length class, which a segment's dead bytes book with no read
    fn middle(&self) -> u32 {
        self.least() + self.width() / 2
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

/// The slots holding one key's bits, at most both of its buckets full
#[derive(Clone, Debug)]
struct Places {
    held: [Place; MAX_CANDIDATES],
    count: usize,
}

impl Places {
    fn new() -> Places {
        Places {
            held: [Place::default(); MAX_CANDIDATES],
            count: 0,
        }
    }

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

impl PartialEq for Places {
    fn eq(&self, other: &Places) -> bool {
        **self == **other
    }
}

impl Eq for Places {}

struct Table {
    /// The buckets, one cache line of slots each
    buckets: Vec<Bucket>,

    /// How many buckets a key's mid can land on as its home, which is every bucket
    homes: usize,

    /// How many slots are in use
    held: usize,

    /// Random state that picks which slot each kick moves
    seed: u64,

    /// How far into a growth step this shard's ladder sits
    phase: f64,
}

/// Where shard `at`'s ladder sits within one growth step
fn phase_of(at: usize) -> f64 {
    at as f64 / SHARDS as f64
}

/// The next rung for a table of `len` homes, on a ladder offset by `phase` so shards grow at different times
fn next_rung(len: usize, phase: f64) -> usize {
    let least = len as f64 * LEAST_GROWTH / FIRST_BUCKETS as f64;
    let rung = (least.ln() / GROWTH.ln() - phase).ceil();
    let buckets = (FIRST_BUCKETS as f64 * GROWTH.powf(rung + phase)).round() as usize;
    buckets.max(len + 1)
}

/// The lowest rung of the ladder offset by `phase`
fn first_rung(phase: f64) -> usize {
    (FIRST_BUCKETS as f64 * GROWTH.powf(phase)).round() as usize
}

impl Table {
    fn with_buckets(count: usize, phase: f64) -> Table {
        let homes = count.max(2);
        Table {
            buckets: vec![Bucket::default(); homes],
            homes,
            held: 0,
            seed: count as u64,
            phase,
        }
    }

    fn home(&self, mid: u32) -> usize {
        ((u64::from(mid) * self.homes as u64) >> 32) as usize
    }

    fn set_at(&mut self, at: usize, slot: Slot) {
        self.buckets[at / WAYS].slots[at % WAYS] = slot;
    }

    fn step(&self, tag: u32) -> usize {
        1 + (mix(u64::from(tag)) % (self.buckets.len() as u64 - 1)) as usize
    }

    /// The other of the two buckets a slot's key may use
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
        let mut found = Places::new();
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

    /// Put a slot in either of its key's buckets, moving others along, or hand back whichever slot ends up homeless
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
        self.held as f64 >= (self.homes * WAYS) as f64 * LOAD
    }

    /// A copy of this table at the next rung that fits every slot
    fn grown(&self) -> Table {
        let mut count = next_rung(self.homes, self.phase);
        loop {
            let mut table = Table::with_buckets(count, self.phase);
            let slots = self
                .buckets
                .iter()
                .flat_map(|bucket| bucket.slots.iter())
                .copied();
            if slots
                .filter(|slot| !slot.is_empty())
                .all(|slot| table.place(slot).is_ok())
            {
                return table;
            }
            count = next_rung(count, self.phase);
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

    /// Take out the slot pointing at one record, wherever displacement has moved it, handing it back
    fn take(&mut self, hash: u64, slot: &Slot) -> Option<Slot> {
        let found = self.matches(hash);
        let place = *found.iter().find(|place| place.slot.same_place(slot))?;
        self.set_at(place.bucket * WAYS + place.way, Slot::default());
        self.held -= 1;
        Some(place.slot)
    }

    /// Mark the slot pointing at one record displaced, unless it already is
    fn mark_displaced(&mut self, hash: u64, slot: &Slot) -> bool {
        let found = self.matches(hash);
        match found
            .iter()
            .find(|place| place.slot.same_place(slot) && !place.slot.is_displaced())
        {
            Some(place) => {
                self.buckets[place.bucket].slots[place.way].meta |= DISPLACED;
                true
            }
            None => false,
        }
    }

    /// Keep the slots a test passes and empty the rest, handing back how many went and how many of those were displaced
    fn retain(&mut self, keep: impl Fn(&Slot) -> bool) -> (usize, u64) {
        let before = self.held;
        let mut displaced = 0;
        for bucket in self.buckets.iter_mut() {
            for slot in bucket.slots.iter_mut() {
                if !slot.is_empty() && !keep(slot) {
                    displaced += u64::from(slot.is_displaced());
                    *slot = Slot::default();
                    self.held -= 1;
                }
            }
        }
        (before - self.held, displaced)
    }
}

/// An older version passed over by a lookup, for the cleaner to take out
struct Stale {
    key: RecordKey,
    hash: u64,
    slot: Slot,
    len: u32,
}

/// The slot for a set-aside row, a grave for a tombstone
fn slot_of(hash: u64, row: &SetAside) -> Slot {
    match row.is_tombstone {
        true => Slot::new(hash, row.loc).as_grave(),
        false => Slot::new(hash, row.loc),
    }
}

/// A sealed row set aside by an open, because a slot already shared its key's bits
struct SetAside {
    key: RecordKey,
    loc: Loc,
    lsn: Lsn,
    is_tombstone: bool,
}

/// One shard of a column's table, and how many slots have left it
struct Shard {
    /// The shard's table of slots
    table: RwLock<Table>,

    /// Slots taken out so far, which a lookup that found nothing checks for a race
    taken: AtomicU64,

    /// Slots marked displaced and still held
    displaced: AtomicU64,
}

impl Shard {
    fn new(at: usize) -> Shard {
        Shard {
            table: RwLock::new(Table::with_buckets(first_rung(phase_of(at)), phase_of(at))),
            taken: AtomicU64::new(0),
            displaced: AtomicU64::new(0),
        }
    }

    fn read(&self) -> RwLockReadGuard<'_, Table> {
        read(&self.table)
    }

    fn write(&self) -> Writing<'_> {
        Writing {
            table: write(&self.table),
            taken: &self.taken,
            displaced: &self.displaced,
        }
    }
}

/// A shard held for writing, which counts every slot it takes out before it lets go
struct Writing<'a> {
    table: RwLockWriteGuard<'a, Table>,
    taken: &'a AtomicU64,
    displaced: &'a AtomicU64,
}

impl Writing<'_> {
    /// Take out the slot pointing at one record, wherever displacement has moved it
    fn take(&mut self, hash: u64, slot: &Slot) -> bool {
        self.take_slot(hash, slot).is_some()
    }

    /// Take out the slot pointing at one record, handing back what it held
    fn take_slot(&mut self, hash: u64, slot: &Slot) -> Option<Slot> {
        let took = self.table.take(hash, slot)?;
        self.taken.fetch_add(1, Ordering::Release);
        if took.is_displaced() {
            self.displaced.fetch_sub(1, Ordering::Relaxed);
        }
        Some(took)
    }

    /// Take out the slot pointing at one record unless an overwrite already booked it
    fn take_unbooked(&mut self, hash: u64, slot: &Slot) -> bool {
        let found = self.table.matches(hash);
        let is_booked = found
            .iter()
            .any(|place| place.slot.same_place(slot) && place.slot.is_displaced());
        !is_booked && self.take(hash, slot)
    }

    /// Mark the slot pointing at one record displaced, unless it already is
    fn mark_displaced(&mut self, hash: u64, slot: &Slot) -> bool {
        let marked = self.table.mark_displaced(hash, slot);
        if marked {
            self.displaced.fetch_add(1, Ordering::Relaxed);
        }
        marked
    }

    /// Count slots a sweep emptied by hand, and how many of them were displaced
    fn count_taken(&self, slots: u64, displaced: u64) {
        if slots > 0 {
            self.taken.fetch_add(slots, Ordering::Release);
        }
        if displaced > 0 {
            self.displaced.fetch_sub(displaced, Ordering::Relaxed);
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

/// A column's sealed keys as record locations, in shards picked by their bits
pub struct SpotColumn {
    /// The shards, picked by the top byte of a key's hash
    shards: Vec<Shard>,

    /// Where lookups read records, set once by attach
    records: OnceLock<Arc<dyn RecordSource>>,

    /// The counters that order segments, set once by attach
    segments: OnceLock<Arc<SegmentTable>>,

    /// Older versions waiting for the cleaner
    stale: Mutex<VecDeque<Stale>>,

    /// How many older versions wait for the cleaner
    beside: AtomicU64,

    /// How far the class bookings may sit below the truth, at most a class width each
    slack: AtomicU64,

    /// Rows taken by an open before it could read a header, settled once it can
    set_aside: Mutex<Vec<SetAside>>,

    /// Whether a live slot whose segment went stays for the next pass, as a read-only open needs
    follows: AtomicBool,

    /// How many lookups on a read-only open met a live slot whose segment went
    behind: AtomicU64,
}

impl Default for SpotColumn {
    fn default() -> SpotColumn {
        SpotColumn::new()
    }
}

impl SpotColumn {
    /// An empty column, which reads no records until it is attached
    pub fn new() -> SpotColumn {
        SpotColumn {
            shards: (0..SHARDS).map(Shard::new).collect(),
            records: OnceLock::new(),
            segments: OnceLock::new(),
            stale: Mutex::new(VecDeque::new()),
            beside: AtomicU64::new(0),
            slack: AtomicU64::new(0),
            set_aside: Mutex::new(Vec::new()),
            follows: AtomicBool::new(false),
            behind: AtomicU64::new(0),
        }
    }

    /// Where lookups read records, and the counters that order segments
    pub fn attach(&self, records: Arc<dyn RecordSource>, segments: Arc<SegmentTable>) {
        let _ = self.records.set(records);
        let _ = self.segments.set(segments);
    }

    /// Keep each live slot whose segment went, for a read-only open whose next pass moves or books it
    pub fn follow(&self) {
        self.follows.store(true, Ordering::Release);
    }

    /// How many lookups on a read-only open met a live slot whose segment went
    pub fn behind(&self) -> u64 {
        self.behind.load(Ordering::Acquire)
    }

    /// Whether a read-only open keeps this slot whose segment went, counting the lookup that met it
    fn keeps_gone(&self, slot: &Slot) -> bool {
        let keeps = self.follows.load(Ordering::Acquire) && !slot.is_displaced();
        if keeps {
            self.behind.fetch_add(1, Ordering::AcqRel);
        }
        keeps
    }

    /// Older versions waiting for the cleaner
    pub fn beside(&self) -> u64 {
        self.beside.load(Ordering::Relaxed)
    }

    /// How far the byte counters may sit from the truth, from every class booking so far
    pub fn slack(&self) -> u64 {
        self.slack.load(Ordering::Relaxed)
    }

    /// Overwritten versions booked from their length class and held until compaction
    pub fn displaced(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| shard.displaced.load(Ordering::Relaxed))
            .sum()
    }

    /// Entries held
    pub fn held(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| shard.read().held as u64)
            .sum()
    }

    /// How many bytes the bucket tables take
    pub fn heap_bytes(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| shard.read().buckets.len() as u64 * BUCKET_BYTES)
            .sum()
    }

    /// Hold the location of a key's newest sealed record, leaving older entries to the cleaner, and say whether it took a new slot
    pub fn insert(&self, key: &[u8], loc: Loc) -> bool {
        let hash = hash_of(key);
        put_in(&mut self.shards[shard_of(hash)].write(), hash, loc)
    }

    /// Put in the keys of the shards whose number leaves `lane` over `lanes`, and say which took a new slot
    pub fn insert_lane(&self, rows: &[(&[u8], Loc)], lane: usize, lanes: usize) -> Vec<bool> {
        let mut inserted = vec![false; rows.len()];
        for (shard, ats) in lane_groups(rows, lane, lanes) {
            for chunk in ats.chunks(LOCK_CHUNK) {
                let mut table = self.shards[shard].write();
                for &at in chunk {
                    let (key, loc) = rows[at];
                    inserted[at] = put_in(&mut table, hash_of(key), loc);
                }
            }
        }
        inserted
    }

    /// Take out the slots of one lane of shards that point at these records
    pub fn remove_lane(&self, rows: &[(&[u8], Loc)], lane: usize, lanes: usize) {
        for (shard, ats) in lane_groups(rows, lane, lanes) {
            for chunk in ats.chunks(LOCK_CHUNK) {
                let mut table = self.shards[shard].write();
                for &at in chunk {
                    let (key, loc) = rows[at];
                    let hash = hash_of(key);
                    let held = table
                        .matches(hash)
                        .iter()
                        .find(|place| place.slot.at(loc))
                        .copied();
                    if let Some(place) = held {
                        table.take(hash, &place.slot);
                    }
                }
            }
        }
    }

    /// Size every shard for about this many keys, so a load does not grow them a step at a time
    pub fn reserve(&self, keys: u64) {
        let per_shard = keys.div_ceil(SHARDS as u64) as f64;
        let buckets = (per_shard / (WAYS as f64 * LOAD)).ceil() as usize;
        for (at, shard) in self.shards.iter().enumerate() {
            let mut table = shard.write();
            if table.held == 0 && table.homes < buckets {
                *table = Table::with_buckets(buckets, phase_of(at));
            }
        }
    }

    /// Take a sealed footer partition's rows during an open, and hand back each fresh record's row and length
    pub fn take_partition(
        &self,
        segment: SegmentId,
        partition: &FooterPartition,
    ) -> Result<Vec<(u32, u32)>> {
        let spread = segment.as_u32() as usize;
        self.take_rows(partition.column, spread, partition.len(), |at| {
            let found = partition.row_at(at)?;
            let key = partition
                .key_at(at)
                .ok_or_else(|| ReelError::Corruption("footer row is out of range".to_string()))?;
            Ok((!found.is_range_tombstone()).then(|| Taken {
                key,
                loc: Loc::new(segment, found.offset, found.len),
                lsn: found.lsn,
                is_tombstone: found.is_tombstone(),
            }))
        })
    }

    /// Take sealed rows during an open, setting aside each that meets a slot, and hand back each fresh record's row and length
    pub fn take_rows<'a>(
        &self,
        column: ColumnId,
        spread: usize,
        count: usize,
        row: impl Fn(usize) -> Result<Option<Taken<'a>>>,
    ) -> Result<Vec<(u32, u32)>> {
        let mut by_shard: Vec<Vec<(u64, Slot, u32, u32)>> = vec![Vec::new(); SHARDS];
        for at in 0..count {
            let Some(taken) = row(at)? else {
                continue;
            };
            let hash = hash_of(taken.key);
            let slot = Slot::new(hash, taken.loc);
            let slot = match taken.is_tombstone {
                true => slot.as_grave(),
                false => slot,
            };
            by_shard[shard_of(hash)].push((hash, slot, at as u32, taken.loc.len));
        }
        // Loaders start at different shards, so two of them rarely want the same lock
        let first = spread % SHARDS;
        let (mut aside, mut fresh) = (Vec::new(), Vec::new());
        for shard in (first..SHARDS).chain(0..first) {
            let rows = &by_shard[shard];
            if rows.is_empty() {
                continue;
            }
            let mut table = self.shards[shard].write();
            for (hash, slot, at, len) in rows {
                match table.matches(*hash).is_empty() {
                    true => {
                        table.insert(*slot);
                        if !slot.is_grave() {
                            fresh.push((*at, *len));
                        }
                    }
                    false => aside.push(*at),
                }
            }
        }
        fresh.sort_unstable();
        if aside.is_empty() {
            return Ok(fresh);
        }
        let mut set_aside = Vec::with_capacity(aside.len());
        for at in aside {
            if let Some(taken) = row(at as usize)? {
                set_aside.push(SetAside {
                    key: RecordKey::from_bytes(column, taken.key)?,
                    loc: taken.loc,
                    lsn: taken.lsn,
                    is_tombstone: taken.is_tombstone,
                });
            }
        }
        lock(&self.set_aside).extend(set_aside);
        Ok(fresh)
    }

    /// Settle the rows an open set aside against the footers of the slots they met, reading headers for the rest
    ///
    /// `lost` hears each data version a footer settled out, with the number of the version that came next, and a key the headers settle tells it nothing.
    pub fn settle_rows(
        &self,
        column: ColumnId,
        footers: &dyn FooterSource,
        book: &(dyn Fn(&[u8], Booking) + Sync),
        lost: &(dyn Fn(&[u8], Loc, Lsn) + Sync),
    ) -> Result<()> {
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
                1 => by_segment
                    .entry(seen[0].slot.segment())
                    .or_default()
                    .push(at),
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
                        while let Some((segment, group)) =
                            groups.get(next.fetch_add(1, Ordering::Relaxed))
                        {
                            let (left, booked, gone) =
                                self.settle_against(column, *segment, group, &rows, footers)?;
                            for (at, booking) in booked {
                                book(rows[at].key.as_slice(), booking);
                            }
                            for (at, loc, after) in gone {
                                lost(rows[at].key.as_slice(), loc, after);
                            }
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
            book(
                row.key.as_slice(),
                self.load(&row.key, row.loc, row.lsn, row.is_tombstone)?,
            );
        }
        Ok(())
    }

    /// Settle the rows whose slot sits in one segment, handing back those its footer cannot, what the rest booked and each version that lost
    fn settle_against(
        &self,
        column: ColumnId,
        segment: SegmentId,
        group: &[usize],
        rows: &[SetAside],
        footers: &dyn FooterSource,
    ) -> Result<Settling> {
        let Some(footer) = footers.footer_once(segment)? else {
            return Ok((group.to_vec(), Vec::new(), Vec::new()));
        };
        let Some(partition) = footer.partition(column) else {
            return Ok((group.to_vec(), Vec::new(), Vec::new()));
        };
        let mut group = group.to_vec();
        group.sort_unstable_by(|left, right| rows[*left].key.cmp(&rows[*right].key));
        // A slot counts as this key's only when its footer row says so, since keys can share bits
        let mut rows_at: Option<HashMap<u32, usize>> = None;
        let mut row_at = |offset: u32| -> Result<Option<usize>> {
            if rows_at.is_none() {
                let mut at_offset = HashMap::with_capacity(partition.len());
                for at in 0..partition.len() {
                    at_offset.insert(partition.row_at(at)?.offset, at);
                }
                rows_at = Some(at_offset);
            }
            Ok(rows_at.as_ref().and_then(|rows| rows.get(&offset)).copied())
        };
        let (mut unsettled, mut booked, mut gone) = (Vec::new(), Vec::new(), Vec::new());
        for same_key in group.chunk_by(|left, right| rows[*left].key == rows[*right].key) {
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
                // The slot this row met holds another key
                FooterFind::Missing | FooterFind::RuledOut => {
                    unsettled.extend_from_slice(same_key);
                    continue;
                }
            };
            let hash = hash_of(row.key.as_slice());
            let mut table = self.shards[shard_of(hash)].write();
            let held = table.matches(hash);
            let place = held
                .iter()
                .find(|place| place.slot.segment() == segment)
                .copied();
            let is_newer = (row.lsn, row.loc.segment) > (standing.lsn, segment);
            let came = (!row.is_tombstone).then_some(row.loc.len);
            // The version the slot held, with its place when it is data
            let slotted = match place {
                // The slot is the version this footer holds, so the newer of the two stands
                Some(place) if place.slot.offset == standing.offset => {
                    let data = (!place.slot.is_grave()).then_some(standing.len);
                    if is_newer {
                        table.take(hash, &place.slot);
                        table.insert(slot_of(hash, row));
                        booked.push((newest, Booking { gone: data, came }));
                    }
                    let data = data.map(|len| Loc::new(segment, standing.offset, len));
                    Some((standing.lsn, segment, data))
                }
                // The slot is an older version of the key in this row's segment, whose footer points at this row
                Some(place) if segment == row.loc.segment && standing.offset == row.loc.offset => {
                    match row_at(place.slot.offset)? {
                        Some(older) if partition.key_at(older) == Some(row.key.as_slice()) => {
                            let older = partition.row_at(older)?;
                            table.take(hash, &place.slot);
                            table.insert(slot_of(hash, row));
                            let data = (!place.slot.is_grave()).then_some(older.len);
                            booked.push((newest, Booking { gone: data, came }));
                            let data = data.map(|len| Loc::new(segment, place.slot.offset, len));
                            Some((older.lsn, segment, data))
                        }
                        Some(_) | None => None,
                    }
                }
                Some(_) | None => None,
            };
            drop(table);
            let Some(slotted) = slotted else {
                unsettled.extend_from_slice(same_key);
                continue;
            };
            // Every version met here in order, each data version followed by the one that outversioned it
            let mut versions: Vec<(Lsn, SegmentId, Option<Loc>)> = same_key
                .iter()
                .map(|at| {
                    let met = &rows[*at];
                    (
                        met.lsn,
                        met.loc.segment,
                        (!met.is_tombstone).then_some(met.loc),
                    )
                })
                .collect();
            versions.push(slotted);
            versions.sort_unstable_by_key(|(lsn, segment, _)| (*lsn, *segment));
            for pair in versions.windows(2) {
                if let Some(loc) = pair[0].2 {
                    gone.push((newest, loc, pair[1].0));
                }
            }
        }
        Ok((unsettled, booked, gone))
    }

    /// Load one sealed row, keeping each key's newest version, a tie to the newer segment, a tombstone as a grave
    pub fn load(&self, key: &RecordKey, loc: Loc, lsn: Lsn, is_tombstone: bool) -> Result<Booking> {
        let Some(records) = self.records.get() else {
            return Ok(Booking::default());
        };
        let hash = hash_of(key.as_slice());
        let slot = match is_tombstone {
            true => Slot::new(hash, loc).as_grave(),
            false => Slot::new(hash, loc),
        };
        let came = Booking {
            gone: None,
            came: (!is_tombstone).then_some(loc.len),
        };
        let shard = shard_of(hash);
        {
            let mut table = self.shards[shard].write();
            if table.matches(hash).is_empty() {
                table.insert(slot);
                return Ok(came);
            }
        }
        loop {
            let seen = self.shards[shard].read().matches(hash);
            let mut standing = None;
            for place in seen.iter() {
                if let HeadRead::Same(head) =
                    records.head(key.as_ref(), place.slot.segment(), place.slot.offset)?
                {
                    standing = Some((place.slot, head));
                }
            }
            let mut table = self.shards[shard].write();
            // Another thread loaded a row of this key while the headers were read
            if *table.matches(hash) != *seen {
                continue;
            }
            return Ok(match standing {
                Some((held, head)) if (lsn, slot.segment) > (head.lsn, held.segment) => {
                    table.take(hash, &held);
                    table.insert(slot);
                    Booking {
                        gone: (!head.is_tombstone).then_some(head.len),
                        ..came
                    }
                }
                Some(_) => Booking::default(),
                None => {
                    table.insert(slot);
                    came
                }
            });
        }
    }

    /// Close a load by dropping the graves that only kept older rows out
    pub fn finish_load(&self) {
        for shard in &self.shards {
            let mut table = shard.write();
            let (dropped, displaced) = table.retain(|slot| !slot.is_grave());
            table.count_taken(dropped as u64, displaced);
        }
    }

    /// Whether a key's one live slot points at this record, which a caller that read it can trust with no read
    pub fn only_at(&self, key: &[u8], loc: Loc) -> bool {
        let hash = hash_of(key);
        let seen = self.shards[shard_of(hash)].read().matches(hash);
        let mut live = seen.iter().filter(|place| !place.slot.is_displaced());
        match (live.next(), live.next()) {
            (Some(place), None) => place.slot.at(loc),
            (Some(_), Some(_)) | (None, _) => false,
        }
    }

    /// Whether a key's one live slot points somewhere other than this record, which makes the record an older version
    pub fn live_elsewhere(&self, key: &[u8], loc: Loc) -> bool {
        let hash = hash_of(key);
        let seen = self.shards[shard_of(hash)].read().matches(hash);
        let mut live = seen.iter().filter(|place| !place.slot.is_displaced());
        match (live.next(), live.next()) {
            (Some(place), None) => !place.slot.at(loc),
            (Some(_), Some(_)) | (None, _) => false,
        }
    }

    /// Where a key's one live record sits, nothing for a grave or a second live slot
    pub fn only_live(&self, key: &[u8]) -> Option<(SegmentId, u32)> {
        let hash = hash_of(key);
        let seen = self.shards[shard_of(hash)].read().matches(hash);
        let mut live = seen.iter().filter(|place| !place.slot.is_displaced());
        match (live.next(), live.next()) {
            (Some(place), None) if !place.slot.is_grave() => {
                Some((place.slot.segment(), place.slot.offset))
            }
            (Some(_), _) | (None, _) => None,
        }
    }

    /// Take out the live entry pointing at one record, leaving a displaced one for compaction to rebook
    pub fn take_live(&self, key: &[u8], loc: Loc) -> bool {
        let hash = hash_of(key);
        let mut table = self.shards[shard_of(hash)].write();
        let held = table
            .matches(hash)
            .iter()
            .find(|place| place.slot.at(loc) && !place.slot.is_displaced())
            .copied();
        match held {
            Some(place) => table.take(hash, &place.slot),
            None => false,
        }
    }

    /// Book a key's entries older than `newer` dead, leaving unread ones in place for lookups in flight
    pub fn displace(&self, key: &RecordKey, newer: Lsn) -> Result<Displaced> {
        let (Some(records), Some(segments)) = (self.records.get(), self.segments.get()) else {
            return Ok(Displaced::default());
        };
        let hash = hash_of(key.as_slice());
        let shard = shard_of(hash);
        let seen = self.shards[shard].read().matches(hash);
        if seen.is_empty() {
            return Ok(Displaced::default());
        }
        let must_read = seen.len() >= SETTLE_AT;
        let (mut unread, mut older, mut gone) = (Vec::new(), Vec::new(), Vec::new());
        for place in seen.iter() {
            let slot = place.slot;
            let is_older = segments
                .max_lsn_of(slot.segment())
                .is_some_and(|ceiling| ceiling < newer);
            match (must_read, slot.is_displaced(), is_older) {
                (false, true, _) => continue,
                (false, false, true) => {
                    unread.push(slot);
                    continue;
                }
                (false, false, false) | (true, _, _) => {}
            }
            let answer = match records.cached_head(key.as_ref(), slot.segment(), slot.offset)? {
                HeadRead::Cold => {
                    // A version in an older segment is older than this write, so its own check settles it with no footer search
                    let checked = match is_older && !slot.is_grave() {
                        true => records.checked_len(key, slot.segment(), slot.offset, slot.bound())?,
                        false => None,
                    };
                    if let Some(len) = checked {
                        older.push((slot, len));
                        continue;
                    }
                    records.head(key.as_ref(), slot.segment(), slot.offset)?
                }
                answer => answer,
            };
            match answer {
                HeadRead::Same(head) if head.lsn < newer => older.push((slot, head.len)),
                HeadRead::Same(_) | HeadRead::Other | HeadRead::Cold => {}
                // The length went with the segment, so a version still counted is booked from its class
                HeadRead::Missing if !slot.is_displaced() => unread.push(slot),
                HeadRead::Missing => gone.push(slot),
            }
        }
        let mut settled = Displaced::default();
        let mut table = self.shards[shard].write();
        for slot in &gone {
            table.take(hash, slot);
        }
        for (slot, len) in &older {
            let loc = Loc::new(slot.segment(), slot.offset, *len);
            match table.take_slot(hash, slot) {
                // Booked from its class earlier, so the header just read corrects that booking
                Some(took) if took.is_displaced() => {
                    settled.rebooked.push((loc, took.least()));
                    self.unslack(&took);
                }
                Some(_) => settled.booked.push(loc),
                None => {}
            }
        }
        for slot in &unread {
            if table.mark_displaced(hash, slot) {
                settled.classed.push((
                    Loc::new(slot.segment(), slot.offset, slot.middle()),
                    slot.least(),
                ));
                self.slack
                    .fetch_add(u64::from(slot.width()), Ordering::Relaxed);
            }
        }
        Ok(settled)
    }

    /// Drop the slack one class booking stood for, once its true length is known
    fn unslack(&self, slot: &Slot) {
        let width = u64::from(slot.width());
        let _ = self
            .slack
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |slack| {
                Some(slack.saturating_sub(width))
            });
    }

    /// Take out every displaced entry pointing into a retiring segment, with the true length from its footer row
    pub fn settle_displaced_in(
        &self,
        segment: SegmentId,
        partition: &FooterPartition,
    ) -> Result<Vec<Rebooked>> {
        let mut rebooked = Vec::new();
        if self.displaced() == 0 {
            return Ok(rebooked);
        }
        for at in 0..partition.len() {
            let entry = partition.entry_at(at)?;
            if entry.is_tombstone() || entry.is_range_tombstone() {
                continue;
            }
            let hash = hash_of(entry.key.as_slice());
            let shard = &self.shards[shard_of(hash)];
            if shard.displaced.load(Ordering::Relaxed) == 0 {
                continue;
            }
            let loc = Loc::new(segment, entry.offset, entry.len);
            let held = shard
                .read()
                .matches(hash)
                .iter()
                .find(|place| place.slot.at(loc) && place.slot.is_displaced())
                .map(|place| place.slot);
            let Some(slot) = held else {
                continue;
            };
            if shard.write().take_slot(hash, &slot).is_some() {
                self.unslack(&slot);
                rebooked.push(Rebooked {
                    key: entry.key,
                    booked: slot.least(),
                    actual: entry.len,
                });
            }
        }
        Ok(rebooked)
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
            let took = self.shards[shard_of(stale.hash)]
                .write()
                .take_unbooked(stale.hash, &stale.slot);
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
        let mut ordered: Vec<(Option<Lsn>, Slot)> =
            seen.iter().map(|place| (None, place.slot)).collect();
        if ordered.len() > 1 {
            for (ceiling, slot) in ordered.iter_mut() {
                *ceiling = segments.max_lsn_of(slot.segment());
            }
            ordered.sort_unstable_by(|left, right| {
                (right.0, right.1.segment).cmp(&(left.0, left.1.segment))
            });
        }
        (hash, ordered)
    }

    /// A key's newest payload in one read of each candidate a ceiling cannot rule out
    pub fn read(&self, key: &RecordKey) -> Result<Lookup> {
        self.read_taking(key, true)
    }

    /// The same read for a cue reader, which needs every answer's sequence number
    pub fn read_versioned(&self, key: &RecordKey) -> Result<Lookup> {
        self.read_taking(key, false)
    }

    fn read_taking(&self, key: &RecordKey, takes_newest: bool) -> Result<Lookup> {
        let Some(records) = self.records.get() else {
            return Ok(Lookup::Unsettled);
        };
        'tries: for _ in 0..LOOKUP_TRIES {
            let Some(mut pick) = self.pick(key) else {
                return Ok(Lookup::Unsettled);
            };
            pick.takes_newest = takes_newest;
            while let Some(candidate) = self.next(&mut pick) {
                let read = records.record(
                    key,
                    candidate.segment,
                    candidate.offset,
                    candidate.bound,
                    candidate.alone,
                )?;
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

    /// Continue a started lookup, one read of each candidate left, and settle it
    pub fn read_on(&self, key: &RecordKey, mut pick: Pick) -> Result<Lookup> {
        let Some(records) = self.records.get() else {
            return Ok(Lookup::Unsettled);
        };
        while let Some(candidate) = self.next(&mut pick) {
            let read = records.record(
                key,
                candidate.segment,
                candidate.offset,
                candidate.bound,
                candidate.alone,
            )?;
            match self.offer(&mut pick, candidate, read) {
                Offered::Next => {}
                Offered::Again | Offered::Unsettled => return Ok(Lookup::Unsettled),
            }
        }
        Ok(self.settle(key, pick))
    }

    /// The key's one live candidate, which a single read can answer for, leaving displaced ones to the full lookup
    pub fn sole(&self, key: &RecordKey) -> Option<Candidate> {
        let pick = self.pick(key)?;
        let slot = sole_live(&pick.ordered)?;
        Some(Candidate {
            segment: slot.segment(),
            offset: slot.offset,
            bound: slot.bound(),
            alone: !slot.is_grave(),
            slot,
        })
    }

    /// Whether a slot for the key's bits sits in a segment holding anything newer than `lsn`, with no header read
    pub fn may_hold_newer(&self, key: &[u8], lsn: Lsn) -> bool {
        let Some(segments) = self.segments.get() else {
            return true;
        };
        let hash = hash_of(key);
        self.shards[shard_of(hash)]
            .read()
            .matches(hash)
            .iter()
            // A displaced slot was booked as older than a version written since, so it is never the newer one
            .any(|place| {
                !place.slot.is_displaced()
                    && segments
                        .max_lsn_of(place.slot.segment())
                        .is_none_or(|max| max > lsn)
            })
    }

    /// Whether one of the key's slots points at a version newer than `lsn`, reading the slots in segments that may hold one
    pub fn holds_newer(&self, key: KeyRef<'_>, lsn: Lsn) -> Result<bool> {
        let (Some(records), Some(segments)) = (self.records.get(), self.segments.get()) else {
            return Ok(false);
        };
        let hash = hash_of(key.bytes);
        let seen = self.shards[shard_of(hash)].read().matches(hash);
        for place in seen.iter() {
            let segment = place.slot.segment();
            if segments.max_lsn_of(segment).is_some_and(|max| max <= lsn) {
                continue;
            }
            match records.head(key, segment, place.slot.offset)? {
                HeadRead::Same(head) if head.lsn > lsn => return Ok(true),
                HeadRead::Same(_) | HeadRead::Other | HeadRead::Missing | HeadRead::Cold => {}
            }
        }
        Ok(false)
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
    pub fn moved(&self, since: Since) -> bool {
        // A key reaches the map before its slot goes, so a miss in both stands only if nothing left
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
            best_displaced: false,
            takes_newest: true,
            is_versionless: false,
            stale: Vec::new(),
        })
    }

    /// The next candidate to read, past any whose segment's ceiling rules it out
    pub fn next(&self, pick: &mut Pick) -> Option<Candidate> {
        // A key's only live slot holding data is its newest sealed version, and every displaced slot beside it is older
        if pick.takes_newest {
            if let Some(slot) = sole_live(&pick.ordered).filter(|slot| !slot.is_grave()) {
                let first = pick.next == 0;
                pick.next = pick.ordered.len();
                return first.then(|| Candidate {
                    segment: slot.segment(),
                    offset: slot.offset,
                    bound: slot.bound(),
                    alone: true,
                    slot,
                });
            }
        }
        while let Some((ceiling, slot)) = pick.ordered.get(pick.next).copied() {
            pick.next += 1;
            if let (Some((head, _)), Some(ceiling)) = (&pick.best, ceiling) {
                if ceiling < head.lsn {
                    continue;
                }
            }
            return Some(Candidate {
                segment: slot.segment(),
                offset: slot.offset,
                bound: slot.bound(),
                alone: false,
                slot,
            });
        }
        None
    }

    /// Fold one candidate's read into a lookup
    pub fn offer(&self, pick: &mut Pick, candidate: Candidate, read: SpotRead) -> Offered {
        let (head, value) = match read {
            SpotRead::Found(head, value) => (head, Some(value)),
            // Only a lone candidate answers with no version, so nothing is held to order it against
            SpotRead::Newest(len, value) => {
                if !candidate.alone || pick.best.is_some() {
                    return Offered::Unsettled;
                }
                let head = Head {
                    lsn: Lsn::NONE,
                    len,
                    is_tombstone: false,
                };
                pick.best = Some((head, Some(value)));
                pick.best_displaced = false;
                pick.is_versionless = true;
                return Offered::Next;
            }
            SpotRead::Tombstone(head) => (head, None),
            SpotRead::Other => return Offered::Next,
            // A read-only open keeps the slot for the pass that moves or books its version
            SpotRead::Gone if self.keeps_gone(&candidate.slot) => return Offered::Unsettled,
            // The segment is gone, so the slot points at nothing and goes before the next look
            SpotRead::Gone => {
                self.shards[shard_of(pick.hash)]
                    .write()
                    .take(pick.hash, &candidate.slot);
                return Offered::Again;
            }
            SpotRead::Unsure => return Offered::Unsettled,
        };
        match &pick.best {
            Some((current, _)) if current.lsn >= head.lsn => {
                // An equal version is a compaction copy beside its source, and both stay
                if current.lsn > head.lsn {
                    pick.stale.push((candidate.slot, head.len));
                }
            }
            Some(_) | None => {
                pick.best = Some((head, value));
                pick.best_displaced = candidate.slot.is_displaced();
            }
        }
        Offered::Next
    }

    /// Close a lookup with its newest version, handing the older ones it read to the cleaner
    pub fn settle(&self, key: &RecordKey, pick: Pick) -> Lookup {
        for (slot, len) in pick
            .stale
            .into_iter()
            .filter(|(slot, _)| !slot.is_displaced())
        {
            self.queue(Stale {
                key: key.clone(),
                hash: pick.hash,
                slot,
                len,
            });
        }
        match pick.best {
            // A displaced version was overwritten or deleted once, so what came after it is the footers' to say
            Some(_) if pick.best_displaced => Lookup::Unsettled,
            Some((_, Some(value))) if pick.is_versionless => Lookup::Newest(value),
            Some((head, Some(value))) => Lookup::Found(head.lsn, value),
            Some((_, None)) | None => Lookup::Missing,
        }
    }

    /// A key's newest sealed entry, reading every candidate's header, for a caller that reads the record itself
    pub fn entry(&self, key: &RecordKey) -> Result<Settled> {
        let (Some(records), Some(segments)) = (self.records.get(), self.segments.get()) else {
            return Ok(Settled::Entry(None));
        };
        for _ in 0..LOOKUP_TRIES {
            if let Some(settled) = self.entry_once(key, records.as_ref(), segments)? {
                return Ok(settled);
            }
        }
        // Segments kept going under the look, and the footers hold still
        Ok(Settled::Footers)
    }

    /// One look at the table and a header read of each candidate, or nothing when a segment went under it
    fn entry_once(
        &self,
        key: &RecordKey,
        records: &dyn RecordSource,
        segments: &SegmentTable,
    ) -> Result<Option<Settled>> {
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
                // A read-only open keeps the slot for the pass that moves or books its version
                HeadRead::Missing if self.keeps_gone(slot) => {}
                // The segment is gone, so the slot points at nothing and goes before the next look
                HeadRead::Missing => {
                    self.shards[shard_of(hash)].write().take(hash, slot);
                    return Ok(None);
                }
                HeadRead::Same(_) | HeadRead::Other | HeadRead::Cold => {}
            }
        }
        if best.is_some_and(|(_, slot)| slot.is_displaced()) {
            return Ok(Some(Settled::Footers));
        }
        Ok(Some(Settled::Entry(best.map(
            |(head, slot)| match head.is_tombstone {
                true => Entry::grave(head.lsn),
                false => {
                    let stamp = segments.incarnation_of(slot.segment());
                    Entry::new(Loc::new(slot.segment(), slot.offset, head.len), head.lsn)
                        .stamped(stamp)
                }
            },
        ))))
    }

    /// Drop every entry pointing into a segment no longer standing, with no reads, and hand back how many were live
    pub fn forget_retired(&self, is_standing: impl Fn(SegmentId) -> bool) -> u64 {
        let mut live = 0;
        for shard in &self.shards {
            let mut table = shard.write();
            let (dropped, displaced) = table.retain(|slot| is_standing(slot.segment()));
            table.count_taken(dropped as u64, displaced);
            live += dropped as u64 - displaced;
        }
        live
    }

    /// Drop every entry, for a rebuild starting over
    pub fn clear(&self) {
        for (at, shard) in self.shards.iter().enumerate() {
            let mut table = shard.write();
            let held = table.held as u64;
            let displaced = table.displaced.load(Ordering::Relaxed);
            *table = Table::with_buckets(first_rung(phase_of(at)), phase_of(at));
            table.count_taken(held, displaced);
        }
        lock(&self.stale).clear();
        lock(&self.set_aside).clear();
        self.beside.store(0, Ordering::Relaxed);
        self.slack.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
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
        heads: AtomicU64,
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
            self.heads.fetch_add(1, Ordering::Relaxed);
            Ok(self.answer(key, segment, offset))
        }

        fn cached_head(
            &self,
            key: KeyRef<'_>,
            segment: SegmentId,
            offset: u32,
        ) -> Result<HeadRead> {
            Ok(match self.is_cold.load(Ordering::Relaxed) {
                true => HeadRead::Cold,
                false => self.answer(key, segment, offset),
            })
        }

        fn checked_len(
            &self,
            _key: &RecordKey,
            _segment: SegmentId,
            _offset: u32,
            _bound: u32,
        ) -> Result<Option<u32>> {
            Ok(None)
        }

        fn record(
            &self,
            key: &RecordKey,
            segment: SegmentId,
            offset: u32,
            bound: u32,
            _alone: bool,
        ) -> Result<SpotRead> {
            Ok(match self.answer(key.as_ref(), segment, offset) {
                HeadRead::Same(head) if head.len <= bound => {
                    SpotRead::Found(head, Value::from(head.lsn.as_u64().to_le_bytes().to_vec()))
                }
                HeadRead::Same(_) => SpotRead::Unsure,
                HeadRead::Missing => SpotRead::Gone,
                HeadRead::Other | HeadRead::Cold => SpotRead::Other,
            })
        }
    }

    fn key(at: u64) -> RecordKey {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&at.to_be_bytes());
        RecordKey::from_bytes(COLUMN, &bytes).expect("key")
    }

    /// Keys under this prefix all hash alike, so a test can stand two keys on one fingerprint
    const SHARED: &[u8] = b"shared!!";

    /// Every key under the shared prefix takes this hash
    const SHARED_HASH: u64 = 0x5EED_0000_5EED_0000;

    pub(super) fn shared_hash(key: &[u8]) -> Option<u64> {
        key.starts_with(SHARED).then_some(SHARED_HASH)
    }

    fn shared_key(at: u64) -> RecordKey {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(SHARED);
        bytes[8..].copy_from_slice(&at.to_be_bytes());
        RecordKey::from_bytes(COLUMN, &bytes).expect("key")
    }

    // an overwrite that meets another key's slot on a shared fingerprint books it, and that key's lookups go to the footers
    #[test]
    fn a_shared_fingerprint_never_loses_the_other_key() {
        let records = Arc::new(Records::default());
        let segments = Arc::new(SegmentTable::new());
        let column = SpotColumn::new();
        column.attach(
            Arc::clone(&records) as Arc<dyn RecordSource>,
            Arc::clone(&segments),
        );
        records.is_cold.store(true, Ordering::Relaxed);
        let (overwritten, bystander) = (shared_key(1), shared_key(2));
        let (old, other) = (
            Loc::new(SegmentId(1), 0, 200),
            Loc::new(SegmentId(2), 0, 300),
        );
        records.write(old, overwritten.as_slice(), Lsn(1));
        records.write(other, bystander.as_slice(), Lsn(2));
        segments.note_max(SegmentId(1), Lsn(5));
        segments.note_max(SegmentId(2), Lsn(5));
        column.insert(overwritten.as_slice(), old);
        column.insert(bystander.as_slice(), other);

        // with no read the overwrite cannot tell the two slots apart, so it books both
        let displaced = column.displace(&overwritten, Lsn(10)).expect("displace");
        assert_eq!(displaced.classed.len(), 2);
        assert_eq!(column.displaced(), 2);

        // the bystander's newest version sits in a displaced slot, so both paths leave it to the footers
        assert!(matches!(
            column.read(&bystander).expect("read"),
            Lookup::Unsettled
        ));
        assert_eq!(column.entry(&bystander).expect("entry"), Settled::Footers);
        // a single-candidate read and a move's shortcut leave a displaced slot to the full lookup
        assert!(column.sole(&bystander).is_none());
        assert!(!column.only_at(bystander.as_slice(), other));
    }

    fn column(records: &Arc<Records>) -> SpotColumn {
        let column = SpotColumn::new();
        column.attach(
            Arc::clone(records) as Arc<dyn RecordSource>,
            Arc::new(SegmentTable::new()),
        );
        column
    }

    fn version(column: &SpotColumn, at: u64) -> Option<u64> {
        match column.read(&key(at)).expect("read") {
            Lookup::Found(lsn, _) => Some(lsn.as_u64()),
            Lookup::Missing => None,
            Lookup::Newest(_) => {
                panic!("key {at} answered with no version from a source that reads them all")
            }
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
            let loc = Loc::new(
                SegmentId(1 + (at / 10_000) as u32),
                (at % 10_000) as u32 * 64,
                40,
            );
            records.write(loc, key(at).as_slice(), Lsn(at + 1));
            column.insert(key(at).as_slice(), loc);
        }
        assert_eq!(column.held(), keys);
        for at in 0..keys {
            assert_eq!(version(&column, at), Some(at + 1), "key {at}");
        }
        assert_eq!(version(&column, keys + 1), None);
    }

    // an overwritten version is booked from its length class with no read, and stays until its segment retires
    #[test]
    fn overwritten_versions_are_booked_from_their_class() {
        let records = Arc::new(Records::default());
        let segments = Arc::new(SegmentTable::new());
        let column = SpotColumn::new();
        column.attach(
            Arc::clone(&records) as Arc<dyn RecordSource>,
            Arc::clone(&segments),
        );
        records.is_cold.store(true, Ordering::Relaxed);

        let old = Loc::new(SegmentId(1), 0, 200);
        records.write(old, key(1).as_slice(), Lsn(1));
        segments.note_max(SegmentId(1), Lsn(5));
        column.insert(key(1).as_slice(), old);
        // 200 bytes sit in the class from 193 to 256, so segments book its middle and live bytes its least
        assert_eq!(
            column.displace(&key(1), Lsn(10)).expect("displace").classed,
            vec![(Loc::new(SegmentId(1), 0, 224), 193)]
        );
        assert_eq!(records.heads.load(Ordering::Relaxed), 0);
        assert_eq!((column.held(), column.displaced()), (1, 1));
        assert_eq!(
            column.displace(&key(1), Lsn(11)).expect("displace"),
            Displaced::default()
        );
        // a displaced version alone never answers, since it was overwritten or deleted once
        assert!(matches!(
            column.read(&key(1)).expect("read"),
            Lookup::Unsettled
        ));
        column.forget_retired(|segment| segment != SegmentId(1));
        assert_eq!((column.held(), column.displaced()), (0, 0));

        // a segment that may hold a newer version is read, and the newer version stays
        let newer = Loc::new(SegmentId(2), 0, 40);
        records.write(newer, key(3).as_slice(), Lsn(30));
        segments.note_max(SegmentId(2), Lsn(30));
        column.insert(key(3).as_slice(), newer);
        assert!(column
            .displace(&key(3), Lsn(12))
            .expect("displace")
            .booked
            .is_empty());
        assert_eq!(version(&column, 3), Some(30));

        // a key holding several versions reads them all and each goes, booked exactly
        let held: Vec<Loc> = (0..SETTLE_AT as u32)
            .map(|at| Loc::new(SegmentId(3), at * 64, 40))
            .collect();
        for (at, loc) in held.iter().enumerate() {
            records.write(*loc, key(4).as_slice(), Lsn(40 + at as u64));
            column.insert(key(4).as_slice(), *loc);
        }
        segments.note_max(SegmentId(3), Lsn(45));
        let mut booked = column.displace(&key(4), Lsn(50)).expect("displace").booked;
        booked.sort_by_key(|loc| loc.offset);
        assert_eq!(booked, held);
        assert_eq!(version(&column, 4), None);

        // a header read takes back a class booking, at the record's own length
        let first = Loc::new(SegmentId(4), 0, 200);
        records.write(first, key(5).as_slice(), Lsn(60));
        segments.note_max(SegmentId(4), Lsn(61));
        column.insert(key(5).as_slice(), first);
        assert_eq!(
            column.displace(&key(5), Lsn(70)).expect("displace").classed,
            vec![(Loc::new(SegmentId(4), 0, 224), 193)]
        );
        let slack = column.slack();
        for (at, lsn) in [(64, 71), (128, 72)] {
            let loc = Loc::new(SegmentId(5), at, 40);
            records.write(loc, key(5).as_slice(), Lsn(lsn));
            column.insert(key(5).as_slice(), loc);
        }
        segments.note_max(SegmentId(5), Lsn(72));
        let settled = column.displace(&key(5), Lsn(80)).expect("displace");
        assert_eq!(settled.rebooked, vec![(first, 193)]);
        assert_eq!(settled.booked.len(), 2);
        assert!(
            column.slack() < slack,
            "the corrected booking still counted in the slack"
        );
        assert_eq!(column.displaced(), 0);
    }

    // an overwrite reading every version of a key books the one whose segment went from its class
    #[test]
    fn a_gone_version_is_booked_from_its_class() {
        let records = Arc::new(Records::default());
        let segments = Arc::new(SegmentTable::new());
        let column = SpotColumn::new();
        column.attach(
            Arc::clone(&records) as Arc<dyn RecordSource>,
            Arc::clone(&segments),
        );
        let gone = Loc::new(SegmentId(1), 0, 200);
        column.insert(key(6).as_slice(), gone);
        for at in 0..2u32 {
            let loc = Loc::new(SegmentId(2), at * 64, 40);
            records.write(loc, key(6).as_slice(), Lsn(10 + u64::from(at)));
            column.insert(key(6).as_slice(), loc);
        }
        segments.note_max(SegmentId(2), Lsn(11));

        let settled = column.displace(&key(6), Lsn(20)).expect("displace");

        assert_eq!(settled.booked.len(), 2);
        assert_eq!(
            settled.classed,
            vec![(Loc::new(SegmentId(1), 0, 224), 193)],
            "the version whose segment went was dropped unbooked"
        );
    }

    // the newest version answers, only the older one goes to the cleaner, and a copy answers once its source goes
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
        assert!(column.take_live(key(7).as_slice(), new));
        assert_eq!(version(&column, 7), Some(9), "the copy still answers");
    }

    // a load keeps each key's newest row, gives a tie to the newer segment, and drops deleted keys
    #[test]
    fn a_load_keeps_the_newest_row_of_each_key() {
        let records = Arc::new(Records::default());
        let column = column(&records);
        let rows = [
            (1u64, Loc::new(SegmentId(2), 0, 40), 5u64, false),
            (1, Loc::new(SegmentId(1), 0, 30), 3, false),
            (2, Loc::new(SegmentId(1), 64, 20), 4, false),
            (2, Loc::new(SegmentId(3), 64, 25), 4, false),
            (3, Loc::new(SegmentId(1), 128, 10), 2, false),
            (3, Loc::new(SegmentId(2), 128, 0), 6, true),
            (4, Loc::new(SegmentId(2), 192, 0), 1, true),
            (4, Loc::new(SegmentId(3), 192, 50), 7, false),
        ];
        let (mut keys, mut bytes) = (0i64, 0i64);
        for (at, loc, lsn, is_tombstone) in rows {
            records.put(loc, key(at).as_slice(), Lsn(lsn), is_tombstone);
            let booked = column
                .load(&key(at), loc, Lsn(lsn), is_tombstone)
                .expect("load");
            for (len, sign) in [(booked.came, 1), (booked.gone, -1)] {
                if let Some(len) = len {
                    keys += sign;
                    bytes += sign * i64::from(len);
                }
            }
        }
        column.finish_load();
        assert_eq!(
            (keys, bytes),
            (3, 115),
            "the loads booked the newest live rows"
        );
        assert_eq!(version(&column, 1), Some(5), "the newer row stands");
        assert_eq!(version(&column, 3), None, "a newer tombstone drops the key");
        assert_eq!(
            version(&column, 4),
            Some(7),
            "a newer row stands over an older tombstone"
        );
        assert_eq!(column.held(), 3);
        assert!(
            column.take_live(key(2).as_slice(), Loc::new(SegmentId(3), 64, 40)),
            "a tie went to the copy"
        );
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
        assert!(
            matches!(column.entry(&key(9)).expect("entry"), Settled::Entry(Some(entry)) if entry.lsn == Lsn(1))
        );
        assert_eq!(column.held(), 1);
    }

    // a write reads only the slots whose segment may hold anything newer, and refuses on a newer version there
    #[test]
    fn holds_newer_reads_only_the_slots_a_ceiling_admits() {
        let records = Arc::new(Records::default());
        let segments = Arc::new(SegmentTable::new());
        let column = SpotColumn::new();
        column.attach(
            Arc::clone(&records) as Arc<dyn RecordSource>,
            Arc::clone(&segments),
        );
        let below = Loc::new(SegmentId(3), 0, 40);
        let older = Loc::new(SegmentId(1), 0, 40);
        let newer = Loc::new(SegmentId(2), 0, 40);
        records.write(below, key(1).as_slice(), Lsn(2));
        records.write(older, key(1).as_slice(), Lsn(3));
        records.write(newer, key(1).as_slice(), Lsn(15));
        segments.note_max(SegmentId(3), Lsn(4));
        segments.note_max(SegmentId(1), Lsn(10));
        segments.note_max(SegmentId(2), Lsn(20));

        // A ceiling at or below the write rules its slot out with no read
        column.insert(key(1).as_slice(), below);
        assert!(!column.holds_newer(key(1).as_ref(), Lsn(5)).expect("below"));
        assert_eq!(records.heads.load(Ordering::Relaxed), 0);

        // A ceiling above the write is read, and an older version there refuses nothing
        column.insert(key(1).as_slice(), older);
        assert!(!column.holds_newer(key(1).as_ref(), Lsn(5)).expect("older"));
        assert_eq!(records.heads.load(Ordering::Relaxed), 1);

        // A newer version behind any slot refuses the write
        column.insert(key(1).as_slice(), newer);
        assert!(column.holds_newer(key(1).as_ref(), Lsn(5)).expect("newer"));
        assert!(!column
            .holds_newer(key(1).as_ref(), Lsn(15))
            .expect("the same version"));
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
        for len in [
            0u32,
            1,
            63,
            64,
            65,
            200,
            1024,
            1025,
            4096,
            65_536,
            1 << 20,
            10_192_000,
            u32::MAX,
        ] {
            let bound = bound_of(class_of(len));
            assert!(bound >= len, "length {len}");
            assert!(
                u64::from(bound) <= u64::from(len) * 110 / 100 + SMALL_STEP,
                "length {len} over-reads to {bound}"
            );
        }
    }

    // a class ends at the keyless ceiling, so a lone record that long still reads off its own check
    #[test]
    fn the_keyless_ceiling_ends_a_class() {
        let ceiling = crate::format::record::KEYLESS_MAX;
        assert_eq!(bound_of(class_of(ceiling)), ceiling);
    }
}
