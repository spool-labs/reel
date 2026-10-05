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
use crate::index::playback::Way;
use crate::sync::{lock, read, write};

/// Slots in one bucket, which fills one cache line
const WAYS: usize = 4;

/// Share of a shard's slots it fills before it grows
const LOAD: f64 = 0.85;

/// How much a shard grows by when it fills
const GROWTH: f64 = 1.5;

/// Shards, picked by the top byte of a key's hash
const SHARDS: usize = 256;

/// Buckets the lowest rung of a shard's ladder holds
const FIRST_BUCKETS: usize = 8;

/// Least a growth multiplies a shard by, so a table sized between rungs never grows by a sliver
const LEAST_GROWTH: f64 = 1.2;

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

/// Versions one key may hold before an overwrite reads them all, so its two buckets never fill
const SETTLE_AT: usize = 3;

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
pub enum FastRead {
    Found(Head, Value),
    Tombstone(Head),
    Other,

    /// The segment is gone, so whatever the entry pointed at has moved on or died
    Gone,

    /// A record the checked read has to settle, such as one failing its checksum
    Unsure,
}

/// What an overwrite settled: records it booked dead, and class bookings it corrected
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Displaced {
    /// Records taken out, each at its true length
    pub booked: Vec<Loc>,

    /// Records marked displaced, each at the middle of its class beside the least length its class covers
    pub classed: Vec<(Loc, u32)>,

    /// Records an earlier overwrite booked from their class, each at its true length beside the least length booked
    pub rebooked: Vec<(Loc, u32)>,
}

/// A class booking a retiring segment's footer corrects
#[derive(Debug, PartialEq, Eq)]
pub struct Rebooked {
    pub key: RecordKey,

    /// The least length the class booked
    pub booked: u32,

    /// The length the record held
    pub actual: u32,
}

/// One record a lookup reads: where it sits, and how much of its payload to ask for
#[derive(Clone, Copy, Debug)]
pub struct Candidate {
    pub segment: SegmentId,
    pub offset: u32,
    pub bound: u32,
    slot: Slot,
}

/// One sealed row of an ordered walk, in the order its table holds it
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalkRow {
    /// The key's leading seven bytes as placed, which order rows up to a tie
    pub lead: u64,
    pub segment: SegmentId,
    pub offset: u32,
    pub bound: u32,
    pub is_grave: bool,

    /// Whether an overwrite or a delete booked this version as older, so it never answers alone
    pub is_displaced: bool,
}

impl WalkRow {
    fn of(shard: usize, slot: Slot) -> WalkRow {
        WalkRow {
            lead: ((shard as u64) << 56) | (u64::from(slot.mid) << 24) | u64::from(slot.tag()),
            segment: slot.segment(),
            offset: slot.offset,
            bound: slot.bound(),
            is_grave: slot.is_grave(),
            is_displaced: slot.is_displaced(),
        }
    }
}

/// A lookup in progress: the candidates in order, and the newest version read so far
pub struct Pick {
    hash: u64,
    ordered: Vec<(Option<Lsn>, Slot)>,
    next: usize,
    best: Option<(Head, Option<Value>)>,

    /// Whether the best version so far came from a displaced slot
    best_displaced: bool,
    stale: Vec<(Slot, u32)>,
}

/// What the header path makes of a key's slots
#[derive(Debug, PartialEq, Eq)]
pub enum Settled {
    /// The newest version a live slot holds, a grave for a tombstone, or nothing
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
    Found(Lsn, Value),
    Missing,

    /// Something only the checked read can settle
    Unsettled,
}

/// One walked row to read, whatever key it holds
#[derive(Clone, Copy, Debug)]
pub struct RowAsk {
    pub segment: SegmentId,
    pub offset: u32,

    /// Payload bytes to read beside the header, zero for the key alone
    pub bound: u32,
}

/// What reading one walked row settled
pub enum RowRead {
    /// A record of the column: its header, and its payload when the read was for it
    ///
    /// The key goes in the caller's packed buffer. A read for the key alone stops short
    /// of the payload, so nothing checks those bytes against the record's checksum.
    Found { head: Head, value: Option<Value> },

    /// The segment is gone, so the row moved or died under the walk
    Gone,

    /// No record of the column starts there, or one did and failed its check
    Other,

    /// A coded record, or one a read came up short on, which the checked path reads
    Unsure,
}

/// Where the index reads the records it points at
pub trait RecordSource: Send + Sync {
    /// The records at several places, whatever keys they hold, in one submission
    ///
    /// Each found record's key goes into `keys` at its ask's place, packed at the
    /// column's width.
    fn rows(&self, column: ColumnId, width: usize, asks: &[RowAsk], keys: &mut Vec<u8>) -> Result<Vec<RowRead>>;

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

/// How a column places its sealed keys, which decides whether its table can be walked
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Layout {
    /// Spread by a hash, so keys of any shape share the load
    #[default]
    Hashed,

    /// Kept in key order by the key's own leading bytes, so a walk reads the table
    ///
    /// For keys that are already uniform, such as content hashes and public keys. A
    /// column whose keys share a prefix piles them into one cluster.
    Ordered,
}

/// The bits a key is placed by under a layout
fn bits_of(layout: Layout, key: &[u8]) -> u64 {
    match layout {
        Layout::Hashed => hash_of(key),
        Layout::Ordered => lead_of(key),
    }
}

/// A key's first seven bytes, laid out so the shard, mid and tag read them in order
///
/// The shard takes byte 0, the mid bytes 1 to 4 and the tag bytes 5 and 6, so two
/// keys compare by these bits as they compare by those bytes. A short key reads as
/// zeros past its end.
fn lead_of(key: &[u8]) -> u64 {
    let mut lead = [0u8; 7];
    let len = key.len().min(lead.len());
    lead[..len].copy_from_slice(&key[..len]);
    let mid = u32::from_be_bytes([lead[1], lead[2], lead[3], lead[4]]);
    let tag = u16::from_be_bytes([lead[5], lead[6]]);
    (u64::from(lead[0]) << 56) | (u64::from(mid) << 24) | u64::from(tag)
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

    fn is_displaced(&self) -> bool {
        self.meta & DISPLACED != 0
    }

    /// The least length the slot's class covers, which live bytes book with no read so they never go below the truth
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

    /// Where the slot sorts in an ordered table, its key's leading bits below the shard
    fn order(&self) -> (u32, u32) {
        (self.mid, self.tag())
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

/// The slots holding one key's bits
///
/// A hashed key has at most both of its buckets full. An ordered table can hold any
/// number of keys sharing seven leading bytes, so past that count the places spill to
/// the heap.
#[derive(Clone, Debug)]
struct Places {
    held: [Place; MAX_CANDIDATES],
    count: usize,
    spilled: Vec<Place>,
}

impl Places {
    fn new() -> Places {
        Places {
            held: [Place::default(); MAX_CANDIDATES],
            count: 0,
            spilled: Vec::new(),
        }
    }

    fn push(&mut self, place: Place) {
        if self.count < MAX_CANDIDATES && self.spilled.is_empty() {
            self.held[self.count] = place;
            self.count += 1;
            return;
        }
        if self.spilled.is_empty() {
            self.spilled.extend_from_slice(&self.held[..self.count]);
        }
        self.spilled.push(place);
    }
}

impl std::ops::Deref for Places {
    type Target = [Place];

    fn deref(&self) -> &[Place] {
        match self.spilled.is_empty() {
            true => &self.held[..self.count],
            false => &self.spilled,
        }
    }
}

impl PartialEq for Places {
    fn eq(&self, other: &Places) -> bool {
        **self == **other
    }
}

impl Eq for Places {}

/// Buckets past the last home an ordered table keeps, for a cluster that runs off the end
const TAIL_BUCKETS: usize = 64;

struct Table {
    buckets: Vec<Bucket>,

    /// How many buckets a key's mid can land on as its home, every bucket for a hashed table
    homes: usize,
    held: usize,
    seed: u64,
    layout: Layout,

    /// Share of a growth step this shard's ladder is offset by
    phase: f64,
}

/// Where shard `at`'s ladder sits within one growth step
fn phase_of(at: usize) -> f64 {
    at as f64 / SHARDS as f64
}

/// The rung a table of `len` homes grows to, on the ladder offset by `phase`
///
/// Shards fill at one rate, so shards on one ladder cross their limit together and each
/// holds its old table beside its new one: at 100M keys that was 1.45 GiB beside 2.16 GiB.
/// Offset ladders put each shard's growth at its own key count.
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
    fn with_buckets(count: usize, layout: Layout, phase: f64) -> Table {
        let homes = count.max(2);
        let total = match layout {
            Layout::Hashed => homes,
            Layout::Ordered => homes + TAIL_BUCKETS,
        };
        Table {
            buckets: vec![Bucket::default(); total],
            homes,
            held: 0,
            seed: count as u64,
            layout,
            phase,
        }
    }

    fn home(&self, mid: u32) -> usize {
        ((u64::from(mid) * self.homes as u64) >> 32) as usize
    }

    /// Slots the table has room for, past the last home included
    fn width(&self) -> usize {
        self.buckets.len() * WAYS
    }

    fn slot_at(&self, at: usize) -> Slot {
        self.buckets[at / WAYS].slots[at % WAYS]
    }

    fn set_at(&mut self, at: usize, slot: Slot) {
        self.buckets[at / WAYS].slots[at % WAYS] = slot;
    }

    fn place_at(&self, at: usize) -> Place {
        Place {
            bucket: at / WAYS,
            way: at % WAYS,
            slot: self.slot_at(at),
        }
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
        if self.layout == Layout::Ordered {
            return self.matches_in_order(hash);
        }
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
        self.held as f64 >= (self.homes * WAYS) as f64 * LOAD
    }

    /// A table this one's slots fit into at the next size
    fn grown(&self) -> Table {
        let mut count = next_rung(self.homes, self.phase);
        loop {
            let mut table = Table::with_buckets(count, self.layout, self.phase);
            let mut slots = self.buckets.iter().flat_map(|bucket| bucket.slots.iter()).copied();
            let fits = match self.layout {
                Layout::Hashed => slots.filter(|slot| !slot.is_empty()).all(|slot| table.place(slot).is_ok()),
                Layout::Ordered => table.lay(&mut slots),
            };
            if fits {
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
        if self.layout == Layout::Ordered {
            while !self.insert_in_order(slot) {
                *self = self.grown();
            }
            return;
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
        let at = place.bucket * WAYS + place.way;
        match self.layout {
            Layout::Hashed => self.set_at(at, Slot::default()),
            Layout::Ordered => self.close_gap(at),
        }
        self.held -= 1;
        Some(place.slot)
    }

    /// Mark the slot pointing at one record displaced, unless it already is
    ///
    /// The mark sits outside a slot's order, so an ordered table stays sorted.
    fn mark_displaced(&mut self, hash: u64, slot: &Slot) -> bool {
        let found = self.matches(hash);
        match found.iter().find(|place| place.slot.same_place(slot) && !place.slot.is_displaced()) {
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
        let displaced = self
            .buckets
            .iter()
            .flat_map(|bucket| bucket.slots.iter())
            .filter(|slot| !slot.is_empty() && !keep(slot) && slot.is_displaced())
            .count() as u64;
        match self.layout {
            Layout::Hashed => {
                for bucket in self.buckets.iter_mut() {
                    for slot in bucket.slots.iter_mut() {
                        if !slot.is_empty() && !keep(slot) {
                            *slot = Slot::default();
                            self.held -= 1;
                        }
                    }
                }
            }
            // Taking slots out only frees room, so the kept ones lay back down in the
            // same table, each at or before where it stood.
            Layout::Ordered => {
                let kept: Vec<Slot> = self
                    .buckets
                    .iter()
                    .flat_map(|bucket| bucket.slots.iter())
                    .copied()
                    .filter(|slot| !slot.is_empty() && keep(slot))
                    .collect();
                self.buckets.iter_mut().for_each(|bucket| *bucket = Bucket::default());
                self.held = 0;
                let laid = self.lay(&mut kept.into_iter());
                debug_assert!(laid, "kept slots always fit where they stood");
            }
        }
        (before - self.held, displaced)
    }

    /// The slots holding a key's bits in an ordered table, from its home to past its order
    ///
    /// Every slot sits at or after its home and nothing is empty between them, so the
    /// key's slots are the run with its order before the first empty or later one.
    fn matches_in_order(&self, hash: u64) -> Places {
        let order = (mid_of(hash), tag_of(hash));
        let mut found = Places::new();
        for at in self.home(order.0) * WAYS..self.width() {
            let slot = self.slot_at(at);
            if slot.is_empty() {
                break;
            }
            match slot.order().cmp(&order) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => found.push(self.place_at(at)),
                std::cmp::Ordering::Greater => break,
            }
        }
        found
    }

    /// Put a slot in its order, moving the rest of its cluster up one, or false at the end
    ///
    /// A slot whose order equals one held goes after it, so slots of one bit pattern keep
    /// the order they arrived in.
    fn insert_in_order(&mut self, slot: Slot) -> bool {
        let width = self.width();
        let mut at = self.home(slot.mid) * WAYS;
        while at < width {
            let held = self.slot_at(at);
            if held.is_empty() || held.order() > slot.order() {
                break;
            }
            at += 1;
        }
        let mut end = at;
        while end < width && !self.slot_at(end).is_empty() {
            end += 1;
        }
        if end == width {
            return false;
        }
        for from in (at..end).rev() {
            let moving = self.slot_at(from);
            self.set_at(from + 1, moving);
        }
        self.set_at(at, slot);
        self.held += 1;
        true
    }

    /// Empty one slot of an ordered table and pull its cluster's tail down over the gap
    ///
    /// A slot moves down only while that keeps it at or after its home, so the table
    /// stays sorted with nothing empty between a slot and its home.
    fn close_gap(&mut self, at: usize) {
        let width = self.width();
        let mut gap = at;
        loop {
            let next = gap + 1;
            if next >= width {
                break;
            }
            let moving = self.slot_at(next);
            if moving.is_empty() || self.home(moving.mid) * WAYS > gap {
                break;
            }
            self.set_at(gap, moving);
            gap = next;
        }
        self.set_at(gap, Slot::default());
    }

    /// Where an ordered walk from a key's bits starts in this table, in its direction
    ///
    /// Up starts at the first slot ordered at or past the bits, down at the last slot
    /// ordered at or before them. Nothing ordered past the bits can sit before their
    /// home, so both scans start there.
    fn walk_start(&self, hash: u64, way: Way) -> Option<usize> {
        let order = (mid_of(hash), tag_of(hash));
        let mut at = self.home(order.0) * WAYS;
        while at < self.width() {
            let slot = self.slot_at(at);
            if slot.is_empty() || slot.order() > order || (way == Way::Up && slot.order() == order) {
                break;
            }
            at += 1;
        }
        match way {
            Way::Up => Some(at),
            Way::Down => at.checked_sub(1),
        }
    }

    /// Slots in order from a position, up or down, until `limit` or the table's end
    ///
    /// A run of slots sharing their order is never split, so the last row handed back
    /// is never a tie with the next one left behind.
    fn walk_from(&self, mut at: usize, way: Way, limit: usize, shard: usize, out: &mut Vec<WalkRow>) {
        loop {
            if at >= self.width() {
                return;
            }
            let slot = self.slot_at(at);
            if !slot.is_empty() {
                let row = WalkRow::of(shard, slot);
                let is_tied = out.last().is_some_and(|last| last.lead == row.lead);
                if out.len() >= limit && !is_tied {
                    return;
                }
                out.push(row);
            }
            match way {
                Way::Up => at += 1,
                Way::Down => match at.checked_sub(1) {
                    Some(below) => at = below,
                    None => return,
                },
            }
        }
    }

    /// Lay sorted slots into an empty ordered table, or false when they run off its end
    fn lay(&mut self, slots: &mut dyn Iterator<Item = Slot>) -> bool {
        let width = self.width();
        let mut next = 0;
        for slot in slots.filter(|slot| !slot.is_empty()) {
            let at = next.max(self.home(slot.mid) * WAYS);
            if at >= width {
                return false;
            }
            self.set_at(at, slot);
            self.held += 1;
            next = at + 1;
        }
        true
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

    /// Slots marked displaced and still held
    displaced: AtomicU64,
}

impl Shard {
    fn new(at: usize, layout: Layout) -> Shard {
        Shard {
            table: RwLock::new(Table::with_buckets(first_rung(phase_of(at)), layout, phase_of(at))),
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
        let is_booked = found.iter().any(|place| place.slot.same_place(slot) && place.slot.is_displaced());
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
pub struct FastColumn {
    shards: Vec<Shard>,
    records: OnceLock<Arc<dyn RecordSource>>,
    segments: OnceLock<Arc<SegmentTable>>,
    stale: Mutex<VecDeque<Stale>>,
    beside: AtomicU64,

    /// Bytes the class bookings may sit below the truth, a class width each at most
    slack: AtomicU64,

    /// Rows an open took before it could read a header, settled once it can
    set_aside: Mutex<Vec<SetAside>>,

    /// Whether keys are placed by a hash or held in key order
    layout: Layout,
}

impl Default for FastColumn {
    fn default() -> FastColumn {
        FastColumn::new()
    }
}

impl FastColumn {
    pub fn new() -> FastColumn {
        FastColumn::with_layout(Layout::Hashed)
    }

    /// An empty column placing its keys by a layout
    pub fn with_layout(layout: Layout) -> FastColumn {
        FastColumn {
            shards: (0..SHARDS).map(|at| Shard::new(at, layout)).collect(),
            records: OnceLock::new(),
            segments: OnceLock::new(),
            stale: Mutex::new(VecDeque::new()),
            beside: AtomicU64::new(0),
            slack: AtomicU64::new(0),
            set_aside: Mutex::new(Vec::new()),
            layout,
        }
    }

    /// How this column places its keys
    pub fn layout(&self) -> Layout {
        self.layout
    }

    /// Read the records walked rows point at, keys packed into `keys`, or nothing before records can be read
    pub fn read_rows(&self, column: ColumnId, width: usize, asks: &[RowAsk], keys: &mut Vec<u8>) -> Result<Option<Vec<RowRead>>> {
        match self.records.get() {
            Some(records) => records.rows(column, width, asks, keys).map(Some),
            None => Ok(None),
        }
    }

    /// Take out the slot a walk found pointing into a gone segment, so the next walk skips it
    ///
    /// An ordered column places a key by its lead, so the row's lead finds its slot
    /// with no key to read.
    pub fn forget_row(&self, row: &WalkRow) {
        let hash = row.lead;
        let slot = self.shards[shard_of(hash)]
            .read()
            .matches(hash)
            .iter()
            .find(|place| place.slot.segment() == row.segment && place.slot.offset == row.offset)
            .map(|place| place.slot);
        if let Some(slot) = slot {
            self.shards[shard_of(hash)].write().take(hash, &slot);
        }
    }

    /// Up to `limit` sealed rows in key order from a key, on an ordered column
    ///
    /// Rows come in the order of their keys' leading seven bytes: going up, from those
    /// at or past the key's, going down, from those at or before. A caller reads each
    /// row's record for its key, which settles a tie on those bytes and a row of the
    /// bound's own lead on the wrong side of it. A hashed column has no order and hands
    /// back nothing.
    pub fn walk(&self, from: Option<&[u8]>, way: Way, limit: usize, out: &mut Vec<WalkRow>) {
        self.walk_lead(from.map(lead_of), way, limit, out);
    }

    /// The leading bits a key is placed by in an ordered column, which a walk resumes from
    pub fn lead(key: &[u8]) -> u64 {
        lead_of(key)
    }

    /// The same walk from a lead, inclusive, for a page resuming past the rows it read
    pub fn walk_lead(&self, bits: Option<u64>, way: Way, limit: usize, out: &mut Vec<WalkRow>) {
        out.clear();
        if self.layout != Layout::Ordered || limit == 0 {
            return;
        }
        let first = match (bits, way) {
            (Some(bits), _) => shard_of(bits),
            (None, Way::Up) => 0,
            (None, Way::Down) => SHARDS - 1,
        };
        let shards: Box<dyn Iterator<Item = usize>> = match way {
            Way::Up => Box::new(first..SHARDS),
            Way::Down => Box::new((0..=first).rev()),
        };
        // A lead's top byte is its shard, so no tie spans two shards.
        for shard in shards {
            let table = self.shards[shard].read();
            let start = match bits.filter(|_| shard == first) {
                Some(bits) => table.walk_start(bits, way),
                None => match way {
                    Way::Up => Some(0),
                    Way::Down => table.width().checked_sub(1),
                },
            };
            if let Some(at) = start {
                table.walk_from(at, way, limit, shard, out);
            }
            if out.len() >= limit {
                return;
            }
        }
    }

    /// The bits a key is placed by in this column
    fn bits(&self, key: &[u8]) -> u64 {
        bits_of(self.layout, key)
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

    /// Bytes the byte counters may sit from the truth, from every class booking so far
    pub fn slack(&self) -> u64 {
        self.slack.load(Ordering::Relaxed)
    }

    /// Overwritten versions booked from their length class and held until compaction
    pub fn displaced(&self) -> u64 {
        self.shards.iter().map(|shard| shard.displaced.load(Ordering::Relaxed)).sum()
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

    /// Which of `lanes` runs of shards holds a key, so threads split a hand-over without sharing one
    pub fn lane_of(key: &[u8], lanes: usize) -> usize {
        shard_of(hash_of(key)) * lanes / SHARDS
    }

    /// Hold a sealed record's location
    ///
    /// The caller hands over the newest version, so an older entry of the same key is
    /// one the map displaced and the cleaner will settle.
    pub fn insert(&self, key: &[u8], loc: Loc) {
        let hash = self.bits(key);
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
        for (at, shard) in self.shards.iter().enumerate() {
            let mut table = shard.write();
            if table.held == 0 && table.homes < buckets {
                *table = Table::with_buckets(buckets, self.layout, phase_of(at));
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
            let hash = self.bits(entry.key.as_slice());
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
            let hash = self.bits(row.key.as_slice());
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
        // Keys sharing their bits, as keys sharing a lead do in an ordered table, meet each
        // other's slots, so a slot counts as this key's only when its footer row says so.
        let mut keys_at: Option<HashMap<u32, usize>> = None;
        let mut key_at = |offset: u32| -> Result<Option<&[u8]>> {
            if keys_at.is_none() {
                let mut at_offset = HashMap::with_capacity(partition.len());
                for at in 0..partition.len() {
                    at_offset.insert(partition.row_at(at)?.offset, at);
                }
                keys_at = Some(at_offset);
            }
            Ok(keys_at.as_ref().and_then(|keys| keys.get(&offset)).and_then(|at| partition.key_at(*at)))
        };
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
            let hash = self.bits(row.key.as_slice());
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
                Some(place)
                    if segment == row.loc.segment
                        && standing.offset == row.loc.offset
                        && key_at(place.slot.offset)? == Some(row.key.as_slice()) =>
                {
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
        let hash = self.bits(key.as_slice());
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
            let (dropped, displaced) = table.retain(|slot| !slot.is_grave());
            table.count_taken(dropped as u64, displaced);
        }
    }

    /// Whether a key's one live slot points at this record, which a caller that read it can trust with no read
    pub fn only_at(&self, key: &[u8], loc: Loc) -> bool {
        let hash = self.bits(key);
        let seen = self.shards[shard_of(hash)].read().matches(hash);
        let mut live = seen.iter().filter(|place| !place.slot.is_displaced());
        match (live.next(), live.next()) {
            (Some(place), None) => place.slot.at(loc),
            (Some(_), Some(_)) | (None, _) => false,
        }
    }

    /// Take out the entry pointing at one record, for a compaction move, an eviction or a release
    pub fn remove_at(&self, key: &[u8], loc: Loc) -> bool {
        let hash = self.bits(key);
        let mut table = self.shards[shard_of(hash)].write();
        let held = table.matches(hash).iter().find(|place| place.slot.at(loc)).copied();
        match held {
            Some(place) => table.take(hash, &place.slot),
            None => false,
        }
    }

    /// Book a key's entries older than `newer` dead, now that the map holds that version
    ///
    /// An entry in a segment holding nothing at or past `newer` is older with no read. It
    /// is marked displaced in place and booked at the middle of its length class, then
    /// stays for lookups in flight until compaction retires its segment. An entry whose
    /// segment may hold a newer version reads its header and goes, booked exactly, and so
    /// does every entry of a key holding `SETTLE_AT` versions or more. What comes back is
    /// every record booked, for the caller to settle.
    pub fn displace(&self, key: &RecordKey, newer: Lsn) -> Result<Displaced> {
        let (Some(records), Some(segments)) = (self.records.get(), self.segments.get()) else {
            return Ok(Displaced::default());
        };
        let hash = self.bits(key.as_slice());
        let shard = shard_of(hash);
        let seen = self.shards[shard].read().matches(hash);
        if seen.is_empty() {
            return Ok(Displaced::default());
        }
        let must_read = seen.len() >= SETTLE_AT;
        let (mut unread, mut older, mut gone) = (Vec::new(), Vec::new(), Vec::new());
        for place in seen.iter() {
            let slot = place.slot;
            let is_older = segments.max_lsn_of(slot.segment()).is_some_and(|ceiling| ceiling < newer);
            match (must_read, slot.is_displaced(), is_older) {
                (false, true, _) => continue,
                (false, false, true) => {
                    unread.push(slot);
                    continue;
                }
                (false, false, false) | (true, _, _) => {}
            }
            let answer = match records.cached_head(key.as_ref(), slot.segment(), slot.offset)? {
                HeadRead::Cold => records.head(key.as_ref(), slot.segment(), slot.offset)?,
                answer => answer,
            };
            match answer {
                HeadRead::Same(head) if head.lsn < newer => older.push((slot, head.len)),
                HeadRead::Same(_) | HeadRead::Other | HeadRead::Cold => {}
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
                // Marked earlier and booked from its class, so the header just read corrects that booking.
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
                settled.classed.push((Loc::new(slot.segment(), slot.offset, slot.middle()), slot.least()));
                self.slack.fetch_add(u64::from(slot.width()), Ordering::Relaxed);
            }
        }
        Ok(settled)
    }

    /// Drop the slack one class booking stood for, once its true length is known
    fn unslack(&self, slot: &Slot) {
        let width = u64::from(slot.width());
        let _ = self.slack.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |slack| Some(slack.saturating_sub(width)));
    }

    /// Take out every displaced entry pointing into a retiring segment, with the true length its footer row gives
    ///
    /// A row reads its shard under the read lock first, so a segment with nothing displaced in it costs no write lock.
    pub fn settle_displaced_in(&self, segment: SegmentId, partition: &FooterPartition) -> Result<Vec<Rebooked>> {
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
            let held = shard.read().matches(hash).iter().find(|place| place.slot.at(loc) && place.slot.is_displaced()).map(|place| place.slot);
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
            let took = self.shards[shard_of(stale.hash)].write().take_unbooked(stale.hash, &stale.slot);
            if took {
                let loc = Loc::new(stale.slot.segment(), stale.slot.offset, stale.len);
                taken.push((stale.key, loc));
            }
        }
        taken
    }

    /// The candidates for a key, newest segment ceiling first, ties to the newer segment
    fn ordered(&self, key: &RecordKey, segments: &SegmentTable) -> (u64, Vec<(Option<Lsn>, Slot)>) {
        let hash = self.bits(key.as_slice());
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
    ///
    /// A displaced entry was booked as an overwritten version, so it is left to the full lookup.
    pub fn sole(&self, key: &RecordKey) -> Option<Candidate> {
        let pick = self.pick(key)?;
        let mut live = pick.ordered.iter().filter(|(_, slot)| !slot.is_displaced());
        match (live.next(), live.next()) {
            (Some((_, slot)), None) => Some(Candidate {
                segment: slot.segment(),
                offset: slot.offset,
                bound: slot.bound(),
                slot: *slot,
            }),
            (Some(_), Some(_)) | (None, _) => None,
        }
    }

    /// Whether a slot for the key's bits sits in a segment holding anything newer than `lsn`
    ///
    /// No header is read, so a caller holding the map's lock can ask. A slot whose
    /// segment's ceiling is unknown counts as newer.
    pub fn may_hold_newer(&self, key: &[u8], lsn: Lsn) -> bool {
        let Some(segments) = self.segments.get() else {
            return true;
        };
        let hash = self.bits(key);
        self.shards[shard_of(hash)]
            .read()
            .matches(hash)
            .iter()
            // A displaced slot was booked as older than a version written since, so it is never the newer one.
            .any(|place| !place.slot.is_displaced() && segments.max_lsn_of(place.slot.segment()).is_none_or(|max| max > lsn))
    }

    /// How many slots have left each shard so far, read before a walk asks the map
    pub fn taken_all(&self) -> [u64; SHARDS] {
        std::array::from_fn(|at| self.shards[at].taken.load(Ordering::Acquire))
    }

    /// Whether a slot left any shard between two leads since a walk took its counts
    ///
    /// A key that moved from this index to the map between the walk's two looks is in
    /// neither, so a walk that saw nothing leave the shards it crossed stands.
    pub fn moved_between(&self, before: &[u64], from: u64, to: u64) -> bool {
        let (low, high) = (shard_of(from.min(to)), shard_of(from.max(to)));
        (low..=high).any(|shard| self.shards[shard].taken.load(Ordering::Acquire) != before[shard])
    }

    /// Where the key's shard stands, read before the map is asked
    pub fn since(&self, key: &RecordKey) -> Since {
        let shard = shard_of(self.bits(key.as_slice()));
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
            best_displaced: false,
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
            Some(_) | None => {
                pick.best = Some((head, value));
                pick.best_displaced = candidate.slot.is_displaced();
            }
        }
        Offered::Next
    }

    /// Close a lookup with its newest version, handing the older ones it read to the cleaner
    pub fn settle(&self, key: &RecordKey, pick: Pick) -> Lookup {
        for (slot, len) in pick.stale.into_iter().filter(|(slot, _)| !slot.is_displaced()) {
            self.queue(Stale {
                key: key.clone(),
                hash: pick.hash,
                slot,
                len,
            });
        }
        match pick.best {
            // A displaced version was overwritten or deleted once, so what came after it is the footers' to say.
            Some(_) if pick.best_displaced => Lookup::Unsettled,
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
        // Segments kept going under the look, and the footers hold still.
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
                // The segment is gone, so the slot points at nothing and goes before the next look.
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
        Ok(Some(Settled::Entry(best.map(|(head, slot)| match head.is_tombstone {
            true => Entry::grave(head.lsn),
            false => {
                let stamp = segments.incarnation_of(slot.segment());
                Entry::new(Loc::new(slot.segment(), slot.offset, head.len), head.lsn).stamped(stamp)
            }
        }))))
    }

    /// Drop every entry pointing into a segment no longer standing, with no reads
    pub fn forget_retired(&self, is_standing: impl Fn(SegmentId) -> bool) -> u64 {
        let mut forgotten = 0;
        for shard in &self.shards {
            let mut table = shard.write();
            let (dropped, displaced) = table.retain(|slot| is_standing(slot.segment()));
            table.count_taken(dropped as u64, displaced);
            forgotten += dropped as u64;
        }
        forgotten
    }

    /// Drop every entry, for a rebuild starting over
    pub fn clear(&self) {
        for (at, shard) in self.shards.iter().enumerate() {
            let mut table = shard.write();
            let held = table.held as u64;
            let displaced = table.displaced.load(Ordering::Relaxed);
            *table = Table::with_buckets(first_rung(phase_of(at)), self.layout, phase_of(at));
            table.count_taken(held, displaced);
        }
        lock(&self.stale).clear();
        lock(&self.set_aside).clear();
        self.beside.store(0, Ordering::Relaxed);
        self.slack.store(0, Ordering::Relaxed);
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
        fn rows(&self, _column: ColumnId, width: usize, asks: &[RowAsk], keys: &mut Vec<u8>) -> Result<Vec<RowRead>> {
            let written = lock(&self.written);
            keys.clear();
            keys.resize(asks.len() * width, 0);
            let mut rows = Vec::with_capacity(asks.len());
            for (at, ask) in asks.iter().enumerate() {
                rows.push(match written.get(&(ask.segment.as_u32(), ask.offset)) {
                    Some(row) if row.key.len() == width => {
                        keys[at * width..(at + 1) * width].copy_from_slice(&row.key);
                        RowRead::Found {
                            head: Head {
                                lsn: row.lsn,
                                len: row.len,
                                is_tombstone: row.is_tombstone,
                            },
                            value: (ask.bound > 0 && !row.is_tombstone)
                                .then(|| Value::from(row.lsn.as_u64().to_le_bytes().to_vec())),
                        }
                    }
                    Some(_) => RowRead::Other,
                    None => RowRead::Gone,
                });
            }
            Ok(rows)
        }

        fn head(&self, key: KeyRef<'_>, segment: SegmentId, offset: u32) -> Result<HeadRead> {
            self.heads.fetch_add(1, Ordering::Relaxed);
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

    /// Keys under this prefix all hash alike, so a test can stand two keys on one fingerprint
    const SHARED: &[u8] = b"shared!!";

    /// The hash every key under the shared prefix takes
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

    // an overwrite that meets another key's slot on a shared fingerprint costs a booking, never that key
    #[test]
    fn a_shared_fingerprint_never_loses_the_other_key() {
        let records = Arc::new(Records::default());
        let segments = Arc::new(SegmentTable::new());
        let column = FastColumn::new();
        column.attach(Arc::clone(&records) as Arc<dyn RecordSource>, Arc::clone(&segments));
        records.is_cold.store(true, Ordering::Relaxed);
        let (overwritten, bystander) = (shared_key(1), shared_key(2));
        let (old, other) = (Loc::new(SegmentId(1), 0, 200), Loc::new(SegmentId(2), 0, 300));
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
        assert!(matches!(column.read(&bystander).expect("read"), Lookup::Unsettled));
        assert_eq!(column.entry(&bystander).expect("entry"), Settled::Footers);
        // a single-candidate read and a move's shortcut leave a displaced slot to the full lookup
        assert!(column.sole(&bystander).is_none());
        assert!(!column.only_at(bystander.as_slice(), other));
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

    // an overwritten version is booked from its length class with no read, and stays until its segment retires
    #[test]
    fn overwritten_versions_are_booked_from_their_class() {
        let records = Arc::new(Records::default());
        let segments = Arc::new(SegmentTable::new());
        let column = FastColumn::new();
        column.attach(Arc::clone(&records) as Arc<dyn RecordSource>, Arc::clone(&segments));
        records.is_cold.store(true, Ordering::Relaxed);

        let old = Loc::new(SegmentId(1), 0, 200);
        records.write(old, key(1).as_slice(), Lsn(1));
        segments.note_max(SegmentId(1), Lsn(5));
        column.insert(key(1).as_slice(), old);
        // 200 bytes sit in the class from 193 to 256
        // segments book the class middle and live bytes the least it covers
        assert_eq!(column.displace(&key(1), Lsn(10)).expect("displace").classed, vec![(Loc::new(SegmentId(1), 0, 224), 193)]);
        assert_eq!(records.heads.load(Ordering::Relaxed), 0);
        assert_eq!((column.held(), column.displaced()), (1, 1));
        assert_eq!(column.displace(&key(1), Lsn(11)).expect("displace"), Displaced::default());
        // a displaced version alone never answers, since it was overwritten or deleted once
        assert!(matches!(column.read(&key(1)).expect("read"), Lookup::Unsettled));
        column.forget_retired(|segment| segment != SegmentId(1));
        assert_eq!((column.held(), column.displaced()), (0, 0));

        // a segment that may hold a newer version is read, and the newer version stays
        let newer = Loc::new(SegmentId(2), 0, 40);
        records.write(newer, key(3).as_slice(), Lsn(30));
        segments.note_max(SegmentId(2), Lsn(30));
        column.insert(key(3).as_slice(), newer);
        assert!(column.displace(&key(3), Lsn(12)).expect("displace").booked.is_empty());
        assert_eq!(version(&column, 3), Some(30));

        // a key holding several versions reads them all and each goes, booked exactly
        let held: Vec<Loc> = (0..SETTLE_AT as u32).map(|at| Loc::new(SegmentId(3), at * 64, 40)).collect();
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
        assert_eq!(column.displace(&key(5), Lsn(70)).expect("displace").classed, vec![(Loc::new(SegmentId(4), 0, 224), 193)]);
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
        assert!(column.slack() < slack, "the corrected booking still counted in the slack");
        assert_eq!(column.displaced(), 0);
    }

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
        assert!(matches!(column.entry(&key(9)).expect("entry"), Settled::Entry(Some(entry)) if entry.lsn == Lsn(1)));
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

    /// A key whose leading bytes are spread like a hash, as an ordered column's keys are
    fn scattered(at: u64) -> RecordKey {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&mix(at ^ 0x5DEE_CE66).to_be_bytes());
        bytes[8..].copy_from_slice(&at.to_be_bytes());
        RecordKey::from_bytes(COLUMN, &bytes).expect("key")
    }

    fn ordered_column(records: &Arc<Records>) -> FastColumn {
        let column = FastColumn::with_layout(Layout::Ordered);
        column.attach(Arc::clone(records) as Arc<dyn RecordSource>, Arc::new(SegmentTable::new()));
        column
    }

    fn version_of(column: &FastColumn, key: &RecordKey) -> Option<u64> {
        match column.read(key).expect("read") {
            Lookup::Found(lsn, _) => Some(lsn.as_u64()),
            Lookup::Missing => None,
            Lookup::Unsettled => panic!("a key needed the checked read"),
        }
    }

    /// Check every shard of an ordered column and hand back the slots it holds
    ///
    /// Each shard is sorted, each slot sits at or past its home with nothing empty in
    /// between, and its count matches what it holds.
    fn assert_ordered(column: &FastColumn) -> u64 {
        let mut held = 0;
        for shard in &column.shards {
            let table = shard.read();
            let mut last = None;
            let mut count = 0;
            for at in 0..table.width() {
                let slot = table.slot_at(at);
                if slot.is_empty() {
                    continue;
                }
                count += 1;
                let home = table.home(slot.mid) * WAYS;
                assert!(at >= home, "a slot sits before its home");
                assert!((home..at).all(|between| !table.slot_at(between).is_empty()), "a gap between a slot and its home");
                assert!(last.is_none_or(|last| last <= slot.order()), "a shard is out of order");
                last = Some(slot.order());
            }
            assert_eq!(count, table.held, "a shard miscounts what it holds");
            held += count as u64;
        }
        held
    }

    // an ordered column keeps its order and its keys through inserts, takes, growth and a sweep
    #[test]
    fn an_ordered_column_keeps_order_through_change() {
        let records = Arc::new(Records::default());
        let column = ordered_column(&records);
        let keys = 30_000u64;
        let loc_of = |at: u64| Loc::new(SegmentId(1 + (at % 7) as u32), at as u32 * 64, 40);
        for at in 0..keys {
            records.write(loc_of(at), scattered(at).as_slice(), Lsn(at + 1));
            column.insert(scattered(at).as_slice(), loc_of(at));
        }
        assert_eq!(assert_ordered(&column), keys);

        for at in (0..keys).step_by(3) {
            assert!(column.remove_at(scattered(at).as_slice(), loc_of(at)), "key {at} was not taken");
        }
        assert_eq!(assert_ordered(&column), keys - keys.div_ceil(3));
        assert!(column.forget_retired(|segment| segment != SegmentId(2)) > 0);
        assert_ordered(&column);

        for at in 0..keys {
            let expected = (at % 3 != 0 && at % 7 != 1).then_some(at + 1);
            assert_eq!(version_of(&column, &scattered(at)), expected, "key {at}");
        }
    }

    // an ordered walk hands rows back in key order, up or down, from a key held or not
    #[test]
    fn an_ordered_walk_reads_rows_in_key_order() {
        let records = Arc::new(Records::default());
        let column = ordered_column(&records);
        let keys = 20_000u64;
        let mut leads = Vec::new();
        for at in 0..keys {
            let key = scattered(at);
            column.insert(key.as_slice(), Loc::new(SegmentId(1), at as u32 * 64, 40));
            leads.push(lead_of(key.as_slice()));
        }
        leads.sort_unstable();

        let mut out = Vec::new();
        for probe in 0..400u64 {
            // Half the walks start at a key held, half at one that is not.
            let from = scattered(probe * 37 % (2 * keys));
            let start = lead_of(from.as_slice());
            column.walk(Some(from.as_slice()), Way::Up, 50, &mut out);
            let up: Vec<u64> = leads.iter().copied().filter(|lead| *lead >= start).take(50).collect();
            assert_eq!(out.iter().map(|row| row.lead).collect::<Vec<_>>(), up, "up from probe {probe}");

            column.walk(Some(from.as_slice()), Way::Down, 50, &mut out);
            let down: Vec<u64> = leads.iter().rev().copied().filter(|lead| *lead <= start).take(50).collect();
            assert_eq!(out.iter().map(|row| row.lead).collect::<Vec<_>>(), down, "down from probe {probe}");
        }

        column.walk(None, Way::Up, usize::MAX, &mut out);
        assert_eq!(out.iter().map(|row| row.lead).collect::<Vec<_>>(), leads);
        column.walk(None, Way::Down, usize::MAX, &mut out);
        assert_eq!(out.len() as u64, keys);
        assert!(out.windows(2).all(|pair| pair[0].lead >= pair[1].lead));
    }

    // keys sharing seven leading bytes spill past a bucket pair, each answers, and a walk keeps them together
    #[test]
    fn keys_sharing_a_lead_spill_and_stay_together() {
        let records = Arc::new(Records::default());
        let column = ordered_column(&records);
        let shared = |at: u64| {
            let mut bytes = [0u8; 16];
            bytes[..7].copy_from_slice(&[9, 8, 7, 6, 5, 4, 3]);
            bytes[8..].copy_from_slice(&at.to_be_bytes());
            RecordKey::from_bytes(COLUMN, &bytes).expect("key")
        };
        let ties = 3 * MAX_CANDIDATES as u64;
        for at in 0..ties {
            let loc = Loc::new(SegmentId(1), at as u32 * 64, 40);
            records.write(loc, shared(at).as_slice(), Lsn(at + 1));
            column.insert(shared(at).as_slice(), loc);
        }
        assert_ordered(&column);
        for at in 0..ties {
            assert_eq!(version_of(&column, &shared(at)), Some(at + 1), "tied key {at}");
        }
        let mut out = Vec::new();
        column.walk(Some(shared(0).as_slice()), Way::Up, 5, &mut out);
        assert_eq!(out.len() as u64, ties, "a walk split a tie");
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
