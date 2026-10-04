//! FastForward, the index of sealed keys, which keeps record locations and no keys
//!
//! A lookup confirms each candidate against the key in the record's own header.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use reel_core::Value;

use crate::error::Result;
use crate::format::column::{KeyRef, RecordKey};
use crate::format::loc::{Loc, SegmentId};
use crate::format::lsn::Lsn;
use crate::index::counters::SegmentTable;
use crate::index::entry::Entry;
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

/// Times a lookup starts over when a candidate's segment went while it read
///
/// A compaction can move a record, seal its copy and retire the source inside one
/// slow read, and only a fresh look at the table finds the copy.
const LOOKUP_TRIES: usize = 4;

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
    hash_seeded(key, 0)
}

/// A second hash of a key, which a load keeps beside a slot so two keys sharing its bits stay apart
fn check_of(key: &[u8]) -> u32 {
    (hash_seeded(key, 1) >> 32) as u32
}

fn hash_seeded(key: &[u8], seed: u64) -> u64 {
    const ODD: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut state = 0xCBF2_9CE4_8422_2325 ^ key.len() as u64 ^ seed.wrapping_mul(ODD);
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

/// What a load keeps beside one slot: the version it holds and the key's second hash
#[derive(Clone, Copy, Default)]
struct Carry {
    lsn: u64,
    check: u32,
}

struct Table {
    buckets: Vec<Bucket>,
    held: usize,
    seed: u64,

    /// One carry per slot while a load runs, and nothing otherwise
    carries: Vec<Carry>,
}

impl Table {
    fn with_buckets(count: usize) -> Table {
        Table {
            buckets: vec![Bucket::default(); count.max(2)],
            held: 0,
            seed: count as u64,
            carries: Vec::new(),
        }
    }

    fn is_loading(&self) -> bool {
        !self.carries.is_empty()
    }

    fn carry_at(&self, bucket: usize, way: usize) -> Carry {
        match self.is_loading() {
            true => self.carries[bucket * WAYS + way],
            false => Carry::default(),
        }
    }

    /// Write a slot and, while loading, what it carries
    fn put(&mut self, bucket: usize, way: usize, slot: Slot, carry: Carry) {
        self.buckets[bucket].slots[way] = slot;
        if self.is_loading() {
            self.carries[bucket * WAYS + way] = carry;
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
    fn place(&mut self, slot: Slot, carry: Carry) -> std::result::Result<(), (Slot, Carry)> {
        let home = self.home(slot.mid);
        let second = (home + self.step(slot.tag())) % self.buckets.len();
        for (bucket, is_second) in [(home, false), (second, true)] {
            if let Some(way) = self.free_way(bucket) {
                self.put(bucket, way, slot.in_second(is_second), carry);
                self.held += 1;
                return Ok(());
            }
        }
        let (mut bucket, mut moving, mut carried) = (home, slot.in_second(false), carry);
        for _ in 0..MAX_KICKS {
            self.seed = mix(self.seed);
            let way = (self.seed % WAYS as u64) as usize;
            let (evicted, evicted_carry) = (self.buckets[bucket].slots[way], self.carry_at(bucket, way));
            self.put(bucket, way, moving, carried);
            bucket = self.other(bucket, &evicted);
            moving = evicted.in_second(!evicted.is_second());
            carried = evicted_carry;
            if let Some(free) = self.free_way(bucket) {
                self.put(bucket, free, moving, carried);
                self.held += 1;
                return Ok(());
            }
        }
        Err((moving, carried))
    }

    fn is_full(&self) -> bool {
        self.held as f64 >= (self.buckets.len() * WAYS) as f64 * LOAD
    }

    /// A table this one's slots fit into at the next size, carries and all
    fn grown(&self) -> Table {
        let mut count = ((self.buckets.len() as f64) * GROWTH).ceil() as usize;
        loop {
            let mut table = Table::with_buckets(count);
            if self.is_loading() {
                table.carries = vec![Carry::default(); count.max(2) * WAYS];
            }
            let mut fits = true;
            'slots: for (bucket, held) in self.buckets.iter().enumerate() {
                for (way, slot) in held.slots.iter().enumerate() {
                    if !slot.is_empty() && table.place(*slot, self.carry_at(bucket, way)).is_err() {
                        fits = false;
                        break 'slots;
                    }
                }
            }
            if fits {
                return table;
            }
            count = ((count as f64) * GROWTH).ceil() as usize;
        }
    }

    /// Put a slot in, growing the table first when it is full or the pair has no room
    fn insert(&mut self, slot: Slot, carry: Carry) {
        if self.is_full() {
            *self = self.grown();
        }
        let mut homeless = (slot, carry);
        while let Err(left) = self.place(homeless.0, homeless.1) {
            *self = self.grown();
            homeless = left;
        }
    }

    /// Take out the slot pointing at one record, wherever displacement has moved it
    fn take(&mut self, hash: u64, slot: &Slot) -> bool {
        let found = self.matches(hash);
        match found.iter().find(|place| place.slot.same_place(slot)) {
            Some(place) => {
                self.put(place.bucket, place.way, Slot::default(), Carry::default());
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

/// A column's sealed keys as record locations, in shards picked by hash
pub struct FastColumn {
    shards: Vec<RwLock<Table>>,
    records: OnceLock<Arc<dyn RecordSource>>,
    segments: OnceLock<Arc<SegmentTable>>,
    stale: Mutex<VecDeque<Stale>>,
    beside: AtomicU64,
}

impl Default for FastColumn {
    fn default() -> FastColumn {
        FastColumn::new()
    }
}

impl FastColumn {
    pub fn new() -> FastColumn {
        FastColumn {
            shards: (0..SHARDS)
                .map(|_| RwLock::new(Table::with_buckets(FIRST_BUCKETS)))
                .collect(),
            records: OnceLock::new(),
            segments: OnceLock::new(),
            stale: Mutex::new(VecDeque::new()),
            beside: AtomicU64::new(0),
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
        self.shards.iter().map(|shard| read(shard).held as u64).sum()
    }

    pub fn heap_bytes(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| read(shard).buckets.len() as u64 * BUCKET_BYTES)
            .sum()
    }

    /// Hold a sealed record's location
    ///
    /// The caller hands over the newest version, so an older entry of the same key is
    /// one the map displaced and the cleaner will settle.
    pub fn insert(&self, key: &[u8], loc: Loc) {
        let hash = hash_of(key);
        let slot = Slot::new(hash, loc);
        let mut table = write(&self.shards[shard_of(hash)]);
        if table.matches(hash).iter().any(|place| place.slot.same_place(&slot)) {
            return;
        }
        table.insert(slot, Carry::default());
    }

    /// Get ready to load sealed rows, keeping a version and a check beside each slot
    pub fn begin_load(&self) {
        for shard in &self.shards {
            let mut table = write(shard);
            table.carries = vec![Carry::default(); table.buckets.len() * WAYS];
        }
    }

    /// Load one sealed row, keeping each key's newest version and a tombstone as a grave
    ///
    /// A tie goes to the newer segment, which is a compaction copy of the other.
    pub fn load(&self, key: &[u8], loc: Loc, lsn: Lsn, is_tombstone: bool) {
        let hash = hash_of(key);
        let check = check_of(key);
        let slot = match is_tombstone {
            true => Slot::new(hash, loc).as_grave(),
            false => Slot::new(hash, loc),
        };
        let carry = Carry {
            lsn: lsn.as_u64(),
            check,
        };
        let mut table = write(&self.shards[shard_of(hash)]);
        let held = table
            .matches(hash)
            .iter()
            .copied()
            .find(|place| table.carry_at(place.bucket, place.way).check == check);
        match held {
            Some(place) => {
                let standing = table.carry_at(place.bucket, place.way).lsn;
                if (carry.lsn, slot.segment) > (standing, place.slot.segment) {
                    let moved = slot.in_second(place.slot.is_second());
                    table.put(place.bucket, place.way, moved, carry);
                }
            }
            None => table.insert(slot, carry),
        }
    }

    /// Close a load: drop the graves that only kept older rows out, and the carries
    pub fn finish_load(&self) {
        for shard in &self.shards {
            let mut table = write(shard);
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
            table.carries = Vec::new();
        }
    }

    /// Take out the entry pointing at one record, for a compaction move, an eviction or a release
    pub fn remove_at(&self, key: &[u8], loc: Loc) -> bool {
        let hash = hash_of(key);
        let mut table = write(&self.shards[shard_of(hash)]);
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
        let seen = read(&self.shards[shard]).matches(hash);
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
        let mut table = write(&self.shards[shard]);
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
            let took = write(&self.shards[shard_of(stale.hash)]).take(stale.hash, &stale.slot);
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
        let seen = read(&self.shards[shard_of(hash)]).matches(hash);
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
        let (Some(records), Some(segments)) = (self.records.get(), self.segments.get()) else {
            return Ok(Lookup::Unsettled);
        };
        for _ in 0..LOOKUP_TRIES {
            if let Some(lookup) = self.read_once(key, records.as_ref(), segments)? {
                return Ok(lookup);
            }
        }
        Ok(Lookup::Unsettled)
    }

    /// One look at the table and a read of each candidate, or nothing when a segment went under it
    fn read_once(&self, key: &RecordKey, records: &dyn RecordSource, segments: &SegmentTable) -> Result<Option<Lookup>> {
        let (hash, ordered) = self.ordered(key, segments);
        let mut best: Option<(Head, Option<Value>)> = None;
        let mut stale = Vec::new();
        for (ceiling, slot) in &ordered {
            // A segment holding nothing newer than the version in hand needs no read.
            if let (Some((head, _)), Some(ceiling)) = (&best, ceiling) {
                if *ceiling < head.lsn {
                    continue;
                }
            }
            let (head, value) = match records.record(key, slot.segment(), slot.offset, slot.bound())? {
                FastRead::Found(head, value) => (head, Some(value)),
                FastRead::Tombstone(head) => (head, None),
                FastRead::Other => continue,
                FastRead::Gone => return Ok(None),
                FastRead::Unsure => return Ok(Some(Lookup::Unsettled)),
            };
            match &best {
                Some((current, _)) if current.lsn >= head.lsn => {
                    if current.lsn > head.lsn {
                        stale.push((*slot, head.len));
                    }
                }
                Some(_) | None => best = Some((head, value)),
            }
        }
        for (slot, len) in stale {
            self.queue(Stale {
                key: key.clone(),
                hash,
                slot,
                len,
            });
        }
        Ok(Some(match best {
            Some((head, Some(value))) => Lookup::Found(head.lsn, value),
            Some((_, None)) | None => Lookup::Missing,
        }))
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
        let (_, ordered) = self.ordered(key, segments);
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
                HeadRead::Missing => return Ok(None),
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
            let mut table = write(shard);
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
            forgotten += dropped as u64;
        }
        forgotten
    }

    /// Drop every entry, for a rebuild starting over
    pub fn clear(&self) {
        for shard in &self.shards {
            *write(shard) = Table::with_buckets(FIRST_BUCKETS);
        }
        lock(&self.stale).clear();
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
    }

    /// Records kept in memory, the way a segment would answer
    #[derive(Default)]
    struct Records {
        written: Mutex<HashMap<(u32, u32), Written>>,
        is_cold: AtomicBool,
    }

    impl Records {
        fn write(&self, loc: Loc, key: &[u8], lsn: Lsn) {
            let written = Written {
                key: key.to_vec(),
                lsn,
                len: loc.len,
            };
            lock(&self.written).insert((loc.segment.as_u32(), loc.offset), written);
        }

        fn answer(&self, key: KeyRef<'_>, segment: SegmentId, offset: u32) -> HeadRead {
            match lock(&self.written).get(&(segment.as_u32(), offset)) {
                Some(written) if written.key == key.bytes => HeadRead::Same(Head {
                    lsn: written.lsn,
                    len: written.len,
                    is_tombstone: false,
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
        column.begin_load();
        for (at, loc, lsn, is_tombstone) in rows {
            if !is_tombstone {
                records.write(loc, key(at).as_slice(), Lsn(lsn));
            }
            column.load(key(at).as_slice(), loc, Lsn(lsn), is_tombstone);
        }
        column.finish_load();
        assert_eq!(version(&column, 1), Some(5), "the newer row stands");
        assert_eq!(version(&column, 3), None, "a newer tombstone drops the key");
        assert_eq!(version(&column, 4), Some(7), "a newer row stands over an older tombstone");
        assert_eq!(column.held(), 3);
        assert!(column.remove_at(key(2).as_slice(), Loc::new(SegmentId(3), 64, 40)), "a tie went to the copy");
        assert_eq!(column.held(), 2);
    }

    // a candidate whose segment went sends the lookup to the checked path, never to an older version
    #[test]
    fn a_gone_candidate_never_lets_an_older_version_answer() {
        let records = Arc::new(Records::default());
        let column = column(&records);
        let older = Loc::new(SegmentId(1), 0, 40);
        let moved = Loc::new(SegmentId(2), 0, 40);
        records.write(older, key(9).as_slice(), Lsn(1));
        column.insert(key(9).as_slice(), older);
        column.insert(key(9).as_slice(), moved);
        assert!(matches!(column.read(&key(9)).expect("read"), Lookup::Unsettled));
        assert_eq!(column.entry(&key(9)).expect("entry"), None);
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
