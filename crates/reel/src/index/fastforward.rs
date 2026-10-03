//! FastForward, a column index that keeps record locations and no keys
//!
//! Each lookup confirms its candidates against the key in the record's own header.

use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

use crate::engine::Totals;
use crate::error::Result;
use crate::format::column::{ColumnSpec, KeyBytes, MapShape, RecordKey};
use crate::format::loc::{Loc, SegmentId};
use crate::format::lsn::Lsn;
use crate::index::column::{ColumnMark, KeyMove, Landed, PendingCover};
use crate::index::counters::{Bookings, SegmentTable};
use crate::index::entry::{span_of, Entry};
use crate::index::page::KeyPage;
use crate::index::paged::SealedRanges;
use crate::sync::{read, write};
use crate::units::ByteCount;

/// The only key width this index holds
pub const WIDTH: usize = 16;

/// Entries in one bucket, which fills one cache line
const WAYS: usize = 7;

/// Share of a table's slots it is sized to fill
const LOAD: f64 = 0.85;

/// Shards, one for each leading key byte
const SHARDS: usize = 256;

/// Keys a column is sized for when the environment gives no count
const DEFAULT_KEYS: u64 = 1 << 20;

/// Environment variable giving the keys a column is sized for
const KEYS_VARIABLE: &str = "REEL_FF_KEYS";

/// Entries a full pair of buckets moves along before the table counts as full
const MAX_KICKS: usize = 512;

/// Bytes one bucket takes
const BUCKET_BYTES: u64 = 64;

const TAG_BITS: u32 = 19;
const TAG_SHIFT: u32 = 64 - TAG_BITS;
const TAG_MASK: u64 = (1 << TAG_BITS) - 1;
const SEGMENT_BITS: u32 = 16;
const SEGMENT_SHIFT: u32 = TAG_SHIFT - SEGMENT_BITS;
const SEGMENT_MASK: u64 = (1 << SEGMENT_BITS) - 1;
const OFFSET_BITS: u32 = 27;
const OFFSET_SHIFT: u32 = SEGMENT_SHIFT - OFFSET_BITS;
const OFFSET_MASK: u64 = (1 << OFFSET_BITS) - 1;
const GRAVE: u64 = 1 << 1;
const SECOND: u64 = 1;

/// Mixed into a key's second word so its halves hash apart
const HASH_SALT: u64 = 0x5851_F42D_4C95_7F2D;

/// What a record's header says about it
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Head {
    pub key: [u8; WIDTH],
    pub lsn: Lsn,
    pub len: u32,
    pub is_tombstone: bool,
}

/// Where the index reads the header of a record it points at
pub trait RecordSource: Send + Sync {
    /// The header of the record at a place, or nothing when no record is there
    fn head(&self, segment: SegmentId, offset: u32) -> Result<Option<Head>>;
}

#[repr(C, align(64))]
#[derive(Clone, Copy, Default)]
struct Bucket {
    entries: [u64; WAYS],
    spare: u64,
}

/// One place in a table and the entry found there
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Place {
    bucket: usize,
    way: usize,
    entry: u64,
}

/// A candidate whose record holds the key asked about
#[derive(Clone, Copy)]
struct Found {
    place: Place,
    head: Head,
}

impl Found {
    fn is_grave(&self) -> bool {
        self.place.entry & GRAVE != 0 || self.head.is_tombstone
    }
}

struct Table {
    buckets: Vec<Bucket>,
    seed: u64,
}

fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

fn word(bytes: &[u8]) -> u64 {
    let mut word = [0u8; 8];
    word.copy_from_slice(bytes);
    u64::from_le_bytes(word)
}

fn hash_of(key: &[u8]) -> Option<u64> {
    match key.len() == WIDTH {
        true => Some(mix(word(&key[..8]) ^ mix(word(&key[8..]) ^ HASH_SALT))),
        false => None,
    }
}

fn tag_of_hash(hash: u64) -> u64 {
    (hash & TAG_MASK).max(1)
}

fn tag_of(entry: u64) -> u64 {
    entry >> TAG_SHIFT
}

fn segment_of(entry: u64) -> SegmentId {
    SegmentId(((entry >> SEGMENT_SHIFT) & SEGMENT_MASK) as u32)
}

fn offset_of(entry: u64) -> u32 {
    ((entry >> OFFSET_SHIFT) & OFFSET_MASK) as u32
}

/// An entry for a record, or nothing for a segment or offset past what an entry holds
fn entry_for(tag: u64, loc: Loc, is_grave: bool) -> Option<u64> {
    let segment = u64::from(loc.segment.as_u32());
    let offset = u64::from(loc.offset);
    if segment > SEGMENT_MASK || offset > OFFSET_MASK {
        return None;
    }
    let grave = match is_grave {
        true => GRAVE,
        false => 0,
    };
    Some((tag << TAG_SHIFT) | (segment << SEGMENT_SHIFT) | (offset << OFFSET_SHIFT) | grave)
}

impl Table {
    fn sized(keys: u64) -> Table {
        let count = ((keys as f64 / (WAYS as f64 * LOAD)).ceil() as usize).max(2);
        Table {
            buckets: vec![Bucket::default(); count],
            seed: count as u64,
        }
    }

    fn step(&self, tag: u64) -> usize {
        1 + (mix(tag) % (self.buckets.len() as u64 - 1)) as usize
    }

    fn pair(&self, hash: u64) -> [usize; 2] {
        let count = self.buckets.len();
        let first = ((u128::from(hash) * count as u128) >> 64) as usize;
        [first, (first + self.step(tag_of_hash(hash))) % count]
    }

    /// The other bucket an entry may sit in
    fn other(&self, bucket: usize, entry: u64) -> usize {
        let count = self.buckets.len();
        let step = self.step(tag_of(entry));
        match entry & SECOND == 0 {
            true => (bucket + step) % count,
            false => (bucket + count - step) % count,
        }
    }

    /// Every entry carrying a key's tag, and where it sits
    fn matches(&self, hash: u64) -> Vec<Place> {
        let tag = tag_of_hash(hash);
        let mut found = Vec::new();
        for bucket in self.pair(hash) {
            for (way, entry) in self.buckets[bucket].entries.iter().enumerate() {
                if *entry != 0 && tag_of(*entry) == tag {
                    found.push(Place {
                        bucket,
                        way,
                        entry: *entry,
                    });
                }
            }
        }
        found
    }

    fn free_way(&self, bucket: usize) -> Option<usize> {
        self.buckets[bucket].entries.iter().position(|entry| *entry == 0)
    }

    /// Put an entry in either of its buckets, moving others along when both are full
    fn place(&mut self, hash: u64, entry: u64) -> bool {
        let [first, second] = self.pair(hash);
        for (bucket, flag) in [(first, 0), (second, SECOND)] {
            if let Some(way) = self.free_way(bucket) {
                self.buckets[bucket].entries[way] = entry | flag;
                return true;
            }
        }
        let (mut bucket, mut moving) = (first, entry);
        for _ in 0..MAX_KICKS {
            self.seed = mix(self.seed);
            let way = (self.seed % WAYS as u64) as usize;
            let evicted = std::mem::replace(&mut self.buckets[bucket].entries[way], moving);
            bucket = self.other(bucket, evicted);
            moving = evicted ^ SECOND;
            if let Some(free) = self.free_way(bucket) {
                self.buckets[bucket].entries[free] = moving;
                return true;
            }
        }
        false
    }

    fn set(&mut self, place: Place, entry: u64) {
        self.buckets[place.bucket].entries[place.way] = entry;
    }
}

/// A column's keys as record locations, sharded by their leading byte
pub struct FastColumn {
    shards: Vec<RwLock<Table>>,
    records: OnceLock<Arc<dyn RecordSource>>,
    pub(crate) shard_bytes: u8,
    live: AtomicU64,
    bytes: AtomicU64,
    graves: AtomicU64,
}

impl FastColumn {
    /// An empty column sized for the keys the environment says it will hold
    pub fn new(_spec: &ColumnSpec) -> FastColumn {
        FastColumn {
            shards: (0..SHARDS)
                .map(|_| RwLock::new(Table::sized(Self::keys_per_shard())))
                .collect(),
            records: OnceLock::new(),
            shard_bytes: 1,
            live: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            graves: AtomicU64::new(0),
        }
    }

    fn keys_per_shard() -> u64 {
        let keys = std::env::var(KEYS_VARIABLE)
            .ok()
            .and_then(|count| count.parse().ok())
            .unwrap_or(DEFAULT_KEYS);
        keys.div_ceil(SHARDS as u64)
    }

    /// Where lookups and puts read record headers from
    pub fn attach(&self, records: Arc<dyn RecordSource>) {
        let _ = self.records.set(records);
    }

    fn shard(&self, key: &[u8]) -> &RwLock<Table> {
        &self.shards[usize::from(key[0])]
    }

    /// The candidates whose records hold this key, read with no lock held
    fn same_key(records: &dyn RecordSource, seen: &[Place], key: &[u8]) -> Vec<Found> {
        let mut found = Vec::with_capacity(seen.len());
        for place in seen {
            match records.head(segment_of(place.entry), offset_of(place.entry)) {
                Ok(Some(head)) if head.key.as_slice() == key => found.push(Found {
                    place: *place,
                    head,
                }),
                Ok(Some(_)) | Ok(None) => {}
                Err(error) => tracing::warn!("a FastForward lookup could not read a record header: {error}"),
            }
        }
        found
    }

    fn newest(found: &[Found]) -> Option<Found> {
        found.iter().copied().max_by_key(|candidate| candidate.head.lsn)
    }

    fn book_new<Book: Bookings>(&self, loc: Loc, lsn: Lsn, is_delete: bool, books: &Book) {
        match is_delete {
            true => {
                self.graves.fetch_add(1, Ordering::Relaxed);
            }
            false => {
                books.mark_live(loc.segment, lsn, span_of(WIDTH as u16, loc.len));
                self.live.fetch_add(1, Ordering::Relaxed);
                self.bytes.fetch_add(u64::from(loc.len), Ordering::Relaxed);
            }
        }
    }

    fn book_gone<Book: Bookings>(&self, older: &Found, books: &Book) {
        match older.is_grave() {
            true => {
                self.graves.fetch_sub(1, Ordering::Relaxed);
            }
            false => {
                books.shadow(segment_of(older.place.entry), span_of(WIDTH as u16, older.head.len));
                self.live.fetch_sub(1, Ordering::Relaxed);
                self.bytes.fetch_sub(u64::from(older.head.len), Ordering::Relaxed);
            }
        }
    }

    /// Put one version of a key, ordered against what the table holds for it
    fn settle<Book: Bookings>(
        &self,
        key: &[u8],
        loc: Loc,
        lsn: Lsn,
        is_delete: bool,
        books: &Book,
    ) -> Landed {
        let Some(hash) = hash_of(key) else {
            return Landed::Newer;
        };
        let Some(entry) = entry_for(tag_of_hash(hash), loc, is_delete) else {
            unimplemented!("a FastForward entry for a segment or offset past what eight bytes hold")
        };
        let records = match (books.can_read_records(), self.records.get()) {
            (true, Some(records)) => Some(records),
            (true, None) | (false, Some(_)) | (false, None) => None,
        };
        let shard = self.shard(key);
        loop {
            let seen = read(shard).matches(hash);
            let found = match records {
                Some(records) => Self::same_key(records.as_ref(), &seen, key),
                None => Vec::new(),
            };
            let mut table = write(shard);
            // A put elsewhere moved these entries while their headers were read.
            if table.matches(hash) != seen {
                continue;
            }
            return match Self::newest(&found) {
                Some(newest) if newest.head.lsn >= lsn => {
                    if !is_delete {
                        books.mark_dead(loc.segment, lsn, span_of(WIDTH as u16, loc.len));
                    }
                    Landed::Newer
                }
                Some(newest) => {
                    for older in &found {
                        self.book_gone(older, books);
                        table.set(older.place, 0);
                    }
                    table.set(newest.place, entry | (newest.place.entry & SECOND));
                    self.book_new(loc, lsn, is_delete, books);
                    match newest.is_grave() {
                        true => Landed::Grave,
                        false => Landed::Record,
                    }
                }
                None => {
                    if !table.place(hash, entry) {
                        unimplemented!("growing a full FastForward table")
                    }
                    self.book_new(loc, lsn, is_delete, books);
                    Landed::Nothing
                }
            };
        }
    }

    pub fn apply_moves<Book: Bookings>(
        &self,
        moves: &[KeyMove<'_>],
        segments: &Book,
        landed: &mut Vec<Landed>,
    ) {
        for moving in moves {
            if moving.is_delete {
                segments.mark_held(moving.loc.segment, moving.lsn, span_of(WIDTH as u16, moving.loc.len));
            }
            landed.push(self.settle(moving.key, moving.loc, moving.lsn, moving.is_delete, segments));
        }
    }

    pub fn insert(&self, key: &[u8], entry: Entry, segments: &SegmentTable) -> Landed {
        self.settle(key, entry.loc, entry.lsn, false, segments)
    }

    pub fn remove(&self, key: &[u8], lsn: Lsn, tombstone: Loc, segments: &SegmentTable) -> Landed {
        segments.mark_held(tombstone.segment, lsn, span_of(WIDTH as u16, tombstone.len));
        self.settle(key, tombstone, lsn, true, segments)
    }

    pub fn entry_or_grave(&self, key: &[u8]) -> Option<Entry> {
        let hash = hash_of(key)?;
        let records = self.records.get()?;
        let seen = read(self.shard(key)).matches(hash);
        let newest = Self::newest(&Self::same_key(records.as_ref(), &seen, key))?;
        let segment = segment_of(newest.place.entry);
        Some(match newest.is_grave() {
            true => Entry::grave_from(newest.head.lsn, segment),
            false => Entry::new(
                Loc::new(segment, offset_of(newest.place.entry), newest.head.len),
                newest.head.lsn,
            ),
        })
    }

    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        self.entry_or_grave(key).filter(|entry| !entry.is_grave())
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    pub fn size_of(&self, key: &[u8]) -> Option<ByteCount> {
        self.get(key).map(|entry| ByteCount::from_bytes(u64::from(entry.loc.len)))
    }

    pub fn entry_many(&self, keys: &[RecordKey], run: &[usize], out: &mut Vec<Option<Entry>>) {
        out.clear();
        for at in run {
            out.push(self.entry_or_grave(keys[*at].as_slice()));
        }
    }

    pub fn repoint(&self, key: &[u8], new_loc: Loc, expected_lsn: Lsn, segments: &SegmentTable) -> bool {
        let (Some(hash), Some(records)) = (hash_of(key), self.records.get()) else {
            return false;
        };
        let shard = self.shard(key);
        loop {
            let seen = read(shard).matches(hash);
            let found = Self::same_key(records.as_ref(), &seen, key);
            let mut table = write(shard);
            if table.matches(hash) != seen {
                continue;
            }
            let moved = found
                .iter()
                .find(|candidate| candidate.head.lsn == expected_lsn && !candidate.is_grave());
            let span = span_of(WIDTH as u16, new_loc.len);
            return match moved.map(|candidate| (candidate, entry_for(tag_of_hash(hash), new_loc, false))) {
                Some((candidate, Some(entry))) => {
                    segments.release_live(
                        segment_of(candidate.place.entry),
                        span_of(WIDTH as u16, candidate.head.len),
                    );
                    segments.mark_live(new_loc.segment, expected_lsn, span);
                    table.set(candidate.place, entry | (candidate.place.entry & SECOND));
                    true
                }
                Some((_, None)) | None => {
                    segments.mark_dead(new_loc.segment, expected_lsn, span);
                    false
                }
            };
        }
    }

    pub fn evict_at(&self, key: &[u8], loc: Loc, segments: &SegmentTable) -> bool {
        let Some(hash) = hash_of(key) else {
            return false;
        };
        let mut table = write(self.shard(key));
        let held = table.matches(hash).into_iter().find(|place| {
            place.entry & GRAVE == 0
                && segment_of(place.entry) == loc.segment
                && offset_of(place.entry) == loc.offset
        });
        match held {
            Some(place) => {
                table.set(place, 0);
                segments.shadow(loc.segment, span_of(WIDTH as u16, loc.len));
                self.live.fetch_sub(1, Ordering::Relaxed);
                self.bytes.fetch_sub(u64::from(loc.len), Ordering::Relaxed);
                true
            }
            None => false,
        }
    }

    pub fn key_width(&self) -> u16 {
        WIDTH as u16
    }

    pub fn overhead_per_key(&self) -> u64 {
        (BUCKET_BYTES as f64 / (WAYS as f64 * LOAD)) as u64
    }

    pub fn filter_bytes(&self) -> u64 {
        0
    }

    pub fn heap_bytes(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| read(shard).buckets.len() as u64 * BUCKET_BYTES)
            .sum()
    }

    pub fn map_shape(&self) -> MapShape {
        MapShape::Open
    }

    pub fn settle_paged(&self, _key: &[u8], _loc: Loc, _counted: bool, _segments: &SegmentTable) -> bool {
        false
    }

    pub fn evict_paged(&self, _key: &[u8], _loc: Loc, _lsn: Lsn, _counted: bool, _segments: &SegmentTable) -> bool {
        false
    }

    pub fn repoint_paged(
        &self,
        _key: &[u8],
        _from: Loc,
        _to: Loc,
        _lsn: Lsn,
        _counted: bool,
        _segments: &SegmentTable,
    ) -> bool {
        false
    }

    pub fn page_out(&self, _key: &[u8], _loc: Loc) -> bool {
        false
    }

    pub fn remove_range(&self, _start: &[u8], _end: Option<&[u8]>, _lsn: Lsn) {
        unimplemented!("a range delete on a FastForward column")
    }

    pub fn next_pending_cover(&self) -> Option<PendingCover> {
        None
    }

    pub fn advance_release(&self, _lsn: Lsn, _resume: Option<&[u8]>) {}

    pub fn sweep_run(&self, _lsn: Lsn, _budget: usize, _segments: &SegmentTable) -> (u64, usize, bool) {
        (0, 0, true)
    }

    pub fn release_covered(&self, _key: &[u8], _loc: Loc, _below: Lsn, _counted: bool, _segments: &SegmentTable) -> bool {
        false
    }

    pub fn covered_by_swept(&self, _key: &[u8], _lsn: Lsn) -> bool {
        false
    }

    pub fn pending_overlaps(&self, _low: &[u8], _high: &[u8]) -> bool {
        false
    }

    pub fn has_pending_covers(&self) -> bool {
        false
    }

    pub fn prune_tombstones(&self, _before: Lsn, _sealed: Option<&SealedRanges>) -> u64 {
        0
    }

    pub fn grave_count(&self) -> u64 {
        self.graves.load(Ordering::Relaxed)
    }

    pub fn cover_count(&self) -> u64 {
        0
    }

    pub fn is_covered_key(&self, _key: &[u8], _lsn: Lsn) -> bool {
        false
    }

    pub fn is_covered_key_at(&self, _key: &[u8], _lsn: Lsn, _snapshot: Lsn) -> bool {
        false
    }

    pub fn held(&self) -> Vec<(KeyBytes, Entry)> {
        unimplemented!("listing a FastForward column")
    }

    pub fn drop_shadowed(&self, _key: &[u8], _segments: &SegmentTable) {}

    pub fn fit(&self) {}

    pub fn clear(&self) {
        for shard in &self.shards {
            *write(shard) = Table::sized(Self::keys_per_shard());
        }
        self.live.store(0, Ordering::Relaxed);
        self.bytes.store(0, Ordering::Relaxed);
        self.graves.store(0, Ordering::Relaxed);
    }

    pub fn totals(&self) -> Totals {
        Totals {
            count: self.live.load(Ordering::Relaxed),
            bytes: ByteCount::from_bytes(self.bytes.load(Ordering::Relaxed)),
        }
    }

    pub fn resident_keys(&self) -> u64 {
        self.live.load(Ordering::Relaxed) + self.graves.load(Ordering::Relaxed)
    }

    pub fn lead_tie_rate(&self) -> Option<f64> {
        None
    }

    pub fn prefix_totals(&self, _prefix: &[u8]) -> Option<Totals> {
        None
    }

    pub fn page(&self, _start: Bound<&[u8]>, _limit: usize, _out: &mut KeyPage) {
        unimplemented!("walking a FastForward column")
    }

    pub fn sweep_prefix(
        &self,
        _nonce: u64,
        _prefix: &[u8],
        _from: Option<&ColumnMark>,
        _limit: usize,
        _out: &mut KeyPage,
    ) -> Option<ColumnMark> {
        unimplemented!("walking a FastForward column")
    }

    pub fn sweep(&self, _nonce: u64, _from: Option<&ColumnMark>, _limit: usize, _out: &mut KeyPage) -> Option<ColumnMark> {
        unimplemented!("walking a FastForward column")
    }

    pub fn page_back(&self, _end: Bound<&[u8]>, _limit: usize, _out: &mut KeyPage) {
        unimplemented!("walking a FastForward column")
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::*;
    use crate::format::column::{Codec, ColumnId, KeyWidth};

    const SPEC: ColumnSpec = ColumnSpec {
        id: ColumnId(1),
        name: "fast",
        key_width: KeyWidth::Fixed(16),
        shard_bytes: 1,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Open,
    };

    /// Record headers kept in memory, the way a segment would answer
    #[derive(Default)]
    struct Records {
        heads: Mutex<HashMap<(u32, u32), Head>>,
    }

    impl Records {
        fn write(&self, loc: Loc, key: [u8; WIDTH], lsn: Lsn, is_tombstone: bool) {
            let head = Head {
                key,
                lsn,
                len: loc.len,
                is_tombstone,
            };
            self.heads
                .lock()
                .expect("records")
                .insert((loc.segment.as_u32(), loc.offset), head);
        }
    }

    impl RecordSource for Records {
        fn head(&self, segment: SegmentId, offset: u32) -> Result<Option<Head>> {
            Ok(self
                .heads
                .lock()
                .expect("records")
                .get(&(segment.as_u32(), offset))
                .copied())
        }
    }

    fn key(at: u64) -> [u8; WIDTH] {
        let mut key = [0u8; WIDTH];
        key[..8].copy_from_slice(&mix(at).to_be_bytes());
        key[8..].copy_from_slice(&at.to_be_bytes());
        key
    }

    fn column(records: &Arc<Records>) -> FastColumn {
        let column = FastColumn::new(&SPEC);
        column.attach(Arc::clone(records) as Arc<dyn RecordSource>);
        column
    }

    // every key put reads back at its newest version, through updates and deletes
    #[test]
    fn puts_updates_and_deletes_read_back() {
        let records = Arc::new(Records::default());
        let column = column(&records);
        let segments = SegmentTable::new();
        let mut lsn = 0u64;
        let place = |segment: u32, offset: &mut u32| {
            *offset += 64;
            Loc::new(SegmentId(segment), *offset, 40)
        };
        let mut offset = 0u32;
        let keys = 20_000u64;
        for at in 0..keys {
            lsn += 1;
            let loc = place(1, &mut offset);
            records.write(loc, key(at), Lsn(lsn), false);
            assert_eq!(column.insert(&key(at), Entry::new(loc, Lsn(lsn)), &segments), Landed::Nothing);
        }
        for at in (0..keys).step_by(3) {
            lsn += 1;
            let loc = place(2, &mut offset);
            records.write(loc, key(at), Lsn(lsn), false);
            assert_eq!(column.insert(&key(at), Entry::new(loc, Lsn(lsn)), &segments), Landed::Record);
        }
        for at in (0..keys).step_by(7) {
            lsn += 1;
            let tombstone = Loc::new(SegmentId(3), offset + 64, 0);
            offset += 64;
            records.write(tombstone, key(at), Lsn(lsn), true);
            column.remove(&key(at), Lsn(lsn), tombstone, &segments);
        }
        for at in 0..keys {
            let found = column.get(&key(at));
            match at % 7 == 0 {
                true => assert!(found.is_none(), "key {at} was deleted"),
                false => {
                    let found = found.expect("held");
                    let segment = match at % 3 == 0 {
                        true => SegmentId(2),
                        false => SegmentId(1),
                    };
                    assert_eq!(found.loc.segment, segment, "key {at} reads its newest version");
                }
            }
        }
        assert!(column.get(&key(keys + 1)).is_none());
    }

    // a late put of an older version is refused, and a repoint moves only the version it names
    #[test]
    fn older_puts_are_refused_and_repoints_move_one_version() {
        let records = Arc::new(Records::default());
        let column = column(&records);
        let segments = SegmentTable::new();
        let newer = Loc::new(SegmentId(5), 128, 40);
        let older = Loc::new(SegmentId(4), 64, 40);
        records.write(newer, key(1), Lsn(9), false);
        records.write(older, key(1), Lsn(3), false);
        column.insert(&key(1), Entry::new(newer, Lsn(9)), &segments);
        assert_eq!(column.insert(&key(1), Entry::new(older, Lsn(3)), &segments), Landed::Newer);
        assert_eq!(column.get(&key(1)).map(|entry| entry.loc), Some(newer));

        let copy = Loc::new(SegmentId(6), 256, 40);
        records.write(copy, key(1), Lsn(9), false);
        assert!(!column.repoint(&key(1), copy, Lsn(3), &segments));
        assert!(column.repoint(&key(1), copy, Lsn(9), &segments));
        assert_eq!(column.get(&key(1)).map(|entry| entry.loc), Some(copy));
        assert!(column.evict_at(&key(1), copy, &segments));
        assert!(column.get(&key(1)).is_none());
    }

    // a version put with no records to read sits beside the first, and a lookup answers the newest
    #[test]
    fn versions_put_unread_sit_side_by_side() {
        let records = Arc::new(Records::default());
        let column = FastColumn::new(&SPEC);
        let segments = SegmentTable::new();
        let first = Loc::new(SegmentId(1), 64, 40);
        let second = Loc::new(SegmentId(2), 64, 40);
        records.write(first, key(8), Lsn(1), false);
        records.write(second, key(8), Lsn(2), false);
        column.insert(&key(8), Entry::new(first, Lsn(1)), &segments);
        column.insert(&key(8), Entry::new(second, Lsn(2)), &segments);
        column.attach(Arc::clone(&records) as Arc<dyn RecordSource>);
        assert_eq!(column.get(&key(8)).map(|entry| entry.loc), Some(second));
    }
}
