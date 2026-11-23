//! A column's sorted keys: every live sealed key of the column with where its newest row sits, so a scan steps one sorted list and
//! a get reads no footers
//!
//! The keys sit in 4 KiB pages from one pool fixed at open, as a B+ tree in each of 256 parts split by the key's first byte. A part's
//! lock guards the pages its tree holds. A page's words are atomics read and written relaxed under that lock, so a page can pass from
//! one part to another through the pool with no unsafe code. Keys are packed big-endian into words, so a word compare orders them.

use std::cmp::Ordering;
use std::hint::black_box;
use std::sync::{Arc, Mutex, RwLockWriteGuard};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering as Memory};

use reel_core::units::{ByteCount, FileOffset, RowCount, WritePlace};

use crate::guard::{Shards, hold};
use crate::index::value_pages::{HeldChain, NO_VALUE_PAGE, ValuePages};
use crate::pool::{ask_huge_pages, touched_capacity};
use crate::segment::segment_tail::SegmentNumber;

/// Words of one page
pub const PAGE_WORDS: usize = 1024;
/// The span of an entry that came with no held value
pub const NO_HELD: HeldRef = HeldRef { chain: HeldChain::NONE, slot: u32::MAX };

/// Where a leaf holds an entry's value: its chain and the entry's slot in it
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeldRef {
    pub chain: HeldChain,
    pub slot: u32,
}
/// Parts of an index, one for each first byte of a key
pub const PARTS: usize = 256;
/// Words at the head of a page: the count, the next leaf, the previous leaf, and a leaf's held values: its chain's first page and
/// that page's generation
pub const HEAD_WORDS: usize = 5;
/// The head word of a leaf that names its value chain's first page
const CHAIN_WORD: usize = 3;
/// The head word of a leaf that keeps its value chain's generation, with LIVE_CHAIN set while it names one
const GENERATION_WORD: usize = 4;
/// The bit of a generation word that says the leaf names a chain, so a page never set up for values names none
const LIVE_CHAIN: u32 = 1 << 31;
/// Words of an entry after its key: the place's two halves, the segment, the offset and the length
pub const ENTRY_TAIL_WORDS: usize = 5;
/// The widest key an index holds
pub const MOST_KEY_BYTES: usize = 64;
/// Words of the widest key
pub const MOST_KEY_WORDS: usize = MOST_KEY_BYTES / size_of::<u32>();
/// Deepest a tree can get. The widest keys give inner pages a fan-out of 60, so this reaches far past any pool
pub const MAX_DEPTH: usize = 8;
/// The page a missing link points at
pub const NO_PAGE: u32 = u32::MAX;
/// The bit of a place's high word that marks a grave
pub const GRAVE_BIT: u32 = 1 << 31;
/// An index with no mark serves every row from its entries
pub const NO_MARK: u64 = u64::MAX;
/// Leaves fill this many thirds on average under random upserts, which sizes the pool from a row count
pub const FILL_THIRDS: usize = 2;
/// Most rows one pass merges into a leaf
pub const MERGE_ROWS: usize = 256;
/// Rows an upsert of many goes through under one hold of a part's lock
pub const HOLD_ROWS: usize = 256;
/// Most entries one scan call looks at, graves among them, so a run of graves holds a part's lock this long and no more
pub const MOST_LOOKED: usize = 256;

/// Where a key's newest sealed row sits
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct KeyEntry {
    pub place: WritePlace,
    pub segment: SegmentNumber,
    pub offset: FileOffset,
    pub length: ByteCount,
    pub is_grave: bool,
}

/// A key packed as big-endian words, padded with zeros past its width
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PackedKey {
    pub words: [u32; MOST_KEY_WORDS],
}

/// The places under which an upsert drops graves
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Floors {
    /// An incoming grave under this drops its key's entry, since every older row of its key is in the index
    pub drops: WritePlace,
    /// A grave entry under this leaves a full leaf before it splits, since every row older than it is in the index
    pub purges: WritePlace,
}

impl Floors {
    /// Use one place for both floors
    pub const fn at(place: WritePlace) -> Self {
        Self { drops: place, purges: place }
    }
}

/// What an upsert did
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Upserted {
    /// The key had no entry and has one now
    Added,
    /// The row was newer than the key's entry and took its place
    Replaced,
    /// The key's entry was as new or newer, and stays
    Kept,
    /// A grave under the drop floor has nothing older left to hide, so the key has no entry now
    Dropped,
    /// The pool had no page for the row, so the index marked its segment
    NoRoom,
}

/// Where a find left an entry: its part, leaf and slot, and the part's version then
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeafAt {
    pub part: u32,
    pub leaf: u32,
    pub index: u32,
    pub version: u64,
}

impl LeafAt {
    /// No spot noted, so a read of the value descends
    pub const NONE: LeafAt = LeafAt { part: 0, leaf: NO_PAGE, index: 0, version: 0 };
}

/// The inner pages a descent passed through, and the child it took in each
#[derive(Clone, Copy, Debug)]
pub struct PagePath {
    pub pages: [u32; MAX_DEPTH],
    pub children: [u32; MAX_DEPTH],
    pub depth: usize,
}

/// One part's tree
#[derive(Clone, Copy, Debug)]
pub struct PartTree {
    pub root: u32,
    /// Levels of the tree, the leaves among them. An empty part has none
    pub levels: usize,
    pub first_leaf: u32,
    pub last_leaf: u32,
    pub entries: u64,
    /// Moves at every change to the part, so a reader that noted where an entry sat can tell the spot still holds it
    pub version: u64,
}

/// The pool's unused pages, chained through each page's next word
#[derive(Debug)]
pub struct FreePages {
    pub head: u32,
    pub count: u32,
}

/// Where a scan starts
#[derive(Clone, Copy, Debug)]
pub enum ScanFrom<'key> {
    /// The first key of the column, or the last on a descending scan
    Edge,
    /// A key, and whether the key itself is in
    Key { key: &'key [u8], is_inclusive: bool },
}

/// A batch of live entries a scan copied out under a part's lock, in scan order
#[derive(Clone, Debug)]
pub struct KeyBatch {
    pub keys: Vec<u8>,
    pub entries: Vec<KeyEntry>,
    /// Where each entry's leaf held its value when the batch was copied, NO_HELD for none. The value stays in its page, and a reader
    /// copies it once into its own buffer
    pub held: Vec<HeldRef>,
    /// The last key a call looked at, which the next call starts after
    resume: [u8; MOST_KEY_BYTES],
    /// The batch takes graves too, as a scan does: a grave hides an older open row of its key
    pub is_with_graves: bool,
    /// The batch takes the values the leaves hold beside their entries, which a walk for keys alone leaves
    pub is_with_values: bool,
    width: usize,
}

/// A column's sorted keys
#[derive(Debug)]
pub struct SortedKeys {
    width: usize,
    key_words: usize,
    leaf_stride: usize,
    leaf_capacity: usize,
    inner_stride: usize,
    inner_capacity: usize,
    words: Vec<AtomicU32>,
    free: Mutex<FreePages>,
    parts: Shards<PartTree>,
    /// Each part's entry count, set under its lock, so a scan passes an empty part with no lock taken
    filled: Vec<AtomicU64>,
    entries: AtomicU64,
    mark: AtomicU64,
    past_capacity: AtomicU64,
    /// Goes up each time open rows go in ahead of their seal, before they leave the open counts
    entered: AtomicU64,
    /// Values the leaves keep, from one pool every sorted index of the store shares, sized at open
    pub values: Arc<ValuePages>,
    /// This index counts the shared pool's memory and fills, so the store counts them once
    owns_values: bool,
    /// This index's place among the pool's sharers, whose floor of pages it may always take
    sharer: usize,
    /// The rows the column was told to hold, which sized the pool
    told_rows: RowCount,
}

impl PackedKey {
    /// Pack a key's bytes into words
    pub fn new(key: &[u8]) -> Self {
        let mut words = [0; MOST_KEY_WORDS];
        for (word, bytes) in words.iter_mut().zip(key.chunks(size_of::<u32>())) {
            let mut padded = [0; size_of::<u32>()];
            padded[..bytes.len()].copy_from_slice(bytes);
            *word = u32::from_be_bytes(padded);
        }
        Self { words }
    }
}

impl PagePath {
    /// Start an empty path
    pub fn new() -> Self {
        Self {
            pages: [NO_PAGE; MAX_DEPTH],
            children: [0; MAX_DEPTH],
            depth: 0,
        }
    }
}

impl Default for PagePath {
    fn default() -> Self {
        Self::new()
    }
}

impl PartTree {
    /// Start a part with no tree
    pub fn empty() -> Self {
        Self {
            root: NO_PAGE,
            levels: 0,
            first_leaf: NO_PAGE,
            last_leaf: NO_PAGE,
            entries: 0,
            version: 0,
        }
    }
}

impl KeyBatch {
    /// Allocate a batch with room for this many entries of keys this wide, every page of it written, since a scan fills it later
    pub fn new(width: usize, most: usize) -> Self {
        Self {
            keys: touched_capacity(width * most, 0),
            entries: touched_capacity(most, KeyEntry::default()),
            held: touched_capacity(most, NO_HELD),
            resume: [0; MOST_KEY_BYTES],
            is_with_graves: false,
            is_with_values: true,
            width,
        }
    }

    /// Read the width of the batch's keys
    pub fn width(&self) -> usize {
        self.width
    }

    /// Read the heap the batch holds
    pub fn heap_bytes(&self) -> usize {
        self.keys.capacity() + self.entries.capacity() * size_of::<KeyEntry>() + self.held.capacity() * size_of::<HeldRef>()
    }

    /// Empty the batch for keys of another width, which its room holds
    pub fn set_width(&mut self, width: usize) {
        self.clear();
        self.width = width;
    }

    /// Read the key a scan that isn't done goes on after
    pub fn resume_key(&self) -> &[u8] {
        &self.resume[..self.width]
    }

    /// Count the batch's entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check the batch has no entries
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Read one entry's key
    pub fn key(&self, index: usize) -> &[u8] {
        &self.keys[index * self.width..(index + 1) * self.width]
    }

    /// Read where an entry's leaf held its value when the batch was copied
    pub fn held_value(&self, index: usize) -> Option<HeldRef> {
        self.held.get(index).copied().filter(|held| *held != NO_HELD)
    }

    /// Empty the batch and keep its room
    pub fn clear(&mut self) {
        self.keys.clear();
        self.entries.clear();
        self.held.clear();
    }
}

impl SortedKeys {
    /// Allocate and touch an index with room for these rows of keys this wide
    pub fn new(width: usize, rows: RowCount) -> Self {
        assert!((1..=MOST_KEY_BYTES).contains(&width), "a sorted index holds keys of 1 to {MOST_KEY_BYTES} bytes");
        let key_words = width.div_ceil(size_of::<u32>());
        let leaf_stride = key_words + ENTRY_TAIL_WORDS;
        let inner_stride = key_words + 1;
        let leaf_capacity = (PAGE_WORDS - HEAD_WORDS) / leaf_stride;
        let inner_capacity = (PAGE_WORDS - HEAD_WORDS) / inner_stride;
        let pages = planned_pages(width, rows);
        Self {
            width,
            key_words,
            leaf_stride,
            leaf_capacity,
            inner_stride,
            inner_capacity,
            words: touched_words(pages as usize * PAGE_WORDS),
            free: Mutex::new(chain_pages(pages)),
            parts: Shards((0..PARTS).map(|_part| std::sync::RwLock::new(PartTree::empty())).collect()),
            filled: (0..PARTS).map(|_part| AtomicU64::new(0)).collect(),
            entries: AtomicU64::new(0),
            mark: AtomicU64::new(NO_MARK),
            past_capacity: AtomicU64::new(0),
            entered: AtomicU64::new(0),
            values: Arc::new(ValuePages::new(0)),
            owns_values: false,
            sharer: 0,
            told_rows: rows,
        }
        .chained()
    }

    /// Write the free chain into the pages
    fn chained(self) -> Self {
        let count = hold(&self.free).count;
        for page in 0..count {
            let next = if page + 1 == count { NO_PAGE } else { page + 1 };
            self.store(page, 1, next);
        }
        self
    }

    /// Read the width of the index's keys
    pub fn width(&self) -> usize {
        self.width
    }

    /// Read how many entries a leaf holds
    pub fn leaf_capacity(&self) -> usize {
        self.leaf_capacity
    }

    /// Read the heap the index holds
    pub fn heap_bytes(&self) -> ByteCount {
        ByteCount((self.words.capacity() * size_of::<AtomicU32>()) as u64 + if self.owns_values { self.values.heap_bytes().0 } else { 0 })
    }

    /// Count the pages the pool has
    pub fn page_count(&self) -> u32 {
        (self.words.len() / PAGE_WORDS) as u32
    }

    /// Count the pages no part holds
    pub fn free_pages(&self) -> u32 {
        hold(&self.free).count
    }

    /// Read the levels of the deepest part, for tests
    pub fn deepest(&self) -> usize {
        (0..PARTS).map(|part| self.parts.read(part).levels).max().unwrap_or(0)
    }

    /// Count the entries, graves among them
    pub fn entry_count(&self) -> RowCount {
        RowCount(self.entries.load(Memory::Acquire))
    }

    /// Read the rows the column was told to hold
    pub fn told_rows(&self) -> RowCount {
        self.told_rows
    }

    /// Note open rows went in ahead of their seal. It comes before they leave the open counts, so a walk that sees them gone
    /// sees this and reads its index batch again
    pub fn note_entered(&self) {
        self.entered.fetch_add(1, Memory::AcqRel);
    }

    /// Read how many times open rows went in ahead of their seal
    pub fn entered(&self) -> u64 {
        self.entered.load(Memory::Acquire)
    }

    /// Read how many rows found no room
    pub fn past_capacity(&self) -> RowCount {
        RowCount(self.past_capacity.load(Memory::Acquire))
    }

    /// Read the segment a read's footer search starts from, if rows have gone past the index
    pub fn mark(&self) -> Option<SegmentNumber> {
        match self.mark.load(Memory::Acquire) {
            NO_MARK => None,
            mark => Some(SegmentNumber(mark)),
        }
    }

    /// Mark the index at a segment whose rows it couldn't take, so reads from that segment on ask the footers until a refill
    pub fn mark_from(&self, segment: SegmentNumber) {
        self.mark.fetch_min(segment.0, Memory::AcqRel);
    }

    /// Check a quarter of the pool is free, so rows past the mark can have entries again
    pub fn has_room(&self) -> bool {
        self.free_pages() * 4 >= self.page_count()
    }

    /// Clear a mark a refill from it made good, and say whether it went. A row that found no room since the refill began keeps it
    pub fn clear_mark(&self, mark: SegmentNumber, missed: RowCount) -> bool {
        if self.mark.compare_exchange(mark.0, NO_MARK, Memory::AcqRel, Memory::Acquire).is_err() {
            return false;
        }
        if self.past_capacity.load(Memory::Acquire) != missed.0 {
            self.mark.fetch_min(mark.0, Memory::AcqRel);
            return false;
        }
        self.past_capacity.fetch_sub(missed.0, Memory::AcqRel);
        true
    }

    /// Find the part a key sits in
    pub fn part_of(&self, key: &[u8]) -> usize {
        key.first().map_or(0, |byte| usize::from(*byte))
    }

    /// Give a sealed row an entry when it's newer than the key's. A grave under the drop floor takes the key's entry out, and a full
    /// leaf sheds its graves under the purge floor before it splits
    pub fn upsert(&self, key: &[u8], entry: KeyEntry, floors: Floors) -> Upserted {
        assert_eq!(key.len(), self.width, "a sorted index holds keys of one width");
        let part = self.part_of(key);
        let mut tree = self.write_part(part);
        let upserted = self.upsert_in(&mut tree, &PackedKey::new(key), entry, floors);
        self.filled[part].store(tree.entries, Memory::Release);
        drop(tree);
        if upserted == Upserted::NoRoom {
            self.past_capacity.fetch_add(1, Memory::AcqRel);
            self.mark.fetch_min(entry.segment.0, Memory::AcqRel);
        }
        upserted
    }

    /// Give sealed rows in ascending key order their entries, taking each part's lock once for the rows in it. The rows bound
    /// for one leaf go in with one pass over the leaf from its end, so a seal moves each leaf's words once
    pub fn upsert_many(&self, keys: &[u8], entries: &[KeyEntry], floors: Floors, mut done: impl FnMut(usize, Upserted)) {
        assert_eq!(keys.len(), entries.len() * self.width, "a sorted index holds keys of one width");
        let mut at = 0;
        while at < entries.len() {
            let part = self.part_of(&keys[at * self.width..]);
            let mut tree = self.write_part(part);
            // a reader of the part waits for one short hold at most
            let held_from = at;
            while at < entries.len() && at - held_from < HOLD_ROWS && self.part_of(&keys[at * self.width..]) == part {
                let merged = self.merge_into_leaf(&mut tree, keys, entries, at, floors.drops, &mut done);
                if merged > 0 {
                    at += merged;
                    continue;
                }
                let key = PackedKey::new(&keys[at * self.width..(at + 1) * self.width]);
                let upserted = self.upsert_in(&mut tree, &key, entries[at], floors);
                if upserted == Upserted::NoRoom {
                    self.past_capacity.fetch_add(1, Memory::AcqRel);
                    self.mark.fetch_min(entries[at].segment.0, Memory::AcqRel);
                }
                done(at, upserted);
                at += 1;
            }
            self.filled[part].store(tree.entries, Memory::Release);
        }
    }

    /// Merge the rows from one on that belong in one leaf into it in one pass, and say how many went in. Nothing goes in, and the
    /// rows take the one-row path, when the leaf lacks room for them, a row would take an entry out, or the part has no tree
    fn merge_into_leaf(
        &self,
        tree: &mut PartTree,
        keys: &[u8],
        entries: &[KeyEntry],
        first: usize,
        floor: WritePlace,
        done: &mut impl FnMut(usize, Upserted),
    ) -> usize {
        if tree.levels == 0 {
            return 0;
        }
        let key_of = |row: usize| PackedKey::new(&keys[row * self.width..(row + 1) * self.width]);
        let first_key = key_of(first);
        let mut path = PagePath::new();
        let leaf = self.descend(tree, &first_key, &mut path);
        let bound = self.leaf_bound(&path);
        let part = first_byte(&first_key);
        let count = self.count(leaf);
        let room = self.leaf_capacity - count;
        // each row's slot in the leaf as it stands, and whether it's the key there
        let mut plan = [(0u32, false); MERGE_ROWS];
        let mut inserts = 0;
        let mut rows = 0;
        let mut previous: Option<PackedKey> = None;
        while first + rows < entries.len() && rows < MERGE_ROWS {
            let row = first + rows;
            let key = key_of(row);
            let is_in_leaf = first_byte(&key) == part && bound.is_none_or(|bound| self.compare_keys(&key, &bound) == Ordering::Less);
            // a key twice in one run would take two slots, so its second row starts the next run
            if !is_in_leaf || previous.is_some_and(|previous| previous == key) {
                break;
            }
            let entry = entries[row];
            if u32::try_from(entry.segment.0).is_err() || u32::try_from(entry.offset.0).is_err() || u32::try_from(entry.length.0).is_err() {
                break;
            }
            let (index, is_found) = self.leaf_search(leaf, &key);
            let is_dropped = entry.is_grave && entry.place < floor;
            if is_found && is_dropped && entry.place.0 > self.entry_place(leaf, index) {
                break;
            }
            let is_insert = !is_found && !is_dropped;
            if is_insert && inserts == room {
                break;
            }
            inserts += usize::from(is_insert);
            plan[rows] = (index as u32, is_found);
            previous = Some(key);
            rows += 1;
        }
        if rows == 0 {
            return 0;
        }
        // replaces first, at slots the inserts haven't moved yet
        for (offset, &(index, is_found)) in plan[..rows].iter().enumerate() {
            let row = first + offset;
            let entry = entries[row];
            let index = index as usize;
            if !is_found {
                continue;
            }
            if entry.place.0 <= self.entry_place(leaf, index) {
                done(row, Upserted::Kept);
                continue;
            }
            self.write_tail(leaf, index, &EntryWords::of(&entry));
            done(row, Upserted::Replaced);
        }
        // then the inserts from the last, each run of old entries moved once
        let mut end = count;
        let mut write = count + inserts;
        for offset in (0..rows).rev() {
            let (index, is_found) = plan[offset];
            let row = first + offset;
            let entry = entries[row];
            if is_found {
                continue;
            }
            if entry.is_grave && entry.place < floor {
                done(row, Upserted::Dropped);
                continue;
            }
            let index = index as usize;
            let moved = end - index;
            write -= moved;
            self.copy_within(leaf, self.leaf_entry(index), self.leaf_entry(write), moved * self.leaf_stride);
            write -= 1;
            let base = self.leaf_entry(write);
            for (at, word) in key_of(row).words[..self.key_words].iter().enumerate() {
                self.store(leaf, base + at, *word);
            }
            self.write_tail(leaf, write, &EntryWords::of(&entry));
            end = index;
            done(row, Upserted::Added);
        }
        self.set_count(leaf, count + inserts);
        tree.entries += inserts as u64;
        self.entries.fetch_add(inserts as u64, Memory::AcqRel);
        rows
    }

    /// Read the first key past a leaf a path reached, from the nearest separator after it, or nothing for a part's last leaf
    fn leaf_bound(&self, path: &PagePath) -> Option<PackedKey> {
        (0..path.depth).rev().find_map(|depth| {
            let (page, child) = (path.pages[depth], path.children[depth] as usize + 1);
            (child < self.count(page)).then(|| self.key_at(page, self.inner_entry(child)))
        })
    }

    /// Upsert one row into a part's tree
    fn upsert_in(&self, tree: &mut PartTree, key: &PackedKey, entry: KeyEntry, floors: Floors) -> Upserted {
        let is_dropped = entry.is_grave && entry.place < floors.drops;
        let (Ok(segment), Ok(offset), Ok(length)) =
            (u32::try_from(entry.segment.0), u32::try_from(entry.offset.0), u32::try_from(entry.length.0))
        else {
            return Upserted::NoRoom;
        };
        if tree.levels == 0 {
            if is_dropped {
                return Upserted::Dropped;
            }
            let Some(leaf) = self.take_page() else {
                return Upserted::NoRoom;
            };
            self.init_page(leaf);
            *tree = PartTree {
                root: leaf,
                levels: 1,
                first_leaf: leaf,
                last_leaf: leaf,
                entries: 0,
                version: tree.version,
            };
        }
        let words = EntryWords { place: entry.place.0, is_grave: entry.is_grave, segment, offset, length };
        let mut path = PagePath::new();
        let mut leaf = self.descend(tree, key, &mut path);
        let (mut index, is_found) = self.leaf_search(leaf, key);
        if is_found {
            if entry.place.0 <= self.entry_place(leaf, index) {
                return Upserted::Kept;
            }
            if is_dropped {
                self.remove_at(tree, &path, leaf, index);
                return Upserted::Dropped;
            }
            self.write_tail(leaf, index, &words);
            return Upserted::Replaced;
        }
        if is_dropped {
            return Upserted::Dropped;
        }
        if self.count(leaf) == self.leaf_capacity {
            self.purge_graves(tree, leaf, floors.purges);
            if self.count(leaf) == self.leaf_capacity {
                if !self.split_leaf(tree, &path, leaf, key) {
                    return Upserted::NoRoom;
                }
                path = PagePath::new();
                leaf = self.descend(tree, key, &mut path);
            }
            (index, _) = self.leaf_search(leaf, key);
        }
        self.insert_at(leaf, index, key, &words);
        tree.entries += 1;
        self.entries.fetch_add(1, Memory::AcqRel);
        Upserted::Added
    }

    /// Point a key's entry at the row a merge wrote, when the entry is that row, and say whether it moved
    pub fn move_entry(&self, key: &[u8], place: WritePlace, segment: SegmentNumber, offset: FileOffset) -> bool {
        let (Ok(segment), Ok(offset)) = (u32::try_from(segment.0), u32::try_from(offset.0)) else {
            return false;
        };
        let tree = self.write_part(self.part_of(key));
        if tree.levels == 0 {
            return false;
        }
        let packed = PackedKey::new(key);
        let leaf = self.descend(&tree, &packed, &mut PagePath::new());
        let (index, is_found) = self.leaf_search(leaf, &packed);
        if !is_found || self.entry_place(leaf, index) != place.0 {
            return false;
        }
        let base = self.leaf_entry(index) + self.key_words;
        self.store(leaf, base + 2, segment);
        self.store(leaf, base + 3, offset);
        true
    }

    /// Remove the entries of keys from a start up to an end, the end out, whose rows are older than a place. A cover's apply calls
    /// this, so no entry outlives the segment it points into. Say how many went
    pub fn remove_range(&self, start: &[u8], end: Option<&[u8]>, below: WritePlace) -> u64 {
        assert!(start.len() <= self.width && end.is_none_or(|end| end.len() <= self.width), "a range's keys fit the index's width");
        let end = end.map(PackedKey::new);
        let last_part = end.map_or(PARTS - 1, |end| (end.words[0] >> 24) as usize);
        let mut removed = 0;
        let mut from = PackedKey::new(start);
        for part in first_byte(&from)..=last_part {
            if part != first_byte(&from) {
                from = PackedKey { words: [0; MOST_KEY_WORDS] };
                from.words[0] = (part as u32) << 24;
            }
            let mut tree = self.write_part(part);
            removed += self.remove_range_in(&mut tree, from, end.as_ref(), below);
            self.filled[part].store(tree.entries, Memory::Release);
        }
        self.entries.fetch_sub(removed, Memory::AcqRel);
        removed
    }

    /// Remove the entries a cover hides: keys from its start up to its end, the end out, with rows older than its place. A start
    /// wider than the keys can't be packed without taking in a key it leaves out, so those entries stay, and a read's own cover
    /// check hides them. An end wider than the keys cuts to their width, which leaves a few in. Say how many went
    pub fn remove_covered(&self, start: &[u8], end: &[u8], place: WritePlace) -> u64 {
        if start.len() > self.width {
            return 0;
        }
        self.remove_range(start, Some(&end[..end.len().min(self.width)]), place)
    }

    /// Remove a range's older entries from one part, a leaf a descent
    fn remove_range_in(&self, tree: &mut PartTree, mut from: PackedKey, end: Option<&PackedKey>, below: WritePlace) -> u64 {
        let mut removed = 0;
        while tree.levels > 0 {
            let mut path = PagePath::new();
            let leaf = self.descend(tree, &from, &mut path);
            let (start, _) = self.leaf_search(leaf, &from);
            let count = self.count(leaf);
            let next = self.next(leaf);
            let mut kept = start;
            let mut is_past_end = false;
            for index in start..count {
                if end.is_some_and(|end| self.compare_at(leaf, self.leaf_entry(index), end) != Ordering::Less) {
                    is_past_end = true;
                }
                if !is_past_end && self.entry_place(leaf, index) < below.0 {
                    removed += 1;
                    continue;
                }
                if kept != index {
                    self.copy_within(leaf, self.leaf_entry(index), self.leaf_entry(kept), self.leaf_stride);
                }
                kept += 1;
            }
            let went = (count - kept) as u64;
            tree.entries -= went;
            self.set_count(leaf, kept);
            if is_past_end || next == NO_PAGE {
                if kept == 0 {
                    self.remove_leaf(tree, &path, leaf);
                }
                return removed;
            }
            from = self.key_at(next, self.leaf_entry(0));
            if kept == 0 {
                self.remove_leaf(tree, &path, leaf);
            }
        }
        removed
    }

    /// Say whether a scan takes the stored key a start packs to. A short start packs with zeros to a key that sorts above it, so
    /// a scan going up takes it and one going down doesn't. A long start cuts to a key that sorts below it, the other way round
    pub fn start_inclusion(&self, start: &[u8], is_inclusive: bool, is_ascending: bool) -> bool {
        match start.len().cmp(&self.width) {
            Ordering::Less => is_ascending,
            Ordering::Greater => !is_ascending,
            Ordering::Equal => is_inclusive,
        }
    }

    /// Find a key's entry, a grave or a value
    pub fn find(&self, key: &[u8]) -> Option<KeyEntry> {
        self.find_at(key).map(|(entry, _at)| entry)
    }

    /// Find a key's entry and note where it sits, so a read of the value it holds takes no second descent
    pub fn find_at(&self, key: &[u8]) -> Option<(KeyEntry, LeafAt)> {
        let part = self.part_of(key);
        let tree = self.parts.read(part);
        if tree.levels == 0 {
            return None;
        }
        let packed = PackedKey::new(key);
        let leaf = self.descend(&tree, &packed, &mut PagePath::new());
        let (index, is_found) = self.leaf_search(leaf, &packed);
        let at = LeafAt { part: part as u32, leaf, index: index as u32, version: tree.version };
        is_found.then(|| (self.read_entry(leaf, index), at))
    }

    /// Copy the value an entry holds onto the end of `out` from where a find noted it, when the part hasn't changed since and the
    /// entry still names this row. A part that changed, or a spot no find noted, takes the descent
    pub fn held_value_at(&self, at: LeafAt, key: &[u8], row: (WritePlace, SegmentNumber, FileOffset), out: &mut Vec<u8>) -> bool {
        if at.leaf == NO_PAGE || self.values.page_count() == 0 {
            return self.held_value(key, row, out);
        }
        let tree = self.parts.read(at.part as usize);
        if tree.version != at.version {
            drop(tree);
            return self.held_value(key, row, out);
        }
        let (leaf, index) = (at.leaf, at.index as usize);
        self.names_row(leaf, index, row) && self.values.read(self.chain_of(leaf), index, out)
    }

    /// Take a part's lock to change it, and move its version so a noted spot is read again
    fn write_part(&self, part: usize) -> RwLockWriteGuard<'_, PartTree> {
        let mut tree = self.parts.write(part);
        tree.version = tree.version.wrapping_add(1);
        tree
    }

    /// Take a part's lock to change it if no one holds it, moving its version as a write does
    fn try_write_part(&self, part: usize) -> Option<RwLockWriteGuard<'_, PartTree>> {
        self.parts.try_write(part).map(|mut tree| {
            tree.version = tree.version.wrapping_add(1);
            tree
        })
    }

    /// Give the leaves the store's pool of value pages to keep values in, which one index owns for the counts
    pub fn holding_values(mut self, pool: Arc<ValuePages>, (sharer, owns): (usize, bool)) -> Self {
        self.values = pool;
        self.sharer = sharer;
        self.owns_values = owns;
        self
    }

    /// Check this index counts the shared pool's memory and fills
    pub fn owns_values(&self) -> bool {
        self.owns_values
    }

    /// Copy a value a batch noted held onto the end of `out`, or say the leaf let it go since
    pub fn read_held(&self, held: HeldRef, out: &mut Vec<u8>) -> bool {
        self.values.read(held.chain, held.slot as usize, out)
    }

    /// Copy the value a key's entry holds onto the end of `out`, when the entry still names this row and its leaf holds the value
    pub fn held_value(&self, key: &[u8], row: (WritePlace, SegmentNumber, FileOffset), out: &mut Vec<u8>) -> bool {
        if self.values.page_count() == 0 || key.len() > self.width {
            return false;
        }
        let tree = self.parts.read(self.part_of(key));
        if tree.levels == 0 {
            return false;
        }
        let packed = PackedKey::new(key);
        let leaf = self.descend(&tree, &packed, &mut PagePath::new());
        let (index, is_found) = self.leaf_search(leaf, &packed);
        is_found && self.names_row(leaf, index, row) && self.values.read(self.chain_of(leaf), index, out)
    }

    /// Keep a value a read checked beside its key's entry, when the entry still names the row it was read from, and the pool has room
    pub fn hold_value(&self, key: &[u8], row: (WritePlace, SegmentNumber, FileOffset), value: &[u8]) -> bool {
        self.hold_values([(key, row, value)].into_iter()) > 0
    }

    /// Keep values a read checked, under one hold of each part's lock for the run of keys in it. A part whose lock another caller
    /// has takes none: the rows were answered already, and a fill never waits. Say how many went in
    pub fn hold_values<'value>(&self, items: impl Iterator<Item = (&'value [u8], (WritePlace, SegmentNumber, FileOffset), &'value [u8])>) -> u64 {
        if self.values.page_count() == 0 || !self.values.could_take(self.sharer) {
            return 0;
        }
        let mut held = 0;
        let mut filled = 0;
        let mut current: Option<(usize, Option<RwLockWriteGuard<'_, PartTree>>)> = None;
        // a batch's keys run in order, so after one descent each key is in the leaf the last one was in, or the next
        let mut cursor = NO_PAGE;
        for (key, row, value) in items {
            if key.len() > self.width {
                continue;
            }
            let part = self.part_of(key);
            if current.as_ref().is_none_or(|(at, _guard)| *at != part) {
                drop(current.take());
                current = Some((part, self.try_write_part(part)));
                cursor = NO_PAGE;
            }
            let Some((_part, Some(tree))) = current.as_ref() else {
                continue;
            };
            if tree.levels == 0 {
                continue;
            }
            let packed = PackedKey::new(key);
            let Some((leaf, index)) = self.leaf_near(tree, cursor, &packed) else {
                continue;
            };
            cursor = leaf;
            if !self.names_row(leaf, index, row) {
                continue;
            }
            if let Some((chain, is_new)) = self.values.fill((self.sharer, leaf), self.chain_of(leaf), (index, self.leaf_capacity), value) {
                self.set_chain(leaf, chain);
                held += 1;
                filled += u64::from(is_new);
            }
        }
        drop(current);
        self.values.note_fills(filled);
        held
    }

    /// Find a key's leaf and place from the leaf the last key of a run was in: that leaf, then its next, then a descent. Nothing
    /// when the key has no entry. The caller holds the part's lock
    fn leaf_near(&self, tree: &PartTree, cursor: u32, packed: &PackedKey) -> Option<(u32, usize)> {
        if cursor != NO_PAGE {
            for leaf in [cursor, self.next(cursor)] {
                if leaf == NO_PAGE {
                    break;
                }
                let (index, is_found) = self.leaf_search(leaf, packed);
                if is_found {
                    return Some((leaf, index));
                }
                // a key that sorts inside this leaf and isn't there has no entry, and one before its first may sit in an earlier leaf
                if index == 0 {
                    break;
                }
                if index < self.count(leaf) {
                    return None;
                }
            }
        }
        let leaf = self.descend(tree, packed, &mut PagePath::new());
        let (index, is_found) = self.leaf_search(leaf, packed);
        is_found.then_some((leaf, index))
    }

    /// Check an entry still names a row: its place, segment and offset
    fn names_row(&self, leaf: u32, index: usize, (place, segment, offset): (WritePlace, SegmentNumber, FileOffset)) -> bool {
        let entry = self.read_entry(leaf, index);
        !entry.is_grave && entry.place == place && entry.segment == segment && entry.offset == offset
    }

    /// Read the chain a leaf's head names
    fn chain_of(&self, leaf: u32) -> HeldChain {
        let generation = self.load(leaf, GENERATION_WORD);
        if generation & LIVE_CHAIN == 0 {
            return HeldChain::NONE;
        }
        HeldChain { first: self.load(leaf, CHAIN_WORD), generation: generation & !LIVE_CHAIN }
    }

    /// Name a chain in a leaf's head, or none
    fn set_chain(&self, leaf: u32, chain: HeldChain) {
        if chain == HeldChain::NONE {
            self.store(leaf, GENERATION_WORD, 0);
            self.store(leaf, CHAIN_WORD, NO_VALUE_PAGE);
            return;
        }
        self.store(leaf, CHAIN_WORD, chain.first);
        self.store(leaf, GENERATION_WORD, chain.generation | LIVE_CHAIN);
    }

    /// Give a leaf's held values back, as any change to its entries does
    fn drop_values(&self, page: u32) {
        let chain = self.chain_of(page);
        if chain != HeldChain::NONE {
            self.values.drop_chain(self.sharer, chain);
            self.set_chain(page, HeldChain::NONE);
        }
    }

    /// Copy the live entries after a start into a batch until it holds this many, in ascending key order, and say whether the
    /// column's keys ran out. A call looks at `MOST_LOOKED` entries at most, graves among them, and a scan that isn't done goes on
    /// after the batch's resume key
    pub fn read_after(&self, from: ScanFrom, most: usize, batch: &mut KeyBatch) -> bool {
        assert!(most > 0, "a scan reads one entry a call at least");
        batch.clear();
        let (first_part, mut start) = match from {
            ScanFrom::Edge => (0, None),
            ScanFrom::Key { key, is_inclusive } => (self.part_of(key), Some((PackedKey::new(key), self.start_inclusion(key, is_inclusive, true)))),
        };
        let mut looked = 0;
        for part in first_part..PARTS {
            if self.filled[part].load(Memory::Acquire) == 0 {
                start = None;
                continue;
            }
            let tree = self.parts.read(part);
            if tree.levels == 0 {
                start = None;
                continue;
            }
            let (mut leaf, mut index) = match start.take() {
                None => (tree.first_leaf, 0),
                Some((key, is_inclusive)) => {
                    let leaf = self.descend(&tree, &key, &mut PagePath::new());
                    let (index, is_found) = self.leaf_search(leaf, &key);
                    (leaf, index + usize::from(is_found && !is_inclusive))
                }
            };
            while leaf != NO_PAGE {
                let count = self.count(leaf);
                while index < count {
                    self.copy_live(leaf, index, batch);
                    looked += 1;
                    if batch.len() == most || looked == MOST_LOOKED {
                        self.unpack_key(leaf, self.leaf_entry(index), &mut batch.resume);
                        return false;
                    }
                    index += 1;
                }
                (leaf, index) = (self.next(leaf), 0);
            }
        }
        true
    }

    /// Copy the live entries before a start into a batch until it holds this many, in descending key order, and say whether the
    /// column's keys ran out. A call looks at `MOST_LOOKED` entries at most, as `read_after` does
    pub fn read_before(&self, from: ScanFrom, most: usize, batch: &mut KeyBatch) -> bool {
        assert!(most > 0, "a scan reads one entry a call at least");
        batch.clear();
        let (first_part, mut start) = match from {
            ScanFrom::Edge => (PARTS - 1, None),
            ScanFrom::Key { key, is_inclusive } => (self.part_of(key), Some((PackedKey::new(key), self.start_inclusion(key, is_inclusive, false)))),
        };
        let mut looked = 0;
        for part in (0..=first_part).rev() {
            if self.filled[part].load(Memory::Acquire) == 0 {
                start = None;
                continue;
            }
            let tree = self.parts.read(part);
            if tree.levels == 0 {
                start = None;
                continue;
            }
            // the index is one past the next entry to copy
            let (mut leaf, mut index) = match start.take() {
                None => (tree.last_leaf, self.count(tree.last_leaf)),
                Some((key, is_inclusive)) => {
                    let leaf = self.descend(&tree, &key, &mut PagePath::new());
                    let (index, is_found) = self.leaf_search(leaf, &key);
                    (leaf, index + usize::from(is_found && is_inclusive))
                }
            };
            while leaf != NO_PAGE {
                while index > 0 {
                    index -= 1;
                    self.copy_live(leaf, index, batch);
                    looked += 1;
                    if batch.len() == most || looked == MOST_LOOKED {
                        self.unpack_key(leaf, self.leaf_entry(index), &mut batch.resume);
                        return false;
                    }
                }
                leaf = self.previous(leaf);
                if leaf != NO_PAGE {
                    index = self.count(leaf);
                }
            }
        }
        true
    }

    /// Count the live entries under a prefix a part's lock at a time, or nothing when one is newer than the point, since then the
    /// key's row at the point may be another
    pub fn count_live(&self, prefix: &[u8], point: WritePlace) -> Option<u64> {
        let mut key = [0; MOST_KEY_BYTES];
        let mut live = 0;
        let first_part = if prefix.is_empty() { 0 } else { self.part_of(prefix) };
        let mut start = (!prefix.is_empty()).then(|| PackedKey::new(prefix));
        for part in first_part..PARTS {
            if self.filled[part].load(Memory::Acquire) == 0 {
                start = None;
                continue;
            }
            let tree = self.parts.read(part);
            if tree.levels == 0 {
                start = None;
                continue;
            }
            let (mut leaf, mut index) = match start.take() {
                None => (tree.first_leaf, 0),
                Some(packed) => {
                    let leaf = self.descend(&tree, &packed, &mut PagePath::new());
                    (leaf, self.leaf_search(leaf, &packed).0)
                },
            };
            while leaf != NO_PAGE {
                for at in index..self.count(leaf) {
                    if !prefix.is_empty() {
                        self.unpack_key(leaf, self.leaf_entry(at), &mut key);
                        if !key[..self.width].starts_with(prefix) {
                            return Some(live);
                        }
                    }
                    let entry = self.read_entry(leaf, at);
                    if entry.place > point {
                        return None;
                    }
                    live += u64::from(!entry.is_grave);
                }
                (leaf, index) = (self.next(leaf), 0);
            }
        }
        Some(live)
    }

    /// Visit every entry of every part in key order, a part's lock at a time, for checks and counts
    pub fn visit(&self, mut visit: impl FnMut(&[u8], &KeyEntry)) {
        let mut key = [0; MOST_KEY_BYTES];
        for part in 0..PARTS {
            let tree = self.parts.read(part);
            let mut leaf = if tree.levels == 0 { NO_PAGE } else { tree.first_leaf };
            while leaf != NO_PAGE {
                for index in 0..self.count(leaf) {
                    self.unpack_key(leaf, self.leaf_entry(index), &mut key);
                    visit(&key[..self.width], &self.read_entry(leaf, index));
                }
                leaf = self.next(leaf);
            }
        }
    }

    /// Check every part's tree holds its keys in order with every link and count right, for tests
    pub fn check(&self) -> Result<(), String> {
        let mut total = 0;
        let mut pages = 0;
        for part in 0..PARTS {
            let tree = self.parts.read(part);
            if tree.levels == 0 {
                continue;
            }
            let mut leaves = Vec::new();
            pages += self.check_page(&tree, tree.root, tree.levels, None, None, &mut leaves)?;
            if leaves.first() != Some(&tree.first_leaf) || leaves.last() != Some(&tree.last_leaf) {
                return Err(format!("part {part}: first or last leaf is wrong"));
            }
            let mut previous = NO_PAGE;
            let mut entries = 0;
            for (at, &leaf) in leaves.iter().enumerate() {
                let next = leaves.get(at + 1).copied().unwrap_or(NO_PAGE);
                if self.previous(leaf) != previous || self.next(leaf) != next {
                    return Err(format!("part {part}: leaf {leaf} has broken links"));
                }
                if self.count(leaf) == 0 {
                    return Err(format!("part {part}: leaf {leaf} is empty"));
                }
                for index in 0..self.count(leaf) {
                    if first_byte(&self.key_at(leaf, self.leaf_entry(index))) != part {
                        return Err(format!("part {part}: a key of another part"));
                    }
                }
                entries += self.count(leaf) as u64;
                previous = leaf;
            }
            if self.filled[part].load(Memory::Acquire) != entries {
                return Err(format!("part {part}: its count beside the lock is {}", self.filled[part].load(Memory::Acquire)));
            }
            if entries != tree.entries {
                return Err(format!("part {part}: counts {} entries and holds {entries}", tree.entries));
            }
            total += entries;
        }
        if total != self.entries.load(Memory::Acquire) {
            return Err(format!("the index counts {} entries and holds {total}", self.entries.load(Memory::Acquire)));
        }
        if pages + self.free_pages() != self.page_count() {
            return Err(format!("{pages} pages in trees and {} free of {}", self.free_pages(), self.page_count()));
        }
        Ok(())
    }

    /// Check one page and the pages under it, and count them
    fn check_page(
        &self,
        tree: &PartTree,
        page: u32,
        levels: usize,
        low: Option<PackedKey>,
        high: Option<PackedKey>,
        leaves: &mut Vec<u32>,
    ) -> Result<u32, String> {
        let count = self.count(page);
        let in_bounds = |key: &PackedKey| {
            low.is_none_or(|low| self.compare_keys(key, &low) != Ordering::Less)
                && high.is_none_or(|high| self.compare_keys(key, &high) == Ordering::Less)
        };
        if levels == 1 {
            for index in 0..count {
                let key = self.key_at(page, self.leaf_entry(index));
                if !in_bounds(&key) {
                    return Err(format!("leaf {page}: key {index} is outside its separators"));
                }
                if index > 0 && self.compare_keys(&self.key_at(page, self.leaf_entry(index - 1)), &key) != Ordering::Less {
                    return Err(format!("leaf {page}: keys out of order at {index}"));
                }
            }
            leaves.push(page);
            return Ok(1);
        }
        if count == 0 || (page == tree.root && count < 2) {
            return Err(format!("inner page {page} holds {count} children"));
        }
        let mut pages = 1;
        for index in 0..count {
            let base = self.inner_entry(index);
            let separator = (index > 0).then(|| self.key_at(page, base));
            if let Some(separator) = separator
                && !in_bounds(&separator)
            {
                return Err(format!("inner page {page}: separator {index} is outside its bounds"));
            }
            let child_low = separator.or(low);
            let child_high = if index + 1 < count { Some(self.key_at(page, self.inner_entry(index + 1))) } else { high };
            let child = self.load(page, base + self.key_words);
            pages += self.check_page(tree, child, levels - 1, child_low, child_high, leaves)?;
        }
        Ok(pages)
    }

    /// Walk from a part's root to the leaf a key belongs in, noting the inner pages on the way
    fn descend(&self, tree: &PartTree, key: &PackedKey, path: &mut PagePath) -> u32 {
        let mut page = tree.root;
        for _level in 1..tree.levels {
            // the last child whose separator is at or before the key, and the first child takes every key before the second
            let (mut low, mut high) = (1, self.count(page));
            while low < high {
                let middle = (low + high) / 2;
                if self.compare_at(page, self.inner_entry(middle), key) == Ordering::Greater {
                    high = middle;
                } else {
                    low = middle + 1;
                }
            }
            let child = low - 1;
            path.pages[path.depth] = page;
            path.children[path.depth] = child as u32;
            path.depth += 1;
            page = self.load(page, self.inner_entry(child) + self.key_words);
        }
        page
    }

    /// Find where a key sits in a leaf, the first entry at or after it, and whether that entry is the key
    fn leaf_search(&self, leaf: u32, key: &PackedKey) -> (usize, bool) {
        let (mut low, mut high) = (0, self.count(leaf));
        while low < high {
            let middle = (low + high) / 2;
            match self.compare_at(leaf, self.leaf_entry(middle), key) {
                Ordering::Less => low = middle + 1,
                Ordering::Equal => return (middle, true),
                Ordering::Greater => high = middle,
            }
        }
        (low, false)
    }

    /// Split a full leaf in two and link the new one into the tree, and say whether the pool had the pages. A split at the end of a
    /// part's last leaf moves nothing, so keys that arrive in order fill their leaves whole
    fn split_leaf(&self, tree: &mut PartTree, path: &PagePath, leaf: u32, key: &PackedKey) -> bool {
        let mut pages = [NO_PAGE; MAX_DEPTH + 1];
        let needed = path.depth + 2;
        if !self.take_pages(&mut pages[..needed]) {
            return false;
        }
        let mut taken = needed;
        let mut take = || {
            taken -= 1;
            pages[taken]
        };
        let count = self.count(leaf);
        let is_append = leaf == tree.last_leaf && self.compare_at(leaf, self.leaf_entry(count - 1), key) == Ordering::Less;
        let new_leaf = take();
        self.init_page(new_leaf);
        let separator = if is_append {
            *key
        } else {
            let keep = count / 2;
            self.copy_between(leaf, self.leaf_entry(keep), new_leaf, self.leaf_entry(0), (count - keep) * self.leaf_stride);
            self.set_count(leaf, keep);
            self.set_count(new_leaf, count - keep);
            self.key_at(new_leaf, self.leaf_entry(0))
        };
        let next = self.next(leaf);
        self.set_next(new_leaf, next);
        self.set_previous(new_leaf, leaf);
        self.set_next(leaf, new_leaf);
        if next == NO_PAGE {
            tree.last_leaf = new_leaf;
        } else {
            self.set_previous(next, new_leaf);
        }
        self.insert_child(tree, path, path.depth, separator, new_leaf, &mut take);
        self.give_back(&pages[..taken]);
        true
    }

    /// Put a child after the one a path took at a level, splitting full inner pages up to a new root
    fn insert_child(&self, tree: &mut PartTree, path: &PagePath, depth: usize, separator: PackedKey, child: u32, take: &mut impl FnMut() -> u32) {
        if depth == 0 {
            let root = take();
            self.init_page(root);
            self.write_child(root, 0, &PackedKey { words: [0; MOST_KEY_WORDS] }, tree.root);
            self.write_child(root, 1, &separator, child);
            self.set_count(root, 2);
            tree.root = root;
            tree.levels += 1;
            return;
        }
        let parent = path.pages[depth - 1];
        let at = path.children[depth - 1] as usize + 1;
        let count = self.count(parent);
        if count < self.inner_capacity {
            self.open_inner(parent, at, count);
            self.write_child(parent, at, &separator, child);
            self.set_count(parent, count + 1);
            return;
        }
        let sibling = take();
        self.init_page(sibling);
        let keep = count / 2;
        self.copy_between(parent, self.inner_entry(keep), sibling, self.inner_entry(0), (count - keep) * self.inner_stride);
        self.set_count(parent, keep);
        self.set_count(sibling, count - keep);
        let (page, index) = if at <= keep { (parent, at) } else { (sibling, at - keep) };
        let page_count = self.count(page);
        self.open_inner(page, index, page_count);
        self.write_child(page, index, &separator, child);
        self.set_count(page, page_count + 1);
        let up = self.key_at(sibling, self.inner_entry(0));
        self.insert_child(tree, path, depth - 1, up, sibling, take);
    }

    /// Take an entry out of a leaf, and the leaf out of the tree when it empties
    fn remove_at(&self, tree: &mut PartTree, path: &PagePath, leaf: u32, index: usize) {
        let count = self.count(leaf);
        self.copy_within(leaf, self.leaf_entry(index + 1), self.leaf_entry(index), (count - index - 1) * self.leaf_stride);
        self.set_count(leaf, count - 1);
        tree.entries -= 1;
        self.entries.fetch_sub(1, Memory::AcqRel);
        if count == 1 {
            self.remove_leaf(tree, path, leaf);
        }
    }

    /// Take graves under the floor out of a full leaf before it splits. The leaf takes a row next, so it never empties here
    fn purge_graves(&self, tree: &mut PartTree, leaf: u32, floor: WritePlace) {
        let count = self.count(leaf);
        let mut kept = 0;
        for index in 0..count {
            let tail = self.leaf_entry(index) + self.key_words;
            let high = self.load(leaf, tail);
            let place = (u64::from(high & !GRAVE_BIT) << 32) | u64::from(self.load(leaf, tail + 1));
            if high & GRAVE_BIT != 0 && place < floor.0 {
                continue;
            }
            if kept != index {
                self.copy_within(leaf, self.leaf_entry(index), self.leaf_entry(kept), self.leaf_stride);
            }
            kept += 1;
        }
        let went = (count - kept) as u64;
        self.set_count(leaf, kept);
        tree.entries -= went;
        self.entries.fetch_sub(went, Memory::AcqRel);
    }

    /// Unlink an empty leaf and give its page back, with any inner page it leaves empty, and fold a root left with one child
    fn remove_leaf(&self, tree: &mut PartTree, path: &PagePath, leaf: u32) {
        let (previous, next) = (self.previous(leaf), self.next(leaf));
        if previous == NO_PAGE {
            tree.first_leaf = next;
        } else {
            self.set_next(previous, next);
        }
        if next == NO_PAGE {
            tree.last_leaf = previous;
        } else {
            self.set_previous(next, previous);
        }
        self.give_back(&[leaf]);
        let mut depth = path.depth;
        loop {
            if depth == 0 {
                *tree = PartTree::empty();
                return;
            }
            let parent = path.pages[depth - 1];
            let at = path.children[depth - 1] as usize;
            let count = self.count(parent);
            self.copy_within(parent, self.inner_entry(at + 1), self.inner_entry(at), (count - at - 1) * self.inner_stride);
            self.set_count(parent, count - 1);
            if count > 1 {
                break;
            }
            self.give_back(&[parent]);
            depth -= 1;
        }
        while tree.levels > 1 && self.count(tree.root) == 1 {
            let root = tree.root;
            tree.root = self.load(root, self.inner_entry(0) + self.key_words);
            tree.levels -= 1;
            self.give_back(&[root]);
        }
    }

    /// Copy an entry into a batch, and skip a grave unless the batch takes them
    fn copy_live(&self, leaf: u32, index: usize, batch: &mut KeyBatch) {
        let entry = self.read_entry(leaf, index);
        if entry.is_grave && !batch.is_with_graves {
            return;
        }
        // the key goes straight into the batch, a word at a time
        let at = batch.keys.len();
        batch.keys.resize(at + self.width, 0);
        let base = self.leaf_entry(index);
        for (word, bytes) in batch.keys[at..].chunks_mut(size_of::<u32>()).enumerate() {
            bytes.copy_from_slice(&self.load(leaf, base + word).to_be_bytes()[..bytes.len()]);
        }
        batch.entries.push(entry);
        // a held value stays in its page, and the batch keeps where it is, so a reader copies it once
        let chain = self.chain_of(leaf);
        let is_held = !entry.is_grave && batch.is_with_values && self.values.holds(chain, index);
        batch.held.push(if is_held { HeldRef { chain, slot: index as u32 } } else { NO_HELD });
    }

    /// Read an entry
    fn read_entry(&self, leaf: u32, index: usize) -> KeyEntry {
        let tail = self.leaf_entry(index) + self.key_words;
        let high = self.load(leaf, tail);
        KeyEntry {
            place: WritePlace((u64::from(high & !GRAVE_BIT) << 32) | u64::from(self.load(leaf, tail + 1))),
            segment: SegmentNumber(u64::from(self.load(leaf, tail + 2))),
            offset: FileOffset(u64::from(self.load(leaf, tail + 3))),
            length: ByteCount(u64::from(self.load(leaf, tail + 4))),
            is_grave: high & GRAVE_BIT != 0,
        }
    }

    /// Read an entry's place
    fn entry_place(&self, leaf: u32, index: usize) -> u64 {
        let tail = self.leaf_entry(index) + self.key_words;
        (u64::from(self.load(leaf, tail) & !GRAVE_BIT) << 32) | u64::from(self.load(leaf, tail + 1))
    }

    /// Open a slot in a leaf and write an entry into it
    fn insert_at(&self, leaf: u32, index: usize, key: &PackedKey, words: &EntryWords) {
        let count = self.count(leaf);
        self.copy_within(leaf, self.leaf_entry(index), self.leaf_entry(index + 1), (count - index) * self.leaf_stride);
        let base = self.leaf_entry(index);
        for (at, word) in key.words[..self.key_words].iter().enumerate() {
            self.store(leaf, base + at, *word);
        }
        self.write_tail(leaf, index, words);
        self.set_count(leaf, count + 1);
    }

    /// Write the words of an entry after its key
    fn write_tail(&self, leaf: u32, index: usize, words: &EntryWords) {
        self.drop_values(leaf);
        let tail = self.leaf_entry(index) + self.key_words;
        let grave = if words.is_grave { GRAVE_BIT } else { 0 };
        self.store(leaf, tail, (words.place >> 32) as u32 | grave);
        self.store(leaf, tail + 1, words.place as u32);
        self.store(leaf, tail + 2, words.segment);
        self.store(leaf, tail + 3, words.offset);
        self.store(leaf, tail + 4, words.length);
    }

    /// Make room for a child at an index of an inner page
    fn open_inner(&self, page: u32, index: usize, count: usize) {
        self.copy_within(page, self.inner_entry(index), self.inner_entry(index + 1), (count - index) * self.inner_stride);
    }

    /// Write a child and its separator into an inner page
    fn write_child(&self, page: u32, index: usize, separator: &PackedKey, child: u32) {
        let base = self.inner_entry(index);
        for (at, word) in separator.words[..self.key_words].iter().enumerate() {
            self.store(page, base + at, *word);
        }
        self.store(page, base + self.key_words, child);
    }

    /// Compare the key at a word of a page against a packed key
    fn compare_at(&self, page: u32, base: usize, key: &PackedKey) -> Ordering {
        for at in 0..self.key_words {
            match self.load(page, base + at).cmp(&key.words[at]) {
                Ordering::Equal => {}
                order => return order,
            }
        }
        Ordering::Equal
    }

    /// Compare two packed keys over the index's width
    fn compare_keys(&self, left: &PackedKey, right: &PackedKey) -> Ordering {
        left.words[..self.key_words].cmp(&right.words[..self.key_words])
    }

    /// Read the key at a word of a page
    fn key_at(&self, page: u32, base: usize) -> PackedKey {
        let mut key = PackedKey { words: [0; MOST_KEY_WORDS] };
        for at in 0..self.key_words {
            key.words[at] = self.load(page, base + at);
        }
        key
    }

    /// Write the key at a word of a page out as bytes
    fn unpack_key(&self, page: u32, base: usize, key: &mut [u8; MOST_KEY_BYTES]) {
        for at in 0..self.key_words {
            key[at * size_of::<u32>()..(at + 1) * size_of::<u32>()].copy_from_slice(&self.load(page, base + at).to_be_bytes());
        }
    }

    /// Read the word an entry of a leaf starts at
    fn leaf_entry(&self, index: usize) -> usize {
        HEAD_WORDS + index * self.leaf_stride
    }

    /// Read the word a child of an inner page starts at
    fn inner_entry(&self, index: usize) -> usize {
        HEAD_WORDS + index * self.inner_stride
    }

    /// Copy words inside a page, either way
    fn copy_within(&self, page: u32, from: usize, to: usize, length: usize) {
        if to > from {
            for at in (0..length).rev() {
                self.store(page, to + at, self.load(page, from + at));
            }
        } else {
            for at in 0..length {
                self.store(page, to + at, self.load(page, from + at));
            }
        }
    }

    /// Copy words from one page to another
    fn copy_between(&self, from_page: u32, from: usize, to_page: u32, to: usize, length: usize) {
        for at in 0..length {
            self.store(to_page, to + at, self.load(from_page, from + at));
        }
    }

    /// Clear a page's head for a new leaf or inner page
    fn init_page(&self, page: u32) {
        self.store(page, GENERATION_WORD, 0);
        self.store(page, CHAIN_WORD, NO_VALUE_PAGE);
        self.set_count(page, 0);
        self.set_next(page, NO_PAGE);
        self.set_previous(page, NO_PAGE);
    }

    /// Take a page from the pool
    fn take_page(&self) -> Option<u32> {
        let mut page = [NO_PAGE];
        self.take_pages(&mut page).then_some(page[0])
    }

    /// Take pages from the pool, all or none
    fn take_pages(&self, pages: &mut [u32]) -> bool {
        let mut free = hold(&self.free);
        if (free.count as usize) < pages.len() {
            return false;
        }
        for page in pages.iter_mut() {
            *page = free.head;
            free.head = self.load(free.head, 1);
        }
        free.count -= pages.len() as u32;
        true
    }

    /// Give pages back to the pool
    fn give_back(&self, pages: &[u32]) {
        for &page in pages {
            self.drop_values(page);
        }
        let mut free = hold(&self.free);
        for &page in pages {
            self.store(page, 1, free.head);
            free.head = page;
        }
        free.count += pages.len() as u32;
    }

    /// Read a page's count
    fn count(&self, page: u32) -> usize {
        self.load(page, 0) as usize
    }

    /// Set a page's count. Entries move with it, so a leaf's held values go first
    fn set_count(&self, page: u32, count: usize) {
        self.drop_values(page);
        self.store(page, 0, count as u32);
    }

    /// Read a leaf's next leaf
    fn next(&self, page: u32) -> u32 {
        self.load(page, 1)
    }

    /// Set a leaf's next leaf
    fn set_next(&self, page: u32, next: u32) {
        self.store(page, 1, next);
    }

    /// Read a leaf's previous leaf
    fn previous(&self, page: u32) -> u32 {
        self.load(page, 2)
    }

    /// Set a leaf's previous leaf
    fn set_previous(&self, page: u32, previous: u32) {
        self.store(page, 2, previous);
    }

    /// Read a word of a page
    fn load(&self, page: u32, at: usize) -> u32 {
        self.words[page as usize * PAGE_WORDS + at].load(Memory::Relaxed)
    }

    /// Write a word of a page
    fn store(&self, page: u32, at: usize, word: u32) {
        self.words[page as usize * PAGE_WORDS + at].store(word, Memory::Relaxed);
    }
}

/// An entry's words after its key, checked to fit
#[derive(Clone, Copy, Debug)]
struct EntryWords {
    place: u64,
    is_grave: bool,
    segment: u32,
    offset: u32,
    length: u32,
}

impl EntryWords {
    /// Take an entry's words, which a caller checked fit
    fn of(entry: &KeyEntry) -> Self {
        Self {
            place: entry.place.0,
            is_grave: entry.is_grave,
            segment: entry.segment.0 as u32,
            offset: entry.offset.0 as u32,
            length: entry.length.0 as u32,
        }
    }
}

/// Read the part a packed key sits in
fn first_byte(key: &PackedKey) -> usize {
    (key.words[0] >> 24) as usize
}

/// Chain every page into the free list
fn chain_pages(pages: u32) -> FreePages {
    FreePages {
        head: if pages == 0 { NO_PAGE } else { 0 },
        count: pages,
    }
}

/// Allocate words with every page written, and huge pages asked for first
fn touched_words(length: usize) -> Vec<AtomicU32> {
    let mut words: Vec<AtomicU32> = Vec::with_capacity(length);
    ask_huge_pages(words.as_mut_ptr(), length);
    black_box(words.as_mut_ptr());
    words.resize_with(length, || AtomicU32::new(u32::MAX));
    for word in &words {
        word.store(0, Memory::Relaxed);
    }
    black_box(words.as_ptr());
    words
}

/// Work out the pages an index for these rows of keys this wide holds
pub fn planned_pages(width: usize, rows: RowCount) -> u32 {
    let key_words = width.clamp(1, MOST_KEY_BYTES).div_ceil(size_of::<u32>());
    let leaf_capacity = (PAGE_WORDS - HEAD_WORDS) / (key_words + ENTRY_TAIL_WORDS);
    let inner_capacity = (PAGE_WORDS - HEAD_WORDS) / (key_words + 1);
    let leaves = (rows.0 as usize).div_ceil((leaf_capacity * FILL_THIRDS / 3).max(1));
    // inner pages fill half at least, and each part can hold a leaf of its own however few rows it has
    let pages = leaves + leaves.div_ceil(inner_capacity / 2) * 2 + PARTS;
    // page numbers below the missing link's number are all a pool can have
    pages.min(NO_PAGE as usize - 1) as u32
}

/// Work out the heap an index for these rows of keys this wide holds, as `SortedKeys::new` sizes it
pub fn planned_index_bytes(width: usize, rows: RowCount) -> ByteCount {
    ByteCount(u64::from(planned_pages(width, rows)) * (PAGE_WORDS * size_of::<AtomicU32>()) as u64)
}

/// Work out the most rows of keys this wide an index holds in these bytes, as `SortedKeys::new` sizes it
pub fn rows_for_bytes(width: usize, bytes: ByteCount) -> RowCount {
    let (mut low, mut high) = (0_u64, 1_u64 << 40);
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if planned_index_bytes(width, RowCount(middle)).0 <= bytes.0 {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    RowCount(low)
}
