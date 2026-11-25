//! Values a sorted index keeps beside its leaves, so a scan or a get of a held key reads no record
//!
//! The values sit in 4 KiB pages from one pool fixed at open. A leaf that holds values points at a chain of pages: the first starts
//! with a slot for each of the leaf's entries, and every page takes value bytes from where its last value ended. Words are atomics,
//! so a page passes between leaves with no unsafe code. Each page carries a generation that moves before the page is given back, and
//! a leaf keeps the generation of its first page, so a reader that copied bytes and still sees that generation copied a live value.
//! A fill and a drop run under the owning part's write lock, and a read under its read lock. A full pool takes no more values
//! until a change to some leaf gives pages back: with keys read evenly, clearing one held value for another gains nothing.
//!
//! Based on calculations:
//! - A leaf's chain is whole pages. Each page has 24 B of head, the first has a 4 B slot for every position of the leaf's
//!   capacity, and each value takes its length rounded up to 8 B, with no value crossing a page.
//! - A leaf of e entries of v bytes, v8 rounded, takes ceil((24 x pages + 4 x capacity + e x v8) / 4,096) pages, and half a page
//!   sits unused on average.
//! - So a held value costs about v8 + (4 x capacity + 2,048) / e bytes, and a column is held whole once the pool is that times its
//!   keys.
//! - A value over MOST_HELD_VALUE is never held, a chain stops at MOST_CHAIN_PAGES, and a scan's index batch carries
//!   HELD_BYTES_PER_ENTRY for each entry, so values past that share ride its batch as misses.

use std::io::Result;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use reel_core::units::ByteCount;

use crate::guard::hold;
use crate::pool_room::PoolRoom;

/// Words of one value page
pub const VALUE_PAGE_WORDS: usize = 512;
/// Bytes of one value page
pub const VALUE_PAGE_BYTES: usize = VALUE_PAGE_WORDS * size_of::<u64>();
/// The page a missing link points at
pub const NO_VALUE_PAGE: u32 = u32::MAX;
/// The largest value a leaf holds
pub const MOST_HELD_VALUE: usize = 1024;
/// Words at the head of a page: the owner and next page, the generation, and the used bytes with the table length
pub const VALUE_HEAD_WORDS: usize = 3;
/// Longest chain a leaf keeps
pub const MOST_CHAIN_PAGES: usize = 64;
/// The bit of an owner that marks a page past its chain's first, which holds no slot table
pub const FOLLOW_BIT: u32 = 1 << 31;

/// Where a first page's used word keeps its slot table's length in words
const TABLE_SHIFT: u32 = 40;
/// Bits of a slot that hold a value's length plus one, so a zero slot holds nothing
const LENGTH_BITS: u32 = 11;
/// Bits of a slot that hold a value's byte offset in its page
const OFFSET_BITS: u32 = 12;

/// A leaf's held values: the chain's first page and its generation, as the leaf keeps them in its head
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeldChain {
    pub first: u32,
    pub generation: u32,
}

impl HeldChain {
    /// A leaf that holds no values
    pub const NONE: Self = Self { first: NO_VALUE_PAGE, generation: 0 };
}

/// The value pages of one sorted index
#[derive(Debug)]
pub struct ValuePages {
    /// The pages' words, reserved for every page the pool can grow to
    room: Arc<PoolRoom>,
    most_pages: u32,
    /// The pages each index sharing the pool holds now, under the lock every take and give holds
    taken: Mutex<Vec<u32>>,
    /// The pages each index sharing the pool is sized for. Every index is sure of half its floor: no take leaves fewer free pages
    /// than the others still lack of their halves. Past its floor, an index takes a page only while half of every other index's
    /// unused floor stays free, so a busy column uses what idle ones leave and a walk of a cold column never starves a hot one
    floors: Vec<u32>,
    /// The pages each index holds now, mirrored from the free list's counts so a fill can see a refusal with no lock
    taken_now: Vec<AtomicU32>,
    /// The next generation a page taken gets
    generations: AtomicU32,
    /// Values held now, and the fills and drops so far, for the report
    pub held: AtomicU64,
    pub fills: AtomicU64,
    pub drops: AtomicU64,
}

impl ValuePages {
    /// Allocate and touch a pool of this many pages, every one free, for one index
    pub fn new(pages: u32) -> Result<Self> {
        Self::shared(pages, &[pages])
    }

    /// Build a pool of this many pages for indexes that share it, each with the floor it is sized for, all of it in memory
    pub fn shared(pages: u32, floors: &[u32]) -> Result<Self> {
        Self::growing(pages, floors, pages, pages)
    }

    /// Build a shared pool with its first pages in memory, which grows into the rest on the reserve job
    pub fn growing(pages: u32, floors: &[u32], first: u32, most_pages: u32) -> Result<Self> {
        let pages = pages.max(most_pages).min(u32::MAX - 1);
        let bytes = |count: u32| ByteCount(u64::from(count) * VALUE_PAGE_BYTES as u64);
        let room = PoolRoom::reserve_paged(bytes(pages), bytes(first.min(pages)), VALUE_PAGE_BYTES)?.read_unlocked().for_cache();
        Ok(Self {
            room: Arc::new(room),
            most_pages: pages,
            taken: Mutex::new(vec![0; floors.len()]),
            floors: floors.to_vec(),
            taken_now: floors.iter().map(|_floor| AtomicU32::new(0)).collect(),
            generations: AtomicU32::new(1),
            held: AtomicU64::new(0),
            fills: AtomicU64::new(0),
            drops: AtomicU64::new(0),
        })
    }

    /// Read the room, which the growing job raises
    pub fn room(&self) -> &Arc<PoolRoom> {
        &self.room
    }

    /// Read every word of the pages the pool can grow to. Only pages handed out are read or written, and all of those are in memory
    fn words(&self) -> &[AtomicU64] {
        // SAFETY: the room is reserved for this many pages, readable and writable, and zero until written; a page past the limit is
        // never handed out, so no access reaches it while the growing job brings it in
        unsafe { std::slice::from_raw_parts(self.room.base().cast::<AtomicU64>(), self.most_pages as usize * VALUE_PAGE_WORDS) }
    }

    /// Read the heap the pool holds: its pages in memory
    pub fn heap_bytes(&self) -> ByteCount {
        ByteCount(u64::from(self.page_count()) * VALUE_PAGE_BYTES as u64)
    }

    /// Count the pool's pages in ready extents
    pub fn page_count(&self) -> u32 {
        self.room.ready_pages()
    }

    /// Count the pages the pool can grow to
    pub fn most_pages(&self) -> u32 {
        self.most_pages
    }

    /// Count the pages in ready extents no leaf holds, a moment old at most
    pub fn free_pages(&self) -> u32 {
        self.room.free_pages()
    }

    /// Check the pool has no free page, so no fill can start a chain or add a page
    pub fn is_full(&self) -> bool {
        self.free_pages() == 0
    }

    /// Check a page lies in the pool and in a ready extent, so a reader with no lock may read it
    fn is_ready(&self, page: u32) -> bool {
        page < self.most_pages && self.room.is_ready_at(page as usize * VALUE_PAGE_BYTES)
    }

    /// Check a chain a leaf names is still the one it filled
    pub fn is_live(&self, chain: HeldChain) -> bool {
        chain.first != NO_VALUE_PAGE && self.is_ready(chain.first) && self.generation(chain.first) == chain.generation
    }

    /// Check a leaf's entry holds a value, with no copy
    pub fn holds(&self, chain: HeldChain, index: usize) -> bool {
        self.is_live(chain) && self.has_slot(chain.first, index) && unpack_slot(self.slot(chain.first, index)).is_some()
    }

    /// Check an entry's slot lies in its chain's first page. A reader takes no lock, so a page reused under it can hold value bytes
    /// where the table was, and every word it reads is checked to lie in the pool before it's read
    fn has_slot(&self, first: u32, index: usize) -> bool {
        index < self.table_words(first) * 2 && VALUE_HEAD_WORDS + index / 2 < VALUE_PAGE_WORDS
    }

    /// Copy the value a leaf's entry holds onto the end of `out`, and say whether it held one
    pub fn read(&self, chain: HeldChain, index: usize, out: &mut Vec<u8>) -> bool {
        if !self.is_live(chain) || !self.has_slot(chain.first, index) {
            return false;
        }
        let Some((link, offset, length)) = unpack_slot(self.slot(chain.first, index)) else {
            return false;
        };
        if offset + length > VALUE_PAGE_BYTES {
            return false;
        }
        let mut page = chain.first;
        for _step in 0..link {
            page = self.next(page);
            if page == NO_VALUE_PAGE || !self.is_ready(page) {
                return false;
            }
        }
        let start = out.len();
        self.copy_out(page, offset, length, out);
        if !self.is_live(chain) {
            out.truncate(start);
            return false;
        }
        true
    }

    /// Count values a run of fills put in, once for the run, so a fill writes no shared count
    pub fn note_fills(&self, count: u64) {
        if count > 0 {
            self.fills.fetch_add(count, Ordering::Relaxed);
            self.held.fetch_add(count, Ordering::Relaxed);
        }
    }

    /// Put a value in a leaf's chain for its entry, starting a chain when the leaf has none, and return the chain the leaf keeps now
    /// and whether this put the value in. Nothing when the value is too long or the pool has no free page. The caller counts what
    /// went in with `note_fills`
    pub fn fill(&self, (sharer, owner): (usize, u32), chain: HeldChain, (index, entries): (usize, usize), value: &[u8]) -> Option<(HeldChain, bool)> {
        let table = slot_table_words(entries);
        if value.len() > MOST_HELD_VALUE || index >= entries || VALUE_HEAD_WORDS + table >= VALUE_PAGE_WORDS {
            return None;
        }
        let chain = if self.is_live(chain) && self.table_words(chain.first) >= table {
            chain
        } else {
            let first = self.take(sharer, owner)?;
            self.set_used(first, (VALUE_HEAD_WORDS + table) * size_of::<u64>(), table);
            HeldChain { first, generation: self.generation(first) }
        };
        if self.slot(chain.first, index) != 0 {
            return Some((chain, false));
        }
        let (link, page) = self.room_for((sharer, owner), chain, value.len())?;
        let offset = self.used(page);
        self.copy_in(page, offset, value);
        self.set_used(page, align(offset + value.len()), self.table_words(page));
        self.set_slot(chain.first, index, pack_slot(link, offset, value.len()));
        Some((chain, true))
    }

    /// Give a leaf's whole chain back, moving every page's generation first
    pub fn drop_chain(&self, sharer: usize, chain: HeldChain) {
        if !self.is_live(chain) {
            return;
        }
        self.held.fetch_sub(self.count_slots(chain.first), Ordering::Relaxed);
        self.give_chain(sharer, chain.first);
        self.drops.fetch_add(1, Ordering::Relaxed);
    }

    /// Find room for a value of this length at the end of a chain, adding a page when the last is full
    fn room_for(&self, (sharer, owner): (usize, u32), chain: HeldChain, length: usize) -> Option<(usize, u32)> {
        let mut page = chain.first;
        let mut link = 0;
        loop {
            let next = self.next(page);
            if next == NO_VALUE_PAGE {
                break;
            }
            page = next;
            link += 1;
        }
        // a value that would end the page exactly leaves no room for the next one's offset, so a page keeps one byte past its last
        if self.used(page) + length < VALUE_PAGE_BYTES {
            return Some((link, page));
        }
        if link + 1 >= MOST_CHAIN_PAGES {
            return None;
        }
        let added = self.take(sharer, owner | FOLLOW_BIT)?;
        self.set_used(added, VALUE_HEAD_WORDS * size_of::<u64>(), 0);
        self.set_links(page, self.owner(page), added);
        Some((link + 1, added))
    }

    /// Take a free page for a leaf of an index, past its floor only while half of every other index's unused share stays free
    fn take(&self, sharer: usize, owner: u32) -> Option<u32> {
        let page = {
            let mut taken = hold(&self.taken);
            let available = self.room.free_pages();
            if available == 0 || !self.may_take(sharer, |index| taken[index], available) {
                return None;
            }
            let page = self.room.take_page()?;
            if let Some(count) = taken.get_mut(sharer) {
                *count += 1;
                self.taken_now[sharer].store(*count, Ordering::Relaxed);
            }
            page
        };
        // a page from an extent that went back to the kernel comes back zeroed, so a page taken gets a generation no chain has had,
        // and a chain noted before can't match it. None is 0, which a reader that raced a release reads there
        let generation = (self.generations.fetch_add(1, Ordering::Relaxed) & !(1 << 31)).max(1);
        self.word(page, 1).store(u64::from(generation), Ordering::SeqCst);
        self.set_links(page, owner, NO_VALUE_PAGE);
        self.word(page, 2).store(0, Ordering::Relaxed);
        for at in VALUE_HEAD_WORDS..VALUE_PAGE_WORDS {
            self.word(page, at).store(0, Ordering::Relaxed);
        }
        Some(page)
    }

    /// Check an index may take one more page, from the pages each index holds and the free count. No take leaves fewer free pages
    /// than the other indexes still lack of half their floors, and past its own floor an index also leaves half of every other
    /// index's unused floor free
    fn may_take(&self, sharer: usize, taken: impl Fn(usize) -> u32, free: u32) -> bool {
        let Some(&own_floor) = self.floors.get(sharer) else {
            return false;
        };
        let others = (0..self.floors.len()).filter(|other| *other != sharer);
        let sure: u64 = others.clone().map(|other| u64::from((self.floors[other] / 2).saturating_sub(taken(other)))).sum();
        if u64::from(free) <= sure {
            return false;
        }
        if taken(sharer) < own_floor {
            return true;
        }
        let owed: u64 = others.map(|other| u64::from(self.floors[other].saturating_sub(taken(other))) / 2).sum();
        u64::from(free) > owed
    }

    /// Check, with no lock, whether an index could take a page now, so a fill the pool would refuse takes no part's lock. The
    /// counts may be a moment old, so a take still checks under the lock
    pub fn could_take(&self, sharer: usize) -> bool {
        let free = self.free_pages();
        free > 0 && self.may_take(sharer, |index| self.taken_now[index].load(Ordering::Relaxed), free)
    }

    /// Move each page's generation, then give the chain to the free list and take its pages off the index's count
    fn give_chain(&self, sharer: usize, first: u32) {
        let mut pages = [NO_VALUE_PAGE; MOST_CHAIN_PAGES];
        let mut count = 0;
        let mut page = first;
        while page != NO_VALUE_PAGE && count < MOST_CHAIN_PAGES {
            self.word(page, 1).fetch_add(1, Ordering::SeqCst);
            pages[count] = page;
            count += 1;
            page = self.next(page);
        }
        let mut taken = hold(&self.taken);
        for &page in &pages[..count] {
            self.set_links(page, NO_VALUE_PAGE, NO_VALUE_PAGE);
            self.room.give_page(page);
        }
        if let Some(held) = taken.get_mut(sharer) {
            *held = held.saturating_sub(count as u32);
            self.taken_now[sharer].store(*held, Ordering::Relaxed);
        }
    }

    fn count_slots(&self, first: u32) -> u64 {
        let mut count = 0;
        for index in 0..self.table_words(first) * 2 {
            count += u64::from(self.slot(first, index) != 0);
        }
        count
    }

    fn slot(&self, first: u32, index: usize) -> u32 {
        let word = self.word(first, VALUE_HEAD_WORDS + index / 2).load(Ordering::Acquire);
        if index.is_multiple_of(2) { word as u32 } else { (word >> 32) as u32 }
    }

    fn set_slot(&self, first: u32, index: usize, slot: u32) {
        let word = self.word(first, VALUE_HEAD_WORDS + index / 2);
        let old = word.load(Ordering::Relaxed);
        let new = if index.is_multiple_of(2) { (old & !0xFFFF_FFFF) | u64::from(slot) } else { (old & 0xFFFF_FFFF) | (u64::from(slot) << 32) };
        word.store(new, Ordering::Release);
    }

    fn copy_in(&self, page: u32, offset: usize, value: &[u8]) {
        for (index, chunk) in value.chunks(size_of::<u64>()).enumerate() {
            let mut bytes = [0; size_of::<u64>()];
            bytes[..chunk.len()].copy_from_slice(chunk);
            self.word(page, offset / size_of::<u64>() + index).store(u64::from_le_bytes(bytes), Ordering::Relaxed);
        }
    }

    fn copy_out(&self, page: u32, offset: usize, length: usize, out: &mut Vec<u8>) {
        // the page's words are found once, and each lands in the buffer's spare room with no fill before it
        let first = page as usize * VALUE_PAGE_WORDS + offset / size_of::<u64>();
        let words = &self.words()[first..first + length.div_ceil(size_of::<u64>())];
        let start = out.len();
        out.reserve(length);
        let room = out.spare_capacity_mut();
        let (whole, tail) = (length / size_of::<u64>(), length % size_of::<u64>());
        for (word, bytes) in words.iter().zip(room.chunks_exact_mut(size_of::<u64>())).take(whole) {
            for (byte, value) in bytes.iter_mut().zip(word.load(Ordering::Relaxed).to_le_bytes()) {
                byte.write(value);
            }
        }
        if tail > 0 {
            let last = words[whole].load(Ordering::Relaxed).to_le_bytes();
            for (byte, value) in room[whole * size_of::<u64>()..length].iter_mut().zip(last) {
                byte.write(value);
            }
        }
        // SAFETY: the loops above wrote each of the `length` bytes past `start`, and the room holds them
        unsafe { out.set_len(start + length) };
    }

    fn used(&self, page: u32) -> usize {
        (self.word(page, 2).load(Ordering::Relaxed) & 0xFFFF_FFFF) as usize
    }

    fn table_words(&self, page: u32) -> usize {
        (self.word(page, 2).load(Ordering::Relaxed) >> TABLE_SHIFT) as usize
    }

    fn set_used(&self, page: u32, used: usize, table: usize) {
        self.word(page, 2).store(((table as u64) << TABLE_SHIFT) | used as u64, Ordering::Relaxed);
    }

    fn generation(&self, page: u32) -> u32 {
        // the top bit stays clear for the leaf's own mark that it names a chain
        (self.word(page, 1).load(Ordering::SeqCst) as u32) & !(1 << 31)
    }

    fn owner(&self, page: u32) -> u32 {
        (self.word(page, 0).load(Ordering::Relaxed) >> 32) as u32
    }

    fn next(&self, page: u32) -> u32 {
        self.word(page, 0).load(Ordering::Relaxed) as u32
    }

    fn set_links(&self, page: u32, owner: u32, next: u32) {
        self.word(page, 0).store((u64::from(owner) << 32) | u64::from(next), Ordering::Relaxed);
    }

    fn word(&self, page: u32, at: usize) -> &AtomicU64 {
        &self.words()[page as usize * VALUE_PAGE_WORDS + at]
    }
}

/// Words a first page's slot table takes for a leaf of this many entries
pub fn slot_table_words(entries: usize) -> usize {
    entries.div_ceil(2)
}

/// Round a byte offset up to the next word
fn align(offset: usize) -> usize {
    offset.next_multiple_of(size_of::<u64>())
}

/// Pack a value's page in its chain, byte offset and length into a slot, with its length counted plus one so a zero holds nothing
fn pack_slot(link: usize, offset: usize, length: usize) -> u32 {
    ((link as u32) << (OFFSET_BITS + LENGTH_BITS)) | ((offset as u32) << LENGTH_BITS) | (length as u32 + 1)
}

/// Read a slot back, or nothing when it holds no value
fn unpack_slot(slot: u32) -> Option<(usize, usize, usize)> {
    // a length field of zero is no slot this pool wrote, a page reused under a reader
    let length = ((slot & ((1 << LENGTH_BITS) - 1)) as usize).checked_sub(1)?;
    let offset = ((slot >> LENGTH_BITS) & ((1 << OFFSET_BITS) - 1)) as usize;
    let link = (slot >> (OFFSET_BITS + LENGTH_BITS)) as usize;
    Some((link, offset, length))
}

#[cfg(test)]
mod tests {
    use super::*;

    // takes across the counter's wrap never hand out generation 0, which a zeroed page reads as
    #[test]
    fn generations_skip_zero_across_the_wrap() {
        let pool = ValuePages::new(8).expect("pool");
        pool.generations.store(u32::MAX - 2, Ordering::Relaxed);
        let mut seen = Vec::new();
        for owner in 0..8 {
            let page = pool.take(0, owner).expect("take");
            seen.push(pool.generation(page));
        }
        assert!(seen.iter().all(|&generation| generation != 0), "{seen:?}");
        pool.generations.store(1 << 31, Ordering::Relaxed);
        let page = pool.take(0, 9);
        assert!(page.is_none_or(|page| pool.generation(page) != 0));
    }
}
