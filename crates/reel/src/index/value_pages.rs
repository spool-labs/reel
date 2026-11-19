//! Values a sorted index keeps beside its leaves, so a scan or a get of a held key reads no record
//!
//! The values sit in 4 KiB pages from one pool fixed at open. A leaf that holds values points at a chain of pages: the first starts
//! with a slot for each of the leaf's entries, and every page takes value bytes from where its last value ended. Words are atomics,
//! so a page passes between leaves with no unsafe code. Each page carries a generation that moves before the page is given back, and
//! a leaf keeps the generation of its first page, so a reader that copied bytes and still sees that generation copied a live value.
//! A fill, a drop and an eviction run under the owning part's write lock, and a read under its read lock.
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

use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use reel_core::units::ByteCount;

use crate::guard::hold;
use crate::pool::ask_huge_pages;
use std::hint::black_box;

/// Words of one value page
pub const VALUE_PAGE_WORDS: usize = 512;
/// Bytes of one value page
pub const VALUE_PAGE_BYTES: usize = VALUE_PAGE_WORDS * size_of::<u64>();
/// The page a missing link points at
pub const NO_VALUE_PAGE: u32 = u32::MAX;
/// The largest value a leaf holds
pub const MOST_HELD_VALUE: usize = 1024;
/// Words at the head of a page: the owner and next page, the generation, and the used bytes with the clock bit and table length
pub const VALUE_HEAD_WORDS: usize = 3;
/// Pages a clock looks at for one chain to clear before it gives up
pub const MOST_CLOCK_LOOKS: u32 = 64;
/// Longest chain a leaf keeps
pub const MOST_CHAIN_PAGES: usize = 64;
/// The bit of an owner that marks a page past its chain's first, which holds no slot table
pub const FOLLOW_BIT: u32 = 1 << 31;

/// The bit of a page's used word that the clock clears and a read sets
const REFERENCED_BIT: u64 = 1 << 32;
/// Where a first page's used word keeps its slot table's length in words
const TABLE_SHIFT: u32 = 40;
/// Bits of a slot that hold a value's length plus one, so a zero slot holds nothing
const LENGTH_BITS: u32 = 11;
/// Bits of a slot that hold a value's byte offset in its page
const OFFSET_BITS: u32 = 12;

/// The pool's unused pages, chained through each page's next half
#[derive(Debug)]
struct FreeValuePages {
    head: u32,
    count: u32,
}

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

/// A chain the clock found nobody read since its last pass, with the leaf that owns it
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClockPick {
    pub chain: HeldChain,
    pub owner: u32,
}

/// The value pages of one sorted index
#[derive(Debug)]
pub struct ValuePages {
    words: Vec<AtomicU64>,
    free: Mutex<FreeValuePages>,
    clock: AtomicU32,
    /// Values held now, and the fills, drops and evictions so far, for the report
    pub held: AtomicU64,
    pub fills: AtomicU64,
    pub drops: AtomicU64,
    pub evictions: AtomicU64,
}

impl ValuePages {
    /// Allocate and touch a pool of this many pages, every one free
    pub fn new(pages: u32) -> Self {
        let words = touched_words(pages as usize * VALUE_PAGE_WORDS);
        let pool = Self {
            words,
            free: Mutex::new(FreeValuePages { head: if pages == 0 { NO_VALUE_PAGE } else { 0 }, count: pages }),
            clock: AtomicU32::new(0),
            held: AtomicU64::new(0),
            fills: AtomicU64::new(0),
            drops: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        };
        for page in 0..pages {
            let next = if page + 1 == pages { NO_VALUE_PAGE } else { page + 1 };
            pool.set_links(page, NO_VALUE_PAGE, next);
        }
        pool
    }

    /// Read the heap the pool holds
    pub fn heap_bytes(&self) -> ByteCount {
        ByteCount((self.words.capacity() * size_of::<AtomicU64>()) as u64)
    }

    /// Count the pool's pages
    pub fn page_count(&self) -> u32 {
        (self.words.len() / VALUE_PAGE_WORDS) as u32
    }

    /// Count the pages no leaf holds
    pub fn free_pages(&self) -> u32 {
        hold(&self.free).count
    }

    /// Check a chain a leaf names is still the one it filled
    pub fn is_live(&self, chain: HeldChain) -> bool {
        chain.first != NO_VALUE_PAGE && (chain.first as usize) < self.words.len() / VALUE_PAGE_WORDS && self.generation(chain.first) == chain.generation
    }

    /// Copy the value a leaf's entry holds onto the end of `out`, and say whether it held one
    pub fn read(&self, chain: HeldChain, index: usize, out: &mut Vec<u8>) -> bool {
        if !self.is_live(chain) || index >= self.table_words(chain.first) * 2 {
            return false;
        }
        let Some((link, offset, length)) = unpack_slot(self.slot(chain.first, index)) else {
            return false;
        };
        let mut page = chain.first;
        for _step in 0..link {
            page = self.next(page);
            if page == NO_VALUE_PAGE {
                return false;
            }
        }
        let start = out.len();
        self.copy_out(page, offset, length, out);
        if !self.is_live(chain) {
            out.truncate(start);
            return false;
        }
        self.mark_referenced(chain.first);
        true
    }

    /// Put a value in a leaf's chain for its entry, starting a chain when the leaf has none, and return the chain the leaf keeps now.
    /// Nothing when the value is too long or the pool has no free page, and then the caller may clear a chain and ask again
    pub fn fill(&self, owner: u32, chain: HeldChain, (index, entries): (usize, usize), value: &[u8]) -> Option<HeldChain> {
        let table = slot_table_words(entries);
        if value.len() > MOST_HELD_VALUE || index >= entries || VALUE_HEAD_WORDS + table >= VALUE_PAGE_WORDS {
            return None;
        }
        let chain = if self.is_live(chain) && self.table_words(chain.first) >= table {
            chain
        } else {
            let first = self.take(owner)?;
            self.set_used(first, (VALUE_HEAD_WORDS + table) * size_of::<u64>(), table);
            HeldChain { first, generation: self.generation(first) }
        };
        if self.slot(chain.first, index) != 0 {
            return Some(chain);
        }
        let (link, page) = self.room_for(owner, chain, value.len())?;
        let offset = self.used(page);
        self.copy_in(page, offset, value);
        self.set_used(page, align(offset + value.len()), self.table_words(page));
        self.set_slot(chain.first, index, pack_slot(link, offset, value.len()));
        self.fills.fetch_add(1, Ordering::Relaxed);
        self.held.fetch_add(1, Ordering::Relaxed);
        Some(chain)
    }

    /// Give a leaf's whole chain back, moving every page's generation first
    pub fn drop_chain(&self, chain: HeldChain) {
        if !self.is_live(chain) {
            return;
        }
        self.held.fetch_sub(self.count_slots(chain.first), Ordering::Relaxed);
        self.give_chain(chain.first);
        self.drops.fetch_add(1, Ordering::Relaxed);
    }

    /// Give back a chain the clock picked, once its owner's part is held and the owner still names it
    pub fn evict(&self, pick: ClockPick) {
        if !self.is_live(pick.chain) {
            return;
        }
        self.held.fetch_sub(self.count_slots(pick.chain.first), Ordering::Relaxed);
        self.give_chain(pick.chain.first);
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    /// Turn the clock to a chain nobody read since its last pass, clearing the marks it passes
    pub fn clock_pick(&self) -> Option<ClockPick> {
        let pages = self.page_count();
        if pages == 0 {
            return None;
        }
        for _look in 0..MOST_CLOCK_LOOKS {
            let page = self.clock.fetch_add(1, Ordering::Relaxed) % pages;
            let owner = self.owner(page);
            if owner == NO_VALUE_PAGE || owner & FOLLOW_BIT != 0 || self.take_referenced(page) {
                continue;
            }
            return Some(ClockPick { chain: HeldChain { first: page, generation: self.generation(page) }, owner });
        }
        None
    }

    /// Find room for a value of this length at the end of a chain, adding a page when the last is full
    fn room_for(&self, owner: u32, chain: HeldChain, length: usize) -> Option<(usize, u32)> {
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
        let added = self.take(owner | FOLLOW_BIT)?;
        self.set_used(added, VALUE_HEAD_WORDS * size_of::<u64>(), 0);
        self.set_links(page, self.owner(page), added);
        Some((link + 1, added))
    }

    /// Take a free page for a leaf
    fn take(&self, owner: u32) -> Option<u32> {
        let page = {
            let mut free = hold(&self.free);
            if free.head == NO_VALUE_PAGE {
                return None;
            }
            let page = free.head;
            free.head = self.next(page);
            free.count -= 1;
            page
        };
        self.set_links(page, owner, NO_VALUE_PAGE);
        self.word(page, 2).store(0, Ordering::Relaxed);
        for at in VALUE_HEAD_WORDS..VALUE_PAGE_WORDS {
            self.word(page, at).store(0, Ordering::Relaxed);
        }
        Some(page)
    }

    /// Move each page's generation, then give the chain to the free list
    fn give_chain(&self, first: u32) {
        let mut pages = [NO_VALUE_PAGE; MOST_CHAIN_PAGES];
        let mut count = 0;
        let mut page = first;
        while page != NO_VALUE_PAGE && count < MOST_CHAIN_PAGES {
            self.word(page, 1).fetch_add(1, Ordering::SeqCst);
            pages[count] = page;
            count += 1;
            page = self.next(page);
        }
        let mut free = hold(&self.free);
        for &page in &pages[..count] {
            self.set_links(page, NO_VALUE_PAGE, free.head);
            free.head = page;
            free.count += 1;
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
        let mut left = length;
        let mut at = offset / size_of::<u64>();
        while left > 0 {
            let bytes = self.word(page, at).load(Ordering::Relaxed).to_le_bytes();
            let take = left.min(size_of::<u64>());
            out.extend_from_slice(&bytes[..take]);
            left -= take;
            at += 1;
        }
    }

    fn take_referenced(&self, page: u32) -> bool {
        self.word(page, 2).fetch_and(!REFERENCED_BIT, Ordering::Relaxed) & REFERENCED_BIT != 0
    }

    fn mark_referenced(&self, page: u32) {
        let word = self.word(page, 2);
        if word.load(Ordering::Relaxed) & REFERENCED_BIT == 0 {
            word.fetch_or(REFERENCED_BIT, Ordering::Relaxed);
        }
    }

    fn used(&self, page: u32) -> usize {
        (self.word(page, 2).load(Ordering::Relaxed) & 0xFFFF_FFFF) as usize
    }

    fn table_words(&self, page: u32) -> usize {
        (self.word(page, 2).load(Ordering::Relaxed) >> TABLE_SHIFT) as usize
    }

    fn set_used(&self, page: u32, used: usize, table: usize) {
        let word = self.word(page, 2);
        let referenced = word.load(Ordering::Relaxed) & REFERENCED_BIT;
        word.store(((table as u64) << TABLE_SHIFT) | referenced | used as u64, Ordering::Relaxed);
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
        &self.words[page as usize * VALUE_PAGE_WORDS + at]
    }
}

/// Allocate words, every page of them written, so no read faults later
fn touched_words(length: usize) -> Vec<AtomicU64> {
    let mut words: Vec<AtomicU64> = Vec::with_capacity(length);
    ask_huge_pages(words.as_mut_ptr(), length);
    black_box(words.as_mut_ptr());
    words.resize_with(length, || AtomicU64::new(u64::MAX));
    for word in &words {
        word.store(0, Ordering::Relaxed);
    }
    black_box(words.as_ptr());
    words
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
    if slot == 0 {
        return None;
    }
    let length = (slot & ((1 << LENGTH_BITS) - 1)) as usize - 1;
    let offset = ((slot >> LENGTH_BITS) & ((1 << OFFSET_BITS) - 1)) as usize;
    let link = (slot >> (OFFSET_BITS + LENGTH_BITS)) as usize;
    Some((link, offset, length))
}
