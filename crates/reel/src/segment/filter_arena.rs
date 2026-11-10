//! Filter blocks held in memory while they fit, so a get that misses reads nothing
//!
//! A segment's filters go in as one extent: a sorted list of its blocks, then their bits. An extent shows only once it's whole,
//! and it leaves with its segment. A segment that doesn't fit stays out, and its filters go through the block cache as before.
//! A probe takes no lock and writes nothing: the bytes are atomic words, and a generation on the extent tells a probe that crossed
//! a load or a drop to throw its answer away.

use std::hint::black_box;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering, fence};

use reel_api::units::ByteCount;

use crate::guard::hold;
use crate::pool::touched_capacity;
use crate::index::key_hash::KeyHash;
use crate::segment::block_filter::may_contain_bytes;
use crate::segment::segment_tail::SegmentNumber;

/// Words of one block's line in an extent: its filter's offset in the file, and where its bits start and end in the arena
pub const FILTER_LINE_WORDS: usize = 3;
/// Bytes of one block's line in an extent
pub const FILTER_LINE_BYTES: usize = FILTER_LINE_WORDS * WORD_BYTES;
/// Bytes of one word of the arena
pub const WORD_BYTES: usize = size_of::<u32>();
/// The most a block's bits can waste, since each starts on a word
pub const BLOCK_PAD_BYTES: u32 = WORD_BYTES as u32 - 1;
/// A map place with no number in it, since a number is kept one above itself
pub const NO_NUMBER: u64 = 0;

/// Bytes of the tables behind one segment: its extent, its book, its place on two free lists, and at most four map places
pub const SEGMENT_TABLE_BYTES: usize = size_of::<FilterExtent>()
    + size_of::<ExtentBook>()
    + size_of::<u32>()
    + size_of::<ArenaSpan>()
    + 4 * (size_of::<AtomicU64>() + size_of::<AtomicU32>());
/// The tables take at most one part in this many of the share, so a small share holds fewer segments
pub const TABLE_SHARE_DIVISOR: usize = 8;

/// A run of the arena's bytes, starting on a word
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ArenaSpan {
    pub start: u32,
    pub length: u32,
}

/// What a probe reads of one segment's filters. An odd generation means the extent is changing
#[derive(Debug, Default)]
pub struct FilterExtent {
    generation: AtomicU32,
    segment: AtomicU64,
    start: AtomicU32,
    blocks: AtomicU32,
}

/// What only a writer reads of one extent
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExtentBook {
    pub span: ArenaSpan,
    pub blocks: u32,
    pub bits_used: u32,
    pub lines_used: u32,
}

/// What only a writer reads of the arena
#[derive(Debug)]
pub struct ArenaBooks {
    extents: Vec<ExtentBook>,
    free_extents: Vec<u32>,
    free: Vec<ArenaSpan>,
}

/// The filters of every volume's segments that fit
#[derive(Debug)]
pub struct FilterArena {
    words: Vec<AtomicU32>,
    numbers: Vec<AtomicU64>,
    slots: Vec<AtomicU32>,
    mask: usize,
    extents: Vec<FilterExtent>,
    /// Moves at every change to any segment's filters, so an answer worked out from which filters were resident can tell it still holds
    changes: AtomicU64,
    books: Mutex<ArenaBooks>,
}

impl ArenaBooks {
    /// Find the first free span that holds a length, and take it
    pub fn take_span(&mut self, length: u32) -> Option<ArenaSpan> {
        if length == 0 {
            return Some(ArenaSpan::default());
        }
        let index = self.free.iter().position(|span| span.length >= length)?;
        let found = self.free[index];
        if found.length == length {
            self.free.remove(index);
        } else {
            self.free[index] = ArenaSpan {
                start: found.start + length,
                length: found.length - length,
            };
        }
        Some(ArenaSpan { start: found.start, length })
    }

    /// Give a span back, joining the free spans beside it. The list stays in order, and has room for every extent's span
    pub fn give_span(&mut self, span: ArenaSpan) {
        let at = self.free.partition_point(|free| free.start < span.start);
        self.free.insert(at, span);
        if at + 1 < self.free.len() && self.free[at].start + self.free[at].length == self.free[at + 1].start {
            self.free[at].length += self.free[at + 1].length;
            self.free.remove(at + 1);
        }
        if at > 0 && self.free[at - 1].start + self.free[at - 1].length == self.free[at].start {
            self.free[at - 1].length += self.free[at].length;
            self.free.remove(at);
        }
    }
}

impl FilterExtent {
    /// Mark the extent as changing, ahead of any store to it or to its bytes
    pub fn start_change(&self) {
        let generation = self.generation.load(Ordering::Relaxed);
        self.generation.store(generation | 1, Ordering::Relaxed);
        fence(Ordering::Release);
    }

    /// Mark a change as done, behind every store to the extent and its bytes
    pub fn end_change(&self) {
        let generation = self.generation.load(Ordering::Relaxed);
        self.generation.store((generation | 1).wrapping_add(1), Ordering::Release);
    }
}

impl FilterArena {
    /// Allocate and touch the arena, with room for a number of segments
    pub fn new(share: ByteCount, most_segments: usize) -> Self {
        let most_segments = most_segments.min(share.0 as usize / TABLE_SHARE_DIVISOR / SEGMENT_TABLE_BYTES);
        let tables = (most_segments * SEGMENT_TABLE_BYTES) as u64;
        let length = (share.0 - tables).min(u64::from(u32::MAX)) as u32 / WORD_BYTES as u32 * WORD_BYTES as u32;
        let mut free = touched_capacity(most_segments + 2, ArenaSpan::default());
        if length > 0 {
            free.push(ArenaSpan { start: 0, length });
        }
        let size = (most_segments.max(1) * 2).next_power_of_two();
        Self {
            words: touched_words(length as usize / WORD_BYTES),
            numbers: (0..size).map(|_place| AtomicU64::new(NO_NUMBER)).collect(),
            slots: (0..size).map(|_place| AtomicU32::new(0)).collect(),
            mask: size - 1,
            extents: (0..most_segments).map(|_slot| FilterExtent::default()).collect(),
            changes: AtomicU64::new(0),
            books: Mutex::new(ArenaBooks {
                extents: vec![ExtentBook::default(); most_segments],
                free_extents: (0..most_segments as u32).rev().collect(),
                free,
            }),
        }
    }

    /// Read how many times any segment's filters changed
    pub fn changes(&self) -> u64 {
        self.changes.load(Ordering::Acquire)
    }

    /// Read the heap the arena holds
    pub fn heap_bytes(&self) -> ByteCount {
        let books = hold(&self.books);
        let extents = self.extents.capacity() * size_of::<FilterExtent>() + books.extents.capacity() * size_of::<ExtentBook>();
        let lists = books.free.capacity() * size_of::<ArenaSpan>() + books.free_extents.capacity() * size_of::<u32>();
        let map = self.numbers.capacity() * size_of::<AtomicU64>() + self.slots.capacity() * size_of::<AtomicU32>();
        ByteCount((self.words.capacity() * WORD_BYTES + extents + lists + map) as u64)
    }

    /// Find where a segment sits in the map, with no lock. A place read beside a change only sends the reader to the block cache
    pub fn find(&self, segment: SegmentNumber) -> Option<usize> {
        let wanted = segment.0 + 1;
        let mut place = home_of(wanted, self.mask);
        for _step in 0..=self.mask {
            match self.numbers[place].load(Ordering::Relaxed) {
                NO_NUMBER => return None,
                number if number == wanted => return Some(place),
                _other => place = (place + 1) & self.mask,
            }
        }
        None
    }

    /// Ask a segment's filter for a block, or return nothing when the arena can't say: it doesn't hold the block, or the extent
    /// changed under this probe. Nothing here is a verdict, and the reader goes to the block cache
    pub fn probe(&self, segment: SegmentNumber, offset: u32, hash: KeyHash) -> Option<bool> {
        self.in_window(segment, |extent| {
            let (start, end) = self.block_bits(extent, offset)?;
            may_contain_bytes(end.checked_sub(start)?, |at| self.byte_at(start + at), hash)
        })
    }

    /// Copy a block's bits out, for a check that wants them whole
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn copy_filter(&self, segment: SegmentNumber, offset: u32) -> Option<Vec<u8>> {
        self.in_window(segment, |extent| {
            let (start, end) = self.block_bits(extent, offset)?;
            (start..end).map(|at| self.byte_at(at)).collect()
        })
    }

    /// Run a read of a segment's extent, and keep its answer only if the extent was the segment's, whole and unchanged throughout
    pub fn in_window<Answer>(&self, segment: SegmentNumber, read: impl FnOnce(&FilterExtent) -> Option<Answer>) -> Option<Answer> {
        let place = self.find(segment)?;
        let extent = self.extents.get(self.slots[place].load(Ordering::Relaxed) as usize)?;
        let before = extent.generation.load(Ordering::Acquire);
        if before % 2 == 1 {
            return None;
        }
        let answer = if extent.segment.load(Ordering::Relaxed) == segment.0 + 1 { read(extent) } else { None };
        fence(Ordering::Acquire);
        if extent.generation.load(Ordering::Relaxed) != before {
            return None;
        }
        answer
    }

    /// Find where a block's bits lie, by a search of the extent's sorted lines
    pub fn block_bits(&self, extent: &FilterExtent, offset: u32) -> Option<(usize, usize)> {
        let lines = extent.start.load(Ordering::Relaxed) as usize / WORD_BYTES;
        let (mut low, mut high) = (0, extent.blocks.load(Ordering::Relaxed) as usize);
        while low < high {
            let middle = (low + high) / 2;
            let at = lines + middle * FILTER_LINE_WORDS;
            let line_offset = self.words.get(at)?.load(Ordering::Relaxed);
            if line_offset == offset {
                let start = self.words.get(at + 1)?.load(Ordering::Relaxed) as usize;
                let end = self.words.get(at + 2)?.load(Ordering::Relaxed) as usize;
                return Some((start, end));
            }
            if line_offset < offset {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        None
    }

    /// Read one byte of the arena
    pub fn byte_at(&self, at: usize) -> Option<u8> {
        let word = self.words.get(at / WORD_BYTES)?.load(Ordering::Relaxed);
        Some(word.to_le_bytes()[at % WORD_BYTES])
    }

    /// Check a segment's extent stands, whole or filling
    pub fn holds(&self, segment: SegmentNumber) -> bool {
        self.find(segment).is_some()
    }

    /// Check the arena can answer for a segment's filters
    pub fn has_filters(&self, segment: SegmentNumber) -> bool {
        self.in_window(segment, |extent| (extent.blocks.load(Ordering::Relaxed) > 0).then_some(())).is_some()
    }

    /// Set aside an extent for a segment's blocks and bits, or say there's no room
    pub fn begin(&self, segment: SegmentNumber, blocks: u32, bits: u32) -> bool {
        let mut books = hold(&self.books);
        let Some(slot) = books.free_extents.last().copied() else {
            return false;
        };
        let room = blocks * FILTER_LINE_BYTES as u32 + bits + blocks * BLOCK_PAD_BYTES;
        let Some(span) = books.take_span(room.next_multiple_of(WORD_BYTES as u32)) else {
            return false;
        };
        books.free_extents.pop();
        books.extents[slot as usize] = ExtentBook { span, blocks, bits_used: 0, lines_used: 0 };
        let extent = &self.extents[slot as usize];
        extent.start_change();
        extent.segment.store(segment.0 + 1, Ordering::Relaxed);
        extent.start.store(span.start, Ordering::Relaxed);
        extent.blocks.store(blocks, Ordering::Relaxed);
        self.insert(segment, slot);
        // an extent with nothing to fill is whole already
        if blocks == 0 {
            extent.end_change();
            self.changes.fetch_add(1, Ordering::Release);
        }
        true
    }

    /// Add the next block in file order, and the extent shows once its last block is in
    pub fn add(&self, segment: SegmentNumber, offset: u32, bits: &[u8]) {
        let mut books = hold(&self.books);
        let Some(place) = self.find(segment) else {
            return;
        };
        let slot = self.slots[place].load(Ordering::Relaxed) as usize;
        let mut book = books.extents[slot];
        let bits_at = book.span.start + book.blocks * FILTER_LINE_BYTES as u32 + book.bits_used;
        let line_at = (book.span.start as usize / WORD_BYTES) + book.lines_used as usize * FILTER_LINE_WORDS;
        for (word, chunk) in self.words[bits_at as usize / WORD_BYTES..].iter().zip(bits.chunks(WORD_BYTES)) {
            let mut staged = [0; WORD_BYTES];
            staged[..chunk.len()].copy_from_slice(chunk);
            word.store(u32::from_le_bytes(staged), Ordering::Relaxed);
        }
        self.words[line_at].store(offset, Ordering::Relaxed);
        self.words[line_at + 1].store(bits_at, Ordering::Relaxed);
        self.words[line_at + 2].store(bits_at + bits.len() as u32, Ordering::Relaxed);
        book.bits_used += (bits.len() as u32).next_multiple_of(WORD_BYTES as u32);
        book.lines_used += 1;
        books.extents[slot] = book;
        if book.lines_used == book.blocks {
            self.extents[slot].end_change();
            self.changes.fetch_add(1, Ordering::Release);
        }
    }

    /// Read the bytes the extents in use took, and the arena's whole room
    pub fn used_bytes(&self) -> (ByteCount, ByteCount) {
        let books = hold(&self.books);
        let free: u64 = books.free.iter().map(|span| u64::from(span.length)).sum();
        let room = (self.words.len() * WORD_BYTES) as u64;
        (ByteCount(room.saturating_sub(free)), ByteCount(room))
    }

    /// Take a segment's extent out. Its bytes can go to another at once, since a probe that read them fails its window
    pub fn drop_segment(&self, segment: SegmentNumber) {
        let mut books = hold(&self.books);
        let Some(place) = self.find(segment) else {
            return;
        };
        let slot = self.slots[place].load(Ordering::Relaxed);
        let extent = &self.extents[slot as usize];
        extent.start_change();
        extent.segment.store(NO_NUMBER, Ordering::Relaxed);
        extent.end_change();
        self.changes.fetch_add(1, Ordering::Release);
        self.remove(place);
        let span = books.extents[slot as usize].span;
        books.free_extents.push(slot);
        if span.length > 0 {
            books.give_span(span);
        }
    }

    /// Put a segment in the map, at the first empty place from its home. The caller holds the books
    pub fn insert(&self, segment: SegmentNumber, slot: u32) {
        let number = segment.0 + 1;
        let mut place = home_of(number, self.mask);
        while self.numbers[place].load(Ordering::Relaxed) != NO_NUMBER {
            place = (place + 1) & self.mask;
        }
        self.slots[place].store(slot, Ordering::Relaxed);
        self.numbers[place].store(number, Ordering::Release);
    }

    /// Empty a map place, moving back what sits past it so every number stays reachable from its home. The caller holds the books
    pub fn remove(&self, place: usize) {
        let mut hole = place;
        let mut next = (hole + 1) & self.mask;
        loop {
            let number = self.numbers[next].load(Ordering::Relaxed);
            if number == NO_NUMBER {
                break;
            }
            let home = home_of(number, self.mask);
            if next.wrapping_sub(home) & self.mask >= next.wrapping_sub(hole) & self.mask {
                self.slots[hole].store(self.slots[next].load(Ordering::Relaxed), Ordering::Relaxed);
                self.numbers[hole].store(number, Ordering::Release);
                hole = next;
            }
            next = (next + 1) & self.mask;
        }
        self.numbers[hole].store(NO_NUMBER, Ordering::Release);
    }
}

/// Read where a number's probe starts
pub fn home_of(number: u64, mask: usize) -> usize {
    number.wrapping_mul(0x9E37_79B9_7F4A_7C15) as usize & mask
}

/// Allocate zeroed words with every page written
pub fn touched_words(length: usize) -> Vec<AtomicU32> {
    let mut words: Vec<AtomicU32> = Vec::with_capacity(length);
    black_box(words.as_mut_ptr());
    words.resize_with(length, || AtomicU32::new(u32::MAX));
    for word in &words {
        word.store(0, Ordering::Relaxed);
    }
    black_box(words.as_ptr());
    words
}
