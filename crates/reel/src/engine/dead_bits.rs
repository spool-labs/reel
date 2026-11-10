//! One bit for each row the dead-byte walk has charged, for every segment born since open, so a merge can tell a dead row from a
//! live one with no lookup

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use reel_api::units::ByteCount;

use crate::fixed_list::FixedList;
use crate::guard::hold;
use crate::row_kind::RowKind;
use crate::segment::footer_reader::RowAt;
use crate::segment::segment_tail::SegmentNumber;
use crate::segment::summary::DeadCount;

/// Bits one word holds
pub const WORD_BITS: u32 = u64::BITS;
/// Causes a segment's bits stop answering, in the report's order
pub const INEXACT_PLACE: usize = 0;
pub const INEXACT_OVERFLOW: usize = 1;
pub const INEXACT_DAMAGE: usize = 2;
pub const INEXACT_POOL: usize = 3;
pub const INEXACT_KEPT: usize = 4;
pub const INEXACT_SHORT: usize = 5;
/// Rows a merge decides by bit for each one it also asks, in a build without the test hooks
pub const BIT_SAMPLE_EVERY: u64 = 1024;
/// Places in the merge counts: set and dropped, clear and kept, asked as well, set with no newer row, clear with one
pub const DECIDED_SET: usize = 0;
pub const DECIDED_CLEAR: usize = 1;
pub const DECIDED_ASKED: usize = 2;
pub const DECIDED_WRONG: usize = 3;
pub const DECIDED_SHORT: usize = 4;

/// A row the walk charged, kept beside its block's count until the count lands
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DeadRow {
    pub segment: SegmentNumber,
    pub block: u32,
    pub at: RowAt,
    pub length: u64,
    pub is_grave: bool,
}

/// A run of the pool's words
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WordSpan {
    pub start: u32,
    pub length: u32,
}

/// One segment's bits: its words, laid out a block at a time, and whether every charge on it carried the row's place
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BitsExtent {
    pub span: WordSpan,
    pub block_count: u32,
    pub block_words: u32,
    pub is_exact: bool,
}

/// What only the books' holder reads: the extents and the free spans, in order
#[derive(Debug)]
pub struct BitsBooks {
    extents: HashMap<SegmentNumber, BitsExtent>,
    free: Vec<WordSpan>,
}

/// The pool, sized at open
#[derive(Debug)]
pub struct DeadBits {
    words: Vec<AtomicU64>,
    books: Mutex<BitsBooks>,
    /// Segments made inexact, by cause: unknown place, list overflow, damage, pool short, a merge output with counts that came
    /// with no bits, and a clear bit the ask found replaced, where the walk missed a charge and every row asks from then on
    pub inexact: [AtomicU64; 6],
    /// Spoils so far, so a merge that read an extent reads it again after one
    pub spoils: AtomicU64,
    /// Merge rows by what their bit gave: set and dropped, clear and kept, asked as well, a set bit the ask found no newer row
    /// for, a clear bit the ask found one for
    pub decided: [AtomicU64; 5],
}

/// Words one block of a segment takes, enough for the most rows any of its blocks holds
pub fn block_words(most_block_rows: u32) -> u32 {
    most_block_rows.div_ceil(WORD_BITS).max(1)
}

impl DeadBits {
    /// Allocate a pool of some bytes, with room in the map for a number of segments
    pub fn new(share: ByteCount, most_segments: usize) -> Self {
        let words = (share.0 / size_of::<u64>() as u64) as usize;
        let mut free = Vec::with_capacity(most_segments + 1);
        if words > 0 {
            free.push(WordSpan { start: 0, length: words as u32 });
        }
        Self {
            words: (0..words).map(|_word| AtomicU64::new(0)).collect(),
            books: Mutex::new(BitsBooks { extents: HashMap::with_capacity(most_segments), free }),
            inexact: Default::default(),
            spoils: AtomicU64::new(0),
            decided: Default::default(),
        }
    }

    /// Read the heap the pool and its books hold
    pub fn heap_bytes(&self) -> ByteCount {
        let books = hold(&self.books);
        let map = books.extents.capacity() * (size_of::<SegmentNumber>() + size_of::<BitsExtent>() + size_of::<u64>());
        ByteCount((self.words.capacity() * size_of::<u64>() + map + books.free.capacity() * size_of::<WordSpan>()) as u64)
    }

    /// Read the bytes the extents in use take, and the segments that have bits
    pub fn used(&self) -> (ByteCount, u64) {
        let books = hold(&self.books);
        let words: u64 = books.extents.values().map(|extent| u64::from(extent.span.length)).sum();
        (ByteCount(words * size_of::<u64>() as u64), books.extents.len() as u64)
    }

    /// Give a segment born since open a clear extent, or none when the pool or the map is full. A segment that had one starts over
    pub fn begin(&self, segment: SegmentNumber, block_count: u32, most_block_rows: u32) -> bool {
        let mut books = hold(&self.books);
        if let Some(old) = books.extents.remove(&segment) {
            give_span(&mut books.free, old.span);
        }
        if books.extents.len() == books.extents.capacity() {
            self.inexact[INEXACT_POOL].fetch_add(1, Ordering::Relaxed);
            return false;
        }
        let block_words = block_words(most_block_rows);
        let length = block_count.saturating_mul(block_words);
        let Some(span) = take_span(&mut books.free, length) else {
            self.inexact[INEXACT_POOL].fetch_add(1, Ordering::Relaxed);
            return false;
        };
        for word in &self.words[span.start as usize..(span.start + span.length) as usize] {
            word.store(0, Ordering::Relaxed);
        }
        books.extents.insert(segment, BitsExtent { span, block_count, block_words, is_exact: true });
        true
    }

    /// Give a segment's extent back, when it leaves the table
    pub fn end(&self, segment: SegmentNumber) {
        let mut books = hold(&self.books);
        if let Some(old) = books.extents.remove(&segment) {
            give_span(&mut books.free, old.span);
        }
    }

    /// Read a segment's extent while its bits answer for every row, or nothing
    pub fn exact(&self, segment: SegmentNumber) -> Option<BitsExtent> {
        hold(&self.books).extents.get(&segment).copied().filter(|extent| extent.is_exact)
    }

    /// Read one row's bit through an extent read before, or nothing when the row's place lies outside it
    pub fn is_set(&self, extent: &BitsExtent, block: u32, index: u32) -> Option<bool> {
        let (word, bit) = word_of(extent, block, index)?;
        Some(self.words[word].load(Ordering::Acquire) & bit != 0)
    }

    /// Set the bits of a block's charged rows as their counts land, and take a row whose bit was set already back out of its
    /// block's count, so a row counts once. A charge with no place, or past the extent, leaves its segment's bits inexact
    pub fn land(&self, rows: &[DeadRow], counts: &mut FixedList<DeadCount>) {
        let mut books = hold(&self.books);
        for row in rows {
            let Some(extent) = books.extents.get_mut(&row.segment) else {
                continue;
            };
            let Some((word, bit)) = row.at.is_known().then(|| word_of(extent, row.block, row.at.index)).flatten() else {
                self.inexact[INEXACT_PLACE].fetch_add(u64::from(extent.is_exact), Ordering::Relaxed);
                extent.is_exact = false;
                continue;
            };
            if self.words[word].fetch_or(bit, Ordering::AcqRel) & bit == 0 {
                continue;
            }
            if let Some(count) = counts.iter_mut().find(|count| count.segment == row.segment && count.block == row.block && count.rows > 0) {
                count.rows -= 1;
                count.graves -= u32::from(row.is_grave && count.graves > 0);
                count.bytes = ByteCount(count.bytes.0.saturating_sub(row.length));
            }
        }
    }

    /// Mark the segments a block charged inexact, when the block's rows didn't fit their list
    pub fn spoil(&self, counts: &[DeadCount]) {
        self.spoils.fetch_add(1, Ordering::AcqRel);
        let mut books = hold(&self.books);
        for count in counts {
            if let Some(extent) = books.extents.get_mut(&count.segment) {
                self.inexact[INEXACT_OVERFLOW].fetch_add(u64::from(extent.is_exact), Ordering::Relaxed);
                extent.is_exact = false;
            }
        }
    }

    /// Mark one segment's bits inexact for a cause, when counts land on it with no bits
    pub fn spoil_segment(&self, segment: SegmentNumber, cause: usize) {
        let mut books = hold(&self.books);
        if let Some(extent) = books.extents.get_mut(&segment).filter(|extent| extent.is_exact) {
            extent.is_exact = false;
            self.inexact[cause].fetch_add(1, Ordering::Relaxed);
            self.spoils.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Mark every segment's bits inexact, after damage or a rebuild, since a bit can't say which row set it and that row may be gone
    pub fn spoil_all(&self) {
        self.spoils.fetch_add(1, Ordering::AcqRel);
        let mut books = hold(&self.books);
        let mut spoiled = 0;
        for extent in books.extents.values_mut().filter(|extent| extent.is_exact) {
            extent.is_exact = false;
            spoiled += 1;
        }
        self.inexact[INEXACT_DAMAGE].fetch_add(spoiled, Ordering::Relaxed);
    }

    /// Count the set bits of one block, for the tests
    pub fn block_set(&self, segment: SegmentNumber, block: u32) -> Option<u32> {
        let extent = hold(&self.books).extents.get(&segment).copied()?;
        let start = (extent.span.start + block * extent.block_words) as usize;
        let words = self.words.get(start..start + extent.block_words as usize)?;
        Some(words.iter().map(|word| word.load(Ordering::Acquire).count_ones()).sum())
    }
}

impl DeadRow {
    /// Note one charged row
    pub fn of(segment: SegmentNumber, block: u32, at: RowAt, length: u64, kind: RowKind) -> Self {
        Self { segment, block, at, length, is_grave: kind == RowKind::Grave }
    }
}

/// Find a row's word and bit in an extent
pub fn word_of(extent: &BitsExtent, block: u32, index: u32) -> Option<(usize, u64)> {
    if block >= extent.block_count || index >= extent.block_words * WORD_BITS {
        return None;
    }
    let word = extent.span.start + block * extent.block_words + index / WORD_BITS;
    Some((word as usize, 1 << (index % WORD_BITS)))
}

/// Take the first free span that holds a length
pub fn take_span(free: &mut Vec<WordSpan>, length: u32) -> Option<WordSpan> {
    if length == 0 {
        return Some(WordSpan::default());
    }
    let index = free.iter().position(|span| span.length >= length)?;
    let found = free[index];
    if found.length == length {
        free.remove(index);
    } else {
        free[index] = WordSpan { start: found.start + length, length: found.length - length };
    }
    Some(WordSpan { start: found.start, length })
}

/// Give a span back, joining the free spans beside it
pub fn give_span(free: &mut Vec<WordSpan>, span: WordSpan) {
    if span.length == 0 {
        return;
    }
    let at = free.partition_point(|other| other.start < span.start);
    free.insert(at, span);
    if at + 1 < free.len() && free[at].start + free[at].length == free[at + 1].start {
        free[at].length += free[at + 1].length;
        free.remove(at + 1);
    }
    if at > 0 && free[at - 1].start + free[at - 1].length == free[at].start {
        free[at - 1].length += free[at].length;
        free.remove(at);
    }
}
