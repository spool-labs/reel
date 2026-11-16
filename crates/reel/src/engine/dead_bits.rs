//! One bit for each row the dead-byte walk has charged, for every segment born since open, so a merge can tell a dead row from a
//! live one with no lookup

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use reel_core::units::{ByteCount, Nanoseconds};

use crate::fixed_list::FixedList;
use crate::record::record_check::checksum;
use crate::guard::hold;
use crate::pool::{touched_atomics, touched_capacity};
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
    /// Bits or extents changed since the last save, so an idle planner saves them
    pub is_changed: AtomicBool,
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
        let mut free = touched_capacity(most_segments + 1, WordSpan::default());
        if words > 0 {
            free.push(WordSpan { start: 0, length: words as u32 });
        }
        Self {
            words: touched_atomics(words),
            books: Mutex::new(BitsBooks { extents: touched_map(most_segments), free }),
            inexact: Default::default(),
            spoils: AtomicU64::new(0),
            is_changed: AtomicBool::new(false),
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
        self.is_changed.store(true, Ordering::Release);
        true
    }

    /// Give a segment's extent back, when it leaves the table
    pub fn end(&self, segment: SegmentNumber) {
        let mut books = hold(&self.books);
        if let Some(old) = books.extents.remove(&segment) {
            give_span(&mut books.free, old.span);
        }
    }

    /// Check a segment has bits at all, exact or not. One with none was born when the pool was full, and each of its rows asks
    pub fn has_extent(&self, segment: SegmentNumber) -> bool {
        hold(&self.books).extents.contains_key(&segment)
    }

    /// Read a segment's extent while its bits answer for every row, or nothing
    pub fn exact(&self, segment: SegmentNumber) -> Option<BitsExtent> {
        hold(&self.books).extents.get(&segment).copied().filter(|extent| extent.is_exact)
    }

    /// Set one row's bit and say whether it was clear, or nothing when the segment has no exact bits with room for the row
    pub fn set_row(&self, segment: SegmentNumber, block: u32, index: u32) -> Option<bool> {
        let books = hold(&self.books);
        let (word, bit) = books.extents.get(&segment).filter(|extent| extent.is_exact).and_then(|extent| word_of(extent, block, index))?;
        self.is_changed.store(true, Ordering::Release);
        Some(self.words[word].fetch_or(bit, Ordering::AcqRel) & bit == 0)
    }

    /// Set the bits of a block's first so many rows, for a block a cover kills whole
    pub fn set_block(&self, segment: SegmentNumber, block: u32, rows: u32) {
        let books = hold(&self.books);
        let Some(extent) = books.extents.get(&segment).filter(|extent| extent.is_exact) else {
            return;
        };
        for index in 0..rows {
            if let Some((word, bit)) = word_of(extent, block, index) {
                self.words[word].fetch_or(bit, Ordering::AcqRel);
            }
        }
        self.is_changed.store(true, Ordering::Release);
    }

    /// Read one row's bit through an extent read before, or nothing when the row's place lies outside it
    pub fn is_set(&self, extent: &BitsExtent, block: u32, index: u32) -> Option<bool> {
        let (word, bit) = word_of(extent, block, index)?;
        Some(self.words[word].load(Ordering::Acquire) & bit != 0)
    }

    /// Set the bits of a block's charged rows as their counts land, and take a row whose bit was set already back out of its
    /// block's count, so a row counts once. A charge with no place, or past the extent, leaves its segment's bits inexact
    pub fn land(&self, rows: &[DeadRow], counts: &mut FixedList<DeadCount>) {
        self.is_changed.store(true, Ordering::Release);
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
            self.is_changed.store(true, Ordering::Release);
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


/// The bits file in a volume's first root, and the name a save writes before it replaces it
pub const DEAD_BITS_NAME: &str = "dead-bits";
pub const NEW_DEAD_BITS_NAME: &str = "dead-bits.new";
/// Words a save copies under one take of the books and writes with one checksum
pub const CHUNK_WORDS: usize = 8 * 1024;
/// Extent heads a save lists at a time
pub const SAVE_HEADS_PAGE: usize = 4096;
/// How long a volume goes between saves of its dead bits, beside a summary save. A starting value, tuned at the end
pub const DEAD_BITS_SAVE_PERIOD: Nanoseconds = Nanoseconds(60_000_000_000);
/// Bytes of an extent's head in the file: segment, blocks, words a block
pub const SAVED_HEAD_BYTES: usize = 8 + 4 + 4;

/// One extent's head, as a save writes it
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SavedHead {
    pub segment: SegmentNumber,
    pub block_count: u32,
    pub block_words: u32,
}

impl SavedHead {
    /// Write the head
    pub fn bytes(&self) -> [u8; SAVED_HEAD_BYTES] {
        let mut out = [0; SAVED_HEAD_BYTES];
        out[..8].copy_from_slice(&self.segment.0.to_le_bytes());
        out[8..12].copy_from_slice(&self.block_count.to_le_bytes());
        out[12..].copy_from_slice(&self.block_words.to_le_bytes());
        out
    }

    /// Read a head
    pub fn of(bytes: &[u8]) -> Option<Self> {
        Some(Self {
            segment: SegmentNumber(u64::from_le_bytes(bytes.get(..8)?.try_into().ok()?)),
            block_count: u32::from_le_bytes(bytes.get(8..12)?.try_into().ok()?),
            block_words: u32::from_le_bytes(bytes.get(12..16)?.try_into().ok()?),
        })
    }

    /// Count an extent's words
    pub fn words(&self) -> usize {
        self.block_count as usize * self.block_words as usize
    }
}

/// Check a chunk: its segment and place in the extent, then its words, so a chunk of another segment never passes
pub fn chunk_sum(segment: SegmentNumber, index: usize, words: &[u8]) -> u64 {
    let mut head = [0; 16];
    head[..8].copy_from_slice(&segment.0.to_le_bytes());
    head[8..].copy_from_slice(&(index as u64).to_le_bytes());
    checksum(&head).0 ^ checksum(words).0.rotate_left(17)
}

impl DeadBits {
    /// List a page of the exact extents' heads past the first so many, for a save to copy one at a time
    pub fn saved_heads(&self, out: &mut Vec<SavedHead>, skip: usize) {
        out.clear();
        let books = hold(&self.books);
        for (segment, extent) in books.extents.iter().filter(|(_segment, extent)| extent.is_exact).skip(skip) {
            if out.len() == out.capacity() {
                break;
            }
            out.push(SavedHead { segment: *segment, block_count: extent.block_count, block_words: extent.block_words });
        }
    }

    /// Copy one chunk of an extent's words while it stays the extent the head names, or nothing once it changed
    pub fn copy_chunk(&self, head: &SavedHead, index: usize, out: &mut Vec<u8>) -> bool {
        out.clear();
        let books = hold(&self.books);
        let Some(extent) = books.extents.get(&head.segment).filter(|extent| {
            extent.is_exact && extent.block_count == head.block_count && extent.block_words == head.block_words
        }) else {
            return false;
        };
        let first = index * CHUNK_WORDS;
        let last = (first + CHUNK_WORDS).min(head.words());
        for word in &self.words[extent.span.start as usize + first..extent.span.start as usize + last] {
            out.extend_from_slice(&word.load(Ordering::Acquire).to_le_bytes());
        }
        true
    }

    /// Load the extents a saved file holds, each only when every chunk's checksum passes and every block's set bits match the
    /// dead count the summary gives it, so bits a crash left behind or ahead of the counts stay missing and their rows ask
    pub fn restore(&self, bytes: &[u8], matches: impl Fn(SegmentNumber, u32, u32) -> bool) -> u64 {
        let mut loaded = 0;
        let mut at = 0;
        while let Some(head) = bytes.get(at..at + SAVED_HEAD_BYTES).and_then(SavedHead::of) {
            at += SAVED_HEAD_BYTES;
            let chunks = head.words().div_ceil(CHUNK_WORDS);
            let mut is_whole = self.begin_words(head.segment, head.block_count, head.block_words);
            let extent = hold(&self.books).extents.get(&head.segment).copied();
            for index in 0..chunks {
                let length = (head.words() - index * CHUNK_WORDS).min(CHUNK_WORDS) * size_of::<u64>();
                let (Some(words), Some(sum)) = (bytes.get(at..at + length), bytes.get(at + length..at + length + 8)) else {
                    return loaded;
                };
                at += length + 8;
                is_whole &= sum == chunk_sum(head.segment, index, words).to_le_bytes();
                let Some(extent) = extent.filter(|_extent| is_whole) else {
                    continue;
                };
                for (offset, chunk) in words.chunks_exact(size_of::<u64>()).enumerate() {
                    let word = u64::from_le_bytes(chunk.try_into().unwrap_or_default());
                    self.words[extent.span.start as usize + index * CHUNK_WORDS + offset].store(word, Ordering::Release);
                }
            }
            let is_matched = is_whole
                && (0..head.block_count).all(|block| self.block_set(head.segment, block).is_some_and(|set| matches(head.segment, block, set)));
            if is_matched {
                loaded += 1;
            } else {
                self.end(head.segment);
            }
        }
        loaded
    }

    /// Give a segment a clear extent laid out with so many words a block
    pub fn begin_words(&self, segment: SegmentNumber, block_count: u32, block_words: u32) -> bool {
        self.begin(segment, block_count, block_words.saturating_mul(WORD_BITS))
    }
}

/// Allocate the extents map with its buckets written, so open backs every page it reserves. A map's buckets come back
/// unwritten, and its first inserts after open would fault on a job's thread
pub fn touched_map(most_segments: usize) -> HashMap<SegmentNumber, BitsExtent> {
    let mut map = HashMap::with_capacity(most_segments);
    for number in 0..most_segments as u64 {
        map.insert(SegmentNumber(number), BitsExtent::default());
    }
    map.clear();
    map
}
