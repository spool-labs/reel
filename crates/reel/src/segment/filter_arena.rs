//! Filter blocks held in memory while they fit, so a get that misses reads nothing
//!
//! A segment's filters go in as one extent: a sorted list of its blocks, then their bits. An extent shows only once it's whole,
//! and it leaves with its segment. A segment that doesn't fit stays out, and its filters go through the block cache as before.

use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use reel_api::units::ByteCount;

use crate::little_endian::read_half;
use crate::pool::{touched_capacity, touched_filled};
use crate::segment::segment_table::NumberMap;
use crate::segment::segment_tail::SegmentNumber;

/// Bytes of one block's line in an extent: its filter's offset in the file, and where its bits start and end
pub const FILTER_LINE_BYTES: usize = 3 * size_of::<u32>();

/// Bytes of the tables behind one segment: its extent, its place on two free lists, and at most four map slots
pub const SEGMENT_TABLE_BYTES: usize =
    size_of::<FilterExtent>() + size_of::<u32>() + size_of::<ArenaSpan>() + 4 * (size_of::<SegmentNumber>() + size_of::<u32>());
/// The tables take at most one part in this many of the share, so a small share holds fewer segments
pub const TABLE_SHARE_DIVISOR: usize = 8;

/// A run of the arena's bytes
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ArenaSpan {
    pub start: u32,
    pub length: u32,
}

/// One segment's filters
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FilterExtent {
    pub span: ArenaSpan,
    pub blocks: u32,
    pub bits_used: u32,
    pub lines_used: u32,
}

/// What the arena holds
#[derive(Debug)]
pub struct ArenaState {
    bytes: Vec<u8>,
    map: NumberMap,
    extents: Vec<FilterExtent>,
    free_extents: Vec<u32>,
    free: Vec<ArenaSpan>,
}

/// The filters of every volume's segments that fit
#[derive(Debug)]
pub struct FilterArena {
    state: RwLock<ArenaState>,
}

impl ArenaState {
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

    /// Find a segment's extent
    pub fn extent_of(&self, segment: SegmentNumber) -> Option<&FilterExtent> {
        self.extents.get(self.map.get(segment)? as usize)
    }

    /// Find the bits of a block in an extent, by a search of its sorted lines
    pub fn bits_of(&self, extent: &FilterExtent, offset: u32) -> Option<&[u8]> {
        let lines = extent.span.start as usize;
        let (mut low, mut high) = (0, extent.blocks as usize);
        while low < high {
            let middle = (low + high) / 2;
            let at = lines + middle * FILTER_LINE_BYTES;
            let line_offset = read_half(&self.bytes, at)?;
            if line_offset == offset {
                let start = read_half(&self.bytes, at + size_of::<u32>())? as usize;
                let end = read_half(&self.bytes, at + 2 * size_of::<u32>())? as usize;
                return self.bytes.get(start..end);
            }
            if line_offset < offset {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        None
    }
}

impl FilterArena {
    /// Allocate and touch the arena, with room for a number of segments
    pub fn new(share: ByteCount, most_segments: usize) -> Self {
        let most_segments = most_segments.min(share.0 as usize / TABLE_SHARE_DIVISOR / SEGMENT_TABLE_BYTES);
        let tables = (most_segments * SEGMENT_TABLE_BYTES) as u64;
        let length = (share.0 - tables).min(u64::from(u32::MAX)) as u32;
        let mut free = touched_capacity(most_segments + 2, ArenaSpan::default());
        if length > 0 {
            free.push(ArenaSpan { start: 0, length });
        }
        Self {
            state: RwLock::new(ArenaState {
                bytes: touched_filled(length as usize, 0),
                map: NumberMap::new(most_segments),
                extents: vec![FilterExtent::default(); most_segments],
                free_extents: (0..most_segments as u32).rev().collect(),
                free,
            }),
        }
    }

    /// Read the arena
    pub fn read(&self) -> RwLockReadGuard<'_, ArenaState> {
        match self.state.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Change the arena
    pub fn write(&self) -> RwLockWriteGuard<'_, ArenaState> {
        match self.state.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Read the heap the arena holds
    pub fn heap_bytes(&self) -> ByteCount {
        let state = self.read();
        let extents = state.extents.capacity() * size_of::<FilterExtent>();
        let lists = state.free.capacity() * size_of::<ArenaSpan>() + state.free_extents.capacity() * size_of::<u32>();
        ByteCount((state.bytes.capacity() + extents + lists + state.map.heap_bytes()) as u64)
    }

    /// Ask a segment's filter for a block, or return nothing when the arena doesn't hold it
    pub fn with_filter<Answer>(&self, segment: SegmentNumber, offset: u32, visit: impl FnOnce(&[u8]) -> Answer) -> Option<Answer> {
        let state = self.read();
        let extent = state.extent_of(segment)?;
        if extent.lines_used < extent.blocks {
            return None;
        }
        state.bits_of(extent, offset).map(visit)
    }

    /// Check a segment's extent stands, whole or filling
    pub fn holds(&self, segment: SegmentNumber) -> bool {
        self.read().extent_of(segment).is_some()
    }

    /// Set aside an extent for a segment's blocks and bits, or say there's no room
    pub fn begin(&self, segment: SegmentNumber, blocks: u32, bits: u32) -> bool {
        let mut state = self.write();
        let Some(slot) = state.free_extents.last().copied() else {
            return false;
        };
        let Some(span) = state.take_span(blocks * FILTER_LINE_BYTES as u32 + bits) else {
            return false;
        };
        state.free_extents.pop();
        state.extents[slot as usize] = FilterExtent { span, blocks, bits_used: 0, lines_used: 0 };
        state.map.insert(segment, slot);
        true
    }

    /// Add the next block in file order, and the extent shows once its last block is in
    pub fn add(&self, segment: SegmentNumber, offset: u32, bits: &[u8]) {
        let mut state = self.write();
        let Some(slot) = state.map.get(segment) else {
            return;
        };
        let mut extent = state.extents[slot as usize];
        let bits_at = extent.span.start + extent.blocks * FILTER_LINE_BYTES as u32 + extent.bits_used;
        let line_at = (extent.span.start + extent.lines_used * FILTER_LINE_BYTES as u32) as usize;
        let end = bits_at + bits.len() as u32;
        state.bytes[bits_at as usize..end as usize].copy_from_slice(bits);
        state.bytes[line_at..line_at + 4].copy_from_slice(&offset.to_le_bytes());
        state.bytes[line_at + 4..line_at + 8].copy_from_slice(&bits_at.to_le_bytes());
        state.bytes[line_at + 8..line_at + 12].copy_from_slice(&end.to_le_bytes());
        extent.bits_used += bits.len() as u32;
        extent.lines_used += 1;
        state.extents[slot as usize] = extent;
    }

    /// Take a segment's extent out
    pub fn drop_segment(&self, segment: SegmentNumber) {
        let mut state = self.write();
        let Some(slot) = state.map.get(segment) else {
            return;
        };
        let span = state.extents[slot as usize].span;
        state.map.remove(segment);
        state.free_extents.push(slot);
        if span.length > 0 {
            state.give_span(span);
        }
    }
}
