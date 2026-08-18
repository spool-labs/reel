//! What stays resident when the keys do not
//!
//! A paged index answers outright only for the segments no footer covers yet; for
//! everything else it answers with the segments the key could be in, and the caller
//! finds it in one binary search per footer. What stays resident is a key range per
//! column per sealed segment, so the footprint follows the segment count rather than
//! the record count. Ruling a segment out by its range only works on a column
//! written in key order; uniform keys need the filter the footer reserves room for.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::error::Result;
use crate::format::block::{FooterMap, RowBlock};
use crate::format::column::{ColumnId, KeyBytes};
use crate::format::footer::{FooterFind, FooterRow, SegmentFooter};
use crate::format::loc::SegmentId;
use crate::index::tbtreemap::{TBTreeMap, NODE_WIDTH};
use crate::sync::{read, write};

/// Segments one lookup carries without reaching for the heap
const CANDIDATES_INLINE: usize = 4;

/// Pools the footer cache splits its bound across: footers, directories, blocks
const POOLS: usize = 3;

/// Where a paged index reads the footers it resolves its sealed keys through
///
/// The index owns which segments to ask and what an answer means; io is the one
/// thing it asks somebody else for.
pub trait FooterSource: Send + Sync {
    /// One sealed segment's footer, parsed
    ///
    /// The whole footer rather than one row, because a playback cannot ask by key.
    fn footer(&self, segment: SegmentId) -> Result<Option<Arc<SegmentFooter>>>;

    /// What a sealed segment's footer says about a key, if it says anything
    ///
    /// A caller wanting the value the row carries passes a buffer for it, left
    /// empty by a row that carries none.
    fn find(
        &self,
        segment: SegmentId,
        column: ColumnId,
        key: &[u8],
        carry: Option<&mut Vec<u8>>,
    ) -> Result<Option<FooterRow>> {
        let Some(footer) = self.footer(segment)? else {
            return Ok(None);
        };
        let Some(partition) = footer
            .partitions
            .iter()
            .find(|partition| partition.column == column)
        else {
            return Ok(None);
        };
        match partition.lookup(key, carry)? {
            FooterFind::Found(row) => Ok(Some(row)),
            FooterFind::RuledOut | FooterFind::Missing => Ok(None),
        }
    }
}

/// One sealed segment's key range, and how far the ranges before it reach
///
/// The runs are sorted by their low end, but a range starting earlier can still
/// reach past a key's place and hold it. The furthest reach at or before each run
/// says when to stop: once that is below the key, nothing earlier can hold it.
struct Run {
    /// The segment's low and high key, shared with the segment-ordered view
    span: Arc<Span>,

    /// Which run at or before this one reaches furthest, as an index into the runs
    reach: u32,

    /// The segment those keys were sealed into
    segment: SegmentId,
}

/// One sealed segment's low and high key, owned once and pointed at twice
struct Span {
    lowest: KeyBytes,
    highest: KeyBytes,
}

/// The sealed segments holding one column, and the keys each of them covers
///
/// Held twice over, by segment number and sorted by key. Seals are rare and lookups
/// are not, so the key-ordered view is rebuilt on the seal.
#[derive(Default)]
pub struct SealedRanges {
    /// The sealed set, held both ways
    ranges: RwLock<Sealed>,

    /// Counted up whenever the set changes, so a playback knows its cursors are stale
    generation: AtomicU64,
}

/// What one column knows about its sealed segments
#[derive(Default)]
struct Sealed {
    /// By segment, which is what a retire names; the option is how the tree fills a
    /// node's value array and is never None here
    by_segment: TBTreeMap<SegmentId, NODE_WIDTH, Option<Arc<Span>>>,

    /// The same ranges sorted by their low end, with the reach carried along
    by_key: Vec<Run>,
}

impl Sealed {
    /// Rebuild the key-ordered view, which the two mutations share
    fn reindex(&mut self) {
        self.by_key = self
            .by_segment
            .iter()
            .filter_map(|(segment, span)| {
                span.as_ref().map(|span| Run {
                    span: Arc::clone(span),
                    reach: 0,
                    segment: *segment,
                })
            })
            .collect();
        self.by_key.sort_by(|left, right| {
            left.span
                .lowest
                .as_slice()
                .cmp(right.span.lowest.as_slice())
        });

        // The furthest reach at or before each run, carried as the index of the run
        // holding it rather than as a third copy of the key.
        let mut furthest = 0usize;
        for at in 0..self.by_key.len() {
            let held = self.by_key[furthest].span.highest.as_slice();
            let mine = self.by_key[at].span.highest.as_slice();
            if at == 0 || mine > held {
                furthest = at;
            }
            self.by_key[at].reach = furthest as u32;
        }
    }

    /// Every run that could hold a key, in the order they are held
    ///
    /// The search lands past the last run that starts at or below the key, then
    /// walks back while anything before it still reaches far enough.
    fn covering(&self, key: &[u8], mut take: impl FnMut(SegmentId)) {
        let past = self
            .by_key
            .partition_point(|run| run.span.lowest.as_slice() <= key);
        for run in self.by_key[..past].iter().rev() {
            if self.by_key[run.reach as usize].span.highest.as_slice() < key {
                return;
            }
            if run.span.highest.as_slice() >= key {
                take(run.segment);
            }
        }
    }
}

impl SealedRanges {
    /// An empty set of sealed segments
    pub fn new() -> SealedRanges {
        SealedRanges::default()
    }

    /// Record what one newly sealed segment covers for this column
    ///
    /// A segment seals once, so a repeat is a rebuild seeing what it already saw
    /// and replaces rather than duplicates.
    pub fn note(&self, segment: SegmentId, lowest: KeyBytes, highest: KeyBytes) {
        let mut sealed = write(&self.ranges);
        sealed
            .by_segment
            .insert(segment, Some(Arc::new(Span { lowest, highest })));
        sealed.reindex();
        drop(sealed);
        self.generation.fetch_add(1, Ordering::Release);
    }

    /// Replace every span with what a rebuild swept out of the footers
    ///
    /// Nothing is kept, since a rebuild is the authority on what is on disk.
    pub fn replace(&self, spans: Vec<(SegmentId, KeyBytes, KeyBytes)>) {
        let mut sealed = write(&self.ranges);
        sealed.by_segment.clear();
        for (segment, lowest, highest) in spans {
            sealed
                .by_segment
                .insert(segment, Some(Arc::new(Span { lowest, highest })));
        }
        sealed.reindex();
        drop(sealed);
        self.generation.fetch_add(1, Ordering::Release);
    }

    /// Forget a segment the compactor has retired
    ///
    /// One retired segment is offered to every column, and most of them never held
    /// it, so the generation moves only where the set actually changed.
    pub fn forget(&self, segment: SegmentId) {
        let mut sealed = write(&self.ranges);
        // Packed, since the segment numbers climb and the compactor retires from the
        // low end, where a bare removal leaves the emptied leaves behind in the chain
        // reindex walks on every seal and every forget.
        if sealed.by_segment.remove_packed(&segment).is_none() {
            return;
        }
        sealed.reindex();
        drop(sealed);
        self.generation.fetch_add(1, Ordering::Release);
    }

    /// How many times this column's sealed set has changed
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// The segments a key could be in, newest first
    ///
    /// Newest first is load-bearing: a compaction copy carries its source's sequence
    /// number, so while both segments are sealed the two rows tie, and this order
    /// breaks the tie for the copy.
    pub fn candidates(&self, key: &[u8]) -> Candidates {
        let mut found = Candidates::default();
        read(&self.ranges).covering(key, |segment| found.push(segment));
        // The search walks the runs by key, so what it finds is in no segment order.
        found.newest_first();
        found
    }

    /// Every segment this column has sealed, in order
    ///
    /// One lock and one copy rather than a lock per question, for a prune asking
    /// about every grave it holds. Sorted, since the map it comes from is.
    pub fn segments(&self) -> Vec<SegmentId> {
        read(&self.ranges)
            .by_segment
            .iter()
            .map(|(segment, _)| *segment)
            .collect()
    }

    /// Whether any sealed segment holds keys inside a half-open range
    ///
    /// The one caller is a range delete, whose end is exclusive, which is why this
    /// is not the closed test below.
    pub fn overlaps(&self, low: &[u8], high: Option<&[u8]>) -> bool {
        read(&self.ranges)
            .by_segment
            .iter()
            .filter_map(|(_, span)| span.as_ref())
            .any(|span| {
                span.highest.as_slice() >= low
                    && high.is_none_or(|high| span.lowest.as_slice() < high)
            })
    }

    /// The segments whose keys fall inside a range, oldest first
    ///
    /// What a playback asks for, since it crosses keys rather than landing on one.
    /// The order is the segments' own, which a merge preferring the highest sequence
    /// number never has to think about.
    pub fn spanning(&self, low: Option<&[u8]>, high: Option<&[u8]>) -> Vec<SegmentId> {
        read(&self.ranges)
            .by_segment
            .iter()
            .filter(|(_, span)| span.as_ref().is_some_and(|span| reaches(span, low, high)))
            .map(|(segment, _)| *segment)
            .collect()
    }

    /// Whether this column has a footer covering one segment
    ///
    /// Asked before anything takes a key out of the map on a paging column: a record
    /// in a segment no footer covers has nothing behind it, so removing the key
    /// removes the record with it.
    pub fn holds(&self, segment: SegmentId) -> bool {
        read(&self.ranges).by_segment.contains_key(&segment)
    }

    /// Whether any segment has been sealed for this column
    pub fn is_empty(&self) -> bool {
        read(&self.ranges).by_segment.is_empty()
    }

    /// Sealed segments recorded for this column
    pub fn len(&self) -> usize {
        read(&self.ranges).by_segment.len()
    }
}

/// A short run of segment numbers, held without allocating while it stays short
///
/// One or two on a column whose segments cover disjoint ranges, every segment on
/// the volume on one whose keys are uniform.
#[derive(Default)]
pub struct Candidates {
    /// The first few, held in the answer itself
    inline: [SegmentId; CANDIDATES_INLINE],

    /// How many of those are filled
    held: usize,

    /// The rest, once there are more than the answer carries
    spilled: Vec<SegmentId>,
}

impl Candidates {
    fn push(&mut self, segment: SegmentId) {
        match self.held < CANDIDATES_INLINE {
            true => {
                self.inline[self.held] = segment;
                self.held += 1;
            }
            false => self.spilled.push(segment),
        }
    }

    /// The segments, in the order they were found
    pub fn iter(&self) -> impl Iterator<Item = SegmentId> + '_ {
        self.inline[..self.held]
            .iter()
            .chain(self.spilled.iter())
            .copied()
    }

    /// Whether the key was ruled out of every sealed segment
    pub fn is_empty(&self) -> bool {
        self.held == 0
    }

    /// Segments the key was not ruled out of
    pub fn len(&self) -> usize {
        self.held + self.spilled.len()
    }

    /// Put the newest segment first, which is what a tie between two rows wants
    fn newest_first(&mut self) {
        self.inline[..self.held.min(CANDIDATES_INLINE)].sort_unstable_by(|a, b| b.cmp(a));
        self.spilled.sort_unstable_by(|a, b| b.cmp(a));
        // A spilled run holds the segments the inline part could not, and both are
        // now descending, so the larger ones have to come first overall.
        if !self.spilled.is_empty() {
            let mut all: Vec<SegmentId> = self.inline[..self.held]
                .iter()
                .copied()
                .chain(self.spilled.drain(..))
                .collect();
            all.sort_unstable_by(|a, b| b.cmp(a));
            self.held = 0;
            for segment in all {
                self.push(segment);
            }
        }
    }
}

/// Whether one segment's key range meets a closed range, either end open
fn reaches(span: &Span, low: Option<&[u8]>, high: Option<&[u8]>) -> bool {
    low.is_none_or(|low| span.highest.as_slice() >= low)
        && high.is_none_or(|high| span.lowest.as_slice() <= high)
}

/// Parsed footers of sealed segments, kept so a repeated search rereads nothing
///
/// A footer is the sorted index of its own segment, so the first search costs two
/// reads and every search after it costs none. Bounded, since the point of a paged
/// index is that resident memory stops following the volume.
pub struct FooterCache {
    /// Bytes each of the three pools will hold at once, a third of the knob apiece
    share: usize,

    /// What the cache is holding, behind one lock
    held: RwLock<FooterHeld>,
}

#[derive(Default)]
struct FooterHeld {
    /// Parsed footers, by the segment they came from
    footers: HashMap<SegmentId, Arc<SegmentFooter>>,

    /// Directories of segments read a block at a time, held to a share of their own
    maps: HashMap<SegmentId, Arc<FooterMap>>,

    /// Bytes the held directories weigh, which is almost all filter
    map_bytes: usize,

    /// The order they were taken in, which is the order they are given up
    map_order: VecDeque<SegmentId>,

    /// Blocks of rows read on demand, by the segment and column they came from
    blocks: HashMap<(SegmentId, ColumnId, usize), Arc<RowBlock>>,

    /// Bytes the blocks weigh, held to their own share of the bound
    block_bytes: usize,

    /// The order blocks were taken in, which is the order they are given up
    block_order: VecDeque<(SegmentId, ColumnId, usize)>,

    /// Bytes the held footers add up to, so the bound is on what they weigh
    bytes: usize,

    /// The order they were taken in, which is the order they are given up
    taken: VecDeque<SegmentId>,
}

impl FooterCache {
    /// A cache holding at most this many bytes of parsed footer state
    ///
    /// Bytes rather than a count, since a footer is sized by its segment's key count.
    /// The footers, the directories and the blocks take a third of it each, so the
    /// number asked for is what all three together weigh rather than what one does.
    /// A bound too small for a single entry holds nothing, which is what asking for
    /// no cache on a paged volume means.
    pub fn new(capacity: usize) -> FooterCache {
        FooterCache {
            share: capacity / POOLS,
            held: RwLock::new(FooterHeld::default()),
        }
    }

    /// Bytes the cache is holding across its three pools
    pub fn held_bytes(&self) -> usize {
        let (footers, maps, blocks) = self.held_split();
        footers + maps + blocks
    }

    /// Bytes held per pool: footers, directories, blocks
    pub fn held_split(&self) -> (usize, usize, usize) {
        let held = read(&self.held);
        (held.bytes, held.map_bytes, held.block_bytes)
    }

    /// The footer of a segment, if it is still held
    pub fn get(&self, segment: SegmentId) -> Option<Arc<SegmentFooter>> {
        read(&self.held).footers.get(&segment).cloned()
    }

    /// The directory of a segment, if it is still held
    pub fn map_of(&self, segment: SegmentId) -> Option<Arc<FooterMap>> {
        read(&self.held).maps.get(&segment).cloned()
    }

    /// Hold a segment's directory, giving up the one taken longest ago when full
    ///
    /// Losing one costs the next reader a directory read and a segment it cannot
    /// rule out, never a wrong answer.
    pub fn insert_map(&self, segment: SegmentId, map: Arc<FooterMap>) {
        let weight = map.weight();
        if weight > self.share {
            return;
        }
        let mut held = write(&self.held);
        if held.maps.contains_key(&segment) {
            return;
        }
        while held.map_bytes + weight > self.share {
            let Some(oldest) = held.map_order.pop_front() else {
                break;
            };
            if let Some(given) = held.maps.remove(&oldest) {
                held.map_bytes = held.map_bytes.saturating_sub(given.weight());
            }
        }
        held.map_bytes += weight;
        held.maps.insert(segment, map);
        held.map_order.push_back(segment);
    }

    /// One block of a column's rows, if it is still held
    pub fn block_of(
        &self,
        segment: SegmentId,
        column: ColumnId,
        at: usize,
    ) -> Option<Arc<RowBlock>> {
        read(&self.held).blocks.get(&(segment, column, at)).cloned()
    }

    /// Hold one block, giving up the ones taken longest ago if the cache is full
    pub fn insert_block(
        &self,
        segment: SegmentId,
        column: ColumnId,
        at: usize,
        block: Arc<RowBlock>,
    ) {
        let weight = block.weight();
        if weight > self.share {
            return;
        }
        let key = (segment, column, at);
        let mut held = write(&self.held);
        if held.blocks.contains_key(&key) {
            return;
        }
        while held.block_bytes + weight > self.share {
            let Some(oldest) = held.block_order.pop_front() else {
                break;
            };
            if let Some(given) = held.blocks.remove(&oldest) {
                held.block_bytes = held.block_bytes.saturating_sub(given.weight());
            }
        }
        held.block_bytes += weight;
        held.blocks.insert(key, block);
        held.block_order.push_back(key);
    }

    /// Hold a footer, giving up the one taken longest ago if the cache is full
    ///
    /// One weighing more than the pool is turned away rather than taken in alone: the
    /// caller keeps the footer it just read either way, so admitting it would empty
    /// the pool for a tenant that fits nothing beside it.
    pub fn insert(&self, segment: SegmentId, footer: Arc<SegmentFooter>) {
        let weight = footer.encoded_len();
        if weight > self.share {
            return;
        }
        let mut held = write(&self.held);
        if held.footers.contains_key(&segment) {
            return;
        }
        while held.bytes + weight > self.share {
            let Some(oldest) = held.taken.pop_front() else {
                break;
            };
            if let Some(given) = held.footers.remove(&oldest) {
                held.bytes = held.bytes.saturating_sub(given.encoded_len());
            }
        }
        held.bytes += weight;
        held.footers.insert(segment, footer);
        held.taken.push_back(segment);
    }

    /// Give up a segment's footer, for one the compactor has retired
    pub fn forget(&self, segment: SegmentId) {
        let mut held = write(&self.held);
        if let Some(given) = held.footers.remove(&segment) {
            held.bytes = held.bytes.saturating_sub(given.encoded_len());
        }
        held.taken.retain(|taken| *taken != segment);
        if let Some(given) = held.maps.remove(&segment) {
            held.map_bytes = held.map_bytes.saturating_sub(given.weight());
        }
        held.map_order.retain(|taken| *taken != segment);
        // A retired segment's blocks name bytes in a file that is gone, so they go
        // with it rather than waiting to be evicted by pressure.
        held.block_order.retain(|key| key.0 != segment);
        let mut given = 0usize;
        held.blocks.retain(|key, block| {
            if key.0 == segment {
                given += block.weight();
                return false;
            }
            true
        });
        held.block_bytes = held.block_bytes.saturating_sub(given);
    }

    /// Give up every footer, for a reader rebuilding its view of the volume
    pub fn clear(&self) {
        let mut held = write(&self.held);
        held.footers.clear();
        held.taken.clear();
        held.bytes = 0;
        held.maps.clear();
        held.map_order.clear();
        held.map_bytes = 0;
        held.blocks.clear();
        held.block_order.clear();
        held.block_bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The segments a key could be in, as a list the assertions can compare
    fn found(ranges: &SealedRanges, key: &[u8]) -> Vec<SegmentId> {
        ranges.candidates(key).iter().collect()
    }

    /// A sealed footer listing this many rows, for weighing the cache
    fn footer_of(rows: u8) -> Arc<SegmentFooter> {
        use crate::format::footer::FooterEntry;
        use crate::format::lsn::Lsn;
        use crate::format::record::Flags;

        let entries: Vec<FooterEntry> = (0..rows)
            .map(|byte| {
                let key = crate::format::column::RecordKey::from_bytes(ColumnId(1), &[byte; 8])
                    .expect("key");
                FooterEntry::new(key, Lsn(u64::from(byte) + 1), 0, 100, Flags::DATA)
            })
            .collect();
        let mut footer = SegmentFooter::build(entries);
        let _ = footer.pack(0);
        Arc::new(footer)
    }

    fn key(byte: u8) -> KeyBytes {
        KeyBytes::new(&[byte, 0, 0, 0, 0, 0, 0, 0]).expect("key")
    }

    // a key is looked for only in the segments whose range could hold it
    #[test]
    fn rules_out() {
        let ranges = SealedRanges::new();
        ranges.note(SegmentId(1), key(0), key(9));
        ranges.note(SegmentId(2), key(10), key(19));
        ranges.note(SegmentId(3), key(20), key(29));

        assert_eq!(
            found(&ranges, &[15, 0, 0, 0, 0, 0, 0, 0]),
            vec![SegmentId(2)]
        );
        assert!(ranges.candidates(&[99, 0, 0, 0, 0, 0, 0, 0]).is_empty());
    }

    // a range that starts early and reaches far is still found behind a later one
    #[test]
    fn a_wide_range_is_found_behind_narrow_ones() {
        let ranges = SealedRanges::new();
        ranges.note(SegmentId(1), key(0), key(90));
        ranges.note(SegmentId(2), key(10), key(19));
        ranges.note(SegmentId(3), key(20), key(29));

        assert_eq!(
            found(&ranges, &[25, 0, 0, 0, 0, 0, 0, 0]),
            vec![SegmentId(3), SegmentId(1)],
            "the wide one covers the key too, and the newest comes first"
        );
        assert_eq!(
            found(&ranges, &[95, 0, 0, 0, 0, 0, 0, 0]),
            Vec::new(),
            "and past every reach nothing is looked at"
        );
    }

    // a key below every range finds nothing without walking them
    #[test]
    fn below_every_range() {
        let ranges = SealedRanges::new();
        ranges.note(SegmentId(1), key(10), key(19));
        ranges.note(SegmentId(2), key(20), key(29));

        assert_eq!(found(&ranges, &[5, 0, 0, 0, 0, 0, 0, 0]), Vec::new());
    }

    // a key written twice across a seal is looked for newest first
    #[test]
    fn newest_first() {
        let ranges = SealedRanges::new();
        ranges.note(SegmentId(1), key(0), key(50));
        ranges.note(SegmentId(4), key(0), key(50));

        assert_eq!(
            found(&ranges, &[10, 0, 0, 0, 0, 0, 0, 0]),
            vec![SegmentId(4), SegmentId(1)],
            "the newer segment is asked first"
        );
    }

    // the cache gives footers up by what they weigh, not by how many there are
    #[test]
    fn footers_are_held_by_weight() {
        let small = footer_of(4);
        let large = footer_of(64);
        // The footer pool takes a third of the knob, and this leaves it room for the
        // small ones several times over but not for two large.
        let pool = large.encoded_len() + small.encoded_len();
        let cache = FooterCache::new(pool * POOLS);

        cache.insert(SegmentId(1), Arc::clone(&small));
        cache.insert(SegmentId(2), Arc::clone(&small));
        assert!(cache.get(SegmentId(1)).is_some(), "two small ones both fit");
        assert!(cache.get(SegmentId(2)).is_some());

        cache.insert(SegmentId(3), Arc::clone(&large));
        assert!(cache.get(SegmentId(3)).is_some(), "the large one is held");
        assert!(
            cache.get(SegmentId(1)).is_none(),
            "and the oldest was given up to make room for its bytes"
        );
        assert!(cache.held_bytes() <= pool);
    }

    // a footer larger than the pool is turned away instead of emptying it
    #[test]
    fn an_oversized_footer_keeps_the_pool() {
        let small = footer_of(4);
        let large = footer_of(64);
        // A footer pool of exactly two small ones, which the large one is well past.
        let cache = FooterCache::new(2 * small.encoded_len() * POOLS);
        assert!(
            large.encoded_len() > 2 * small.encoded_len(),
            "the large footer has to be the one that does not fit"
        );

        cache.insert(SegmentId(1), Arc::clone(&small));
        cache.insert(SegmentId(2), Arc::clone(&small));
        cache.insert(SegmentId(3), Arc::clone(&large));

        assert!(
            cache.get(SegmentId(3)).is_none(),
            "the oversized one was not taken in"
        );
        assert!(
            cache.get(SegmentId(1)).is_some(),
            "and refusing it cost the pool nothing"
        );
        assert!(cache.get(SegmentId(2)).is_some());
        assert_eq!(cache.held_bytes(), 2 * small.encoded_len());
    }

    // a bound too small for one entry holds nothing rather than one of everything
    #[test]
    fn a_bound_of_nothing_holds_nothing() {
        let large = footer_of(64);
        let cache = FooterCache::new(0);

        cache.insert(SegmentId(1), Arc::clone(&large));

        assert!(
            cache.get(SegmentId(1)).is_none(),
            "asking for no cache got one"
        );
        assert_eq!(cache.held_bytes(), 0);
    }

    // a retired segment stops being searched
    #[test]
    fn forgotten_segment() {
        let ranges = SealedRanges::new();
        ranges.note(SegmentId(1), key(0), key(9));
        ranges.note(SegmentId(2), key(0), key(9));

        ranges.forget(SegmentId(1));

        assert_eq!(
            found(&ranges, &[5, 0, 0, 0, 0, 0, 0, 0]),
            vec![SegmentId(2)]
        );
    }

    // sealing the same segment twice records it once, which a rebuild does
    #[test]
    fn reseal_replaces() {
        let ranges = SealedRanges::new();
        ranges.note(SegmentId(7), key(0), key(9));
        ranges.note(SegmentId(7), key(0), key(20));

        assert_eq!(
            found(&ranges, &[15, 0, 0, 0, 0, 0, 0, 0]),
            vec![SegmentId(7)]
        );
    }
}
