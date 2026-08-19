//! The volume's directory of append tails
//!
//! A reel owns every segment file on the volume, the append sequence counter that
//! orders its records, and the monotonic segment numbering its tails draw from.
//! Writes route to the least-loaded tail. Every column shares the one log, so a
//! batch spanning columns is one durability point and one recovery domain.

pub mod bias;
pub mod checkpoint;
pub mod cue;
pub mod payload;
mod read;
pub mod segment;
pub mod tail;
pub mod volumes;

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use crate::append::admission::InflightBudget;
use crate::append::{Appender, BatchRecord, Commit, Committed};
use crate::config::{PointReads, RangedReads, ReelConfig};
use crate::error::{ReelError, Result};
use std::sync::OnceLock;

use crate::format::block::{lookup_in_span, FooterMap, RowBlock};
use crate::format::column::{ColumnId, ColumnSet, KeyRef, RecordKey};
use crate::format::fence::{FenceCut, FenceReach};
use crate::format::footer::{FooterFind, FooterRow, FooterTally, SegmentFooter};
use crate::format::loc::{Loc, SegmentId};
use crate::format::lsn::{Lsn, LsnCounter};
use crate::format::record::HEADER_LEN;
use crate::index::counters::{FilterProbes, SegmentTable};
use crate::index::paged::{FooterCache, FooterSource};
use crate::index::recovery::read_footer;
use crate::index::tbtreemap::{TBTreeMap, NODE_WIDTH};
use crate::io::op::{Advice, ColdRoute, Completion, FileId, Op, WarmFirst};
use crate::reel::segment::{DirectOpen, FdCache, IoDriver, SegmentHandle, SplitRead};
use crate::sync::{lock, read, write};

use reel_core::Value;

pub use read::coded_range;
use read::{
    check_in_block, deep_range, frame_to_range, frame_to_read, framed_or_nothing, merge_runs_into,
    near_range, window_or_nothing, window_start, Planned, Run, MERGE_GAP,
};

/// The first segment number a fresh reel numbers from
const FIRST_SEGMENT: u32 = 1;

/// Slots in the lookup from a column identifier to what it declared
const COLUMN_SLOTS: usize = 256;

/// Width of the zero-padded segment number in a file name
const SEGMENT_DIGITS: usize = 6;

/// Suffix every segment file carries
pub const SEGMENT_SUFFIX: &str = ".reel";

/// Record bytes below which a window keeps the page cache
const DIRECT_RECORD_FLOOR: u32 = 1024 * 1024;

/// Cold reads already in flight before a window may go around the page cache
///
/// Direct's only win is a large record under concurrent pressure, and a lone reader
/// is the case it loses.
const DIRECT_DEPTH_FLOOR: u64 = 2;

/// The footers a paged index resolves its sealed keys through
impl FooterSource for ReelShared {
    fn footer(&self, segment: SegmentId) -> Result<Option<Arc<SegmentFooter>>> {
        self.footer_of(segment)
    }

    /// One key, answered by reading the blocks the search touches and no more
    fn find(
        &self,
        segment: SegmentId,
        column: ColumnId,
        key: &[u8],
        carry: Option<&mut Vec<u8>>,
    ) -> Result<Option<FooterRow>> {
        self.probes.note_probe();
        if let Some(footer) = self.footers.get(segment) {
            let Some(partition) = footer
                .partitions
                .iter()
                .find(|partition| partition.column == column)
            else {
                return Ok(None);
            };
            return Ok(self.answer_of(partition.lookup(key, carry)?));
        }

        let Some(map) = self.footer_map_of(segment)? else {
            return Ok(None);
        };
        let Some((span, filter)) = map.locate(column) else {
            return Ok(None);
        };
        // The descriptor is resolved inside the loader, so a segment the filter
        // rules out costs no io. A segment gone by then ends the search as missing.
        let mut handle = None;
        let outcome = lookup_in_span(
            &span,
            filter,
            key,
            || self.fence_cut(&map, column, key, segment),
            |at| {
                self.probes.note_block();
                if let Some(block) = self.footers.block_of(segment, column, at) {
                    return Ok(Some(block));
                }
                let opened = match handle.as_ref() {
                    Some(opened) => opened,
                    None => match self.handle_for(segment)? {
                        Some(opened) => handle.insert(opened),
                        None => return Ok(None),
                    },
                };
                self.probes.note_block_read();
                let block = Arc::new(RowBlock::read(
                    &self.driver,
                    opened.file(),
                    &span,
                    at,
                    map.restarts_of(span.column),
                )?);
                self.footers
                    .insert_block(segment, column, at, Arc::clone(&block));
                Ok(Some(block))
            },
            carry,
        )?;
        Ok(self.answer_of(outcome))
    }
}

/// State every tail of the reel shares
pub struct ReelShared {
    /// The volume roots the reel's segment files live across
    pub volumes: crate::reel::volumes::Volumes,

    /// Append sequence counter that orders the reel's records
    pub lsn: LsnCounter,

    /// Ring-shaped backend every tail submits through
    pub driver: Arc<IoDriver>,

    /// Per-volume admission budget shared across every tail
    pub budget: Arc<InflightBudget>,

    /// Descriptor cache of sealed segments shared across the volume
    pub fd_cache: Arc<FdCache>,

    /// What the footer filters were asked and how often they answered
    pub probes: FilterProbes,

    /// The per-segment counters, so a seal can write down what its segment weighs
    pub segments: OnceLock<Arc<SegmentTable>>,

    /// Footers of sealed segments, which a paged column resolves its keys through
    pub footers: FooterCache,

    /// Load-time settings for the volume
    pub config: ReelConfig,

    /// The columns this reel serves, for the widths a record's column declares
    pub columns: ColumnSet,

    /// Inline width per column identifier, so a record's is one index rather than a scan
    row_carries: Vec<u16>,

    /// Monotonic segment number every tail draws from
    next_segment: AtomicU32,

    /// Segments sealed since the last pass, waiting to be given to their footers
    sealed_pending: Mutex<Vec<Pending>>,

    /// Whether anything is waiting above, read before the lock rather than under it
    sealed_waiting: AtomicBool,

    /// Segments held from the draw of their number until every record is published
    holds: RwLock<TBTreeMap<SegmentId, NODE_WIDTH, Option<Arc<SegmentHolds>>>>,

    /// Lowest segment number anything still holds, or all ones when nothing does
    held_floor: AtomicU32,

    /// Highest segment number given up on without a footer, or zero when none was
    unsealed_high: AtomicU32,

    /// Segments retired holding acknowledged bytes no sync ever covered
    past_saving: AtomicU64,

    /// Rolled segments whose seals failed, parked for the maintenance tick
    pub(crate) broken_seals: Mutex<Vec<crate::append::BrokenSeal>>,

    /// Whether windows may still be read around the page cache
    cold_direct: AtomicBool,

    /// Cold window reads in flight right now, on either plane
    cold_depth: AtomicU64,

    /// Sequence numbers drawn for records that have not yet taken a segment hold
    drawn: AtomicU64,

    /// Segments a merge writer drew, which nothing else ever writes into
    merge_output: Mutex<std::collections::HashSet<SegmentId>>,
}

/// Drawn sequence numbers' place in the gauge, given back when their records land
///
/// A guard rather than a pair of calls, so a placement that fails between the draw
/// and the claim cannot wedge the prune floor closed for the life of the volume.
pub struct DrawnRecords<'a> {
    shared: &'a ReelShared,
    count: u64,
}

impl Drop for DrawnRecords<'_> {
    fn drop(&mut self) {
        self.shared.drawn.fetch_sub(self.count, Ordering::AcqRel);
    }
}

/// One cold window read's place in the depth count, given back when it lands
struct ColdDepth<'a> {
    shared: &'a ReelShared,
}

impl Drop for ColdDepth<'_> {
    fn drop(&mut self) {
        self.shared.cold_depth.fetch_sub(1, Ordering::Relaxed);
    }
}

/// What is keeping one segment from being retired
#[derive(Default)]
pub struct SegmentHolds {
    /// Whether a tail can still append to it
    is_tail: AtomicBool,

    /// Records on the device that nobody has published to the index yet
    unpublished: AtomicU64,
}

impl SegmentHolds {
    fn is_free(&self) -> bool {
        !self.is_tail.load(Ordering::Acquire) && self.unpublished.load(Ordering::Acquire) == 0
    }

    /// Take a hold for a record that has landed but has not been published
    pub fn hold_record(&self) {
        self.unpublished.fetch_add(1, Ordering::AcqRel);
    }

    /// Whether giving up a record's hold leaves the segment ready to be forgotten
    pub fn release_record(&self) -> bool {
        self.unpublished.fetch_sub(1, Ordering::AcqRel) == 1
            && !self.is_tail.load(Ordering::Acquire)
    }
}

/// A sealed segment the index has not been told about yet
///
/// The entry stays on the queue while anybody holds its footer, since the queue is
/// what holds compaction off the segment, and is_taken keeps two callers from each
/// settling it off while the other still owes its spans.
struct Pending {
    /// The segment sealed
    segment: SegmentId,

    /// When it sealed, so a residency tier ages it from the seal and not the tick
    sealed_at: Instant,

    /// Whether a caller is already reading this one's footer
    is_taken: bool,
}

impl ReelShared {
    /// The leads a blocked search descends, read off the volume when none are held
    ///
    /// Nothing comes back on a volume with no fence, which leaves the search the walk
    /// it always was.
    fn fence_cut(
        &self,
        map: &FooterMap,
        column: ColumnId,
        key: &[u8],
        segment: SegmentId,
    ) -> Result<Option<FenceCut>> {
        let Some(fence) = map.fence_of(column) else {
            return Ok(None);
        };
        match fence.reach(key) {
            FenceReach::Ready(cut) => Ok(Some(cut)),
            FenceReach::Read { at, len, first } => {
                let Some(handle) = self.handle_for(segment)? else {
                    return Ok(None);
                };
                self.probes.note_block();
                self.probes.note_block_read();
                let leads = self.driver.pread(handle.file(), at, len as u64)?;
                if leads.len() < len {
                    return Err(ReelError::Corruption(
                        "a footer's fence is truncated".to_string(),
                    ));
                }
                Ok(Some(FenceCut::try_new(Arc::from(&leads[..len]), first)?))
            }
        }
    }

    /// Turn a footer lookup into the trait's answer, counting a ruled-out skip
    fn answer_of(&self, outcome: FooterFind) -> Option<FooterRow> {
        match outcome {
            FooterFind::RuledOut => {
                self.probes.note_skip();
                None
            }
            FooterFind::Missing => None,
            FooterFind::Found(row) => Some(row),
        }
    }

    /// Shared reel state numbering segments from a starting point
    pub fn new(
        dir: PathBuf,
        driver: Arc<IoDriver>,
        budget: Arc<InflightBudget>,
        fd_cache: Arc<FdCache>,
        config: ReelConfig,
        columns: ColumnSet,
        next_segment: u32,
    ) -> ReelShared {
        let mut row_carries = vec![0u16; COLUMN_SLOTS];
        for spec in columns {
            row_carries[spec.id.as_index()] = spec.row_carry_width();
        }
        // The primary root stays first: the lock and the manifest live on it, and
        // it is always the fast tier.
        let mut roots = vec![dir];
        let mut classes = vec![crate::config::VolumeClass::Fast];
        let mut dead = vec![false];
        for volume in &config.volumes {
            roots.push(volume.path.clone());
            classes.push(volume.class);
            dead.push(volume.dead);
        }
        let watermark = config.segment_bytes.to_bytes() * crate::reel::volumes::WATERMARK_SEGMENTS;
        ReelShared {
            volumes: crate::reel::volumes::Volumes::new(roots, classes, dead, watermark),
            lsn: LsnCounter::new(),
            driver,
            budget,
            fd_cache,
            probes: FilterProbes::default(),
            segments: OnceLock::new(),
            footers: FooterCache::new(config.footer_cache.to_bytes() as usize),
            config,
            columns,
            row_carries,
            next_segment: AtomicU32::new(next_segment.max(FIRST_SEGMENT)),
            sealed_pending: Mutex::new(Vec::new()),
            sealed_waiting: AtomicBool::new(false),
            holds: RwLock::new(TBTreeMap::new()),
            held_floor: AtomicU32::new(u32::MAX),
            unsealed_high: AtomicU32::new(0),
            past_saving: AtomicU64::new(0),
            broken_seals: Mutex::new(Vec::new()),
            cold_direct: AtomicBool::new(true),
            cold_depth: AtomicU64::new(0),
            drawn: AtomicU64::new(0),
            merge_output: Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// Note that a merge writer drew this segment, so nothing else wrote into it
    pub fn note_merge_output(&self, segment: SegmentId) {
        lock(&self.merge_output).insert(segment);
    }

    /// Whether a merge writer drew this segment
    pub fn is_merge_output(&self, segment: SegmentId) -> bool {
        lock(&self.merge_output).contains(&segment)
    }

    /// Forget that a merge drew this segment, once the index has been told about it
    pub fn forget_merge_output(&self, segment: SegmentId) -> bool {
        lock(&self.merge_output).remove(&segment)
    }

    /// Bits this segment's seal spends per key on its filters
    ///
    /// Merge output gets none: its rows span the whole keyspace, so its fence
    /// answers placement and a search reaching it is nearly always a hit.
    pub fn filter_bits_for(&self, segment: SegmentId) -> u8 {
        match self.is_merge_output(segment) {
            true => 0,
            false => self.config.seal_filter_bits(),
        }
    }

    /// Bytes a column asks a footer row to carry of the value itself
    pub fn row_carry(&self, column: ColumnId) -> u16 {
        self.row_carries[column.as_index()]
    }

    /// A handle on a segment file, from the descriptor cache or from a fresh open
    ///
    /// Every reader here asks for a range it already knows, so readahead serves
    /// nothing and costs the pages it faulted.
    pub fn handle_for(&self, segment: SegmentId) -> Result<Option<SegmentHandle>> {
        if let Some(handle) = self.fd_cache.get(segment) {
            return Ok(Some(handle));
        }
        let path = self.segment_path(segment);
        let file = match self.driver.open(&path, false) {
            Ok(file) => file,
            Err(error) if error.is_missing() => return Ok(None),
            Err(error) => return Err(error),
        };
        // A refused hint leaves the reads correct and only the readahead wrong,
        // which is not worth failing an open over.
        let _ = self.driver.advise(file, 0, 0, Advice::Random);
        let handle = SegmentHandle::new(segment, path, file, Arc::clone(&self.driver));
        self.fd_cache.insert(handle.clone());
        Ok(Some(handle))
    }

    /// The parsed footer of a sealed segment, from the cache or from the file
    pub fn footer_of(&self, segment: SegmentId) -> Result<Option<Arc<SegmentFooter>>> {
        if let Some(footer) = self.footers.get(segment) {
            return Ok(Some(footer));
        }
        let handle = match self.handle_for(segment)? {
            Some(handle) => handle,
            None => return Ok(None),
        };
        let file_len = match self.driver.length(handle.file()) {
            Ok(len) => len,
            Err(error) if error.is_missing() => return Ok(None),
            Err(error) => return Err(error),
        };
        let footer = match read_footer(&self.driver, handle.file(), file_len)? {
            Some(footer) => Arc::new(footer),
            None => return Ok(None),
        };
        self.footers.insert(segment, Arc::clone(&footer));
        Ok(Some(footer))
    }

    /// What a segment weighs, live against dead, for the seal that writes it down
    ///
    /// Zero before the counters are wired, which is a reader that never seals.
    pub fn tally_of(&self, segment: SegmentId) -> FooterTally {
        let Some(segments) = self.segments.get() else {
            return FooterTally::default();
        };
        let bytes = segments.bytes_of(segment);
        FooterTally {
            live: bytes.live,
            dead: bytes.dead,
        }
    }

    /// Hand the seal the counters it writes a segment's tally from
    pub fn set_segments(&self, segments: Arc<SegmentTable>) {
        let _ = self.segments.set(segments);
    }

    /// Note the oldest number a segment can surface, as soon as its bytes are down
    ///
    /// Booked at landing rather than at the publish, since a caller that goes away
    /// between the two leaves a record a rebuild still finds and no entry names. The
    /// publish books the same number again, which a minimum takes twice for free.
    pub fn note_landed(&self, segment: SegmentId, lsn: Lsn) {
        if let Some(segments) = self.segments.get() {
            segments.note_min(segment, lsn);
        }
    }

    /// A sealed segment's directory, read once and held for the life of the segment
    pub fn footer_map_of(&self, segment: SegmentId) -> Result<Option<Arc<FooterMap>>> {
        if let Some(map) = self.footers.map_of(segment) {
            return Ok(Some(map));
        }
        let Some(handle) = self.handle_for(segment)? else {
            return Ok(None);
        };
        let file_len = match self.driver.length(handle.file()) {
            Ok(len) => len,
            Err(error) if error.is_missing() => return Ok(None),
            Err(error) => return Err(error),
        };
        let read = FooterMap::read(
            &self.driver,
            handle.file(),
            file_len,
            self.config.fence,
            &self.probes,
        )?;
        let Some(map) = read else {
            return Ok(None);
        };
        let map = Arc::new(map);
        self.footers.insert_map(segment, Arc::clone(&map));
        Ok(Some(map))
    }

    /// Note a segment whose footer is now on disk, for the index to page out
    pub fn note_sealed(&self, segment: SegmentId) {
        // The window where a seal is durable but the index has not been told.
        crate::sync::rendezvous::at("seal/queued");
        lock(&self.sealed_pending).push(Pending {
            segment,
            sealed_at: Instant::now(),
            is_taken: false,
        });
        self.sealed_waiting.store(true, Ordering::Release);
    }

    /// Whether a segment has sealed that the index has not been told about
    pub fn has_sealed_waiting(&self) -> bool {
        self.sealed_waiting.load(Ordering::Acquire)
    }

    /// The segments sealed since this was last asked, with when each sealed
    ///
    /// They stay on the queue, where a compaction pass can still see their spans
    /// are owed; clearing the flag only sends other callers past this batch.
    pub fn peek_sealed(&self) -> Vec<(SegmentId, Instant)> {
        let mut pending = lock(&self.sealed_pending);
        self.sealed_waiting.store(false, Ordering::Release);
        let mut taken = Vec::with_capacity(pending.len());
        for entry in pending.iter_mut().filter(|entry| !entry.is_taken) {
            entry.is_taken = true;
            taken.push((entry.segment, entry.sealed_at));
        }
        taken
    }

    /// Take off the queue the segments a pass has told the index about
    ///
    /// What is left stays owed, but the flag is not raised again for it: that would
    /// put every following read into the same failing footer read. The retry rides
    /// the maintenance tick, which asks whether or not the flag is up.
    pub fn settle_sealed(&self, named: &[SegmentId]) {
        lock(&self.sealed_pending).retain(|entry| !named.contains(&entry.segment));
    }

    /// Put back segments a pass took and could not name, for the next one to take
    ///
    /// The claim has to come off whichever way the caller leaves, or the entry holds
    /// compaction off the segment for ever and no later pass will take it.
    pub fn release_sealed(&self, named: &[SegmentId]) {
        let mut pending = lock(&self.sealed_pending);
        for entry in pending
            .iter_mut()
            .filter(|entry| named.contains(&entry.segment))
        {
            entry.is_taken = false;
        }
    }

    /// Segments whose spans the index has not been told about yet
    pub fn pending_seals(&self) -> Vec<SegmentId> {
        lock(&self.sealed_pending)
            .iter()
            .map(|entry| entry.segment)
            .collect()
    }

    /// Draw the next monotonic segment number, holding it for the tail that drew it
    ///
    /// Numbers are never given back, and a wrap would name a fresh file after a
    /// segment the index still points at, so the draw refuses at the end of the
    /// range instead of rolling over.
    pub fn next_segment(&self) -> Result<(SegmentId, Arc<SegmentHolds>)> {
        let drawn = self
            .next_segment
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|current| {
                ReelError::Rejected(format!("reel segment numbers exhausted at {current}"))
            })?;
        let id = SegmentId(drawn);
        let holds = {
            let mut map = write(&self.holds);
            let held = match map.get(&id) {
                Some(Some(held)) => Arc::clone(held),
                // A node's unfilled slots are None, so the hold is made here
                // rather than defaulted into every one of them.
                _ => {
                    let held: Arc<SegmentHolds> = Arc::default();
                    map.insert(id, Some(Arc::clone(&held)));
                    held
                }
            };
            self.refresh_held_floor(&map);
            held
        };
        holds.is_tail.store(true, Ordering::Release);
        Ok((id, holds))
    }

    /// Give up a segment no tail will append to again, sealed or given up on
    pub fn release_segment(&self, id: SegmentId) {
        {
            let holds = read(&self.holds);
            match holds.get(&id).and_then(|held| held.as_ref()) {
                Some(held) => held.is_tail.store(false, Ordering::Release),
                None => return,
            }
        }
        self.forget_if_free(id);
    }

    /// Drop a segment's entry once nothing stands between it and retirement
    ///
    /// The check is made again under the exclusive lock, since a record can take a
    /// fresh hold between the release that emptied the count and this.
    pub fn forget_if_free(&self, id: SegmentId) {
        let mut holds = write(&self.holds);
        if holds
            .get(&id)
            .and_then(|held| held.as_ref())
            .map(|held| held.is_free())
            .unwrap_or(false)
        {
            holds.remove(&id);
            self.refresh_held_floor(&holds);
        }
    }

    /// Recompute the lowest held number, under the write lock the caller holds
    fn refresh_held_floor(
        &self,
        holds: &TBTreeMap<SegmentId, NODE_WIDTH, Option<Arc<SegmentHolds>>>,
    ) {
        let floor = holds
            .first_key_value()
            .map(|(id, _)| id.as_u32())
            .unwrap_or(u32::MAX);
        self.held_floor.store(floor, Ordering::Relaxed);
    }

    /// Whether anything still stands between this segment and its retirement
    pub fn is_held(&self, id: SegmentId) -> bool {
        read(&self.holds).contains_key(&id)
    }

    /// Whether this segment is sealed, synced, and past every hold
    ///
    /// The window that matters is between a tail rolling off a segment and the
    /// sealer's fsync returning: no tail owns it and its pages are still dirty. A
    /// segment given up on without a footer never gets that far, so the unsealed
    /// mark answers before the holds do.
    pub fn is_settled(&self, id: SegmentId) -> bool {
        let number = id.as_u32();
        if number <= self.unsealed_high.load(Ordering::Relaxed) {
            return false;
        }
        number < self.held_floor.load(Ordering::Relaxed) || !self.is_held(id)
    }

    /// Note a segment released without its footer, which never reads as settled
    pub fn note_unsealed(&self, id: SegmentId) {
        self.unsealed_high.fetch_max(id.as_u32(), Ordering::Relaxed);
    }

    /// Count a segment retired holding acknowledged bytes no sync covered
    pub fn note_past_saving(&self) {
        self.past_saving.fetch_add(1, Ordering::Relaxed);
    }

    /// Give one past-saving count back, for a parked seal that landed after all
    pub fn un_note_past_saving(&self) {
        let _ = self
            .past_saving
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |held| {
                held.checked_sub(1)
            });
    }

    /// Segments holding acknowledged records a failed sync left past saving
    pub fn past_saving_count(&self) -> u64 {
        self.past_saving.load(Ordering::Relaxed)
    }

    /// Whether windows may still be read around the page cache on this volume
    pub fn cold_direct_live(&self) -> bool {
        self.cold_direct.load(Ordering::Relaxed)
    }

    /// Stop asking for direct descriptors on a filesystem that serves none
    pub fn retire_cold_direct(&self) {
        self.cold_direct.store(false, Ordering::Relaxed);
    }

    /// Count one cold window read in for as long as its guard is held
    fn enter_cold(&self) -> ColdDepth<'_> {
        self.cold_depth.fetch_add(1, Ordering::Relaxed);
        ColdDepth { shared: self }
    }

    /// Count drawn sequence numbers in until their records take segment holds
    ///
    /// Taken before the numbers are drawn, so there is no instant where a drawn
    /// number is in flight and neither counter says so.
    pub fn draw_gauge(&self, count: u64) -> DrawnRecords<'_> {
        self.drawn.fetch_add(count, Ordering::AcqRel);
        DrawnRecords {
            shared: self,
            count,
        }
    }

    /// Whether every number drawn so far has been published to the index
    ///
    /// A record leaves the drawn gauge only after its segment hold is taken, so the
    /// gauge has to be read first or both reads could miss it.
    pub fn nothing_unpublished(&self) -> bool {
        if self.drawn.load(Ordering::Acquire) > 0 {
            return false;
        }
        let holds = read(&self.holds);
        // Bound rather than left as the tail expression: the walk borrows the guard.
        let quiet = holds
            .iter()
            .filter_map(|(_, held)| held.as_ref())
            .all(|held| held.unpublished.load(Ordering::Acquire) == 0);
        quiet
    }

    /// The number below which no record can still land, the floor a delete is done at
    ///
    /// The frontier is read first, so a draw between the two reads is one the check
    /// sees rather than one the floor lets past. A volume with something in flight
    /// falls back to the same window a grave holds a key against.
    pub fn settled_below(&self) -> Lsn {
        let peek = self.lsn.peek().as_u64();
        match self.nothing_unpublished() {
            true => Lsn(peek),
            false => Lsn(peek.saturating_sub(crate::engine::GRAVE_WINDOW)),
        }
    }

    /// Cold window reads in flight, not counting the one about to be routed
    fn cold_depth(&self) -> u64 {
        self.cold_depth.load(Ordering::Relaxed)
    }

    /// Whether an awaited whole-record read asks the page cache before it queues
    pub fn warm_first(&self) -> WarmFirst {
        match self.config.point_reads {
            PointReads::Probed => WarmFirst::Ask,
            PointReads::Queued => WarmFirst::Skip,
        }
    }

    /// The routed read this volume's knob asks for, over a direct descriptor
    fn routed(&self, file: FileId) -> ColdRoute {
        match self.config.ranged_reads {
            RangedReads::Probed => ColdRoute::Probed(file),
            // Cached never reaches here: the route settles it before any descriptor
            // is named.
            RangedReads::Cached | RangedReads::Direct => ColdRoute::Direct(file),
        }
    }

    /// The number the next segment will take, without taking it
    ///
    /// Numbers climb, so once a cue has sealed every tail, every segment below this
    /// is sealed and immutable and every later roll lands at or above it.
    pub fn peek_segment(&self) -> SegmentId {
        SegmentId(self.next_segment.load(Ordering::Relaxed))
    }

    /// Raise the segment counter above a number found on disk during rebuild
    pub fn recover_next_segment(&self, highest_seen: SegmentId) {
        let floor = highest_seen.as_u32().saturating_add(1).max(FIRST_SEGMENT);
        self.next_segment.fetch_max(floor, Ordering::Relaxed);
    }

    /// The next segment number to be drawn, which is the write head's age zero
    pub fn segment_head(&self) -> u32 {
        self.next_segment.load(Ordering::Relaxed)
    }

    /// Path of one segment file, on whichever volume holds it
    pub fn segment_path(&self, id: SegmentId) -> PathBuf {
        self.volumes.path_of(id)
    }

    /// The directory a segment's file sits in, for the sync its create owes
    pub fn segment_dir(&self, id: SegmentId) -> &Path {
        self.volumes.root_dir_of(id)
    }

    /// Whether every write this volume issues has to cover whole blocks
    ///
    /// A direct backend hands its writes straight to the device, which takes whole
    /// blocks or nothing, so a record closes by writing its alignment fill.
    pub fn writes_whole_blocks(&self) -> bool {
        self.config.io_backend.is_direct()
    }
}

/// What resolving one index pointer against the files produced
#[derive(Debug, Eq, PartialEq)]
pub enum RecordRead {
    /// The payload, checked against its checksum when the volume asked
    Found(Value),

    /// The pointer no longer names this key, so resolving it again may find it
    Stale,

    /// The segment the pointer names is not on disk any more
    Gone,

    /// The record is where the pointer said, but its bytes failed the checksum
    Corrupt,
}

/// One record a resolved batch asks the device for
///
/// The key is named by position rather than carried, so the list of asks holds no
/// borrow and a thread can keep it between submissions.
#[derive(Clone, Copy)]
pub struct Ask {
    /// Where the index says the record sits
    pub loc: Loc,

    /// The version the index resolved, which the record's header has to match
    pub lsn: Lsn,

    /// Which of the caller's keys this ask is for
    pub at: u32,
}

/// The lists one thread's batched reads work through, kept between submissions
///
/// A batch resolves, plans, merges, submits and cuts through a stack of vectors as
/// wide as the batch and nothing wider, so they stay with the thread and only the
/// answers leave. Every one is cleared on the way back, since a plan holds segment
/// handles open and a filled read holds pooled payload buffers.
#[derive(Default)]
struct ReadScratch {
    /// Asks sorted into volume order, so neighbours are neighbours before merging
    order: Vec<usize>,

    /// Every ask resolved to a place on the volume
    plan: Vec<Planned>,

    /// The reads the plan was grouped into
    runs: Vec<Run>,

    /// One op per run, built before anything is submitted
    ops: Vec<Op>,

    /// Completions the backend answered a batch with, in submit order
    completions: Vec<Completion>,

    /// What each run's read filled
    filled: Vec<SplitRead>,

    /// Windows a merged read is cut into, one per record in the run
    cuts: Vec<(usize, usize)>,

    /// Why each record of a merged read was rejected, or the codec it carries
    verdicts: Vec<std::result::Result<u8, RecordRead>>,

    /// The windows themselves, handed out to the answers
    windows: Vec<Option<Value>>,
}

impl ReadScratch {
    const fn empty() -> ReadScratch {
        ReadScratch {
            order: Vec::new(),
            plan: Vec::new(),
            runs: Vec::new(),
            ops: Vec::new(),
            completions: Vec::new(),
            filled: Vec::new(),
            cuts: Vec::new(),
            verdicts: Vec::new(),
            windows: Vec::new(),
        }
    }

    /// Drop everything the last batch left, keeping the room it grew into
    fn release(&mut self) {
        self.order.clear();
        self.plan.clear();
        self.runs.clear();
        self.ops.clear();
        self.completions.clear();
        self.filled.clear();
        self.cuts.clear();
        self.verdicts.clear();
        self.windows.clear();
    }
}

thread_local! {
    /// One set of read lists per reading thread, handed back after every batch
    static READ_SCRATCH: std::cell::Cell<ReadScratch> =
        const { std::cell::Cell::new(ReadScratch::empty()) };
}

/// This thread's read lists, given back however the batch that borrowed them ends
struct HeldScratch(ReadScratch);

impl HeldScratch {
    /// Borrow this thread's lists, leaving it empty ones until they come back
    fn take() -> HeldScratch {
        HeldScratch(READ_SCRATCH.with(std::cell::Cell::take))
    }
}

impl Drop for HeldScratch {
    fn drop(&mut self) {
        let mut held = std::mem::take(&mut self.0);
        held.release();
        // A read that nested inside another gives its own back first and this
        // overwrites it, which keeps the lists the outer batch grew.
        READ_SCRATCH.with(|spare| spare.set(held));
    }
}

/// The volume's reel: shared state plus its active append tails
pub struct Reel {
    shared: Arc<ReelShared>,
    tails: Vec<Appender>,
}

impl Reel {
    /// Open a reel with the configured number of active tails
    ///
    /// A volume that rewrites at seal, or that owns a capacity tier, keeps one extra
    /// tail back for compaction: a sorted run is only sorted if nothing else is
    /// writing into it. The reserved tail is the last one and route never offers it.
    pub fn open(shared: Arc<ReelShared>) -> Result<Reel> {
        let count = shared.config.tail_count();
        let reserved = shared.config.rewrite_on_seal || shared.volumes.has_capacity();
        let total = count + usize::from(reserved);
        let mut tails = Vec::with_capacity(total);
        for index in 0..total {
            tails.push(Appender::open(Arc::clone(&shared), index as u64)?);
        }
        Ok(Reel { shared, tails })
    }

    /// The tail compaction owns, which no foreground write is offered
    pub fn reserved_tail(&self) -> Option<usize> {
        let held = self.shared.config.rewrite_on_seal || self.shared.volumes.has_capacity();
        (held && !self.tails.is_empty()).then(|| self.tails.len() - 1)
    }

    /// The tails a foreground write may be routed to
    fn foreground(&self) -> &[Appender] {
        match self.reserved_tail() {
            Some(reserved) => &self.tails[..reserved],
            None => &self.tails,
        }
    }

    /// Open a reel with no append tails, for a read-only open that never writes
    pub fn open_read_only(shared: Arc<ReelShared>) -> Reel {
        Reel {
            shared,
            tails: Vec::new(),
        }
    }

    /// Shared reel state, for wiring an index and counters over it
    pub fn shared(&self) -> &Arc<ReelShared> {
        &self.shared
    }

    /// The reel's active append tails
    pub fn tails(&self) -> &[Appender] {
        &self.tails
    }

    /// Count one cold window read in and leave it counted
    ///
    /// A lone reader cannot clear the depth floor, so a fixture standing in for a
    /// busy volume needs pressure that outlives any one call.
    #[cfg(test)]
    pub(crate) fn hold_cold_read_open_ended(&self) {
        self.shared.cold_depth.fetch_add(1, Ordering::Relaxed);
    }

    /// Append or overwrite a payload, routed to the least-loaded tail
    pub fn put(
        &self,
        key: RecordKey,
        payload: Vec<u8>,
        codec: u8,
        commit: Commit,
    ) -> Result<Committed> {
        self.route().append_data(key, payload, codec, commit)
    }

    /// The same append awaited, taking its durability point on the async door
    ///
    /// Admission is awaited, and the sync a per-record commit owes is forwarded to
    /// the tail's sealer rather than run on the caller.
    pub async fn put_wait(
        &self,
        key: RecordKey,
        payload: Vec<u8>,
        codec: u8,
        commit: Commit,
    ) -> Result<Committed> {
        self.route()
            .append_data_wait(key, payload, codec, commit)
            .await
    }

    /// Append a tombstone for one key, routed to the least-loaded tail
    pub fn delete(&self, key: RecordKey, commit: Commit) -> Result<Committed> {
        self.route().append_tombstone(key, commit)
    }

    /// Append a tombstone covering a half-open key range within one column
    pub fn delete_range(
        &self,
        start: RecordKey,
        end: Option<&[u8]>,
        commit: Commit,
    ) -> Result<Committed> {
        self.route().append_range_tombstone(start, end, commit)
    }

    /// Append a whole batch to one tail as one reservation and one write
    ///
    /// Everything the batch carries lands together or not at all: the records go down
    /// back to back behind a frame declaring their count and their span, and a rebuild
    /// keeps the run only when it reads exactly what the frame declared.
    pub fn write_batch(&self, records: Vec<BatchRecord>) -> Result<Vec<Committed>> {
        self.route().append_batch(records)
    }

    /// The same batch awaited, admitted without holding a thread for the budget
    pub async fn write_batch_wait(&self, records: Vec<BatchRecord>) -> Result<Vec<Committed>> {
        self.route().append_batch_wait(records).await
    }

    /// Sync every active tail
    pub fn flush(&self) -> Result<()> {
        for tail in &self.tails {
            tail.flush()?;
        }
        self.check_past_saving()
    }

    /// Sync every active tail, awaited, with no fsync on the calling thread
    pub async fn flush_wait(&self) -> Result<()> {
        for tail in &self.tails {
            tail.flush_wait().await?;
        }
        self.check_past_saving()
    }

    /// Refuse to answer clean while any segment holds records past saving
    ///
    /// A segment a failed sync ended holds acknowledged records that read from cache
    /// and are gone at the next open, so a flush cannot answer Ok over them.
    fn check_past_saving(&self) -> Result<()> {
        let stranded = self.shared.past_saving_count();
        if stranded == 0 {
            return Ok(());
        }
        Err(ReelError::Io(std::io::Error::other(format!(
            "{stranded} segments hold acknowledged records a failed sync left past saving"
        ))))
    }

    /// Take the sync a batch left owed on every tail it touched
    pub fn sync_if_owed(&self) -> Result<()> {
        for tail in &self.tails {
            tail.sync_if_owed()?;
        }
        Ok(())
    }

    /// The same durability point awaited, with owed turns forwarded to the sealers
    pub async fn sync_if_owed_wait(&self) -> Result<()> {
        for tail in &self.tails {
            tail.sync_if_owed_wait().await?;
        }
        Ok(())
    }

    /// Seal every tail for a clean shutdown, so a reopen reads footers only
    ///
    /// Every tail is closed even when one of them fails, and the first failure is
    /// what the caller hears about.
    pub fn close(&self) -> Result<()> {
        let mut outcome = Ok(());
        for tail in &self.tails {
            if let Err(error) = tail.close() {
                outcome = outcome.and(Err(error));
            }
        }
        outcome
    }

    /// Read one record's payload, checking it still resolves the expected key
    ///
    /// The segment is resolved to a refcounted handle first, so the file cannot be
    /// unlinked while the read is in flight. A pointer the index has since moved
    /// reads as stale rather than as a wrong payload.
    pub fn read_record(
        &self,
        loc: Loc,
        expected: KeyRef<'_>,
        lsn: Lsn,
        is_verified: bool,
    ) -> Result<RecordRead> {
        let handle = match self.handle_for(loc.segment)? {
            Some(handle) => handle,
            None => return Ok(RecordRead::Gone),
        };
        let prefix = HEADER_LEN + expected.width();
        let framed = self.read_framed(&handle, u64::from(loc.offset), prefix, loc.len as usize)?;
        let (head, body) = match framed {
            Some(framed) => framed,
            None => return Ok(RecordRead::Stale),
        };
        Ok(frame_to_read(head, body, expected, lsn, loc, is_verified))
    }

    /// Read one record as a future, always through the driver
    ///
    /// The mapping is left to the blocking door: a page fault cannot be awaited and
    /// a device error inside one arrives as SIGBUS on whichever worker was polling.
    pub async fn read_record_wait(
        &self,
        loc: Loc,
        expected: KeyRef<'_>,
        lsn: Lsn,
        is_verified: bool,
    ) -> Result<RecordRead> {
        let handle = match self.handle_for(loc.segment)? {
            Some(handle) => handle,
            None => return Ok(RecordRead::Gone),
        };
        let prefix = HEADER_LEN + expected.width();
        let len = loc.len as usize;
        let spare = take_header();
        let read = self
            .shared
            .driver
            .wait_split_reusing(
                handle.file(),
                u64::from(loc.offset),
                prefix,
                len,
                spare,
                self.shared.warm_first(),
            )
            .await;
        let (head, body) = match framed_or_nothing(read, prefix, len)? {
            Some(framed) => framed,
            None => return Ok(RecordRead::Stale),
        };
        Ok(frame_to_read(head, body, expected, lsn, loc, is_verified))
    }

    /// Read one window of a record's payload with no echo, one device read
    ///
    /// For the caller whose index already vouched for the offset: a live segment's
    /// record bytes never change and its number is never reused. Nothing comes back
    /// for a window the volume cannot answer whole, an absent segment included.
    pub fn read_window(
        &self,
        loc: Loc,
        key_width: u16,
        at: u64,
        len: usize,
    ) -> Result<Option<Value>> {
        let Some(handle) = self.handle_for(loc.segment)? else {
            return Ok(None);
        };
        let start = window_start(loc, key_width, at);

        if self.shared.config.maps(len) {
            if let Some(map) = handle.mapping() {
                if let Some(bytes) = map.slice(start, len) {
                    let mut body = crate::reel::payload::take(len);
                    body.extend_from_slice(bytes);
                    return Ok(Some(Value::pooled(body, crate::reel::payload::give)));
                }
            }
        }

        let route = self.window_route(&handle, loc);
        let _depth = self.shared.enter_cold();
        let read = self.shared.driver.pread_cold(
            handle.file(),
            route,
            start,
            len as u64,
            crate::reel::payload::take(len),
        );
        window_or_nothing(read, len)
    }

    /// Read one window as a future, always through the driver
    pub async fn read_window_wait(
        &self,
        loc: Loc,
        key_width: u16,
        at: u64,
        len: usize,
    ) -> Result<Option<Value>> {
        let Some(handle) = self.handle_for(loc.segment)? else {
            return Ok(None);
        };
        let start = window_start(loc, key_width, at);
        let route = self.window_route(&handle, loc);
        let _depth = self.shared.enter_cold();
        let read = self
            .shared
            .driver
            .wait_pread_cold(
                handle.file(),
                route,
                start,
                len as u64,
                crate::reel::payload::take(len),
            )
            .await;
        window_or_nothing(read, len)
    }

    /// Which plane answers this window, and the descriptor it reads
    ///
    /// The route picks a descriptor and a staging buffer, never the byte range or
    /// what the answer means. Both floors err toward the page cache.
    fn window_route(&self, handle: &SegmentHandle, loc: Loc) -> ColdRoute {
        if self.shared.config.ranged_reads == RangedReads::Cached
            || loc.len < DIRECT_RECORD_FLOOR
            || self.shared.cold_depth() < DIRECT_DEPTH_FLOOR
            || !self.shared.cold_direct_live()
        {
            return ColdRoute::Cached;
        }
        if let Some(file) = handle.direct_file() {
            return self.shared.routed(file);
        }
        // A segment anything still holds reads buffered. A direct read of one runs
        // filemap_write_and_wait_range over its range and flushes the appender's
        // dirty pages from inside the read.
        if !self.shared.is_settled(handle.id()) {
            return ColdRoute::Cached;
        }
        match handle.direct_file_or_open() {
            DirectOpen::Ready(file) => self.shared.routed(file),
            // One file's refusal costs this segment its plane and nothing more: it
            // says nothing about whether the next segment can be opened.
            DirectOpen::Refused => ColdRoute::Cached,
            DirectOpen::Unsupported => {
                self.shared.retire_cold_direct();
                ColdRoute::Cached
            }
        }
    }

    /// Read part of one record's payload, without reading the rest of it
    ///
    /// The header still rides along for the echo, and a range beginning within
    /// MERGE_GAP of the payload's start comes back in that same read; a deeper one
    /// takes a read of its own in the same submission.
    ///
    /// It cannot verify: the checksum covers the whole payload and this read holds a
    /// piece of it.
    pub fn read_range(
        &self,
        loc: Loc,
        expected: KeyRef<'_>,
        lsn: Lsn,
        at: u64,
        len: usize,
    ) -> Result<RecordRead> {
        let handle = match self.handle_for(loc.segment)? {
            Some(handle) => handle,
            None => return Ok(RecordRead::Gone),
        };
        let prefix = HEADER_LEN + expected.width();
        let offset = u64::from(loc.offset);

        if let Some((head, body)) = self.map_range(&handle, offset, prefix, at, len) {
            let verdict = frame_to_range(&head, body, expected, lsn, loc);
            recycle_header(head);
            return verdict;
        }

        if at <= MERGE_GAP {
            let span = at as usize + len;
            let read = self.shared.driver.pread_split_reusing(
                handle.file(),
                offset,
                prefix,
                span,
                take_header(),
                self.shared.warm_first(),
            );
            return near_range(read, prefix, at, len, expected, lsn, loc);
        }

        let ops = self.range_ops(&handle, offset, prefix, at, len);
        let filled = self.shared.driver.run_split_reads(ops)?;
        deep_range(filled, prefix, len, expected, lsn, loc)
    }

    /// Read part of one record's payload as a future, always through the driver
    pub async fn read_range_wait(
        &self,
        loc: Loc,
        expected: KeyRef<'_>,
        lsn: Lsn,
        at: u64,
        len: usize,
    ) -> Result<RecordRead> {
        let handle = match self.handle_for(loc.segment)? {
            Some(handle) => handle,
            None => return Ok(RecordRead::Gone),
        };
        let prefix = HEADER_LEN + expected.width();
        let offset = u64::from(loc.offset);

        if at <= MERGE_GAP {
            let span = at as usize + len;
            // Unprobed: a window that reached here has already been past the
            // ranged route.
            let read = self
                .shared
                .driver
                .wait_split_reusing(
                    handle.file(),
                    offset,
                    prefix,
                    span,
                    take_header(),
                    WarmFirst::Skip,
                )
                .await;
            return near_range(read, prefix, at, len, expected, lsn, loc);
        }

        let ops = self.range_ops(&handle, offset, prefix, at, len);
        let filled = self.shared.driver.wait_split_reads(ops).await?;
        deep_range(filled, prefix, len, expected, lsn, loc)
    }

    /// The two reads a deep range takes: the record's header, and the range itself
    ///
    /// Both are built here and submitted together, so the second is not a round trip
    /// behind the first.
    fn range_ops(
        &self,
        handle: &SegmentHandle,
        offset: u64,
        prefix: usize,
        at: u64,
        len: usize,
    ) -> Vec<Op> {
        let driver = &self.shared.driver;
        vec![
            driver.split_read(handle.file(), offset, 0, prefix),
            driver.split_read(handle.file(), offset + prefix as u64 + at, 0, len),
        ]
    }

    /// Copy a range and the record's header out of a segment mapping
    ///
    /// A mapping that does not cover both of them leaves the read to the driver.
    fn map_range(
        &self,
        handle: &SegmentHandle,
        offset: u64,
        prefix: usize,
        at: u64,
        len: usize,
    ) -> Option<(Vec<u8>, Value)> {
        if !self.shared.config.maps(len) {
            return None;
        }
        let map = handle.mapping()?;
        let head_bytes = map.slice(offset, prefix)?;
        let body_bytes = map.slice(offset + prefix as u64 + at, len)?;

        let mut head = take_header();
        head.clear();
        head.extend_from_slice(head_bytes);
        let mut body = crate::reel::payload::take(len);
        body.extend_from_slice(body_bytes);
        Some((head, Value::pooled(body, crate::reel::payload::give)))
    }

    /// Read several records with one submission, answered in the order asked
    ///
    /// One answer per ask, in the order asked, into a vector the caller keeps. A
    /// record whose segment is gone is reported in its own slot. Records written in
    /// one batch are one contiguous byte range, so they are read together and cut
    /// back out of the block. Every list the submission works through between here
    /// and the device belongs to this thread, so its price does not follow the width.
    pub fn read_records(
        &self,
        asks: &[Ask],
        keys: &[KeyRef<'_>],
        is_verified: bool,
        answers: &mut Vec<RecordRead>,
    ) -> Result<()> {
        stale_slots(asks.len(), answers);
        let mut held = HeldScratch::take();
        let scratch = &mut held.0;
        self.plan_reads(asks, keys, scratch, answers)?;
        if scratch.plan.is_empty() {
            return Ok(());
        }

        merge_runs_into(&scratch.plan, &mut scratch.runs);
        self.read_ops(scratch);
        self.shared.driver.run_split_reads_into(
            &mut scratch.ops,
            &mut scratch.completions,
            &mut scratch.filled,
        )?;
        self.frame_runs(scratch, asks, keys, is_verified, answers)
    }

    /// Read several records as one future, answered in the order asked
    pub async fn read_records_wait(
        &self,
        asks: &[Ask],
        keys: &[KeyRef<'_>],
        is_verified: bool,
        answers: &mut Vec<RecordRead>,
    ) -> Result<()> {
        stale_slots(asks.len(), answers);
        let mut held = HeldScratch::take();
        let scratch = &mut held.0;
        self.plan_reads(asks, keys, scratch, answers)?;
        if scratch.plan.is_empty() {
            return Ok(());
        }

        merge_runs_into(&scratch.plan, &mut scratch.runs);
        self.read_ops(scratch);
        self.shared
            .driver
            .wait_split_reads_into(&mut scratch.ops, &mut scratch.filled)
            .await?;
        self.frame_runs(scratch, asks, keys, is_verified, answers)
    }

    /// Resolve every ask to a place on the volume, before anything is submitted
    ///
    /// Key order is not offset order, so the asks are sorted by place first: runs
    /// only form once neighbours are neighbours. Every answer lands by its own slot,
    /// so the caller's order is untouched.
    fn plan_reads(
        &self,
        asks: &[Ask],
        keys: &[KeyRef<'_>],
        scratch: &mut ReadScratch,
        answers: &mut [RecordRead],
    ) -> Result<()> {
        scratch.order.clear();
        scratch.order.extend(0..asks.len());
        scratch
            .order
            .sort_unstable_by_key(|&at| (asks[at].loc.segment, asks[at].loc.offset));

        scratch.plan.clear();
        scratch.plan.reserve(asks.len());
        for slot in 0..scratch.order.len() {
            let at = scratch.order[slot];
            let ask = &asks[at];
            let handle = match self.handle_for(ask.loc.segment)? {
                Some(handle) => handle,
                None => {
                    answers[at] = RecordRead::Gone;
                    continue;
                }
            };
            scratch.plan.push(Planned {
                at,
                segment: ask.loc.segment,
                // Kept until the batch has been collected, so a segment cannot be
                // unlinked out from under a read in flight.
                handle,
                offset: u64::from(ask.loc.offset),
                prefix: HEADER_LEN + keys[ask.at as usize].width(),
                len: ask.loc.len as usize,
            });
        }
        Ok(())
    }

    /// One split read per run, each taking its buffers uninitialised from the pool
    fn read_ops(&self, scratch: &mut ReadScratch) {
        scratch.ops.clear();
        scratch.ops.reserve(scratch.runs.len());
        for at in 0..scratch.runs.len() {
            let run = scratch.runs[at];
            let held = &scratch.plan[run.start];
            let op = match run.is_single() {
                true => self.shared.driver.split_read(
                    held.handle.file(),
                    held.offset,
                    held.prefix,
                    held.len,
                ),
                // A merged read has no header of its own: every record's header sits
                // inside the block, so the whole span is the body.
                false => self.shared.driver.split_read(
                    held.handle.file(),
                    held.offset,
                    0,
                    run.span as usize,
                ),
            };
            scratch.ops.push(op);
        }
    }

    /// Turn what the runs filled into one answer per record asked for
    fn frame_runs(
        &self,
        scratch: &mut ReadScratch,
        asks: &[Ask],
        keys: &[KeyRef<'_>],
        is_verified: bool,
        answers: &mut [RecordRead],
    ) -> Result<()> {
        for at in 0..scratch.runs.len().min(scratch.filled.len()) {
            let run = scratch.runs[at];
            // Moved out of the list rather than borrowed, so the cut below is free
            // to take the rest of the lists with it. An empty pair costs nothing to
            // leave in its place.
            let framed = std::mem::replace(&mut scratch.filled[at], Ok((Vec::new(), Vec::new())));
            let block = match framed {
                Ok(block) => block,
                Err(error) if is_missing(&error) => continue,
                Err(error) => return Err(error),
            };
            match run.is_single() {
                true => {
                    let held = &scratch.plan[run.start];
                    let (head, body) = block;
                    let ask = &asks[held.at];
                    let key = keys[ask.at as usize];
                    answers[held.at] = match head.len() == held.prefix && body.len() == held.len {
                        true => frame_to_read(head, body, key, ask.lsn, ask.loc, is_verified),
                        false => {
                            recycle_header(head);
                            RecordRead::Stale
                        }
                    };
                }
                false => {
                    let (head, body) = block;
                    recycle_header(head);
                    self.cut_run(
                        run.start,
                        run.end,
                        body,
                        scratch,
                        asks,
                        keys,
                        is_verified,
                        answers,
                    );
                }
            }
        }
        scratch.filled.clear();
        Ok(())
    }

    /// Frame every record inside one merged read and hand each its own window
    ///
    /// The block goes back to the pool once the last window taken from it drops, so
    /// a caller keeping one record of a run keeps the run's buffer with it.
    #[allow(clippy::too_many_arguments)]
    fn cut_run(
        &self,
        start: usize,
        end: usize,
        block: Vec<u8>,
        scratch: &mut ReadScratch,
        asks: &[Ask],
        keys: &[KeyRef<'_>],
        is_verified: bool,
        answers: &mut [RecordRead],
    ) {
        let base = scratch.plan[start].offset;
        scratch.cuts.clear();
        scratch.verdicts.clear();

        for slot in start..end {
            let held = &scratch.plan[slot];
            let at = (held.offset - base) as usize;
            let ask = &asks[held.at];
            let cut = (at + held.prefix, held.len);
            let verdict = check_in_block(
                &block,
                at,
                held,
                keys[ask.at as usize],
                ask.lsn,
                ask.loc,
                is_verified,
            );
            scratch.cuts.push(cut);
            scratch.verdicts.push(verdict);
        }

        Value::windows_into(
            block,
            crate::reel::payload::give,
            &scratch.cuts,
            &mut scratch.windows,
        );
        for slot in 0..end - start {
            let placed = scratch.plan[start + slot].at;
            let verdict = std::mem::replace(&mut scratch.verdicts[slot], Ok(0));
            answers[placed] = match (verdict, scratch.windows[slot].take()) {
                (Err(rejected), _) => rejected,
                (Ok(0), Some(window)) => RecordRead::Found(window),
                // A coded record decodes straight out of the shared block into its
                // own pooled buffer, so the batch's single read is still the only
                // pass over the stored bytes.
                (Ok(codec), Some(window)) => match crate::append::codec::decode(codec, &window) {
                    Some(decoded) => {
                        RecordRead::Found(Value::pooled(decoded, crate::reel::payload::give))
                    }
                    None => RecordRead::Corrupt,
                },
                (Ok(_), None) => RecordRead::Stale,
            };
        }
    }

    /// Resolve a segment number to a handle, opening and caching it on a miss
    pub fn handle_for(&self, segment: SegmentId) -> Result<Option<SegmentHandle>> {
        self.shared.handle_for(segment)
    }

    /// Read a framed record into its header and key and its payload, or nothing
    /// when the segment no longer holds a whole record at that offset
    fn read_framed(
        &self,
        handle: &SegmentHandle,
        offset: u64,
        prefix: usize,
        len: usize,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        // A mapped volume serves a record the mapping covers straight out of the
        // page cache; anything it does not cover takes the driver below.
        if self.shared.config.maps(len) {
            if let Some(map) = handle.mapping() {
                let head_at = map.slice(offset, prefix);
                let body_at = map.slice(offset + prefix as u64, len);
                if let (Some(head_bytes), Some(body_bytes)) = (head_at, body_at) {
                    let mut head = take_header();
                    head.clear();
                    head.extend_from_slice(head_bytes);
                    let mut body = crate::reel::payload::take(len);
                    body.extend_from_slice(body_bytes);
                    return Ok(Some((head, body)));
                }
            }
        }

        let spare = take_header();
        let read = self.shared.driver.pread_split_reusing(
            handle.file(),
            offset,
            prefix,
            len,
            spare,
            self.shared.warm_first(),
        );
        framed_or_nothing(read, prefix, len)
    }

    /// The tail a foreground write goes to, which is the least loaded of them
    fn route(&self) -> &Appender {
        let foreground = self.foreground();
        let mut chosen = &foreground[0];
        let mut lowest = chosen.load();
        for tail in &foreground[1..] {
            let load = tail.load();
            if load < lowest {
                lowest = load;
                chosen = tail;
            }
        }
        chosen
    }
}

thread_local! {
    /// One header buffer per reading thread, handed back after every framed read
    static HEADER_SPARE: std::cell::Cell<Vec<u8>> = const { std::cell::Cell::new(Vec::new()) };
}

/// Leave one answer slot per ask, each reading as a record that has moved
///
/// Every slot the reads land in is written over, so what stays is what nothing
/// answered, which is a pointer the caller has to resolve again.
fn stale_slots(wanted: usize, answers: &mut Vec<RecordRead>) {
    answers.clear();
    answers.reserve(wanted);
    for _ in 0..wanted {
        answers.push(RecordRead::Stale);
    }
}

/// This thread's header buffer, or a fresh one when it has none to lend
fn take_header() -> Vec<u8> {
    HEADER_SPARE.with(|held| held.take())
}

/// Hand a header buffer back for the next read on this thread to fill
fn recycle_header(head: Vec<u8>) {
    // The roomier of the two is kept, since a merged read hands back a header
    // buffer it never filled.
    HEADER_SPARE.with(|held| {
        let spare = held.take();
        held.set(match spare.capacity() > head.capacity() {
            true => spare,
            false => head,
        });
    });
}

fn is_missing(error: &ReelError) -> bool {
    if let ReelError::Io(source) = error {
        return source.kind() == std::io::ErrorKind::NotFound;
    }
    false
}

/// File name a segment number resolves to within a reel directory
pub fn segment_file_name(id: SegmentId) -> String {
    format!(
        "{number:0width$}{suffix}",
        number = id.as_u32(),
        width = SEGMENT_DIGITS,
        suffix = SEGMENT_SUFFIX,
    )
}

/// Segment number parsed back from a segment file name
pub fn segment_number(name: &str) -> Option<u32> {
    name.strip_suffix(SEGMENT_SUFFIX)?.parse().ok()
}
