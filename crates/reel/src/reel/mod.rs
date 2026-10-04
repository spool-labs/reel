//! The volume's directory of append tails
//!
//! A reel owns every segment file on the volume, the append sequence counter that
//! orders its records, and the monotonic segment numbering its tails draw from.
//! Writes route to the least-loaded tail. Every column shares the one log, so a
//! batch spanning columns is one durability point and one recovery domain.

pub mod bands;
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

use crate::append::admission::InflightBudget;
use crate::append::{Appender, BatchRecord, BatchWrite, Commit, Committed};
use crate::config::{PointReads, RangedReads, ReelConfig};
use crate::error::{ReelError, Result};
use std::sync::OnceLock;

use crate::format::band::Band;
use crate::format::block::{lookup_in_span, FooterMap, RowBlock};
use crate::format::column::{ColumnId, ColumnSet, KeyRef, PurgeMark, RecordKey};
use crate::format::fence::{FenceCut, FenceReach};
use crate::format::footer::{FooterFind, FooterRow, FooterTally, SegmentFooter};
use crate::format::loc::{Loc, SegmentId};
use crate::format::lsn::{Lsn, LsnCounter};
use crate::format::record::HEADER_LEN;
use crate::index::counters::{FilterProbes, SegmentTable};
use crate::index::fastforward::{FastRead, Head, HeadRead, RecordSource};
use crate::index::paged::{FooterCache, FooterSource};
use crate::index::recovery::{read_footer, ResumableTail};
use crate::index::tbtreemap::{TBTreeMap, NODE_WIDTH};
use crate::io::op::{Advice, ColdRoute, Completion, FileId, Op, WarmFirst};
use crate::reel::bands::BandPool;
use crate::reel::segment::{DirectOpen, FdCache, IoDriver, SegmentHandle, SplitAnswer, SplitRead};
use crate::sync::{lock, read, write};

use reel_core::{ReadBlock, Value};

use read::{
    check_in_block, cut_range, deep_range, frame_to_range, frame_to_read, framed_or_nothing, merge_runs_into,
    merge_span, near_range, place_runs, window_or_nothing, window_start, Planned, Run, MERGE_GAP,
};

/// The first segment number a fresh reel numbers from
const FIRST_SEGMENT: u32 = 1;

/// The floor of a volume that has purged nothing, which no key sits below
pub const NOTHING_PURGED: u64 = 0;

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

/// Where FastForward reads the records its entries point at
impl RecordSource for ReelShared {
    fn head(&self, key: KeyRef<'_>, segment: SegmentId, offset: u32) -> Result<HeadRead> {
        let Some(handle) = self.handle_for(segment)? else {
            return Ok(HeadRead::Missing);
        };
        let prefix = (HEADER_LEN + key.bytes.len()) as u64;
        let bytes = self.driver.pread(handle.file(), u64::from(offset), prefix)?;
        Ok(head_read(&bytes, key))
    }

    fn cached_head(&self, key: KeyRef<'_>, segment: SegmentId, offset: u32) -> Result<HeadRead> {
        let Some(handle) = self.handle_for(segment)? else {
            return Ok(HeadRead::Missing);
        };
        let prefix = HEADER_LEN + key.bytes.len();
        Ok(match self.driver.warm_only(handle.file(), u64::from(offset), prefix) {
            Some(bytes) => head_read(&bytes, key),
            None => HeadRead::Cold,
        })
    }

    fn record(&self, key: &RecordKey, segment: SegmentId, offset: u32, bound: u32) -> Result<FastRead> {
        let Some(handle) = self.handle_for(segment)? else {
            return Ok(FastRead::Gone);
        };
        let prefix = HEADER_LEN + key.as_slice().len();
        let answer = self.driver.pread_split_reusing(
            handle.file(),
            u64::from(offset),
            prefix,
            bound as usize,
            take_header(),
            self.warm_first(),
        );
        match self.fast_verdict(answer, key, segment, offset)? {
            Verdict::Read(read) => Ok(read),
            Verdict::Whole(head) => {
                self.whole_record(handle.file(), key, head, Loc::new(segment, offset, head.len))
            }
        }
    }
}

/// What one FastForward range read settled
pub enum FastRange {
    /// The key's record at this candidate, and the window of its payload asked for
    Found(Head, Value),
    Tombstone(Head),
    Other,
    Gone,

    /// A coded record, or one the read came up short on, which the checked path reads
    Unsure,
}

/// One FastForward candidate a batch reads
pub struct FastAsk<'a> {
    pub key: &'a RecordKey,
    pub segment: SegmentId,
    pub offset: u32,
    pub bound: u32,
}

/// What one bounded read of a FastForward candidate settled
enum Verdict {
    Read(FastRead),

    /// The file ends inside the bound, so the record reads again at its own length
    Whole(Head),
}

impl ReelShared {
    /// The record at a place, read as a future in one read of its header and up to `bound` payload bytes
    pub async fn fast_record_wait(
        &self,
        key: &RecordKey,
        segment: SegmentId,
        offset: u32,
        bound: u32,
    ) -> Result<FastRead> {
        let Some(handle) = self.handle_for(segment)? else {
            return Ok(FastRead::Gone);
        };
        let prefix = HEADER_LEN + key.as_slice().len();
        let answer = self
            .driver
            .wait_split_reusing(
                handle.file(),
                u64::from(offset),
                prefix,
                bound as usize,
                take_header(),
                self.warm_first(),
            )
            .await;
        match self.fast_verdict(answer, key, segment, offset)? {
            Verdict::Read(read) => Ok(read),
            Verdict::Whole(head) => {
                let loc = Loc::new(segment, offset, head.len);
                self.whole_record_wait(handle.file(), key, head, loc).await
            }
        }
    }

    /// One record read as a future at the length its header gave
    async fn whole_record_wait(&self, file: FileId, key: &RecordKey, head: Head, loc: Loc) -> Result<FastRead> {
        let prefix = HEADER_LEN + key.as_slice().len();
        let len = loc.len as usize;
        let read = self
            .driver
            .wait_split_reusing(file, u64::from(loc.offset), prefix, len, take_header(), self.warm_first())
            .await;
        Ok(match framed_or_nothing(read, prefix, len)? {
            Some((bytes, body)) => fast_read_of(
                frame_to_read(bytes, body, key.as_ref(), head.lsn, loc, self.config.verify_reads),
                head,
            ),
            None => FastRead::Other,
        })
    }

    /// A window of the record at a place, its header and the window in one round trip
    ///
    /// A window near the payload's front comes in one span with the header, and a deeper
    /// one is a second read submitted with the first. The key is confirmed from the
    /// header and the window cut to the payload the header gives.
    pub fn fast_range(&self, key: &RecordKey, segment: SegmentId, offset: u32, at: u64, len: usize) -> Result<FastRange> {
        let Some(handle) = self.handle_for(segment)? else {
            return Ok(FastRange::Gone);
        };
        let prefix = HEADER_LEN + key.as_slice().len();
        let base = u64::from(offset);
        if at <= MERGE_GAP {
            let span = at as usize + len;
            let answer = self.driver.pread_split_reusing(handle.file(), base, prefix, span, take_header(), self.warm_first());
            return near_fast_range(answer, key, at, len);
        }
        let ops = vec![
            self.driver.split_read(handle.file(), base, 0, prefix),
            self.driver.split_read(handle.file(), base + prefix as u64 + at, 0, len),
        ];
        deep_fast_range(self.driver.run_split_reads(ops)?, key, at, len)
    }

    /// The same window as a future, through the driver
    pub async fn fast_range_wait(
        &self,
        key: &RecordKey,
        segment: SegmentId,
        offset: u32,
        at: u64,
        len: usize,
    ) -> Result<FastRange> {
        let Some(handle) = self.handle_for(segment)? else {
            return Ok(FastRange::Gone);
        };
        let prefix = HEADER_LEN + key.as_slice().len();
        let base = u64::from(offset);
        if at <= MERGE_GAP {
            let span = at as usize + len;
            let answer = self
                .driver
                .wait_split_reusing(handle.file(), base, prefix, span, take_header(), WarmFirst::Skip)
                .await;
            return near_fast_range(answer, key, at, len);
        }
        let ops = vec![
            self.driver.split_read(handle.file(), base, 0, prefix),
            self.driver.split_read(handle.file(), base + prefix as u64 + at, 0, len),
        ];
        deep_fast_range(self.driver.wait_split_reads(ops).await?, key, at, len)
    }

    /// One bounded read of each FastForward candidate, submitted together
    pub fn fast_records(&self, asks: &[FastAsk<'_>]) -> Result<Vec<FastRead>> {
        let (ops, handles) = self.fast_ops(asks)?;
        let filled = self.driver.run_split_reads(ops)?;
        let mut filled = filled.into_iter();
        let mut reads = Vec::with_capacity(asks.len());
        for (ask, handle) in asks.iter().zip(&handles) {
            let Some(handle) = handle else {
                reads.push(FastRead::Gone);
                continue;
            };
            let answer = next_split(&mut filled)?;
            reads.push(match self.fast_verdict(answer, ask.key, ask.segment, ask.offset)? {
                Verdict::Read(read) => read,
                Verdict::Whole(head) => {
                    self.whole_record(handle.file(), ask.key, head, Loc::new(ask.segment, ask.offset, head.len))?
                }
            });
        }
        Ok(reads)
    }

    /// The same batch as a future, one submission and one wait
    pub async fn fast_records_wait(&self, asks: &[FastAsk<'_>]) -> Result<Vec<FastRead>> {
        let (ops, handles) = self.fast_ops(asks)?;
        let filled = self.driver.wait_split_reads(ops).await?;
        let mut filled = filled.into_iter();
        let mut reads = Vec::with_capacity(asks.len());
        for (ask, handle) in asks.iter().zip(&handles) {
            let Some(handle) = handle else {
                reads.push(FastRead::Gone);
                continue;
            };
            let answer = next_split(&mut filled)?;
            reads.push(match self.fast_verdict(answer, ask.key, ask.segment, ask.offset)? {
                Verdict::Read(read) => read,
                Verdict::Whole(head) => {
                    let loc = Loc::new(ask.segment, ask.offset, head.len);
                    self.whole_record_wait(handle.file(), ask.key, head, loc).await?
                }
            });
        }
        Ok(reads)
    }

    /// The bounded split read of each candidate, and the handle holding its segment open
    fn fast_ops(&self, asks: &[FastAsk<'_>]) -> Result<(Vec<Op>, Vec<Option<SegmentHandle>>)> {
        let mut ops = Vec::with_capacity(asks.len());
        let mut handles = Vec::with_capacity(asks.len());
        for ask in asks {
            let handle = self.handle_for(ask.segment)?;
            if let Some(handle) = &handle {
                let prefix = HEADER_LEN + ask.key.as_slice().len();
                ops.push(self.driver.split_read(handle.file(), u64::from(ask.offset), prefix, ask.bound as usize));
            }
            handles.push(handle);
        }
        Ok((ops, handles))
    }

    /// Settle one bounded read of a candidate, or say it needs a read at the record's own length
    fn fast_verdict(&self, answer: SplitAnswer, key: &RecordKey, segment: SegmentId, offset: u32) -> Result<Verdict> {
        let (bytes, mut body) = match answer {
            Ok(read) => read,
            Err((error, spare)) => {
                recycle_header(spare);
                return match is_missing(&error) {
                    true => Ok(Verdict::Read(FastRead::Gone)),
                    false => Err(error),
                };
            }
        };
        let verdict = match head_read(&bytes, key.as_ref()) {
            HeadRead::Same(head) if head.is_tombstone => Verdict::Read(FastRead::Tombstone(head)),
            HeadRead::Same(head) if body.len() >= head.len as usize => {
                body.truncate(head.len as usize);
                let loc = Loc::new(segment, offset, head.len);
                let read = frame_to_read(bytes, body, key.as_ref(), head.lsn, loc, self.config.verify_reads);
                return Ok(Verdict::Read(fast_read_of(read, head)));
            }
            HeadRead::Same(head) => Verdict::Whole(head),
            HeadRead::Other | HeadRead::Missing | HeadRead::Cold => Verdict::Read(FastRead::Other),
        };
        recycle_header(bytes);
        crate::reel::payload::give(body);
        Ok(verdict)
    }

    /// One record read at the length its header gave
    fn whole_record(&self, file: FileId, key: &RecordKey, head: Head, loc: Loc) -> Result<FastRead> {
        let prefix = HEADER_LEN + key.as_slice().len();
        let len = loc.len as usize;
        let read = self.driver.pread_split_reusing(
            file,
            u64::from(loc.offset),
            prefix,
            len,
            take_header(),
            self.warm_first(),
        );
        Ok(match framed_or_nothing(read, prefix, len)? {
            Some((bytes, body)) => fast_read_of(
                frame_to_read(bytes, body, key.as_ref(), head.lsn, loc, self.config.verify_reads),
                head,
            ),
            None => FastRead::Other,
        })
    }
}

/// The payload bytes of a window that a record of this length holds
fn window_of(head: &Head, at: u64, len: usize) -> usize {
    (u64::from(head.len).saturating_sub(at)).min(len as u64) as usize
}

/// What a header says about a ranged read: the record, or why the window cannot come from it
fn range_head(prefix: &[u8], key: &RecordKey) -> std::result::Result<Head, FastRange> {
    match head_read(prefix, key.as_ref()) {
        HeadRead::Same(head) if head.is_tombstone => Err(FastRange::Tombstone(head)),
        // A coded record's window is of the payload it decodes to, so only a whole read cuts it.
        HeadRead::Same(head) => match crate::format::record::data_codec(prefix, key.as_ref(), head.lsn, head.len) {
            Some(0) => Ok(head),
            Some(_) | None => Err(FastRange::Unsure),
        },
        HeadRead::Other | HeadRead::Missing | HeadRead::Cold => Err(FastRange::Other),
    }
}

/// A window that came in one span with its record's header
fn near_fast_range(answer: SplitAnswer, key: &RecordKey, at: u64, len: usize) -> Result<FastRange> {
    let (bytes, body) = match answer {
        Ok(read) => read,
        Err((error, spare)) => {
            recycle_header(spare);
            return match is_missing(&error) {
                true => Ok(FastRange::Gone),
                false => Err(error),
            };
        }
    };
    let verdict = match range_head(&bytes, key) {
        Ok(head) => {
            let wanted = window_of(&head, at, len);
            match body.len() >= at as usize + wanted {
                true => match cut_range(body, at as usize, wanted) {
                    Some(window) => FastRange::Found(head, window),
                    None => FastRange::Unsure,
                },
                false => {
                    crate::reel::payload::give(body);
                    FastRange::Unsure
                }
            }
        }
        Err(verdict) => {
            crate::reel::payload::give(body);
            verdict
        }
    };
    recycle_header(bytes);
    Ok(verdict)
}

/// A window that came as its own read beside its record's header
fn deep_fast_range(filled: Vec<SplitRead>, key: &RecordKey, at: u64, len: usize) -> Result<FastRange> {
    let mut filled = filled.into_iter();
    let header = next_split(&mut filled)?;
    let window = next_split(&mut filled)?;
    let (head_bytes, window_bytes) = match (header, window) {
        (Ok((empty, head)), Ok((spare, window))) => {
            crate::reel::payload::give(empty);
            crate::reel::payload::give(spare);
            (head, window)
        }
        (Err((error, _)), _) | (_, Err((error, _))) => {
            return match is_missing(&error) {
                true => Ok(FastRange::Gone),
                false => Err(error),
            };
        }
    };
    let verdict = match range_head(&head_bytes, key) {
        Ok(head) => {
            let wanted = window_of(&head, at, len);
            match window_bytes.len() >= wanted {
                true => match cut_range(window_bytes, 0, wanted) {
                    Some(window) => FastRange::Found(head, window),
                    None => FastRange::Unsure,
                },
                false => {
                    crate::reel::payload::give(window_bytes);
                    FastRange::Unsure
                }
            }
        }
        Err(verdict) => {
            crate::reel::payload::give(window_bytes);
            verdict
        }
    };
    crate::reel::payload::give(head_bytes);
    Ok(verdict)
}

/// The next answer of a batch, as a single read would have it
fn next_split(filled: &mut impl Iterator<Item = SplitRead>) -> Result<SplitAnswer> {
    match filled.next() {
        Some(read) => Ok(read.map_err(|error| (error, Vec::new()))),
        None => Err(ReelError::Backend(
            "a FastForward batch came back with fewer answers than it asked".to_string(),
        )),
    }
}

/// What a record's header says about one key, a pad or another key reading as other
fn head_read(prefix: &[u8], key: KeyRef<'_>) -> HeadRead {
    match crate::format::record::head_for(prefix, key) {
        Some((lsn, len, flags)) if flags.is_data() || flags.is_tombstone() => HeadRead::Same(Head {
            lsn,
            len,
            is_tombstone: flags.is_tombstone(),
        }),
        Some(_) | None => HeadRead::Other,
    }
}

/// What a checked read of one FastForward candidate settles
fn fast_read_of(read: RecordRead, head: Head) -> FastRead {
    match read {
        RecordRead::Found(value) => FastRead::Found(head, value),
        RecordRead::Stale => FastRead::Other,
        RecordRead::Gone | RecordRead::Corrupt | RecordRead::Coded => FastRead::Unsure,
    }
}

/// The footers a paged index resolves its sealed keys through
impl FooterSource for ReelShared {
    fn footer(&self, segment: SegmentId) -> Result<Option<Arc<SegmentFooter>>> {
        self.footer_of(segment)
    }

    fn footer_once(&self, segment: SegmentId) -> Result<Option<Arc<SegmentFooter>>> {
        if let Some(footer) = self.footers.get(segment) {
            return Ok(Some(footer));
        }
        Ok(self.footer_from_disk(segment)?.map(Arc::new))
    }

    /// One key, answered by reading the blocks the search touches and no more
    fn find(&self, segment: SegmentId, column: ColumnId, key: &[u8]) -> Result<Option<FooterRow>> {
        self.probes.note_probe();
        if let Some(footer) = self.footers.get(segment) {
            let Some(partition) = footer.partition(column) else {
                return Ok(None);
            };
            return Ok(self.answer_of(partition.lookup(key)?));
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

    /// The mark of each column placed by it, so a write's band is one index
    placement_marks: Vec<Option<PurgeMark>>,

    /// Timeline position everything below which the volume is finished with
    purge_floor: AtomicU64,

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

    /// Segments released without a footer, until one lands or their file goes
    unsealed: Mutex<std::collections::HashSet<SegmentId>>,

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

    /// Whether a caller is already noting this one's spans
    is_taken: bool,

    /// The footer the seal wrote, so naming and handing over never read it back
    footer: Arc<SegmentFooter>,
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
        let mut placement_marks = vec![None; COLUMN_SLOTS];
        for spec in columns {
            placement_marks[spec.id.as_index()] = spec.placement_mark();
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
            placement_marks,
            purge_floor: AtomicU64::new(NOTHING_PURGED),
            next_segment: AtomicU32::new(next_segment.max(FIRST_SEGMENT)),
            sealed_pending: Mutex::new(Vec::new()),
            sealed_waiting: AtomicBool::new(false),
            holds: RwLock::new(TBTreeMap::new()),
            held_floor: AtomicU32::new(u32::MAX),
            unsealed: Mutex::new(std::collections::HashSet::new()),
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

    /// The band a write of this key belongs in, for a column placed by its mark
    pub fn band_of(&self, key: &RecordKey) -> Option<Band> {
        let mark = self.placement_marks[key.column.as_index()]?;
        Some(Band::of(mark.read(key.as_slice()), self.purge_floor()))
    }

    /// Move the floor everything below which the volume is finished with, upwards only
    pub fn purge_below(&self, floor: u64) {
        self.purge_floor.fetch_max(floor, Ordering::AcqRel);
    }

    /// The floor compaction drops records below, and bands are measured from
    pub fn purge_floor(&self) -> u64 {
        self.purge_floor.load(Ordering::Acquire)
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
        let Some(footer) = self.footer_from_disk(segment)? else {
            return Ok(None);
        };
        let footer = Arc::new(footer);
        self.footers.insert(segment, Arc::clone(&footer));
        Ok(Some(footer))
    }

    /// One sealed segment's footer read from its file, with no cache asked or filled
    fn footer_from_disk(&self, segment: SegmentId) -> Result<Option<SegmentFooter>> {
        let Some(handle) = self.handle_for(segment)? else {
            return Ok(None);
        };
        let file_len = match self.driver.length(handle.file()) {
            Ok(len) => len,
            Err(error) if error.is_missing() => return Ok(None),
            Err(error) => return Err(error),
        };
        read_footer(&self.driver, handle.file(), file_len)
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
    pub fn note_sealed(&self, segment: SegmentId, footer: Arc<SegmentFooter>) {
        // The window where a seal is durable but the index has not been told.
        crate::sync::rendezvous::at("seal/queued");
        lock(&self.sealed_pending).push(Pending {
            segment,
            is_taken: false,
            footer,
        });
        self.sealed_waiting.store(true, Ordering::Release);
    }

    /// Whether a segment has sealed that the index has not been told about
    pub fn has_sealed_waiting(&self) -> bool {
        self.sealed_waiting.load(Ordering::Acquire)
    }

    /// The segments sealed since this was last asked, each with the footer its seal wrote
    ///
    /// They stay on the queue, where a compaction pass can still see their spans
    /// are owed; clearing the flag only sends other callers past this batch.
    pub fn peek_sealed(&self) -> Vec<(SegmentId, Arc<SegmentFooter>)> {
        let mut pending = lock(&self.sealed_pending);
        self.sealed_waiting.store(false, Ordering::Release);
        let mut taken = Vec::with_capacity(pending.len());
        for entry in pending.iter_mut().filter(|entry| !entry.is_taken) {
            entry.is_taken = true;
            taken.push((entry.segment, Arc::clone(&entry.footer)));
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

    /// Segments whose spans the index has not been told about yet
    pub fn pending_seals(&self) -> Vec<SegmentId> {
        lock(&self.sealed_pending)
            .iter()
            .map(|entry| entry.segment)
            .collect()
    }

    /// Register the hold for a segment an appender resumes, marking it a tail
    ///
    /// The number was drawn by a previous process, so nothing advances here;
    /// the segment only comes back under a tail's hold.
    pub fn adopt_segment(&self, id: SegmentId) -> Arc<SegmentHolds> {
        let mut map = write(&self.holds);
        let held = match map.get(&id) {
            Some(Some(held)) => Arc::clone(held),
            _ => {
                let held: Arc<SegmentHolds> = Arc::default();
                map.insert(id, Some(Arc::clone(&held)));
                held
            }
        };
        self.refresh_held_floor(&map);
        held.is_tail.store(true, Ordering::Release);
        held
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
    /// mark answers before the holds do. The mark names that segment and no other:
    /// one dooming says nothing about the segments already sealed under it.
    pub fn is_settled(&self, id: SegmentId) -> bool {
        if lock(&self.unsealed).contains(&id) {
            return false;
        }
        id.as_u32() < self.held_floor.load(Ordering::Relaxed) || !self.is_held(id)
    }

    /// Note a segment released without its footer, which never reads as settled
    pub fn note_unsealed(&self, id: SegmentId) {
        lock(&self.unsealed).insert(id);
    }

    /// Take the mark off a segment that has landed a footer, or whose file has gone
    pub fn forget_unsealed(&self, id: SegmentId) {
        lock(&self.unsealed).remove(&id);
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

    /// A codec produced the record's stored bytes, so no window of it is on disk
    Coded,
}

/// One record a resolved batch asks the device for
///
/// The key is named by position rather than carried, so the list of asks holds no
/// borrow and a thread can keep it between submissions.
/// Where a placed read left one record: a window of one of its blocks
///
/// A codec other than zero says the window holds the record's stored bytes, which
/// decode to the payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Spot {
    pub block: u32,
    pub at: u32,
    pub len: u32,
    pub codec: u8,
}

impl Spot {
    /// A record the read did not find where its ask said
    pub const MISS: Spot = Spot { block: u32::MAX, at: 0, len: 0, codec: 0 };
}

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
    /// Each ask's place on the volume beside its position, sorted into volume order
    order: Vec<(u64, u32)>,

    /// Every ask resolved to a place on the volume
    plan: Vec<Planned>,

    /// One handle per segment the plan reads, so none is unlinked under a read in flight
    handles: Vec<SegmentHandle>,

    /// The reads the plan was grouped into
    runs: Vec<Run>,

    /// One op per run, built before anything is submitted
    ops: Vec<Op>,

    /// Completions the backend answered a batch with, in submit order
    completions: Vec<Completion>,

    /// What each run's read filled
    filled: Vec<SplitRead>,
}

impl ReadScratch {
    const fn empty() -> ReadScratch {
        ReadScratch {
            order: Vec::new(),
            plan: Vec::new(),
            handles: Vec::new(),
            runs: Vec::new(),
            ops: Vec::new(),
            completions: Vec::new(),
            filled: Vec::new(),
        }
    }

    /// Drop everything the last batch left, keeping the room it grew into
    fn release(&mut self) {
        self.order.clear();
        self.plan.clear();
        self.handles.clear();
        self.runs.clear();
        self.ops.clear();
        self.completions.clear();
        self.filled.clear();
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
    bands: BandPool,
}

impl Reel {
    /// Open a reel with the configured number of active tails
    ///
    /// A volume that rewrites at seal, or that owns a capacity tier, keeps one extra
    /// tail back for compaction: a sorted run is only sorted if nothing else is
    /// writing into it. The reserved tail is the last one and route never offers it.
    ///
    /// Tails a previous process left unsealed are picked up in number order, so a restart
    /// continues its segments rather than drawing new ones. The reserved tail never
    /// resumes: a merge's output has to be nothing but its own runs.
    pub fn open(shared: Arc<ReelShared>, resumable: Vec<ResumableTail>) -> Result<Reel> {
        let count = shared.config.tail_count();
        let reserved = shared.config.rewrite_on_seal || shared.volumes.has_capacity();
        let total = count + usize::from(reserved);
        let mut candidates = resumable.into_iter();
        let mut tails = Vec::with_capacity(total);
        for index in 0..total {
            let is_reserved = reserved && index == total - 1;
            let adopted = match is_reserved {
                true => None,
                false => candidates.next(),
            };
            tails.push(Appender::open(Arc::clone(&shared), index as u64, adopted)?);
        }
        Ok(Reel {
            shared,
            tails,
            bands: BandPool::new(count),
        })
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
            bands: BandPool::new(0),
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

    /// Append or overwrite a payload, routed to the tail its key's band is on
    pub fn put(
        &self,
        key: RecordKey,
        payload: Vec<u8>,
        codec: u8,
        commit: Commit,
    ) -> Result<Committed> {
        self.route(self.shared.band_of(&key))?
            .append_data(key, payload, codec, commit)
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
        self.route(self.shared.band_of(&key))?
            .append_data_wait(key, payload, codec, commit)
            .await
    }

    /// Append a tombstone for one key, routed to the least-loaded tail
    ///
    /// A tombstone takes no band, whatever its key says: it dies when compaction can
    /// drop it rather than when the record it hides was going to die.
    pub fn delete(&self, key: RecordKey, commit: Commit) -> Result<Committed> {
        self.route(None)?.append_tombstone(key, commit)
    }

    /// Append a tombstone covering a half-open key range within one column
    pub fn delete_range(
        &self,
        start: RecordKey,
        end: Option<&[u8]>,
        commit: Commit,
    ) -> Result<Committed> {
        self.route(None)?.append_range_tombstone(start, end, commit)
    }

    /// Append a whole batch to one tail as one reservation and one write
    ///
    /// Everything the batch carries lands together or not at all: the records go down
    /// back to back behind a frame declaring their count and their span, and a rebuild
    /// keeps the run only when it reads exactly what the frame declared. One tail takes
    /// all of it, so one band covers it.
    pub fn write_batch(&self, records: Vec<BatchRecord>) -> Result<Vec<Committed>> {
        self.route(self.batch_band(&records))?.append_batch(records)
    }

    /// The same batch awaited, admitted without holding a thread for the budget
    pub async fn write_batch_wait(&self, records: Vec<BatchRecord>) -> Result<Vec<Committed>> {
        self.route(self.batch_band(&records))?
            .append_batch_wait(records)
            .await
    }

    /// The band each foreground tail is drawing under, for a caller reporting placement
    pub fn tail_bands(&self) -> Vec<Option<Band>> {
        self.bands.owners()
    }

    /// Banded writes that found no tail free and went to the unbanded ones instead
    pub fn band_fallbacks(&self) -> u64 {
        self.bands.fallbacks()
    }

    /// The tail a write of this band belongs in, claiming one where the band has none
    ///
    /// Compaction places its survivors through here too, so a rewritten record ends up
    /// beside the fresh records of its own window rather than back in the mixture.
    pub fn place(&self, band: Option<Band>) -> Result<usize> {
        self.bands
            .place(self.foreground(), band, self.shared.purge_floor())
    }

    /// The band covering a whole batch, which is the last of its records to die
    fn batch_band(&self, records: &[BatchRecord]) -> Option<Band> {
        let mut widest = None;
        for record in records {
            // Anything unplaced takes the batch out of every band, since a window is
            // only worth unlinking whole if everything in it is dead by the number.
            match record.write {
                BatchWrite::Put(_, _) => {}
                BatchWrite::Delete | BatchWrite::DeleteRange(_) => return None,
            }
            let band = self.shared.band_of(&record.key)?;
            widest = Some(widest.map_or(band, |held: Band| held.max(band)));
        }
        widest
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

        if self.maps(loc.segment, len) {
            if let Some(map) = handle.mapping(self.shared.config.segment_bytes.to_bytes()) {
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
        if !self.maps(handle.id(), len) {
            return None;
        }
        let map = handle.mapping(self.shared.config.segment_bytes.to_bytes())?;
        let head_bytes = map.slice(offset, prefix)?;
        let body_bytes = map.slice(offset + prefix as u64 + at, len)?;

        let mut head = take_header();
        head.clear();
        head.extend_from_slice(head_bytes);
        let mut body = crate::reel::payload::take(len);
        body.extend_from_slice(body_bytes);
        Some((head, Value::pooled(body, crate::reel::payload::give)))
    }

    /// Read several records with one submission, each left in place in its run's block
    ///
    /// One spot per key, in the caller's order, into vectors the caller keeps. Records
    /// written in one batch are one contiguous byte range, so neighbours are read
    /// together and every record is framed inside its run's block. A walk lends
    /// straight from the blocks, and a caller keeping a record takes a window of one.
    /// A spot of `Spot::MISS` is a record the caller resolves again: its segment is
    /// gone, the pointer is stale, or the bytes failed their checks.
    pub fn read_placed(
        &self,
        asks: &[Ask],
        keys: &[KeyRef<'_>],
        is_verified: bool,
        blocks: &mut Vec<ReadBlock>,
        spots: &mut Vec<Spot>,
    ) -> Result<()> {
        let mut held = HeldScratch::take();
        let scratch = &mut held.0;
        if self.plan_reads(asks, keys, is_verified, scratch, blocks, spots)? {
            self.shared.driver.run_split_reads_into(
                &mut scratch.ops,
                &mut scratch.completions,
                &mut scratch.filled,
            )?;
            place_runs(scratch, asks, keys, is_verified, blocks, spots)?;
        }
        Ok(())
    }

    /// Read several records as one future, each left in place in its run's block
    pub async fn read_placed_wait(
        &self,
        asks: &[Ask],
        keys: &[KeyRef<'_>],
        is_verified: bool,
        blocks: &mut Vec<ReadBlock>,
        spots: &mut Vec<Spot>,
    ) -> Result<()> {
        let mut held = HeldScratch::take();
        let scratch = &mut held.0;
        if self.plan_reads(asks, keys, is_verified, scratch, blocks, spots)? {
            self.shared
                .driver
                .wait_split_reads_into(&mut scratch.ops, &mut scratch.filled)
                .await?;
            place_runs(scratch, asks, keys, is_verified, blocks, spots)?;
        }
        Ok(())
    }

    /// Place every record a mapping covers and build one read per run of the rest
    ///
    /// A mapped record is checked where it lies and copied out once, into one block
    /// for the batch. Key order is not offset order, so the asks left for the driver
    /// are sorted by place first: runs only form once neighbours are neighbours. An
    /// ask whose segment is gone stays a miss. False when nothing is left to read.
    fn plan_reads(
        &self,
        asks: &[Ask],
        keys: &[KeyRef<'_>],
        is_verified: bool,
        scratch: &mut ReadScratch,
        blocks: &mut Vec<ReadBlock>,
        spots: &mut Vec<Spot>,
    ) -> Result<bool> {
        blocks.clear();
        spots.clear();
        spots.resize(keys.len(), Spot::MISS);
        scratch.order.clear();
        scratch.plan.clear();
        scratch.handles.clear();
        let mut mapped = Vec::new();
        for (at, ask) in asks.iter().enumerate() {
            let key = keys[ask.at as usize];
            let prefix = HEADER_LEN + key.width();
            let len = ask.loc.len as usize;
            let offset = u64::from(ask.loc.offset);
            let record = match self.maps(ask.loc.segment, len) {
                true => match self.hold_segment(ask.loc.segment, &mut scratch.handles)? {
                    Some(handle) => handle
                        .mapping(self.shared.config.segment_bytes.to_bytes())
                        .and_then(|map| map.slice(offset, prefix + len)),
                    None => continue,
                },
                false => None,
            };
            let Some(record) = record else {
                let place = (u64::from(ask.loc.segment.0) << 32) | offset;
                scratch.order.push((place, at as u32));
                continue;
            };
            if let Ok(codec) = check_in_block(record, 0, prefix, key, ask.lsn, ask.loc, is_verified)
            {
                if mapped.capacity() == 0 {
                    let wanted = asks.iter().map(|ask| ask.loc.len as usize).sum();
                    mapped = crate::reel::payload::take(wanted);
                }
                spots[ask.at as usize] = Spot {
                    block: 0,
                    at: mapped.len() as u32,
                    len: len as u32,
                    codec,
                };
                mapped.extend_from_slice(&record[prefix..]);
            }
        }
        if mapped.capacity() != 0 {
            blocks.push(ReadBlock::new(mapped, crate::reel::payload::give));
        }
        scratch.order.sort_unstable_by_key(|&(place, _)| place);

        scratch.plan.reserve(scratch.order.len());
        for slot in 0..scratch.order.len() {
            let at = scratch.order[slot].1 as usize;
            let ask = &asks[at];
            // Sorted by segment, so a segment's handle is asked for once, at its first record.
            let file = match scratch.plan.last() {
                Some(last) if last.segment == ask.loc.segment => last.file,
                _ => match self.hold_segment(ask.loc.segment, &mut scratch.handles)? {
                    Some(handle) => handle.file(),
                    None => continue,
                },
            };
            scratch.plan.push(Planned {
                at,
                segment: ask.loc.segment,
                file,
                offset: u64::from(ask.loc.offset),
                prefix: HEADER_LEN + keys[ask.at as usize].width(),
                len: ask.loc.len as usize,
            });
        }
        if scratch.plan.is_empty() {
            return Ok(false);
        }

        merge_runs_into(
            &scratch.plan,
            merge_span(self.shared.driver.serving()),
            &mut scratch.runs,
        );
        // Every record's header sits inside its run's span, so each run is one read
        // of the whole span, with no header of its own.
        scratch.ops.clear();
        scratch.ops.reserve(scratch.runs.len());
        for run in &scratch.runs {
            let first = &scratch.plan[run.start];
            let op = self
                .shared
                .driver
                .split_read(first.file, first.offset, 0, run.span as usize);
            scratch.ops.push(op);
        }
        Ok(true)
    }

    /// Take a segment's handle once per batch and lend it for every record after
    ///
    /// Held until the batch is done, so no segment is unlinked under a read in flight.
    /// The handles stay in segment order, so finding one is a search. Nothing when the
    /// segment is gone.
    fn hold_segment<'held>(
        &self,
        segment: SegmentId,
        handles: &'held mut Vec<SegmentHandle>,
    ) -> Result<Option<&'held SegmentHandle>> {
        let at = match handles.binary_search_by_key(&segment, SegmentHandle::id) {
            Ok(at) => at,
            Err(at) => match self.handle_for(segment)? {
                Some(handle) => {
                    handles.insert(at, handle);
                    at
                }
                None => return Ok(None),
            },
        };
        Ok(Some(&handles[at]))
    }

    /// Whether a read of `len` bytes in `segment` goes through the segment's mapping
    ///
    /// A tail's pages were just written, so they are in the page cache and a mapping
    /// copies them with no syscall. A scan of 1,000 keys reads its tail keys one at a
    /// time, which made preads 84% of its time.
    fn maps(&self, segment: SegmentId, len: usize) -> bool {
        let config = &self.shared.config;
        config.maps(len)
            || (config.maps_tails() && self.tails().iter().any(|tail| tail.tail().active_segment() == segment))
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
        if self.maps(handle.id(), len) {
            if let Some(map) = handle.mapping(self.shared.config.segment_bytes.to_bytes()) {
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

    /// The tail a foreground write goes to
    ///
    /// The band is settled here and the record is written after, so a claim landing in
    /// between leaves that one record in the segment the tail has just drawn. The window
    /// is one claim wide and it costs placement, not correctness.
    fn route(&self, band: Option<Band>) -> Result<&Appender> {
        let foreground = self.foreground();
        if band.is_none() && self.bands.is_idle() {
            let mut chosen = &foreground[0];
            let mut lowest = chosen.load();
            for tail in &foreground[1..] {
                let load = tail.load();
                if load < lowest {
                    lowest = load;
                    chosen = tail;
                }
            }
            return Ok(chosen);
        }
        let at = self
            .bands
            .place(foreground, band, self.shared.purge_floor())?;
        Ok(&foreground[at])
    }
}

thread_local! {
    /// One header buffer per reading thread, handed back after every framed read
    static HEADER_SPARE: std::cell::Cell<Vec<u8>> = const { std::cell::Cell::new(Vec::new()) };
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
