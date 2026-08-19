//! Per-tail append path over one active segment
//!
//! Writers reserve their own byte range in the active segment and copy their record
//! into it, so the copy runs on the thread that wanted the write. The reservation is a
//! single atomic step over the write head and the only point writers contend on; one
//! that runs past the end of the segment is given up, and its writer rolls and retries.

pub mod admission;
pub mod codec;
mod flush;
pub(crate) mod publish;
mod sealer;

#[cfg(test)]
mod tests;

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::config::{Preallocate, SyncPolicy, VolumeClass};
use crate::error::{ReelError, Result};
use crate::format::column::RecordKey;
use crate::format::footer::{FooterEntry, SegmentFooter};
use crate::format::loc::{Loc, SegmentId};
use crate::format::lsn::{Lsn, LsnCounter};
use crate::format::record::{align_up, BatchFrame, Flags, RecordHeader, BLOCK, HEADER_LEN};
use crate::format::segment_header::SegmentHeader;
use crate::io::op::{Op, OwnedBuf, SyncRangeMode, WriteBuf};
use crate::reel::segment::{IoDriver, SegmentHandle};
use crate::reel::tail::Tail;
use crate::reel::{ReelShared, SegmentHolds};
use crate::sync::{lock, read, try_lock, write};

use flush::{turn_at, Owed, SyncState, Turn};
pub use flush::{Durability, FlushTurn};
use sealer::{
    doom_active, flush_active, park_broken_seal, publish_flush, retire_segment, seal_segment,
    stamp_failed_range, Sealer,
};
pub(crate) use sealer::{retry_broken_seals, BrokenSeal};

/// The block boundary a whole-block volume starts every record on
const ALIGN: u64 = BLOCK;

/// Bytes a tail lets build behind the write head before it starts the device on them
const WRITEBACK_CHUNK: u64 = 1024 * 1024;

/// Highest byte offset a resident pointer can name within a segment
const MAX_SEGMENT_OFFSET: u64 = u32::MAX as u64;

/// No reservation has been given up yet, so the segment ends at its write head
const NO_CUT: u64 = u64::MAX;

/// Where a committed record landed and the sequence number that orders it
///
/// Holding it stands in for the index entry until that entry exists, since compaction
/// reads the index to decide what a segment still holds.
pub struct Committed {
    /// Segment, offset, and length the record occupies
    pub loc: Loc,

    /// Append sequence number the record was written under
    pub lsn: Lsn,

    /// The segment's stay of retirement, given up when this is dropped
    _hold: SegmentHold,
}

/// One reason a segment cannot be retired yet, released on drop
struct SegmentHold {
    shared: Arc<ReelShared>,
    holds: Arc<SegmentHolds>,
    segment: SegmentId,
}

impl Drop for SegmentHold {
    fn drop(&mut self) {
        if self.holds.release_record() {
            self.shared.forget_if_free(self.segment);
        }
    }
}

/// When the sync that makes an append durable is taken
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Commit {
    /// The append takes the sync its policy owes before it returns
    PerRecord,

    /// The append leaves the sync to whoever closes the batch
    Batched,
}

/// What a writer wants appended, resolved to a header once a sequence number is drawn
enum Intent {
    /// A data record carrying a payload, and the codec byte that produced it
    Data(OwnedBuf, u8),

    /// A delete marker for one key, with no payload
    Tombstone,

    /// A delete marker for a key range, carrying the exclusive end of the range
    RangeTombstone(OwnedBuf),
}

/// Where a record's sequence number comes from, and so what kind of write it is
#[derive(Clone, Copy, Eq, PartialEq)]
enum Origin {
    /// A fresh write, drawing the next sequence number of the reel
    Fresh,

    /// Compaction's copy of a record written earlier, under that record's number
    Relocated(Lsn),
}

impl Origin {
    /// The sequence number the record takes, drawing one for a fresh write
    fn lsn(self, counter: &LsnCounter) -> Lsn {
        match self {
            Origin::Fresh => counter.issue(),
            Origin::Relocated(lsn) => lsn,
        }
    }

    /// The flags a record of this origin carries past its kind
    fn applied(self, flags: Flags) -> Flags {
        match self {
            Origin::Fresh => flags,
            Origin::Relocated(_) => flags.relocated(),
        }
    }
}

/// Whether a record belongs to a batch or stands on its own
#[derive(Clone, Copy, Eq, PartialEq)]
enum BatchMark {
    /// Not part of a batch at all, which a batch of one record also is
    Alone,

    /// One of a framed batch's records, which must never be applied alone
    Member,
}

impl BatchMark {
    fn applied(self, flags: Flags) -> Flags {
        match self {
            BatchMark::Alone => flags,
            BatchMark::Member => flags.batched(),
        }
    }
}

/// What one record of a batch does to its key
pub enum BatchWrite {
    /// Land a payload under the key, with the codec byte that produced it
    Put(OwnedBuf, u8),

    /// Drop the key
    Delete,

    /// Drop the half-open range opening at the key, carrying its exclusive end
    ///
    /// No end at all is an empty buffer, the same shape the single-record door takes.
    DeleteRange(OwnedBuf),
}

impl BatchWrite {
    /// Payload bytes the write carries, none for a delete
    fn len(&self) -> usize {
        match self {
            BatchWrite::Put(payload, _) => payload.len(),
            BatchWrite::Delete => 0,
            BatchWrite::DeleteRange(end) => end.len(),
        }
    }
}

/// One record a batch carries
pub struct BatchRecord {
    /// Column and key the record is addressed by
    pub key: RecordKey,

    /// What the record does to that key
    pub write: BatchWrite,
}

/// One segment a tail is appending to, shared by every writer on that tail
struct Active {
    /// The open file every writer on this tail appends into
    handle: SegmentHandle,

    /// Next free byte, moved by a reservation before any bytes are written
    reserved: AtomicU64,

    /// Bytes below the reservation head that have landed or been given up
    settled: AtomicU64,

    /// Lowest reservation that ran past the segment, the offset a seal cuts at
    cut_at: AtomicU64,

    /// End of the space reserved from the filesystem ahead of the write head
    alloc_high: AtomicU64,

    /// Rows for the records written so far, which the seal packs into the footer
    entries: Mutex<SegmentFooter>,

    /// Durability state, shared with whichever writers are flushing this segment
    sync: Arc<SyncState>,

    /// Whether a failed sync has made this segment unwritable
    terminal: AtomicBool,

    /// What stands between this segment and its retirement, drawn with its number
    holds: Arc<SegmentHolds>,
}

impl Active {
    /// The offset a seal cuts this segment at
    fn end(&self) -> u64 {
        self.cut_at
            .load(Ordering::Acquire)
            .min(self.reserved.load(Ordering::Acquire))
    }

    /// Reserve the next span, handing back the base it starts at
    fn claim(&self, span: u64) -> u64 {
        self.reserved.fetch_add(span, Ordering::AcqRel)
    }

    /// Land one claim, however its write went
    ///
    /// Every claim settles exactly once, the turned-away and the failed with the landed,
    /// or the settled count never catches the reservation head.
    fn settle(&self, span: u64) {
        self.settled.fetch_add(span, Ordering::AcqRel);
    }
}

/// One append tail: a reservation head over one active segment
pub struct Appender {
    /// Everything the tails of one reel hold in common
    shared: Arc<ReelShared>,

    /// Watermarks a reader consults before resolving a record here
    tail: Arc<Tail>,

    /// The segment being appended to right now, shared with the sealer
    active: Arc<RwLock<Active>>,

    /// The next segment, drawn before the tail needs it so a roll is a swap
    spare: Mutex<Option<Active>>,

    /// Records in flight against this tail, the routing load signal
    inflight: AtomicU64,

    /// How many writers were in flight as each record went down
    depth: DrainDepth,

    /// The tier this tail's draws go to, fast except when compaction demotes
    draw_class: AtomicU8,

    /// Whether a merge owns this tail, so every segment it draws is merge output
    writes_merge_output: bool,

    /// Seals segments this tail has rolled off, so a writer does not
    sealer: Sealer,
}

/// Writers in flight at the moment a record is written, bucketed
///
/// A batch can only collect writers already in flight together, so this is the ceiling
/// on what batching could save.
#[derive(Debug, Default)]
pub struct DrainDepth {
    buckets: [AtomicU64; DEPTH_BUCKETS],
}

/// Upper bound of each depth bucket, with the last standing for everything above
const DEPTH_BOUNDS: [u64; DEPTH_BUCKETS] = [1, 2, 4, 8, 16, 32, 64, u64::MAX];

const DEPTH_BUCKETS: usize = 8;

impl DrainDepth {
    /// Record that a record went down with this many writers in flight
    fn observe(&self, inflight: u64) {
        let at = DEPTH_BOUNDS
            .iter()
            .position(|bound| inflight <= *bound)
            .unwrap_or(DEPTH_BUCKETS - 1);
        self.buckets[at].fetch_add(1, Ordering::Relaxed);
    }

    /// Counts per bucket, paired with the upper bound each one stands for
    pub fn snapshot(&self) -> Vec<(u64, u64)> {
        DEPTH_BOUNDS
            .iter()
            .enumerate()
            .map(|(at, bound)| (*bound, self.buckets[at].load(Ordering::Relaxed)))
            .collect()
    }

    /// Share of records written with company, which is what a batch could collect
    pub fn batchable_fraction(&self) -> f64 {
        let counts: Vec<u64> = self
            .buckets
            .iter()
            .map(|b| b.load(Ordering::Relaxed))
            .collect();
        let total: u64 = counts.iter().sum();
        if total == 0 {
            return 0.0;
        }
        let alone = counts[0];
        (total - alone) as f64 / total as f64
    }
}

impl Appender {
    /// Open a tail, drawing a fresh segment and writing its header as record zero
    pub fn open(shared: Arc<ReelShared>, index: u64) -> Result<Appender> {
        Appender::start(shared, index, false)
    }

    /// Open a tail of a merge's own, whose segments hold nothing but its output
    ///
    /// A merge writes its runs whole, so a foreground put landing between two of its
    /// records would leave a segment that is not a run. Every segment this draws is
    /// marked as the merge's.
    pub fn open_for_merge(shared: Arc<ReelShared>, index: u64) -> Result<Appender> {
        Appender::start(shared, index, true)
    }

    fn start(shared: Arc<ReelShared>, index: u64, writes_merge_output: bool) -> Result<Appender> {
        let tail = Arc::new(Tail::new(index));
        let driver = Arc::clone(&shared.driver);
        let active = Arc::new(RwLock::new(placeholder_active(driver)));
        let appender = Appender {
            sealer: Sealer::start(Arc::clone(&shared), Arc::clone(&active)),
            shared,
            tail,
            active,
            spare: Mutex::new(None),
            inflight: AtomicU64::new(0),
            depth: DrainDepth::default(),
            draw_class: AtomicU8::new(0),
            writes_merge_output,
        };
        let fresh = appender.prepare_segment()?;
        appender.adopt(&mut write(&appender.active), fresh);
        Ok(appender)
    }

    /// Watermarks a reader consults before resolving a record in this tail
    pub fn tail(&self) -> &Arc<Tail> {
        &self.tail
    }

    /// Records in flight against this tail, the routing load signal
    pub fn load(&self) -> u64 {
        self.inflight.load(Ordering::Relaxed)
    }

    /// How many writers each record went down beside, the batching ceiling
    pub fn drain_depth(&self) -> &DrainDepth {
        &self.depth
    }

    /// Append a data record and await its committed location
    pub fn append_data(
        &self,
        key: RecordKey,
        payload: OwnedBuf,
        codec: u8,
        commit: Commit,
    ) -> Result<Committed> {
        let bytes = framed_bytes(&key, payload.len());
        self.admit(
            bytes,
            key,
            Intent::Data(payload, codec),
            Origin::Fresh,
            commit,
        )
    }

    /// The same append for a caller with a runtime worker to protect
    pub async fn append_data_wait(
        &self,
        key: RecordKey,
        payload: OwnedBuf,
        codec: u8,
        commit: Commit,
    ) -> Result<Committed> {
        let bytes = framed_bytes(&key, payload.len());
        self.admit_wait(
            bytes,
            key,
            Intent::Data(payload, codec),
            Origin::Fresh,
            commit,
        )
        .await
    }

    /// Append a tombstone for one key and await its committed location
    pub fn append_tombstone(&self, key: RecordKey, commit: Commit) -> Result<Committed> {
        self.admit(
            framed_bytes(&key, 0),
            key,
            Intent::Tombstone,
            Origin::Fresh,
            commit,
        )
    }

    /// Append a tombstone covering a half-open key range within one column
    ///
    /// The key is the inclusive start and the payload the exclusive end. No end at all
    /// runs to the top of the column, which a prefix with no successor needs.
    pub fn append_range_tombstone(
        &self,
        start: RecordKey,
        end: Option<&[u8]>,
        commit: Commit,
    ) -> Result<Committed> {
        let end = end.unwrap_or(&[]).to_vec();
        let bytes = framed_bytes(&start, end.len());
        self.admit(
            bytes,
            start,
            Intent::RangeTombstone(end),
            Origin::Fresh,
            commit,
        )
    }

    /// Append a compaction copy under the source record's own sequence number
    ///
    /// Keeping the original number means a crash that leaves both copies sees one
    /// version.
    pub fn append_copy(
        &self,
        key: RecordKey,
        lsn: Lsn,
        payload: OwnedBuf,
        codec: u8,
    ) -> Result<Committed> {
        let bytes = framed_bytes(&key, payload.len());
        self.admit(
            bytes,
            key,
            Intent::Data(payload, codec),
            Origin::Relocated(lsn),
            Commit::Batched,
        )
    }

    /// List a row whose value it holds, writing no record for it
    ///
    /// The value is not durable until the footer is, so the caller must seal the
    /// destination before retiring the source and must not repoint the index at the row
    /// before then.
    pub fn list_carried_row(&self, entry: &FooterEntry) -> Result<SegmentId> {
        let active = read(&self.active);
        lock(&active.entries).push(entry);
        Ok(active.handle.id())
    }

    /// Carry a tombstone into a rewritten segment under its own sequence number
    pub fn append_carried_tombstone(&self, key: RecordKey, lsn: Lsn) -> Result<Committed> {
        self.admit(
            framed_bytes(&key, 0),
            key,
            Intent::Tombstone,
            Origin::Relocated(lsn),
            Commit::Batched,
        )
    }

    /// Carry a range tombstone into a rewritten segment under its own number
    pub fn append_carried_range(
        &self,
        start: RecordKey,
        lsn: Lsn,
        end: OwnedBuf,
    ) -> Result<Committed> {
        let bytes = framed_bytes(&start, end.len());
        self.admit(
            bytes,
            start,
            Intent::RangeTombstone(end),
            Origin::Relocated(lsn),
            Commit::Batched,
        )
    }

    /// Append a whole batch as one reservation, one write, and one frame
    ///
    /// The records take one contiguous range behind a frame declaring how many of them
    /// there are and how many bytes they take, so a crash part way through leaves a run
    /// recovery drops whole. A batch larger than a segment is refused rather than split.
    pub fn append_batch(&self, records: Vec<BatchRecord>) -> Result<Vec<Committed>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let bytes = batch_bytes(&records);

        self.shared.budget.acquire(bytes);
        let _admitted = self.admitted(bytes);
        self.place_batch(records)
    }

    /// The same batch with admission awaited rather than parked on
    pub async fn append_batch_wait(&self, records: Vec<BatchRecord>) -> Result<Vec<Committed>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let bytes = batch_bytes(&records);

        self.shared.budget.reserve(bytes).await;
        let _admitted = self.admitted(bytes);
        self.place_batch(records)
    }

    /// Sync everything the tail has settled, the durability surface
    pub fn flush(&self) -> Result<()> {
        // A segment this tail rolled off is durable only once its footer is down, so a
        // flush waits for the sealer.
        self.sealer.drain();
        let Some(owed) = self.settled_sync() else {
            return Ok(());
        };
        self.sync_up_to(&owed)
    }

    /// The same flush awaited, for a caller with a worker rather than a thread
    pub async fn flush_wait(&self) -> Result<()> {
        self.sealer.drain_wait().await;
        let Some(owed) = self.settled_sync() else {
            return Ok(());
        };
        self.sync_up_to_wait(&owed).await
    }

    /// Take the sync a batch of appends left owed, the end of a batch
    pub fn sync_if_owed(&self) -> Result<()> {
        let Some(owed) = self.owed_sync() else {
            return Ok(());
        };
        self.sync_up_to(&owed)
    }

    /// The same durability point awaited, with the turn forwarded not taken
    pub async fn sync_if_owed_wait(&self) -> Result<()> {
        let Some(owed) = self.owed_sync() else {
            return Ok(());
        };
        self.sync_up_to_wait(&owed).await
    }

    /// The turn behind an owed sync, for a caller that will run it itself
    ///
    /// A writer that finds another already at the device waits here; finding nobody
    /// answers with the turn, which the caller runs where blocking is allowed.
    pub async fn owed_turn(&self) -> Result<Durability> {
        let Some(owed) = self.owed_sync() else {
            return Ok(Durability::Settled);
        };
        self.turn_wait(&owed).await
    }

    /// Run a turn the awaitable wait handed back, on a thread that may block
    pub fn take_turn(&self, mut turn: FlushTurn) -> Result<()> {
        turn.is_taken = true;
        self.run_flush(&turn.owed)?;
        self.sync_up_to(&turn.owed)
    }

    /// Hand a turn to the sealer, which is where an fsync is allowed to block
    ///
    /// How the flush went reaches the caller through the segment's durability state,
    /// not through this call.
    fn forward_turn(&self, mut turn: FlushTurn) -> Result<()> {
        turn.is_taken = true;
        self.sealer.forward(turn.owed.clone())
    }

    /// The segment and byte count an owed sync would have to cover, if one is owed
    fn owed_sync(&self) -> Option<Owed> {
        let active = read(&self.active);
        if !self.owes_sync(&active) {
            return None;
        }
        Some(Owed {
            sync: Arc::clone(&active.sync),
            segment: active.handle.id(),
            target: active.settled.load(Ordering::Acquire),
        })
    }

    /// Everything the tail has settled, or nothing when a flush already covers it
    fn settled_sync(&self) -> Option<Owed> {
        let active = read(&self.active);
        let target = active.settled.load(Ordering::Acquire);
        if target == active.sync.synced_at.load(Ordering::Acquire) {
            return None;
        }
        Some(Owed {
            sync: Arc::clone(&active.sync),
            segment: active.handle.id(),
            target,
        })
    }

    /// Seal the active segment with a footer and roll to a fresh one
    ///
    /// The segment that closed comes back, so a caller that wrote rows nothing points
    /// at can say which segment now speaks for them.
    pub fn seal(&self) -> Result<SegmentId> {
        let retired = {
            let mut active = write(&self.active);
            self.swap_in_fresh(&mut active)?
        };
        // Sealed here rather than handed off: this door promises a sealed segment when
        // it returns.
        let end = retired.end();
        let id = retired.handle.id();
        let sealed = retire_segment(&self.shared, &retired, end);
        if sealed.is_err() {
            // Parked as the sealer's failures are, so the tick finishes what this door
            // started.
            park_broken_seal(&self.shared, retired, end);
        }
        sealed?;
        Ok(id)
    }

    /// Flush, seal the last segment, and give its hold up, for a tail that ends
    ///
    /// The hold is what keeps the maintenance plane off a segment, so a tail that lives
    /// for one pass has to give it up or leak a segment nothing can ever merge, compact
    /// or reclaim again. A failed seal keeps the hold.
    pub fn finish(&self) -> Result<()> {
        self.flush()?;
        self.close()?;
        // Read after the close, so the segment released is the one the close sealed.
        self.shared.release_segment(self.tail.active_segment());
        Ok(())
    }

    /// Seal the active segment and stop the tail, for a clean shutdown
    ///
    /// Sealing on the way out is what tells a shutdown from a crash on disk. The tail is
    /// left with nothing to append to, so a write that still arrives rolls first.
    pub fn close(&self) -> Result<()> {
        if let Some(spare) = lock(&self.spare).take() {
            let drawn = spare.handle.id();
            spare.handle.mark_doomed();
            // The file goes, so a hold or a mark left behind would stop the held floor
            // ever advancing past a segment that is not there.
            self.shared.release_segment(drawn);
            self.shared.forget_merge_output(drawn);
        }
        let active = write(&self.active);
        if active.terminal.load(Ordering::Acquire) {
            return Ok(());
        }
        let end = active.end();
        let sealed = self.seal_active(&active, end);
        active.terminal.store(true, Ordering::Release);
        match &sealed {
            Ok(()) => active.sync.mark_durable(),
            Err(_) => active.sync.mark_broken(),
        }
        sealed
    }

    fn admit(
        &self,
        bytes: u64,
        key: RecordKey,
        intent: Intent,
        origin: Origin,
        commit: Commit,
    ) -> Result<Committed> {
        self.shared.budget.acquire(bytes);
        let _admitted = self.admitted(bytes);
        let (committed, owed) = self.place(key, intent, origin, commit)?;
        if let Some(owed) = owed {
            // A failed sync leaves nothing making the record durable, so the writer is
            // told rather than handed a location the next crash could take back.
            self.sync_up_to(&owed)?;
        }
        Ok(committed)
    }

    /// The same admission awaited, and the same durability point forwarded
    async fn admit_wait(
        &self,
        bytes: u64,
        key: RecordKey,
        intent: Intent,
        origin: Origin,
        commit: Commit,
    ) -> Result<Committed> {
        self.shared.budget.reserve(bytes).await;
        let _admitted = self.admitted(bytes);
        let (committed, owed) = self.place(key, intent, origin, commit)?;
        if let Some(owed) = owed {
            self.sync_up_to_wait(&owed).await?;
        }
        Ok(committed)
    }

    /// Count one record in flight, counted back out however the caller leaves
    ///
    /// The async door may leave by being dropped at either of its waits, so the release
    /// rides on a value rather than on reaching the end of the call.
    fn admitted(&self, bytes: u64) -> Admission<'_> {
        self.inflight.fetch_add(1, Ordering::Relaxed);
        Admission { tail: self, bytes }
    }

    /// Reserve room for one record, write it there, and hand back where it landed
    ///
    /// A reservation that runs past the segment is given up and the tail rolls, which is
    /// the only case that loops. The owed sync comes back rather than being taken here,
    /// drawn with the tail held so it names the segment the record is in.
    fn place(
        &self,
        key: RecordKey,
        intent: Intent,
        origin: Origin,
        commit: Commit,
    ) -> Result<(Committed, Option<Owed>)> {
        // Gauged before the draw, so no instant holds a number neither the gauge nor a
        // segment hold accounts for.
        let mut drawn = (origin == Origin::Fresh).then(|| self.shared.draw_gauge(1));
        let lsn = origin.lsn(&self.shared.lsn);
        let (header, mut payload) = build_record(key, lsn, intent, BatchMark::Alone, origin);
        let span = self.reserved_span(&header);
        let target = self.shared.config.segment_bytes.to_bytes();

        // A record wider than a whole segment fits in none, so the roll below would draw
        // a fresh one, fail the same test, and roll again for as long as there is room.
        if span + ALIGN > target {
            return Err(ReelError::Rejected(format!(
                "a record of {span} bytes does not fit a reel segment of {target}"
            )));
        }

        loop {
            let active = read(&self.active);
            if active.terminal.load(Ordering::Acquire) {
                drop(active);
                self.roll_terminal()?;
                continue;
            }
            let base = active.claim(span);
            if base + span + ALIGN <= target && base + span <= MAX_SEGMENT_OFFSET {
                // The hold is taken with the tail still held shared, so the roll that
                // seals this segment cannot come between the record landing and the
                // maintenance plane being told to wait for it. The gauge is given back
                // only after the hold is up, leaving the prune floor no gap to read.
                active.holds.hold_record();
                drawn.take();
                let hold = SegmentHold {
                    shared: Arc::clone(&self.shared),
                    holds: Arc::clone(&active.holds),
                    segment: active.handle.id(),
                };
                let outcome =
                    self.write_record(&active, base, &header, std::mem::take(&mut payload));
                active.settle(span);
                // A write that failed leaves its range unwritten in the middle of the
                // segment. Stamped as fill, the range is one record a recovery walk
                // hops; only when the stamp will not land either does the seal cut
                // below it and carry the loss.
                if outcome.is_err() {
                    match stamp_failed_range(&self.shared, &active, base, span) {
                        true => self.tail.publish_committed(base + span),
                        false => {
                            active.cut_at.fetch_min(base, Ordering::AcqRel);
                            // An unstamped hole strands whatever commits above it until
                            // the seal.
                            self.shared.note_past_saving();
                        }
                    }
                }
                let loc = outcome?;
                // The bytes are down, so the segment can surface this number whether or
                // not the caller stays to publish it.
                self.shared.note_landed(loc.segment, lsn);
                self.tail.publish_committed(base + span);
                self.step_reservation(&active, base + span);
                self.paced_writeback(&active);
                let is_owed = commit == Commit::PerRecord && self.owes_sync(&active);
                let owed = is_owed.then(|| Owed {
                    sync: Arc::clone(&active.sync),
                    segment: active.handle.id(),
                    target: base + span,
                });
                let wants_spare = base + span + self.spare_margin() >= target;
                drop(active);
                if wants_spare {
                    self.prepare_spare();
                }
                return Ok((
                    Committed {
                        loc,
                        lsn,
                        _hold: hold,
                    },
                    owed,
                ));
            }

            // The reservation ran past the segment, so it names bytes that will never be
            // written and the seal has to cut below it.
            active.cut_at.fetch_min(base, Ordering::AcqRel);
            active.settle(span);
            let doomed = active.handle.id().as_u32();
            drop(active);
            self.roll_from(doomed)?;
        }
    }

    /// Reserve one range for a whole batch, write it, and hand back where it landed
    ///
    /// One reservation covers the frame and every record, so the run is contiguous and
    /// nothing another writer appends falls inside it. That is also what keeps a batch
    /// inside one segment: a reservation that runs past the segment is given up whole
    /// and retaken on the next one.
    fn place_batch(&self, records: Vec<BatchRecord>) -> Result<Vec<Committed>> {
        let count = records.len();
        // Gauged before the first draw, given back once every record of the run holds
        // its segment.
        let mut drawn = Some(self.shared.draw_gauge(count as u64));
        let mut headers = Vec::with_capacity(count);
        let mut payloads = Vec::with_capacity(count);
        // A batch of one is a plain record: there is no middle for a crash to land in,
        // so it carries neither a frame nor a mark and pays for neither.
        let mark = match count > 1 {
            true => BatchMark::Member,
            false => BatchMark::Alone,
        };
        for record in records {
            let lsn = self.shared.lsn.issue();
            let intent = match record.write {
                BatchWrite::Put(payload, codec) => Intent::Data(payload, codec),
                BatchWrite::Delete => Intent::Tombstone,
                BatchWrite::DeleteRange(end) => Intent::RangeTombstone(end),
            };
            let (header, payload) = build_record(record.key, lsn, intent, mark, Origin::Fresh);
            headers.push(header);
            payloads.push(payload);
        }

        let run: u64 = headers.iter().map(|header| header.span()).sum();
        let frame = (mark == BatchMark::Member).then_some(BatchFrame {
            count: count as u32,
            span: run,
        });
        let framed = run + frame.map_or(0, |_| BatchFrame::SPAN);
        let span = self.reserved_batch_span(framed);
        let target = self.shared.config.segment_bytes.to_bytes();
        if span + ALIGN > target {
            return Err(ReelError::Rejected(format!(
                "a batch of {span} bytes does not fit a reel segment"
            )));
        }

        loop {
            let active = read(&self.active);
            if active.terminal.load(Ordering::Acquire) {
                drop(active);
                self.roll_terminal()?;
                continue;
            }
            let base = active.claim(span);
            if base + span + ALIGN <= target && base + span <= MAX_SEGMENT_OFFSET {
                let mut committed = Vec::with_capacity(count);
                for header in &headers {
                    active.holds.hold_record();
                    let hold = SegmentHold {
                        shared: Arc::clone(&self.shared),
                        holds: Arc::clone(&active.holds),
                        segment: active.handle.id(),
                    };
                    committed.push((header.lsn, hold));
                }
                drawn.take();
                let outcome = self.write_run(
                    &active,
                    base,
                    frame,
                    &headers,
                    std::mem::take(&mut payloads),
                    framed,
                );
                active.settle(span);
                // The same stamp a single record leaves, over the whole reservation.
                if outcome.is_err() {
                    match stamp_failed_range(&self.shared, &active, base, span) {
                        true => self.tail.publish_committed(base + span),
                        false => {
                            active.cut_at.fetch_min(base, Ordering::AcqRel);
                            // An unstamped hole strands whatever commits above it until
                            // the seal.
                            self.shared.note_past_saving();
                        }
                    }
                }
                let locs = outcome?;
                // The run took one reservation in one segment and its numbers were
                // issued in order, so the frame's own is the oldest of them.
                if let Some(first) = headers.first() {
                    self.shared.note_landed(active.handle.id(), first.lsn);
                }
                self.tail.publish_committed(base + span);
                self.step_reservation(&active, base + span);
                self.paced_writeback(&active);
                let wants_spare = base + span + self.spare_margin() >= target;
                drop(active);
                if wants_spare {
                    self.prepare_spare();
                }
                return Ok(committed
                    .into_iter()
                    .zip(locs)
                    .map(|((lsn, hold), loc)| Committed {
                        loc,
                        lsn,
                        _hold: hold,
                    })
                    .collect());
            }

            active.cut_at.fetch_min(base, Ordering::AcqRel);
            active.settle(span);
            let doomed = active.handle.id().as_u32();
            drop(active);
            self.roll_from(doomed)?;
        }
    }

    /// Write a run of records into one reservation with a single vectored write
    ///
    /// The frame goes down in the same write as the records it declares, ahead of them,
    /// so nothing can leave a frame standing over a run that was never written.
    fn write_run(
        &self,
        active: &Active,
        base: u64,
        frame: Option<BatchFrame>,
        headers: &[RecordHeader],
        payloads: Vec<OwnedBuf>,
        framed: u64,
    ) -> Result<Vec<Loc>> {
        // Three buffers a record rather than two, since a spilled key rides in one of its
        // own between the header and the payload, and one more for the frame.
        let mut bufs = take_bufs(headers.len() * 3 + 3);
        let mut locs = Vec::with_capacity(headers.len());
        let mut entries = Vec::with_capacity(headers.len());
        let mut at = base;
        if let Some(frame) = frame {
            // Staged rather than owned, so a frame costs the batch no allocation.
            WriteBuf::push_prefix(&mut bufs, frame.pack());
            at += BatchFrame::SPAN;
        }
        for (header, payload) in headers.iter().zip(payloads) {
            let row_carry = self.shared.row_carry(header.key.column);
            if let Some(entry) =
                FooterEntry::from_record(header, at as u32, payload.as_slice(), row_carry)
            {
                entries.push(entry);
            }
            WriteBuf::push_prefix(&mut bufs, header.pack());
            if header.has_payload() {
                bufs.push(WriteBuf::owned(payload));
            }
            locs.push(Loc::new(active.handle.id(), at as u32, header.length));
            at += header.span();
        }

        let mut written = framed;
        if self.shared.writes_whole_blocks() {
            let pad = RecordHeader::fill(self.batch_fill(framed));
            WriteBuf::push_prefix(&mut bufs, pad.pack());
            bufs.push(WriteBuf::zeros(pad.length as usize));
            written += pad.span();
        }

        self.depth.observe(self.inflight.load(Ordering::Relaxed));
        let (wrote, bufs) = self
            .shared
            .driver
            .writev_reusing(active.handle.file(), base, bufs)?;
        recycle_bufs(bufs);
        if wrote != written {
            return Err(ReelError::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "a batch wrote fewer bytes than it framed",
            )));
        }
        let mut pending = lock(&active.entries);
        for entry in &entries {
            pending.push(entry);
        }
        Ok(locs)
    }

    /// Bytes a whole batch holds, including the pad a whole-block volume adds
    fn reserved_batch_span(&self, framed: u64) -> u64 {
        if self.shared.writes_whole_blocks() {
            return align_up(framed + HEADER_LEN as u64, ALIGN);
        }
        framed
    }

    /// Fill the closing pad a batch needs to reach the next block boundary
    fn batch_fill(&self, framed: u64) -> u32 {
        (self.reserved_batch_span(framed) - framed - HEADER_LEN as u64) as u32
    }

    /// Bytes one record holds, including the pad a whole-block volume adds
    ///
    /// A segment stops taking records a block short of its target, so the seal has room
    /// for the closing pad and the footer behind it.
    fn reserved_span(&self, header: &RecordHeader) -> u64 {
        let span = header.span();
        if self.shared.writes_whole_blocks() {
            // The gap left over has to hold the pad header that names it, or it reads
            // back as unwritten space and stops a scan at the record after it.
            return align_up(span + HEADER_LEN as u64, ALIGN);
        }
        span
    }

    /// Write one record into the range a reservation named
    fn write_record(
        &self,
        active: &Active,
        base: u64,
        header: &RecordHeader,
        payload: OwnedBuf,
    ) -> Result<Loc> {
        let row_carry = self.shared.row_carry(header.key.column);
        let listed = FooterEntry::from_record(header, base as u32, payload.as_slice(), row_carry);

        // Five rather than four, since a spilled key rides in a buffer of its own.
        let mut bufs = take_bufs(5);
        WriteBuf::push_prefix(&mut bufs, header.pack());
        if header.has_payload() {
            bufs.push(WriteBuf::owned(payload));
        }
        let mut framed = header.span();
        if self.shared.writes_whole_blocks() {
            let pad = RecordHeader::pad(base + framed);
            WriteBuf::push_prefix(&mut bufs, pad.pack());
            bufs.push(WriteBuf::zeros(pad.length as usize));
            framed += pad.span();
        }

        self.depth.observe(self.inflight.load(Ordering::Relaxed));
        let (wrote, bufs) = self
            .shared
            .driver
            .writev_reusing(active.handle.file(), base, bufs)?;
        recycle_bufs(bufs);
        if wrote != framed {
            return Err(ReelError::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "a record wrote fewer bytes than it framed",
            )));
        }
        if let Some(entry) = listed {
            lock(&active.entries).push(&entry);
        }
        Ok(Loc::new(active.handle.id(), base as u32, header.length))
    }

    fn owes_sync(&self, active: &Active) -> bool {
        let settled = active.settled.load(Ordering::Acquire);
        match self.shared.config.sync {
            SyncPolicy::Never => false,
            SyncPolicy::EveryPut => true,
            SyncPolicy::Bytes(threshold) => {
                let synced = active.sync.synced_at.load(Ordering::Acquire);
                settled.saturating_sub(synced) >= threshold.to_bytes()
            }
        }
    }

    /// Make everything the segment has settled below a byte position durable
    ///
    /// The target is a byte count rather than a record, so one flush answers for every
    /// writer already under it. The loop is what a flush that started too early costs:
    /// it covers less than was asked for, and its waiters take a turn of their own.
    fn sync_up_to(&self, owed: &Owed) -> Result<()> {
        loop {
            match owed
                .sync
                .flush
                .park(|flush| turn_at(&owed.sync, flush, owed.target))
            {
                Turn::Settled => return Ok(()),
                Turn::Broken => return Err(not_durable(owed.segment)),
                Turn::Owed => self.run_flush(owed)?,
            }
        }
    }

    /// The same loop awaited, with every turn it draws handed to the sealer
    async fn sync_up_to_wait(&self, owed: &Owed) -> Result<()> {
        loop {
            match self.turn_wait(owed).await? {
                Durability::Settled => return Ok(()),
                Durability::Owed(turn) => self.forward_turn(turn)?,
            }
        }
    }

    /// Wait for one durability point, answering with the turn if it falls to us
    async fn turn_wait(&self, owed: &Owed) -> Result<Durability> {
        let turn = owed
            .sync
            .flush
            .wait(|flush| turn_at(&owed.sync, flush, owed.target))
            .await;
        match turn {
            Turn::Settled => Ok(Durability::Settled),
            Turn::Broken => Err(not_durable(owed.segment)),
            Turn::Owed => Ok(Durability::Owed(FlushTurn {
                owed: owed.clone(),
                is_taken: false,
            })),
        }
    }

    /// Run the flush this caller holds the turn for, and hand the turn back
    ///
    /// What comes back says the turn is over, not that the target is durable, so a
    /// caller a short flush left wanting goes round again.
    fn run_flush(&self, owed: &Owed) -> Result<()> {
        let flushed = flush_active(&self.shared, &self.active, owed.segment);
        publish_flush(owed, &flushed);
        match flushed {
            Ok(_) => Ok(()),
            Err(error) => {
                self.doom(owed.segment);
                Err(error)
            }
        }
    }

    /// Start the device on what has settled
    ///
    /// The ask is not waited on.
    fn paced_writeback(&self, active: &Active) {
        if matches!(self.shared.config.sync, SyncPolicy::EveryPut) {
            return;
        }
        let settled = active.settled.load(Ordering::Acquire);
        // Another writer is already pacing this segment, and the next writer through
        // takes the chunk it does not.
        let Some(mut pacing) = try_lock(&active.sync.pacing) else {
            return;
        };

        // The ask is rare next to the puts through here, so what is due is settled
        // before anything is allocated to carry it.
        if settled.saturating_sub(pacing.started_to) < WRITEBACK_CHUNK {
            return;
        }

        let ops = vec![Op::SyncRange {
            tag: self.shared.driver.next_tag(),
            file: active.handle.file(),
            offset: pacing.started_to,
            len: settled - pacing.started_to,
            mode: SyncRangeMode::Write,
        }];
        pacing.started_to = settled;
        drop(pacing);
        let _ = self.shared.driver.run(ops);
    }

    /// Roll the tail off a segment a writer could not fit in
    ///
    /// Taking the segment exclusively is itself the wait for the reservations still in
    /// flight, since every writer holds it shared until its write has landed.
    fn roll_from(&self, doomed: u32) -> Result<()> {
        let retired = {
            let mut active = write(&self.active);
            if active.handle.id().as_u32() != doomed {
                return Ok(());
            }
            self.swap_in_fresh(&mut active)?
        };
        self.retire(retired)
    }

    /// Roll off a segment whose sync failed, leaving it for recovery to read back
    ///
    /// The doomed segment is left unsealed on purpose: its footer would claim its
    /// records are all there, which is what a failed sync cannot promise.
    fn roll_terminal(&self) -> Result<()> {
        let doomed = {
            let mut active = write(&self.active);
            if !active.terminal.load(Ordering::Acquire) {
                return Ok(());
            }
            self.swap_in_fresh(&mut active)?
        };
        // Nothing more will be made durable here, so a writer still waiting on the
        // segment hears that rather than waiting on a flush nobody will take.
        doomed.sync.mark_broken();
        // No footer and its pages still dirty, so a window must not read it around the
        // cache once the holds come off.
        self.shared.note_unsealed(doomed.handle.id());
        self.shared.release_segment(doomed.handle.id());
        Ok(())
    }

    /// Put a fresh segment in place of the active one and hand the old one back
    ///
    /// The outgoing segment stays claimed until its seal has landed, so the maintenance
    /// plane leaves alone a segment that is neither being appended to nor finished.
    fn swap_in_fresh(&self, active: &mut Active) -> Result<Active> {
        let fresh = match lock(&self.spare).take() {
            Some(spare) => spare,
            None => self.prepare_segment()?,
        };
        let retiring = std::mem::replace(active, fresh);
        self.adopt_locked(active);
        Ok(retiring)
    }

    /// Hand a segment the tail rolled off to the sealer
    ///
    /// The writer that filled it did not ask for a seal, so it does not pay for one.
    fn retire(&self, retired: Active) -> Result<()> {
        let end = retired.end();
        self.sealer.hand_over(retired, end)
    }

    /// Start the tail on a segment, resetting its watermarks to the segment header
    fn adopt(&self, active: &mut Active, fresh: Active) {
        *active = fresh;
        self.adopt_locked(active);
    }

    fn adopt_locked(&self, active: &Active) {
        self.tail.begin_segment(active.handle.id());
        self.tail
            .publish_committed(active.settled.load(Ordering::Acquire));
    }

    /// Give up on a segment whose sync failed and roll the tail to a fresh one
    ///
    /// A tail that has already rolled off it is left alone, since the failure belongs to
    /// the segment rather than to whichever one replaced it.
    fn doom(&self, segment: SegmentId) {
        if !doom_active(&self.active, segment) {
            return;
        }
        let _ = self.roll_terminal();
    }

    /// Close a segment with its footer, leaving it readable and never written again
    ///
    /// One sync closes it, taken after the footer rather than one on each side: a footer
    /// describing records that never landed reads back as a checksum failure.
    fn seal_active(&self, active: &Active, end: u64) -> Result<()> {
        seal_segment(&self.shared, active, end)
    }

    /// Reserve the next chunk of space once the write head is closing on the last
    ///
    /// The step has to stay ahead of the writers, or records land in a hole and the
    /// filesystem picks extents one write at a time. One writer takes the step, the rest
    /// keep writing.
    fn step_reservation(&self, active: &Active, head: u64) {
        if !matches!(self.shared.config.preallocate, Preallocate::Chunk) {
            return;
        }
        let chunk = self.shared.config.alloc_chunk.to_bytes();
        let target = self.shared.config.segment_bytes.to_bytes();
        let reserved = active.alloc_high.load(Ordering::Acquire);
        if reserved >= target || head + chunk / 2 < reserved {
            return;
        }

        let next = align_up((reserved + chunk).min(target), ALIGN);
        // Only the writer that moves the mark reserves, so a burst of writers past it
        // does not become a burst of identical reservation calls.
        if active
            .alloc_high
            .compare_exchange(reserved, next, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        if let Err(error) =
            self.shared
                .driver
                .allocate(active.handle.file(), reserved, next - reserved)
        {
            // The space is not reserved after all, so the mark goes back and the next
            // writer tries again rather than writing into a hole it believes is claimed.
            active.alloc_high.store(reserved, Ordering::Release);
            tracing::warn!("failed to reserve the next chunk of a reel segment: {error}");
        }
    }

    /// How close to the end of a segment the tail draws the one that follows it
    fn spare_margin(&self) -> u64 {
        (self.shared.config.segment_bytes.to_bytes() / 16).min(WRITEBACK_CHUNK)
    }

    /// Draw the segment the tail will roll to next, before it needs it
    ///
    /// Opening a segment costs a creation, a space reservation and the header write, so
    /// paying that inside the roll would hold every writer on the tail behind it. The
    /// lock is tried rather than taken, since only the first writer in has work to do.
    fn prepare_spare(&self) {
        if self.is_class_managed() {
            return;
        }
        let Some(mut spare) = try_lock(&self.spare) else {
            return;
        };
        if spare.is_some() {
            return;
        }
        match self.prepare_segment() {
            Ok(fresh) => *spare = Some(fresh),
            Err(error) => {
                tracing::warn!("failed to draw the segment a reel tail rolls to next: {error}");
            }
        }
    }

    /// Draw a fresh segment, reserve its space, and write its header as record zero
    ///
    /// Drawing the number claims it, so a segment that never came together gives it
    /// back: nothing was written there and no seal will ever come to release it.
    fn prepare_segment(&self) -> Result<Active> {
        let (id, holds) = self.shared.next_segment()?;
        let prepared = self.place_and_build(id, holds);
        if prepared.is_err() {
            self.shared.release_segment(id);
            return prepared;
        }
        // Marked at the draw rather than at the first record, because the seal reads it
        // and a segment can roll and seal while the pass that filled it writes on.
        if self.writes_merge_output {
            self.shared.note_merge_output(id);
        }
        prepared
    }

    /// The volume this tail draws on, one tail per fast volume in tail order
    ///
    /// Tails past the fast list, and the rewriter's reserved tail behind them, are
    /// unpinned: their draws take the most free volume in class.
    fn pinned_volume(&self) -> Option<usize> {
        self.shared.volumes.fast_at(self.tail.index() as usize)
    }

    /// Place a fresh segment where the draw policy says, hopping a full volume
    ///
    /// ENOSPC is the one failure a draw survives by going elsewhere; everything else
    /// reports as it happened.
    fn place_and_build(&self, id: SegmentId, holds: Arc<SegmentHolds>) -> Result<Active> {
        let volumes = &self.shared.volumes;
        let class = self.draw_class();
        let mut at = volumes.draw(self.pinned_volume(), class);
        let mut ruled_out = Vec::new();
        loop {
            volumes.place(id, at);
            let error = match self.build_segment(id, Arc::clone(&holds)) {
                Ok(active) => return Ok(active),
                Err(error) => error,
            };
            if !error.is_full() {
                return Err(error);
            }
            ruled_out.push(at);
            let Some(next) = volumes.draw_past(&ruled_out, class) else {
                return Err(error);
            };
            self.scrap_attempt(id)?;
            at = next;
        }
    }

    /// The tier this tail's draws go to, fast unless compaction said otherwise
    fn draw_class(&self) -> VolumeClass {
        match self.draw_class.load(Ordering::Relaxed) {
            0 => VolumeClass::Fast,
            _ => VolumeClass::Capacity,
        }
    }

    /// Point the next draw at a tier, meaningful only on the reserved tail
    pub fn set_draw_class(&self, class: VolumeClass) {
        let raw = match class {
            VolumeClass::Fast => 0,
            VolumeClass::Capacity => 1,
        };
        self.draw_class.store(raw, Ordering::Relaxed);
    }

    /// Whether this tail's draws answer to a per-pass tier
    ///
    /// Such a tail skips the spare drawn ahead, since a spare built under one tier would
    /// be the wrong file the moment a pass names the other.
    fn is_class_managed(&self) -> bool {
        self.shared.volumes.has_capacity()
            && self.tail.index() as usize == self.shared.config.tail_count()
    }

    /// Remove what a failed creation left behind, before its id moves volumes
    ///
    /// An id standing on two volumes refuses the next open, so the retry only proceeds
    /// once the scrap is gone and its directory has said so.
    fn scrap_attempt(&self, id: SegmentId) -> Result<()> {
        let driver = &self.shared.driver;
        match driver.unlink(&self.shared.segment_path(id)) {
            Ok(()) => driver.sync_dir(self.shared.segment_dir(id)),
            Err(error) if error.is_missing() => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn build_segment(&self, id: SegmentId, holds: Arc<SegmentHolds>) -> Result<Active> {
        let path = self.shared.segment_path(id);
        let file = self.shared.driver.open(&path, true)?;
        // A file's own sync says nothing about the directory entry naming it, so one
        // directory sync per segment closes that.
        self.shared.driver.sync_dir(self.shared.segment_dir(id))?;
        let handle = SegmentHandle::new(id, path, file, Arc::clone(&self.shared.driver));

        let active = Active {
            handle,
            reserved: AtomicU64::new(0),
            settled: AtomicU64::new(0),
            cut_at: AtomicU64::new(NO_CUT),
            alloc_high: AtomicU64::new(0),
            entries: Mutex::new(SegmentFooter::empty()),
            sync: Arc::new(SyncState::new()),
            terminal: AtomicBool::new(false),
            holds,
        };
        active
            .alloc_high
            .store(self.preallocate(&active)?, Ordering::Release);

        let payload = SegmentHeader::new(id).pack().to_vec();
        let header = RecordHeader::segment_header(&payload);
        let span = self.reserved_span(&header);
        active.reserved.store(span, Ordering::Release);
        let written = self.write_record(&active, 0, &header, payload);
        active.settled.store(span, Ordering::Release);
        written?;
        Ok(active)
    }

    fn preallocate(&self, active: &Active) -> Result<u64> {
        let target = self.shared.config.segment_bytes.to_bytes();
        let reserve = match self.shared.config.preallocate {
            Preallocate::Full => target,
            Preallocate::Chunk => self.shared.config.alloc_chunk.to_bytes().min(target),
        };
        let reserve = align_up(reserve, ALIGN).min(align_up(target, ALIGN));
        self.shared
            .driver
            .allocate(active.handle.file(), 0, reserve)?;
        Ok(reserve)
    }
}

/// One record's place in the volume's admission, released however the caller leaves
struct Admission<'tail> {
    tail: &'tail Appender,
    bytes: u64,
}

impl Drop for Admission<'_> {
    fn drop(&mut self) {
        self.tail.inflight.fetch_sub(1, Ordering::Relaxed);
        self.tail.shared.budget.release(self.bytes);
    }
}

thread_local! {
    /// One vectored-write list per writing thread, handed back after every write
    ///
    /// The buffers come off the completion either way, so the list a write is framed
    /// into is the same one the last write on this thread framed into.
    static BUFS_SPARE: std::cell::Cell<Vec<WriteBuf>> = const { std::cell::Cell::new(Vec::new()) };
}

/// This thread's write list with room for a write of this width, or a fresh one
fn take_bufs(wanted: usize) -> Vec<WriteBuf> {
    let mut bufs = BUFS_SPARE.with(|held| held.take());
    bufs.reserve(wanted);
    bufs
}

/// Hand a write list back for the next write on this thread to frame into
///
/// Cleared on the way in, so the payloads it carried are released here rather than
/// held until this thread writes again.
fn recycle_bufs(mut bufs: Vec<WriteBuf>) {
    bufs.clear();
    BUFS_SPARE.with(|held| {
        let spare = held.take();
        // The roomier of the two is kept, since a batch frames far wider than a
        // single record and a thread that batches once will batch again.
        held.set(match spare.capacity() > bufs.capacity() {
            true => spare,
            false => bufs,
        });
    });
}

/// Bytes a whole batch takes against admission, framing and all
fn batch_bytes(records: &[BatchRecord]) -> u64 {
    let mut bytes = match records.len() > 1 {
        true => BatchFrame::SPAN,
        false => 0,
    };
    for record in records {
        bytes += framed_bytes(&record.key, record.write.len());
    }
    bytes
}

/// A segment that can no longer be made durable, for a writer waiting on one
fn not_durable(segment: SegmentId) -> ReelError {
    ReelError::Io(std::io::Error::other(format!(
        "reel segment {} could not be made durable",
        segment.as_u32()
    )))
}

/// Bytes a record with this key and payload takes on disk
fn framed_bytes(key: &RecordKey, payload_len: usize) -> u64 {
    HEADER_LEN as u64 + u64::from(key.width()) + payload_len as u64
}

/// Resolve an intent to the header and payload the record writes
///
/// The batch mark goes on before the checksum, which covers the flags.
fn build_record(
    key: RecordKey,
    lsn: Lsn,
    intent: Intent,
    mark: BatchMark,
    origin: Origin,
) -> (RecordHeader, OwnedBuf) {
    let flags = |kind: Flags| origin.applied(mark.applied(kind));
    match intent {
        Intent::Data(payload, codec) => (
            RecordHeader::new_coded(
                payload.len() as u32,
                lsn,
                flags(Flags::DATA),
                key,
                codec,
                &payload,
            ),
            payload,
        ),
        Intent::Tombstone => (
            RecordHeader::new(0, lsn, flags(Flags::TOMBSTONE), key, &[]),
            Vec::new(),
        ),
        Intent::RangeTombstone(end) => (
            RecordHeader::new(
                end.len() as u32,
                lsn,
                flags(Flags::RANGE_TOMBSTONE),
                key,
                &end,
            ),
            end,
        ),
    }
}

fn placeholder_active(driver: Arc<IoDriver>) -> Active {
    Active {
        handle: SegmentHandle::placeholder(driver),
        reserved: AtomicU64::new(0),
        settled: AtomicU64::new(0),
        cut_at: AtomicU64::new(NO_CUT),
        alloc_high: AtomicU64::new(0),
        entries: Mutex::new(SegmentFooter::empty()),
        sync: Arc::new(SyncState::new()),
        terminal: AtomicBool::new(false),
        holds: Arc::default(),
    }
}
